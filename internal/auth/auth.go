// Package auth 提供控制面登录所需的 RADIUS 认证与 JWT 会话能力。
//
// 登录流程：前端 POST /api/auth/login {username, password, otp} → 本包用公司 RADIUS
// 做 PAP 认证（User-Password = 密码+动态码 拼接，OTP 模式为 concat）→ 认证通过则签发
// 短时效 HS256 JWT；后续受保护接口凭 Authorization: Bearer <jwt> 鉴权。
//
// RADIUS 实现说明：直接构造 Access-Request 并调用 *radius.Client.Exchange，超时/重试自管
// （基于 context + 重试循环），不依赖各 RADIUS 库的便利方法，降低跨版本 API 漂移风险。
package auth

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"layeh.com/radius"
	"layeh.com/radius/rfc2865"
)

// RadiusConfig 单次 RADIUS 认证所需参数（由运行时配置 auth.radius 映射而来）。
type RadiusConfig struct {
	ServerHost    string // RADIUS 服务器地址
	AuthPort      uint16 // 认证端口，通常 1812
	SharedSecret  string // 共享密钥
	TimeoutMs     int    // 单次请求超时（毫秒）
	Retries       int    // 失败重试次数（至少 1）
	CaseSensitive bool   // 用户名是否大小写敏感；false 时统一转小写
}

// RadiusAuthenticate 对 RADIUS 服务器发起 PAP 认证，动态码以「密码+动态码拼接」形式放入
// User-Password 属性（多数公司服务的约定）。
//
// 返回值语义：
//   - (true, nil)  → Access-Accept（凭证正确）
//   - (false, nil) → Access-Reject（用户名/密码/动态码错误）
//   - (false, err) → 通信故障（网络不可达、超时、服务器无响应等）
func RadiusAuthenticate(ctx context.Context, cfg RadiusConfig, username, password, otp string) (bool, error) {
	secret := []byte(cfg.SharedSecret)
	addr := net.JoinHostPort(cfg.ServerHost, strconv.Itoa(int(cfg.AuthPort)))

	// 直接构造 *radius.Client（该版本无 Dial/Close，Client 为纯配置结构体，按请求复用即可）。
	// Retry 设为 0：不在库内自动重发，由外层循环按 Retries 控制重试，每次重试带独立超时。
	client := &radius.Client{
		Net:             "udp",
		MaxPacketErrors: 10,
	}

	name := username
	if !cfg.CaseSensitive {
		name = strings.ToLower(username)
	}
	pw := password
	if otp != "" {
		pw = password + otp
	}

	packet := radius.New(radius.CodeAccessRequest, secret)
	if err := rfc2865.UserName_AddString(packet, name); err != nil {
		return false, fmt.Errorf("radius set username: %w", err)
	}
	if err := rfc2865.UserPassword_AddString(packet, pw); err != nil {
		return false, fmt.Errorf("radius set password: %w", err)
	}

	retries := cfg.Retries
	if retries < 1 {
		retries = 1
	}
	timeout := time.Duration(cfg.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, e := client.Exchange(reqCtx, packet, addr)
		cancel()
		if e != nil {
			lastErr = e
			continue // 网络/超时错误：按重试次数重试
		}
		return resp.Code == radius.CodeAccessAccept, nil
	}
	return false, fmt.Errorf("radius exchange failed after %d attempts: %w", retries, lastErr)
}

// Claims 是登录 JWT 的载荷。
type Claims struct {
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// IssueJWT 签发一个短时效的登录 JWT（HS256）。secret 为空会导致签发失败。
func IssueJWT(username, secret string, ttl time.Duration) (string, error) {
	if secret == "" {
		return "", fmt.Errorf("jwt secret is empty")
	}
	now := time.Now()
	claims := Claims{
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   username,
			Issuer:    "nacos-sync-backend",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return tok.SignedString([]byte(secret))
}

// ValidateJWT 校验登录 JWT，返回载荷或错误（过期 / 签名错误 / 格式错误）。
func ValidateJWT(tokenStr, secret string) (*Claims, error) {
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}
