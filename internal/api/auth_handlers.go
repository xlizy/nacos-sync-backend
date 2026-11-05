// Package api 的控制面登录相关路由与鉴权中间件。
package api

import (
	"context"
	"crypto/subtle"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/auth"
	"github.com/cxcm/nacos-sync-backend/internal/config"
)

const (
	jwtBearerPrefix = "Bearer "
	// loginTokenTTL 登录 JWT 有效期：12h，前端按需重新登录。
	loginTokenTTL = 12 * time.Hour
)

// LoginHandler 处理 POST /api/auth/login：前端以 RSA 加密传输的密码/动态码 → 服务端解密 →
// RADIUS 校验 → 签发 JWT。请求体：{username, password_enc, otp_enc}，均为 base64(RSA-OAEP) 密文；
// password_enc 必填，otp_enc 为空表示无动态码。登录总开关关闭（auth.enable=false）时直接拒绝。
func LoginHandler(m *config.Manager, logger *zap.Logger, cipher *auth.CredCipher) app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		if !m.Get().AuthEnabled() {
			jsonErr(ctx, consts.StatusForbidden, "login is disabled")
			return
		}
		var req struct {
			Username    string `json:"username"`
			PasswordEnc string `json:"password_enc"`
			OtpEnc      string `json:"otp_enc"`
		}
		if err := ctx.Bind(&req); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		req.Username = strings.TrimSpace(req.Username)
		if req.Username == "" || req.PasswordEnc == "" {
			jsonErr(ctx, consts.StatusBadRequest, "username and password required")
			return
		}
		rc := m.Get().Auth
		if rc == nil || rc.Radius == nil || rc.Radius.ServerHost == "" {
			logger.Warn("login rejected: radius auth not configured")
			jsonErr(ctx, consts.StatusServiceUnavailable, "radius auth not configured")
			return
		}
		// 解密凭据：前端用服务端公钥（RSA-OAEP/SHA-256）加密传输，避免明文暴露。
		pw, derr := cipher.Decrypt(req.PasswordEnc)
		if derr != nil {
			logger.Warn("decrypt password failed", zap.String("username", req.Username), zap.Error(derr))
			jsonErr(ctx, consts.StatusBadRequest, "credential decryption failed")
			return
		}
		otp := ""
		if req.OtpEnc != "" {
			otp, derr = cipher.Decrypt(req.OtpEnc)
			if derr != nil {
				logger.Warn("decrypt otp failed", zap.String("username", req.Username), zap.Error(derr))
				jsonErr(ctx, consts.StatusBadRequest, "credential decryption failed")
				return
			}
		}
		r := rc.Radius
		ok, err := auth.RadiusAuthenticate(c, auth.RadiusConfig{
			ServerHost:    r.ServerHost,
			AuthPort:      r.AuthPort,
			SharedSecret:  r.SharedSecret,
			TimeoutMs:     r.TimeoutMs,
			Retries:       r.Retries,
			CaseSensitive: r.CaseSensitive,
		}, req.Username, pw, otp)
		if err != nil {
			logger.Warn("radius auth error", zap.String("username", req.Username), zap.Error(err))
			jsonErr(ctx, consts.StatusUnauthorized, "authentication failed")
			return
		}
		if !ok {
			logger.Warn("radius auth rejected", zap.String("username", req.Username))
			jsonErr(ctx, consts.StatusUnauthorized, "invalid username, password or otp")
			return
		}
		// RADIUS 通过后，再校验用户白名单（auth.radius.users）：
		// 未配置 users（nil）→ 不限制；已配置（含空数组）→ 仅名单内用户可登录，其余返回无权访问。
		// 匹配大小写不敏感，避免域账号大小写差异导致误拒。
		if !userInWhitelist(r.Users, req.Username) {
			logger.Warn("user not in radius whitelist", zap.String("username", req.Username), zap.Int("whitelist_len", len(r.Users)))
			jsonErr(ctx, consts.StatusForbidden, "您无权访问本服务，联系Nacos管理员")
			return
		}
		secret := m.JwtSecret()
		if secret == "" {
			logger.Error("login failed: jwt secret unavailable (set NACOS_SYNC_JWT_SECRET or bootstrap secret-key)")
			jsonErrInternal(ctx, "login service misconfigured")
			return
		}
		token, err := auth.IssueJWT(req.Username, secret, loginTokenTTL)
		if err != nil {
			logger.Error("issue jwt", zap.Error(err))
			jsonErrInternal(ctx, "login failed")
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true, "token": token, "username": req.Username})
	}
}

// RSAPublicKeyHandler 处理 GET /api/auth/rsa-public-key：返回服务端 RSA 公钥 PEM（SPKI），
// 供前端加密登录凭据。该接口免鉴权（属 /api/auth/ 前缀）。
func RSAPublicKeyHandler(cipher *auth.CredCipher) app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		ctx.JSON(consts.StatusOK, utils.H{"public_key": cipher.PublicKeyPEM()})
	}
}

// LogoutHandler 处理 POST /api/auth/logout：JWT 无状态，服务端不做失效处理，前端丢弃即可。
func LogoutHandler() app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	}
}

// AuthStatusHandler 处理 GET /api/auth/status：前端据此判断是否需跳转登录页。
func AuthStatusHandler(m *config.Manager, staticToken string) app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		ctx.JSON(consts.StatusOK, utils.H{
			"enabled":        m.Get().AuthEnabled(),
			"radius_enabled": m.Get().RadiusEnabled(),
			"token_enabled":  staticToken != "",
			"otp_mode":       "concat",
		})
	}
}

// JWTAuthMiddleware 控制面鉴权中间件：受保护路由要求有效 JWT；若配置了静态共享令牌
// （NACOS_SYNC_API_TOKEN）也允许其作为机器旁路。下列路径免鉴权（存活/指标/登录/状态/登出）。
func JWTAuthMiddleware(m *config.Manager, jwtSecret, staticToken string) app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		path := string(ctx.Request.URI().Path())
		// 免鉴权：存活/指标、以及 /api/auth/* 全部（登录/登出/状态/公钥均无需令牌）。
		if path == "/api/health" || path == "/api/metrics" || strings.HasPrefix(path, "/api/auth/") {
			ctx.Next(c)
			return
		}
		// 登录总开关关闭（auth.enable=false）：控制面全开放，无需任何令牌。
		if !m.Get().AuthEnabled() {
			ctx.Next(c)
			return
		}
		authz := string(ctx.Request.Header.Peek("Authorization"))
		// 1) 优先校验登录 JWT
		if strings.HasPrefix(authz, jwtBearerPrefix) {
			token := strings.TrimPrefix(authz, jwtBearerPrefix)
			if _, err := auth.ValidateJWT(token, jwtSecret); err == nil {
				ctx.Next(c)
				return
			}
			// JWT 非法/过期：若还配了静态令牌则继续走静态令牌校验，否则直接拒绝
			if staticToken == "" {
				ctx.JSON(consts.StatusUnauthorized, utils.H{"error": "unauthorized: session expired or invalid, please login again"})
				ctx.Abort()
				return
			}
		}
		// 2) 静态共享令牌旁路（机器/脚本调用）
		if staticToken != "" && strings.HasPrefix(authz, jwtBearerPrefix) {
			got := strings.TrimPrefix(authz, jwtBearerPrefix)
			if subtle.ConstantTimeCompare([]byte(got), []byte(staticToken)) == 1 {
				ctx.Next(c)
				return
			}
		}
	ctx.JSON(consts.StatusUnauthorized, utils.H{"error": "unauthorized: valid session token required"})
	ctx.Abort()
}

}

// userInWhitelist 判断 username 是否在登录白名单内。
// users 为 nil 表示未配置白名单（放行）；已配置则按大小写不敏感匹配；空数组表示无任何人可登录。
func userInWhitelist(users []string, username string) bool {
	if users == nil {
		return true
	}
	u := strings.TrimSpace(username)
	for _, w := range users {
		if strings.EqualFold(strings.TrimSpace(w), u) {
			return true
		}
	}
	return false
}
