// Package auth 的 RSA 凭据加密传输实现。
//
// 背景：登录接口原以明文（HTTP body）传输用户名/密码/动态码，内网明文 HTTP 下存在凭据泄露风险。
// 采用「客户端用 RSA 公钥加密、服务端用私钥解密」的字段级加密：前端登录前从
// /api/auth/rsa-public-key 拉取服务端公钥（PEM），用 Web Crypto（RSA-OAEP / SHA-256）分别加密
// 密码与动态码，POST 密文；服务端用私钥解密后再送入 RADIUS。私钥仅在服务端内存，绝不出网。
//
// 说明：字段级加密防的是「链路明文嗅探/日志落地」，不替代 TLS；正式环境控制面仍应走 HTTPS。
// 多实例部署须在各实例配置同一份 auth.rsa_private_key_pem，否则轮询会导致公私钥不匹配、解密失败。
package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
)

// CredCipher 凭据加解密器：持有 RSA 私钥，对外提供公钥与解密能力。
type CredCipher struct {
	priv *rsa.PrivateKey
}

// NewCredCipher 构造凭据加解密器。
//   - privPEM 非空：解析之作为私钥（多实例须各实例一致）。支持 PKCS#8 与 PKCS#1 两种 PEM。
//   - privPEM 为空：生成 2048 位临时密钥（单实例/开发友好，重启即变，并在启动时告警）。
func NewCredCipher(privPEM string) (*CredCipher, error) {
	if privPEM != "" {
		block, _ := pem.Decode([]byte(privPEM))
		if block == nil {
			return nil, fmt.Errorf("invalid rsa_private_key_pem: not a PEM block")
		}
		var priv *rsa.PrivateKey
		if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
			if rk, ok := k.(*rsa.PrivateKey); ok {
				priv = rk
			}
		}
		if priv == nil {
			if rk, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
				priv = rk
			}
		}
		if priv == nil {
			return nil, fmt.Errorf("rsa_private_key_pem is not an RSA private key")
		}
		return &CredCipher{priv: priv}, nil
	}
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate rsa key: %w", err)
	}
	return &CredCipher{priv: priv}, nil
}

// PublicKeyPEM 返回 SPKI 格式的公钥 PEM，供前端加密使用。
func (c *CredCipher) PublicKeyPEM() string {
	der, err := x509.MarshalPKIXPublicKey(&c.priv.PublicKey)
	if err != nil {
		return ""
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

// Decrypt 用私钥以 RSA-OAEP/SHA-256 解密前端传来的 base64 密文。
// 入参为空串时返回空串（用于「未提供动态码」的情形，避免对空串解密报错）。
func (c *CredCipher) Decrypt(b64 string) (string, error) {
	if b64 == "" {
		return "", nil
	}
	ct, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", fmt.Errorf("base64 decode ciphertext: %w", err)
	}
	plain, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, c.priv, ct, nil)
	if err != nil {
		return "", fmt.Errorf("rsa decrypt: %w", err)
	}
	return string(plain), nil
}
