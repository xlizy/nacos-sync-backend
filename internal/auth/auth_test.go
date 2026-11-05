package auth

import (
	"testing"
	"time"
)

func TestIssueValidateJWT(t *testing.T) {
	secret := "test-secret"
	tok, err := IssueJWT("alice", secret, time.Hour)
	if err != nil {
		t.Fatalf("issue jwt: %v", err)
	}
	claims, err := ValidateJWT(tok, secret)
	if err != nil {
		t.Fatalf("validate jwt: %v", err)
	}
	if claims.Username != "alice" {
		t.Fatalf("username mismatch: got %q", claims.Username)
	}

	// 错误密钥应校验失败
	if _, err := ValidateJWT(tok, "wrong-secret"); err == nil {
		t.Fatal("expected error with wrong secret")
	}

	// 过期令牌应校验失败
	expired, err := IssueJWT("bob", secret, -time.Hour)
	if err != nil {
		t.Fatalf("issue expired jwt: %v", err)
	}
	if _, err := ValidateJWT(expired, secret); err == nil {
		t.Fatal("expected expired token to fail validation")
	}
}

func TestRadiusAuthenticateBadAddress(t *testing.T) {
	// 用不可达地址验证：应返回通信错误（而非 Access-Accept），证明超时/重试逻辑能正常返回。
	_, err := RadiusAuthenticate(
		t.Context(),
		RadiusConfig{
			ServerHost: "127.0.0.1",
			AuthPort:   1, // 极少服务监听，确保快速失败
			SharedSecret: "x",
			TimeoutMs:  50,
			Retries:    1,
		},
		"u", "p", "123456",
	)
	if err == nil {
		t.Fatal("expected communication error for unreachable radius server")
	}
}
