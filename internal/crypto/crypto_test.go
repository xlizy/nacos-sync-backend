package crypto

import (
	"strings"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	secret := "z*t%0HTeFZ(E^lLh1CAfm2fPuZq31kIX"
	cases := []string{
		"nacos",
		"user:pass@tcp(127.0.0.1:3306)/nacos?parseTime=true",
		"带中文与符号的口令 #@!%",
		"", // 空明文应得空串
	}
	for _, plain := range cases {
		enc, err := Encrypt(secret, plain)
		if err != nil {
			t.Fatalf("encrypt %q: %v", plain, err)
		}
		if plain == "" {
			if enc != "" {
				t.Fatalf("empty plain should yield empty token, got %q", enc)
			}
			continue
		}
		if !strings.HasPrefix(enc, encPrefix) || !strings.HasSuffix(enc, encSuffix) {
			t.Fatalf("token format wrong: %q", enc)
		}
		dec, err := Decrypt(secret, enc)
		if err != nil {
			t.Fatalf("decrypt %q: %v", enc, err)
		}
		if dec != plain {
			t.Fatalf("round trip mismatch: in=%q out=%q", plain, dec)
		}
		// 不同密钥应解密失败
		if _, err := Decrypt("wrong-key", enc); err == nil {
			t.Fatalf("decrypt with wrong key should fail")
		}
	}
}

func TestDecryptYAML(t *testing.T) {
	secret := "my-secret-key"
	encAuth, _ := Encrypt(secret, "s3cret")
	encDSN, _ := Encrypt(secret, "user:p@ss@tcp(h:3306)/nacos")

	raw := "nacos-group:\n" +
		"  - grp_id: g1\n" +
		"    auth_password: " + encAuth + "\n" +
		"    grp_dsn: " + encDSN + "\n"

	out, err := DecryptYAML(secret, raw)
	if err != nil {
		t.Fatalf("DecryptYAML: %v", err)
	}
	if !strings.Contains(out, "auth_password: \"s3cret\"") {
		t.Fatalf("auth_password not decrypted:\n%s", out)
	}
	if !strings.Contains(out, `grp_dsn: "user:p@ss@tcp(h:3306)/nacos"`) {
		t.Fatalf("grp_dsn not decrypted:\n%s", out)
	}

	// 引号包裹形式也应正确还原
	rawQ := "auth_password: \"" + encAuth + "\"\n"
	outQ, err := DecryptYAML(secret, rawQ)
	if err != nil {
		t.Fatalf("DecryptYAML quoted: %v", err)
	}
	if !strings.Contains(outQ, "auth_password: \"s3cret\"") {
		t.Fatalf("quoted form not decrypted:\n%s", outQ)
	}

	// 无 ENC() 应原样返回
	plain := "nacos-group:\n  - grp_id: g1\n"
	if out2, _ := DecryptYAML(secret, plain); out2 != plain {
		t.Fatalf("plain config should pass through, got %q", out2)
	}

	// 含 ENC() 但密钥为空应报错
	if _, err := DecryptYAML("", encAuth+" rest"); err == nil {
		t.Fatalf("empty secret with ENC() should error")
	}
}
