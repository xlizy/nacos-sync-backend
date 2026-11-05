package config

import (
	"strings"
	"testing"

	"github.com/cxcm/nacos-sync-backend/internal/crypto"
)

// TestRuntimeConfigEncryptRoundTrip 验证「发布加密 → 落盘不含明文 → 读取解密还原」完整链路，
// 不经过配置中心，避免污染线上 dataId。
func TestRuntimeConfigEncryptRoundTrip(t *testing.T) {
	secret := "unit-test-secret-key"
	c := &Config{
		NacosGroup: []NacosGroup{
			{
				GrpID:        "g1",
				GrpName:      "测试组",
				EnableAuth:   true,
				AuthUsername: "nacos",
				AuthPassword: "s3cret-pass",
				DBDSN:        "user:pass@tcp(h:3306)/nacos",
			},
		},
		ConfigSync: map[string]bool{"g1": true},
	}

	// 发布路径：加密敏感字段后序列化（等价 Manager.Publish）。
	enc, err := c.EncryptSecrets(secret)
	if err != nil {
		t.Fatalf("EncryptSecrets: %v", err)
	}
	data, err := enc.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out := string(data)
	if !strings.Contains(out, "ENC(") {
		t.Fatalf("published config should contain ENC():\n%s", out)
	}
	if strings.Contains(out, "s3cret-pass") {
		t.Fatalf("published config leaked plaintext password:\n%s", out)
	}
	if strings.Contains(out, "user:pass@tcp") {
		t.Fatalf("published config leaked plaintext DSN:\n%s", out)
	}

	// 读取路径：先解密再解析（等价 Manager.parseRuntime）。
	dec, err := crypto.DecryptYAML(secret, out)
	if err != nil {
		t.Fatalf("DecryptYAML: %v", err)
	}
	rc, err := ParseRuntime(dec)
	if err != nil {
		t.Fatalf("ParseRuntime: %v", err)
	}
	if rc.NacosGroup[0].AuthPassword != "s3cret-pass" {
		t.Fatalf("auth_password not restored: %q", rc.NacosGroup[0].AuthPassword)
	}
	if rc.NacosGroup[0].DBDSN != "user:pass@tcp(h:3306)/nacos" {
		t.Fatalf("grp_dsn not restored: %q", rc.NacosGroup[0].DBDSN)
	}
}

// TestParseRuntimeDecryptsENC 验证解析前对 ENC(...) 的解密，覆盖 auth_password 与 grp_dsn 两个字段。
func TestParseRuntimeDecryptsENC(t *testing.T) {
	secret := "unit-test-secret-key"
	encAuth, _ := crypto.Encrypt(secret, "topsecret")
	encDSN, _ := crypto.Encrypt(secret, "root:pw@tcp(1.2.3.4:3306)/nacos")
	raw := "nacos-group:\n" +
		"  - grp_id: g1\n" +
		"    auth_password: " + encAuth + "\n" +
		"    grp_dsn: " + encDSN + "\n"

	// 等价 Manager.parseRuntime 的解析前解密逻辑。
	dec, err := crypto.DecryptYAML(secret, raw)
	if err != nil {
		t.Fatalf("DecryptYAML: %v", err)
	}
	rc, err := ParseRuntime(dec)
	if err != nil {
		t.Fatalf("ParseRuntime: %v", err)
	}
	if rc.NacosGroup[0].AuthPassword != "topsecret" {
		t.Fatalf("auth_password got %q", rc.NacosGroup[0].AuthPassword)
	}
	if rc.NacosGroup[0].DBDSN != "root:pw@tcp(1.2.3.4:3306)/nacos" {
		t.Fatalf("grp_dsn got %q", rc.NacosGroup[0].DBDSN)
	}
}
