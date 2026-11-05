package config

import (
	"strings"
	"testing"

	"github.com/cxcm/nacos-sync-backend/internal/crypto"
)

// TestEncryptSecretsPlainAndIdempotent 验证：明文敏感字段被加密为 ENC(...)，
// 而本身已是 ENC(...) 的值保持不变（不双重加密，否则解密会损坏）。
func TestEncryptSecretsPlainAndIdempotent(t *testing.T) {
	secret := "test-secret-key-1234567890"
	plainPw := "myPlainPassword"
	plainDsn := "user:pass@tcp(h:3306)/nacos"
	existingEnc := "ENC(abc123def456)" // 已是令牌，应原样保留

	c := &Config{
		NacosGroup: []NacosGroup{
			{GrpID: "g1", AuthPassword: plainPw, DBDSN: plainDsn},
			{GrpID: "g2", AuthPassword: existingEnc},
		},
	}
	out, err := c.EncryptSecrets(secret)
	if err != nil {
		t.Fatalf("EncryptSecrets: %v", err)
	}
	g1 := out.NacosGroup[0]
	if !strings.HasPrefix(g1.AuthPassword, "ENC(") || !strings.HasSuffix(g1.AuthPassword, ")") {
		t.Fatalf("plain auth_password not encrypted: %q", g1.AuthPassword)
	}
	if !strings.HasPrefix(g1.DBDSN, "ENC(") || !strings.HasSuffix(g1.DBDSN, ")") {
		t.Fatalf("plain grp_dsn not encrypted: %q", g1.DBDSN)
	}
	// 加密后必须可解密还原（发布→落盘→读回链路正确）。
	decPw, err := crypto.Decrypt(secret, g1.AuthPassword)
	if err != nil {
		t.Fatalf("decrypt auth_password: %v", err)
	}
	if decPw != plainPw {
		t.Fatalf("auth_password roundtrip: got %q want %q", decPw, plainPw)
	}
	decDsn, err := crypto.Decrypt(secret, g1.DBDSN)
	if err != nil {
		t.Fatalf("decrypt grp_dsn: %v", err)
	}
	if decDsn != plainDsn {
		t.Fatalf("grp_dsn roundtrip: got %q want %q", decDsn, plainDsn)
	}
	// 已是 ENC 的值保持原样（单层 ENC，不被再包一层）。
	g2 := out.NacosGroup[1]
	if g2.AuthPassword != existingEnc {
		t.Fatalf("existing ENC should be kept as-is, got %q", g2.AuthPassword)
	}
}

// TestEncryptSecretsKeepsDubboSync 回归：EncryptSecrets 构造发布副本时必须带上 dubbo-sync 段，
// 否则写回配置中心的内容会丢失该段（接口返回 200 但 Nacos 上无 dubbo-sync，随后监听推送把本地配置覆盖）。
func TestEncryptSecretsKeepsDubboSync(t *testing.T) {
	secret := "unit-test-secret-key"
	in := &Config{
		NacosGroup: []NacosGroup{{GrpID: "g1", GrpName: "组1"}},
		DubboSync: &DubboSyncConfig{
			Entries: []DubboSyncEntry{{ClusterA: "a1", ClusterB: "b1", Namespace: "nsX", Enable: true}},
		},
	}
	out, err := in.EncryptSecrets(secret)
	if err != nil {
		t.Fatalf("EncryptSecrets: %v", err)
	}
	if out.DubboSync == nil || len(out.DubboSync.Entries) != 1 {
		t.Fatalf("dubbo-sync section lost in publish copy: %+v", out.DubboSync)
	}
	e := out.DubboSync.Entries[0]
	if e.ClusterA != "a1" || e.ClusterB != "b1" || e.Namespace != "nsX" || !e.Enable {
		t.Fatalf("dubbo-sync entry mangled: %+v", e)
	}
	// 完整链路：加密 → 序列化 → 解密 → 解析，dubbo-sync 段应原样保留。
	data, err := out.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	dec, err := crypto.DecryptYAML(secret, string(data))
	if err != nil {
		t.Fatalf("DecryptYAML: %v", err)
	}
	rc, err := ParseRuntime(dec)
	if err != nil {
		t.Fatalf("ParseRuntime: %v", err)
	}
	if rc.DubboSync == nil || len(rc.DubboSync.Entries) != 1 || rc.DubboSync.Entries[0].ClusterA != "a1" {
		t.Fatalf("dubbo-sync not restored after publish round-trip: %+v", rc.DubboSync)
	}
}
