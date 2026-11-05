package config

import (
	"os"
	"testing"
)

// TestValidateNodesPortRange 校验节点列表的端口范围与去重：
// 端口 0 与越界必须显式拒绝（早期版本会把 0 静默回退 8848，前后不一致）。
func TestValidateNodesPortRange(t *testing.T) {
	cases := []struct {
		name    string
		in      []string
		want    []string
		wantErr bool
	}{
		{
			name: "合法端口原样保留",
			in:   []string{"10.0.0.1:8848"},
			want: []string{"10.0.0.1:8848"},
		},
		{
			name:    "端口 0 应被拒绝",
			in:      []string{"10.0.0.1:0"},
			wantErr: true,
		},
		{
			name:    "端口越界应被拒绝",
			in:      []string{"10.0.0.1:65536"},
			wantErr: true,
		},
		{
			name:    "非数字端口应被拒绝",
			in:      []string{"10.0.0.1:abc"},
			wantErr: true,
		},
		{
			name:    "缺端口应被拒绝（不回退 8848）",
			in:      []string{"10.0.0.1"},
			wantErr: true,
		},
		{
			name: "重复项去重",
			in:   []string{"10.0.0.1:8848", "10.0.0.2:8848", "10.0.0.1:8848"},
			want: []string{"10.0.0.1:8848", "10.0.0.2:8848"},
		},
		{
			name: "空白项跳过",
			in:   []string{"   ", "10.0.0.1:8848"},
			want: []string{"10.0.0.1:8848"},
		},
		{
			name:    "全为空应报错",
			in:      []string{"", "   "},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateNodes(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got nil (out=%v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("want %v, got %v", tc.want, got)
				}
			}
		})
	}
}

// TestParseRuntimeExpandsDBDSNEnv 校验分组 DSN 支持 ${ENV} 占位符：
// 早期仅展开 Clusters[].Auth.Password，${ENV} 形式的 DSN 会变成字面量导致连库失败，
// 反过来诱使运维把真实口令明文写进配置（扩大泄漏面）。
func TestParseRuntimeExpandsDBDSNEnv(t *testing.T) {
	const envKey = "NACOS_SYNC_TEST_DSN"
	os.Setenv(envKey, "user:secret@tcp(db:3306)/nacos")
	defer os.Unsetenv(envKey)

	yamlData := `
nacos-group:
  - grp_id: g1
    grp_name: 分组一
    grp_dsn: "${NACOS_SYNC_TEST_DSN}"
`
	c, err := ParseRuntime(yamlData)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := "user:secret@tcp(db:3306)/nacos"
	if c.NacosGroup[0].DBDSN != want {
		t.Fatalf("DBDSN not expanded: got %q, want %q", c.NacosGroup[0].DBDSN, want)
	}
}
