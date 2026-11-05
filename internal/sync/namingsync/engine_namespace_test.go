package namingsync

import (
	"sort"
	"testing"

	"github.com/cxcm/nacos-sync-backend/internal/config"
)

// TestNormNamespace 锁定 public 命名空间归一化：
// Nacos 的 public 真实 ID 是空串 ""，字面量 "public" 必须归一化为 ""，
// 否则方向 d.Namespace 与孤儿清扫扫描 s.ns 体系不一致（2026-10-16 churn bug 的根因）。
func TestNormNamespace(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"public", ""},
		{"PUBLIC", "PUBLIC"}, // 仅小写 public 归一化，避免误伤真实命名空间
		{"other", "other"},
		{"dev-ns", "dev-ns"},
	}
	for _, c := range cases {
		if got := normNamespace(c.in); got != c.want {
			t.Errorf("normNamespace(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestSweepNamespacesNormalizesPublic 锁定：配置里写 "public" 时，扫描集合必须把
// "public" 归一化为 "" 并去重，结果只含 ""（public）与其余非 public 命名空间，
// 不得出现 "/public/" 与 "//" 两套标签导致 public 被扫两次、健康实例被误删。
func TestSweepNamespacesNormalizesPublic(t *testing.T) {
	cfg := &config.Config{
		ServiceSync: &config.ServiceSyncConfig{
			SingleSync: map[string]config.ServiceSyncEntry{
				"g1": {Namespace: "public", Enable: true},
			},
			MultipleSync: []config.MultipleSyncEntry{
				{Namespace: "public"},
			},
		},
		Namespaces: []string{"public", "dev"},
	}
	e := &Engine{}
	got := e.sweepNamespaces(cfg)
	sort.Strings(got)

	want := []string{"", "dev"}
	if len(got) != len(want) {
		t.Fatalf("sweepNamespaces = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sweepNamespaces[%d] = %q, want %q (full = %v)", i, got[i], want[i], got)
		}
	}
}
