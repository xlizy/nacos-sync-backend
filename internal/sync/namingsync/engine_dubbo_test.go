package namingsync

import (
	"strings"
	"testing"

	"github.com/cxcm/nacos-sync-backend/internal/config"
)

// TestIsDubboService 锁定 dubbo 前缀过滤规则（已与业务方确认：dubbo 应用按接口注册，
// 服务名均为 providers:/consumers: 前缀；其它服务不代注册）。
func TestIsDubboService(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"providers:com.foo.BarService:1.0.0", true},
		{"consumers:com.foo.BazService", true},
		{"providers-foo", false},      // 前缀必须带冒号
		{"order-service", false},       // 普通应用服务
		{"NACOS-SYNC", false},          // 同步服务自身
		{"", false},
		{"xproviders:com.foo.Bar", false},
	}
	for _, c := range cases {
		if got := isDubboService(c.name); got != c.want {
			t.Fatalf("isDubboService(%q)=%v want %v", c.name, got, c.want)
		}
	}
}

// TestDirIDForDubbo 锁定方向 ID 格式：dubbo 动态方向带 "dubbo" 段，与静态方向区分。
func TestDirIDForDubbo(t *testing.T) {
	if got := dirIDFor("A", "a1", "B", "b1", "", true); got != "dubbo/A/a1->B/b1/" {
		t.Fatalf("dubbo dirID = %q", got)
	}
	if got := dirIDFor("A", "a1", "B", "b1", "", false); got != "A/a1->B/b1/" {
		t.Fatalf("static dirID = %q", got)
	}
	if got := dirID("A", "a1", "B", "b1", "ns1"); got != "A/a1->B/b1/ns1" {
		t.Fatalf("legacy dirID = %q", got)
	}
}

// dubboCfg 构造含两个分组（G1: c1,c2；G2: d1）与若干 dubbo 条目的运行时配置。
func dubboTestConfig() *config.Config {
	return &config.Config{
		NacosGroup: []config.NacosGroup{
			{GrpID: "G1", Clusters: []config.NacosCluster{{CID: "c1"}, {CID: "c2"}}},
			{GrpID: "G2", Clusters: []config.NacosCluster{{CID: "d1"}}},
		},
		DubboSync: &config.DubboSyncConfig{
			Entries: []config.DubboSyncEntry{
				{ClusterA: "c1", ClusterB: "d1", Namespace: "", Enable: true},
				{ClusterA: "c2", ClusterB: "d1", Namespace: "nsX", Enable: false}, // 未启用
				{ClusterA: "c1", ClusterB: "d1", Namespace: "public", Enable: true},
			},
		},
	}
}

// TestResolveDirectionsDubbo 锁定 dubbo 条目 → 双向动态方向解析：
//  1. 每条启用条目生成 A→B 与 B→A 两条方向，ID 带 dubbo 段；
//  2. 未启用条目不生成方向；
//  3. "public" 命名空间归一化为 ""（与空串同方向，配置层条目去重已防重复，见 config 包测试）。
func TestResolveDirectionsDubbo(t *testing.T) {
	e := &Engine{
		dirs:       make(map[string]*direction),
		runs:       make(map[string]*runningDir),
		dubboDisco: make(map[string]*discoState),
	}
	dirs := e.resolveDirections(dubboTestConfig())
	got := make(map[string]*direction, len(dirs))
	for _, d := range dirs {
		got[d.ID] = d
	}
	// 启用条目 2 条（c1↔d1 public、c1↔d1 "public"，后者归一化后与前者同方向 ID，
	// resolve 层不去重——同 ID 方向由 reconcile 以 map 天然合并），c2 条目未启用 → 共 4 条方向。
	if len(dirs) != 4 {
		t.Fatalf("dirs len = %d, want 4 (实际=%v)", len(dirs), keysOf(got))
	}
	for id, d := range got {
		if !d.Dubbo {
			t.Fatalf("方向 %s 应为 dubbo 动态方向", id)
		}
		if !strings.HasPrefix(id, "dubbo/") {
			t.Fatalf("dubbo 方向 ID 应带 dubbo 段: %s", id)
		}
		if len(d.Services) != 0 {
			t.Fatalf("dubbo 动态方向 Services 应恒为空: %s", id)
		}
	}
	if d, ok := got["dubbo/G1/c1->G2/d1/"]; !ok || d.SrcCID != "c1" || d.DstCID != "d1" || d.Namespace != "" {
		t.Fatalf("缺少/错误方向 c1->d1: %v", got["dubbo/G1/c1->G2/d1/"])
	}
	if d, ok := got["dubbo/G2/d1->G1/c1/"]; !ok || d.SrcCID != "d1" || d.DstCID != "c1" {
		t.Fatalf("缺少/错误方向 d1->c1")
	}
	if _, ok := got["dubbo/G1/c2->G2/d1/nsX"]; ok {
		t.Fatalf("未启用条目不应生成方向")
	}
}

// TestResolveDirectionsDubboMissingCluster 集群不存在（已被删除）的条目应跳过，不产生方向。
func TestResolveDirectionsDubboMissingCluster(t *testing.T) {
	cfg := dubboTestConfig()
	cfg.DubboSync.Entries = []config.DubboSyncEntry{{ClusterA: "gone", ClusterB: "d1", Enable: true}}
	e := &Engine{dubboDisco: make(map[string]*discoState)}
	if dirs := e.resolveDirections(cfg); len(dirs) != 0 {
		t.Fatalf("dirs len = %d, want 0", len(dirs))
	}
}

// TestResolveDirectionsDubboWithoutServiceSync 未配置 service-sync（ss=nil）时 dubbo 方向仍须解析。
func TestResolveDirectionsDubboWithoutServiceSync(t *testing.T) {
	cfg := dubboTestConfig()
	cfg.ServiceSync = nil
	e := &Engine{dubboDisco: make(map[string]*discoState)}
	dirs := e.resolveDirections(cfg)
	if len(dirs) != 4 {
		t.Fatalf("dirs len = %d, want 4", len(dirs))
	}
}
