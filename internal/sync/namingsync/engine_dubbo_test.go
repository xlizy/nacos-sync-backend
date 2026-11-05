package namingsync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cxcm/nacos-sync-backend/internal/config"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
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

// TestDubboStaleServicesReturnsLastSuccess 锁定枚举失败容错的核心语义：
// dubboStaleServices 返回上次成功枚举的服务列表，无论缓存是否过期。
// 这使 dubbo 方向在枚举偶发失败/退避期间仍能沿用上次列表继续对账实例，
// 像普通服务同步一样服务列表不丢失、不整方向停摆。
func TestDubboStaleServicesReturnsLastSuccess(t *testing.T) {
	dirID := "dubbo/G1/c1->G2/d1/"
	want := []string{"providers:com.foo.Bar:1.0.0", "consumers:com.foo.Baz"}
	e := &Engine{dubboDisco: map[string]*discoState{
		dirID: {
			lastAt:   time.Now().Add(-10 * time.Second), // 远超 dubboDiscoCache(2s)，缓存已过期
			services: want,
		},
	}}
	got := e.dubboStaleServices(&direction{ID: dirID})
	if len(got) != len(want) {
		t.Fatalf("dubboStaleServices len = %d, want %d (even when cache expired)", len(got), len(want))
	}
	for i, s := range want {
		if got[i] != s {
			t.Fatalf("dubboStaleServices[%d] = %q, want %q", i, got[i], s)
		}
	}
}

// TestDubboStaleServicesNilWhenNeverSucceeded 锁定从未成功枚举时返回 nil，
// 调用方据此跳过本轮（无已知服务可对账）。
func TestDubboStaleServicesNilWhenNeverSucceeded(t *testing.T) {
	e := &Engine{dubboDisco: map[string]*discoState{}}
	if got := e.dubboStaleServices(&direction{ID: "dubbo/none->none/"}); got != nil {
		t.Fatalf("dubboStaleServices on unknown direction = %v, want nil", got)
	}
	e.dubboDisco["dubbo/x->y/"] = &discoState{} // lastAt 零值，从未成功
	if got := e.dubboStaleServices(&direction{ID: "dubbo/x->y/"}); len(got) != 0 {
		t.Fatalf("dubboStaleServices on never-succeeded direction = %v, want empty", got)
	}
}

// TestDubboServicesForReconcileUnionsEnumAndRegistered 锁定重构核心语义：
// dubbo 方向本轮对账列表 = 枚举结果 ∪ registered 服务名。
// 消失的 dubbo 服务（provider 下线，枚举不再返回，但仍有代注册实例在 registered）
// 仍被纳入对账列表 → fastPoll/syncService 会查它的源实例（返回空）→ 注销代注册实例。
// 这与普通服务的对账路径完全一致，无需基于服务名集合的单独清理。
func TestDubboServicesForReconcileUnionsEnumAndRegistered(t *testing.T) {
	dirID := "dubbo/G1/c1->G2/d1/"
	enumResult := []string{"providers:com.foo.Bar:1.0.0", "providers:com.foo.Baz"}
	disappearedSvc := "providers:com.gone.Deprecated" // 已从枚举消失，但 registered 仍有代注册实例

	e := &Engine{
		dirs:       make(map[string]*direction),
		runs:       make(map[string]*runningDir),
		dubboDisco: map[string]*discoState{
			dirID: {lastAt: time.Now(), services: enumResult}, // 有效缓存
		},
	}
	rd := &runningDir{
		d:          &direction{ID: dirID, Dubbo: true},
		registered: map[string]regEntry{
			regKey(disappearedSvc, model.Instance{Ip: "10.0.0.1", Port: 20880}): {
				svc: disappearedSvc, inst: model.Instance{Ip: "10.0.0.1", Port: 20880},
			},
		},
		failCount: map[string]int{},
		dead:      map[string]regEntry{},
	}
	got := e.dubboServicesForReconcile(context.Background(), rd)

	want := map[string]bool{
		"providers:com.foo.Bar:1.0.0": true,
		"providers:com.foo.Baz":       true,
		disappearedSvc:                true, // 消失服务必须被纳入（由 registered 补入）
	}
	if len(got) != len(want) {
		t.Fatalf("dubboServicesForReconcile len = %d, want %d (enum ∪ registered): got %v", len(got), len(want), got)
	}
	for _, s := range got {
		if !want[s] {
			t.Fatalf("unexpected service %q in reconcile list (want enum ∪ registered only)", s)
		}
	}
}

// TestDubboServicesForReconcileEnumOnly 锁定 registered 为空时只返回枚举结果
// （新增 dubbo 方向首次枚举成功、尚无代注册实例的场景）。
func TestDubboServicesForReconcileEnumOnly(t *testing.T) {
	dirID := "dubbo/G1/c1->G2/d1/"
	enumResult := []string{"providers:com.foo.Bar:1.0.0"}
	e := &Engine{
		dubboDisco: map[string]*discoState{
			dirID: {lastAt: time.Now(), services: enumResult},
		},
	}
	rd := &runningDir{
		d:          &direction{ID: dirID, Dubbo: true},
		registered: map[string]regEntry{},
		failCount:  map[string]int{},
		dead:       map[string]regEntry{},
	}
	got := e.dubboServicesForReconcile(context.Background(), rd)
	if len(got) != 1 || got[0] != enumResult[0] {
		t.Fatalf("dubboServicesForReconcile = %v, want %v (enum only when registered empty)", got, enumResult)
	}
}
