package nacos

import (
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
)

func inst(ip string, port uint64, cluster, scope string, enable, healthy bool) model.Instance {
	return model.Instance{
		Ip:          ip,
		Port:        port,
		ClusterName: cluster,
		Enable:      enable,
		Healthy:     healthy,
		Metadata:    map[string]string{leaderDirectionMeta: scope},
	}
}

// TestChooseLeaderDeterministic 同一候选集合必须选出确定且一致的写主：
// 多节点以相同输入并发判断时结论必须一致，否则会退化为并发写。
func TestChooseLeaderDeterministic(t *testing.T) {
	scope := "grpA|c1->c2"
	cluster := leaderCluster(scope)
	candidates := []model.Instance{
		inst("10.0.0.9", 8080, cluster, scope, true, true),
		inst("10.0.0.2", 8080, cluster, scope, true, true),
		inst("10.0.0.10", 8080, cluster, scope, true, true),
	}
	first, ok := chooseLeader(candidates, scope)
	if !ok {
		t.Fatal("expected a leader")
	}
	for i := 0; i < 10; i++ {
		// 乱序输入（map/网络返回顺序不定）也必须选同一节点。
		shuffled := []model.Instance{candidates[2], candidates[0], candidates[1]}
		got, ok := chooseLeader(shuffled, scope)
		if !ok || got.Ip != first.Ip || got.Port != first.Port {
			t.Fatalf("leader must be deterministic; want %s:%d got %v", first.Ip, first.Port, got)
		}
	}
	if first.Ip != "10.0.0.10" {
		t.Fatalf("expected lowest-ordered ip to win; got %s", first.Ip)
	}
}

// TestChooseLeaderSkipsUnhealthy 禁用/不健康的候选不参与选主。
func TestChooseLeaderSkipsUnhealthy(t *testing.T) {
	scope := "s"
	cluster := leaderCluster(scope)
	candidates := []model.Instance{
		inst("10.0.0.1", 80, cluster, scope, false, true), // 禁用
		inst("10.0.0.2", 80, cluster, scope, true, false), // 不健康
		inst("10.0.0.3", 80, cluster, scope, true, true),  // 唯一可用
	}
	got, ok := chooseLeader(candidates, scope)
	if !ok || got.Ip != "10.0.0.3" {
		t.Fatalf("expected healthy candidate to win; got %+v ok=%v", got, ok)
	}
	if _, ok := chooseLeader([]model.Instance{candidates[0], candidates[1]}, scope); ok {
		t.Fatal("no usable candidate must yield ok=false (fail-closed)")
	}
}

// TestChooseLeaderScopeIsolation 不同 scope（方向）使用不同 hash 集群，互不干扰。
func TestChooseLeaderScopeIsolation(t *testing.T) {
	scopeA, scopeB := "dirA", "dirB"
	instances := []model.Instance{
		inst("10.0.0.1", 80, leaderCluster(scopeA), scopeA, true, true),
		inst("10.0.0.2", 80, leaderCluster(scopeB), scopeB, true, true),
	}
	la, ok := chooseLeader(instances, scopeA)
	if !ok || la.Ip != "10.0.0.1" {
		t.Fatalf("scope A must elect its own candidate; got %+v", la)
	}
	lb, ok := chooseLeader(instances, scopeB)
	if !ok || lb.Ip != "10.0.0.2" {
		t.Fatalf("scope B must elect its own candidate; got %+v", lb)
	}
}

// TestActiveScopes 孤儿清扫据全局候选判断 owner：只统计存活候选。
func TestActiveScopes(t *testing.T) {
	instances := []model.Instance{
		inst("10.0.0.1", 80, "c1", "dirA", true, true),
		inst("10.0.0.2", 80, "c2", "dirB", false, true), // 禁用 → 不算存活
		inst("10.0.0.3", 80, "c3", "dirC", true, false), // 不健康 → 不算存活
		inst("10.0.0.4", 80, "c4", "", true, true),      // 无 scope 元数据 → 忽略
	}
	active := ActiveScopes(instances)
	if len(active) != 1 || !active["dirA"] {
		t.Fatalf("expected only dirA active; got %v", active)
	}
}

// TestHasOwnCandidate 自身候选判定按 ip:port+cluster+scope 精确匹配。
func TestHasOwnCandidate(t *testing.T) {
	scope := "dirX"
	cluster := leaderCluster(scope)
	instances := []model.Instance{
		inst("10.0.0.9", 8080, cluster, scope, true, true),
	}
	if !hasOwnCandidate(instances, scope, "10.0.0.9", 8080) {
		t.Fatal("own live candidate must be found")
	}
	if hasOwnCandidate(instances, scope, "10.0.0.9", 9090) {
		t.Fatal("different port must not match")
	}
	if hasOwnCandidate(instances, scope, "10.0.0.8", 8080) {
		t.Fatal("different ip must not match")
	}
	// 候选过期（不健康）时不应误判为「已注册」，否则 Ensure 会跳过重注册导致永久失主。
	if hasOwnCandidate([]model.Instance{inst("10.0.0.9", 8080, cluster, scope, true, false)}, scope, "10.0.0.9", 8080) {
		t.Fatal("unhealthy candidate must not count as registered")
	}
}
