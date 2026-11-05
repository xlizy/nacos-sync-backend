package config

import "testing"

// TestDubboPairKey 锁定无序对规范化：字典序较小的在前。
func TestDubboPairKey(t *testing.T) {
	if a, b := dubboPairKey("QxIc", "Odd1"); a != "Odd1" || b != "QxIc" {
		t.Fatalf("dubboPairKey = %s,%s", a, b)
	}
	if a, b := dubboPairKey("Odd1", "QxIc"); a != "Odd1" || b != "QxIc" {
		t.Fatalf("dubboPairKey reversed = %s,%s", a, b)
	}
}

// TestFindDubboEntry 锁定条目定位：无序对（A↔B 与 B↔A 等价）+ 命名空间参与唯一性。
func TestFindDubboEntry(t *testing.T) {
	c := &Config{DubboSync: &DubboSyncConfig{
		Entries: []DubboSyncEntry{{ClusterA: "a1", ClusterB: "b1", Namespace: ""}},
	}}
	if i := findDubboEntry(c, "b1", "a1", ""); i != 0 {
		t.Fatalf("unordered pair lookup = %d, want 0", i)
	}
	if i := findDubboEntry(c, "a1", "b1", "nsX"); i != -1 {
		t.Fatalf("不同命名空间不应命中: %d", i)
	}
	if i := findDubboEntry(c, "a1", "c9", ""); i != -1 {
		t.Fatalf("不存在集群对不应命中: %d", i)
	}
	if i := findDubboEntry(&Config{}, "a1", "b1", ""); i != -1 {
		t.Fatalf("nil DubboSync 不应命中: %d", i)
	}
}

// TestRemoveDubboEntriesForCluster 锁定删组/删集群时的级联清理：
// 仅移除引用该集群的条目，其余保留；无引用时无变更。
func TestRemoveDubboEntriesForCluster(t *testing.T) {
	c := &Config{DubboSync: &DubboSyncConfig{
		Entries: []DubboSyncEntry{
			{ClusterA: "a1", ClusterB: "b1", Namespace: ""},
			{ClusterA: "b2", ClusterB: "c1", Namespace: "ns"},
			{ClusterA: "d1", ClusterB: "e1", Namespace: ""},
		},
	}}
	if changed := removeDubboEntriesForCluster(c, "c1"); !changed || len(c.DubboSync.Entries) != 2 {
		t.Fatalf("changed=%v entries=%d, want changed=true len=2", changed, len(c.DubboSync.Entries))
	}
	if changed := removeDubboEntriesForCluster(c, "zzz"); changed || len(c.DubboSync.Entries) != 2 {
		t.Fatalf("无引用不应变更: changed=%v len=%d", changed, len(c.DubboSync.Entries))
	}
	if changed := removeDubboEntriesForCluster(&Config{}, "a1"); changed {
		t.Fatalf("nil DubboSync 不应报告变更")
	}
}

// TestParseRuntimeDubboSync 锁定 dubbo-sync 段从 YAML 的解析形态（含 entries 缺省）。
func TestParseRuntimeDubboSync(t *testing.T) {
	yaml := `
nacos-group: []
dubbo-sync:
  entries:
    - cluster_a: c1
      cluster_b: d1
      namespace: nsX
      enable: true
`
	c, err := ParseRuntime(yaml)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.DubboSync == nil || len(c.DubboSync.Entries) != 1 {
		t.Fatalf("dubbo-sync not parsed: %+v", c.DubboSync)
	}
	e := c.DubboSync.Entries[0]
	if e.ClusterA != "c1" || e.ClusterB != "d1" || e.Namespace != "nsX" || !e.Enable {
		t.Fatalf("entry mismatch: %+v", e)
	}
}
