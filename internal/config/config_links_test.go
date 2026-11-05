package config

import "testing"

// TestRebuildConfigLinksGlobalSwitch 锁定 config 同步「全局总闸 + 全同步」不变量：
// 任一新分组（GrpVersion=3X）开关开启即拉起所有新集群两两双向 config 同步（含组内），
// 老集群（2X）不参与；非 config 链路保留；全部开关关闭时不生成 config 链路。
func TestRebuildConfigLinksGlobalSwitch(t *testing.T) {
	newGroupA := NacosGroup{
		GrpID:      "A",
		GrpVersion: "3X",
		Clusters: []NacosCluster{
			{CID: "a1"}, {CID: "a2"}, // 组内两集群，也应互相同步
		},
	}
	newGroupB := NacosGroup{
		GrpID:      "B",
		GrpVersion: "3X",
		Clusters:   []NacosCluster{{CID: "b1"}},
	}
	oldGroupC := NacosGroup{ // 老集群，不参与 config 同步
		GrpID:      "C",
		GrpVersion: "2X",
		Clusters:   []NacosCluster{{CID: "c1"}},
	}

	base := func() *Config {
		return &Config{
			NacosGroup: []NacosGroup{newGroupA, newGroupB, oldGroupC},
			Links: []SyncLink{
				{From: "x", To: "y", Kind: "naming", Enabled: true}, // 非 config 链路，须保留
			},
		}
	}

	// 场景1：开启 A 即触发全局全量同步（A/B 的新集群 a1,a2,b1 两两双向 = C(3,2)*2 = 6 条）
	c1 := base()
	c1.ConfigSync = map[string]bool{"A": true}
	c1.rebuildConfigLinks()

	var configLinks []SyncLink
	for _, l := range c1.Links {
		switch l.Kind {
		case "naming":
			// 非 config 链路必须保留
		case "config":
			configLinks = append(configLinks, l)
		default:
			t.Fatalf("unexpected link kind %q", l.Kind)
		}
	}
	if len(configLinks) != 6 {
		t.Fatalf("config links = %d, want 6 (a1-a2, a1-b1, a2-b1 各双向); got %+v", len(configLinks), configLinks)
	}
	for _, l := range configLinks {
		if !l.Enabled {
			t.Fatalf("config link should be enabled when any switch on: %+v", l)
		}
		if l.From == "c1" || l.To == "c1" {
			t.Fatalf("old cluster c1 must not appear in config links: %+v", l)
		}
	}
	// 确认非 config 链路仍在
	if len(c1.Links) != 7 {
		t.Fatalf("total links = %d, want 7 (1 naming + 6 config)", len(c1.Links))
	}

	// 场景2：全部关闭 → 不生成任何 config 链路，仅保留原非 config 链路
	c2 := base()
	c2.ConfigSync = map[string]bool{"A": false, "B": false}
	c2.rebuildConfigLinks()
	for _, l := range c2.Links {
		if l.Kind == "config" {
			t.Fatalf("no config link expected when all switches off, got %+v", l)
		}
	}
	if len(c2.Links) != 1 {
		t.Fatalf("links = %d, want 1 (only the preserved naming link)", len(c2.Links))
	}

	// 场景3：单新集群（无配对）不生成 config 链路
	c3 := &Config{
		NacosGroup: []NacosGroup{{GrpID: "S", GrpVersion: "3X", Clusters: []NacosCluster{{CID: "s1"}}}},
		ConfigSync: map[string]bool{"S": true},
	}
	c3.rebuildConfigLinks()
	for _, l := range c3.Links {
		if l.Kind == "config" {
			t.Fatalf("single new cluster should produce no config link, got %+v", l)
		}
	}
}
