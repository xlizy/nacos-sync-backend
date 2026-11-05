package nacos

import (
	"strings"
	"testing"

	"github.com/cxcm/nacos-sync-backend/internal/config"
)

func TestResolveClusterConnRejectsCrossGroupDuplicateID(t *testing.T) {
	cfg := &config.Config{NacosGroup: []config.NacosGroup{
		{GrpID: "group-a", GrpVersion: "2X", Clusters: []config.NacosCluster{{CID: "dup", CNodes: []string{"10.0.0.1:8848"}}}},
		{GrpID: "group-b", GrpVersion: "3X", Clusters: []config.NacosCluster{{CID: "dup", CNodes: []string{"10.0.0.2:8848"}}}},
	}}
	_, err := resolveClusterConn(cfg, "dup")
	if err == nil || !strings.Contains(err.Error(), "ambiguous cluster ID") {
		t.Fatalf("expected ambiguous cluster ID error, got %v", err)
	}
}

func TestResolveClusterConnReturnsDetachedClusterConfig(t *testing.T) {
	cfg := &config.Config{NacosGroup: []config.NacosGroup{
		{GrpID: "group-a", GrpVersion: "3X", Clusters: []config.NacosCluster{{CID: "unique", CNodes: []string{"10.0.0.1:8848"}}}},
	}}
	got, err := resolveClusterConn(cfg, "unique")
	if err != nil {
		t.Fatalf("resolve unique cluster: %v", err)
	}
	got.Nodes[0] = "mutated"
	if cfg.NacosGroup[0].Clusters[0].CNodes[0] != "10.0.0.1:8848" {
		t.Fatal("resolved connection must not alias mutable config slices")
	}
}
