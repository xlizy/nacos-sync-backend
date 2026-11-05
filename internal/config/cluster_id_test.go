package config

import (
	"strings"
	"testing"
)

func TestValidateUniqueClusterIDsRejectsCrossGroupCollision(t *testing.T) {
	groups := []NacosGroup{
		{GrpID: "group-a", Clusters: []NacosCluster{{CID: "same"}}},
		{GrpID: "group-b", Clusters: []NacosCluster{{CID: "same"}}},
	}
	err := validateUniqueClusterIDs(groups)
	if err == nil || !strings.Contains(err.Error(), "globally unique") {
		t.Fatalf("expected global duplicate ID error, got %v", err)
	}
}

func TestValidateUniqueClusterIDsRejectsWithinGroupCollision(t *testing.T) {
	groups := []NacosGroup{
		{GrpID: "group-a", Clusters: []NacosCluster{{CID: "same"}, {CID: "same"}}},
	}
	if err := validateUniqueClusterIDs(groups); err == nil {
		t.Fatal("expected duplicate ID in one group to be rejected")
	}
}

func TestEnsureClusterIDsGlobalAvoidsOtherGroups(t *testing.T) {
	groups := []NacosGroup{
		{GrpID: "group-a", Clusters: []NacosCluster{{CID: "taken"}}},
	}
	clusters := ensureClusterIDsGlobal([]NacosCluster{{CID: ""}}, groups, "group-b")
	if len(clusters) != 1 || clusters[0].CID == "" || clusters[0].CID == "taken" {
		t.Fatalf("generated ID must be non-empty and globally unused: %#v", clusters)
	}
}

func TestEnsureClusterIDsGlobalAllowsRetainingIDsInReplacedGroup(t *testing.T) {
	groups := []NacosGroup{
		{GrpID: "group-a", Clusters: []NacosCluster{{CID: "keep"}}},
	}
	clusters := ensureClusterIDsGlobal([]NacosCluster{{CID: "keep"}}, groups, "group-a")
	if clusters[0].CID != "keep" {
		t.Fatalf("updating a group must retain its own IDs, got %q", clusters[0].CID)
	}
}

func TestNewClusterIDsDeduplicatesLegacyCollisions(t *testing.T) {
	c := &Config{NacosGroup: []NacosGroup{
		{GrpID: "group-a", GrpVersion: "3X", Clusters: []NacosCluster{{CID: "a"}, {CID: "b"}}},
		{GrpID: "group-b", GrpVersion: "3X", Clusters: []NacosCluster{{CID: "a"}}},
	}}
	got := c.newClusterIDs()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expected deduplicated cluster IDs [a b], got %#v", got)
	}
}
