package config

import (
	"strings"
	"testing"
)

func TestGroupDSNRename(t *testing.T) {
	// 新字段名 grp_dsn
	newYAML := `
nacos-group:
  - grp_id: g1
    grp_name: 新字段
    grp_version: "3X"
    grp_dsn: "user:pass@tcp(h:3306)/nacos"
`
	c, err := ParseRuntime(newYAML)
	if err != nil {
		t.Fatalf("parse new: %v", err)
	}
	if len(c.NacosGroup) != 1 {
		t.Fatalf("want 1 group, got %d", len(c.NacosGroup))
	}
	if c.NacosGroup[0].DBDSN != "user:pass@tcp(h:3306)/nacos" {
		t.Fatalf("new grp_dsn not read: %q", c.NacosGroup[0].DBDSN)
	}

	// 旧字段名 db_dsn 兼容读回
	legacyYAML := `
nacos-group:
  - grp_id: g2
    grp_name: 旧字段
    grp_version: "3X"
    db_dsn: "legacy@tcp(h:3306)/nacos"
`
	c2, err := ParseRuntime(legacyYAML)
	if err != nil {
		t.Fatalf("parse legacy: %v", err)
	}
	if c2.NacosGroup[0].DBDSN != "legacy@tcp(h:3306)/nacos" {
		t.Fatalf("legacy db_dsn not migrated: %q", c2.NacosGroup[0].DBDSN)
	}

	// 落盘应以 grp_dsn 写出（验证 Marshal），且不含 db_dsn
	out, err := c.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(out), "grp_dsn") {
		t.Fatalf("marshal missing grp_dsn:\n%s", out)
	}
	if strings.Contains(string(out), "db_dsn") {
		t.Fatalf("marshal should not emit legacy db_dsn:\n%s", out)
	}
}

