// 本文件承载「dubbo 服务同步」的运行时配置与 Manager 写操作。
//
// 背景：部分业务应用使用阿里 dubbo，按接口在 Nacos 上注册大量服务（服务名以
// providers: / consumers: 前缀开头，已与业务方确认），接口在迭代中会新增/消失。
// dubbo 同步不做服务白名单，而是引擎侧实时枚举两个集群指定命名空间下全部服务、
// 按前缀动态过滤并全量代注册（防回环/巡检等机制与普通命名同步一致）。
package config

import "fmt"

// DubboSyncEntry 一条 dubbo 集群同步配置：两个集群间按前缀动态全量代注册 dubbo 服务。
type DubboSyncEntry struct {
	ClusterA  string `yaml:"cluster_a" json:"cluster_a"`   // 集群 A（c_id）
	ClusterB  string `yaml:"cluster_b" json:"cluster_b"`   // 集群 B（c_id）
	Namespace string `yaml:"namespace" json:"namespace"`   // 命名空间（空 = public）
	Enable    bool   `yaml:"enable" json:"enable"`         // 是否启用
}

// DubboSyncConfig dubbo 同步配置，持久化于运行时配置的 dubbo-sync 段。
// 条目去重规则：集群对按无序对唯一（A↔B 与 B↔A 视为同一条），命名空间参与唯一性。
type DubboSyncConfig struct {
	Entries []DubboSyncEntry `yaml:"entries,omitempty" json:"entries,omitempty"`
}

// dubboPairKey 将两个集群 ID 规范为无序对（字典序较小的在前），用于条目去重。
func dubboPairKey(a, b string) (string, string) {
	if a < b {
		return a, b
	}
	return b, a
}

// findDubboEntry 按（无序集群对, 命名空间）查找条目下标；未找到返回 -1。
func findDubboEntry(c *Config, a, b, ns string) int {
	aa, ab := dubboPairKey(a, b)
	if c.DubboSync == nil {
		return -1
	}
	for i, e := range c.DubboSync.Entries {
		if ea, eb := dubboPairKey(e.ClusterA, e.ClusterB); ea == aa && eb == ab && e.Namespace == ns {
			return i
		}
	}
	return -1
}

// AddDubboSync 新增一条 dubbo 集群同步。两集群不可相同、必须存在于 nacos-group，
// 且与已有条目按（无序集群对, 命名空间）去重（A↔B 与 B↔A 视为同一条）。
func (m *Manager) AddDubboSync(a, b, ns string, enable bool) error {
	if a == "" || b == "" {
		return fmt.Errorf("cluster_a/cluster_b required")
	}
	if a == b {
		return fmt.Errorf("cluster_a and cluster_b must differ")
	}
	if !m.clusterExists(a) || !m.clusterExists(b) {
		return notFoundf("cluster %s or %s not found", a, b)
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	pa, pb := dubboPairKey(a, b)
	if findDubboEntry(c, a, b, ns) >= 0 {
		return fmt.Errorf("dubbo-sync entry for clusters %s<->%s (ns %q) already exists", pa, pb, ns)
	}
	nc := m.cloneForWrite(c)
	if nc.DubboSync == nil {
		nc.DubboSync = &DubboSyncConfig{}
	}
	nc.DubboSync.Entries = append(nc.DubboSync.Entries, DubboSyncEntry{ClusterA: pa, ClusterB: pb, Namespace: ns, Enable: enable})
	return m.publishDubboSync(nc)
}

// UpdateDubboSync 更新一条 dubbo 集群同步（按无序集群对 + 命名空间定位；集群对不可变更）。
func (m *Manager) UpdateDubboSync(a, b, ns string, enable bool) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	idx := findDubboEntry(c, a, b, ns)
	if idx < 0 {
		return notFoundf("dubbo-sync entry for clusters %s<->%s (ns %q) not found", a, b, ns)
	}
	nc := m.cloneForWrite(c)
	nc.DubboSync.Entries[idx].Enable = enable
	nc.DubboSync.Entries[idx].Namespace = ns
	return m.publishDubboSync(nc)
}

// DeleteDubboSync 删除一条 dubbo 集群同步（按无序集群对 + 命名空间定位）。
func (m *Manager) DeleteDubboSync(a, b, ns string) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	idx := findDubboEntry(c, a, b, ns)
	if idx < 0 {
		return notFoundf("dubbo-sync entry for clusters %s<->%s (ns %q) not found", a, b, ns)
	}
	nc := m.cloneForWrite(c)
	entries := nc.DubboSync.Entries
	entries[idx] = entries[len(entries)-1]
	entries = entries[:len(entries)-1]
	nc.DubboSync.Entries = entries
	return m.publishDubboSync(nc)
}

// clusterExists 报告某 c_id 是否存在于当前 nacos-group 集群目录。
func (m *Manager) clusterExists(cID string) bool {
	for _, g := range m.Get().NacosGroup {
		for _, cl := range g.Clusters {
			if cl.CID == cID {
				return true
			}
		}
	}
	return false
}

// publishDubboSync 写回配置中心并原子替换生效配置；段为空时置 nil 保持 YAML 整洁。
// 与 publishServiceSync 一致：落盘后立即本地触发运行时变化回调，使启用/禁用当次请求即生效。
func (m *Manager) publishDubboSync(nc *Config) error {
	if nc.DubboSync != nil && len(nc.DubboSync.Entries) == 0 {
		nc.DubboSync = nil
	}
	if err := m.Publish(nc); err != nil {
		return err
	}
	m.store.Set(nc)
	m.fireRuntimeChange(nc)
	return nil
}

// removeDubboEntriesForCluster 删除引用某集群的 dubbo-sync 条目（删组/删集群时级联调用），
// 避免残留悬挂引用导致引擎跳过或前端展示无意义 ID。返回是否有变更。
func removeDubboEntriesForCluster(nc *Config, cID string) bool {
	if nc.DubboSync == nil {
		return false
	}
	kept := make([]DubboSyncEntry, 0, len(nc.DubboSync.Entries))
	changed := false
	for _, e := range nc.DubboSync.Entries {
		if e.ClusterA == cID || e.ClusterB == cID {
			changed = true
			continue
		}
		kept = append(kept, e)
	}
	nc.DubboSync.Entries = kept
	return changed
}
