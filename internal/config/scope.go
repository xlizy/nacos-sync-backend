package config

import (
	"regexp"
	"sync/atomic"
)

// ScopeStore 以原子方式持有当前配置，支持运行时热替换（文件变更或 API 更新）。
type ScopeStore struct {
	v atomic.Value // *Config
}

// NewScopeStore 构造。
func NewScopeStore(c *Config) *ScopeStore {
	s := &ScopeStore{}
	s.v.Store(c)
	return s
}

// Get 返回当前生效配置（并发安全）。
func (s *ScopeStore) Get() *Config { return s.v.Load().(*Config) }

// Set 替换生效配置。
func (s *ScopeStore) Set(c *Config) { s.v.Store(c) }

// UpdateScope 原地更新（或新增）命名范围，仅原子替换生效配置；
// 持久化由 Manager.UpdateScope 负责发布回配置中心 Nacos。
func (s *ScopeStore) UpdateScope(name string, sc SyncScope) error {
	c := s.Get()
	nc := *c
	nc.Scopes = make(map[string]SyncScope, len(c.Scopes)+1)
	for k, v := range c.Scopes {
		nc.Scopes[k] = v
	}
	nc.Scopes[name] = sc
	s.Set(&nc)
	return nil
}

// InScope 判定 (namespace, group, service) 是否在范围内。
// 规则：exclude 命中即排除（优先级最高）；否则 include 为空表示全放，
// 非空则要求 namespace/group/service 三个维度各自放行（维度列表为空=该维度不约束）。
func (sc SyncScope) InScope(namespace, group, service string) bool {
	if matchesFilter(sc.Exclude, namespace, group, service) {
		return false
	}
	inc := sc.Include
	nsPass := len(inc.Namespaces) == 0 || contains(inc.Namespaces, "*") || contains(inc.Namespaces, namespace)
	grpPass := len(inc.Groups) == 0 || contains(inc.Groups, "*") || contains(inc.Groups, group)
	svcPass := len(inc.Services) == 0 || matchAny(inc.Services, service)
	return nsPass && grpPass && svcPass
}

func matchesFilter(f ScopeFilter, namespace, group, service string) bool {
	if len(f.Namespaces) > 0 && (contains(f.Namespaces, "*") || contains(f.Namespaces, namespace)) {
		return true
	}
	if len(f.Groups) > 0 && (contains(f.Groups, "*") || contains(f.Groups, group)) {
		return true
	}
	if len(f.Services) > 0 && matchAny(f.Services, service) {
		return true
	}
	return false
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// matchAny 先尝试精确匹配，再尝试正则（锚定 ^...$）。
func matchAny(patterns []string, v string) bool {
	for _, p := range patterns {
		if p == v {
			return true
		}
		if re, err := regexp.Compile("^" + p + "$"); err == nil && re.MatchString(v) {
			return true
		}
	}
	return false
}
