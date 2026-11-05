// Package nacos 统一对接 Nacos 2.1.2（无鉴权）与 3.2.4（有鉴权），
// 按 (clusterID, namespace) 缓存 SDK 客户端，处理 namespace 透传与按需创建。
package nacos

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/cxcm/nacos-sync-backend/internal/config"
)

// ClientManager 管理与各 (集群, namespace) 对应的 SDK 客户端。
//
// 集群连接信息在调用时按 c_id 从运行时配置（nacos-group）动态解析并缓存，
// 而非启动时一次性固化：这样即使配置中心在 boot 阶段不可达（降级为空），
// 后续热更新拉到真实分组/集群后，引擎也能即时连上，无需重启或重建管理器。
type ClientManager struct {
	mu            sync.RWMutex
	conf          *config.Manager
	cfgClients    map[string]config_client.IConfigClient
	namingClients map[string]naming_client.INamingClient
	// liveProxies 缓存每个 (clusterID, namespace) 的实时查询闭包，基于常驻客户端底层 gRPC 代理，
	// 经反射调用其未导出的 serviceProxy.QueryInstancesOfService，发起不读本地缓存的实时 ServiceQuery
	// （见 InstancesLive / liveProxy）。value 为 *liveProxyEntry，含底层客户端与创建时间，用于 TTL 重建。
	liveProxies map[string]*liveProxyEntry

	// enumClients 缓存每个 (clusterID, namespace) 的「枚举服务列表」客户端（GetAllServicesInfo 用）。
	// 与 liveProxies 同源：该调用也走常驻 gRPC 连接，会随时间劣化（约 10 分钟返回空），故同样按
	// liveProxyTTL 周期强制重建，避免长运行后端对查找服务返回静默漏报。新建/关闭经 liveProxyMu 串行化，
	// 杜绝并发写 SDK 全局 clientMap。
	enumClients map[string]*liveProxyEntry

	// stopCh 用于关闭后台 sweep goroutine（Close 时关闭）。
	stopCh chan struct{}
	// closeOnce 保证 Close 幂等：重复调用 close(stopCh) 会 panic，用 Once 防御。
	closeOnce sync.Once
}

// liveProxyEntry 一条 live 查询客户端及其元数据。
type liveProxyEntry struct {
	client    *naming_client.NamingClient // SDK 底层命名客户端（TTL 到期即关闭重建）
	query     func(string, string) (*model.Service, error)
	createdAt time.Time
	// lastUsed 最近一次被访问（快/慢路径命中）的 UnixNano，由 atomic 读写避免与 map 锁竞争。
	// 仅用于 idle 淘汰：长期不被访问的 entry（如命名空间/分组已删除后残留的 key）由 sweep 关闭并清除，
	// 防止 liveProxies/enumClients 两张 map 只增不减导致内存泄漏与孤儿 SDK 客户端累积。
	lastUsed atomic.Int64
}

// liveProxyTTL 实时查询客户端最长存活时间。到期即重建新客户端——规避 nacos-sdk-go 常驻
// gRPC 客户端连接层随时间劣化（实测约 10 分钟会对个别服务返回空结果）的问题。
// 取 3 分钟，远小于劣化周期，保证连接始终新鲜。
const liveProxyTTL = 3 * time.Minute

// clientIdleTTL 客户端空闲淘汰阈值。liveProxies/enumClients 的 entry 若超过该时长未被访问
// （即 lastUsed 距今超过此值），视为孤儿（典型场景：命名空间/分组被删除后残留的 key），由 sweep 关闭并删除。
// 必须显著大于 liveProxyTTL：活跃客户端每 TTL 必被刷新一次 lastUsed，不会被误删；只有彻底不再被访问的 key 才会被淘汰。
const clientIdleTTL = 10 * time.Minute

// sweepInterval 后台淘汰扫描周期。
const sweepInterval = 1 * time.Minute

// liveProxyCloseGrace 关闭旧客户端前的宽限：让在途查询先完成，避免关闭瞬间仍有查询打在被关客户端上。
const liveProxyCloseGrace = 2 * time.Second

// liveProxyMu 串行化所有「SDK 客户端创建 + 关闭」。nolint:gochecknoglobals
// 原因：nacos-sdk-go 内部维护一张全局 clientMap，NewNamingClient 与 CloseClient 都会写它，
// 且该 map 非并发安全；先前每 1s 新建+立即关闭导致大量并发写 → fatal: concurrent map writes 崩溃。
// 所有对 SDK 客户端的创建/关闭统一经此锁串行化，彻底消除该并发写。
var liveProxyMu sync.Mutex


// clusterConn 一个 Nacos 服务端集群的连接信息（按 c_id 解析自 nacos-group）。
type clusterConn struct {
	CID        string
	Nodes      []string // host:port 列表
	GrpVersion string   // 2X | 3X，预留
	EnableAuth bool
	Username   string
	Password   string
}

// NewClientManager 构造。conf 用于按 c_id 动态解析集群连接。
func NewClientManager(conf *config.Manager) *ClientManager {
	m := &ClientManager{
		conf:          conf,
		cfgClients:    make(map[string]config_client.IConfigClient),
		namingClients: make(map[string]naming_client.INamingClient),
		liveProxies:   make(map[string]*liveProxyEntry),
		enumClients:   make(map[string]*liveProxyEntry),
		stopCh:        make(chan struct{}),
	}
	// 后台淘汰孤儿客户端（命名空间/分组删除后残留的 key），防止 map 只增不减。
	go m.sweep()
	return m
}

// connFor 按 c_id 在运行时配置的 nacos-group 中查找对应集群连接信息。
// 注：c_id 在分组内唯一；跨分组极小概率碰撞（4 位字母数字），这里取首个匹配。
func (m *ClientManager) connFor(cid string) (clusterConn, error) {
	cfg := m.conf.Get()
	for _, g := range cfg.NacosGroup {
		for _, cl := range g.Clusters {
			if cl.CID == cid {
				return clusterConn{
					CID:        cl.CID,
					Nodes:      cl.CNodes,
					GrpVersion: g.GrpVersion,
					EnableAuth: g.EnableAuth,
					Username:   g.AuthUsername,
					Password:   g.AuthPassword,
				}, nil
			}
		}
	}
	return clusterConn{}, fmt.Errorf("unknown cluster %q", cid)
}

func (m *ClientManager) serverConfigs(c clusterConn) []constant.ServerConfig {
	const ctxPath = "/nacos"
	out := make([]constant.ServerConfig, 0, len(c.Nodes))
	for _, addr := range c.Nodes {
		host, port := config.SplitHostPort(addr)
		out = append(out, constant.ServerConfig{IpAddr: host, Port: port, ContextPath: ctxPath})
	}
	return out
}

func (m *ClientManager) clientConfig(c clusterConn, namespace string) constant.ClientConfig {
	cc := constant.ClientConfig{
		NamespaceId:         namespace,
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogLevel:            "warn",
		LogDir:              "/tmp/nacos-sync-agent/log",
		CacheDir:            "/tmp/nacos-sync-agent/cache",
	}
	if c.EnableAuth {
		cc.Username = expandEnv(c.Username)
		cc.Password = expandEnv(c.Password)
	}
	return cc
}

// ConfigClient 返回（并按需缓存）指定集群+namespace 的配置客户端。
func (m *ClientManager) ConfigClient(clusterID, namespace string) (config_client.IConfigClient, error) {
	key := clusterID + "|" + namespace
	m.mu.RLock()
	c, ok := m.cfgClients[key]
	m.mu.RUnlock()
	if ok {
		return c, nil
	}
	conn, err := m.connFor(clusterID)
	if err != nil {
		return nil, err
	}
	cc := m.clientConfig(conn, namespace)
	cli, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  &cc,
		ServerConfigs: m.serverConfigs(conn),
	})
	if err != nil {
		return nil, fmt.Errorf("build config client for %s/%s: %w", clusterID, namespace, err)
	}
	m.mu.Lock()
	m.cfgClients[key] = cli
	m.mu.Unlock()
	return cli, nil
}

// NamingClient 返回（并按需缓存）指定集群+namespace 的命名客户端。
func (m *ClientManager) NamingClient(clusterID, namespace string) (naming_client.INamingClient, error) {
	key := clusterID + "|" + namespace
	m.mu.RLock()
	c, ok := m.namingClients[key]
	m.mu.RUnlock()
	if ok {
		return c, nil
	}
	// 创建 SDK 客户端会写其全局 clientMap（非并发安全），须与 liveProxy 重建/关闭统一串行化。
	liveProxyMu.Lock()
	defer liveProxyMu.Unlock()
	// 二次检查：可能另一协程已创建。
	m.mu.RLock()
	c, ok = m.namingClients[key]
	m.mu.RUnlock()
	if ok {
		return c, nil
	}
	nc, err := m.buildSDKClientUnlocked(clusterID, namespace)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.namingClients[key] = nc
	m.mu.Unlock()
	return nc, nil
}

// BuildDedicatedNamingClient 为单条同步方向构建「专属、不进入共享缓存」的命名客户端。
// 该客户端的生命周期由调用方（同步方向）全权负责：方向停止时必须调用 CloseDedicatedNamingClient 销毁，
// 以断开其 gRPC 连接并清空 SDK 内部注册缓存（registeredInstanceCached）——否则该连接断线重连时，
// SDK 的 connection_event_listener.redoRegisterEachService 会把已代注册、本应下线的实例自动重声明回服务端，
// 导致「禁用同步后代注册实例下不了线」（实测：进程存活期间每 60s 孤儿清扫删一次、随即又被重注册回来）。
// 构建与关闭均经 liveProxyMu 串行化，避免并发写 SDK 全局 clientMap。
func (m *ClientManager) BuildDedicatedNamingClient(clusterID, namespace string) (*naming_client.NamingClient, error) {
	liveProxyMu.Lock()
	defer liveProxyMu.Unlock()
	return m.buildSDKClientUnlocked(clusterID, namespace)
}

// CloseDedicatedNamingClient 关闭由 BuildDedicatedNamingClient 创建的专属客户端，断开其连接并清空注册缓存。
// 入参为 INamingClient 接口（调用方以接口持有即可），内部断言为具体类型后再关闭。
func (m *ClientManager) CloseDedicatedNamingClient(c naming_client.INamingClient) {
	if c == nil {
		return
	}
	nc, ok := c.(*naming_client.NamingClient)
	if !ok {
		return
	}
	liveProxyMu.Lock()
	defer liveProxyMu.Unlock()
	nc.CloseClient()
}

// NewClientManagerStub 仅供单元测试：返回一个 namingClients 已预置（clusterID|namespace → c）的
// ClientManager，使引擎层在不连真实 Nacos 的情况下也能取到命名客户端（走测试替身）。
// 生产代码不应调用本函数。
func NewClientManagerStub(clusterID, namespace string, c naming_client.INamingClient) *ClientManager {
	m := &ClientManager{}
	m.namingClients = map[string]naming_client.INamingClient{clusterID + "|" + namespace: c}
	return m
}

// enumClient 返回（并按需缓存/TTL 重建）指定集群+namespace 的命名客户端，专用于「枚举服务列表」
// （GetAllServicesInfo）。与 liveProxies 同源：该调用也走常驻 gRPC 连接，会随时间劣化（约 10 分钟
// 返回空），故以相同 liveProxyTTL 周期强制重建，避免长运行后端对查找服务返回静默漏报。
//
// 返回底层 *naming_client.NamingClient（而非 INamingClient 缓存）：枚举用客户端不应被同步引擎复用，
// 因此其生命周期独立管理（新建/关闭同样经 liveProxyMu 串行化，杜绝并发写 SDK 全局 clientMap）。
// EnumClient 是 enumClient 的导出形式，供同步引擎（namingsync）的 dubbo 动态方向
// 枚举源集群服务列表使用；缓存/TTL 重建语义与内部调用完全一致。
func (m *ClientManager) EnumClient(clusterID, namespace string) (*naming_client.NamingClient, error) {
	return m.enumClient(clusterID, namespace)
}

// 锁顺序与 NamingClient/liveProxy 一致：liveProxyMu 在外、m.mu 在内，无死锁风险。
func (m *ClientManager) enumClient(clusterID, namespace string) (*naming_client.NamingClient, error) {
	key := "enum|" + clusterID + "|" + namespace

	// 快路径：命中且未过期，直接返回（不加生命周期锁）。
	m.mu.RLock()
	e, ok := m.enumClients[key]
	if ok && time.Since(e.createdAt) < liveProxyTTL {
		c := e.client
		e.lastUsed.Store(time.Now().UnixNano())
		m.mu.RUnlock()
		return c, nil
	}
	m.mu.RUnlock()

	// 慢路径：重建。liveProxyMu 串行化「新建+关闭」，避免并发写 SDK 全局 clientMap。
	liveProxyMu.Lock()
	defer liveProxyMu.Unlock()

	// 二次检查：可能另一协程已重建。
	m.mu.RLock()
	e, ok = m.enumClients[key]
	if ok && time.Since(e.createdAt) < liveProxyTTL {
		c := e.client
		m.mu.RUnlock()
		return c, nil
	}
	m.mu.RUnlock()

	nc, err := m.buildSDKClientUnlocked(clusterID, namespace)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	old := m.enumClients[key]
	entry := &liveProxyEntry{client: nc, createdAt: time.Now()}
	entry.lastUsed.Store(time.Now().UnixNano())
	m.enumClients[key] = entry
	m.mu.Unlock()

	if old != nil {
		// 宽限后在持锁下关闭旧客户端：宽限让在途查询先完成；持锁避免与任何新建/关闭并发写 clientMap。
		go func(c *naming_client.NamingClient) {
			time.Sleep(liveProxyCloseGrace)
			liveProxyMu.Lock()
			c.CloseClient()
			liveProxyMu.Unlock()
		}(old.client)
	}
	return nc, nil
}

// buildSDKClientUnlocked 在持有 liveProxyMu 的前提下新建一个 SDK 命名客户端。
// 调用方必须已持有 liveProxyMu（NamingClient / liveProxy 均如此），以保证对 SDK 全局 clientMap 的写被串行化。
func (m *ClientManager) buildSDKClientUnlocked(clusterID, namespace string) (*naming_client.NamingClient, error) {
	conn, err := m.connFor(clusterID)
	if err != nil {
		return nil, err
	}
	cc := m.clientConfig(conn, namespace)
	cli, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  &cc,
		ServerConfigs: m.serverConfigs(conn),
	})
	if err != nil {
		return nil, fmt.Errorf("build naming client for %s/%s: %w", clusterID, namespace, err)
	}
	nc, ok := cli.(*naming_client.NamingClient)
	if !ok {
		return nil, fmt.Errorf("unexpected naming client type %T for %s/%s", cli, clusterID, namespace)
	}
	return nc, nil
}

// InstancesLive 绕过 SDK 本地实例缓存，使用命名客户端的底层 gRPC 代理发起实时
// ServiceQuery，返回源集群当前实例列表。
//
// 为什么这样取数：
//   - Nacos 3.x 已移除列出实例的 HTTP OpenAPI（实测 /v1/ns/instances 返回 501），唯一读路径是 gRPC。
//   - SDK 的 SelectAllInstances 读本地 host 缓存，仅由推送/周期刷新更新（本环境 gRPC 推送不可靠，
//     缓存 ~10s 陈旧），直接用它会使「源实例消失→目标注销」延迟 10s+。
//   - 底层 INamingProxy.QueryInstancesOfService 直接发送 ServiceQueryRequest、不读本地缓存，
//     因此每次轮询都是实时数据（注销延迟 ≤ 轮询间隔）。
//
// 客户端生命周期（见 liveProxy）：
//   - 早期方案每 1s 新建瞬时客户端再 CloseClient，触发 SDK 内部 RpcClient.Shutdown 与后台
//     health-check 协程并发写同一张全局 map，导致 fatal error: concurrent map writes 进程崩溃。
//   - 现改为：每个 (clusterID, namespace) 维护一条 live 客户端，最长存活 liveProxyTTL（3 分钟）即
//     “新建+关闭旧”重建（绕过命名客户端缓存以拿到新鲜连接），规避 SDK 常驻 gRPC 连接层随时间劣化
//     （实测约 10 分钟会对个别服务返回空）。所有新建/关闭经 liveProxyMu 串行化，杜绝并发写 clientMap。
func (m *ClientManager) InstancesLive(clusterID, namespace, serviceName, groupName string) ([]model.Instance, error) {
	query, err := m.liveProxy(clusterID, namespace)
	if err != nil {
		return nil, err
	}

	// 常驻客户端首查时 gRPC 可能尚在异步建连，偶发 STARTING/not connected；轻量重试，
	// 避免把「建连慢」误判为源不可达。后续轮询复用已连连接，不再重试。
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		var svc *model.Service
		svc, lastErr = query(serviceName, groupName)
		if lastErr == nil {
			if svc == nil {
				return nil, nil
			}
			return svc.Hosts, nil
		}
		if strings.Contains(lastErr.Error(), "STARTING") || strings.Contains(lastErr.Error(), "not connected") {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		break
	}
	if isConnError(lastErr) {
		// 连接层故障（节点重启/UNHEALTHY）：立即失效缓存的 live 客户端，
		// 下一次快轮询（≤ sourcePollInterval）即重建全新 gRPC 连接自愈，
		// 无需等待 liveProxyTTL，也无需重启后端进程。
		m.invalidateLiveProxy(clusterID, namespace)
	}
	return nil, fmt.Errorf("live query source %s/%s: %w", serviceName, groupName, lastErr)
}

// liveProxy 返回（并按需缓存/重建）指定 clusterID+namespace 的实时查询闭包。
// Nacos SDK v2.3.5 的 NamingClient 未公开 QueryInstancesOfService（直接发实时 ServiceQueryRequest、
// 不读本地缓存），故通过反射取出其未导出字段 serviceProxy 并绑定其导出的 QueryInstancesOfService
// 方法，封装为一个类型安全的闭包供 InstancesLive 复用。注意：不可用 rv.Interface() 取未导出字段的值
// （Go 会 panic），因此这里用 MethodByName+Call 直接调用，不暴露底层接口值。
//
// TTL 重建：每条 live 客户端最长存活 liveProxyTTL，到期即“新建+关闭旧”重建。原因：SDK 常驻 gRPC 客户端
// 连接层会随时间劣化（约 10 分钟对个别服务返回空），且重建时必须绕过命名客户端缓存（NamingClient 复用
// 同一实例，无法拿到新连接）。所有「新建/关闭」均经 liveProxyMu 串行化，杜绝对 SDK 全局 clientMap 的并发写
// （否则 fatal: concurrent map writes）。
func (m *ClientManager) liveProxy(clusterID, namespace string) (func(string, string) (*model.Service, error), error) {
	key := "live|" + clusterID + "|" + namespace

	// 快路径：命中且未过期，直接返回（不加生命周期锁）。
	m.mu.RLock()
	if e, ok := m.liveProxies[key]; ok && time.Since(e.createdAt) < liveProxyTTL {
		q := e.query
		e.lastUsed.Store(time.Now().UnixNano())
		m.mu.RUnlock()
		return q, nil
	}
	m.mu.RUnlock()

	// 慢路径：重建。liveProxyMu 串行化“新建+关闭”，避免并发写 SDK 全局 clientMap。
	liveProxyMu.Lock()
	defer liveProxyMu.Unlock()

	// 二次检查：可能另一协程已重建。
	m.mu.RLock()
	if e, ok := m.liveProxies[key]; ok && time.Since(e.createdAt) < liveProxyTTL {
		q := e.query
		m.mu.RUnlock()
		return q, nil
	}
	m.mu.RUnlock()

	// 绕过命名客户端缓存，强制新建 SDK 客户端以拿到新鲜连接。
	nc, err := m.buildSDKClientUnlocked(clusterID, namespace)
	if err != nil {
		return nil, err
	}
	query, err := buildLiveQuery(nc)
	if err != nil {
		return nil, err
	}

	newEntry := &liveProxyEntry{client: nc, query: query, createdAt: time.Now()}
	newEntry.lastUsed.Store(time.Now().UnixNano())
	m.mu.Lock()
	old := m.liveProxies[key]
	m.liveProxies[key] = newEntry
	m.mu.Unlock()

	if old != nil {
		// 宽限后在持锁下关闭旧客户端：宽限让在途查询先完成；持锁避免与任何新建/关闭并发写 clientMap。
		go func(c *naming_client.NamingClient) {
			time.Sleep(liveProxyCloseGrace)
			liveProxyMu.Lock()
			c.CloseClient()
			liveProxyMu.Unlock()
		}(old.client)
	}
	return query, nil
}

// buildLiveQuery 从 *naming_client.NamingClient 的未导出 serviceProxy 上反射出实时查询闭包。
// 用 unsafe 取出字段地址并以 reflect.NewAt 重建一个不带「来自未导出字段」标记的 Value，
// 从而合法调用其导出的 QueryInstancesOfService（直接发实时 ServiceQuery、不读本地缓存）。
// 若直接对原 rv 用 .Interface()/.Call，Go 会 panic（未导出字段的值不可经反射暴露/调用）。
func buildLiveQuery(cli *naming_client.NamingClient) (func(string, string) (*model.Service, error), error) {
	rv := reflect.ValueOf(cli).Elem().FieldByName("serviceProxy")
	if !rv.IsValid() {
		return nil, fmt.Errorf("cannot access serviceProxy of naming client")
	}
	if !rv.CanAddr() {
		return nil, fmt.Errorf("serviceProxy not addressable")
	}
	fieldPtr := unsafe.Pointer(rv.UnsafeAddr())
	clean := reflect.NewAt(rv.Type(), fieldPtr).Elem()
	method := clean.MethodByName("QueryInstancesOfService")
	if !method.IsValid() {
		return nil, fmt.Errorf("serviceProxy has no QueryInstancesOfService")
	}
	return func(serviceName, groupName string) (*model.Service, error) {
		out := method.Call([]reflect.Value{
			reflect.ValueOf(serviceName),
			reflect.ValueOf(groupName),
			reflect.ValueOf(""),
			reflect.ValueOf(0),
			reflect.ValueOf(false),
		})
		if !out[1].IsNil() {
			return nil, out[1].Interface().(error)
		}
		svc, _ := out[0].Interface().(*model.Service)
		return svc, nil
	}, nil
}

// isConnError 判断错误是否由 gRPC 连接层故障（而非业务错误）引起。nacos-sdk-go 在连接断开/
// 尚未建立时返回 "client not connected, current status:UNHEALTHY" 或刚建连时的 "STARTING"，
// 节点被重启/下线后常驻连接会落入此态。此类错误重试无意义，必须通过「关闭并重建底层客户端」
// 才能恢复——这正是「单节点重启后同步卡死、且 disable+enable 也救不回」的根因。
func isConnError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "not connected") ||
		strings.Contains(msg, "UNHEALTHY") ||
		strings.Contains(msg, "STARTING") ||
		strings.Contains(msg, "client is closed") ||
		strings.Contains(msg, "no available server") ||
		strings.Contains(msg, "Connection refused")
}

// IsConnectionError exposes the SDK connection-failure classifier to sync engines
// so dedicated clients can be rebuilt instead of remaining permanently unhealthy.
func IsConnectionError(err error) bool { return isConnError(err) }

// invalidateLiveProxy 关闭并移除指定 (clusterID, namespace) 的实时查询客户端缓存，
// 使下次 InstancesLive 强制重建一条全新 gRPC 连接。用于连接层故障（节点重启/UNHEALTHY）的
// 即时自愈：无需等待 liveProxyTTL，下一次快轮询（≤ sourcePollInterval）即拿到新连接。
// 关闭在持 liveProxyMu 下延迟 liveProxyCloseGrace 执行，避免与在途查询/其它新建并发写 SDK 全局 clientMap。
func (m *ClientManager) invalidateLiveProxy(clusterID, namespace string) {
	key := "live|" + clusterID + "|" + namespace
	m.mu.Lock()
	e, ok := m.liveProxies[key]
	delete(m.liveProxies, key)
	m.mu.Unlock()
	if ok && e != nil {
		go func(c *naming_client.NamingClient) {
			time.Sleep(liveProxyCloseGrace)
			liveProxyMu.Lock()
			c.CloseClient()
			liveProxyMu.Unlock()
		}(e.client)
	}
}

// invalidateNamingClient 关闭并移除指定 (clusterID, namespace) 的常驻命名客户端缓存（写路径自愈）。
// 与 invalidateLiveProxy 同源：节点重启后缓存的 gRPC 连接可能卡在 UNHEALTHY，需在写操作失败时即时失效重建。
func (m *ClientManager) invalidateNamingClient(clusterID, namespace string) {
	key := clusterID + "|" + namespace
	m.mu.Lock()
	c, ok := m.namingClients[key]
	delete(m.namingClients, key)
	m.mu.Unlock()
	if ok {
		if nc, ok := c.(*naming_client.NamingClient); ok {
			go func(c *naming_client.NamingClient) {
				time.Sleep(liveProxyCloseGrace)
				liveProxyMu.Lock()
				c.CloseClient()
				liveProxyMu.Unlock()
			}(nc)
		}
	}
}

// InvalidateOnConnError 若 err 为连接层故障，则失效指定集群的常驻命名客户端（写路径自愈）。
// 供引擎层 registerProxied/deregisterProxied 在写操作失败时调用，使目标集群节点重启后能自动恢复。
func (m *ClientManager) InvalidateOnConnError(err error, clusterID, namespace string) {
	if isConnError(err) {
		m.invalidateNamingClient(clusterID, namespace)
	}
}

// clientCloser 两类 SDK 客户端（命名/配置）共同实现的关闭接口，用于对统一切片逐一关闭。
type clientCloser interface {
	CloseClient()
}

// sweep 后台周期扫描并淘汰空闲客户端（见 clientIdleTTL）。stopCh 关闭时退出。
func (m *ClientManager) sweep() {
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
			m.sweepOnce()
		}
	}
}

// sweepOnce 收集并关闭超过 clientIdleTTL 未被访问的 liveProxies/enumClients entry，再从 map 删除。
// 锁顺序遵循 liveProxyMu 在外、m.mu 在内（与 liveProxy/enumClient 一致），避免死锁。
func (m *ClientManager) sweepOnce() {
	liveProxyMu.Lock()
	m.mu.Lock()
	now := time.Now()
	var toClose []clientCloser
	for k, e := range m.liveProxies {
		if now.Sub(time.Unix(0, e.lastUsed.Load())) > clientIdleTTL {
			toClose = append(toClose, e.client)
			delete(m.liveProxies, k)
		}
	}
	for k, e := range m.enumClients {
		if now.Sub(time.Unix(0, e.lastUsed.Load())) > clientIdleTTL {
			toClose = append(toClose, e.client)
			delete(m.enumClients, k)
		}
	}
	m.mu.Unlock()
	liveProxyMu.Unlock()

	for _, c := range toClose {
		liveProxyMu.Lock()
		c.CloseClient()
		liveProxyMu.Unlock()
	}
}

// Close 停止后台 sweep 并关闭所有缓存的 SDK 客户端，释放其后台 goroutine。
// 应在引擎 Stop() 之后调用（此时方向 goroutine 已全部退出，不再持有 dstClient）。
func (m *ClientManager) Close() {
	m.closeOnce.Do(func() {
		if m.stopCh != nil {
			close(m.stopCh)
		}
	})
	liveProxyMu.Lock()
	m.mu.Lock()
	var toClose []clientCloser
	for _, e := range m.liveProxies {
		toClose = append(toClose, e.client)
	}
	for _, e := range m.enumClients {
		toClose = append(toClose, e.client)
	}
	for _, c := range m.namingClients {
		if nc, ok := c.(*naming_client.NamingClient); ok {
			toClose = append(toClose, nc)
		}
	}
	for _, c := range m.cfgClients {
		if cc, ok := c.(*config_client.ConfigClient); ok {
			toClose = append(toClose, cc)
		}
	}
	m.liveProxies = make(map[string]*liveProxyEntry)
	m.enumClients = make(map[string]*liveProxyEntry)
	m.namingClients = make(map[string]naming_client.INamingClient)
	m.cfgClients = make(map[string]config_client.IConfigClient)
	m.mu.Unlock()
	liveProxyMu.Unlock()

	for _, c := range toClose {
		liveProxyMu.Lock()
		c.CloseClient()
		liveProxyMu.Unlock()
	}
}

func expandEnv(s string) string {
	if len(s) > 3 && s[0] == '$' && s[1] == '{' && s[len(s)-1] == '}' {
		if v, ok := os.LookupEnv(s[2 : len(s)-1]); ok {
			return v
		}
		// 未定义的环境变量不再静默返回空串：保留原始占位符，避免 Nacos 口令 / 鉴权用户名为空导致静默鉴权失败。
		return s
	}
	return s
}

// TestConnectionSpec 探测目标集群连通性所需的连接参数。
type TestConnectionSpec struct {
	Address    string // host:port
	EnableAuth bool
	Username   string
	Password   string
}

// TestConnection 分两步校验目标集群：
//  1. 网络/naming 可达性：临时构建命名客户端并做一次只读调用；
//  2. 若开启鉴权，再用登录接口核验凭据（naming 读取接口服务端通常不强制鉴权，
//     仅靠第 1 步无法发现密码错误，故必须显式登录校验）。
//
// 返回 nil 表示可达且（若要求）鉴权通过；否则返回具体错误（连接拒绝 / 鉴权失败等）。
// 该函数不写入客户端缓存，仅用于管理面「测试连接」按钮。
func TestConnection(spec TestConnectionSpec) error {
	if err := probeNaming(spec); err != nil {
		return err
	}
	if spec.EnableAuth {
		if err := verifyAuth(spec); err != nil {
			return err
		}
	}
	return nil
}

// probeNaming 临时构建一个命名客户端并做一次只读调用，确认目标集群网络可达。
//
// Nacos 2.x/3.x 的 naming 服务强依赖 gRPC 端口（HTTP 端口 + 1000）。若该端口被防火墙
// 拦截、或 Nacos 根本未在该地址监听，SDK 只会返回晦涩的 “client not connected,
// current status:STARTING”。这里先做 TCP 预检把“地址不可达 / 端口未监听”讲清楚，
// 再做 SDK 探测并对偶发的 STARTING（客户端异步建连未完成）做轻量重试。
func probeNaming(spec TestConnectionSpec) error {
	host, port := config.SplitHostPort(spec.Address)
	grpcPort := port + 1000

	// 预检：区分“基础设施不可达”与“SDK 业务层错误”。
	httpOk := tcpReachable(host, port, 3*time.Second)
	grpcOk := tcpReachable(host, grpcPort, 3*time.Second)
	if !httpOk && !grpcOk {
		return fmt.Errorf("probe failed: 地址 %s:%d 不可达（TCP 连接被拒绝或超时）——请确认该主机已启动 Nacos 并监听 %d（gRPC %d），或地址填写有误",
			host, port, port, grpcPort)
	}
	if !grpcOk {
		return fmt.Errorf("probe failed: Nacos gRPC 端口 %s:%d 不可达（naming 服务强依赖 gRPC）——请确认防火墙/安全组放行 %d，或地址填写有误",
			host, grpcPort, grpcPort)
	}

	user, pass := "", ""
	if spec.EnableAuth {
		user, pass = expandEnv(spec.Username), expandEnv(spec.Password)
	}
	nc, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig: &constant.ClientConfig{
			NamespaceId:         "", // public 命名空间真实 ID 为空串
			TimeoutMs:           3000,
			NotLoadCacheAtStart: true,
			LogLevel:            "error",
			LogDir:              "/tmp/nacos-sync-probe/log",
			CacheDir:            "/tmp/nacos-sync-probe/cache",
			Username:            user,
			Password:            pass,
		},
		ServerConfigs: []constant.ServerConfig{
			{IpAddr: host, Port: port, ContextPath: "/nacos"},
		},
	})
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}

	// SDK 客户端异步建连，刚创建时可能处于 STARTING 状态。对偶发的 STARTING 轻量重试，
	// 避免把“建连慢”误判为不可达；遇到其它真实错误（鉴权/超时）立即返回。
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		_, lastErr = nc.GetAllServicesInfo(vo.GetAllServiceInfoParam{NameSpace: "", PageNo: 1, PageSize: 1})
		if lastErr == nil {
			return nil
		}
		if strings.Contains(lastErr.Error(), "STARTING") {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		break
	}
	if lastErr != nil {
		return fmt.Errorf("probe failed: %w", lastErr)
	}
	return nil
}

// NodeReachable 判断 host:port 形态的 Nacos 节点是否 TCP 可达。
// 供管理面「随机挑一个可用节点取数」使用（分组内无主从之分，任一可用节点都等价）。
func NodeReachable(addr string, timeout time.Duration) bool {
	host, port := config.SplitHostPort(addr)
	return tcpReachable(host, port, timeout)
}

// tcpReachable 对 host:port 做一次 TCP 连接探测。用于快速区分“基础设施不可达”
// （主机宕机/路由不通/端口未监听/防火墙拦截）与“SDK 业务层错误”。
func tcpReachable(host string, port uint64, timeout time.Duration) bool {
	addr := net.JoinHostPort(host, strconv.FormatUint(port, 10))
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// verifyAuth 通过 Nacos 登录接口（/nacos/v1/auth/login）核验用户名/密码是否正确。
// 当服务端启用鉴权且密码错误时，该接口返回非 200（通常为 403）或带 errorCode 的响应；
// 当服务端未启用鉴权时，该接口会直接返回 token（无法据此校验密码，属于服务端配置问题）。
func verifyAuth(spec TestConnectionSpec) error {
	host, port := config.SplitHostPort(spec.Address)
	u := fmt.Sprintf("http://%s:%d/nacos/v1/auth/login", host, port)
	form := url.Values{}
	form.Set("username", expandEnv(spec.Username))
	form.Set("password", expandEnv(spec.Password))
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(u, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("鉴权请求失败（节点不可达？）：%w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("鉴权失败：服务器返回 HTTP %d（用户名或密码错误）", resp.StatusCode)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err == nil {
		if _, ok := m["accessToken"]; ok {
			return nil
		}
		if _, ok := m["token"]; ok {
			return nil
		}
		if _, ok := m["errorCode"]; ok {
			msg, _ := m["message"].(string)
			if msg == "" {
				msg, _ = m["errorMsg"].(string)
			}
			return fmt.Errorf("鉴权失败（错误码 %v）：%s", m["errorCode"], msg)
		}
	}
	// 200 但结构无法识别（多为未启用鉴权的集群返回占位 token），保守放行。
	return nil
}
