package nacos

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/cxcm/nacos-sync-backend/internal/config"
)

// lookupSvcGroup 业务服务通常注册在 DEFAULT_GROUP（与同步引擎一致）。
// 当前查找仅匹配该分组下的服务；非 DEFAULT_GROUP 的服务暂不纳入。
const lookupSvcGroup = "DEFAULT_GROUP"

// lookupNodeProbeTimeout 取数前挑选「随机可达节点」时的单次 TCP 预检超时。
const lookupNodeProbeTimeout = 1 * time.Second

// LookupInstance 一个命中 IP 的实例（精简字段，供前端展示）。
type LookupInstance struct {
	IP      string  `json:"ip"`
	Port    uint64  `json:"port"`
	Healthy bool    `json:"healthy"`
	Enabled bool    `json:"enabled"`
	Weight  float64 `json:"weight"`
}

// LookupHit 一个「服务名 + 命中实例」的结果条目，含其所在分组/集群/命名空间定位信息。
type LookupHit struct {
	GrpID       string           `json:"grp_id"`
	GrpName     string           `json:"grp_name"`
	ClusterID   string           `json:"cluster_id"`
	ClusterName string           `json:"cluster_name"`
	Namespace   string           `json:"namespace"` // "" = public
	Service     string           `json:"service"`
	Group       string           `json:"group"`
	Node        string           `json:"node"` // 本次取数实际使用的节点（该集群内随机一个可达节点）
	Instances   []LookupInstance `json:"instances"`
}

// LookupResult 按 IP（可选端口）查找匹配服务的汇总结果。
type LookupResult struct {
	IP     string      `json:"ip"`
	Port   *uint64     `json:"port"` // nil 表示仅按 IP 匹配
	Hits   []LookupHit `json:"hits"`
	Errors []string    `json:"errors"` // 各分组/集群的取数失败原因（不阻断其它分组）
}

// LookupServicesByIP 在【所有分组】下查找：每个分组随机挑一个集群（集群内任选一个可达节点）作为取数源，
// 枚举其命名空间，再枚举每个命名空间下的服务名，实时查询实例并按 IP（及可选端口）过滤。
//
// 分组内各集群互为镜像，随机一个集群即代表该分组；结果汇总返回所有匹配的服务名及其定位信息。
// 取数走实时 gRPC 查询（InstancesLive），不读 SDK 本地缓存，保证命中实例是当前真实状态。
//
// 注意：当前仅匹配 DEFAULT_GROUP 下的服务（与同步引擎一致）。非 DEFAULT_GROUP 的服务暂不纳入。
func (m *ClientManager) LookupServicesByIP(ctx context.Context, ip string, port *uint64) (*LookupResult, error) {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return nil, fmt.Errorf("ip required")
	}
	res := &LookupResult{IP: ip, Port: port, Hits: []LookupHit{}, Errors: []string{}}
	var resMu sync.Mutex
	// addErr 线程安全地追加告警/错误到 res.Errors（并发分组、并发命名空间下共享同一切片）。
	addErr := func(s string) {
		resMu.Lock()
		res.Errors = append(res.Errors, s)
		resMu.Unlock()
	}
	cfg := m.conf.Get()

	var wg sync.WaitGroup
	for _, g := range cfg.NacosGroup {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			if len(g.Clusters) == 0 {
				return
			}
			// 随机挑一个集群（分组内各集群互为镜像，一个即代表该分组）。
			cl := g.Clusters[rand.Intn(len(g.Clusters))]
			node := pickReachableNode(cl.CNodes)
			if node == "" {
				addErr(fmt.Sprintf("分组 %s/%s 集群 %s/%s 无可用节点", g.GrpID, g.GrpName, cl.CID, cl.CName))
				return
			}
			// 从该随机节点枚举命名空间（3.x 需 admin/console 接口；失败则降级为仅 public）。
			nss, nsErr := m.enumerateNamespaces(node, g)
			if nsErr != nil {
				addErr(fmt.Sprintf("分组 %s/%s 集群 %s/%s 命名空间枚举失败: %s", g.GrpID, g.GrpName, cl.CID, cl.CName, nsErr.Error()))
			}
			if len(nss) == 0 {
				nss = []NamespaceCount{{Namespace: "", NamespaceShowName: "public"}}
			}
			for _, ns := range nss {
				if ctx.Err() != nil {
					return
				}
				// 即便某命名空间查找出错，也已尽力返回已匹配的命中（不丢弃），仅追加告警。
				hits, err := m.lookupInNamespace(ctx, ip, port, g, cl, node, ns.Namespace, addErr)
				resMu.Lock()
				res.Hits = append(res.Hits, hits...)
				resMu.Unlock()
				if err != nil {
					addErr(fmt.Sprintf("分组 %s/%s 命名空间 %q 查找失败: %s", g.GrpID, g.GrpName, ns.Namespace, err.Error()))
				}
			}
		}()
	}
	wg.Wait()
	return res, nil
}

// pickReachableNode 在节点列表中随机打散，优先返回一个 TCP 可达节点；若全部不可达，
// 退化为随机首项（让取数报出具体错误，而不是笼统地说无可用节点）。
func pickReachableNode(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	for _, i := range rand.Perm(len(nodes)) {
		host, p := config.SplitHostPort(nodes[i])
		if tcpReachable(host, p, lookupNodeProbeTimeout) {
			return nodes[i]
		}
	}
	return nodes[rand.Intn(len(nodes))]
}

// enumerateNamespaces 从指定节点枚举命名空间列表。
//   - 3X：走 V3 接口（HTTP 枚举全部命名空间，含 configCount）；
//   - 2X 或未设置版本：先试 HTTP（2.x 原生 /v1/console/namespaces），失败（很可能是 3.x 已移除该接口）
//     再试 V3 兜底，最大限度枚举命名空间。
func (m *ClientManager) enumerateNamespaces(node string, g config.NacosGroup) ([]NamespaceCount, error) {
	spec := ConfigOverviewSpec{
		Address:    node,
		GrpVersion: g.GrpVersion,
		EnableAuth: g.EnableAuth,
		Username:   g.AuthUsername,
		Password:   g.AuthPassword,
	}
	if g.GrpVersion == "3X" {
		return FetchNamespacesV3(spec)
	}
	if ns, err := FetchNamespacesHTTP(spec); err == nil {
		return ns, nil
	}
	if g.GrpVersion == "" {
		// 版本未显式设置：HTTP 失败很可能是 3.x，再试 V3 兜底。
		return FetchNamespacesV3(spec)
	}
	return nil, fmt.Errorf("namespace enumeration failed")
}

// lookupInNamespace 在单个命名空间内枚举服务并实时查询实例，按 IP（及可选端口）过滤。
// addErr 用于线程安全地追加非阻断告警（单服务查询失败汇总、超量截断），由调用方在并发分组下共享。
func (m *ClientManager) lookupInNamespace(ctx context.Context, ip string, port *uint64, g config.NacosGroup, cl config.NacosCluster, node, namespace string, addErr func(string)) ([]LookupHit, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// 枚举用客户端需 TTL 重建（与 InstancesLive 同源的劣化问题），故走 enumClient 而非永久缓存的 NamingClient。
	nc, err := m.enumClient(cl.CID, namespace)
	if err != nil {
		return nil, err
	}
	hits := make([]LookupHit, 0)
	pageSize := uint32(200)
	page := uint32(1)
	processed := 0
	// skipped：单个服务实时查询失败（被静默 continue）的计数；循环结束汇总告警，避免假阴性无提示。
	skipped := 0
	for {
		if ctx.Err() != nil {
			return hits, ctx.Err()
		}
		sl, e := nc.GetAllServicesInfo(vo.GetAllServiceInfoParam{NameSpace: namespace, PageNo: page, PageSize: pageSize})
		if e != nil {
			return hits, e
		}
		for _, svc := range sl.Doms {
			if ctx.Err() != nil {
				return hits, ctx.Err()
			}
			// 实时查询（不读本地缓存），按 IP/端口过滤。单个服务查询失败计入 skipped，不阻断整体。
			insts, ie := m.InstancesLive(cl.CID, namespace, svc, lookupSvcGroup)
			if ie != nil {
				skipped++
				continue
			}
			if matched := filterByIP(insts, ip, port); len(matched) > 0 {
				hits = append(hits, LookupHit{
					GrpID:       g.GrpID,
					GrpName:     g.GrpName,
					ClusterID:   cl.CID,
					ClusterName: cl.CName,
					Namespace:   namespace,
					Service:     svc,
					Group:       lookupSvcGroup,
					Node:        node,
					Instances:   matched,
				})
			}
		}
		processed += len(sl.Doms)
		// 末页（不足一页）：停止翻页。
		if int64(len(sl.Doms)) < int64(pageSize) {
			break
		}
		// 超量保护：仍返回已匹配的命中，但上报截断告警（可能漏报靠后的服务）。
		if processed > 5000 {
			addErr(fmt.Sprintf("分组 %s/%s 命名空间 %q 服务数超过 5000，结果已截断（processed=%d），可能漏报部分服务", g.GrpID, g.GrpName, namespace, processed))
			break
		}
		page++
	}
	if skipped > 0 {
		addErr(fmt.Sprintf("分组 %s/%s 命名空间 %q 有 %d 个服务的实时实例查询失败，可能漏报", g.GrpID, g.GrpName, namespace, skipped))
	}
	return hits, nil
}

// filterByIP 从实例列表中筛选出 IP（及可选端口）匹配的实例。
func filterByIP(insts []model.Instance, ip string, port *uint64) []LookupInstance {
	out := make([]LookupInstance, 0)
	for _, it := range insts {
		if it.Ip != ip {
			continue
		}
		if port != nil && it.Port != *port {
			continue
		}
		out = append(out, LookupInstance{
			IP:      it.Ip,
			Port:    it.Port,
			Healthy: it.Healthy,
			Enabled: it.Enable,
			Weight:  it.Weight,
		})
	}
	return out
}
