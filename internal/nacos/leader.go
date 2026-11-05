package nacos

import (
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

const (
	// LeaderService 是同步后端在目标集群中的内部选主服务。候选实例是临时实例，
	// 进程退出或连接失效后由 Nacos 自动摘除，不依赖本地状态或额外数据库。
	LeaderService       = "NACOS-SYNC-LEADER"
	leaderDirectionMeta = "syncDirection"
)

// DirectionLeader 基于目标 Nacos 的实时候选实例做确定性选主。
// 相同配置的多个后端使用相同 scope；只有排序后的第一个候选可以执行写对账。
type DirectionLeader struct {
	mgr       *ClientManager
	agentIP   string
	agentPort uint64
}

func NewDirectionLeader(mgr *ClientManager, agentIP string, agentPort uint64) *DirectionLeader {
	return &DirectionLeader{mgr: mgr, agentIP: agentIP, agentPort: agentPort}
}

func leaderCluster(scope string) string {
	sum := sha256.Sum256([]byte(scope))
	return fmt.Sprintf("SYNC_%x", sum[:8])
}

func (l *DirectionLeader) candidateParam(scope string) vo.RegisterInstanceParam {
	return vo.RegisterInstanceParam{
		Ip:          l.agentIP,
		Port:        l.agentPort,
		ServiceName: LeaderService,
		GroupName:   "DEFAULT_GROUP",
		ClusterName: leaderCluster(scope),
		Weight:      1,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{leaderDirectionMeta: scope},
	}
}

// Ensure 确保本节点候选存在，并返回本节点是否为该 scope 的当前写主。
// 查询失败时返回错误，调用方必须停止所有写操作，不能退化为多节点同时写。
func (l *DirectionLeader) Ensure(client naming_client.INamingClient, clusterID, namespace, scope string) (bool, error) {
	instances, err := l.mgr.InstancesLive(clusterID, namespace, LeaderService, "DEFAULT_GROUP")
	if err != nil {
		return false, fmt.Errorf("query sync leader candidates: %w", err)
	}
	if !hasOwnCandidate(instances, scope, l.agentIP, l.agentPort) {
		ok, err := client.RegisterInstance(l.candidateParam(scope))
		if err != nil {
			return false, fmt.Errorf("register sync leader candidate: %w", err)
		}
		if !ok {
			return false, fmt.Errorf("register sync leader candidate rejected by nacos")
		}
		instances, err = l.mgr.InstancesLive(clusterID, namespace, LeaderService, "DEFAULT_GROUP")
		if err != nil {
			return false, fmt.Errorf("requery sync leader candidates: %w", err)
		}
	}
	leader, ok := chooseLeader(instances, scope)
	if !ok {
		return false, fmt.Errorf("sync leader candidate list has no usable instance")
	}
	return leader.Ip == l.agentIP && leader.Port == l.agentPort, nil
}

// ActiveScopes 返回目标集群中仍存活的同步方向。孤儿清扫据此识别其他节点
// 正在管理的实例，不能再仅凭当前进程的 runs/registered 判断。
func ActiveScopes(instances []model.Instance) map[string]bool {
	out := make(map[string]bool)
	for _, in := range instances {
		if !in.Enable || !in.Healthy {
			continue
		}
		if scope := in.Metadata[leaderDirectionMeta]; scope != "" {
			out[scope] = true
		}
	}
	return out
}

func hasOwnCandidate(instances []model.Instance, scope, ip string, port uint64) bool {
	for _, in := range instances {
		if in.Ip == ip && in.Port == port && in.ClusterName == leaderCluster(scope) &&
			in.Metadata[leaderDirectionMeta] == scope && in.Enable && in.Healthy {
			return true
		}
	}
	return false
}

func chooseLeader(instances []model.Instance, scope string) (model.Instance, bool) {
	candidates := make([]model.Instance, 0, len(instances))
	for _, in := range instances {
		if in.ClusterName == leaderCluster(scope) && in.Metadata[leaderDirectionMeta] == scope &&
			in.Enable && in.Healthy {
			candidates = append(candidates, in)
		}
	}
	if len(candidates) == 0 {
		return model.Instance{}, false
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Ip != candidates[j].Ip {
			return candidates[i].Ip < candidates[j].Ip
		}
		return candidates[i].Port < candidates[j].Port
	})
	return candidates[0], true
}

// Leave 主动撤销候选；失败只由调用方记录，临时实例最终仍会被 Nacos 摘除。
func (l *DirectionLeader) Leave(client naming_client.INamingClient, scope string) error {
	if client == nil {
		return nil
	}
	p := l.candidateParam(scope)
	ok, err := client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip: p.Ip, Port: p.Port, ServiceName: p.ServiceName, GroupName: p.GroupName,
		Cluster: p.ClusterName, Ephemeral: true,
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("deregister sync leader candidate rejected by nacos")
	}
	return nil
}
