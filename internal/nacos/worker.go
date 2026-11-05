package nacos

import (
	"crypto/sha256"
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

const (
	// WorkerService stores one ephemeral liveness record per node and maintained direction.
	// It is deliberately separate from the data-plane registration client and is not an election.
	WorkerService       = "NACOS-SYNC-WORKER"
	workerDirectionMeta = "syncDirection"
	workerIDMeta        = "syncWorkerID"
)

// workerQuery 限定 DirectionWorker 对 ClientManager 的依赖面：仅需实时查询实例列表。
// 收窄为未导出接口便于在无需连接真实 Nacos 的情况下单测 Ensure 的登记/幂等语义；
// *ClientManager 已实现 InstancesLive，生产代码 NewDirectionWorker 传入 *ClientManager 即满足。
type workerQuery interface {
	InstancesLive(clusterID, namespace, serviceName, groupName string) ([]model.Instance, error)
}

// DirectionWorker maintains this process's liveness record in each destination Nacos.
// Multiple workers may be healthy for the same direction; all of them are allowed to reconcile.
type DirectionWorker struct {
	mgr       workerQuery
	agentIP   string
	agentPort uint64
	workerID  string
}

func NewDirectionWorker(mgr *ClientManager, agentIP string, agentPort uint64) *DirectionWorker {
	return &DirectionWorker{
		mgr: mgr, agentIP: agentIP, agentPort: agentPort,
		workerID: fmt.Sprintf("%s:%d", agentIP, agentPort),
	}
}

// workerCluster 为每条同步方向的 Worker 心跳生成一个稳定的 cluster 名。
// 使用 sha256(scope) 的前 8 字节 hex 作为后缀，保证不同方向 cluster 不同（同一方向跨节点一致，
// 便于 HasOtherWorker 识别对端）。分隔符必须用 '-' 而非 '_'：Nacos 服务端校验 clusterName
// 只允许 [0-9a-zA-Z-.]，下划线会被拒绝（Instance 'clusterName' should be characters with
// only 0-9a-zA-Z-.），导致 Worker 心跳注册失败、ensureWorker fail-closed、所有同步方向全停。
func workerCluster(scope string) string {
	sum := sha256.Sum256([]byte(scope))
	return fmt.Sprintf("WORKER-%x", sum[:8])
}

func (w *DirectionWorker) workerParam(scope string) vo.RegisterInstanceParam {
	return vo.RegisterInstanceParam{
		Ip: w.agentIP, Port: w.agentPort,
		ServiceName: WorkerService, GroupName: "DEFAULT_GROUP",
		ClusterName: workerCluster(scope), Weight: 1,
		Enable: true, Healthy: true, Ephemeral: true,
		Metadata: map[string]string{
			workerDirectionMeta: scope,
			workerIDMeta:        w.workerID,
		},
	}
}

// Ensure guarantees that this node's Worker heartbeat is registered.
// Query or registration failures are returned so callers can fail closed before data writes.
func (w *DirectionWorker) Ensure(client naming_client.INamingClient, clusterID, namespace, scope string) error {
	instances, err := w.mgr.InstancesLive(clusterID, namespace, WorkerService, "DEFAULT_GROUP")
	if err != nil {
		return fmt.Errorf("query naming worker heartbeats: %w", err)
	}
	if hasOwnWorker(instances, w.workerParam(scope)) {
		return nil
	}
	ok, err := client.RegisterInstance(w.workerParam(scope))
	if err != nil {
		return fmt.Errorf("register naming worker heartbeat: %w", err)
	}
	if !ok {
		return fmt.Errorf("register naming worker heartbeat rejected by nacos")
	}
	// 注册成功即视为登记完成，不再立即反查可见性。
	//
	// Worker 心跳是存活登记而非选主（见 DirectionWorker 注释与 README「Worker 心跳」段）：
	// RegisterInstance 返回 ok=true 表示服务端已接受写入，登记即告完成。此前在此处用另一条
	// gRPC 连接（InstancesLive 的 liveProxy，与注册所用的 workerCli 是不同连接）立即反查
	// hasOwnWorker，在多节点 Nacos 集群下会因为「注册写入节点 A、查询读到节点 B、节点间同步
	// 存在亚秒~秒级延迟」误判为「不可见」，返回 "not visible after registration"。
	// 该错误会触发 startDirection 失败路径 CloseDedicatedNamingClient(workerCli)，断开 worker
	// 连接使服务端回收刚注册的 ephemeral 心跳实例，下一轮 reconcile 又查不到、再注册、再校验
	// 失败……陷入死循环，导致组内/组外/dubbo 全部同步方向永远建不起来（即使侥幸建起，每轮
	// ensureWorker 的 Ensure 仍会因偶发查询打到未同步节点而报错，使 fastPoll/syncService 整轮跳过）。
	//
	// 移除二次校验后：首次注册成功即通过；后续每轮 ensureWorker 的 Ensure 经 hasOwnWorker
	// 命中已存在的 worker 直接返回 nil；偶发查询未同步节点时触发一次幂等重注册（SDK 对同一
	// (group,service) 的 publisher 为覆盖语义），不再因「立即反查不可见」报错跳过整轮对账。
	return nil
}

func hasOwnWorker(instances []model.Instance, want vo.RegisterInstanceParam) bool {
	for _, in := range instances {
		if in.Ip == want.Ip && in.Port == want.Port &&
			in.ClusterName == want.ClusterName &&
			in.Metadata[workerDirectionMeta] == want.Metadata[workerDirectionMeta] &&
			in.Metadata[workerIDMeta] == want.Metadata[workerIDMeta] &&
			in.Ephemeral && in.Enable && in.Healthy {
			return true
		}
	}
	return false
}

// WorkerScopes returns every direction currently maintained by at least one healthy Worker.
func validWorkerInstance(in model.Instance) bool {
	scope := in.Metadata[workerDirectionMeta]
	return scope != "" && in.Ephemeral && in.Enable && in.Healthy &&
		in.Metadata[workerIDMeta] == fmt.Sprintf("%s:%d", in.Ip, in.Port) &&
		in.ClusterName == workerCluster(scope)
}

func WorkerScopes(instances []model.Instance) map[string]bool {
	out := make(map[string]bool)
	for _, in := range instances {
		if validWorkerInstance(in) {
			out[in.Metadata[workerDirectionMeta]] = true
		}
	}
	return out
}

// HasOtherWorker reports whether a different healthy ephemeral Worker still maintains scope.
// It is used during graceful shutdown to avoid deregistering instances that a peer is reconciling.
func HasOtherWorker(instances []model.Instance, scope string, self *DirectionWorker) bool {
	if self == nil {
		return false
	}
	for _, in := range instances {
		if !validWorkerInstance(in) || in.Metadata[workerDirectionMeta] != scope {
			continue
		}
		if in.Ip == self.agentIP && in.Port == self.agentPort && in.Metadata[workerIDMeta] == self.workerID {
			continue
		}
		return true
	}
	return false
}

// Leave withdraws this process's Worker heartbeat. The caller must do this only after
// it has completed data-plane cleanup for the direction.
func (w *DirectionWorker) Leave(client naming_client.INamingClient, scope string) error {
	if client == nil {
		return nil
	}
	p := w.workerParam(scope)
	ok, err := client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip: p.Ip, Port: p.Port, ServiceName: p.ServiceName,
		GroupName: p.GroupName, Cluster: p.ClusterName, Ephemeral: true,
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("deregister naming worker heartbeat rejected by nacos")
	}
	return nil
}
