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

// DirectionWorker maintains this process's liveness record in each destination Nacos.
// Multiple workers may be healthy for the same direction; all of them are allowed to reconcile.
type DirectionWorker struct {
	mgr       *ClientManager
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

func workerCluster(scope string) string {
	sum := sha256.Sum256([]byte(scope))
	return fmt.Sprintf("WORKER_%x", sum[:8])
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
	instances, err = w.mgr.InstancesLive(clusterID, namespace, WorkerService, "DEFAULT_GROUP")
	if err != nil {
		return fmt.Errorf("verify naming worker heartbeat: %w", err)
	}
	if !hasOwnWorker(instances, w.workerParam(scope)) {
		return fmt.Errorf("naming worker heartbeat is not visible after registration")
	}
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
