package nacos

import (
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
)

func workerInstance(ip string, port uint64, scope, id string, healthy bool) model.Instance {
	return model.Instance{
		Ip: ip, Port: port, ClusterName: workerCluster(scope),
		Ephemeral: true, Enable: true, Healthy: healthy,
		Metadata: map[string]string{workerDirectionMeta: scope, workerIDMeta: id},
	}
}

func TestWorkerScopesTracksAllHealthyWorkers(t *testing.T) {
	instances := []model.Instance{
		workerInstance("10.0.0.1", 9696, "dir-a", "10.0.0.1:9696", true),
		workerInstance("10.0.0.2", 9696, "dir-a", "10.0.0.2:9696", true),
		workerInstance("10.0.0.3", 9696, "dir-b", "10.0.0.3:9696", false),
		{Ip: "10.0.0.4", Port: 9696, Enable: true, Healthy: true,
			Metadata: map[string]string{workerDirectionMeta: "dir-c", workerIDMeta: "10.0.0.4:9696"}},
	}
	got := WorkerScopes(instances)
	if len(got) != 1 || !got["dir-a"] {
		t.Fatalf("expected only healthy ephemeral dir-a workers, got %v", got)
	}
}

func TestHasOtherWorkerProtectsPeerDuringShutdown(t *testing.T) {
	self := NewDirectionWorker(nil, "10.0.0.1", 9696)
	instances := []model.Instance{
		workerInstance("10.0.0.1", 9696, "dir-a", "10.0.0.1:9696", true),
		workerInstance("10.0.0.2", 9696, "dir-a", "10.0.0.2:9696", true),
	}
	if !HasOtherWorker(instances, "dir-a", self) {
		t.Fatal("peer Worker must protect a direction while this node shuts down")
	}
	if HasOtherWorker(instances[:1], "dir-a", self) {
		t.Fatal("a node must not count its own heartbeat as a peer")
	}
	if HasOtherWorker(instances, "dir-b", self) {
		t.Fatal("workers for another direction must not protect this direction")
	}
}
