package nacos

import (
	"errors"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
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
		{Ip: "10.0.0.5", Port: 9696, ClusterName: "WORKER_WRONG", Ephemeral: true, Enable: true, Healthy: true,
			Metadata: map[string]string{workerDirectionMeta: "dir-d", workerIDMeta: "10.0.0.5:9696"}},
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

func TestPeerRemainsActiveAfterNodeDisconnectAndRecovery(t *testing.T) {
	self := NewDirectionWorker(nil, "10.0.0.1", 9696)
	peer := workerInstance("10.0.0.2", 9696, "dir-a", "10.0.0.2:9696", true)

	// Node 1 disappears: node 2's heartbeat alone must keep dir-a globally active.
	active := WorkerScopes([]model.Instance{peer})
	if !active["dir-a"] || !HasOtherWorker([]model.Instance{peer}, "dir-a", self) {
		t.Fatalf("remaining peer must keep direction active after node disconnect: %v", active)
	}

	// A recovered node registers a new heartbeat; the direction remains active with both workers.
	recovered := workerInstance("10.0.0.1", 9696, "dir-a", "10.0.0.1:9696", true)
	active = WorkerScopes([]model.Instance{peer, recovered})
	if !active["dir-a"] || !HasOtherWorker([]model.Instance{peer, recovered}, "dir-a", self) {
		t.Fatalf("recovered node must rejoin the active Worker set: %v", active)
	}
}

func TestWorkerScopesRejectsMismatchedWorkerID(t *testing.T) {
	bad := workerInstance("10.0.0.9", 9696, "dir-a", "forged-worker-id", true)
	if got := WorkerScopes([]model.Instance{bad}); len(got) != 0 {
		t.Fatalf("worker with mismatched identity metadata must not keep a direction alive: %v", got)
	}
	self := NewDirectionWorker(nil, "10.0.0.1", 9696)
	if HasOtherWorker([]model.Instance{bad}, "dir-a", self) {
		t.Fatal("worker with mismatched identity metadata must not count as a peer")
	}
}

// ---- Ensure 真实流程覆盖 ----
// 此前所有 namingsync 单测里 Engine.worker 为 nil，ensureWorker 直接返回 true，完全绕过了
// worker.Ensure 的「注册 + 注册后校验」路径；worker_test.go 也只覆盖了 WorkerScopes/HasOtherWorker
// 等纯函数。这里对 Ensure 本身做行为级覆盖，锁定「注册成功即登记完成、不再二次反查可见性」的语义。

type fakeWorkerQuery struct {
	instances []model.Instance
	err       error
	calls     int
}

func (f *fakeWorkerQuery) InstancesLive(clusterID, namespace, serviceName, groupName string) ([]model.Instance, error) {
	f.calls++
	return f.instances, f.err
}

type fakeNamingClient struct {
	naming_client.INamingClient
	registerCalls int
	registerOk    bool
	registerErr   error
	lastParam     vo.RegisterInstanceParam
}

func (c *fakeNamingClient) RegisterInstance(p vo.RegisterInstanceParam) (bool, error) {
	c.registerCalls++
	c.lastParam = p
	return c.registerOk, c.registerErr
}

func newTestWorker(q workerQuery) *DirectionWorker {
	const ip = "10.0.0.1"
	const port uint64 = 9696
	return &DirectionWorker{
		mgr: q, agentIP: ip, agentPort: port,
		workerID: ip + ":9696",
	}
}

// 注册成功即返回，不再二次反查可见性（核心回归：此前注册后立即用另一条 gRPC 连接反查，
// 多节点集群下因节点间同步延迟误判「不可见」→ startDirection 失败 → 关闭 worker 连接 →
// ephemeral 心跳被服务端回收 → 下一轮又查不到 → 死循环，组内/组外同步全部失效）。
func TestEnsureRegistersWithoutPostRegistrationVerify(t *testing.T) {
	q := &fakeWorkerQuery{instances: nil, err: nil} // 查询返回空：尚无自己的 worker
	cli := &fakeNamingClient{registerOk: true, registerErr: nil}
	w := newTestWorker(q)

	if err := w.Ensure(cli, "dst-cluster", "", "dir-a"); err != nil {
		t.Fatalf("Ensure must succeed right after a successful registration, got %v", err)
	}
	if cli.registerCalls != 1 {
		t.Fatalf("RegisterInstance must be called exactly once when no own worker exists, got %d", cli.registerCalls)
	}
	// 关键断言：注册成功后不再触发第二次 InstancesLive 反查。
	if q.calls != 1 {
		t.Fatalf("Ensure must NOT issue a second InstancesLive after registration (post-registration verify removed); got %d query calls", q.calls)
	}
	if cli.lastParam.ServiceName != WorkerService || cli.lastParam.GroupName != "DEFAULT_GROUP" || !cli.lastParam.Ephemeral {
		t.Fatalf("worker heartbeat registration param mismatch: %+v", cli.lastParam)
	}
	if cli.lastParam.Metadata[workerDirectionMeta] != "dir-a" {
		t.Fatalf("worker heartbeat must carry direction scope metadata, got %+v", cli.lastParam.Metadata)
	}
}

// 已有自己的 worker 时幂等返回，不再重复注册。
func TestEnsureIdempotentWhenWorkerAlreadyVisible(t *testing.T) {
	w := newTestWorker(&fakeWorkerQuery{
		instances: []model.Instance{
			workerInstance("10.0.0.1", 9696, "dir-a", "10.0.0.1:9696", true),
		},
	})
	cli := &fakeNamingClient{registerOk: true}

	if err := w.Ensure(cli, "dst-cluster", "", "dir-a"); err != nil {
		t.Fatalf("Ensure must be idempotent when own worker is already visible, got %v", err)
	}
	if cli.registerCalls != 0 {
		t.Fatalf("RegisterInstance must not be called when own worker already exists, got %d", cli.registerCalls)
	}
}

// 查询失败时 fail-closed，绝不继续注册或假定登记成功。
func TestEnsureFailsClosedOnQueryError(t *testing.T) {
	qErr := errors.New("rpc error: code = unavailable")
	w := newTestWorker(&fakeWorkerQuery{err: qErr})
	cli := &fakeNamingClient{registerOk: true}

	err := w.Ensure(cli, "dst-cluster", "", "dir-a")
	if err == nil {
		t.Fatal("Ensure must return error when heartbeat query fails (fail-closed)")
	}
	if cli.registerCalls != 0 {
		t.Fatalf("must not register when query failed, got %d register calls", cli.registerCalls)
	}
}

// 注册返回错误时 fail-closed。
func TestEnsureFailsClosedOnRegisterError(t *testing.T) {
	w := newTestWorker(&fakeWorkerQuery{instances: nil})
	cli := &fakeNamingClient{registerOk: false, registerErr: errors.New("nacos rejected")}

	if err := w.Ensure(cli, "dst-cluster", "", "dir-a"); err == nil {
		t.Fatal("Ensure must return error when RegisterInstance returns an error")
	}
}

// 注册被服务端拒绝（ok=false）时 fail-closed。
func TestEnsureFailsClosedOnRegisterRejected(t *testing.T) {
	w := newTestWorker(&fakeWorkerQuery{instances: nil})
	cli := &fakeNamingClient{registerOk: false, registerErr: nil}

	if err := w.Ensure(cli, "dst-cluster", "", "dir-a"); err == nil {
		t.Fatal("Ensure must return error when RegisterInstance returns ok=false")
	}
}

// TestWorkerClusterUsesNacosLegalCharset 锁定 workerCluster 生成的 cluster 名只含
// Nacos 服务端允许的字符 [0-9a-zA-Z-.]，不含下划线等非法字符。
// 根因：Nacos 校验 clusterName 只允许 0-9a-zA-Z-.，下划线 '_' 会被拒绝
// （"Instance 'clusterName' should be characters with only 0-9a-zA-Z-."），
// 导致 Worker 心跳 RegisterInstance 返回 err → ensureWorker fail-closed →
// 所有同步方向全部失效。此前 workerCluster 用 "WORKER_%x" 含下划线，正是线上故障的直接根因。
func TestWorkerClusterUsesNacosLegalCharset(t *testing.T) {
	seen := make(map[string]bool)
	for _, scope := range []string{"g/a->g/b/", "dubbo/x->y/", "", "long/scope/with/many/segments"} {
		c := workerCluster(scope)
		for _, r := range c {
			legal := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') ||
				(r >= 'A' && r <= 'Z') || r == '-' || r == '.'
			if !legal {
				t.Fatalf("workerCluster(%q)=%q contains illegal char %q; Nacos allows only [0-9a-zA-Z-.]",
					scope, c, r)
			}
		}
		if seen[c] {
			t.Fatalf("workerCluster must produce distinct names per distinct scope; collision on %q", c)
		}
		seen[c] = true
	}
}
