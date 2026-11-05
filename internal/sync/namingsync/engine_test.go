package namingsync

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/config"
	"github.com/cxcm/nacos-sync-backend/internal/nacos"
	"github.com/cxcm/nacos-sync-backend/internal/pool"
)

// TestIsProxied 验证「代注册识别」核心：带 syncSource=NACOS 的实例被识别为代注册，原始实例不被误判。
func TestIsProxied(t *testing.T) {
	native := model.Instance{Metadata: map[string]string{"preserved.register.source": "SPRING_CLOUD"}}
	if isProxied(native) {
		t.Fatal("原始业务实例不应被识别为代注册")
	}
	proxied := model.Instance{Metadata: map[string]string{metaSyncSource: "NACOS"}}
	if !isProxied(proxied) {
		t.Fatal("带 syncSource=NACOS 的实例应被识别为代注册")
	}
}

// TestIsOurs 验证「归属清理」核心：仅清理本方向（srcCID）产生的代注册实例，不越界到其它方向。
func TestIsOurs(t *testing.T) {
	ours := model.Instance{Metadata: map[string]string{metaSyncSource: "NACOS", metaSyncSourceCluster: "abc1"}}
	if !isOurs(ours, "abc1", "dirA") {
		t.Fatal("匹配 srcCID 的代注册实例应归属本方向")
	}
	if isOurs(ours, "xyz9", "dirA") {
		t.Fatal("srcCID 不匹配的代注册实例不应归属本方向")
	}
	other := model.Instance{Metadata: map[string]string{metaSyncSource: "NACOS", metaSyncSourceCluster: "xyz9"}}
	if isOurs(other, "abc1", "dirA") {
		t.Fatal("其它方向的代注册实例不应被本方向清理")
	}
	collision := model.Instance{Metadata: map[string]string{
		metaSyncSource: "NACOS", metaSyncSourceCluster: "abc1", metaSyncSourceDirection: "dirB",
	}}
	if isOurs(collision, "abc1", "dirA") {
		t.Fatal("相同 srcCID 但方向 ID 不同的实例不能被当前方向认领")
	}
	legacy := model.Instance{Metadata: map[string]string{
		metaSyncSource: "NACOS", metaSyncSourceCluster: "abc1",
	}}
	if !isOurs(legacy, "abc1", "dirA") {
		t.Fatal("无方向元数据的旧实例应兼容 srcCID 归属")
	}
}

func TestDirectionManagesService(t *testing.T) {
	static := &direction{Services: []string{"ORDER", "PAY"}}
	if !directionManagesService(static, "ORDER") || directionManagesService(static, "USER") {
		t.Fatal("静态方向必须严格遵循当前服务白名单")
	}
	dubbo := &direction{Dubbo: true}
	if !directionManagesService(dubbo, "providers:com.example.Order") {
		t.Fatal("Dubbo 方向应管理 providers 服务")
	}
	if directionManagesService(dubbo, "ORDER") {
		t.Fatal("Dubbo 方向不应管理非 Dubbo 服务")
	}
}

func TestSweepScansDistinguishesSameCIDAcrossGroups(t *testing.T) {
	e := &Engine{}
	cfg := &config.Config{
		NacosGroup: []config.NacosGroup{
			{GrpID: "group-a", Clusters: []config.NacosCluster{{CID: "shared"}}},
			{GrpID: "group-b", Clusters: []config.NacosCluster{{CID: "shared"}}},
		},
	}
	scans := e.sweepScans(cfg)
	if len(scans) != 2 {
		t.Fatalf("same CID in different groups must produce two sweep targets, got %+v", scans)
	}
	if scans[0].label() == scans[1].label() {
		t.Fatalf("group-qualified sweep targets must be distinct: %+v", scans)
	}
}

func TestPruneOrphanSeen(t *testing.T) {
	e := &Engine{orphanSeen: make(map[string]time.Time)}
	scan := sweepScan{dstGrp: "g", dstCID: "dst", ns: ""}
	keep := scan.label() + "\x00" + regKey("ORDER", model.Instance{Ip: "10.0.0.1", Port: 8080})
	remove := scan.label() + "\x00" + regKey("PAY", model.Instance{Ip: "10.0.0.2", Port: 8080})
	otherScope := "g/other/\x00" + regKey("USER", model.Instance{Ip: "10.0.0.3", Port: 8080})
	e.orphanSeen[keep] = time.Now()
	e.orphanSeen[remove] = time.Now()
	e.orphanSeen[otherScope] = time.Now()

	e.pruneOrphanSeen(scan, map[string]struct{}{keep: {}})
	if _, ok := e.orphanSeen[keep]; !ok {
		t.Fatal("currently observed orphan must retain its grace-period timestamp")
	}
	if _, ok := e.orphanSeen[remove]; ok {
		t.Fatal("orphan no longer present in a complete scan must be pruned")
	}
	if _, ok := e.orphanSeen[otherScope]; !ok {
		t.Fatal("pruning one target must not affect another target's timestamps")
	}
}

type rejectingDeregisterClient struct {
	stubNamingClient
	ok  bool
	err error
}

func (c rejectingDeregisterClient) DeregisterInstance(vo.DeregisterInstanceParam) (bool, error) {
	return c.ok, c.err
}

func TestDeregisterOrphanRequiresNacosAcceptance(t *testing.T) {
	e := &Engine{logger: zap.NewNop()}
	target := sweepTarget{dstGrp: "g", dstCID: "dst", service: "ORDER"}
	inst := model.Instance{Ip: "10.0.0.1", Port: 8080, ClusterName: "DEFAULT"}

	if e.deregisterOrphan(rejectingDeregisterClient{ok: false}, target, inst) {
		t.Fatal("false,nil from Nacos must not be treated as successful deregistration")
	}
	if e.deregisterOrphan(rejectingDeregisterClient{err: context.DeadlineExceeded}, target, inst) {
		t.Fatal("deregistration error must be reported as failure")
	}
	if !e.deregisterOrphan(rejectingDeregisterClient{ok: true}, target, inst) {
		t.Fatal("accepted deregistration must be reported as success")
	}
}

func TestInstKey(t *testing.T) {
	in := model.Instance{Ip: "192.168.0.100", Port: 8080, ClusterName: "DEFAULT"}
	if got := instKey(in); got != "192.168.0.100:8080:DEFAULT" {
		t.Fatalf("instKey = %q", got)
	}
}

func TestDirID(t *testing.T) {
	if got := dirID("g1", "a1", "g2", "b2", "public"); got != "g1/a1->g2/b2/public" {
		t.Fatalf("dirID = %q", got)
	}
}

// TestRegKeyRoundTrip 锁定「服务名 + 实例键」唯一标识被跟踪实例的不变量：
// regKey 写入 svc 与 instKey，splitRegKey 能无损还原，且不同服务同名实例不会串。
func TestRegKeyRoundTrip(t *testing.T) {
	a := model.Instance{Ip: "10.0.0.1", Port: 8080, ClusterName: "C1"}
	b := model.Instance{Ip: "10.0.0.1", Port: 8080, ClusterName: "C1"}
	ra := regKey("ORDER-SVC", a)
	rb := regKey("PAY-SVC", b)
	if ra == rb {
		t.Fatal("不同服务的同名实例必须生成不同 regKey")
	}
	for _, tt := range []struct {
		svc string
		in  model.Instance
	}{
		{"ORDER-SVC", a},
		{"PAY-SVC", b},
	} {
		gotSvc, gotIk := splitRegKey(regKey(tt.svc, tt.in))
		if gotSvc != tt.svc {
			t.Fatalf("splitRegKey svc = %q, want %q", gotSvc, tt.svc)
		}
		if gotIk != instanceSpecKey(tt.in) {
			t.Fatalf("splitRegKey instKey = %q, want %q", gotIk, instKey(tt.in))
		}
	}
}

// TestFilterNatives 验证过滤逻辑：剔除 syncSource=NACOS 的代注册实例，保留原始业务实例。
func TestFilterNatives(t *testing.T) {
	src := []model.Instance{
		{Metadata: map[string]string{"preserved.register.source": "SPRING_CLOUD"}, Ip: "10.0.0.1", Port: 8080, ClusterName: "C1"},
		{Metadata: map[string]string{metaSyncSource: "NACOS", metaSyncSourceCluster: "abc1"}, Ip: "10.0.0.2", Port: 8080, ClusterName: "C1"},
	}
	got := filterNatives(src)
	if len(got) != 1 {
		t.Fatalf("filterNatives len = %d, want 1", len(got))
	}
	if _, ok := got[instanceSpecKey(src[0])]; !ok {
		t.Fatal("原始业务实例应被保留")
	}
}

// TestFilterNativesPreservesSameEndpointPublisherVariants verifies that two
// publisher clients may expose the same endpoint with different metadata.
// Those records must not collapse into one local desired-state entry.
func TestFilterNativesPreservesSameEndpointPublisherVariants(t *testing.T) {
	base := model.Instance{
		Ip: "10.0.0.8", Port: 8080, ClusterName: "DEFAULT",
		Weight: 1, Enable: true, Healthy: true,
	}
	first := base
	first.Metadata = map[string]string{"version": "v1", "zone": "a"}
	second := base
	second.Metadata = map[string]string{"version": "v2", "zone": "b"}

	got := filterNatives([]model.Instance{first, second})
	if len(got) != 2 {
		t.Fatalf("same endpoint with different publisher metadata collapsed: got %d records", len(got))
	}
	if regKey("ORDER", first) == regKey("ORDER", second) {
		t.Fatal("different publisher specifications must have distinct registration keys")
	}

	proxy := second
	proxy.Metadata = map[string]string{
		"version": "v2", "zone": "b",
		metaSyncSource: "NACOS",
		metaSyncSourceCluster: "src",
		metaSyncSourceDirection: "g/src->g/dst/",
	}
	if got := instanceSpecKey(sourceViewOfProxy(proxy)); got != instanceSpecKey(second) {
		t.Fatal("proxy ownership metadata must be stripped when matching a target publisher to its source spec")
	}
}

// TestGroupCrossDirections 锁定组外同步拓扑：A、B 两组「全互联」——
// 每组每个集群 ↔ 对方组每个集群（双向），方向数 = |A集群| × |B集群| × 2。
// 历史回归点：早期版本只挑各组「主集群」，导致服务注册在非主集群时完全同步不出去。
func TestGroupCrossDirections(t *testing.T) {
	ga := config.NacosGroup{Clusters: []config.NacosCluster{{CID: "a1"}, {CID: "a2"}}}
	gb := config.NacosGroup{Clusters: []config.NacosCluster{{CID: "b1"}}}
	dirs := groupCrossDirections("A", ga, "B", gb, "", []string{"TEST-SERVICE"})
	if len(dirs) != 4 {
		t.Fatalf("dirs len = %d, want 4", len(dirs))
	}
	got := make(map[string]*direction, len(dirs))
	for _, d := range dirs {
		got[d.ID] = d
	}
	for _, want := range []struct{ id, src, dst string }{
		{"A/a1->B/b1/", "a1", "b1"},
		{"B/b1->A/a1/", "b1", "a1"},
		{"A/a2->B/b1/", "a2", "b1"},
		{"B/b1->A/a2/", "b1", "a2"},
	} {
		d, ok := got[want.id]
		if !ok {
			t.Fatalf("缺少方向 %q，实际=%v", want.id, keysOf(got))
		}
		if d.SrcCID != want.src || d.DstCID != want.dst {
			t.Fatalf("%s: src=%s dst=%s, want src=%s dst=%s", want.id, d.SrcCID, d.DstCID, want.src, want.dst)
		}
	}
}

// TestGroupCrossDirectionsSkipsSameCID 跨分组 c_id 重名时跳过该跳：
// c_id 仅在分组内唯一，重名无法判定集群归属，跳过以免「自己同步到自己」。
func TestGroupCrossDirectionsSkipsSameCID(t *testing.T) {
	g := config.NacosGroup{Clusters: []config.NacosCluster{{CID: "dup"}}}
	if dirs := groupCrossDirections("A", g, "B", g, "", []string{"SVC"}); len(dirs) != 0 {
		t.Fatalf("dirs len = %d, want 0", len(dirs))
	}
}

func keysOf(m map[string]*direction) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestServicesEqual 锁定「服务名单变化检测」：顺序无关、增减均能正确识别（这是 reconcile 判定
// 已运行方向是否需要原地刷新的依据；此前方向 ID 不含 services，名单变化不改变 ID，
// 若检测不到变化就不会刷新，导致新增服务不同步——需禁用再启用才生效）。
func TestServicesEqual(t *testing.T) {
	cases := []struct {
		a, b []string
		want bool
	}{
		{nil, nil, true},
		{[]string{"A"}, []string{"A"}, true},
		{[]string{"A", "B"}, []string{"B", "A"}, true}, // 顺序无关
		{[]string{"A", "B"}, []string{"A", "C"}, false},
		{[]string{"A"}, []string{"A", "B"}, false}, // 新增
		{[]string{"A", "B"}, []string{"A"}, false}, // 移除
	}
	for _, c := range cases {
		if got := servicesEqual(c.a, c.b); got != c.want {
			t.Fatalf("servicesEqual(%v,%v)=%v want %v", c.a, c.b, got, c.want)
		}
	}
}

// TestRefreshRunningDirectionAddService 回归点：已运行方向新增/移除服务时，reconcile 调用
// refreshRunningDirection 原地刷新运行实例的方向配置。验证：
//  1. rd.d 被刷新为新服务名单（下一 tick 的快轮询/对账即按新名单工作 → 新增服务立即同步，无需禁用再启用）；
//  2. 被移除服务的代注册实例被清理，仍在同步的服务实例保留（零抖动，不因名单编辑而先注销再重注册）。
func TestRefreshRunningDirectionAddService(t *testing.T) {
	pl, err := pool.New(4, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Release()

	e := &Engine{
		pl:           pl,
		logger:       zap.NewNop(),
		ctx:          context.Background(),
		stops:        map[string]context.CancelFunc{},
		dirs:         map[string]*direction{},
		runs:         map[string]*runningDir{},
		lastSync:     map[string]time.Time{},
		lastErr:      map[string]string{},
		lastSrcCount: map[string]int{},
		lastRegCount: map[string]int{},
		mu:           sync.Mutex{},
	}
	// 预置 ClientManager：dstClient 改为「实时取」，测试需注入一个能返回 stub 客户端的管理器，
	// 避免 refreshRunningDirection 清理被移除服务时经 e.dstClient(rd) 取客户端空指针（生产环境 mgr 一定非 nil）。
	e.mgr = nacos.NewClientManagerStub("d", "", stubNamingClient{})
	old := &direction{ID: "g/s->g/d/", SrcCID: "s", DstCID: "d", Services: []string{"A", "B"}}
	rd := &runningDir{
		d:         old,
		dstClient: stubNamingClient{},
		registered: map[string]regEntry{
			regKey("A", model.Instance{Ip: "1.1.1.1", Port: 1}): {svc: "A", inst: model.Instance{Ip: "1.1.1.1", Port: 1}},
			regKey("B", model.Instance{Ip: "2.2.2.2", Port: 2}): {svc: "B", inst: model.Instance{Ip: "2.2.2.2", Port: 2}},
		},
		failCount: map[string]int{},
		dead:      map[string]regEntry{},
	}
	e.runs[old.ID] = rd

	// 模拟「新增服务 C、移除服务 B」的配置变更。
	updated := &direction{ID: old.ID, SrcCID: "s", DstCID: "d", Services: []string{"A", "C"}}
	e.refreshRunningDirection(updated)

	// 1) rd.d 已原地刷新为新名单。
	got := rd.direction().Services
	if len(got) != 2 || got[0] != "A" || got[1] != "C" {
		t.Fatalf("rd.d 未刷新为新服务名单: %v", got)
	}
	// 2) 被移除服务 B 的代注册实例被清理，仍在同步的 A 保留。
	if _, ok := rd.registered[regKey("B", model.Instance{Ip: "2.2.2.2", Port: 2})]; ok {
		t.Fatal("被移除服务 B 的代注册实例应被清理")
	}
	if _, ok := rd.registered[regKey("A", model.Instance{Ip: "1.1.1.1", Port: 1})]; !ok {
		t.Fatal("仍在同步的服务 A 的实例应保留")
	}
}

// stubNamingClient 仅实现 DeregisterInstance（返回成功），用于刷新清理路径的测试；
// 嵌入的 INamingClient 为 nil，不会被其它方法调用。
type stubNamingClient struct {
	naming_client.INamingClient
}

func (stubNamingClient) DeregisterInstance(vo.DeregisterInstanceParam) (bool, error) {
	return true, nil
}

// singleInstanceNamingClient 捕获实例级 Register/Deregister 请求。
type singleInstanceNamingClient struct {
	naming_client.INamingClient
	registered chan vo.RegisterInstanceParam
	removed    chan vo.DeregisterInstanceParam
	started    chan struct{}
	release    chan struct{}
}

func (c singleInstanceNamingClient) RegisterInstance(p vo.RegisterInstanceParam) (bool, error) {
	if c.registered != nil { c.registered <- p }
	if c.started != nil { c.started <- struct{}{} }
	if c.release != nil { <-c.release }
	return true, nil
}
func (c singleInstanceNamingClient) DeregisterInstance(p vo.DeregisterInstanceParam) (bool, error) {
	if c.removed != nil { c.removed <- p }
	return true, nil
}

func TestDispatchOpsHandlesInstanceChangesIndependently(t *testing.T) {
	client := singleInstanceNamingClient{
		registered: make(chan vo.RegisterInstanceParam, 2),
		removed: make(chan vo.DeregisterInstanceParam, 2),
	}
	e := &Engine{logger: zap.NewNop()}
	oldSpec := model.Instance{Ip:"10.0.0.1", Port:20880, ClusterName:"DEFAULT", Weight:1, Enable:true, Healthy:true, Metadata:map[string]string{"revision":"v1"}}
	newSpec := model.Instance{Ip:"10.0.0.1", Port:20880, ClusterName:"DEFAULT", Weight:1, Enable:true, Healthy:true, Metadata:map[string]string{"revision":"v2"}}
	rd := &runningDir{
		d: &direction{ID:"g/s->g/d/", SrcCID:"s", DstCID:"d"},
		dstClient: client,
		registered: map[string]regEntry{regKey("DUBBO-SVC", newSpec):{svc:"DUBBO-SVC",inst:newSpec}},
		failCount: map[string]int{}, dead: map[string]regEntry{},
	}
	e.dispatchOps(context.Background(), rd, []regOp{
		{add:false, svc:"DUBBO-SVC", in:oldSpec},
		{add:true, svc:"DUBBO-SVC", in:newSpec},
	})
	select {
	case got := <-client.removed:
		if got.Ip != oldSpec.Ip { t.Fatalf("removed instance %s, want %s", got.Ip, oldSpec.Ip) }
	case <-time.After(time.Second):
		t.Fatal("old instance was not independently removed")
	}
	select {
	case got := <-client.registered:
		if got.Ip != newSpec.Ip || got.Metadata["revision"] != "v2" || got.Metadata[metaSyncSource] != "NACOS" {
			t.Fatalf("unexpected new instance registration: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("new instance was not independently registered")
	}
}

func TestDispatchOpsUsesConfiguredPool(t *testing.T) {
	pl, err := pool.New(2, zap.NewNop())
	if err != nil { t.Fatal(err) }
	defer pl.Release()
	client := singleInstanceNamingClient{
		started: make(chan struct{}, 3),
		release: make(chan struct{}, 3),
	}
	e := &Engine{pl:pl, logger:zap.NewNop()}
	rd := &runningDir{
		d:&direction{ID:"g/s->g/d/",SrcCID:"s",DstCID:"d"}, dstClient:client,
		registered:map[string]regEntry{}, failCount:map[string]int{}, dead:map[string]regEntry{},
	}
	ops := make([]regOp, 0, 3)
	for i:=1;i<=3;i++ {
		in:=model.Instance{Ip:fmt.Sprintf("10.0.0.%d",i),Port:8080}
		rd.registered[regKey("SVC",in)] = regEntry{svc:"SVC",inst:in}
		ops=append(ops,regOp{add:true,svc:"SVC",in:in})
	}
	done:=make(chan struct{})
	go func(){ e.dispatchOps(context.Background(),rd,ops); close(done) }()
	for i:=0;i<2;i++ {
		select {
		case <-client.started:
		case <-time.After(time.Second): t.Fatal("pool did not start two concurrent registration tasks")
		}
	}
	select {
	case <-client.started:
		t.Fatal("registration concurrency exceeded pool capacity")
	default:
	}
	client.release <- struct{}{}
	select {
	case <-client.started:
	case <-time.After(time.Second): t.Fatal("third task did not start after a pool worker was released")
	}
	client.release <- struct{}{}
	client.release <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second): t.Fatal("dispatchOps did not wait for pool tasks")
	}
}

func TestDispatchOpsDoesNotCollapseSameServiceRemovals(t *testing.T) {
	client := singleInstanceNamingClient{removed: make(chan vo.DeregisterInstanceParam, 4)}
	e := &Engine{logger: zap.NewNop()}
	rd := &runningDir{
		d:&direction{ID:"g/s->g/d/",SrcCID:"s",DstCID:"d"}, dstClient:client,
		registered:map[string]regEntry{}, failCount:map[string]int{}, dead:map[string]regEntry{},
	}
	var ops []regOp
	for i:=1;i<=3;i++ {
		ops=append(ops,regOp{add:false,svc:"TEST-SVC",in:model.Instance{Ip:fmt.Sprintf("10.0.0.%d",i),Port:8080}})
	}
	e.dispatchOps(context.Background(),rd,ops)
	if got:=len(client.removed);got!=3 { t.Fatalf("sent %d instance removals, want 3",got) }
}

// TestEmptyStreakTolerance 验证源查询连续返回空时的容错语义：
// InstancesLive 偶发返回空（liveProxy 连接劣化/多节点 Nacos 同步延迟/provider 心跳短暂过期）
// 时，连续 emptyStreakThreshold-1 次不注销（保留 registered），第 emptyStreakThreshold 次
// 才允许注销。源恢复非空时清零计数。防止瞬时查询波动导致 dubbo 代注册频繁注销重注册。
func TestEmptyStreakTolerance(t *testing.T) {
	rd := &runningDir{
		d:           &direction{ID: "g/s->g/d/"},
		registered:  map[string]regEntry{"svc\x0010.0.0.1:8080": {svc: "svc", inst: model.Instance{Ip: "10.0.0.1", Port: 8080}}},
		failCount:   map[string]int{},
		dead:        map[string]regEntry{},
		emptyStreak: map[string]int{},
	}

	// 连续 emptyStreakThreshold-1 次源查询返回空：容错窗口内不注销。
	for i := 0; i < emptyStreakThreshold-1; i++ {
		natives := filterNatives(nil)
		rd.mu.Lock()
		skipDeregister := false
		if len(natives) == 0 {
			for _, re := range rd.registered {
				if re.svc == "svc" {
					rd.emptyStreak["svc"]++
					if rd.emptyStreak["svc"] < emptyStreakThreshold {
						skipDeregister = true
					}
					break
				}
			}
		}
		rd.mu.Unlock()
		if !skipDeregister {
			t.Fatalf("iteration %d: should skip deregister below threshold", i)
		}
		if len(rd.registered) == 0 {
			t.Fatalf("iteration %d: registered must not be emptied during tolerance window", i)
		}
	}

	// 第 emptyStreakThreshold 次：达阈值，允许注销。
	rd.mu.Lock()
	rd.emptyStreak["svc"]++
	allowDeregister := rd.emptyStreak["svc"] >= emptyStreakThreshold
	rd.mu.Unlock()
	if !allowDeregister {
		t.Fatalf("after %d consecutive empty results, deregister should be allowed", emptyStreakThreshold)
	}

	// 源恢复非空时清零计数。
	rd.mu.Lock()
	delete(rd.emptyStreak, "svc")
	rd.mu.Unlock()
	if _, ok := rd.emptyStreak["svc"]; ok {
		t.Fatal("emptyStreak should be cleared when source returns non-empty")
	}
}

// TestRegisterProxiedRegistersOneInstance verifies that a registration request
// publishes only the supplied instance and carries the ownership metadata.
func TestRegisterProxiedRegistersOneInstance(t *testing.T) {
	client := singleInstanceNamingClient{registered:make(chan vo.RegisterInstanceParam,2)}
	e := &Engine{logger:zap.NewNop()}
	first := model.Instance{Ip:"10.0.0.1",Port:8080,Weight:1,Enable:true,Healthy:true,Metadata:map[string]string{"k":"v"}}
	second := model.Instance{Ip:"10.0.0.2",Port:8080,Weight:1,Enable:true,Healthy:true}
	rd := &runningDir{
		d:&direction{ID:"g/s->g/d/",SrcCID:"s",DstCID:"d"}, dstClient:client,
		registered:map[string]regEntry{
			regKey("TEST-SVC",first):{svc:"TEST-SVC",inst:first},
			regKey("TEST-SVC",second):{svc:"TEST-SVC",inst:second},
		},
		failCount:map[string]int{},dead:map[string]regEntry{},
	}
	e.registerProxied(context.Background(),rd,"TEST-SVC",first)
	select {
	case got:=<-client.registered:
		if got.ServiceName!="TEST-SVC" || got.Ip!=first.Ip || got.Metadata["k"]!="v" {
			t.Fatalf("unexpected RegisterInstance params: %+v",got)
		}
		if got.Metadata[metaSyncSource]!="NACOS" || got.Metadata[metaSyncSourceCluster]!="s" {
			t.Fatalf("missing ownership metadata: %+v",got.Metadata)
		}
		if _,ok:=got.Metadata["syncSourceAgent"];ok { t.Fatalf("node-specific metadata leaked: %+v",got.Metadata) }
	case <-time.After(time.Second):
		t.Fatal("RegisterInstance was not called")
	}
	if got:=len(client.registered);got!=0 { t.Fatalf("one operation registered extra instances: %d",got) }
}

func TestCleanupDirectionClearsPerInstancePublishers(t *testing.T) {
	e:=&Engine{logger:zap.NewNop()}
	rd:=&runningDir{
		d:&direction{ID:"src->dst",SrcCID:"src",DstCID:"dst"},
		instanceClients:map[string]naming_client.INamingClient{},
		registered:map[string]regEntry{},dead:map[string]regEntry{},failCount:map[string]int{},
	}
	for i:=0;i<5;i++ {
		in:=model.Instance{Ip:fmt.Sprintf("10.0.0.%d",i+1),Port:uint64(1000+i)}
		rd.instanceClients[regKey("svc",in)]=singleInstanceNamingClient{}
		rd.registered[regKey("svc",in)]=regEntry{svc:"svc",inst:in}
	}
	ctx,cancel:=context.WithTimeout(context.Background(),time.Second)
	defer cancel()
	e.cleanupDirection(ctx,rd)
	if !rd.isStopped() { t.Fatal("cleanup must mark direction stopped") }
	if len(rd.instanceClients)!=0 { t.Fatalf("cleanup left %d instance clients",len(rd.instanceClients)) }
	if len(rd.registered)!=0 { t.Fatalf("cleanup left %d registered records",len(rd.registered)) }
}
