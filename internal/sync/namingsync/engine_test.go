package namingsync

import (
	"context"
	"net"
	"strconv"
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
		if gotIk != instKey(tt.in) {
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
	if _, ok := got[instKey(src[0])]; !ok {
		t.Fatal("原始业务实例应被保留")
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

// captureNamingClient 捕获写路径实际发出的 RegisterInstance 参数，用于断言代注册 metadata。
type captureNamingClient struct {
	naming_client.INamingClient
	got chan vo.RegisterInstanceParam
}

func (c captureNamingClient) RegisterInstance(p vo.RegisterInstanceParam) (bool, error) {
	c.got <- p
	return true, nil
}

// TestRegisterProxiedWritesSyncSourceAgent 锁定「代注册溯源」不变量：registerProxied 写入的
// 目标实例必须带齐两个溯源元数据——syncSource=NACOS、syncSourceCluster=源CID
// （syncSourceAgent=本机 IP 的写入已临时关闭，见 engine.go registerProxied，恢复时一并放开）；同时原样保留源实例 metadata。
func TestRegisterProxiedWritesSyncSourceAgent(t *testing.T) {
	pl, err := pool.New(4, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Release()

	// 监听本地端口，使源实例 ip:port 通过「僵尸门禁」的 TCP 探活（否则被判定为僵尸跳过代注册）。
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, portStr, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.ParseUint(portStr, 10, 16)

	const agentIP = "10.10.171.157"
	capCh := make(chan vo.RegisterInstanceParam, 1)
	e := &Engine{
		pl:      pl,
		logger:  zap.NewNop(),
		agentIP: agentIP,
	}
	e.mgr = nacos.NewClientManagerStub("d", "", captureNamingClient{got: capCh})
	rd := &runningDir{
		d:          &direction{ID: "g/s->g/d/", SrcCID: "s", DstCID: "d"},
		dstClient:  captureNamingClient{got: capCh},
		registered: map[string]regEntry{},
		failCount:  map[string]int{},
		dead:       map[string]regEntry{},
	}
	in := model.Instance{
		Ip:       "127.0.0.1",
		Port:     port,
		Weight:   1,
		Enable:   true,
		Healthy:  true,
		Metadata: map[string]string{"k": "v", "preserved.register.source": "SPRING_CLOUD"},
	}

	e.registerProxied(context.Background(), rd, "TEST-SVC", in)

	select {
	case p := <-capCh:
		if p.Metadata[metaSyncSource] != "NACOS" {
			t.Fatalf("syncSource = %q, want NACOS", p.Metadata[metaSyncSource])
		}
		if p.Metadata[metaSyncSourceCluster] != "s" {
			t.Fatalf("syncSourceCluster = %q, want s", p.Metadata[metaSyncSourceCluster])
		}
		// 同步服务本机 IP 溯源字段（syncSourceAgent）已临时关闭，恢复时取消下方注释：
		// if p.Metadata[metaSyncSourceAgent] != agentIP {
		// 	t.Fatalf("syncSourceAgent = %q, want %s", p.Metadata[metaSyncSourceAgent], agentIP)
		// }
		// 源实例 metadata 必须原样带到目标（除三个溯源键外）。
		if p.Metadata["k"] != "v" || p.Metadata["preserved.register.source"] != "SPRING_CLOUD" {
			t.Fatalf("源 metadata 未被原样保留: %v", p.Metadata)
		}
		// 目标实例 ip/port 必须源自源实例。
		if p.Ip != "127.0.0.1" || p.Port != port {
			t.Fatalf("目标实例地址错误: %s:%d", p.Ip, p.Port)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("registerProxied 未在 2s 内调用 RegisterInstance（可能被僵尸门禁误跳过）")
	}
}
