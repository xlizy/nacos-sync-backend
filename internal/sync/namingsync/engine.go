// Package namingsync 负责「代注册」式服务同步（naming proxy registration）。
//
// 设计（与本次需求一致）：
//   - 配置来源：运行时配置的 service-sync 段（single-sync 组内 / multiple-sync 组外）。
//   - 源集群 → 目标集群：把源集群上「真实业务应用注册」的实例，原样复制到目标集群，
//     并打上元数据 syncSource=NACOS（外加 syncSourceCluster=源集群CID 用于归属与清理），
//     以便目标集群也能发现这些跨集群/跨组的服务实例。
//   - 防回环：复制实例带 syncSource=NACOS，拉取源实例时过滤掉这些，避免 A→B→A 风暴；
//     同时按 syncSourceCluster 归属，各方向仅清理自己产生的代注册实例，互不越界。
//   - 生命周期（硬性要求：源节点不论优雅停还是崩溃停，代注册实例都要第一时间注销）：
//     代注册实例采用 ephemeral=true（走 gRPC），并绑定在本同步服务到目标集群的专属连接上。
//     源实例消失时由两条路径收敛：
//     ① 源实例列表快轮询：每 sourcePollInterval 通过 ClientManager.InstancesLive 使用独立 gRPC 查询
//        拉取源端实时实例列表（绕过 SDK 本地缓存），与本方向 desired 集合对账并增删。
//     ② 周期全量对账：修正目标漂移、补注册失败项，并清理源端已消失的残留。
//     TCP 探活不再参与存活判定（K8S 网络策略下无法直连源 Pod，误判会误注销健康实例）；
//     保留的 probe 仅处理 dead 集合中的恢复项，目前没有常规写入路径。
//     （注：若本同步服务自身崩溃，代注册实例因 ephemeral 绑定本服务连接，会由目标集群自动摘除。）
//
// 拓扑解析（统一为「全互联」，没有主集群概念）：
//   - 组内（single-sync，grp_id）：组内所有集群两两全互联（每个集群的原始实例同步到组内其余集群）。
//   - 组外（multiple-sync，A↔B）：A、B 两组全互联——每组每个集群的原始实例都同步到对方组的每个集群，
//     双向。即「A 组内任一集群注册的服务，在 B 组任一集群都可见」，反之亦然。
//     方向数 = |A集群| × |B集群| × 2；不再挑「主集群」（分组内各集群是对等关系，任一集群都可能是
//     某服务的注册入口，只挑一个必然漏同步）。
package namingsync

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/config"
	"github.com/cxcm/nacos-sync-backend/internal/metrics"
	"github.com/cxcm/nacos-sync-backend/internal/nacos"
	"github.com/cxcm/nacos-sync-backend/internal/pool"
)

const (
	metaSyncSource          = "syncSource"
	metaSyncSourceCluster   = "syncSourceCluster"
	metaSyncSourceDirection = "syncSourceDirection"
	defaultGroup            = "DEFAULT_GROUP" // 业务服务通常使用 DEFAULT_GROUP

	sourcePollInterval = 1 * time.Second  // ①' 源实例列表快轮询：优雅停的确定性即时路径（≤1s 注销），替代不可靠的 SDK 订阅推送
	reconcileInterval  = 10 * time.Second // ③ 周期全量兜底对账（含目标漂移修正与失败注册重试）

	// reconcileTickInterval 周期性 reconcile 间隔：自愈「startDirection 失败后方向永不重建」
	// （此前 reconcile 仅由配置变更触发，启动时目标集群暂不可达会导致该方向静默丢失直到下次改配置）
	// 与极端竞态下的 runs/dirs 状态漂移。reconcile 幂等：已运行且名单未变的方向无操作。
	reconcileTickInterval = 15 * time.Second

	// orphanSweepInterval 孤儿代注册实例周期清扫间隔：清理「目标集群上由本服务代注册、但当前无对应运行方向」的残留实例。
	// 这些残留来自更早的进程（崩溃/优雅停服未覆盖/旧版本 cleanup 缺失），reconcile 的 cleanup 只清当前运行方向
	// 跟踪到的实例，历史残留清不到；禁用同步后「代注册服务没下线」即此。仅删带 syncSource=NACOS 元数据的实例。
	orphanSweepInterval = 20 * time.Second
	orphanGrace         = 60 * time.Second // 多节点滚动变更/候选过期宽限：连续观察到无 owner 后才删除
	probeInterval       = 1 * time.Second  // ② TCP 主动探活源实例 ip:port 的间隔（仅用于恢复 dead 实例，失败不再判定崩溃）
	probeTimeout        = 1 * time.Second  // ② 单次 TCP 探活超时
	// emptyStreakThreshold 源实例列表连续返回空的容错阈值。InstancesLive 偶发返回空
	// （liveProxy 连接劣化 / 多节点 Nacos 同步延迟 / provider 心跳短暂过期）时，连续
	// 达到此次数才判定源实例真实消失并注销，给恢复窗口。fastPoll 1s × 3 = 3s，
	// 覆盖瞬时查询波动，不延迟真实删除（Nacos ephemeral 心跳删除 ≥30s）。
	emptyStreakThreshold = 3
	dirCleanupTimeout   = 15 * time.Second // ④ 单方向停用/删除时，注销其目标代注册实例的每方向等待上限（reconcile 触发）
	cleanupTotalTimeout = 30 * time.Second // ④ Stop 全程注销所有方向代注册实例的全局等待上限
	probeConcurrency    = 64               // ② 单引擎全局并发探活上限：把"每实例一个 goroutine"限流为有限并发，削去 400 节点下的 goroutine 风暴
	cleanupConcurrency  = 64               // ④ 单方向并发注销上限，避免数百实例瞬时打满目标 Nacos

	// ---- dubbo 动态方向参数 ----
	// dubboDiscoCache：动态服务枚举结果的共享有效期。快轮询与全量对账共用缓存，
	// 到期后重新向当前方向的源 Nacos 查询；双向方向分别查询两端，正常情况下每 30 秒一次。
	// 枚举失败时沿用上次成功结果（stale），下一周期重试，不让实例同步整体停摆。
	dubboDiscoCache = 30 * time.Second
)

// dubboPrefixes dubbo 服务名前缀（已与业务方确认：dubbo 应用按接口注册，服务名均为这两个前缀）。
var dubboPrefixes = []string{"providers:", "consumers:"}

// Engine 代注册引擎。
type Engine struct {
	conf   *config.Manager
	mgr    *nacos.ClientManager
	pl     *pool.Pool
	logger *zap.Logger

	orphanMu   sync.Mutex
	orphanSeen map[string]time.Time // 首次确认无全局 owner 的时刻；宽限期后才允许删除

	mu       sync.Mutex
	ctx      context.Context
	started  bool
	stops    map[string]context.CancelFunc
	dirs     map[string]*direction
	runs     map[string]*runningDir // 运行时状态（订阅/探活/已代注册集合），与 dirs 平行
	lastSync map[string]time.Time
	lastErr  map[string]string
	// 可观测性（gauge）：当前方向已跟踪的源实例数，与当前已在目标代注册（存活）的实例数。
	lastSrcCount map[string]int
	lastRegCount map[string]int
	wg           sync.WaitGroup

	// reconcileMu 串行化 reconcile：配置回调可能由本地发布与配置中心推送并发触发，
	// 且停方向时的清理（注销目标代注册实例）需要独占，避免两条 reconcile 交叉造成状态错乱。
	reconcileMu sync.Mutex

	// probeSem 单引擎全局探活并发信号量：限制 probe 每 tick 同时发起的 TCP 探活数，避免
	// 400 节点规模下每实例起一个 goroutine 造成的瞬时 goroutine 风暴。
	probeSem chan struct{}

	// dubboDisco 各 dubbo 动态方向「源集群服务枚举」的共享状态（方向 ID -> 枚举缓存）。
	// fastPoll（1s）负责刷新，syncService（10s）在缓存有效期内复用，避免同 tick 重复枚举。
	dubboDisco map[string]*discoState
}

// direction 一条同步方向：把 srcCID（在 srcGrpID 分组）的原始实例代注册到 dstCID（在 dstGrpID 分组）。
type direction struct {
	ID        string
	SrcGrpID  string
	SrcCID    string
	DstGrpID  string
	DstCID    string
	Namespace string // "" = public
	Services  []string
	// Dubbo 标记「动态服务」方向：不维护静态服务白名单，每个 tick 实时枚举源集群命名空间下
	// 全部服务并按 dubbo 前缀过滤（接口迭代的新增/消失自动跟随）。Services 恒为空。
	Dubbo bool
}

// regEntry 某条方向运行期跟踪的一个源原生实例（含其所属服务名，便于注销/重建）。
type regEntry struct {
	svc  string
	inst model.Instance
}

// discoState dubbo 动态方向「源集群服务枚举」的共享状态（按方向独占，字段读写均持 discoMu）。
type discoState struct {
	discoMu  sync.Mutex
	lastAt   time.Time // 上次成功枚举时刻（缓存基准）
	services []string  // 上次成功枚举并按 dubbo 前缀过滤后的服务名
}

// runningDir 某条方向运行期的状态与清理句柄。
type runningDir struct {
	d *direction

	// opMu 串行化本方向的源目标读取和写操作，禁止快轮询/慢对账/清理交叉执行。
	// 所有写操作同步完成，彻底避免共享协程池导致的“旧注册晚于新注销落地”。
	opMu       sync.Mutex
	mu         sync.Mutex
	registered map[string]regEntry // 当前已在目标代注册的源原生实例（快手路径的“应然”集合），key=regKey
	failCount  map[string]int      // 各源实例连续 TCP 探活失败次数，key=regKey
	dead       map[string]regEntry // 已被探活判定崩溃、待 Nacos 最终确认的源实例，key=regKey（期间禁止重建，保留实例本体以便自愈）
	emptyStreak map[string]int     // 各 svc 源实例列表连续返回空的次数（容错：达 emptyStreakThreshold 才注销），key=svc
	stopped    bool                // 方向已停止（准备/正在清理）：此后拒绝新的代注册，防清理后残留

	// dstClient 本方向专属的目标集群命名客户端（gRPC 连接），不进入 ClientManager 的共享缓存。
	// 由本方向独占：ephemeral 代注册实例绑定在该连接上，方向停止时 CloseClient 销毁连接与 SDK 注册缓存，
	// 令服务端随连接关闭一并回收这些实例，且断线重连不会把已下线实例自动重声明回去。
	// 相较共享缓存客户端：共享客户端被其它方向复用、不能随单方向停止而关闭，且其 registeredInstanceCached
	// 在连接重连时会被 SDK 自动重注册（见 connection_event_listener.redoRegisterEachService），正是「禁用同步后代注册实例下不了线」的根因。
	clientMu     sync.Mutex
	dstClient    naming_client.INamingClient
	directionDead atomic.Bool // 任一方向协程异常退出；reconcile 据此重启整个方向
	republishAll atomic.Bool // 目标写连接重建后，本方向全部服务都需要重新发布
}

// isStopped 报告该方向是否已被停止（准备/正在清理）。
func (rd *runningDir) isStopped() bool {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	return rd.stopped
}

// direction 安全读取当前方向配置。方向的服务名单会在配置热更新时经
// refreshRunningDirection 原地替换，所有对 rd.d 的读取必须走锁，避免与替换产生数据竞争。
func (rd *runningDir) direction() *direction {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	return rd.d
}

// setDirection 安全替换方向配置（reconcile 检测到服务名单变化时调用）。
func (rd *runningDir) setDirection(d *direction) {
	rd.mu.Lock()
	rd.d = d
	rd.mu.Unlock()
}

// regKey 用「服务名 + 实例规格键」唯一标识一个被跟踪的源发布。
// 同一 endpoint 可能由多个 Nacos Client 发布不同 metadata/weight；这些是服务端聚合视图中的不同记录。
func regKey(svc string, in model.Instance) string {
	return svc + "\x00" + instanceSpecKey(in)
}

// splitRegKey 从 regKey（svc\x00instanceSpecKey）拆出 svc 段与实例规格键。
func splitRegKey(rk string) (svc, ik string) {
	if i := strings.IndexByte(rk, 0); i >= 0 {
		return rk[:i], rk[i+1:]
	}
	return "", rk
}

// New 构造。
func New(conf *config.Manager, mgr *nacos.ClientManager, pl *pool.Pool, _ uint64, logger *zap.Logger) *Engine {
	return &Engine{
		conf:         conf,
		mgr:          mgr,
		pl:           pl,
		logger:       logger,
		orphanSeen:   make(map[string]time.Time),
		stops:        make(map[string]context.CancelFunc),
		dirs:         make(map[string]*direction),
		runs:         make(map[string]*runningDir),
		lastSync:     make(map[string]time.Time),
		lastErr:      make(map[string]string),
		lastSrcCount: make(map[string]int),
		lastRegCount: make(map[string]int),
		probeSem:     make(chan struct{}, probeConcurrency),
		dubboDisco:   make(map[string]*discoState),
	}
}

// Start 启动：初始 reconcile + 注册配置热更新 reconcile。
func (e *Engine) Start(ctx context.Context) {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return
	}
	e.ctx = ctx
	e.started = true
	e.mu.Unlock()

	e.reconcile(e.conf.Get())
	e.conf.OnRuntimeChange(func(c *config.Config) {
		e.reconcile(c)
	})
	// 周期性 reconcile：配置变更之外的自愈路径——① startDirection 失败（如启动时目标集群
	// 暂不可达）后方向永不重建；② 极端竞态下 runs/dirs 与协程实际状态失配。
	// reconcile 幂等（已运行且服务名单未变的方向无操作），周期触发无副作用。
	go e.runReconcileLoop(ctx)
	go e.runOrphanSweep(ctx)
}

// runReconcileLoop 周期触发 reconcile，自愈方向启动失败与状态漂移（见 Start 注释）。
func (e *Engine) runReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(reconcileTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.reconcile(e.conf.Get())
		}
	}
}

// Stop 取消所有方向、等待协程退出，并注销各方向在目标集群代注册的实例
// （防止 Goroutine 泄露；同时避免本服务停止后目标集群残留代注册实例）。
func (e *Engine) Stop() {
	e.reconcileMu.Lock()
	defer e.reconcileMu.Unlock()

	e.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(e.stops))
	rds := make([]*runningDir, 0, len(e.runs))
	for id, cancel := range e.stops {
		cancels = append(cancels, cancel)
		if rd, ok := e.runs[id]; ok {
			rds = append(rds, rd)
		}
	}
	e.stops = make(map[string]context.CancelFunc)
	e.runs = make(map[string]*runningDir)
	e.dubboDisco = make(map[string]*discoState)
	e.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	e.wg.Wait()
	// 全局超时约束所有方向的注销总时长：避免 Σ(方向数×旧串行超时) 的长尾阻塞优雅关闭；
	// 超时残留实例由 Nacos ephemeral 心跳最终回收（本服务进程仍在时其连接存活，是有界折中）。
	gctx, gcancel := context.WithTimeout(context.Background(), cleanupTotalTimeout)
	defer gcancel()
	for _, rd := range rds {
		e.cleanupDirection(gctx, rd)
	}
}

// reconcile 依据当前配置计算期望方向集合，增删对应协程。
// 关闭方向（禁用开关 / 删除条目 / 配置移除）时会同步注销其在目标集群代注册的实例——
// 因为代注册实例是 ephemeral，其存活绑定在本服务到目标的连接上，禁用并不会让它们自动消失。
func (e *Engine) reconcile(cfg *config.Config) {
	e.reconcileMu.Lock()
	defer e.reconcileMu.Unlock()

	e.mu.Lock()
	ctx := e.ctx
	e.mu.Unlock()
	if ctx == nil || ctx.Err() != nil {
		return
	}
	desired := e.resolveDirections(cfg)
	desiredSet := make(map[string]bool, len(desired))
	for _, d := range desired {
		desiredSet[d.ID] = true
		e.mu.Lock()
		old, hasOld := e.dirs[d.ID]
		// 以「协程句柄是否存活」（stops）判定运行中，而非 dirs（期望快照）：startDirection 失败时
		// 会 stopDir 移除句柄但 dirs 残留，若按 dirs 判定「已运行」，周期 reconcile 也永远跳过重建。
		cancel, running := e.stops[d.ID]
		rd, hasRun := e.runs[d.ID]
		e.dirs[d.ID] = d
		if running && hasRun && rd.directionDead.Load() {
			delete(e.stops, d.ID)
			delete(e.runs, d.ID)
			running = false
		} else {
			cancel = nil
		}
		e.mu.Unlock()
		if cancel != nil {
			cancel()
			// 与「停方向」路径同口径：单方向清理受 dirCleanupTimeout 约束。此处持有 reconcileMu，
			// 若目标集群挂起导致注销无响应，无超时会阻塞整个 reconcile（含其他方向的配置变更处理）。
			cctx, ccancel := context.WithTimeout(e.ctx, dirCleanupTimeout)
			e.cleanupDirection(cctx, rd)
			ccancel()
			e.logger.Error("naming direction task exited unexpectedly; restarting",
				zap.String("dir", d.ID))
		}
		if !running {
			e.startDirection(ctx, d)
			continue
		}
		// 已运行、但服务名单变化（新增/移除服务）→ 原地刷新方向配置，使新服务无需禁用再启用即可立即同步，
		// 被移除服务的代注册实例也被清理。方向 ID 不含 services，名单变化不会改变 ID，
		// 因此不会触发「停→启」，需在此显式处理（否则只有禁用再启用才生效）。组内/组外共用此逻辑。
		if hasOld && !servicesEqual(old.Services, d.Services) {
			e.refreshRunningDirection(d)
		}
	}

	// 停止不再需要的方向：先在锁内摘除句柄，再在锁外取消协程并清理目标代注册实例，
	// 避免持锁执行网络清理阻塞状态接口。
	type stopItem struct {
		id     string
		cancel context.CancelFunc
		rd     *runningDir
	}
	var toStop []stopItem
	e.mu.Lock()
	// Iterate the desired-direction snapshot, not only active cancel handles.
	// startDirection can fail before goroutines are launched and remove its stop
	// handle; such a direction must still disappear from Status when config removes it.
	for id := range e.dirs {
		if desiredSet[id] {
			continue
		}
		toStop = append(toStop, stopItem{id: id, cancel: e.stops[id], rd: e.runs[id]})
		delete(e.stops, id)
		delete(e.dirs, id)
		delete(e.runs, id)
	}
	e.mu.Unlock()

	for _, it := range toStop {
		if it.cancel != nil {
			it.cancel()
		}
		// 单方向注销受 per-direction 超时约束，避免某方向实例极多时拖住整个 reconcile。
		dctx, dcancel := context.WithTimeout(e.ctx, dirCleanupTimeout)
		e.cleanupDirection(dctx, it.rd) // 注销目标上该方向的全部代注册实例
		dcancel()
		e.mu.Lock()
		delete(e.lastSync, it.id)
		delete(e.lastErr, it.id)
		delete(e.lastSrcCount, it.id)
		delete(e.lastRegCount, it.id)
		delete(e.dubboDisco, it.id) // 方向已停，枚举缓存一并释放
		e.mu.Unlock()
		e.logger.Info("direction stopped", zap.String("dir", it.id))
	}
}

// refreshRunningDirection 当一条已在运行的方向其服务名单变化时调用：原地刷新运行实例的方向配置，
// 使快轮询/对账 goroutine 下一 tick 即采用新服务名单——新增服务立即同步（无需禁用再启用），
// 同时清理「已从同步名单移除」的服务在目标集群的代注册实例（避免残留）。
//
// 与「停→启重建」相比，原地刷新对仍在同步的服务零抖动：它们的代注册实例保留在 registered 中，
// 不会被先注销再重注册。仅被移除的服务才在此处主动注销。
func (e *Engine) refreshRunningDirection(d *direction) {
	e.mu.Lock()
	rd, ok := e.runs[d.ID]
	e.mu.Unlock()
	if !ok {
		// 运行记录缺失（极端竞态，方向刚被并发停止）→ 直接以新配置启动。
		e.startDirection(e.ctx, d)
		return
	}

	// 原地替换方向：后续 goroutine tick 即按新 services 工作（新增服务立即同步）。
	rd.opMu.Lock()
	defer rd.opMu.Unlock()
	rd.setDirection(d)

	// dubbo 动态方向无静态名单：服务集合由枚举驱动，名单变化刷新不适用（此处 d.Services 恒为空，
	// 若不清理则会把所有已代注册实例当作「已移除」全部注销——必须整段跳过）。
	if d.Dubbo {
		return
	}

	// 清理已从名单移除的服务的代注册实例。
	keep := make(map[string]bool, len(d.Services))
	for _, s := range d.Services {
		keep[s] = true
	}
	rd.mu.Lock()
	var removed []regEntry
	for rk, re := range rd.registered {
		if !keep[re.svc] {
			removed = append(removed, re)
			delete(rd.registered, rk)
			delete(rd.failCount, rk)
			delete(rd.dead, rk)
		}
	}
	for rk, re := range rd.dead {
		if !keep[re.svc] {
			removed = append(removed, re)
			delete(rd.dead, rk)
		}
	}
	rd.mu.Unlock()

	if len(removed) > 0 {
		e.logger.Info("direction service list changed, cleaning removed services",
			zap.String("dir", d.ID), zap.Int("count", len(removed)))
		for _, re := range removed {
			e.deregisterProxied(e.ctx, rd, re.svc, re.inst)
		}
	}
}

// servicesEqual 比较两个服务名单是否内容相同（顺序无关）。用于判断方向是否需因名单变化而刷新。
func servicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		if !set[s] {
			return false
		}
	}
	return true
}

// normNamespace 将命名空间归一化为 Nacos 真实 ID。
// Nacos 的 public 命名空间真实 ID 是空串 ""，字面量 "public" 与其等价，必须统一为 "" 才能正确命中；
// 其余命名空间原样返回。方向与孤儿清扫两侧都必须走同一归一化，否则 isLiveManaged 的 ns 比对会失配，
// 导致健康实例被误判孤儿反复删除（见 2026-10-16 的 churn bug）。
func normNamespace(n string) string {
	if n == "" || n == "public" {
		return ""
	}
	return n
}

// resolveDirections 把 service-sync 配置解析为一组同步方向。
func (e *Engine) resolveDirections(cfg *config.Config) []*direction {
	var dirs []*direction
	ss := cfg.ServiceSync
	nsOf := normNamespace

	// 服务同步（service-sync 段）：ss 为 nil 时整段跳过，但 dubbo 段仍须解析（两者相互独立）。
	if ss != nil {
		// 组内：全互联（每组内每个集群的原始实例 → 组内其余集群）。
		if ss.SingleSync != nil {
			for grpID, entry := range ss.SingleSync {
				// 空白名单 = 失败关闭（不同步任何服务）；未启用或分组不存在/不足两集群则跳过。
				if !entry.Enable || len(entry.Services) == 0 {
					continue
				}
				g, ok := e.conf.GetGroup(grpID)
				if !ok || len(g.Clusters) < 2 {
					continue
				}
				ns := nsOf(entry.Namespace)
				for i := range g.Clusters {
					for j := range g.Clusters {
						if g.Clusters[i].CID == g.Clusters[j].CID {
							continue
						}
						dirs = append(dirs, &direction{
							ID:        dirID(grpID, g.Clusters[i].CID, grpID, g.Clusters[j].CID, ns),
							SrcGrpID:  grpID,
							SrcCID:    g.Clusters[i].CID,
							DstGrpID:  grpID,
							DstCID:    g.Clusters[j].CID,
							Namespace: ns,
							Services:  entry.Services,
						})
					}
				}
			}
		}

		// 组外：A、B 两组全互联（每组每个集群 ↔ 对方组每个集群，双向）。
		// 不再挑「主集群」：分组内各集群对等，服务可能注册在其中任何一个上，只取一个必然漏同步。
		for _, m := range ss.MultipleSync {
			if !m.Enable || len(m.Services) == 0 {
				continue
			}
			ga, oka := e.conf.GetGroup(m.SourceA)
			gb, okb := e.conf.GetGroup(m.SourceB)
			if !oka || !okb {
				continue
			}
			ns := nsOf(m.Namespace)
			dirs = append(dirs, groupCrossDirections(m.SourceA, ga, m.SourceB, gb, ns, m.Services)...)
		}
	}

	// dubbo：每个启用条目解析为「集群A → 集群B」「集群B → 集群A」两条动态方向。
	// 动态方向不维护静态服务白名单：引擎每个 tick 实时枚举源集群命名空间下全部服务，
	// 仅保留 providers:/consumers: 前缀的 dubbo 服务（接口迭代的新增/消失自动跟随）。
	if ds := cfg.DubboSync; ds != nil {
		// 按 c_id 反查分组（c_id 仅在分组内唯一；跨分组重名时取首个匹配，与 connFor 口径一致）。
		grpOf := func(cID string) (config.NacosGroup, bool) {
			for _, g := range cfg.NacosGroup {
				for _, cl := range g.Clusters {
					if cl.CID == cID {
						return g, true
					}
				}
			}
			return config.NacosGroup{}, false
		}
		for _, e := range ds.Entries {
			if !e.Enable {
				continue
			}
			ga, oka := grpOf(e.ClusterA)
			gb, okb := grpOf(e.ClusterB)
			if !oka || !okb {
				continue // 集群已被删除：跳过（条目由 DeleteCluster/DeleteGroup 级联清理，此处兜底）
			}
			ns := nsOf(e.Namespace)
			for _, hop := range []struct{ srcGrp, srcCID, dstGrp, dstCID string }{
				{ga.GrpID, e.ClusterA, gb.GrpID, e.ClusterB},
				{gb.GrpID, e.ClusterB, ga.GrpID, e.ClusterA},
			} {
				if hop.srcCID == hop.dstCID {
					continue
				}
				dirs = append(dirs, &direction{
					ID:        dirIDFor(hop.srcGrp, hop.srcCID, hop.dstGrp, hop.dstCID, ns, true),
					SrcGrpID:  hop.srcGrp,
					SrcCID:    hop.srcCID,
					DstGrpID:  hop.dstGrp,
					DstCID:    hop.dstCID,
					Namespace: ns,
					Dubbo:     true,
				})
			}
		}
	}
	return dirs
}

// groupCrossDirections 生成组外同步（multiple-sync）的方向集合：A、B 两组全互联，
// 即每组每个集群的原始实例都同步到对方组的每个集群（双向）。方向数 = |A集群| × |B集群| × 2。
//
// 纯函数（不依赖 Engine 状态），便于单测锁定拓扑。
func groupCrossDirections(grpA string, ga config.NacosGroup, grpB string, gb config.NacosGroup, ns string, services []string) []*direction {
	var dirs []*direction
	// 一跳：srcGrp 的某集群 → dstGrp 的某集群。srcCID == dstCID 时跳过——
	// c_id 仅在分组内保证唯一，跨分组理论上可重名，此时无法判定集群归属（connFor 取首个匹配），
	// 直接跳过避免「自己同步到自己」。
	addHop := func(srcGrp, srcCID, dstGrp, dstCID string) {
		if srcCID == "" || dstCID == "" || srcCID == dstCID {
			return
		}
		dirs = append(dirs, &direction{
			ID:        dirID(srcGrp, srcCID, dstGrp, dstCID, ns),
			SrcGrpID:  srcGrp,
			SrcCID:    srcCID,
			DstGrpID:  dstGrp,
			DstCID:    dstCID,
			Namespace: ns,
			Services:  services,
		})
	}
	for i := range ga.Clusters {
		for j := range gb.Clusters {
			addHop(grpA, ga.Clusters[i].CID, grpB, gb.Clusters[j].CID)
			addHop(grpB, gb.Clusters[j].CID, grpA, ga.Clusters[i].CID)
		}
	}
	return dirs
}

func dirID(srcGrp, srcCID, dstGrp, dstCID, ns string) string {
	// dubbo 动态方向带 "dubbo" 段区分（见 dirIDFor）；本函数保留原签名给静态方向。
	return dirIDFor(srcGrp, srcCID, dstGrp, dstCID, ns, false)
}

// dirIDFor 生成方向 ID。dubbo=true 时插入 "dubbo" 段，与静态方向明确区分（展示/日志可读）。
func dirIDFor(srcGrp, srcCID, dstGrp, dstCID, ns string, dubbo bool) string {
	if dubbo {
		return fmt.Sprintf("dubbo/%s/%s->%s/%s/%s", srcGrp, srcCID, dstGrp, dstCID, ns)
	}
	return fmt.Sprintf("%s/%s->%s/%s/%s", srcGrp, srcCID, dstGrp, dstCID, ns)
}

// startDirection 为一条方向建立客户端、订阅源集群、启动探活与兜底对账。
func (e *Engine) startDirection(parent context.Context, d *direction) {
	e.mu.Lock()
	if _, ok := e.stops[d.ID]; ok {
		e.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	e.stops[d.ID] = cancel
	e.mu.Unlock()

	// 为本方向构建「专属」目标集群客户端（不进共享缓存）。该客户端由本方向独占：
	// ephemeral 代注册实例绑定在此连接上，方向停止时 CloseClient 会断开连接、清空 SDK 注册缓存，
	// 令服务端随连接关闭一并回收这些实例，且断线重连不会把已下线实例自动重声明回去——
	// 这正是「禁用同步后代注册实例下不了线」的根因所在（共享客户端会在重连时 redoRegisterEachService）。
	// 绑定失败即放弃本方向、等待下次 reconcile 重试。
	dstCli, err := e.mgr.BuildDedicatedNamingClient(d.DstCID, d.Namespace)
	if err != nil {
		e.setErr(d.ID, err)
		e.stopDir(d.ID)
		return
	}

	rd := &runningDir{
		d:          d,
		dstClient:  dstCli,
		registered:  make(map[string]regEntry),
		failCount:   make(map[string]int),
		dead:        make(map[string]regEntry),
		emptyStreak: make(map[string]int),
	}
	e.mu.Lock()
	e.runs[d.ID] = rd
	e.mu.Unlock()

	// 同步方向只使用专属数据面连接，不注册 NACOS-SYNC-WORKER，也不依赖心跳。
	// 初始全量对账：立即把已存在实例代注册出去。
	e.syncService(ctx, rd)

	e.wg.Add(3)
	// ①' 源实例列表快轮询（确定性即时路径）：优雅停 ≤ sourcePollInterval 内注销。
	// 替代 SDK 订阅推送（本环境实测移除场景延迟数秒），不受推送丢失/延迟影响。
	go func() {
		defer e.wg.Done()
		defer func() {
			if ctx.Err() == nil {
				rd.directionDead.Store(true)
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				e.logger.Error("naming direction goroutine panicked",
					zap.String("dir", d.ID), zap.String("role", "fastPoll"), zap.Any("panic", r))
			}
		}()
		ticker := time.NewTicker(sourcePollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.fastPoll(ctx, rd)
			}
		}
	}()
	// ② 探活 goroutine：仅负责 dead 崩溃实例的假死恢复（探活成功即重建代注册）；不判定崩溃——
	// 存活实例的注销由 fastPoll/syncService 基于 Nacos 源实例列表裁决（源消失即注销）。
	go func() {
		defer e.wg.Done()
		defer func() {
			if ctx.Err() == nil {
				rd.directionDead.Store(true)
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				e.logger.Error("naming direction goroutine panicked",
					zap.String("dir", d.ID), zap.String("role", "probe"), zap.Any("panic", r))
			}
		}()
		ticker := time.NewTicker(probeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.probe(ctx, rd)
			}
		}
	}()
	// ③ 兜底对账 goroutine：修正漂移、清理 Nacos 最终确认下线的残留。
	go func() {
		defer e.wg.Done()
		defer func() {
			if ctx.Err() == nil {
				rd.directionDead.Store(true)
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				e.logger.Error("naming direction goroutine panicked",
					zap.String("dir", d.ID), zap.String("role", "reconcile"), zap.Any("panic", r))
			}
		}()
		ticker := time.NewTicker(reconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.syncService(ctx, rd)
			}
		}
	}()
}

// stopDir 仅在客户端构建失败时调用：取消并清理本方向的 stop 句柄（无 goroutine 需要等待）。
func (e *Engine) stopDir(id string) {
	e.mu.Lock()
	if cancel, ok := e.stops[id]; ok {
		cancel()
		delete(e.stops, id)
	}
	// startDirection invokes stopDir only before launching its goroutines.
	// Drop the failed runtime record; dirs remains as the desired config snapshot.
	delete(e.runs, id)
	e.mu.Unlock()
}

// cleanupDirection 方向被禁用/删除（或引擎停止）时，把它在目标集群代注册过的实例全部注销，并关闭本方向专属目标客户端。
//
// 为什么必须显式注销 + 关闭客户端：代注册实例是 ephemeral（gRPC 连接绑定，Nacos 3.x 仅支持此方式），
// 由本服务显式 Deregister 移除；但 ephemeral 实例的注册会被 SDK 缓存在客户端的 registeredInstanceCached，
// 连接重连时由 connection_event_listener.redoRegisterEachService 自动重声明回服务端。因此光 Deregister 不够——
// 必须再 CloseClient 销毁该连接与其注册缓存（含已代注册实例），才能令服务端随连接关闭一并回收这些实例，
// 否则「禁用同步后代注册服务下不了线」（实测：孤儿清扫每 60s 删一次、随即被重注册回来）。
// 本方向专属客户端随方向停止而关闭，不会误伤其它方向的目标连接。
func (e *Engine) cleanupDirection(ctx context.Context, rd *runningDir) {
	if rd == nil {
		return
	}
	d := rd.direction() // 捕获方向用于日志/指标（ID 在方向生命周期内稳定；走锁避免与 setDirection 竞争）
	rd.opMu.Lock()
	defer rd.opMu.Unlock()

	rd.mu.Lock()
	rd.stopped = true // 先置停止标记：此后在途/新提交的代注册任务一律跳过，避免清理后又被注册回来
	entries := make([]regEntry, 0, len(rd.registered)+len(rd.dead))
	for _, re := range rd.registered {
		entries = append(entries, re)
	}
	// dead 中的实例在判定崩溃时已注销过，这里一并兜底（注销幂等，防漂移残留）。
	for _, re := range rd.dead {
		entries = append(entries, re)
	}
	rd.registered = make(map[string]regEntry)
	rd.dead = make(map[string]regEntry)
	rd.failCount = make(map[string]int)
	rd.mu.Unlock()

	rd.clientMu.Lock()
	client := rd.dstClient
	rd.clientMu.Unlock()
	defer func() {
		rd.clientMu.Lock()
		c := rd.dstClient
		rd.dstClient = nil
		rd.clientMu.Unlock()
		if c != nil {
			e.mgr.CloseDedicatedNamingClient(c)
		}
	}()
	if client == nil || len(entries) == 0 {
		return
	}

	// 每个进程使用独立的 ephemeral gRPC 发布连接；关闭本方向连接只回收本连接发布的实例，
	// 不会影响其它进程仍在维护的同一服务发布。
	// Nacos v2 的连接客户端对每个 (namespace, group, service) 只保存一个 publisher；
	// BatchRegisterInstance 把该 publisher 设为完整实例集合，而服务端 DeregisterInstance
	// 会直接 removeServiceInstance(service)，即一次注销会移除本连接该服务的整份发布集合。
	// 因此这里必须按服务去重，不能对同一服务的每个实例重复发送注销 RPC。
	services := make([]regEntry, 0)
	seenServices := make(map[string]struct{}, len(entries))
	for _, re := range entries {
		if _, ok := seenServices[re.svc]; ok {
			continue
		}
		seenServices[re.svc] = struct{}{}
		services = append(services, re) // SDK request still carries a representative instance identity
	}
	e.logger.Info("cleanup direction: deregistering proxied services",
		zap.String("dir", d.ID), zap.Int("services", len(services)), zap.Int("instances", len(entries)))

	// 有界并发按服务执行注销：大量实例属于同一服务时只需一个 RPC；多个服务仍分批并发。
	for start := 0; start < len(services); start += cleanupConcurrency {
		select {
		case <-ctx.Done():
			e.logger.Warn("cleanup deregister aborted by timeout, some services may linger", zap.String("dir", d.ID))
			return
		default:
		}
		end := start + cleanupConcurrency
		if end > len(services) {
			end = len(services)
		}
		var batch sync.WaitGroup
		for _, entry := range services[start:end] {
			re := entry
			batch.Add(1)
			go func() {
				defer batch.Done()
				ok, err := client.DeregisterInstance(vo.DeregisterInstanceParam{
					ServiceName: re.svc, GroupName: defaultGroup, Ip: re.inst.Ip, Port: re.inst.Port,
					Ephemeral: true, Cluster: re.inst.ClusterName,
				})
				if err == nil && !ok {
					err = fmt.Errorf("nacos rejected cleanup deregister")
				}
				if err != nil {
					e.logger.Error("cleanup deregister proxied service failed", zap.String("dir", d.ID), zap.String("svc", re.svc), zap.Error(err))
					metrics.SyncErrors.WithLabelValues(d.ID, "naming").Inc()
					return
				}
				e.logger.Info("cleanup deregister proxied service ok", zap.String("dir", d.ID), zap.String("svc", re.svc))
			}()
		}
		batch.Wait()
	}

	
}

// sweepTarget 孤儿清扫目标：目标集群 + 命名空间 + 服务。
type sweepTarget struct {
	dstGrp  string
	dstCID  string
	ns      string
	service string
}

func (t sweepTarget) label() string {
	return fmt.Sprintf("%s/%s/%s/%s", t.dstGrp, t.dstCID, t.ns, t.service)
}

// runOrphanSweep 启动期先扫一次，之后每 orphanSweepInterval 周期清扫孤儿代注册实例。
func (e *Engine) runOrphanSweep(ctx context.Context) {
	e.sweepOrphans(ctx, e.conf.Get())
	ticker := time.NewTicker(orphanSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.sweepOrphans(ctx, e.conf.Get())
		}
	}
}

// sweepScan 孤儿清扫的扫描单元：一个「目标集群 + 命名空间」。
// 与服务名解耦——服务名在扫描时由服务端实时枚举（GetAllServicesInfo），
// 从而覆盖「服务已从配置白名单移除、但实例仍残留」这类孤儿（旧实现依赖配置白名单，会漏扫）。
type sweepScan struct {
	dstGrp string
	dstCID string
	ns     string
}

func (s sweepScan) label() string {
	return fmt.Sprintf("%s/%s/%s", s.dstGrp, s.dstCID, s.ns)
}

// sweepNamespaces 返回需清扫的命名空间集合：public（空串）永远包含，
// 再并入配置里出现过的非 public 命名空间，避免漏掉曾在某命名空间同步过的目标集群。
func (e *Engine) sweepNamespaces(cfg *config.Config) []string {
	// Nacos 的 public 命名空间真实 ID 是空串 ""，与字面量 "public" 等价，必须归一化后再入集合。
	// 否则配置写 "public" 时会被扫两次（标签 "/public/" 与 "//" 各一次），且孤儿清扫侧 s.ns 若不为 ""，
	// 与方向的 d.Namespace（已归一化为 ""）比对不一致，isLiveManaged 永远失败 → 健康实例被误删。
	norm := normNamespace
	set := map[string]bool{"": true}
	if ss := cfg.ServiceSync; ss != nil {
		for _, en := range ss.SingleSync {
			if ns := norm(en.Namespace); ns != "" {
				set[ns] = true
			}
		}
		for _, m := range ss.MultipleSync {
			if ns := norm(m.Namespace); ns != "" {
				set[ns] = true
			}
		}
	}
	if ds := cfg.DubboSync; ds != nil {
		for _, e := range ds.Entries {
			if ns := norm(e.Namespace); ns != "" {
				set[ns] = true
			}
		}
	}
	for _, n := range cfg.Namespaces {
		if ns := norm(n); ns != "" {
			set[ns] = true
		}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	return out
}

// sweepScans 枚举所有需清扫的 (目标集群, 命名空间) 对。
// 覆盖配置里出现过的**全部集群**（来自 NacosGroup，含已禁用/删除同步链路所指向的集群），
// 不依赖当前同步条目是否启用、也不依赖服务白名单——孤儿恰恰常驻在这些已停方向的目标上。
func (e *Engine) sweepScans(cfg *config.Config) []sweepScan {
	var out []sweepScan
	seen := make(map[string]bool)
	for _, ns := range e.sweepNamespaces(cfg) {
		for _, g := range cfg.NacosGroup {
			for _, c := range g.Clusters {
				key := g.GrpID + "\x00" + c.CID + "\x00" + ns
				if seen[key] {
					continue
				}
				seen[key] = true
				out = append(out, sweepScan{dstGrp: g.GrpID, dstCID: c.CID, ns: ns})
			}
		}
	}
	return out
}

// sweepOrphans 清理目标集群上「由本服务代注册、但归属方向当前未运行」的孤儿实例。
// 仅处理带 syncSource=NACOS 元数据的实例（绝不误删原生实例）；归属方向仍在运行的由其自身管理，跳过。
// 读取走 InstancesLive 实时查询（绕过 SDK 本地 host 缓存，本环境 gRPC 推送不可靠）；
// 服务名由服务端 GetAllServicesInfo 实时枚举，不依赖配置白名单。
func (e *Engine) sweepOrphans(ctx context.Context, cfg *config.Config) {
	scans := e.sweepScans(cfg)
	if len(scans) == 0 {
		return
	}
	for _, s := range scans {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// 一个目标集群用一个专属客户端枚举实例并执行孤儿清理；清扫不参与选举。
		cli, err := e.mgr.BuildDedicatedNamingClient(s.dstCID, s.ns)
		if err != nil {
			e.logger.Warn("orphan sweep: build client failed", zap.String("target", s.label()), zap.Error(err))
			continue
		}
		names, err := listServiceNames(cli, s.ns)
		if err != nil {
			e.logger.Warn("orphan sweep: list services failed", zap.String("target", s.label()), zap.Error(err))
			e.mgr.CloseDedicatedNamingClient(cli)
			continue
		}
		cleaned := 0
		observedOrphans := make(map[string]struct{})
		completeScan := true
		for _, svc := range names {
			// 注意：NACOS-SYNC（appName）不再在此跳过。本后端自注册的 NACOS-SYNC 心跳不带
			// syncSource=NACOS 元数据，下方 isProxied 检查已将其排除，不会误删；而代注册上来的
			// NACOS-SYNC 实例（带 syncSource=NACOS）若归属方向未实时持有，正应作为孤儿清算，
			// 否则会残留本机 IP 这类代注册实例。
			list, err := e.mgr.InstancesLive(s.dstCID, s.ns, svc, defaultGroup)
			if err != nil {
				// 任一服务查询失败时不做本轮 map GC，避免把暂时不可见的实例误当作已消失。
				completeScan = false
				e.logger.Debug("orphan sweep: query instances skipped", zap.String("service", svc), zap.Error(err))
				continue
			}
			for _, inst := range list {
				if inst.Metadata[metaSyncSource] != "NACOS" {
					continue // 非本服务代注册，跳过（保护原生实例）
				}
				seenKey := s.label() + "\x00" + regKey(svc, inst)
				observedOrphans[seenKey] = struct{}{}
				if e.isGloballyManaged(s, svc, inst, cfg) {
					e.clearOrphanSeen(s, svc, inst)
					continue
				}
				// Ephemeral instances are owned by the gRPC connection that published them.
				// This sweep client cannot deregister another connection's publication;
				// closing the owning connection removes it automatically. Never report a
				// successful cleanup for a foreign ephemeral publication.
				if inst.Ephemeral {
					continue
				}
				// 多节点滚动更新与候选过期都有短暂窗口：必须连续观察到无全局 owner 并超过宽限期才删。
				if !e.orphanReady(s, svc, inst) {
					continue
				}
				target := sweepTarget{dstGrp: s.dstGrp, dstCID: s.dstCID, ns: s.ns, service: svc}
				if e.deregisterOrphan(cli, target, inst) {
					e.clearOrphanSeen(s, svc, inst)
					cleaned++
				}
			}
		}
		if completeScan {
			e.pruneOrphanSeen(s, observedOrphans)
		}
		e.mgr.CloseDedicatedNamingClient(cli)
		if cleaned > 0 {
			e.logger.Info("orphan sweep: cleaned", zap.String("target", s.label()), zap.Int("count", cleaned))
		}
	}
}

// isGloballyManaged 根据当前同步配置和实例归属元数据判断是否仍有方向管理该实例。
func (e *Engine) isGloballyManaged(s sweepScan, svc string, inst model.Instance, cfg *config.Config) bool {
	srcCID := inst.Metadata[metaSyncSourceCluster]
	directions := e.resolveDirections(cfg)

	// 新实例带有方向 ID：只要当前配置仍包含该方向，就保留其归属实例。
	// 不依赖跨节点 Worker 心跳；ephemeral 发布的实际生命周期由各节点自己的 gRPC 连接管理。
	if dirID := inst.Metadata[metaSyncSourceDirection]; dirID != "" {
		for _, d := range directions {
			if d.ID != dirID || d.SrcCID != srcCID || d.DstCID != s.dstCID || d.Namespace != s.ns {
				continue
			}
			return directionManagesService(d, svc)
		}
		return false
	}

	// 兼容旧版本没有 syncSourceDirection 的实例：从当前拓扑反推归属。
	for _, d := range directions {
		if d.SrcCID == srcCID && d.DstCID == s.dstCID && d.Namespace == s.ns && directionManagesService(d, svc) {
			return true
		}
	}
	return false
}

// directionManagesService 判断当前方向配置是否仍包含指定服务。
// Dubbo 方向的服务集由源端动态枚举驱动，静态方向则必须命中 Services 白名单。
func directionManagesService(d *direction, svc string) bool {
	if d == nil {
		return false
	}
	if d.Dubbo {
		return isDubboService(svc)
	}
	for _, configured := range d.Services {
		if configured == svc {
			return true
		}
	}
	return false
}

func (e *Engine) orphanReady(s sweepScan, svc string, inst model.Instance) bool {
	key := s.label() + "\x00" + regKey(svc, inst)
	e.orphanMu.Lock()
	defer e.orphanMu.Unlock()
	first, ok := e.orphanSeen[key]
	if !ok {
		e.orphanSeen[key] = time.Now()
		return false
	}
	return time.Since(first) >= orphanGrace
}

func (e *Engine) clearOrphanSeen(s sweepScan, svc string, inst model.Instance) {
	e.orphanMu.Lock()
	delete(e.orphanSeen, s.label()+"\x00"+regKey(svc, inst))
	e.orphanMu.Unlock()
}

// pruneOrphanSeen removes grace-period records for instances no longer present in a
// complete target scan. It is deliberately not called after a partial scan: transient
// query failures must not reset the grace period of instances that were not observed.
func (e *Engine) pruneOrphanSeen(s sweepScan, observed map[string]struct{}) {
	prefix := s.label() + "\x00"
	e.orphanMu.Lock()
	for key := range e.orphanSeen {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, ok := observed[key]; !ok {
			delete(e.orphanSeen, key)
		}
	}
	e.orphanMu.Unlock()
}

// deregisterOrphan 注销一个孤儿代注册实例。
// 代注册实例始终以 ephemeral=true 注册（走 gRPC 代理；Nacos 3.x 不支持实例 HTTP OpenAPI，返回 501）。
// 故此处按 ephemeral=true 走 gRPC DeregisterInstance 一次性移除；不尝试 HTTP 路径，避免 3.x 目标上无意义的 501 报错与指标污染。
func (e *Engine) deregisterOrphan(client naming_client.INamingClient, t sweepTarget, inst model.Instance) bool {
	ok, err := client.DeregisterInstance(vo.DeregisterInstanceParam{
		ServiceName: t.service,
		GroupName:   defaultGroup,
		Ip:          inst.Ip,
		Port:        inst.Port,
		Ephemeral:   true, // 与注册一致，走 gRPC
		Cluster:     inst.ClusterName,
	})
	if err == nil && !ok {
		err = fmt.Errorf("nacos rejected orphan deregister")
	}
	if err != nil {
		e.logger.Error("orphan sweep: deregister failed",
			zap.String("target", t.label()), zap.String("inst", instKey(inst)), zap.Error(err))
		metrics.SyncErrors.WithLabelValues(t.label(), "naming").Inc()
		return false
	}
	e.logger.Info("orphan sweep: deregistered proxied instance",
		zap.String("target", t.label()), zap.String("svc", t.service), zap.String("inst", instKey(inst)))
	return true
}

// listServiceNames 通过服务端 GetAllServicesInfo 实时枚举某命名空间下的全部服务名（按页拉全）。
// 不依赖本地缓存，也不依赖配置里的服务白名单——这是孤儿清扫能覆盖「配置已移除服务」的关键。
func listServiceNames(cli naming_client.INamingClient, namespace string) ([]string, error) {
	var out []string
	pageNo := uint32(1)
	pageSize := uint32(100)
	for {
		sl, err := cli.GetAllServicesInfo(vo.GetAllServiceInfoParam{
			NameSpace: namespace,
			GroupName: defaultGroup,
			PageNo:    pageNo,
			PageSize:  pageSize,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, sl.Doms...)
		// 已拉全或服务列表为空：结束。防御性上限避免极端分页死循环。
		if int64(len(out)) >= sl.Count || len(sl.Doms) == 0 || pageNo >= 1000 {
			break
		}
		pageNo++
	}
	return out, nil
}

// isDubboService 按已确认的 dubbo 命名规律过滤：服务名以 providers: / consumers: 开头。
func isDubboService(name string) bool {
	for _, p := range dubboPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// dubboDiscoFresh 返回该动态方向仍在有效期（dubboDiscoCache）内的枚举结果；过期/无缓存返回 (nil, false)。
// 成功过则 (services, true)（services 可能为空 = 源当前无 dubbo 服务）；从未成功则 (nil, false)。
func (e *Engine) dubboDiscoFresh(d *direction) ([]string, bool) {
	e.mu.Lock()
	st, ok := e.dubboDisco[d.ID]
	e.mu.Unlock()
	if !ok {
		return nil, false
	}
	st.discoMu.Lock()
	defer st.discoMu.Unlock()
	if !st.lastAt.IsZero() && time.Since(st.lastAt) < dubboDiscoCache {
		return st.services, true
	}
	return nil, false
}

// dubboStaleServices 返回上次成功枚举的服务列表（无论缓存是否过期）。
// 枚举失败/退避期间，fastPoll 与 syncService 沿用此列表继续对账实例，
// 使 dubbo 方向像普通服务同步一样：服务列表不因枚举偶发失败而丢失，
// 对账（查实例/注册/注销）持续进行，不整方向停摆。
//
// 安全性：对账基于实例（InstancesLive），不基于服务名。stale 列表可能漏掉
// 新增的 dubbo 服务（枚举恢复后由 dubboSetChanged 补），但不会误处理已有
// 服务——provider 真下线时 InstancesLive 返回空即注销，属正确行为。
// stale 列表不得用于 reconcileDisappearedDubboServices（消失对账需新鲜结果）。
func (e *Engine) dubboStaleServices(d *direction) []string {
	e.mu.Lock()
	st, ok := e.dubboDisco[d.ID]
	e.mu.Unlock()
	if !ok || st == nil {
		return nil
	}
	st.discoMu.Lock()
	defer st.discoMu.Unlock()
	return st.services
}

// dubboRefreshDisco 枚举源集群命名空间下全部服务并按 dubbo 前缀过滤，写入方向共享缓存。
// 由 fastPoll 每 tick 调用（缓存有效期内复用，见 dubboDiscoFresh）。
// 枚举失败时沿用上次成功结果（stale），下一缓存周期重试——不退避、不停摆：
// 对账基于实例（InstancesLive），stale 列表只决定"查哪些服务"，漏掉新增服务不误处理已有。
func (e *Engine) dubboRefreshDisco(ctx context.Context, rd *runningDir) []string {
	d := rd.direction()
	e.mu.Lock()
	st, ok := e.dubboDisco[d.ID]
	e.mu.Unlock()
	if !ok {
		st = &discoState{}
		e.mu.Lock()
		e.dubboDisco[d.ID] = st
		e.mu.Unlock()
	}

	st.discoMu.Lock()
	defer st.discoMu.Unlock()
	now := time.Now()
	// 缓存有效期检查：freshness 内不重枚举（返回 nil，调用方经 dubboDiscoFresh 走缓存）。
	if !st.lastAt.IsZero() && now.Sub(st.lastAt) < dubboDiscoCache {
		return nil
	}

	cli, err := e.mgr.EnumClient(d.SrcCID, d.Namespace)
	if err == nil {
		if err = ctx.Err(); err != nil {
			return nil
		}
		names, lerr := listServiceNames(cli, d.Namespace)
		if lerr != nil {
			err = lerr
		} else {
			svcs := make([]string, 0, len(names))
			for _, n := range names {
				if n == e.conf.AppName() {
					continue // 同步服务自身注册非业务服务，与静态方向同口径排除
				}
				if isDubboService(n) {
					svcs = append(svcs, n)
				}
			}
			st.lastAt = now
			st.services = svcs
			return svcs
		}
	}
	// 失败：沿用 stale（dubboStaleServices），下一缓存周期重试。对账继续不停摆。
	e.logger.Warn("dubbo service enumeration failed, using last known set",
		zap.String("dir", d.ID), zap.Error(err))
	return nil
}

// dubboServicesForReconcile 返回 dubbo 方向本轮要对账的服务列表：
// 枚举结果（新鲜或 stale）∪ rd.registered 中已有的服务名。
//
// 并入 registered 服务名是统一 dubbo 与普通服务对账的关键：dubbo 服务从枚举结果
// 消失（provider 下线）后，只要该服务仍有代注册实例在 registered 中，就会被继续
// 对账——fastPoll/syncService 的"源实例消失→注销"自然清理，与普通服务的对账路径
// 完全一致。无需基于服务名集合的单独清理。
//
// 新增 dubbo 服务：枚举返回 → 在列表 → 对账 → 注册。
// 消失 dubbo 服务：枚举不返回 → 但仍在 registered → 在列表 → 对账 → 源空 → 注销 → registered 清空 → 下轮不在列表。
// 枚举失败：沿用 stale ∪ registered → 继续对账，不停摆。

func (e *Engine) dubboServicesForReconcile(ctx context.Context, rd *runningDir) []string {
	d := rd.direction()
	var svcs []string
	if refreshed := e.dubboRefreshDisco(ctx, rd); refreshed != nil {
		svcs = refreshed
	} else if cached, fresh := e.dubboDiscoFresh(d); fresh {
		svcs = cached
	} else {
		svcs = e.dubboStaleServices(d)
	}
	// 并入 registered 里的服务名（让消失服务被自然对账注销）。
	seen := make(map[string]bool, len(svcs))
	for _, s := range svcs {
		seen[s] = true
	}
	rd.mu.Lock()
	for _, re := range rd.registered {
		if !seen[re.svc] {
			seen[re.svc] = true
			svcs = append(svcs, re.svc)
		}
	}
	rd.mu.Unlock()
	return svcs
}

// fastPoll ①' 源实例列表快轮询（确定性即时路径）：每 sourcePollInterval 拉源原生实例，
// 与已代注册集合对账，即时增删目标代注册实例。
// 为什么不用 SDK 订阅推送作为主路径：本环境实测推送在实例移除场景有数秒延迟（~9s），不可靠；
// 快轮询保证优雅停 ≤ sourcePollInterval 内注销。崩溃停同样经本路径收敛：源进程崩溃后其
// Nacos ephemeral 心跳超时（实例从源列表消失）→ 快轮询检出即注销；TCP 探活不参与崩溃判定
// （K8S 网络策略下无法直连源 pod，误判会误注销健康实例）。目标漂移/失败注册重试由 ③ 周期对账覆盖。
func (e *Engine) fastPoll(ctx context.Context, rd *runningDir) {
	rd.opMu.Lock()
	defer rd.opMu.Unlock()
	if rd.isStopped() {
		return // 方向已停止，不再读写状态（避免清理后又写入/注册）
	}
	d := rd.direction()
	// 服务名单：静态方向用配置白名单；dubbo 动态方向由 dubboServicesForReconcile
	// 提供（枚举结果 ∪ registered 服务名），后续对账与静态方向完全一致。
	var svcs []string
	if d.Dubbo {
		svcs = e.dubboServicesForReconcile(ctx, rd)
		if len(svcs) == 0 {
			return // 从未成功枚举且 registered 为空：无服务可对账。
		}
	} else {
		svcs = d.Services
	}

	// A rebuilt destination connection has lost all publications owned by the
	// previous connection. Force every configured service to publish its full
	// desired snapshot, even when the local registered map itself did not change.
	forceRepublish := rd.republishAll.Swap(false)
	queryFailed := false

	for _, svc := range svcs {
		// 跳过同步服务自身的服务发现注册（appName）：它非业务服务，若被跨集群代注册，会把本机/
		// 对端后端自注册实例误当作业务实例同步，导致目标集群出现本机 IP 的代注册残留。
		if svc == e.conf.AppName() {
			continue
		}
		srcList, err := e.mgr.InstancesLive(d.SrcCID, d.Namespace, svc, defaultGroup)
		if err != nil {
			e.logger.Error("fast poll select source failed",
				zap.String("dir", d.ID), zap.String("svc", svc), zap.Error(err))
			metrics.SyncErrors.WithLabelValues(d.ID, "naming").Inc()
			e.setErr(d.ID, err) // 暴露真实失败，避免 /api/naming-sync 的 last_error 被掩盖为空
			queryFailed = true
			continue
		}
		natives := filterNatives(srcList)

		// 锁内只做状态登记/移除并收集网络操作，锁外经 dispatchOps 串行执行：
		// 整个 tick 由 rd.opMu 串行化，保证同一方向的注册/注销严格有序——
		// 乱序会让迟到的旧注册覆盖新注销（频繁发版时旧 IP 回魂的根因之一）。
		var ops []regOp
		rd.mu.Lock()
		// 新增的源原生实例 → 登记（dead 中的崩溃实例暂不重建，等 Nacos 最终确认）。
		for _, in := range natives {
			rk := regKey(svc, in)
			if _, ok := rd.dead[rk]; ok {
				continue
			}
			if cur, ok := rd.registered[rk]; !ok {
				rd.registered[rk] = regEntry{svc: svc, inst: in}
				ops = append(ops, regOp{add: true, svc: svc, in: in})
			} else if !sameSourceSpec(cur.inst, in) {
				rd.registered[rk] = regEntry{svc: svc, inst: in}
				ops = append(ops, regOp{add: true, svc: svc, in: in})
			}
		}
	// 源已消失的实例 → 移除并收集注销（仅本 svc）。
	// 容错：整个服务无实例（len(natives)==0）时，可能是 liveProxy 连接劣化、
	// 多节点 Nacos 同步延迟、provider 心跳短暂过期，不立即注销——连续
	// emptyStreakThreshold 次空才判定真实消失。源非空时清零计数。
	skipDeregister := false
	if len(natives) == 0 {
		for _, re := range rd.registered {
			if re.svc == svc {
				rd.emptyStreak[svc]++
				if rd.emptyStreak[svc] < emptyStreakThreshold {
					skipDeregister = true
				}
				break
			}
		}
	} else {
		delete(rd.emptyStreak, svc)
	}
	if !skipDeregister {
		for rk, re := range rd.registered {
			if re.svc != svc {
				continue
			}
			if _, ok := natives[instanceSpecKey(re.inst)]; ok {
				continue
			}
			delete(rd.registered, rk)
			delete(rd.failCount, rk)
			delete(rd.dead, rk)
			ops = append(ops, regOp{add: false, svc: re.svc, in: re.inst})
		}
	}
	// Nacos 已不再报告的 dead 实例 → 释放标记（兜底）。
	for rk := range rd.dead {
		svcOf, ik := splitRegKey(rk)
		if svcOf != svc {
			continue
		}
		if _, ok := natives[ik]; !ok {
			delete(rd.dead, rk)
		}
	}
	rd.mu.Unlock()

		if forceRepublish {
			// A synthetic add marks the service as touched; dispatchOps then
			// publishes the complete current snapshot from rd.registered.
			ops = append(ops, regOp{add: true, svc: svc})
		}
		e.dispatchOps(ctx, rd, ops)
	}
	if queryFailed && forceRepublish {
		rd.republishAll.Store(true)
	}
	e.updateCounts(rd)
}

// regOp 一轮对账在锁内收集出的待执行网络操作（代注册/注销），锁外统一提交协程池。
type regOp struct {
	add bool // true=代注册；false=注销
	svc string
	in  model.Instance
}

// dispatchOps 按收集顺序串行执行一批代注册/注销网络操作。必须在**未持有 rd.mu** 时调用
// （任务体内部需要 rd.mu）；调用方（fastPoll/syncService）持 rd.opMu，保证写序稳定。
func (e *Engine) dispatchOps(ctx context.Context, rd *runningDir, ops []regOp) {
	// SDK v2.3.5 keeps one redo-publication per (group, service) and
	// BatchRegisterInstance replaces that publication with the full instance set.
	// Server-side deregistration removes this connection's whole publisher for
	// the service, so collapse any number of instance removals into one RPC and
	// republish the complete remaining desired set once.
	//
	// Churn avoidance: when a service has both an add op and a remove op in the
	// same tick (the classic case is source metadata drift — the old spec key
	// disappears from natives and a new one appears), skip deregisterProxied
	// and let registerServiceProxied's BatchRegisterInstance overwrite the full
	// publication. BatchRegister is replace-semantics: the old spec is dropped
	// from the server-side instance set without the publisher being torn down
	// first, so the target cluster never observes an "all instances gone then
	// back" flap. Deregistration is still issued when a service has only
	// removals (the source endpoint truly disappeared); the subsequent
	// republish restores whatever remains in the desired set.
	touched := make(map[string]struct{})
	hasAdd := make(map[string]bool)
	removals := make(map[string]regOp)
	for _, op := range ops {
		touched[op.svc] = struct{}{}
		if op.add {
			hasAdd[op.svc] = true
		} else if _, exists := removals[op.svc]; !exists {
			removals[op.svc] = op
		}
	}
	for svc, op := range removals {
		if hasAdd[svc] {
			continue // 同 tick 有注册：BatchRegister 覆盖语义会移除旧 spec，跳过注销避免 churn。
		}
		e.deregisterProxied(ctx, rd, svc, op.in)
	}
	for svc := range touched {
		e.registerServiceProxied(ctx, rd, svc)
	}
}

// syncService ③ 全量兜底对账：拉源实例 → 过滤代注册 → 与真实目标对账（幂等补/清）。
func (e *Engine) syncService(ctx context.Context, rd *runningDir) {
	rd.opMu.Lock()
	defer rd.opMu.Unlock()
	if rd.isStopped() {
		return // 方向已停止，不再读写状态（避免清理后又写入/注册）
	}
	d := rd.direction()
	// failed 记录本轮是否出现过取数/对账失败：失败时不能用 setOK 覆盖错误，
	// 否则 /api/naming-sync 的 last_error 永远为空，排查时看不到真实失败原因。
	var failed bool

	// dubbo 动态方向：与 fastPoll 共用 dubboServicesForReconcile（枚举 ∪ registered），
	// 后续对账与静态方向完全一致。目标残留清理由全局 sweepOrphans 兜底（基于实例 + 宽限期，
	// 不依赖本轮枚举结果集合，避免枚举波动误注销）。
	var svcs []string
	if d.Dubbo {
		svcs = e.dubboServicesForReconcile(ctx, rd)
		if len(svcs) == 0 {
			return // 从未成功枚举且 registered 为空。
		}
	} else {
		svcs = d.Services
	}

	forceRepublish := rd.republishAll.Swap(false)

	for _, svc := range svcs {
		// 跳过同步服务自身的服务发现注册（appName）：它非业务服务，若被跨集群代注册，会把本机/
		// 对端后端自注册实例误当作业务实例同步，导致目标集群出现本机 IP 的代注册残留。
		if svc == e.conf.AppName() {
			continue
		}
		srcList, err := e.mgr.InstancesLive(d.SrcCID, d.Namespace, svc, defaultGroup)
		if err != nil {
			e.logger.Error("reconcile select source failed",
				zap.String("dir", d.ID), zap.String("svc", svc), zap.Error(err))
			metrics.SyncErrors.WithLabelValues(d.ID, "naming").Inc()
			e.setErr(d.ID, err)
			failed = true
			continue
		}
		natives := filterNatives(srcList)

		// 与真实目标对账：InstancesLive 实时 gRPC 查询（ServiceQuery 直发服务端，不读本地缓存）。
		// 不能用 dst 专属客户端的 SelectAllInstances：其读 SDK 本地缓存+推送，本环境推送不可靠，
		// 会把「目标实际缺失」误报为存在 → 注册失败后永远跳过补注册（2026-10-23 发版换 IP 后
		// 偶发长期缺代注册实例的根因之一）；反向也会误报「不存在」产生幽灵注销。
		dstAll, err := e.mgr.InstancesLive(d.DstCID, d.Namespace, svc, defaultGroup)
		if err != nil {
			e.logger.Error("reconcile select target failed",
				zap.String("dir", d.ID), zap.String("svc", svc), zap.Error(err))
			metrics.SyncErrors.WithLabelValues(d.ID, "naming").Inc()
			// 连接层故障由 InstancesLive 内部 invalidateLiveProxy 即时重建，无需重建本方向写客户端。
			e.setErr(d.ID, err)
			failed = true
			continue
		}
		ours := make(map[string]model.Instance, len(dstAll))
		for _, in := range dstAll {
			if isOurs(in, d.SrcCID, d.ID) {
				ours[instanceSpecKey(sourceViewOfProxy(in))] = in
			}
		}

		// 锁内只做状态登记/移除并收集网络操作，锁外经 dispatchOps 串行执行（写序保障见 fastPoll 注释）。
		var ops []regOp
		rd.mu.Lock()
		// 源有、目标缺（或尚未代注册）→ 登记/补注册（dead 中崩溃实例跳过）。
		for _, in := range natives {
			rk := regKey(svc, in)
			if _, ok := rd.dead[rk]; ok {
				continue
			}
			if cur, ok := rd.registered[rk]; !ok {
				rd.registered[rk] = regEntry{svc: svc, inst: in}
				ops = append(ops, regOp{add: true, svc: svc, in: in})
			} else {
				if !sameSourceSpec(cur.inst, in) {
					rd.registered[rk] = regEntry{svc: svc, inst: in}
					ops = append(ops, regOp{add: true, svc: svc, in: in})
					continue
				}
				dst, exists := ours[instanceSpecKey(in)]
				if !exists || e.instanceNeedsUpdate(d, in, dst) {
					ops = append(ops, regOp{add: true, svc: svc, in: in})
				}
			}
		}
	// 目标有、源无（含 dead 中已确认崩溃）→ 移除并收集注销。
	// 容错：与 fastPoll 共用 emptyStreak——整个服务无实例时连续 emptyStreakThreshold 次空才注销。
	skipDeregister := false
	if len(natives) == 0 && len(ours) > 0 {
		rd.emptyStreak[svc]++
		if rd.emptyStreak[svc] < emptyStreakThreshold {
			skipDeregister = true
		}
	} else if len(natives) > 0 {
		delete(rd.emptyStreak, svc)
	}
	if !skipDeregister {
		for key, in := range ours {
			if _, ok := natives[key]; ok {
				continue
			}
			rk := svc + "\x00" + key
			delete(rd.registered, rk)
			delete(rd.failCount, rk)
			delete(rd.dead, rk)
			ops = append(ops, regOp{add: false, svc: svc, in: in})
		}
	}
		for rk := range rd.dead {
			svcOf, ik := splitRegKey(rk)
			if svcOf != svc {
				continue
			}
			if _, ok := natives[ik]; !ok {
				delete(rd.dead, rk)
			}
		}
		rd.mu.Unlock()

		if forceRepublish {
			ops = append(ops, regOp{add: true, svc: svc})
		}
		e.dispatchOps(ctx, rd, ops)
	}
	if failed && forceRepublish {
		rd.republishAll.Store(true)
	}
	e.updateCounts(rd)
	if failed {
		return // 保留本轮错误详情，不用 ok 覆盖
	}
	e.setOK(d.ID)
}

// probe ② TCP 主动探活（仅 dead 集合）：自愈「曾被判崩溃、现已假死恢复」的源实例——探活成功即
// 重建其代注册。不可达**不再判定崩溃**（K8S 网络策略/仅 Service 可达的部署下无法直连源 pod，
// TCP 探测不可靠，误判会误注销健康实例）；存活实例的注销统一由 fastPoll/syncService 基于
// Nacos 源实例列表裁决（源从列表消失即注销）。当前 dead 集合无写入者（崩溃判定已移除），
// 故常规情况下每 tick 零探活——保留本协程仅为未来恢复崩溃判定时承接自愈路径。
func (e *Engine) probe(ctx context.Context, rd *runningDir) {
	rd.opMu.Lock()
	defer rd.opMu.Unlock()
	if rd.isStopped() {
		return
	}
	// 仅快照 dead 集合：registered 存活实例不参与探活（结果无决策作用，纯 TCP 开销）。
	// dead 常规恒为空（崩溃判定已移除），空集合直接返回。
	rd.mu.Lock()
	if len(rd.dead) == 0 {
		rd.mu.Unlock()
		return
	}
	snapshot := make(map[string]regEntry, len(rd.dead))
	for rk, re := range rd.dead {
		snapshot[rk] = re
	}
	rd.mu.Unlock()

	// 有界并发探活：经引擎级信号量限制同时发起的 TCP 探活数，削去每实例一 goroutine 的风暴
	// （400 节点下原本每秒凭空数千 goroutine）。信号量全局共享，使所有方向的探活并发受总上限约束。
	var wg sync.WaitGroup
	for rk, re := range snapshot {
		rk, re := rk, re
		// ctx 取消时等待已派发任务完成再退出，避免长时间阻塞在令牌获取上。
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case e.probeSem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-e.probeSem }()
			alive := tcpReach(re.inst.Ip, re.inst.Port, probeTimeout)
			rd.mu.Lock()
			// 护栏：若已被其它路径（优雅停/兜底对账）清理，本 tick 不再处理，防误伤。
			if _, inReg := rd.registered[rk]; !inReg {
				if _, inDead := rd.dead[rk]; !inDead {
					rd.mu.Unlock()
					return
				}
			}
			if alive {
				rd.failCount[rk] = 0
				// 假死恢复：dead 中实例重新探活成功 → 清除标记并重建代注册。
				if _, ok := rd.dead[rk]; ok {
					delete(rd.dead, rk)
					if _, ok := rd.registered[rk]; !ok {
						rd.registered[rk] = re
					}
					rd.mu.Unlock()
					e.registerProxied(ctx, rd, re.svc, re.inst)
					return
				}
			}
			rd.mu.Unlock()
			// 探活不可达：在无法直连源 pod 的部署（K8S 网络策略/仅 Service 可达）下，TCP 探测不可靠，
			// 不据此降级为崩溃、也不主动注销。源实例是否真正消失由 reconcile/fastPoll 基于 Nacos 实例
			// 列表最终裁决（源从列表消失即自然 deregister）。保持当前 registered 状态即可。
			return
		}()
	}
	wg.Wait()
}

// filterNatives 从源实例列表中过滤出"原始业务注册"（剔除 syncSource=NACOS 的代注册实例，防回环）。
func filterNatives(srcList []model.Instance) map[string]model.Instance {
	natives := make(map[string]model.Instance, len(srcList))
	for _, in := range srcList {
		if isProxied(in) {
			continue
		}
		natives[instanceSpecKey(in)] = in
	}
	return natives
}

func (e *Engine) proxiedMetadata(d *direction, in model.Instance) map[string]string {
	md := make(map[string]string, len(in.Metadata)+4)
	for k, v := range in.Metadata {
		md[k] = v
	}
	md[metaSyncSource] = "NACOS"
	md[metaSyncSourceCluster] = d.SrcCID
	md[metaSyncSourceDirection] = d.ID
	return md
}

func sameSourceSpec(a, b model.Instance) bool {
	return a.Ip == b.Ip && a.Port == b.Port && a.ClusterName == b.ClusterName &&
		a.Weight == b.Weight && a.Enable == b.Enable && a.Healthy == b.Healthy &&
		reflect.DeepEqual(a.Metadata, b.Metadata)
}

// instanceNeedsUpdate 比较源实例与目标真实实例的全部业务属性。只按 ip:port 判断存在
// 会漏掉发版过程中的权重/metadata 变化；这里以完整应然 spec 为准做幂等刷新。
func (e *Engine) instanceNeedsUpdate(d *direction, src, dst model.Instance) bool {
	if src.Ip != dst.Ip || src.Port != dst.Port || src.ClusterName != dst.ClusterName ||
		src.Weight != dst.Weight || src.Enable != dst.Enable || src.Healthy != dst.Healthy {
		return true
	}
	return !reflect.DeepEqual(e.proxiedMetadata(d, src), dst.Metadata)
}

// dstClient 取得向目标集群写实例的客户端——即本方向【专属】的命名客户端（见 runningDir.dstClient）。
// 该客户端在 startDirection 时构建、方向停止时 CloseClient 销毁，由本方向独占，不进入共享缓存，
// 因此关闭它不会误伤其它方向；而代注册的 ephemeral 实例随连接关闭被服务端回收、且不会在重连时被 SDK 自动重声明。
// 写失败自愈由 rebuildDstClient 负责（关闭旧连接、新建本方向专属连接），而非失效共享缓存。
func (e *Engine) dstClient(rd *runningDir) (naming_client.INamingClient, error) {
	rd.clientMu.Lock()
	c := rd.dstClient
	rd.clientMu.Unlock()
	if c != nil {
		return c, nil
	}

	// A previous rebuild may have failed after closing the old client. Do not
	// leave the direction permanently wedged on a nil pointer: retry creation
	// on the next write attempt and mark the whole direction dirty.
	d := rd.direction()
	nc, err := e.mgr.BuildDedicatedNamingClient(d.DstCID, d.Namespace)
	if err != nil {
		rd.republishAll.Store(true) // retry client creation on the next fast poll
		return nil, fmt.Errorf("rebuild destination client for direction %s: %w", d.ID, err)
	}
	rd.clientMu.Lock()
	if rd.dstClient == nil {
		rd.dstClient = nc
		c = nc
		rd.republishAll.Store(true)
	} else {
		c = rd.dstClient
	}
	rd.clientMu.Unlock()
	if c != nc {
		e.mgr.CloseDedicatedNamingClient(nc)
	}
	return c, nil
}

// rebuildDstClient 本方向写操作遭遇连接层故障时，重建本方向专属目标客户端（关闭旧连接、新建连接）。
// 仅影响本方向：旧客户端被关闭即销毁其 registeredInstanceCached，新客户端独立，不会误伤其它方向的目标连接。
func (e *Engine) rebuildDstClient(rd *runningDir) {
	// Callers hold rd.opMu, so no direction write can race this replacement.
	rd.clientMu.Lock()
	old := rd.dstClient
	rd.dstClient = nil
	rd.clientMu.Unlock()
	if old != nil {
		e.mgr.CloseDedicatedNamingClient(old)
	}

	d := rd.direction()
	nc, err := e.mgr.BuildDedicatedNamingClient(d.DstCID, d.Namespace)
	if err != nil {
		e.logger.Error("rebuild direction dst client failed", zap.String("dir", d.ID), zap.Error(err))
		// Keep republishAll set. dstClient() will retry construction on the next
		// operation, and the successful retry will republish every service.
		rd.republishAll.Store(true)
		return
	}
	rd.clientMu.Lock()
	rd.dstClient = nc
	rd.clientMu.Unlock()
	// Closing the old connection removes every ephemeral publication owned by
	// it, not merely the instance whose write failed.
	rd.republishAll.Store(true)
}

// registerProxied 向目标集群发起代注册（临时实例 ephemeral=true，走 gRPC，Nacos 3.x 支持）。
// 注意：Nacos 3.x 已移除持久实例的 HTTP 写接口，SDK 持久实例注册会被 501 拒绝，故必须用 ephemeral。
// 生命周期通过本服务的 Nacos 实时快轮询与周期全量对账管理：源实例消失即 DeregisterInstance 注销。
func (e *Engine) registerProxied(ctx context.Context, rd *runningDir, svc string, _ model.Instance) {
	// Recovery paths also publish the complete service snapshot.
	e.registerServiceProxied(ctx, rd, svc)
}

// registerServiceProxied publishes the complete desired ephemeral instance set
// for one service. SDK v2.3.5 caches registrations by service; repeated
// RegisterInstance calls overwrite that cache, so a reconnect would restore
// only the last instance. BatchRegisterInstance stores the complete set.
func (e *Engine) registerServiceProxied(ctx context.Context, rd *runningDir, svc string) {
	if ctx.Err() != nil {
		return
	}
	rd.mu.Lock()
	if rd.stopped {
		rd.mu.Unlock()
		return
	}
	d := rd.d
	instances := make([]vo.RegisterInstanceParam, 0)
	for _, re := range rd.registered {
		if re.svc != svc {
			continue
		}
		in := re.inst
		instances = append(instances, vo.RegisterInstanceParam{
			ServiceName: svc, GroupName: defaultGroup,
			Ip: in.Ip, Port: in.Port, Weight: in.Weight,
			Enable: in.Enable, Healthy: in.Healthy, Ephemeral: true,
			Metadata: e.proxiedMetadata(d, in), ClusterName: in.ClusterName,
		})
	}
	rd.mu.Unlock()

	// The preceding deregistration already removed the SDK redo cache. An
	// empty desired set needs no batch request (the SDK rejects empty batches).
	if len(instances) == 0 {
		return
	}
	client, err := e.dstClient(rd)
	if err != nil {
		e.setErr(d.ID, err)
		return
	}
	ok, err := client.BatchRegisterInstance(vo.BatchRegisterInstanceParam{
		ServiceName: svc, GroupName: defaultGroup, Instances: instances,
	})
	// nacos-sdk-go v2.3.5 only exposes (success, error) here. When the server
	// responds success=false, the SDK drops the RPC response's errorCode/message.
	// Preserve enough request context to identify the rejected service and endpoints.
	endpoints := make([]string, 0, min(len(instances), 20))
	for i, p := range instances {
		if i >= 20 {
			endpoints = append(endpoints, fmt.Sprintf("... and %d more", len(instances)-i))
			break
		}
		endpoints = append(endpoints, fmt.Sprintf("%s:%d@%s", p.Ip, p.Port, p.ClusterName))
	}
	requestContext := fmt.Sprintf(
		"target_cluster=%q namespace=%q service=%q group=%q instances=%d endpoints=[%s]",
		d.DstCID, d.Namespace, svc, defaultGroup, len(instances), strings.Join(endpoints, ", "),
	)
	if err != nil {
		err = fmt.Errorf("batch register request failed (%s): %w", requestContext, err)
	} else if !ok {
		err = fmt.Errorf("nacos rejected batch register instances (%s; nacos-sdk-go v2.3.5 did not expose the RPC response message)", requestContext)
	}
	if err != nil {
		// Roll back this service's local desired snapshot so the next fast poll
		// reconstructs it from the source and retries the entire batch.
		rd.mu.Lock()
		for rk, re := range rd.registered {
			if re.svc == svc {
				delete(rd.registered, rk)
				rd.failCount[rk]++
			}
		}
		rd.mu.Unlock()
		metrics.SyncErrors.WithLabelValues(d.ID, "naming").Inc()
		e.setErr(d.ID, err)
		e.logger.Error("batch register proxied failed",
			zap.String("dir", d.ID), zap.String("svc", svc),
			zap.Int("instances", len(instances)), zap.Error(err))
		e.rebuildDstClient(rd)
		return
	}
	rd.mu.Lock()
	for _, p := range instances {
		delete(rd.failCount, regKey(svc, model.Instance{Ip: p.Ip, Port: p.Port, ClusterName: p.ClusterName}))
	}
	rd.mu.Unlock()
	e.logger.Info("batch register proxied ok",
		zap.String("dir", d.ID), zap.String("svc", svc), zap.Int("instances", len(instances)))
	metrics.ReconcileDrift.WithLabelValues(d.ID).Inc()
}
// deregisterProxied 从目标集群注销本方向的代注册实例（源已消失/崩溃时调用）。
func (e *Engine) deregisterProxied(ctx context.Context, rd *runningDir, svc string, in model.Instance) {
	if ctx.Err() != nil {
		return
	}
	d := rd.direction()
	client, cerr := e.dstClient(rd)
	if cerr != nil {
		e.logger.Error("deregister proxied: get dst client failed",
			zap.String("dir", d.ID), zap.String("svc", svc), zap.String("inst", instKey(in)), zap.Error(cerr))
		metrics.SyncErrors.WithLabelValues(d.ID, "naming").Inc()
		e.setErr(d.ID, cerr)
		return
	}
	ok, err := client.DeregisterInstance(vo.DeregisterInstanceParam{
		ServiceName: svc,
		GroupName:   defaultGroup,
		Ip:          in.Ip,
		Port:        in.Port,
		Ephemeral:   true, // 必须与注册时一致（均为 ephemeral，走 gRPC）；Nacos 3.x 不支持实例 HTTP OpenAPI（501）。
		Cluster:     in.ClusterName,
	})
	if err == nil && !ok {
		err = fmt.Errorf("nacos rejected deregister instance")
	}
	if err != nil {
		e.setErr(d.ID, err)
		e.logger.Error("deregister proxied failed",
			zap.String("dir", d.ID), zap.String("svc", svc), zap.String("inst", instKey(in)), zap.Error(err))
		metrics.SyncErrors.WithLabelValues(d.ID, "naming").Inc()
		// 目标集群节点重启等连接层故障：重建本方向专属目标客户端（关闭旧连接、新建连接），下次写操作即自愈。
		e.rebuildDstClient(rd)
		return
	}
	e.logger.Info("deregister proxied ok",
		zap.String("dir", d.ID), zap.String("svc", svc), zap.String("inst", instKey(in)))
	metrics.ReconcileDrift.WithLabelValues(d.ID).Inc()
}

func (e *Engine) setOK(id string) {
	e.mu.Lock()
	e.lastSync[id] = time.Now()
	e.lastErr[id] = ""
	e.mu.Unlock()
}

func (e *Engine) setErr(id string, err error) {
	e.mu.Lock()
	e.lastErr[id] = err.Error()
	e.mu.Unlock()
}

// updateCounts 把方向当前跟踪/代注册的实例数刷到可观测 gauge（供状态接口排查）。
func (e *Engine) updateCounts(rd *runningDir) {
	dirID := rd.direction().ID // 走锁读取方向 ID（配置热更新可能替换 rd.d）
	rd.mu.Lock()
	tracked := len(rd.registered) + len(rd.dead)
	proxied := len(rd.registered)
	rd.mu.Unlock()
	e.mu.Lock()
	e.lastSrcCount[dirID] = tracked
	e.lastRegCount[dirID] = proxied
	e.mu.Unlock()
}

// isProxied 判断实例是否为本同步服务代注册上去的（用于拉源时过滤，防回环）。
func isProxied(in model.Instance) bool {
	if in.Metadata == nil {
		return false
	}
	return in.Metadata[metaSyncSource] == "NACOS"
}

// isOurs 判断目标实例是否为「本方向（srcCID）」代注册上去的（用于精准清理，不越界）。
func isOurs(in model.Instance, srcCID, directionID string) bool {
	if in.Metadata == nil ||
		in.Metadata[metaSyncSource] != "NACOS" ||
		in.Metadata[metaSyncSourceCluster] != srcCID {
		return false
	}
	// 新版本代注册实例必须匹配方向 ID，避免不同分组的相同 c_id 串归属。
	// 空值仅兼容升级前遗留实例。
	if owner := in.Metadata[metaSyncSourceDirection]; owner != "" && owner != directionID {
		return false
	}
	return true
}

// instanceSpecKey distinguishes publisher records that share an endpoint but
// differ in metadata or operational attributes. Nacos aggregates publisher
// clients independently; its service view can therefore contain both records.
func instanceSpecKey(in model.Instance) string {
	metadata := in.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	spec := struct {
		IP          string            `json:"ip"`
		Port        uint64            `json:"port"`
		ClusterName string            `json:"clusterName"`
		Weight      float64           `json:"weight"`
		Enable      bool              `json:"enable"`
		Healthy     bool              `json:"healthy"`
		Metadata    map[string]string `json:"metadata"`
	}{
		IP: in.Ip, Port: in.Port, ClusterName: in.ClusterName,
		Weight: in.Weight, Enable: in.Enable, Healthy: in.Healthy, Metadata: metadata,
	}
	encoded, _ := json.Marshal(spec) // string-keyed metadata is always JSON encodable
	return string(encoded)
}

// sourceViewOfProxy removes the ownership metadata added by proxiedMetadata so
// a target publisher can be matched to its corresponding source specification.
func sourceViewOfProxy(in model.Instance) model.Instance {
	out := in
	metadata := make(map[string]string, len(in.Metadata))
	for key, value := range in.Metadata {
		switch key {
		case metaSyncSource, metaSyncSourceCluster, metaSyncSourceDirection:
			continue
		default:
			metadata[key] = value
		}
	}
	out.Metadata = metadata
	return out
}

func instKey(in model.Instance) string {
	return fmt.Sprintf("%s:%d:%s", in.Ip, in.Port, in.ClusterName)
}

// tcpReach 对 ip:port 做一次 TCP 连接探测，用于主动判断源实例进程是否仍存活监听。
func tcpReach(ip string, port uint64, timeout time.Duration) bool {
	addr := net.JoinHostPort(ip, strconv.FormatUint(port, 10))
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// DirectionStatus 供状态接口展示。
type DirectionStatus struct {
	ID              string   `json:"id"`
	Src             string   `json:"src"`
	Dst             string   `json:"dst"`
	Namespace       string   `json:"namespace"`
	Services        []string `json:"services"`
	Running         bool     `json:"running"`
	Dubbo           bool     `json:"dubbo"`  // true = dubbo 动态方向（服务集合由枚举驱动，Services 恒为空）
	LastSync        string   `json:"last_sync"`
	LastError       string   `json:"last_error"`
	LastSrcCount    int      `json:"last_source_count"` // 当前已跟踪的源实例数（含已判崩溃待确认）
	LastRegAttempts int      `json:"last_registered"`   // 当前已在目标代注册的实例数（存活）
}

// Status 返回各同步方向状态（供排查）。
func (e *Engine) Status() []DirectionStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]DirectionStatus, 0, len(e.dirs))
	for id, d := range e.dirs {
		nsDisp := d.Namespace
		if nsDisp == "" {
			nsDisp = "public" // 空串即 public 命名空间，展示时归一化
		}
		st := DirectionStatus{
			ID:        id,
			Src:       d.SrcGrpID + "/" + d.SrcCID,
			Dst:       d.DstGrpID + "/" + d.DstCID,
			Namespace: nsDisp,
			Services:  d.Services,
			Running:   false,
			Dubbo:     d.Dubbo,
		}
		if _, ok := e.stops[id]; ok {
			st.Running = true
		}
		if t, ok := e.lastSync[id]; ok {
			st.LastSync = t.Format(time.RFC3339)
		}
		if er, ok := e.lastErr[id]; ok {
			st.LastError = er
		}
		if c, ok := e.lastSrcCount[id]; ok {
			st.LastSrcCount = c
		}
		if c, ok := e.lastRegCount[id]; ok {
			st.LastRegAttempts = c
		}
		out = append(out, st)
	}
	return out
}
