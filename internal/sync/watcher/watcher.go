// Package watcher 管理「配置同步」轮询任务。
//
// 设计（与部署模型对齐）：
//   - 同一机房/集群可能部署 2 个 nacos-sync-backend 节点；页面改配置只写配置中心，
//     不在处理请求的节点上直接启停业务，而是由每个节点在启动/配置变更时自行 reconcile。
//   - 每个「开启配置同步的 3.X 分组」起一个 3s 轮询 goroutine：直接查该分组 Nacos 后端
//     MySQL 的 config_info(+config_info_gray) md5 指纹，与内存值比对；不一致则调用
//     /nacos/v3/admin/cs/ops/localCache 刷新该分组所有集群所有节点的服务端缓存。
//   - 通过 Manager.OnRuntimeChange 在 runtime.yaml 热更新时 reconcile（增删轮询）。
//   - 30s 巡检：检测异常终止（goroutine 退出）的轮询并重建。
//
// 2.X 分组无本地缓存刷新 admin 接口，且前端已禁用其配置同步开关，故此处直接跳过。
package watcher

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/config"
	"github.com/cxcm/nacos-sync-backend/internal/nacos"
)

const (
	pollInterval   = 3 * time.Second  // 配置指纹轮询间隔
	patrolInterval = 30 * time.Second // 异常监听巡检间隔
	digestTimeout  = 10 * time.Second // 单次 DB 指纹查询超时：防 goroutine 存活但查询长期挂起
	// lastOK 超过该时长未刷新即视为不健康（覆盖 5 个轮询周期 + 单次刷新耗时）。
	lastOKStaleAfter = 20 * time.Second
)

// PollerStatus 供状态接口展示。
type PollerStatus struct {
	GrpID      string `json:"grp_id"`
	Healthy    bool   `json:"healthy"`
	LastOK     string `json:"last_ok"`     // RFC3339，无则为空
	LastDigest string `json:"last_digest"` // 最近一次指纹（仅用于排障，非机密）
	Nodes      int    `json:"nodes"`       // 该分组集群节点总数
}

type poller struct {
	grpID      string
	dsn        string
	db         *sql.DB
	stopCh     chan struct{}
	done       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	lastOK     time.Time
	lastDigest string
}

// Manager 配置同步轮询管理器。
type Manager struct {
	mgr         *config.Manager
	logger      *zap.Logger
	mu          sync.Mutex
	reconcileMu sync.Mutex // 串行化慢速建连阶段，避免并发 reconcile 互相覆盖
	pollers     map[string]*poller

	// refresh 可在测试中替换（依赖注入）；生产路径为 refreshGroup。
	refresh func(grpID string) error

	reconcileMu sync.Mutex // 串行化 reconcile 与 shutdown
	started     bool
	stopped     bool
}

// New 构造。
func New(mgr *config.Manager, logger *zap.Logger) *Manager {
	return &Manager{mgr: mgr, logger: logger, pollers: make(map[string]*poller), refresh: nil}
}

// doRefresh 执行分组缓存刷新（生产用 refreshGroup；测试可覆盖 Manager.refresh 注入故障）。
func (w *Manager) doRefresh(grpID string) error {
	if w.refresh != nil {
		return w.refresh(grpID)
	}
	return w.refreshGroup(grpID)
}

// Start 启动：初始 reconcile + 注册配置变更 reconcile + 启动巡检 goroutine。
func (w *Manager) Start(ctx context.Context) {
	w.reconcileMu.Lock()
	if w.started {
		w.reconcileMu.Unlock()
		return
	}
	w.started = true
	w.reconcileMu.Unlock()

	w.reconcile(w.mgr.Get())
	w.mgr.OnRuntimeChange(func(c *config.Config) {
		w.reconcile(c)
	})
	go w.patrol(ctx)
}

// patrol 每 patrolInterval 重新 reconcile：配置变更或异常终止的轮询都会被重建。
func (w *Manager) patrol(ctx context.Context) {
	t := time.NewTicker(patrolInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			w.stopAll()
			return
		case <-t.C:
			w.reconcile(w.mgr.Get())
		}
	}
}

// desired 计算应存在的轮询集合：开启配置同步 + 3.X + 已配置 DSN 的分组。
func (w *Manager) desired(cfg *config.Config) map[string]config.NacosGroup {
	out := make(map[string]config.NacosGroup)
	for _, g := range cfg.NacosGroup {
		if cfg.ConfigSync[g.GrpID] && g.GrpVersion == "3X" && g.DBDSN != "" {
			out[g.GrpID] = g
		}
	}
	return out
}

// reconcile 增删/重建轮询任务。
// 为避免「持 w.mu 状态下执行 OpenGroupDB（网络/IO 建连）」阻塞整个管理器（包括 /api/pollers 读取
// 与所有 reconcile），本函数只在 w.mu 下做「停旧 + 收集待建列表」的轻量操作；实际的 DB 建连
// （可能慢/不可达）移到锁外执行，建好后再加锁安装。
func (w *Manager) reconcile(cfg *config.Config) {
	w.reconcileMu.Lock()
	defer w.reconcileMu.Unlock()
	if w.stopped {
		return
	}

	desired := w.desired(cfg)
	w.mu.Lock()
	// 收集需停止的 poller，关闭 DB 连接放到 w.mu 外执行。
	var toStop []*poller
	for id, p := range w.pollers {
		if _, ok := desired[id]; !ok {
			toStop = append(toStop, p)
			delete(w.pollers, id)
		}
	}
	// 收集需要新建/重建的分组（锁内只做判定，DB 建连在锁外）。
	var toStart []config.NacosGroup
	for id, g := range desired {
		p, ok := w.pollers[id]
		restart := false
		if !ok {
			restart = true
		} else {
			select {
			case <-p.done:
				restart = true // 异常终止，需重建
			default:
				if p.dsn != g.DBDSN {
					restart = true // DSN 变更，需重建连接
				}
			}
		}
		if restart {
			if ok {
				toStop = append(toStop, p) // 旧连接稍后在锁外关闭
				delete(w.pollers, id)
			}
			toStart = append(toStart, g)
		}
	}
	w.mu.Unlock()
	for _, p := range toStop {
		p.stop()
	}

	if len(toStart) == 0 {
		return
	}
	// 锁外建连（慢/不可达不影响管理器其余操作）。
	started := make(map[string]*poller, len(toStart))
	for _, g := range toStart {
		started[g.GrpID] = w.newPoller(g)
	}
	// DB 建连期间配置可能已再次变化。必须以 Manager 当前配置复核，而不能只相信
	// 本次 reconcile 的旧快照；否则旧 DSN 的慢建连可能在新 DSN 已安装后反向覆盖它。
	currentDesired := w.desired(w.mgr.Get())
	w.mu.Lock()
	for id, p := range started {
		g, stillDesired := currentDesired[id]
		if !stillDesired || g.DBDSN != p.dsn {
			p.stop()
			continue
		}
		if existing, ok := w.pollers[id]; ok {
			// reconcileMu 保证不会有另一个 reconcile 并发安装；若仍存在，
			// 同 DSN 保留现有 poller，不同 DSN 则替换并在锁外停止旧实例。
			if existing.dsn == p.dsn {
				p.stop()
				continue
			}
			toStop = append(toStop, existing)
		}
		w.pollers[id] = p
	}
	w.mu.Unlock()
	for _, p := range toStop {
		p.stop()
	}
}

// newPoller 建连并启动轮询 goroutine；建连失败则标记异常终止，交由巡检重建。
func (w *Manager) newPoller(g config.NacosGroup) *poller {
	p := &poller{
		grpID:  g.GrpID,
		dsn:    g.DBDSN,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	db, err := nacos.OpenGroupDB(g.DBDSN)
	if err != nil {
		w.logger.Error("open group db for poller failed", zap.String("grp_id", g.GrpID), zap.Error(err))
		close(p.done) // 标记异常终止，patrol 会重建
		return p
	}
	p.db = db
	go p.run(w)
	return p
}

// run 轮询主循环：每 pollInterval 计算指纹并比对；stop 时退出。
func (p *poller) run(w *Manager) {
	defer close(p.done)
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("config poller panicked", zap.String("grp_id", p.grpID), zap.Any("panic", r))
		}
	}()
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-t.C:
			w.pollOnce(p)
		}
	}
}

// pollOnce 计算指纹、比对；变化则刷新该分组所有节点服务端缓存。
// 关键状态机（修复 P1「刷新失败后永不重试」）：
//   - lastDigest 仅在该分组**全部节点刷新成功**后才提交；
//   - 刷新失败时保留旧指纹（保持 pending），下一轮 digest 与旧指纹不等即自动重试；
//   - 无变化且查询成功才更新 lastOK（查询健康证明），刷新失败同样不更新。
//
// ConfigDigest 使用 poller 自身的可取消 ctx + 单次超时：stop 时在途查询及时返回，
// 查询挂起（DB 不可达但连接未断）也不会拖住轮询周期。
func (w *Manager) pollOnce(p *poller) {
	qctx, qcancel := context.WithTimeout(p.ctx, digestTimeout)
	digest, err := nacos.ConfigDigest(qctx, p.db)
	qcancel()
	if err != nil {
		w.logger.Warn("config digest failed", zap.String("grp_id", p.grpID), zap.Error(err))
		return // 不更新 lastOK；goroutine 仍存活，下个周期重试
	}
	w.applyDigest(p, digest)
}

// applyDigest 指纹状态机（独立出来便于单元测试注入 refresh 故障）：
// 首轮（lastDigest 为空）建立基线，也触发一次全量刷新（冷启动对齐各节点缓存）；
// 后续指纹变化即刷新；所有路径均遵循「刷新成功才提交指纹与 lastOK」。
func (w *Manager) applyDigest(p *poller, digest string) {
	p.mu.Lock()
	oldDigest := p.lastDigest
	firstRound := oldDigest == ""
	changed := !firstRound && oldDigest != digest
	p.mu.Unlock()

	if firstRound || changed {
		if changed {
			w.logger.Info("config digest changed (db fingerprint differs)",
				zap.String("grp_id", p.grpID),
				zap.String("old_digest", oldDigest),
				zap.String("new_digest", digest))
		}
		if err := w.doRefresh(p.grpID); err != nil {
			// 刷新失败：**不提交**新指纹（保持 pending），下一轮 digest != oldDigest 会再次刷新；
			// 也不更新 lastOK，让 /api/pollers 的健康判定能看到刷新一直未成功。
			w.logger.Warn("refresh group cache failed; digest not committed, will retry next poll",
				zap.String("grp_id", p.grpID), zap.Error(err))
			return
		}
		p.mu.Lock()
		p.lastDigest = digest
		p.lastOK = time.Now()
		p.mu.Unlock()
		if changed {
			w.logger.Info("config changed, refreshed nacos server cache", zap.String("grp_id", p.grpID))
		}
		return
	}
	// 无变化且查询成功 → 更新 lastOK。
	p.mu.Lock()
	p.lastOK = time.Now()
	p.mu.Unlock()
}

// refreshGroup 刷新该分组所有集群所有节点的服务端本地缓存。
func (w *Manager) refreshGroup(grpID string) error {
	g, ok := w.mgr.GetGroup(grpID)
	if !ok {
		return fmt.Errorf("group %s not found", grpID)
	}
	var nodeList []string
	for _, cl := range g.Clusters {
		nodeList = append(nodeList, cl.CNodes...)
	}
	// 可观测性：明确记录「触发了远程配置刷新（/nacos/v3/admin/cs/ops/localCache）」这一关键动作，
	// 以及涉及的分组与节点列表，便于确认刷新确实被发起、覆盖到哪些节点。
	w.logger.Info("triggering remote config refresh (localCache)",
		zap.String("grp_id", grpID),
		zap.Int("node_count", len(nodeList)),
		zap.Strings("nodes", nodeList))
	var firstErr error
	for _, cl := range g.Clusters {
		for _, node := range cl.CNodes {
			if err := nacos.RefreshLocalCache(node, g); err != nil {
				w.logger.Warn("refresh node cache failed", zap.String("grp_id", grpID), zap.String("node", node), zap.Error(err))
				if firstErr == nil {
					firstErr = err
				}
			} else {
				w.logger.Info("refreshed node cache ok", zap.String("grp_id", grpID), zap.String("node", node))
			}
		}
	}
	return firstErr
}

// stop 幂等停止：关闭 stop 信号、取消 ctx（中断在途 DB 查询）与 DB 连接（连接关闭可能短暂阻塞，但不在锁内）。
func (p *poller) stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		if p.cancel != nil {
			p.cancel()
		}
		if p.db != nil {
			_ = p.db.Close()
		}
	})
}

// stopAll 全部停止（ctx 取消时）。与 reconcile 串行化，且禁止 shutdown 后的配置回调重建 poller。
func (w *Manager) stopAll() {
	w.reconcileMu.Lock()
	w.stopped = true
	w.mu.Lock()
	pollers := make([]*poller, 0, len(w.pollers))
	for id, p := range w.pollers {
		pollers = append(pollers, p)
		delete(w.pollers, id)
	}
	w.mu.Unlock()
	w.reconcileMu.Unlock()

	for _, p := range pollers {
		p.stop()
	}
}

// Status 返回各轮询任务状态（供排查）。
// Healthy 判定（修复 P2「goroutine 存活即健康」）：goroutine 在跑 **且** 最近一次
// 成功轮询在 lastOKStaleAfter 以内。DB 长期不可达/查询挂起时 lastOK 不再前进，
// 状态如实显示不健康，而不是仅凭 goroutine 存活假报健康。
func (w *Manager) Status() []PollerStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]PollerStatus, 0, len(w.pollers))
	for id, p := range w.pollers {
		p.mu.Lock()
		st := PollerStatus{GrpID: id, LastOK: p.lastOK.Format(time.RFC3339), LastDigest: p.lastDigest}
		lastOK := p.lastOK
		p.mu.Unlock()
		select {
		case <-p.done:
			st.Healthy = false
		default:
			st.Healthy = !lastOK.IsZero() && time.Since(lastOK) < lastOKStaleAfter
		}
		if w.mgr != nil {
			if g, ok := w.mgr.GetGroup(id); ok {
				for _, cl := range g.Clusters {
					st.Nodes += len(cl.CNodes)
				}
			}
		}
		if lastOK.IsZero() {
			st.LastOK = ""
		}
		out = append(out, st)
	}
	return out
}
