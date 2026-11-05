// Package configsync 负责 new↔new 配置同步。
//
// 关键认知：新 3 集群共享同一 DB，因此目标集群的 config_info 在源发布后已是新值，
// 但 Nacos 的变更通知是「集群内」的，连在源集群的客户端才被推，目标集群客户端收不到。
// 因此同步服务必须向目标集群再 PublishConfig 一次，以「点燃」其本地通知。
//
// 由于目标内容已相等，不能靠「内容相等跳过」做幂等（那会永远不发布、通知永不抵达）。
// 这里改用「回声抑制」：记录「我们从 A 推到 B 的内容」，当 B 的监听器又触发同一内容时，
// 判定为我们的回声而跳过，从而既点燃通知又杜绝 A→B→A 风暴。
package configsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/config"
	"github.com/cxcm/nacos-sync-backend/internal/metrics"
	"github.com/cxcm/nacos-sync-backend/internal/nacos"
	"github.com/cxcm/nacos-sync-backend/internal/pool"
)

const echoTTL = 30 * time.Second

// igniteCooldown 同一 (link, namespace) 的「初始点火」冷却：避免 patrol 因监听重建而周期性重发布风暴。
// 首次建立监听必点火；之后若该链路在冷却期内重建（如瞬时故障自愈），跳过重复点火。
const igniteCooldown = 5 * time.Minute

// pushRec 回声表条目。仅存内容哈希（而非完整配置内容），避免大配置对象长期常驻内存；
// 即便存在未触发 GC 的残留 key，其内存占用也可忽略。
type pushRec struct {
	hash string
	at   time.Time
}

// hashContent 计算配置内容的 sha256 十六进制摘要，用于回声比对（避免存储明文/大对象）。
func hashContent(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// Engine 配置同步引擎。
type Engine struct {
	conf   *config.Manager
	mgr    *nacos.ClientManager
	pl     *pool.Pool
	logger *zap.Logger

	mu         sync.Mutex
	parentCtx  context.Context               // 引擎生命周期 ctx（Start 时设置），供 reconcile 启动新链路派生
	stops      map[string]context.CancelFunc // linkKey -> 取消函数（link 级生命周期）
	linkCtxs   map[string]context.Context    // linkKey -> 该 link 的 ctx（供各命名空间监听派生，stop 时整条链路取消）
	watches    map[string]*watchTask         // linkKey|ns -> 命名空间监听任务（失败由 patrol 重建）
	wg         sync.WaitGroup
	pushed     sync.Map // pushKey(from,to,ns,group,dataId) -> pushRec（仅存内容哈希，避免大对象常驻）
	gcOnce     sync.Once
	igniteMu   sync.Mutex
	lastIgnite map[string]time.Time // linkKey|ns -> 上次初始点火时刻（冷却去重，防重发布风暴）
}

// watchTask 单个 (link, namespace) 监听任务。
type watchTask struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// New 构造。
func New(conf *config.Manager, mgr *nacos.ClientManager, pl *pool.Pool, logger *zap.Logger) *Engine {
	return &Engine{
		conf:       conf,
		mgr:        mgr,
		pl:         pl,
		logger:     logger,
		stops:      make(map[string]context.CancelFunc),
		linkCtxs:   make(map[string]context.Context),
		watches:    make(map[string]*watchTask),
		lastIgnite: make(map[string]time.Time),
	}
}

// Start 启动所有 enabled 的 config 链路，并启动回声表 GC、监听巡检与配置 reconcile。
func (e *Engine) Start(ctx context.Context) {
	e.parentCtx = ctx
	e.gcOnce.Do(func() { go e.gcEcho(ctx) })
	c := e.conf.Get()
	for i := range c.Links {
		link := c.Links[i]
		if link.Kind != "config" || !link.Enabled {
			continue
		}
		e.startLink(ctx, link)
	}
	// 配置变更 reconcile：链路启用/禁用、命名空间增删时增删监听与链路，避免
	// 「命名空间删除后旧监听仍存活」「link 禁用后后台仍在同步」等残留。
	e.conf.OnRuntimeChange(func(c *config.Config) {
		e.reconcileLinks(c)
	})
	go e.patrol(ctx)
}

func (e *Engine) startLink(parent context.Context, link config.SyncLink) {
	key := linkKey(link)
	e.mu.Lock()
	if _, ok := e.stops[key]; ok {
		e.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	e.stops[key] = cancel
	e.linkCtxs[key] = ctx
	e.mu.Unlock()

	e.ensureWatches(ctx, link)
}

// ensureWatches 为该 link 的每个命名空间确保一个监听任务（已存在且存活则跳过）。
func (e *Engine) ensureWatches(ctx context.Context, link config.SyncLink) {
	c := e.conf.Get()
	for _, ns := range c.Namespaces {
		e.ensureWatch(link, ns)
	}
}

// ensureWatch 确保单个 (link, ns) 监听任务存活：不存在或已退出则重建（派生自 link ctx，
// 故 link 被 stop 时该监听会随之取消，不会复活已停链路）。
func (e *Engine) ensureWatch(link config.SyncLink, ns string) {
	key := linkKey(link) + "|" + ns
	e.mu.Lock()
	if t, ok := e.watches[key]; ok {
		select {
		case <-t.done:
			// 已退出，下方重建
		default:
			e.mu.Unlock()
			return // 仍在运行
		}
	}
	linkKey1 := linkKey(link)
	linkCtx, ok := e.linkCtxs[linkKey1]
	if !ok {
		e.mu.Unlock()
		return
	}
	wctx, cancel := context.WithCancel(linkCtx)
	t := &watchTask{cancel: cancel, done: make(chan struct{})}
	e.watches[key] = t
	e.mu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer close(t.done)
		defer cancel()
		e.watchNamespace(wctx, link, ns)
	}()
}

// patrol 周期性确保每条 enabled config link 的所有命名空间都在被监听；
// 某命名空间因建连/枚举失败而退出时，此处会重建它（自愈），避免「失败即永久丢弃」。
func (e *Engine) patrol(ctx context.Context) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c := e.conf.Get()
			for _, link := range c.Links {
				if link.Kind != "config" || !link.Enabled {
					continue
				}
				lkey := linkKey(link)
				e.mu.Lock()
				_, alive := e.stops[lkey]
				e.mu.Unlock()
				if !alive {
					e.startLink(ctx, link) // 链路本身没了（理论上不会，兜底）
					continue
				}
				e.ensureWatches(ctx, link)
			}
		}
	}
}

func (e *Engine) runLink(ctx context.Context, link config.SyncLink) {
	<-ctx.Done()
}

// reconcileLinks 依据当前配置增删 config 链路及其命名空间监听：
//   - 链路被禁用/删除 → 取消其 linkCtx（级联取消所有派生 watch）并从 maps 移除；
//   - 某命名空间从配置移除 → 取消该 (link, ns) 监听；
//   - 新链路/新命名空间 → 启动对应监听（startLink/ensureWatch 幂等）。
//
// 这是 configsync 此前缺失的 reconcile：namingsync/watcher 都具备，唯独 configsync 没有，
// 导致命名空间删除后旧监听仍存活、link 禁用后后台仍在同步，直至进程重启。
func (e *Engine) reconcileLinks(cfg *config.Config) {
	desiredLinks := map[string]config.SyncLink{}
	desiredNS := map[string]map[string]bool{} // linkKey -> set(ns)
	for _, l := range cfg.Links {
		if l.Kind != "config" || !l.Enabled {
			continue
		}
		k := linkKey(l)
		desiredLinks[k] = l
		nsSet := make(map[string]bool, len(cfg.Namespaces))
		for _, ns := range cfg.Namespaces {
			nsSet[ns] = true
		}
		desiredNS[k] = nsSet
	}

	e.mu.Lock()
	// 1) 停掉已不再期望的链路（禁用/删除）：级联取消其所有 watch。
	for k, cancel := range e.stops {
		if _, ok := desiredLinks[k]; ok {
			continue
		}
		cancel()
		delete(e.stops, k)
		delete(e.linkCtxs, k)
		for wk, wt := range e.watches {
			if strings.HasPrefix(wk, k+"|") {
				wt.cancel()
				delete(e.watches, wk)
			}
		}
	}
	// 2) 链路仍启用，但某命名空间已从配置移除 → 取消对应 watch。
	for wk, wt := range e.watches {
		parts := strings.SplitN(wk, "|", 2)
		if len(parts) != 2 {
			continue
		}
		lk, ns := parts[0], parts[1]
		nsSet, ok := desiredNS[lk]
		if !ok || !nsSet[ns] {
			wt.cancel()
			delete(e.watches, wk)
		}
	}
	e.mu.Unlock()

	// 3) 启动缺失的链路（startLink 内部幂等：已存在的不会重复建）。
	for _, l := range desiredLinks {
		e.mu.Lock()
		_, running := e.stops[linkKey(l)]
		e.mu.Unlock()
		if !running {
			e.startLink(e.parentCtx, l)
		}
	}
}

func (e *Engine) watchNamespace(ctx context.Context, link config.SyncLink, namespace string) error {
	// 建连/枚举/注册监听是 setup 阶段，可能因源集群瞬时不可达失败；此处加重试（指数退避，上限若干次），
	// 让瞬时故障自愈，而非「首次失败即永久丢弃该命名空间」（最终兜底由 engine.patrol 重建）。
	const maxRetry = 5
	var lastErr error
	for attempt := 0; attempt < maxRetry; attempt++ {
		src, tgt, items, err := e.setupWatch(ctx, link, namespace)
		if err != nil {
			lastErr = err
			e.logger.Warn("config watch namespace setup failed, retrying",
				zap.String("link", linkKey(link)), zap.String("ns", namespace),
				zap.Int("attempt", attempt+1), zap.Error(err))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 2 * time.Second):
			}
			continue
		}
		// setup 成功后持续巡检命名空间配置集合。Nacos ListenConfig 是按
		// DataId/Group 注册的，不会收到“新建了另一个 DataId”的通知；
		// 因此必须重新枚举，才能发现运行期间新增的配置项。
		watched := make(map[string]dataRef, len(items))
		for _, it := range items {
			watched[dataRefKey(it)] = it
		}
		e.monitorNamespaceWatches(ctx, link, namespace, src, tgt, watched)
		for _, it := range watched {
			_ = src.CancelListenConfig(vo.ConfigParam{DataId: it.DataId, Group: it.Group})
		}
		return nil
	}
	if lastErr != nil {
		metrics.SyncErrors.WithLabelValues(linkKey(link), "config").Inc()
	}
	return lastErr
}

// monitorNamespaceWatches 定期重新枚举命名空间，动态增删具体 DataId/Group 监听。
func (e *Engine) monitorNamespaceWatches(ctx context.Context, link config.SyncLink, namespace string, src, tgt config_client.IConfigClient, watched map[string]dataRef) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		items, err := e.enumerate(ctx, src)
		if err != nil {
			e.logger.Warn("config namespace rescan failed; existing listeners retained",
				zap.String("link", linkKey(link)), zap.String("ns", namespace), zap.Error(err))
			continue
		}
		desired := make(map[string]dataRef, len(items))
		for _, it := range items {
			desired[dataRefKey(it)] = it
		}
		for key, it := range watched {
			if _, ok := desired[key]; ok {
				continue
			}
			// SearchConfig no longer lists this item. Confirm with GetConfig before
			// treating it as deleted, then explicitly fire the target-side delete
			// before removing the listener. Otherwise a delayed source delete event
			// could be lost if this rescan cancels the listener first.
			content, getErr := src.GetConfig(vo.ConfigParam{DataId: it.DataId, Group: it.Group})
			if getErr != nil {
				e.logger.Warn("verify removed config failed; listener retained for retry",
					zap.String("link", linkKey(link)), zap.String("ns", namespace),
					zap.String("group", it.Group), zap.String("dataId", it.DataId), zap.Error(getErr))
				continue
			}
			if content != "" {
				continue // search pagination/race; source still has the config
			}
			if err := e.forward(ctx, link, namespace, it.Group, it.DataId, "", src, tgt); err != nil {
				e.logger.Warn("propagate removed config failed; listener retained for retry",
					zap.String("link", linkKey(link)), zap.String("ns", namespace),
					zap.String("group", it.Group), zap.String("dataId", it.DataId), zap.Error(err))
				continue
			}
			if err := src.CancelListenConfig(vo.ConfigParam{DataId: it.DataId, Group: it.Group}); err != nil {
				e.logger.Warn("cancel removed config listener failed",
					zap.String("link", linkKey(link)), zap.String("ns", namespace),
					zap.String("group", it.Group), zap.String("dataId", it.DataId), zap.Error(err))
			}
			delete(watched, key)
		}
		for key, it := range desired {
			if _, ok := watched[key]; ok {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if err := src.ListenConfig(vo.ConfigParam{
				DataId: it.DataId,
				Group: it.Group,
				OnChange: func(ns, group, dataId, data string) {
					e.onChange(ctx, link, namespace, group, dataId, data, src, tgt)
				},
			}); err != nil {
				e.logger.Warn("listen newly discovered config failed; will retry on next scan",
					zap.String("link", linkKey(link)), zap.String("ns", namespace),
					zap.String("group", it.Group), zap.String("dataId", it.DataId), zap.Error(err))
				continue
			}
			// Listener is installed before the initial publish so changes during
			// propagation are not silently missed. If propagation fails, remove
			// the listener and retry both steps on the next scan.
			if err := e.initialPropagate(ctx, link, namespace, it, src, tgt); err != nil {
				_ = src.CancelListenConfig(vo.ConfigParam{DataId: it.DataId, Group: it.Group})
				e.logger.Warn("initial propagate for newly discovered config failed; will retry",
					zap.String("link", linkKey(link)), zap.String("ns", namespace),
					zap.String("group", it.Group), zap.String("dataId", it.DataId), zap.Error(err))
				continue
			}
			watched[key] = it
		}
	}
}

func dataRefKey(it dataRef) string { return it.Group + "\x00" + it.DataId }

// setupWatch 完成单次监听建立（建连 + 枚举 + 初始点火 + 注册监听）。失败时返回错误，由 watchNamespace 重试。
// 成功时回传源客户端与已注册监听的配置项，供调用方在取消时逐一撤销监听（释放 SDK 回调）。
func (e *Engine) setupWatch(ctx context.Context, link config.SyncLink, namespace string) (config_client.IConfigClient, config_client.IConfigClient, []dataRef, error) {
	src, err := e.mgr.ConfigClient(link.From, namespace)
	if err != nil {
		return nil, nil, nil, err
	}
	tgt, err := e.mgr.ConfigClient(link.To, namespace)
	if err != nil {
		return nil, nil, nil, err
	}

	items, err := e.enumerate(ctx, src)
	if err != nil {
		return nil, nil, nil, err
	}

	// 初始点火：把源集群当前配置推一次到目标，确保既有配置的客户端也被通知。
	// 冷却按 (link, ns) 批次判定（整批跳过或整批执行），绝不能在每个 dataId 内单独判冷却：
	// 曾用 link|ns 作为逐项冷却 key，导致并发批次里第一项写入时间后其余全部被冷却跳过，
	// 每条链路实际只点火一个配置（P0）。
	e.igniteMu.Lock()
	igniteKey := linkKey(link) + "|" + namespace
	ignited := false
	if t, ok := e.lastIgnite[igniteKey]; ok && time.Since(t) < igniteCooldown {
		ignited = false // 冷却期内重建（patrol/重试）：跳过整批评点火，防重发布风暴
	} else {
		e.lastIgnite[igniteKey] = time.Now()
		ignited = true
	}
	e.igniteMu.Unlock()

	if ignited {
		var igniteFailed atomic.Bool
		var wg sync.WaitGroup
		sem := make(chan struct{}, 16)
		for _, it := range items {
			it := it
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if err := e.initialPropagate(ctx, link, namespace, it, src, tgt); err != nil {
					igniteFailed.Store(true)
				}
			}()
		}
		wg.Wait()
		// 点火存在失败项：清除冷却标记，让 patrol 重建时能整批重试，而不是等 5 分钟。
		if igniteFailed.Load() {
			e.igniteMu.Lock()
			delete(e.lastIgnite, igniteKey)
			e.igniteMu.Unlock()
		}
	}

	// 注册监听，捕获后续变更。任一 dataId 注册失败即视为整体失败：回滚已注册监听并返回错误，
	// 由 watchNamespace 重建重试——否则该 dataId 永远无监听，且 patrol 只看 goroutine 存活
	// 会误判健康（P1「监听注册失败假健康」）。
	for _, it := range items {
		it := it
		if err := src.ListenConfig(vo.ConfigParam{
			DataId: it.DataId,
			Group:  it.Group,
			OnChange: func(ns, group, dataId, data string) {
				e.onChange(ctx, link, namespace, group, dataId, data, src, tgt)
			},
		}); err != nil {
			// 回滚本批已注册成功的监听，防止重试时同一 dataId 挂双份回调。
			for _, prev := range items {
				_ = src.CancelListenConfig(vo.ConfigParam{DataId: prev.DataId, Group: prev.Group})
			}
			e.logger.Error("listen config failed; rolled back partial listens",
				zap.String("link", linkKey(link)), zap.String("ns", namespace),
				zap.String("dataId", it.DataId), zap.Error(err))
			return nil, nil, nil, fmt.Errorf("listen config %s: %w", it.DataId, err)
		}
	}
	return src, tgt, items, nil
}

// initialPropagate 推送单个配置到目标以点燃其本地通知。冷却判定已上移到 setupWatch 批次级。
// 返回错误供批次级失败汇总（失败时清除冷却标记，重建即整批重试）。
func (e *Engine) initialPropagate(ctx context.Context, link config.SyncLink, ns string, it dataRef, src, tgt config_client.IConfigClient) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	content, err := src.GetConfig(vo.ConfigParam{DataId: it.DataId, Group: it.Group})
	if err != nil {
		e.logger.Warn("initial propagate get config failed",
			zap.String("link", linkKey(link)), zap.String("dataId", it.DataId), zap.Error(err))
		return err
	}
	if content == "" {
		return nil
	}
	// 已运行在受限并发 goroutine（setupWatch 的信号量）里，直接同步发布：
	// 发布结果即时可知（失败即返回），无需再经协程池转手丢失结果。
	if err := e.forward(ctx, link, ns, it.Group, it.DataId, content, src, tgt); err != nil {
		return err
	}
	e.logger.Info("initial propagate fired (source content pushed to target)",
		zap.String("link", linkKey(link)), zap.String("ns", ns),
		zap.String("group", it.Group), zap.String("dataId", it.DataId),
		zap.Int("content_len", len(content)))
	return nil
}

func (e *Engine) onChange(ctx context.Context, link config.SyncLink, ns, group, dataId, data string, src, tgt config_client.IConfigClient) {
	metrics.SyncEvents.WithLabelValues(linkKey(link), "config").Inc()
	// 可观测性：配置变更事件已被本引擎捕获（无论是否回声）。这是排查「源端改了配置但目标端无变化」的
	// 第一现场——若此处无日志，说明源端监听根本没收到变更事件（监听未建立/连接断开）。
	if e.isEcho(link.From, link.To, ns, group, dataId, data) {
		e.logger.Debug("config change event is our own echo, skip",
			zap.String("link", linkKey(link)), zap.String("ns", ns),
			zap.String("group", group), zap.String("dataId", dataId))
		return // 我们自己的回声，跳过
	}
	e.logger.Info("config change event received, forwarding to target",
		zap.String("link", linkKey(link)), zap.String("ns", ns),
		zap.String("group", group), zap.String("dataId", dataId),
		zap.Int("content_len", len(data)))
	if err := e.pl.Submit(ctx, func(ctx context.Context) {
		if err := e.forward(ctx, link, ns, group, dataId, data, src, tgt); err != nil {
			e.logger.Error("forward config to target failed after retries",
				zap.String("link", linkKey(link)), zap.String("ns", ns),
				zap.String("group", group), zap.String("dataId", dataId), zap.Error(err))
			metrics.SyncErrors.WithLabelValues(linkKey(link), "config").Inc()
		}
	}); err != nil {
		e.logger.Error("forward config submit failed",
			zap.String("link", linkKey(link)), zap.String("dataId", dataId), zap.Error(err))
		metrics.SyncErrors.WithLabelValues(linkKey(link), "config").Inc()
	}
}

// forward 真正向目标发布（或删除）并记入回声表。
// 失败时按指数退避重试（有界次数），且每次重试前**重新拉取源端最新内容**：
// 集群共享同一 config DB，用事件时刻的旧内容重试可能把已被再次修改的 DB 值改回旧值。
// 成功后 recordPush 内容以实际发布内容为准。返回最终错误供调用方聚合判断（此前静默失败
// 会造成目标端长期不通知且无人知晓，P1「发布失败无重试」）。
func (e *Engine) forward(ctx context.Context, link config.SyncLink, ns, group, dataId, data string, src, tgt config_client.IConfigClient) error {
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second): // 1s、2s 退避
			}
		}
		// Listener callbacks are submitted to a concurrent worker pool, so an older
		// event can execute after a newer event for the same DataId/Group. Always
		// re-read the authoritative source immediately before deciding what to write,
		// including on the first attempt; never trust the callback payload as current.
		fresh, err := src.GetConfig(vo.ConfigParam{DataId: dataId, Group: group})
		if err != nil {
			lastErr = fmt.Errorf("refresh source config before publish: %w", err)
			e.logger.Warn("refresh source config before publish failed",
				zap.String("link", linkKey(link)), zap.String("dataId", dataId),
				zap.Int("attempt", attempt+1), zap.Error(err))
			continue
		}
		data = fresh

		// Active-Active 节点各自维护本地 echo 表，其他节点无法识别本次写入来源。
		// 发布前比较目标当前值：若另一个同步节点已写入相同内容，则只登记本地 echo
		// 并跳过写操作，避免重复通知进一步触发跨节点回声。
		current, readErr := tgt.GetConfig(vo.ConfigParam{DataId: dataId, Group: group})
		if readErr == nil && current == data {
			e.recordPush(link.From, link.To, ns, group, dataId, data)
			e.logger.Debug("target config already matches source, skip duplicate publish",
				zap.String("link", linkKey(link)), zap.String("ns", ns),
				zap.String("group", group), zap.String("dataId", dataId))
			return nil
		}

		start := time.Now()
		if data == "" {
			ok, err := tgt.DeleteConfig(vo.ConfigParam{DataId: dataId, Group: group})
			if err == nil && !ok {
				err = fmt.Errorf("nacos rejected delete config")
			}
			if err != nil {
				lastErr = err
				e.logger.Warn("delete config on target failed",
					zap.String("link", linkKey(link)), zap.String("dataId", dataId),
					zap.Int("attempt", attempt+1), zap.Error(err))
				continue
			}
			e.logger.Info("config deleted on target",
				zap.String("link", linkKey(link)), zap.String("ns", ns),
				zap.String("group", group), zap.String("dataId", dataId))
			e.recordPush(link.From, link.To, ns, group, dataId, data)
			metrics.SyncLag.WithLabelValues(linkKey(link), "config").Observe(time.Since(start).Seconds())
			return nil
		}
		ok, err := tgt.PublishConfig(vo.ConfigParam{DataId: dataId, Group: group, Content: data})
		if err == nil && !ok {
			err = fmt.Errorf("nacos rejected publish config")
		}
		if err != nil {
			lastErr = err
			e.logger.Warn("publish config to target failed",
				zap.String("link", linkKey(link)), zap.String("dataId", dataId),
				zap.Int("attempt", attempt+1), zap.Error(err))
			continue
		}
		e.logger.Info("config published to target",
			zap.String("link", linkKey(link)), zap.String("ns", ns),
			zap.String("group", group), zap.String("dataId", dataId),
			zap.Int("content_len", len(data)))
		e.recordPush(link.From, link.To, ns, group, dataId, data)
		metrics.SyncLag.WithLabelValues(linkKey(link), "config").Observe(time.Since(start).Seconds())
		return nil
	}
	return lastErr
}

// ---------------------------------------------------------------------------
// 回声抑制
// ---------------------------------------------------------------------------

func pushKey(from, to, ns, group, dataId string) string {
	return from + "|" + to + "|" + ns + "|" + group + "|" + dataId
}

func (e *Engine) recordPush(from, to, ns, group, dataId, content string) {
	e.pushed.Store(pushKey(from, to, ns, group, dataId), pushRec{hash: hashContent(content), at: time.Now()})
}

// isEcho 判断该变更是否由 to 推回 from 的回声。仅比对内容哈希，不持有完整配置内容。
// 命中即删除记录（一次性消费）：否则 30s TTL 内内容改回相同值的真实变更会被误吞（P2），
// 且 SDK 重连等场景的重复通知也会被反复吞掉。删除后如再有同内容事件，会被当作真实变更转发；
// 转发发布相同内容本身幂等（共享 DB 值不变），只是多触发一次目标端通知，无风暴风险。
func (e *Engine) isEcho(src, dst, ns, group, dataId, content string) bool {
	key := pushKey(dst, src, ns, group, dataId)
	v, ok := e.pushed.Load(key)
	if !ok {
		return false
	}
	rec := v.(pushRec)
	if time.Since(rec.at) > echoTTL {
		e.pushed.Delete(key)
		return false
	}
	if rec.hash == hashContent(content) {
		e.pushed.Delete(key)
		return true
	}
	return false
}

// gcEcho 周期性清理回声表中超过 echoTTL 的残留条目。回声表本就在 isEcho 命中时删除，
// 但「单向推送（无回声）」的 key 永远不会被访问，必须靠本 GC 兜底，否则无界增长。
func (e *Engine) gcEcho(ctx context.Context) {
	t := time.NewTicker(echoTTL)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.pushed.Range(func(k, v any) bool {
				if rec, ok := v.(pushRec); !ok || time.Since(rec.at) > echoTTL {
					e.pushed.Delete(k)
				}
				return true
			})
		}
	}
}

// ---------------------------------------------------------------------------
// 配置枚举
// ---------------------------------------------------------------------------

type dataRef struct {
	DataId string
	Group  string
}

func (e *Engine) enumerate(ctx context.Context, src config_client.IConfigClient) ([]dataRef, error) {
	var out []dataRef
	const size = 100
	for page := 1; page <= 1000; page++ {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		res, err := src.SearchConfig(vo.SearchConfigParam{
			Search:   "accurate",
			DataId:   "",
			Group:    "",
			PageNo:   page,
			PageSize: size,
		})
		if err != nil {
			return out, err
		}
		for _, it := range res.PageItems {
			out = append(out, dataRef{DataId: it.DataId, Group: it.Group})
		}
		if len(res.PageItems) < size {
			break
		}
	}
	return out, nil
}

// Stop 取消所有链路并等待监听协程退出。
func (e *Engine) Stop() {
	e.mu.Lock()
	for _, cancel := range e.stops {
		cancel()
	}
	e.stops = make(map[string]context.CancelFunc)
	for _, t := range e.watches {
		t.cancel()
	}
	e.watches = make(map[string]*watchTask)
	e.linkCtxs = make(map[string]context.Context)
	e.mu.Unlock()
	e.wg.Wait()
}

func linkKey(link config.SyncLink) string {
	return link.From + "->" + link.To + "/" + link.Kind
}
