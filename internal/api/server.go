// Package api 提供 Hertz 控制面：nacos-group 集群目录的增删改查、连接测试、
// scope 热更新、配置重载、指标。
package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/auth"
	"github.com/cxcm/nacos-sync-backend/internal/config"
	"github.com/cxcm/nacos-sync-backend/internal/logging"
	"github.com/cxcm/nacos-sync-backend/internal/nacos"
	"github.com/cxcm/nacos-sync-backend/internal/sync/configsync"
	"github.com/cxcm/nacos-sync-backend/internal/sync/namingsync"
	"github.com/cxcm/nacos-sync-backend/internal/sync/watcher"
)

// DefaultHTTPPort 控制面端口（与管理约定一致，固定 9696；运行配置不再携带 server 段）。
// 导出供启动流程在注册到 Nacos 注册中心时使用，避免端口硬编码漂移。
const DefaultHTTPPort = 9696

// authTokenEnv 控制面共享令牌的环境变量名。设置后，所有非只读控制面接口要求
// `Authorization: Bearer <token>`；未设置则关闭鉴权（开发/内网默认）。
const authTokenEnv = "NACOS_SYNC_API_TOKEN"

// idPattern 资源 ID（grp_id / c_id / source 等）的合法格式：1..16 位数字或大小写字母。
// 畸形 ID 进路由/日志会造成歧义，统一在入口校验。
var idPattern = regexp.MustCompile(`^[0-9a-zA-Z]{1,16}$`)

// lookupMaxPerMin 单客户端（按来源 IP 粗粒度）对 /api/lookup 的每分钟上限，防止高频枚举放大对受管 Nacos 的探测压力。
const lookupMaxPerMin = 60

// nodeProbeTimeout 取数选节点时单次 TCP 预检超时。取值偏小：正常节点是毫秒级响应，
// 预检只是为了跳过已下线节点，不应让配置同步页整体变慢。
const nodeProbeTimeout = 1 * time.Second

// Server 控制面 HTTP 服务。
type Server struct {
	h *server.Hertz
}

// New 构造、设监听端口并注册路由（不立即监听）。
// 所有实例均运行完整控制面；多实例部署采用 Active-Active。
func New(mgr *config.Manager, clientMgr *nacos.ClientManager, cfgEngine *configsync.Engine, nameEngine *namingsync.Engine, wk *watcher.Manager, logger *zap.Logger) *Server {
	h := server.New(server.WithHostPorts(fmt.Sprintf(":%d", DefaultHTTPPort)))

	// 全链路 traceId：每个 HTTP 请求生成一个，写入 context 并绑定到当前 goroutine，
	// 请求结束后清空（defer cleanup），避免 goroutine 复用导致 traceId 串味。
	h.Use(func(c context.Context, ctx *app.RequestContext) {
		tr := logging.NewTrace(ctx.ClientIP())
		c = logging.WithTrace(c, tr)
		cleanup := logging.BindCurrent(tr)
		defer cleanup()
		ctx.Next(c)
	})
	token := os.Getenv(authTokenEnv)
	jwtSecret := mgr.JwtSecret()
	if token != "" {
		logger.Info("control-plane static token auth enabled (NACOS_SYNC_API_TOKEN set)", zap.Int("token_len", len(token)))
	} else {
		logger.Warn("control-plane static token (NACOS_SYNC_API_TOKEN) DISABLED")
	}
	if mgr.Get().RadiusEnabled() {
		logger.Info("control-plane RADIUS login enabled", zap.String("radius_host", mgr.Get().Auth.Radius.ServerHost))
		if jwtSecret == "" {
			logger.Error("RADIUS login enabled but JWT signing secret missing: set NACOS_SYNC_JWT_SECRET or bootstrap secret-key")
		}
	} else {
		logger.Warn("RADIUS login DISABLED: configure auth.radius in runtime config to enable")
	}
	// 登录凭据 RSA 加解密器：公钥经 /api/auth/rsa-public-key 暴露，私钥仅留服务端内存。
	// 配置 auth.rsa_private_key_pem 则用之（多实例须各实例一致）；否则临时生成（重启即变，仅单实例/开发用）。
	rsaPriv := ""
	if mgr.Get().Auth != nil {
		rsaPriv = mgr.Get().Auth.RSAPrivateKeyPEM
	}
	cipher, cerr := auth.NewCredCipher(rsaPriv)
	if cerr != nil {
		logger.Error("init RSA credential cipher failed, fallback to ephemeral key", zap.Error(cerr))
		cipher, _ = auth.NewCredCipher("")
	}
	if rsaPriv != "" {
		logger.Info("RSA credential cipher loaded from auth.rsa_private_key_pem (shared key for multi-instance)")
	} else {
		logger.Warn("RSA credential cipher is ephemeral (generated at startup); set auth.rsa_private_key_pem for multi-instance or stable keys")
	}
	// 限流仅作用于高成本的枚举探测接口 /api/lookup，避免放大对受管 Nacos 的探测压力。
	h.Use(lookupRateLimit())
	register(h, mgr, clientMgr, cfgEngine, nameEngine, wk, logger, token, jwtSecret, cipher)
	return &Server{h: h}
}

// lookupRateLimit 对 /api/lookup 做基于真实对端地址的简易令牌桶限流（每分钟 lookupMaxPerMin 次）。
// 仅拦截该路径，其余接口不受影响。限流状态保存在进程内，重启即清空（够用，非分布式场景）。
// buckets map 设上限并惰性清理过期桶，避免「每个新 IP 永久加一条」的无界内存增长。
func lookupRateLimit() app.HandlerFunc {
	type bucket struct {
		mu        sync.Mutex
		count     int
		windowEnd time.Time
	}
	const maxBuckets = 4096
	var (
		mu      sync.Mutex
		buckets = map[string]*bucket{}
	)
	// sweepExpired 惰性清理过期桶，仅在桶数量接近上限时执行，平摊代价可忽略。
	sweepExpired := func() {
		now := time.Now()
		for ip, b := range buckets {
			b.mu.Lock()
			expired := now.After(b.windowEnd)
			b.mu.Unlock()
			if expired {
				delete(buckets, ip)
			}
		}
		if len(buckets) > maxBuckets {
			buckets = make(map[string]*bucket)
		}
	}
	return func(c context.Context, ctx *app.RequestContext) {
		if string(ctx.Request.URI().Path()) != "/api/lookup" {
			ctx.Next(c)
			return
		}
		ip := remoteHost(ctx)
		mu.Lock()
		b, ok := buckets[ip]
		if ok {
			if len(buckets) > maxBuckets {
				sweepExpired()
			}
		} else {
			if len(buckets) >= maxBuckets {
				sweepExpired()
			}
			b = &bucket{}
			buckets[ip] = b
		}
		mu.Unlock()
		b.mu.Lock()
		now := time.Now()
		if now.After(b.windowEnd) {
			b.count = 0
			b.windowEnd = now.Add(time.Minute)
		}
		b.count++
		allowed := b.count <= lookupMaxPerMin
		b.mu.Unlock()
		if !allowed {
			ctx.JSON(consts.StatusTooManyRequests, utils.H{"error": "rate limit exceeded for /api/lookup, retry later"})
			ctx.Abort()
			return
		}
		ctx.Next(c)
	}
}

// remoteHost 取请求的真实 TCP 对端主机地址（绕过 X-Forwarded-For 等可伪造头），
// 用于限流基数，避免攻击者伪造 XFF 绕过 /api/lookup 限流。无对端地址时回退到 ClientIP。
func remoteHost(ctx *app.RequestContext) string {
	if addr := ctx.RemoteAddr(); addr != nil {
		if host, _, err := net.SplitHostPort(addr.String()); err == nil {
			return host
		}
		return addr.String()
	}
	return ctx.ClientIP()
}

// jsonErrInternal 用于 500 级内部错误：仅回显通用文案，具体错误记录日志，
// 避免把 Nacos SDK / DB / 加解密内部细节泄露给调用方。调用前请先用 logger.Error 落盘详情。
func jsonErrInternal(ctx *app.RequestContext, msg string) {
	ctx.JSON(consts.StatusInternalServerError, utils.H{"error": msg})
}

// Run 启动监听（非阻塞，内部 Spin 在独立 goroutine）。
func (s *Server) Run(ctx context.Context) error {
	go s.h.Spin()
	return nil
}

// Close 优雅关闭（若存在）。
func (s *Server) Close() {
	if s.h != nil {
		_ = s.h.Close()
	}
}

func jsonErr(ctx *app.RequestContext, status int, msg string) {
	ctx.JSON(status, utils.H{"error": msg})
}

// ---- nacos-group 脱敏视图 ----

func maskGroup(g config.NacosGroup) utils.H {
	pw := ""
	if g.EnableAuth {
		pw = "***"
	}
	dsn := ""
	if g.DBDSN != "" {
		dsn = "***" // DSN 含数据库口令，绝不回显明文
	}
	clusters := make([]utils.H, 0, len(g.Clusters))
	for _, cl := range g.Clusters {
		clusters = append(clusters, utils.H{
			"c_id":    cl.CID,
			"c_name":  cl.CName,
			"c_nodes": cl.CNodes,
		})
	}
	return utils.H{
		"grp_id":        g.GrpID,
		"grp_name":      g.GrpName,
		"grp_version":   g.GrpVersion,
		"enable_auth":   g.EnableAuth,
		"auth_username": g.AuthUsername,
		"auth_password": pw,
		"grp_dsn":       dsn,
		"cluster":       clusters,
	}
}

func register(h *server.Hertz, m *config.Manager, clientMgr *nacos.ClientManager, _ *configsync.Engine, nameEngine *namingsync.Engine, wk *watcher.Manager, logger *zap.Logger, token, jwtSecret string, cipher *auth.CredCipher) {
	// 控制面鉴权：登录 JWT（Authorization: Bearer <jwt>）或静态共享令牌（NACOS_SYNC_API_TOKEN）均可通过；
	// 存活/指标/登录相关接口（/api/auth/*）免鉴权；auth.enable=false 时全部放行。详见 JWTAuthMiddleware。
	h.Use(JWTAuthMiddleware(m, jwtSecret, token))

	// 存活/就绪探测：深检关键依赖（配置中心 Nacos）。K8S 探针匹配 body 中的 "UP" 判定健康，
	// 故健康返回 status:"UP"（200），依赖不可达返回 status:"DOWN"（503），
	// 使编排系统据真实依赖状态摘除/重启。
	h.GET("/api/health", func(c context.Context, ctx *app.RequestContext) {
		ok, detail := m.Health(c)
		if ok {
			ctx.JSON(consts.StatusOK, utils.H{"status": "UP", "deps": utils.H{"config_center": "UP"}})
			return
		}
		ctx.JSON(consts.StatusServiceUnavailable, utils.H{"status": "DOWN", "deps": utils.H{"config_center": detail}})
	})

	// 鉴权状态：前端据此判断是否需要在请求头附带 Bearer（RADIUS 未配置时回退静态令牌）。
	h.GET("/api/auth/status", AuthStatusHandler(m, token))

	// 登录 / 登出 / 公钥（控制面 RADIUS 登录）。公钥接口供前端加密凭据，免鉴权。
	h.POST("/api/auth/login", LoginHandler(m, logger, cipher))
	h.POST("/api/auth/logout", LogoutHandler())
	h.GET("/api/auth/rsa-public-key", RSAPublicKeyHandler(cipher))

	// 配置同步轮询任务状态（排查用）：每个开启配置同步的 3.X 分组一个轮询。
	h.GET("/api/pollers", func(c context.Context, ctx *app.RequestContext) {
		ctx.JSON(consts.StatusOK, utils.H{"pollers": wk.Status()})
	})

	// ---------- nacos-group 集群目录 ----------

	h.GET("/api/groups", func(c context.Context, ctx *app.RequestContext) {
		groups := m.Get().NacosGroup
		out := make([]utils.H, 0, len(groups))
		for _, g := range groups {
			out = append(out, maskGroup(g))
		}
		ctx.JSON(consts.StatusOK, utils.H{"groups": out})
	})

	// 新建或更新组（grp_id 可空，自动生成）。
	h.POST("/api/groups", func(c context.Context, ctx *app.RequestContext) {
		var g config.NacosGroup
		if err := ctx.Bind(&g); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		saved, err := m.UpsertGroup(g)
		if err != nil {
			jsonErr(ctx, consts.StatusBadRequest, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true, "group": maskGroup(*saved)})
	})

	// 删除组（含其下集群）。
	h.DELETE("/api/groups/:grpId", func(c context.Context, ctx *app.RequestContext) {
		grpID := ctx.Param("grpId")
		if !validID(grpID) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid grp_id format")
			return
		}
		if err := m.DeleteGroup(grpID); err != nil {
			jsonErr(ctx, consts.StatusNotFound, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 在某组下新建或更新集群（c_id 可空，自动生成）。
	h.POST("/api/groups/:grpId/clusters", func(c context.Context, ctx *app.RequestContext) {
		grpID := ctx.Param("grpId")
		if !validID(grpID) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid grp_id format")
			return
		}
		var cl config.NacosCluster
		if err := ctx.Bind(&cl); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		saved, err := m.UpsertCluster(grpID, cl)
		if err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true, "cluster": saved})
	})

	// 删除某组下的集群。
	h.DELETE("/api/groups/:grpId/clusters/:cId", func(c context.Context, ctx *app.RequestContext) {
		grpID := ctx.Param("grpId")
		cID := ctx.Param("cId")
		if !validID(grpID) || !validID(cID) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid grp_id/c_id format")
			return
		}
		if err := m.DeleteCluster(grpID, cID); err != nil {
			jsonErr(ctx, consts.StatusNotFound, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 连接测试：对组内每个集群做一次连通性 + 鉴权探测。
	h.POST("/api/groups/:grpId/test", func(c context.Context, ctx *app.RequestContext) {
		grpID := ctx.Param("grpId")
		if !validID(grpID) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid grp_id format")
			return
		}
		g, ok := m.GetGroup(grpID)
		if !ok {
			jsonErr(ctx, consts.StatusNotFound, "group "+grpID+" not found")
			return
		}
		results := make([]utils.H, 0, len(g.Clusters))
		for _, cl := range g.Clusters {
			for _, node := range cl.CNodes {
				err := nacos.TestConnection(nacos.TestConnectionSpec{
					Address:    node,
					EnableAuth: g.EnableAuth,
					Username:   g.AuthUsername,
					Password:   g.AuthPassword,
				})
				res := utils.H{"c_id": cl.CID, "c_name": cl.CName, "node": node}
				if err != nil {
					res["reachable"] = false
					res["error"] = err.Error()
				} else {
					res["reachable"] = true
				}
				results = append(results, res)
			}
		}
		ctx.JSON(consts.StatusOK, utils.H{"grp_id": grpID, "results": results})
	})

	// ---------- 配置同步页概览 ----------
	// 返回每个分组的配置概览：选定节点（分组内随机一个可用节点）
	// + 该节点命名空间列表及每个命名空间的配置文件数量。
	// 该取数走目标 Nacos 的 HTTP OpenAPI，对 Nacos 3.x（已移除该接口）会返回明确错误，
	// 由前端在对应卡片上提示，不影响其它分组卡片渲染。
	h.GET("/api/config-sync", func(c context.Context, ctx *app.RequestContext) {
		// 整体超时：分组并发取数，但单个分组探测/拉取仍可能卡住，加 30s 上限避免请求挂死。
		qctx, cancel := context.WithTimeout(c, 30*time.Second)
		defer cancel()
		res := configSyncOverview(qctx, m)
		// 附带各分组的配置同步轮询状态（watcher）：其中包含 total_md5（DB 指纹，字段为 last_digest）、
		// 健康状态、最近成功时间、节点数。前端据此在配置同步页展示「当前记录的 total_md5」。
		ctx.JSON(consts.StatusOK, utils.H{"groups": res, "pollers": wk.Status()})
	})

	// 设置某分组「配置同步」开关（持久化到运行时配置的 config-sync 段）。
	h.POST("/api/groups/:grpId/config-sync", func(c context.Context, ctx *app.RequestContext) {
		grpID := ctx.Param("grpId")
		if !validID(grpID) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid grp_id format")
			return
		}
		var req struct {
			Enabled bool `json:"enabled"`
		}
		if err := ctx.Bind(&req); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		if err := m.SetConfigSync(grpID, req.Enabled); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true, "grp_id": grpID, "sync_enabled": req.Enabled})
	})

	// ---------- 旧拓扑视图（保留，待从 nacos-group 派生同步链路时启用） ----------

	h.GET("/api/clusters", func(c context.Context, ctx *app.RequestContext) {
		cfg := m.Get()
		out := make([]utils.H, 0, len(cfg.Clusters))
		for _, cl := range cfg.Clusters {
			pw := ""
			if cl.Auth.Enabled {
				pw = "***"
			}
			out = append(out, utils.H{
				"id":        cl.ID,
				"group":     cl.Group,
				"addresses": cl.Addresses,
				"version":   cl.Version,
				"auth":      cl.Auth.Enabled,
				"username":  cl.Auth.Username,
				"password":  pw,
			})
		}
		ctx.JSON(consts.StatusOK, utils.H{"clusters": out})
	})

	h.GET("/api/links", func(c context.Context, ctx *app.RequestContext) {
		cfg := m.Get()
		out := make([]utils.H, 0, len(cfg.Links))
		for _, l := range cfg.Links {
			out = append(out, utils.H{
				"from":    l.From,
				"to":      l.To,
				"kind":    l.Kind,
				"enabled": l.Enabled,
				"scope":   l.Scope,
			})
		}
		ctx.JSON(consts.StatusOK, utils.H{"links": out})
	})

	h.GET("/api/scopes", func(c context.Context, ctx *app.RequestContext) {
		ctx.JSON(consts.StatusOK, utils.H{"scopes": m.Get().Scopes})
	})

	// scope 热更新：POST /api/scopes  body: {name, include, exclude}
	// 写入配置中心 Nacos 后，监听回调会热替换各节点生效配置。
	h.POST("/api/scopes", func(c context.Context, ctx *app.RequestContext) {
		var req struct {
			Name    string               `json:"name"`
			Include config.IncludeFilter `json:"include"`
			Exclude config.ScopeFilter   `json:"exclude"`
		}
		if err := ctx.Bind(&req); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, err.Error())
			return
		}
		if req.Name == "" {
			jsonErr(ctx, consts.StatusBadRequest, "name required")
			return
		}
		if err := m.UpdateScope(req.Name, config.SyncScope{Include: req.Include, Exclude: req.Exclude}); err != nil {
			logger.Error("update scope", zap.String("name", req.Name), zap.Error(err))
			jsonErrInternal(ctx, "update scope failed, see server logs")
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true, "name": req.Name})
	})

	// 配置重载：从配置中心 Nacos 主动重新拉取并原子替换生效配置。
	h.POST("/api/reload", func(c context.Context, ctx *app.RequestContext) {
		if err := m.Reload(); err != nil {
			logger.Error("reload config", zap.Error(err))
			jsonErrInternal(ctx, "reload failed, see server logs")
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 全量重加密：把当前生效配置（解密后明文）重新加密写回配置中心，
	// 确保 nacos-sync-backend-runtime.yaml 上 grp_dsn / auth_password 最终都以 ENC() 包裹。
	// 写回前会先 Reload 一次拿到最新版本，避免覆盖并发修改。
	h.POST("/api/reencrypt", func(c context.Context, ctx *app.RequestContext) {
		if err := m.Reencrypt(); err != nil {
			logger.Error("reencrypt config", zap.Error(err))
			jsonErrInternal(ctx, "reencrypt failed, see server logs")
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 预览即将写回配置中心的加密后 YAML：敏感字段（auth_password、grp_dsn）均为 ENC() 形态，
	// 不含明文口令，可安全查看，用于确认落盘形态。
	h.GET("/api/config-preview", func(c context.Context, ctx *app.RequestContext) {
		s, err := m.PreviewEncrypted()
		if err != nil {
			logger.Error("preview encrypted config", zap.Error(err))
			jsonErrInternal(ctx, "preview failed, see server logs")
			return
		}
		ctx.SetContentType("text/yaml; charset=utf-8")
		ctx.Response.SetBody([]byte(s))
	})

	// ---------- 服务同步（naming） ----------
	// 返回当前服务同步配置：single（组内同步，key=grp_id）与 multiple（组外同步，无序对唯一）。
	h.GET("/api/service-sync", func(c context.Context, ctx *app.RequestContext) {
		view := serviceSyncView(m)
		ctx.JSON(consts.StatusOK, view)
	})

	// 命名同步（代注册）实时状态：每个同步方向的源/目标、命名空间、白名单、是否运行、最近成功时间、最近错误。
	// 用于逐个排查代注册是否生效、是否有集群连不上等。
	h.GET("/api/naming-sync", func(c context.Context, ctx *app.RequestContext) {
		ctx.JSON(consts.StatusOK, utils.H{"directions": nameEngine.Status()})
	})

	// ---------- 按 IP 查找服务名 ----------
	// 通过 IP（或 IP+端口）在所有分组下查找匹配的服务名：每个分组随机挑一个集群、集群内随机一个可达节点取数，
	// 枚举其命名空间与服务，实时查询实例后按 IP（及可选端口）过滤，汇总返回所有匹配的服务名及其定位信息。
	// 入参：ip（必填）、port（可选，仅 IP 匹配时留空）。
	h.GET("/api/lookup", func(c context.Context, ctx *app.RequestContext) {
		ip := strings.TrimSpace(ctx.Query("ip"))
		if ip == "" {
			jsonErr(ctx, consts.StatusBadRequest, "ip required")
			return
		}
		var port *uint64
		if ps := strings.TrimSpace(ctx.Query("port")); ps != "" {
			// 严格解析：拒绝 "12.5"（Sscanf 会截成 12 误接受）、负数、0、超范围。
			p, perr := strconv.ParseUint(ps, 10, 64)
			if perr != nil || p == 0 || p > 65535 {
				jsonErr(ctx, consts.StatusBadRequest, "port must be an integer between 1 and 65535")
				return
			}
			port = &p
		}
		// 整体超时：留 10s 余量给前端 60s 超时，确保后端 goroutine 可被取消、结果不浪费。
		qctx, cancel := context.WithTimeout(c, 50*time.Second)
		defer cancel()
		res, err := clientMgr.LookupServicesByIP(qctx, ip, port)
		if err != nil {
			jsonErr(ctx, consts.StatusBadRequest, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{
			"ip":     res.IP,
			"port":   res.Port,
			"hits":   res.Hits,
			"errors": res.Errors,
		})
	})

	// 新增组内同步：body {grp_id, enable, services, namespace}；该分组若已存在 single-sync 则报错。
	h.POST("/api/service-sync/single", func(c context.Context, ctx *app.RequestContext) {
		var req struct {
			GrpID     string   `json:"grp_id"`
			Enable    bool     `json:"enable"`
			Services  []string `json:"services"`
			Namespace string   `json:"namespace"`
		}
		if err := ctx.Bind(&req); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		if err := m.AddSingleSync(req.GrpID, req.Enable, req.Services, req.Namespace); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 更新组内同步：body {grp_id, enable, services, namespace}。
	h.PUT("/api/service-sync/single", func(c context.Context, ctx *app.RequestContext) {
		var req struct {
			GrpID     string   `json:"grp_id"`
			Enable    bool     `json:"enable"`
			Services  []string `json:"services"`
			Namespace string   `json:"namespace"`
		}
		if err := ctx.Bind(&req); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		if err := m.UpdateSingleSync(req.GrpID, req.Enable, req.Services, req.Namespace); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 删除组内同步：/api/service-sync/single/:grpId
	h.DELETE("/api/service-sync/single/:grpId", func(c context.Context, ctx *app.RequestContext) {
		grpID := ctx.Param("grpId")
		if !validID(grpID) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid grp_id format")
			return
		}
		if err := m.DeleteSingleSync(grpID); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 新增组外同步：body {source_a, source_b, enable, services, namespace}；
	// 两分组不可相同，且与已有条目按无序对去重（A↔B 与 B↔A 视为同一条）。
	h.POST("/api/service-sync/multiple", func(c context.Context, ctx *app.RequestContext) {
		var req struct {
			SourceA   string   `json:"source_a"`
			SourceB   string   `json:"source_b"`
			Enable    bool     `json:"enable"`
			Services  []string `json:"services"`
			Namespace string   `json:"namespace"`
		}
		if err := ctx.Bind(&req); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		if err := m.AddMultipleSync(req.SourceA, req.SourceB, req.Enable, req.Services, req.Namespace); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 更新组外同步：body {source_a, source_b, enable, services, namespace}（按无序对定位）。
	h.PUT("/api/service-sync/multiple", func(c context.Context, ctx *app.RequestContext) {
		var req struct {
			SourceA   string   `json:"source_a"`
			SourceB   string   `json:"source_b"`
			Enable    bool     `json:"enable"`
			Services  []string `json:"services"`
			Namespace string   `json:"namespace"`
		}
		if err := ctx.Bind(&req); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		if err := m.UpdateMultipleSync(req.SourceA, req.SourceB, req.Enable, req.Services, req.Namespace); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 删除组外同步：?sourceA=X&sourceB=Y（按无序对定位）。
	h.DELETE("/api/service-sync/multiple", func(c context.Context, ctx *app.RequestContext) {
		a := ctx.Query("sourceA")
		b := ctx.Query("sourceB")
		if !validID(a) || !validID(b) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid source_a/source_b format")
			return
		}
		if err := m.DeleteMultipleSync(a, b); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// ---------- dubbo 服务同步（两集群按 providers:/consumers: 前缀动态全量代注册） ----------

	// 列表：当前 dubbo-sync 条目（前端列表展示）。
	h.GET("/api/dubbo-sync", func(c context.Context, ctx *app.RequestContext) {
		d := m.Get().DubboSync
		entries := []config.DubboSyncEntry{}
		if d != nil {
			entries = append(entries, d.Entries...)
		}
		ctx.JSON(consts.StatusOK, utils.H{"entries": entries})
	})

	// 新增：body {cluster_a, cluster_b, namespace, enable}；两集群不可相同、须存在，
	// 且与已有条目按（无序集群对, 命名空间）去重（A↔B 与 B↔A 视为同一条）。
	h.POST("/api/dubbo-sync", func(c context.Context, ctx *app.RequestContext) {
		var req struct {
			ClusterA  string `json:"cluster_a"`
			ClusterB  string `json:"cluster_b"`
			Namespace string `json:"namespace"`
			Enable    bool   `json:"enable"`
		}
		if err := ctx.Bind(&req); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		if !validID(req.ClusterA) || !validID(req.ClusterB) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid cluster_a/cluster_b format")
			return
		}
		if err := m.AddDubboSync(req.ClusterA, req.ClusterB, req.Namespace, req.Enable); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 更新：body {cluster_a, cluster_b, namespace, enable}（按无序集群对 + 命名空间定位）。
	h.PUT("/api/dubbo-sync", func(c context.Context, ctx *app.RequestContext) {
		var req struct {
			ClusterA  string `json:"cluster_a"`
			ClusterB  string `json:"cluster_b"`
			Namespace string `json:"namespace"`
			Enable    bool   `json:"enable"`
		}
		if err := ctx.Bind(&req); err != nil {
			jsonErr(ctx, consts.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
		if !validID(req.ClusterA) || !validID(req.ClusterB) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid cluster_a/cluster_b format")
			return
		}
		if err := m.UpdateDubboSync(req.ClusterA, req.ClusterB, req.Namespace, req.Enable); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	// 删除：?clusterA=X&clusterB=Y&namespace=Z（按无序集群对 + 命名空间定位）。
	h.DELETE("/api/dubbo-sync", func(c context.Context, ctx *app.RequestContext) {
		a := ctx.Query("clusterA")
		b := ctx.Query("clusterB")
		ns := ctx.Query("namespace")
		if !validID(a) || !validID(b) {
			jsonErr(ctx, consts.StatusBadRequest, "invalid clusterA/clusterB format")
			return
		}
		if err := m.DeleteDubboSync(a, b, ns); err != nil {
			status := consts.StatusBadRequest
			if containsNotFound(err) {
				status = consts.StatusNotFound
			}
			jsonErr(ctx, status, err.Error())
			return
		}
		ctx.JSON(consts.StatusOK, utils.H{"ok": true})
	})

	h.GET("/api/metrics", func(c context.Context, ctx *app.RequestContext) {
		mfs, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			ctx.String(consts.StatusInternalServerError, "gather metrics: %v", err)
			return
		}
		var buf bytes.Buffer
		enc := expfmt.NewEncoder(&buf, expfmt.FmtText)
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				ctx.String(consts.StatusInternalServerError, "encode metrics: %v", err)
				return
			}
		}
		ctx.SetContentType("text/plain; version=0.0.4")
		ctx.Response.SetBody(buf.Bytes())
	})
}

// containsNotFound 判断错误是否为「未找到」类（基于 config.ErrNotFound 哨兵），用于选择 404 还是 400。
// 相比原先的字符串后缀匹配，哨兵判定不受错误措辞/中文化影响，更稳健。
func containsNotFound(err error) bool {
	return errors.Is(err, config.ErrNotFound)
}

// validID 校验资源 ID（grp_id / c_id / source 等）格式：1..16 位数字或大小写字母。
func validID(s string) bool {
	return idPattern.MatchString(s)
}

// serviceSyncView 把当前服务同步配置序列化为前端视图：
// single 为组内同步列表（grp_id 已用于前端 JOIN 分组名）；
// multiple 为组外同步列表（source_a/source_b 无序对，展示时前端渲染为 A ↔ B）。
func serviceSyncView(m *config.Manager) utils.H {
	c := m.Get()
	ss := c.ServiceSync
	single := make([]utils.H, 0)
	multiple := make([]utils.H, 0)
	if ss != nil {
		for grpID, e := range ss.SingleSync {
			single = append(single, utils.H{
				"grp_id":    grpID,
				"enable":    e.Enable,
				"services":  e.Services,
				"namespace": e.Namespace,
			})
		}
		for _, e := range ss.MultipleSync {
			multiple = append(multiple, utils.H{
				"source_a":  e.SourceA,
				"source_b":  e.SourceB,
				"enable":    e.Enable,
				"services":  e.Services,
				"namespace": e.Namespace,
			})
		}
	}
	return utils.H{"single": single, "multiple": multiple}
}

// configSyncOverview 构造配置同步页所需的每分组概览：
// 选定节点 = 分组内随机一个可用节点（无主从概念，随机打散集群与节点顺序，取首个 TCP 可达的）；
// 再按分组设置的 grp_version 选择取数通道
// （"3X" → gRPC 取 public 配置数；"2X" 或空 → HTTP 全量命名空间列表；空时退化为探测）。
// 取数失败的分组仅填 error，不影响其余。返回的 mode/version/warning 用于前端展示实际使用的通道与限制说明。
//
// 性能：分组间并发取数（整体耗时由 Σ 降为 max），并以「当前生效配置对象指针」为键做短 TTL 缓存，
// 避免每次刷新都对全部分组做 TCP 预检 + 命名空间拉取。配置中心任何变更都会产生新的 *Config
// （store.Set 整体替换），故指针不变即代表配置未变，可直接复用缓存；TTL 提供额外兜底。
// ctx 用于超时取消，避免请求中途断开后后端仍长时间空跑。
func configSyncOverview(ctx context.Context, m *config.Manager) []utils.H {
	c := m.Get()
	if cached, ok := overviewCache.get(c); ok {
		return cached
	}
	groups := c.NacosGroup
	out := make([]utils.H, len(groups))
	var wg sync.WaitGroup
	var anyErr atomic.Bool
	for i := range groups {
		wg.Add(1)
		go func(i int, g config.NacosGroup) {
			defer wg.Done()
			ov := utils.H{
				"grp_id":       g.GrpID,
				"grp_name":     g.GrpName,
				"grp_version":  g.GrpVersion,
				"sync_enabled": c.ConfigSync[g.GrpID], // 缺省 false（不同步）
			}
			node := pickGroupNode(g)
			if node == "" {
				ov["error"] = "该分组下集群均未配置节点"
				anyErr.Store(true)
			} else {
				// 早退：ctx 已取消（超时/客户端断开）时不再发起取数。
				select {
				case <-ctx.Done():
					ov["error"] = "查询超时或已取消，请稍后重试"
					out[i] = ov
					anyErr.Store(true)
					return
				default:
				}
				ov["node"] = node
				res, err := nacos.FetchConfigOverview(nacos.ConfigOverviewSpec{
					Address:    node,
					GrpVersion: g.GrpVersion,
					EnableAuth: g.EnableAuth,
					Username:   g.AuthUsername,
					Password:   g.AuthPassword,
				})
				if err != nil {
					ov["error"] = err.Error()
					anyErr.Store(true)
				} else {
					ov["mode"] = res.Mode
					ov["version"] = res.Version
					if res.Warning != "" {
						ov["warning"] = res.Warning
					}
					nsList := make([]utils.H, 0, len(res.Namespaces))
					for _, n := range res.Namespaces {
						nsList = append(nsList, utils.H{
							"namespace":         n.Namespace,
							"namespaceShowName": n.NamespaceShowName,
							"configCount":       n.ConfigCount,
						})
					}
					ov["namespaces"] = nsList
				}
			}
			out[i] = ov
		}(i, groups[i])
	}
	wg.Wait()
	result := out
	// 仅当全部分组成功取数（无 error）才写入缓存，避免把残差/错误结果缓存 15s 误导用户。
	if !anyErr.Load() {
		overviewCache.set(c, result)
	}
	return result
}

// overviewCache 缓存配置同步概览结果，避免每次刷新都全量取数。
// 以「当前生效配置对象指针」为键：配置中心任何变更都会产生新的 *Config（store.Set 整体替换），
// 故指针不变即代表配置未变；TTL 提供额外兜底。
type overviewCacheT struct {
	mu     sync.Mutex
	cfgPtr uintptr
	value  []utils.H
	expire time.Time
	ttl    time.Duration
}

var overviewCache = &overviewCacheT{ttl: 15 * time.Second}

func (oc *overviewCacheT) get(cfg *config.Config) ([]utils.H, bool) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.cfgPtr == uintptr(unsafe.Pointer(cfg)) && time.Now().Before(oc.expire) {
		return oc.value, true
	}
	return nil, false
}

func (oc *overviewCacheT) set(cfg *config.Config, v []utils.H) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.cfgPtr = uintptr(unsafe.Pointer(cfg))
	oc.value = v
	oc.expire = time.Now().Add(oc.ttl)
}

// pickGroupNode 在分组内随机挑一个取数节点：把「集群 × 节点」打平后随机打散，并发探测 TCP 可达性，
// 首个探测到可达的节点立即返回（并取消其余探测），保持「首个可达即返回」短路语义；
// 若全部不可达，则返回随机首选项（让取数报出具体错误，而不是笼统地说无可用节点）。
// 分组内集群对等（已无主集群概念），随机序既打散负载，又避免固定落到某个已下线节点。
// 并发 + 命中即取消：总耗时收敛为单次超时（nodeProbeTimeout），且不会为单请求瞬时起 400+ goroutine/连接。
func pickGroupNode(g config.NacosGroup) string {
	cands := make([]string, 0, len(g.Clusters))
	for i := range g.Clusters {
		cands = append(cands, g.Clusters[i].CNodes...)
	}
	if len(cands) == 0 {
		return ""
	}
	rand.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	found := make(chan string, 1)
	var wg sync.WaitGroup
	for _, n := range cands {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			if nacos.NodeReachable(n, nodeProbeTimeout) {
				select {
				case found <- n:
					cancel() // 命中即停止其余探测
				default:
				}
			}
		}(n)
	}
	go func() { wg.Wait(); close(found) }()

	// 等待首个可达节点，或所有探测完成（无可达则回退首项）。
	select {
	case n, ok := <-found:
		if ok && n != "" {
			return n
		}
	case <-time.After(nodeProbeTimeout + 200*time.Millisecond):
	}
	return cands[0]
}
