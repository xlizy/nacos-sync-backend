package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/api"
	"github.com/cxcm/nacos-sync-backend/internal/config"
	"github.com/cxcm/nacos-sync-backend/internal/logging"
	"github.com/cxcm/nacos-sync-backend/internal/nacos"
	"github.com/cxcm/nacos-sync-backend/internal/pool"
	"github.com/cxcm/nacos-sync-backend/internal/registry"
	"github.com/cxcm/nacos-sync-backend/internal/sync/configsync"
	"github.com/cxcm/nacos-sync-backend/internal/sync/namingsync"
	"github.com/cxcm/nacos-sync-backend/internal/sync/watcher"
)

func main() {
	// 早期 logger（stdout）：logger 就绪前（读配置阶段）的报错仍要可见。
	logger := logging.NewStdout()

	cfgPath := flag.String("c", "", "引导配置文件路径，如 configs/config-prod.yaml（也可用环境变量 NACOS_SYNC_CONFIG）")
	flag.Parse()

	if *cfgPath == "" {
		*cfgPath = os.Getenv("NACOS_SYNC_CONFIG")
	}
	if *cfgPath == "" {
		*cfgPath = "configs/config.yaml"
	}

	boot, err := config.LoadBootstrap(*cfgPath)
	if err != nil {
		logger.Fatal("load bootstrap", zap.String("path", *cfgPath), zap.Error(err))
	}

	// 切换到正式 logger：日志写到 logging-path 目录（按天切割、保留 7 天）。
	if l, lerr := logging.New(boot.LoggingPath, ""); lerr == nil {
		_ = logger.Sync()
		logger = l
	} else {
		logger.Error("init file logger failed, fallback to stdout", zap.Error(lerr))
	}
	defer func() { _ = logger.Sync() }()

	logger.Info("bootstrap loaded",
		zap.String("config", *cfgPath),
		zap.String("logging_path", boot.LoggingPath))

	// 端口唯一性预检：Hertz 的 Spin() 在独立 goroutine 里吞掉绑定错误，
	// 若不预检，端口被占用时进程会「无 HTTP 却继续跑同步引擎」，形成看不见的幽灵进程。
	// 这里显式预检并在冲突时立即退出，保证同一时刻只有一个实例。
	if ln, lerr := net.Listen("tcp", fmt.Sprintf(":%d", api.DefaultHTTPPort)); lerr != nil {
		logger.Fatal("http port unavailable, refuse to start (another instance running?)",
			zap.Int("port", api.DefaultHTTPPort), zap.Error(lerr))
	} else {
		_ = ln.Close()
	}

	mgr, err := config.NewManager(boot, logger)
	if err != nil {
		logger.Fatal("init config manager", zap.Error(err))
	}

	// 协程池：实例量大，所有异步注册/发布统一经此提交，防止 Goroutine 泄露。
	pl, err := pool.New(256, logger) // 按规模调优：88 服务×~400 源节点注册风暴下，128 偏紧会阻塞 fastPoll/probe 调用方
	if err != nil {
		logger.Fatal("init pool", zap.Error(err))
	}

	clientMgr := nacos.NewClientManager(mgr)

	cfgEngine := configsync.New(mgr, clientMgr, pl, logger)
	nameEngine := namingsync.New(mgr, clientMgr, pl, api.DefaultHTTPPort, logger)
	syncWatcher := watcher.New(mgr, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfgEngine.Start(ctx)
	nameEngine.Start(ctx)
	syncWatcher.Start(ctx)

	srv := api.New(mgr, clientMgr, cfgEngine, nameEngine, syncWatcher, logger)
	if err := srv.Run(ctx); err != nil {
		logger.Fatal("http server run failed", zap.Error(err))
	}

	// 自监听校验：Run 内部用 goroutine 起 Spin()，绑定失败会被吞掉；
	// 端口冲突时若不自检，进程会「无 HTTP 却继续跑同步引擎」，成为看不见的幽灵进程。
	// 这里启动后探测本端口是否可达，不可达立即退出，保证端口唯一、避免残留进程干扰。
	{
		ok := false
		for i := 0; i < 20; i++ {
			if c, derr := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", api.DefaultHTTPPort), 300*time.Millisecond); derr == nil {
				_ = c.Close()
				ok = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !ok {
			logger.Fatal("http server not listening, refuse to run headless",
				zap.Int("port", api.DefaultHTTPPort),
				zap.Error(fmt.Errorf("self dial to %d failed after startup", api.DefaultHTTPPort)))
		}
	}

	// 启动后向 Nacos 注册中心注册自身（reg-namespace + appname），进程退出时注销。
	deregister, rerr := registry.Register(ctx, boot, api.DefaultHTTPPort, logger)
	if rerr != nil {
		logger.Warn("service registration init failed", zap.Error(rerr))
	}
	defer deregister()

	<-ctx.Done()
	logger.Info("shutting down nacos-sync")
	// 优雅关闭顺序（关键，不可随意调整）：
	// 1) 先关 HTTP 入口：停止接新请求，期间其余组件仍在线，在途请求可正常完成；
	// 2) 停同步引擎：nameEngine.Stop 会注销各方向在目标的代注册实例（注销任务提交到 pool）；
	// 3) 带超时的池释放：等待在途注销/转发任务落地后再关闭 SDK 客户端，
	//    避免「不等在途 → 已关客户端 → 注销失败」击穿优雅关闭的核心保证；
	// 4) 最后关闭 SDK 客户端与管理器，释放其后台 goroutine。
	srv.Close()
	cfgEngine.Stop()
	nameEngine.Stop()

	// 带超时的池释放：等待在途注销/转发任务落地（最长 15s）后再关闭 SDK 客户端，
	// 避免「不等在途 → 已关客户端 → 注销失败」击穿优雅关闭的核心保证。
	pl.ReleaseTimeout(15 * time.Second)

	clientMgr.Close() // 关闭全部缓存 SDK 客户端并停 sweep，释放其后台 goroutine，避免滚动重启累积
	mgr.Close()
}
