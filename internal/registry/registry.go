// Package registry 负责把本后端服务实例注册到 Nacos 注册中心，并在进程退出时注销。
//
// 注册所用命名空间为引导配置中的 reg-namespace（与配置文件所在的 cfg-namespace 相互独立）；
// 服务名取自 appname。注册中心不可达或鉴权失败时仅告警、不致命——控制面照常启动，
// 待注册中心就绪后重启即可补注册。
package registry

import (
	"context"
	"fmt"
	"net"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/config"
	"github.com/cxcm/nacos-sync-backend/internal/netutil"
)

// Register 向 Nacos 注册中心注册本后端服务实例，返回注销函数。
//   - appname 为空：跳过注册，返回空注销函数（保持旧行为，不影响启动）；
//   - 注册中心不可达/鉴权失败：记录告警并返回空注销函数，控制面照常运行；
//   - 注册成功：返回可在进程退出时调用的注销函数（幂等，失败仅告警）。
//
// 注册实例为临时实例（Ephemeral=true），由 SDK 后台心跳维持；进程崩溃时 Nacos 自动摘除，
// 无需依赖优雅退出的注销（但 Register 仍提供 deregister 以便在关闭路径显式摘除）。
func Register(ctx context.Context, boot *config.Bootstrap, listenPort int, logger *zap.Logger) (func(), error) {
	appName := boot.Nacos.AppName
	if appName == "" {
		logger.Warn("appname not configured, skip nacos service registration")
		return func() {}, nil
	}

	ip := netutil.LocalIP(net.JoinHostPort(boot.Nacos.Address, fmt.Sprintf("%d", boot.Nacos.Port)))

	client, err := newNamingClient(boot)
	if err != nil {
		return func() {}, fmt.Errorf("build registry naming client: %w", err)
	}

	param := vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        uint64(listenPort),
		ServiceName: appName,
		Weight:      1,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
	}
	if ok, rerr := client.RegisterInstance(param); rerr != nil {
		logger.Error("register service instance to nacos failed",
			zap.String("service", appName), zap.String("namespace", boot.Nacos.RegNamespace), zap.Error(rerr))
		return func() {}, nil // 非致命：注册失败不影响控制面运行
	} else if !ok {
		logger.Warn("register service instance returned false (nacos rejected?)",
			zap.String("service", appName))
	}

	logger.Info("service registered to nacos registry",
		zap.String("service", appName),
		zap.String("namespace", boot.Nacos.RegNamespace),
		zap.String("ip", ip),
		zap.Int("port", listenPort))

	deregister := func() {
		p := vo.DeregisterInstanceParam{
			Ip:          ip,
			Port:        uint64(listenPort),
			ServiceName: appName,
			Ephemeral:   true,
		}
		if _, derr := client.DeregisterInstance(p); derr != nil {
			logger.Warn("deregister service instance failed",
				zap.String("service", appName), zap.Error(derr))
		}
	}
	return deregister, nil
}

// newNamingClient 构造指向「配置中心/注册中心 Nacos」的命名客户端，使用 reg-namespace 与引导凭据。
// 注册中心与配置中心为同一 Nacos 实例（仅命名空间不同），故复用 boot.Nacos 的地址/端口/鉴权。
func newNamingClient(boot *config.Bootstrap) (naming_client.INamingClient, error) {
	return clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig: &constant.ClientConfig{
			NamespaceId:         boot.Nacos.RegNamespace,
			TimeoutMs:           5000,
			NotLoadCacheAtStart: true,
			LogLevel:            "warn",
			LogDir:              "/tmp/nacos-sync-registry/log",
			CacheDir:            "/tmp/nacos-sync-registry/cache",
			Username:            boot.Nacos.Username,
			Password:            boot.Nacos.Password,
		},
		ServerConfigs: []constant.ServerConfig{
			{IpAddr: boot.Nacos.Address, Port: boot.Nacos.Port, ContextPath: "/nacos"},
		},
	})
}
