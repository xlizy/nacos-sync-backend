# nacos-sync-backend

Nacos 多集群同步服务后端（数据面 nacos-sdk-go + 控制面 Hertz 9696）。
详细架构与决策依据见 [docs/design.md](../docs/design.md)（v0.3，与代码对齐）。

## 技术栈
- Go 1.27.0
- `nacos-sdk-go v2.3.5`：统一对接 Nacos 2.1.2（无 auth）/ 3.2.4（有 auth）
- Hertz：控制面 HTTP，端口 **9696**
- ants：协程池（容量 256，ctx 提交 + ReleaseTimeout 防 Goroutine 泄露）
- Prometheus：指标（`/api/metrics`）
- zap：日志（按天切割保留 7 天，全链路 traceId）

## 目录
```
cmd/server            启动入口：端口预检 + 自拨测 + Nacos 自注册 + 优雅关闭（顺序见 main.go 注释）
internal/config       两层配置：磁盘引导配置 + 配置中心运行时配置（nacos-group 权威模型）
                      Manager：拉取/ListenConfig 热替换/PublishConfig 写回，敏感字段 AES-GCM 加密为 ENC(...)
internal/pool         ants 协程池封装（ctx 提交 + Release/ReleaseTimeout）
internal/nacos        ClientManager（(clusterID, namespace) 缓存 SDK 客户端）+ InstancesLive 瞬时客户端
                      + localCache admin 刷新 + 分组 MySQL 指纹（config_info/gray）+ 连通性/鉴权探测
internal/sync/configsync   new↔new 配置同步：监听点火 + 回声抑制（sha256，30s TTL）+ patrol/reconcile 自愈
internal/sync/namingsync   服务同步（代注册）：ephemeral + syncSource 元数据防回环，
                           1s Nacos 源实例快轮询 + 10s 全量对账 + 20s 孤儿清扫；
                           dubbo 动态方向：按 providers:/consumers: 前缀实时枚举源集群服务全量代注册
internal/sync/watcher      配置同步辅助：3s 轮询目标 Nacos 后端 MySQL 指纹，变化刷新各节点 localCache
internal/api          Hertz 控制面（组/集群 CRUD、同步开关、状态查询、按 IP 查服务、reload/reencrypt）
internal/auth         控制面登录：RADIUS PAP（密码+动态码）+ JWT(12h) + RSA 凭据加密
internal/registry     本服务向 Nacos 注册中心自注册/注销
internal/logging      zap + traceId；internal/metrics Prometheus；internal/crypto AES-GCM；internal/netutil
configs/config*.yaml  引导配置（仅连「配置中心 Nacos」：address/port/username/password/namespace/dataId + secret-key）
configs/runtime-config.example.yaml  运行时配置 schema 示例（实际存于配置中心 dataId）
```

## 运行
```bash
# 主节点：默认 active，启动 NamingSync / ConfigSync / watcher
go run ./cmd/server -c configs/config.yaml

# 备用节点：只开放只读控制面与健康检查，不启动同步引擎
NACOS_SYNC_MODE=standby NACOS_SYNC_CONFIG=/path/to/config.yaml ./nacos-sync

# NACOS_SYNC_MODE 可选 active（默认）或 standby

# 必需环境变量
export NACOS_SYNC_SECRET_KEY=...   # ENC(...) 加解密主密钥（引导配置 secret-key 占位注入）
# 可选
export NACOS_SYNC_JWT_SECRET=...   # JWT 签名密钥（缺省回退 secret-key）
export NACOS_SYNC_API_TOKEN=...    # 静态共享令牌（机器/脚本旁路鉴权）
```

## 主备部署

支持通过环境变量 `NACOS_SYNC_MODE` 选择运行模式：

- `active`（默认）：启动 ConfigSync、NamingSync、watcher，并向 Nacos 注册工作节点。
- `standby`：不启动任何同步引擎、不向 Nacos 注册工作节点；保留 HTTP 控制面与健康检查，写请求返回 HTTP 503，只允许查询及登录/登出。

推荐部署 1 主 1 备：A 使用默认 `active`，B 设置 `NACOS_SYNC_MODE=standby`。故障切换必须先确认 A 进程已停止或主机已隔离，再将 B 切换为 `active`（重启进程生效）。A 恢复后保持停止，直到确认 B 已停止并完成反向切换。不要仅凭健康检查失败自动提升备用节点；网络分区或进程暂停可能造成双主。

standby 模式不提供自动故障转移，也不构成分布式 fencing；同一时刻只能由运维流程保证一个 active 实例。

## 控制面 API（9696）

鉴权：`auth.enable=true` 时受保护接口要求 `Authorization: Bearer <jwt>`（RADIUS 登录签发）或静态令牌；`/api/health`、`/api/metrics`、`/api/auth/*` 免鉴权。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET  | `/api/health` | 深检配置中心；UP=200 / DOWN=503（K8S 探针） |
| GET/POST | `/api/auth/status` `/api/auth/login` `/api/auth/logout` | 登录状态 / RADIUS 登录（凭据 RSA 加密提交）/ 登出 |
| GET  | `/api/auth/rsa-public-key` | 登录凭据加密公钥 |
| GET/POST | `/api/groups` | 组列表（脱敏）/ 新建或更新（grp_id 可空自动生成） |
| DELETE | `/api/groups/:grpId` | 删组（级联清理 service-sync 引用） |
| POST/DELETE | `/api/groups/:grpId/clusters[/:cId]` | 组下集群新增/更新（c_id 可空生成）/删除 |
| POST | `/api/groups/:grpId/test` | 逐节点连通性 + 鉴权探测 |
| POST | `/api/groups/:grpId/config-sync` | 配置同步开关（联动派生 config 链路） |
| GET  | `/api/config-sync` | 每分组配置概览 + 轮询状态（last_digest/健康） |
| GET  | `/api/pollers` | watcher 轮询任务状态 |
| GET  | `/api/service-sync` | 服务同步 single/multiple 配置视图 |
| POST/PUT/DELETE | `/api/service-sync/single[/:grpId]`、`/api/service-sync/multiple` | 服务同步增改删（multiple 无序对唯一） |
| GET  | `/api/naming-sync` | 各代注册方向实时状态 |
| GET  | `/api/dubbo-sync` | dubbo 集群同步条目列表 |
| POST/PUT/DELETE | `/api/dubbo-sync` | dubbo 集群同步增/改/删（两集群按 providers:/consumers: 前缀动态全量代注册） |
| GET  | `/api/lookup?ip=&port=` | 按 IP(+端口) 跨组查服务名（限流 60/min/IP） |
| GET/POST | `/api/scopes` | 旧模型 scope 热更新（保留） |
| POST | `/api/reload` `/api/reencrypt` | 主动重拉配置中心 / 存量明文全量重加密写回 |
| GET  | `/api/config-preview` | 预览写回配置中心的加密 YAML |
| GET  | `/api/clusters` `/api/links` `/api/metrics` | 旧拓扑视图 / Prometheus 指标 |

## 测试
```bash
go build ./... && go vet ./...
go test ./...
```
