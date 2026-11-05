// Package config 负责两层配置：
//  1. 磁盘 config.yaml = 引导配置（Bootstrap）：连接「配置中心 Nacos」的凭据
//     （address/port/username/password/cfg-namespace/configDataId），以及本后端注册到
//     Nacos 注册中心所需的 appname 与 reg-namespace；
//  2. 配置中心 Nacos 的 dataId = 运行时配置（RuntimeConfig），即真正的同步拓扑。
//
// Manager 负责连配置中心、拉取/监听/发布运行时配置，并以原子方式热替换生效配置。
//
// 运行时配置当前以 nacos-group（Nacos 分组 -> 集群目录）为权威模型，
// 承载「要同步的 Nacos 集群都在哪、怎么连」。老字段（Clusters/Links/Scopes）
// 暂保留以便同步引擎编译，待后续把同步拓扑从 nacos-group 派生时再启用。
package config

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cxcm/nacos-sync-backend/internal/crypto"
	"github.com/cxcm/nacos-sync-backend/internal/netutil"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// ===================== 引导配置（磁盘 config.yaml） =====================

// BootstrapNacos 连接「配置中心 Nacos」所需的凭据。
//   - ConfigNamespace：配置文件（运行时配置 dataId）所在命名空间；
//   - RegNamespace：本后端服务注册到 Nacos 注册中心时使用的命名空间（二者相互独立）；
//   - AppName：注册到注册中心的服务名；为空则启动时不注册。
type BootstrapNacos struct {
	Address         string `yaml:"address"`
	Port            uint64 `yaml:"port"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`      // 支持 ${ENV} 占位符，加载时注入
	ConfigNamespace string `yaml:"cfg-namespace"` // 配置文件（运行时配置 dataId）所在命名空间（空串 = public）
	RegNamespace    string `yaml:"reg-namespace"` // 注册服务时使用的命名空间（空串 = public）
	AppName         string `yaml:"appname"`       // 注册到 Nacos 注册中心的服务名；为空则不注册
	ConfigDataID    string `yaml:"configDataId"`  // 存放运行时配置的 dataId
}

// Bootstrap 磁盘引导配置。
type Bootstrap struct {
	Nacos       BootstrapNacos `yaml:"nacos"`
	SecretKey   string         `yaml:"secret-key"`   // 用于解密运行时配置中 ENC(...) 的 AES-GCM 密钥（支持 ${ENV} 占位符）
	LoggingPath string         `yaml:"logging-path"` // 日志文件目录；日志名 nacos-sync.log，按天切割、保留 7 天
	Path        string         `yaml:"-"`
}

// LoadBootstrap 读取磁盘引导配置，解析 ${ENV} 密码并填缺省。
func LoadBootstrap(path string) (*Bootstrap, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read bootstrap %s: %w", path, err)
	}
	b := &Bootstrap{}
	if err := yaml.Unmarshal(data, b); err != nil {
		return nil, fmt.Errorf("parse bootstrap %s: %w", path, err)
	}
	b.Path = path
	b.Nacos.Password = expandEnv(b.Nacos.Password)
	b.Nacos.Username = expandEnv(b.Nacos.Username)
	b.SecretKey = expandEnv(b.SecretKey)
	// 引导配置里的 nacos 凭据同样支持 ENC() 加密（与运行时配置一致），用 secret-key 解密。
	// 解密失败（密钥不匹配/密文损坏）只告警并保留原值，不致命——否则引导配置写错一个字符
	// 后端就起不来；保留原值后由配置中心返回 401 走降级，方便排错。
	if b.SecretKey != "" {
		if b.Nacos.Password != "" && strings.HasPrefix(b.Nacos.Password, "ENC(") {
			if dec, derr := crypto.Decrypt(b.SecretKey, b.Nacos.Password); derr == nil {
				b.Nacos.Password = dec
			} else {
				fmt.Printf("WARN: bootstrap nacos password ENC decrypt failed (secret-key mismatch? keeps raw value): %v\n", derr)
			}
		}
		if b.Nacos.Username != "" && strings.HasPrefix(b.Nacos.Username, "ENC(") {
			if dec, derr := crypto.Decrypt(b.SecretKey, b.Nacos.Username); derr == nil {
				b.Nacos.Username = dec
			} else {
				fmt.Printf("WARN: bootstrap nacos username ENC decrypt failed (secret-key mismatch? keeps raw value): %v\n", derr)
			}
		}
	}
	if b.Nacos.Port == 0 {
		b.Nacos.Port = 8848
	}
	if b.Nacos.ConfigDataID == "" {
		b.Nacos.ConfigDataID = "nacos-sync-backend-runtime.yaml"
	}
	return b, nil
}

// ===================== 运行时配置（来自配置中心 Nacos 的 dataId） =====================

// NacosCluster 一个 Nacos 集群（属于某个组）。分组内各集群对等，没有主/从之分。
type NacosCluster struct {
	CID    string   `yaml:"c_id" json:"c_id"`       // 集群 ID，可自动生成
	CName  string   `yaml:"c_name" json:"c_name"`   // 集群名称
	CNodes []string `yaml:"c_nodes" json:"c_nodes"` // 节点列表，元素为 host:port
}

// NacosGroup 一组 Nacos 集群（如「203那套」「本地那套」）。
// grp_id 可自动生成；enable_auth 为真时需带 auth_username/auth_password。
// GrpVersion 记录该分组的 Nacos 大版本（"2X" | "3X"），用于决定配置同步取数通道
// （2.x → HTTP OpenAPI；3.x → gRPC），避免运行时逐个节点探测版本。
type NacosGroup struct {
	GrpID        string         `yaml:"grp_id" json:"grp_id"`
	GrpName      string         `yaml:"grp_name" json:"grp_name"`                           // 组名称，最大 16 字符
	GrpVersion   string         `yaml:"grp_version,omitempty" json:"grp_version,omitempty"` // 分组 Nacos 版本：2X | 3X
	EnableAuth   bool           `yaml:"enable_auth" json:"enable_auth"`
	AuthUsername string         `yaml:"auth_username,omitempty" json:"auth_username,omitempty"`
	AuthPassword string         `yaml:"auth_password,omitempty" json:"auth_password,omitempty"`
	DBDSN        string         `yaml:"grp_dsn,omitempty" json:"grp_dsn,omitempty"` // 该分组 Nacos 后端 MySQL DSN（开启配置同步时用于轮询 config_info 变化），与 grp_name 平级
	Clusters     []NacosCluster `yaml:"cluster" json:"cluster"`
}

// UnmarshalYAML 兼容早期字段名 db_dsn：重命名后优先读取 grp_dsn，缺失时回退 db_dsn，
// 避免已写入配置中心的旧数据（以 db_dsn 存储）在读回时丢失。下次 PublishConfig 会以 grp_dsn 落盘，完成迁移。
func (g *NacosGroup) UnmarshalYAML(value *yaml.Node) error {
	type alias struct {
		GrpID        string         `yaml:"grp_id"`
		GrpName      string         `yaml:"grp_name"`
		GrpVersion   string         `yaml:"grp_version,omitempty"`
		EnableAuth   bool           `yaml:"enable_auth"`
		AuthUsername string         `yaml:"auth_username,omitempty"`
		AuthPassword string         `yaml:"auth_password,omitempty"`
		DBDSN        string         `yaml:"grp_dsn,omitempty"`
		DBDSNLegacy  string         `yaml:"db_dsn,omitempty"`
		Clusters     []NacosCluster `yaml:"cluster"`
	}
	var a alias
	if err := value.Decode(&a); err != nil {
		return err
	}
	g.GrpID = a.GrpID
	g.GrpName = a.GrpName
	g.GrpVersion = a.GrpVersion
	g.EnableAuth = a.EnableAuth
	g.AuthUsername = a.AuthUsername
	g.AuthPassword = a.AuthPassword
	g.Clusters = a.Clusters
	g.DBDSN = a.DBDSN
	if g.DBDSN == "" {
		g.DBDSN = a.DBDSNLegacy
	}
	return nil
}

// AuthConfig 集群鉴权配置；Password 支持 ${ENV} 形式在加载时注入。
type AuthConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// ===================== 控制面登录（RADIUS）配置 =====================

// RadiusConfig 控制面登录所用的公司 RADIUS 服务器配置（运行时配置 auth.radius 段）。
// 动态码以「密码+动态码拼接」形式放入 RADIUS User-Password 属性（多数公司服务的做法）。
type RadiusConfig struct {
	ServerHost    string `yaml:"server-host"`    // RADIUS 服务器地址（如 172.27.1.1）
	AuthPort      uint16 `yaml:"auth-port"`      // 认证端口，通常 1812
	SharedSecret  string `yaml:"shared-secret"`  // 共享密钥；支持 ${ENV} 与 ENC()（与集群口令同口径）
	TimeoutMs     int    `yaml:"timeout-ms"`     // 单次请求超时（毫秒）
	Retries       int    `yaml:"retries"`        // 重试次数
	CaseSensitive bool   `yaml:"case-sensitive"` // 用户名是否大小写敏感；false 时统一转小写后再发请求
	Users         []string `yaml:"users"`          // 允许登录本服务的用户白名单；未配置（nil）表示不限制，仅 RADIUS 校验即可；配置了（含空数组）则仅名单内用户可登录
}

// AuthSection 运行时配置顶层的 auth 段（当前仅 radius 一种登录后端）。
//
//	enable           : 总开关。true 才开启登录校验（受保护接口要求 JWT）；false 则控制面全开放，无需登录。
//	radius          : 登录后端（公司 RADIUS）。
//	rsa_private_key_pem : 用于解密前端加密传输的登录凭据的 RSA 私钥（PEM，PKCS#8 或 PKCS#1）。
//	                  为空则启动时临时生成 2048 位密钥（单实例/开发友好，重启即变）；
//	                  多实例部署须在【各实例】配置同一份，否则 nginx 轮询会让「取公钥」与「收登录请求」
//	                  落到不同实例、私钥不一致导致解密失败。
type AuthSection struct {
	Enable           bool          `yaml:"enable"`
	Radius           *RadiusConfig `yaml:"radius,omitempty"`
	RSAPrivateKeyPEM string        `yaml:"rsa_private_key_pem,omitempty"`
}

// AuthEnabled 报告是否开启登录校验（auth.enable）。false 时控制面全开放。
func (c *Config) AuthEnabled() bool {
	return c.Auth != nil && c.Auth.Enable
}

// RadiusEnabled 报告运行时配置是否已启用 RADIUS 登录。
func (c *Config) RadiusEnabled() bool {
	return c.Auth != nil && c.Auth.Radius != nil && c.Auth.Radius.ServerHost != ""
}

// ClusterSpec 描述一个 Nacos 集群（老或新），用于构建 SDK 客户端。
// 当前未直接出现在 nacos-group 运行时配置中，保留以便同步引擎编译。
type ClusterSpec struct {
	ID          string     `yaml:"id"`
	Group       string     `yaml:"group"` // old | new
	Addresses   []string   `yaml:"addresses"`
	Version     string     `yaml:"version"` // 2.1.2 | 3.2.4
	Auth        AuthConfig `yaml:"auth"`
	ContextPath string     `yaml:"context_path"` // 默认 /nacos
}

// SyncLink 一条同步链路（单向、可启停）。保留以便同步引擎编译。
type SyncLink struct {
	From    string `yaml:"from"`
	To      string `yaml:"to"`
	Kind    string `yaml:"kind"` // config | naming
	Enabled bool   `yaml:"enabled"`
	Scope   string `yaml:"scope"` // 可选，引用 Scopes 中的命名（naming 链路常用）
}

// IncludeFilter 白名单维度。
type IncludeFilter struct {
	Namespaces []string `yaml:"namespaces"` // "public" 或 namespace id；"*" 表示全部
	Groups     []string `yaml:"groups"`
	Services   []string `yaml:"services"` // 精确名或正则（锚定 ^...$）
}

// ScopeFilter 黑名单维度。
type ScopeFilter struct {
	Namespaces []string `yaml:"namespaces"`
	Groups     []string `yaml:"groups"`
	Services   []string `yaml:"services"`
}

// SyncScope 一条链路的同步范围：include 白名单 + exclude 黑名单（exclude 优先）。
type SyncScope struct {
	Include IncludeFilter `yaml:"include"`
	Exclude ScopeFilter   `yaml:"exclude"`
}

// ===================== 服务同步（naming 同步）配置 =====================

// ServiceSyncEntry 组内同步（single-sync）：一个分组内的服务同步配置。
// key 在 SingleSync map 中为 grp_id（单一分组）。Enable 控制是否启用；
// Services 为需要同步的服务名白名单（空表示不同步任何服务，即失败关闭）；
// Namespace 限定同步范围所属命名空间（空表示 public）。
type ServiceSyncEntry struct {
	Enable    bool     `yaml:"enable" json:"enable"`
	Services  []string `yaml:"services,omitempty" json:"services,omitempty"`
	Namespace string   `yaml:"namespace,omitempty" json:"namespace,omitempty"`
}

// MultipleSyncEntry 组外同步（multiple-sync）：两个分组（source-a、source-b）间的服务同步配置。
// 两分组的无序对唯一（A↔B 与 B↔A 视为同一条）；Services 为需同步的服务名白名单
// （空表示不同步任何服务，即失败关闭）；Namespace 限定同步范围所属命名空间（空表示 public）。
type MultipleSyncEntry struct {
	SourceA   string   `yaml:"source-a" json:"source_a"`
	SourceB   string   `yaml:"source-b" json:"source_b"`
	Enable    bool     `yaml:"enable" json:"enable"`
	Services  []string `yaml:"services,omitempty" json:"services,omitempty"`
	Namespace string   `yaml:"namespace,omitempty" json:"namespace,omitempty"`
}

// ServiceSyncConfig 服务同步（naming）配置，持久化于运行时配置的 service-sync 段。
//   - SingleSync：组内同步，key=grp_id，一个分组至多一条；
//   - MultipleSync：组外同步，无序对唯一（A↔B 与 B↔A 视为同一条）。
type ServiceSyncConfig struct {
	SingleSync   map[string]ServiceSyncEntry `yaml:"single-sync,omitempty" json:"single_sync,omitempty"`
	MultipleSync []MultipleSyncEntry         `yaml:"multiple-sync,omitempty" json:"multiple_sync,omitempty"`
}

// Config 顶层运行时配置。
// NacosGroup 为当前权威模型（集群目录）；其余字段保留供同步引擎编译，暂为空。
type Config struct {
	Namespaces []string             `yaml:"namespaces,omitempty"`
	Clusters   []ClusterSpec        `yaml:"clusters,omitempty"`
	Links      []SyncLink           `yaml:"links,omitempty"`
	Scopes     map[string]SyncScope `yaml:"scopes,omitempty"`
	NacosGroup []NacosGroup         `yaml:"nacos-group"`
	// ConfigSync 记录哪些分组开启了「配置同步」。key=grp_id，value=是否开启。
	// 持久化于运行时配置的 config-sync 段；新建分组时默认写入 false（不同步）。
	ConfigSync map[string]bool `yaml:"config-sync,omitempty"`
	// ServiceSync 服务同步（naming）配置，持久化于 service-sync 段。
	ServiceSync *ServiceSyncConfig `yaml:"service-sync,omitempty"`
	// DubboSync dubbo 服务同步配置：两集群间按 providers:/consumers: 前缀动态全量代注册，
	// 持久化于 dubbo-sync 段。
	DubboSync *DubboSyncConfig `yaml:"dubbo-sync,omitempty"`
	// Auth 控制面登录配置（RADIUS）。未配置时控制面沿用静态共享令牌（NACOS_SYNC_API_TOKEN）或保持开放。
	Auth *AuthSection `yaml:"auth,omitempty"`
}

// Default 返回内置默认值（空配置，用于配置中心不可达时的降级）。
func Default() *Config {
	return &Config{}
}

// ParseRuntime 从配置中心 dataId 的 YAML 内容解析运行时配置，做缺省填充与密码注入。
func ParseRuntime(content string) (*Config, error) {
	c := Default()
	if err := yaml.Unmarshal([]byte(content), c); err != nil {
		return nil, fmt.Errorf("parse runtime config: %w", err)
	}
	for i := range c.Clusters {
		if c.Clusters[i].Auth.Password != "" {
			c.Clusters[i].Auth.Password = expandEnv(c.Clusters[i].Auth.Password)
		}
		if c.Clusters[i].ContextPath == "" {
			c.Clusters[i].ContextPath = "/nacos"
		}
	}
	// 分组级 DBDSN（配置同步轮询用）同样支持 ${ENV} 占位符，避免明文口令写进配置中心。
	// 注意：若配置了 secret-key，DBDSN 在写回前已被 EncryptSecrets 加密为 ENC(...)，
	// 解密后此处再展开 ${ENV} 是幂等的（非 ${...} 形式不改动）。
	for i := range c.NacosGroup {
		if c.NacosGroup[i].DBDSN != "" {
			c.NacosGroup[i].DBDSN = expandEnv(c.NacosGroup[i].DBDSN)
		}
	}
	// RADIUS 共享密钥支持 ${ENV}（避免明文落配置中心）；ENC() 已由 DecryptYAML 在解析前还原，此处不重复处理。
	if c.Auth != nil && c.Auth.Radius != nil && c.Auth.Radius.SharedSecret != "" {
		c.Auth.Radius.SharedSecret = expandEnv(c.Auth.Radius.SharedSecret)
	}
	return c, nil
}

// parseRuntime 解析前先解密内容中的 ENC(...) 令牌（使用引导配置的 secret-key）。
// 解密失败时返回错误，避免把明文口令静默写回或解析出错误配置。
func (m *Manager) parseRuntime(content string) (*Config, error) {
	dec, err := crypto.DecryptYAML(m.boot.SecretKey, content)
	if err != nil {
		return nil, fmt.Errorf("decrypt runtime config: %w", err)
	}
	return ParseRuntime(dec)
}

// EncryptSecrets 返回一份副本，将敏感字段（auth_password、grp_dsn）以 AES-256-GCM 加密为
// ENC(...) 形式，用于写回配置中心，避免明文落盘。secretKey 为空时不加密（原样返回）。
// 注意：仅对这两个指定字段加密；若运维对其它字段使用了 ENC()，解密后再次发布会以明文写出。
func (c *Config) EncryptSecrets(secretKey string) (*Config, error) {
	if secretKey == "" {
		return c, nil
	}
	nc := &Config{
		Namespaces:  c.Namespaces,
		Clusters:    c.Clusters,
		Links:       c.Links,
		Scopes:      c.Scopes,
		NacosGroup:  make([]NacosGroup, len(c.NacosGroup)),
		ConfigSync:  c.ConfigSync,
		ServiceSync: c.ServiceSync,
		// DubboSync 必须一并带上：Publish 落盘的内容取自本方法返回的副本，此前漏拷
		// 导致 dubbo-sync 段在写回配置中心时被静默丢弃（新增接口仍返回 200，但 Nacos
		// 上看不到 dubbo-sync 段，随后配置中心推回旧内容把本地生效配置覆盖掉）。
		DubboSync: c.DubboSync,
		Auth:        c.Auth,
	}
	// RADIUS 共享密钥纳入加密范围：配置中心落盘不含明文口令。
	if c.Auth != nil && c.Auth.Radius != nil {
		r := *c.Auth.Radius
		if r.SharedSecret != "" && !strings.HasPrefix(r.SharedSecret, encTokenPrefix) {
			enc, err := crypto.Encrypt(secretKey, r.SharedSecret)
			if err != nil {
				return nil, fmt.Errorf("encrypt radius shared-secret: %w", err)
			}
			r.SharedSecret = enc
		}
		nc.Auth = &AuthSection{Radius: &r}
	}
	for i, g := range c.NacosGroup {
		ng := g
		// 防御双重加密：若值本身已为 ENC(...) 令牌（存量已加密 / 前端误传），直接保留，不再加密，
		// 否则 ENC(ENC(密文)) 会在解密时只解一层、得到 ENC(密文) 而非明文，导致数据损坏。
		if g.AuthPassword != "" && !strings.HasPrefix(g.AuthPassword, encTokenPrefix) {
			enc, err := crypto.Encrypt(secretKey, g.AuthPassword)
			if err != nil {
				return nil, fmt.Errorf("encrypt auth_password for group %s: %w", g.GrpID, err)
			}
			ng.AuthPassword = enc
		}
		if g.DBDSN != "" && !strings.HasPrefix(g.DBDSN, encTokenPrefix) {
			enc, err := crypto.Encrypt(secretKey, g.DBDSN)
			if err != nil {
				return nil, fmt.Errorf("encrypt grp_dsn for group %s: %w", g.GrpID, err)
			}
			ng.DBDSN = enc
		}
		ng.Clusters = append([]NacosCluster(nil), g.Clusters...)
		nc.NacosGroup[i] = ng
	}
	return nc, nil
}

// Marshal 序列化回 YAML（用于发布回配置中心）。
// 仅含非空段：nacos-group 恒输出，旧字段因 omitempty 为空时不写回，避免污染配置。
func (c *Config) Marshal() ([]byte, error) {
	return yaml.Marshal(c)
}

// expandEnv 支持 ${ENV} 占位符注入机密。未定义的环境变量不再静默返回空串：
// 保留原始 ${VAR} 占位符，使下游（如 DBDSN / Nacos 口令）以明显「未解析」形态失败（报错而非静默跳过），
// 避免配置同步等依赖因空值而悄然失效。
func expandEnv(s string) string {
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		name := strings.TrimSuffix(strings.TrimPrefix(s, "${"), "}")
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		return s
	}
	return s
}

// SplitHostPort 解析 host:port，缺省端口回退 8848。
func SplitHostPort(addr string) (string, uint64) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 8848
	}
	var p uint64
	_, _ = fmt.Sscanf(portStr, "%d", &p)
	if p == 0 {
		p = 8848
	}
	return host, p
}

// ===================== nacos-group CRUD 辅助 =====================

// ErrNotFound 标记「资源不存在」类错误，供 API 层用 errors.Is 判定返回 404（替代脆弱的字符串后缀匹配）。
var ErrNotFound = errors.New("not found")

// notFoundf 构造带 ErrNotFound 包裹的「未找到」错误，保留结尾 "not found" 文案以保证过渡期兼容。
func notFoundf(format string, args ...any) error {
	return fmt.Errorf(format+": %w", append(append([]any{}, args...), ErrNotFound)...)
}

const idAlphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// randID 生成 n 位「数字 + 大小写字母」ID（对应注释中的「4位数字加大小写字母」）。
func randID(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = idAlphabet[rand.Intn(len(idAlphabet))]
	}
	return string(b)
}

func groupIDSet(groups []NacosGroup) map[string]bool {
	m := make(map[string]bool, len(groups))
	for _, g := range groups {
		m[g.GrpID] = true
	}
	return m
}

func clusterIDSet(clusters []NacosCluster) map[string]bool {
	m := make(map[string]bool, len(clusters))
	for _, c := range clusters {
		m[c.CID] = true
	}
	return m
}

// genGroupID 生成不重复的 4 位组 ID。
func genGroupID(existing []NacosGroup) string {
	used := groupIDSet(existing)
	for i := 0; i < 100; i++ {
		id := randID(4)
		if !used[id] {
			return id
		}
	}
	return randID(8) // 极端碰撞兜底
}

// genClusterID 生成不重复的 4 位集群 ID（仅供同组旧逻辑使用）。
func genClusterID(existing []NacosCluster) string {
	used := clusterIDSet(existing)
	for i := 0; i < 100; i++ {
		id := randID(4)
		if !used[id] {
			return id
		}
	}
	return randID(8)
}

// validateUniqueClusterIDs 保证 c_id 在所有 nacos-group 中全局唯一。
// ClientManager 的客户端缓存与连接解析均以 c_id 为唯一键，不能只保证组内唯一。
func validateUniqueClusterIDs(groups []NacosGroup) error {
	owners := make(map[string]string)
	for _, g := range groups {
		for _, cl := range g.Clusters {
			if cl.CID == "" {
				continue
			}
			if owner, ok := owners[cl.CID]; ok {
					return fmt.Errorf("duplicate c_id %q across nacos-group %q and %q; c_id must be globally unique", cl.CID, owner, g.GrpID)
				}
			owners[cl.CID] = g.GrpID
		}
	}
	return nil
}

// ensureClusterIDsGlobal 为待写入分组补 ID，生成时避开其它所有分组的 c_id。
func ensureClusterIDsGlobal(clusters []NacosCluster, groups []NacosGroup, replacingGrpID string) []NacosCluster {
	used := make(map[string]bool)
	for _, g := range groups {
		if g.GrpID == replacingGrpID {
			continue
		}
		for _, cl := range g.Clusters {
			if cl.CID != "" {
				used[cl.CID] = true
			}
		}
	}
	for _, cl := range clusters {
		if cl.CID != "" {
			used[cl.CID] = true
		}
	}
	for i := range clusters {
		if clusters[i].CID == "" {
			id := genClusterIDAvoiding(used)
			clusters[i].CID = id
			used[id] = true
		}
	}
	return clusters
}

// genClusterIDGlobal 生成一个未被任何分组使用的集群 ID。
func genClusterIDGlobal(groups []NacosGroup) string {
	used := make(map[string]bool)
	for _, g := range groups {
		for _, cl := range g.Clusters {
			if cl.CID != "" {
				used[cl.CID] = true
			}
		}
	}
	return genClusterIDAvoiding(used)
}

// ensureClusterIDs 为 c_id 为空的集群生成不重复 ID（同一次调用内也避免碰撞）。
func ensureClusterIDs(clusters []NacosCluster) []NacosCluster {
	used := clusterIDSet(clusters)
	for i := range clusters {
		if clusters[i].CID == "" {
			id := genClusterIDAvoiding(used)
			clusters[i].CID = id
			used[id] = true
		}
	}
	return clusters
}

func genClusterIDAvoiding(used map[string]bool) string {
	for i := 0; i < 100; i++ {
		id := randID(4)
		if !used[id] {
			return id
		}
	}
	return randID(8)
}

// validateNodes 校验并规整节点列表：每项须为 host:port，端口须在 1..65535，
// 去除空白项与重复项（重复判定用「host:port 归一化」形式，避免 8848 与缺省端口的歧义）。
// 全部无效/为空时返回错误（节点是连通性前提）。
func validateNodes(nodes []string) ([]string, error) {
	seen := map[string]bool{}
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		host, portStr, err := net.SplitHostPort(n)
		if err != nil {
			return nil, fmt.Errorf("invalid node %q: expected host:port", n)
		}
		port, perr := strconv.ParseUint(portStr, 10, 16)
		if perr != nil || port == 0 {
			return nil, fmt.Errorf("invalid node %q: port must be in 1..65535", n)
		}
		norm := net.JoinHostPort(host, strconv.FormatUint(port, 10))
		if seen[norm] {
			continue
		}
		seen[norm] = true
		out = append(out, norm)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("c_nodes must not be empty")
	}
	return out, nil
}

// dedupeStrings 去除空白项与重复项（保持首次出现顺序），用于 services 白名单去重。
func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// ===================== Manager：连配置中心、拉取/监听/发布 =====================

// RuntimeConfigGroup 运行时配置在配置中心 Nacos 中的 group（引导配置未含 group，固定默认值）。
const RuntimeConfigGroup = "DEFAULT_GROUP"

// Manager 管理配置中心连接与运行时配置的热更新。
type Manager struct {
	boot  *Bootstrap
	cc    config_client.IConfigClient
	store *ScopeStore
	group string

	mu    sync.Mutex
	hooks []func(*Config) // 运行时配置变化回调（驱动业务监听器 reconcile）

	// writeMu 串行化所有「读-改-写回配置中心-替换生效配置」的写路径，避免两个并发写
	// 各自基于同一份旧配置提交、后者覆盖前者（写丢失）。与 mu 分离：写路径不调用 hooks，
	// 而 hooks 回调（fireRuntimeChange）只在复制切片瞬间持 mu，二者不会重入死锁。
	writeMu sync.Mutex
}

// NewManager 连配置中心 Nacos，拉取 dataId 运行时配置并注册监听热更新。
// 若配置中心暂不可达，降级为内置空默认配置，控制面仍可启动；
// 待配置中心就绪后，ListenConfig 或手动 /api/reload 会补拉真实配置。
func NewManager(boot *Bootstrap, logger *zap.Logger) (*Manager, error) {
	cc, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig: &constant.ClientConfig{
			NamespaceId:         boot.Nacos.ConfigNamespace,
			TimeoutMs:           5000,
			NotLoadCacheAtStart: true,
			LogLevel:            "warn",
			LogDir:              "/tmp/nacos-sync-config/log",
			CacheDir:            "/tmp/nacos-sync-config/cache",
			Username:            boot.Nacos.Username,
			Password:            boot.Nacos.Password,
		},
		ServerConfigs: []constant.ServerConfig{
			{IpAddr: boot.Nacos.Address, Port: boot.Nacos.Port, ContextPath: "/nacos"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("build config-center client: %w", err)
	}
	m := &Manager{boot: boot, cc: cc, group: RuntimeConfigGroup}

	param := vo.ConfigParam{DataId: boot.Nacos.ConfigDataID, Group: m.group}
	if content, gerr := cc.GetConfig(param); gerr != nil {
		logger.Warn("get runtime config from config-center failed, start with empty default",
			zap.String("dataId", boot.Nacos.ConfigDataID), zap.Error(gerr))
		m.store = NewScopeStore(Default())
	} else if rc, perr := m.parseRuntime(content); perr != nil {
		logger.Warn("parse runtime config failed, start with empty default", zap.Error(perr))
		m.store = NewScopeStore(Default())
	} else {
		m.store = NewScopeStore(rc)
	}

	if lerr := cc.ListenConfig(vo.ConfigParam{
		DataId: boot.Nacos.ConfigDataID,
		Group:  m.group,
			OnChange: func(namespace, group, dataId, data string) {
				nc, e := m.parseRuntime(data)
				if e != nil {
					logger.Error("runtime config onChange parse failed", zap.Error(e))
					return
				}
				// 与任何 CRUD 写路径同持 writeMu：避免「配置中心推送」与「并发 POST 写回」各自 store.Set
				// 形成 TOCTOU 写丢失（比当初修的 UpdateScope/Reencrypt 频率高得多）。fireRuntimeChange 在锁外执行，
				// 避免回调内再进入持锁方法导致重入死锁。
				m.writeMu.Lock()
				m.store.Set(nc)
				m.writeMu.Unlock()
				logger.Info("runtime config hot-reloaded from config-center", zap.String("dataId", dataId))
				m.fireRuntimeChange(nc)
			},
	}); lerr != nil {
		logger.Warn("listen runtime config failed", zap.Error(lerr))
	}
	return m, nil
}

// Get 返回当前生效的运行时配置（并发安全）。
func (m *Manager) Get() *Config { return m.store.Get() }

// AppName 返回引导配置中的注册服务名（本后端在 Nacos 注册中心注册自身的服务名）。
// 供孤儿清扫跳过「本服务自身心跳」这一保留服务，避免误删其它后端实例的服务发现注册。
func (m *Manager) AppName() string {
	if m.boot == nil {
		return ""
	}
	return m.boot.Nacos.AppName
}

// JwtSecret 返回控制面登录 JWT 的签名密钥。优先取环境变量 NACOS_SYNC_JWT_SECRET，
// 未设置时回退到引导配置 secret-key（与运行时配置 ENC() 加解密同源，避免新增独立密钥管理负担）。
// 两者皆空时返回空串——此时签发/校验 JWT 会失败（调用方应在 RADIUS 启用时确保至少其一存在）。
func (m *Manager) JwtSecret() string {
	if v, ok := os.LookupEnv("NACOS_SYNC_JWT_SECRET"); ok && v != "" {
		return v
	}
	if m.boot == nil {
		return ""
	}
	return m.boot.SecretKey
}

// AgentIP 返回本同步服务所在主机的 IPv4 地址，供代注册实例写入 metadata.syncSourceAgent，
// 以便在多后端实例部署下溯源「这条代注册是谁写的」。优先取能路由到配置中心 Nacos 的本机地址，
// 与 NACOS-SYNC 自注册所用 IP 口径一致；失败回退首个非 loopback 地址，再不行用 127.0.0.1。
func (m *Manager) AgentIP() string {
	if m.boot == nil {
		return "127.0.0.1"
	}
	return netutil.LocalIP(net.JoinHostPort(m.boot.Nacos.Address, fmt.Sprintf("%d", m.boot.Nacos.Port)))
}

// Health 检查关键依赖（配置中心 Nacos）是否可达，供 /api/health 做深检。
// 返回 (ok, detail)：ok=false 表示配置中心不可达（或超时），调用方应据此返回 503（degraded）。
// 通过独立 goroutine + 超时避免 GetConfig 自身无 ctx 参数导致的无限期阻塞。
func (m *Manager) Health(ctx context.Context) (bool, string) {
	param := vo.ConfigParam{DataId: m.boot.Nacos.ConfigDataID, Group: m.group}
	type result struct {
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_, e := m.cc.GetConfig(param)
		ch <- result{e}
	}()
	select {
	case <-ctx.Done():
		return false, "config-center health check cancelled"
	case r := <-ch:
		if r.err != nil {
			return false, fmt.Sprintf("config-center unreachable: %v", r.err)
		}
		return true, "ok"
	case <-time.After(3 * time.Second):
		return false, "config-center health check timed out (3s)"
	}
}

// Store 暴露底层原子容器（用于引擎读取）。
func (m *Manager) Store() *ScopeStore { return m.store }

// OnRuntimeChange 注册运行时配置变化回调。回调在 ListenConfig 热更新并 store.Set 之后触发，
// 用于驱动业务监听器（如配置同步轮询任务）的 reconcile：配置增删或开关变化时增删监听。
func (m *Manager) OnRuntimeChange(cb func(*Config)) {
	m.mu.Lock()
	m.hooks = append(m.hooks, cb)
	m.mu.Unlock()
}

// fireRuntimeChange 在热更新后通知所有已注册回调（先复制切片避免持锁调用）。
func (m *Manager) fireRuntimeChange(c *Config) {
	m.mu.Lock()
	cbs := make([]func(*Config), len(m.hooks))
	copy(cbs, m.hooks)
	m.mu.Unlock()
	for _, cb := range cbs {
		cb(c)
	}
}

// GetGroup 按 grp_id 查找组。
func (m *Manager) GetGroup(grpID string) (NacosGroup, bool) {
	for _, g := range m.Get().NacosGroup {
		if g.GrpID == grpID {
			return g, true
		}
	}
	return NacosGroup{}, false
}

// cloneForWrite 深拷贝当前配置（复制 nacos-group 与 config-sync 两层），
// 避免写操作污染并发读取的生效配置，或共享 map 触发并发读写。
func (m *Manager) cloneForWrite(c *Config) *Config {
	nc := *c
	nc.NacosGroup = make([]NacosGroup, len(c.NacosGroup))
	for i, g := range c.NacosGroup {
		ng := g
		ng.Clusters = make([]NacosCluster, len(g.Clusters))
		copy(ng.Clusters, g.Clusters)
		nc.NacosGroup[i] = ng
	}
	if c.ConfigSync != nil {
		nc.ConfigSync = make(map[string]bool, len(c.ConfigSync))
		for k, v := range c.ConfigSync {
			nc.ConfigSync[k] = v
		}
	}
	// 服务同步：深拷贝 map 与切片，避免写操作污染并发读取的生效配置。
	if c.ServiceSync != nil {
		ss := &ServiceSyncConfig{}
		if c.ServiceSync.SingleSync != nil {
			ss.SingleSync = make(map[string]ServiceSyncEntry, len(c.ServiceSync.SingleSync))
			for k, v := range c.ServiceSync.SingleSync {
				ss.SingleSync[k] = ServiceSyncEntry{Enable: v.Enable, Services: append([]string(nil), v.Services...), Namespace: v.Namespace}
			}
		}
		if len(c.ServiceSync.MultipleSync) > 0 {
			ss.MultipleSync = make([]MultipleSyncEntry, len(c.ServiceSync.MultipleSync))
			for i, e := range c.ServiceSync.MultipleSync {
				ss.MultipleSync[i] = MultipleSyncEntry{
					SourceA:   e.SourceA,
					SourceB:   e.SourceB,
					Enable:    e.Enable,
					Services:  append([]string(nil), e.Services...),
					Namespace: e.Namespace,
				}
			}
		}
		nc.ServiceSync = ss
	}
	// dubbo 同步：深拷贝条目切片，避免写操作污染并发读取的生效配置。
	if c.DubboSync != nil {
		d := &DubboSyncConfig{}
		d.Entries = make([]DubboSyncEntry, len(c.DubboSync.Entries))
		copy(d.Entries, c.DubboSync.Entries)
		nc.DubboSync = d
	}
	return &nc
}

// UpsertGroup 新建或更新一个组，写回配置中心并原子替换生效配置。
// grp_id 为空时自动生成 4 位 ID；enable_auth 为真且入参口令为空时保留既有口令（防误清）。
func (m *Manager) UpsertGroup(g NacosGroup) (*NacosGroup, error) {
	if g.GrpName == "" {
		return nil, fmt.Errorf("grp_name required")
	}
	if len(g.GrpName) > 16 {
		return nil, fmt.Errorf("grp_name too long: max 16 chars, got %d", len(g.GrpName))
	}
	// 节点格式校验；c_id 在持写锁后结合全局配置生成与校验。
	if len(g.Clusters) > 0 {
		for i := range g.Clusters {
			cleaned, verr := validateNodes(g.Clusters[i].CNodes)
			if verr != nil {
				return nil, verr
			}
			g.Clusters[i].CNodes = cleaned
		}
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	idx := -1
	for i := range nc.NacosGroup {
		if nc.NacosGroup[i].GrpID == g.GrpID {
			idx = i
			break
		}
	}
	if g.GrpID == "" {
		g.GrpID = genGroupID(nc.NacosGroup)
	}
	g.Clusters = ensureClusterIDsGlobal(g.Clusters, nc.NacosGroup, g.GrpID)
	// 新建分组时，在 config-sync 段默认登记一条「不同步」（false）。
	if idx < 0 {
		if nc.ConfigSync == nil {
			nc.ConfigSync = map[string]bool{}
		}
		nc.ConfigSync[g.GrpID] = false
	}
	// 鉴权组：编辑时入参为空的用户名/口令表示「保留既有」（防误清）；补齐后仍空则非法。
	if g.EnableAuth {
		if idx >= 0 {
			if g.AuthUsername == "" {
				g.AuthUsername = nc.NacosGroup[idx].AuthUsername
			}
			if g.AuthPassword == "" {
				g.AuthPassword = nc.NacosGroup[idx].AuthPassword
			}
		}
		if g.AuthUsername == "" || g.AuthPassword == "" {
			return nil, fmt.Errorf("enable_auth requires non-empty auth_username and auth_password")
		}
	}
	// DSN 同理：编辑时入参为空表示保留既有连接（防误清）。
	if idx >= 0 && g.DBDSN == "" {
		g.DBDSN = nc.NacosGroup[idx].DBDSN
	}
	if idx >= 0 {
		nc.NacosGroup[idx] = g
	} else {
		nc.NacosGroup = append(nc.NacosGroup, g)
	}
	if err := validateUniqueClusterIDs(nc.NacosGroup); err != nil {
		return nil, err
	}
	if err := m.Publish(nc); err != nil {
		return nil, err
	}
	m.store.Set(nc)
	return &g, nil
}

// DeleteGroup 删除一个组（含其下所有集群），写回配置中心。
// 同时级联清理引用该 grp_id 的服务同步条目（single-sync 直接键、multiple-sync 含该分组的无序对），
// 避免残留悬挂引用导致引擎跳过或前端展示一堆无意义 ID。
func (m *Manager) DeleteGroup(grpID string) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	out := make([]NacosGroup, 0, len(nc.NacosGroup))
	found := false
	for _, g := range nc.NacosGroup {
		if g.GrpID == grpID {
			found = true
			continue
		}
		out = append(out, g)
	}
	if !found {
		return notFoundf("group %s not found", grpID)
	}
	nc.NacosGroup = out
	// 同步清理 config-sync 段里该分组的记录。
	delete(nc.ConfigSync, grpID)
	// 级联清理服务同步条目：single-sync 直接键、multiple-sync 中含该分组的无序对。
	if nc.ServiceSync != nil {
		delete(nc.ServiceSync.SingleSync, grpID)
		kept := make([]MultipleSyncEntry, 0, len(nc.ServiceSync.MultipleSync))
		for _, e := range nc.ServiceSync.MultipleSync {
			if e.SourceA == grpID || e.SourceB == grpID {
				continue
			}
			kept = append(kept, e)
		}
		nc.ServiceSync.MultipleSync = kept
	}
	// 级联清理 dubbo-sync 段里引用该分组任一集群的条目（被删组已从 nc.NacosGroup 移出，从原配置取集群 ID）。
	var removedGroup *NacosGroup
	for i := range c.NacosGroup {
		if c.NacosGroup[i].GrpID == grpID {
			removedGroup = &c.NacosGroup[i]
			break
		}
	}
	for _, cl := range removedGroup.Clusters {
		removeDubboEntriesForCluster(nc, cl.CID)
	}
	if err := m.Publish(nc); err != nil {
		return err
	}
	m.store.Set(nc)
	return nil
}

// UpsertCluster 新建或更新某组下的一个集群，写回配置中心。
// c_id 为空时自动生成 4 位 ID；要求 c_nodes 非空且均为合法 host:port（自动去重）。
func (m *Manager) UpsertCluster(grpID string, cl NacosCluster) (*NacosCluster, error) {
	if len(cl.CNodes) == 0 {
		return nil, fmt.Errorf("c_nodes must not be empty")
	}
	nodes, err := validateNodes(cl.CNodes)
	if err != nil {
		return nil, err
	}
	cl.CNodes = nodes
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	gi := -1
	for i := range nc.NacosGroup {
		if nc.NacosGroup[i].GrpID == grpID {
			gi = i
			break
		}
	}
	if gi < 0 {
		return nil, notFoundf("group %s not found", grpID)
	}
	g := nc.NacosGroup[gi]
	ci := -1
	for i := range g.Clusters {
		if g.Clusters[i].CID == cl.CID {
			ci = i
			break
		}
	}
	if cl.CID == "" {
		cl.CID = genClusterIDGlobal(nc.NacosGroup)
	}
	if ci >= 0 {
		g.Clusters[ci] = cl
	} else {
		g.Clusters = append(g.Clusters, cl)
	}
	nc.NacosGroup[gi] = g
	if err := validateUniqueClusterIDs(nc.NacosGroup); err != nil {
		return nil, err
	}
	if err := m.Publish(nc); err != nil {
		return nil, err
	}
	m.store.Set(nc)
	return &cl, nil
}

// DeleteCluster 删除某组下的一个集群，写回配置中心。
func (m *Manager) DeleteCluster(grpID, cID string) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	gi := -1
	for i := range nc.NacosGroup {
		if nc.NacosGroup[i].GrpID == grpID {
			gi = i
			break
		}
	}
	if gi < 0 {
		return notFoundf("group %s not found", grpID)
	}
	g := nc.NacosGroup[gi]
	out := make([]NacosCluster, 0, len(g.Clusters))
	found := false
	for _, cl := range g.Clusters {
		if cl.CID == cID {
			found = true
			continue
		}
		out = append(out, cl)
	}
	if !found {
		return notFoundf("cluster %s not found in group %s", cID, grpID)
	}
	g.Clusters = out
	nc.NacosGroup[gi] = g
	// 级联清理 dubbo-sync 段里引用被删集群的条目。
	removeDubboEntriesForCluster(nc, cID)
	if err := m.Publish(nc); err != nil {
		return err
	}
	m.store.Set(nc)
	return nil
}

// SetConfigSync 设置某分组「配置同步」开关，写回配置中心并原子替换生效配置。
// enabled=true 表示该分组开启配置同步；新建分组时默认 false（见 UpsertGroup）。
// configSyncAnyEnabled 报告是否任一分组开启了「配置同步」开关（全局总闸语义）：
// 任一新分组开启即拉起所有新集群两两双向全量 config 同步。
func (c *Config) configSyncAnyEnabled() bool {
	for _, v := range c.ConfigSync {
		if v {
			return true
		}
	}
	return false
}

// newClusterIDs 返回所有新版本（GrpVersion=="3X"）集群的 c_id 列表。
// 老集群（2.x）不参与 config 同步，仅新集群（3.x）之间互相同步。
func (c *Config) newClusterIDs() []string {
	var ids []string
	seen := make(map[string]bool)
	for _, g := range c.NacosGroup {
		if g.GrpVersion != "3X" {
			continue
		}
		for _, cl := range g.Clusters {
			if cl.CID != "" && !seen[cl.CID] {
				seen[cl.CID] = true
				ids = append(ids, cl.CID)
			}
		}
	}
	return ids
}

// rebuildConfigLinks 依据 ConfigSync 开关与集群拓扑，重建 Links 中的 config 同步链路
// （保留非 config 类型链路不变）。全同步语义：任一新分组开关开启即拉起所有新集群两两
// 双向 config 同步（含组内集群互相同步），覆盖全部 namespace（由引擎遍历 c.Namespaces 决定）。
// 全部开关关闭时则不生成 config 链路（引擎停止同步）。
func (c *Config) rebuildConfigLinks() {
	kept := make([]SyncLink, 0, len(c.Links))
	for _, l := range c.Links {
		if l.Kind != "config" {
			kept = append(kept, l)
		}
	}
	if !c.configSyncAnyEnabled() {
		c.Links = kept
		return
	}
	ids := c.newClusterIDs()
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			kept = append(kept,
				SyncLink{From: ids[i], To: ids[j], Kind: "config", Enabled: true},
				SyncLink{From: ids[j], To: ids[i], Kind: "config", Enabled: true},
			)
		}
	}
	c.Links = kept
}

func (m *Manager) SetConfigSync(grpID string, enabled bool) error {
	if _, ok := m.GetGroup(grpID); !ok {
		return notFoundf("group %s not found", grpID)
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	if nc.ConfigSync == nil {
		nc.ConfigSync = map[string]bool{}
	}
	nc.ConfigSync[grpID] = enabled
	nc.rebuildConfigLinks() // 依据开关+拓扑重建 config 同步链路（全局总闸语义）
	if err := m.Publish(nc); err != nil {
		return err
	}
	m.store.Set(nc)
	m.fireRuntimeChange(nc) // 当次请求即生效，不依赖配置中心推送回传（对齐 publishServiceSync）
	return nil
}

// Reload 主动从配置中心重新拉取并原子替换生效配置。持 writeMu，与任何 CRUD 写路径互斥，
// 避免「手动重载」与「并发 POST 写回」各自的 store.Set 形成 TOCTOU 写丢失。
func (m *Manager) Reload() error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.reloadLocked()
}

// reloadLocked 实际执行「拉取→解析→替换」，调用方必须已持 writeMu。供 Reload 与 Reencrypt 复用，
// 避免 Reencrypt 在已持 writeMu 时再调 Reload 造成非可重入互斥锁死锁。
func (m *Manager) reloadLocked() error {
	content, err := m.cc.GetConfig(vo.ConfigParam{DataId: m.boot.Nacos.ConfigDataID, Group: m.group})
	if err != nil {
		return err
	}
	rc, err := m.parseRuntime(content)
	if err != nil {
		return err
	}
	m.store.Set(rc)
	return nil
}

// ===================== 服务同步（naming）Manager 方法 =====================

// ensureServiceSync 返回 c 的 service-sync 配置，若顶层为 nil 则初始化空结构（就地，不写回）。
func ensureServiceSync(c *Config) *ServiceSyncConfig {
	if c.ServiceSync == nil {
		c.ServiceSync = &ServiceSyncConfig{SingleSync: map[string]ServiceSyncEntry{}}
	}
	if c.ServiceSync.SingleSync == nil {
		c.ServiceSync.SingleSync = map[string]ServiceSyncEntry{}
	}
	return c.ServiceSync
}

// publishServiceSync 写回配置中心并原子替换生效配置；若 service-sync 段已空则整体置 nil，保持 YAML 整洁。
// 落盘后立即在本地触发一次运行时变化回调，使「启用/禁用」当次请求即生效，
// 不依赖配置中心 ListenConfig 推送回传（推送延迟或丢失会导致开关看似不生效）。
func (m *Manager) publishServiceSync(nc *Config) error {
	if nc.ServiceSync != nil && len(nc.ServiceSync.SingleSync) == 0 && len(nc.ServiceSync.MultipleSync) == 0 {
		nc.ServiceSync = nil
	}
	if err := m.Publish(nc); err != nil {
		return err
	}
	m.store.Set(nc)
	m.fireRuntimeChange(nc)
	return nil
}

// AddSingleSync 新增组内同步（一个分组）。该分组不可已存在于 single-sync（否则报错），
// 且必须存在对应的 nacos-group。enable 缺省 false，services 为需同步的服务名列表，
// namespace 限定同步范围所属命名空间（空 = public）。
func (m *Manager) AddSingleSync(grpID string, enable bool, services []string, namespace string) error {
	if grpID == "" {
		return fmt.Errorf("grp_id required")
	}
	services = dedupeStrings(services)
	if _, ok := m.GetGroup(grpID); !ok {
		return notFoundf("group %s not found", grpID)
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	ss := ensureServiceSync(nc)
	if _, exists := ss.SingleSync[grpID]; exists {
		return fmt.Errorf("group %s already has a single-sync entry", grpID)
	}
	ss.SingleSync[grpID] = ServiceSyncEntry{Enable: enable, Services: services, Namespace: namespace}
	return m.publishServiceSync(nc)
}

// UpdateSingleSync 更新组内同步的启用开关、服务列表与命名空间（grp_id 必须已存在）。
func (m *Manager) UpdateSingleSync(grpID string, enable bool, services []string, namespace string) error {
	services = dedupeStrings(services)
	if _, ok := m.GetGroup(grpID); !ok {
		return notFoundf("group %s not found", grpID)
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	ss := ensureServiceSync(nc)
	if _, exists := ss.SingleSync[grpID]; !exists {
		return notFoundf("single-sync entry for group %s not found", grpID)
	}
	ss.SingleSync[grpID] = ServiceSyncEntry{Enable: enable, Services: services, Namespace: namespace}
	return m.publishServiceSync(nc)
}

// DeleteSingleSync 删除组内同步。
func (m *Manager) DeleteSingleSync(grpID string) error {
	if grpID == "" {
		return fmt.Errorf("grp_id required")
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	ss := ensureServiceSync(nc)
	if _, exists := ss.SingleSync[grpID]; !exists {
		return notFoundf("single-sync entry for group %s not found", grpID)
	}
	delete(ss.SingleSync, grpID)
	return m.publishServiceSync(nc)
}

// pairKey 将两个分组 ID 规范为无序对（字典序较小的在前），用于组外同步去重。
func pairKey(a, b string) (string, string) {
	if a < b {
		return a, b
	}
	return b, a
}

// AddMultipleSync 新增组外同步（两个分组 A↔B）。两分组不可相同，且不可与已有条目重复
// （顺序无关：已有 A↔B 则不能再加 B↔A；但 A↔B 与 A↔C 不冲突）。
func (m *Manager) AddMultipleSync(a, b string, enable bool, services []string, namespace string) error {
	if a == "" || b == "" {
		return fmt.Errorf("source_a and source_b required")
	}
	if a == b {
		return fmt.Errorf("source_a and source_b must be different")
	}
	services = dedupeStrings(services)
	if _, ok := m.GetGroup(a); !ok {
		return notFoundf("group %s not found", a)
	}
	if _, ok := m.GetGroup(b); !ok {
		return notFoundf("group %s not found", b)
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	ss := ensureServiceSync(nc)
	ka, kb := pairKey(a, b)
	for _, e := range ss.MultipleSync {
		ea, eb := pairKey(e.SourceA, e.SourceB)
		if ea == ka && eb == kb {
			return fmt.Errorf("sync between %s and %s already exists", a, b)
		}
	}
	ss.MultipleSync = append(ss.MultipleSync, MultipleSyncEntry{
		SourceA:   a,
		SourceB:   b,
		Enable:    enable,
		Services:  services,
		Namespace: namespace,
	})
	return m.publishServiceSync(nc)
}

// UpdateMultipleSync 更新组外同步（按无序对定位已有条目）。
func (m *Manager) UpdateMultipleSync(a, b string, enable bool, services []string, namespace string) error {
	if a == "" || b == "" || a == b {
		return fmt.Errorf("invalid source pair")
	}
	services = dedupeStrings(services)
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	ss := ensureServiceSync(nc)
	ka, kb := pairKey(a, b)
	for i, e := range ss.MultipleSync {
		ea, eb := pairKey(e.SourceA, e.SourceB)
		if ea == ka && eb == kb {
			ss.MultipleSync[i] = MultipleSyncEntry{
				SourceA:   a,
				SourceB:   b,
				Enable:    enable,
				Services:  services,
				Namespace: namespace,
			}
			return m.publishServiceSync(nc)
		}
	}
	return notFoundf("multiple-sync entry between %s and %s not found", a, b)
}

// DeleteMultipleSync 删除组外同步（按无序对定位）。
func (m *Manager) DeleteMultipleSync(a, b string) error {
	if a == "" || b == "" {
		return fmt.Errorf("source_a and source_b required")
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	c := m.Get()
	nc := m.cloneForWrite(c)
	ss := ensureServiceSync(nc)
	ka, kb := pairKey(a, b)
	out := make([]MultipleSyncEntry, 0, len(ss.MultipleSync))
	found := false
	for _, e := range ss.MultipleSync {
		ea, eb := pairKey(e.SourceA, e.SourceB)
		if ea == ka && eb == kb {
			found = true
			continue
		}
		out = append(out, e)
	}
	if !found {
		return notFoundf("multiple-sync entry between %s and %s not found", a, b)
	}
	ss.MultipleSync = out
	return m.publishServiceSync(nc)
}

// encTokenPrefix ENC(...) 令牌前缀；用于判断敏感字段是否已加密，避免重复加密。
const encTokenPrefix = "ENC("

// PreviewEncrypted 返回「将写回配置中心」的加密后 YAML 文本（敏感字段 auth_password、grp_dsn
// 以 ENC(...) 包裹），不写盘，仅供确认落盘形态。ENC(...) 为密文，不含明文口令，可安全展示。
// secret-key 未配置时返回错误。
func (m *Manager) PreviewEncrypted() (string, error) {
	if m.boot.SecretKey == "" {
		return "", fmt.Errorf("secret-key not configured, cannot preview encrypted form")
	}
	enc, err := m.Get().EncryptSecrets(m.boot.SecretKey)
	if err != nil {
		return "", err
	}
	data, err := enc.Marshal()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Reencrypt 将当前生效配置（解密后的明文）重新加密并写回配置中心，确保存量明文被 ENC() 包裹。
// 写回前先 Reload 一次，确保拿到配置中心最新版本（避免覆盖并发修改）。
// secret-key 为空时返回错误（无可加密密钥）；若配置中心当前内容已全是 ENC()，则幂等（重新生成密文，明文不变）。
// 全程持 writeMu，避免与并发的其它写操作（UpsertGroup/DeleteGroup/...）竞争导致写丢失。
func (m *Manager) Reencrypt() error {
	if m.boot.SecretKey == "" {
		return fmt.Errorf("secret-key not configured, cannot encrypt")
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if err := m.reloadLocked(); err != nil {
		return fmt.Errorf("reload before reencrypt: %w", err)
	}
	return m.Publish(m.Get())
}

// Publish 将运行时配置写回配置中心（持久化配置项变更）。
// 写回前若配置了 secret-key，会把敏感字段（auth_password、grp_dsn）重新加密为 ENC(...)，
// 确保配置中心落盘内容不含明文口令。
func (m *Manager) Publish(rc *Config) error {
	toWrite := rc
	if m.boot.SecretKey != "" {
		enc, err := rc.EncryptSecrets(m.boot.SecretKey)
		if err != nil {
			return err
		}
		toWrite = enc
	}
	data, err := toWrite.Marshal()
	if err != nil {
		return err
	}
	ok, err := m.cc.PublishConfig(vo.ConfigParam{
		DataId:  m.boot.Nacos.ConfigDataID,
		Group:   m.group,
		Content: string(data),
		Type:    "yaml", // 配置中心以 YAML 格式存储（否则默认 TEXT），保持控制台语法高亮与可读性
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("publish runtime config returned false (dataId=%s)", m.boot.Nacos.ConfigDataID)
	}
	return nil
}

// UpdateScope 原地更新命名范围，发布回配置中心并原子替换生效配置。
// 持 writeMu，避免与并发写操作竞争导致「发布旧配置覆盖他人修改」的写丢失（TOCTOU）。
func (m *Manager) UpdateScope(name string, sc SyncScope) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if err := m.store.UpdateScope(name, sc); err != nil {
		return err
	}
	return m.Publish(m.store.Get())
}

// Close 取消配置监听。
func (m *Manager) Close() {
	_ = m.cc.CancelListenConfig(vo.ConfigParam{DataId: m.boot.Nacos.ConfigDataID, Group: m.group})
}
