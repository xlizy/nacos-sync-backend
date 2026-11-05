// Package nacos 统一对接 Nacos 2.1.2（无鉴权）与 3.2.4（有鉴权），
// 按 (clusterID, namespace) 缓存 SDK 客户端，处理 namespace 透传与按需创建。
package nacos

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/cxcm/nacos-sync-backend/internal/config"
)

// NamespaceCount 一个命名空间及其配置数量（用于配置同步页概览）。
type NamespaceCount struct {
	Namespace         string `json:"namespace"`         // 命名空间 ID；"" 表示 public
	NamespaceShowName string `json:"namespaceShowName"` // 展示名
	ConfigCount       int    `json:"configCount"`       // 该命名空间下配置文件数量
}

// ConfigOverviewSpec 拉取某节点配置概览所需的连接参数。
type ConfigOverviewSpec struct {
	Address    string // host:port，取自分组选定的集群节点
	GrpVersion string // 分组 Nacos 版本（"2X" | "3X"）；为空表示未设置，退化为探测
	EnableAuth bool   // 该分组是否开启鉴权
	Username   string // 分组鉴权用户名
	Password   string // 分组鉴权密码（可能含 ${ENV} 占位符）
}

// ConfigOverviewResult 版本感知的配置概览结果。
type ConfigOverviewResult struct {
	Namespaces []NamespaceCount `json:"namespaces"`
	Mode       string           `json:"mode"`    // "http" | "grpc"：实际使用的取数通道
	Version    string           `json:"version"` // 分组 Nacos 版本；取分组 grp_version；未设置时退化为探测结果，探测仍失败则为 ""
	Warning    string           `json:"warning"` // 补充说明（如 3.x 命名空间枚举失败降级为仅 public）
}

// loginToken 用用户名/密码登录目标 Nacos，返回 accessToken（仅 EnableAuth 时需要）。
func loginToken(address, user, pass string) (string, error) {
	host, port := config.SplitHostPort(address)
	u := fmt.Sprintf("http://%s:%d/nacos/v1/auth/login", host, port)
	form := url.Values{}
	form.Set("username", expandEnv(user))
	form.Set("password", expandEnv(pass))
	req, err := http.NewRequest(http.MethodPost, u, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// Proxy:nil 强制直连，避免从启动环境继承的 HTTP 代理把内网 Nacos 目标转成 502。
	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
	resp, err := cli.Do(req)
	if err != nil {
		return "", fmt.Errorf("登录请求失败（节点不可达？）：%w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("登录失败：HTTP %d（用户名或密码错误）", resp.StatusCode)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err == nil {
		if t, ok := m["accessToken"].(string); ok && t != "" {
			return t, nil
		}
		if t, ok := m["token"].(string); ok && t != "" {
			return t, nil
		}
	}
	return "", fmt.Errorf("登录成功但无法解析 token")
}

// DetectVersion 探测节点 Nacos 服务端版本，用于决定取数通道（HTTP/gRPC）。
//
//   - 2.x：GET /nacos/v1/console/server/state 返回 {"version":"2.1.2",...}（匿名可访问）；
//   - 3.x：该端点已移除（404）。改探 3.x 专属的 admin 接口
//     GET /nacos/v3/admin/core/cluster/node/self（起始版本 3.0.0，需鉴权），
//     其 data.extendInfo.version 即节点 Server 版本（如 "3.2.4"）。
//
// 返回 (version, is3x, err)：
//   - 2.x 命中 → version="2.1.2"，is3x=false；
//   - 3.x admin 接口命中 → version="3.2.4"，is3x=true（真实版本，不再只推断）；
//   - 两接口都拿不到版本（如 3.x 未带鉴权被 401、或节点不可达）→ version=""，is3x=true（保守推断）；
//   - 2.x 端点网络不可达（odefined err）→ err 非 nil，交由上层按节点不可达处理。
func DetectVersion(spec ConfigOverviewSpec) (version string, is3x bool, err error) {
	host, port := config.SplitHostPort(spec.Address)
	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}

	// ① 先探 2.x 端点（匿名）。
	stateURL := fmt.Sprintf("http://%s:%d/nacos/v1/console/server/state", host, port)
	sreq, _ := http.NewRequest(http.MethodGet, stateURL, nil)
	sresp, derr := cli.Do(sreq)
	if derr != nil {
		return "", false, fmt.Errorf("探测版本失败（节点不可达？）：%w", derr)
	}
	sbody, _ := io.ReadAll(sresp.Body)
	sresp.Body.Close()
	if sresp.StatusCode == 200 {
		var m struct {
			Version string `json:"version"`
		}
		if json.Unmarshal(sbody, &m) == nil && m.Version != "" {
			return m.Version, strings.HasPrefix(m.Version, "3"), nil
		}
	}

	// ② 2.x 端点非 200（多为 3.x 的 404）→ 试 3.x admin self 接口（需鉴权）。
	token := ""
	if spec.EnableAuth {
		if t, lerr := loginToken(spec.Address, spec.Username, spec.Password); lerr == nil {
			token = t
		}
	}
	selfURL := fmt.Sprintf("http://%s:%d/nacos/v3/admin/core/cluster/node/self", host, port)
	if token != "" {
		selfURL += "?accessToken=" + token
	}
	creq, _ := http.NewRequest(http.MethodGet, selfURL, nil)
	cresp, cerr := cli.Do(creq)
	if cerr != nil {
		// 连不上，但 2.x 端点也失败了 → 保守推断 3.x。
		return "", true, nil
	}
	defer cresp.Body.Close()
	cbody, _ := io.ReadAll(cresp.Body)
	if cresp.StatusCode == 200 {
		var m struct {
			Data struct {
				ExtendInfo struct {
					Version string `json:"version"`
				} `json:"extendInfo"`
			} `json:"data"`
		}
		if json.Unmarshal(cbody, &m) == nil && m.Data.ExtendInfo.Version != "" {
			v := m.Data.ExtendInfo.Version
			return v, strings.HasPrefix(v, "3"), nil
		}
	}
	// 两接口都没拿到版本 → 保守推断为 3.x。
	return "", true, nil
}

// cfgClientCache 按 address|namespace 缓存 gRPC 配置客户端，避免每次概览请求都新建
// 长连接（SDK 客户端会起后台 gRPC/健康检查 goroutine，不缓存会泄漏）。
var cfgClientCache sync.Map

// grpcConfigClient 按节点地址（而非 clusterID）构建一个 gRPC 配置客户端并缓存。
func grpcConfigClient(address string, enableAuth bool, username, password, namespace string) (config_client.IConfigClient, error) {
	key := address + "|" + namespace
	if c, ok := cfgClientCache.Load(key); ok {
		return c.(config_client.IConfigClient), nil
	}
	host, port := config.SplitHostPort(address)
	cc := constant.ClientConfig{
		NamespaceId:         namespace,
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogLevel:            "warn",
		LogDir:              "/tmp/nacos-sync-config-overview/log",
		CacheDir:            "/tmp/nacos-sync-config-overview/cache",
	}
	if enableAuth {
		cc.Username = expandEnv(username)
		cc.Password = expandEnv(password)
	}
	cli, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig: &cc,
		ServerConfigs: []constant.ServerConfig{
			{IpAddr: host, Port: port, ContextPath: "/nacos"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("构建 gRPC 配置客户端失败：%w", err)
	}
	actual, _ := cfgClientCache.LoadOrStore(key, cli)
	return actual.(config_client.IConfigClient), nil
}

// FetchNamespacesHTTP 通过 HTTP OpenAPI 列出节点上的命名空间及其配置数量。
// 仅适用于 Nacos 2.x（/nacos/v1/console/namespaces 直接返回每个命名空间的 configCount）。
func FetchNamespacesHTTP(spec ConfigOverviewSpec) ([]NamespaceCount, error) {
	host, port := config.SplitHostPort(spec.Address)
	base := fmt.Sprintf("http://%s:%d/nacos", host, port)
	token := ""
	if spec.EnableAuth {
		t, err := loginToken(spec.Address, spec.Username, spec.Password)
		if err != nil {
			return nil, err
		}
		token = t
	}
	u := base + "/v1/console/namespaces"
	if token != "" {
		u += "?accessToken=" + token
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	cli := &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{Proxy: nil}}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, fmt.Errorf("获取命名空间列表失败（节点不可达？）：%w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		// 3.x 移除了该 HTTP 接口，Spring 风格 404 会带 "No endpoint GET ..."。
		return nil, fmt.Errorf("获取命名空间列表失败：HTTP %d；该节点可能是 Nacos 3.x（已移除 /nacos/v1/console OpenAPI，改走 gRPC），暂不支持通过 HTTP 取数", resp.StatusCode)
	}
	var out struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    []struct {
			Namespace         string `json:"namespace"`
			NamespaceShowName string `json:"namespaceShowName"`
			ConfigCount       int    `json:"configCount"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("解析命名空间列表失败：%w", err)
	}
	if out.Code != 200 && out.Message != "" {
		return nil, fmt.Errorf("命名空间列表返回错误：%s", out.Message)
	}
	res := make([]NamespaceCount, 0, len(out.Data))
	for _, d := range out.Data {
		res = append(res, NamespaceCount{
			Namespace:         d.Namespace,
			NamespaceShowName: d.NamespaceShowName,
			ConfigCount:       d.ConfigCount,
		})
	}
	return res, nil
}

// FetchNamespacesV3 枚举 Nacos 3.x 集群的命名空间列表（含每个命名空间的 configCount）。
//
// 3.x 移除了 2.x 的 /nacos/v1/console/namespaces，改由运维/控制台 API 提供：
//   - GET /nacos/v3/admin/core/namespace/list  （需管理员权限）
//   - GET /nacos/v3/console/core/namespace/list（任意有效身份）
//
// 依次尝试，任一成功即返回；全部失败返回错误，由上层降级为「仅 public（gRPC）」。
// 响应为 Nacos 统一返回体：{"code":0,"message":"success","data":[{namespace, namespaceShowName,
// namespaceDesc, quota, configCount, type}, ...]}。public 的 namespace 可能为 "" 或 "public"，
// 统一归一化为 ""（与 SDK/2.x 语义一致，前端据此展示 "public"）。
func FetchNamespacesV3(spec ConfigOverviewSpec) ([]NamespaceCount, error) {
	host, port := config.SplitHostPort(spec.Address)
	token := ""
	if spec.EnableAuth {
		t, err := loginToken(spec.Address, spec.Username, spec.Password)
		if err != nil {
			return nil, err
		}
		token = t
	}
	cli := &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{Proxy: nil}}

	var lastErr error
	for _, path := range []string{
		"/nacos/v3/admin/core/namespace/list",
		"/nacos/v3/console/core/namespace/list",
	} {
		u := fmt.Sprintf("http://%s:%d%s", host, port, path)
		if token != "" {
			u += "?accessToken=" + url.QueryEscape(token)
		}
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			lastErr = err
			continue
		}
		if token != "" {
			req.Header.Set("accessToken", token)
		}
		resp, err := cli.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s 请求失败：%w", path, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("%s 返回 HTTP %d", path, resp.StatusCode)
			continue
		}
		var out struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Data    []struct {
				Namespace         string `json:"namespace"`
				NamespaceShowName string `json:"namespaceShowName"`
				ConfigCount       int    `json:"configCount"`
				Type              int    `json:"type"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			lastErr = fmt.Errorf("%s 响应解析失败：%w", path, err)
			continue
		}
		if out.Code != 0 && out.Code != 200 {
			lastErr = fmt.Errorf("%s 返回 code=%d message=%s", path, out.Code, out.Message)
			continue
		}
		res := make([]NamespaceCount, 0, len(out.Data))
		for _, d := range out.Data {
			id := d.Namespace
			if id == "public" {
				id = ""
			}
			show := d.NamespaceShowName
			if show == "" && id == "" {
				show = "public"
			}
			res = append(res, NamespaceCount{Namespace: id, NamespaceShowName: show, ConfigCount: d.ConfigCount})
		}
		if len(res) == 0 {
			lastErr = fmt.Errorf("%s 返回空命名空间列表", path)
			continue
		}
		return res, nil
	}
	return nil, fmt.Errorf("获取 Nacos 3.x 命名空间列表失败：%v", lastErr)
}

// FetchNamespacesGRPC 通过 gRPC（nacos-sdk-go SearchConfig）读取「public」命名空间的配置数量。
// 这是 Nacos 3.x 的降级通道：仅当 FetchNamespacesV3（HTTP 枚举全部命名空间）失败时使用。
// 受 SDK 能力限制：只能以「已知 tenant」为入口、无法枚举命名空间，故固定查询 public（tenant=""）。
func FetchNamespacesGRPC(spec ConfigOverviewSpec) ([]NamespaceCount, error) {
	cli, err := grpcConfigClient(spec.Address, spec.EnableAuth, spec.Username, spec.Password, "")
	if err != nil {
		return nil, err
	}
	page, err := cli.SearchConfig(vo.SearchConfigParam{
		Search: "blur", DataId: "", Group: "", PageNo: 1, PageSize: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC 查询配置失败：%w", err)
	}
	return []NamespaceCount{
		{Namespace: "", NamespaceShowName: "public", ConfigCount: page.TotalCount},
	}, nil
}

// FetchConfigOverview 按分组设置的 Nacos 版本选择取数通道：
//   - grp_version="3X" → 优先 HTTP 枚举全部命名空间（/v3/{admin,console}/core/namespace/list，含 configCount）；
//     失败（凭据非管理员 / 接口不可用）时降级为 gRPC 仅取 public 配置数（见 FetchNamespacesGRPC）；
//   - grp_version="2X"（或空且探测不到 3.x）→ HTTP 全量命名空间列表（含每个命名空间的 configCount）。
//
// 不再默认逐节点探测版本；仅当分组未显式设置 grp_version（历史配置尚未迁移）时，
// 退化为 DetectVersion 一次以保持兼容——设置 grp_version 后此兜底分支不再触发。
func FetchConfigOverview(spec ConfigOverviewSpec) (ConfigOverviewResult, error) {
	grpVersion := spec.GrpVersion
	useGrpc := grpVersion == "3X"
	// 兼容期：分组未设置版本时探测一次决定通道（探测失败则保守走 HTTP）。
	if grpVersion == "" {
		if ver, is3x, derr := DetectVersion(spec); derr == nil {
			if is3x {
				useGrpc = true
			}
			grpVersion = ver
		}
	}
	if useGrpc {
		// mode 恒为 "grpc"：这是前端判定「Nacos 3.x / 支持配置同步」的语义标记，
		// 与实际取数走 HTTP 还是 gRPC 无关（改成别的值会把 3.x 的同步开关误置灰）。
		if ns, e := FetchNamespacesV3(spec); e == nil {
			res := ConfigOverviewResult{
				Namespaces: ns,
				Mode:       "grpc",
				Version:    grpVersion,
			}
			if spec.GrpVersion == "" {
				res.Warning = "该分组未设置 Nacos 版本（grp_version），已按探测结果走 3.x 通道；建议到「集群目录」设置分组版本。"
			}
			return res, nil
		}
		ns, e := FetchNamespacesGRPC(spec)
		if e != nil {
			return ConfigOverviewResult{}, e
		}
		warn := "Nacos 3.x 命名空间枚举接口调用失败（需管理员权限），已降级为仅通过 gRPC 返回 public 命名空间的配置数量；其它命名空间需在其同步配置中显式列出 ID 后再取数。"
		if spec.GrpVersion == "" {
			warn = "该分组未设置 Nacos 版本（grp_version），已按探测结果兜底走 gRPC；建议到「集群目录」设置分组版本。" + warn
		}
		return ConfigOverviewResult{
			Namespaces: ns,
			Mode:       "grpc",
			Version:    grpVersion,
			Warning:    warn,
		}, nil
	}
	ns, e := FetchNamespacesHTTP(spec)
	if e != nil {
		return ConfigOverviewResult{}, e
	}
	return ConfigOverviewResult{
		Namespaces: ns,
		Mode:       "http",
		Version:    grpVersion,
	}, nil
}
