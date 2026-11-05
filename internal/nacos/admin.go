package nacos

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cxcm/nacos-sync-backend/internal/config"
)

// RefreshLocalCache 触发单个 Nacos 节点（host:port）的服务端本地缓存更新：
//   POST /nacos/v3/admin/cs/ops/localCache
//
// 实测该端点仅接受 POST（GET 返回 405），且 Nacos 3.x 默认需 admin 鉴权。
// 仅对 3.X 分组调用；鉴权凭据取分组 AuthUsername/AuthPassword（已展开 ${ENV}）。
// 2.X 分组无此 admin 接口，调用方应在上层过滤掉（不传入）。
func RefreshLocalCache(nodeAddr string, grp config.NacosGroup) error {
	token, err := loginNode(nodeAddr, grp)
	if err != nil {
		return err
	}
	u := fmt.Sprintf("http://%s/nacos/v3/admin/cs/ops/localCache", nodeAddr)
	if token != "" {
		u += "?accessToken=" + url.QueryEscape(token)
	}
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Post(u, "application/x-www-form-urlencoded", strings.NewReader(""))
	if err != nil {
		return fmt.Errorf("refresh localCache %s: %w", nodeAddr, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("refresh localCache %s: HTTP %d: %s", nodeAddr, resp.StatusCode, string(body))
	}
	// Nacos 统一返回体：{"code":0,"message":"success","data":null}
	var r struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &r); err == nil && r.Code != 0 {
		return fmt.Errorf("refresh localCache %s: code %d: %s", nodeAddr, r.Code, r.Message)
	}
	return nil
}

// loginNode 用分组凭据登录拿 accessToken；未开启鉴权时返回空 token（部分部署可能仍需）。
func loginNode(nodeAddr string, grp config.NacosGroup) (string, error) {
	if !grp.EnableAuth {
		return "", nil
	}
	u := fmt.Sprintf("http://%s/nacos/v1/auth/login", nodeAddr)
	form := url.Values{}
	form.Set("username", expandEnv(grp.AuthUsername))
	form.Set("password", expandEnv(grp.AuthPassword))
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Post(u, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("login %s: %w", nodeAddr, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("login %s: HTTP %d", nodeAddr, resp.StatusCode)
	}
	var r struct {
		AccessToken string `json:"accessToken"`
		Token       string `json:"token"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return "", fmt.Errorf("parse login resp %s: %w", nodeAddr, err)
	}
	if r.AccessToken != "" {
		return r.AccessToken, nil
	}
	if r.Token != "" {
		return r.Token, nil
	}
	return "", fmt.Errorf("login %s: no token in response", nodeAddr)
}
