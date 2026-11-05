// Command secret 是运行时配置 ENC() 加密串的生成/校验工具。
//
// 用法：
//
//	# 加密（从引导配置读 secret-key）
//	go run ./cmd/secret -config configs/config.yaml -value "明文口令"
//
//	# 加密（直接指定密钥）
//	go run ./cmd/secret -key "your-secret" -value "明文口令"
//
//	# 从管道读取明文
//	echo "明文口令" | go run ./cmd/secret -config configs/config.yaml
//
//	# 反向解密校验（不会落到任何配置，仅本地验证）
//	echo "ENC(...)" | go run ./cmd/secret -config configs/config.yaml -decrypt
//
// 输出为标准 ENC(<base64>) 令牌，可直接粘进 nacos-sync-backend-runtime.yaml 的
// auth_password / grp_dsn 等字段。算法与 internal/crypto 完全一致（AES-256-GCM）。
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cxcm/nacos-sync-backend/internal/config"
	"github.com/cxcm/nacos-sync-backend/internal/crypto"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "引导配置路径（读取其中的 secret-key）")
	key := flag.String("key", "", "secret-key 覆盖（否则从 -config 读取）")
	value := flag.String("value", "", "待加密明文（缺省时从 stdin 读取）")
	decrypt := flag.Bool("decrypt", false, "反向解密模式：把 stdin 的 ENC(...) 还原为明文")
	flag.Parse()

	var secret string
	if *key != "" {
		secret = *key
	} else {
		b, err := config.LoadBootstrap(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "读取引导配置失败:", err)
			os.Exit(1)
		}
		secret = b.SecretKey
	}
	if secret == "" {
		fmt.Fprintln(os.Stderr, "secret-key 为空：请用 -key 指定或从 -config 读取")
		os.Exit(1)
	}

	input := *value
	if input == "" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintln(os.Stderr, "读取 stdin 失败:", err)
			os.Exit(1)
		}
		input = strings.TrimRight(string(data), "\n")
	}
	if input == "" {
		fmt.Fprintln(os.Stderr, "待处理内容为空")
		os.Exit(1)
	}

	if *decrypt {
		out, err := crypto.Decrypt(secret, strings.TrimSpace(input))
		if err != nil {
			fmt.Fprintln(os.Stderr, "解密失败:", err)
			os.Exit(1)
		}
		fmt.Println(out)
		return
	}

	enc, err := crypto.Encrypt(secret, input)
	if err != nil {
		fmt.Fprintln(os.Stderr, "加密失败:", err)
		os.Exit(1)
	}
	fmt.Println(enc)
}
