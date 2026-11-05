// Package crypto 提供运行时配置中 ENC(...) 加密串的 AES-256-GCM 加解密能力。
//
// 实现思路与 common-go/utils/common_crypto 的 AesEncryptGCM/AesDecryptGCM 保持一致（字节级兼容）：
//   - 密钥：由 secret-key 经 generateKey 折成 32 字节 AES-256 密钥（任意长度 key 按位 XOR 折叠，
//     不是 SHA-256）。这样与 common-go 用同一 secret-key 生成的密文可互相解密。
//   - 密文格式：base64( nonce[12] || ciphertext || tag )，nonce 随机 12 字节并前置（与 common-go 一致）。
//   - 令牌格式：ENC(<base64>)；解析 YAML 前把内容中所有 ENC(...) 还原为明文。
//     Decrypt 同时兼容裸 base64（common-go 原始输出）与 ENC(...) 包裹两种入参。
//   - 发布回配置中心前，再把敏感字段（auth_password、grp_dsn）重新加密为 ENC(...)，避免明文落盘。
//   - 解密失败（密文被篡改 / 错误 key / 格式不对）返回 error，由调用方处理，不静默返回空串。
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
)

const (
	encPrefix = "ENC("
	encSuffix = ")"
)

// generateKey 由任意长度 keyStr 折成 32 字节 AES-256 密钥（与 common-go 的 generateKey 字节级一致）：
// 前 32 字节直接拷贝，超出部分按位 XOR 折叠进 32 字节循环缓冲。
func generateKey(key []byte) []byte {
	genKey := make([]byte, 32)
	copy(genKey, key)
	for i := 32; i < len(key); {
		for j := 0; j < 32 && i < len(key); j, i = j+1, i+1 {
			genKey[j] ^= key[i]
		}
	}
	return genKey
}

// Encrypt 以 AES-256-GCM 加密明文，返回 ENC(<base64>) 形式令牌。明文为空时返回空串。
func Encrypt(secret, plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	block, err := aes.NewCipher(generateKey([]byte(secret)))
	if err != nil {
		return "", fmt.Errorf("aes new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("new gcm: %w", err)
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("rand nonce: %w", err)
	}
	ct := aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return encPrefix + base64.StdEncoding.EncodeToString(ct) + encSuffix, nil
}

// Decrypt 解密 AES-256-GCM 密文。token 可为裸 base64（common-go 原始输出）或 ENC(<base64>) 包裹形式。
func Decrypt(secret, token string) (string, error) {
	tok := token
	if strings.HasPrefix(token, encPrefix) && strings.HasSuffix(token, encSuffix) {
		tok = token[len(encPrefix) : len(token)-len(encSuffix)]
	}
	ct, err := decodeB64(tok)
	if err != nil {
		return "", fmt.Errorf("decode ciphertext: %w", err)
	}
	block, err := aes.NewCipher(generateKey([]byte(secret)))
	if err != nil {
		return "", fmt.Errorf("aes new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("new gcm: %w", err)
	}
	ns := aead.NonceSize()
	if len(ct) < ns {
		return "", fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := ct[:ns], ct[ns:]
	plain, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt (wrong key or corrupted): %w", err)
	}
	return string(plain), nil
}

// decodeB64 优先标准 base64，失败回退 URL-safe（兼容不同生成工具）。
func decodeB64(s string) ([]byte, error) {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.URLEncoding.DecodeString(s)
}

// DecryptYAML 解析前将内容中所有 ENC(...) 令牌解密为明文，并按需加双引号保证 YAML 合法。
//   - 内容不含 ENC() 时原样返回（向后兼容普通配置）；
//   - 含 ENC() 但 secret 为空时返回错误（配置错误，不应静默放行）。
func DecryptYAML(secret, raw string) (string, error) {
	if !strings.Contains(raw, encPrefix) {
		return raw, nil
	}
	if secret == "" {
		return "", fmt.Errorf("runtime config contains ENC() but secret-key is empty")
	}
	var b strings.Builder
	i := 0
	for i < len(raw) {
		idx := strings.Index(raw[i:], encPrefix)
		if idx < 0 {
			b.WriteString(raw[i:])
			break
		}
		start := i + idx
		// 定位闭合的 )
		end := strings.IndexByte(raw[start:], ')')
		if end < 0 {
			b.WriteString(raw[start:])
			break
		}
		close := start + end
		token := raw[start : close+1]

		// 兼容 "ENC(...)" 与 ENC(...) 两种写法：若两侧恰为双引号则整体替换。
		before := start - 1
		after := close + 1
		quoted := before >= 0 && raw[before] == '"' && after < len(raw) && raw[after] == '"'

		plain, err := Decrypt(secret, token)
		if err != nil {
			return "", err
		}
		// 双引号包裹明文，转义内部反斜杠与双引号，确保任意明文都是合法 YAML 标量。
		safe := `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(plain) + `"`
		if quoted {
			b.WriteString(raw[i : start-1]) // 不含开头引号
			b.WriteString(safe)
			i = after + 1 // 跳过结尾引号
		} else {
			b.WriteString(raw[i:start])
			b.WriteString(safe)
			i = close + 1
		}
	}
	return b.String(), nil
}
