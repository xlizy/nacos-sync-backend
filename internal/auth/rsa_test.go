package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"strings"
	"testing"
)

// clientEncryptPEM 模拟前端：用服务端公钥 PEM（SPKI）以 RSA-OAEP/SHA-256 加密，返回 base64 密文。
// 与前端 Web Crypto 使用相同算法参数，确保前后端对齐。
func clientEncryptPEM(pubPEM, plaintext string) (string, error) {
	block, _ := pem.Decode([]byte(pubPEM))
	if block == nil {
		return "", errDemo("not PEM")
	}
	pubIfc, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return "", err
	}
	pub, ok := pubIfc.(*rsa.PublicKey)
	if !ok {
		return "", errDemo("not RSA public key")
	}
	ct, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, []byte(plaintext), nil)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ct), nil
}

type errDemo string

func (e errDemo) Error() string { return string(e) }

// TestCredCipherRoundtrip 验证 RSA-OAEP 加解密往返：前端公钥加密、服务端私钥解密后内容一致。
func TestCredCipherRoundtrip(t *testing.T) {
	c, err := NewCredCipher("")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	pub := c.PublicKeyPEM()
	if !strings.Contains(pub, "BEGIN PUBLIC KEY") {
		t.Fatalf("public key PEM malformed: %q", pub)
	}
	for _, plain := range []string{"hunter2", "123456", "a-very-long-password-!@#$%^&*()_+-=", ""} {
		encB64, e := clientEncryptPEM(pub, plain)
		if e != nil {
			t.Fatalf("client encrypt %q: %v", plain, e)
		}
		got, e := c.Decrypt(encB64)
		if e != nil {
			t.Fatalf("decrypt %q: %v", plain, e)
		}
		if got != plain {
			t.Fatalf("roundtrip mismatch: got %q want %q", got, plain)
		}
	}
}

// TestCredCipherLoadFromPEM 验证从配置私钥 PEM 加载（多实例共享密钥场景）。
func TestCredCipherLoadFromPEM(t *testing.T) {
	c0, _ := NewCredCipher("")
	privPEM := exportPrivatePEM(c0.priv)
	c1, err := NewCredCipher(privPEM)
	if err != nil {
		t.Fatalf("load from PEM: %v", err)
	}
	// 用 c1 公钥加密、c1 私钥解密，且 c1 与 c0 应为同一密钥对（公钥一致）。
	if c1.PublicKeyPEM() != c0.PublicKeyPEM() {
		t.Fatalf("loaded cipher public key differs from source")
	}
	encB64, _ := clientEncryptPEM(c1.PublicKeyPEM(), "secret")
	if got, _ := c1.Decrypt(encB64); got != "secret" {
		t.Fatalf("decrypt with loaded cipher failed: %q", got)
	}
}

// exportPrivatePEM 把私钥导出为 PKCS#8 PEM（仅供测试构造配置片段）。
func exportPrivatePEM(priv *rsa.PrivateKey) string {
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}
