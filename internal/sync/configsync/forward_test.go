package configsync

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/config"
)

type forwardConfigClient struct {
	config_client.IConfigClient
	mu       sync.Mutex
	content   string
	getErr    error
	published []string
	deleted   int
}

func (c *forwardConfigClient) GetConfig(vo.ConfigParam) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.content, c.getErr
}

func (c *forwardConfigClient) PublishConfig(p vo.ConfigParam) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.published = append(c.published, p.Content)
	c.content = p.Content
	return true, nil
}

func (c *forwardConfigClient) DeleteConfig(vo.ConfigParam) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted++
	c.content = ""
	return true, nil
}

func TestForwardUsesCurrentSourceInsteadOfStaleCallbackPayload(t *testing.T) {
	src := &forwardConfigClient{content: "newest-source-value"}
	tgt := &forwardConfigClient{content: "old-target-value"}
	e := &Engine{logger: zap.NewNop()}

	err := e.forward(
		context.Background(),
		config.SyncLink{From: "source", To: "target", Kind: "config"},
		"namespace", "DEFAULT_GROUP", "application.yaml",
		"stale-listener-payload", src, tgt,
	)
	if err != nil {
		t.Fatalf("forward returned error: %v", err)
	}
	if len(tgt.published) != 1 {
		t.Fatalf("publish count = %d, want 1", len(tgt.published))
	}
	if got := tgt.published[0]; got != "newest-source-value" {
		t.Fatalf("published stale callback payload %q, want current source value", got)
	}
}


func TestForwardDoesNotPublishStalePayloadWhenSourceReadFails(t *testing.T) {
	src := &forwardConfigClient{content: "current-but-unverified", getErr: errors.New("source unavailable")}
	tgt := &forwardConfigClient{content: "old-target-value"}
	e := &Engine{logger: zap.NewNop()}

	err := e.forward(
		context.Background(),
		config.SyncLink{From: "source", To: "target", Kind: "config"},
		"namespace", "DEFAULT_GROUP", "application.yaml",
		"stale-listener-payload", src, tgt,
	)
	if err == nil {
		t.Fatal("expected source read failure")
	}
	if len(tgt.published) != 0 || tgt.deleted != 0 {
		t.Fatalf("published/deleted despite source read failure: published=%v deleted=%d", tgt.published, tgt.deleted)
	}
}


func TestIsEchoConsumesMatchingRecordOnlyOnce(t *testing.T) {
	e := &Engine{}
	e.recordPush("target", "source", "namespace", "DEFAULT_GROUP", "app.yaml", "echo-content")

	const callers = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	var consumed atomic.Int32
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if e.isEcho("source", "target", "namespace", "DEFAULT_GROUP", "app.yaml", "echo-content") {
				consumed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := consumed.Load(); got != 1 {
		t.Fatalf("matching echo record consumed by %d callbacks, want exactly 1", got)
	}
}

func TestIsEchoDoesNotConsumeDifferentContent(t *testing.T) {
	e := &Engine{}
	e.recordPush("target", "source", "namespace", "DEFAULT_GROUP", "app.yaml", "expected-content")

	if e.isEcho("source", "target", "namespace", "DEFAULT_GROUP", "app.yaml", "different-content") {
		t.Fatal("different content must not be treated as an echo")
	}
	if !e.isEcho("source", "target", "namespace", "DEFAULT_GROUP", "app.yaml", "expected-content") {
		t.Fatal("mismatched event must not consume the pending echo record")
	}
}
