package configsync

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/config"
)

type getResult struct {
	content string
	err     error
}

type retrySourceClient struct {
	config_client.IConfigClient
	mu      sync.Mutex
	results []getResult
	calls   int
}

func (c *retrySourceClient) GetConfig(vo.ConfigParam) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if len(c.results) == 0 {
		return "", errors.New("unexpected GetConfig call")
	}
	r := c.results[0]
	c.results = c.results[1:]
	return r.content, r.err
}

type retryTargetClient struct {
	config_client.IConfigClient
	mu       sync.Mutex
	failNext bool
	current  string
	published []string
}

func (c *retryTargetClient) GetConfig(vo.ConfigParam) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current, nil
}

func (c *retryTargetClient) PublishConfig(p vo.ConfigParam) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.published = append(c.published, p.Content)
	if c.failNext {
		c.failNext = false
		return false, errors.New("temporary target failure")
	}
	return true, nil
}

func TestForwardRefreshesSourceBeforeFirstPublishAndRetry(t *testing.T) {
	src := &retrySourceClient{results: []getResult{
		{content: "fresh-source-value"},
		{content: "newer-source-value"},
	}}
	tgt := &retryTargetClient{failNext: true, current: "target-old"}
	e := &Engine{logger: zap.NewNop()}
	link := config.SyncLink{From: "source", To: "target", Kind: "config"}

	if err := e.forward(context.Background(), link, "public", "DEFAULT_GROUP", "app.yaml", "stale-event-value", src, tgt); err != nil {
		t.Fatalf("forward should succeed after a later fresh source read: %v", err)
	}

	tgt.mu.Lock()
	defer tgt.mu.Unlock()
	if len(tgt.published) != 2 {
		t.Fatalf("expected first attempt and one verified retry, got %d publishes: %v", len(tgt.published), tgt.published)
	}
	if tgt.published[0] != "fresh-source-value" {
		t.Fatalf("first publish must use current source value, got %q", tgt.published[0])
	}
	if tgt.published[1] != "newer-source-value" {
		t.Fatalf("retry must re-read the latest source value, got %q", tgt.published[1])
	}
	if src.calls != 2 {
		t.Fatalf("expected two source refresh attempts, got %d", src.calls)
	}
}

func TestForwardPublishesWhenTargetAlreadyHasSameContent(t *testing.T) {
	src := &retrySourceClient{results: []getResult{{content: "same-content"}}}
	tgt := &retryTargetClient{current: "same-content"}
	e := &Engine{logger: zap.NewNop()}
	link := config.SyncLink{From: "source", To: "target", Kind: "config"}

	if err := e.forward(context.Background(), link, "public", "DEFAULT_GROUP", "app.yaml", "same-content", src, tgt); err != nil {
		t.Fatalf("forward should treat matching target as success: %v", err)
	}
	tgt.mu.Lock()
	defer tgt.mu.Unlock()
	if len(tgt.published) != 1 || tgt.published[0] != "same-content" {
		t.Fatalf("matching target content must still be published to notify target listeners, got %v", tgt.published)
	}
	if _, ok := e.pushed.Load(pushKey("source", "target", "public", "DEFAULT_GROUP", "app.yaml")); !ok {
		t.Fatal("successful publish must record local echo")
	}
}
