package watcher

import (
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
)

func newTestManager(t *testing.T, refresh func(grpID string) error) (*Manager, *poller) {
	t.Helper()
	w := &Manager{logger: zap.NewNop(), pollers: make(map[string]*poller), refresh: refresh}
	p := &poller{
		grpID:  "grp-test",
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
	return w, p
}

// TestApplyDigestFirstRoundRefreshRetries 首轮刷新失败时不得提交指纹：
// 保持 pending 状态，下一轮重试（修复 P1「刷新失败后永不重试」）。
func TestApplyDigestFirstRoundRefreshRetries(t *testing.T) {
	var calls int
	w, p := newTestManager(t, func(string) error {
		calls++
		if calls <= 2 {
			return errors.New("node unreachable")
		}
		return nil
	})

	w.applyDigest(p, "digest-v1")
	if p.lastDigest != "" || !p.lastOK.IsZero() {
		t.Fatalf("failed refresh must not commit digest/lastOK; got digest=%q lastOK=%v", p.lastDigest, p.lastOK)
	}

	// 第二轮仍失败：依旧不提交。
	w.applyDigest(p, "digest-v1")
	if p.lastDigest != "" {
		t.Fatalf("second failed refresh must still not commit digest; got %q", p.lastDigest)
	}

	// 第三轮成功：提交指纹与 lastOK。
	w.applyDigest(p, "digest-v1")
	if p.lastDigest != "digest-v1" || p.lastOK.IsZero() {
		t.Fatalf("successful refresh must commit digest and lastOK; got digest=%q", p.lastDigest)
	}
	if calls != 3 {
		t.Fatalf("expected 3 refresh attempts, got %d", calls)
	}
}

// TestApplyDigestChangedRefreshFailsKeepsOld 指纹变化但刷新失败：保留旧指纹，
// 下一轮 digest 与旧指纹不等 → 自动再次刷新重试。
func TestApplyDigestChangedRefreshFailsKeepsOld(t *testing.T) {
	var fail bool
	w, p := newTestManager(t, func(string) error {
		if fail {
			return errors.New("partial node failure")
		}
		return nil
	})

	// 建立基线。
	w.applyDigest(p, "v1")
	if p.lastDigest != "v1" {
		t.Fatalf("baseline commit failed; got %q", p.lastDigest)
	}

	// 变更 + 刷新失败：指纹不得前进到 v2。
	fail = true
	w.applyDigest(p, "v2")
	if p.lastDigest != "v1" {
		t.Fatalf("failed refresh must keep old digest (pending retry); got %q", p.lastDigest)
	}

	// 下一轮刷新成功：指纹前进到 v2。
	fail = false
	w.applyDigest(p, "v2")
	if p.lastDigest != "v2" {
		t.Fatalf("successful retry must commit new digest; got %q", p.lastDigest)
	}
}

// TestApplyDigestUnchangedUpdatesLastOK 无变化且查询成功 → 更新 lastOK（查询健康证明）。
func TestApplyDigestUnchangedUpdatesLastOK(t *testing.T) {
	w, p := newTestManager(t, func(string) error { return nil })

	w.applyDigest(p, "v1")
	if p.lastOK.IsZero() {
		t.Fatal("baseline success must set lastOK")
	}

	old := p.lastOK
	time.Sleep(2 * time.Millisecond)
	w.applyDigest(p, "v1")
	if !p.lastOK.After(old) {
		t.Fatal("unchanged digest with successful query must refresh lastOK")
	}
	if p.lastDigest != "v1" {
		t.Fatalf("digest must stay committed; got %q", p.lastDigest)
	}
}

// TestStatusHealthyRequiresFreshLastOK 健康判定必须结合 lastOK 新鲜度：
// goroutine 存活但长期没有成功轮询（DB 挂起/不可达）应显示不健康。
func TestStatusHealthyRequiresFreshLastOK(t *testing.T) {
	w, p := newTestManager(t, func(string) error { return nil })
	w.pollers[p.grpID] = p

	// 尚未成功过 → 不健康。
	st := w.Status()
	if len(st) != 1 || st[0].Healthy {
		t.Fatalf("poller without any successful round must be unhealthy; got %+v", st)
	}

	// 刚成功 → 健康。
	w.applyDigest(p, "v1")
	st = w.Status()
	if !st[0].Healthy {
		t.Fatalf("fresh lastOK must be healthy; got %+v", st)
	}

	// lastOK 过期 → 不健康。
	p.mu.Lock()
	p.lastOK = time.Now().Add(-2 * lastOKStaleAfter)
	p.mu.Unlock()
	st = w.Status()
	if st[0].Healthy {
		t.Fatalf("stale lastOK must be unhealthy; got %+v", st)
	}

	// goroutine 已退出 → 不健康（即使 lastOK 新鲜）。
	p.mu.Lock()
	p.lastOK = time.Now()
	p.mu.Unlock()
	close(p.done)
	st = w.Status()
	if st[0].Healthy {
		t.Fatalf("exited poller must be unhealthy; got %+v", st)
	}
}
