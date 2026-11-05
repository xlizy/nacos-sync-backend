// Package pool 封装协程池，落实 AGENTS.md 的 Goroutine 退出机制，防止协程泄露。
package pool

import (
	"context"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// Pool 基于 ants 的协程池，所有异步任务统一从此提交，避免无节制 go 泛滥。
type Pool struct {
	p      *ants.Pool
	logger *zap.Logger
}

// New 创建协程池；size<=0 时回退到默认 64。
func New(size int, logger *zap.Logger) (*Pool, error) {
	if size <= 0 {
		size = 64
	}
	p, err := ants.NewPool(size,
		ants.WithNonblocking(false),
		ants.WithPanicHandler(func(r interface{}) {
			logger.Error("goroutine panic recovered in pool", zap.Any("recover", r))
		}),
	)
	if err != nil {
		return nil, err
	}
	return &Pool{p: p, logger: logger}, nil
}

// Submit 在池内执行 f；ctx 透传给任务，便于长期任务响应取消。
func (pl *Pool) Submit(ctx context.Context, f func(ctx context.Context)) error {
	return pl.p.Submit(func() {
		f(ctx)
	})
}

// Running 当前活跃协程数。
func (pl *Pool) Running() int { return pl.p.Running() }

// Waiting 排队任务数。
func (pl *Pool) Waiting() int { return pl.p.Waiting() }

// Cap 池容量。
func (pl *Pool) Cap() int { return pl.p.Cap() }

// Release 立即释放：不再接受新任务，但未等待在跑任务完成（在途任务可能被中断丢弃）。
// 注意：ants 的 Release 不等在途任务完成——优雅关闭若需等待在跑任务落地（如注销代注册实例），
// 应改用 ReleaseTimeout。
func (pl *Pool) Release() {
	pl.p.Release()
}

// ReleaseTimeout 在超时内等待在跑任务完成后释放（阻断后续 Submit）。用于优雅关闭时确保
// 注销/转发等后台任务先完成，再关闭 SDK 客户端——避免「不等在途 → 已关客户端 → 注销失败」
// 击穿优雅关闭的核心保证。
func (pl *Pool) ReleaseTimeout(timeout time.Duration) {
	pl.p.ReleaseTimeout(timeout)
}
