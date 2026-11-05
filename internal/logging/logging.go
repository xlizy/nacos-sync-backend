// Package logging 统一日志：按天切割、保留 7 天，格式
//
//	[2026-09-21 14:57:52:822][4f3a9c2e1b7d-127.0.0.1][INFO]engine.go:702: 日志内容
//
// traceId 在「收到 HTTP 请求」或「监听到 Nacos 配置变更事件」时生成，
// 通过 context 传递；写入链路入口处绑定到当前 goroutine，处理结束后清空。
package logging

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// DefaultClientIP 无法获取来源 IP 时（如监听 Nacos 变更事件）使用的占位 IP。
const DefaultClientIP = "127.0.0.1"

// LogFileName 当日日志文件名（切割后的历史文件为 nacos-sync-<yyyymmdd>.log）。
const LogFileName = "nacos-sync.log"

// RetentionDays 历史日志保留天数。
const RetentionDays = 7

// ---- traceId ----

type traceKey struct{}

// Trace 一次链路的追踪信息。
type Trace struct {
	ID       string // traceId
	ClientIP string // 客户端 IP，不可获取时为 127.0.0.1
}

// NewTraceID 生成 16 位十六进制 traceId。
func NewTraceID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// NewTrace 生成一条新的追踪信息（clientIP 为空时用 127.0.0.1）。
func NewTrace(clientIP string) Trace {
	if strings.TrimSpace(clientIP) == "" {
		clientIP = DefaultClientIP
	}
	return Trace{ID: NewTraceID(), ClientIP: clientIP}
}

// WithTrace 把追踪信息放进 context，供下游 goroutine 透传。
func WithTrace(ctx context.Context, tr Trace) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, traceKey{}, tr)
}

// FromContext 从 context 取出追踪信息。
func FromContext(ctx context.Context) (Trace, bool) {
	if ctx == nil {
		return Trace{}, false
	}
	tr, ok := ctx.Value(traceKey{}).(Trace)
	return tr, ok
}

// ---- goroutine 绑定：让不改调用签名的既有 logger.Info(...) 也能带上 traceId ----

var traces sync.Map // goid(uint64) -> Trace

func goid() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i > 0 {
		if n, err := strconv.ParseUint(string(b[:i]), 10, 64); err == nil {
			return n
		}
	}
	return 0
}

// BindCurrent 把追踪信息绑定到当前 goroutine，返回清理函数。
// 调用方必须在处理结束后调用它清空 traceId（HTTP 中间件 / Nacos 事件回调）。
func BindCurrent(tr Trace) func() {
	id := goid()
	traces.Store(id, tr)
	return func() { traces.Delete(id) }
}

// Current 取当前 goroutine 绑定的追踪信息；未绑定时返回占位值。
func Current() Trace {
	if v, ok := traces.Load(goid()); ok {
		return v.(Trace)
	}
	return Trace{ID: "-", ClientIP: DefaultClientIP}
}

// ---- 日志格式 ----

// traceEncoder 在既有 console encoder 之上补 [时间][traceId-IP][级别]文件:行号 前缀。
type traceEncoder struct {
	zapcore.Encoder
}

func (e traceEncoder) Clone() zapcore.Encoder {
	return traceEncoder{Encoder: e.Encoder.Clone()}
}

func (e traceEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	inner, err := e.Encoder.EncodeEntry(ent, fields)
	if err != nil {
		return nil, err
	}
	inner.TrimNewline()

	tr := Current()
	buf := buffer.NewPool().Get()
	buf.AppendByte('[')
	buf.AppendString(ent.Time.Format("2006-01-02 15:04:05"))
	buf.AppendByte(':')
	buf.AppendString(fmt.Sprintf("%03d", ent.Time.Nanosecond()/int(time.Millisecond)))
	buf.AppendString("][")
	buf.AppendString(tr.ID)
	buf.AppendByte('-')
	buf.AppendString(tr.ClientIP)
	buf.AppendString("][")
	buf.AppendString(ent.Level.CapitalString())
	buf.AppendByte(']')
	if ent.Caller.Defined {
		buf.AppendString(filepath.Base(ent.Caller.File))
		buf.AppendByte(':')
		buf.AppendString(strconv.Itoa(ent.Caller.Line))
		buf.AppendString(": ")
	}
	if _, err := buf.Write(inner.Bytes()); err != nil {
		return nil, err
	}
	buf.AppendString(zapcore.DefaultLineEnding)
	inner.Free()
	return buf, nil
}

func newEncoder() zapcore.Encoder {
	// 内嵌 encoder 只负责「消息 + 结构化字段」，时间/级别/调用位置由 traceEncoder 输出，
	// 故把对应 key 置空，避免重复。
	ec := zapcore.EncoderConfig{
		TimeKey:        "",
		LevelKey:       "",
		NameKey:        "",
		CallerKey:      "",
		MessageKey:     "msg",
		StacktraceKey:  "stack",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeDuration: zapcore.SecondsDurationEncoder,
	}
	return traceEncoder{Encoder: zapcore.NewConsoleEncoder(ec)}
}

// ---- 按天切割的文件 sink ----

type dailyFile struct {
	mu   sync.Mutex
	dir  string
	day  string
	file *os.File
}

func newDailyFile(dir string) (*dailyFile, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir %s: %w", dir, err)
	}
	d := &dailyFile{dir: dir}
	if err := d.rotate(time.Now()); err != nil {
		return nil, err
	}
	return d, nil
}

// rotate 切割到新的一天：把当日文件重命名为 nacos-sync-<yyyymmdd>.log，再新建当日文件。
func (d *dailyFile) rotate(now time.Time) error {
	cur := filepath.Join(d.dir, LogFileName)
	day := now.Format("20060102")
	if d.file != nil {
		_ = d.file.Close()
		d.file = nil
		if d.day != "" && d.day != day {
			// 仅当确实存在旧文件时才需要重命名
			if _, err := os.Stat(cur); err == nil {
				_ = os.Rename(cur, filepath.Join(d.dir, "nacos-sync-"+d.day+".log"))
			}
		}
	}
	f, err := os.OpenFile(cur, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log file %s: %w", cur, err)
	}
	d.file = f
	d.day = day
	d.cleanup()
	return nil
}

// cleanup 删除超过 RetentionDays 天的历史日志。
func (d *dailyFile) cleanup() {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -RetentionDays)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name != LogFileName && !strings.HasPrefix(name, "nacos-sync-") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(d.dir, name))
		}
	}
	// 同名历史文件按时间排序，超出保留天数的部分也清理
	var hist []string
	for _, e := range entries {
		if n := e.Name(); strings.HasPrefix(n, "nacos-sync-") && strings.HasSuffix(n, ".log") {
			hist = append(hist, n)
		}
	}
	if len(hist) > RetentionDays {
		sort.Strings(hist)
		for _, n := range hist[:len(hist)-RetentionDays] {
			_ = os.Remove(filepath.Join(d.dir, n))
		}
	}
}

func (d *dailyFile) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	day := time.Now().Format("20060102")
	if d.file == nil || day != d.day {
		if err := d.rotate(time.Now()); err != nil {
			return 0, err
		}
	}
	return d.file.Write(p)
}

func (d *dailyFile) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		return nil
	}
	return d.file.Sync()
}

func (d *dailyFile) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		return nil
	}
	err := d.file.Close()
	d.file = nil
	return err
}

// ---- 构造 logger ----

// New 按 logging-path 指定的目录构建 logger（按天切割，保留 7 天）。
func New(dir string, level string) (*zap.Logger, error) {
	sink, err := newDailyFile(dir)
	if err != nil {
		return nil, err
	}
	lv, err := zapcore.ParseLevel(level)
	if err != nil || level == "" {
		lv = zapcore.InfoLevel
	}
	core := zapcore.NewCore(newEncoder(), zapcore.AddSync(sink), lv)
	return zap.New(core, zap.AddCaller(), zap.ErrorOutput(zapcore.AddSync(sink))), nil
}

// NewStdout 构建输出到标准输出的 logger，用于 logger 就绪之前的早期启动日志。
func NewStdout() *zap.Logger {
	core := zapcore.NewCore(newEncoder(), zapcore.AddSync(os.Stdout), zapcore.InfoLevel)
	return zap.New(core, zap.AddCaller())
}
