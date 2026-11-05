// Package metrics 定义同步服务的 Prometheus 指标。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// SyncEvents 同步事件计数（按链路/类型）。
	SyncEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_events_total",
		Help: "Total number of sync events processed.",
	}, []string{"link", "kind"})

	// SyncErrors 同步失败计数。
	SyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_errors_total",
		Help: "Total number of sync errors.",
	}, []string{"link", "kind"})

	// SyncLag 同步处理耗时（秒）。
	SyncLag = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sync_lag_seconds",
		Help:    "Sync processing latency in seconds.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5},
	}, []string{"link", "kind"})

	// ReconcileDrift 对账修复计数（发现漂移并修正）。
	ReconcileDrift = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reconcile_drift_total",
		Help: "Total number of drift corrections during reconcile.",
	}, []string{"link"})
)
