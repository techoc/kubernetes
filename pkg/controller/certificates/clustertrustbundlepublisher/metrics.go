/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clustertrustbundlepublisher

import (
	"errors"
	"strconv"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// clustertrustbundlePublisher 是 clustertrustbundle_publisher 使用的指标子系统名称。
const (
	clustertrustbundlePublisher = "clustertrustbundle_publisher"
)

var (
	// syncCounter 按同步结果状态码统计同步次数，便于区分成功与各类失败。
	syncCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      clustertrustbundlePublisher,
			Name:           "sync_total",
			Help:           "Number of syncs that occurred in cluster trust bundle publisher.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code"},
	)
	// syncLatency 按结果状态码记录同步耗时，用于观察控制器性能回归。
	syncLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      clustertrustbundlePublisher,
			Name:           "sync_duration_seconds",
			Help:           "The time it took to sync a cluster trust bundle.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code"},
	)
)

// recordMetrics 根据一次同步的结果记录耗时与计数。
// 优先使用 API 错误携带的真实 HTTP 状态码作为标签，便于定位特定失败类型；
// 非 API 错误统一记为 500。
func recordMetrics(start time.Time, err error) {
	code := "500"
	if err == nil {
		code = "200"
	} else {
		var statusErr apierrors.APIStatus
		// 保留 API 错误的状态码（如 404、409），使指标能反映具体的失败原因。
		if errors.As(err, &statusErr) && statusErr.Status().Code != 0 {
			code = strconv.Itoa(int(statusErr.Status().Code))
		}
	}
	syncLatency.WithLabelValues(code).Observe(time.Since(start).Seconds())
	syncCounter.WithLabelValues(code).Inc()
}

// once 确保指标只注册一次，重复注册到 legacyregistry 会触发 panic。
var once sync.Once

// registerMetrics 将该包的指标一次性注册到 legacyregistry。
func registerMetrics() {
	once.Do(func() {
		legacyregistry.MustRegister(syncCounter)
		legacyregistry.MustRegister(syncLatency)
	})
}
