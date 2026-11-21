/*
Copyright 2020 The Kubernetes Authors.

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

package rootcacertpublisher

import (
	"strconv"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// RootCACertPublisher 是本控制器指标的子系统名前缀。
const RootCACertPublisher = "root_ca_cert_publisher"

// 同步次数与同步耗时按结果为标签分别统计，便于区分成功与各类失败。
var (
	syncCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      RootCACertPublisher,
			Name:           "sync_total",
			Help:           "Number of namespace syncs happened in root ca cert publisher.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code"},
	)
	syncLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      RootCACertPublisher,
			Name:           "sync_duration_seconds",
			Help:           "Number of namespace syncs happened in root ca cert publisher.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code"},
	)
)

// recordMetrics 按同步结果记录耗时与计数：无错误记为 200；
// 带状态码的 API 错误沿用其原始状态码，其余错误统一记为 500。
func recordMetrics(start time.Time, err error) {
	// 默认按服务端错误处理，只有明确成功的场景才改成 200。
	code := "500"
	if err == nil {
		code = "200"
	} else if se, ok := err.(*apierrors.StatusError); ok && se.Status().Code != 0 {
		code = strconv.Itoa(int(se.Status().Code))
	}
	syncLatency.WithLabelValues(code).Observe(time.Since(start).Seconds())
	// 耗时与计数使用同一个状态码标签，保证两类指标的语义一致。
	syncCounter.WithLabelValues(code).Inc()
}

// once 保证指标只注册一次，避免控制器重启或重复初始化时 panic。
var once sync.Once

// registerMetrics 将本控制器的指标注册到旧版全局注册表。
func registerMetrics() {
	once.Do(func() {
		legacyregistry.MustRegister(syncCounter)
		legacyregistry.MustRegister(syncLatency)
	})
}
