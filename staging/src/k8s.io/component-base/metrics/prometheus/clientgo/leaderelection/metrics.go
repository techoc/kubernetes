/*
Copyright 2018 The Kubernetes Authors.

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

// Package leaderelection 把 client-go 领导者选举的指标接入 legacyregistry。
// 注册延迟到首次构造 LeaderMetric 时，避免在 feature gate 尚未生效前创建指标。
package leaderelection

import (
	"sync"

	"k8s.io/client-go/tools/leaderelection"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	// leaderGauge 以 1/0 表示本实例当前是否为对应 lease 的持有者。
	leaderGauge = k8smetrics.NewGaugeVec(&k8smetrics.GaugeOpts{
		Name:           "leader_election_master_status",
		StabilityLevel: k8smetrics.ALPHA,
		Help:           "Gauge of if the reporting system is master of the relevant lease, 0 indicates backup, 1 indicates master. 'name' is the string used to identify the lease. Please make sure to group by name.",
	}, []string{"name"})
	// 慢路径计数器：按名称累计，用于估算慢路径比例；快路径不计数以省开销。
	leaderSlowpathCounter = k8smetrics.NewCounterVec(&k8smetrics.CounterOpts{
		Name:           "leader_election_slowpath_total",
		StabilityLevel: k8smetrics.ALPHA,
		Help:           "Total number of slow path exercised in renewing leader leases. 'name' is the string used to identify the lease. Please make sure to group by name.",
	}, []string{"name"})
)

var registerOnce sync.Once

// init 只向 leaderelection 安装 provider，实际到 legacyregistry 的注册延迟到首次调用
// 工厂方法（运行时常在首次构造领导者选举管理器时发生）。
func init() {
	leaderelection.SetProvider(prometheusMetricsProvider{})
}

// prometheusMetricsProvider 实现 client-go 的 leader election 指标 provider。
type prometheusMetricsProvider struct{}

// register 向旧版注册表注册 leader election 指标。provider 的 factory 方法通过
// registerOnce 调用它，使注册（以及 Create）在运行时发生。
func register() {
	registerOnce.Do(func() {
		legacyregistry.MustRegister(leaderGauge)
		legacyregistry.MustRegister(leaderSlowpathCounter)
	})
}

// NewLeaderMetric 返回绑定到上述指标的适配器，并确保指标已注册。
func (prometheusMetricsProvider) NewLeaderMetric() leaderelection.LeaderMetric {
	register()
	return &leaderAdapter{gauge: leaderGauge, counter: leaderSlowpathCounter}
}

// leaderAdapter 把 client-go 的 LeaderMetric 回调映射到 gauge/counter。
type leaderAdapter struct {
	gauge   *k8smetrics.GaugeVec
	counter *k8smetrics.CounterVec
}

// On 表示获得领导权。
func (s *leaderAdapter) On(name string) {
	s.gauge.WithLabelValues(name).Set(1.0)
}

// Off 表示失去领导权。
func (s *leaderAdapter) Off(name string) {
	s.gauge.WithLabelValues(name).Set(0.0)
}

// SlowpathExercised 记录一次慢路径续约。
func (s *leaderAdapter) SlowpathExercised(name string) {
	s.counter.WithLabelValues(name).Inc()
}
