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

package leaderelection

import (
	"sync"
)

// 本文件为设置指标 provider（如 prometheus）提供抽象层：
// 通过接口 + 全局工厂把指标实现解耦出来，避免 leaderelection 直接依赖
// 具体指标库。默认实现是空操作，未注册 Provider 时没有额外开销。

// leaderMetricsAdapter 是包内使用的指标适配器，屏蔽 nil 与空实现的差异。
type leaderMetricsAdapter interface {
	leaderOn(name string)
	leaderOff(name string)
	slowpathExercised(name string)
}

// LeaderMetric 对领导者选举中使用的指标进行埋点。
// 实现方按 lease 名称区分多把锁的指标；name 通常是 LeaderElectionConfig.Name。
type LeaderMetric interface {
	// On 记录本进程成为租约持有者。
	On(name string)
	// Off 记录本进程失去租约持有者身份。
	Off(name string)
	// SlowpathExercised 记录一次慢路径（未走优化路径）的租约获取尝试。
	SlowpathExercised(name string)
}

// noopMetric 是 LeaderMetric 的空实现。
type noopMetric struct{}

func (noopMetric) On(name string)                {}
func (noopMetric) Off(name string)               {}
func (noopMetric) SlowpathExercised(name string) {}

// defaultLeaderMetrics 要求调用方在设置任何指标前先自行加锁。
// 调用方需自行保证并发安全：本结构自身不加锁。
type defaultLeaderMetrics struct {
	// leader 表示当前进程是否为 name 租约的持有者
	leader LeaderMetric
}

// leaderOn 在 m 为 nil 时安全返回（nil 接收者可用）。
func (m *defaultLeaderMetrics) leaderOn(name string) {
	if m == nil {
		return
	}
	m.leader.On(name)
}

// leaderOff 在 m 为 nil 时安全返回（nil 接收者可用）。
func (m *defaultLeaderMetrics) leaderOff(name string) {
	if m == nil {
		return
	}
	m.leader.Off(name)
}

// slowpathExercised 在 m 为 nil 时安全返回（nil 接收者可用）。
func (m *defaultLeaderMetrics) slowpathExercised(name string) {
	if m == nil {
		return
	}
	m.leader.SlowpathExercised(name)
}

// noMetrics 是 leaderMetricsAdapter 的空实现，用于未配置 Provider 的场景。
type noMetrics struct{}

func (noMetrics) leaderOn(name string)          {}
func (noMetrics) leaderOff(name string)         {}
func (noMetrics) slowpathExercised(name string) {}

// MetricsProvider 生成领导者选举使用的各类指标。
type MetricsProvider interface {
	NewLeaderMetric() LeaderMetric
}

// noopMetricsProvider 返回空指标实现。
type noopMetricsProvider struct{}

func (noopMetricsProvider) NewLeaderMetric() LeaderMetric {
	return noopMetric{}
}

// globalMetricsFactory 是进程级单例；首个 SetProvider 生效，之后忽略。
var globalMetricsFactory = leaderMetricsFactory{
	metricsProvider: noopMetricsProvider{},
}

type leaderMetricsFactory struct {
	metricsProvider MetricsProvider

	// onlyOnce 保证 Provider 只被设置一次，避免不同组件互相覆盖。
	onlyOnce sync.Once
}

// setProvider 只在第一次调用时生效。
func (f *leaderMetricsFactory) setProvider(mp MetricsProvider) {
	f.onlyOnce.Do(func() {
		f.metricsProvider = mp
	})
}

// newLeaderMetrics 为每个 leader elector 创建独立的指标适配器；
// 未配置 Provider 时返回轻量的 noMetrics。
func (f *leaderMetricsFactory) newLeaderMetrics() leaderMetricsAdapter {
	mp := f.metricsProvider
	if mp == (noopMetricsProvider{}) {
		return noMetrics{}
	}
	return &defaultLeaderMetrics{
		leader: mp.NewLeaderMetric(),
	}
}

// SetProvider 为后续创建的所有选举器设置指标 provider，只有第一次调用生效。
// 已创建的选举器不会切换 Provider；应在程序初始化阶段调用。
func SetProvider(metricsProvider MetricsProvider) {
	globalMetricsFactory.setProvider(metricsProvider)
}
