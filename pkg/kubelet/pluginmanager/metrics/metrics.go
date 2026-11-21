/*
Copyright 2019 The Kubernetes Authors.

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

package metrics

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
)

const (
	// Plugin Manager 的指标键。
	pluginManagerTotalPlugins = "plugin_manager_total_plugins"
)

var (
	// registerMetrics 保证指标只注册一次。
	registerMetrics sync.Once

	// totalPluginsDesc 描述插件总数指标。
	totalPluginsDesc = metrics.NewDesc(
		pluginManagerTotalPlugins,
		"Number of plugins in Plugin Manager",
		[]string{"socket_path", "state"},
		nil,
		metrics.ALPHA,
		"",
	)
)

// pluginCount 是用嵌套 map 实现的计数器。
type pluginCount map[string]map[string]int64

// add 把指定状态下的插件计数加一。
func (pc pluginCount) add(state, pluginName string) {
	count, ok := pc[state]
	if !ok {
		count = map[string]int64{}
	}
	count[pluginName]++
	pc[state] = count
}

// Register 注册 Plugin Manager 指标。
// asw 是实际状态缓存。
// dsw 是期望状态缓存。
func Register(asw cache.ActualStateOfWorld, dsw cache.DesiredStateOfWorld) {
	// 使用 Once 避免重复注册。
	registerMetrics.Do(func() {
		legacyregistry.CustomMustRegister(&totalPluginsCollector{asw: asw, dsw: dsw})
	})
}

// totalPluginsCollector 统计实际状态与期望状态中的插件数量。
type totalPluginsCollector struct {
	metrics.BaseStableCollector

	asw cache.ActualStateOfWorld
	dsw cache.DesiredStateOfWorld
}

// 编译期断言：totalPluginsCollector 实现了 StableCollector 接口。
var _ metrics.StableCollector = &totalPluginsCollector{}

// DescribeWithStability 输出插件总数指标描述符。
func (c *totalPluginsCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- totalPluginsDesc
}

// CollectWithStability 按 socket 路径与状态输出插件数量。
func (c *totalPluginsCollector) CollectWithStability(ch chan<- metrics.Metric) {
	// 分别输出实际状态与期望状态中的插件数量。
	for stateName, pluginCount := range c.getPluginCount() {
		for socketPath, count := range pluginCount {
			ch <- metrics.NewLazyConstMetric(totalPluginsDesc,
				metrics.GaugeValue,
				float64(count),
				socketPath,
				stateName)
		}
	}
}

// getPluginCount 汇总实际状态与期望状态中的插件数量。
func (c *totalPluginsCollector) getPluginCount() pluginCount {
	counter := make(pluginCount)
	// 统计已注册插件。
	for _, registeredPlugin := range c.asw.GetRegisteredPlugins() {
		socketPath := registeredPlugin.SocketPath
		counter.add("actual_state_of_world", socketPath)
	}

	// 统计待注册插件。
	for _, pluginToRegister := range c.dsw.GetPluginsToRegister() {
		socketPath := pluginToRegister.SocketPath
		counter.add("desired_state_of_world", socketPath)
	}
	return counter
}
