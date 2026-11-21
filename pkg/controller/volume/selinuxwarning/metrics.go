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

package selinuxwarning

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/volume/selinuxwarning/cache"
)

var (
	// seLinuxConflictDesc 描述冲突指标；标签覆盖冲突属性及两个 Pod 的命名空间、名称和取值，
	// 便于定位究竟是哪两个 Pod 因共享卷而产生 SELinux 冲突。
	seLinuxConflictDesc = metrics.NewDesc(
		metrics.BuildFQName("", "selinux_warning_controller", "selinux_volume_conflict"),
		"Conflict between two Pods using the same volume",
		[]string{"property", "pod1_namespace", "pod1_name", "pod1_value", "pod2_namespace", "pod2_name", "pod2_value"}, nil,
		metrics.ALPHA, "")
)

// registerMetrics 保证 collector 只注册一次；重复注册会导致指标重复暴露而 panic。
var registerMetrics sync.Once

// RegisterMetrics 把 SELinux 冲突指标注册到 legacy registry，重复调用无副作用。
func RegisterMetrics(logger klog.Logger, cache cache.VolumeCache) {
	registerMetrics.Do(func() {
		legacyregistry.CustomMustRegister(newCollector(logger, cache))
	})
}

// newCollector 构造从卷缓存实时读取冲突的指标收集器。
func newCollector(logger klog.Logger, cache cache.VolumeCache) *collector {
	return &collector{
		logger: logger,
		cache:  cache,
	}
}

// 编译期断言：collector 必须实现 StableCollector 才能使用稳定的 Describe/Collect 语义。
var _ metrics.StableCollector = &collector{}

// collector 从卷缓存中读取当前冲突并暴露为 Prometheus 指标。
type collector struct {
	metrics.BaseStableCollector
	cache  cache.VolumeCache
	logger klog.Logger
}

// DescribeWithStability 向系统声明本收集器会暴露的指标描述符。
func (c *collector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- seLinuxConflictDesc
}

// CollectWithStability 每次抓取时从缓存读取当前冲突；缓存是唯一数据源，
// 因此冲突消失后下一次抓取就不会再上报该指标。
func (c *collector) CollectWithStability(ch chan<- metrics.Metric) {
	for _, conflict := range c.cache.GetConflicts(c.logger) {
		ch <- metrics.NewLazyConstMetric(seLinuxConflictDesc,
			metrics.GaugeValue,
			1.0,
			conflict.PropertyName,
			conflict.Pod.Namespace,
			conflict.Pod.Name,
			conflict.PropertyValue,
			conflict.OtherPod.Namespace,
			conflict.OtherPod.Name,
			conflict.OtherPropertyValue,
		)
	}
}
