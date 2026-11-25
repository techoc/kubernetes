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

package metrics

import (
	"errors"
	"sync"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/util"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

// pluginNameNotAvailable 用于找不到插件名时的占位标签，避免丢弃这条卷记录。
const pluginNameNotAvailable = "N/A"

const (
	// ForceDetachReasonTimeout 表示强制卸载的原因是操作超时。
	ForceDetachReasonTimeout = "timeout"
	// ForceDetachReasonOutOfService 表示节点带有 out-of-service 污点，需要强制卸载。
	ForceDetachReasonOutOfService = "out-of-service"
	attachDetachController        = "attach_detach_controller"
)

var (
	inUseVolumeMetricDesc = metrics.NewDesc(
		metrics.BuildFQName("", "storage_count", "attachable_volumes_in_use"),
		"Measure number of volumes in use",
		[]string{"node", "volume_plugin"}, nil,
		metrics.ALPHA, "")

	totalVolumesMetricDesc = metrics.NewDesc(
		metrics.BuildFQName("", "attachdetach_controller", "total_volumes"),
		"Number of volumes in A/D Controller",
		[]string{"plugin_name", "state"}, nil,
		metrics.ALPHA, "")

	// ForceDetachMetricCounter 按原因统计 A/D 控制器执行强制 detach 的次数。
	ForceDetachMetricCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      attachDetachController,
			Name:           "attachdetach_controller_forced_detaches",
			Help:           "Number of times the A/D Controller performed a forced detach",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"reason"},
	)
)

// registerMetrics 保证指标只注册一次，重复注册会导致 legacyregistry 冲突。
var registerMetrics sync.Once

// Register 把 A/D 控制器的状态采集器和强制卸载计数器注册到 legacyregistry。
// pvc/pv/pod lister 与 asw/dsw 被采集器持有，用于抓取时计算当前卷状态。
func Register(pvcLister corelisters.PersistentVolumeClaimLister,
	pvLister corelisters.PersistentVolumeLister,
	podLister corelisters.PodLister,
	asw cache.ActualStateOfWorld,
	dsw cache.DesiredStateOfWorld,
	pluginMgr *volume.VolumePluginMgr,
	csiMigratedPluginManager csimigration.PluginManager,
	intreeToCSITranslator csimigration.InTreeToCSITranslator) {
	registerMetrics.Do(func() {
		legacyregistry.CustomMustRegister(newAttachDetachStateCollector(pvcLister,
			podLister,
			pvLister,
			asw,
			dsw,
			pluginMgr,
			csiMigratedPluginManager,
			intreeToCSITranslator))
		legacyregistry.MustRegister(ForceDetachMetricCounter)
	})
}

// attachDetachStateCollector 从 lister 和期望/实际状态世界采集卷指标。
// 嵌入 BaseStableCollector 表明该采集器的指标集合是稳定的。
type attachDetachStateCollector struct {
	metrics.BaseStableCollector

	pvcLister                corelisters.PersistentVolumeClaimLister
	podLister                corelisters.PodLister
	pvLister                 corelisters.PersistentVolumeLister
	asw                      cache.ActualStateOfWorld
	dsw                      cache.DesiredStateOfWorld
	volumePluginMgr          *volume.VolumePluginMgr
	csiMigratedPluginManager csimigration.PluginManager
	intreeToCSITranslator    csimigration.InTreeToCSITranslator
}

// volumeCount 是两级映射，按维度（节点或状态）与插件名累计卷数量，例如：
//
//	node 172.168.1.100.ec2.internal 正在使用 10 个 EBS 和 3 个 glusterfs PVC：
//	{"172.168.1.100.ec2.internal": {"aws-ebs": 10, "glusterfs": 3}}
//	状态 actual_state_of_world 中共有 10 个 EBS 卷：
//	{"actual_state_of_world": {"aws-ebs": 10}}
type volumeCount map[string]map[string]int64

// add 对指定维度与插件名的计数加一；维度首次出现时创建内部映射。
func (v volumeCount) add(typeKey, counterKey string) {
	count, ok := v[typeKey]
	if !ok {
		count = map[string]int64{}
	}
	count[counterKey]++
	v[typeKey] = count
}

// newAttachDetachStateCollector 构造采集器，保存计算指标所需的全部 lister 与状态引用。
func newAttachDetachStateCollector(
	pvcLister corelisters.PersistentVolumeClaimLister,
	podLister corelisters.PodLister,
	pvLister corelisters.PersistentVolumeLister,
	asw cache.ActualStateOfWorld,
	dsw cache.DesiredStateOfWorld,
	pluginMgr *volume.VolumePluginMgr,
	csiMigratedPluginManager csimigration.PluginManager,
	intreeToCSITranslator csimigration.InTreeToCSITranslator) *attachDetachStateCollector {
	return &attachDetachStateCollector{pvcLister: pvcLister, podLister: podLister, pvLister: pvLister, asw: asw, dsw: dsw, volumePluginMgr: pluginMgr, csiMigratedPluginManager: csiMigratedPluginManager, intreeToCSITranslator: intreeToCSITranslator}
}

// 编译期断言：确保采集器实现了 StableCollector 接口。
var _ metrics.StableCollector = &attachDetachStateCollector{}

// DescribeWithStability 向注册表声明本采集器会产生的两个指标描述符。
func (collector *attachDetachStateCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- inUseVolumeMetricDesc
	ch <- totalVolumesMetricDesc
}

// CollectWithStability 在每次抓取时重新计算各节点/插件的在用卷数与状态世界中的卷数。
func (collector *attachDetachStateCollector) CollectWithStability(ch chan<- metrics.Metric) {
	nodeVolumeMap := collector.getVolumeInUseCount(klog.TODO())
	for nodeName, pluginCount := range nodeVolumeMap {
		for pluginName, count := range pluginCount {
			ch <- metrics.NewLazyConstMetric(inUseVolumeMetricDesc,
				metrics.GaugeValue,
				float64(count),
				string(nodeName),
				pluginName)
		}
	}

	stateVolumeMap := collector.getTotalVolumesCount()
	for stateName, pluginCount := range stateVolumeMap {
		for pluginName, count := range pluginCount {
			ch <- metrics.NewLazyConstMetric(totalVolumesMetricDesc,
				metrics.GaugeValue,
				float64(count),
				pluginName,
				string(stateName))
		}
	}
}

// getVolumeInUseCount 遍历已调度 Pod 的卷声明，统计每个节点上各插件的在用卷数。
// 未调度 Pod 与无法解析/找不到插件的卷被忽略，因为它们尚不占用节点资源。
func (collector *attachDetachStateCollector) getVolumeInUseCount(logger klog.Logger) volumeCount {
	// lister 读取的是本地缓存；列表失败时返回 nil，本次抓取不产生指标。
	pods, err := collector.podLister.List(labels.Everything())
	if err != nil {
		logger.Error(errors.New("Error getting pod list"), "Get pod list failed")
		return nil
	}

	nodeVolumeMap := make(volumeCount)
	for _, pod := range pods {
		if len(pod.Spec.Volumes) <= 0 {
			continue
		}

		// 未调度的 Pod 没有节点可归属，跳过。
		if pod.Spec.NodeName == "" {
			continue
		}
		for _, podVolume := range pod.Spec.Volumes {
			// CSI 迁移场景下需先转换为CSI卷规范，再查找插件；转换或查找失败均跳过该卷。
			volumeSpec, err := util.CreateVolumeSpecWithNodeMigration(logger, podVolume, pod, types.NodeName(pod.Spec.NodeName), collector.volumePluginMgr, collector.pvcLister, collector.pvLister, collector.csiMigratedPluginManager, collector.intreeToCSITranslator)
			if err != nil {
				continue
			}
			volumePlugin, err := collector.volumePluginMgr.FindPluginBySpec(volumeSpec)
			if err != nil {
				continue
			}
			pluginName := volumeutil.GetFullQualifiedPluginNameForVolume(volumePlugin.GetPluginName(), volumeSpec)
			nodeVolumeMap.add(pod.Spec.NodeName, pluginName)
		}
	}
	return nodeVolumeMap
}

// getTotalVolumesCount 分别统计期望状态世界与实际状态世界中的卷数，按插件名分组。
// 两个状态分别作为独立维度上报，便于对比期望与实际是否一致。
func (collector *attachDetachStateCollector) getTotalVolumesCount() volumeCount {
	stateVolumeMap := make(volumeCount)
	// 期望状态世界包含所有需要挂载的卷。
	for _, v := range collector.dsw.GetVolumesToAttach() {
		if plugin, err := collector.volumePluginMgr.FindPluginBySpec(v.VolumeSpec); err == nil {
			// 卷规范存在但插件解析为 nil 时用占位符，保留计数而不丢数据。
			pluginName := pluginNameNotAvailable
			if plugin != nil {
				pluginName = volumeutil.GetFullQualifiedPluginNameForVolume(plugin.GetPluginName(), v.VolumeSpec)
			}
			stateVolumeMap.add("desired_state_of_world", pluginName)
		}
	}
	// 实际状态世界包含已经挂载到节点的卷。
	for _, v := range collector.asw.GetAttachedVolumes() {
		if plugin, err := collector.volumePluginMgr.FindPluginBySpec(v.VolumeSpec); err == nil {
			// 卷规范存在但插件解析为 nil 时用占位符，保留计数而不丢数据。
			pluginName := pluginNameNotAvailable
			if plugin != nil {
				pluginName = volumeutil.GetFullQualifiedPluginNameForVolume(plugin.GetPluginName(), v.VolumeSpec)
			}
			stateVolumeMap.add("actual_state_of_world", pluginName)
		}
	}
	return stateVolumeMap
}

// RecordForcedDetachMetric 按原因记录一次强制卸载，reason 对应超时或节点失服务等取值。
func RecordForcedDetachMetric(forceDetachReason string) {
	ForceDetachMetricCounter.WithLabelValues(forceDetachReason).Inc()
}
