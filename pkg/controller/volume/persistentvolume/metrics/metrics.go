/*
Copyright 2017 The Kubernetes Authors.

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
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	"k8s.io/kubernetes/pkg/volume"
	metricutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/ptr"
)

const (
	// 指标子系统名称。
	pvControllerSubsystem = "pv_collector"

	// 指标名称。
	totalPVKey    = "total_pv_count"
	boundPVKey    = "bound_pv_count"
	unboundPVKey  = "unbound_pv_count"
	boundPVCKey   = "bound_pvc_count"
	unboundPVCKey = "unbound_pvc_count"

	// 标签名称；标签基数直接决定指标的时间序列数量。
	namespaceLabel             = "namespace"
	storageClassLabel          = "storage_class"
	volumeAttributesClassLabel = "volume_attributes_class"
	pluginNameLabel            = "plugin_name"
	volumeModeLabel            = "volume_mode"

	// 无法确定卷插件名称时使用的占位值，避免产生空标签。
	pluginNameNotAvailable = "N/A"
)

var registerMetrics sync.Once

// PVLister 抽象出列出持久卷（PV）的能力，使指标收集器直接读取 informer 缓存而不访问 API。
type PVLister interface {
	List() []interface{}
}

// PVCLister 抽象出列出持久卷声明（PVC）的能力，设计原因同 PVLister。
type PVCLister interface {
	List() []interface{}
}

// Register 注册 PV 控制器的全部指标。
// sync.Once 确保多调用方并发注册时不会因重复注册而 panic。
func Register(pvLister PVLister, pvcLister PVCLister, pluginMgr *volume.VolumePluginMgr) {
	registerMetrics.Do(func() {
		legacyregistry.CustomMustRegister(newPVAndPVCCountCollector(pvLister, pvcLister, pluginMgr))
		legacyregistry.MustRegister(volumeOperationErrorsMetric)
		legacyregistry.MustRegister(volumeOperationErrorMetric)
		legacyregistry.MustRegister(retroactiveStorageClassMetric)
		legacyregistry.MustRegister(retroactiveStorageClassErrorMetric)
	})
}

// newPVAndPVCCountCollector 创建一个 PV/PVC 计数收集器，注入 lister 与插件管理器。
func newPVAndPVCCountCollector(pvLister PVLister, pvcLister PVCLister, pluginMgr *volume.VolumePluginMgr) *pvAndPVCCountCollector {
	return &pvAndPVCCountCollector{pvLister: pvLister, pvcLister: pvcLister, pluginMgr: pluginMgr}
}

// pvAndPVCCountCollector 是自定义的 Prometheus 收集器，按维度统计 PV 与 PVC 的数量。
type pvAndPVCCountCollector struct {
	metrics.BaseStableCollector

	// pvLister 是访问 PV 信息的缓存入口（通常是 informer 的 indexer）。
	pvLister PVLister
	// pvcLister 是访问 PVC 信息的缓存入口。
	pvcLister PVCLister
	// pluginMgr 用于把 PV 映射到具体的卷插件名称。
	pluginMgr *volume.VolumePluginMgr
}

// pvcBindingMetricDimensions 聚合 PVC 绑定指标的全部标签维度，
// 作为 map 的 key 以便先聚合再生成指标，避免为单个 PVC 输出时间序列。
type pvcBindingMetricDimensions struct {
	namespace, storageClassName, volumeAttributesClassName string
}

// getPVCMetricDimensions 从 PVC 提取指标标签维度。
// StorageClass 通过 helper 获取以兼容 beta 注解；VolumeAttributesClass 缺失时为空字符串。
func getPVCMetricDimensions(pvc *v1.PersistentVolumeClaim) pvcBindingMetricDimensions {
	return pvcBindingMetricDimensions{
		namespace:                 pvc.Namespace,
		storageClassName:          storagehelpers.GetPersistentVolumeClaimClass(pvc),
		volumeAttributesClassName: ptr.Deref(pvc.Spec.VolumeAttributesClassName, ""),
	}
}

// 编译期断言收集器实现了 metrics.StableCollector 接口。
var _ metrics.StableCollector = &pvAndPVCCountCollector{}

var (
	totalPVCountDesc = metrics.NewDesc(
		metrics.BuildFQName("", pvControllerSubsystem, totalPVKey),
		"Gauge measuring total number of persistent volumes",
		[]string{pluginNameLabel, volumeModeLabel}, nil,
		metrics.ALPHA, "")
	boundPVCountDesc = metrics.NewDesc(
		metrics.BuildFQName("", pvControllerSubsystem, boundPVKey),
		"Gauge measuring number of persistent volume currently bound",
		[]string{storageClassLabel}, nil,
		metrics.ALPHA, "")
	unboundPVCountDesc = metrics.NewDesc(
		metrics.BuildFQName("", pvControllerSubsystem, unboundPVKey),
		"Gauge measuring number of persistent volume currently unbound",
		[]string{storageClassLabel}, nil,
		metrics.ALPHA, "")

	boundPVCCountDesc = metrics.NewDesc(
		metrics.BuildFQName("", pvControllerSubsystem, boundPVCKey),
		"Gauge measuring number of persistent volume claim currently bound",
		[]string{namespaceLabel, storageClassLabel, volumeAttributesClassLabel}, nil,
		metrics.ALPHA, "")
	unboundPVCCountDesc = metrics.NewDesc(
		metrics.BuildFQName("", pvControllerSubsystem, unboundPVCKey),
		"Gauge measuring number of persistent volume claim currently unbound",
		[]string{namespaceLabel, storageClassLabel, volumeAttributesClassLabel}, nil,
		metrics.ALPHA, "")

	volumeOperationErrorsMetric = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:              "volume_operation_total_errors",
			Help:              "Total volume operation errors",
			StabilityLevel:    metrics.ALPHA,
			DeprecatedVersion: "1.36.0",
		},
		[]string{"plugin_name", "operation_name"})

	volumeOperationErrorMetric = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           "volume_operation_errors_total",
			Help:           "Total volume operation errors",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"plugin_name", "operation_name"})

	retroactiveStorageClassMetric = metrics.NewCounter(
		&metrics.CounterOpts{
			Name:           "retroactive_storageclass_total",
			Help:           "Total number of retroactive StorageClass assignments to persistent volume claim",
			StabilityLevel: metrics.ALPHA,
		})

	retroactiveStorageClassErrorMetric = metrics.NewCounter(
		&metrics.CounterOpts{
			Name:           "retroactive_storageclass_errors_total",
			Help:           "Total number of failed retroactive StorageClass assignments to persistent volume claim",
			StabilityLevel: metrics.ALPHA,
		})
)

// volumeCount 按插件名称和卷模式两级维度累计 PV 数量。
type volumeCount map[string]map[string]int

// add 将指定插件与卷模式的计数加一，按需初始化内层 map。
func (v volumeCount) add(pluginName string, volumeMode string) {
	count, ok := v[pluginName]
	if !ok {
		count = map[string]int{}
	}
	count[volumeMode]++
	v[pluginName] = count
}

// DescribeWithStability 向注册中心声明本收集器会输出的全部指标描述符。
func (collector *pvAndPVCCountCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- totalPVCountDesc
	ch <- boundPVCountDesc
	ch <- unboundPVCountDesc
	ch <- boundPVCCountDesc
	ch <- unboundPVCCountDesc
}

// CollectWithStability 在每次抓取时从缓存重新计算指标并发送到 channel。
func (collector *pvAndPVCCountCollector) CollectWithStability(ch chan<- metrics.Metric) {
	collector.pvCollect(ch)
	collector.pvcCollect(ch)
}

// getPVPluginName 查询 PV 对应的卷插件名称；找不到插件时返回 N/A 占位值，
// 避免因为无法识别插件而丢弃该 PV 的计数。
func (collector *pvAndPVCCountCollector) getPVPluginName(pv *v1.PersistentVolume) string {
	spec := volume.NewSpecFromPersistentVolume(pv, true)
	fullPluginName := pluginNameNotAvailable
	if plugin, err := collector.pluginMgr.FindPluginBySpec(spec); err == nil {
		fullPluginName = metricutil.GetFullQualifiedPluginNameForVolume(plugin.GetPluginName(), spec)
	}
	return fullPluginName
}

// pvCollect 统计 PV 总数（按插件与卷模式）以及绑定/未绑定数（按 StorageClass）。
// 先聚合再输出，保证每个维度组合只产生一个时间序列。
func (collector *pvAndPVCCountCollector) pvCollect(ch chan<- metrics.Metric) {
	boundNumberByStorageClass := make(map[string]int)
	unboundNumberByStorageClass := make(map[string]int)
	totalCount := make(volumeCount)
	for _, pvObj := range collector.pvLister.List() {
		// lister 返回 interface{}，类型不匹配的对象直接跳过，避免 panic。
		pv, ok := pvObj.(*v1.PersistentVolume)
		if !ok {
			continue
		}
		pluginName := collector.getPVPluginName(pv)
		totalCount.add(pluginName, string(*pv.Spec.VolumeMode))
		if pv.Status.Phase == v1.VolumeBound {
			boundNumberByStorageClass[pv.Spec.StorageClassName]++
		} else {
			unboundNumberByStorageClass[pv.Spec.StorageClassName]++
		}
	}
	for storageClassName, number := range boundNumberByStorageClass {
		ch <- metrics.NewLazyConstMetric(
			boundPVCountDesc,
			metrics.GaugeValue,
			float64(number),
			storageClassName)
	}
	for storageClassName, number := range unboundNumberByStorageClass {
		ch <- metrics.NewLazyConstMetric(
			unboundPVCountDesc,
			metrics.GaugeValue,
			float64(number),
			storageClassName)
	}
	for pluginName, volumeModeCount := range totalCount {
		for volumeMode, number := range volumeModeCount {
			ch <- metrics.NewLazyConstMetric(
				totalPVCountDesc,
				metrics.GaugeValue,
				float64(number),
				pluginName,
				volumeMode)
		}
	}
}

// pvcCollect 统计 PVC 的绑定与未绑定数，维度为命名空间、StorageClass 和 VolumeAttributesClass。
func (collector *pvAndPVCCountCollector) pvcCollect(ch chan<- metrics.Metric) {
	boundNumber := make(map[pvcBindingMetricDimensions]int)
	unboundNumber := make(map[pvcBindingMetricDimensions]int)
	for _, pvcObj := range collector.pvcLister.List() {
		// 与 pvCollect 相同：跳过类型不匹配的对象。
		pvc, ok := pvcObj.(*v1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		if pvc.Status.Phase == v1.ClaimBound {
			boundNumber[getPVCMetricDimensions(pvc)]++
		} else {
			unboundNumber[getPVCMetricDimensions(pvc)]++
		}
	}
	for pvcLabels, number := range boundNumber {
		ch <- metrics.NewLazyConstMetric(
			boundPVCCountDesc,
			metrics.GaugeValue,
			float64(number),
			pvcLabels.namespace, pvcLabels.storageClassName, pvcLabels.volumeAttributesClassName)
	}
	for pvcLabels, number := range unboundNumber {
		ch <- metrics.NewLazyConstMetric(
			unboundPVCCountDesc,
			metrics.GaugeValue,
			float64(number),
			pvcLabels.namespace, pvcLabels.storageClassName, pvcLabels.volumeAttributesClassName)
	}
}

// RecordRetroactiveStorageClassMetric 记录追溯性 StorageClass 分配次数。
// 成功时只增加总数计数器；失败时同时增加总数和错误计数器，
// 以便通过两者之差计算成功率。
func RecordRetroactiveStorageClassMetric(success bool) {
	if !success {
		retroactiveStorageClassMetric.Inc()
		retroactiveStorageClassErrorMetric.Inc()
	} else {
		retroactiveStorageClassMetric.Inc()
	}
}

// RecordVolumeOperationErrorMetric 记录卷操作（如创建/删除）的错误次数。
// 同时更新新旧两个计数器：旧指标已标记废弃，保留是为了兼容现有监控。
func RecordVolumeOperationErrorMetric(pluginName, opName string) {
	// 空插件名替换为 N/A，保证标签值稳定且可用于聚合。
	if pluginName == "" {
		pluginName = "N/A"
	}
	volumeOperationErrorsMetric.WithLabelValues(pluginName, opName).Inc()
	volumeOperationErrorMetric.WithLabelValues(pluginName, opName).Inc()
}

// operationTimestamp 记录某个插件执行某次操作的开始时间，用于计算操作延迟。
type operationTimestamp struct {
	pluginName string
	operation  string
	startTs    time.Time
}

// newOperationTimestamp 以当前时间创建操作时间戳记录。
func newOperationTimestamp(pluginName, operationName string) *operationTimestamp {
	return &operationTimestamp{
		pluginName: pluginName,
		operation:  operationName,
		startTs:    time.Now(),
	}
}

// OperationStartTimeCache 是并发安全的操作开始时间缓存，供多个 worker 同时读写。
type OperationStartTimeCache struct {
	cache sync.Map // [string]operationTimestamp
}

// NewOperationStartTimeCache 创建一个空的操作时间戳缓存。
func NewOperationStartTimeCache() OperationStartTimeCache {
	return OperationStartTimeCache{
		cache: sync.Map{}, // [string]operationTimestamp {}
	}
}

// AddIfNotExist 在 key 不存在时记录操作开始时间；已存在则直接返回。
// 使用 LoadOrStore 保证并发下只有第一个操作的时间戳被保留，避免重复操作互相覆盖起始时间。
func (c *OperationStartTimeCache) AddIfNotExist(key, pluginName, operationName string) {
	ts := newOperationTimestamp(pluginName, operationName)
	c.cache.LoadOrStore(key, ts)
}

// Delete 删除指定 key 的时间戳记录。
func (c *OperationStartTimeCache) Delete(key string) {
	c.cache.Delete(key)
}

// Has 判断缓存中是否存在指定 key。
func (c *OperationStartTimeCache) Has(key string) bool {
	_, exists := c.cache.Load(key)
	return exists
}

// RecordMetric 在操作结束时记录指标：失败则累加错误计数，成功则记录操作延迟。
// 成功时删除时间戳条目，避免同一操作被重复计时；失败时保留条目，
// 以便调用方在重试成功后仍能统计从最初开始到成功的完整耗时。
func RecordMetric(key string, c *OperationStartTimeCache, err error) {
	obj, exists := c.cache.Load(key)
	if !exists {
		return
	}
	ts, ok := obj.(*operationTimestamp)
	if !ok {
		return
	}
	if err != nil {
		RecordVolumeOperationErrorMetric(ts.pluginName, ts.operation)
	} else {
		timeTaken := time.Since(ts.startTs).Seconds()
		metricutil.RecordOperationLatencyMetric(ts.pluginName, ts.operation, timeTaken)
		// end of this operation, remove the timestamp entry from cache
		c.Delete(key)
	}
}
