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

package util

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

const (
	statusSuccess     = "success"
	statusFailUnknown = "fail-unknown"
)

/*
 * 默认情况下，下列所有指标均属于 ALPHA 稳定性级别，参见
 * https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
 * 提升指标稳定性级别是组件负责人的职责，因为按指标稳定性策略，
 * 这意味着要明确承诺在多个版本中持续支持该指标。
 */

// StorageOperationMetric 记录各存储操作的耗时分布，按插件、操作名、状态和是否迁移分组。
var StorageOperationMetric = metrics.NewHistogramVec(
	&metrics.HistogramOpts{
		Name:           "storage_operation_duration_seconds",
		Help:           "Storage operation duration",
		Buckets:        []float64{.1, .25, .5, 1, 2.5, 5, 10, 15, 25, 50, 120, 300, 600},
		StabilityLevel: metrics.BETA,
	},
	[]string{"volume_plugin", "operation_name", "status", "migrated"},
)

// storageOperationEndToEndLatencyMetric 记录存储操作从发起到结束的端到端耗时。
var storageOperationEndToEndLatencyMetric = metrics.NewHistogramVec(
	&metrics.HistogramOpts{
		Name:           "volume_operation_total_seconds",
		Help:           "Storage operation end to end duration in seconds",
		Buckets:        []float64{.1, .25, .5, 1, 2.5, 5, 10, 15, 25, 50, 120, 300, 600},
		StabilityLevel: metrics.BETA,
	},
	[]string{"plugin_name", "operation_name"},
)

// csiOperationsLatencyMetric 记录 CSI 操作耗时及对应的 gRPC 状态码。
var csiOperationsLatencyMetric = metrics.NewHistogramVec(
	&metrics.HistogramOpts{
		Subsystem:      "csi",
		Name:           "operations_seconds",
		Help:           "Container Storage Interface operation duration with gRPC error code status total",
		Buckets:        []float64{.1, .25, .5, 1, 2.5, 5, 10, 15, 25, 50, 120, 300, 600},
		StabilityLevel: metrics.ALPHA,
	},
	[]string{"driver_name", "method_name", "grpc_status_code", "migrated"},
)

// 保证指标只注册一次。
var registerMetricsOnce sync.Once

// RegisterMetrics 向 legacy registry 注册卷操作相关指标。
// 不要在 init() 中调用：需等特性门控（如 NativeHistograms）生效后再创建直方图指标。
// 该函数可安全重复调用，实际注册至多执行一次。
func RegisterMetrics() {
	registerMetricsOnce.Do(func() {
		// legacyregistry 是 k8s 对 prometheus 全局注册表的封装，
		// 专门用于强制实施指标稳定性级别约束。
		legacyregistry.MustRegister(StorageOperationMetric)
		legacyregistry.MustRegister(storageOperationEndToEndLatencyMetric)
		legacyregistry.MustRegister(csiOperationsLatencyMetric)
	})
}

// OperationCompleteHook 返回一个钩子，在操作完成时调用以记录耗时和成功/失败状态。
func OperationCompleteHook(plugin, operationName string) func(types.CompleteFuncParam) {
	RegisterMetrics()
	requestTime := time.Now()
	opComplete := func(c types.CompleteFuncParam) {
		timeTaken := time.Since(requestTime).Seconds()
		// 用操作名和插件名构造指标标签。
		status := statusSuccess
		if *c.Err != nil {
			// TODO: Establish well-known error codes to be able to distinguish
			// user configuration errors from system errors.
			// 目前无法区分错误类型，统一记为 fail-unknown。
			status = statusFailUnknown
		}
		migrated := false
		if c.Migrated != nil {
			migrated = *c.Migrated
		}
		StorageOperationMetric.WithLabelValues(plugin, operationName, status, strconv.FormatBool(migrated)).Observe(timeTaken)
	}
	return opComplete
}

// FSGroupCompleteHook 返回一个钩子，在卷递归权限变更（fsGroup 应用）完成时调用。
func FSGroupCompleteHook(plugin volume.VolumePlugin, spec *volume.Spec) func(types.CompleteFuncParam) {
	return OperationCompleteHook(GetFullQualifiedPluginNameForVolume(plugin.GetPluginName(), spec), "volume_apply_access_control")
}

// GetFullQualifiedPluginNameForVolume 返回卷的完全限定插件名。
// 对 CSI 插件会在名称后附加驱动名，例如 kubernetes.io/csi:csi-hostpath，
// 以便区分由不同 CSI 驱动处理的卷所产生的指标。
func GetFullQualifiedPluginNameForVolume(pluginName string, spec *volume.Spec) string {
	if spec != nil {
		if spec.Volume != nil && spec.Volume.CSI != nil {
			return fmt.Sprintf("%s:%s", pluginName, spec.Volume.CSI.Driver)
		}
		if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.CSI != nil {
			return fmt.Sprintf("%s:%s", pluginName, spec.PersistentVolume.Spec.CSI.Driver)
		}
	}
	return pluginName
}

// RecordOperationLatencyMetric 将某操作的端到端耗时记录到
// volume_operation_total_seconds 指标中。
func RecordOperationLatencyMetric(plugin, operationName string, secondsTaken float64) {
	RegisterMetrics()
	storageOperationEndToEndLatencyMetric.WithLabelValues(plugin, operationName).Observe(secondsTaken)
}

// RecordCSIOperationLatencyMetrics 将 CSI 操作耗时和 gRPC 状态记录到
// csi_kubelet_operations_seconds 指标中。
func RecordCSIOperationLatencyMetrics(driverName string,
	operationName string,
	operationErr error,
	operationDuration time.Duration,
	migrated string) {
	RegisterMetrics()
	csiOperationsLatencyMetric.WithLabelValues(driverName, operationName, getErrorCode(operationErr), migrated).Observe(operationDuration.Seconds())
}

// getErrorCode 提取错误对应的 gRPC 状态码字符串。
func getErrorCode(err error) string {
	if err == nil {
		return codes.OK.String()
	}

	st, ok := status.FromError(err)
	if !ok {
		// 非 gRPC 错误说明操作在调用 gRPC 方法之前就已失败，
		// 否则拿到的会是 gRPC 错误。
		return "unknown-non-grpc"
	}

	return st.Code().String()
}
