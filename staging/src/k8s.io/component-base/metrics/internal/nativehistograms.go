/*
Copyright The Kubernetes Authors.

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

// Package internal 保存 metrics 包中不允许外部调用方直接访问的共享状态。
package internal

import "sync/atomic"

// nativeHistogramsEnabled 由特性门控在启动时写入；使用原子变量，
// 使指标构造路径可以在无锁情况下确定是否需要原生直方图配置。
var nativeHistogramsEnabled atomic.Bool

// NativeHistogramOptions 保存原生直方图的桶配置选项。
type NativeHistogramOptions struct {
	BucketFactor    float64
	MaxBucketNumber uint32
}

// defaultNativeHistogramOptions 是启用原生直方图时应用于所有直方图指标的默认配置。
var defaultNativeHistogramOptions = NativeHistogramOptions{
	// BucketFactor 是原生直方图桶的增长系数；1.1 表示每个桶最多比前一个宽 10%。
	BucketFactor: 1.1,
	// MaxBucketNumber 是每个原生直方图的最大桶数，依据 OTel SDK 对 base2 指数桶聚合的建议设置。
	MaxBucketNumber: 160,
}

// SetNativeHistogramsEnabled 记录是否启用原生直方图，应由 NativeHistograms 特性门控在组件初始化时恰好调用一次。
func SetNativeHistogramsEnabled(enabled bool) {
	nativeHistogramsEnabled.Store(enabled)
}

// NativeHistogramsEnabled 报告当前是否启用原生直方图。
func NativeHistogramsEnabled() bool {
	return nativeHistogramsEnabled.Load()
}

// NativeHistogramConfig 返回原生直方图的默认桶配置。
// 返回配置副本；当前所有指标共用同一组参数，便于统一调整精度与内存开销。
func NativeHistogramConfig() NativeHistogramOptions {
	return defaultNativeHistogramOptions
}
