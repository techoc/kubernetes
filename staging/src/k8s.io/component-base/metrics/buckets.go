/*
Copyright 2022 The Kubernetes Authors.

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
	"github.com/prometheus/client_golang/prometheus"
)

// DefBuckets 是 prometheus.DefBuckets 的包装。
// 重新导出而非直接引用，便于调用方只依赖 component-base 的 metrics 包。
var DefBuckets = prometheus.DefBuckets

// LinearBuckets 是 prometheus.LinearBuckets 的包装。
func LinearBuckets(start, width float64, count int) []float64 {
	return prometheus.LinearBuckets(start, width, count)
}

// ExponentialBuckets 是 prometheus.ExponentialBuckets 的包装。
func ExponentialBuckets(start, factor float64, count int) []float64 {
	return prometheus.ExponentialBuckets(start, factor, count)
}

// ExponentialBucketsRange 创建 'count' 个桶，最低边界为 'min'，最高边界为 'max'。
// 末尾的 +Inf 桶不计入数量，也不会出现在返回切片中；返回切片用于 HistogramOpts 的 Buckets 字段。
// 若 'count' 或 'min' 小于等于 0，函数会 panic。
func ExponentialBucketsRange(min, max float64, count int) []float64 {
	return prometheus.ExponentialBucketsRange(min, max, count)
}

// MergeBuckets 合并多个桶列表。
// 结果从 1 个元素开始，第一个桶固定为 0，作为最低边界；调用方依赖这一前缀。
func MergeBuckets(buckets ...[]float64) []float64 {
	result := make([]float64, 1)
	for _, s := range buckets {
		result = append(result, s...)
	}
	return result
}
