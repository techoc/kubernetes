/*
Copyright 2014 The Kubernetes Authors.

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

package volume

var _ MetricsProvider = &MetricsNil{}

// MetricsNil 是一个不支持返回 Metrics 的 MetricsProvider，
// 用作尚未支持 metrics 的卷的占位实现。
type MetricsNil struct{}

// SupportsMetrics 对 MetricsNil 类型始终返回 false。
func (*MetricsNil) SupportsMetrics() bool {
	return false
}

// GetMetrics 返回空的 Metrics 和一个错误。
// 参见 MetricsProvider.GetMetrics。
func (*MetricsNil) GetMetrics() (*Metrics, error) {
	return &Metrics{}, NewNotSupportedError()
}
