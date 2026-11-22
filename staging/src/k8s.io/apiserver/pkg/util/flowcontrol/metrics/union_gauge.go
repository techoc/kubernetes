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

// unionGauge 把一次 Gauge 操作广播给多个底层 Gauge。
// 典型用途是让同一逻辑值同时写入多个指标后端（如稳定版与实验版指标）。
type unionGauge []Gauge

// 编译期断言：unionGauge 实现 Gauge 接口。
var _ Gauge = unionGauge(nil)

// NewUnionGauge 构造一个把操作委托给所有给定 Gauge 的 Gauge。
func NewUnionGauge(elts ...Gauge) Gauge {
	return unionGauge(elts)
}

// Set 把值写入所有底层 Gauge。
func (ug unionGauge) Set(x float64) {
	for _, gauge := range ug {
		gauge.Set(x)
	}
}

// Add 把增量加到所有底层 Gauge。
func (ug unionGauge) Add(x float64) {
	for _, gauge := range ug {
		gauge.Add(x)
	}
}

// Inc 让所有底层 Gauge 自增。
func (ug unionGauge) Inc() {
	for _, gauge := range ug {
		gauge.Inc()
	}
}

// Dec 让所有底层 Gauge 自减。
func (ug unionGauge) Dec() {
	for _, gauge := range ug {
		gauge.Dec()
	}
}

// SetToCurrentTime 把所有底层 Gauge 设为当前时间（unix 秒）。
func (ug unionGauge) SetToCurrentTime() {
	for _, gauge := range ug {
		gauge.SetToCurrentTime()
	}
}
