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

// Gauge 是被插桩代码使用的 gauge 方法集合。
// 只暴露 APF 实际需要的子集，便于用简单实现替换 Prometheus gauge 进行测试。
type Gauge interface {
	Set(float64)
	Inc()
	Dec()
	Add(float64)
	SetToCurrentTime()
}

// RatioedGauge 跟踪比率。
// 分子通过 Gauge 的方法设置/修改，
// 分母通过 SetDenominator 更新。
// 分子或分母变化时会记录一次比率。
//
// 用于表达“当前值 / 容量”的利用率；分母变化时无需重设分子。
type RatioedGauge interface {
	Gauge

	// SetDenominator 设置分母，直到下次修改
	SetDenominator(float64)
}

// RatioedGaugeVec 创建一组 observer，
// 通过一组标签值区分
type RatioedGaugeVec interface {
	// NewForLabelValuesSafe 为给定标签值元组创建新的向量成员，
	// 以给定的分子与分母初始化。
	// 与常规 Vec WithLabelValues 不同，此方法每个向量成员只在生命周期开始时
	// 调用一次。
	// "Safe" 表示即使在注册前调用，返回对象在指标注册后也能正常工作。
	//
	NewForLabelValuesSafe(initialNumerator, initialDenominator float64, labelValues []string) RatioedGauge
}

//////////////////////////////// Pairs ////////////////////////////////
//
// APF 倾向成对使用 RatioedGaugeVec 成员，
// 一个记录队列中等待的请求，一个记录正在执行的请求。
// 下面的定义是便捷层，为这种用法提供支持。
//

// RatioedGaugePair 是一对对应的 gauge：
// 一个记录队列中等待的请求数，一个记录正在执行的请求数。
//
// 两者共享同一分母（并发上限），从而可以分别观察排队与执行的利用率。
type RatioedGaugePair struct {
	// RequestsWaiting 接收当前排队请求数的观测值
	RequestsWaiting RatioedGauge

	// RequestsExecuting 接收当前执行中请求数的观测值
	RequestsExecuting RatioedGauge
}
