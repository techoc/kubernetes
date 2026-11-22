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

// RatioedGaugeVecPhasedElementPair extracts a pair of elements that differ in handling phase
//
// 从同一个带标签的指标向量中取出"排队中"和"执行中"两个元素，二者的标签仅在
// 处理阶段（phase）上不同，其余标签由调用方传入。之所以成对返回，是因为 API 优先级
// 与公平性需要分别观测等待与执行两个阶段，却又共享同一组维度标签。
func RatioedGaugeVecPhasedElementPair(vec RatioedGaugeVec, initialWaitingDenominator, initialExecutingDenominator float64, labelValues []string) RatioedGaugePair {
	return RatioedGaugePair{
		// 阶段标签放在最前面，与指标定义中的标签顺序保持一致，避免向量查找失配。
		RequestsWaiting:   vec.NewForLabelValuesSafe(0, initialWaitingDenominator, append([]string{LabelValueWaiting}, labelValues...)),
		RequestsExecuting: vec.NewForLabelValuesSafe(0, initialExecutingDenominator, append([]string{LabelValueExecuting}, labelValues...)),
	}
}
