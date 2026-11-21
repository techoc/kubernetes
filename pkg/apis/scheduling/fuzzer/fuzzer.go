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

package fuzzer

import (
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"sigs.k8s.io/randfill"
)

// Funcs 返回 scheduling API 组的 fuzzer 函数集合。
// 这些函数先随机填充对象，再把受默认化影响的可选字段固定为合法值：
// 若让这些指针字段保持 nil，默认化会在 roundtrip 过程中补全它们，导致编解码前后对象不相等。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// PriorityClass 的 fuzzer：只需要修正抢占策略，其余字段交给通用填充逻辑。
		func(s *scheduling.PriorityClass, c randfill.Continue) {
			c.FillNoCustom(s)
			// 抢占策略为空会被默认化为 PreemptLowerPriority，
			// 这里直接写入该默认值，使 fuzz 结果在默认化前后保持稳定。
			if s.PreemptionPolicy == nil {
				preemptLowerPriority := core.PreemptLowerPriority
				s.PreemptionPolicy = &preemptLowerPriority
			}
		},
		// PodGroup 的 fuzzer：disruption mode 与抢占策略都必须有确定值，
		// 否则默认化会补齐它们，破坏序列化往返的一致性。
		func(s *scheduling.PodGroup, c randfill.Continue) {
			c.FillNoCustom(s)
			// 未指定中断模式时默认采用 Single 模式。
			if s.Spec.DisruptionMode == nil {
				s.Spec.DisruptionMode = &scheduling.DisruptionMode{Single: &scheduling.SingleDisruptionMode{}}
			}
			// 未指定抢占策略时默认允许抢占低优先级 Pod。
			if s.Spec.PreemptionPolicy == nil {
				preemptLowerPriority := scheduling.PreemptLowerPriority
				s.Spec.PreemptionPolicy = &preemptLowerPriority
			}
		},
		// CompositePodGroup 的 fuzzer：与 PodGroup 类似，但使用复合中断模式类型，
		// 因为两者在内部类型中对应不同的字段结构。
		func(s *scheduling.CompositePodGroup, c randfill.Continue) {
			c.FillNoCustom(s)
			// 复合PodGroup 的中断模式默认值使用 Composite 版本，不能复用 PodGroup 的类型。
			if s.Spec.DisruptionMode == nil {
				s.Spec.DisruptionMode = &scheduling.CompositeDisruptionMode{Single: &scheduling.SingleCompositeDisruptionMode{}}
			}
			// 抢占策略默认值同样需要显式写入，避免默认化在 roundtrip 中改变对象。
			if s.Spec.PreemptionPolicy == nil {
				preemptLowerPriority := scheduling.PreemptLowerPriority
				s.Spec.PreemptionPolicy = &preemptLowerPriority
			}
		},
	}
}
