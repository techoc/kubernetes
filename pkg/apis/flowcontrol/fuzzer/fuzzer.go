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

package fuzzer

import (
	"sigs.k8s.io/randfill"

	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/kubernetes/pkg/apis/flowcontrol"
	"k8s.io/utils/ptr"
)

// Funcs 返回 flowcontrol API 组的 fuzzer 函数集合。
// 这些函数在通用随机填充后运行，把对象修正为合法状态，避免往返测试误报。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// LimitedPriorityLevelConfiguration 的名义并发份额不能为 0，
		// 否则内部类型转换为 v1beta2/v1beta1 后往返测试会失败。
		func(obj *flowcontrol.LimitedPriorityLevelConfiguration, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数

			// 注意：这里设置为零值会导致往返测试（内部类型到 v1beta2、v1beta1）失败。
			if obj.NominalConcurrencyShares == 0 {
				obj.NominalConcurrencyShares = int32(1)
			}
			if obj.LendablePercent == nil {
				obj.LendablePercent = ptr.To(int32(0))
			}
		},
		// ExemptPriorityLevelConfiguration 的两个可选字段有默认值，补齐以保证往返一致。
		func(obj *flowcontrol.ExemptPriorityLevelConfiguration, c randfill.Continue) {
			c.FillNoCustom(obj) // 只填充自身字段，避免递归调用本函数
			if obj.NominalConcurrencyShares == nil {
				obj.NominalConcurrencyShares = ptr.To(int32(0))
			}
			if obj.LendablePercent == nil {
				obj.LendablePercent = ptr.To(int32(0))
			}
		},
	}
}
