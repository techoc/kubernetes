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
	"sigs.k8s.io/randfill"

	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/kubernetes/pkg/apis/policy"
)

// Funcs 返回 policy API 组使用的 fuzzer 函数。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	// 手工 fuzz 用于补充自动生成的随机值无法保证的字段约束。
	return []interface{}{
		// DisruptionsAllowed 必须是非负整数，随机取值限定为 0 或 1。
		func(s *policy.PodDisruptionBudgetStatus, c randfill.Continue) {
			c.FillNoCustom(s) // 用通用规则填充，避免递归调用本函数造成无限循环。
			s.DisruptionsAllowed = int32(c.Rand.Intn(2))
		},
	}
}
