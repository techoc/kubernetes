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
	"k8s.io/kubernetes/pkg/apis/rbac"
)

// Funcs 返回 rbac API 组的 fuzzer 函数集合。
// 这里的自定义填充会补齐有默认值或受约束的字段，
// 保证随机生成的对象在内部类型与版本化类型之间往返时不失真。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// RoleRef 的 APIGroup 有默认值；随机填充可能留空，
		// 这里补成 RBAC 组名，避免默认值处理在往返测试中引入差异。
		func(r *rbac.RoleRef, c randfill.Continue) {
			c.FillNoCustom(r) // 只填充自身字段，避免递归调用本函数

			// 与 defaulter 保持一致：APIGroup 为空时必须是 RBAC 组名
			if len(r.APIGroup) == 0 {
				r.APIGroup = rbac.GroupName
			}
		},
		// Subject 的 Kind/APIGroup/Namespace 存在联动约束，
		// 不能让随机填充自由组合，否则会产生无法通过校验的对象。
		func(r *rbac.Subject, c randfill.Continue) {
			switch c.Int31n(3) {
			case 0:
				r.Kind = rbac.ServiceAccountKind
				r.APIGroup = ""
				c.FillNoCustom(&r.Name)
				c.FillNoCustom(&r.Namespace)
			case 1:
				// User kind 的 APIGroup 固定为 RBAC 组名。
				r.Kind = rbac.UserKind
				r.APIGroup = rbac.GroupName
				c.FillNoCustom(&r.Name)
				// "*" 会被转换成 system:authenticated 组，导致往返后无法还原；重新生成直到不是 "*"。
				for r.Name == "*" {
					c.FillNoCustom(&r.Name)
				}
			case 2:
				// Group kind 的 APIGroup 同样固定为 RBAC 组名。
				r.Kind = rbac.GroupKind
				r.APIGroup = rbac.GroupName
				c.FillNoCustom(&r.Name)
			}
		},
	}
}
