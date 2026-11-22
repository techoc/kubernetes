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

package schema

// Unfold 把 x-kubernetes-int-or-string 展开为显式的 integer/string anyOf。
// 下游消费者（校验器、OpenAPI 转换）因此不必单独理解该扩展；该方法会就地修改接收者。
func (s *Structural) Unfold() *Structural {
	if s == nil {
		return nil
	}

	mapper := Visitor{
		Structural: func(s *Structural) bool {
			if !s.XIntOrString {
				return false
			}

			// 已经展开过的节点不再处理，避免重复叠加 anyOf。
			skipAnyOf := isIntOrStringAnyOfPattern(s)
			skipFirstAllOfAnyOf := isIntOrStringAllOfPattern(s)
			if skipAnyOf || skipFirstAllOfAnyOf {
				return false
			}

			if s.ValueValidation == nil {
				s.ValueValidation = &ValueValidation{}
			}
			if s.ValueValidation.AnyOf == nil {
				s.ValueValidation.AnyOf = []NestedValueValidation{
					{ForbiddenGenerics: Generic{Type: "integer"}},
					{ForbiddenGenerics: Generic{Type: "string"}},
				}
			} else {
				// 已有 anyOf 时不能覆盖，改为在最前面追加一个 allOf 分支，
				// 既保留原有约束，又强制 int-or-string 语义。
				s.ValueValidation.AllOf = append([]NestedValueValidation{
					{
						ValueValidation: ValueValidation{
							AnyOf: []NestedValueValidation{
								{ForbiddenGenerics: Generic{Type: "integer"}},
								{ForbiddenGenerics: Generic{Type: "string"}},
							},
						},
					},
				}, s.ValueValidation.AllOf...)
			}

			return true
		},
		NestedValueValidation: nil, // x-kubernetes-int-or-string 不会出现在嵌套值校验中
	}
	mapper.Visit(s)

	return s
}
