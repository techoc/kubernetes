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

// Package v2 负责把 CRD 的结构化 schema 转换为 OpenAPI v2 输出。
package v2

import (
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
)

// ToStructuralOpenAPIV2 将内部 OpenAPI v3 结构化 schema 转换为 v2 兼容的 schema。
func ToStructuralOpenAPIV2(in *structuralschema.Structural) *structuralschema.Structural {
	if in == nil {
		return nil
	}

	// 深拷贝，避免修改调用方持有的 schema。
	out := in.DeepCopy()

	// 递归移除 OpenAPI v2 不支持的字段
	mapper := structuralschema.Visitor{
		Structural: func(s *structuralschema.Structural) bool {
			changed := false
			if s.ValueValidation != nil {
				// OpenAPI v2 不支持这些组合关键字，直接丢弃。
				if s.ValueValidation.AllOf != nil {
					s.ValueValidation.AllOf = nil
					changed = true
				}
				if s.ValueValidation.OneOf != nil {
					s.ValueValidation.OneOf = nil
					changed = true
				}
				if s.ValueValidation.AnyOf != nil {
					s.ValueValidation.AnyOf = nil
					changed = true
				}
				if s.ValueValidation.Not != nil {
					s.ValueValidation.Not = nil
					changed = true
				}
			}

			// https://github.com/kubernetes/kube-openapi/pull/143/files#diff-ce77fea74b9dd098045004410023e0c3R219
			// v2 无法表达 nullable；去掉类型并清空子 schema，退化为无类型值。
			if s.Nullable {
				s.Type = ""
				s.Nullable = false

				// 无类型值若同时设置了 items 或 properties，会让 kubectl 出错
				// https://github.com/kubernetes/kube-openapi/pull/143/files#diff-62afddb578e9db18fb32ffb6b7802d92R183
				s.Items = nil
				s.Properties = nil

				changed = true
			}

			if s.XPreserveUnknownFields {
				// 同上：保留未知字段时设置 items 或 properties 会破坏 kubectl
				s.Items = nil
				s.Properties = nil

				changed = true
			}

			if s.Items == nil && s.Type == "array" {
				// kubectl 无法处理缺少 item schema 的数组，例如上面 XPreserveUnknownFields 的情形
				// https://github.com/kubernetes/kube-openapi/blob/64514a1d5d596b96e6f957e2be275ae14d6b0804/pkg/util/proto/document.go#L185
				s.Type = ""

				changed = true
			}

			if s.XPreserveUnknownFields && s.Type == "object" {
				// 与上面类似，kubectl 无法正确处理带 `x-kubernetes-preserve-unknown-fields: true` 的 object 字段
				s.Type = ""

				changed = true
			}

			for f, fs := range s.Properties {
				// v2 的 nullable 字段在 required 列表里会导致客户端解析失败，需剔除。
				if fs.Nullable {
					s.ValueValidation.Required, changed = filterOut(s.ValueValidation.Required, f)
				}
			}
			if s.AdditionalProperties != nil && s.AdditionalProperties.Structural != nil && s.AdditionalProperties.Structural.Nullable {
				s.ValueValidation.Required, changed = nil, true
			}

			return changed
		},
		// 上面的分支已丢弃所有 junctor，因此永远不会访问到嵌套的 value validation
		NestedValueValidation: nil,
	}
	mapper.Visit(out)

	return out
}

// filterOut 从切片 ss 中删除所有等于 x 的元素，返回新切片以及是否发生删除。
func filterOut(ss []string, x string) ([]string, bool) {
	var filtered []string
	for i, s := range ss {
		if s == x {
			// 首次命中时才分配 filtered，并拷贝前缀；之后的元素惰性追加。
			if filtered == nil {
				filtered = make([]string, i, len(ss))
				copy(filtered, ss[:i])
			}
		} else if filtered != nil {
			filtered = append(filtered, s)
		}
	}
	if filtered != nil {
		// 全部被删光时返回 nil，与“未修改”区分靠 bool。
		if len(filtered) == 0 {
			return nil, true
		}
		return filtered, true
	}
	return ss, false
}
