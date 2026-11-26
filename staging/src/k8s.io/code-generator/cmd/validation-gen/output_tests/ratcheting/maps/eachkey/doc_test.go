/*
Copyright 2025 The Kubernetes Authors.

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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// 这是一个测试包。
// eachkey 验证 map 键校验（+k8s:eachKey）在不同键类型定义下的生成结果，
// 以及更新时默认 ratcheting 会跳过未变化的 map。
package eachkey

import (
	"testing"
)

// Test_Struct 覆盖四种 map 键校验来源：字段 eachKey、未校验键类型、
// 已校验键类型，以及类型定义上的 eachKey。错误路径需带上 "(keys)" 后缀。
func Test_Struct(t *testing.T) {
	// 每个 map 都放一个元素，确保键迭代代码被执行。
	mkTest := func() *Struct {
		return &Struct{
			MapField: map[string]string{"x": "y"},
			MapTypedefField: map[UnvalidatedStringType]string{
				"x": "y",
			},
			MapValidatedTypedefField: map[ValidatedStringType]string{
				"x": "y",
			},
			ValidatedMapTypeField: map[string]string{
				"x": "y",
			},
		}
	}
	st := localSchemeBuilder.Test(t)
	st.Value(mkTest()).ExpectValidateFalseByPath(map[string][]string{
		"mapField":                 {"field Struct.MapField(keys)"},
		"mapTypedefField":          {"field Struct.MapTypedefField(keys)"},
		"mapValidatedTypedefField": {"ValidatedStringType", "field Struct.MapValidatedTypedefField(keys)"},
		"validatedMapTypeField":    {"field Struct.ValidatedMapTypeField(keys)", "type ValidatedMapType(keys)"},
	})

	// 旧值相同时默认 ratcheting 应直接跳过，不重复报错。
	st.Value(mkTest()).OldValue(mkTest()).ExpectValid()
}
