/*
Copyright 2024 The Kubernetes Authors.

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

package eachkey

import (
	"testing"
)

// Test 验证 eachKey 对普通 map、typedef map 以及带自身校验的 typedef 键
// 都会对每个键执行校验；错误信息里同时保留字段路径与类型来源。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		// 全部为零值。
	}).ExpectValid()

	st.Value(&Struct{
		MapField:                 map[string]string{"a": "A", "b": "B"},
		MapTypedefField:          map[UnvalidatedStringType]string{"a": "A", "b": "B"},
		MapValidatedTypedefField: map[ValidatedStringType]string{"a": "A", "b": "B"},
		MapTypeField:             UnvalidatedMapType{"a": "A", "b": "B"},
		ValidatedMapTypeField:    ValidatedMapType{"a": "A", "b": "B"},
	}).ExpectValidateFalseByPath(map[string][]string{
		"mapField": {
			"Struct.MapField(keys)",
			"Struct.MapField(keys)",
		},
		"mapTypedefField": {
			"Struct.MapTypedefField(keys)",
			"Struct.MapTypedefField(keys)",
		},
		"mapValidatedTypedefField": {
			"Struct.MapValidatedTypedefField(keys)", "ValidatedStringType",
			"Struct.MapValidatedTypedefField(keys)", "ValidatedStringType",
		},
		"mapTypeField": {
			"Struct.MapTypeField(keys)",
			"Struct.MapTypeField(keys)",
		},
		"validatedMapTypeField": {
			"Struct.ValidatedMapTypeField(keys)", "ValidatedMapType(keys)",
			"Struct.ValidatedMapTypeField(keys)", "ValidatedMapType(keys)",
		},
	})
}
