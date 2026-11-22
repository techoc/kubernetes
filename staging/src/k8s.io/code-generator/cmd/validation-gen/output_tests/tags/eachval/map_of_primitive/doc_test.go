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

package mapofprimitive

import (
	"testing"
)

// Test 验证 eachVal 对 map 基本类型值与 typedef 值逐项生效，
// 且空 map 不产生任何错误。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 零值 map 没有任何元素，eachVal 不应上报错误。
	st.Value(&Struct{
		// All zero values.
	}).ExpectValid()

	// 每个 key 对应一条错误，路径带 [key] 后缀，校验信息来自 eachVal 标签。
	st.Value(&Struct{
		MapField:        map[string]string{"a": "A", "b": "B"},
		MapTypedefField: map[string]StringType{"a": "A", "b": "B"},
	}).ExpectValidateFalseByPath(map[string][]string{
		"mapField[a]":        {"field Struct.MapField[*]"},
		"mapField[b]":        {"field Struct.MapField[*]"},
		"mapTypedefField[a]": {"field Struct.MapTypedefField[*]"},
		"mapTypedefField[b]": {"field Struct.MapTypedefField[*]"},
	})
}
