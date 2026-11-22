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

package mapofstruct

import (
	"testing"
)

// Test 验证 eachVal 作用于 map 的值：每个条目各产生一条错误，
// 路径带上键名（mapField[a]），typedef 值类型也不例外。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		// All zero values.
	}).ExpectValid()

	st.Value(&Struct{
		MapField:        map[string]OtherStruct{"a": {}, "b": {}},
		MapTypedefField: map[string]OtherTypedefStruct{"a": {}, "b": {}},
	}).ExpectValidateFalseByPath(map[string][]string{
		"mapField[a]":        {"field Struct.MapField[*]"},
		"mapField[b]":        {"field Struct.MapField[*]"},
		"mapTypedefField[a]": {"field Struct.MapTypedefField[*]"},
		"mapTypedefField[b]": {"field Struct.MapTypedefField[*]"},
	})
}
