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

package validatefalse

import (
	"testing"
)

// Test 验证 +k8s:validateFalse 始终产生指定消息的错误，
// 并确认 ratcheting 在值未变化时抑制该错误。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 零值同样触发校验。
	st.Value(&Struct{
		// 全为零值。
	}).ExpectValidateFalseByPath(map[string][]string{
		"stringField": {"field Struct.StringField"},
	})
	// 测试 ratcheting 行为
	st.Value(&Struct{}).OldValue(&Struct{}).ExpectValid()

	st.Value(&Struct{
		StringField: "abc",
	}).ExpectValidateFalseByPath(map[string][]string{
		"stringField": {"field Struct.StringField"},
	})
	// 测试 ratcheting 行为
	st.Value(&Struct{
		StringField: "abc",
	}).OldValue(&Struct{
		StringField: "abc",
	}).ExpectValid()
}
