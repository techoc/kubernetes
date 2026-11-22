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

package nonincluded

import (
	"testing"
)

// TestSubfieldObjectMetaValidationWithValidateFalse 验证 subfield 能指向
// 未包含在生成范围内（nonincluded）的外部包类型字段，且错误路径使用
// 完整的子字段路径。
func TestSubfieldObjectMetaValidationWithValidateFalse(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{}).ExpectValidateFalseByPath(map[string][]string{
		"other.StructType.stringField": {"subfield Struct.(other.StructType).StringField"},
	})
}
