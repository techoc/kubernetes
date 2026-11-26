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

package subfields

import (
	"testing"
)

// Test 验证 ifEnabled/ifDisabled 条件作用在 subfield 上：
// FeatureX 关闭时只校验 metadataDisabled.xDisabledField，
// 开启时只校验 metadata.xEnabledField。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// FeatureX 关闭：仅 disabled 分支的校验生效。
	st.Value(&Struct{
		// 全部为零值
	}).Opts(map[string]bool{"FeatureX": false}).ExpectValidateFalseByPath(map[string][]string{
		"metadataDisabled.xDisabledField": {"field Struct.ObjectMetaDisabled.XDisabledField"},
	})

	// FeatureX 开启：仅 enabled 分支的校验生效。
	st.Value(&Struct{
		// 全部为零值
	}).Opts(map[string]bool{"FeatureX": true}).ExpectValidateFalseByPath(map[string][]string{
		"metadata.xEnabledField": {"field Struct.ObjectMeta.XEnabledField"},
	})
}
