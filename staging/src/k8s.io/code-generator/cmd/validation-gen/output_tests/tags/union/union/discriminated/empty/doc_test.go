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

package empty

import (
	"testing"
)

// Test 覆盖空 discriminated union 的边界：判别字段为空或未知值时，
// 生成代码不应因缺少成员而误报（成员本身均未设置）。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 联合体的判别式可选。
	st.Value(&Struct{D: D("")}).ExpectValid()

	// 联合体的判别式应校验合法取值。
	st.Value(&Struct{D: D("Unknown")}).ExpectValid()
}
