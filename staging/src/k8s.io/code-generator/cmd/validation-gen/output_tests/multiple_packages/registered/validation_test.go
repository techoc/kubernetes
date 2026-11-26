/*
Copyright The Kubernetes Authors.

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

package registered

import (
	"testing"

	"k8s.io/code-generator/cmd/validation-gen/output_tests/multiple_packages/types"
)

// 测试通过 scheme 运行已注册的副本。
//
// 该包是注册了 scheme 的副本：校验函数通过 scheme 按类型查找，
// 因此错误路径能反映嵌套字段 t2.s，而不仅仅是顶层字段。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&types.T1{}).ExpectValidateFalseByPath(map[string][]string{
		"t2":   {"field T1.T2"},
		"t2.s": {"field T2.S"},
	})
}
