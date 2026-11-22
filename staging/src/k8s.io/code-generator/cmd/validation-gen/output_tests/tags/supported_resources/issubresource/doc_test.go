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

package issubresource

import (
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestRegisterValidations 验证 +k8s:subresource 注册的资源路径：
// 只有声明的子资源与默认 "/" 命中校验，未声明的子资源返回内部错误，
// 而非静默放行。
func TestRegisterValidations(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 未指定子资源时走默认分支，但 T1 上仍有 validateFalse，故报内部错误。
	t1 := &T1{}
	st.Value(t1).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.InternalError(nil, fmt.Errorf("")),
	})

	// 已注册的 "scale" 子资源应走正常校验并通过。
	st.Value(t1).Subresources([]string{"scale"}).ExpectValid()

	// 未注册的子资源应报内部错误（不支持），不能当作合法请求处理。
	st.Value(t1).Subresources([]string{"unknown"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.InternalError(nil, fmt.Errorf("")),
	})
}
