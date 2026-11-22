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

package root

import (
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestRegisterValidations 验证根资源的校验注册：根资源默认受支持，
// 而声明子资源时需显式 supportsSubresource，否则应报内部错误。
func TestRegisterValidations(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	t1 := &T1{}

	// 不带子资源时正常通过。
	st.Value(t1).ExpectValid()

	// 声明了 "scale" 子资源，但 T1 未声明支持它，应报 InternalError。
	st.Value(t1).Subresources([]string{"scale"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.InternalError(nil, fmt.Errorf("")),
	})

	// 多级子资源路径同样按不支持处理。
	st.Value(t1).Subresources([]string{"x", "y"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.InternalError(nil, fmt.Errorf("")),
	})
}
