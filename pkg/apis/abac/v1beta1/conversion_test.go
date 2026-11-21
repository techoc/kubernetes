/*
Copyright 2016 The Kubernetes Authors.

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

package v1beta1_test

import (
	"reflect"
	"testing"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/kubernetes/pkg/apis/abac"
	"k8s.io/kubernetes/pkg/apis/abac/v1beta1"
)

// TestV1Beta1Conversion 验证 v1beta1 版 ABAC Policy 向内部 Policy 的转换。
// 与 v0 不同，v1beta1 已有明确的 Spec 结构，缺省字段无需展开成通配符；
// 这里重点验证 User/Group 的 "*" 会被归一化成 system:authenticated，
// 且保留在合法字段上，避免把主体语义带错位置。
func TestV1Beta1Conversion(t *testing.T) {
	// 用被测语义命名 case，失败时能直接从用例名看出是哪种主体形式。
	testcases := map[string]struct {
		old      *v1beta1.Policy
		expected *abac.Policy
	}{
		// 普通 user 原样保留，不触发 "*" 的主体归一化。
		"user": {
			old:      &v1beta1.Policy{Spec: v1beta1.PolicySpec{User: "bob"}},
			expected: &abac.Policy{Spec: abac.PolicySpec{User: "bob"}},
		},

		// 普通 group 也原样保留；与 user 分开覆盖，防止映射时混淆两个字段。
		"group": {
			old:      &v1beta1.Policy{Spec: v1beta1.PolicySpec{Group: "mygroup"}},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: "mygroup"}},
		},

		// user 和 group 写 "*" 都代表所有已认证主体，转换后应一律落到 Group，
		// 并清空 User；这是内部格式唯一等价的表达，两个 case 必须一致。
		"* user": {
			old:      &v1beta1.Policy{Spec: v1beta1.PolicySpec{User: "*"}},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: user.AllAuthenticated}},
		},
		"* group": {
			old:      &v1beta1.Policy{Spec: v1beta1.PolicySpec{Group: "*"}},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: user.AllAuthenticated}},
		},
	}
	// 通过 Scheme.Convert 走真实转换路径，再逐字段比较内部对象，
	// 确保特殊主体映射没有改变其他字段。
	for k, tc := range testcases {
		internal := &abac.Policy{}
		if err := abac.Scheme.Convert(tc.old, internal, nil); err != nil {
			t.Errorf("%s: unexpected error: %v", k, err)
		}
		if !reflect.DeepEqual(internal, tc.expected) {
			t.Errorf("%s: expected\n\t%#v, got \n\t%#v", k, tc.expected, internal)
		}
	}
}
