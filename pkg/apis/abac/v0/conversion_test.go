/*
Copyright 2015 The Kubernetes Authors.

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

package v0_test

import (
	"reflect"
	"testing"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/kubernetes/pkg/apis/abac"
	"k8s.io/kubernetes/pkg/apis/abac/v0"
)

// TestV0Conversion 验证 v0 版 ABAC Policy 向内部 Policy 的转换结果。
// v0 是最早的策略格式，缺省字段表示“通配”，所以转换必须把这些缺省值
// 显式展开成明确的通配符（Resource、Namespace、APIGroup、NonResourcePath 的 "*"），
// 并把 user/group 的 "*" 归一化为 system:authenticated；漏掉任何一项，
// 后续鉴权都会把策略解读成另一种含义。
func TestV0Conversion(t *testing.T) {
	// 用输入策略的语义特征作为 case 名，便于在失败信息里直接定位是哪类策略。
	// 期望值写全所有字段，是为了用 DeepEqual 精确校验“缺省即通配”的展开规则。
	testcases := map[string]struct {
		old      *v0.Policy
		expected *abac.Policy
	}{
		// 完全空的规则对所有人放行所有资源：v0 的空值等价于全通配，
		// 因此期望值里除了展开资源维度，主体也要落到 system:authenticated。
		"empty": {
			old:      &v0.Policy{},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: user.AllAuthenticated, Readonly: false, NonResourcePath: "*", Namespace: "*", Resource: "*", APIGroup: "*"}},
		},

		// 显式指定 user 时原样保留，主体不会被归一化为 all authenticated。
		"user": {
			old:      &v0.Policy{User: "bob"},
			expected: &abac.Policy{Spec: abac.PolicySpec{User: "bob", Readonly: false, NonResourcePath: "*", Namespace: "*", Resource: "*", APIGroup: "*"}},
		},

		// 显式指定 group 时同样保留；此时不再匹配所有用户，
		// 所以不能套用 user/group 为 "*" 时的归一化规则。
		"group": {
			old:      &v0.Policy{Group: "mygroup"},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: "mygroup", Readonly: false, NonResourcePath: "*", Namespace: "*", Resource: "*", APIGroup: "*"}},
		},

		// user 或 group 写 "*" 都表示所有已认证主体，转换后统一落到 Group 字段，
		// 并清空 User——这是内部格式表达“所有主体”的唯一方式。
		"* user": {
			old:      &v0.Policy{User: "*"},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: user.AllAuthenticated, Readonly: false, NonResourcePath: "*", Namespace: "*", Resource: "*", APIGroup: "*"}},
		},
		"* group": {
			old:      &v0.Policy{Group: "*"},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: user.AllAuthenticated, Readonly: false, NonResourcePath: "*", Namespace: "*", Resource: "*", APIGroup: "*"}},
		},

		// 指定 namespace 后，非资源路径不能再通配：否则一条只针对某命名空间的规则
		// 会意外匹配所有非资源请求，必须把 NonResourcePath 清空。
		"namespace": {
			old:      &v0.Policy{Namespace: "myns"},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: user.AllAuthenticated, Readonly: false, NonResourcePath: "", Namespace: "myns", Resource: "*", APIGroup: "*"}},
		},

		// 同理，指定 resource 后也要去掉非资源路径上的 "*"，
		// 避免策略同时覆盖资源型与非资源型请求。
		"resource": {
			old:      &v0.Policy{Resource: "myresource"},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: user.AllAuthenticated, Readonly: false, NonResourcePath: "", Namespace: "*", Resource: "myresource", APIGroup: "*"}},
		},

		// 同时指定 namespace 和 resource 时仍要清空非资源路径：
		// 这两个维度都是资源请求专属，与非资源路径互斥。
		"namespace+resource": {
			old:      &v0.Policy{Namespace: "myns", Resource: "myresource"},
			expected: &abac.Policy{Spec: abac.PolicySpec{Group: user.AllAuthenticated, Readonly: false, NonResourcePath: "", Namespace: "myns", Resource: "myresource", APIGroup: "*"}},
		},
	}
	// 逐个用 Scheme 把旧版本对象转换成内部对象；Convert 失败属于测试失败。
	// 最后用 DeepEqual 做全字段比较，防止遗漏任何被展开的通配字段。
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
