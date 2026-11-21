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

package v0

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubernetes/pkg/apis/abac"
)

// allAuthenticated 与 k8s.io/apiserver/pkg/authentication/user.AllAuthenticated 取值相同，
// 但这里刻意不直接引用：客户端库（必须包含类型定义）不应依赖服务端库。
const allAuthenticated = "system:authenticated"

// Convert_v0_Policy_To_abac_Policy 将 v0 策略转换为内部 abac 策略。
// v0 使用空字符串表达“匹配全部”的语义，需要在这里显式展开为通配符或 system:authenticated，
// 以保证转换后的内部策略与旧版行为一致。
func Convert_v0_Policy_To_abac_Policy(in *Policy, out *abac.Policy, s conversion.Scope) error {
	out.Spec.User = in.User
	out.Spec.Group = in.Group
	out.Spec.Namespace = in.Namespace
	out.Spec.Resource = in.Resource
	out.Spec.Readonly = in.Readonly

	// 在 v0 中，user 和 group 都未指定时匹配所有已认证主体。
	if len(in.User) == 0 && len(in.Group) == 0 {
		out.Spec.Group = allAuthenticated
	}
	// 在 v0 中，user 或 group 为 * 表示匹配所有已认证主体。
	if in.User == "*" || in.Group == "*" {
		out.Spec.Group = allAuthenticated
		out.Spec.User = ""
	}

	// 在 v0 中，namespace 留空匹配所有命名空间，需要展开为通配符 *。
	if len(in.Namespace) == 0 {
		out.Spec.Namespace = "*"
	}
	// 在 v0 中，resource 留空匹配所有资源，同样展开为通配符 *。
	if len(in.Resource) == 0 {
		out.Spec.Resource = "*"
	}
	// v0 中没有 API 组概念，任何规则都应匹配所有 API 组。
	out.Spec.APIGroup = "*"

	// 在 v0 中，namespace 和 resource 同时留空表示允许非资源路径访问。
	if len(in.Namespace) == 0 && len(in.Resource) == 0 {
		out.Spec.NonResourcePath = "*"
	}

	return nil
}
