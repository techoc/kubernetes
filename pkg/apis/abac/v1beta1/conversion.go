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

package v1beta1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubernetes/pkg/apis/abac"
)

// allAuthenticated 的取值与 k8s.io/apiserver/pkg/authentication/user.AllAuthenticated 一致。
// 这里复制常量而不是直接引用，是因为客户端库（必须包含本包的 types）不能反过来依赖服务端库；
// 否则会把整个 apiserver 拖进客户端的依赖图。改动此值时需与上游定义保持一致。
const allAuthenticated = "system:authenticated"

// Convert_v1beta1_Policy_To_abac_Policy 把 v1beta1 策略转换为内部策略。
// 先交给生成的 autoConvert 处理字段逐一映射，再补上生成器无法表达的语义归一化：
// 旧版本用 "*" 表示所有主体，内部格式则只用 Group=system:authenticated 表达。
func Convert_v1beta1_Policy_To_abac_Policy(in *Policy, out *abac.Policy, s conversion.Scope) error {
	if err := autoConvert_v1beta1_Policy_To_abac_Policy(in, out, s); err != nil {
		return err
	}

	// v1beta1 中 user 或 group 的 "*" 都表示所有已认证主体；必须清空 User，
	// 否则会和 Group 同时匹配，导致策略主体语义被收窄或产生歧义。
	if in.Spec.User == "*" || in.Spec.Group == "*" {
		out.Spec.Group = allAuthenticated
		out.Spec.User = ""
	}

	return nil
}
