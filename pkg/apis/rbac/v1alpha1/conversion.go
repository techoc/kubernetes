/*
Copyright 2017 The Kubernetes Authors.

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

package v1alpha1

import (
	rbacv1alpha1 "k8s.io/api/rbac/v1alpha1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime/schema"
	api "k8s.io/kubernetes/pkg/apis/rbac"
)

// allAuthenticated 与 k8s.io/apiserver/pkg/authentication/user.AllAuthenticated 取值相同，
// 但客户端库（必须包含类型定义）不应依赖服务端库，故在此复制该常量。
const allAuthenticated = "system:authenticated"

// 把 v1alpha1 的 Subject 转换为内部 rbac.Subject。
func Convert_v1alpha1_Subject_To_rbac_Subject(in *rbacv1alpha1.Subject, out *api.Subject, s conversion.Scope) error {
	if err := autoConvert_v1alpha1_Subject_To_rbac_Subject(in, out, s); err != nil {
		return err
	}

	// 对 v1alpha1 能识别的三类 subject 显式设置 APIGroup。
	switch {
	case in.Kind == rbacv1alpha1.ServiceAccountKind:
		out.APIGroup = ""
	case in.Kind == rbacv1alpha1.UserKind:
		out.APIGroup = GroupName
	case in.Kind == rbacv1alpha1.GroupKind:
		out.APIGroup = GroupName
	default:
		// 对无法识别的 kind，尽量从 APIVersion 中解析出 group 作为 APIGroup。
		if gv, err := schema.ParseGroupVersion(in.APIVersion); err == nil {
			out.APIGroup = gv.Group
		}
	}

	// 在 v1alpha1 中，kind 为 User 且名称为 * 只匹配所有已认证用户。
	// 这样转换只是为了兼容旧的 RBAC 绑定。
	// v1beta1 中不应再包含针对 * 的特殊处理。
	if out.Kind == rbacv1alpha1.UserKind && out.APIGroup == GroupName && out.Name == "*" {
		out.Kind = rbacv1alpha1.GroupKind
		out.Name = allAuthenticated
	}

	return nil
}

// 把内部 rbac.Subject 转换为 v1alpha1 的 Subject。
func Convert_rbac_Subject_To_v1alpha1_Subject(in *api.Subject, out *rbacv1alpha1.Subject, s conversion.Scope) error {
	if err := autoConvert_rbac_Subject_To_v1alpha1_Subject(in, out, s); err != nil {
		return err
	}

	switch {
	case in.Kind == rbacv1alpha1.ServiceAccountKind && in.APIGroup == "":
		// ServiceAccount 使用 v1 作为 API 版本。
		out.APIVersion = "v1"
	case in.Kind == rbacv1alpha1.UserKind && in.APIGroup == GroupName:
		// rbac API 组中的 User 使用 v1alpha1 版本。
		out.APIVersion = SchemeGroupVersion.String()
	case in.Kind == rbacv1alpha1.GroupKind && in.APIGroup == GroupName:
		// rbac API 组中的 Group 使用 v1alpha1 版本。
		out.APIVersion = SchemeGroupVersion.String()
	default:
		// 其余情况只保留 group 信息，版本留空。
		out.APIVersion = schema.GroupVersion{Group: in.APIGroup}.String()
	}

	return nil
}
