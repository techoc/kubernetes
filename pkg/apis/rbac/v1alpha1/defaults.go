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

package v1alpha1

import (
	rbacv1alpha1 "k8s.io/api/rbac/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 向 scheme 注册本版本的默认值设置函数。
// 经由 RegisterDefaults 统一挂载，使反序列化后的对象自动补全缺省字段。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ClusterRoleBinding 为 ClusterRoleBinding 的 RoleRef 补全缺省 APIGroup。
// RBAC 内建角色的 API 组为空字符串，这里显式设为 rbac.authorization.k8s.io，
// 使后续校验与存储使用统一的规范值。
func SetDefaults_ClusterRoleBinding(obj *rbacv1alpha1.ClusterRoleBinding) {
	if len(obj.RoleRef.APIGroup) == 0 {
		obj.RoleRef.APIGroup = GroupName
	}
}

// SetDefaults_RoleBinding 为 RoleBinding 的 RoleRef 补全缺省 APIGroup。
// 与 ClusterRoleBinding 同理，未指定的 APIGroup 默认为本 RBAC API 组。
func SetDefaults_RoleBinding(obj *rbacv1alpha1.RoleBinding) {
	if len(obj.RoleRef.APIGroup) == 0 {
		obj.RoleRef.APIGroup = GroupName
	}
}

// SetDefaults_Subject 根据 Subject 的 Kind 推断并补全缺省的 APIVersion。
// ServiceAccount 属于核心 v1 组；User 与 Group 是本 RBAC 组下的虚拟资源，
// 因此使用本版本的 group/version。仅当用户未显式指定 APIVersion 时才填充。
func SetDefaults_Subject(obj *rbacv1alpha1.Subject) {
	if len(obj.APIVersion) == 0 {
		switch obj.Kind {
		case rbacv1alpha1.ServiceAccountKind:
			obj.APIVersion = "v1"
		case rbacv1alpha1.UserKind:
			obj.APIVersion = SchemeGroupVersion.String()
		case rbacv1alpha1.GroupKind:
			obj.APIVersion = SchemeGroupVersion.String()
		}
	}
}
