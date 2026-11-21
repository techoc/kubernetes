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

package v1beta1

import (
	rbacv1beta1 "k8s.io/api/rbac/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 向 scheme 注册本版本的默认值设定函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ClusterRoleBinding 为 ClusterRoleBinding 的 RoleRef 填充默认 APIGroup。
func SetDefaults_ClusterRoleBinding(obj *rbacv1beta1.ClusterRoleBinding) {
	// RBAC 内置角色默认归属 rbac.authorization.k8s.io 组。
	if len(obj.RoleRef.APIGroup) == 0 {
		obj.RoleRef.APIGroup = GroupName
	}
}

// SetDefaults_RoleBinding 为 RoleBinding 的 RoleRef 填充默认 APIGroup。
func SetDefaults_RoleBinding(obj *rbacv1beta1.RoleBinding) {
	// 与 ClusterRoleBinding 一致，未指定 APIGroup 时视为 RBAC 内置组。
	if len(obj.RoleRef.APIGroup) == 0 {
		obj.RoleRef.APIGroup = GroupName
	}
}

// SetDefaults_Subject 按主体类型填充默认 APIGroup。
func SetDefaults_Subject(obj *rbacv1beta1.Subject) {
	if len(obj.APIGroup) == 0 {
		switch obj.Kind {
		case rbacv1beta1.ServiceAccountKind:
			// ServiceAccount 属于 core 组，APIGroup 必须为空字符串。
			obj.APIGroup = ""
		case rbacv1beta1.UserKind:
			obj.APIGroup = GroupName
		case rbacv1beta1.GroupKind:
			obj.APIGroup = GroupName
		}
	}
}
