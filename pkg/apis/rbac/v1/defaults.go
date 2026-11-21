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

package v1

import (
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 向 scheme 注册本版本的默认值函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ClusterRoleBinding 为 RoleRef 补齐默认的 APIGroup。
func SetDefaults_ClusterRoleBinding(obj *rbacv1.ClusterRoleBinding) {
	// 未指定 APIGroup 时填充 RBAC 内置组名。
	if len(obj.RoleRef.APIGroup) == 0 {
		obj.RoleRef.APIGroup = GroupName
	}
}

// SetDefaults_RoleBinding 为 RoleRef 补齐默认的 APIGroup。
func SetDefaults_RoleBinding(obj *rbacv1.RoleBinding) {
	if len(obj.RoleRef.APIGroup) == 0 {
		obj.RoleRef.APIGroup = GroupName
	}
}

// SetDefaults_Subject 按主体类型推导默认的 APIGroup。
func SetDefaults_Subject(obj *rbacv1.Subject) {
	if len(obj.APIGroup) == 0 {
		// ServiceAccount 属于核心组（空串），User 和 Group 属于 RBAC 组。
		switch obj.Kind {
		case rbacv1.ServiceAccountKind:
			obj.APIGroup = ""
		case rbacv1.UserKind:
			obj.APIGroup = GroupName
		case rbacv1.GroupKind:
			obj.APIGroup = GroupName
		}
	}
}
