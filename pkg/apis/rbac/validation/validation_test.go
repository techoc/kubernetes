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

package validation

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/rbac"
)

// 测试 ClusterRoleBinding 校验：合法绑定应通过；roleRef、subject 等字段非法时应报出对应类型与字段的错误。
func TestValidateClusterRoleBinding(t *testing.T) {
	errs := ValidateClusterRoleBinding(
		&rbac.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "master"},
			RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "ClusterRole", Name: "valid"},
			Subjects: []rbac.Subject{
				{Name: "validsaname", APIGroup: "", Namespace: "foo", Kind: rbac.ServiceAccountKind},
				{Name: "valid@username", APIGroup: rbac.GroupName, Kind: rbac.UserKind},
				{Name: "valid@groupname", APIGroup: rbac.GroupName, Kind: rbac.GroupKind},
			},
		},
	)
	if len(errs) != 0 {
		t.Errorf("expected success: %v", errs)
	}

	errorCases := map[string]struct {
		A rbac.ClusterRoleBinding
		T field.ErrorType
		F string
	}{
		"bad group": {
			A: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				RoleRef:    rbac.RoleRef{APIGroup: "rbac.GroupName", Kind: "ClusterRole", Name: "valid"},
			},
			T: field.ErrorTypeNotSupported,
			F: "roleRef.apiGroup",
		},
		"bad kind": {
			A: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Type", Name: "valid"},
			},
			T: field.ErrorTypeNotSupported,
			F: "roleRef.kind",
		},
		"reference role": {
			A: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "valid"},
			},
			T: field.ErrorTypeNotSupported,
			F: "roleRef.kind",
		},
		"zero-length name": {
			A: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "ClusterRole", Name: "valid"},
			},
			T: field.ErrorTypeRequired,
			F: "metadata.name",
		},
		"bad role": {
			A: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "ClusterRole"},
			},
			T: field.ErrorTypeRequired,
			F: "roleRef.name",
		},
		"bad subject kind": {
			A: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "master"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "ClusterRole", Name: "valid"},
				Subjects:   []rbac.Subject{{Name: "subject"}},
			},
			T: field.ErrorTypeNotSupported,
			F: "subjects[0].kind",
		},
		"bad subject name": {
			A: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "master"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "ClusterRole", Name: "valid"},
				Subjects:   []rbac.Subject{{Namespace: "foo", Name: "subject:bad", Kind: rbac.ServiceAccountKind}},
			},
			T: field.ErrorTypeInvalid,
			F: "subjects[0].name",
		},
		"missing SA namespace": {
			A: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "master"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "ClusterRole", Name: "valid"},
				Subjects:   []rbac.Subject{{Name: "good", Kind: rbac.ServiceAccountKind}},
			},
			T: field.ErrorTypeRequired,
			F: "subjects[0].namespace",
		},
		"missing subject name": {
			A: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "master"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "ClusterRole", Name: "valid"},
				Subjects:   []rbac.Subject{{Namespace: "foo", Kind: rbac.ServiceAccountKind}},
			},
			T: field.ErrorTypeRequired,
			F: "subjects[0].name",
		},
	}
	for k, v := range errorCases {
		errs := ValidateClusterRoleBinding(&v.A)
		if len(errs) == 0 {
			t.Errorf("expected failure %s for %v", k, v.A)
			continue
		}
		for i := range errs {
			if errs[i].Type != v.T {
				t.Errorf("%s: expected errors to have type %s: %v", k, v.T, errs[i])
			}
			if errs[i].Field != v.F {
				t.Errorf("%s: expected errors to have field %s: %v", k, v.F, errs[i])
			}
		}
	}
}

// 测试 RoleBinding 校验，与 ClusterRoleBinding 类似，额外要求 namespace 非空。
func TestValidateRoleBinding(t *testing.T) {
	errs := ValidateRoleBinding(
		&rbac.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "master"},
			RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "valid"},
			Subjects: []rbac.Subject{
				{Name: "validsaname", APIGroup: "", Kind: rbac.ServiceAccountKind},
				{Name: "valid@username", APIGroup: rbac.GroupName, Kind: rbac.UserKind},
				{Name: "valid@groupname", APIGroup: rbac.GroupName, Kind: rbac.GroupKind},
			},
		},
	)
	if len(errs) != 0 {
		t.Errorf("expected success: %v", errs)
	}

	errorCases := map[string]struct {
		A rbac.RoleBinding
		T field.ErrorType
		F string
	}{
		"bad group": {
			A: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "default"},
				RoleRef:    rbac.RoleRef{APIGroup: "rbac.GroupName", Kind: "ClusterRole", Name: "valid"},
			},
			T: field.ErrorTypeNotSupported,
			F: "roleRef.apiGroup",
		},
		"bad kind": {
			A: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "default"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Type", Name: "valid"},
			},
			T: field.ErrorTypeNotSupported,
			F: "roleRef.kind",
		},
		"zero-length namespace": {
			A: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "valid"},
			},
			T: field.ErrorTypeRequired,
			F: "metadata.namespace",
		},
		"zero-length name": {
			A: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "valid"},
			},
			T: field.ErrorTypeRequired,
			F: "metadata.name",
		},
		"bad role": {
			A: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "default"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role"},
			},
			T: field.ErrorTypeRequired,
			F: "roleRef.name",
		},
		"bad subject kind": {
			A: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "master"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "valid"},
				Subjects:   []rbac.Subject{{Name: "subject"}},
			},
			T: field.ErrorTypeNotSupported,
			F: "subjects[0].kind",
		},
		"bad subject name": {
			A: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "master"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "valid"},
				Subjects:   []rbac.Subject{{Name: "subject:bad", Kind: rbac.ServiceAccountKind}},
			},
			T: field.ErrorTypeInvalid,
			F: "subjects[0].name",
		},
		"missing subject name": {
			A: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "master"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "valid"},
				Subjects:   []rbac.Subject{{Kind: rbac.ServiceAccountKind}},
			},
			T: field.ErrorTypeRequired,
			F: "subjects[0].name",
		},
	}
	for k, v := range errorCases {
		errs := ValidateRoleBinding(&v.A)
		if len(errs) == 0 {
			t.Errorf("expected failure %s for %v", k, v.A)
			continue
		}
		for i := range errs {
			if errs[i].Type != v.T {
				t.Errorf("%s: expected errors to have type %s: %v", k, v.T, errs[i])
			}
			if errs[i].Field != v.F {
				t.Errorf("%s: expected errors to have field %s: %v", k, v.F, errs[i])
			}
		}
	}
}

// 测试 RoleBinding 更新校验：roleRef 属于不可变字段，修改应被拒绝。
func TestValidateRoleBindingUpdate(t *testing.T) {
	old := &rbac.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "master", ResourceVersion: "1"},
		RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "valid"},
	}

	errs := ValidateRoleBindingUpdate(
		&rbac.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "master", ResourceVersion: "1"},
			RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "valid"},
		},
		old,
	)
	if len(errs) != 0 {
		t.Errorf("expected success: %v", errs)
	}

	errorCases := map[string]struct {
		A rbac.RoleBinding
		T field.ErrorType
		F string
	}{
		"changedRef": {
			A: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "master", ResourceVersion: "1"},
				RoleRef:    rbac.RoleRef{APIGroup: rbac.GroupName, Kind: "Role", Name: "changed"},
			},
			T: field.ErrorTypeInvalid,
			F: "roleRef",
		},
	}
	for k, v := range errorCases {
		errs := ValidateRoleBindingUpdate(&v.A, old)
		if len(errs) == 0 {
			t.Errorf("expected failure %s for %v", k, v.A)
			continue
		}
		for i := range errs {
			if errs[i].Type != v.T {
				t.Errorf("%s: expected errors to have type %s: %v", k, v.T, errs[i])
			}
			if errs[i].Field != v.F {
				t.Errorf("%s: expected errors to have field %s: %v", k, v.F, errs[i])
			}
		}
	}
}

// Role 校验的测试数据结构：包含待校验角色、期望是否报错、错误类型与字段路径。
type ValidateRoleTest struct {
	role    rbac.Role
	wantErr bool
	errType field.ErrorType
	field   string
}

// 按期望执行 ValidateRole 校验并核对错误类型与字段路径。
func (v ValidateRoleTest) test(t *testing.T) {
	errs := ValidateRole(&v.role)
	if len(errs) == 0 {
		if v.wantErr {
			t.Fatal("expected validation error")
		}
		return
	}
	if !v.wantErr {
		t.Errorf("didn't expect error, got %v", errs)
		return
	}
	for i := range errs {
		if errs[i].Type != v.errType {
			t.Errorf("expected errors to have type %s: %v", v.errType, errs[i])
		}
		if errs[i].Field != v.field {
			t.Errorf("expected errors to have field %s: %v", v.field, errs[i])
		}
	}
}

// ClusterRole 校验的测试数据结构，字段含义同 ValidateRoleTest。
type ValidateClusterRoleTest struct {
	role    rbac.ClusterRole
	wantErr bool
	errType field.ErrorType
	field   string
}

// 按期望执行 ValidateClusterRole 校验并核对错误类型与字段路径。
func (v ValidateClusterRoleTest) test(t *testing.T) {
	errs := ValidateClusterRole(&v.role, ClusterRoleValidationOptions{false})
	if len(errs) == 0 {
		if v.wantErr {
			t.Fatal("expected validation error")
		}
		return
	}
	if !v.wantErr {
		t.Errorf("didn't expect error, got %v", errs)
		return
	}
	for i := range errs {
		if errs[i].Type != v.errType {
			t.Errorf("expected errors to have type %s: %v", v.errType, errs[i])
		}
		if errs[i].Field != v.field {
			t.Errorf("expected errors to have field %s: %v", v.field, errs[i])
		}
	}
}

// 校验 Role 缺少 namespace 时应报 Required 错误。
func TestValidateRoleZeroLengthNamespace(t *testing.T) {
	ValidateRoleTest{
		role: rbac.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "default"},
		},
		wantErr: true,
		errType: field.ErrorTypeRequired,
		field:   "metadata.namespace",
	}.test(t)
}

// 校验 Role 缺少 name 时应报 Required 错误。
func TestValidateRoleZeroLengthName(t *testing.T) {
	ValidateRoleTest{
		role: rbac.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
		},
		wantErr: true,
		errType: field.ErrorTypeRequired,
		field:   "metadata.name",
	}.test(t)
}

// 校验合法的 Role 应通过。
func TestValidateRoleValidRole(t *testing.T) {
	ValidateRoleTest{
		role: rbac.Role{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "default",
			},
		},
		wantErr: false,
	}.test(t)
}

// 校验合法 ClusterRole（无需 namespace）应通过。
func TestValidateRoleValidRoleNoNamespace(t *testing.T) {
	ValidateClusterRoleTest{
		role: rbac.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name: "default",
			},
		},
		wantErr: false,
	}.test(t)
}

// 校验 ClusterRole 中的非资源 URL 规则应通过。
func TestValidateRoleNonResourceURL(t *testing.T) {
	ValidateClusterRoleTest{
		role: rbac.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name: "default",
			},
			Rules: []rbac.PolicyRule{{
				Verbs:           []string{"get"},
				NonResourceURLs: []string{"/*"},
			}},
		},
		wantErr: false,
	}.test(t)
}

// 校验 Role（命名空间内）中使用非资源 URL 应报 Invalid 错误。
func TestValidateRoleNamespacedNonResourceURL(t *testing.T) {
	ValidateRoleTest{
		role: rbac.Role{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "default",
			},
			Rules: []rbac.PolicyRule{{
				// 非资源 URL 对命名空间内的规则无效。
				Verbs:           []string{"get"},
				NonResourceURLs: []string{"/*"},
			}},
		},
		wantErr: true,
		errType: field.ErrorTypeInvalid,
		field:   "rules[0].nonResourceURLs",
	}.test(t)
}

// 校验非资源 URL 规则未提供 verbs 时应报 Required 错误。
func TestValidateRoleNonResourceURLNoVerbs(t *testing.T) {
	ValidateClusterRoleTest{
		role: rbac.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name: "default",
			},
			Rules: []rbac.PolicyRule{{
				Verbs:           []string{},
				NonResourceURLs: []string{"/*"},
			}},
		},
		wantErr: true,
		errType: field.ErrorTypeRequired,
		field:   "rules[0].verbs",
	}.test(t)
}

// 校验同一条规则中混用非资源 URL 与资源字段应报 Invalid 错误。
func TestValidateRoleMixedNonResourceAndResource(t *testing.T) {
	ValidateRoleTest{
		role: rbac.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "default",
				Namespace: "default",
			},
			Rules: []rbac.PolicyRule{{
				Verbs:           []string{"get"},
				NonResourceURLs: []string{"/*"},
				APIGroups:       []string{"v1"},
				Resources:       []string{"pods"},
			}},
		},
		wantErr: true,
		errType: field.ErrorTypeInvalid,
		field:   "rules[0].nonResourceURLs",
	}.test(t)
}

// 校验合法的资源规则应通过。
func TestValidateRoleValidResource(t *testing.T) {
	ValidateRoleTest{
		role: rbac.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "default",
				Namespace: "default",
			},
			Rules: []rbac.PolicyRule{{
				Verbs:     []string{"get"},
				APIGroups: []string{"v1"},
				Resources: []string{"pods"},
			}},
		},
		wantErr: false,
	}.test(t)
}

// 校验资源规则缺少 apiGroups 时应报 Required 错误。
func TestValidateRoleNoAPIGroup(t *testing.T) {
	ValidateRoleTest{
		role: rbac.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "default",
				Namespace: "default",
			},
			Rules: []rbac.PolicyRule{{
				Verbs:     []string{"get"},
				Resources: []string{"pods"},
			}},
		},
		wantErr: true,
		errType: field.ErrorTypeRequired,
		field:   "rules[0].apiGroups",
	}.test(t)
}

// 校验资源规则缺少 resources 时应报 Required 错误。
func TestValidateRoleNoResources(t *testing.T) {
	ValidateRoleTest{
		role: rbac.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "default",
				Namespace: "default",
			},
			Rules: []rbac.PolicyRule{{
				Verbs:     []string{"get"},
				APIGroups: []string{"v1"},
			}},
		},
		wantErr: true,
		errType: field.ErrorTypeRequired,
		field:   "rules[0].resources",
	}.test(t)
}
