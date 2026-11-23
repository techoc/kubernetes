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

package reconciliation

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

// binding 构造只填 RoleRef 与 Subjects 的绑定；
// 被测逻辑只关心身份名，用最小对象能让差异断言不被无关字段干扰。
func binding(roleRef rbacv1.RoleRef, subjects []rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{RoleRef: roleRef, Subjects: subjects}
}

// ref 构造只含名称的 RoleRef；被测逻辑只比较角色名，其他字段留零值。
func ref(name string) rbacv1.RoleRef {
	return rbacv1.RoleRef{Name: name}
}

// subject 构造只含名称的 Subject，避免无关字段干扰差异断言。
func subject(name string) rbacv1.Subject {
	return rbacv1.Subject{Name: name}
}

// subjects 把若干名字批量转成 Subject 列表，方便表驱动用例声明主体集合。
func subjects(names ...string) []rbacv1.Subject {
	r := []rbacv1.Subject{}
	for _, name := range names {
		r = append(r, subject(name))
	}
	return r
}

// TestDiffObjectReferenceLists 覆盖集合差运算的几个关键性质：顺序无关、去重，
// 以及双向差异（只存在于 A / 只存在于 B）。
// 这些性质是删主体、补主体等协调操作正确性的前提。
func TestDiffObjectReferenceLists(t *testing.T) {
	tests := map[string]struct {
		A             []rbacv1.Subject
		B             []rbacv1.Subject
		ExpectedOnlyA []rbacv1.Subject
		ExpectedOnlyB []rbacv1.Subject
	}{
		"empty": {},

		// API 里的 subjects 顺序不保证稳定，只要集合相同就不该产生更新。
		"matching, order-independent": {
			A: subjects("foo", "bar"),
			B: subjects("bar", "foo"),
		},

		"partial match": {
			A:             subjects("foo", "bar"),
			B:             subjects("foo", "baz"),
			ExpectedOnlyA: subjects("bar"),
			ExpectedOnlyB: subjects("baz"),
		},

		"missing": {
			A:             subjects("foo"),
			B:             subjects("bar"),
			ExpectedOnlyA: subjects("foo"),
			ExpectedOnlyB: subjects("bar"),
		},

		// 重复的 subject 没有语义，差集应当把它折叠成一个，避免生成无意义的重复操作。
		"remove duplicates": {
			A:             subjects("foo", "foo"),
			B:             subjects("bar", "bar"),
			ExpectedOnlyA: subjects("foo"),
			ExpectedOnlyB: subjects("bar"),
		},
	}

	for k, tc := range tests {
		onlyA, onlyB := diffSubjectLists(tc.A, tc.B)
		if !apiequality.Semantic.DeepEqual(onlyA, tc.ExpectedOnlyA) {
			t.Errorf("%s: Expected %#v, got %#v", k, tc.ExpectedOnlyA, onlyA)
		}
		if !apiequality.Semantic.DeepEqual(onlyB, tc.ExpectedOnlyB) {
			t.Errorf("%s: Expected %#v, got %#v", k, tc.ExpectedOnlyB, onlyB)
		}
	}
}

// TestComputeUpdate 验证协调结果的两个维度：是否需要更新，以及更新后的绑定长什么样。
// 表的重点在 RemoveExtraSubjects 开关：开启时严格对齐期望状态，关闭时只补齐缺失主体，
// 因为其他控制器可能也会往绑定里加主体，全量覆盖会互相打架。
func TestComputeUpdate(t *testing.T) {
	tests := map[string]struct {
		ExpectedBinding     *rbacv1.ClusterRoleBinding
		ActualBinding       *rbacv1.ClusterRoleBinding
		RemoveExtraSubjects bool

		ExpectedUpdatedBinding *rbacv1.ClusterRoleBinding
		ExpectedUpdateNeeded   bool
	}{
		"match without union": {
			ExpectedBinding:     binding(ref("role"), subjects("a")),
			ActualBinding:       binding(ref("role"), subjects("a")),
			RemoveExtraSubjects: true,

			ExpectedUpdatedBinding: nil,
			ExpectedUpdateNeeded:   false,
		},
		"match with union": {
			ExpectedBinding:     binding(ref("role"), subjects("a")),
			ActualBinding:       binding(ref("role"), subjects("a")),
			RemoveExtraSubjects: false,

			ExpectedUpdatedBinding: nil,
			ExpectedUpdateNeeded:   false,
		},

		// RoleRef 决定绑定授予哪个角色，不属于可合并的集合，必须严格对齐。
		"different roleref with identical subjects": {
			ExpectedBinding:     binding(ref("role"), subjects("a")),
			ActualBinding:       binding(ref("differentRole"), subjects("a")),
			RemoveExtraSubjects: false,

			ExpectedUpdatedBinding: binding(ref("role"), subjects("a")),
			ExpectedUpdateNeeded:   true,
		},

		"extra subjects without union": {
			ExpectedBinding:     binding(ref("role"), subjects("a")),
			ActualBinding:       binding(ref("role"), subjects("a", "b")),
			RemoveExtraSubjects: true,

			ExpectedUpdatedBinding: binding(ref("role"), subjects("a")),
			ExpectedUpdateNeeded:   true,
		},
		// 合并模式下额外主体是别人加的，保留它们并判定无需更新。
		"extra subjects with union": {
			ExpectedBinding:     binding(ref("role"), subjects("a")),
			ActualBinding:       binding(ref("role"), subjects("a", "b")),
			RemoveExtraSubjects: false,

			ExpectedUpdatedBinding: nil,
			ExpectedUpdateNeeded:   false,
		},

		"missing subjects without union": {
			ExpectedBinding:     binding(ref("role"), subjects("a", "c")),
			ActualBinding:       binding(ref("role"), subjects("a", "b")),
			RemoveExtraSubjects: true,

			ExpectedUpdatedBinding: binding(ref("role"), subjects("a", "c")),
			ExpectedUpdateNeeded:   true,
		},
		// 合并模式下只补缺失的 c，已有的 b 原样保留。
		"missing subjects with union": {
			ExpectedBinding:     binding(ref("role"), subjects("a", "c")),
			ActualBinding:       binding(ref("role"), subjects("a", "b")),
			RemoveExtraSubjects: false,

			ExpectedUpdatedBinding: binding(ref("role"), subjects("a", "b", "c")),
			ExpectedUpdateNeeded:   true,
		},
	}

	for k, tc := range tests {
		actualRoleBinding := ClusterRoleBindingAdapter{ClusterRoleBinding: tc.ActualBinding}
		expectedRoleBinding := ClusterRoleBindingAdapter{ClusterRoleBinding: tc.ExpectedBinding}
		result, err := computeReconciledRoleBinding(actualRoleBinding, expectedRoleBinding, tc.RemoveExtraSubjects)
		if err != nil {
			t.Errorf("%s: %v", k, err)
			continue
		}
		updateNeeded := result.Operation != ReconcileNone
		updatedBinding := result.RoleBinding.(ClusterRoleBindingAdapter).ClusterRoleBinding
		if updateNeeded != tc.ExpectedUpdateNeeded {
			t.Errorf("%s: Expected\n\t%v\ngot\n\t%v (%v)", k, tc.ExpectedUpdateNeeded, updateNeeded, result.Operation)
			continue
		}
		if updateNeeded && !apiequality.Semantic.DeepEqual(updatedBinding, tc.ExpectedUpdatedBinding) {
			t.Errorf("%s: Expected\n\t%v %v\ngot\n\t%v %v", k, tc.ExpectedUpdatedBinding.RoleRef, tc.ExpectedUpdatedBinding.Subjects, updatedBinding.RoleRef, updatedBinding.Subjects)
		}
	}
}
