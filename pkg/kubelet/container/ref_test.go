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

package container

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
)

// TestFieldPath 验证根据容器在 Pod spec 中的位置生成字段路径：
// 有名容器用 {name}，无名容器回退到下标；找不到容器时应报错。
func TestFieldPath(t *testing.T) {
	pod := &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{
		{Name: "foo"},
		{Name: "bar"},
		{Name: ""},
		{Name: "baz"},
	}}}
	table := map[string]struct {
		pod       *v1.Pod
		container *v1.Container
		path      string
		success   bool
	}{
		"basic":            {pod, &v1.Container{Name: "foo"}, "spec.containers{foo}", true},
		"basic2":           {pod, &v1.Container{Name: "baz"}, "spec.containers{baz}", true},
		"emptyName":        {pod, &v1.Container{Name: ""}, "spec.containers[2]", true},
		"basicSamePointer": {pod, &pod.Spec.Containers[0], "spec.containers{foo}", true},
		"missing":          {pod, &v1.Container{Name: "qux"}, "", false},
	}

	// 逐项验证：失败用例要求返回错误，成功用例要求路径完全一致。
	for name, item := range table {
		res, err := fieldPath(item.pod, item.container)
		if !item.success {
			if err == nil {
				t.Errorf("%v: unexpected non-error", name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%v: unexpected error: %v", name, err)
			continue
		}
		if e, a := item.path, res; e != a {
			t.Errorf("%v: wanted %v, got %v", name, e, a)
		}
	}
}

// TestGenerateContainerRef 验证由 Pod 与容器生成 ObjectReference 的各字段。
func TestGenerateContainerRef(t *testing.T) {
	var (
		okPod = v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:            "ok",
				Namespace:       "test-ns",
				UID:             "bar",
				ResourceVersion: "42",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "by-name",
					},
					{},
				},
			},
		}
	)

	// 覆盖按名字匹配、空名字回退下标，以及隐式必需容器三种情况。
	cases := []struct {
		name      string
		pod       *v1.Pod
		container *v1.Container
		expected  *v1.ObjectReference
		success   bool
	}{
		{
			name: "by-name",
			pod:  &okPod,
			container: &v1.Container{
				Name: "by-name",
			},
			expected: &v1.ObjectReference{
				Kind:            "Pod",
				APIVersion:      "v1",
				Name:            "ok",
				Namespace:       "test-ns",
				UID:             "bar",
				ResourceVersion: "42",
				FieldPath:       ".spec.containers{by-name}",
			},
			success: true,
		},
		{
			name:      "no-name",
			pod:       &okPod,
			container: &v1.Container{},
			expected: &v1.ObjectReference{
				Kind:            "Pod",
				APIVersion:      "v1",
				Name:            "ok",
				Namespace:       "test-ns",
				UID:             "bar",
				ResourceVersion: "42",
				FieldPath:       ".spec.containers[1]",
			},
			success: true,
		},
		{
			name: "implicitly-required",
			pod:  &okPod,
			container: &v1.Container{
				Name: "net",
			},
			expected: &v1.ObjectReference{
				Kind:            "Pod",
				APIVersion:      "v1",
				Name:            "ok",
				Namespace:       "test-ns",
				UID:             "bar",
				ResourceVersion: "42",
				FieldPath:       "implicitly required container net",
			},
			success: true,
		},
	}

	// 先校验成功/失败预期，再逐字段比对引用内容。
	for _, tc := range cases {
		actual, err := GenerateContainerRef(tc.pod, tc.container)
		if err != nil {
			if tc.success {
				t.Errorf("%v: unexpected error: %v", tc.name, err)
			}

			continue
		}

		if !tc.success {
			t.Errorf("%v: unexpected success", tc.name)
			continue
		}

		if e, a := tc.expected.Kind, actual.Kind; e != a {
			t.Errorf("%v: kind: expected %v, got %v", tc.name, e, a)
		}
		if e, a := tc.expected.APIVersion, actual.APIVersion; e != a {
			t.Errorf("%v: apiVersion: expected %v, got %v", tc.name, e, a)
		}
		if e, a := tc.expected.Name, actual.Name; e != a {
			t.Errorf("%v: name: expected %v, got %v", tc.name, e, a)
		}
		if e, a := tc.expected.Namespace, actual.Namespace; e != a {
			t.Errorf("%v: namespace: expected %v, got %v", tc.name, e, a)
		}
		if e, a := tc.expected.UID, actual.UID; e != a {
			t.Errorf("%v: uid: expected %v, got %v", tc.name, e, a)
		}
		if e, a := tc.expected.ResourceVersion, actual.ResourceVersion; e != a {
			t.Errorf("%v: kind: expected %v, got %v", tc.name, e, a)
		}
	}
}
