/*
Copyright 2018 The Kubernetes Authors.

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

package reference

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestRuntimeObj 是用于测试的最小对象：只有 TypeMeta/ObjectMeta。
// DeepCopyObject 故意 panic，确保 GetReference 不会意外触发深拷贝。
type TestRuntimeObj struct {
	metav1.TypeMeta
	metav1.ObjectMeta
}

func (o *TestRuntimeObj) DeepCopyObject() runtime.Object {
	panic("die")
}

// TestGetReferenceRefVersion 验证 Reference.APIVersion 由 scheme 中注册的 GV 决定，
// 核心组（空 group）只输出版本号，命名组输出 group/version。
func TestGetReferenceRefVersion(t *testing.T) {
	tests := []struct {
		name               string
		input              *TestRuntimeObj
		groupVersion       schema.GroupVersion
		expectedRefVersion string
	}{
		{
			name: "v1 GV from scheme",
			input: &TestRuntimeObj{
				ObjectMeta: metav1.ObjectMeta{},
			},
			groupVersion:       schema.GroupVersion{Group: "", Version: "v1"},
			expectedRefVersion: "v1",
		},
		{
			name: "foo.group/v3 GV from scheme",
			input: &TestRuntimeObj{
				ObjectMeta: metav1.ObjectMeta{},
			},
			groupVersion:       schema.GroupVersion{Group: "foo.group", Version: "v3"},
			expectedRefVersion: "foo.group/v3",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			scheme.AddKnownTypes(test.groupVersion, &TestRuntimeObj{})
			ref, err := GetReference(scheme, test.input)
			if err != nil {
				t.Fatal(err)
			}
			if test.expectedRefVersion != ref.APIVersion {
				t.Errorf("expected %q, got %q", test.expectedRefVersion, ref.APIVersion)
			}
		})
	}
}

// TestGetReferenceNilScheme 验证没有 scheme 时无法推断 GVK，应返回明确错误。
func TestGetReferenceNilScheme(t *testing.T) {
	input := &TestRuntimeObj{
		ObjectMeta: metav1.ObjectMeta{},
	}
	_, err := GetReference(nil, input)
	if err == nil {
		t.Fatal("expected an error")
	}
	if err.Error() != "scheme is required to look up gvk" {
		t.Errorf("expected %q, got %q", "scheme is required to look up gvk", err.Error())
	}
}

// TestGetReferenceNilSchemeWithPopulatedGVK 验证对象自带 TypeMeta 时，
// 即使没有 scheme 也能构造 Reference（这是 scheme 为 nil 时唯一可用的路径）。
func TestGetReferenceNilSchemeWithPopulatedGVK(t *testing.T) {
	input := &TestRuntimeObj{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "foo.group/v3",
			Kind:       "Bar",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "a-name",
		},
	}
	ref, err := GetReference(nil, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ref.APIVersion != "foo.group/v3" {
		t.Errorf("expected %q, got %q", "foo.group/v3", ref.APIVersion)
	}
	if ref.Kind != "Bar" {
		t.Errorf("expected %q, got %q", "Bar", ref.Kind)
	}
	if ref.Name != "a-name" {
		t.Errorf("expected %q, got %q", "a-name", ref.Name)
	}
}
