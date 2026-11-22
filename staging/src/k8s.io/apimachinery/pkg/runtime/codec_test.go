/*
Copyright 2019 The Kubernetes Authors.

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

package runtime_test

import (
	"io"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimetesting "k8s.io/apimachinery/pkg/runtime/testing"
)

// gv 是测试中构造 GroupVersion 的简写。
func gv(group, version string) schema.GroupVersion {
	return schema.GroupVersion{Group: group, Version: version}
}

// gvk 是测试中构造 GroupVersionKind 的简写。
func gvk(group, version, kind string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: group, Version: version, Kind: kind}
}

// gk 是测试中构造 GroupKind 的简写。
func gk(group, kind string) schema.GroupKind {
	return schema.GroupKind{Group: group, Kind: kind}
}

// TestCoercingMultiGroupVersioner 验证 CoercingMultiGroupVersioner 的 kind 选择优先级：
// 先匹配带 kind 的首选 GroupKind，其次匹配仅指定 group 的首选项，都没有时退化为列表首项；
// 同时校验 Identifier 输出的稳定性（它参与序列化缓存的 key）。
func TestCoercingMultiGroupVersioner(t *testing.T) {
	testcases := []struct {
		name           string
		target         schema.GroupVersion
		preferredKinds []schema.GroupKind
		kinds          []schema.GroupVersionKind
		expectKind     schema.GroupVersionKind
		expectedId     string
	}{
		{
			name:           "matched preferred group/kind",
			target:         gv("mygroup", "__internal"),
			preferredKinds: []schema.GroupKind{gk("mygroup", "Foo"), gk("anothergroup", "Bar")},
			kinds:          []schema.GroupVersionKind{gvk("yetanother", "v1", "Baz"), gvk("anothergroup", "v1", "Bar")},
			expectKind:     gvk("mygroup", "__internal", "Bar"),
			expectedId:     "{\"accepted\":\"Foo.mygroup,Bar.anothergroup\",\"coerce\":\"true\",\"name\":\"multi\",\"target\":\"mygroup/__internal\"}",
		},
		{
			name:           "matched preferred group",
			target:         gv("mygroup", "__internal"),
			preferredKinds: []schema.GroupKind{gk("mygroup", ""), gk("anothergroup", "")},
			kinds:          []schema.GroupVersionKind{gvk("yetanother", "v1", "Baz"), gvk("anothergroup", "v1", "Bar")},
			expectKind:     gvk("mygroup", "__internal", "Bar"),
			expectedId:     "{\"accepted\":\".mygroup,.anothergroup\",\"coerce\":\"true\",\"name\":\"multi\",\"target\":\"mygroup/__internal\"}",
		},
		{
			name:           "no preferred group/kind match, uses first kind in list",
			target:         gv("mygroup", "__internal"),
			preferredKinds: []schema.GroupKind{gk("mygroup", ""), gk("anothergroup", "")},
			kinds:          []schema.GroupVersionKind{gvk("yetanother", "v1", "Baz"), gvk("yetanother", "v1", "Bar")},
			expectKind:     gvk("mygroup", "__internal", "Baz"),
			expectedId:     "{\"accepted\":\".mygroup,.anothergroup\",\"coerce\":\"true\",\"name\":\"multi\",\"target\":\"mygroup/__internal\"}",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			v := runtime.NewCoercingMultiGroupVersioner(tc.target, tc.preferredKinds...)
			kind, ok := v.KindForGroupVersionKinds(tc.kinds)
			if !ok {
				t.Error("got no kind")
			}
			if kind != tc.expectKind {
				t.Errorf("expected %#v, got %#v", tc.expectKind, kind)
			}
			if e, a := tc.expectedId, v.Identifier(); e != a {
				t.Errorf("unexpected identifier: %s, expected: %s", a, e)
			}
		})
	}
}

// mockEncoder 是只写出固定字符串的最小 Encoder 实现，用于隔离 Base64Serializer 的行为。
type mockEncoder struct{}

// Encode 写出固定内容，忽略传入对象。
func (m *mockEncoder) Encode(obj runtime.Object, w io.Writer) error {
	_, err := w.Write([]byte("mock-result"))
	return err
}

// Identifier 返回固定标识，满足 runtime.Encoder 的缓存 key 契约。
func (m *mockEncoder) Identifier() runtime.Identifier {
	return runtime.Identifier("mock-identifier")
}

// TestCacheableObject 用 mock encoder 包装出 Base64Serializer，交给通用
// CacheableObjectTest 校验其缓存标识与序列化一致性。
func TestCacheableObject(t *testing.T) {
	serializer := runtime.NewBase64Serializer(&mockEncoder{}, nil)
	runtimetesting.CacheableObjectTest(t, serializer)
}
