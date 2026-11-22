/*
Copyright 2014 The Kubernetes Authors.

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

package meta

import (
	"errors"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestMultiRESTMapperResourceFor 验证 MultiRESTMapper.ResourceFor 在多映射器组合下的行为：
// 空映射器、忽略未找到、以及首个映射器出错时直接返回该错误（不继续询问后面的映射器）。
func TestMultiRESTMapperResourceFor(t *testing.T) {
	tcs := []struct {
		name string

		mapper MultiRESTMapper
		input  schema.GroupVersionResource
		result schema.GroupVersionResource
		err    error
	}{
		{
			name:   "empty",
			mapper: MultiRESTMapper{},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: schema.GroupVersionResource{},
			err:    &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "foo"}},
		},
		{
			name:   "ignore not found",
			mapper: MultiRESTMapper{fixedRESTMapper{err: &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "IGNORE_THIS"}}}},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: schema.GroupVersionResource{},
			err:    &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "foo"}},
		},
		{
			name:   "accept first failure",
			mapper: MultiRESTMapper{fixedRESTMapper{err: errors.New("fail on this")}, fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{{Resource: "unused"}}}},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: schema.GroupVersionResource{},
			err:    errors.New("fail on this"),
		},
	}

	for _, tc := range tcs {
		actualResult, actualErr := tc.mapper.ResourceFor(tc.input)
		if e, a := tc.result, actualResult; e != a {
			t.Errorf("%s: expected %v, got %v", tc.name, e, a)
		}
		switch {
		case tc.err == nil && actualErr == nil:
		case tc.err == nil:
			t.Errorf("%s: unexpected error: %v", tc.name, actualErr)
		case actualErr == nil:
			t.Errorf("%s: expected error: %v got nil", tc.name, tc.err)
		case tc.err.Error() != actualErr.Error():
			t.Errorf("%s: expected %v, got %v", tc.name, tc.err, actualErr)
		}
	}
}

// TestMultiRESTMapperResourcesFor 验证 ResourcesFor 的合并规则：结果取各子映射器结果的并集，
// 并按首次出现的顺序去重；遇到 NoResourceMatchError 时跳过该子映射器并继续。
func TestMultiRESTMapperResourcesFor(t *testing.T) {
	tcs := []struct {
		name string

		mapper MultiRESTMapper
		input  schema.GroupVersionResource
		result []schema.GroupVersionResource
		err    error
	}{
		{
			name:   "empty",
			mapper: MultiRESTMapper{},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: nil,
			err:    &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "foo"}},
		},
		{
			name:   "ignore not found",
			mapper: MultiRESTMapper{fixedRESTMapper{err: &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "IGNORE_THIS"}}}},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: nil,
			err:    &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "foo"}},
		},
		{
			name:   "accept first failure",
			mapper: MultiRESTMapper{fixedRESTMapper{err: errors.New("fail on this")}, fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{{Resource: "unused"}}}},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: nil,
			err:    errors.New("fail on this"),
		},
		{
			name: "union and dedup",
			mapper: MultiRESTMapper{
				fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{{Resource: "dupe"}, {Resource: "first"}}},
				fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{{Resource: "dupe"}, {Resource: "second"}}},
			},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: []schema.GroupVersionResource{{Resource: "dupe"}, {Resource: "first"}, {Resource: "second"}},
		},
		{
			name: "skip not and continue",
			mapper: MultiRESTMapper{
				fixedRESTMapper{err: &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "IGNORE_THIS"}}},
				fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{{Resource: "first"}, {Resource: "second"}}},
			},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: []schema.GroupVersionResource{{Resource: "first"}, {Resource: "second"}},
		},
	}

	for _, tc := range tcs {
		actualResult, actualErr := tc.mapper.ResourcesFor(tc.input)
		if e, a := tc.result, actualResult; !reflect.DeepEqual(e, a) {
			t.Errorf("%s: expected %v, got %v", tc.name, e, a)
		}
		switch {
		case tc.err == nil && actualErr == nil:
		case tc.err == nil:
			t.Errorf("%s: unexpected error: %v", tc.name, actualErr)
		case actualErr == nil:
			t.Errorf("%s: expected error: %v got nil", tc.name, tc.err)
		case tc.err.Error() != actualErr.Error():
			t.Errorf("%s: expected %v, got %v", tc.name, tc.err, actualErr)
		}
	}
}

// TestMultiRESTMapperKindsFor 与 ResourcesFor 对应，验证 KindsFor 的并集去重与
// 跳过 NoResourceMatchError 的行为。
func TestMultiRESTMapperKindsFor(t *testing.T) {
	tcs := []struct {
		name string

		mapper MultiRESTMapper
		input  schema.GroupVersionResource
		result []schema.GroupVersionKind
		err    error
	}{
		{
			name:   "empty",
			mapper: MultiRESTMapper{},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: nil,
			err:    &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "foo"}},
		},
		{
			name:   "ignore not found",
			mapper: MultiRESTMapper{fixedRESTMapper{err: &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "IGNORE_THIS"}}}},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: nil,
			err:    &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "foo"}},
		},
		{
			name:   "accept first failure",
			mapper: MultiRESTMapper{fixedRESTMapper{err: errors.New("fail on this")}, fixedRESTMapper{kindsFor: []schema.GroupVersionKind{{Kind: "unused"}}}},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: nil,
			err:    errors.New("fail on this"),
		},
		{
			name: "union and dedup",
			mapper: MultiRESTMapper{
				fixedRESTMapper{kindsFor: []schema.GroupVersionKind{{Kind: "dupe"}, {Kind: "first"}}},
				fixedRESTMapper{kindsFor: []schema.GroupVersionKind{{Kind: "dupe"}, {Kind: "second"}}},
			},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: []schema.GroupVersionKind{{Kind: "dupe"}, {Kind: "first"}, {Kind: "second"}},
		},
		{
			name: "skip not and continue",
			mapper: MultiRESTMapper{
				fixedRESTMapper{err: &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "IGNORE_THIS"}}},
				fixedRESTMapper{kindsFor: []schema.GroupVersionKind{{Kind: "first"}, {Kind: "second"}}},
			},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: []schema.GroupVersionKind{{Kind: "first"}, {Kind: "second"}},
		},
	}

	for _, tc := range tcs {
		actualResult, actualErr := tc.mapper.KindsFor(tc.input)
		if e, a := tc.result, actualResult; !reflect.DeepEqual(e, a) {
			t.Errorf("%s: expected %v, got %v", tc.name, e, a)
		}
		switch {
		case tc.err == nil && actualErr == nil:
		case tc.err == nil:
			t.Errorf("%s: unexpected error: %v", tc.name, actualErr)
		case actualErr == nil:
			t.Errorf("%s: expected error: %v got nil", tc.name, tc.err)
		case tc.err.Error() != actualErr.Error():
			t.Errorf("%s: expected %v, got %v", tc.name, tc.err, actualErr)
		}
	}
}

// TestMultiRESTMapperKindFor 验证 KindFor 在首个子映射器返回非 NoResourceMatchError
// 时立即返回该错误，而不是向后续子映射器继续查询。
func TestMultiRESTMapperKindFor(t *testing.T) {
	tcs := []struct {
		name string

		mapper MultiRESTMapper
		input  schema.GroupVersionResource
		result schema.GroupVersionKind
		err    error
	}{
		{
			name:   "empty",
			mapper: MultiRESTMapper{},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: schema.GroupVersionKind{},
			err:    &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "foo"}},
		},
		{
			name:   "ignore not found",
			mapper: MultiRESTMapper{fixedRESTMapper{err: &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "IGNORE_THIS"}}}},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: schema.GroupVersionKind{},
			err:    &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Resource: "foo"}},
		},
		{
			name:   "accept first failure",
			mapper: MultiRESTMapper{fixedRESTMapper{err: errors.New("fail on this")}, fixedRESTMapper{kindsFor: []schema.GroupVersionKind{{Kind: "unused"}}}},
			input:  schema.GroupVersionResource{Resource: "foo"},
			result: schema.GroupVersionKind{},
			err:    errors.New("fail on this"),
		},
	}

	for _, tc := range tcs {
		actualResult, actualErr := tc.mapper.KindFor(tc.input)
		if e, a := tc.result, actualResult; e != a {
			t.Errorf("%s: expected %v, got %v", tc.name, e, a)
		}
		switch {
		case tc.err == nil && actualErr == nil:
		case tc.err == nil:
			t.Errorf("%s: unexpected error: %v", tc.name, actualErr)
		case actualErr == nil:
			t.Errorf("%s: expected error: %v got nil", tc.name, tc.err)
		case tc.err.Error() != actualErr.Error():
			t.Errorf("%s: expected %v, got %v", tc.name, tc.err, actualErr)
		}
	}
}

// TestMultiRESTMapperRESTMappings 验证 RESTMappings 的合并与版本查找行为：
// 未指定版本时把检索到的版本回填到 NoKindMatchError，子映射器的结果按顺序拼接。
func TestMultiRESTMapperRESTMappings(t *testing.T) {
	mapping1, mapping2 := &RESTMapping{}, &RESTMapping{}
	tcs := []struct {
		name string

		mapper    MultiRESTMapper
		groupKind schema.GroupKind
		versions  []string
		result    []*RESTMapping
		err       error
	}{
		{
			name:      "empty with no versions",
			mapper:    MultiRESTMapper{},
			groupKind: schema.GroupKind{Kind: "Foo"},
			result:    nil,
			err:       &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}},
		},
		{
			name:      "empty with one version",
			mapper:    MultiRESTMapper{},
			groupKind: schema.GroupKind{Kind: "Foo"},
			versions:  []string{"v1beta"},
			result:    nil,
			err:       &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}, SearchedVersions: []string{"v1beta"}},
		},
		{
			name:      "empty with multi(two) vesions",
			mapper:    MultiRESTMapper{},
			groupKind: schema.GroupKind{Kind: "Foo"},
			versions:  []string{"v1beta", "v2"},
			result:    nil,
			err:       &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}, SearchedVersions: []string{"v1beta", "v2"}},
		},
		{
			name:      "ignore not found with kind not exist",
			mapper:    MultiRESTMapper{fixedRESTMapper{err: &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "IGNORE_THIS"}}}},
			groupKind: schema.GroupKind{Kind: "Foo"},
			versions:  nil,
			result:    nil,
			err:       &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}},
		},
		{
			name:      "ignore not found with version not exist",
			mapper:    MultiRESTMapper{fixedRESTMapper{err: &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}, SearchedVersions: []string{"v1"}}}},
			groupKind: schema.GroupKind{Kind: "Foo"},
			versions:  []string{"v1beta"},
			result:    nil,
			err:       &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}, SearchedVersions: []string{"v1beta"}},
		},
		{
			name:      "ignore not found with multi versions not exist",
			mapper:    MultiRESTMapper{fixedRESTMapper{err: &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}, SearchedVersions: []string{"v1"}}}},
			groupKind: schema.GroupKind{Kind: "Foo"},
			versions:  []string{"v1beta", "v2"},
			result:    nil,
			err:       &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}, SearchedVersions: []string{"v1beta", "v2"}},
		},
		{
			name:      "accept first failure",
			mapper:    MultiRESTMapper{fixedRESTMapper{err: errors.New("fail on this")}, fixedRESTMapper{mappings: []*RESTMapping{mapping1}}},
			groupKind: schema.GroupKind{Kind: "Foo"},
			versions:  []string{"v1beta"},
			result:    nil,
			err:       errors.New("fail on this"),
		},
		{
			name:      "return both",
			mapper:    MultiRESTMapper{fixedRESTMapper{mappings: []*RESTMapping{mapping1}}, fixedRESTMapper{mappings: []*RESTMapping{mapping2}}},
			groupKind: schema.GroupKind{Kind: "Foo"},
			versions:  []string{"v1beta"},
			result:    []*RESTMapping{mapping1, mapping2},
		},
	}

	for _, tc := range tcs {
		actualResult, actualErr := tc.mapper.RESTMappings(tc.groupKind, tc.versions...)
		if e, a := tc.result, actualResult; !reflect.DeepEqual(e, a) {
			t.Errorf("%s: expected %v, got %v", tc.name, e, a)
		}
		switch {
		case tc.err == nil && actualErr == nil:
		case tc.err == nil:
			t.Errorf("%s: unexpected error: %v", tc.name, actualErr)
		case actualErr == nil:
			t.Errorf("%s: expected error: %v got nil", tc.name, tc.err)
		case tc.err.Error() != actualErr.Error():
			t.Errorf("%s: expected %v, got %v", tc.name, tc.err, actualErr)
		}
	}
}

// fixedRESTMapper 是 RESTMapper 的测试替身：每个方法直接返回预先配置好的结果或错误，
// 让用例能够精确控制 MultiRESTMapper 组合时各子映射器的表现。
type fixedRESTMapper struct {
	// resourcesFor/kindsFor/resourceFor/kindFor/mappings 是各查询方法返回的固定值。
	resourcesFor []schema.GroupVersionResource
	// kindsFor 是 KindsFor 的返回值，KindFor 复用它表示“唯一匹配”场景。
	kindsFor    []schema.GroupVersionKind
	resourceFor schema.GroupVersionResource
	kindFor     schema.GroupVersionKind
	mappings    []*RESTMapping

	// err 是所有方法统一返回的错误；测试通过它模拟“未找到”或“首个失败”。
	err error
}

// ResourceSingularizer 未在 MultiRESTMapper 中被调用，这里仅满足接口，固定返回配置的错误。
func (m fixedRESTMapper) ResourceSingularizer(resource string) (singular string, err error) {
	return "", m.err
}

// ResourcesFor 返回固定的复数资源列表。
func (m fixedRESTMapper) ResourcesFor(resource schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return m.resourcesFor, m.err
}

// KindsFor 返回固定的 Kind 列表，供 KindFor 构造单结果或多结果场景。
func (m fixedRESTMapper) KindsFor(resource schema.GroupVersionResource) (gvk []schema.GroupVersionKind, err error) {
	return m.kindsFor, m.err
}

// ResourceFor 返回固定的资源。
func (m fixedRESTMapper) ResourceFor(resource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return m.resourceFor, m.err
}

// KindFor 返回固定的 Kind。
func (m fixedRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return m.kindFor, m.err
}

// RESTMapping 始终返回错误：MultiRESTMapper 只使用 RESTMappings，不需要单数映射。
func (m fixedRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (mapping *RESTMapping, err error) {
	return nil, m.err
}

// RESTMappings 返回固定的映射列表。
func (m fixedRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) (mappings []*RESTMapping, err error) {
	return m.mappings, m.err
}

// ResourceIsValid 固定返回 false，避免替身意外地让资源被判定有效。
func (m fixedRESTMapper) ResourceIsValid(resource schema.GroupVersionResource) bool {
	return false
}
