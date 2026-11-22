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

package meta

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestPriorityRESTMapperResourceForErrorHandling 覆盖 ResourceFor 在代理返回错误时的行为：
// 错误必须原样上抛，同时仍要尽量返回已匹配到的资源，便于调用方在报错时提供上下文。
func TestPriorityRESTMapperResourceForErrorHandling(t *testing.T) {
	tcs := []struct {
		name string

		delegate         RESTMapper
		resourcePatterns []schema.GroupVersionResource
		result           schema.GroupVersionResource
		err              string
	}{
		{
			name:     "error",
			delegate: fixedRESTMapper{err: errors.New("delegateError")},
			err:      "delegateError",
		},
		{
			// 只有一个匹配时，即使代理报错也返回该结果。
			name:     "single hit + error",
			delegate: fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{{Resource: "single-hit"}}, err: errors.New("delegateError")},
			result:   schema.GroupVersionResource{Resource: "single-hit"},
			err:      "delegateError",
		},
		{
			// 通过 Group+AnyVersion+AnyResource 模式收窄到一组。
			name: "group selection + error",
			delegate: fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{
				{Group: "one", Version: "a", Resource: "first"},
				{Group: "two", Version: "b", Resource: "second"},
			}, err: errors.New("delegateError")},
			resourcePatterns: []schema.GroupVersionResource{
				{Group: "one", Version: AnyVersion, Resource: AnyResource},
			},
			result: schema.GroupVersionResource{Group: "one", Version: "a", Resource: "first"},
			err:    "delegateError",
		},

		{
			name:     "single hit",
			delegate: fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{{Resource: "single-hit"}}},
			result:   schema.GroupVersionResource{Resource: "single-hit"},
		},
		{
			// 无优先级模式且多个资源都能匹配时，不能擅自选择。
			name: "ambiguous match",
			delegate: fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{
				{Group: "one", Version: "a", Resource: "first"},
				{Group: "two", Version: "b", Resource: "second"},
			}},
			err: "matches multiple resources",
		},
		{
			name: "group selection",
			delegate: fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{
				{Group: "one", Version: "a", Resource: "first"},
				{Group: "two", Version: "b", Resource: "second"},
			}},
			resourcePatterns: []schema.GroupVersionResource{
				{Group: "one", Version: AnyVersion, Resource: AnyResource},
			},
			result: schema.GroupVersionResource{Group: "one", Version: "a", Resource: "first"},
		},
		{
			// 第一个模式匹配为空时应继续尝试后续模式，而不是立即失败。
			name: "empty match continues",
			delegate: fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{
				{Group: "one", Version: "a", Resource: "first"},
				{Group: "two", Version: "b", Resource: "second"},
			}},
			resourcePatterns: []schema.GroupVersionResource{
				{Group: "fail", Version: AnyVersion, Resource: AnyResource},
				{Group: "one", Version: AnyVersion, Resource: AnyResource},
			},
			result: schema.GroupVersionResource{Group: "one", Version: "a", Resource: "first"},
		},
		{
			// 先按组收窄，再按版本收窄；第一个模式已经唯一确定时后一个模式不再参与。
			name: "group followed by version selection",
			delegate: fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{
				{Group: "one", Version: "a", Resource: "first"},
				{Group: "two", Version: "b", Resource: "second"},
				{Group: "one", Version: "c", Resource: "third"},
			}},
			resourcePatterns: []schema.GroupVersionResource{
				{Group: "one", Version: AnyVersion, Resource: AnyResource},
				{Group: AnyGroup, Version: "a", Resource: AnyResource},
			},
			result: schema.GroupVersionResource{Group: "one", Version: "a", Resource: "first"},
		},
		{
			// 同一组版本下按资源名收窄。
			name: "resource selection",
			delegate: fixedRESTMapper{resourcesFor: []schema.GroupVersionResource{
				{Group: "one", Version: "a", Resource: "first"},
				{Group: "one", Version: "a", Resource: "second"},
			}},
			resourcePatterns: []schema.GroupVersionResource{
				{Group: AnyGroup, Version: AnyVersion, Resource: "second"},
			},
			result: schema.GroupVersionResource{Group: "one", Version: "a", Resource: "second"},
		},
	}

	for _, tc := range tcs {
		mapper := PriorityRESTMapper{Delegate: tc.delegate, ResourcePriority: tc.resourcePatterns}

		// ResourceFor 的入参只作为匹配目标；约束来自 ResourcePriority，
		// 这里传空 GVR 以让优先级模式自行筛选。
		actualResult, actualErr := mapper.ResourceFor(schema.GroupVersionResource{})
		if e, a := tc.result, actualResult; e != a {
			t.Errorf("%s: expected %v, got %v", tc.name, e, a)
		}
		if len(tc.err) == 0 && actualErr == nil {
			continue
		}
		if len(tc.err) == 0 && actualErr != nil {
			t.Errorf("%s: unexpected err: %v", tc.name, actualErr)
			continue
		}
		if len(tc.err) > 0 && actualErr == nil {
			t.Errorf("%s: missing expected err: %v", tc.name, tc.err)
			continue
		}
		if !strings.Contains(actualErr.Error(), tc.err) {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.err, actualErr)
		}
	}
}

// TestPriorityRESTMapperKindForErrorHandling 与 ResourceFor 版本一一对应，
// 验证 KindFor 的参数错误传播、优先级收窄与歧义检测。
func TestPriorityRESTMapperKindForErrorHandling(t *testing.T) {
	tcs := []struct {
		name string

		delegate     RESTMapper
		kindPatterns []schema.GroupVersionKind
		result       schema.GroupVersionKind
		err          string
	}{
		{
			name:     "error",
			delegate: fixedRESTMapper{err: errors.New("delegateErr")},
			err:      "delegateErr",
		},
		{
			name:     "single hit + error",
			delegate: fixedRESTMapper{kindsFor: []schema.GroupVersionKind{{Kind: "single-hit"}}, err: errors.New("delegateErr")},
			result:   schema.GroupVersionKind{Kind: "single-hit"},
			err:      "delegateErr",
		},
		{
			name: "group selection + error",
			delegate: fixedRESTMapper{kindsFor: []schema.GroupVersionKind{
				{Group: "one", Version: "a", Kind: "first"},
				{Group: "two", Version: "b", Kind: "second"},
			}, err: errors.New("delegateErr")},
			kindPatterns: []schema.GroupVersionKind{
				{Group: "one", Version: AnyVersion, Kind: AnyKind},
			},
			result: schema.GroupVersionKind{Group: "one", Version: "a", Kind: "first"},
			err:    "delegateErr",
		},

		{
			name:     "single hit",
			delegate: fixedRESTMapper{kindsFor: []schema.GroupVersionKind{{Kind: "single-hit"}}},
			result:   schema.GroupVersionKind{Kind: "single-hit"},
		},
		{
			name: "ambiguous match",
			delegate: fixedRESTMapper{kindsFor: []schema.GroupVersionKind{
				{Group: "one", Version: "a", Kind: "first"},
				{Group: "two", Version: "b", Kind: "second"},
			}},
			err: "matches multiple kinds",
		},
		{
			name: "group selection",
			delegate: fixedRESTMapper{kindsFor: []schema.GroupVersionKind{
				{Group: "one", Version: "a", Kind: "first"},
				{Group: "two", Version: "b", Kind: "second"},
			}},
			kindPatterns: []schema.GroupVersionKind{
				{Group: "one", Version: AnyVersion, Kind: AnyKind},
			},
			result: schema.GroupVersionKind{Group: "one", Version: "a", Kind: "first"},
		},
		{
			name: "empty match continues",
			delegate: fixedRESTMapper{kindsFor: []schema.GroupVersionKind{
				{Group: "one", Version: "a", Kind: "first"},
				{Group: "two", Version: "b", Kind: "second"},
			}},
			kindPatterns: []schema.GroupVersionKind{
				{Group: "fail", Version: AnyVersion, Kind: AnyKind},
				{Group: "one", Version: AnyVersion, Kind: AnyKind},
			},
			result: schema.GroupVersionKind{Group: "one", Version: "a", Kind: "first"},
		},
		{
			name: "group followed by version selection",
			delegate: fixedRESTMapper{kindsFor: []schema.GroupVersionKind{
				{Group: "one", Version: "a", Kind: "first"},
				{Group: "two", Version: "b", Kind: "second"},
				{Group: "one", Version: "c", Kind: "third"},
			}},
			kindPatterns: []schema.GroupVersionKind{
				{Group: "one", Version: AnyVersion, Kind: AnyKind},
				{Group: AnyGroup, Version: "a", Kind: AnyKind},
			},
			result: schema.GroupVersionKind{Group: "one", Version: "a", Kind: "first"},
		},
		{
			name: "kind selection",
			delegate: fixedRESTMapper{kindsFor: []schema.GroupVersionKind{
				{Group: "one", Version: "a", Kind: "first"},
				{Group: "one", Version: "a", Kind: "second"},
			}},
			kindPatterns: []schema.GroupVersionKind{
				{Group: AnyGroup, Version: AnyVersion, Kind: "second"},
			},
			result: schema.GroupVersionKind{Group: "one", Version: "a", Kind: "second"},
		},
	}

	for _, tc := range tcs {
		mapper := PriorityRESTMapper{Delegate: tc.delegate, KindPriority: tc.kindPatterns}

		actualResult, actualErr := mapper.KindFor(schema.GroupVersionResource{})
		if e, a := tc.result, actualResult; e != a {
			t.Errorf("%s: expected %v, got %v", tc.name, e, a)
		}
		if len(tc.err) == 0 && actualErr == nil {
			continue
		}
		if len(tc.err) == 0 && actualErr != nil {
			t.Errorf("%s: unexpected err: %v", tc.name, actualErr)
			continue
		}
		if len(tc.err) > 0 && actualErr == nil {
			t.Errorf("%s: missing expected err: %v", tc.name, tc.err)
			continue
		}
		if !strings.Contains(actualErr.Error(), tc.err) {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.err, actualErr)
		}
	}
}

// TestPriorityRESTMapperRESTMapping 验证 RESTMapping 在代理可能返回多个候选项时，
// 如何依据 KindPriority 选择唯一映射，以及如何传播/合并错误。
func TestPriorityRESTMapperRESTMapping(t *testing.T) {
	// 三个映射器分别代表：同 Kind 的 v1alpha1、同 Kind 的 v1、其他 group 的 v1。
	mapping1 := &RESTMapping{
		GroupVersionKind: schema.GroupVersionKind{Kind: "Foo", Version: "v1alpha1"},
	}
	mapping2 := &RESTMapping{
		GroupVersionKind: schema.GroupVersionKind{Kind: "Foo", Version: "v1"},
	}
	mapping3 := &RESTMapping{
		GroupVersionKind: schema.GroupVersionKind{Group: "other", Kind: "Foo", Version: "v1"},
	}
	allMappers := MultiRESTMapper{
		fixedRESTMapper{mappings: []*RESTMapping{mapping1}},
		fixedRESTMapper{mappings: []*RESTMapping{mapping2}},
		fixedRESTMapper{mappings: []*RESTMapping{mapping3}},
	}
	tcs := []struct {
		name string

		mapper PriorityRESTMapper
		input  schema.GroupKind
		result *RESTMapping
		err    error
	}{
		{
			name:   "empty",
			mapper: PriorityRESTMapper{Delegate: MultiRESTMapper{}},
			input:  schema.GroupKind{Kind: "Foo"},
			err:    &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}},
		},
		{
			// NoKindMatchError 表示"这个代理不认识该类型"，不算致命错误，继续尝试下一个代理。
			name:   "ignore not found",
			mapper: PriorityRESTMapper{Delegate: MultiRESTMapper{fixedRESTMapper{err: &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "IGNORE_THIS"}}}}},
			input:  schema.GroupKind{Kind: "Foo"},
			err:    &NoKindMatchError{GroupKind: schema.GroupKind{Kind: "Foo"}},
		},
		{
			// 非 NoKindMatch 的错误是真正的失败，应立即返回而不是继续。
			name:   "accept first failure",
			mapper: PriorityRESTMapper{Delegate: MultiRESTMapper{fixedRESTMapper{err: errors.New("fail on this")}, fixedRESTMapper{mappings: []*RESTMapping{mapping1}}}},
			input:  schema.GroupKind{Kind: "Foo"},
			err:    errors.New("fail on this"),
		},
		{
			name:   "result + error",
			mapper: PriorityRESTMapper{Delegate: fixedRESTMapper{mappings: []*RESTMapping{mapping1}, err: errors.New("fail on this")}},
			input:  schema.GroupKind{Kind: "Foo"},
			result: mapping1,
			err:    errors.New("fail on this"),
		},
		{
			// 歧义时返回 AmbiguousKindError，并携带全部候选项供调用方展示。
			name: "return error for ambiguous",
			mapper: PriorityRESTMapper{
				Delegate: allMappers,
			},
			input: schema.GroupKind{Kind: "Foo"},
			err: &AmbiguousKindError{
				PartialKind: schema.GroupVersionKind{Kind: "Foo"},
				MatchingKinds: []schema.GroupVersionKind{
					{Kind: "Foo", Version: "v1alpha1"},
					{Kind: "Foo", Version: "v1"},
					{Group: "other", Kind: "Foo", Version: "v1"},
				},
			},
		},
		{
			name: "accept only item",
			mapper: PriorityRESTMapper{
				Delegate: fixedRESTMapper{mappings: []*RESTMapping{mapping1}},
			},
			input:  schema.GroupKind{Kind: "Foo"},
			result: mapping1,
		},
		{
			// 优先级把 v1 排在 v1alpha1 之前，应选择 mapping2。
			name: "return single priority",
			mapper: PriorityRESTMapper{
				Delegate:     allMappers,
				KindPriority: []schema.GroupVersionKind{{Version: "v1", Kind: AnyKind}, {Version: "v1alpha1", Kind: AnyKind}},
			},
			input:  schema.GroupKind{Kind: "Foo"},
			result: mapping2,
		},
		{
			// core group 没有候选，AnyGroup+v1 模式跨组匹配到 other group 的 v1。
			name: "return out of group match",
			mapper: PriorityRESTMapper{
				Delegate:     allMappers,
				KindPriority: []schema.GroupVersionKind{{Group: AnyGroup, Version: "v1", Kind: AnyKind}, {Group: "other", Version: AnyVersion, Kind: AnyKind}},
			},
			input:  schema.GroupKind{Kind: "Foo"},
			result: mapping3,
		},
	}

	for _, tc := range tcs {
		actualResult, actualErr := tc.mapper.RESTMapping(tc.input)
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

// TestPriorityRESTMapperRESTMappingHonorsUserVersion 验证调用方显式指定版本时，
// 版本参数优先于 KindPriority：用户要 v1 就不能返回 preferred 的 v2alpha1。
func TestPriorityRESTMapperRESTMappingHonorsUserVersion(t *testing.T) {
	mappingV2alpha1 := &RESTMapping{
		GroupVersionKind: schema.GroupVersionKind{Group: "Bar", Kind: "Foo", Version: "v2alpha1"},
	}
	mappingV1 := &RESTMapping{
		GroupVersionKind: schema.GroupVersionKind{Group: "Bar", Kind: "Foo", Version: "v1"},
	}

	allMappers := MultiRESTMapper{
		fixedRESTMapper{mappings: []*RESTMapping{mappingV2alpha1}},
		fixedRESTMapper{mappings: []*RESTMapping{mappingV1}},
	}

	mapper := PriorityRESTMapper{
		Delegate:     allMappers,
		KindPriority: []schema.GroupVersionKind{{Group: "Bar", Version: "v2alpha1", Kind: AnyKind}, {Group: "Bar", Version: AnyVersion, Kind: AnyKind}},
	}

	// 优先级配置偏向 v2alpha1，但显式请求 v1 必须得到 v1 的映射。
	outMapping1, err := mapper.RESTMapping(schema.GroupKind{Group: "Bar", Kind: "Foo"}, "v1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if outMapping1 != mappingV1 {
		t.Errorf("asked for version %v, expected mapping for %v, got mapping for %v", "v1", mappingV1.GroupVersionKind, outMapping1.GroupVersionKind)
	}

	outMapping2, err := mapper.RESTMapping(schema.GroupKind{Group: "Bar", Kind: "Foo"}, "v2alpha1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if outMapping2 != mappingV2alpha1 {
		t.Errorf("asked for version %v, expected mapping for %v, got mapping for %v", "v2alpha1", mappingV2alpha1.GroupVersionKind, outMapping2.GroupVersionKind)
	}
}
