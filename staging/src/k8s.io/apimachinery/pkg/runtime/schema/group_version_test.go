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

package schema

import (
	"testing"
)

// TestGroupVersionParse 验证 "group/version" 字符串的解析：
// 无斜杠时整体作为 version；"/v1" 视为空 group + v1；"v1/" 视为 group=v1 + 空 version；
// 两个斜杠则报错。
func TestGroupVersionParse(t *testing.T) {
	tests := []struct {
		input string
		out   GroupVersion
		err   func(error) bool
	}{
		{input: "v1", out: GroupVersion{Version: "v1"}},
		{input: "v2", out: GroupVersion{Version: "v2"}},
		{input: "/v1", out: GroupVersion{Version: "v1"}},
		{input: "v1/", out: GroupVersion{Group: "v1"}},
		{input: "/v1/", err: func(err error) bool { return err.Error() == "unexpected GroupVersion string: /v1/" }},
		{input: "v1/a", out: GroupVersion{Group: "v1", Version: "a"}},
	}
	for i, test := range tests {
		out, err := ParseGroupVersion(test.input)
		if test.err == nil && err != nil || err == nil && test.err != nil {
			t.Errorf("%d: unexpected error: %v", i, err)
			continue
		}
		if test.err != nil && !test.err(err) {
			t.Errorf("%d: unexpected error: %v", i, err)
			continue
		}
		if out != test.out {
			t.Errorf("%d: unexpected output: %#v", i, out)
		}
	}
}

// TestGroupResourceParse 验证 "resource.group" 形式的解析：只按最后一个点切分，
// 因此 "b.v1.a" 解析为 resource=b、group=v1.a（group 本身可以含点）。
func TestGroupResourceParse(t *testing.T) {
	tests := []struct {
		input string
		out   GroupResource
	}{
		{input: "v1", out: GroupResource{Resource: "v1"}},
		{input: ".v1", out: GroupResource{Group: "v1"}},
		{input: "v1.", out: GroupResource{Resource: "v1"}},
		{input: "v1.a", out: GroupResource{Group: "a", Resource: "v1"}},
		{input: "b.v1.a", out: GroupResource{Group: "v1.a", Resource: "b"}},
	}
	for i, test := range tests {
		out := ParseGroupResource(test.input)
		if out != test.out {
			t.Errorf("%d: unexpected output: %#v", i, out)
		}
	}
}

// TestParseResourceArg 验证 kubectl 风格 "resource.version.group" 参数的解析：
// 段数不足 3 时只返回 GroupResource，段数足够时同时返回带版本的 GVR。
func TestParseResourceArg(t *testing.T) {
	tests := []struct {
		input string
		gvr   *GroupVersionResource
		gr    GroupResource
	}{
		{input: "v1", gr: GroupResource{Resource: "v1"}},
		{input: ".v1", gr: GroupResource{Group: "v1"}},
		{input: "v1.", gr: GroupResource{Resource: "v1"}},
		{input: "v1.a", gr: GroupResource{Group: "a", Resource: "v1"}},
		{input: "b.v1.a", gvr: &GroupVersionResource{Group: "a", Version: "v1", Resource: "b"}, gr: GroupResource{Group: "v1.a", Resource: "b"}},
	}
	for i, test := range tests {
		gvr, gr := ParseResourceArg(test.input)
		if (gvr != nil && test.gvr == nil) || (gvr == nil && test.gvr != nil) || (test.gvr != nil && *gvr != *test.gvr) {
			t.Errorf("%d: unexpected output: %#v", i, gvr)
		}
		if gr != test.gr {
			t.Errorf("%d: unexpected output: %#v", i, gr)
		}
	}
}

// TestKindForGroupVersionKinds 验证 GroupVersions 的优先级选择：
// 输入中若存在该组已注册的版本，则原样返回；否则回退到该组中优先级最高的版本；
// 组完全未知时 ok=false。
func TestKindForGroupVersionKinds(t *testing.T) {
	gvks := GroupVersions{
		GroupVersion{Group: "batch", Version: "v1"},
		GroupVersion{Group: "batch", Version: "v2alpha1"},
		GroupVersion{Group: "policy", Version: "v1beta1"},
	}
	cases := []struct {
		input  []GroupVersionKind
		target GroupVersionKind
		ok     bool
	}{
		{
			input:  []GroupVersionKind{{Group: "batch", Version: "v2alpha1", Kind: "ScheduledJob"}},
			target: GroupVersionKind{Group: "batch", Version: "v2alpha1", Kind: "ScheduledJob"},
			ok:     true,
		},
		{
			input:  []GroupVersionKind{{Group: "batch", Version: "v3alpha1", Kind: "CronJob"}},
			target: GroupVersionKind{Group: "batch", Version: "v1", Kind: "CronJob"},
			ok:     true,
		},
		{
			input:  []GroupVersionKind{{Group: "policy", Version: "v1beta1", Kind: "PodDisruptionBudget"}},
			target: GroupVersionKind{Group: "policy", Version: "v1beta1", Kind: "PodDisruptionBudget"},
			ok:     true,
		},
		{
			input:  []GroupVersionKind{{Group: "apps", Version: "v1alpha1", Kind: "StatefulSet"}},
			target: GroupVersionKind{},
			ok:     false,
		},
	}

	for i, c := range cases {
		target, ok := gvks.KindForGroupVersionKinds(c.input)
		if c.target != target {
			t.Errorf("%d: unexpected target: %v, expected %v", i, target, c.target)
		}
		if c.ok != ok {
			t.Errorf("%d: unexpected ok: %v, expected %v", i, ok, c.ok)
		}
	}
}

// TestParseKindArg 验证 kubectl 风格 "kind.version.group" 参数的解析，
// 与 ParseResourceArg 类似，段数不足 3 时只返回 GroupKind。
func TestParseKindArg(t *testing.T) {
	tests := []struct {
		input string
		gvk   *GroupVersionKind
		gk    GroupKind
	}{
		{input: "Pod", gk: GroupKind{Kind: "Pod"}},
		{input: ".apps", gk: GroupKind{Group: "apps"}},
		{input: "Pod.", gk: GroupKind{Kind: "Pod"}},
		{input: "StatefulSet.apps", gk: GroupKind{Group: "apps", Kind: "StatefulSet"}},
		{input: "StatefulSet.v1.apps", gvk: &GroupVersionKind{Group: "apps", Version: "v1", Kind: "StatefulSet"}, gk: GroupKind{Group: "v1.apps", Kind: "StatefulSet"}},
	}
	for i, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			gvk, gk := ParseKindArg(test.input)
			if (gvk != nil && test.gvk == nil) || (gvk == nil && test.gvk != nil) || (test.gvk != nil && *gvk != *test.gvk) {
				t.Errorf("%d: expected output: %#v, got: %#v", i, test.gvk, gvk)
			}
			if gk != test.gk {
				t.Errorf("%d: expected output: %#v, got: %#v", i, test.gk, gk)
			}
		})
	}
}

// TestParseGroupKind 验证 "Kind.group" 形式的解析：只按最后一个点切分。
func TestParseGroupKind(t *testing.T) {
	tests := []struct {
		input string
		out   GroupKind
	}{
		{input: "Pod", out: GroupKind{Kind: "Pod"}},
		{input: ".StatefulSet", out: GroupKind{Group: "StatefulSet"}},
		{input: "StatefulSet.apps", out: GroupKind{Group: "apps", Kind: "StatefulSet"}},
	}
	for i, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			out := ParseGroupKind(test.input)
			if out != test.out {
				t.Errorf("%d: expected output: %#v, got: %#v", i, test.out, out)
			}
		})
	}
}

// TestToAPIVersionAndKind 验证 GroupVersionKind 到 (apiVersion, kind) 的拆分，
// 包括空值这一边界。
func TestToAPIVersionAndKind(t *testing.T) {
	tests := []struct {
		desc         string
		input        GroupVersionKind
		GroupVersion string
		Kind         string
	}{
		{
			desc:         "gvk object is not empty",
			input:        GroupVersionKind{Version: "V1", Kind: "pod"},
			GroupVersion: "V1",
			Kind:         "pod",
		},
		{
			desc:         "gvk object is empty",
			input:        GroupVersionKind{},
			GroupVersion: "",
			Kind:         "",
		},
	}
	for i, test := range tests {
		version, kind := test.input.ToAPIVersionAndKind()
		if version != test.GroupVersion {
			t.Errorf("%d: expected version: %#v, got: %#v", i, test.GroupVersion, version)
		}
		if kind != test.Kind {
			t.Errorf("%d: expected kind: %#v, got: %#v", i, test.Kind, kind)
		}
	}
}

// TestBestMatch 验证 bestMatch 的选择策略：
// 优先选择 targets 中与 kinds 匹配的项；若没有匹配项，则回退到第一个 target。
func TestBestMatch(t *testing.T) {
	tests := []struct {
		desc    string
		kinds   []GroupVersionKind
		targets []GroupVersionKind
		output  GroupVersionKind
	}{
		{
			desc:    "targets and kinds have match items",
			kinds:   []GroupVersionKind{{Version: "V1", Kind: "pod"}, {Version: "V2", Kind: "pod"}},
			targets: []GroupVersionKind{{Version: "V1", Kind: "pod"}},
			output:  GroupVersionKind{Version: "V1", Kind: "pod"},
		},
		{
			desc:    "targets and kinds do not have match items",
			kinds:   []GroupVersionKind{{Version: "V1", Kind: "pod"}, {Version: "V2", Kind: "pod"}},
			targets: []GroupVersionKind{{Version: "V3", Kind: "pod"}, {Version: "V4", Kind: "pod"}},
			output:  GroupVersionKind{Version: "V3", Kind: "pod"},
		},
	}

	for i, test := range tests {
		out := bestMatch(test.kinds, test.targets)
		if out != test.output {
			t.Errorf("%d: expected out: %#v, got: %#v", i, test.output, out)
		}
	}
}
