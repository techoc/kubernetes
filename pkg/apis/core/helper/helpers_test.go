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

package helper

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/apis/core"
)

// TestSemantic 验证 Semantic 深度比较会忽略 Quantity 的格式差异，但比较数值大小。
func TestSemantic(t *testing.T) {
	table := []struct {
		a, b        interface{}
		shouldEqual bool
	}{
		{resource.MustParse("0"), resource.Quantity{}, true},
		{resource.Quantity{}, resource.MustParse("0"), true},
		{resource.Quantity{}, resource.MustParse("1m"), false},
		{
			resource.NewQuantity(5, resource.BinarySI),
			resource.NewQuantity(5, resource.DecimalSI),
			true,
		},
		{resource.MustParse("2m"), resource.MustParse("1m"), false},
	}

	for index, item := range table {
		if e, a := item.shouldEqual, Semantic.DeepEqual(item.a, item.b); e != a {
			t.Errorf("case[%d], expected %v, got %v.", index, e, a)
		}
	}
}

// TestIsStandardResource 验证标准资源名（含大页资源）的判定结果。
func TestIsStandardResource(t *testing.T) {
	testCases := []struct {
		input  string
		output bool
	}{
		{"cpu", true},
		{"memory", true},
		{"disk", false},
		{"blah", false},
		{"x.y.z", false},
		{"hugepages-2Mi", true},
		{"requests.hugepages-2Mi", true},
	}
	for i, tc := range testCases {
		if IsStandardResourceName(core.ResourceName(tc.input)) != tc.output {
			t.Errorf("case[%d], input: %s, expected: %t, got: %t", i, tc.input, tc.output, !tc.output)
		}
	}
}

// TestIsStandardContainerResource 验证容器可申请的标准资源名判定。
func TestIsStandardContainerResource(t *testing.T) {
	testCases := []struct {
		input  string
		output bool
	}{
		{"cpu", true},
		{"memory", true},
		{"disk", false},
		{"hugepages-2Mi", true},
	}
	for i, tc := range testCases {
		if IsStandardContainerResourceName(core.ResourceName(tc.input)) != tc.output {
			t.Errorf("case[%d], input: %s, expected: %t, got: %t", i, tc.input, tc.output, !tc.output)
		}
	}
}

// TestIsNodeAllocatableResource 验证 DRA 可分配资源名的判定（仅 cpu/内存/大页）。
func TestIsNodeAllocatableResource(t *testing.T) {
	testCases := []struct {
		input  string
		output bool
	}{
		{"cpu", true},
		{"memory", true},
		{"ephemeral-storage", false},
		{"hugepages-2Mi", true},
		{"disk", false},
		{"blah", false},
	}
	for i, tc := range testCases {
		if IsNodeAllocatableResourceName(core.ResourceName(tc.input)) != tc.output {
			t.Errorf("case[%d], input: %s, expected: %t, got: %t", i, tc.input, tc.output, !tc.output)
		}
	}
}

// TestGetAccessModesFromString 验证访问模式字符串能正确解析为对应的模式集合。
func TestGetAccessModesFromString(t *testing.T) {
	modes := GetAccessModesFromString("ROX")
	if !ContainsAccessMode(modes, core.ReadOnlyMany) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadOnlyMany, modes)
	}

	modes = GetAccessModesFromString("ROX,RWX")
	if !ContainsAccessMode(modes, core.ReadOnlyMany) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadOnlyMany, modes)
	}
	if !ContainsAccessMode(modes, core.ReadWriteMany) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadWriteMany, modes)
	}

	modes = GetAccessModesFromString("RWO,ROX,RWX")
	if !ContainsAccessMode(modes, core.ReadWriteOnce) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadWriteOnce, modes)
	}
	if !ContainsAccessMode(modes, core.ReadOnlyMany) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadOnlyMany, modes)
	}
	if !ContainsAccessMode(modes, core.ReadWriteMany) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadWriteMany, modes)
	}

	modes = GetAccessModesFromString("RWO,ROX,RWX,RWOP")
	if !ContainsAccessMode(modes, core.ReadWriteOnce) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadWriteOnce, modes)
	}
	if !ContainsAccessMode(modes, core.ReadOnlyMany) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadOnlyMany, modes)
	}
	if !ContainsAccessMode(modes, core.ReadWriteMany) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadWriteMany, modes)
	}
	if !ContainsAccessMode(modes, core.ReadWriteOncePod) {
		t.Errorf("Expected mode %s, but got %+v", core.ReadWriteOncePod, modes)
	}
}

// TestRemoveDuplicateAccessModes 验证重复访问模式会被去重。
func TestRemoveDuplicateAccessModes(t *testing.T) {
	modes := []core.PersistentVolumeAccessMode{
		core.ReadWriteOnce, core.ReadOnlyMany, core.ReadOnlyMany, core.ReadOnlyMany,
	}
	modes = removeDuplicateAccessModes(modes)
	if len(modes) != 2 {
		t.Errorf("Expected 2 distinct modes in set but found %v", len(modes))
	}
}

// TestIsHugePageResourceName 验证大页资源名前缀的识别。
func TestIsHugePageResourceName(t *testing.T) {
	testCases := []struct {
		name   core.ResourceName
		result bool
	}{
		{
			name:   core.ResourceName("hugepages-2Mi"),
			result: true,
		},
		{
			name:   core.ResourceName("hugepages-1Gi"),
			result: true,
		},
		{
			name:   core.ResourceName("cpu"),
			result: false,
		},
		{
			name:   core.ResourceName("memory"),
			result: false,
		},
	}
	for _, testCase := range testCases {
		if testCase.result != IsHugePageResourceName(testCase.name) {
			t.Errorf("resource: %v expected result: %v", testCase.name, testCase.result)
		}
	}
}

// TestIsHugePageResourceValueDivisible 验证存储值能否被页大小整除的判定，含非法资源名场景。
func TestIsHugePageResourceValueDivisible(t *testing.T) {
	testCases := []struct {
		name     core.ResourceName
		quantity resource.Quantity
		result   bool
	}{
		{
			name:     core.ResourceName("hugepages-2Mi"),
			quantity: resource.MustParse("4Mi"),
			result:   true,
		},
		{
			name:     core.ResourceName("hugepages-2Mi"),
			quantity: resource.MustParse("5Mi"),
			result:   false,
		},
		{
			name:     core.ResourceName("hugepages-1Gi"),
			quantity: resource.MustParse("2Gi"),
			result:   true,
		},
		{
			name:     core.ResourceName("hugepages-1Gi"),
			quantity: resource.MustParse("2.1Gi"),
			result:   false,
		},
		{
			name:     core.ResourceName("hugepages-1Mi"),
			quantity: resource.MustParse("2.1Mi"),
			result:   false,
		},
		{
			name:     core.ResourceName("hugepages-64Ki"),
			quantity: resource.MustParse("128Ki"),
			result:   true,
		},
		{
			name:     core.ResourceName("hugepages-"),
			quantity: resource.MustParse("128Ki"),
			result:   false,
		},
		{
			name:     core.ResourceName("hugepages"),
			quantity: resource.MustParse("128Ki"),
			result:   false,
		},
	}
	for _, testCase := range testCases {
		if testCase.result != IsHugePageResourceValueDivisible(testCase.name, testCase.quantity) {
			t.Errorf("resource: %v storage:%v expected result: %v", testCase.name, testCase.quantity, testCase.result)
		}
	}
}

// TestHugePageResourceName 验证由页大小生成规范大页资源名，并检查规范表示不会退化。
func TestHugePageResourceName(t *testing.T) {
	testCases := []struct {
		pageSize resource.Quantity
		name     core.ResourceName
	}{
		{
			pageSize: resource.MustParse("2Mi"),
			name:     core.ResourceName("hugepages-2Mi"),
		},
		{
			pageSize: resource.MustParse("1Gi"),
			name:     core.ResourceName("hugepages-1Gi"),
		},
		{
			// 验证规范表示不会退化：字节数形式的页大小应生成同样的资源名。
			pageSize: *resource.NewQuantity(int64(2097152), resource.BinarySI),
			name:     core.ResourceName("hugepages-2Mi"),
		},
	}
	for _, testCase := range testCases {
		if result := HugePageResourceName(testCase.pageSize); result != testCase.name {
			t.Errorf("pageSize: %v, expected: %v, but got: %v", testCase.pageSize.String(), testCase.name, result.String())
		}
	}
}

// TestHugePageSizeFromResourceName 验证从大页资源名解析页大小，非法名称应返回错误。
func TestHugePageSizeFromResourceName(t *testing.T) {
	testCases := []struct {
		name      core.ResourceName
		expectErr bool
		pageSize  resource.Quantity
	}{
		{
			name:      core.ResourceName("hugepages-2Mi"),
			pageSize:  resource.MustParse("2Mi"),
			expectErr: false,
		},
		{
			name:      core.ResourceName("hugepages-1Gi"),
			pageSize:  resource.MustParse("1Gi"),
			expectErr: false,
		},
		{
			name:      core.ResourceName("hugepages-bad"),
			expectErr: true,
		},
	}
	for _, testCase := range testCases {
		value, err := HugePageSizeFromResourceName(testCase.name)
		if testCase.expectErr && err == nil {
			t.Errorf("Expected an error for %v", testCase.name)
		} else if !testCase.expectErr && err != nil {
			t.Errorf("Unexpected error for %v, got %v", testCase.name, err)
		} else if testCase.pageSize.Value() != value.Value() {
			t.Errorf("Unexpected pageSize for resource %v got %v", testCase.name, value.String())
		}
	}
}

// TestIsOvercommitAllowed 验证允许超卖的资源范围：原生资源允许，大页资源不允许。
func TestIsOvercommitAllowed(t *testing.T) {
	testCases := []struct {
		name    core.ResourceName
		allowed bool
	}{
		{
			name:    core.ResourceCPU,
			allowed: true,
		},
		{
			name:    core.ResourceMemory,
			allowed: true,
		},
		{
			name:    HugePageResourceName(resource.MustParse("2Mi")),
			allowed: false,
		},
	}
	for _, testCase := range testCases {
		if testCase.allowed != IsOvercommitAllowed(testCase.name) {
			t.Errorf("Unexpected result for %v", testCase.name)
		}
	}
}

// TestIsServiceIPSet 验证 Service ClusterIP 设置判定，覆盖 nil、headless 与各地址族场景。
func TestIsServiceIPSet(t *testing.T) {
	testCases := []struct {
		input  core.ServiceSpec
		output bool
		name   string
	}{
		{
			name: "nil cluster ip",
			input: core.ServiceSpec{
				ClusterIPs: nil,
			},

			output: false,
		},
		{
			name: "headless service",
			input: core.ServiceSpec{
				ClusterIP:  "None",
				ClusterIPs: []string{"None"},
			},
			output: false,
		},
		// true cases
		{
			name: "one ipv4",
			input: core.ServiceSpec{
				ClusterIP:  "1.2.3.4",
				ClusterIPs: []string{"1.2.3.4"},
			},
			output: true,
		},
		{
			name: "one ipv6",
			input: core.ServiceSpec{
				ClusterIP:  "2001::1",
				ClusterIPs: []string{"2001::1"},
			},
			output: true,
		},
		{
			name: "v4, v6",
			input: core.ServiceSpec{
				ClusterIP:  "1.2.3.4",
				ClusterIPs: []string{"1.2.3.4", "2001::1"},
			},
			output: true,
		},
		{
			name: "v6, v4",
			input: core.ServiceSpec{
				ClusterIP:  "2001::1",
				ClusterIPs: []string{"2001::1", "1.2.3.4"},
			},

			output: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := core.Service{
				Spec: tc.input,
			}
			if IsServiceIPSet(&s) != tc.output {
				t.Errorf("case, input: %v, expected: %v, got: %v", tc.input, tc.output, !tc.output)
			}
		})
	}
}

// TestHasInvalidLabelValueInNodeSelectorTerms 验证 NodeSelectorTerm 中非法 label 值的检测。
func TestHasInvalidLabelValueInNodeSelectorTerms(t *testing.T) {
	testCases := []struct {
		name   string
		terms  []core.NodeSelectorTerm
		expect bool
	}{
		{
			name: "valid values",
			terms: []core.NodeSelectorTerm{{
				MatchExpressions: []core.NodeSelectorRequirement{{
					Key:      "foo",
					Operator: core.NodeSelectorOpIn,
					Values:   []string{"far"},
				}},
			}},
			expect: false,
		},
		{
			name:   "empty terms",
			terms:  []core.NodeSelectorTerm{},
			expect: false,
		},
		{
			name: "invalid label value",
			terms: []core.NodeSelectorTerm{{
				MatchExpressions: []core.NodeSelectorRequirement{{
					Key:      "foo",
					Operator: core.NodeSelectorOpIn,
					Values:   []string{"-1"},
				}},
			}},
			expect: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := HasInvalidLabelValueInNodeSelectorTerms(tc.terms)
			if got != tc.expect {
				t.Errorf("exepct %v, got %v", tc.expect, got)
			}
		})
	}
}
