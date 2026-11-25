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

// Package v1 提供资源配额计算的基础工具：ResourceList 的相等、比较、加减、
// 去重与集合运算。这些函数被配额准入插件与控制器共同使用，因此语义必须
// 保持稳定。
package v1

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestEquals 验证 ResourceList 的语义相等：键集合与数量都必须一致，
// 空列表之间视为相等。
func TestEquals(t *testing.T) {
	testCases := map[string]struct {
		a        corev1.ResourceList
		b        corev1.ResourceList
		expected bool
	}{
		"isEqual": {
			a:        corev1.ResourceList{},
			b:        corev1.ResourceList{},
			expected: true,
		},
		"isEqualWithKeys": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			b: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			expected: true,
		},
		"isNotEqualSameKeys": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			b: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			expected: false,
		},
		"isNotEqualDiffKeys": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			b: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
				corev1.ResourcePods:   resource.MustParse("1"),
			},
			expected: false,
		},
	}
	for testName, testCase := range testCases {
		if result := Equals(testCase.a, testCase.b); result != testCase.expected {
			t.Errorf("%s expected: %v, actual: %v, a=%v, b=%v", testName, testCase.expected, result, testCase.a, testCase.b)
		}
	}
}

// TestLessThanOrEqual 验证 a <= b 的逐资源比较，并返回超出限制的资源名。
// 注意：a 中存在而 b 中缺失的资源不会被视为超出，调用方需另行处理。
func TestLessThanOrEqual(t *testing.T) {
	testCases := map[string]struct {
		a        corev1.ResourceList
		b        corev1.ResourceList
		expected bool
		out      []corev1.ResourceName
	}{
		"isEmpty": {
			a:        corev1.ResourceList{},
			b:        corev1.ResourceList{},
			expected: true,
			out:      []corev1.ResourceName{},
		},
		"isEqual": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			b:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			expected: true,
			out:      []corev1.ResourceName{},
		},
		"isLessThan": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			b:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")},
			expected: true,
			out:      []corev1.ResourceName{},
		},
		"isGreaterThan": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")},
			b:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			expected: false,
			out:      []corev1.ResourceName{corev1.ResourceCPU},
		},
	}
	for testName, testCase := range testCases {
		if result, out := LessThanOrEqual(testCase.a, testCase.b); result != testCase.expected && !reflect.DeepEqual(out, testCase.out) {
			t.Errorf("%s expected: %v/%v, actual: %v/%v", testName, testCase.expected, testCase.out, result, out)
		}
	}
}

// TestMax 验证逐资源取较大值，缺失的键按零值参与比较。
func TestMax(t *testing.T) {
	testCases := map[string]struct {
		a        corev1.ResourceList
		b        corev1.ResourceList
		expected corev1.ResourceList
	}{
		"noKeys": {
			a:        corev1.ResourceList{},
			b:        corev1.ResourceList{},
			expected: corev1.ResourceList{},
		},
		"toEmpty": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			b:        corev1.ResourceList{},
			expected: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
		"matching": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			b:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("150m")},
			expected: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("150m")},
		},
		"matching(reverse)": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("150m")},
			b:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			expected: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("150m")},
		},
		"matching-equal": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			b:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			expected: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
	}
	for testName, testCase := range testCases {
		sum := Max(testCase.a, testCase.b)
		if result := Equals(testCase.expected, sum); !result {
			t.Errorf("%s expected: %v, actual: %v", testName, testCase.expected, sum)
		}
	}
}

// TestAdd 验证逐资源求和，结果只包含至少在一侧出现的键。
func TestAdd(t *testing.T) {
	testCases := map[string]struct {
		a        corev1.ResourceList
		b        corev1.ResourceList
		expected corev1.ResourceList
	}{
		"noKeys": {
			a:        corev1.ResourceList{},
			b:        corev1.ResourceList{},
			expected: corev1.ResourceList{},
		},
		"toEmpty": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			b:        corev1.ResourceList{},
			expected: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
		"matching": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			b:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			expected: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")},
		},
	}
	for testName, testCase := range testCases {
		sum := Add(testCase.a, testCase.b)
		if result := Equals(testCase.expected, sum); !result {
			t.Errorf("%s expected: %v, actual: %v", testName, testCase.expected, sum)
		}
	}
}

// TestSubtract 验证逐资源求差；结果允许为负数（空值与非空值相减即如此）。
func TestSubtract(t *testing.T) {
	testCases := map[string]struct {
		a        corev1.ResourceList
		b        corev1.ResourceList
		expected corev1.ResourceList
	}{
		"noKeys": {
			a:        corev1.ResourceList{},
			b:        corev1.ResourceList{},
			expected: corev1.ResourceList{},
		},
		"value-empty": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			b:        corev1.ResourceList{},
			expected: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
		"empty-value": {
			a:        corev1.ResourceList{},
			b:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			expected: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-100m")},
		},
		"value-value": {
			a:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")},
			b:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			expected: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
	}
	for testName, testCase := range testCases {
		sub := Subtract(testCase.a, testCase.b)
		if result := Equals(testCase.expected, sub); !result {
			t.Errorf("%s expected: %v, actual: %v", testName, testCase.expected, sub)
		}
	}
}

// TestResourceNames 验证从 ResourceList 提取键名；顺序不保证，因此按集合比较。
func TestResourceNames(t *testing.T) {
	testCases := map[string]struct {
		a        corev1.ResourceList
		expected []corev1.ResourceName
	}{
		"empty": {
			a:        corev1.ResourceList{},
			expected: []corev1.ResourceName{},
		},
		"values": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			expected: []corev1.ResourceName{corev1.ResourceMemory, corev1.ResourceCPU},
		},
	}
	for testName, testCase := range testCases {
		actualSet := ToSet(ResourceNames(testCase.a))
		expectedSet := ToSet(testCase.expected)
		if !actualSet.Equal(expectedSet) {
			t.Errorf("%s expected: %v, actual: %v", testName, expectedSet, actualSet)
		}
	}
}

// TestContains 验证资源名列表的成员判断。
func TestContains(t *testing.T) {
	testCases := map[string]struct {
		a        []corev1.ResourceName
		b        corev1.ResourceName
		expected bool
	}{
		"does-not-contain": {
			a:        []corev1.ResourceName{corev1.ResourceMemory},
			b:        corev1.ResourceCPU,
			expected: false,
		},
		"does-contain": {
			a:        []corev1.ResourceName{corev1.ResourceMemory, corev1.ResourceCPU},
			b:        corev1.ResourceCPU,
			expected: true,
		},
	}
	for testName, testCase := range testCases {
		if actual := Contains(testCase.a, testCase.b); actual != testCase.expected {
			t.Errorf("%s expected: %v, actual: %v", testName, testCase.expected, actual)
		}
	}
}

// TestContainsPrefix 验证按前缀匹配资源名（如 hugepages- 前缀）。
func TestContainsPrefix(t *testing.T) {
	testCases := map[string]struct {
		a        []string
		b        corev1.ResourceName
		expected bool
	}{
		"does-not-contain": {
			a:        []string{corev1.ResourceHugePagesPrefix},
			b:        corev1.ResourceCPU,
			expected: false,
		},
		"does-contain": {
			a:        []string{corev1.ResourceHugePagesPrefix},
			b:        corev1.ResourceName(corev1.ResourceHugePagesPrefix + "2Mi"),
			expected: true,
		},
	}
	for testName, testCase := range testCases {
		if actual := ContainsPrefix(testCase.a, testCase.b); actual != testCase.expected {
			t.Errorf("%s expected: %v, actual: %v", testName, testCase.expected, actual)
		}
	}
}

// TestIsZero 验证零值判断：空列表与所有数量均为零的列表都算零。
func TestIsZero(t *testing.T) {
	testCases := map[string]struct {
		a        corev1.ResourceList
		expected bool
	}{
		"empty": {
			a:        corev1.ResourceList{},
			expected: true,
		},
		"zero": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("0"),
				corev1.ResourceMemory: resource.MustParse("0"),
			},
			expected: true,
		},
		"non-zero": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			expected: false,
		},
	}
	for testName, testCase := range testCases {
		if result := IsZero(testCase.a); result != testCase.expected {
			t.Errorf("%s expected: %v, actual: %v", testName, testCase.expected, result)
		}
	}
}

// TestRemoveZeros 验证剔除数量为零的条目，保留非零项。
func TestRemoveZeros(t *testing.T) {
	testCases := map[string]struct {
		a        corev1.ResourceList
		expected corev1.ResourceList
	}{
		"empty": {
			a:        corev1.ResourceList{},
			expected: corev1.ResourceList{},
		},
		"all-zeros": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("0"),
				corev1.ResourceMemory: resource.MustParse("0"),
			},
			expected: corev1.ResourceList{},
		},
		"some-zeros": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:     resource.MustParse("0"),
				corev1.ResourceMemory:  resource.MustParse("0"),
				corev1.ResourceStorage: resource.MustParse("100Gi"),
			},
			expected: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("100Gi"),
			},
		},
		"non-zero": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			expected: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
	}
	for testName, testCase := range testCases {
		if result := RemoveZeros(testCase.a); !Equals(result, testCase.expected) {
			t.Errorf("%s expected: %v, actual: %v", testName, testCase.expected, result)
		}
	}
}

// TestIsNegative 验证找出数量为负的资源名；顺序不保证，按集合比较。
func TestIsNegative(t *testing.T) {
	testCases := map[string]struct {
		a        corev1.ResourceList
		expected []corev1.ResourceName
	}{
		"empty": {
			a:        corev1.ResourceList{},
			expected: []corev1.ResourceName{},
		},
		"some-negative": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("-10"),
				corev1.ResourceMemory: resource.MustParse("0"),
			},
			expected: []corev1.ResourceName{corev1.ResourceCPU},
		},
		"all-negative": {
			a: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("-200m"),
				corev1.ResourceMemory: resource.MustParse("-1Gi"),
			},
			expected: []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
		},
	}
	for testName, testCase := range testCases {
		actual := IsNegative(testCase.a)
		actualSet := ToSet(actual)
		expectedSet := ToSet(testCase.expected)
		if !actualSet.Equal(expectedSet) {
			t.Errorf("%s expected: %v, actual: %v", testName, expectedSet, actualSet)
		}
	}
}

// TestIntersection 验证交集：结果去重并排序。
func TestIntersection(t *testing.T) {
	testCases := map[string]struct {
		a        []corev1.ResourceName
		b        []corev1.ResourceName
		expected []corev1.ResourceName
	}{
		"empty": {
			a:        []corev1.ResourceName{},
			b:        []corev1.ResourceName{},
			expected: []corev1.ResourceName{},
		},
		"equal": {
			a:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
			b:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
			expected: []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
		},
		"a has extra": {
			a:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
			b:        []corev1.ResourceName{corev1.ResourceCPU},
			expected: []corev1.ResourceName{corev1.ResourceCPU},
		},
		"b has extra": {
			a:        []corev1.ResourceName{corev1.ResourceCPU},
			b:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
			expected: []corev1.ResourceName{corev1.ResourceCPU},
		},
		"dedupes": {
			a:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceMemory},
			b:        []corev1.ResourceName{corev1.ResourceCPU},
			expected: []corev1.ResourceName{corev1.ResourceCPU},
		},
		"sorts": {
			a:        []corev1.ResourceName{corev1.ResourceMemory, corev1.ResourceMemory, corev1.ResourceCPU, corev1.ResourceCPU},
			b:        []corev1.ResourceName{corev1.ResourceMemory, corev1.ResourceMemory, corev1.ResourceCPU, corev1.ResourceCPU},
			expected: []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
		},
	}
	for testName, testCase := range testCases {
		actual := Intersection(testCase.a, testCase.b)
		if !reflect.DeepEqual(actual, testCase.expected) {
			t.Errorf("%s expected: %#v, actual: %#v", testName, testCase.expected, actual)
		}
	}
}

// TestDifference 验证差集（a 有而 b 没有）：结果去重并排序。
func TestDifference(t *testing.T) {
	testCases := map[string]struct {
		a        []corev1.ResourceName
		b        []corev1.ResourceName
		expected []corev1.ResourceName
	}{
		"empty": {
			a:        []corev1.ResourceName{},
			b:        []corev1.ResourceName{},
			expected: []corev1.ResourceName{},
		},
		"equal": {
			a:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
			b:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
			expected: []corev1.ResourceName{},
		},
		"a has extra": {
			a:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
			b:        []corev1.ResourceName{corev1.ResourceCPU},
			expected: []corev1.ResourceName{corev1.ResourceMemory},
		},
		"b has extra": {
			a:        []corev1.ResourceName{corev1.ResourceCPU},
			b:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
			expected: []corev1.ResourceName{},
		},
		"dedupes": {
			a:        []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceMemory},
			b:        []corev1.ResourceName{corev1.ResourceCPU},
			expected: []corev1.ResourceName{corev1.ResourceMemory},
		},
		"sorts": {
			a:        []corev1.ResourceName{corev1.ResourceMemory, corev1.ResourceMemory, corev1.ResourceCPU, corev1.ResourceCPU},
			b:        []corev1.ResourceName{},
			expected: []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory},
		},
	}
	for testName, testCase := range testCases {
		actual := Difference(testCase.a, testCase.b)
		if !reflect.DeepEqual(actual, testCase.expected) {
			t.Errorf("%s expected: %#v, actual: %#v", testName, testCase.expected, actual)
		}
	}
}
