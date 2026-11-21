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
	"fmt"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
)

// TestIsNativeResource 验证 IsNativeResource 对资源名所属命名空间的判定。
// 不带 "/" 的名称隐式属于 kubernetes.io；含 "/" 时仅 kubernetes.io/ 前缀算原生资源。
func TestIsNativeResource(t *testing.T) {
	testCases := []struct {
		resourceName v1.ResourceName
		expectVal    bool
	}{
		{
			resourceName: "pod.alpha.kubernetes.io/opaque-int-resource-foo",
			expectVal:    true,
		},
		{
			resourceName: "kubernetes.io/resource-foo",
			expectVal:    true,
		},
		{
			resourceName: "foo",
			expectVal:    true,
		},
		{
			// 自定义前缀不属于默认命名空间，判为非原生。
			resourceName: "a/b",
			expectVal:    false,
		},
		{
			// 空名没有 "/"，按隐式默认命名空间处理。
			resourceName: "",
			expectVal:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("resourceName input=%s, expected value=%v", tc.resourceName, tc.expectVal), func(t *testing.T) {
			t.Parallel()
			v := IsNativeResource(tc.resourceName)
			if v != tc.expectVal {
				t.Errorf("Got %v but expected %v", v, tc.expectVal)
			}
		})
	}
}

// TestHugePageSizeFromResourceName 验证从资源名解析大页大小的成功与失败路径。
// 只有 hugepages- 前缀且后缀为合法 Quantity 时才返回大小，否则应报错。
func TestHugePageSizeFromResourceName(t *testing.T) {
	// 预解析期望值，避免在每个子用例里重复解析并忽略错误。
	expected100m, _ := resource.ParseQuantity("100m")
	testCases := []struct {
		resourceName v1.ResourceName
		expectVal    resource.Quantity
		expectErr    bool
	}{
		{
			// 非大页前缀：在前缀检查处即失败。
			resourceName: "pod.alpha.kubernetes.io/opaque-int-resource-foo",
			expectVal:    resource.Quantity{},
			expectErr:    true,
		},
		{
			// 前缀正确但后缀为空，ParseQuantity 会失败。
			resourceName: "hugepages-",
			expectVal:    resource.Quantity{},
			expectErr:    true,
		},
		{
			// 合法大页资源名，期望解析出 100m。
			resourceName: "hugepages-100m",
			expectVal:    expected100m,
			expectErr:    false,
		},
		{
			resourceName: "",
			expectVal:    resource.Quantity{},
			expectErr:    true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("resourceName input=%s, expected value=%v", tc.resourceName, tc.expectVal), func(t *testing.T) {
			t.Parallel()
			v, err := HugePageSizeFromResourceName(tc.resourceName)
			if err == nil && tc.expectErr {
				t.Errorf("[%v]expected error but got none.", i)
			}
			if err != nil && !tc.expectErr {
				t.Errorf("[%v]did not expect error but got: %v", i, err)
			}
			if v != tc.expectVal {
				t.Errorf("Got %v but expected %v", v, tc.expectVal)
			}
		})
	}
}

// TestHugePageSizeFromMedium 验证从 StorageMedium 解析大页大小。
// 区分非大页介质、无大小的 HugePages、以及带合法大小的 HugePages-<size>。
func TestHugePageSizeFromMedium(t *testing.T) {
	testCases := []struct {
		description string
		medium      v1.StorageMedium
		expectVal   resource.Quantity
		expectErr   bool
	}{
		{
			description: "Invalid hugepages medium",
			// 普通内存介质不是大页介质。
			medium:    "Memory",
			expectVal: resource.Quantity{},
			expectErr: true,
		},
		{
			description: "Invalid hugepages medium",
			medium:      "Memory",
			expectVal:   resource.Quantity{},
			expectErr:   true,
		},
		{
			description: "Invalid: HugePages without size",
			// 大页介质但未携带大小后缀，应报错。
			medium:    "HugePages",
			expectVal: resource.Quantity{},
			expectErr: true,
		},
		{
			description: "Invalid: HugePages without size",
			medium:      "HugePages",
			expectVal:   resource.Quantity{},
			expectErr:   true,
		},
		{
			description: "Valid: HugePages-1Gi",
			// 以下三个用例验证合法后缀能被正确解析成 Quantity。
			medium:    "HugePages-1Gi",
			expectVal: resource.MustParse("1Gi"),
			expectErr: false,
		},
		{
			description: "Valid: HugePages-2Mi",
			medium:      "HugePages-2Mi",
			expectVal:   resource.MustParse("2Mi"),
			expectErr:   false,
		},
		{
			description: "Valid: HugePages-64Ki",
			medium:      "HugePages-64Ki",
			expectVal:   resource.MustParse("64Ki"),
			expectErr:   false,
		},
	}
	for i, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			v, err := HugePageSizeFromMedium(tc.medium)
			if err == nil && tc.expectErr {
				t.Errorf("[%v]expected error but got none.", i)
			}
			if err != nil && !tc.expectErr {
				t.Errorf("[%v]did not expect error but got: %v", i, err)
			}
			if v != tc.expectVal {
				t.Errorf("Got %v but expected %v", v, tc.expectVal)
			}
		})
	}
}

// TestIsOvercommitAllowed 验证资源是否允许超卖。
// 仅默认命名空间的非大页资源允许超卖，hugepages- 资源禁止。
func TestIsOvercommitAllowed(t *testing.T) {
	testCases := []struct {
		resourceName v1.ResourceName
		expectVal    bool
	}{
		{
			resourceName: "pod.alpha.kubernetes.io/opaque-int-resource-foo",
			expectVal:    true,
		},
		{
			resourceName: "kubernetes.io/resource-foo",
			expectVal:    true,
		},
		{
			// 大页资源即使属于默认命名空间也不允许超卖。
			resourceName: "hugepages-100m",
			expectVal:    false,
		},
		{
			resourceName: "",
			expectVal:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("resourceName input=%s, expected value=%v", tc.resourceName, tc.expectVal), func(t *testing.T) {
			t.Parallel()
			v := IsOvercommitAllowed(tc.resourceName)
			if v != tc.expectVal {
				t.Errorf("Got %v but expected %v", v, tc.expectVal)
			}
		})
	}
}

// TestGetAccessModesFromString 验证访问模式字符串的反序列化。
// 逐级覆盖单模式、多模式以及新增的 RWOP，确认解析不遗漏。
func TestGetAccessModesFromString(t *testing.T) {
	// 单个模式。
	modes := GetAccessModesFromString("ROX")
	if !ContainsAccessMode(modes, v1.ReadOnlyMany) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadOnlyMany, modes)
	}

	// 多模式以逗号分隔。
	modes = GetAccessModesFromString("ROX,RWX")
	if !ContainsAccessMode(modes, v1.ReadOnlyMany) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadOnlyMany, modes)
	}
	if !ContainsAccessMode(modes, v1.ReadWriteMany) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadWriteMany, modes)
	}

	// 三个标准模式同时出现。
	modes = GetAccessModesFromString("RWO,ROX,RWX")
	if !ContainsAccessMode(modes, v1.ReadWriteOnce) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadWriteOnce, modes)
	}
	if !ContainsAccessMode(modes, v1.ReadOnlyMany) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadOnlyMany, modes)
	}
	if !ContainsAccessMode(modes, v1.ReadWriteMany) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadWriteMany, modes)
	}

	// 包含 ReadWriteOncePod：新模式不能因未识别而被丢弃。
	modes = GetAccessModesFromString("RWO,ROX,RWX,RWOP")
	if !ContainsAccessMode(modes, v1.ReadWriteOnce) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadWriteOnce, modes)
	}
	if !ContainsAccessMode(modes, v1.ReadOnlyMany) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadOnlyMany, modes)
	}
	if !ContainsAccessMode(modes, v1.ReadWriteMany) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadWriteMany, modes)
	}
	if !ContainsAccessMode(modes, v1.ReadWriteOncePod) {
		t.Errorf("Expected mode %s, but got %+v", v1.ReadWriteOncePod, modes)
	}
}

// TestRemoveDuplicateAccessModes 验证访问模式去重。
// ROX 重复三次，期望去重后只剩两个不同模式。
func TestRemoveDuplicateAccessModes(t *testing.T) {
	// ROX 连续重复三次，用于验证去重而非排序。
	modes := []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce, v1.ReadOnlyMany, v1.ReadOnlyMany, v1.ReadOnlyMany,
	}
	modes = removeDuplicateAccessModes(modes)
	if len(modes) != 2 {
		t.Errorf("Expected 2 distinct modes in set but found %v", len(modes))
	}
}

// TestTopologySelectorRequirementsAsSelector 验证拓扑选择器需求到 labels.Selector 的转换。
// 空输入返回 labels.Nothing()；非法 key 或空 Values 应报错。
func TestTopologySelectorRequirementsAsSelector(t *testing.T) {
	// mustParse 解析硬编码的合法表达式；解析失败说明测试数据有误，直接 panic。
	mustParse := func(s string) labels.Selector {
		out, e := labels.Parse(s)
		if e != nil {
			panic(e)
		}
		return out
	}
	tc := []struct {
		in        []v1.TopologySelectorLabelRequirement
		out       labels.Selector
		expectErr bool
	}{
		// nil 与空切片都表示不匹配任何对象。
		{in: nil, out: labels.Nothing()},
		{in: []v1.TopologySelectorLabelRequirement{}, out: labels.Nothing()},
		{
			in: []v1.TopologySelectorLabelRequirement{{
				Key:    "foo",
				Values: []string{"bar", "baz"},
			}},
			out: mustParse("foo in (baz,bar)"),
		},
		{
			in: []v1.TopologySelectorLabelRequirement{{
				Key: "foo",
				// Values 为空时 NewRequirement 校验失败。
				Values: []string{},
			}},
			expectErr: true,
		},
		{
			in: []v1.TopologySelectorLabelRequirement{
				{
					Key:    "foo",
					Values: []string{"bar", "baz"},
				},
				{
					Key:    "invalid",
					Values: []string{},
				},
			},
			expectErr: true,
		},
		{
			in: []v1.TopologySelectorLabelRequirement{{
				// 以 "/" 开头的 key 不是合法限定名。
				Key:    "/invalidkey",
				Values: []string{"bar", "baz"},
			}},
			expectErr: true,
		},
	}

	for i, tc := range tc {
		out, err := TopologySelectorRequirementsAsSelector(tc.in)
		if err == nil && tc.expectErr {
			t.Errorf("[%v]expected error but got none.", i)
		}
		if err != nil && !tc.expectErr {
			t.Errorf("[%v]did not expect error but got: %v", i, err)
		}
		if !reflect.DeepEqual(out, tc.out) {
			t.Errorf("[%v]expected:\n\t%+v\nbut got:\n\t%+v", i, tc.out, out)
		}
	}
}

// TestMatchTopologySelectorTerms 验证标签是否匹配任一拓扑选择器项。
// 项之间为 OR、项内需求为 AND；空项列表匹配所有对象，空项不匹配任何对象。
func TestMatchTopologySelectorTerms(t *testing.T) {
	type args struct {
		topologySelectorTerms []v1.TopologySelectorTerm
		labels                labels.Set
	}

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			// 空项列表：不设限制，应匹配所有对象。
			name: "nil term list",
			args: args{
				topologySelectorTerms: nil,
				labels:                nil,
			},
			want: true,
		},
		{
			// 项存在但表达式为空：该项不匹配任何对象。
			name: "nil term",
			args: args{
				topologySelectorTerms: []v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{},
					},
				},
				labels: nil,
			},
			want: false,
		},
		{
			// 基本匹配：key 与 value 都相等。
			name: "label matches MatchLabelExpressions terms",
			args: args{
				topologySelectorTerms: []v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{{
							Key:    "label_1",
							Values: []string{"label_1_val"},
						}},
					},
				},
				labels: map[string]string{"label_1": "label_1_val"},
			},
			want: true,
		},
		{
			// value 不等：不应匹配。
			name: "label does not match MatchLabelExpressions terms",
			args: args{
				topologySelectorTerms: []v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{{
							Key:    "label_1",
							Values: []string{"label_1_val"},
						}},
					},
				},
				labels: map[string]string{"label_1": "label_1_val-failed"},
			},
			want: false,
		},
		{
			// 单需求多值：命中任意一个值即可。
			name: "multi-values in one requirement, one matched",
			args: args{
				topologySelectorTerms: []v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{{
							Key:    "label_1",
							Values: []string{"label_1_val1", "label_1_val2"},
						}},
					},
				},
				labels: map[string]string{"label_1": "label_1_val2"},
			},
			want: true,
		},
		{
			// 多 term：只要有一个 term 匹配即可（OR）。
			name: "multi-terms was set, one matched",
			args: args{
				topologySelectorTerms: []v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{{
							Key:    "label_1",
							Values: []string{"label_1_val"},
						}},
					},
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{{
							Key:    "label_2",
							Values: []string{"label_2_val"},
						}},
					},
				},
				labels: map[string]string{
					"label_2": "label_2_val",
				},
			},
			want: true,
		},
		{
			// 单 term 多需求：全部满足才匹配（AND）。
			name: "multi-requirement in one term, fully matched",
			args: args{
				topologySelectorTerms: []v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
							{
								Key:    "label_1",
								Values: []string{"label_1_val"},
							},
							{
								Key:    "label_2",
								Values: []string{"label_2_val"},
							},
						},
					},
				},
				labels: map[string]string{
					"label_1": "label_1_val",
					"label_2": "label_2_val",
				},
			},
			want: true,
		},
		{
			// 只满足部分需求：不应匹配，验证 AND 语义。
			name: "multi-requirement in one term, partial matched",
			args: args{
				topologySelectorTerms: []v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
							{
								Key:    "label_1",
								Values: []string{"label_1_val"},
							},
							{
								Key:    "label_2",
								Values: []string{"label_2_val"},
							},
						},
					},
				},
				labels: map[string]string{
					"label_1": "label_1_val-failed",
					"label_2": "label_2_val",
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MatchTopologySelectorTerms(tt.args.topologySelectorTerms, tt.args.labels); got != tt.want {
				t.Errorf("MatchTopologySelectorTermsORed() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestNodeSelectorRequirementKeysExistInNodeSelectorTerms 验证 key 存在性检查。
// 只比较 Key，不比较 Operator 或 Values；任一 req 的 Key 命中任一 term 即为 true。
func TestNodeSelectorRequirementKeyExistsInNodeSelectorTerms(t *testing.T) {
	tests := []struct {
		name   string
		reqs   []v1.NodeSelectorRequirement
		terms  []v1.NodeSelectorTerm
		exists bool
	}{
		{
			// 双方都为空：没有可命中的 key。
			name:   "empty set of keys in empty set of terms",
			reqs:   []v1.NodeSelectorRequirement{},
			terms:  []v1.NodeSelectorTerm{},
			exists: false,
		},
		{
			// reqs 的 key 分散在多个 term 中，仍应命中。
			name: "key existence in terms with all keys specified",
			reqs: []v1.NodeSelectorRequirement{
				{
					Key:      "key1",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value1"},
				},
				{
					Key:      "key2",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value2"},
				},
			},
			terms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "key2",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value2"},
						},
						{
							Key:      "key3",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value3"},
						},
					},
				},
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "key1",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value11, test-value12"},
						},
						{
							Key:      "key4",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value41, test-value42"},
						},
					},
				},
			},
			exists: true,
		},
		{
			// 仅部分 key 命中即返回 true；Values 中的逗号只是普通字符串，不参与比较。
			name: "key existence in terms with one of the keys specified",
			reqs: []v1.NodeSelectorRequirement{
				{
					Key:      "key1",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value1"},
				},
				{
					Key:      "key2",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value2"},
				},
				{
					Key:      "key3",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value3"},
				},
				{
					Key:      "key6",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value6"},
				},
			},
			terms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "key2",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value2"},
						}, {
							Key:      "key4",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value4"},
						},
					},
				},
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "key5",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value5"},
						},
					},
				},
			},
			exists: true,
		},
		{
			// 没有任何 key 重合，期望 false。
			name: "key existence in terms without any of the keys specified",
			reqs: []v1.NodeSelectorRequirement{
				{
					Key:      "key2",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value2"},
				},
				{
					Key:      "key3",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value3"},
				},
			},
			terms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "key4",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value"},
						},
						{
							Key:      "key5",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value"},
						},
					},
				},
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "key6",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value"},
						},
					},
				},
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "key7",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value"},
						},
						{
							Key:      "key8",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-value"},
						},
					},
				},
			},
			exists: false,
		},
		{
			// term 为空：无从匹配，期望 false。
			name: "key existence in empty set of terms",
			reqs: []v1.NodeSelectorRequirement{
				{
					Key:      "key2",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value2"},
				},
				{
					Key:      "key3",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"test-value3"},
				},
			},
			terms:  []v1.NodeSelectorTerm{},
			exists: false,
		},
	}
	for _, test := range tests {
		keyExists := NodeSelectorRequirementKeysExistInNodeSelectorTerms(test.reqs, test.terms)
		if test.exists != keyExists {
			t.Errorf("test %s failed. Expected %v but got %v", test.name, test.exists, keyExists)
		}
	}
}

// TestHugePageUnitSizeFromByteSize 验证字节大小到最大可用单位的转换。
// 仅当能整除 1024 且结果不超过 1024 时才升级单位，否则报错。
func TestHugePageUnitSizeFromByteSize(t *testing.T) {
	tests := []struct {
		size     int64
		expected string
		wantErr  bool
	}{
		{
			size:     1024,
			expected: "1KB",
			wantErr:  false,
		},
		{
			size:     33554432,
			expected: "32MB",
			wantErr:  false,
		},
		{
			size:     3221225472,
			expected: "3GB",
			wantErr:  false,
		},
		{
			// 该值整除到 MB 后为 3069，超过 1024 无法升级为 GB，期望报错。
			size:     1024 * 1024 * 1023 * 3,
			expected: "3069MB",
			wantErr:  true,
		},
	}
	for _, test := range tests {
		size := test.size
		result, err := HugePageUnitSizeFromByteSize(size)
		// wantErr 用例只校验是否返回错误，不校验错误文本内容。
		if err != nil {
			if test.wantErr {
				t.Logf("HugePageUnitSizeFromByteSize() expected error = %v", err)
			} else {
				t.Errorf("HugePageUnitSizeFromByteSize() error = %v, wantErr %v", err, test.wantErr)
			}
			continue
		}
		if test.expected != result {
			t.Errorf("HugePageUnitSizeFromByteSize() expected %v but got %v", test.expected, result)
		}
	}
}
