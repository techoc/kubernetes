/*
Copyright The Kubernetes Authors.

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

package nodedeclaredfeatures

import (
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
)

// TestFeatureMapper 同时覆盖三个映射入口，因为它们的契约不同：
// MapSorted/MustMapSorted 遇到未知 feature 必须拒绝，TryMap 要跳过，
// Unmap 则要把 bit 还原成名字。用例里的 unknown 分别放在开头、中间、结尾，
// 就是为了保证双指针扫描在所有位置都不会提前漏检或误报。
func TestFeatureMapper(t *testing.T) {
	features := []string{"a", "b", "c", "d"}
	mapper := NewFeatureMapper(features)

	tests := []struct {
		name        string
		input       []string
		expectError bool
		expectedSet sets.Set[int] // 期望被置位的特征下标（对应 features 的下标）
	}{
		{
			name:        "Empty input",
			input:       nil,
			expectedSet: nil,
		},
		{
			name:        "Single valid feature",
			input:       []string{"a"},
			expectedSet: sets.New(0),
		},
		{
			name:        "Multiple valid features",
			input:       []string{"a", "c"},
			expectedSet: sets.New(0, 2),
		},
		{
			name:        "All features",
			input:       []string{"a", "b", "c", "d"},
			expectedSet: sets.New(0, 1, 2, 3),
		},
		{
			name:        "Unknown feature (start)",
			input:       []string{"0", "a"},
			expectError: true,
			expectedSet: sets.New(0),
		},
		{
			name:        "Unknown feature (middle)",
			input:       []string{"a", "ab", "b"},
			expectError: true,
			expectedSet: sets.New(0, 1),
		},
		{
			name:        "Unknown feature (end)",
			input:       []string{"d", "e"},
			expectError: true,
			expectedSet: sets.New(3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 宽松映射：未知特征被忽略，逐位核对结果是否与期望集合一致。
			fs := mapper.TryMap(tt.input)
			for i, known := range features {
				expected := tt.expectedSet.Has(i)
				got := fs.Get(i)
				if got != expected {
					t.Errorf("Feature %s (index %d) state mismatch: got %v, want %v", known, i, got, expected)
				}
			}

			// 严格映射：按 expectError 决定是断言报错还是与宽松映射结果一致。
			fs2, err := mapper.MapSorted(tt.input)
			if tt.expectError {
				if err == nil {
					t.Error("expected error from MapSorted, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error from MapSorted: %v", err)
				}
				if !fs.Equal(fs2) {
					t.Errorf("fs != fs2: got %v, want %v", fs2, fs)
				}
			}

			// 便捷封装：错误路径断言 panic，正常路径断言结果与宽松映射一致。
			if tt.expectError {
				func() {
					defer func() {
						if r := recover(); r == nil {
							t.Error("expected panic from MustMapSorted, but did not panic")
						}
					}()
					mapper.MustMapSorted(tt.input)
				}()
			} else {
				got := mapper.MustMapSorted(tt.input)
				if !fs.Equal(got) {
					t.Errorf("MustMapSorted result mismatch: got %v, want %v", got, fs)
				}
			}

			// 逆向映射：校验名字数量与内容；报错路径只要求结果来自输入。
			unmapped, err := mapper.Unmap(fs)
			if err != nil {
				t.Fatalf("unexpected Unmap error: %v", err)
			}
			if len(unmapped) != tt.expectedSet.Len() {
				t.Errorf("len(unmapped) = %d, want %d", len(unmapped), tt.expectedSet.Len())
			}
			if tt.expectError {
				// TryMap 在报错路径上会返回“成功映射的那部分”，这里只要求这部分
				// 来自输入，不强求完整，因为未知项的具体跳过策略不在本测试的契约里。
				inputSets := sets.New(tt.input...)
				for _, u := range unmapped {
					if !inputSets.Has(u) {
						t.Errorf("unmapped result %q not found in input %v", u, tt.input)
					}
				}
			} else {
				if !slices.Equal(tt.input, unmapped) {
					t.Errorf("Unmap result mismatch: got %v, want %v", unmapped, tt.input)
				}
			}
		})
	}
}
