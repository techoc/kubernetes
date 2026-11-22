/*
Copyright 2021 The Kubernetes Authors.

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

package flag

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

// TestStringSlice 覆盖 StringSlice 标志的累加语义：
// 默认值在首次显式传参时被清空，同一标志多次出现则按出现顺序追加。
func TestStringSlice(t *testing.T) {
	tests := []struct {
		args       []string
		def        []string
		expected   []string
		parseError string
		changed    bool
	}{
		{
			// 未传参时保持 nil 默认值。
			args:     []string{},
			expected: nil,
		},
		{
			// 单次传参产生单元素切片，并标记 changed。
			args:     []string{"a"},
			expected: []string{"a"},
			changed:  true,
		},
		{
			// 多次传参按顺序累加。
			args:     []string{"a", "b"},
			expected: []string{"a", "b"},
			changed:  true,
		},
		{
			// 有默认值时，显式传参应替换而非叠加默认值。
			def:      []string{"a"},
			args:     []string{"a", "b"},
			expected: []string{"a", "b"},
			changed:  true,
		},
		{
			// 默认值与显式值相同的边界情况。
			def:      []string{"a", "b"},
			args:     []string{"a", "b"},
			expected: []string{"a", "b"},
			changed:  true,
		},
	}
	for i, test := range tests {
		fs := pflag.NewFlagSet("testStringSlice", pflag.ContinueOnError)
		var s []string
		s = append(s, test.def...)

		v := NewStringSlice(&s)
		fs.Var(v, "slice", "usage")

		// 每个参数单独展开成一次 --slice=...，模拟用户重复传标志。
		args := []string{}
		for _, a := range test.args {
			args = append(args, fmt.Sprintf("--slice=%s", a))
		}

		err := fs.Parse(args)
		if test.parseError != "" {
			// 期望错误：必须非 nil 且包含预期片段。
			if err == nil {
				t.Errorf("%d: expected error %q, got nil", i, test.parseError)
			} else if !strings.Contains(err.Error(), test.parseError) {
				t.Errorf("%d: expected error %q, got %q", i, test.parseError, err)
			}
		} else if err != nil {
			t.Errorf("%d: expected nil error, got %v", i, err)
		}
		if !reflect.DeepEqual(s, test.expected) {
			t.Errorf("%d: expected %+v, got %+v", i, test.expected, s)
		}
		// changed 用于区分“用户显式设置”与“默认值”，配置合并逻辑依赖它。
		if v.changed != test.changed {
			t.Errorf("%d: expected %t got %t", i, test.changed, v.changed)
		}
	}
}
