/*
Copyright 2024 The Kubernetes Authors.

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

package validate

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestFixedResult 验证 FixedResult 在给定 pass 预期下的行为：
// pass 为 true 时必须不产生任何错误，为 false 时必须恰好产生一条
// 带有指定来源与 detail 的 Invalid 错误。
//
// 用例覆盖字符串、数值、布尔、nil 指针、空/非空切片与空/非空 map 等常见取值，
// 同一取值会分别以 pass=false 和 pass=true 各跑一次，以确认结果只由 pass 决定，
// 而不被具体取值的内容干扰。
func TestFixedResult(t *testing.T) {
	cases := []struct {
		value any
		pass  bool
	}{{
		value: "",
		pass:  false,
	}, {
		value: "",
		pass:  true,
	}, {
		value: "nonempty",
		pass:  false,
	}, {
		value: "nonempty",
		pass:  true,
	}, {
		value: 0,
		pass:  false,
	}, {
		value: 0,
		pass:  true,
	}, {
		value: 1,
		pass:  false,
	}, {
		value: 1,
		pass:  true,
	}, {
		value: false,
		pass:  false,
	}, {
		value: false,
		pass:  true,
	}, {
		value: true,
		pass:  false,
	}, {
		value: true,
		pass:  true,
	}, {
		value: nil,
		pass:  false,
	}, {
		value: nil,
		pass:  true,
	}, {
		value: ptr.To(""),
		pass:  false,
	}, {
		value: ptr.To(""),
		pass:  true,
	}, {
		value: ptr.To("nonempty"),
		pass:  false,
	}, {
		value: ptr.To("nonempty"),
		pass:  true,
	}, {
		value: []string(nil),
		pass:  false,
	}, {
		value: []string(nil),
		pass:  true,
	}, {
		value: []string{},
		pass:  false,
	}, {
		value: []string{},
		pass:  true,
	}, {
		value: []string{"s"},
		pass:  false,
	}, {
		value: []string{"s"},
		pass:  true,
	}, {
		value: map[string]string(nil),
		pass:  false,
	}, {
		value: map[string]string(nil),
		pass:  true,
	}, {
		value: map[string]string{},
		pass:  false,
	}, {
		value: map[string]string{},
		pass:  true,
	}, {
		value: map[string]string{"k": "v"},
		pass:  false,
	}, {
		value: map[string]string{"k": "v"},
		pass:  true,
	}}

	// 只比对来源与 detail，字段名由调用方固定为 fldpath，因此无需纳入匹配条件。
	matcher := field.ErrorMatcher{}.ByOrigin().ByDetailExact()
	for i, tc := range cases {
		result := FixedResult(context.Background(), operation.Operation{}, field.NewPath("fldpath"), tc.value, nil, tc.pass, "detail string")
		// pass=true 却出现错误，说明 FixedResult 未能按预期放行。
		if len(result) != 0 && tc.pass {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		// pass=false 却没产生错误，说明强制失败未生效。
		if len(result) == 0 && !tc.pass {
			t.Errorf("case %d: unexpected success", i)
			continue
		}
		// pass=false 的场景要求恰好一条错误，多条说明结果合并逻辑有偏差。
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			wantErrorList := field.ErrorList{
				field.Invalid(field.NewPath("fldpath"), tc.value, "forced failure: detail string").WithOrigin("validateFalse"),
			}
			matcher.Test(t, wantErrorList, result)
		}
	}
}
