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

package validate

import (
	"context"
	"regexp"
	"testing"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestDependentRequired 验证 DependentRequired 的核心语义：
// 当 trigger 被设置时 dependent 必须同时被设置；并且该规则是"棘轮"式的——
// 更新时只看设置状态是否变化，不因无关字段改动或 trigger 取值变化而重新报错。
func TestDependentRequired(t *testing.T) {
	type obj struct {
		Trigger    *string
		Dependent  *string
		OtherField *string
	}

	triggerIsSet := func(o *obj) bool { return o != nil && o.Trigger != nil }
	dependentIsSet := func(o *obj) bool { return o != nil && o.Dependent != nil }

	cases := []struct {
		name   string
		op     operation.Operation
		obj    *obj
		oldObj *obj
		err    string // 正则表达式；为空表示预期无错误。
	}{{
		// trigger 未设置时不施加任何约束，dependent 可自由设置。
		name: "create: trigger unset, dependent set",
		op:   operation.Operation{Type: operation.Create},
		obj:  &obj{Dependent: new("d")},
	}, {
		// 正常满足约束的组合。
		name: "create: trigger set, dependent set",
		op:   operation.Operation{Type: operation.Create},
		obj:  &obj{Trigger: new("t"), Dependent: new("d")},
	}, {
		// 创建时违反约束：trigger 已设置但 dependent 缺失。
		name: "create: trigger set, dependent unset",
		op:   operation.Operation{Type: operation.Create},
		obj:  &obj{Trigger: new("t")},
		err:  `fldpath\.dependent: Required value: must be set when trigger is set`,
	}, {
		// nil 对象不触发任何校验。
		name: "create: nil obj",
		op:   operation.Operation{Type: operation.Create},
		obj:  nil,
	}, {
		// 棘轮：仅无关字段变化，设置状态未变，不应报错。
		name:   "ratchet: unrelated field changed, trigger and dependent set-ness unchanged",
		op:     operation.Operation{Type: operation.Update},
		obj:    &obj{Trigger: new("t"), OtherField: new("new")},
		oldObj: &obj{Trigger: new("t"), OtherField: new("old")},
	}, {
		// 棘轮：trigger 的取值变化不算设置状态变化，不应报错。
		name:   "ratchet: trigger value changed, set-ness unchanged",
		op:     operation.Operation{Type: operation.Update},
		obj:    &obj{Trigger: new("t")},
		oldObj: &obj{Trigger: new("old")},
	}, {
		// 更新时新设置 trigger 却没有 dependent，属于新的违规。
		name:   "update: trigger newly set",
		op:     operation.Operation{Type: operation.Update},
		obj:    &obj{Trigger: new("t")},
		oldObj: &obj{},
		err:    `fldpath\.dependent: Required value: must be set when trigger is set`,
	}, {
		// 更新时清空原本存在的 dependent，同样构成违规。
		name:   "update: dependent newly cleared",
		op:     operation.Operation{Type: operation.Update},
		obj:    &obj{Trigger: new("t")},
		oldObj: &obj{Trigger: new("t"), Dependent: new("d")},
		err:    `fldpath\.dependent: Required value: must be set when trigger is set`,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := DependentRequired(context.Background(), tc.op,
				field.NewPath("fldpath"), tc.obj, tc.oldObj,
				"trigger", triggerIsSet,
				"dependent", dependentIsSet)
			if len(result) > 0 && tc.err == "" {
				t.Fatalf("unexpected failure: %v", fmtErrs(result))
			}
			if len(result) == 0 && tc.err != "" {
				t.Fatalf("unexpected success: expected %q", tc.err)
			}
			if len(result) > 1 {
				t.Fatalf("unexpected multi-error: %v", fmtErrs(result))
			}
			// 只应产生一条错误，且 origin 必须标记为 dependentRequired，便于声明式校验对齐。
			if len(result) > 0 {
				if !regexp.MustCompile(tc.err).MatchString(result[0].Error()) {
					t.Errorf("wrong error\nexpected: %q\n     got: %v", tc.err, fmtErrs(result))
				}
				if result[0].Origin != "dependentRequired" {
					t.Errorf("expected origin %q, got %q", "dependentRequired", result[0].Origin)
				}
			}
		})
	}
}

// TestDependentForbidden 验证 DependentForbidden 的核心语义：
// 当 trigger 被设置时 dependent 不得被设置；同样采用棘轮语义，
// 设置状态未变的更新不重新报错。
func TestDependentForbidden(t *testing.T) {
	type obj struct {
		Trigger    *string
		Dependent  *string
		OtherField *string
	}

	triggerIsSet := func(o *obj) bool { return o != nil && o.Trigger != nil }
	dependentIsSet := func(o *obj) bool { return o != nil && o.Dependent != nil }

	cases := []struct {
		name   string
		op     operation.Operation
		obj    *obj
		oldObj *obj
		err    string // 正则表达式；为空表示预期无错误。
	}{{
		// trigger 未设置时 dependent 允许存在。
		name: "create: trigger unset, dependent set",
		op:   operation.Operation{Type: operation.Create},
		obj:  &obj{Dependent: new("d")},
	}, {
		// trigger 设置而 dependent 未设置，满足约束。
		name: "create: trigger set, dependent unset",
		op:   operation.Operation{Type: operation.Create},
		obj:  &obj{Trigger: new("t")},
	}, {
		// 创建时违反约束：两者同时设置。
		name: "create: trigger set, dependent set",
		op:   operation.Operation{Type: operation.Create},
		obj:  &obj{Trigger: new("t"), Dependent: new("d")},
		err:  `fldpath\.dependent: Forbidden: may not be set when trigger is set`,
	}, {
		// nil 对象不触发任何校验。
		name: "create: nil obj",
		op:   operation.Operation{Type: operation.Create},
		obj:  nil,
	}, {
		// 棘轮：仅无关字段变化，设置状态未变，不应报错。
		name:   "ratchet: unrelated field changed, trigger and dependent set-ness unchanged",
		op:     operation.Operation{Type: operation.Update},
		obj:    &obj{Trigger: new("t"), Dependent: new("d"), OtherField: new("new")},
		oldObj: &obj{Trigger: new("t"), Dependent: new("d"), OtherField: new("old")},
	}, {
		// 棘轮：trigger 取值变化不影响设置状态判断。
		name:   "ratchet: trigger value changed, set-ness unchanged",
		op:     operation.Operation{Type: operation.Update},
		obj:    &obj{Trigger: new("t"), Dependent: new("d")},
		oldObj: &obj{Trigger: new("old"), Dependent: new("d")},
	}, {
		// 更新时新设置 trigger，而 dependent 仍存在，构成新的违规。
		name:   "update: trigger newly set",
		op:     operation.Operation{Type: operation.Update},
		obj:    &obj{Trigger: new("t"), Dependent: new("d")},
		oldObj: &obj{Dependent: new("d")},
		err:    `fldpath\.dependent: Forbidden: may not be set when trigger is set`,
	}, {
		// 更新时新设置 dependent，而 trigger 已设置，构成新的违规。
		name:   "update: dependent newly set",
		op:     operation.Operation{Type: operation.Update},
		obj:    &obj{Trigger: new("t"), Dependent: new("d")},
		oldObj: &obj{Trigger: new("t")},
		err:    `fldpath\.dependent: Forbidden: may not be set when trigger is set`,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := DependentForbidden(context.Background(), tc.op,
				field.NewPath("fldpath"), tc.obj, tc.oldObj,
				"trigger", triggerIsSet,
				"dependent", dependentIsSet)
			if len(result) > 0 && tc.err == "" {
				t.Fatalf("unexpected failure: %v", fmtErrs(result))
			}
			if len(result) == 0 && tc.err != "" {
				t.Fatalf("unexpected success: expected %q", tc.err)
			}
			if len(result) > 1 {
				t.Fatalf("unexpected multi-error: %v", fmtErrs(result))
			}
			// 只应产生一条错误，且 origin 必须标记为 dependentForbidden。
			if len(result) > 0 {
				if !regexp.MustCompile(tc.err).MatchString(result[0].Error()) {
					t.Errorf("wrong error\nexpected: %q\n     got: %v", tc.err, fmtErrs(result))
				}
				if result[0].Origin != "dependentForbidden" {
					t.Errorf("expected origin %q, got %q", "dependentForbidden", result[0].Origin)
				}
			}
		})
	}
}
