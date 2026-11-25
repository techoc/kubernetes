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
	"regexp"
	"testing"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestRequiredValue 覆盖 RequiredValue 对常见 Go 类型的零值判定。
//
// 每个 case 用闭包构造输入，err 是该 case 期望的错误正则（空表示应通过）。
// 注意指针类型：指向零值的非 nil 指针不算零值，而 nil 指针算零值。
func TestRequiredValue(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := "value"
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := "" // 零值。
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := 123
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := 0 // 零值。
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := true
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := false // 零值。
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := struct{ S string }{"value"}
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := struct{ S string }{} // 零值。
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := ptr.To("")
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := (*string)(nil) // 零值。
			return RequiredValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Required value",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestRequiredPointer 验证 RequiredPointer 只关心指针本身是否为 nil，
// 与指向的值是否为零值无关。
func TestRequiredPointer(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := ""
			return RequiredPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*string)(nil)
			return RequiredPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := 0
			return RequiredPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*int)(nil)
			return RequiredPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := false
			return RequiredPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*bool)(nil)
			return RequiredPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := struct{ S string }{}
			return RequiredPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*struct{ S string })(nil)
			return RequiredPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := (*string)(nil)
			return RequiredPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (**string)(nil)
			return RequiredPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath: Required value",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestRequiredSlice 验证 RequiredSlice 以长度为判据：元素内容即使是零值也算已设置。
func TestRequiredSlice(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []string{""}
			return RequiredSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []string{}
			return RequiredSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []int{0}
			return RequiredSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []int{}
			return RequiredSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []bool{false}
			return RequiredSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []bool{}
			return RequiredSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []*string{nil}
			return RequiredSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []*string{}
			return RequiredSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Required value",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestRequiredMap 验证 RequiredMap 以条目数为判据，key/value 为零值不影响结果。
func TestRequiredMap(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[string]string{"": ""}
			return RequiredMap(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[string]string{}
			return RequiredMap(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[int]int{0: 0}
			return RequiredMap(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[int]int{}
			return RequiredMap(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Required value",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[bool]bool{false: false}
			return RequiredMap(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[string]bool{}
			return RequiredMap(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Required value",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestOptionalValue 与 TestRequiredValue 场景相同，但期望错误信息带有
// "optional value was not specified" 说明。
func TestOptionalValue(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := "value"
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := "" // 零值。
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := 123
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := 0 // 零值。
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := true
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := false // 零值。
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := struct{ S string }{"value"}
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := struct{ S string }{} // 零值。
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := ptr.To("")
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := (*string)(nil) // 零值。
			return OptionalValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestOptionalPointer 验证 OptionalPointer 的 nil 指针判定及错误文案。
func TestOptionalPointer(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := ""
			return OptionalPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*string)(nil)
			return OptionalPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := 0
			return OptionalPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*int)(nil)
			return OptionalPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := false
			return OptionalPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*bool)(nil)
			return OptionalPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := struct{ S string }{}
			return OptionalPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*struct{ S string })(nil)
			return OptionalPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := (*string)(nil)
			return OptionalPointer(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (**string)(nil)
			return OptionalPointer(context.Background(), op, fp, pointer, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestOptionalSlice 验证 OptionalSlice 的空切片判定及错误文案。
func TestOptionalSlice(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []string{""}
			return OptionalSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []string{}
			return OptionalSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []int{0}
			return OptionalSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []int{}
			return OptionalSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []bool{false}
			return OptionalSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []bool{}
			return OptionalSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []*string{nil}
			return OptionalSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []*string{}
			return OptionalSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestOptionalMap 验证 OptionalMap 的空 map 判定及错误文案。
func TestOptionalMap(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[string]string{"": ""}
			return OptionalMap(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[string]string{}
			return OptionalMap(context.Background(), op, fp, value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[int]int{0: 0}
			return OptionalMap(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[int]int{}
			return OptionalMap(context.Background(), op, fp, value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[bool]bool{false: false}
			return OptionalMap(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[string]bool{}
			return OptionalMap(context.Background(), op, fp, value, nil)
		},
		err: "fldpath:.*optional value was not specified",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestForbiddenValue 验证 ForbiddenValue 仅在零值时通过。
func TestForbiddenValue(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := ""
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := "value"
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := 0
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := 123
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := false
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := true
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := struct{ S string }{}
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := struct{ S string }{"value"}
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := (*string)(nil)
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := ptr.To("")
			return ForbiddenValue(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestForbiddenPointer 验证 ForbiddenPointer 仅在指针为 nil 时通过。
func TestForbiddenPointer(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*string)(nil)
			return ForbiddenPointer(context.Background(), op, fp, pointer, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := ""
			return ForbiddenPointer(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*int)(nil)
			return ForbiddenPointer(context.Background(), op, fp, pointer, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := 0
			return ForbiddenPointer(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*bool)(nil)
			return ForbiddenPointer(context.Background(), op, fp, pointer, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := false
			return ForbiddenPointer(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (*struct{ S string })(nil)
			return ForbiddenPointer(context.Background(), op, fp, pointer, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := struct{ S string }{}
			return ForbiddenPointer(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			pointer := (**string)(nil)
			return ForbiddenPointer(context.Background(), op, fp, pointer, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := (*string)(nil)
			return ForbiddenPointer(context.Background(), op, fp, &value, nil)
		},
		err: "fldpath: Forbidden",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestForbiddenSlice 验证 ForbiddenSlice 仅在切片为空时通过。
func TestForbiddenSlice(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []string{}
			return ForbiddenSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []string{""}
			return ForbiddenSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []int{}
			return ForbiddenSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []int{0}
			return ForbiddenSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []bool{}
			return ForbiddenSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []bool{false}
			return ForbiddenSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []*string{}
			return ForbiddenSlice(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := []*string{nil}
			return ForbiddenSlice(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Forbidden",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}

// TestForbiddenMap 验证 ForbiddenMap 仅在 map 为空时通过。
func TestForbiddenMap(t *testing.T) {
	cases := []struct {
		fn  func(op operation.Operation, fp *field.Path) field.ErrorList
		err string // 正则表达式。
	}{{
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[string]string{}
			return ForbiddenMap(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[string]string{"": ""}
			return ForbiddenMap(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[int]int{}
			return ForbiddenMap(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[int]int{0: 0}
			return ForbiddenMap(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Forbidden",
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[string]bool{}
			return ForbiddenMap(context.Background(), op, fp, value, nil)
		},
	}, {
		fn: func(op operation.Operation, fp *field.Path) field.ErrorList {
			value := map[bool]bool{false: false}
			return ForbiddenMap(context.Background(), op, fp, value, nil)
		},
		err: "fldpath: Forbidden",
	}}

	for i, tc := range cases {
		result := tc.fn(operation.Operation{}, field.NewPath("fldpath"))
		if len(result) > 0 && tc.err == "" {
			t.Errorf("case %d: unexpected failure: %v", i, fmtErrs(result))
			continue
		}
		if len(result) == 0 && tc.err != "" {
			t.Errorf("case %d: unexpected success: expected %q", i, tc.err)
			continue
		}
		if len(result) > 0 {
			if len(result) > 1 {
				t.Errorf("case %d: unexepected multi-error: %v", i, fmtErrs(result))
				continue
			}
			if re := regexp.MustCompile(tc.err); !re.MatchString(result[0].Error()) {
				t.Errorf("case %d: wrong error\nexpected: %q\n     got: %v", i, tc.err, fmtErrs(result))
			}
		}
	}
}
