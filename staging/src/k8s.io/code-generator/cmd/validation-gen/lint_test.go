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

package main

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/code-generator/cmd/validation-gen/validators"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/types"
)

// ruleAlwaysPass 是一个恒定通过的测试规则，只用来统计被调用的次数。
func ruleAlwaysPass(container *types.Type, t *types.Type, tags []codetags.Tag) (string, error) {
	return "", nil
}

// ruleAlwaysFail 恒定返回一条违规消息，用于验证 lint 错误会被收集但不会中断遍历。
func ruleAlwaysFail(container *types.Type, t *types.Type, tags []codetags.Tag) (string, error) {
	return "lintfail", nil
}

// ruleAlwaysErr 恒定返回 error，用于验证规则内部错误会终止 lintComments 遍历。
func ruleAlwaysErr(container *types.Type, t *types.Type, tags []codetags.Tag) (string, error) {
	return "", errors.New("linterr")
}

// mkCountRule 包装真实规则并记录调用次数，供测试断言调用行为。
func mkCountRule(counter *int, realRule lintRule) lintRule {
	return func(container *types.Type, t *types.Type, tags []codetags.Tag) (string, error) {
		(*counter)++
		return realRule(container, t, tags)
	}
}

// validator 复用一个全局标签解析器，避免每个用例重复初始化。
var validator = validators.InitGlobalValidator(&generator.Context{}, nil)

// TestLintCommentsRuleInvocation 验证 lintComments 对规则与注释分组的遍历语义：
// 每个注释分组会依次应用每条规则；某条规则返回 error 时立即停止当前分组的后续规则。
func TestLintCommentsRuleInvocation(t *testing.T) {
	tests := []struct {
		name              string
		rules             []lintRule
		commentLineGroups [][]string
		wantErr           bool
		wantCount         int
	}{
		{
			name:              "0 rules, 0 comments",
			rules:             []lintRule{},
			commentLineGroups: [][]string{},
			wantErr:           false,
			wantCount:         0,
		},
		{
			name:              "1 rule, 1 comment",
			rules:             []lintRule{ruleAlwaysPass},
			commentLineGroups: [][]string{{"comment"}},
			wantErr:           false,
			wantCount:         1,
		},
		{
			name:              "3 rules, 3 comments",
			rules:             []lintRule{ruleAlwaysPass, ruleAlwaysFail, ruleAlwaysErr},
			commentLineGroups: [][]string{{"comment1"}, {"comment2"}, {"comment3"}},
			wantErr:           true,
			wantCount:         9,
		},
		{
			name:              "1 rule, 1 comment, rule fails",
			rules:             []lintRule{ruleAlwaysFail},
			commentLineGroups: [][]string{{"comment"}},
			wantErr:           false,
			wantCount:         1,
		},
		{
			name:              "1 rule, 1 comment, rule errors",
			rules:             []lintRule{ruleAlwaysErr},
			commentLineGroups: [][]string{{"comment"}},
			wantErr:           true,
			wantCount:         1,
		},
		{
			name:              "3 rules, 1 comment, rule errors in the middle",
			rules:             []lintRule{ruleAlwaysPass, ruleAlwaysErr, ruleAlwaysFail},
			commentLineGroups: [][]string{{"comment"}},
			wantErr:           true,
			wantCount:         2,
		},
	}

	// 每条规则都被计数包装；wantCount 精确反映预期的调用次数。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counter := 0
			rules := make([]lintRule, len(tt.rules))
			for i, rule := range tt.rules {
				rules[i] = mkCountRule(&counter, rule)
			}
			l := newLinter(rules...)
			for _, commentLines := range tt.commentLineGroups {
				_, err := l.lintComments(nil, nil, commentLines)
				gotErr := err != nil
				if gotErr != tt.wantErr {
					t.Errorf("lintComments() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
			if counter != tt.wantCount {
				t.Errorf("expected %d rule invocations, got %d", tt.wantCount, counter)
			}
		})
	}
}

// TestRuleAlphaBetaPrefix 验证 alphaBetaPrefix 规则：+k8s:alpha/+k8s:beta 的值必须
// 本身是一个校验标签，且只能出现在最外层（不能嵌在 item/listType 等链式标签中间）。
func TestRuleAlphaBetaPrefix(t *testing.T) {
	tests := []struct {
		name     string
		comments []string
		wantMsg  string
	}{
		{
			name:     "valid alpha prefix",
			comments: []string{"+k8s:alpha=+k8s:required"},
			wantMsg:  "",
		},
		{
			name:     "valid beta prefix",
			comments: []string{"+k8s:beta=+k8s:required"},
			wantMsg:  "",
		},
		{
			name:     "invalid alpha prefix (no value)",
			comments: []string{"+k8s:alpha"},
			wantMsg:  `tag "k8s:alpha" requires a validation tag as its value payload`,
		},
		{
			name:     "invalid beta prefix (no value)",
			comments: []string{"+k8s:beta"},
			wantMsg:  `tag "k8s:beta" requires a validation tag as its value payload`,
		},
		{
			name:     "invalid alpha prefix (value not tag)",
			comments: []string{"+k8s:alpha=foo"},
			wantMsg:  `tag "k8s:alpha" requires a validation tag as its value payload`,
		},
		{
			name:     "invalid usage of alpha prefix",
			comments: []string{`+k8s:item(type: "Approved")=+k8s:alpha=+k8s:zeroOrOneOfMember`},
			wantMsg:  `tag "k8s:alpha" can't be used in between`,
		},
		{
			name:     "nested alpha in item",
			comments: []string{`+k8s:item=+k8s:alpha=+k8s:required`},
			wantMsg:  `tag "k8s:alpha" can't be used in between`,
		},
		{
			name:     "alpha nested in listType",
			comments: []string{`+k8s:listType=+k8s:alpha=+k8s:required`},
			wantMsg:  `tag "k8s:alpha" can't be used in between`,
		},
		{
			name:     "deeply nested alpha",
			comments: []string{`+k8s:item=+k8s:listType=+k8s:alpha=+k8s:required`},
			wantMsg:  `tag "k8s:alpha" can't be used in between`,
		},
	}

	// 先解析标签再应用规则，模拟真实的 lint 流程。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tags, _ := validator.ExtractTags(validators.Context{}, tt.comments)
			msg, err := alphaBetaPrefix()(nil, nil, tags)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			} else if msg != tt.wantMsg {
				t.Errorf("got %q, want %q", msg, tt.wantMsg)
			}
		})
	}
}

// TestRuleStability 验证 validationStability 规则：
// 稳定性较低的标签（Alpha/Beta）不能出现在更稳定的上下文中；
// 包的版本名（v1alpha1/v1beta1）可提升允许的上下文级别。
func TestRuleStability(t *testing.T) {
	tests := []struct {
		name     string
		comments []string
		pkg      string
		wantMsg  string
	}{
		{
			name:     "stable context, stable tag",
			comments: []string{"+k8s:required"}, // Stable
			wantMsg:  "",
		},
		{
			name:     "beta context, stable tag",
			comments: []string{"+k8s:beta=+k8s:required"}, // Beta context, Stable tag
			wantMsg:  "",
		},
		{
			name:     "alpha context, stable tag",
			comments: []string{"+k8s:alpha=+k8s:required"}, // Alpha context, Stable tag
			wantMsg:  "",
		},
		{
			name:     "alpha context, alpha tag",
			comments: []string{"+k8s:alpha=+k8s:validateTrueAlpha"}, // Alpha context, Alpha tag
			wantMsg:  "",
		},
		{
			name:     "stable context, alpha tag",
			comments: []string{"+k8s:validateTrueAlpha"}, // Stable context, Alpha tag
			wantMsg:  `tag "k8s:validateTrueAlpha" with stability level "Alpha" cannot be used in Stable validation`,
		},
		{
			name:     "beta context, alpha tag",
			comments: []string{"+k8s:beta=+k8s:validateTrueAlpha"}, // Beta context, Alpha tag
			wantMsg:  `tag "k8s:validateTrueAlpha" with stability level "Alpha" cannot be used in Beta validation`,
		},
		{
			name:     "alpha pkg context, beta tag (allowed)",
			comments: []string{"+k8s:validateTrueBeta"}, // Beta tag in Alpha package
			pkg:      "k8s.io/api/apps/v1alpha1",
			wantMsg:  "",
		},
		{
			name:     "alpha pkg context, alpha tag (allowed)",
			comments: []string{"+k8s:validateTrueAlpha"}, // Alpha tag in Alpha package
			pkg:      "k8s.io/api/apps/v1alpha1",
			wantMsg:  "",
		},
		{
			name:     "beta pkg context, beta tag (allowed)",
			comments: []string{"+k8s:validateTrueBeta"}, // Beta tag in Beta package
			pkg:      "k8s.io/api/apps/v1beta1",
			wantMsg:  "",
		},
		{
			name:     "beta pkg context, alpha tag (fails)",
			comments: []string{"+k8s:validateTrueAlpha"}, // Alpha tag in Beta package
			pkg:      "k8s.io/api/apps/v1beta1",
			wantMsg:  `tag "k8s:validateTrueAlpha" with stability level "Alpha" cannot be used in Beta validation`,
		},
		{
			name:     "ifEnabled context allows beta tag",
			comments: []string{"+k8s:ifEnabled(SomeFeature)=+k8s:validateTrueBeta"}, // Beta tag in ifEnabled
			wantMsg:  "",
		},
		{
			name:     "ifEnabled context fails alpha tag",
			comments: []string{"+k8s:ifEnabled(SomeFeature)=+k8s:validateTrueAlpha"}, // Alpha tag in ifEnabled
			wantMsg:  `tag "k8s:validateTrueAlpha" with stability level "Alpha" cannot be used in Beta validation`,
		},
	}

	// dummyType 的包名决定“包上下文”的稳定性级别。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dummyType := &types.Type{Name: types.Name{Package: tt.pkg, Name: "Dummy"}}
			rule := validationStability()
			tags, _ := validator.ExtractTags(validators.Context{}, tt.comments)
			msg, err := rule(nil, dummyType, tags)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			} else if msg != tt.wantMsg {
				t.Errorf("got %q, want %q", msg, tt.wantMsg)
			}
		})
	}
}

// TestLintType 验证 lintType 的递归遍历：指针、slice、map、alias、struct 成员
// 以及嵌套类型中的注释标签都会被逐层检查，因此规则调用次数随嵌套深度增长。
func TestLintType(t *testing.T) {
	tests := []struct {
		name        string
		typeToLint  *types.Type
		wantCount   int
		expectError bool
	}{
		{
			name: "No comments",
			typeToLint: &types.Type{
				Name:         types.Name{Package: "testpkg", Name: "TestType"},
				CommentLines: nil,
			},
			wantCount:   0,
			expectError: false,
		},
		{
			name: "Valid comments",
			typeToLint: &types.Type{
				Name:         types.Name{Package: "testpkg", Name: "TestType"},
				CommentLines: []string{"+k8s:optional"},
			},
			wantCount:   1,
			expectError: false,
		},
		{
			name: "Pointer type",
			typeToLint: &types.Type{
				Name:         types.Name{Package: "testpkg", Name: "TestPointer"},
				Kind:         types.Pointer,
				Elem:         &types.Type{Name: types.Name{Package: "testpkg", Name: "ElemType"}, CommentLines: []string{"+k8s:optional"}},
				CommentLines: []string{"+k8s:optional"},
			},
			wantCount:   2,
			expectError: false,
		},
		{
			name: "Slice of pointers",
			typeToLint: &types.Type{
				Name: types.Name{Package: "testpkg", Name: "TestSlice"},
				Kind: types.Slice,
				Elem: &types.Type{
					Name:         types.Name{Package: "testpkg", Name: "PointerElem"},
					Kind:         types.Pointer,
					Elem:         &types.Type{Name: types.Name{Package: "testpkg", Name: "ElemType"}, CommentLines: []string{"+k8s:optional"}},
					CommentLines: []string{"+k8s:optional"},
				},
				CommentLines: []string{"+k8s:optional"},
			},
			wantCount:   3,
			expectError: false,
		},
		{
			name: "Map to pointers",
			typeToLint: &types.Type{
				Name: types.Name{Package: "testpkg", Name: "TestMap"},
				Kind: types.Map,
				Key:  &types.Type{Name: types.Name{Package: "testpkg", Name: "KeyType"}, CommentLines: []string{"+k8s:required"}},
				Elem: &types.Type{
					Name:         types.Name{Package: "testpkg", Name: "PointerElem"},
					Kind:         types.Pointer,
					Elem:         &types.Type{Name: types.Name{Package: "testpkg", Name: "ElemType"}, CommentLines: []string{"+k8s:optional"}},
					CommentLines: []string{"+k8s:optional"},
				},
				CommentLines: []string{"+k8s:optional"},
			},
			wantCount:   4,
			expectError: false,
		},
		{
			name: "Alias to pointers",
			typeToLint: &types.Type{
				Name: types.Name{Package: "testpkg", Name: "TestAlias"},
				Kind: types.Alias,
				Underlying: &types.Type{
					Name:         types.Name{Package: "testpkg", Name: "PointerElem"},
					Kind:         types.Pointer,
					Elem:         &types.Type{Name: types.Name{Package: "testpkg", Name: "ElemType"}, CommentLines: []string{"+k8s:optional"}},
					CommentLines: []string{"+k8s:optional"},
				},
				CommentLines: []string{"+k8s:optional"},
			},
			wantCount:   3,
			expectError: false,
		},
		{
			name: "Struct with members",
			typeToLint: &types.Type{
				Name: types.Name{Package: "testpkg", Name: "TestStruct"},
				Kind: types.Struct,
				Members: []types.Member{
					{
						Name:         "Field1",
						Type:         &types.Type{Name: types.Name{Package: "testpkg", Name: "FieldType"}},
						CommentLines: []string{"+k8s:optional"},
					},
					{
						Name:         "Field2",
						Type:         &types.Type{Name: types.Name{Package: "testpkg", Name: "FieldType"}},
						CommentLines: []string{"+k8s:required"},
					},
				},
			},
			wantCount:   2,
			expectError: false,
		},
		{
			name: "Nested types",
			typeToLint: &types.Type{
				Name: types.Name{Package: "testpkg", Name: "TestStruct"},
				Kind: types.Struct,
				Members: []types.Member{
					{
						Name: "Field1",
						Type: &types.Type{
							Name:         types.Name{Package: "testpkg", Name: "NestedStruct"},
							Kind:         types.Struct,
							CommentLines: []string{"+k8s:optional"},
							Members: []types.Member{
								{
									Name:         "NestedField1",
									Type:         &types.Type{Name: types.Name{Package: "testpkg", Name: "NestedFieldType"}},
									CommentLines: []string{"+k8s:required"},
								},
							},
						},
					},
				},
			},
			wantCount:   3,
			expectError: false,
		},
	}

	// 用计数规则记录遍历到多少处标签；错误检查走 linter 自身的 lintErrors。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counter := 0
			rules := []lintRule{mkCountRule(&counter, ruleAlwaysPass)}
			l := newLinter(rules...)
			if err := l.lintType(tt.typeToLint); err != nil {
				t.Fatal(err)
			}
			gotErr := len(l.lintErrors) > 0
			if gotErr != tt.expectError {
				t.Errorf("LintType() errors = %v, expectError %v", l.lintErrors, tt.expectError)
			}
			if counter != tt.wantCount {
				t.Errorf("expected %d rule invocations, got %d", tt.wantCount, counter)
			}
		})
	}
}

// TestHasAnyValidationTag 验证 hasNonOpaqueValidationTag：
// required/forbidden 等必填性标签以及带值负载的链式校验标签都算“有校验”；
// optional 只声明可选性，不算；非校验类 +k8s: 标签（如 openapi-gen）也不算。
func TestHasAnyValidationTag(t *testing.T) {
	tests := []struct {
		name     string
		comments []string
		want     bool
	}{
		{
			name:     "empty",
			comments: []string{},
			want:     false,
		},
		{
			name:     "no k8s tags",
			comments: []string{"just a comment"},
			want:     false,
		},
		{
			name:     "optional only",
			comments: []string{"+k8s:optional"},
			want:     false,
		},
		{
			name:     "required only",
			comments: []string{"+k8s:required"},
			want:     true,
		},
		{
			name:     "forbidden only",
			comments: []string{"+k8s:forbidden"},
			want:     true,
		},
		{
			name:     "unrecognized k8s tag",
			comments: []string{"+k8s:openapi-gen=true"},
			want:     false,
		},
		{
			name:     "minimum tag",
			comments: []string{"+k8s:minimum=0"},
			want:     true,
		},
		{
			name:     "enum tag",
			comments: []string{"+k8s:enum"},
			want:     true,
		},
		{
			name:     "mixed with optional",
			comments: []string{"+k8s:optional", "+k8s:minimum=0"},
			want:     true,
		},
	}

	// 链式标签集合来自全局校验器的文档：只有 PayloadsType 为 Tag 的标签
	// 才能作为另一个标签的负载。
	chainTags := sets.New[string]()
	for _, doc := range validator.Docs() {
		if doc.PayloadsType == codetags.ValueTypeTag {
			chainTags.Insert(doc.Tag)
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tags, _ := validator.ExtractTags(validators.Context{}, tt.comments)
			if got := hasNonOpaqueValidationTag(validator, chainTags, tags); got != tt.want {
				t.Errorf("hasNonOpaqueValidationTag() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHasRequirednessTag 验证 hasRequirednessTag 能识别直接以及被
// alpha/beta 前缀包裹的 optional/required 标签。
func TestHasRequirednessTag(t *testing.T) {
	tests := []struct {
		name     string
		comments []string
		want     bool
	}{
		{
			name:     "empty",
			comments: []string{},
			want:     false,
		},
		{
			name:     "no requireness",
			comments: []string{"+k8s:minimum=0"},
			want:     false,
		},
		{
			name:     "optional",
			comments: []string{"+k8s:optional"},
			want:     true,
		},
		{
			name:     "required",
			comments: []string{"+k8s:required"},
			want:     true,
		},
		{
			name:     "optional with value",
			comments: []string{"+k8s:optional=true"},
			want:     true,
		},
		{
			name:     "conditional optional",
			comments: []string{`+k8s:alpha(since:"1.35")=+k8s:optional`},
			want:     true,
		},
		{
			name:     "conditional required",
			comments: []string{`+k8s:alpha(since:"1.35")=+k8s:required`},
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tags, _ := validator.ExtractTags(validators.Context{}, tt.comments)
			if got := hasRequirednessTag(tags); got != tt.want {
				t.Errorf("hasRequirednessTag() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestLintRequiredness 验证 requiredAndOptional lint 规则：
// 指针/切片/映射字段一旦带有（直接或传递的）校验，就必须显式声明
// optional/required/forbidden，否则无法判断“零值是否合法”。
// 覆盖递归类型、opaqueType/eachVal 等逃逸标记以及缓存复用等边界场景。
func TestLintRequiredness(t *testing.T) {
	sharedAlias := testAlias("MyAlias", testSlice(testStruct("Inner", []types.Member{
		testField("Bar", testType("int"), "+k8s:minimum=0"),
	})))

	tests := []struct {
		name       string
		typeToLint *types.Type
		wantError  string
	}{
		{
			name: "pointer field without validation - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(testType("string"))),
			}),
			wantError: "",
		},
		{
			name: "pointer field with direct validation, no requireness - error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(testType("int")), "+k8s:minimum=0"),
			}),
			wantError: "field Foo: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "pointer field with transitive validation, no requireness - error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(
					testStruct("Inner", []types.Member{
						testField("Bar", testType("int"), "+k8s:minimum=0"),
					}),
				)),
			}),
			wantError: "field Foo: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "pointer field with validation and +k8s:optional - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(testType("int")), "+k8s:optional", "+k8s:minimum=0"),
			}),
			wantError: "",
		},
		{
			name: "slice field with validation, no requireness - error",
			typeToLint: testStruct("T", []types.Member{
				testField("Items", testSlice(testType("string")), "+k8s:maxItems=10"),
			}),
			wantError: "field Items: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "map field with validation, no requireness - error",
			typeToLint: testStruct("T", []types.Member{
				testField("Data", testMap(testType("string"), testType("string")), "+k8s:maxItems=5"),
			}),
			wantError: "field Data: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "non-pointer struct field with validation - no error (exempt)",
			typeToLint: testStruct("T", []types.Member{
				testField("Nested", testStruct("Inner", nil, "+k8s:minimum=0")),
			}),
			wantError: "",
		},
		{
			name: "recursive type with pointer to self - no infinite loop",
			typeToLint: func() *types.Type {
				t := testStruct("Node", nil)
				t.Members = []types.Member{
					testField("Next", testPtr(t), "+k8s:optional"),
				}
				return t
			}(),
			wantError: "",
		},
		{
			name: "recursive type with pointer to self - no infinite loop, missing required validation",
			typeToLint: func() *types.Type {
				t := testStruct("Node", nil)
				t.Members = []types.Member{
					testField("Next", testPtr(t), "+k8s:immutable"),
				}
				return t
			}(),
			wantError: "field Next: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "recursive type with validation - detects validation on first visit",
			typeToLint: func() *types.Type {
				t := testStruct("Node", nil, "+k8s:immutable")
				t.Members = []types.Member{
					testField("Next", testPtr(t), "+k8s:optional"),
				}
				return t
			}(),
			wantError: "",
		},
		{
			name: "array field with transitive validation, no requiredNess - error",
			typeToLint: testStruct("T", []types.Member{
				testField("Arr", testArray(testStruct("Inner", []types.Member{
					testField("Bar", testType("int"), "+k8s:minimum=0"),
				}))),
			}),
			wantError: "field Arr: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "pointer field with transitive validation but marked opaqueType - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(
					testStruct("Inner", []types.Member{
						testField("Bar", testType("int"), "+k8s:minimum=0"),
					}),
				), "+k8s:opaqueType"),
			}),
			wantError: "",
		},
		{
			name: "pointer to alias field with transitive validation but marked opaqueType - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(
					testAlias("MyAlias",
						testStruct("Inner", []types.Member{
							testField("Bar", testType("int"), "+k8s:minimum=0"),
						}),
					),
				), "+k8s:opaqueType"),
			}),
			wantError: "",
		},
		{
			name: "alias field (to slice) with transitive validation but marked opaqueType - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testAlias("MyAlias",
					testSlice(
						testStruct("Inner", []types.Member{
							testField("Bar", testType("int"), "+k8s:minimum=0"),
						}),
					),
				), "+k8s:opaqueType"),
			}),
			wantError: "",
		},
		{
			name: "slice field with transitive validation marked opaqueValType - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testSlice(
					testStruct("Inner", []types.Member{
						testField("Bar", testType("int"), "+k8s:minimum=0"),
					}),
				), "+k8s:eachVal=+k8s:opaqueType"),
			}),
			wantError: "",
		},
		{
			name: "array field with transitive validation marked opaqueValType - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testArray(
					testStruct("Inner", []types.Member{
						testField("Bar", testType("int"), "+k8s:minimum=0"),
					}),
				), "+k8s:eachVal=+k8s:opaqueType"),
			}),
			wantError: "",
		},
		{
			name: "map field with transitive validation on key, marked opaqueKeyType - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testMap(
					testAlias("KeyType", types.String, "+k8s:minLength=1"),
					types.String,
				), "+k8s:eachKey=+k8s:opaqueType"),
			}),
			wantError: "",
		},
		{
			name: "map field with transitive validation on key, not marked opaqueKeyType - error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testMap(
					testAlias("KeyType", types.String, "+k8s:minLength=1"),
					types.String,
				)),
			}),
			wantError: "field Foo: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "map field with transitive validation on value, marked opaqueValType - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testMap(
					types.String,
					testStruct("Inner", []types.Member{
						testField("Bar", testType("int"), "+k8s:minimum=0"),
					}),
				), "+k8s:eachVal=+k8s:opaqueType"),
			}),
			wantError: "",
		},
		{
			name: "map field with transitive validation on both, both marked opaque - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testMap(
					testAlias("KeyType", types.String, "+k8s:minLength=1"),
					testStruct("Inner", []types.Member{
						testField("Bar", testType("int"), "+k8s:minimum=0"),
					}),
				), "+k8s:eachKey=+k8s:opaqueType", "+k8s:eachVal=+k8s:opaqueType"),
			}),
			wantError: "",
		},
		{
			name: "map field with transitive validation on both, only key marked opaque - error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testMap(
					testAlias("KeyType", types.String, "+k8s:minLength=1"),
					testStruct("Inner", []types.Member{
						testField("Bar", testType("int"), "+k8s:minimum=0"),
					}),
				), "+k8s:eachKey=+k8s:opaqueType"),
			}),
			wantError: "field Foo: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "map field with transitive validation on both, only value marked opaque - error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testMap(
					testAlias("KeyType", types.String, "+k8s:minLength=1"),
					testStruct("Inner", []types.Member{
						testField("Bar", testType("int"), "+k8s:minimum=0"),
					}),
				), "+k8s:eachVal=+k8s:opaqueType"),
			}),
			wantError: "field Foo: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "pointer field with nested opaque field - bypasses transitive validation",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(
					testStruct("Inner", []types.Member{
						testField("Bar", testPtr(
							testStruct("NestedInner", []types.Member{
								testField("Val", testType("int"), "+k8s:minimum=0"),
							}),
						), "+k8s:opaqueType"),
					}),
				)),
			}),
			wantError: "",
		},
		{
			name: "alias field (to slice) with transitive validation but no opacity tags on type - error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testAlias("MyAlias",
					testSlice(
						testStruct("Inner", []types.Member{
							testField("Bar", testType("int"), "+k8s:minimum=0"),
						}),
					),
				)),
			}),
			wantError: "field Foo: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "alias field (to slice) with transitive validation and eachVal marked opaque on the field - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testAlias("MyAlias",
					testSlice(
						testStruct("Inner", []types.Member{
							testField("Bar", testType("int"), "+k8s:minimum=0"),
						}),
					),
				), "+k8s:eachVal=+k8s:opaqueType"),
			}),
			wantError: "",
		},
		{
			name: "alias field (to slice) with transitive validation and eachVal marked opaque on the type definition - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testAlias("MyAlias",
					testSlice(
						testStruct("Inner", []types.Member{
							testField("Bar", testType("int"), "+k8s:minimum=0"),
						}),
					),
					"+k8s:eachVal=+k8s:opaqueType",
				)),
			}),
			wantError: "",
		},
		{
			name: "pointer to alias field (to slice) with transitive validation, eachVal marked opaque on type definition - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(
					testAlias("MyAlias",
						testSlice(
							testStruct("Inner", []types.Member{
								testField("Bar", testType("int"), "+k8s:minimum=0"),
							}),
						),
						"+k8s:eachVal=+k8s:opaqueType",
					),
				)),
			}),
			wantError: "",
		},
		{
			name: "alias field (to map) with transitive validation, eachVal marked opaque on type definition - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testAlias("MyAlias",
					testMap(
						types.String,
						testStruct("Inner", []types.Member{
							testField("Bar", testType("int"), "+k8s:minimum=0"),
						}),
					),
					"+k8s:eachVal=+k8s:opaqueType",
				)),
			}),
			wantError: "",
		},
		{
			name: "pointer to alias field (to struct) with transitive validation, opaqueType marked on type definition - no error",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(
					testAlias("MyAlias",
						testStruct("Inner", []types.Member{
							testField("Bar", testType("int"), "+k8s:minimum=0"),
						}),
						"+k8s:opaqueType",
					),
				)),
			}),
			wantError: "",
		},
		{
			name: "alias field (to slice) with local validation tag and elements marked opaque on the type definition - reports error",
			typeToLint: testStruct("T", []types.Member{
				testField("Widgets", testAlias("WidgetList",
					testSlice(
						testStruct("Inner", []types.Member{
							testField("Bar", testType("int"), "+k8s:minimum=0"),
						}),
					),
					"+k8s:maxItems=100",
					"+k8s:eachVal=+k8s:opaqueType",
				)),
			}),
			wantError: "field Widgets: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
		{
			name: "pointer field with transitive malformed tag on alias type definition - reports error as lint warning instead of crashing",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(
					testAlias("MyString", testType("string"), "+k8s:minimum=0"),
				)),
			}),
			wantError: "field Foo: invalid validation tags: tag \"k8s:minimum\": can only be used on integer types (pkg.MyString -> string)",
		},
		{
			name: "pointer field with transitive malformed tag on struct type definition - reports error as lint warning instead of crashing",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", testPtr(
					testStruct("MyStruct", []types.Member{
						testField("Bar", testType("int")),
					}, "+k8s:minimum=0"),
				)),
			}),
			wantError: "field Foo: invalid validation tags: tag \"k8s:minimum\": can only be used on integer types (pkg.MyStruct)",
		},
		{
			name: "same alias used with different opacity contexts caches correctly",
			typeToLint: testStruct("T", []types.Member{
				testField("Foo", sharedAlias, "+k8s:opaqueType"),
				testField("Bar", sharedAlias),
			}),
			wantError: "field Bar: field with validation must have +k8s:optional, +k8s:required or +k8s:forbidden",
		},
	}

	// 每个用例应产生 0 或 1 条错误；多于 1 条即视为规则行为异常。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLinter(requiredAndOptional(validator))
			if err := l.lintType(tt.typeToLint); err != nil {
				t.Fatalf("lintType() unexpected error: %v", err)
			}
			errs := l.lintErrors[tt.typeToLint]
			if len(errs) > 1 {
				t.Fatalf("got %d errors, but expected 0 or 1 error: %v", len(errs), errs)
			}
			var gotError string
			if len(errs) == 1 {
				gotError = errs[0].Error()
			}
			if gotError != tt.wantError {
				t.Errorf("lintRequiredness() error = %q, want %q", gotError, tt.wantError)
			}
		})
	}
}

// testType 构造一个内置类型，可附带注释标签。
func testType(name string, comments ...string) *types.Type {
	return &types.Type{
		Name:         types.Name{Package: "", Name: name},
		Kind:         types.Builtin,
		CommentLines: comments,
	}
}

// testPtr 构造指针类型；名称带 Ptr 后缀以免与元素类型混淆。
func testPtr(elem *types.Type, comments ...string) *types.Type {
	return &types.Type{
		Name:         types.Name{Package: "pkg", Name: elem.Name.Name + "Ptr"},
		Kind:         types.Pointer,
		Elem:         elem,
		CommentLines: comments,
	}
}

// testStruct 构造结构体类型，成员与注释由调用方给出。
func testStruct(name string, members []types.Member, comments ...string) *types.Type {
	return &types.Type{
		Name:         types.Name{Package: "pkg", Name: name},
		Kind:         types.Struct,
		Members:      members,
		CommentLines: comments,
	}
}

// testField 构造结构体成员。
func testField(name string, fieldType *types.Type, comments ...string) types.Member {
	return types.Member{
		Name:         name,
		Type:         fieldType,
		CommentLines: comments,
	}
}

// testSlice 构造切片类型。
func testSlice(elem *types.Type, comments ...string) *types.Type {
	return &types.Type{
		Name:         types.Name{Package: "pkg", Name: "[]" + elem.Name.Name},
		Kind:         types.Slice,
		Elem:         elem,
		CommentLines: comments,
	}
}

// testArray 构造数组类型；与切片区分开以验证遍历逻辑处理 Array。
func testArray(elem *types.Type, comments ...string) *types.Type {
	return &types.Type{
		Name:         types.Name{Package: "pkg", Name: "array_" + elem.Name.Name},
		Kind:         types.Array,
		Elem:         elem,
		CommentLines: comments,
	}
}

// testMap 构造映射类型。
func testMap(key, val *types.Type, comments ...string) *types.Type {
	return &types.Type{
		Name:         types.Name{Package: "pkg", Name: "map_" + key.Name.Name + "_" + val.Name.Name},
		Kind:         types.Map,
		Key:          key,
		Elem:         val,
		CommentLines: comments,
	}
}

// testAlias 构造别名类型，模拟 Go typedef。
func testAlias(name string, underlying *types.Type, comments ...string) *types.Type {
	return &types.Type{
		Name:         types.Name{Package: "pkg", Name: name},
		Kind:         types.Alias,
		Underlying:   underlying,
		CommentLines: comments,
	}
}
