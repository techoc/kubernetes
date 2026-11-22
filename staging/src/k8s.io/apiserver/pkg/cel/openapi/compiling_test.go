/*
Copyright 2023 The Kubernetes Authors.

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

// 本文件验证由 OpenAPI schema 推导出的 CEL 类型在编译期与运行期的一致性：
// 同名但类型不同的字段必须在编译期被拒绝，而不是等到求值时产生难以理解的错误。

package openapi

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/interpreter"

	"k8s.io/apimachinery/pkg/util/version"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// TestMultipleTypes 验证多个同名不同类型的对象变量在同一表达式中时的类型检查行为。
func TestMultipleTypes(t *testing.T) {
	env, err := buildTestEnv()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		expression         string
		expectCompileError bool
		expectEvalResult   bool
	}{
		{
			// 两边类型一致（都是 string），编译通过且值相等。
			expression:       "foo.foo == bar.bar",
			expectEvalResult: true,
		},
		{
			// foo 没有 bar 字段：未知字段属于编译期错误，必须被拒绝。
			expression:         "foo.bar == 'value'",
			expectCompileError: true,
		},
		{
			// 同名但类型不同：foo.foo 是 string，bar.foo 不存在，直接比较字符串字面量仍可编译。
			expression:       "foo.foo == 'value'",
			expectEvalResult: true,
		},
		{
			expression:       "bar.bar == 'value'",
			expectEvalResult: true,
		},
		{
			// 两边都有 common 字段且都是 int64，1 + 2 = 3 > 2。
			expression:       "foo.common + bar.common <= 2",
			expectEvalResult: false, // 3 > 2
		},
		{
			// confusion 在 foo 中是 string、在 bar 中是 int64，跨类型比较应在编译期被拒绝，
			// 这正是“同名不同类型不能互相混淆”的核心断言。
			expression:         "foo.confusion == bar.confusion",
			expectCompileError: true,
		},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			ast, issues := env.Compile(tc.expression)
			if issues != nil {
				if tc.expectCompileError {
					return
				}
				t.Fatalf("compile error: %v", issues)
			}
			if issues != nil {
				t.Fatal(issues)
			}
			p, err := env.Program(ast)
			if err != nil {
				t.Fatal(err)
			}
			ret, _, err := p.Eval(&simpleActivation{
				foo: map[string]any{"foo": "value", "common": 1, "confusion": "114514"},
				bar: map[string]any{"bar": "value", "common": 2, "confusion": 114514},
			})
			if err != nil {
				t.Fatal(err)
			}
			if ret.Type() != types.BoolType {
				t.Errorf("bad result type: %v", ret.Type())
			}
			if res := ret.Value().(bool); tc.expectEvalResult != res {
				t.Errorf("expectEvalResult expression evaluates to %v, got %v", tc.expectEvalResult, res)
			}
		})
	}

}

// buildTestEnv sets up an environment that contains two variables, "foo" and
// "bar".
// foo is an object with a string field "foo", an integer field "common", and a string field "confusion"
// bar is an object with a string field "bar", an integer field "common", and an integer field "confusion"
//
// 两个变量共享字段名 common 但类型一致，confusion 则类型冲突——用于验证类型系统的隔离性。
func buildTestEnv() (*cel.Env, error) {
	fooType := common.SchemaDeclType(simpleMapSchema("foo", spec.StringProperty()), true).MaybeAssignTypeName("fooType")
	barType := common.SchemaDeclType(simpleMapSchema("bar", spec.Int64Property()), true).MaybeAssignTypeName("barType")

	env, err := environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()).Extend(
		environment.VersionedOptions{
			IntroducedVersion: version.MajorMinor(1, 26),
			EnvOptions: []cel.EnvOption{
				cel.Variable("foo", fooType.CelType()),
				cel.Variable("bar", barType.CelType()),
			},
			DeclTypes: []*apiservercel.DeclType{
				fooType,
				barType,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return env.Env(environment.NewExpressions)
}

// simpleMapSchema 构造一个只含三个字段的对象 schema；
// confusionSchema 由调用方指定，用于制造两个变量间的类型差异。
func simpleMapSchema(fieldName string, confusionSchema *spec.Schema) common.Schema {
	return &Schema{Schema: &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				fieldName:   *spec.StringProperty(),
				"common":    *spec.Int64Property(),
				"confusion": *confusionSchema,
			},
		},
	}}
}

// simpleActivation 是只暴露 foo/bar 两个变量的最小 interpreter.Activation 实现。
type simpleActivation struct {
	foo any
	bar any
}

// ResolveName 按名字解析变量；未声明的名字返回 false，交由 CEL 报告未定义变量。
func (a *simpleActivation) ResolveName(name string) (interface{}, bool) {
	switch name {
	case "foo":
		return a.foo, true
	case "bar":
		return a.bar, true
	default:
		return nil, false
	}
}

// Parent 返回 nil：测试不需要嵌套作用域。
func (a *simpleActivation) Parent() interpreter.Activation {
	return nil
}
