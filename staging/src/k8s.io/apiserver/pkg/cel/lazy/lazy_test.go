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

package lazy

import (
	"fmt"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"

	"k8s.io/apimachinery/pkg/util/version"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
)

// TestLazyMapType 验证 lazy MapValue 的三条核心性质：
//  1. 通过 MapValue 暴露的变量可被 CEL 正常求值；
//  2. 每个变量表达式至多求值一次（结果被缓存），重复引用不会重复计算；
//  3. 未被引用的变量绝不求值，包括被布尔短路跳过的分支。
func TestLazyMapType(t *testing.T) {
	env, variablesType, err := buildTestEnv()
	if err != nil {
		t.Fatal(err)
	}
	variablesMap := NewMapValue(variablesType)
	activation := &testActivation{variables: variablesMap}

	// 以字符串形式添加 foo
	variablesType.Fields["foo"] = apiservercel.NewDeclField("foo", apiservercel.StringType, true, nil, nil)
	variablesMap.Append("foo", func(_ *MapValue) ref.Val {
		return types.String("foo-string")
	})

	exp := "variables.foo == 'foo-string'"
	v, err := compileAndRun(env, activation, exp)
	if err != nil {
		t.Fatalf("%q: %v", exp, err)
	}
	if !v.Value().(bool) {
		t.Errorf("expected equal but got non-equal")
	}

	evalCounter := 0
	// 以表达式构造的 map 形式添加 dict
	variablesType.Fields["dict"] = apiservercel.NewDeclField("dict", apiservercel.DynType, true, nil, nil)
	variablesMap.Append("dict", func(_ *MapValue) ref.Val {
		evalCounter++
		v, err := compileAndRun(env, activation, `{"a": "a"}`)
		if err != nil {
			return types.NewErr("%s", err.Error())
		}
		return v
	})

	// 用 .all 遍历 map
	exp = `variables.all(n, n != "")`
	v, err = compileAndRun(env, activation, exp)
	if err != nil {
		t.Fatalf("%q: %v", exp, err)
	}
	if v.Value().(bool) != true {
		t.Errorf("%q: wrong result: %v", exp, v.Value())
	}

	// 以字符串形式添加 unused
	variablesType.Fields["unused"] = apiservercel.NewDeclField("unused", apiservercel.StringType, true, nil, nil)
	variablesMap.Append("unused", func(_ *MapValue) ref.Val {
		t.Fatalf("unused variable must not be evaluated")
		return nil
	})

	exp = "variables.dict.a + ' ' + variables.dict.a + ' ' + variables.foo"
	v, err = compileAndRun(env, activation, exp)
	if err != nil {
		t.Fatalf("%q: %v", exp, err)
	}
	if v.Value().(string) != "a a foo-string" {
		t.Errorf("%q: wrong result: %v", exp, v.Value())
	}
	if evalCounter != 1 {
		t.Errorf("expected eval %d times but got %d", 1, evalCounter)
	}

	// 因布尔短路而未被使用
	// 若 `variables.unused` 被求值，整个测试将 fatal 并退出。
	exp = "variables.dict.a == 'wrong' && variables.unused == 'unused'"
	v, err = compileAndRun(env, activation, exp)
	if err != nil {
		t.Fatalf("%q: %v", exp, err)
	}
	if v.Value().(bool) != false {
		t.Errorf("%q: wrong result: %v", exp, v.Value())
	}
}

// testActivation 是 CEL 求值激活环境，只暴露 variables 一个名字。
type testActivation struct {
	variables *MapValue
}

// compileAndRun 是“编译→建程序→求值”的便捷封装，失败时带上原始表达式便于定位。
func compileAndRun(env *cel.Env, activation *testActivation, exp string) (ref.Val, error) {
	ast, issues := env.Compile(exp)
	if issues != nil {
		return nil, fmt.Errorf("fail to compile: %v", issues)
	}
	prog, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("cannot create program: %w", err)
	}
	v, _, err := prog.Eval(activation)
	if err != nil {
		return nil, fmt.Errorf("cannot eval program: %w", err)
	}
	return v, nil
}

// buildTestEnv 构造带 variables 变量声明的 CEL 环境；Fields 手工填充，
// 以便测试按需动态追加变量而无需重新构建环境。
func buildTestEnv() (*cel.Env, *apiservercel.DeclType, error) {
	variablesType := apiservercel.NewMapType(apiservercel.StringType, apiservercel.AnyType, 0)
	variablesType.Fields = make(map[string]*apiservercel.DeclField)
	envSet, err := environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()).Extend(
		environment.VersionedOptions{
			IntroducedVersion: version.MajorMinor(1, 28),
			EnvOptions: []cel.EnvOption{
				cel.Variable("variables", variablesType.CelType()),
			},
			DeclTypes: []*apiservercel.DeclType{
				variablesType,
			},
		})
	if err != nil {
		return nil, nil, err
	}
	env, err := envSet.Env(environment.NewExpressions)
	return env, variablesType, err
}

// ResolveName 实现 interpreter.Activation，只解析 variables 名字。
func (a *testActivation) ResolveName(name string) (any, bool) {
	switch name {
	case "variables":
		return a.variables, true
	default:
		return nil, false
	}
}

// Parent 返回 nil，表示没有上层激活作用域。
func (a *testActivation) Parent() interpreter.Activation {
	return nil
}
