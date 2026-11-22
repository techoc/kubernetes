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

package cel

import (
	"fmt"

	"github.com/google/cel-go/cel"

	"k8s.io/apimachinery/pkg/util/version"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
)

const (
	// claimsVarName 是 token claims 映射在 CEL 中的变量名
	claimsVarName = "claims"
	// userVarName 是 UserInfo 在 CEL 中的变量名
	userVarName = "user"
)

// compiler 实现 Compiler 接口。
type compiler struct {
	// varEnvs 是以 CEL 变量名为 key 的环境映射，表达式只能看到对应的变量。
	// 这里有两套环境：claims 与 user。分开是为了让 claims 表达式无法访问 user，
	// 反之亦然——认证配置的两类表达式有不同的可用输入。
	varEnvs map[string]*environment.EnvSet
}

// NewDefaultCompiler 返回遵循默认兼容版本的新 Compiler。
// 注意：编译器的构造依赖 feature gate 与兼容版本已初始化。
func NewDefaultCompiler() Compiler {
	return NewCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
}

// NewCompiler 返回基于给定环境集的新 Compiler。
func NewCompiler(env *environment.EnvSet) Compiler {
	return &compiler{
		varEnvs: mustBuildEnvs(env),
	}
}

// CompileClaimsExpression 把 expressionAccessor 编译为可求值的 CEL 程序。
// 表达式中可使用 claims 变量。
func (c compiler) CompileClaimsExpression(expressionAccessor ExpressionAccessor) (CompilationResult, error) {
	return c.compile(expressionAccessor, claimsVarName)
}

// CompileUserExpression 把 expressionAccessor 编译为可求值的 CEL 程序。
// 表达式中可使用 user 变量。
func (c compiler) CompileUserExpression(expressionAccessor ExpressionAccessor) (CompilationResult, error) {
	return c.compile(expressionAccessor, userVarName)
}

// compile 是两类表达式共用的编译流程：选取环境、编译、校验返回类型、生成程序。
func (c compiler) compile(expressionAccessor ExpressionAccessor, envVarName string) (CompilationResult, error) {
	resultError := func(errorString string, errType apiservercel.ErrorType) (CompilationResult, error) {
		return CompilationResult{}, &apiservercel.Error{
			Type:   errType,
			Detail: errorString,
		}
	}

	env, err := c.varEnvs[envVarName].Env(environment.StoredExpressions)
	if err != nil {
		return resultError(fmt.Sprintf("unexpected error loading CEL environment: %v", err), apiservercel.ErrorTypeInternal)
	}

	ast, issues := env.Compile(expressionAccessor.GetExpression())
	if issues != nil {
		return resultError("compilation failed: "+issues.String(), apiservercel.ErrorTypeInvalid)
	}

	// 返回类型必须与调用方声明的一致；cel.AnyType 作为通配，
	// 允许调用方明确表示“任意类型均可”。
	found := false
	returnTypes := expressionAccessor.ReturnTypes()
	for _, returnType := range returnTypes {
		if ast.OutputType() == returnType || cel.AnyType == returnType {
			found = true
			break
		}
	}
	if !found {
		var reason string
		if len(returnTypes) == 1 {
			reason = fmt.Sprintf("must evaluate to %v", returnTypes[0].String())
		} else {
			reason = fmt.Sprintf("must evaluate to one of %v", returnTypes)
		}

		return resultError(reason, apiservercel.ErrorTypeInvalid)
	}

	if _, err = cel.AstToCheckedExpr(ast); err != nil {
		// env.Compile 未报错时理论上不会发生
		return resultError("unexpected compilation error: "+err.Error(), apiservercel.ErrorTypeInternal)
	}
	prog, err := env.Program(ast)
	if err != nil {
		return resultError("program instantiation failed: "+err.Error(), apiservercel.ErrorTypeInternal)
	}

	return CompilationResult{
		Program:            prog,
		AST:                ast,
		ExpressionAccessor: expressionAccessor,
	}, nil
}

// buildUserType 声明 CEL 中的 kubernetes.UserInfo 对象类型。
// 字段均为可选，因为认证配置的 user 表达式允许用 has() 探测字段是否存在。
func buildUserType() *apiservercel.DeclType {
	field := func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField {
		return apiservercel.NewDeclField(name, declType, required, nil, nil)
	}
	fields := func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField {
		result := make(map[string]*apiservercel.DeclField, len(fields))
		for _, f := range fields {
			result[f.Name] = f
		}
		return result
	}

	// groups 与 extra 用 -1 表示无上限的列表/映射大小。
	return apiservercel.NewObjectType("kubernetes.UserInfo", fields(
		field("username", apiservercel.StringType, false),
		field("uid", apiservercel.StringType, false),
		field("groups", apiservercel.NewListType(apiservercel.StringType, -1), false),
		field("extra", apiservercel.NewMapType(apiservercel.StringType, apiservercel.NewListType(apiservercel.StringType, -1), -1), false),
	))
}

// mustBuildEnvs 在基础环境上扩展出 claims 与 user 两套环境。
// 配置错误属于编程错误（而非用户输入错误），因此直接 panic。
func mustBuildEnvs(baseEnv *environment.EnvSet) map[string]*environment.EnvSet {
	buildEnvSet := func(envOpts []cel.EnvOption, declTypes []*apiservercel.DeclType) *environment.EnvSet {
		env, err := baseEnv.Extend(environment.VersionedOptions{
			IntroducedVersion: version.MajorMinor(1, 0),
			EnvOptions:        envOpts,
			DeclTypes:         declTypes,
		})
		if err != nil {
			panic(fmt.Sprintf("environment misconfigured: %v", err))
		}
		return env
	}

	userType := buildUserType()
	// claims 是 claim 名到任意值的映射，因为 JWT claim 的 value 类型不受约束。
	claimsType := apiservercel.NewMapType(apiservercel.StringType, apiservercel.AnyType, -1)

	envs := make(map[string]*environment.EnvSet, 2) // 构建两套环境：claims 与 user
	envs[claimsVarName] = buildEnvSet([]cel.EnvOption{cel.Variable(claimsVarName, claimsType.CelType())}, []*apiservercel.DeclType{claimsType})
	envs[userVarName] = buildEnvSet([]cel.EnvOption{cel.Variable(userVarName, userType.CelType())}, []*apiservercel.DeclType{userType})

	return envs
}
