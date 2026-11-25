/*
Copyright 2022 The Kubernetes Authors.

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

// Package library 提供 Kubernetes 在 CEL 表达式中可用的扩展函数库，如
// 正则、URL、CIDR、IP、quantity、semver 等。各库通过 cel.EnvOption 注入
// CEL 环境，并由 environment 包按兼容版本启用。
package library

import (
	"regexp"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"
)

// Regex provides a CEL function library extension of regex utility functions.
//
// find / findAll
//
// Returns substrings that match the provided regular expression. find returns the first match. findAll may optionally
// be provided a limit. If the limit is set and >= 0, no more than the limit number of matches are returned.
//
//	<string>.find(<string>) <string>
//	<string>.findAll(<string>) <list <string>>
//	<string>.findAll(<string>, <int>) <list <string>>
//
// Examples:
//
//	"abc 123".find('[0-9]*') // returns '123'
//	"abc 123".find('xyz') // returns ''
//	"123 abc 456".findAll('[0-9]*') // returns ['123', '456']
//	"123 abc 456".findAll('[0-9]*', 1) // returns ['123']
//	"123 abc 456".findAll('xyz') // returns []
//
// Regex 返回注入正则库的 CEL 环境选项。
// 提供 find/findAll 两个字符串成员函数。find 返回第一个匹配
// （无匹配返回空串），findAll 可选传入上限，-1 表示不限数量。它们常用于
// 校验策略中提取版本号、端口等模式化字段。
func Regex() cel.EnvOption {
	return cel.Lib(regexLib)
}

// regexLib 是单例；库本身无状态，可安全共享。
var regexLib = &regex{}

// regex 实现 cel.Library 接口；零大小结构体，仅为满足接口方法集。
type regex struct{}

// LibraryName 返回稳定的库名，供兼容性检查与去重使用。
func (*regex) LibraryName() string {
	return "kubernetes.regex"
}

// Types 返回库引入的 CEL 类型；正则库只操作 string，无自定义类型。
func (*regex) Types() []*cel.Type {
	return []*cel.Type{}
}

// declarations 暴露库的函数声明，供兼容性测试等外部检查使用。
func (*regex) declarations() map[string][]cel.FunctionOpt {
	return regexLibraryDecls
}

// regexLibraryDecls 定义函数重载。findAll 提供两参数（不限）与三参数
// （带上限）两个重载，名称相同、参数数量不同。
var regexLibraryDecls = map[string][]cel.FunctionOpt{
	"find": {
		cel.MemberOverload("string_find_string", []*cel.Type{cel.StringType, cel.StringType}, cel.StringType,
			cel.BinaryBinding(find))},
	"findAll": {
		cel.MemberOverload("string_find_all_string", []*cel.Type{cel.StringType, cel.StringType},
			cel.ListType(cel.StringType),
			cel.BinaryBinding(func(str, regex ref.Val) ref.Val {
				// 不限数量时用 -1，与 regexp.FindAllString 的约定一致。
				return findAll(str, regex, types.Int(-1))
			})),
		cel.MemberOverload("string_find_all_string_int",
			[]*cel.Type{cel.StringType, cel.StringType, cel.IntType},
			cel.ListType(cel.StringType),
			cel.FunctionBinding(findAll)),
	},
}

// CompileOptions 把每个函数注册进 CEL 编译环境。
func (*regex) CompileOptions() []cel.EnvOption {
	options := []cel.EnvOption{}
	for name, overloads := range regexLibraryDecls {
		options = append(options, cel.Function(name, overloads...))
	}
	return options
}

// ProgramOptions 启用正则优化：表达式中的正则字面量在程序构建期编译，
// 避免每次求值重复编译，并把非法正则提前暴露为构建错误。
func (*regex) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{
		cel.OptimizeRegex(FindRegexOptimization, FindAllRegexOptimization),
	}
}

// find 是未启用优化时的回退实现：每次调用都编译正则。
// 非法正则返回 CEL 错误而非 Go error，符合 CEL 的运行期错误模型。
func find(strVal ref.Val, regexVal ref.Val) ref.Val {
	str, ok := strVal.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(strVal)
	}
	regex, ok := regexVal.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(regexVal)
	}
	re, err := regexp.Compile(regex)
	if err != nil {
		return types.NewErr("Illegal regex: %v", err.Error())
	}
	result := re.FindString(str)
	return types.String(result)
}

// findAll 是未启用优化时的回退实现，参数个数为 2 或 3；n 为 -1 表示不限。
func findAll(args ...ref.Val) ref.Val {
	argn := len(args)
	if argn < 2 || argn > 3 {
		return types.NoSuchOverloadErr()
	}
	str, ok := args[0].Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(args[0])
	}
	regex, ok := args[1].Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(args[1])
	}
	n := int64(-1)
	if argn == 3 {
		n, ok = args[2].Value().(int64)
		if !ok {
			return types.MaybeNoSuchOverloadErr(args[2])
		}
	}

	re, err := regexp.Compile(regex)
	if err != nil {
		return types.NewErr("Illegal regex: %v", err.Error())
	}

	result := re.FindAllString(str, int(n))

	return types.NewStringList(types.DefaultTypeAdapter, result)
}

// FindRegexOptimization optimizes the 'find' function by compiling the regex pattern and
// reporting any compilation errors at program creation time, and using the compiled regex pattern for all function
// call invocations.
//
// FindRegexOptimization 在程序构建期编译 find 的正则参数（索引 1），此后
// 所有调用复用同一 *regexp.Regexp。正则在表达式源码中通常是常量，因此
// 收益明显，且非法正则会提前在构建期报错。
var FindRegexOptimization = &interpreter.RegexOptimization{
	Function:   "find",
	RegexIndex: 1,
	Factory: func(call interpreter.InterpretableCall, regexPattern string) (interpreter.InterpretableCall, error) {
		compiledRegex, err := regexp.Compile(regexPattern)
		if err != nil {
			return nil, err
		}
		return interpreter.NewCall(call.ID(), call.Function(), call.OverloadID(), call.Args(), func(args ...ref.Val) ref.Val {
			if len(args) != 2 {
				return types.NoSuchOverloadErr()
			}
			in, ok := args[0].Value().(string)
			if !ok {
				return types.MaybeNoSuchOverloadErr(args[0])
			}
			return types.String(compiledRegex.FindString(in))
		}), nil
	},
}

// FindAllRegexOptimization optimizes the 'findAll' function by compiling the regex pattern and
// reporting any compilation errors at program creation time, and using the compiled regex pattern for all function
// call invocations.
//
// FindAllRegexOptimization 与 FindRegexOptimization 同理，额外兼容
// findAll 的带数量上限的三参数形态。
var FindAllRegexOptimization = &interpreter.RegexOptimization{
	Function:   "findAll",
	RegexIndex: 1,
	Factory: func(call interpreter.InterpretableCall, regexPattern string) (interpreter.InterpretableCall, error) {
		compiledRegex, err := regexp.Compile(regexPattern)
		if err != nil {
			return nil, err
		}
		return interpreter.NewCall(call.ID(), call.Function(), call.OverloadID(), call.Args(), func(args ...ref.Val) ref.Val {
			argn := len(args)
			if argn < 2 || argn > 3 {
				return types.NoSuchOverloadErr()
			}
			str, ok := args[0].Value().(string)
			if !ok {
				return types.MaybeNoSuchOverloadErr(args[0])
			}
			n := int64(-1)
			if argn == 3 {
				n, ok = args[2].Value().(int64)
				if !ok {
					return types.MaybeNoSuchOverloadErr(args[2])
				}
			}

			result := compiledRegex.FindAllString(str, int(n))
			return types.NewStringList(types.DefaultTypeAdapter, result)
		}), nil
	},
}
