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

package cache

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// nameForHandler 为 ResourceEventHandler 生成稳定的可读名称，用于日志与
// 指标标签。名称只依赖类型与函数位置，不依赖实例，因此可安全用作低基数标签。
func nameForHandler(handler ResourceEventHandler) (name string) {
	defer func() {
		// Last resort: let Sprintf handle it.
		if name == "" {
			name = fmt.Sprintf("%T", handler)
		}
	}()

	if handler == nil {
		return ""
	}
	// 函数回调式 handler 没有有意义的类型名，改用函数所在位置描述。
	switch handler := handler.(type) {
	case *ResourceEventHandlerFuncs:
		return nameForHandlerFuncs(*handler)
	case ResourceEventHandlerFuncs:
		return nameForHandlerFuncs(handler)
	default:
		// We can use the fully qualified name of whatever
		// provides the interface. We don't care whether
		// it contains fields or methods which provide
		// the interface methods.
		value := reflect.ValueOf(handler)
		if value.Type().Kind() == reflect.Interface {
			// Probably not needed, but let's play it safe.
			value = value.Elem()
		}
		// 解引用指针以获得底层类型名，避免同一类型因指针/值接收者得到不同名字。
		if value.Type().Kind() == reflect.Pointer {
			if !value.IsNil() {
				value = value.Elem()
			}
		}
		name := value.Type().PkgPath()
		if name != "" {
			name += "."
		}
		if typeName := value.Type().Name(); typeName != "" {
			name += typeName
		}
		return name
	}
}

// nameForHandlerFuncs 由三个回调函数名推导 handler 名。
func nameForHandlerFuncs(funcs ResourceEventHandlerFuncs) string {
	return nameForFunctions(funcs.AddFunc, funcs.UpdateFunc, funcs.DeleteFunc)
}

// nameForFunctions 尝试用一个公共限定符（如包名或接收者类型）概括一组函数；
// 若函数来自不同位置，则退化为用 "+" 连接全部函数名，保证名称唯一且可诊断。
func nameForFunctions(fs ...any) string {
	// If all functions are defined in the same place, then we
	// don't care about the actual function name in
	// e.g. "main.FuncName" or "main.(*Foo).FuncName-fm", instead
	// we use the common qualifier.
	//
	// But we don't know that yet, so we also collect all names.
	var qualifier string
	singleQualifier := true
	var names []string
	for _, f := range fs {
		if f == nil {
			continue
		}
		name := nameForFunction(f)
		if name == "" {
			continue
		}
		names = append(names, name)

		// 限定符取最后一个 '.' 之前的部分，即 "main" 或 "pkg.Type"。
		newQualifier := name
		index := strings.LastIndexByte(newQualifier, '.')
		if index > 0 {
			newQualifier = newQualifier[:index]
		}
		switch qualifier {
		case "":
			qualifier = newQualifier
		case newQualifier:
			// So far, so good...
		default:
			// Nope, different.
			singleQualifier = false
		}
	}

	if singleQualifier {
		return qualifier
	}

	return strings.Join(names, "+")
}

// nameForFunction 返回函数的完全限定名；PC 无法解析（如某些运行时生成的
// 函数）时返回空串，由调用方决定回退策略。
func nameForFunction(f any) string {
	fn := runtime.FuncForPC(reflect.ValueOf(f).Pointer())
	if fn == nil {
		return ""
	}
	return fn.Name()
}
