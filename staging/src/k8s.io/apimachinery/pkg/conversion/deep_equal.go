/*
Copyright 2015 The Kubernetes Authors.

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

package conversion

import (
	"k8s.io/apimachinery/third_party/forked/golang/reflect"
)

// 该类型的实现必须位于 third_party，因为它是从 Go 标准库 fork 出来的代码。
// 为方便调用方，这里再暴露一次该类型。
type Equalities struct {
	reflect.Equalities
}

// EqualitiesOrDie 为方便使用而封装构造过程，出错时直接 panic。
// 调用方传入的通常是包级静态定义的比较函数，失败属于编程错误而非运行时状况，
// 因此在初始化阶段 panic 比返回错误更合适。
func EqualitiesOrDie(funcs ...interface{}) Equalities {
	e := Equalities{reflect.Equalities{}}
	if err := e.AddFuncs(funcs...); err != nil {
		panic(err)
	}
	return e
}

// Copy 执行 equalities map 的浅拷贝。
//
// 拷贝出的 map 与原始 map 相互独立，因此在转换器上按需扩展比较函数时
// 不会污染共享的全局实例；map 中的函数值本身不可变，无需深拷贝。
func (e Equalities) Copy() Equalities {
	result := Equalities{reflect.Equalities{}}

	for key, value := range e.Equalities {
		result.Equalities[key] = value
	}

	return result
}
