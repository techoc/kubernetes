/*
Copyright 2019 The Kubernetes Authors.

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

package fuzzer

import (
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
)

// Funcs 返回 apiserverinternal API 组的 fuzzer 函数列表，供 round-trip 测试注册使用。
// 当前返回空切片：该组的类型没有需要特殊处理的字段（如指针默认值或枚举），
// 完全交给通用反射 fuzzer 即可；保留此入口是为了让 fuzzer 注册逻辑保持统一。
func Funcs(codecs runtimeserializer.CodecFactory) []interface{} {
	// 没有自定义 fuzzer：返回值留空，保持与生成代码的约定一致。
	return []interface{}{}
}
