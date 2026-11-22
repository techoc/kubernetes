/*
Copyright 2025 The Kubernetes Authors.

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

// Package constraints 提供泛型约束，用于限定校验函数可以接受的整数类型。
//
// 这些约束使用 ~ 类型近似，因此允许传入自定义底层类型（如 type MyInt int），
// 而不会被限制在标准库的具名类型上，从而让校验逻辑可以复用在不同来源的整数上。
package constraints

// Signed 约束允许任意有符号整数类型。
type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

// Unsigned 约束允许任意无符号整数类型。
// uintptr 被包含在内，是因为它同样承载非负的整数语义，用于地址计数等场景。
type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// Integer 约束允许任意整数类型，即有符号与无符号的并集。
//
// 声明为 Signed 与 Unsigned 的联合而非展开全部类型，可以避免在新增整数类型时漏改约束。
type Integer interface {
	Signed | Unsigned
}
