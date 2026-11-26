//go:build go1.18

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

package transport

// 只为让 "unused" 检查通过。
// 同时触发下方的编译期严格可比较断言。
var _ = isCacheKeyComparable[tlsCacheKey]

// 在编译期断言 tlsCacheKey 的比较绝不会在运行时 panic。
//
// Go 1.20 放宽了类型约束：只要「可比较」即可满足 `comparable` 约束，并不要求严格可比较，
// 于是接口也能满足该约束。但按定义，"两个动态类型相同的接口值比较时，若该类型不可比较就会 panic"。
//
// 为确保比较两个 tlsCacheKey 不会 panic，这里强制它严格可比较，从而排除包含接口字段的可能。
// 做法是利用另一条规则："类型参数严格可比较时才可比较"。先从 tlsCacheKey 构造类型参数，
// 再把它用于 comparable 检查，以此验证严格可比较性。
//
// 思路来自 https://github.com/golang/go/issues/56548#issuecomment-1317673963
func isCacheKeyComparable[K tlsCacheKey]() {
	// 若 tlsCacheKey 含 interface 等非严格可比较字段，此处无法满足 comparable 约束。
	_ = isComparable[K]
}

// isComparable 仅用于在编译期约束 T 必须是严格可比较类型。
func isComparable[T comparable]() {}
