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

// Package v1 是 example2 组的 lister 示例包。
// 手写的 *_expansion.go 用于给生成的 lister 增加自定义方法，
// 从而在不修改生成代码的前提下扩展行为。
package v1

// TestTypeListerExpansion 允许向 TestTypeLister 添加自定义方法。
// TestTypeLister.
type TestTypeListerExpansion interface {
	CheckExpansionSupport()
}

// TestTypeNamespaceListerExpansion 允许向 TestTypeNamespaceLister 添加自定义方法。
// TestTypeNamespaceLister.
type TestTypeNamespaceListerExpansion interface{}

// CheckExpansionSupport 是一个空实现，用来证明 lister-gen 生成的类型
// 会内嵌对应的 Expansion 接口，手写方法因此可用。
func (*testTypeLister) CheckExpansionSupport() {
	// 用于验证 lister-gen 正确支持接口扩展
}
