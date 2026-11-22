/*
Copyright 2018 The Kubernetes Authors.

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

// Package fakeclient contains examples on how to use fakeclient in tests.
// Note: This file is here to avoid warnings on go build since there are no
// non-test files in this package.
//
// 本包只包含示例性测试，用来演示如何使用 fake clientset。示例代码在测试之外没有
// 使用者，故保留一个仅含包声明的文件以满足 go build 对“非测试文件”的要求；
// 其中的中文注释同样遵循这一约束，不引入任何可执行代码。
package fakeclient
