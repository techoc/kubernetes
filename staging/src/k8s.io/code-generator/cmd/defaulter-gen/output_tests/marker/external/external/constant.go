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

// Package external 是 defaulter-gen 的输出测试夹具：它与另一目录下的同名包
// 一起用来验证“多个同名包”时的导入别名与类型引用是否正确。
package external

// AnotherConstant 用于制造与同名包的符号冲突，测试生成代码能否正确区分。
// Used for test with multiple packages of the same name
const AnotherConstant string = "AnotherConstantString"
