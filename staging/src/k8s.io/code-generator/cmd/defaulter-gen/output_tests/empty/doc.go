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

// +k8s:defaulter-gen=covers

// This is a test package.
// 该包不定义任何需要默认值处理的类型，仅提供 TypeMeta 等公共内嵌类型，
// 并用 +k8s:defaulter-gen=covers 要求生成器为整个包生成覆盖率桩，
// 以此验证“空包”输入也能正常产出代码。
package empty
