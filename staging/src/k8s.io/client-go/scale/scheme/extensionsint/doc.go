/*
Copyright 2017 The Kubernetes Authors.

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

// Package extensionsint 提供转换逻辑所需的内部版本脚手架，自身不定义类型，
// 只是为了让 runtime.Scheme.ConvertToVersion 等方法按预期工作。
//
// Package extensionsint 只提供 extensions 内部版本在转换逻辑中的脚手架，
// 自身不定义类型。这样 runtime.Scheme.ConvertToVersion 才能在
// extensions/v1beta1 与内部版本之间找到正确的转换路径。
package extensionsint
