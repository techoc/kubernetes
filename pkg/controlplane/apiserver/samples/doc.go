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

// Package samples 包含两个类似 kube 的通用控制面 apiserver：一个带 CRD（generic），一个不带（minimum）。
//
// 它们主要用于验证构建这类控制面的可行性。未来或许会将其推广为供第三方参考的示例。
package samples
