/*
Copyright 2014 The Kubernetes Authors.

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

// Package kubectl 提供 k8s.io/kubernetes/cmd 下 kubectl 命令行工具使用的函数。
// 这些函数放在本包中是为了便于单元测试；kubectl 的 main() 只是入口，
// 不应包含任何实际功能。
package kubectl
