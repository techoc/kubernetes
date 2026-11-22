/*
Copyright 2021 The Kubernetes Authors.

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

// Package componentbase 提供 Kubernetes 各控制面组件共用的基础库：
// 命令行 flag 解析、日志、metrics、tracing、feature gate 与版本兼容性处理。
// 各组件通过 staging 目录下的子包引用这些能力，以保持行为一致。
package componentbase
