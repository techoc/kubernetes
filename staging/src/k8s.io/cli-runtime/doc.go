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

// Package cliruntime 汇集构建 Kubernetes 命令行工具所需的通用构件：
// 配置加载、资源解析（resource）、输出格式化（printers）以及可复用的 flag 集合（genericclioptions）。
// 各子包彼此解耦，命令行项目（如 kubectl）按需组合，避免把 CLI 逻辑塞进 client-go。
package cliruntime
