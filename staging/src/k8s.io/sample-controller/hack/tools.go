//go:build tools

/*
Copyright 2019 The Kubernetes Authors.

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

// This package imports things required by build scripts, to force `go mod` to see them as dependencies
//
// 本文件用 tools 构建标签隔离，正常编译不会引入 code-generator；
// 这样它只作为依赖声明存在，不进入运行时的二进制。
// 空导入是刻意的：只为让 go mod 记录依赖，不使用包里的任何符号。
package tools

import _ "k8s.io/code-generator"
