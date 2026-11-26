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

// 本包导入构建脚本所需的工具，让 `go mod` 把它们视为依赖。
// 这个包配合 tools build tag 使用，正常构建时会被排除。
// 之所以只做空导入，是为了让 go mod 把这些仅由构建脚本使用的工具也纳入依赖管理，
// 避免它们在执行 make update 等目标时因不在 go.mod 中而被剔除。
package tools

import _ "k8s.io/code-generator"
