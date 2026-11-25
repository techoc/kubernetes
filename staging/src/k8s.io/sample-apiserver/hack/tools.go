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

// tools 包导入构建脚本所需的依赖，强制 go mod 将其记录为依赖。
// 本文件带 tools 构建标签，只在构建脚本里生效，不会进入正式二进制。
package tools

// 空白导入让 code-generator 保留在依赖图里，便于生成器可复现地运行。
import _ "k8s.io/code-generator"
