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

// Package componenthelpers 本身不承载任何逻辑，只是给 k8s.io/component-helpers/... 这棵
// 目录树一个可导入的 Go 包名，使 go list、依赖校验等工具能把它当作正式的 module 根处理。
// 真正可复用的代码按领域分散在 apps、auth、node、storage 等子包里。
package componenthelpers
