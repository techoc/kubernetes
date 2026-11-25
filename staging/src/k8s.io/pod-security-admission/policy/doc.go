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

// Package policy 实现 Pod 安全标准检查。
//
// 本包把每条标准拆成可按版本演进的检查项，按等级聚合后产出允许/拒绝结果；
// 具体字段约束写在各检查文件中，新增检查通过 init 注册而非改求值主流程。
package policy
