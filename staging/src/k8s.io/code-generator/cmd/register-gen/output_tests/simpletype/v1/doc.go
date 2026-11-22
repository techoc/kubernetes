/*
Copyright 2025 The Kubernetes Authors.

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

// +k8s:register-gen=simpletype
// +k8s:deepcopy-gen=false

// Package v1 是 register-gen 的测试包。
// 指令 +k8s:register-gen=simpletype 让生成器只处理名为 simpletype 的 group，
// 指令 +k8s:deepcopy-gen=false 则关闭本包的 DeepCopy 生成，以验证两种生成器互不干扰。
package v1
