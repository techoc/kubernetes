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

// +k8s:deepcopy-gen=package
// +k8s:defaulter-gen=TypeMeta
// +groupName=conflicting.test.crd.code-generator.k8s.io
// +groupGoName=ConflictingExample

// Package v1 是 CRD 示例中的冲突测试版本：包名 v1 与 group 名不一致的
// 场景用于验证生成的注册/默认值代码按 +groupName 而非目录名分组。
package v1
