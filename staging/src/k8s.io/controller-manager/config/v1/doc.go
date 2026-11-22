/*
Copyright 2022 The Kubernetes Authors.

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

// v1 是领导者迁移配置的稳定外部版本。
// 该包只暴露 LeaderMigrationConfiguration；ResourceLock 字段在 v1 中固定为 leases，
// 转换到内部版本时由 conversion.go 自动补齐。
// +k8s:deepcopy-gen=package
// +groupName=controllermanager.config.k8s.io

package v1
