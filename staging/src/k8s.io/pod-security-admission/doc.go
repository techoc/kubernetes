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

// Package podsecurityadmission 仅保留顶层包路径，便于 import 此 staging 模块。
//
// 实际实现已拆分到 policy、admission、api 等子包：policy 负责逐条检查，
// admission 负责接入准入链，api 定义对外契约。
package podsecurityadmission
