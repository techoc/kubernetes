/*
Copyright 2017 The Kubernetes Authors.

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
// +groupName=custom.metrics.k8s.io

// Package custom_metrics 定义使用自定义指标的 API。
// 本包是自定义指标的内部版本，作为各外部版本之间转换的中枢。
// 代码生成指令集中放在本文件，避免散落到类型定义处。
package custom_metrics
