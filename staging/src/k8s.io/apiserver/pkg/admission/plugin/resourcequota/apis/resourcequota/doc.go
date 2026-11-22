/*
Copyright 2016 The Kubernetes Authors.

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

// Package resourcequota 定义 ResourceQuota 准入控制器的配置 API（内部版本）。
// 该配置用于声明“默认受限资源”：资源一旦出现在 LimitedResources 中，
// 若没有配额覆盖它，创建请求就会被拒绝，从而防止绕过配额。
package resourcequota
