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

// +k8s:deepcopy-gen=package
// +k8s:conversion-gen=k8s.io/kubelet/pkg/apis/credentialprovider
// +k8s:defaulter-gen=TypeMeta
// +groupName=credentialprovider.kubelet.k8s.io

// 本包是镜像凭证插件 API 的稳定版本，通过 stdin/stdout 与 exec 插件交换 JSON。
// 上面的指令分别驱动 deepcopy/conversion/defaulter 代码生成与 API 组名标注。
package v1
