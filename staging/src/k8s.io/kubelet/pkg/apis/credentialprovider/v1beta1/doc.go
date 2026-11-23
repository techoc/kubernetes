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

// 本文件仅为生成器提供包级指令：本包是 exec 凭据插件 API 的 v1beta1 版本，
// 通过指令转换到内部版本 credentialprovider.kubelet.k8s.io，API 对象定义在同包 types.go。
package v1beta1
