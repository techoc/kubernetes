/*
Copyright 2020 The Kubernetes Authors.

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

// 本包是 exec 凭据插件的 v1alpha1 外部版本，保留下来主要为兼容旧插件。
// 请求结构比 v1 少服务账号 token 与注解字段，由 conversion.go 手写转换丢弃这些字段。
// 指令 +k8s:conversion-gen 指向内部版本，生成函数再由手写转换覆盖。
// 指令 +k8s:deepcopy-gen=package 与 +k8s:defaulter-gen=TypeMeta 分别生成深拷贝与默认值代码。

package v1alpha1
