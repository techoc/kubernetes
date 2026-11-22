/*
Copyright 2015 The Kubernetes Authors.

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

// +k8s:conversion-gen=k8s.io/client-go/tools/clientcmd/api
// +k8s:deepcopy-gen=package
// +k8s:defaulter-gen=Kind
// +k8s:validation-gen=false

// Package v1 定义 kubeconfig 文件的外部版本（v1）类型。
//
// 该版本是写入磁盘与对外交换的稳定格式；内部表示见
// k8s.io/client-go/tools/clientcmd/api。两者之间的转换由生成的
// zz_generated.conversion.go 完成，因此新增字段时必须同时更新两处。
package v1
