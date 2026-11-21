/*
Copyright 2019 The Kubernetes Authors.

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

// +k8s:conversion-gen=k8s.io/kubernetes/pkg/apis/node
// +k8s:conversion-gen-external-types=k8s.io/api/node/v1alpha1

// 显式禁用默认值生成，以保持既有行为不变。
// 开启后会使用 corev1 的 SetDefaults_ResourceList（defaults.go），
// 它会对 RuntimeClass 的 overhead.podFixed 字段做默认值填充。
// +k8s:defaulter-gen=false

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-input=k8s.io/api/node/v1alpha1

// +groupName=node.k8s.io

package v1alpha1
