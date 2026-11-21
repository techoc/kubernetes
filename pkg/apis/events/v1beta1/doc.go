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

// +k8s:conversion-gen=k8s.io/kubernetes/pkg/apis/events
// +k8s:conversion-gen-external-types=k8s.io/api/events/v1beta1
// +k8s:defaulter-gen=TypeMeta
// +k8s:defaulter-gen-input=k8s.io/api/events/v1beta1

// v1beta1 已不再提供服务，其遗留校验逻辑与
// 与 v1 不一致，违反了声明式校验框架的要求，
// 用于跨资源所有版本的校验等价性检查。
// +k8s:validation-gen=false

// +groupName=events.k8s.io

package v1beta1
