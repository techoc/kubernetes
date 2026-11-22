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

// +k8s:conversion-gen=k8s.io/client-go/scale/scheme
// +k8s:conversion-gen-external-types=k8s.io/api/extensions/v1beta1

// Package extensionsv1beta1 为 extensions/v1beta1 的 Scale 类型注册转换与
// 序列化入口，使旧版 API 组上的 scale 子资源可以复用统一的 scale 客户端。
//
// 上面的 conversion-gen 指令驱动生成代码，请勿手工修改生成结果。
package extensionsv1beta1
