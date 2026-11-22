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
// +k8s:conversion-gen-external-types=k8s.io/api/autoscaling/v1

// Package autoscalingv1 注册 scale 内部类型到 autoscaling/v1 的版本信息与转换函数。
//
// 该包本身不定义类型，只为 scale 子资源客户端提供 GroupVersion/Resource
// 辅助方法，并作为 conversion-gen 生成的转换代码的挂载点。
package autoscalingv1
