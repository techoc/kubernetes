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
// +k8s:conversion-gen-external-types=k8s.io/api/apps/v1beta1

// Package appsv1beta1 是 scale 转换体系的外部版本包（external version），
// 对应 apps/v1beta1 的 Scale 类型。
//
// 它依赖 conversion-gen 生成的转换函数，在对外（wire）版本与内部统一版本
// k8s.io/client-go/scale/scheme 之间来回转换。转换函数由上面的标记生成，
// 手写的注册代码位于 register.go。
package appsv1beta1
