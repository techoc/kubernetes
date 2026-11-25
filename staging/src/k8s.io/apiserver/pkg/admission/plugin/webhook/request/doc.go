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

// Package request 基于 admission attributes 创建 admissionReview 请求。
//
// 该包只负责把准入属性（对象、旧对象、用户信息、dry-run 等）编码成
// admission.k8s.io/v1 AdmissionReview 请求体，供 webhook 调用方使用；
// 它不发送网络请求，也不解释 webhook 的响应。
package request
