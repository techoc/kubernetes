/*
Copyright 2024 The Kubernetes Authors.

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

// +groupName=output.kubeadm.k8s.io
// +k8s:deepcopy-gen=package
// +k8s:conversion-gen=k8s.io/kubernetes/cmd/kubeadm/app/apis/output

// Package v1alpha3 定义 kubeadm 结构化输出的 v1alpha3 版本数据结构。
//
// 自 v1alpha2 以来的变更：
//   - 支持通过 CertificateExpirationInfo 输出 "kubeadm certs check-expiration" 的证书到期信息。
//   - "kubeadm upgrade plan" 使用的 UpgradePlan 结构发生不兼容变更：现包含 AvailableUpgrade 列表。
package v1alpha3
