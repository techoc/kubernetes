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

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WebhookAdmission 提供 webhook 准入控制器的配置。
// 该配置通过 apiserver 的 --admission-control-config-file 加载，
// 决定 webhook 配置从哪里获取。
type WebhookAdmission struct {
	metav1.TypeMeta `json:""`

	// KubeConfigFile 是 kubeconfig 文件的路径。
	KubeConfigFile string `json:"kubeConfigFile"`

	// StaticManifestsDir 是包含静态 webhook 配置的目录路径，这些配置在启动时加载。
	// 会读取扩展名为 .yaml、.yml 与 .json 的文件。仅支持
	// admissionregistration.k8s.io/v1 的 ValidatingWebhookConfiguration 与
	// MutatingWebhookConfiguration 资源。
	// 使用该字段需要启用 ManifestBasedAdmissionControlConfig 特性门控。
	// +optional
	//
	// 静态清单目录在启动时一次性加载，不随文件变化热更新；
	// 启用需开启 ManifestBasedAdmissionControlConfig 特性门控，
	// 且只接受 admissionregistration.k8s.io/v1 的两种 webhook 配置资源。
	StaticManifestsDir string `json:"staticManifestsDir,omitempty"`
}
