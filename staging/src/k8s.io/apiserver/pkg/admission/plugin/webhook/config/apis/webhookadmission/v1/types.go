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

// WebhookAdmission provides configuration for the webhook admission controller.
// 该配置通过 apiserver 的 --admission-control-config-file 加载，
// 决定 webhook 配置从哪里获取。
type WebhookAdmission struct {
	metav1.TypeMeta `json:""`

	// KubeConfigFile is the path to the kubeconfig file.
	// 指向 kubeconfig 的路径；该 kubeconfig 中的集群地址即 webhook 配置的来源。
	KubeConfigFile string `json:"kubeConfigFile"`

	// StaticManifestsDir is the path to a directory containing static webhook
	// configurations to be loaded at startup. Files with extensions .yaml,
	// .yml, and .json are read. Only admissionregistration.k8s.io/v1
	// ValidatingWebhookConfiguration and MutatingWebhookConfiguration
	// resources are supported.
	// Using this field requires the ManifestBasedAdmissionControlConfig
	// feature gate to be enabled.
	// +optional
	//
	// 中文说明：静态清单目录在启动时一次性加载，不随文件变化热更新；
	// 启用需开启 ManifestBasedAdmissionControlConfig 特性门控，
	// 且只接受 admissionregistration.k8s.io/v1 的两种 webhook 配置资源。
	StaticManifestsDir string `json:"staticManifestsDir,omitempty"`
}
