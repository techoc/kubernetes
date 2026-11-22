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

package webhookadmission

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WebhookAdmission provides configuration for the webhook admission controller.
//
// 该类型是外部配置 API 的一部分，字段名与 JSON 标签一经发布即不可更改。
type WebhookAdmission struct {
	metav1.TypeMeta

	// KubeConfigFile is the path to the kubeconfig file.
	KubeConfigFile string

	// StaticManifestsDir is the path to a directory containing static webhook
	// configurations to be loaded at startup. Files with extensions .yaml,
	// .yml, and .json are read. Only admissionregistration.k8s.io/v1
	// ValidatingWebhookConfiguration and MutatingWebhookConfiguration
	// resources are supported.
	// Using this field requires the ManifestBasedAdmissionControlConfig
	// feature gate to be enabled.
	//
	// 与 KubeConfigFile 不同，这里加载的是完整的 WebhookConfiguration 对象，支持运行时
	// 自动重载；未开启 feature gate 时该字段会被拒绝，以免用户误以为配置已生效。
	// +optional
	StaticManifestsDir string
}
