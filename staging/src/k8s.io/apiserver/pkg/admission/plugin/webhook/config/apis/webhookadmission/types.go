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

// WebhookAdmission 提供 webhook 准入控制器的配置。
//
// 该类型是外部配置 API 的一部分，字段名与 JSON 标签一经发布即不可更改。
type WebhookAdmission struct {
	metav1.TypeMeta

	// KubeConfigFile 是 kubeconfig 文件的路径。
	KubeConfigFile string

	// StaticManifestsDir 是包含静态 webhook 配置的目录路径，这些配置在启动时加载。
	// 会读取扩展名为 .yaml、.yml 与 .json 的文件。仅支持
	// admissionregistration.k8s.io/v1 的 ValidatingWebhookConfiguration 与
	// MutatingWebhookConfiguration 资源。
	// 使用该字段需要启用 ManifestBasedAdmissionControlConfig 特性门控。
	//
	// 与 KubeConfigFile 不同，这里加载的是完整的 WebhookConfiguration 对象，支持运行时
	// 自动重载；未开启 feature gate 时该字段会被拒绝，以免用户误以为配置已生效。
	// +optional
	StaticManifestsDir string
}
