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

package v1alpha3

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bootstraptokenv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/bootstraptoken/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BootstrapToken 表示 kubeadm 生成的引导令牌输出信息。
type BootstrapToken struct {
	metav1.TypeMeta `json:""`

	bootstraptokenv1.BootstrapToken
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Images 表示 `kubeadm config images list` 生成的输出信息。
type Images struct {
	metav1.TypeMeta `json:""`

	// Images 列出命令解析出的容器镜像引用。
	Images []string `json:"images"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ComponentUpgradePlan 表示单个组件的升级计划信息。
type ComponentUpgradePlan struct {
	metav1.TypeMeta `json:""`

	// Name 是组件名称。
	Name string `json:"name"`
	// CurrentVersion 是节点当前使用的组件版本。
	CurrentVersion string `json:"currentVersion"`
	// NewVersion 是本次升级的目标版本。
	NewVersion string `json:"newVersion"`
	// NodeName 标识组件所在节点；无节点归属的组件留空。
	NodeName string `json:"nodeName,omitempty"`
}

// ComponentConfigVersionState 描述组件配置的当前版本和目标版本。
type ComponentConfigVersionState struct {
	// Group 指向涵盖该配置的 Kubernetes API 组。
	Group string `json:"group"`

	// CurrentVersion 是当前生效的组件配置版本。
	// 注意：若集群中不存在该配置，或其版本不是受支持的 kubeadm 版本生成的，此字段可以为空。
	CurrentVersion string `json:"currentVersion"`

	// PreferredVersion 是 kubeadm 当前推荐使用的组件配置版本。
	// 注意：目前这是 kubeadm 唯一支持的版本。
	PreferredVersion string `json:"preferredVersion"`

	// ManualUpgradeRequired 表示用户是否需要手动升级组件配置版本。
	// 当配置的 CurrentVersion 由用户提供或修改且不再受支持时，需要手动升级。
	// 用户应将组件配置升级到 PreferredVersion 或其他受支持的版本。
	ManualUpgradeRequired bool `json:"manualUpgradeRequired"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AvailableUpgrade 表示一个可用的升级选项。
type AvailableUpgrade struct {
	metav1.TypeMeta `json:""`

	// Description 描述升级选项的版本类别或所处版本系列。
	Description string `json:"description"`

	// Components 列出该升级选项涉及的组件及版本变更。
	Components []ComponentUpgradePlan `json:"components"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// UpgradePlan 表示 `kubeadm upgrade plan` 生成的升级计划信息。
type UpgradePlan struct {
	metav1.TypeMeta `json:""`

	// AvailableUpgrades 列出可升级到的目标版本系列及各组件的版本变更。
	AvailableUpgrades []AvailableUpgrade `json:"availableUpgrades,omitempty"`

	// ConfigVersions 列出组件配置版本的状态，供升级检查使用。
	ConfigVersions []ComponentConfigVersionState `json:"configVersions,omitempty"`
}

// Certificate 表示使用 check-expiration 命令时显示的证书或证书颁发机构的信息。
type Certificate struct {
	// Name 是证书名称。
	Name string `json:"name"`

	// ExpirationDate 是采用 RFC3339 格式的 UTC 证书到期时间。
	ExpirationDate metav1.Time `json:"expirationDate"`

	// ResidualTimeSeconds 是距离证书到期剩余时间的秒数。
	ResidualTimeSeconds int64 `json:"residualTime"`

	// ExternallyManaged 表示证书是否由外部管理。
	ExternallyManaged bool `json:"externallyManaged"`

	// CAName 是签发该证书的证书颁发机构（CA）名称。
	// 自签名根证书的此字段为空。
	CAName string `json:"caName,omitempty"`

	// Missing 表示证书是否缺失。
	Missing bool `json:"missing"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CertificateExpirationInfo 表示 `kubeadm certs check-expiration` 生成的输出信息。
type CertificateExpirationInfo struct {
	metav1.TypeMeta `json:""`

	// Certificates 列出需要显示到期信息的证书。
	Certificates []Certificate `json:"certificates"`

	// CertificateAuthorities 列出需要显示到期信息的证书颁发机构。
	CertificateAuthorities []Certificate `json:"certificateAuthorities"`
}
