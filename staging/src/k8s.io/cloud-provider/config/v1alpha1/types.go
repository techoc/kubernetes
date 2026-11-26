/*
Copyright 2018 The Kubernetes Authors.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	nodeconfigv1alpha1 "k8s.io/cloud-provider/controllers/node/config/v1alpha1"
	nodelifecycleconfigv1alpha1 "k8s.io/cloud-provider/controllers/nodelifecycle/config/v1alpha1"
	serviceconfigv1alpha1 "k8s.io/cloud-provider/controllers/service/config/v1alpha1"
	cmconfigv1alpha1 "k8s.io/controller-manager/config/v1alpha1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CloudControllerManagerConfiguration 汇总描述 cloud-controller manager 的各项配置。
//
// 这是 v1alpha1 外部配置根对象，仅用于 --config 解析与 /configz 序列化；
// 默认值在 defaults.go 中补齐，随后通过生成的转换函数转为内部 config 类型。
type CloudControllerManagerConfiguration struct {
	metav1.TypeMeta `json:""`

	// Generic 保存通用 controller-manager 的配置。
	Generic cmconfigv1alpha1.GenericControllerManagerConfiguration
	// KubeCloudSharedConfiguration 保存 cloud controller manager 与 kube-controller manager 共用的配置。
	KubeCloudShared KubeCloudSharedConfiguration
	// NodeController 保存节点控制器的相关配置。
	NodeController nodeconfigv1alpha1.NodeControllerConfiguration
	// NodeLifecycleController 保存节点生命周期控制器的相关配置。
	NodeLifecycleController nodelifecycleconfigv1alpha1.NodeLifecycleControllerConfiguration
	// ServiceControllerConfiguration 保存 ServiceController 的相关配置。
	ServiceController serviceconfigv1alpha1.ServiceControllerConfiguration
	// NodeStatusUpdateFrequency 是控制器更新节点 status 的频率。
	NodeStatusUpdateFrequency metav1.Duration
	// Webhook 是 cloud-controller-manager 内置 webhook 的配置。
	Webhook WebhookConfiguration
}

// KubeCloudSharedConfiguration 包含 kube-controller-manager 与 cloud-controller-manager
// 共用、但不属于 genericconfig 的配置项。
type KubeCloudSharedConfiguration struct {
	// CloudProviderConfiguration 保存云 provider 的相关配置。
	CloudProvider CloudProviderConfiguration
	// externalCloudVolumePlugin 指定 cloudProvider 为 "external" 时使用的插件；
	// 目前由仓库内的云提供商用它让 KCM 处理节点与卷控制。
	ExternalCloudVolumePlugin string
	// useServiceAccountCredentials 表示控制器是否用各自独立的 service account 凭据运行。
	UseServiceAccountCredentials bool
	// 允许使用未打标签的云实例运行。
	AllowUntaggedCloud bool
	// routeReconciliationPeriod 是对账云 provider 为节点创建的路由的周期。
	RouteReconciliationPeriod metav1.Duration
	// clusterName 是集群实例名的前缀，也是云资源归属的标识。
	ClusterName string
	// clusterCIDR 是集群中 Pod 的 CIDR 范围。
	ClusterCIDR string
	// AllocateNodeCIDRs 开启为 Pod 分配 CIDR；若 ConfigureCloudRoutes 为真，
	// 这些 CIDR 还会配置到云 provider 上。
	AllocateNodeCIDRs bool
	// CIDRAllocatorType 决定使用哪种 Pod CIDR 分配器。
	CIDRAllocatorType string
	// ConfigureCloudRoutes 会把通过 allocateNodeCIDRs 分配出来的 CIDR 配置到云提供商上。
	//
	// 使用指针以区分“未设置”和“显式设为 false”——默认值需要覆盖前者，
	// 但不能抹掉用户明确关闭的选择。
	ConfigureCloudRoutes *bool
	// nodeSyncPeriod 是从云 provider 同步节点的周期。周期越长调用越少，但新节点加入集群会被延迟。
	NodeSyncPeriod metav1.Duration
}

// CloudProviderConfiguration 包含云 provider 的基本配置项。
type CloudProviderConfiguration struct {
	// Name 是云服务的 provider 名称。
	Name string
	// cloudConfigFile 是云 provider 配置文件的路径。
	CloudConfigFile string
}

// WebhookConfiguration 包含 cloud-controller-manager 内置 webhook 的相关配置。
type WebhookConfiguration struct {
	// Webhooks 是待启用或禁用的 webhook 列表：
	// '*' 表示“所有默认启用的 webhook”
	// 'foo' 表示“启用 foo”
	// '-foo' 表示“禁用 foo”
	// 同名条目以第一个为准。
	Webhooks []string
}
