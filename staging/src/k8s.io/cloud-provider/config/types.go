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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	nodeconfig "k8s.io/cloud-provider/controllers/node/config"
	nodelifecycleconfig "k8s.io/cloud-provider/controllers/nodelifecycle/config"
	serviceconfig "k8s.io/cloud-provider/controllers/service/config"
	cmconfig "k8s.io/controller-manager/config"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CloudControllerManagerConfiguration 汇总描述 cloud-controller manager 的各项配置。
//
// 这是 CCM 的内部（未版本化）配置根对象，由 --config 解析出的
// v1alpha1 版本经转换得到；运行期各控制器只读取这份内部表示。
type CloudControllerManagerConfiguration struct {
	metav1.TypeMeta

	// Generic 保存通用 controller-manager 的配置：leader election、客户端限流、健康检查端口等。
	Generic cmconfig.GenericControllerManagerConfiguration
	// KubeCloudShared 保存 cloud controller manager 与 kube-controller manager 共用的相关配置，
	// 即与 kube-controller-manager 共享的云相关配置（云厂商名、集群名、CIDR 等）。
	KubeCloudShared KubeCloudSharedConfiguration

	// NodeController 保存节点控制器的相关配置。
	NodeController nodeconfig.NodeControllerConfiguration

	// NodeLifecycleController 保存节点生命周期控制器的相关配置。
	NodeLifecycleController nodelifecycleconfig.NodeLifecycleControllerConfiguration

	// ServiceControllerConfiguration 保存 ServiceController 的相关配置。
	ServiceController serviceconfig.ServiceControllerConfiguration

	// NodeStatusUpdateFrequency 是控制器更新节点 status 的频率。
	// 周期性地从云厂商拉取节点地址并回写 Node.Status；过短会放大云 API 调用量。
	NodeStatusUpdateFrequency metav1.Duration

	// Webhook 是 cloud-controller-manager 内置 webhook 的配置。
	Webhook WebhookConfiguration
}

// KubeCloudSharedConfiguration 包含 kube-controller-manager 与 cloud-controller-manager
// 共用、但不属于 genericconfig 的配置项。
//
// 这些字段与 kube-controller-manager 的同名配置保持语义一致，
// 便于同一套云配置在两个组件之间复用。
type KubeCloudSharedConfiguration struct {
	// CloudProviderConfiguration 保存云 provider 的相关配置。
	CloudProvider CloudProviderConfiguration
	// externalCloudVolumePlugin 指定 cloudProvider 为 "external" 时使用的插件；
	// 目前由仓库内的云提供商用它把节点与卷控制放在 KCM 中处理。
	// 当 --cloud-provider=external 时，KCM 仍需要一个 in-tree 插件来接管节点与卷操作。
	ExternalCloudVolumePlugin string
	// useServiceAccountCredentials 表示控制器是否使用各自独立的 service account 凭据运行。
	// 为 true 时各控制器使用自己的 ServiceAccount 而非共享 CCM 身份，便于最小权限。
	UseServiceAccountCredentials bool
	// AllowUntaggedCloud 表示以未打标签的云实例运行。
	// 允许集群包含没有云侧标签的实例；默认要求实例带集群标签以避免误纳管。
	AllowUntaggedCloud bool
	// routeReconciliationPeriod 是对账云 provider 为节点创建的路由的周期。
	// 路由全量对账周期；仅在启用 --configure-cloud-routes 时有意义。
	RouteReconciliationPeriod metav1.Duration
	// clusterName 是集群实例名称的前缀。
	// 云资源名称前缀，同时用于识别哪些资源属于本集群，避免多集群互相干扰。
	ClusterName string
	// clusterCIDR 是集群内 Pod 的 CIDR 范围。
	// 逗号分隔的 Pod CIDR；route controller 用它界定自己负责的路由范围。
	ClusterCIDR string
	// AllocateNodeCIDRs 开启为 Pod 分配 CIDR；若 ConfigureCloudRoutes 为真，
	// 这些 CIDR 还会配置到云 provider 上。
	AllocateNodeCIDRs bool
	// CIDRAllocatorType 决定使用哪种 Pod CIDR 分配器。
	CIDRAllocatorType string
	// configureCloudRoutes 让 allocateNodeCIDRs 分配出的 CIDR 配置到云 provider 上。
	ConfigureCloudRoutes bool
	// nodeSyncPeriod 是从云 provider 同步节点的周期。周期越长调用越少，但新节点加入集群会被延迟。
	NodeSyncPeriod metav1.Duration
}

// CloudProviderConfiguration 包含云 provider 的基本配置项。
type CloudProviderConfiguration struct {
	// Name 是云服务的 provider 名称，对应 --cloud-provider；为空表示不使用任何云提供商。
	Name string
	// cloudConfigFile 是云 provider 配置文件的路径。
	// 各云厂商自定义的配置文件，格式由插件自身解释。
	CloudConfigFile string
}

// WebhookConfiguration 包含 cloud-controller-manager 内置 webhook 的相关配置。
type WebhookConfiguration struct {
	// Webhooks 是待启用或禁用的 webhook 列表
	// '*' 表示“全部默认启用的 webhook”
	// 'foo' 表示“启用名为 foo 的 webhook”
	// '-foo' 表示“禁用名为 foo 的 webhook”
	// 同名条目以第一个出现的为准
	//
	// 解析顺序敏感——同名条目中先出现者生效，因此 '-' 前缀无法覆盖
	// 前面的显式启用；校验阶段会拒绝未知的 webhook 名称。
	Webhooks []string
}
