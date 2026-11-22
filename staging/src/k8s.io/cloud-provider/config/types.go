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

// CloudControllerManagerConfiguration contains elements describing cloud-controller manager.
//
// 中文说明：这是 CCM 的内部（未版本化）配置根对象，由 --config 解析出的
// v1alpha1 版本经转换得到；运行期各控制器只读取这份内部表示。
type CloudControllerManagerConfiguration struct {
	metav1.TypeMeta

	// Generic holds configuration for a generic controller-manager
	// 通用 controller-manager 配置：leader election、客户端限流、健康检查端口等。
	Generic cmconfig.GenericControllerManagerConfiguration
	// KubeCloudSharedConfiguration holds configuration for shared related features
	// both in cloud controller manager and kube-controller manager.
	// 与 kube-controller-manager 共用的云相关配置（云厂商名、集群名、CIDR 等）。
	KubeCloudShared KubeCloudSharedConfiguration

	// NodeController holds configuration for node controller
	// related features.
	NodeController nodeconfig.NodeControllerConfiguration

	// NodeLifecycleController holds configuration for node lifecycle controller
	// related features.
	NodeLifecycleController nodelifecycleconfig.NodeLifecycleControllerConfiguration

	// ServiceControllerConfiguration holds configuration for ServiceController
	// related features.
	ServiceController serviceconfig.ServiceControllerConfiguration

	// NodeStatusUpdateFrequency is the frequency at which the controller updates nodes' status
	// 周期性地从云厂商拉取节点地址并回写 Node.Status；过短会放大云 API 调用量。
	NodeStatusUpdateFrequency metav1.Duration

	// Webhook is the configuration for cloud-controller-manager hosted webhooks
	Webhook WebhookConfiguration
}

// KubeCloudSharedConfiguration contains elements shared by both kube-controller manager
// and cloud-controller manager, but not genericconfig.
//
// 中文说明：这些字段与 kube-controller-manager 的同名配置保持语义一致，
// 便于同一套云配置在两个组件之间复用。
type KubeCloudSharedConfiguration struct {
	// CloudProviderConfiguration holds configuration for CloudProvider related features.
	CloudProvider CloudProviderConfiguration
	// externalCloudVolumePlugin specifies the plugin to use when cloudProvider is "external".
	// It is currently used by the in repo cloud providers to handle node and volume control in the KCM.
	// 当 --cloud-provider=external 时，KCM 仍需要一个 in-tree 插件来接管节点与卷操作。
	ExternalCloudVolumePlugin string
	// useServiceAccountCredentials indicates whether controllers should be run with
	// individual service account credentials.
	// 为 true 时各控制器使用自己的 ServiceAccount 而非共享 CCM 身份，便于最小权限。
	UseServiceAccountCredentials bool
	// run with untagged cloud instances
	// 允许集群包含没有云侧标签的实例；默认要求实例带集群标签以避免误纳管。
	AllowUntaggedCloud bool
	// routeReconciliationPeriod is the period for reconciling routes created for Nodes by cloud provider..
	// 路由全量对账周期；仅在启用 --configure-cloud-routes 时有意义。
	RouteReconciliationPeriod metav1.Duration
	// clusterName is the instance prefix for the cluster.
	// 云资源名称前缀，同时用于识别哪些资源属于本集群，避免多集群互相干扰。
	ClusterName string
	// clusterCIDR is CIDR Range for Pods in cluster.
	// 逗号分隔的 Pod CIDR；route controller 用它界定自己负责的路由范围。
	ClusterCIDR string
	// AllocateNodeCIDRs enables CIDRs for Pods to be allocated and, if
	// ConfigureCloudRoutes is true, to be set on the cloud provider.
	AllocateNodeCIDRs bool
	// CIDRAllocatorType determines what kind of pod CIDR allocator will be used.
	CIDRAllocatorType string
	// configureCloudRoutes enables CIDRs allocated with allocateNodeCIDRs
	// to be configured on the cloud provider.
	ConfigureCloudRoutes bool
	// nodeSyncPeriod is the period for syncing nodes from cloudprovider. Longer
	// periods will result in fewer calls to cloud provider, but may delay addition
	// of new nodes to cluster.
	NodeSyncPeriod metav1.Duration
}

// CloudProviderConfiguration contains basically elements about cloud provider.
type CloudProviderConfiguration struct {
	// Name is the provider for cloud services.
	// 对应 --cloud-provider；为空表示不使用任何云提供商。
	Name string
	// cloudConfigFile is the path to the cloud provider configuration file.
	// 各云厂商自定义的配置文件，格式由插件自身解释。
	CloudConfigFile string
}

// WebhookConfiguration contains configuration related to
// cloud-controller-manager hosted webhooks
type WebhookConfiguration struct {
	// Webhooks is the list of webhooks to enable or disable
	// '*' means "all enabled by default webhooks"
	// 'foo' means "enable 'foo'"
	// '-foo' means "disable 'foo'"
	// first item for a particular name wins
	//
	// 中文说明：解析顺序敏感——同名条目中先出现者生效，因此 '-' 前缀无法覆盖
	// 前面的显式启用；校验阶段会拒绝未知的 webhook 名称。
	Webhooks []string
}
