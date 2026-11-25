/*
Copyright 2014 The Kubernetes Authors.

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

package cloudprovider

import (
	"context"
	"errors"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

// ControllerClientBuilder 允许为各控制器获取 client 与 config。
// 注意 pkg/controller/client_builder.go 中也有一份拷贝；TODO: 使其依赖独立的 controller utilities 仓库（issues/68947）。
// 控制器按名称获取独立客户端，便于为不同控制器分配不同 ServiceAccount。
type ControllerClientBuilder interface {
	// Config 返回指定控制器使用的 REST 配置，可能因 ServiceAccount 不同而不同。
	Config(name string) (*restclient.Config, error)
	// ConfigOrDie 与 Config 相同，但在出错时直接终止进程，适用于启动阶段。
	ConfigOrDie(name string) *restclient.Config
	// Client 返回指定控制器的 clientset。
	Client(name string) (clientset.Interface, error)
	// ClientOrDie 与 Client 相同，但在出错时直接终止进程。
	ClientOrDie(name string) clientset.Interface
}

// Interface 是云 provider 的抽象、可插拔接口，也是 CCM 与云实现之间的唯一契约：各能力接口通过返回 (impl, supported) 可选地暴露，
// 控制器据此决定是否启用对应控制循环。所有实现必须并发安全，因为控制器 worker 会并行调用。
type Interface interface {
	// Initialize 为 cloud provider 提供 kubernetes client builder，并可启动用于日常维护或云厂商自定义控制器的 goroutine；这些任务必须在 stop channel 关闭时被清理。
	// 该调用在所有控制器启动之前执行一次；厂商可在此启动自有控制器，但必须监听 stop。
	Initialize(clientBuilder ControllerClientBuilder, stop <-chan struct{})
	// LoadBalancer returns a balancer interface. Also returns true if the interface is supported, false otherwise.
	LoadBalancer() (LoadBalancer, bool)
	// Instances returns an instances interface. Also returns true if the interface is supported, false otherwise.
	Instances() (Instances, bool)
	// InstancesV2 是 instances 的实现，只应由外部云 provider 实现。其行为与 Instances 相同，但经过优化以显著减少注册/同步节点时对云的 API 调用；实现该接口后将不再调用 Zones。
	// 若返回 supported=true，CCM 会优先使用 InstancesV2 并停止调用 Instances/Zones，
	// 因此实现方必须同时提供元数据中的 zone/region 信息。
	InstancesV2() (InstancesV2, bool)
	// Zones 返回一个 zones 接口；若支持该接口也返回 true，否则返回 false。
	// DEPRECATED: Zones is deprecated in favor of retrieving zone/region information from InstancesV2.
	// 启用 InstancesV2 后不会再调用该接口；新实现应通过 InstanceMetadata 返回拓扑。
	Zones() (Zones, bool)
	// Clusters returns a clusters interface.  Also returns true if the interface is supported, false otherwise.
	Clusters() (Clusters, bool)
	// Routes returns a routes interface along with whether the interface is supported.
	Routes() (Routes, bool)
	// ProviderName 返回云 provider 的 ID。
	// 该值同样用于拼接 ProviderID（<provider>://<instance-id>），必须稳定且不含 "://"。
	ProviderName() string
	// HasClusterID 在需要且已设置 ClusterID 时返回 true。
	// 仅当配置了 cluster-id 且云要求它时返回 true；false 时 CCM 允许无 cluster-id 运行。
	HasClusterID() bool
}

// InformerUser 是可选接口：云实现若需要共享 informer 缓存，可额外实现它。
// 这样云实现不必自行创建 informer，避免与 CCM 的缓存重复。
type InformerUser interface {
	// SetInformers sets the informer on the cloud object.
	SetInformers(informerFactory informers.SharedInformerFactory)
}

// Clusters is an abstract, pluggable interface for clusters of containers.
type Clusters interface {
	// ListClusters lists the names of the available clusters.
	ListClusters(ctx context.Context) ([]string, error)
	// Master gets back the address (either DNS name or IP address) of the master node for the cluster.
	Master(ctx context.Context, clusterName string) (string, error)
}

// (DEPRECATED) DefaultLoadBalancerName 是 LoadBalancer.GetLoadBalancerName 调用的默认负载均衡器名称，
// 用于为 Kubernetes v1.12 之前创建的 LoadBalancer 保持向后兼容的命名；各 provider 今后应在 GetLoadBalancerName 中
// 替换为该 provider 自己的、不再依赖 Service UUID 的命名实现。
// 该函数基于 Service UID 生成名称，虽然不可读，但能保证删除重建后名称不冲突。
func DefaultLoadBalancerName(service *v1.Service) string {
	// GCE 要求负载均衡器名称以小写字母开头。
	// 前置 "a" 同时保证 UID 以数字开头时也满足 GCE 的命名限制。
	ret := "a" + string(service.UID)
	ret = strings.Replace(ret, "-", "", -1)
	// AWS 要求负载均衡器名称短于 32 字节。
	// AWS 的 32 字节上限要求截断；UID 的随机性足以避免截断后碰撞。
	if len(ret) > 32 {
		ret = ret[:32]
	}
	return ret
}

// GetInstanceProviderID 为云中的节点构造 ProviderID，统一为 <provider-name>://<instance-id>；协议头让不同云的 ID 可以共存于同一集群。
// 若实例不存在，必须返回 ("", cloudprovider.InstanceNotFound)；但对存在而处于停止/休眠状态的实例不得返回 InstanceNotFound。
func GetInstanceProviderID(ctx context.Context, cloud Interface, nodeName types.NodeName) (string, error) {
	instances, ok := cloud.Instances()
	if !ok {
		return "", fmt.Errorf("failed to get instances from cloud provider")
	}
	instanceID, err := instances.InstanceID(ctx, nodeName)
	if err != nil {
		// NotImplemented/InstanceNotFound 是调用方需要识别的哨兵错误，原样上抛。
		if err == NotImplemented {
			return "", err
		}
		if err == InstanceNotFound {
			return "", err
		}

		return "", fmt.Errorf("failed to get instance ID from cloud provider: %w", err)
	}
	return cloud.ProviderName() + "://" + instanceID, nil
}

// LoadBalancer 是负载均衡器的抽象、可插拔接口。
// 云 provider 可以选择在独立于 ServiceController 的控制器中实现创建/销毁特定负载均衡器的逻辑；此时 {Ensure,Update}LoadBalancer 必须返回 ImplementedElsewhere 错误。
// 对启用负载均衡器的 Service，若存在由 ServiceController 创建的 LoadBalancer 实例，GetLoadBalancer 必须返回 "exists=True"；其余所有情况必须返回 NotFound 错误。
// 为确保 ServiceController 分配的资源被正确拆除，EnsureLoadBalancerDeleted 不得返回 ImplementedElsewhere——用户更改 LB 类型、或从 ServiceController 迁移到替代实现时就可能遇到。
// 无论是否返回 ImplementedElsewhere，ServiceController 都会负责增删 Service 上的 finalizer；LB Service 的其他 finalizer 必须在替代实现中自行管理。
// 关键不变量：所有方法都把 Service/Node 视为只读；幂等由实现方保证，
// 因为控制器可能在任意步骤失败并重试整个 Ensure。
type LoadBalancer interface {
	// GetLoadBalancer 返回指定负载均衡器是否存在，若存在则返回其状态。
	// 实现必须将 *v1.Service 参数视为只读，不得修改；参数 'clusterName' 是提交给 kube-controller-manager 的集群名称。
	// exists=false 且无错误会让控制器进入创建流程；资源已部分存在时也应返回 true。
	// TODO: Break this up into different interfaces (LB, etc) when we have more than one type of service
	GetLoadBalancer(ctx context.Context, clusterName string, service *v1.Service) (status *v1.LoadBalancerStatus, exists bool, err error)
	// GetLoadBalancerName 返回负载均衡器的名称；实现必须将 *v1.Service 参数视为只读，不得修改。
	// 名称必须根据 Service 确定性地生成，才能跨重启、跨副本定位同一 LB。
	GetLoadBalancerName(ctx context.Context, clusterName string, service *v1.Service) string
	// EnsureLoadBalancer 创建名为 'name' 的新负载均衡器或更新已有的，并返回负载均衡器状态。
	// 实现必须将 *v1.Service 与 *v1.Node 参数视为只读，不得修改；参数 'clusterName' 是提交给 kube-controller-manager 的集群名称。
	// 实现可以返回（可能被包装的）api.RetryError 来按固定时长退避；例如负载均衡器尚未就绪（仍在创建中）时，固定速率轮询优于指数退避，可将延迟降到最低。
	// 返回 RetryError 可控制重试节奏，避免默认指数退避把正在创建的 LB 拖慢。
	EnsureLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) (*v1.LoadBalancerStatus, error)
	// UpdateLoadBalancer 更新指定负载均衡器下的后端主机；实现必须将 *v1.Service 与 *v1.Node 参数视为只读，不得修改；参数 'clusterName' 是提交给 kube-controller-manager 的集群名称。
	// 仅在后端节点集合变化时调用；不应在此重复创建或销毁 LB。
	UpdateLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) error
	// EnsureLoadBalancerDeleted 删除指定的负载均衡器（若存在）；当负载均衡器本就不存在或已成功删除时返回 nil。
	// 这种约定很有用：许多云 provider 的负载均衡器由多个底层组件构成，Get 可能显示 LB 不存在而部分组件仍然残留。
	// 实现必须将 *v1.Service 参数视为只读，不得修改；参数 'clusterName' 是提交给 kube-controller-manager 的集群名称。
	// 必须清理所有底层组件；仅在确认全部删除后才返回 nil，以便 finalizer 安全移除。
	EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *v1.Service) error
}

// Instances 是实例集合的抽象、可插拔接口。
// 这些方法均以节点名或 ProviderID 为键；实现应返回 InstanceNotFound/NotImplemented
// 等哨兵错误，供控制器区分“实例不存在”与“暂时性 API 失败”。
type Instances interface {
	// NodeAddresses returns the addresses of the specified instance.
	NodeAddresses(ctx context.Context, name types.NodeName) ([]v1.NodeAddress, error)
	// NodeAddressesByProviderID returns the addresses of the specified instance.
	// The instance is specified using the providerID of the node. The
	// ProviderID is a unique identifier of the node. This will not be called
	// from the node whose nodeaddresses are being queried. i.e. local metadata
	// services cannot be used in this method to obtain nodeaddresses
	NodeAddressesByProviderID(ctx context.Context, providerID string) ([]v1.NodeAddress, error)
	// InstanceID returns the cloud provider ID of the node with the specified NodeName.
	// Note that if the instance does not exist, we must return ("", cloudprovider.InstanceNotFound)
	// cloudprovider.InstanceNotFound should NOT be returned for instances that exist but are stopped/sleeping
	InstanceID(ctx context.Context, nodeName types.NodeName) (string, error)
	// InstanceType returns the type of the specified instance.
	InstanceType(ctx context.Context, name types.NodeName) (string, error)
	// InstanceTypeByProviderID returns the type of the specified instance.
	InstanceTypeByProviderID(ctx context.Context, providerID string) (string, error)
	// AddSSHKeyToAllInstances adds an SSH public key as a legal identity for all instances
	// expected format for the key is standard ssh-keygen format: <protocol> <blob>
	AddSSHKeyToAllInstances(ctx context.Context, user string, keyData []byte) error
	// CurrentNodeName returns the name of the node we are currently running on
	// On most clouds (e.g. GCE) this is the hostname, so we provide the hostname
	CurrentNodeName(ctx context.Context, hostname string) (types.NodeName, error)
	// InstanceExistsByProviderID 在给定 providerID 对应的实例存在时返回 true；若返回 false 且无错误，实例会被 cloud controller manager 立即删除。
	// 误报 false 会导致 Node 被删除，因此停止/休眠实例必须仍返回 true。
	InstanceExistsByProviderID(ctx context.Context, providerID string) (bool, error)
	// InstanceShutdownByProviderID 在实例于 cloudprovider 侧已关机时返回 true。
	// 仅表示实例处于可安全卸载卷的关机状态，不等同于实例不存在。
	InstanceShutdownByProviderID(ctx context.Context, providerID string) (bool, error)
}

// InstancesV2 是云 provider 实例的抽象、可插拔接口；与 Instances 不同，它面向外部云 provider 设计，只应由它们实现，实现该接口后将不再调用 Zones。
// 设计目标是一次调用返回节点的全部元数据，显著减少注册/同步节点时的 API 往返。
type InstancesV2 interface {
	// InstanceExists returns true if the instance for the given node exists according to the cloud provider.
	// Use the node.name or node.spec.providerID field to find the node in the cloud provider.
	InstanceExists(ctx context.Context, node *v1.Node) (bool, error)
	// InstanceShutdown returns true if the instance is shutdown according to the cloud provider.
	// Use the node.name or node.spec.providerID field to find the node in the cloud provider.
	InstanceShutdown(ctx context.Context, node *v1.Node) (bool, error)
	// InstanceMetadata 返回实例的元数据；InstanceMetadata 中的值会在节点注册时被翻译为 Node 对象上的特定字段与标签。
	// 实现在查找给定节点对应的实例时应始终先检查 node.spec.providerID；当 node.spec.providerID 为空时，可以使用节点的其他属性，如其名称、标签和注解。
	// 返回 (nil, nil) 表示元数据暂不可用，控制器会把节点重新入队而不是判定失败。
	InstanceMetadata(ctx context.Context, node *v1.Node) (*InstanceMetadata, error)
}

// Route 是一条高级路由规则的表示。
// 控制器以 (TargetNode, DestinationCIDR) 作为逻辑身份；Name 仅由云侧使用。
// EnableNodeAddresses 是兼容开关：老版本云实现不认 TargetNodeAddresses，
// 置 false 时控制器忽略该字段，避免每次对账都误判为需要重建。
type Route struct {
	// Name is the name of the routing rule in the cloud-provider.
	// It will be ignored in a Create (although nameHint may influence it)
	Name string
	// TargetNode is the NodeName of the target instance.
	TargetNode types.NodeName
	// EnableNodeAddresses is a feature gate for TargetNodeAddresses. If false, ignore TargetNodeAddresses.
	// Without this, if users haven't updated their cloud-provider, reconcile() will delete and create same route every time.
	EnableNodeAddresses bool
	// TargetNodeAddresses are the Node IPs of the target Node.
	TargetNodeAddresses []v1.NodeAddress
	// DestinationCIDR is the CIDR format IP range that this routing rule
	// applies to.
	DestinationCIDR string
	// Blackhole is set to true if this is a blackhole route
	// The node controller will delete the route if it is in the managed range.
	Blackhole bool
}

// Routes 是高级路由规则的抽象、可插拔接口。
// 控制器只管理落在 --cluster-cidr 内的路由，其他路由不会传入删除路径。
type Routes interface {
	// ListRoutes lists all managed routes that belong to the specified clusterName
	ListRoutes(ctx context.Context, clusterName string) ([]*Route, error)
	// CreateRoute creates the described managed route
	// route.Name will be ignored, although the cloud-provider may use nameHint
	// to create a more user-meaningful name.
	CreateRoute(ctx context.Context, clusterName string, nameHint string, route *Route) error
	// DeleteRoute deletes the specified managed route
	// Route should be as returned by ListRoutes
	DeleteRoute(ctx context.Context, clusterName string, route *Route) error
}

var (
	// DiskNotFound 表示云侧磁盘已不存在，用于 PV 删除路径的幂等处理。
	DiskNotFound = errors.New("disk is not found")
	// ImplementedElsewhere 表示该能力由外部控制器实现，ServiceController 应跳过相关操作。
	ImplementedElsewhere = errors.New("implemented by alternate to cloud provider")
	// InstanceNotFound 表示实例确实不存在；切勿对已停止实例返回它，否则 Node 会被删除。
	InstanceNotFound = errors.New("instance not found")
	// NotImplemented 表示该接口/方法未实现，调用方应降级或跳过而非视为失败。
	NotImplemented = errors.New("unimplemented")
)

// Zone represents the location of a particular machine.
type Zone struct {
	FailureDomain string
	Region        string
}

// Zones 是用于枚举可用区的抽象、可插拔接口。
// DEPRECATED: Zones is deprecated in favor of retrieving zone/region information from InstancesV2.
// 启用 InstancesV2 后不会再调用该接口；仅在未启用 InstancesV2 的旧实现中使用，新实现应通过 InstanceMetadata 返回拓扑。
type Zones interface {
	// GetZone returns the Zone containing the current failure zone and locality region that the program is running in
	// In most cases, this method is called from the kubelet querying a local metadata service to acquire its zone.
	// For the case of external cloud providers, use GetZoneByProviderID or GetZoneByNodeName since GetZone
	// can no longer be called from the kubelets.
	GetZone(ctx context.Context) (Zone, error)

	// GetZoneByProviderID returns the Zone containing the current zone and locality region of the node specified by providerID
	// This method is particularly used in the context of external cloud providers where node initialization must be done
	// outside the kubelets.
	GetZoneByProviderID(ctx context.Context, providerID string) (Zone, error)

	// GetZoneByNodeName returns the Zone containing the current zone and locality region of the node specified by node name
	// This method is particularly used in the context of external cloud providers where node initialization must be done
	// outside the kubelets.
	GetZoneByNodeName(ctx context.Context, nodeName types.NodeName) (Zone, error)
}

// PVLabeler 是获取卷标签的抽象、可插拔接口。
// DEPRECATED: PVLabeler is deprecated in favor of CSI topology feature.
// 卷拓扑应通过 CSI 的 topology 能力表达，而不是在 PV 上打标签。
type PVLabeler interface {
	GetLabelsForVolume(ctx context.Context, pv *v1.PersistentVolume) (map[string]string, error)
}

// InstanceMetadata 包含特定实例的元数据；其中返回的值会被翻译为 Node 的特定字段和标签。
// 它是 InstancesV2 的返回值，注册节点时按字段分别写入 spec、status 与 labels。
type InstanceMetadata struct {
	// ProviderID is a unique ID used to identify an instance on the cloud provider.
	// The ProviderID set here will be set on the node's spec.providerID field.
	// The provider ID format can be set by the cloud provider but providers should
	// ensure the format does not change in any incompatible way.
	//
	// The provider ID format used by existing cloud provider has been:
	//    <provider-name>://<instance-id>
	// Existing providers setting this field should preserve the existing format
	// currently being set in node.spec.providerID.
	ProviderID string
	// InstanceType is the instance's type.
	// The InstanceType set here will be set using the following labels on the node object:
	//   * node.kubernetes.io/instance-type=<instance-type>
	//   * beta.kubernetes.io/instance-type=<instance-type> (DEPRECATED)
	InstanceType string
	// NodeAddress contains information for the instance's address.
	// The node addresses returned here will be set on the node's status.addresses field.
	NodeAddresses []v1.NodeAddress

	// Zone is the zone that the instance is in.
	// The value set here is applied as the following labels on the node:
	//   * topology.kubernetes.io/zone=<zone>
	//   * failure-domain.beta.kubernetes.io/zone=<zone> (DEPRECATED)
	Zone string
	// Region is the region that the instance is in.
	// The value set here is applied as the following labels on the node:
	//   * topology.kubernetes.io/region=<region>
	//   * failure-domain.beta.kubernetes.io/region=<region> (DEPRECATED)
	Region string

	// AdditionalLabels 是云 provider 提供的附加标签 map；提供时将被应用到节点上，使云 provider 能够给节点打上对该 provider 有价值的信息。
	// 云厂商自定义标签会与标准拓扑标签一起写入；保留键由控制器负责维持正确性。
	AdditionalLabels map[string]string
}
