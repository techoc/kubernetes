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

// ControllerClientBuilder allows you to get clients and configs for controllers
// Please note a copy also exists in pkg/controller/client_builder.go
// TODO: Make this depend on the separate controller utilities repo (issues/68947)
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

// Interface is an abstract, pluggable interface for cloud providers.
// 它是 CCM 与云实现之间的唯一契约：各能力接口通过返回 (impl, supported) 可选地暴露，
// 控制器据此决定是否启用对应控制循环。所有实现必须并发安全，因为控制器 worker 会并行调用。
type Interface interface {
	// Initialize provides the cloud with a kubernetes client builder and may spawn goroutines
	// to perform housekeeping or run custom controllers specific to the cloud provider.
	// Any tasks started here should be cleaned up when the stop channel closes.
	// 在启动所有控制器之前调用一次；厂商可在此启动自有控制器，但必须监听 stop。
	Initialize(clientBuilder ControllerClientBuilder, stop <-chan struct{})
	// LoadBalancer returns a balancer interface. Also returns true if the interface is supported, false otherwise.
	LoadBalancer() (LoadBalancer, bool)
	// Instances returns an instances interface. Also returns true if the interface is supported, false otherwise.
	Instances() (Instances, bool)
	// InstancesV2 is an implementation for instances and should only be implemented by external cloud providers.
	// Implementing InstancesV2 is behaviorally identical to Instances but is optimized to significantly reduce
	// API calls to the cloud provider when registering and syncing nodes. Implementation of this interface will
	// disable calls to the Zones interface. Also returns true if the interface is supported, false otherwise.
	// 若返回 supported=true，CCM 会优先使用 InstancesV2 并停止调用 Instances/Zones，
	// 因此实现方必须同时提供元数据中的 zone/region 信息。
	InstancesV2() (InstancesV2, bool)
	// Zones returns a zones interface. Also returns true if the interface is supported, false otherwise.
	// DEPRECATED: Zones is deprecated in favor of retrieving zone/region information from InstancesV2.
	// This interface will not be called if InstancesV2 is enabled.
	// 已弃用：启用 InstancesV2 后不会再调用该接口，新实现应通过 InstanceMetadata 返回拓扑。
	Zones() (Zones, bool)
	// Clusters returns a clusters interface.  Also returns true if the interface is supported, false otherwise.
	Clusters() (Clusters, bool)
	// Routes returns a routes interface along with whether the interface is supported.
	Routes() (Routes, bool)
	// ProviderName returns the cloud provider ID.
	// 该值同样用于拼接 ProviderID（<provider>://<instance-id>），必须稳定且不含 "://"。
	ProviderName() string
	// HasClusterID returns true if a ClusterID is required and set
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

// (DEPRECATED) DefaultLoadBalancerName is the default load balancer name that is called from
// LoadBalancer.GetLoadBalancerName. Use this method to maintain backward compatible names for
// LoadBalancers that were created prior to Kubernetes v1.12. In the future, each provider should
// replace this method call in GetLoadBalancerName with a provider-specific implementation that
// is less cryptic than the Service's UUID.
// 该函数基于 Service UID 生成名称，虽然不可读，但能保证删除重建后名称不冲突。
func DefaultLoadBalancerName(service *v1.Service) string {
	//GCE requires that the name of a load balancer starts with a lower case letter.
	// 前置 "a" 同时保证 UID 以数字开头时也满足 GCE 的命名限制。
	ret := "a" + string(service.UID)
	ret = strings.Replace(ret, "-", "", -1)
	//AWS requires that the name of a load balancer is shorter than 32 bytes.
	// AWS 的 32 字节上限要求截断；UID 的随机性足以避免截断后碰撞。
	if len(ret) > 32 {
		ret = ret[:32]
	}
	return ret
}

// GetInstanceProviderID builds a ProviderID for a node in a cloud.
// Note that if the instance does not exist, we must return ("", cloudprovider.InstanceNotFound)
// cloudprovider.InstanceNotFound should NOT be returned for instances that exist but are stopped/sleeping
// ProviderID 统一为 <provider-name>://<instance-id>；协议头让不同云的 ID 可以共存于同一集群。
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

// LoadBalancer is an abstract, pluggable interface for load balancers.
//
// Cloud provider may chose to implement the logic for
// constructing/destroying specific kinds of load balancers in a
// controller separate from the ServiceController.  If this is the case,
// then {Ensure,Update}LoadBalancer must return the ImplementedElsewhere error.
// For the given LB service, the GetLoadBalancer must return "exists=True" if
// there exists a LoadBalancer instance created by ServiceController.
// In all other cases, GetLoadBalancer must return a NotFound error.
// EnsureLoadBalancerDeleted must not return ImplementedElsewhere to ensure
// proper teardown of resources that were allocated by the ServiceController.
// This can happen if a user changes the type of LB via an update to the resource
// or when migrating from ServiceController to alternate implementation.
// The finalizer on the service will be added and removed by ServiceController
// irrespective of the ImplementedElsewhere error. Additional finalizers for
// LB services must be managed in the alternate implementation.
// 关键不变量：所有方法都把 Service/Node 视为只读；幂等由实现方保证，
// 因为控制器可能在任意步骤失败并重试整个 Ensure。
type LoadBalancer interface {
	// GetLoadBalancer returns whether the specified load balancer exists, and
	// if so, what its status is.
	// Implementations must treat the *v1.Service parameter as read-only and not modify it.
	// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager.
	// exists=false 且无错误会让控制器进入创建流程；资源已部分存在时也应返回 true。
	// TODO: Break this up into different interfaces (LB, etc) when we have more than one type of service
	GetLoadBalancer(ctx context.Context, clusterName string, service *v1.Service) (status *v1.LoadBalancerStatus, exists bool, err error)
	// GetLoadBalancerName returns the name of the load balancer. Implementations must treat the
	// *v1.Service parameter as read-only and not modify it.
	// 名称必须根据 Service 确定性地生成，才能跨重启、跨副本定位同一 LB。
	GetLoadBalancerName(ctx context.Context, clusterName string, service *v1.Service) string
	// EnsureLoadBalancer creates a new load balancer 'name', or updates the existing one. Returns the status of the balancer
	// Implementations must treat the *v1.Service and *v1.Node
	// parameters as read-only and not modify them.
	// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager.
	//
	// Implementations may return a (possibly wrapped) api.RetryError to enforce
	// backing off at a fixed duration. This can be used for cases like when the
	// load balancer is not ready yet (e.g., it is still being provisioned) and
	// polling at a fixed rate is preferred over backing off exponentially in
	// order to minimize latency.
	// 返回 RetryError 可控制重试节奏，避免默认指数退避把正在创建的 LB 拖慢。
	EnsureLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) (*v1.LoadBalancerStatus, error)
	// UpdateLoadBalancer updates hosts under the specified load balancer.
	// Implementations must treat the *v1.Service and *v1.Node
	// parameters as read-only and not modify them.
	// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
	// 仅在后端节点集合变化时调用；不应在此重复创建或销毁 LB。
	UpdateLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) error
	// EnsureLoadBalancerDeleted deletes the specified load balancer if it
	// exists, returning nil if the load balancer specified either didn't exist or
	// was successfully deleted.
	// This construction is useful because many cloud providers' load balancers
	// have multiple underlying components, meaning a Get could say that the LB
	// doesn't exist even if some part of it is still laying around.
	// Implementations must treat the *v1.Service parameter as read-only and not modify it.
	// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
	// 必须清理所有底层组件；仅在确认全部删除后才返回 nil，以便 finalizer 安全移除。
	EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *v1.Service) error
}

// Instances is an abstract, pluggable interface for sets of instances.
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
	// InstanceExistsByProviderID returns true if the instance for the given provider exists.
	// If false is returned with no error, the instance will be immediately deleted by the cloud controller manager.
	// This method should still return true for instances that exist but are stopped/sleeping.
	// 误报 false 会导致 Node 被删除，因此停止/休眠实例必须仍返回 true。
	InstanceExistsByProviderID(ctx context.Context, providerID string) (bool, error)
	// InstanceShutdownByProviderID returns true if the instance is shutdown in cloudprovider
	// 仅表示实例处于可安全卸载卷的关机状态，不等同于实例不存在。
	InstanceShutdownByProviderID(ctx context.Context, providerID string) (bool, error)
}

// InstancesV2 is an abstract, pluggable interface for cloud provider instances.
// Unlike the Instances interface, it is designed for external cloud providers and should only be used by them.
// Implementation of this interface will disable calls to the Zones interface.
// 设计目标是一次调用返回节点的全部元数据，显著减少注册/同步节点时的 API 往返。
type InstancesV2 interface {
	// InstanceExists returns true if the instance for the given node exists according to the cloud provider.
	// Use the node.name or node.spec.providerID field to find the node in the cloud provider.
	InstanceExists(ctx context.Context, node *v1.Node) (bool, error)
	// InstanceShutdown returns true if the instance is shutdown according to the cloud provider.
	// Use the node.name or node.spec.providerID field to find the node in the cloud provider.
	InstanceShutdown(ctx context.Context, node *v1.Node) (bool, error)
	// InstanceMetadata returns the instance's metadata. The values returned in InstanceMetadata are
	// translated into specific fields and labels in the Node object on registration.
	// Implementations should always check node.spec.providerID first when trying to discover the instance
	// for a given node. In cases where node.spec.providerID is empty, implementations can use other
	// properties of the node like its name, labels and annotations.
	// 返回 (nil, nil) 表示元数据暂不可用，控制器会把节点重新入队而不是判定失败。
	InstanceMetadata(ctx context.Context, node *v1.Node) (*InstanceMetadata, error)
}

// Route is a representation of an advanced routing rule.
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

// Routes is an abstract, pluggable interface for advanced routing rules.
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

// Zones is an abstract, pluggable interface for zone enumeration.
// DEPRECATED: Zones is deprecated in favor of retrieving zone/region information from InstancesV2.
// This interface will not be called if InstancesV2 is enabled.
// 已弃用：仅在未启用 InstancesV2 的旧实现中使用；新实现应通过 InstanceMetadata 返回拓扑。
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

// PVLabeler is an abstract, pluggable interface for fetching labels for volumes
// DEPRECATED: PVLabeler is deprecated in favor of CSI topology feature.
// 已弃用：卷拓扑应通过 CSI 的 topology 能力表达，而不是在 PV 上打标签。
type PVLabeler interface {
	GetLabelsForVolume(ctx context.Context, pv *v1.PersistentVolume) (map[string]string, error)
}

// InstanceMetadata contains metadata about a specific instance.
// Values returned in InstanceMetadata are translated into specific fields and labels for Node.
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

	// AdditionalLabels is a map of additional labels provided by the cloud provider.
	// When provided, they will be applied to the node and enable cloud providers
	// to labels nodes with information that may be valuable to that provider.
	// 云厂商自定义标签会与标准拓扑标签一起写入；保留键由控制器负责维持正确性。
	AdditionalLabels map[string]string
}
