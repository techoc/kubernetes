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
	// LoadBalancer 返回负载均衡器接口，第二个返回值表示该能力是否受支持。
	LoadBalancer() (LoadBalancer, bool)
	// Instances 返回实例接口，第二个返回值表示该能力是否受支持。
	Instances() (Instances, bool)
	// InstancesV2 是 instances 的实现，只应由外部云 provider 实现。其行为与 Instances 相同，但经过优化以显著减少注册/同步节点时对云的 API 调用；实现该接口后将不再调用 Zones。
	// 若返回 supported=true，CCM 会优先使用 InstancesV2 并停止调用 Instances/Zones，
	// 因此实现方必须同时提供元数据中的 zone/region 信息。
	InstancesV2() (InstancesV2, bool)
	// Zones 返回一个 zones 接口；若支持该接口也返回 true，否则返回 false。
	// DEPRECATED: Zones is deprecated in favor of retrieving zone/region information from InstancesV2.
	// 启用 InstancesV2 后不会再调用该接口；新实现应通过 InstanceMetadata 返回拓扑。
	Zones() (Zones, bool)
	// Clusters 返回集群接口，第二个返回值表示该能力是否受支持。
	Clusters() (Clusters, bool)
	// Routes 返回路由接口及该能力是否受支持。
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
	// SetInformers 把 informer 注入云实现，使其复用 CCM 的共享缓存。
	SetInformers(informerFactory informers.SharedInformerFactory)
}

// Clusters 是容器集群的抽象、可插拔接口。
type Clusters interface {
	// ListClusters 列出可用集群的名字。
	ListClusters(ctx context.Context) ([]string, error)
	// Master 返回集群 master 节点的地址，可能是 DNS 名或 IP。
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
	// TODO: 当出现多种 Service 类型时，把该接口拆成 LB 等更细的接口。
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
	// NodeAddresses 返回指定实例的地址。
	NodeAddresses(ctx context.Context, name types.NodeName) ([]v1.NodeAddress, error)
	// NodeAddressesByProviderID 按 providerID 返回指定实例的地址。
	// providerID 是节点的唯一标识。该方法不会在被查询地址的节点上调用，
	// 因此不能依赖本地 metadata service 获取地址。
	NodeAddressesByProviderID(ctx context.Context, providerID string) ([]v1.NodeAddress, error)
	// InstanceID 返回指定 NodeName 节点在云 provider 侧的 ID。
	// 实例不存在时必须返回 ("", cloudprovider.InstanceNotFound)；
	// 实例存在但处于停止/休眠状态时不能返回该错误。
	InstanceID(ctx context.Context, nodeName types.NodeName) (string, error)
	// InstanceType 返回指定实例的类型。
	InstanceType(ctx context.Context, name types.NodeName) (string, error)
	// InstanceTypeByProviderID 按 providerID 返回指定实例的类型。
	InstanceTypeByProviderID(ctx context.Context, providerID string) (string, error)
	// AddSSHKeyToAllInstances 为所有实例添加 SSH 公钥作为合法身份；
	// 密钥格式须为 ssh-keygen 标准格式：<protocol> <blob>。
	AddSSHKeyToAllInstances(ctx context.Context, user string, keyData []byte) error
	// CurrentNodeName 返回当前运行所在节点的名字。
	// 大多数云（如 GCE）上它就是主机名，因此直接传入主机名。
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
	// InstanceExists 判断云 provider 侧该节点对应的实例是否存在。
	// 查找时应使用 node.name 或 node.spec.providerID。
	InstanceExists(ctx context.Context, node *v1.Node) (bool, error)
	// InstanceShutdown 判断云 provider 侧该实例是否已关机。
	// 查找时应使用 node.name 或 node.spec.providerID。
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
	// Name 是路由规则在云 provider 侧的名字。
	// Create 时会忽略它，但 nameHint 可能影响实际命名。
	Name string
	// TargetNode 是目标实例对应的 NodeName。
	TargetNode types.NodeName
	// EnableNodeAddresses 是 TargetNodeAddresses 的特性开关；为 false 时忽略该字段。
	// 若没有它，未升级的云 provider 会导致 reconcile() 每次都删除并重建同一条路由。
	EnableNodeAddresses bool
	// TargetNodeAddresses 是目标节点的 IP 地址。
	TargetNodeAddresses []v1.NodeAddress
	// DestinationCIDR 是本路由规则适用的 CIDR 格式 IP 范围。
	DestinationCIDR string
	// Blackhole 表示这是一条黑洞路由。
	// 落在托管范围内的黑洞路由会被节点控制器删除。
	Blackhole bool
}

// Routes 是高级路由规则的抽象、可插拔接口。
// 控制器只管理落在 --cluster-cidr 内的路由，其他路由不会传入删除路径。
type Routes interface {
	// ListRoutes 列出属于指定 clusterName 的所有托管路由。
	ListRoutes(ctx context.Context, clusterName string) ([]*Route, error)
	// CreateRoute 创建描述的托管路由。
	// route.Name 会被忽略；云 provider 可用 nameHint 生成更易读的名字。
	CreateRoute(ctx context.Context, clusterName string, nameHint string, route *Route) error
	// DeleteRoute 删除指定的托管路由。
	// 传入的 Route 应来自 ListRoutes 的返回值。
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

// Zone 表示某台机器所在的故障域与地域。
type Zone struct {
	FailureDomain string
	Region        string
}

// Zones 是用于枚举可用区的抽象、可插拔接口。
// DEPRECATED: Zones is deprecated in favor of retrieving zone/region information from InstancesV2.
// 启用 InstancesV2 后不会再调用该接口；仅在未启用 InstancesV2 的旧实现中使用，新实现应通过 InstanceMetadata 返回拓扑。
type Zones interface {
	// GetZone 返回程序当前所在故障域与地域构成的 Zone。
	// 通常由 kubelet 查询本地 metadata service 获取；外部云 provider 场景下
	// kubelet 不能再调用它，应改用 GetZoneByProviderID 或 GetZoneByNodeName。
	GetZone(ctx context.Context) (Zone, error)

	// GetZoneByProviderID 返回 providerID 指定节点所在故障域与地域构成的 Zone。
	// 外部云 provider 场景下节点初始化在 kubelet 之外完成，主要用此方法。
	GetZoneByProviderID(ctx context.Context, providerID string) (Zone, error)

	// GetZoneByNodeName 返回节点名指定节点所在故障域与地域构成的 Zone。
	// 外部云 provider 场景下节点初始化在 kubelet 之外完成，主要用此方法。
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
	// ProviderID 是云 provider 侧标识实例的唯一 ID，会写入节点的 spec.providerID。
	// 格式由 provider 自定，但必须保持兼容，不能被控制器误解析。
	// 既有 provider 使用：
	//    <provider-name>://<instance-id>
	// 已有实现填写该字段时应沿用 node.spec.providerID 中当前的格式。
	ProviderID string
	// InstanceType 是实例类型，会写入节点上的以下标签：
	//   * node.kubernetes.io/instance-type=<instance-type>
	//   * beta.kubernetes.io/instance-type=<instance-type> (DEPRECATED)
	InstanceType string
	// NodeAddress 是实例地址信息，会写入节点的 status.addresses。
	NodeAddresses []v1.NodeAddress

	// Zone 是实例所在的可用区，会写入节点上的以下标签：
	//   * topology.kubernetes.io/zone=<zone>
	//   * failure-domain.beta.kubernetes.io/zone=<zone> (DEPRECATED)
	Zone string
	// Region 是实例所在的地域，会写入节点上的以下标签：
	//   * topology.kubernetes.io/region=<region>
	//   * failure-domain.beta.kubernetes.io/region=<region> (DEPRECATED)
	Region string

	// AdditionalLabels 是云 provider 提供的附加标签 map；提供时将被应用到节点上，使云 provider 能够给节点打上对该 provider 有价值的信息。
	// 云厂商自定义标签会与标准拓扑标签一起写入；保留键由控制器负责维持正确性。
	AdditionalLabels map[string]string
}
