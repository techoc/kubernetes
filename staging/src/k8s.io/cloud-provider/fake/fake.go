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

package fake

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	cloudprovider "k8s.io/cloud-provider"
)

// defaultProviderName 是未显式设置 Provider 时 ProviderName 的返回值。
const defaultProviderName = "fake"

// Balancer 是负载均衡器信息的假存储。
// 记录一次 EnsureLoadBalancer 写入的负载均衡器规格，供测试断言。
type Balancer struct {
	Name           string
	Region         string
	LoadBalancerIP string
	Ports          []v1.ServicePort
	Hosts          []*v1.Node
}

// UpdateBalancerCall 表示一次更新负载均衡器的假调用。
// 同时用于 Ensure 与 Update 的调用记录，二者参数形状相同。
type UpdateBalancerCall struct {
	Service *v1.Service
	Hosts   []*v1.Node
}

// 编译期断言：Cloud 必须实现下面所有云提供者接口。
// 接口演进时这里会先编译失败，而不是等到控制器运行期才发现不兼容。
var _ cloudprovider.Interface = (*Cloud)(nil)
var _ cloudprovider.Instances = (*Cloud)(nil)
var _ cloudprovider.LoadBalancer = (*Cloud)(nil)
var _ cloudprovider.Routes = (*Cloud)(nil)
var _ cloudprovider.Zones = (*Cloud)(nil)
var _ cloudprovider.PVLabeler = (*Cloud)(nil)
var _ cloudprovider.Clusters = (*Cloud)(nil)
var _ cloudprovider.InstancesV2 = (*Cloud)(nil)

// Cloud 是 Interface、LoadBalancer、Instances 和 Routes 的测试替身实现，便于测试使用。
// 字段分为三类：行为开关（Disable*/Enable*）、返回值（Err*/Exists* 等）与观测记录
// （Calls/UpdateCalls/EnsureCalls/Balancers/RouteMap）。测试通过组合它们构造场景。
type Cloud struct {
	// Disable* 让对应接口返回“不支持”，用于测试控制器在缺少能力时的降级路径。
	DisableInstances     bool
	DisableRoutes        bool
	DisableLoadBalancers bool
	DisableZones         bool
	DisableClusters      bool

	// Exists/Err 是多数查询方法的通用返回值，便于统一控制成功与失败分支。
	Exists bool
	Err    error

	// EnableInstancesV2 切换到 InstancesV2 路径；一旦启用，控制器不再调用 Zones。
	EnableInstancesV2       bool
	ExistsByProviderID      bool
	ErrByProviderID         error
	NodeShutdown            bool
	ErrShutdownByProviderID error
	MetadataErr             error

	// Calls 记录被调用方法名（如 "create"、"get-zone"），用于断言调用序列。
	Calls          []string
	Addresses      []v1.NodeAddress
	addressesMux   sync.Mutex
	ExtID          map[types.NodeName]string
	ExtIDErr       map[types.NodeName]error
	InstanceTypes  map[types.NodeName]string
	Machines       []types.NodeName
	NodeResources  v1.ResourceList
	ClusterList    []string
	MasterName     string
	ExternalIP     net.IP
	BalancerIPMode *v1.LoadBalancerIPMode
	Balancers      map[string]Balancer
	updateCallLock sync.Mutex
	UpdateCalls    []UpdateBalancerCall
	ensureCallLock sync.Mutex
	EnsureCalls    []UpdateBalancerCall
	// EnsureCallCb/UpdateCallCb 允许测试在调用发生的瞬间执行断言或阻塞。
	EnsureCallCb func(UpdateBalancerCall)
	UpdateCallCb func(UpdateBalancerCall)
	RouteMap     map[string]*Route
	// Lock 保护 RouteMap，路由接口可能在多个 worker 中并发调用。
	Lock        sync.Mutex
	Provider    string
	ProviderID  map[types.NodeName]string
	addCallLock sync.Mutex
	cloudprovider.Zone
	VolumeLabelMap   map[string]map[string]string
	AdditionalLabels map[string]string

	// OverrideInstanceMetadata 若非空，完全接管 InstanceMetadata 的返回值，
	// 用于构造任意元数据或错误，而不必依赖结构体字段组合。
	OverrideInstanceMetadata func(ctx context.Context, node *v1.Node) (*cloudprovider.InstanceMetadata, error)

	// RequestDelay 模拟云 API 延迟，用于测试并发同步的吞吐与超时行为。
	RequestDelay time.Duration
}

// Route 是一条高级路由规则的表示。
// 在 cloudprovider.Route 之外附带集群名，便于按集群过滤。
type Route struct {
	ClusterName string
	Route       cloudprovider.Route
}

// addCall 记录一次方法调用；加锁是因为多个控制器 worker 会并发访问 Calls。
func (f *Cloud) addCall(desc string) {
	time.Sleep(f.RequestDelay)

	f.addCallLock.Lock()
	defer f.addCallLock.Unlock()
	f.Calls = append(f.Calls, desc)
}

// ClearCalls 清空内部记录的方法调用。
func (f *Cloud) ClearCalls() {
	f.Calls = []string{}
}

// Initialize 把 Kubernetes clientBuilder 传给云 provider（fake 中为空实现）。
func (f *Cloud) Initialize(clientBuilder cloudprovider.ControllerClientBuilder, stop <-chan struct{}) {
}

// ListClusters 列出可用的集群名。
func (f *Cloud) ListClusters(ctx context.Context) ([]string, error) {
	return f.ClusterList, f.Err
}

// Master 返回集群 master 节点的地址（DNS 名或 IP）。
func (f *Cloud) Master(ctx context.Context, name string) (string, error) {
	return f.MasterName, f.Err
}

// Clusters 返回集群接口及该能力是否受支持。
func (f *Cloud) Clusters() (cloudprovider.Clusters, bool) {
	return f, !f.DisableClusters
}

// ProviderName 返回云 provider ID。
func (f *Cloud) ProviderName() string {
	if f.Provider == "" {
		return defaultProviderName
	}
	return f.Provider
}

// HasClusterID 返回集群是否带有 clusterID。
func (f *Cloud) HasClusterID() bool {
	return true
}

// LoadBalancer 返回 LoadBalancer 的 fake 实现，实际就是 f 自身。
func (f *Cloud) LoadBalancer() (cloudprovider.LoadBalancer, bool) {
	return f, !f.DisableLoadBalancers
}

// Instances 返回 Instances 的 fake 实现，实际就是 f 自身。
func (f *Cloud) Instances() (cloudprovider.Instances, bool) {
	return f, !f.DisableInstances
}

// InstancesV2 返回 InstancesV2 的 fake 实现，实际就是 f 自身。
func (f *Cloud) InstancesV2() (cloudprovider.InstancesV2, bool) {
	if f.EnableInstancesV2 {
		return f, true
	}
	return nil, false
}

// Zones 返回可用区接口及该能力是否受支持。
func (f *Cloud) Zones() (cloudprovider.Zones, bool) {
	return f, !f.DisableZones
}

// Routes 返回路由接口及该能力是否受支持。
func (f *Cloud) Routes() (cloudprovider.Routes, bool) {
	return f, !f.DisableRoutes
}

// GetLoadBalancer 是 LoadBalancer.GetLoadBalancer 的桩实现。
func (f *Cloud) GetLoadBalancer(ctx context.Context, clusterName string, service *v1.Service) (*v1.LoadBalancerStatus, bool, error) {
	status := &v1.LoadBalancerStatus{}
	status.Ingress = []v1.LoadBalancerIngress{{IP: f.ExternalIP.String()}}

	return status, f.Exists, f.Err
}

// GetLoadBalancerName 是 LoadBalancer.GetLoadBalancerName 的桩实现。
func (f *Cloud) GetLoadBalancerName(ctx context.Context, clusterName string, service *v1.Service) string {
	// TODO: 用 DefaultLoadBalancerName 替换掉，生成更有意义的负载均衡器名字。
	return cloudprovider.DefaultLoadBalancerName(service)
}

// EnsureLoadBalancer 是 LoadBalancer.EnsureLoadBalancer 的测试间谍实现，
// 会在方法调用记录中加入 "create"。
func (f *Cloud) EnsureLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) (*v1.LoadBalancerStatus, error) {
	f.addCall("create")
	f.markEnsureCall(service, nodes)
	if f.Balancers == nil {
		f.Balancers = make(map[string]Balancer)
	}

	name := f.GetLoadBalancerName(ctx, clusterName, service)
	spec := service.Spec

	zone, err := f.GetZone(context.TODO())
	if err != nil {
		return nil, err
	}
	region := zone.Region

	// 以 GetLoadBalancerName 的结果为键保存，模拟云侧按名称幂等覆盖。
	f.Balancers[name] = Balancer{name, region, spec.LoadBalancerIP, spec.Ports, nodes}

	status := &v1.LoadBalancerStatus{}
	// 处理端口列表。
	portStatus := []v1.PortStatus{}
	for _, port := range spec.Ports {
		portStatus = append(portStatus, v1.PortStatus{
			Port:     port.Port,
			Protocol: port.Protocol,
		})
	}
	status.Ingress = []v1.LoadBalancerIngress{{IP: f.ExternalIP.String(), IPMode: f.BalancerIPMode, Ports: portStatus}}

	return status, f.Err
}

// markUpdateCall 记录一次 UpdateLoadBalancer 调用；在锁内回调可让测试同步感知调用发生。
func (f *Cloud) markUpdateCall(service *v1.Service, nodes []*v1.Node) {
	f.updateCallLock.Lock()
	defer f.updateCallLock.Unlock()
	update := UpdateBalancerCall{service, nodes}
	f.UpdateCalls = append(f.UpdateCalls, update)
	if f.UpdateCallCb != nil {
		f.UpdateCallCb(update)
	}
}

// markEnsureCall 记录一次 EnsureLoadBalancer 调用；由单独的 ensureCallLock 保护，
// 这样 Ensure 与 Update 的记录互不阻塞。
func (f *Cloud) markEnsureCall(service *v1.Service, nodes []*v1.Node) {
	f.ensureCallLock.Lock()
	defer f.ensureCallLock.Unlock()
	update := UpdateBalancerCall{service, nodes}
	f.EnsureCalls = append(f.EnsureCalls, update)
	if f.EnsureCallCb != nil {
		f.EnsureCallCb(update)
	}
}

// UpdateLoadBalancer 是 LoadBalancer.UpdateLoadBalancer 的测试间谍实现，
// 会在方法调用记录中加入 "update"。
func (f *Cloud) UpdateLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) error {
	f.addCall("update")
	f.markUpdateCall(service, nodes)
	return f.Err
}

// EnsureLoadBalancerDeleted 是 LoadBalancer.EnsureLoadBalancerDeleted 的测试间谍实现，
// 会在方法调用记录中加入 "delete"。
func (f *Cloud) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *v1.Service) error {
	f.addCall("delete")
	return f.Err
}

// AddSSHKeyToAllInstances 为所有实例添加 SSH 公钥作为合法身份，
// 密钥格式须为 ssh-keygen 标准格式：<protocol> <blob>。
func (f *Cloud) AddSSHKeyToAllInstances(ctx context.Context, user string, keyData []byte) error {
	return cloudprovider.NotImplemented
}

// CurrentNodeName 返回当前运行所在节点的名字。
// 大多数云（如 GCE）上它就是主机名，因此直接传入主机名。
func (f *Cloud) CurrentNodeName(ctx context.Context, hostname string) (types.NodeName, error) {
	return types.NodeName(hostname), nil
}

// NodeAddresses 是 Instances.NodeAddresses 的测试间谍实现，
// 会在方法调用记录中加入 "node-addresses"。
func (f *Cloud) NodeAddresses(ctx context.Context, instance types.NodeName) ([]v1.NodeAddress, error) {
	f.addCall("node-addresses")
	f.addressesMux.Lock()
	defer f.addressesMux.Unlock()
	return f.Addresses, f.Err
}

// SetNodeAddresses 设置节点的地址。
func (f *Cloud) SetNodeAddresses(nodeAddresses []v1.NodeAddress) {
	f.addressesMux.Lock()
	defer f.addressesMux.Unlock()
	f.Addresses = nodeAddresses
}

// NodeAddressesByProviderID 是 Instances.NodeAddressesByProviderID 的测试间谍实现，
// 会在方法调用记录中加入 "node-addresses-by-provider-id"。
func (f *Cloud) NodeAddressesByProviderID(ctx context.Context, providerID string) ([]v1.NodeAddress, error) {
	f.addCall("node-addresses-by-provider-id")
	f.addressesMux.Lock()
	defer f.addressesMux.Unlock()
	return f.Addresses, f.Err
}

// InstanceID 返回指定 Name 节点在云 provider 侧的 ID；若 ExtIDError 中有该节点的条目，
// 则返回预设错误，便于测试错误处理路径。
func (f *Cloud) InstanceID(ctx context.Context, nodeName types.NodeName) (string, error) {
	f.addCall("instance-id")

	err, ok := f.ExtIDErr[nodeName]
	if ok {
		return "", err
	}

	return f.ExtID[nodeName], nil
}

// InstanceType 返回指定实例的类型。
func (f *Cloud) InstanceType(ctx context.Context, instance types.NodeName) (string, error) {
	f.addCall("instance-type")
	return f.InstanceTypes[instance], nil
}

// InstanceTypeByProviderID 按 providerID 返回指定实例的类型。
func (f *Cloud) InstanceTypeByProviderID(ctx context.Context, providerID string) (string, error) {
	f.addCall("instance-type-by-provider-id")
	return f.InstanceTypes[types.NodeName(providerID)], nil
}

// InstanceExistsByProviderID 判断给定 providerID 的实例是否仍然存在且在运行。
// 若返回 false 且无错误，cloud controller manager 会立即删除对应节点。
func (f *Cloud) InstanceExistsByProviderID(ctx context.Context, providerID string) (bool, error) {
	f.addCall("instance-exists-by-provider-id")
	return f.ExistsByProviderID, f.ErrByProviderID
}

// InstanceShutdownByProviderID 判断实例是否处于可安全卸载卷的状态。
func (f *Cloud) InstanceShutdownByProviderID(ctx context.Context, providerID string) (bool, error) {
	f.addCall("instance-shutdown-by-provider-id")

	if providerID == "" {
		return false, fmt.Errorf("cannot shutdown instance with empty providerID")
	}

	return f.NodeShutdown, f.ErrShutdownByProviderID
}

// InstanceExists 判断节点对应的实例是否仍然存在且在运行。
// 若返回 false 且无错误，cloud controller manager 会立即删除对应节点。
func (f *Cloud) InstanceExists(ctx context.Context, node *v1.Node) (bool, error) {
	f.addCall("instance-exists")
	return f.ExistsByProviderID, f.ErrByProviderID
}

// InstanceShutdown 判断实例是否处于可安全卸载卷的状态。
func (f *Cloud) InstanceShutdown(ctx context.Context, node *v1.Node) (bool, error) {
	f.addCall("instance-shutdown")
	return f.NodeShutdown, f.ErrShutdownByProviderID
}

// InstanceMetadata 返回指定实例的元数据。
// 优先走 OverrideInstanceMetadata；否则从各字段拼装，模拟 InstancesV2 的一次调用
// 同时返回地址、类型、拓扑与附加标签，以减少对云 API 的往返。
func (f *Cloud) InstanceMetadata(ctx context.Context, node *v1.Node) (*cloudprovider.InstanceMetadata, error) {
	if f.OverrideInstanceMetadata != nil {
		return f.OverrideInstanceMetadata(ctx, node)
	}
	f.addCall("instance-metadata-by-provider-id")
	// Addresses/ProviderID 可能被并发读取，故复用 addressesMux 保护。
	f.addressesMux.Lock()
	defer f.addressesMux.Unlock()

	// 优先返回 Cloud.ProviderID 中显式记录的 providerID；缺省时返回空串，
	// 调用方据此判断节点尚未初始化并重试。
	providerID := ""
	id, ok := f.ProviderID[types.NodeName(node.Name)]
	if ok {
		providerID = id
	}

	return &cloudprovider.InstanceMetadata{
		ProviderID:       providerID,
		InstanceType:     f.InstanceTypes[types.NodeName(node.Spec.ProviderID)],
		NodeAddresses:    f.Addresses,
		Zone:             f.Zone.FailureDomain,
		Region:           f.Zone.Region,
		AdditionalLabels: f.AdditionalLabels,
	}, f.MetadataErr
}

// List 是 Instances.List 的测试 spy 实现，
// 会在内部方法调用记录中添加一条 "list" 条目。
// filter 作为正则表达式匹配机器名；无效正则按不匹配处理。
func (f *Cloud) List(filter string) ([]types.NodeName, error) {
	f.addCall("list")
	result := []types.NodeName{}
	for _, machine := range f.Machines {
		if match, _ := regexp.MatchString(filter, string(machine)); match {
			result = append(result, machine)
		}
	}
	return result, f.Err
}

// GetZone 返回程序当前所在故障域与地域构成的 Zone。
// 通常由 kubelet 查询本地 metadata service 获取；外部云 provider 场景下
// kubelet 不能再调用它，应改用 GetZoneByProviderID 或 GetZoneByNodeName。
func (f *Cloud) GetZone(ctx context.Context) (cloudprovider.Zone, error) {
	f.addCall("get-zone")
	return f.Zone, f.Err
}

// GetZoneByProviderID 实现 Zones.GetZoneByProviderID。
// 外部云 provider 场景下 kubelet 不初始化节点数据，主要用此方法。
func (f *Cloud) GetZoneByProviderID(ctx context.Context, providerID string) (cloudprovider.Zone, error) {
	f.addCall("get-zone-by-provider-id")
	return f.Zone, f.Err
}

// GetZoneByNodeName 实现 Zones.GetZoneByNodeName。
// 外部云 provider 场景下 kubelet 不初始化节点数据，主要用此方法。
func (f *Cloud) GetZoneByNodeName(ctx context.Context, nodeName types.NodeName) (cloudprovider.Zone, error) {
	f.addCall("get-zone-by-node-name")
	return f.Zone, f.Err
}

// ListRoutes 列出属于指定 clusterName 的所有托管路由。
// 只返回 ClusterName 精确匹配的路由；返回副本，避免调用方修改内部状态。
func (f *Cloud) ListRoutes(ctx context.Context, clusterName string) ([]*cloudprovider.Route, error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	f.addCall("list-routes")
	var routes []*cloudprovider.Route
	for _, fakeRoute := range f.RouteMap {
		if clusterName == fakeRoute.ClusterName {
			routeCopy := fakeRoute.Route
			routes = append(routes, &routeCopy)
		}
	}
	return routes, f.Err
}

// CreateRoute 创建描述的托管路由；
// route.Name 会被忽略，但云厂商可以用 nameHint 生成对用户更有意义的名称。
// 假实现用 cluster-targetNode-CIDR 生成唯一键，重复创建会返回错误，
// 以便测试覆盖“路由已存在”这一云侧冲突场景。
func (f *Cloud) CreateRoute(ctx context.Context, clusterName string, nameHint string, route *cloudprovider.Route) error {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	f.addCall("create-route")
	name := clusterName + "-" + string(route.TargetNode) + "-" + route.DestinationCIDR
	if _, exists := f.RouteMap[name]; exists {
		f.Err = fmt.Errorf("route %q already exists", name)
		return f.Err
	}
	fakeRoute := Route{}
	fakeRoute.Route = *route
	fakeRoute.Route.Name = name
	fakeRoute.ClusterName = clusterName
	f.RouteMap[name] = &fakeRoute
	return nil
}

// DeleteRoute 删除指定的托管路由；
// 传入的 Route 应当是 ListRoutes 返回的那个对象。
// 通过 CIDR、目标节点与集群名三要素定位，而不是依赖 Name，
// 因为真实云侧返回的名称不一定与创建时一致。
func (f *Cloud) DeleteRoute(ctx context.Context, clusterName string, route *cloudprovider.Route) error {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	f.addCall("delete-route")
	name := ""
	for key, saved := range f.RouteMap {
		if route.DestinationCIDR == saved.Route.DestinationCIDR &&
			route.TargetNode == saved.Route.TargetNode &&
			clusterName == saved.ClusterName {
			name = key
			break
		}
	}

	if len(name) == 0 {
		f.Err = fmt.Errorf("no route found for node:%v with DestinationCIDR== %v", route.TargetNode, route.DestinationCIDR)
		return f.Err
	}

	delete(f.RouteMap, name)
	return nil
}

// GetLabelsForVolume 返回卷的标签。
func (f *Cloud) GetLabelsForVolume(ctx context.Context, pv *v1.PersistentVolume) (map[string]string, error) {
	if val, ok := f.VolumeLabelMap[pv.Name]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("label not found for volume")
}
