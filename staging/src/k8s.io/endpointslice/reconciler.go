/*
Copyright 2019 The Kubernetes Authors.

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

package endpointslice

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/endpointslice/metrics"
	"k8s.io/endpointslice/topologycache"
	"k8s.io/endpointslice/trafficdist"
	endpointsliceutil "k8s.io/endpointslice/util"
	"k8s.io/klog/v2"
)

// Reconciler is responsible for transforming current EndpointSlice state into
// desired state
// Reconciler 负责把 EndpointSlice 的当前状态转换为期望状态。
type Reconciler struct {
	// client: 与 API Server 通信的 Kubernetes 客户端，用于增删改 EndpointSlice。
	// nodeLister: 从本地 informer 缓存中按名字读取 Node 的 lister。
	// maxEndpointsPerSlice: 单个 EndpointSlice 允许容纳的最大 Endpoint 数量。
	// endpointSliceTracker: 记录控制器预期产生的变更，抑制由自身写入触发的重复入队。
	// metricsCache: 缓存每个 Service 的 EndpointSlice 效率与流量分发指标。
	client               clientset.Interface
	nodeLister           corelisters.NodeLister
	maxEndpointsPerSlice int32
	endpointSliceTracker *endpointsliceutil.EndpointSliceTracker
	metricsCache         *metrics.Cache
	// topologyCache tracks the distribution of Nodes and endpoints across zones
	// to enable TopologyAwareHints.
	// topologyCache: 记录 Node 与 Endpoint 在各可用区的分布，用于计算 TopologyAwareHints。
	topologyCache *topologycache.TopologyCache
	// preferSameTrafficDistribution determines if the new (PreferSameZone /
	// PreferSameNode) trafficDistribution values should be considered when
	// reconciling EndpointSlice hints.
	// preferSameTrafficDistribution: 是否接受 PreferSameZone / PreferSameNode 这类新的流量分发取值。
	preferSameTrafficDistribution bool
	// eventRecorder allows Reconciler to record and publish events.
	// eventRecorder: 记录并发布事件；controllerName: 本控制器名称，用于标注其管理的 EndpointSlice。
	eventRecorder  record.EventRecorder
	controllerName string
}

// ReconcilerOption 以可选参数的形式配置 Reconciler。
type ReconcilerOption func(*Reconciler)

// WithPreferSameTrafficDistributionEnabled controls whether the Reconciler
// accepts the new `trafficDistribution` values.
// WithPreferSameTrafficDistributionEnabled 控制 Reconciler 是否接受新的 trafficDistribution 取值。
func WithPreferSameTrafficDistributionEnabled(preferSame bool) ReconcilerOption {
	return func(r *Reconciler) {
		r.preferSameTrafficDistribution = preferSame
	}
}

// validTrafficDistribution determines whether TrafficDistribution is set and valid for
// this cluster.
// validTrafficDistribution 判断 trafficDistribution 是否已设置且在本集群中有效：
// PreferClose 始终有效，PreferSameZone/PreferSameNode 需特性开关打开。
func (r *Reconciler) validTrafficDistribution(trafficDistribution *string) bool {
	if trafficDistribution == nil {
		return false
	}
	if *trafficDistribution == corev1.ServiceTrafficDistributionPreferClose {
		return true
	}
	if !r.preferSameTrafficDistribution {
		return false
	}
	if *trafficDistribution == corev1.ServiceTrafficDistributionPreferSameZone ||
		*trafficDistribution == corev1.ServiceTrafficDistributionPreferSameNode {
		return true
	}
	return false
}

// endpointMeta includes the attributes we group slices on, this type helps with
// that logic in Reconciler
// endpointMeta 描述切片分组所依据的属性，供 Reconciler 的分组逻辑使用。
type endpointMeta struct {
	// ports: 该组 EndpointSlice 共用的端口集合。
	// addressType: 该组 EndpointSlice 的地址类型（IPv4/IPv6/FQDN）。
	ports       []discovery.EndpointPort
	addressType discovery.AddressType
}

// Reconcile takes a set of pods currently matching a service selector and
// compares them with the endpoints already present in any existing endpoint
// slices for the given service. It creates, updates, or deletes endpoint slices
// to ensure the desired set of pods are represented by endpoint slices.
// Reconcile 把当前匹配 Service selector 的 Pod 集合与该 Service 现有 EndpointSlice 中的
// Endpoint 做比较，通过创建、更新或删除 EndpointSlice，确保期望的 Pod 集合被正确表达。
// 它是按地址类型分派的入口：先过滤出无效地址类型的切片，再逐个地址类型交给 reconcileByAddressType。
func (r *Reconciler) Reconcile(logger klog.Logger, service *corev1.Service, pods []*corev1.Pod, existingSlices []*discovery.EndpointSlice, triggerTime time.Time) error {
	// 收集三类中间结果：因地址类型不再受支持而要删除的切片、协调过程中累积的错误、按地址类型归类的现有切片。
	slicesToDelete := []*discovery.EndpointSlice{}                                    // slices that are no longer  matching any address the service has
	errs := []error{}                                                                 // all errors generated in the process of reconciling
	slicesByAddressType := make(map[discovery.AddressType][]*discovery.EndpointSlice) // slices by address type

	// addresses that this service supports [o(1) find]
	// 求出该 Service 当前支持的地址类型集合，便于 O(1) 判断一个切片是否仍然有效。
	serviceSupportedAddressesTypes := getAddressTypesForService(logger, service)

	// loop through slices identifying their address type.
	// slices that no longer match address type supported by services
	// go to delete, other slices goes to the Reconciler machinery
	// for further adjustment
	// 遍历现有切片：地址类型已不受支持的直接标记删除，其余按地址类型分组，留给后续协调器调整。
	for _, existingSlice := range existingSlices {
		// service no longer supports that address type, add it to deleted slices
		// 地址类型不再受支持：先清掉拓扑缓存里该切片的 Hints，再放入删除列表。
		if !serviceSupportedAddressesTypes.Has(existingSlice.AddressType) {
			if r.topologyCache != nil {
				// 从切片推导 Service key 失败时只记日志，不阻断删除流程。
				svcKey, err := ServiceControllerKey(existingSlice)
				if err != nil {
					logger.Info("Couldn't get key to remove EndpointSlice from topology cache", "existingSlice", existingSlice, "err", err)
				} else {
					r.topologyCache.RemoveHints(svcKey, existingSlice.AddressType)
				}
			}

			slicesToDelete = append(slicesToDelete, existingSlice)
			continue
		}

		// add list if it is not on our map
		// 某个地址类型首次出现时先初始化切片列表，随后把切片归入该分组。
		if _, ok := slicesByAddressType[existingSlice.AddressType]; !ok {
			slicesByAddressType[existingSlice.AddressType] = make([]*discovery.EndpointSlice, 0, 1)
		}

		slicesByAddressType[existingSlice.AddressType] = append(slicesByAddressType[existingSlice.AddressType], existingSlice)
	}

	// reconcile for existing.
	// 针对每个受支持的地址类型分别协调；单个类型失败不影响其他类型，错误稍后统一聚合。
	for addressType := range serviceSupportedAddressesTypes {
		existingSlices := slicesByAddressType[addressType]
		err := r.reconcileByAddressType(logger, service, pods, existingSlices, triggerTime, addressType)
		if err != nil {
			errs = append(errs, err)
		}
	}

	// delete those which are of addressType that is no longer supported
	// by the service
	// 真正删除地址类型不再受支持的切片；成功后通知 tracker 并累加删除计数，失败则记录错误。
	for _, sliceToDelete := range slicesToDelete {
		err := r.client.DiscoveryV1().EndpointSlices(service.Namespace).Delete(context.TODO(), sliceToDelete.Name, metav1.DeleteOptions{})
		if err != nil {
			errs = append(errs, fmt.Errorf("error deleting %s EndpointSlice for Service %s/%s: %w", sliceToDelete.Name, service.Namespace, service.Name, err))
		} else {
			r.endpointSliceTracker.ExpectDeletion(sliceToDelete)
			metrics.EndpointSliceChanges.WithLabelValues("delete").Inc()
		}
	}

	// 把所有错误聚合成一个返回，部分成功不会被整体失败掩盖。
	return utilerrors.NewAggregate(errs)
}

// reconcileByAddressType takes a set of pods currently matching a service selector and
// compares them with the endpoints already present in any existing endpoint
// slices (by address type) for the given service. It creates, updates, or deletes endpoint slices
// to ensure the desired set of pods are represented by endpoint slices.
// reconcileByAddressType 只处理单一地址类型：把匹配 selector 的 Pod 与该类型下现有切片的
// Endpoint 比较，按端口映射分组计算增删改，再叠加拓扑 Hints / 流量分发处理，最后交给 finalize 落库。
func (r *Reconciler) reconcileByAddressType(logger klog.Logger, service *corev1.Service, pods []*corev1.Pod, existingSlices []*discovery.EndpointSlice, triggerTime time.Time, addressType discovery.AddressType) error {
	// 累积本函数各阶段产生的非致命错误，最终与 finalize 的错误一起聚合返回。
	errs := []error{}

	// 待创建 / 待更新 / 待删除的切片，以及协调过程中产生的事件。
	slicesToCreate := []*discovery.EndpointSlice{}
	slicesToUpdate := []*discovery.EndpointSlice{}
	slicesToDelete := []*discovery.EndpointSlice{}
	events := []*topologycache.EventBuilder{}

	// Build data structures for existing state.
	// 以端口映射的哈希为 key 归类现有切片；不属于本 Service 的切片直接删除。
	existingSlicesByPortMap := map[endpointsliceutil.PortMapKey][]*discovery.EndpointSlice{}
	for _, existingSlice := range existingSlices {
		// ownedBy 判定归属，避免误删同名旧 Service 遗留的切片。
		if ownedBy(existingSlice, service) {
			epHash := endpointsliceutil.NewPortMapKey(existingSlice.Ports)
			existingSlicesByPortMap[epHash] = append(existingSlicesByPortMap[epHash], existingSlice)
		} else {
			slicesToDelete = append(slicesToDelete, existingSlice)
		}
	}

	// Build data structures for desired state.
	// 分别记录每个端口映射组对应的切片元信息（地址类型/端口）与期望 Endpoint 集合。
	desiredMetaByPortMap := map[endpointsliceutil.PortMapKey]*endpointMeta{}
	desiredEndpointsByPortMap := map[endpointsliceutil.PortMapKey]endpointsliceutil.EndpointSet{}

	// 遍历候选 Pod，构造该地址类型下的期望 Endpoint。
	for _, pod := range pods {
		// 终止中或不属于 Endpoint 的 Pod 直接跳过。
		if !endpointsliceutil.ShouldPodBeInEndpoints(pod, true) {
			continue
		}

		// 端口由 Service 与 Pod 共同决定，并据此确定分组哈希。
		endpointPorts := getEndpointPorts(logger, service, pod)
		epHash := endpointsliceutil.NewPortMapKey(endpointPorts)
		if _, ok := desiredEndpointsByPortMap[epHash]; !ok {
			desiredEndpointsByPortMap[epHash] = endpointsliceutil.EndpointSet{}
		}

		// 同一端口组只需记录一份元信息。
		if _, ok := desiredMetaByPortMap[epHash]; !ok {
			desiredMetaByPortMap[epHash] = &endpointMeta{
				addressType: addressType,
				ports:       endpointPorts,
			}
		}

		// 从本地缓存取 Pod 所在 Node，用于拓扑 Hints 的可用区/节点信息。
		node, err := r.nodeLister.Get(pod.Spec.NodeName)
		if err != nil {
			// we are getting the information from the local informer,
			// an error different than IsNotFound should not happen
			// 本地 informer 不应返回 NotFound 以外的错误，遇到则直接中止本轮协调。
			if !errors.IsNotFound(err) {
				return err
			}
			// If the Node specified by the Pod doesn't exist we want to requeue the Service so we
			// retry later, but also update the EndpointSlice without the problematic Pod.
			// Theoretically, the pod Garbage Collector will remove the Pod, but we want to avoid
			// situations where a reference from a Pod to a missing node can leave the EndpointSlice
			// stuck forever.
			// On the other side, if the service.Spec.PublishNotReadyAddresses is set we just add the
			// Pod, since the user is explicitly indicating that the Pod address should be published.
			// Node 缺失时：默认跳过该 Pod 并追加错误以触发 Service 重入队，避免切片永久卡住；
			// 但若 PublishNotReadyAddresses 为真，则按用户意愿照常发布该 Pod 地址。
			if !service.Spec.PublishNotReadyAddresses {
				logger.Info("skipping Pod for Service, Node not found", "pod", klog.KObj(pod), "service", klog.KObj(service), "node", klog.KRef("", pod.Spec.NodeName))
				errs = append(errs, fmt.Errorf("skipping Pod %s for Service %s/%s: Node %s Not Found", pod.Name, service.Namespace, service.Name, pod.Spec.NodeName))
				continue
			}
		}
		// 将 Pod 转为 Endpoint，只有拿到可用地址时才加入期望集合。
		endpoint := podToEndpoint(pod, node, service, addressType)
		if len(endpoint.Addresses) > 0 {
			desiredEndpointsByPortMap[epHash].Insert(&endpoint)
		}
	}

	// 记录每个端口组的效率指标，并累计本轮新增/移除的 Endpoint 数量。
	spMetrics := metrics.NewServicePortCache()
	totalAdded := 0
	totalRemoved := 0

	// Determine changes necessary for each group of slices by port map.
	// 对每个端口映射组调用 reconcileByPortMapping，汇总其增删改结果与指标。
	for portMap, desiredEndpoints := range desiredEndpointsByPortMap {
		numEndpoints := len(desiredEndpoints)
		pmSlicesToCreate, pmSlicesToUpdate, pmSlicesToDelete, added, removed := r.reconcileByPortMapping(
			logger, service, existingSlicesByPortMap[portMap], desiredEndpoints, desiredMetaByPortMap[portMap])

		totalAdded += added
		totalRemoved += removed

		spMetrics.Set(portMap, metrics.EfficiencyInfo{
			Endpoints: numEndpoints,
			Slices:    len(existingSlicesByPortMap[portMap]) + len(pmSlicesToCreate) - len(pmSlicesToDelete),
		})

		slicesToCreate = append(slicesToCreate, pmSlicesToCreate...)
		slicesToUpdate = append(slicesToUpdate, pmSlicesToUpdate...)
		slicesToDelete = append(slicesToDelete, pmSlicesToDelete...)
	}

	// If there are unique sets of ports that are no longer desired, mark
	// the corresponding endpoint slices for deletion.
	// 期望集合中已不存在的端口组，其对应切片全部标记删除。
	for portMap, existingSlices := range existingSlicesByPortMap {
		if _, ok := desiredEndpointsByPortMap[portMap]; !ok {
			slicesToDelete = append(slicesToDelete, existingSlices...)
		}
	}

	// When no endpoint slices would usually exist, we need to add a placeholder.
	// 当所有现有切片都要被删除且没有新切片时，保留一个空的占位切片以维持 Service 语义。
	if len(existingSlices) == len(slicesToDelete) && len(slicesToCreate) < 1 {
		// Check for existing placeholder slice outside of the core control flow
		placeholderSlice := newEndpointSlice(logger, service, &endpointMeta{ports: []discovery.EndpointPort{}, addressType: addressType}, r.controllerName)
		if len(slicesToDelete) == 1 && placeholderSliceCompare.DeepEqual(slicesToDelete[0], placeholderSlice) {
			// We are about to unnecessarily delete/recreate the placeholder, remove it now.
			// 现有占位切片与新建的完全一致，取消删除以避免无意义的删了又建。
			slicesToDelete = slicesToDelete[:0]
		} else {
			slicesToCreate = append(slicesToCreate, placeholderSlice)
		}
		spMetrics.Set(endpointsliceutil.NewPortMapKey(placeholderSlice.Ports), metrics.EfficiencyInfo{
			Endpoints: 0,
			Slices:    1,
		})
	}

	// 上报本轮 Endpoint 增减量，并刷新 Service 的端口效率缓存。
	metrics.EndpointsAddedPerSync.WithLabelValues().Observe(float64(totalAdded))
	metrics.EndpointsRemovedPerSync.WithLabelValues().Observe(float64(totalRemoved))

	serviceNN := types.NamespacedName{Name: service.Name, Namespace: service.Namespace}
	r.metricsCache.UpdateServicePortCache(serviceNN, spMetrics)

	// Topology hints are assigned per address type. This means it is
	// theoretically possible for endpoints of one address type to be assigned
	// hints while another endpoints of another address type are not.
	// 按地址类型收集待创建/更新/未变的切片，供 Hints 计算使用（Hints 是按地址类型分配的）。
	si := &topologycache.SliceInfo{
		ServiceKey:  fmt.Sprintf("%s/%s", service.Namespace, service.Name),
		AddressType: addressType,
		ToCreate:    slicesToCreate,
		ToUpdate:    slicesToUpdate,
		Unchanged:   unchangedSlices(existingSlices, slicesToUpdate, slicesToDelete),
	}

	// 仅当 trafficDistribution 有效且未启用旧 topology 注解时，才走新的流量分发路径。
	canUseTrafficDistribution := r.validTrafficDistribution(service.Spec.TrafficDistribution) && !hintsEnabled(service.Annotations)

	// Check if we need to add/remove hints based on the topology annotation.
	//
	// This if/else clause can be removed once the annotation has been deprecated.
	// Ref: https://github.com/kubernetes/enhancements/tree/master/keps/sig-network/4444-service-routing-preference
	// 注解与 trafficDistribution 是两代机制：注解若启用则用它计算 Hints，否则清理旧状态。
	if r.topologyCache != nil && hintsEnabled(service.Annotations) {
		// Reaching this point means that we need to configure hints based on the
		// topology annotation.
		// 依据 topology 注解重新计算并写入切片的 Hints，得到要记录的事件。
		slicesToCreate, slicesToUpdate, events = r.topologyCache.AddHints(logger, si)

	} else {
		// Reaching this point means that we will not be configuring hints based on
		// the topology annotation. We need to do 2 things:
		//  1. If hints were added previously based on the annotation, we need to
		//     clear up any locally cached hints from the topologyCache object.
		//  2. Optionally remove the actual hints from the EndpointSlice if we know
		//     that the `trafficDistribution` field is also NOT being used. In other
		//     words, if we know that the `trafficDistribution` field has been
		//     correctly configured by the customer, we DO NOT remove the hints and
		//     wait for the trafficDist handlers to correctly configure them. Always
		//     unconditionally removing hints here (and letting them get readded by
		//     the trafficDist) adds extra overhead in the form of DeepCopy (done
		//     within topologyCache.RemoveHints)

		// Check 1.
		// 清理 topologyCache 中由旧注解填充的 Hint 状态；若确实曾填充过，记录一条禁用告警事件。
		if r.topologyCache != nil {
			if r.topologyCache.HasPopulatedHints(si.ServiceKey) {
				logger.Info("TopologyAwareHints annotation has changed, removing hints", "serviceKey", si.ServiceKey, "addressType", si.AddressType)
				events = append(events, &topologycache.EventBuilder{
					EventType: corev1.EventTypeWarning,
					Reason:    "TopologyAwareHintsDisabled",
					Message:   topologycache.FormatWithAddressType(topologycache.TopologyAwareHintsDisabled, si.AddressType),
				})
			}
			r.topologyCache.RemoveHints(si.ServiceKey, addressType)
		}

		// Check 2.
		// 若 trafficDistribution 不可用，则把切片中残留的 Hints 一并移除，避免脏数据。
		if !canUseTrafficDistribution {
			slicesToCreate, slicesToUpdate = topologycache.RemoveHintsFromSlices(si)
		}
	}

	// 走新的流量分发路径：刷新指标并按 trafficDistribution 重算 Hints；否则清空该指标。
	if canUseTrafficDistribution {
		r.metricsCache.UpdateTrafficDistributionForService(serviceNN, service.Spec.TrafficDistribution)
		slicesToCreate, slicesToUpdate, _ = trafficdist.ReconcileHints(service.Spec.TrafficDistribution, slicesToCreate, slicesToUpdate, unchangedSlices(existingSlices, slicesToUpdate, slicesToDelete))
	} else {
		r.metricsCache.UpdateTrafficDistributionForService(serviceNN, nil)
	}

	// 把增删改意图交给 finalize 统一执行 API 调用；失败追加到错误列表。
	err := r.finalize(service, slicesToCreate, slicesToUpdate, slicesToDelete, triggerTime)
	if err != nil {
		errs = append(errs, err)
	}
	// 发布本轮收集到的事件（如 TopologyAwareHints 被禁用）。
	for _, event := range events {
		r.eventRecorder.Event(service, event.EventType, event.Reason, event.Message)
	}
	// 聚合所有非致命错误返回，由上层决定是否重入队。
	return utilerrors.NewAggregate(errs)

}

// NewReconciler 构造 Reconciler，装配客户端、lister、切片上限制、tracker、拓扑缓存、
// 事件记录器与控制器名，并依次应用所有可选配置。
func NewReconciler(client clientset.Interface, nodeLister corelisters.NodeLister, maxEndpointsPerSlice int32, endpointSliceTracker *endpointsliceutil.EndpointSliceTracker, topologyCache *topologycache.TopologyCache, eventRecorder record.EventRecorder, controllerName string, options ...ReconcilerOption) *Reconciler {
	// 先填入必需依赖，metricsCache 依据单切片上限初始化。
	r := &Reconciler{
		client:               client,
		nodeLister:           nodeLister,
		maxEndpointsPerSlice: maxEndpointsPerSlice,
		endpointSliceTracker: endpointSliceTracker,
		metricsCache:         metrics.NewCache(maxEndpointsPerSlice),
		topologyCache:        topologyCache,
		eventRecorder:        eventRecorder,
		controllerName:       controllerName,
	}
	// 可选配置最后生效，可覆盖上面的默认值。
	for _, option := range options {
		option(r)
	}
	return r
}

// placeholderSliceCompare is a conversion func for comparing two placeholder endpoint slices.
// It only compares the specific fields we care about.
// placeholderSliceCompare 用于比较两个“占位” EndpointSlice，只关注 OwnerReference 与 Namespace/Labels，
// 从而判断能否复用现有占位切片而不必删了重建。
var placeholderSliceCompare = conversion.EqualitiesOrDie(
	// 归属引用按字符串表示比较，确保两者属于同一个 Service。
	func(a, b metav1.OwnerReference) bool {
		return a.String() == b.String()
	},
	// 元数据比较：命名空间必须相同，且两个方向的 label 都要逐一核对（防漏防多）。
	func(a, b metav1.ObjectMeta) bool {
		if a.Namespace != b.Namespace {
			return false
		}
		for k, v := range a.Labels {
			if b.Labels[k] != v {
				return false
			}
		}
		for k, v := range b.Labels {
			if a.Labels[k] != v {
				return false
			}
		}
		return true
	},
)

// finalize creates, updates, and deletes slices as specified
// finalize 是最终落库步骤：先把可复用的“待删除切片”改写成更新，再依次执行创建、更新、删除的
// API 调用，维护 tracker 与指标，并统计本轮变更的切片数量。
func (r *Reconciler) finalize(
	service *corev1.Service,
	slicesToCreate,
	slicesToUpdate,
	slicesToDelete []*discovery.EndpointSlice,
	triggerTime time.Time,
) error {
	// If there are slices to create and delete, change the creates to updates
	// of the slices that would otherwise be deleted.
	// 先做“回收复用”：把待创建切片的名字改成待删除切片的名字，转为更新，减少 API 抖动。
	for i := 0; i < len(slicesToDelete); {
		if len(slicesToCreate) == 0 {
			break
		}
		sliceToDelete := slicesToDelete[i]
		slice := slicesToCreate[len(slicesToCreate)-1]
		// Only update EndpointSlices that are owned by this Service and have
		// the same AddressType. We need to avoid updating EndpointSlices that
		// are being garbage collected for an old Service with the same name.
		// The AddressType field is immutable. Since Services also consider
		// IPFamily immutable, the only case where this should matter will be
		// the migration from IP to IPv4 and IPv6 AddressTypes, where there's a
		// chance EndpointSlices with an IP AddressType would otherwise be
		// updated to IPv4 or IPv6 without this check.
		// 只复用归属本 Service 且地址类型相同的切片：地址类型不可变，
		// 这样可避免把旧 Service 正被 GC 的切片（或 IP→IPv4/IPv6 迁移时的旧切片）改名改写。
		if sliceToDelete.AddressType == slice.AddressType && ownedBy(sliceToDelete, service) {
			slice.Name = sliceToDelete.Name
			slicesToCreate = slicesToCreate[:len(slicesToCreate)-1]
			slicesToUpdate = append(slicesToUpdate, slice)
			slicesToDelete = append(slicesToDelete[:i], slicesToDelete[i+1:]...)
		} else {
			// 不能复用则继续检查下一个待删除切片。
			i++
		}
	}

	// Don't create new EndpointSlices if the Service is pending deletion. This
	// is to avoid a potential race condition with the garbage collector where
	// it tries to delete EndpointSlices as this controller replaces them.
	// Service 正在删除时跳过创建，避免与 GC 产生竞态；每个切片写入触发时间注解后创建。
	if service.DeletionTimestamp == nil {
		for _, endpointSlice := range slicesToCreate {
			addTriggerTimeAnnotation(endpointSlice, triggerTime)
			createdSlice, err := r.client.DiscoveryV1().EndpointSlices(service.Namespace).Create(context.TODO(), endpointSlice, metav1.CreateOptions{})
			if err != nil {
				// If the namespace is terminating, creates will continue to fail. Simply drop the item.
				// 命名空间处于终止中时创建会一直失败，直接丢弃该切片而不报错。
				if errors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
					return nil
				}
				return fmt.Errorf("failed to create EndpointSlice for Service %s/%s: %v", service.Namespace, service.Name, err)
			}
			// 创建成功后登记到 tracker 并累加创建计数。
			r.endpointSliceTracker.Update(createdSlice)
			metrics.EndpointSliceChanges.WithLabelValues("create").Inc()
		}
	}

	// 逐个更新切片，失败立即返回；成功后同步 tracker 与更新计数。
	for _, endpointSlice := range slicesToUpdate {
		addTriggerTimeAnnotation(endpointSlice, triggerTime)
		updatedSlice, err := r.client.DiscoveryV1().EndpointSlices(service.Namespace).Update(context.TODO(), endpointSlice, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update %s EndpointSlice for Service %s/%s: %v", endpointSlice.Name, service.Namespace, service.Name, err)
		}
		r.endpointSliceTracker.Update(updatedSlice)
		metrics.EndpointSliceChanges.WithLabelValues("update").Inc()
	}

	// 逐个删除切片，失败立即返回；成功后登记预期删除并累加删除计数。
	for _, endpointSlice := range slicesToDelete {
		err := r.client.DiscoveryV1().EndpointSlices(service.Namespace).Delete(context.TODO(), endpointSlice.Name, metav1.DeleteOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete %s EndpointSlice for Service %s/%s: %v", endpointSlice.Name, service.Namespace, service.Name, err)
		}
		r.endpointSliceTracker.ExpectDeletion(endpointSlice)
		metrics.EndpointSliceChanges.WithLabelValues("delete").Inc()
	}

	// 为指标准备标签：注解开启时为 Auto，否则 Disabled；trafficDistribution 有效时取其值。
	topologyLabel := "Disabled"
	if r.topologyCache != nil && hintsEnabled(service.Annotations) {
		topologyLabel = "Auto"
	}
	var trafficDistribution string
	if r.validTrafficDistribution(service.Spec.TrafficDistribution) && !hintsEnabled(service.Annotations) {
		trafficDistribution = *service.Spec.TrafficDistribution
	}

	// 上报本轮实际变更的切片总数。
	numSlicesChanged := len(slicesToCreate) + len(slicesToUpdate) + len(slicesToDelete)
	metrics.EndpointSlicesChangedPerSync.WithLabelValues(topologyLabel, trafficDistribution).Observe(float64(numSlicesChanged))

	return nil
}

// reconcileByPortMapping compares the endpoints found in existing slices with
// the list of desired endpoints and returns lists of slices to create, update,
// and delete. It also checks that the slices mirror the parent services labels.
// The logic is split up into several main steps:
//  1. Iterate through existing slices, delete endpoints that are no longer
//     desired and update matching endpoints that have changed. It also checks
//     if the slices have the labels of the parent services, and updates them if not.
//  2. Iterate through slices that have been modified in 1 and fill them up with
//     any remaining desired endpoints.
//  3. If there still desired endpoints left, try to fit them into a previously
//     unchanged slice and/or create new ones.
//
// reconcileByPortMapping 针对同一端口映射组，把现有切片中的 Endpoint 与期望集合做差量协调，
// 返回待创建、待更新、待删除的切片以及新增/移除的 Endpoint 数量，并同步父 Service 的标签。
// 核心分三步：① 清理旧 Endpoint 并更新变化项；② 用剩余 Endpoint 填满已修改的切片；
// ③ 仍有剩余时尝试塞进未变切片，否则新建切片。
func (r *Reconciler) reconcileByPortMapping(
	logger klog.Logger,
	service *corev1.Service,
	existingSlices []*discovery.EndpointSlice,
	desiredSet endpointsliceutil.EndpointSet,
	endpointMeta *endpointMeta,
) ([]*discovery.EndpointSlice, []*discovery.EndpointSlice, []*discovery.EndpointSlice, int, int) {
	// 按名字索引切片，并用三个集合分别记录“未变/待更新/待删除”的切片名。
	slicesByName := map[string]*discovery.EndpointSlice{}
	sliceNamesUnchanged := sets.New[string]()
	sliceNamesToUpdate := sets.New[string]()
	sliceNamesToDelete := sets.New[string]()
	numRemoved := 0

	// 1. Iterate through existing slices to delete endpoints no longer desired
	//    and update endpoints that have changed
	// 第一步：遍历现有切片，删除不再期望的 Endpoint，更新内容有变化的 Endpoint。
	for _, existingSlice := range existingSlices {
		slicesByName[existingSlice.Name] = existingSlice
		newEndpoints := []discovery.Endpoint{}
		endpointUpdated := false
		for _, endpoint := range existingSlice.Endpoints {
			got := desiredSet.Get(&endpoint)
			// If endpoint is desired add it to list of endpoints to keep.
			// 仍在期望集合中的 Endpoint 保留到新列表。
			if got != nil {
				newEndpoints = append(newEndpoints, *got)
				// If existing version of endpoint doesn't match desired version
				// set endpointUpdated to ensure endpoint changes are persisted.
				// 除哈希外的字段变化时也要标记更新，确保改动落库。
				if !endpointsliceutil.EndpointsEqualBeyondHash(got, &endpoint) {
					endpointUpdated = true
				}
				// once an endpoint has been placed/found in a slice, it no
				// longer needs to be handled
				// 已被某切片认领的 Endpoint 从期望集合移除，避免重复安置。
				desiredSet.Delete(&endpoint)
			}
		}

		// generate the slice labels and check if parent labels have changed
		// 生成应有的标签并检测父 Service 标签是否已变化。
		labels, labelsChanged := setEndpointSliceLabels(logger, existingSlice, service, r.controllerName)

		// If an endpoint was updated or removed, mark for update or delete
		// Endpoint 有更新或被移除时：统计移除数，空切片标记删除，否则深拷贝后标记更新。
		if endpointUpdated || len(existingSlice.Endpoints) != len(newEndpoints) {
			if len(existingSlice.Endpoints) > len(newEndpoints) {
				numRemoved += len(existingSlice.Endpoints) - len(newEndpoints)
			}
			if len(newEndpoints) == 0 {
				// if no endpoints desired in this slice, mark for deletion
				sliceNamesToDelete.Insert(existingSlice.Name)
			} else {
				// otherwise, copy and mark for update
				epSlice := existingSlice.DeepCopy()
				epSlice.Endpoints = newEndpoints
				epSlice.Labels = labels
				slicesByName[existingSlice.Name] = epSlice
				sliceNamesToUpdate.Insert(epSlice.Name)
			}
		} else if labelsChanged {
			// if labels have changed, copy and mark for update
			// Endpoint 没变但标签变了，同样深拷贝后标记更新。
			epSlice := existingSlice.DeepCopy()
			epSlice.Labels = labels
			slicesByName[existingSlice.Name] = epSlice
			sliceNamesToUpdate.Insert(epSlice.Name)
		} else {
			// slices with no changes will be useful if there are leftover endpoints
			// 完全未变的切片留作第 3 步安置剩余 Endpoint 的候选。
			sliceNamesUnchanged.Insert(existingSlice.Name)
		}
	}

	// 此时期望集合中剩下的都是尚未被安置的 Endpoint，即本组新增数量。
	numAdded := desiredSet.Len()

	// 2. If we still have desired endpoints to add and slices marked for update,
	//    iterate through the slices and fill them up with the desired endpoints.
	// 第二步：若还有剩余 Endpoint 且存在待更新切片，则按切片长度从大到小排序后依次填满。
	if desiredSet.Len() > 0 && sliceNamesToUpdate.Len() > 0 {
		slices := []*discovery.EndpointSlice{}
		for _, sliceName := range sliceNamesToUpdate.UnsortedList() {
			slices = append(slices, slicesByName[sliceName])
		}
		// Sort endpoint slices by length so we're filling up the fullest ones
		// first.
		// 优先填已经较满的切片，减少被占用的切片总数。
		sort.Sort(endpointSliceEndpointLen(slices))

		// Iterate through slices and fill them up with desired endpoints.
		// 逐个弹出期望 Endpoint，直到切片达到上限或期望集合为空。
		for _, slice := range slices {
			for desiredSet.Len() > 0 && len(slice.Endpoints) < int(r.maxEndpointsPerSlice) {
				endpoint, _ := desiredSet.PopAny()
				slice.Endpoints = append(slice.Endpoints, *endpoint)
			}
		}
	}

	// 3. If there are still desired endpoints left at this point, we try to fit
	//    the endpoints in a single existing slice. If there are no slices with
	//    that capacity, we create new slices for the endpoints.
	// 第三步：仍有剩余 Endpoint 时，优先找一个未变切片塞下，否则新建切片。
	slicesToCreate := []*discovery.EndpointSlice{}

	for desiredSet.Len() > 0 {
		var sliceToFill *discovery.EndpointSlice

		// If the remaining amounts of endpoints is smaller than the max
		// endpoints per slice and we have slices that haven't already been
		// filled, try to fit them in one.
		// 剩余数量小于单切片上限且有未变切片时，找一个有足够容量的切片复用。
		if desiredSet.Len() < int(r.maxEndpointsPerSlice) && sliceNamesUnchanged.Len() > 0 {
			unchangedSlices := []*discovery.EndpointSlice{}
			for _, sliceName := range sliceNamesUnchanged.UnsortedList() {
				unchangedSlices = append(unchangedSlices, slicesByName[sliceName])
			}
			sliceToFill = getSliceToFill(unchangedSlices, desiredSet.Len(), int(r.maxEndpointsPerSlice))
		}

		// If we didn't find a sliceToFill, generate a new empty one.
		// 找不到可复用切片就新建一个空切片；复用则必须深拷贝后再改。
		if sliceToFill == nil {
			sliceToFill = newEndpointSlice(logger, service, endpointMeta, r.controllerName)
		} else {
			// deep copy required to modify this slice.
			sliceToFill = sliceToFill.DeepCopy()
			slicesByName[sliceToFill.Name] = sliceToFill
		}

		// Fill the slice up with remaining endpoints.
		// 把剩余 Endpoint 填入该切片，直到上限或清空。
		for desiredSet.Len() > 0 && len(sliceToFill.Endpoints) < int(r.maxEndpointsPerSlice) {
			endpoint, _ := desiredSet.PopAny()
			sliceToFill.Endpoints = append(sliceToFill.Endpoints, *endpoint)
		}

		// New slices will not have a Name set, use this to determine whether
		// this should be an update or create.
		// 新切片没有 Name，据此区分是更新现有切片还是创建新切片。
		if sliceToFill.Name != "" {
			sliceNamesToUpdate.Insert(sliceToFill.Name)
			sliceNamesUnchanged.Delete(sliceToFill.Name)
		} else {
			slicesToCreate = append(slicesToCreate, sliceToFill)
		}
	}

	// Build slicesToUpdate from slice names.
	// 根据名字集合还原出最终的待更新切片列表。
	slicesToUpdate := []*discovery.EndpointSlice{}
	for _, sliceName := range sliceNamesToUpdate.UnsortedList() {
		slicesToUpdate = append(slicesToUpdate, slicesByName[sliceName])
	}

	// Build slicesToDelete from slice names.
	// 根据名字集合还原出最终的待删除切片列表。
	slicesToDelete := []*discovery.EndpointSlice{}
	for _, sliceName := range sliceNamesToDelete.UnsortedList() {
		slicesToDelete = append(slicesToDelete, slicesByName[sliceName])
	}

	// 返回增删改切片及新增/移除计数，供上层汇总与实际执行。
	return slicesToCreate, slicesToUpdate, slicesToDelete, numAdded, numRemoved
}

// DeleteService 在 Service 被删除时清理其对应的指标缓存条目。
func (r *Reconciler) DeleteService(namespace, name string) {
	r.metricsCache.DeleteService(types.NamespacedName{Namespace: namespace, Name: name})
}

// GetControllerName 返回本控制器的名称，用于标注与识别其管理的 EndpointSlice。
func (r *Reconciler) GetControllerName() string {
	return r.controllerName
}

// ManagedByChanged returns true if one of the provided EndpointSlices is
// managed by the EndpointSlice controller while the other is not.
// ManagedByChanged 判断两个 EndpointSlice 中是否恰好只有一个由本控制器管理，
// 即管理权发生了变化（常用于过滤旧控制器与新控制器之间的更新事件）。
func (r *Reconciler) ManagedByChanged(endpointSlice1, endpointSlice2 *discovery.EndpointSlice) bool {
	return r.ManagedByController(endpointSlice1) != r.ManagedByController(endpointSlice2)
}

// ManagedByController returns true if the controller of the provided
// EndpointSlices is the EndpointSlice controller.
// ManagedByController 通过 managed-by 标签判断给定 EndpointSlice 是否由本控制器管理。
func (r *Reconciler) ManagedByController(endpointSlice *discovery.EndpointSlice) bool {
	managedBy := endpointSlice.Labels[discovery.LabelManagedBy]
	return managedBy == r.controllerName
}
