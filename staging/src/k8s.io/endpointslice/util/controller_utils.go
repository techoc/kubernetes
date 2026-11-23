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

// util 包提供 EndpointSlice 控制器在 Pod、Service 与 EndpointSlice 同步过程中使用的公共工具函数。
package util

import (
	"encoding/hex"
	"fmt"
	"hash"
	"hash/fnv"
	"reflect"
	"sort"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	v1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/dump"
)

// semanticIgnoreResourceVersion does semantic deep equality checks for objects
// but excludes ResourceVersion of ObjectReference. They are used when comparing
// endpoints in Endpoints and EndpointSlice objects to avoid unnecessary updates
// caused by Pod resourceVersion change.
// semanticIgnoreResourceVersion 在比较 ObjectReference 时忽略 ResourceVersion，避免因 Pod 版本变化产生无谓的 Endpoint 更新。
var semanticIgnoreResourceVersion = conversion.EqualitiesOrDie(
	func(a, b v1.ObjectReference) bool {
		a.ResourceVersion = ""
		b.ResourceVersion = ""
		return a == b
	},
)

// PodProjectionKey encapsulates all pod information required to find services that may need their endpoints to be updated.
// PodProjectionKey 封装定位需要刷新 endpoint 的 Service 所需的全部 Pod 信息。
// 其中 Namespace 是 Pod 所在命名空间；Labels 是 Pod 当前标签；
// OldLabels 在 Pod 标签变化的 Update 事件中记录旧标签；PodChanged 表示 Pod 字段变化是否影响其 endpoint 归属。
type PodProjectionKey struct {
	Namespace  string
	Labels     labels.Set // this is set to the pod's current labels
	OldLabels  labels.Set // this is set if the pod's labels changed (in an Update event)
	PodChanged bool       // set to true if the pod's fields changed in a way that may affect its endpoints membership
}

// GetPodUpdateProjectionKey 根据 Pod 的新旧对象生成投影键，用于找出可能需要更新 endpoint 的 Service。
func GetPodUpdateProjectionKey(oldObj, newObj interface{}) *PodProjectionKey {
	newPod := getPodFromObject(newObj)
	oldPod := getPodFromObject(oldObj)
	// 新旧对象都为 nil 属于异常事件，记录错误后忽略。
	if newPod == nil && oldPod == nil {
		utilruntime.HandleError(fmt.Errorf("unexpected pod event with both old/new values as nil, ignoring"))
		return nil
	}
	// 仅有新对象（Add 事件）：直接用新 Pod 的命名空间与标签构造投影键。
	if oldPod == nil {
		return &PodProjectionKey{
			Namespace: newPod.Namespace,
			Labels:    labels.Set(newPod.Labels),
		}
	}
	// 仅有旧对象（Delete 事件）：用旧 Pod 的信息构造投影键。
	if newPod == nil {
		return &PodProjectionKey{
			Namespace: oldPod.Namespace,
			Labels:    labels.Set(oldPod.Labels),
		}
	}

	// If we reached here, both old/new pod objects are non-nil, so it's an update event.
	// Safe to ignore pod informer resync events as service informer already handles resync for all services.
	// 若新旧 ResourceVersion 相同，说明是 informer 重同步事件，Service informer 已处理，可直接忽略。
	if newPod.ResourceVersion == oldPod.ResourceVersion {
		return nil
	}

	// 判断 Pod 字段与标签是否发生了变化。
	podChanged, labelsChanged := podEndpointsChanged(oldPod, newPod)

	// If both the pod and labels are unchanged, no update is needed.
	// 两者都未变化时无需更新任何 Service。
	if !podChanged && !labelsChanged {
		return nil
	}

	// If only the pod has changed, projection key can be created with just the new pod.
	// 仅 Pod 字段变化时，用新 Pod 构造投影键即可。
	if !labelsChanged {
		return &PodProjectionKey{
			Namespace: newPod.Namespace,
			Labels:    labels.Set(newPod.Labels),
		}
	}

	// The pod labels have changed, so the projection key needs to include both its old/new labels.
	// Additionally, PodChanged field indicates if union/diff of matching services should be reconciled.
	// 标签变化时需要同时保留新旧标签，并通过 PodChanged 决定取匹配 Service 的并集还是差集。
	return &PodProjectionKey{
		Namespace:  newPod.Namespace,
		Labels:     labels.Set(newPod.Labels),
		OldLabels:  labels.Set(oldPod.Labels),
		PodChanged: podChanged,
	}
}

// GetServicesToUpdate 返回因 Pod 变化而需要更新 endpoint 的 Service 键集合。
func GetServicesToUpdate(serviceLister v1listers.ServiceLister, key *PodProjectionKey) (sets.Set[string], error) {
	// 投影键为 nil 表示无需更新。
	if key == nil {
		return nil, nil
	}

	// 先找出与新标签匹配的 Service。
	services, err := getServicesForPod(serviceLister, key.Namespace, key.Labels)
	if err != nil {
		return nil, err
	}

	// If the pod's labels changed in an update event, we'll need to consider all the affected services.
	// 标签变化时还需考虑旧标签匹配的 Service，以覆盖变化前后的状态。
	if key.OldLabels != nil {
		oldServices, err := getServicesForPod(serviceLister, key.Namespace, key.OldLabels)
		if err != nil {
			return nil, err
		}

		services = determineNeededServiceUpdates(oldServices, services, key.PodChanged)
		// 根据 Pod 是否变化，取新旧 Service 集合的并集或对称差集。
	}

	return services, nil
}

// getServicesForPod returns a set of services matching the given pod's namespace and labels (via service selector).
// getServicesForPod 列出指定命名空间中所有 selector 与给定 Pod 标签匹配的 Service。
func getServicesForPod(serviceLister v1listers.ServiceLister, namespace string, podLabels labels.Set) (sets.Set[string], error) {
	// 从 lister 中取出该命名空间下的全部 Service。
	services, err := serviceLister.Services(namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	set := sets.Set[string]{}
	for _, service := range services {
		if service.Spec.Selector == nil {
			// If the service has a nil selector this means selectors match nothing, not everything.
			continue
			// selector 为 nil 表示不匹配任何 Pod，直接跳过。
		}

		if labels.ValidatedSetSelector(service.Spec.Selector).Matches(podLabels) {
			// selector 匹配时，用命名空间与名称作为键加入结果集。
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(service)
			if err != nil {
				return nil, err
			}
			set.Insert(key)
		}
	}
	return set, nil
}

// PortMapKey is used to uniquely identify groups of endpoint ports.
// PortMapKey 是端口分组的唯一标识，用于在 EndpointSlice 之间复用端口配置。
type PortMapKey string

// NewPortMapKey generates a PortMapKey from endpoint ports.
// NewPortMapKey 对端点端口排序后计算哈希，生成与端口顺序无关的 PortMapKey。
func NewPortMapKey(endpointPorts []discovery.EndpointPort) PortMapKey {
	// Normalize nil to empty slice so they hash the same.
	// 将 nil 归一化为空切片，保证 nil 与空切片得到相同键。
	if endpointPorts == nil {
		endpointPorts = []discovery.EndpointPort{}
	}
	// 排序消除端口顺序差异，再对规范化后的对象做深度哈希。
	sort.Sort(portsInOrder(endpointPorts))
	return PortMapKey(deepHashObjectToString(endpointPorts))
}

// deepHashObjectToString creates a unique hash string from a go object.
// deepHashObjectToString 使用 FNV-1a 128 位哈希生成对象的十六进制摘要字符串。
func deepHashObjectToString(objectToWrite interface{}) string {
	// 128 位 FNV 哈希提供足够低的碰撞概率。
	hasher := fnv.New128a()
	deepHashObject(hasher, objectToWrite)
	// 将全部 16 字节摘要编码为十六进制字符串。
	return hex.EncodeToString(hasher.Sum(nil)[0:])
}

// ShouldPodBeInEndpoints returns true if a specified pod should be in an
// Endpoints or EndpointSlice resource. Terminating pods are only included if
// includeTerminating is true.
// ShouldPodBeInEndpoints 判断 Pod 是否应出现在 Endpoints/EndpointSlice 中；
// includeTerminating 控制是否包含正在终止（带删除时间戳）的 Pod。
func ShouldPodBeInEndpoints(pod *v1.Pod, includeTerminating bool) bool {
	// "Terminal" describes when a Pod is complete (in a succeeded or failed phase).
	// This is distinct from the "Terminating" condition which represents when a Pod
	// is being terminated (metadata.deletionTimestamp is non nil).
	// 终态 Pod（Succeeded/Failed）永远不应出现在 endpoint 中。
	if isPodTerminal(pod) {
		return false
	}

	if len(pod.Status.PodIP) == 0 && len(pod.Status.PodIPs) == 0 {
		// 没有任何 Pod IP 的对象无法作为 endpoint 地址。
		return false
	}

	if !includeTerminating && pod.DeletionTimestamp != nil {
		// 除非显式包含终止中的 Pod，否则带删除时间戳的 Pod 应被排除。
		return false
	}

	return true
}

// ShouldSetHostname returns true if the Hostname attribute should be set on an
// Endpoints Address or EndpointSlice Endpoint.
// ShouldSetHostname 判断是否为 Endpoint 设置 hostname：需同时满足 Pod 有 hostname、subdomain 与 Service 名称一致且命名空间相同。
func ShouldSetHostname(pod *v1.Pod, svc *v1.Service) bool {
	// hostname 与 subdomain 必须同时匹配 Service 的名称和命名空间。
	return len(pod.Spec.Hostname) > 0 && pod.Spec.Subdomain == svc.Name && svc.Namespace == pod.Namespace
}

// podEndpointsChanged returns two boolean values. The first is true if the pod has
// changed in a way that may change existing endpoints. The second value is true if the
// pod has changed in a way that may affect which Services it matches.
// podEndpointsChanged 返回两个布尔值：Pod 字段变化是否影响 endpoint，以及标签变化是否影响 Service 匹配。
func podEndpointsChanged(oldPod, newPod *v1.Pod) (bool, bool) {
	// Check if the pod labels have changed, indicating a possible
	// change in the service membership
	// 标签或 hostname/subdomain 变化都可能改变 Pod 匹配的 Service 集合。
	labelsChanged := false
	if !reflect.DeepEqual(newPod.Labels, oldPod.Labels) ||
		!hostNameAndDomainAreEqual(newPod, oldPod) {
		labelsChanged = true
	}

	// If the pod's deletion timestamp is set, remove endpoint from ready address.
	// 删除时间戳变化意味着 endpoint 需要从未就绪地址中移除。
	if newPod.DeletionTimestamp != oldPod.DeletionTimestamp {
		return true, labelsChanged
	}
	// If the pod's readiness has changed, the associated endpoint address
	// will move from the unready endpoints set to the ready endpoints.
	// So for the purposes of an endpoint, a readiness change on a pod
	// means we have a changed pod.
	// 就绪状态变化会使地址在 ready/unready 集合之间迁移。
	if IsPodReady(oldPod) != IsPodReady(newPod) {
		return true, labelsChanged
	}

	// Check if the pod IPs have changed
	// 逐个比较 PodIPs，数量或任一 IP 不同都视为 endpoint 变化。
	if len(oldPod.Status.PodIPs) != len(newPod.Status.PodIPs) {
		return true, labelsChanged
	}
	for i := range oldPod.Status.PodIPs {
		if oldPod.Status.PodIPs[i].IP != newPod.Status.PodIPs[i].IP {
			return true, labelsChanged
		}
	}

	// Endpoints may also reference a pod's Name, Namespace, UID, and NodeName, but
	// the first three are immutable, and NodeName is immutable once initially set,
	// which happens before the pod gets an IP.
	// 其余被 endpoint 引用的字段（Name/Namespace/UID/NodeName）在 Pod 获得 IP 后不可变，无需比较。

	return false, labelsChanged
}

// getPodFromObject 从 informer 事件中取出 Pod 对象，兼容 DeletedFinalStateUnknown 墓碑。
func getPodFromObject(obj interface{}) *v1.Pod {
	if obj == nil {
		// nil 对象直接返回 nil。
		return nil
	}
	if pod, ok := obj.(*v1.Pod); ok {
		// 常规情况下事件对象就是 *v1.Pod。
		return pod
	}
	// If we reached here it means the pod was deleted but its final state is unrecorded.
	// 删除事件可能只留下墓碑，需要从中解出 Pod；类型不符则记录错误。
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
		return nil
	}
	pod, ok := tombstone.Obj.(*v1.Pod)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a Pod: %#v", obj))
		return nil
	}
	return pod
}

// hostNameAndDomainAreEqual 比较两个 Pod 的 hostname 与 subdomain 是否一致。
func hostNameAndDomainAreEqual(pod1, pod2 *v1.Pod) bool {
	return pod1.Spec.Hostname == pod2.Spec.Hostname &&
		pod1.Spec.Subdomain == pod2.Spec.Subdomain
}

// determineNeededServiceUpdates 根据 Pod 是否变化，计算新旧 Service 集合中需要重算的键。
// podChanged 为 true 时取并集，否则取对称差集。
func determineNeededServiceUpdates(oldServices, services sets.Set[string], podChanged bool) sets.Set[string] {
	if podChanged {
		// Pod 字段变化时，新旧匹配到的 Service 都可能需要重算。
		// if the labels and pod changed, all services need to be updated
		services = services.Union(oldServices)
	} else {
		// 仅标签变化时，只处理只属于新旧其中一方的 Service。
		// if only the labels changed, services not common to both the new
		// and old service set (the disjuntive union) need to be updated
		services = services.Difference(oldServices).Union(oldServices.Difference(services))
	}
	return services
}

// portsInOrder helps sort endpoint ports in a consistent way for hashing.
// portsInOrder 实现 sort.Interface，用于按稳定顺序排列端口以便生成一致的哈希；
// 其中 Len 返回端口数量，Swap 交换两个端口，Less 按端口的哈希字符串决定顺序。
type portsInOrder []discovery.EndpointPort

func (sl portsInOrder) Len() int      { return len(sl) }
func (sl portsInOrder) Swap(i, j int) { sl[i], sl[j] = sl[j], sl[i] }
func (sl portsInOrder) Less(i, j int) bool {
	// 对每个端口单独计算哈希以确定稳定的排序键。
	h1 := deepHashObjectToString(sl[i])
	h2 := deepHashObjectToString(sl[j])
	// 按哈希字符串的字典序排列。
	return h1 < h2
}

// EndpointsEqualBeyondHash returns true if endpoints have equal attributes
// but excludes equality checks that would have already been covered with
// endpoint hashing (see hashEndpoint func for more info) and ignores difference
// in ResourceVersion of TargetRef.
// EndpointsEqualBeyondHash 比较 endpoint 中未被哈希覆盖的属性，并忽略 TargetRef 的 ResourceVersion 差异。
func EndpointsEqualBeyondHash(ep1, ep2 *discovery.Endpoint) bool {
	if stringPtrChanged(ep1.NodeName, ep2.NodeName) {
		// NodeName 属于拓扑属性，变化需要更新。
		return false
	}

	if stringPtrChanged(ep1.Zone, ep2.Zone) {
		// Zone 变化需要更新。
		return false
	}

	if boolPtrChanged(ep1.Conditions.Ready, ep2.Conditions.Ready) {
		// Ready 条件变化需要更新。
		return false
	}

	if boolPtrChanged(ep1.Conditions.Serving, ep2.Conditions.Serving) {
		// Serving 条件变化需要更新。
		return false
	}

	if boolPtrChanged(ep1.Conditions.Terminating, ep2.Conditions.Terminating) {
		// Terminating 条件变化需要更新。
		return false
	}

	if !semanticIgnoreResourceVersion.DeepEqual(ep1.TargetRef, ep2.TargetRef) {
		// TargetRef 比较时忽略 ResourceVersion。
		return false
	}

	return true
}

// boolPtrChanged returns true if a set of bool pointers have different values.
// boolPtrChanged 判断两个布尔指针是否不同（包括 nil 与非 nil 的差异）。
func boolPtrChanged(ptr1, ptr2 *bool) bool {
	if (ptr1 == nil) != (ptr2 == nil) {
		return true
	}
	if ptr1 != nil && ptr2 != nil && *ptr1 != *ptr2 {
		return true
	}
	return false
}

// stringPtrChanged returns true if a set of string pointers have different values.
// stringPtrChanged 判断两个字符串指针是否不同（包括 nil 与非 nil 的差异）。
func stringPtrChanged(ptr1, ptr2 *string) bool {
	if (ptr1 == nil) != (ptr2 == nil) {
		return true
	}
	if ptr1 != nil && ptr2 != nil && *ptr1 != *ptr2 {
		return true
	}
	return false
}

// DeepHashObject writes specified object to hash using the spew library
// which follows pointers and prints actual values of the nested objects
// ensuring the hash does not change when a pointer changes.
// copied from k8s.io/kubernetes/pkg/util/hash
// deepHashObject 借助 dump.ForHash 展开指针指向的实际值，保证指针变化不影响哈希结果。
func deepHashObject(hasher hash.Hash, objectToWrite interface{}) {
	// 重置哈希状态，避免复用时累积上一次的内容。
	hasher.Reset()
	// 写入规范化后的对象表示。
	fmt.Fprint(hasher, dump.ForHash(objectToWrite))
}

// IsPodReady returns true if Pods Ready condition is true
// copied from k8s.io/kubernetes/pkg/api/v1/pod
// IsPodReady 返回 Pod 的 Ready 条件是否为 True。
func IsPodReady(pod *v1.Pod) bool {
	// 从 PodStatus 中提取 Ready 条件并判断。
	return isPodReadyConditionTrue(pod.Status)
}

// IsPodTerminal returns true if a pod is terminal, all containers are stopped and cannot ever regress.
// copied from k8s.io/kubernetes/pkg/api/v1/pod
// isPodTerminal 返回 Pod 是否处于终态（所有容器已停止且不可恢复）。
func isPodTerminal(pod *v1.Pod) bool {
	return isPodPhaseTerminal(pod.Status.Phase)
}

// IsPodPhaseTerminal returns true if the pod's phase is terminal.
// copied from k8s.io/kubernetes/pkg/api/v1/pod
// isPodPhaseTerminal 返回 Pod Phase 是否为 Failed 或 Succeeded。
func isPodPhaseTerminal(phase v1.PodPhase) bool {
	return phase == v1.PodFailed || phase == v1.PodSucceeded
}

// IsPodReadyConditionTrue returns true if a pod is ready; false otherwise.
// copied from k8s.io/kubernetes/pkg/api/v1/pod
// isPodReadyConditionTrue 返回 PodStatus 中 Ready 条件的状态是否为 True。
func isPodReadyConditionTrue(status v1.PodStatus) bool {
	condition := getPodReadyCondition(&status)
	// 条件不存在时视为未就绪。
	return condition != nil && condition.Status == v1.ConditionTrue
}

// getPodReadyCondition extracts the pod ready condition from the given status and returns that.
// Returns nil if the condition is not present.
// copied from k8s.io/kubernetes/pkg/api/v1/pod
// getPodReadyCondition 从 PodStatus 中查找 Ready 条件，不存在时返回 nil。
func getPodReadyCondition(status *v1.PodStatus) *v1.PodCondition {
	if status == nil || status.Conditions == nil {
		// 状态或条件列表为空时直接返回 nil。
		return nil
	}

	for i := range status.Conditions {
		// 遍历条件列表寻找类型为 PodReady 的项。
		if status.Conditions[i].Type == v1.PodReady {
			return &status.Conditions[i]
		}
	}
	return nil
}
