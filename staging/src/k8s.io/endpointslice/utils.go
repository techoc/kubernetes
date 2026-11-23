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

// Package endpointslice 提供从 Service 与 Pod 构建 EndpointSlice 的辅助函数，
// 涵盖端点构造、端口解析、标签管理、地址类型推断等逻辑。
package endpointslice

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	endpointutil "k8s.io/endpointslice/util"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// podToEndpoint returns an Endpoint object generated from a Pod, a Node, and a Service for a particular addressType.
// podToEndpoint 根据 Pod、Node、Service 和指定地址类型生成对应的 Endpoint。
// Serving 取 Pod 自身就绪状态；只要设置了删除时间戳就视为 Terminating；
// Ready 还受 publishNotReadyAddresses 影响，并按需要填充 NodeName、Zone 与 Hostname。
func podToEndpoint(pod *v1.Pod, node *v1.Node, service *v1.Service, addressType discovery.AddressType) discovery.Endpoint {
	// Serving 直接反映 Pod 自身的就绪状态。
	serving := endpointutil.IsPodReady(pod)
	// 只要 Pod 带删除时间戳，就认为它处于终止中。
	terminating := pod.DeletionTimestamp != nil
	// For compatibility reasons, "ready" should never be "true" if a pod is terminatng, unless
	// publishNotReadyAddresses was set.
	// 兼容考虑：终止中的 Pod 不应被标记为 ready，除非设置了 publishNotReadyAddresses。
	ready := service.Spec.PublishNotReadyAddresses || (serving && !terminating)
	// 构造 Endpoint：地址按地址类型从 Pod 状态提取，条件含 Ready/Serving/Terminating，
	// TargetRef 指向作为来源的 Pod。
	ep := discovery.Endpoint{
		Addresses: getEndpointAddresses(pod.Status, service, addressType),
		Conditions: discovery.EndpointConditions{
			Ready:       &ready,
			Serving:     &serving,
			Terminating: &terminating,
		},
		TargetRef: &v1.ObjectReference{
			Kind:      "Pod",
			Namespace: pod.ObjectMeta.Namespace,
			Name:      pod.ObjectMeta.Name,
			UID:       pod.ObjectMeta.UID,
		},
	}

	// Pod 已调度到节点时记录节点名。
	if pod.Spec.NodeName != "" {
		ep.NodeName = &pod.Spec.NodeName
	}

	// 从 Node 的 zone 标签写入拓扑区域信息。
	if node != nil && node.Labels[v1.LabelTopologyZone] != "" {
		ep.Zone = ptr.To(node.Labels[v1.LabelTopologyZone])
	}

	// 需要时（如无头 Service 的 Pod 自定义 hostname）设置主机名。
	if endpointutil.ShouldSetHostname(pod, service) {
		ep.Hostname = &pod.Spec.Hostname
	}

	return ep
}

// getEndpointPorts returns a list of EndpointPorts generated from a Service
// and Pod.
// getEndpointPorts 根据 Service 与 Pod 生成 EndpointPort 列表。
// 无头 Service 允许没有端口；对每个 ServicePort 调用 FindPort 解析容器端口，
// 解析失败时记录日志并跳过该端口，不阻断其余端口。
func getEndpointPorts(logger klog.Logger, service *v1.Service, pod *v1.Pod) []discovery.EndpointPort {
	// 初始化为空切片，而非 nil，保证返回值结构稳定。
	endpointPorts := []discovery.EndpointPort{}

	// Allow headless service not to have ports.
	// 无头 Service 没有端口是合法的，直接返回空列表。
	if len(service.Spec.Ports) == 0 && service.Spec.ClusterIP == v1.ClusterIPNone {
		return endpointPorts
	}

	// 逐个解析 Service 端口。
	for i := range service.Spec.Ports {
		servicePort := &service.Spec.Ports[i]

		// 解析该 ServicePort 对应的容器端口，失败则跳过。
		portNum, err := FindPort(pod, servicePort)
		if err != nil {
			logger.V(4).Info("Failed to find port for service", "service", klog.KObj(service), "err", err)
			continue
		}

		// 复制名称、端口、协议与应用协议，生成 EndpointPort。
		endpointPorts = append(endpointPorts, discovery.EndpointPort{
			Name:        ptr.To(servicePort.Name),
			Port:        ptr.To(int32(portNum)),
			Protocol:    ptr.To(servicePort.Protocol),
			AppProtocol: servicePort.AppProtocol,
		})
	}

	return endpointPorts
}

// getEndpointAddresses returns a list of addresses generated from a pod status.
// getEndpointAddresses 从 Pod 状态中按指定地址类型（IPv4/IPv6）提取地址列表。
// 为避免不规则格式的 IP，会先解析再字符串化，只保留与地址类型匹配的地址。
func getEndpointAddresses(podStatus v1.PodStatus, service *v1.Service, addressType discovery.AddressType) []string {
	// 初始化为空的地址列表。
	addresses := []string{}

	// 遍历 Pod 的所有 IP，按地址族筛选。
	for _, podIP := range podStatus.PodIPs {
		// We parse and restringify the pod IP in case it is in an irregular format.
		// 先宽松解析再输出规范字符串，以统一不规则格式的 IP。
		ip := utilnet.ParseIPSloppy(podIP.IP)
		isIPv6PodIP := utilnet.IsIPv6(ip)
		// IPv6 Pod IP 仅在目标地址类型为 IPv6 时收集。
		if isIPv6PodIP && addressType == discovery.AddressTypeIPv6 {
			addresses = append(addresses, ip.String())
		}

		// IPv4 Pod IP 仅在目标地址类型为 IPv4 时收集。
		if !isIPv6PodIP && addressType == discovery.AddressTypeIPv4 {
			addresses = append(addresses, ip.String())
		}
	}

	return addresses
}

// newEndpointSlice returns an EndpointSlice generated from a service and
// endpointMeta.
// newEndpointSlice 根据 Service 与 endpointMeta 构造一个新的 EndpointSlice。
// 设置 GenerateName、OwnerReferences、Namespace、Ports、AddressType 等字段，
// 最后通过 setEndpointSliceLabels 生成标签。
func newEndpointSlice(logger klog.Logger, service *v1.Service, endpointMeta *endpointMeta, controllerName string) *discovery.EndpointSlice {
	// 用 Service 的 GVK 生成指向该 Service 的控制器 OwnerReference。
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	ownerRef := metav1.NewControllerRef(service, gvk)
	// 初始化空 slice：标签为空 map、名称前缀由 Service 名派生、端点列表为空。
	epSlice := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          map[string]string{},
			GenerateName:    getEndpointSlicePrefix(service.Name),
			OwnerReferences: []metav1.OwnerReference{*ownerRef},
			Namespace:       service.Namespace,
		},
		Ports:       endpointMeta.ports,
		AddressType: endpointMeta.addressType,
		Endpoints:   []discovery.Endpoint{},
	}
	// add parent service labels
	// 复制父 Service 的标签（保留标签由 setEndpointSliceLabels 覆盖）。
	epSlice.Labels, _ = setEndpointSliceLabels(logger, epSlice, service, controllerName)

	return epSlice
}

// getEndpointSlicePrefix returns a suitable prefix for an EndpointSlice name.
// getEndpointSlicePrefix 返回适合作为 EndpointSlice 名称的前缀。
// 名称合法时带尾部连字符以使生成名更美观，否则退回 Service 名本身。
func getEndpointSlicePrefix(serviceName string) string {
	// use the dash (if the name isn't too long) to make the pod name a bit prettier
	// 默认加连字符；若生成的前缀不是合法 DNS 子域名则去掉连字符。
	prefix := fmt.Sprintf("%s-", serviceName)
	if len(apimachineryvalidation.NameIsDNSSubdomain(prefix, true)) != 0 {
		prefix = serviceName
	}
	return prefix
}

// ownedBy returns true if the provided EndpointSlice is owned by the provided
// Service.
// ownedBy 判断给定 EndpointSlice 是否由给定 Service 所有。
// 通过逐一比对 OwnerReferences 中的 UID、Kind 与 APIVersion 来确定。
func ownedBy(endpointSlice *discovery.EndpointSlice, svc *v1.Service) bool {
	// 遍历所有 OwnerReference，匹配 Service 的 UID 与 GVK 即视为归属。
	for _, o := range endpointSlice.OwnerReferences {
		if o.UID == svc.UID && o.Kind == "Service" && o.APIVersion == "v1" {
			return true
		}
	}
	return false
}

// getSliceToFill will return the EndpointSlice that will be closest to full
// when numEndpoints are added. If no EndpointSlice can be found, a nil pointer
// will be returned.
// getSliceToFill 返回加入 numEndpoints 后最接近填满（maxEndpoints）的 EndpointSlice；
// 若没有 slice 能容纳则返回 nil，若恰好填满则立即返回该 slice。
func getSliceToFill(endpointSlices []*discovery.EndpointSlice, numEndpoints, maxEndpoints int) (slice *discovery.EndpointSlice) {
	// 记录当前最接近填满的剩余容量差值，初始为最大容量。
	closestDiff := maxEndpoints
	var closestSlice *discovery.EndpointSlice
	// 遍历所有 slice，计算加入新端点后的剩余空间。
	for _, endpointSlice := range endpointSlices {
		currentDiff := maxEndpoints - (numEndpoints + len(endpointSlice.Endpoints))
		// 只考虑放得下（差值非负）且更接近填满的 slice。
		if currentDiff >= 0 && currentDiff < closestDiff {
			closestDiff = currentDiff
			closestSlice = endpointSlice
			// 恰好填满，无需继续查找。
			if closestDiff == 0 {
				return closestSlice
			}
		}
	}
	return closestSlice
}

// addTriggerTimeAnnotation adds a triggerTime annotation to an EndpointSlice
// addTriggerTimeAnnotation 为 EndpointSlice 写入或清除 triggerTime 注解。
// triggerTime 非零时以 UTC 的 RFC3339Nano 格式写入，为零时删除该注解。
func addTriggerTimeAnnotation(endpointSlice *discovery.EndpointSlice, triggerTime time.Time) {
	// 注解 map 为空时先创建。
	if endpointSlice.Annotations == nil {
		endpointSlice.Annotations = make(map[string]string)
	}

	// 有新的触发时间则写入；否则清除旧注解。
	if !triggerTime.IsZero() {
		endpointSlice.Annotations[v1.EndpointsLastChangeTriggerTime] = triggerTime.UTC().Format(time.RFC3339Nano)
	} else { // No new trigger time, clear the annotation.
		delete(endpointSlice.Annotations, v1.EndpointsLastChangeTriggerTime)
	}
}

// ServiceControllerKey returns a controller key for a Service but derived from
// an EndpointSlice.
// ServiceControllerKey 从 EndpointSlice 推导出对应 Service 的 controller key（namespace/name）。
// slice 为 nil 或缺少 service-name 标签时返回错误。
func ServiceControllerKey(endpointSlice *discovery.EndpointSlice) (string, error) {
	// 空 slice 无法推导，直接报错。
	if endpointSlice == nil {
		return "", fmt.Errorf("nil EndpointSlice passed to ServiceControllerKey()")
	}
	// 必须存在非空的 service-name 标签。
	serviceName, ok := endpointSlice.Labels[discovery.LabelServiceName]
	if !ok || serviceName == "" {
		return "", fmt.Errorf("EndpointSlice missing %s label", discovery.LabelServiceName)
	}
	// 用 slice 所在命名空间与标签中的 Service 名拼接 key。
	return fmt.Sprintf("%s/%s", endpointSlice.Namespace, serviceName), nil
}

// setEndpointSliceLabels returns a map with the new endpoint slices labels and true if there was an update.
// Slices labels must be equivalent to the Service labels except for the reserved IsHeadlessService, LabelServiceName and LabelManagedBy labels
// Changes to IsHeadlessService, LabelServiceName and LabelManagedBy labels on the Service do not result in updates to EndpointSlice labels.
// setEndpointSliceLabels 返回 EndpointSlice 应使用的新标签集合，以及标签是否发生了变化。
// 除保留标签（IsHeadlessService、LabelServiceName、LabelManagedBy）外，slice 标签应与 Service 标签一致；
// 对这些保留标签的改动不会触发 slice 标签更新。
func setEndpointSliceLabels(logger klog.Logger, epSlice *discovery.EndpointSlice, service *v1.Service, controllerName string) (map[string]string, bool) {
	// updated 标记非保留标签是否发生变化；epLabels/svcLabels 分别保存待比较的两组标签。
	updated := false
	epLabels := make(map[string]string)
	svcLabels := make(map[string]string)

	// check if the endpoint slice and the service have the same labels
	// clone current slice labels except the reserved labels
	// 复制 slice 现有标签，但跳过保留标签。
	for key, value := range epSlice.Labels {
		if isReservedLabelKey(key) {
			continue
		}
		// copy endpoint slice labels
		epLabels[key] = value
	}

	// 复制 Service 标签，同样跳过并记录保留标签（防止 Service 劫持 slice 标签）。
	for key, value := range service.Labels {
		if isReservedLabelKey(key) {
			logger.Info("Service using reserved endpoint slices label", "service", klog.KObj(service), "skipping", key, "label", value)
			continue
		}
		// copy service labels
		svcLabels[key] = value
	}

	// if the labels are not identical update the slice with the corresponding service labels
	// 两组非保留标签不一致时需要更新。
	if !apiequality.Semantic.DeepEqual(epLabels, svcLabels) {
		updated = true
	}

	// add or remove headless label depending on the service Type
	// 无 ClusterIP 的 Service 写入 headless 标签，否则删除该标签。
	if !isServiceIPSet(service) {
		svcLabels[v1.IsHeadlessService] = ""
	} else {
		delete(svcLabels, v1.IsHeadlessService)
	}

	// override endpoint slices reserved labels
	// 强制覆盖保留标签，使用真实 Service 名与控制器名。
	svcLabels[discovery.LabelServiceName] = service.Name
	svcLabels[discovery.LabelManagedBy] = controllerName

	return svcLabels, updated
}

// isReservedLabelKey return true if the label is one of the reserved label for slices
// isReservedLabelKey 判断标签键是否为 EndpointSlice 的保留标签之一。
// 保留标签包括 LabelServiceName、LabelManagedBy 与 IsHeadlessService。
func isReservedLabelKey(label string) bool {
	// 命中任一保留标签即返回 true。
	if label == discovery.LabelServiceName ||
		label == discovery.LabelManagedBy ||
		label == v1.IsHeadlessService {
		return true
	}
	return false
}

// endpointSliceEndpointLen helps sort endpoint slices by the number of
// endpoints they contain.
// endpointSliceEndpointLen 是按所含 Endpoint 数量对 EndpointSlice 排序的辅助类型，
// 实现 sort.Interface：Len 返回 slice 数量，Swap 交换两个元素，
// Less 按端点数量降序比较（端点多的排在前面）。
type endpointSliceEndpointLen []*discovery.EndpointSlice

func (sl endpointSliceEndpointLen) Len() int      { return len(sl) }
func (sl endpointSliceEndpointLen) Swap(i, j int) { sl[i], sl[j] = sl[j], sl[i] }
func (sl endpointSliceEndpointLen) Less(i, j int) bool {
	return len(sl[i].Endpoints) > len(sl[j].Endpoints)
}

// returns a map of address types used by a service
// getAddressTypesForService 返回 Service 使用的地址类型集合。
// 优先依据 spec.IPFamilies 推断；为空时（集群升级/旧 apiserver 场景），
// 有 ClusterIP 的 Service 按 ClusterIP 地址族推断，headless Service 默认按双栈处理。
func getAddressTypesForService(logger klog.Logger, service *v1.Service) sets.Set[discovery.AddressType] {
	// 初始化地址类型集合。
	serviceSupportedAddresses := sets.New[discovery.AddressType]()
	// TODO: (khenidak) when address types are removed in favor of
	// v1.IPFamily this will need to be removed, and work directly with
	// v1.IPFamily types

	// IMPORTANT: we assume that IP of (discovery.AddressType enum) is never in use
	// as it gets deprecated
	// 按 IPFamilies 中声明的协议族收集地址类型（IPv4/IPv6）。
	for _, family := range service.Spec.IPFamilies {
		if family == v1.IPv4Protocol {
			serviceSupportedAddresses.Insert(discovery.AddressTypeIPv4)
		}

		if family == v1.IPv6Protocol {
			serviceSupportedAddresses.Insert(discovery.AddressTypeIPv6)
		}
	}

	// 已从 IPFamilies 得到结果则直接返回，无需回退推断。
	if serviceSupportedAddresses.Len() > 0 {
		return serviceSupportedAddresses // we have found families for this service
	}

	// TODO (khenidak) remove when (1) dual stack becomes
	// enabled by default (2) v1.19 falls off supported versions

	// Why do we need this:
	// a cluster being upgraded to the new apis
	// will have service.spec.IPFamilies: nil
	// if the controller manager connected to old api
	// server. This will have the nasty side effect of
	// removing all slices already created for this service.
	// this will disable all routing to service vip (ClusterIP)
	// this ensures that this does not happen. Same for headless services
	// we assume it is dual stack, until they get defaulted by *new* api-server
	// this ensures that traffic is not disrupted  until then. But *may*
	// include undesired families for headless services until then.

	// 回退（有 ClusterIP）：按 ClusterIP 的地址族推断地址类型，避免旧 apiserver 场景下误删 slice。
	if len(service.Spec.ClusterIP) > 0 && service.Spec.ClusterIP != v1.ClusterIPNone { // headfull
		// 默认 IPv4，ClusterIP 为 IPv6 时改为 IPv6。
		addrType := discovery.AddressTypeIPv4
		if utilnet.IsIPv6String(service.Spec.ClusterIP) {
			addrType = discovery.AddressTypeIPv6
		}
		serviceSupportedAddresses.Insert(addrType)
		logger.V(2).Info("Couldn't find ipfamilies for service. This could happen if controller manager is connected to an old apiserver that does not support ip families yet. EndpointSlices for this Service will use addressType as the IP Family based on familyOf(ClusterIP).", "service", klog.KObj(service), "addressType", addrType, "clusterIP", service.Spec.ClusterIP)
		return serviceSupportedAddresses
	}

	// headless
	// for now we assume two families. This should have minimal side effect
	// if the service is headless with no selector, then this will remain the case
	// if the service is headless with selector then chances are pods are still using single family
	// since kubelet will need to restart in order to start patching pod status with multiple ips
	// 回退（headless）：暂无 IPFamilies 时先按双栈处理，待 apiserver 默认值后再收敛。
	serviceSupportedAddresses.Insert(discovery.AddressTypeIPv4)
	serviceSupportedAddresses.Insert(discovery.AddressTypeIPv6)
	logger.V(2).Info("Couldn't find ipfamilies for headless service, likely because controller manager is likely connected to an old apiserver that does not support ip families yet. The service endpoint slice will use dual stack families until api-server default it correctly", "service", klog.KObj(service))
	return serviceSupportedAddresses
}

// unchangedSlices 返回未发生变更的 EndpointSlice 列表。
// 先收集待更新与待删除 slice 的名称，再从现有 slice 中排除这些名称。
func unchangedSlices(existingSlices, slicesToUpdate, slicesToDelete []*discovery.EndpointSlice) []*discovery.EndpointSlice {
	// 已变更（更新或删除）的 slice 名称集合。
	changedSliceNames := sets.New[string]()
	for _, slice := range slicesToUpdate {
		changedSliceNames.Insert(slice.Name)
	}
	for _, slice := range slicesToDelete {
		changedSliceNames.Insert(slice.Name)
	}
	// 从现有 slice 中过滤掉已变更的名称，保留其余部分。
	unchangedSlices := []*discovery.EndpointSlice{}
	for _, slice := range existingSlices {
		if !changedSliceNames.Has(slice.Name) {
			unchangedSlices = append(unchangedSlices, slice)
		}
	}

	return unchangedSlices
}

// hintsEnabled returns true if the provided annotations include either
// v1.AnnotationTopologyMode or v1.DeprecatedAnnotationTopologyAwareHints key
// with a value set to "Auto" or "auto". When both are set,
// v1.DeprecatedAnnotationTopologyAwareHints has precedence.
// hintsEnabled 判断注解是否启用了拓扑感知提示（值需为 "Auto" 或 "auto"）。
// 同时兼容新注解 AnnotationTopologyMode 与旧注解 DeprecatedAnnotationTopologyAwareHints，
// 两者都存在时以旧注解为准。
func hintsEnabled(annotations map[string]string) bool {
	// 优先读取旧注解，缺省时回退到新注解；都没有则未启用。
	val, ok := annotations[v1.DeprecatedAnnotationTopologyAwareHints]
	if !ok {
		val, ok = annotations[v1.AnnotationTopologyMode]
		if !ok {
			return false
		}
	}
	// 仅 "Auto" 与 "auto" 两种拼写视为启用，其他值（含 "aUto"、"enabled"）均不启用。
	return val == "Auto" || val == "auto"
}

// isServiceIPSet aims to check if the service's ClusterIP is set or not
// the objective is not to perform validation here
// copied from k8s.io/kubernetes/pkg/apis/core/v1/helper
// isServiceIPSet 检查 Service 是否设置了 ClusterIP（不做合法性校验）。
// 代码拷贝自 k8s.io/kubernetes/pkg/apis/core/v1/helper。
func isServiceIPSet(service *v1.Service) bool {
	// 既不是 None 也不是空字符串，即视为已设置 ClusterIP。
	return service.Spec.ClusterIP != v1.ClusterIPNone && service.Spec.ClusterIP != ""
}

// FindPort locates the container port for the given pod and portName.  If the
// targetPort is a number, use that.  If the targetPort is a string, look that
// string up in all named ports in all containers in the target pod.  If no
// match is found, fail.
// FindPort 查找给定 Pod 与端口名对应的容器端口。
// targetPort 为数字时直接返回；为字符串时在所有普通容器（以及 restartPolicy=Always 的 sidecar
// init 容器）的命名端口中按名称与协议匹配，找不到则报错。
func FindPort(pod *v1.Pod, svcPort *v1.ServicePort) (int, error) {
	// 取 ServicePort 的 targetPort，按类型分派处理。
	portName := svcPort.TargetPort
	switch portName.Type {
	case intstr.String:
		// 字符串形式：按名称与协议在普通容器中查找。
		name := portName.StrVal
		for _, container := range pod.Spec.Containers {
			for _, port := range container.Ports {
				if port.Name == name && port.Protocol == svcPort.Protocol {
					return int(port.ContainerPort), nil
				}
			}
		}
		// also support sidecar container (initContainer with restartPolicy=Always)
		// 普通容器未命中时，继续查 restartPolicy=Always 的 sidecar init 容器。
		for _, container := range pod.Spec.InitContainers {
			// 非 Always 重启策略的 init 容器直接跳过。
			if container.RestartPolicy == nil || *container.RestartPolicy != v1.ContainerRestartPolicyAlways {
				continue
			}
			for _, port := range container.Ports {
				if port.Name == name && port.Protocol == svcPort.Protocol {
					return int(port.ContainerPort), nil
				}
			}
		}
	case intstr.Int:
		// 数字形式：直接作为容器端口返回。
		return portName.IntValue(), nil
	}

	// 未找到匹配端口时报错。
	return 0, fmt.Errorf("no suitable port for manifest: %s", pod.UID)
}
