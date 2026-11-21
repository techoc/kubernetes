/*
Copyright 2020 The Kubernetes Authors.

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

package endpointslicemirroring

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	endpointsliceutil "k8s.io/endpointslice/util"
	"k8s.io/kubernetes/pkg/apis/discovery/validation"
	netutils "k8s.io/utils/net"
)

// addrTypePortMapKey 用于在缓存中唯一标识一组端点端口与地址类型的组合。
// 同一 Service 的不同地址族（IPv4/IPv6）或不同端口集合必须分别镜像，
// 因此键里同时包含地址类型和端口映射键。
type addrTypePortMapKey string

// newAddrTypePortMapKey 根据端点端口和地址类型生成 PortMapKey。
// 地址类型必须参与键的计算，否则 IPv4 与 IPv6 的相同端口集合会被错误合并成同一个切片。
func newAddrTypePortMapKey(endpointPorts []discovery.EndpointPort, addrType discovery.AddressType) addrTypePortMapKey {
	// 用地址类型作前缀，保证不同地址族下相同的端口集合得到不同的键。
	pmk := fmt.Sprintf("%s-%s", addrType, endpointsliceutil.NewPortMapKey(endpointPorts))
	return addrTypePortMapKey(pmk)
}

// addressType 从键的前缀还原出地址类型。
// 键只可能是 IPv4 或 IPv6 两种，因此非 IPv6 前缀一律按 IPv4 处理。
func (pk addrTypePortMapKey) addressType() discovery.AddressType {
	// newAddrTypePortMapKey 以地址类型作前缀，这里按前缀反解出地址族。
	if strings.HasPrefix(string(pk), string(discovery.AddressTypeIPv6)) {
		return discovery.AddressTypeIPv6
	}
	return discovery.AddressTypeIPv4
}

// newEndpointSlice 根据 Endpoints 资源、端口和地址类型生成对应的 EndpointSlice。
// 镜像切片必须归属于原 Endpoints 对象（ownerRef），这样 Endpoints 被删除时
// 切片能被垃圾回收自动清理，无需镜像控制器额外处理。
func newEndpointSlice(endpoints *corev1.Endpoints, ports []discovery.EndpointPort, addrType discovery.AddressType, sliceName string) *discovery.EndpointSlice {
	// 用 v1 Endpoints 作为 owner，借助 GC 保证切片生命周期与源对象一致。
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Endpoints"}
	ownerRef := metav1.NewControllerRef(endpoints, gvk)
	epSlice := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          map[string]string{},
			Annotations:     map[string]string{},
			OwnerReferences: []metav1.OwnerReference{*ownerRef},
			Namespace:       endpoints.Namespace,
		},
		Ports:       ports,
		AddressType: addrType,
		Endpoints:   []discovery.Endpoint{},
	}

	// 先克隆原 Endpoints 的全部标签，保留用户自定义的标签与选择器语义。
	for label, val := range endpoints.Labels {
		epSlice.Labels[label] = val
	}

	// 覆盖保留标签：服务名用于反查所属 Service，managed-by 用于区分镜像切片与
	// EndpointSlice 控制器原生管理的切片，避免两个控制器互相争抢同一对象。
	epSlice.Labels[discovery.LabelServiceName] = endpoints.Name
	epSlice.Labels[discovery.LabelManagedBy] = ControllerName

	// 克隆注解时剔除 Endpoints 特有的元数据：LastChangeTriggerTime 对切片无意义，
	// LastAppliedConfig 记录的是 Endpoints 的配置，复制到切片上只会产生误导。
	for annotation, val := range endpoints.Annotations {
		if annotation == corev1.EndpointsLastChangeTriggerTime || annotation == corev1.LastAppliedConfigAnnotation {
			continue
		}
		epSlice.Annotations[annotation] = val
	}

	if sliceName == "" {
		epSlice.GenerateName = getEndpointSlicePrefix(endpoints.Name)
	} else {
		epSlice.Name = sliceName
	}

	return epSlice
}

// getEndpointSlicePrefix 返回适合作为 EndpointSlice 名称前缀的字符串。
// 生成名（GenerateName）必须是合法的名称片段，否则 apiserver 会拒绝创建请求。
func getEndpointSlicePrefix(serviceName string) string {
	// 服务名较短时加连字符更美观；若超过长度限制则退回裸服务名，保证前缀合法。
	prefix := fmt.Sprintf("%s-", serviceName)
	// 用与 apiserver 相同的校验规则验证前缀，避免生成无法创建的名称。
	if len(validation.ValidateEndpointSliceName(prefix, true)) != 0 {
		prefix = serviceName
	}
	return prefix
}

// addressToEndpoint 将 Endpoints 中的地址转换为 EndpointSlice 的 endpoint 及地址类型。
// 无法解析的 IP 返回 (nil, nil)，调用方据此跳过非法地址，避免生成损坏的切片。
func addressToEndpoint(address corev1.EndpointAddress, ready bool) (*discovery.Endpoint, *discovery.AddressType) {
	// 使用宽松解析兼容 Endpoints 中可能出现的非规范 IP 文本。
	ip := netutils.ParseIPSloppy(address.IP)
	if ip == nil {
		// 非法 IP 无法判断地址族，也不能写入切片，交由调用方丢弃。
		return nil, nil
	}
	addressType := discovery.AddressTypeIPv4
	// IPv6 地址没有 To4() 表示形式，据此区分地址族以决定切片归属。
	if ip.To4() == nil {
		addressType = discovery.AddressTypeIPv6
	}

	endpoint := &discovery.Endpoint{
		// 重新序列化 IP 可以让非规范格式归一化为标准文本，保证切片内容稳定。
		Addresses: []string{ip.String()},
		Conditions: discovery.EndpointConditions{
			Ready: &ready,
		},
		TargetRef: address.TargetRef,
	}

	if address.NodeName != nil {
		endpoint.NodeName = address.NodeName
	}
	if address.Hostname != "" {
		endpoint.Hostname = &address.Hostname
	}

	return endpoint, &addressType
}

// epPortsToEpsPorts 将 Endpoints 中的端口转换为 EndpointSlice 使用的端口格式。
// 两种资源的端口字段结构不同，这里逐字段映射并深拷贝指针字段，
// 避免改动切片时意外影响缓存中的 Endpoints 对象。
func epPortsToEpsPorts(epPorts []corev1.EndpointPort) []discovery.EndpointPort {
	epsPorts := []discovery.EndpointPort{}
	for _, epPort := range epPorts {
		// 深拷贝后再取地址，防止指针指向循环变量或原对象的字段。
		epp := epPort.DeepCopy()
		epsPorts = append(epsPorts, discovery.EndpointPort{
			Name:        &epp.Name,
			Port:        &epp.Port,
			Protocol:    &epp.Protocol,
			AppProtocol: epp.AppProtocol,
		})
	}
	return epsPorts
}

// getServiceFromDeleteAction 从删除事件中解析出 Service 资源。
// informer 的删除回调可能收到墓碑（tombstone）而非对象本体，必须同时处理两种情况，
// 否则删除事件会被漏掉，导致镜像切片残留。
func getServiceFromDeleteAction(obj interface{}) *corev1.Service {
	if service, ok := obj.(*corev1.Service); ok {
		return service
	}
	// 走到这里说明 Service 已被删除，且 informer 未记录其最终状态（墓碑）。
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
		return nil
	}
	// 墓碑中包裹的类型也需断言，防止意外类型触发 panic。
	service, ok := tombstone.Obj.(*corev1.Service)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a Service resource: %#v", obj))
		return nil
	}
	return service
}

// getEndpointsFromDeleteAction 从删除事件中解析出 Endpoints 资源。
// 与 Service 同理，需要兼容 informer 的墓碑对象，确保删除事件不丢失。
func getEndpointsFromDeleteAction(obj interface{}) *corev1.Endpoints {
	if endpoints, ok := obj.(*corev1.Endpoints); ok {
		return endpoints
	}
	// 走到这里说明 Endpoints 已被删除，且 informer 未记录其最终状态（墓碑）。
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
		return nil
	}
	// 墓碑中包裹的类型也需断言，防止意外类型触发 panic。
	endpoints, ok := tombstone.Obj.(*corev1.Endpoints)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not an Endpoints resource: %#v", obj))
		return nil
	}
	return endpoints
}

// getEndpointSliceFromDeleteAction 从删除事件中解析出 EndpointSlice。
// 兼容 informer 墓碑对象，以便切片被外部删除时控制器能收到通知并重建镜像。
func getEndpointSliceFromDeleteAction(obj interface{}) *discovery.EndpointSlice {
	if endpointSlice, ok := obj.(*discovery.EndpointSlice); ok {
		return endpointSlice
	}
	// 走到这里说明 EndpointSlice 已被删除，且 informer 未记录其最终状态（墓碑）。
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
		return nil
	}
	// 墓碑中包裹的类型也需断言，防止意外类型触发 panic。
	endpointSlice, ok := tombstone.Obj.(*discovery.EndpointSlice)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not an EndpointSlice resource: %#v", obj))
		return nil
	}
	return endpointSlice
}

// endpointsControllerKey 从 EndpointSlice 反推出其对应 Endpoints 资源的控制器键（namespace/name）。
// 切片事件需要映射到源 Endpoints 上处理，因此依赖切片上的服务名标签进行反查。
func endpointsControllerKey(endpointSlice *discovery.EndpointSlice) (string, error) {
	if endpointSlice == nil {
		return "", fmt.Errorf("nil EndpointSlice passed to serviceControllerKey()")
	}
	// 镜像切片由本控制器打上服务名标签；缺失说明不是我们管理的对象，不应继续处理。
	serviceName, ok := endpointSlice.Labels[discovery.LabelServiceName]
	if !ok || serviceName == "" {
		return "", fmt.Errorf("EndpointSlice missing %s label", discovery.LabelServiceName)
	}
	return fmt.Sprintf("%s/%s", endpointSlice.Namespace, serviceName), nil
}

// skipMirror 在 LabelSkipMirror 标签被显式设为 "true" 时返回 true。
// 这是用户选择退出镜像机制的开关：带此标签的 Endpoints 不应被镜像成 EndpointSlice。
func skipMirror(labels map[string]string) bool {
	// 只有精确等于 "true" 才视为跳过，其他值（含缺失）都按正常镜像处理。
	skipMirror, _ := labels[discovery.LabelSkipMirror]
	return skipMirror == "true"
}

// hasLeaderElection 在注解中存在 LeaderElectionRecordAnnotationKey 时返回 true。
// 带 leader election 注解的 Endpoints 由其他组件（如 endpoints 控制器）管理，
// 镜像控制器需要识别并让位，避免重复或冲突的写入。
func hasLeaderElection(annotations map[string]string) bool {
	_, ok := annotations[resourcelock.LeaderElectionRecordAnnotationKey]
	return ok
}

// cloneAndRemoveKeys 是 CloneAndRemoveLabels 的复制版本，此处用于注解和标签。
// 必须在改写到 apiserver 之前拷贝 map，否则会污染 informer 缓存中的只读对象。
func cloneAndRemoveKeys(a map[string]string, keys ...string) map[string]string {
	if len(keys) == 0 {
		// 无需删除任何键时直接返回原 map，避免不必要的拷贝。
		return a
	}
	// 先完整克隆，再删除指定键，保证原 map 不被修改。
	newMap := map[string]string{}
	for k, v := range a {
		newMap[k] = v
	}
	// 逐个剔除需要排除的键。
	for _, key := range keys {
		delete(newMap, key)
	}
	return newMap
}

// managedByChanged 在两个 EndpointSlice 中一个由 EndpointSlice 控制器管理、
// 另一个不由其管理时返回 true。
// 管理方发生变化意味着切片的所有权需要交接，必须触发协调以更正 managed-by 标签。
func managedByChanged(endpointSlice1, endpointSlice2 *discovery.EndpointSlice) bool {
	return managedByController(endpointSlice1) != managedByController(endpointSlice2)
}

// managedByController 在给定 EndpointSlice 由 EndpointSlice 控制器管理时返回 true。
// 仅当 managed-by 标签恰好等于本控制器名时才算归属我们，避免误认其他控制器的切片。
func managedByController(endpointSlice *discovery.EndpointSlice) bool {
	managedBy, _ := endpointSlice.Labels[discovery.LabelManagedBy]
	return managedBy == ControllerName
}
