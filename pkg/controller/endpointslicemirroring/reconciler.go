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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	endpointsliceutil "k8s.io/endpointslice/util"
	"k8s.io/klog/v2"
	endpointsv1 "k8s.io/kubernetes/pkg/api/v1/endpoints"
	"k8s.io/kubernetes/pkg/controller/endpointslicemirroring/metrics"
)

// reconciler 负责把 EndpointSlice 的当前状态转变为期望状态。
type reconciler struct {
	client clientset.Interface

	// endpointSliceTracker 记录每个 Endpoints 资源期望存在的 EndpointSlice
	// 及其资源版本，用于判断缓存中的对象是否已经过期。
	endpointSliceTracker *endpointsliceutil.EndpointSliceTracker

	// eventRecorder 在发现 Endpoints 中存在非法 IP 时记录事件，
	// 让用户能从 API 事件中看到地址被忽略的原因。
	eventRecorder record.EventRecorder

	// maxEndpointsPerSubset 限制单个 EndpointSubset 镜像到 EndpointSlice
	// 的 endpoint 数量，从而维持 EndpointSubset 与 EndpointSlice 的简单 1:1 映射。
	maxEndpointsPerSubset int32

	// metricsCache 跟踪期望 endpoint 总数以及 EndpointSlice 分布效率，
	// 供指标查询使用。
	metricsCache *metrics.Cache
}

// reconcile 接收 Endpoints 资源并确保对应的 EndpointSlice 存在。
// 它通过创建、更新或删除 EndpointSlice，使实际地址集合与 Endpoints 中的期望地址一致。
func (r *reconciler) reconcile(ctx context.Context, endpoints *corev1.Endpoints, existingSlices []*discovery.EndpointSlice) error {
	logger := klog.FromContext(ctx)
	// 先计算期望状态。
	d := newDesiredCalc()

	numInvalidAddresses := 0
	addressesSkipped := 0

	// 先规范化 subset：减少因 Endpoints 中地址排列不同而产生的无意义变更。
	subsets := endpointsv1.RepackSubsets(endpoints.Subsets)
	for _, subset := range subsets {
		multiKey := d.initPorts(subset.Ports)

		totalAddresses := len(subset.Addresses) + len(subset.NotReadyAddresses)
		totalAddressesAdded := 0

		for _, address := range subset.Addresses {
			// 达到单 subset 镜像上限即停止，以维持 EndpointSubset 与
			// EndpointSlice 的 1:1 映射；超出部分稍后计入跳过数。
			if totalAddressesAdded >= int(r.maxEndpointsPerSubset) {
				break
			}
			if ok := d.addAddress(address, multiKey, true); ok {
				totalAddressesAdded++
			} else {
				numInvalidAddresses++
				logger.Info("Address in Endpoints is not a valid IP, it will not be mirrored to an EndpointSlice", "endpoints", klog.KObj(endpoints), "IP", address.IP)
			}
		}

		for _, address := range subset.NotReadyAddresses {
			// 与 ready 地址共享同一配额，先到的地址优先被镜像；
			// 达到上限后剩余地址只计入跳过数。
			if totalAddressesAdded >= int(r.maxEndpointsPerSubset) {
				break
			}
			if ok := d.addAddress(address, multiKey, false); ok {
				totalAddressesAdded++
			} else {
				numInvalidAddresses++
				logger.Info("Address in Endpoints is not a valid IP, it will not be mirrored to an EndpointSlice", "endpoints", klog.KObj(endpoints), "IP", address.IP)
			}
		}

		addressesSkipped += totalAddresses - totalAddressesAdded
	}

	// 该指标同时统计因非法 IP 和因超过上限而被跳过的地址。
	metrics.AddressesSkippedPerSync.WithLabelValues().Observe(float64(addressesSkipped))

	// 对非法 IP 单独记录事件，便于用户定位错误配置。
	if numInvalidAddresses > 0 {
		r.eventRecorder.Eventf(endpoints, corev1.EventTypeWarning, InvalidIPAddress,
			"Skipped %d invalid IP addresses when mirroring to EndpointSlices", numInvalidAddresses)
	}

	// 超上限与非法 IP 是不同原因，分开记录事件才能给出正确的处理建议。
	if addressesSkipped > numInvalidAddresses {
		logger.Info("Addresses in Endpoints were skipped due to exceeding MaxEndpointsPerSubset", "skippedAddresses", addressesSkipped, "endpoints", klog.KObj(endpoints))
		r.eventRecorder.Eventf(endpoints, corev1.EventTypeWarning, TooManyAddressesToMirror,
			"A max of %d addresses can be mirrored to EndpointSlices per Endpoints subset. %d addresses were skipped", r.maxEndpointsPerSubset, addressesSkipped)
	}

	// 构建现有状态的索引，便于按端口映射逐组对账。
	existingSlicesByKey := endpointSlicesByKey(existingSlices)

	// 按端口映射分组计算每组需要执行的变更。
	epMetrics := metrics.NewEndpointPortCache()
	totals := totalsByAction{}
	slices := slicesByAction{}

	for portKey, desiredEndpoints := range d.endpointsByKey {
		numEndpoints := len(desiredEndpoints)
		pmSlices, pmTotals := r.reconcileByPortMapping(
			endpoints, existingSlicesByKey[portKey], desiredEndpoints, d.portsByKey[portKey], portKey.addressType())

		slices.append(pmSlices)
		totals.add(pmTotals)

		epMetrics.Set(endpointsliceutil.PortMapKey(portKey), metrics.EfficiencyInfo{
			Endpoints: numEndpoints,
			Slices:    len(existingSlicesByKey[portKey]) + len(pmSlices.toCreate) - len(pmSlices.toDelete),
		})
	}

	// 期望状态中已不存在的端口组合，其对应的 EndpointSlice 必须删除，
	// 否则会残留无效路由。
	for portKey, existingSlices := range existingSlicesByKey {
		if _, ok := d.endpointsByKey[portKey]; !ok {
			for _, existingSlice := range existingSlices {
				slices.toDelete = append(slices.toDelete, existingSlice)
			}
		}
	}

	metrics.EndpointsAddedPerSync.WithLabelValues().Observe(float64(totals.added))
	metrics.EndpointsUpdatedPerSync.WithLabelValues().Observe(float64(totals.updated))
	metrics.EndpointsRemovedPerSync.WithLabelValues().Observe(float64(totals.removed))

	endpointsNN := types.NamespacedName{Name: endpoints.Name, Namespace: endpoints.Namespace}
	r.metricsCache.UpdateEndpointPortCache(endpointsNN, epMetrics)

	return r.finalize(ctx, endpoints, slices)
}

// reconcileByPortMapping 比较现有 EndpointSlice 中的 endpoint 与期望集合，
// 返回需要创建、更新和删除的 EndpointSlice 列表，以及增/改/删的数量统计。
func (r *reconciler) reconcileByPortMapping(
	endpoints *corev1.Endpoints,
	existingSlices []*discovery.EndpointSlice,
	desiredSet endpointsliceutil.EndpointSet,
	endpointPorts []discovery.EndpointPort,
	addressType discovery.AddressType,
) (slicesByAction, totalsByAction) {
	slices := slicesByAction{}
	totals := totalsByAction{}

	// 不再需要任何 endpoint 时直接删除所有现有 slice：
	// 保留空 slice 没有意义，还会占用资源。
	if desiredSet.Len() == 0 {
		slices.toDelete = existingSlices
		for _, epSlice := range existingSlices {
			totals.removed += len(epSlice.Endpoints)
		}
		return slices, totals
	}

	if len(existingSlices) == 0 {
		// 没有现有 slice 时所有期望 endpoint 都算新增。
		totals.added = desiredSet.Len()
	} else {
		// 存在多个 slice 时只保留第一个作为更新目标，其余标记删除，
		// 逐步收敛到每端口组合一个 slice。
		slices.toDelete = existingSlices[1:]

		// 比较时排除由控制器自身维护或仅用于变更触发的元数据，
		// 避免这些字段导致无意义的反复更新。
		compareAnnotations := cloneAndRemoveKeys(endpoints.Annotations, corev1.EndpointsLastChangeTriggerTime, corev1.LastAppliedConfigAnnotation)
		compareLabels := cloneAndRemoveKeys(existingSlices[0].Labels, discovery.LabelManagedBy, discovery.LabelServiceName)
		// 首个 slice 已与期望状态一致时直接返回，省去一次无用的 Update。
		totals = totalChanges(existingSlices[0], desiredSet)
		if totals.added == 0 && totals.updated == 0 && totals.removed == 0 &&
			apiequality.Semantic.DeepEqual(endpoints.Labels, compareLabels) &&
			apiequality.Semantic.DeepEqual(compareAnnotations, existingSlices[0].Annotations) &&
			!needRebuildExistingSlices(endpoints, existingSlices[0]) {
			if !r.endpointSliceTracker.Has(existingSlices[0]) {
				// 即使无需更新，也要确保该 slice 处于跟踪状态，
				// 否则后续缓存事件会被误判为过期。
				r.endpointSliceTracker.Update(existingSlices[0])
			}
			return slices, totals
		}
	}

	// 用期望的 endpoint 构造目标 slice；复用首个现有 slice 的名称，
	// 使变更以 Update 而非 Create+Delete 的方式落地。
	var sliceName string
	if len(existingSlices) > 0 {
		sliceName = existingSlices[0].Name
	}
	newSlice := newEndpointSlice(endpoints, endpointPorts, addressType, sliceName)
	for desiredSet.Len() > 0 && len(newSlice.Endpoints) < int(r.maxEndpointsPerSubset) {
		endpoint, _ := desiredSet.PopAny()
		newSlice.Endpoints = append(newSlice.Endpoints, *endpoint)
	}

	// 有名称的 slice 走更新路径；新建的 slice 依赖 GenerateName 生成名称以避免冲突。
	if newSlice.Name != "" {
		slices.toUpdate = []*discovery.EndpointSlice{newSlice}
	} else {
		slices.toCreate = []*discovery.EndpointSlice{newSlice}
	}

	return slices, totals
}

// finalize 按切片列表执行创建、更新和删除操作。
func (r *reconciler) finalize(ctx context.Context, endpoints *corev1.Endpoints, slices slicesByAction) error {
	// 有待删除的 slice 时优先复用它们：把创建改为更新，
	// 减少 API 对象增删带来的抖动，也让名称保持稳定。
	recycleSlices(&slices)

	epsClient := r.client.DiscoveryV1().EndpointSlices(endpoints.Namespace)

	// Endpoints 正在删除时不再创建新 slice，避免刚创建就被级联删除。
	if endpoints.DeletionTimestamp == nil {
		for _, endpointSlice := range slices.toCreate {
			createdSlice, err := epsClient.Create(ctx, endpointSlice, metav1.CreateOptions{})
			if err != nil {
				// 命名空间正在终止时创建会持续失败，重试没有意义，
				// 直接丢弃并返回 nil，避免队列无限重排该事件。
				if errors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
					return nil
				}
				return fmt.Errorf("failed to create EndpointSlice for Endpoints %s/%s: %v", endpoints.Namespace, endpoints.Name, err)
			}
			// 记录服务端返回的资源版本，供后续判断缓存是否过期。
			r.endpointSliceTracker.Update(createdSlice)
			metrics.EndpointSliceChanges.WithLabelValues("create").Inc()
		}
	}

	for _, endpointSlice := range slices.toUpdate {
		updatedSlice, err := epsClient.Update(ctx, endpointSlice, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update %s EndpointSlice for Endpoints %s/%s: %v", endpointSlice.Name, endpoints.Namespace, endpoints.Name, err)
		}
		// 用更新后的版本刷新跟踪信息，防止下一次对账误判为过期。
		r.endpointSliceTracker.Update(updatedSlice)
		metrics.EndpointSliceChanges.WithLabelValues("update").Inc()
	}

	for _, endpointSlice := range slices.toDelete {
		err := epsClient.Delete(ctx, endpointSlice.Name, metav1.DeleteOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete %s EndpointSlice for Endpoints %s/%s: %v", endpointSlice.Name, endpoints.Namespace, endpoints.Name, err)
		}
		// 提前登记预期删除，使删除事件到达时不会被视为意外变更。
		r.endpointSliceTracker.ExpectDeletion(endpointSlice)
		metrics.EndpointSliceChanges.WithLabelValues("delete").Inc()
	}

	return nil
}

// deleteEndpoints 删除关联的 EndpointSlice，并清理 metricsCache 中的引用。
func (r *reconciler) deleteEndpoints(ctx context.Context, namespace, name string, endpointSlices []*discovery.EndpointSlice) error {
	// 先清理指标缓存，避免 Endpoints 消失后残留陈旧统计。
	r.metricsCache.DeleteEndpoints(types.NamespacedName{Namespace: namespace, Name: name})
	var errs []error
	for _, endpointSlice := range endpointSlices {
		err := r.client.DiscoveryV1().EndpointSlices(namespace).Delete(ctx, endpointSlice.Name, metav1.DeleteOptions{})
		if err != nil {
			// 收集全部失败后再统一返回，让调用方一次性了解删除情况。
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("error(s) deleting %d/%d EndpointSlices for %s/%s Endpoints, including: %s", len(errs), len(endpointSlices), namespace, name, errs[0])
	}
	return nil
}

// endpointSlicesByKey 按地址类型与端口映射的组合（addrTypePortMapKey）
// 对 EndpointSlice 分组，使不同端口集合能独立对账。
func endpointSlicesByKey(existingSlices []*discovery.EndpointSlice) map[addrTypePortMapKey][]*discovery.EndpointSlice {
	slicesByKey := map[addrTypePortMapKey][]*discovery.EndpointSlice{}
	for _, existingSlice := range existingSlices {
		epKey := newAddrTypePortMapKey(existingSlice.Ports, existingSlice.AddressType)
		slicesByKey[epKey] = append(slicesByKey[epKey], existingSlice)
	}
	return slicesByKey
}

// totalChanges 统计某个 EndpointSlice 匹配期望 endpoint 集合所需的
// 新增、更新与删除数量，用于快速判断是否可以跳过更新。
func totalChanges(existingSlice *discovery.EndpointSlice, desiredSet endpointsliceutil.EndpointSet) totalsByAction {
	totals := totalsByAction{}
	existingMatches := 0

	for _, endpoint := range existingSlice.Endpoints {
		got := desiredSet.Get(&endpoint)
		if got == nil {
			// 不再期望该 endpoint，需要删除。
			totals.removed++
		} else {
			existingMatches++

			// 哈希之外的字段也可能变化，必须比较完整内容才能发现需更新的 endpoint。
			if !endpointsliceutil.EndpointsEqualBeyondHash(got, &endpoint) {
				totals.updated++
			}
		}
	}

	// 未在现有 slice 中匹配到的期望 endpoint 均需新增。
	totals.added = desiredSet.Len() - existingMatches
	return totals
}

// needRebuildExistingSlices 判断 slice 的 Endpoints 属主是否已变更。
// UID 不同说明旧属主已删除并重建，必须重建 slice 而非在原对象上更新。
func needRebuildExistingSlices(endpoints *corev1.Endpoints, existingSlice *discovery.EndpointSlice) bool {
	for index := range existingSlice.OwnerReferences {
		owner := existingSlice.OwnerReferences[index]
		if owner.Kind == "Endpoints" && owner.Name == endpoints.Name && owner.UID != endpoints.UID {
			// 同名但 UID 不同意味着 Endpoints 被删除后重新创建，
			// 旧 slice 上的属主引用已失效，必须重建。
			return true
		}
	}
	return false
}
