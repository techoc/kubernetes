/*
Copyright 2025 The Kubernetes Authors.

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

// Package dynamicresources 实现动态资源分配（DRA）调度插件。
// 本文件实现由 DRA 支撑的扩展资源（extended resources）处理逻辑。
package dynamicresources

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/scheduler/metrics"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/util/retry"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
	"k8s.io/utils/ptr"
)

// DRA 支撑的扩展资源——调度插件各扩展点的工作流程：
//
// PreFilter - preFilterExtendedResources()
// - 对使用扩展资源的 Pod，查找已有 claim，或创建名为 "<extended-resources>" 的内存态 claim。
// - 内存态 claim 用于跟踪与分配资源，真正的 claim 对象在 PreBind 扩展点创建。
// - 将 claim 存入 stateData，供 Filter 扩展点使用。
//
// Filter - filterExtendedResources()
// - 若发现上一轮遗留的带 Spec 的陈旧 claim，返回 Unschedulable，让 PostFilter 清理。
// - 区分哪些资源由 device plugin 满足、哪些需要走 DRA。
// - 对需要 DRA 分配的扩展资源，创建节点专属的 claim。
//
// PostFilter
// - 若扩展资源 claim 已有真实名字（非 "<extended-resources>"）：
//   - 说明是上一轮遗留，删除它并触发重试。
//
// Reserve
// - 把 Filter 阶段的分配结果存入 stateData。
// - 通过 SignalClaimPendingAllocation() 把 claim 标记为"分配进行中"。
//
// Unreserve
// - 从进行中分配里移除 claim 并恢复 assume cache。
// - 若 claim 已有真实名字，则从 API server 删除。
//
// PreBind - bindClaim()
// - 对 "<extended-resources>" claim：在 API server 创建并更新 stateData。
// - 更新 claim 状态：添加 finalizer、分配结果和 Pod 预留。
// - 存入 assume cache（轮询等待扩展资源 claim）。
// - 用请求映射更新 pod.Status.ExtendedResourceClaimStatus。

const (
	// specialClaimInMemName 是只存在于内存中的特殊 resource claim 的名字。
	// 写入 API server 时该 claim 会获得一个生成名。
	//
	// 故意采用不合法的 ResourceClaim 名字，避免与 apiserver 中真实的 ResourceClaim 冲突。
	specialClaimInMemName = "<extended-resources>"

	// AssumeExtendedResourceTimeoutDefaultSeconds 是等待扩展资源 claim 出现在 assume cache 中的默认超时（秒）。
	AssumeExtendedResourceTimeoutDefaultSeconds = 120
)

// draExtendedResource 保存由 DRA 支撑的扩展资源数据；DRAExtendedResource 特性关闭时为空。
type draExtendedResource struct {
	// Pod 请求中可能由 DRA 支撑的标量资源。
	podScalarResources map[v1.ResourceName]int64
}

// hasDeviceClassMappedExtendedResource 判断给定资源列表中是否存在映射到 DeviceClass 的扩展资源。
func hasDeviceClassMappedExtendedResource(reqs v1.ResourceList, cache fwk.DeviceClassResolver) bool {
	for rName, rValue := range reqs {
		if rValue.IsZero() {
			// 只关心当前待调度 Pod 实际请求的资源。
			continue
		}
		if schedutil.IsDRAExtendedResourceName(rName) {
			if cache.GetDeviceClass(rName) != nil {
				return true
			}
		}
	}
	return false
}

// findExtendedResourceClaim 查找扩展资源 claim，即带有特殊注解且以该 Pod 为属主的 claim。
// 调用时必须传入集群中所有 ResourceClaim；返回的 ResourceClaim 应视为只读。
func findExtendedResourceClaim(pod *v1.Pod, resourceClaims []*resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
	for _, c := range resourceClaims {
		if c.Annotations[resourceapi.ExtendedResourceClaimAnnotation] == "true" {
			for _, or := range c.OwnerReferences {
				if or.Name == pod.Name && ptr.Deref(or.Controller, false) && or.UID == pod.UID {
					return c
				}
			}
		}
	}
	return nil
}

// preFilterExtendedResources 检查 Pod 请求中是否存在映射到 DeviceClass 的扩展资源
// （即某 DeviceClass 的 spec.ExtendedResourceName 或隐式扩展资源名与之匹配）。
//
// 该函数会查找上一调度周期为 Pod 创建的特殊 claim；若不存在，则创建一个 Spec 中无 Request、
// 带临时 UID、名字为 specialClaimInMemName 的特殊 claim。两种情况下 claim 都会存入 state.claims。
// 同时，draExtendedResource 也会写入 cycle state。
//
// 返回特殊 ResourceClaim 与错误状态；特性关闭或 Pod 不需要时两者均为 nil。
func (pl *DynamicResources) preFilterExtendedResources(pod *v1.Pod, logger klog.Logger, s *stateData) (*resourceapi.ResourceClaim, *fwk.Status) {
	if !pl.fts.EnableDRAExtendedResource {
		return nil, nil
	}

	// 从缓存构建扩展资源名到 DeviceClass 的映射。
	cache := pl.draManager.DeviceClassResolver()
	reqs := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})

	hasExtendedResource := hasDeviceClassMappedExtendedResource(reqs, cache)
	if !hasExtendedResource {
		return nil, nil
	}
	r := framework.NewResource(reqs)
	s.draExtendedResource.podScalarResources = r.ScalarResources

	resourceClaims, err := pl.draManager.ResourceClaims().List()
	if err != nil {
		return nil, statusError(logger, err, "listing ResourceClaims")
	}

	// 检查上一调度周期是否已创建特殊 claim。
	//
	// 即使之前已分配过，该分配也可能已失效。要精确判断需复刻分配器逻辑，涉及因素太多难以覆盖；
	// 而且一旦发现分配陈旧，这些设备又无法立即用于新一轮分配。这种情况很罕见（表现为绑定失败），
	// 因此采用暴力处理：
	// - 在后台触发释放（deallocation）。
	// - 将 Pod 标记为不可调度；释放成功后即可重新调度。
	extendedResourceClaim := findExtendedResourceClaim(pod, resourceClaims)
	if extendedResourceClaim != nil {
		return extendedResourceClaim, nil
	}
	// 为 Pod 中所有由 DRA 支撑的扩展资源创建一个特殊 claim。
	// 该 claim 以 Pod 为属主，生成名以 <pod name>-extended-resources- 为前缀；名字过长时会被截断。
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pod.Namespace,
			Name:      specialClaimInMemName,
			// 伪造的临时 UID，仅供 SignalClaimPendingAllocation 使用。
			UID:          types.UID(uuid.NewUUID()),
			GenerateName: pod.Name + "-extended-resources-",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       pod.Name,
					UID:        pod.UID,
					Controller: ptr.To(true),
				},
			},
			Annotations: map[string]string{
				resourceapi.ExtendedResourceClaimAnnotation: "true",
			},
		},
		Spec: resourceapi.ResourceClaimSpec{},
	}, nil
}

// filterExtendedResources 根据节点的 Allocatable 计算特殊 claim 的 Request，返回：
// - nil：无需分配，扩展资源全部由 device plugin 满足；或
// - 更新后的特殊 claim：包含该节点上需要通过 DRA 分配的资源。
//
// 当 Pod 的扩展资源请求既无法从节点 Allocatable 分配，也匹配不到任何 DeviceClass 的
// 显式/隐式 ExtendedResourceName 时，返回错误。
func (pl *DynamicResources) filterExtendedResources(state *stateData, pod *v1.Pod, nodeInfo fwk.NodeInfo, logger klog.Logger) (*resourceapi.ResourceClaim, []v1.ContainerExtendedResourceRequest, *fwk.Status) {
	extendedResourceClaim := state.claims.extendedResourceClaim()
	if extendedResourceClaim == nil {
		// 没有特殊 claim，无需处理。
		return nil, nil, nil
	}

	// claim 来自上一调度周期且 Spec 已有 Request，返回不可调度，
	// 使 PostFilter 阶段能删除它并重新尝试。
	if extendedResourceClaim.Spec.Devices.Requests != nil {
		return nil, nil, statusUnschedulable(logger, "cannot schedule extended resource claim", "pod", klog.KObj(pod), "node", klog.KObj(nodeInfo.Node()), "claim", klog.KObj(extendedResourceClaim))
	}

	extendedResources := make(map[v1.ResourceName]int64)
	hasExtendedResource := false
	cache := pl.draManager.DeviceClassResolver()
	for rName, rQuant := range state.draExtendedResource.podScalarResources {
		if !schedutil.IsDRAExtendedResourceName(rName) {
			continue
		}
		// 请求量为 0 的资源跳过。
		if rQuant == 0 {
			continue
		}
		allocatable, okScalar := nodeInfo.GetAllocatable().GetScalarResources()[rName]
		isBackedByDRA := cache.GetDeviceClass(rName) != nil
		if isBackedByDRA && allocatable == 0 {
			// 节点 Allocatable 中该资源为 0，需要经 DRA 提供。
			extendedResources[rName] = rQuant
			hasExtendedResource = true
		} else if !okScalar {
			// 该资源既不由 device plugin 提供、也不由 DRA 支撑，Pod 无法落到该节点。
			return nil, nil, statusUnschedulable(logger, "cannot fit resource", "pod", klog.KObj(pod), "node", klog.KObj(nodeInfo.Node()), "resource", rName)
		}
	}
	// 该节点上没有需要 DRA 支撑的扩展资源。Pod 即便请求了扩展资源，也全部由 device plugin 提供，
	// 节点能否容纳 Pod 由 noderesources 插件负责检查，本插件的 Filter 阶段无需做事。
	if state.claims.noUserClaim() && !hasExtendedResource {
		// 走到这里说明无需分配：上一周期的 claim 已在前面返回不可调度。
		return nil, nil, nil
	}

	if extendedResourceClaim.Status.Allocation != nil {
		// 已分配的 claim 不能再次分配。走到这里说明无需分配：
		// 上一周期的 claim 已在前面返回不可调度。
		return nil, nil, nil
	}

	// 每个节点可能有不同的分配需求，因此需要节点专属的 claim 副本。
	nodeExtendedResourceClaim := extendedResourceClaim.DeepCopy()
	reqs, mappings := createRequestsAndMappings(pod, extendedResources, logger, cache)
	nodeExtendedResourceClaim.Spec.Devices.Requests = reqs

	return nodeExtendedResourceClaim, mappings, nil
}

// isSpecialClaimName 判断名字是否为内存态特殊 claim 的名字。
func isSpecialClaimName(name string) bool {
	return name == specialClaimInMemName
}

// deleteClaim 先移除 claim 上的（若存在）finalizer，再删除该 claim。
func (pl *DynamicResources) deleteClaim(ctx context.Context, claim *resourceapi.ResourceClaim) error {
	refreshClaim := false
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if refreshClaim {
			updatedClaim, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get resourceclaim %s/%s: %w", claim.Namespace, claim.Name, err)
			}
			claim = updatedClaim
		} else {
			refreshClaim = true
		}
		// 先移除 finalizer，否则删除会被阻塞。
		builtinControllerFinalizer := slices.Index(claim.Finalizers, resourceapi.Finalizer)
		if builtinControllerFinalizer >= 0 {
			claim.Finalizers = slices.Delete(claim.Finalizers, builtinControllerFinalizer, builtinControllerFinalizer+1)
		}

		_, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("update resourceclaim %s/%s: %w", claim.Namespace, claim.Name, err)
		}
		return nil
	})
	if retryErr != nil {
		return retryErr
	}

	klog.FromContext(ctx).V(5).Info("Delete", "resourceclaim", klog.KObj(claim))
	err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).Delete(ctx, claim.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// partitionContainerIndices 把容器分成两组：长生命周期容器（普通容器与 sidecar init 容器）
// 和短生命周期容器（非 sidecar 的 init 容器）。前者可复用其资源请求，后者需按峰值单独处理。
func partitionContainerIndices(containers []v1.Container, numInitContainers int) ([]int, []int) {
	longLivedContainerIndices := make([]int, 0, len(containers))
	shortLivedInitContainerIndices := make([]int, 0, numInitContainers)
	for i, c := range containers {
		isInit := i < numInitContainers
		isSidecar := c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways
		if isInit && !isSidecar {
			shortLivedInitContainerIndices = append(shortLivedInitContainerIndices, i)
			continue
		}
		longLivedContainerIndices = append(longLivedContainerIndices, i)
	}
	return longLivedContainerIndices, shortLivedInitContainerIndices
}

// createResourceRequestAndMappings 为给定容器与资源生成 DeviceRequest 及请求映射。
// reusableRequests 是该容器可复用的其他 DeviceRequest（元素可能为 nil），复用不足部分才新建请求。
// 若无需额外请求，返回的 request 为 nil；若容器不使用该资源，返回的 mappings 为空。
func createResourceRequestAndMappings(containerIndex int, container *v1.Container, rName v1.ResourceName, className string, reusableRequests []*resourceapi.DeviceRequest) (*resourceapi.DeviceRequest, []v1.ContainerExtendedResourceRequest) {
	var mappings []v1.ContainerExtendedResourceRequest
	creqs := container.Resources.Requests
	if creqs == nil {
		return nil, nil
	}
	var rQuant resource.Quantity
	var ok bool
	if rQuant, ok = creqs[rName]; !ok {
		return nil, nil
	}
	crq, ok := (&rQuant).AsInt64()
	if !ok || crq == 0 {
		return nil, nil
	}
	sum := int64(0)
	for _, r := range reusableRequests {
		if r != nil {
			sum += r.Exactly.Count
			mappings = append(mappings, v1.ContainerExtendedResourceRequest{
				ContainerName: container.Name,
				ResourceName:  rName.String(),
				RequestName:   r.Name,
			})
			if sum >= crq {
				return nil, mappings
			}
		}
	}
	keys := make([]string, 0, len(creqs))
	for k := range creqs {
		keys = append(keys, k.String())
	}
	// 容器内的资源请求是 map，迭代顺序不确定；排序后才能确定资源在请求中的索引。
	slices.Sort(keys)
	ridx := 0
	for j := range keys {
		if keys[j] == rName.String() {
			ridx = j
			break
		}
	}
	// 请求名由容器在（initContainers + containers）列表中的下标和
	// 资源在容器内排序后请求中的下标拼成，保证稳定且唯一。
	reqName := fmt.Sprintf("container-%d-request-%d", containerIndex, ridx)
	deviceReq := resourceapi.DeviceRequest{
		Name: reqName, // 名字需体现容器下标与扩展资源下标
		Exactly: &resourceapi.ExactDeviceRequest{
			DeviceClassName: className,
			AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
			Count:           crq - sum, // 复用后可省下的设备数，这里只申请差额
		},
	}
	mappings = append(mappings, v1.ContainerExtendedResourceRequest{
		ContainerName: container.Name,
		ResourceName:  rName.String(),
		RequestName:   reqName,
	})

	return &deviceReq, mappings
}

// createRequestsAndMappings 为 Pod 中所有扩展资源生成 DeviceRequest 列表及容器请求映射。
// 普通容器与 sidecar 各自独立申请；短生命周期 init 容器按顺序复用后续长生命周期容器的请求，
// 仅需申请其中的峰值差额。
func createRequestsAndMappings(pod *v1.Pod, extendedResources map[v1.ResourceName]int64, logger klog.Logger, deviceClassMapping fwk.DeviceClassResolver) ([]resourceapi.DeviceRequest, []v1.ContainerExtendedResourceRequest) {
	containers := slices.Clone(pod.Spec.InitContainers)
	containers = append(containers, pod.Spec.Containers...)
	longLivedContainerIndices, shortLivedInitContainerIndices := partitionContainerIndices(containers, len(pod.Spec.InitContainers))

	// 汇总所有容器、所有资源类型的请求与映射。
	var deviceRequests []resourceapi.DeviceRequest
	var mappings []v1.ContainerExtendedResourceRequest

	// Go 的 map 迭代顺序不确定，必须先提取资源名并排序，保证生成的请求与映射顺序稳定。
	resourceNames := make([]v1.ResourceName, 0, len(extendedResources))
	for resource := range extendedResources {
		resourceNames = append(resourceNames, resource)
	}
	slices.Sort(resourceNames)

	for _, resource := range resourceNames {
		class := deviceClassMapping.GetDeviceClass(resource)
		// 没有对应 DeviceClass 的资源跳过，由其他机制处理。
		if class == nil {
			continue
		}

		// 短生命周期容器（非 sidecar init 容器）的 container+resource→request 映射。
		var shortLivedResourceMappings []v1.ContainerExtendedResourceRequest
		// 长生命周期容器（sidecar init 容器或普通容器）的 container+resource→request 映射。
		var longLivedResourceMappings []v1.ContainerExtendedResourceRequest

		// 长生命周期容器对该资源的请求列表，长度与容器数相同；未产生请求的槽位为 nil。
		// 靠后的请求可被更早执行的非 sidecar init 容器复用。
		longLivedResourceRequests := make([]*resourceapi.DeviceRequest, len(containers))
		for _, i := range longLivedContainerIndices {
			containerRequest, containerMappings := createResourceRequestAndMappings(i, &containers[i], resource, class.Name, nil)
			longLivedResourceRequests[i] = containerRequest                                     // 可能为 nil
			longLivedResourceMappings = append(longLivedResourceMappings, containerMappings...) // 可能为空
		}

		// 短生命周期容器中该资源请求量最大的那个。
		var maxShortLivedResourceRequest *resourceapi.DeviceRequest
		// 短生命周期容器产生的所有请求名；指向这些名字的映射都会被改写为最大请求的名字。
		shortLivedRequestNames := sets.New[string]()
		for _, i := range shortLivedInitContainerIndices {
			containerRequest, containerMappings := createResourceRequestAndMappings(i, &containers[i], resource, class.Name, longLivedResourceRequests[i:])
			if containerRequest != nil {
				shortLivedRequestNames.Insert(containerRequest.Name)
				if maxShortLivedResourceRequest == nil || maxShortLivedResourceRequest.Exactly.Count < containerRequest.Exactly.Count {
					maxShortLivedResourceRequest = containerRequest
				}
			}
			shortLivedResourceMappings = append(shortLivedResourceMappings, containerMappings...) // 可能为空
		}

		// init 容器顺序执行，只需一个能覆盖峰值的请求；把各 init 容器的映射统一指向最大请求名。
		if maxShortLivedResourceRequest != nil && len(shortLivedRequestNames) > 1 {
			shortLivedRequestNames.Delete(maxShortLivedResourceRequest.Name)
			for i := range shortLivedResourceMappings {
				if shortLivedRequestNames.Has(shortLivedResourceMappings[i].RequestName) {
					shortLivedResourceMappings[i].RequestName = maxShortLivedResourceRequest.Name
				}
			}
		}

		// 只收集非 nil 的请求。
		if maxShortLivedResourceRequest != nil {
			deviceRequests = append(deviceRequests, *maxShortLivedResourceRequest)
		}
		for _, request := range longLivedResourceRequests {
			if request != nil {
				deviceRequests = append(deviceRequests, *request)
			}
		}
		// 汇总映射。
		mappings = append(mappings, longLivedResourceMappings...)
		mappings = append(mappings, shortLivedResourceMappings...)
	}

	sort.Slice(deviceRequests, func(i, j int) bool {
		return deviceRequests[i].Name < deviceRequests[j].Name
	})
	return deviceRequests, mappings
}

// waitForExtendedClaimInAssumeCache 轮询 assume cache，直到扩展资源 claim 可见。
// claim 由 API server 创建，informer 的更新可能尚未到达 assume cache，
// 而 AssumeClaimAfterAPICall 在 informer 更新未到达时会返回 ErrNotFound，因此需要带超时地轮询。
func (pl *DynamicResources) waitForExtendedClaimInAssumeCache(
	ctx context.Context,
	claim *resourceapi.ResourceClaim,
) {
	logger := klog.FromContext(ctx)
	pollErr := wait.PollUntilContextTimeout(
		ctx,
		1*time.Second,
		time.Duration(AssumeExtendedResourceTimeoutDefaultSeconds)*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			if err := pl.draManager.ResourceClaims().AssumeClaimAfterAPICall(claim); err != nil {
				if errors.Is(err, assumecache.ErrNotFound) {
					return false, nil
				}
				logger.V(5).Info("Claim not stored in assume cache", "claim", klog.KObj(claim), "err", err)
				return false, err
			}
			return true, nil
		},
	)

	if pollErr != nil {
		logger.V(5).Info("Claim not stored in assume cache after retries", "claim", klog.KObj(claim), "err", pollErr)
		// claim 已在 API server 创建成功，此处仅记录日志、不返回失败。
	}
}

// createExtendedResourceClaimInAPI 在 API server 中创建扩展资源 claim。
func (pl *DynamicResources) createExtendedResourceClaimInAPI(
	ctx context.Context,
	pod *v1.Pod,
	nodeName string,
	state *stateData,
) (*resourceapi.ResourceClaim, error) {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("preparing to create claim for extended resources", "pod", klog.KObj(pod), "node", nodeName)
	// 取 Filter 阶段准备好的节点专属 claim。
	nodeAllocation, ok := state.nodeAllocations[nodeName]
	if !ok || nodeAllocation.extendedResourceClaim == nil {
		return nil, fmt.Errorf("extended resource claim not found for node %s", nodeName)
	}
	claim := nodeAllocation.extendedResourceClaim.DeepCopy()

	logger.V(5).Info("create claim for extended resources", "pod", klog.KObj(pod), "node", nodeName, "resourceclaim", klog.Format(claim))
	// 清除创建时不能或无需设置的字段。
	claim.Status.Allocation = nil
	claim.Name = ""
	claim.UID = ""

	createdClaim, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).Create(ctx, claim, metav1.CreateOptions{})
	if err != nil {
		metrics.ResourceClaimCreatesTotal.WithLabelValues("failure", "false", "", "Pod").Inc()
		return nil, fmt.Errorf("create claim for extended resources %v: %w", klog.KObj(claim), err)
	}
	metrics.ResourceClaimCreatesTotal.WithLabelValues("success", "false", "", "Pod").Inc()
	logger.V(5).Info("created claim for extended resources", "pod", klog.KObj(pod), "node", nodeName, "resourceclaim", klog.Format(createdClaim))

	return createdClaim, nil
}

// patchPodExtendedResourceClaimStatus 将扩展资源 claim 的信息（请求映射与 claim 名）写入 Pod status。
func (pl *DynamicResources) patchPodExtendedResourceClaimStatus(
	ctx context.Context,
	pod *v1.Pod,
	claim *resourceapi.ResourceClaim,
	nodeName string,
	state *stateData,
) error {
	var cer []v1.ContainerExtendedResourceRequest
	if nodeAllocation, ok := state.nodeAllocations[nodeName]; ok {
		cer = nodeAllocation.containerResourceRequestMappings
	}
	if len(cer) == 0 {
		return fmt.Errorf("nil or empty request mappings, no update of pod %s/%s ExtendedResourceClaimStatus", pod.Namespace, pod.Name)
	}

	podStatusCopy := pod.Status.DeepCopy()
	podStatusCopy.ExtendedResourceClaimStatus = &v1.PodExtendedResourceClaimStatus{
		RequestMappings:   cer,
		ResourceClaimName: claim.Name,
	}
	err := schedutil.PatchPodStatus(ctx, pl.clientset, pod.Name, pod.Namespace, &pod.Status, podStatusCopy)
	if err != nil {
		return fmt.Errorf("update pod %s/%s ExtendedResourceClaimStatus: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// unreserveExtendedResourceClaim 在调度失败时清理调度器持有的扩展资源 claim：
// 恢复 assume cache，并对已写入 API server 的 claim 执行删除。
func (pl *DynamicResources) unreserveExtendedResourceClaim(ctx context.Context, pod *v1.Pod, state *stateData) {
	extendedResourceClaim := state.claims.extendedResourceClaim()
	if extendedResourceClaim == nil {
		// 没有扩展资源 claim，无需清理。
		return
	}

	// 若 claim 被标记为分配进行中，先移除该标记，并把 assume cache 恢复到此轮调度前的状态。
	if deleted := pl.draManager.ResourceClaims().MaybeRemoveClaimPendingAllocation(state.claims.getInitialExtendedResourceClaimUID(), false); deleted {
		pl.draManager.ResourceClaims().AssumedClaimRestore(extendedResourceClaim.Namespace, extendedResourceClaim.Name)
	}
	if isSpecialClaimName(extendedResourceClaim.Name) {
		// 内存态临时 claim 无需删除。
		return
	}
	// claim 已写入 API server，必须删除，否则会留下孤立资源。
	logger := klog.FromContext(ctx)
	logger.V(5).Info("delete extended resource backed by DRA", "resourceclaim", klog.KObj(extendedResourceClaim), "pod", klog.KObj(pod), "claim.UID", extendedResourceClaim.UID)
	extendedResourceClaim = extendedResourceClaim.DeepCopy()
	if err := pl.deleteClaim(ctx, extendedResourceClaim); err != nil {
		logger.Error(err, "delete", "resourceclaim", klog.KObj(extendedResourceClaim))
	}
}
