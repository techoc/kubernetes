/*
Copyright 2023 The Kubernetes Authors.

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

package core

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/admission"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	v1 "k8s.io/client-go/informers/resource/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/dynamic-resource-allocation/deviceclass/extendedresourcecache"
	resourceinternal "k8s.io/kubernetes/pkg/apis/resource"
	resourceversioned "k8s.io/kubernetes/pkg/apis/resource/v1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/clock"
)

// ClaimObjectCountName 是 ResourceClaim 对象计数配额的资源名（count/resourceclaims）。
// 此评估器负责 ResourceClaim 的计数，因为它的 GroupResource 是 resourceclaims。
var ClaimObjectCountName = generic.ObjectCountQuotaResourceNameFor(resourceapi.SchemeGroupVersion.WithResource("resourceclaims").GroupResource())

// V1ResourceByDeviceClass 根据 DeviceClass 返回配额资源名。
// 例如：myclass -> myclass.deviceclass.resource.k8s.io/devices
func V1ResourceByDeviceClass(className string) corev1.ResourceName {
	return corev1.ResourceName(className + corev1.ResourceClaimsPerClass)
}

// NewResourceClaimEvaluator 创建 ResourceClaim 配额评估器。
// deviceClassMapping 用于 DRA 扩展资源与 DeviceClass 的映射。
// podsGetter 和 claimGetter 用于验证扩展资源 Claim 的 Pod 所有者。
func NewResourceClaimEvaluator(f quota.ListerForResourceFunc, m *extendedresourcecache.ExtendedResourceCache, podsGetter corev1listers.PodLister, claimGetter resourceClaimPodOwnerGetter) quota.Evaluator {
	listFuncByNamespace := generic.ListResourceUsingListerFunc(f, resourceapi.SchemeGroupVersion.WithResource("resourceclaims"))
	claimEvaluator := &claimEvaluator{listFuncByNamespace: listFuncByNamespace, deviceClassMapping: m, podsGetter: podsGetter, claimGetter: claimGetter}
	return claimEvaluator
}

// claimEvaluator 实现 ResourceClaim 配额使用量的计算。
type claimEvaluator struct {
	// listFuncByNamespace 按命名空间列出 ResourceClaim
	listFuncByNamespace generic.ListFuncByNamespace
	// deviceClassMapping 全局 DeviceClass 与扩展资源的映射缓存
	deviceClassMapping *extendedresourcecache.ExtendedResourceCache
	// podsGetter 用于获取 Pod
	podsGetter corev1listers.PodLister
	// claimGetter 用于按命名空间和 Pod 所有者 UID 获取扩展资源的 Claim
	claimGetter resourceClaimPodOwnerGetter
}

// Constraints 验证 ResourceClaim 是否满足配额要求。
// ResourceClaim 没有额外的约束检查，直接返回 nil。
func (p *claimEvaluator) Constraints(required []corev1.ResourceName, item runtime.Object) error {
	return nil
}

// GroupResource 返回此评估器跟踪的 GroupResource（ResourceClaim）。
func (p *claimEvaluator) GroupResource() schema.GroupResource {
	return resourceapi.SchemeGroupVersion.WithResource("resourceclaims").GroupResource()
}

// Handles 判断评估器是否应该处理指定的 admission 操作。
// 处理 Create 和 Update 操作，不处理 subresource。
func (p *claimEvaluator) Handles(a admission.Attributes) bool {
	if a.GetSubresource() != "" {
		return false
	}
	op := a.GetOperation()
	return admission.Create == op || admission.Update == op
}

// Matches 判断给定的 ResourceQuota 是否与输入项（ResourceClaim）匹配。
// ResourceClaim 不支持 scope 匹配。
func (p *claimEvaluator) Matches(resourceQuota *corev1.ResourceQuota, item runtime.Object) (bool, error) {
	return generic.Matches(resourceQuota, item, p.MatchingResources, generic.MatchesNoScopeFunc)
}

// MatchingScopes 返回 ResourceClaim 匹配的 scope selector 列表。
// ResourceClaim 不支持 scope，始终返回空列表。
func (p *claimEvaluator) MatchingScopes(item runtime.Object, scopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	return []corev1.ScopedResourceSelectorRequirement{}, nil
}

// UncoveredQuotaScopes 返回有限制但没有对应配额覆盖的 scope 列表。
// ResourceClaim 不支持 scope，始终返回空列表。
func (p *claimEvaluator) UncoveredQuotaScopes(limitedScopes []corev1.ScopedResourceSelectorRequirement, matchedQuotaScopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	return []corev1.ScopedResourceSelectorRequirement{}, nil
}

// MatchingResources 从输入的资源列表中返回与 ResourceClaim 配额相关的资源子集。
// 包括对象计数、按 DeviceClass 的资源、隐式扩展资源和扩展资源名。
func (p *claimEvaluator) MatchingResources(items []corev1.ResourceName) []corev1.ResourceName {
	result := []corev1.ResourceName{}
	for _, item := range items {
		// 匹配对象计数配额或按 DeviceClass 的资源
		if item == ClaimObjectCountName ||
			strings.HasSuffix(string(item), corev1.ResourceClaimsPerClass) {
			result = append(result, item)
			continue
		}
		// 如果启用了 DRAExtendedResource 特性，匹配隐式扩展资源和扩展资源名
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) {
			// 匹配隐式扩展资源名（requests.deviceclass.resource.kubernetes.io/<className>）
			if strings.HasPrefix(string(item), corev1.ResourceImplicitExtendedClaimsPerClass) {
				className := string(item[len(corev1.ResourceImplicitExtendedClaimsPerClass):])
				if p.deviceClassMapping.GetExtendedResource(className) != "" {
					result = append(result, item)
					continue
				}
			}
			// 匹配扩展资源名（requests.<extendedResourceName>）
			if isExtendedResourceNameForQuota(item) {
				resourceName := string(item[len(corev1.DefaultResourceRequestsPrefix):])
				if p.deviceClassMapping.GetDeviceClass(corev1.ResourceName(resourceName)) != nil {
					result = append(result, item)
				}
			}
		}
	}
	return result
}

// addExtendedResourceQuota 将扩展资源配额添加到 ResourceClaim 的使用量中。
// 将 DeviceClass 的使用量转换为隐式扩展资源名和实际扩展资源名的配额，
// 并减去 Pod 已占用的部分，避免重复计算。
func (p *claimEvaluator) addExtendedResourceQuota(resourceClaimUsage map[corev1.ResourceName]resource.Quantity, podUsage corev1.ResourceList) {
	extendedResourceUsage := make(map[corev1.ResourceName]resource.Quantity)
	for name, quantity := range resourceClaimUsage {
		// 提取 DeviceClass 名称，例如 myclass.deviceclass.resource.k8s.io/devices -> myclass
		deviceClassName, isDeviceClassUsage := strings.CutSuffix(string(name), corev1.ResourceClaimsPerClass)
		if !isDeviceClassUsage || len(deviceClassName) == 0 {
			continue
		}

		// 添加隐式扩展资源配额：requests.deviceclass.resource.kubernetes.io/myclass
		extendedResourceUsage[corev1.ResourceName(corev1.ResourceImplicitExtendedClaimsPerClass+deviceClassName)] = quantity

		// 如果有对应的实际扩展资源名（如 example.com/mygpu），添加其配额
		if extendedResourceName := p.deviceClassMapping.GetExtendedResource(deviceClassName); len(extendedResourceName) > 0 {
			extendedResourceUsage[corev1.ResourceName(corev1.DefaultResourceRequestsPrefix+extendedResourceName)] = quantity
		}
	}

	// 减去 Pod 已占用的部分，避免重复计算
	for name, quantity := range extendedResourceUsage {
		if podQuantity, found := podUsage[name]; found {
			quantity.Sub(podQuantity)
		}
		if quantity.CmpInt64(0) > 0 {
			resourceClaimUsage[name] = quantity
		}
	}
}

// getVerifiedPodUsage 验证扩展资源 Claim 的 Pod 所有者是否存在，并返回 Pod 的配额使用量。
// 只有当 Claim 由 Pod 拥有且 Pod 的 ExtendedResourceClaimStatus 指向该 Claim 时，
// 才返回 Pod 的使用量，用于避免扩展资源在 Pod 和 Claim 层面重复计算。
// 如果验证失败，返回 nil。
func (p *claimEvaluator) getVerifiedPodUsage(claim *resourceapi.ResourceClaim) corev1.ResourceList {
	// 只处理扩展资源 Claim
	if claim.Annotations[resourceapi.ExtendedResourceClaimAnnotation] != "true" {
		return nil
	}
	controllerRef := metav1.GetControllerOfNoCopy(claim)
	if controllerRef == nil {
		return nil
	}
	// 只处理由 Pod 拥有的 Claim
	if controllerRef.Kind != "Pod" || controllerRef.APIVersion != "v1" {
		return nil
	}
	if p.podsGetter == nil {
		return nil
	}
	pod, err := p.podsGetter.Pods(claim.Namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if controllerRef.UID != pod.UID {
		return nil
	}
	// 如果 Pod 标识了扩展资源 Claim，验证是否指向当前 Claim
	if pod.Status.ExtendedResourceClaimStatus != nil && pod.Status.ExtendedResourceClaimStatus.ResourceClaimName != claim.Name {
		return nil
	}
	// 如果 Pod 未标识扩展资源 Claim，确保当前 Claim 是最早创建或唯一的 Claim
	// 避免多个 Claim 都豁免 Pod 使用量导致重复计算
	if pod.Status.ExtendedResourceClaimStatus == nil {
		ownedClaims, err := p.claimGetter(claim.Namespace, pod.UID)
		if err != nil {
			return nil
		}
		for _, ownedClaim := range ownedClaims {
			switch ownedClaim.CreationTimestamp.Time.Compare(claim.CreationTimestamp.Time) {
			case -1:
				// 有更早创建的 Claim，不豁免当前 Claim
				return nil
			case 0:
				// 同时创建时，名称更小的 Claim 优先
				if ownedClaim.Name < claim.Name {
					return nil
				}
			case 1:
				continue
			}
		}
	}

	quotaReqs, err := PodUsageFunc(pod, clock.RealClock{})
	if err != nil {
		return nil
	}
	return quotaReqs
}

// Usage 计算单个 ResourceClaim 的配额使用量。
// 包括对象计数和按 DeviceClass 的设备数量。
// 对于 FirstAvailable 模式，取每个 DeviceClass 的最大设备数（最坏情况）。
func (p *claimEvaluator) Usage(item runtime.Object) (corev1.ResourceList, error) {
	result := corev1.ResourceList{}
	claim, err := toExternalResourceClaimOrError(item)
	if err != nil {
		return result, err
	}

	// 每个 ResourceClaim 计数为 1
	result[ClaimObjectCountName] = *(resource.NewQuantity(1, resource.DecimalSI))
	// 处理设备请求的配额计算
	for _, request := range claim.Spec.Devices.Requests {
		switch {
		case len(request.FirstAvailable) > 0:
			// FirstAvailable 模式：从多个 DeviceClass 中选择一个可用的。
			// 配额需要按最坏情况计算，即每个 DeviceClass 的最大设备数。
			maxQuantityByDeviceClassClaim := make(map[corev1.ResourceName]resource.Quantity)
			for _, subrequest := range request.FirstAvailable {
				deviceClassClaim := V1ResourceByDeviceClass(subrequest.DeviceClassName)
				var numDevices int64
				switch subrequest.AllocationMode {
				case resourceapi.DeviceAllocationModeExactCount:
					numDevices = subrequest.Count
				case resourceapi.DeviceAllocationModeAll:
					// All 模式的最坏情况：所有可能的设备
					numDevices = resourceapi.AllocationResultsMaxSize
				default:
					// 降级后可能出现未知模式，不计入配额
				}

				q := resource.NewQuantity(numDevices, resource.DecimalSI)
				if q.Cmp(maxQuantityByDeviceClassClaim[deviceClassClaim]) > 0 {
					maxQuantityByDeviceClassClaim[deviceClassClaim] = *q
				}
			}
			for deviceClassClaim, q := range maxQuantityByDeviceClassClaim {
				quantity := result[deviceClassClaim]
				quantity.Add(q)
				result[deviceClassClaim] = quantity
			}
			continue
		case request.Exactly != nil:
			// Exactly 模式：指定具体的 DeviceClass
			deviceClassClaim := V1ResourceByDeviceClass(request.Exactly.DeviceClassName)
			var numDevices int64
			switch request.Exactly.AllocationMode {
			case resourceapi.DeviceAllocationModeExactCount:
				numDevices = request.Exactly.Count
			case resourceapi.DeviceAllocationModeAll:
				// All 模式的最坏情况
				numDevices = resourceapi.AllocationResultsMaxSize
			default:
				// 降级后可能出现未知模式，不计入配额
			}
			quantity := result[deviceClassClaim]
			quantity.Add(*(resource.NewQuantity(numDevices, resource.DecimalSI)))
			result[deviceClassClaim] = quantity
		default:
			// 未知的请求类型，无法计算配额
		}
	}

	// 如果启用了 DRAExtendedResource 特性，添加扩展资源配额
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) {
		p.addExtendedResourceQuota(result, p.getVerifiedPodUsage(claim))
	}

	return result, nil
}

// UsageStats 计算命名空间内所有 ResourceClaim 的聚合配额使用统计。
func (p *claimEvaluator) UsageStats(options quota.UsageStatsOptions) (quota.UsageStats, error) {
	return generic.CalculateUsageStats(options, p.listFuncByNamespace, generic.MatchesNoScopeFunc, p.Usage)
}

// ensure we implement required interface
var _ quota.Evaluator = &claimEvaluator{}

// toExternalResourceClaimOrError 将 runtime.Object 转换为外部 ResourceClaim 对象。
func toExternalResourceClaimOrError(obj runtime.Object) (*resourceapi.ResourceClaim, error) {
	claim := &resourceapi.ResourceClaim{}
	switch t := obj.(type) {
	case *resourceapi.ResourceClaim:
		claim = t
	case *resourceinternal.ResourceClaim:
		if err := resourceversioned.Convert_resource_ResourceClaim_To_v1_ResourceClaim(t, claim, nil); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("expect %T or %T, got %v", &resourceapi.ResourceClaim{}, &resourceinternal.ResourceClaim{}, t)
	}
	return claim, nil
}

// extendedResourceClaimPodOwnerIndexName 是按 Pod 所有者 UID 索引 ResourceClaim 的索引器名称。
const extendedResourceClaimPodOwnerIndexName = "extendedResourceClaimPodUIDOwner"

// resourceClaimPodOwnerKey 生成 Pod 所有者的索引键（namespace/podUID）。
func resourceClaimPodOwnerKey(namespace string, podUID types.UID) string {
	return namespace + "/" + string(podUID)
}

// resourceClaimPodOwnerGetter 按命名空间和 Pod 所有者 UID 获取 ResourceClaim 列表。
type resourceClaimPodOwnerGetter func(namespace string, podUID types.UID) ([]*resourceapi.ResourceClaim, error)

// makeResourceClaimPodOwnerGetter 创建 resourceClaimPodOwnerGetter。
// 为 ResourceClaimInformer 添加按 Pod 所有者 UID 的索引器，以便快速查找。
func makeResourceClaimPodOwnerGetter(resourceClaimInformer v1.ResourceClaimInformer) (resourceClaimPodOwnerGetter, error) {
	indexers := cache.Indexers{extendedResourceClaimPodOwnerIndexName: extendedResourceClaimPodUIDOwnerIndex}
	if err := resourceClaimInformer.Informer().AddIndexers(indexers); err != nil {
		_, exists := resourceClaimInformer.Informer().GetIndexer().GetIndexers()[extendedResourceClaimPodOwnerIndexName]
		if !exists {
			return nil, fmt.Errorf("failed to add resource claim pod owner indexer: %w", err)
		}
	}
	indexer := resourceClaimInformer.Informer().GetIndexer()
	return func(namespace string, podUID types.UID) ([]*resourceapi.ResourceClaim, error) {
		objects, err := indexer.ByIndex(extendedResourceClaimPodOwnerIndexName, resourceClaimPodOwnerKey(namespace, podUID))
		if err != nil {
			return nil, err
		}
		claims := make([]*resourceapi.ResourceClaim, 0, len(objects))
		for _, obj := range objects {
			if claim, ok := obj.(*resourceapi.ResourceClaim); ok {
				claims = append(claims, claim)
			} else {
				return nil, fmt.Errorf("failed to get resource claim from indexer")
			}
		}
		return claims, nil
	}, nil
}

// extendedResourceClaimPodUIDOwnerIndex 是 ResourceClaim 的索引函数。
// 只索引由 Pod 拥有的扩展资源 Claim，返回 Pod 的 UID 作为索引键。
func extendedResourceClaimPodUIDOwnerIndex(obj interface{}) ([]string, error) {
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return nil, nil
	}
	if claim.Annotations[resourceapi.ExtendedResourceClaimAnnotation] != "true" {
		return nil, nil
	}
	controllerRef := metav1.GetControllerOfNoCopy(claim)
	if controllerRef == nil {
		return nil, nil
	}
	if controllerRef.Kind != "Pod" || controllerRef.APIVersion != "v1" {
		return nil, nil
	}
	return []string{resourceClaimPodOwnerKey(claim.Namespace, controllerRef.UID)}, nil
}
