/*
Copyright 2016 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	api "k8s.io/kubernetes/pkg/apis/core"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	k8sfeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// pvcObjectCountName 是 PVC 对象计数配额的资源名（count/persistentvolumeclaims）。
var pvcObjectCountName = generic.ObjectCountQuotaResourceNameFor(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims").GroupResource())

// pvcResources 是 PVC 相关的静态配额资源列表。
// 每个资源可以根据 StorageClass 动态细化（如 gold.storageclass.storage.k8s.io/requests.storage）。
var pvcResources = []corev1.ResourceName{
	corev1.ResourcePersistentVolumeClaims,
	corev1.ResourceRequestsStorage,
}

// storageClassSuffix 是按 StorageClass 细分的配额资源名后缀。
// 格式：<storage-class>.storageclass.storage.k8s.io/<resource>
// 例如：
//   - gold.storageclass.storage.k8s.io/persistentvolumeclaims: 500Gi
//   - bronze.storageclass.storage.k8s.io/requests.storage: 500Gi
const storageClassSuffix string = ".storageclass.storage.k8s.io/"

/* TODO: 清理不再使用的实现？
// ResourceByStorageClass 根据 StorageClass 返回配额资源名。
func ResourceByStorageClass(storageClass string, resourceName corev1.ResourceName) corev1.ResourceName {
	return corev1.ResourceName(string(storageClass + storageClassSuffix + string(resourceName)))
}
*/

// V1ResourceByStorageClass 根据 StorageClass 返回配额资源名。
// 用于按 StorageClass 细分存储配额。
func V1ResourceByStorageClass(storageClass string, resourceName corev1.ResourceName) corev1.ResourceName {
	return corev1.ResourceName(string(storageClass + storageClassSuffix + string(resourceName)))
}

// NewPersistentVolumeClaimEvaluator 创建 PVC 配额评估器。
func NewPersistentVolumeClaimEvaluator(f quota.ListerForResourceFunc) quota.Evaluator {
	listFuncByNamespace := generic.ListResourceUsingListerFunc(f, corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"))
	pvcEvaluator := &pvcEvaluator{listFuncByNamespace: listFuncByNamespace}
	return pvcEvaluator
}

// pvcEvaluator 实现 PVC 配额使用量的计算。
type pvcEvaluator struct {
	// listFuncByNamespace 按命名空间列出 PVC
	listFuncByNamespace generic.ListFuncByNamespace
}

// Constraints 验证 PVC 是否满足配额要求。
// PVC 没有额外的约束检查，直接返回 nil。
func (p *pvcEvaluator) Constraints(required []corev1.ResourceName, item runtime.Object) error {
	return nil
}

// GroupResource 返回此评估器跟踪的 GroupResource（PVC）。
func (p *pvcEvaluator) GroupResource() schema.GroupResource {
	return corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims").GroupResource()
}

// Handles 判断评估器是否应该处理指定的 admission 操作。
// 处理 Create 和 Update 操作，以及 status subresource 中需要配额补充的情况。
func (p *pvcEvaluator) Handles(a admission.Attributes) bool {
	op := a.GetOperation()
	switch a.GetSubresource() {
	case "":
		return op == admission.Create || op == admission.Update
	case "status":
		// PVC 的 status 更新可能需要重新计算配额
		// 例如存储扩容失败恢复时，AllocatedResources 可能变化
		pvc, err1 := toExternalPersistentVolumeClaimOrError(a.GetObject())
		oldPVC, err2 := toExternalPersistentVolumeClaimOrError(a.GetOldObject())
		if err1 != nil || err2 != nil {
			return false
		}
		return RequiresQuotaReplenish(pvc, oldPVC)
	default:
		return false
	}
}

// Matches 判断给定的 ResourceQuota 是否与输入项（PVC）匹配。
// 如果启用了 VolumeAttributesClass 特性，支持 scope 匹配。
func (p *pvcEvaluator) Matches(resourceQuota *corev1.ResourceQuota, item runtime.Object) (bool, error) {
	if utilfeature.DefaultFeatureGate.Enabled(k8sfeatures.VolumeAttributesClass) {
		return generic.Matches(resourceQuota, item, p.MatchingResources, pvcMatchesScopeFunc)
	}
	return generic.Matches(resourceQuota, item, p.MatchingResources, generic.MatchesNoScopeFunc)
}

// MatchingScopes 返回 PVC 匹配的 scope selector 列表。
// 只有启用 VolumeAttributesClass 特性时才支持 scope 匹配。
func (p *pvcEvaluator) MatchingScopes(item runtime.Object, scopeSelectors []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	if utilfeature.DefaultFeatureGate.Enabled(k8sfeatures.VolumeAttributesClass) {
		matchedScopes := []corev1.ScopedResourceSelectorRequirement{}
		for _, selector := range scopeSelectors {
			match, err := pvcMatchesScopeFunc(selector, item)
			if err != nil {
				return []corev1.ScopedResourceSelectorRequirement{}, fmt.Errorf("error on matching scope %v: %w", selector, err)
			}
			if match {
				matchedScopes = append(matchedScopes, selector)
			}
		}
		return matchedScopes, nil
	}
	return []corev1.ScopedResourceSelectorRequirement{}, nil
}

// UncoveredQuotaScopes 返回有限制但没有对应配额覆盖的 scope 列表。
func (p *pvcEvaluator) UncoveredQuotaScopes(limitedScopes []corev1.ScopedResourceSelectorRequirement, matchedQuotaScopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	if utilfeature.DefaultFeatureGate.Enabled(k8sfeatures.VolumeAttributesClass) {
		uncoveredScopes := []corev1.ScopedResourceSelectorRequirement{}
		for _, selector := range limitedScopes {
			isCovered := false
			for _, matchedScopeSelector := range matchedQuotaScopes {
				if matchedScopeSelector.ScopeName == selector.ScopeName {
					isCovered = true
					break
				}
			}

			if !isCovered {
				uncoveredScopes = append(uncoveredScopes, selector)
			}
		}
		return uncoveredScopes, nil
	}
	return []corev1.ScopedResourceSelectorRequirement{}, nil
}

// MatchingResources 从输入的资源列表中返回与 PVC 配额相关的资源子集。
// 包括对象计数、PVC 数量、存储请求，以及按 StorageClass 细分的资源。
func (p *pvcEvaluator) MatchingResources(items []corev1.ResourceName) []corev1.ResourceName {
	result := []corev1.ResourceName{}
	for _, item := range items {
		// 匹配对象计数配额（count/persistentvolumeclaims）
		if quota.Contains([]corev1.ResourceName{pvcObjectCountName}, item) {
			result = append(result, item)
			continue
		}
		// 匹配 PVC 基础资源（persistentvolumeclaims, requests.storage）
		if quota.Contains(pvcResources, item) {
			result = append(result, item)
			continue
		}
		// 匹配按 StorageClass 细分的 PVC 资源
		// 格式：<storage-class-name>.storageclass.storage.k8s.io/<resource>
		for _, resource := range pvcResources {
			byStorageClass := storageClassSuffix + string(resource)
			if strings.HasSuffix(string(item), byStorageClass) {
				result = append(result, item)
				break
			}
		}
	}
	return result
}

// Usage 计算单个 PVC 的配额使用量。
// 包括 PVC 计数、对象计数、存储请求，以及按 StorageClass 细分的计数。
func (p *pvcEvaluator) Usage(item runtime.Object) (corev1.ResourceList, error) {
	result := corev1.ResourceList{}
	pvc, err := toExternalPersistentVolumeClaimOrError(item)
	if err != nil {
		return result, err
	}

	// 每个 PVC 计数为 1
	result[corev1.ResourcePersistentVolumeClaims] = *(resource.NewQuantity(1, resource.DecimalSI))
	result[pvcObjectCountName] = *(resource.NewQuantity(1, resource.DecimalSI))
	storageClassRef := storagehelpers.GetPersistentVolumeClaimClass(pvc)
	// 如果 PVC 指定了 StorageClass，同时计入该 StorageClass 的配额
	if len(storageClassRef) > 0 {
		storageClassClaim := corev1.ResourceName(storageClassRef + storageClassSuffix + string(corev1.ResourcePersistentVolumeClaims))
		result[storageClassClaim] = *(resource.NewQuantity(1, resource.DecimalSI))
	}

	requestedStorage := p.getStorageUsage(pvc)
	if requestedStorage != nil {
		result[corev1.ResourceRequestsStorage] = *requestedStorage
		// 如果 PVC 指定了 StorageClass，同时计入该 StorageClass 的存储配额
		if len(storageClassRef) > 0 {
			storageClassStorage := corev1.ResourceName(storageClassRef + storageClassSuffix + string(corev1.ResourceRequestsStorage))
			result[storageClassStorage] = *requestedStorage
		}
	}

	return result, nil
}

// getStorageUsage 获取 PVC 的存储使用量。
// 优先使用用户请求的存储大小，如果启用了 RecoverVolumeExpansionFailure 特性，
// 且 AllocatedResources 大于用户请求，则使用 AllocatedResources。
func (p *pvcEvaluator) getStorageUsage(pvc *corev1.PersistentVolumeClaim) *resource.Quantity {
	var result *resource.Quantity
	// roundUpFunc 确保存储请求是整字节值，通过 ResourceQuota 验证。
	// 见 https://issue.k8s.io/94313
	roundUpFunc := func(i *resource.Quantity) *resource.Quantity {
		roundedRequest := i.DeepCopy()
		if !roundedRequest.RoundUp(0) {
			return &roundedRequest
		}
		return i
	}

	if userRequest, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		result = roundUpFunc(&userRequest)
	}

	// 如果启用了 RecoverVolumeExpansionFailure 特性，且 AllocatedResources 大于用户请求，
	// 则使用 AllocatedResources（存储扩容失败恢复时，实际分配的可能大于用户请求）。
	if utilfeature.DefaultFeatureGate.Enabled(k8sfeatures.RecoverVolumeExpansionFailure) && result != nil {
		if len(pvc.Status.AllocatedResources) == 0 {
			return result
		}

		if allocatedRequest, ok := pvc.Status.AllocatedResources[corev1.ResourceStorage]; ok {
			if allocatedRequest.Cmp(*result) > 0 {
				result = roundUpFunc(&allocatedRequest)
			}
		}
	}
	return result
}

// UsageStats 计算命名空间内所有 PVC 的聚合配额使用统计。
func (p *pvcEvaluator) UsageStats(options quota.UsageStatsOptions) (quota.UsageStats, error) {
	if utilfeature.DefaultFeatureGate.Enabled(k8sfeatures.VolumeAttributesClass) {
		return generic.CalculateUsageStats(options, p.listFuncByNamespace, pvcMatchesScopeFunc, p.Usage)
	}
	return generic.CalculateUsageStats(options, p.listFuncByNamespace, generic.MatchesNoScopeFunc, p.Usage)
}

// 确认实现所需的配额评估器接口。
var _ quota.Evaluator = &pvcEvaluator{}

// toExternalPersistentVolumeClaimOrError 将 runtime.Object 转换为外部 PVC 对象。
func toExternalPersistentVolumeClaimOrError(obj runtime.Object) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	switch t := obj.(type) {
	case *corev1.PersistentVolumeClaim:
		pvc = t
	case *api.PersistentVolumeClaim:
		if err := k8s_api_v1.Convert_core_PersistentVolumeClaim_To_v1_PersistentVolumeClaim(t, pvc, nil); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("expect *api.PersistentVolumeClaim or *v1.PersistentVolumeClaim, got %v", t)
	}
	return pvc, nil
}

// RequiresQuotaReplenish 判断 PVC 的 status 更新是否需要重新计算配额。
// 当 AllocatedResources 变化或 VolumeAttributesClass 变化时需要补充配额。
func RequiresQuotaReplenish(pvc, oldPVC *corev1.PersistentVolumeClaim) bool {
	// 存储扩容失败恢复时，AllocatedResources 可能变化
	if utilfeature.DefaultFeatureGate.Enabled(k8sfeatures.RecoverVolumeExpansionFailure) {
		if oldPVC.Status.AllocatedResources.Storage() != pvc.Status.AllocatedResources.Storage() {
			return true
		}
	}
	// VolumeAttributesClass 变化时，需要重新计算配额
	if utilfeature.DefaultFeatureGate.Enabled(k8sfeatures.VolumeAttributesClass) {
		oldNames := getReferencedVolumeAttributesClassNames(oldPVC)
		newNames := getReferencedVolumeAttributesClassNames(pvc)
		if !oldNames.Equal(newNames) {
			return true
		}
	}
	return false
}

// pvcMatchesScopeFunc 判断 PVC 是否匹配给定的 scope selector。
// 目前只支持 VolumeAttributesClass scope。
func pvcMatchesScopeFunc(selector corev1.ScopedResourceSelectorRequirement, object runtime.Object) (bool, error) {
	pvc, err := toExternalPersistentVolumeClaimOrError(object)
	if err != nil {
		return false, err
	}

	if selector.ScopeName == corev1.ResourceQuotaScopeVolumeAttributesClass {
		// Exists 操作符只需检查 PVC 是否有 VolumeAttributesClass
		if selector.Operator == corev1.ScopeSelectorOpExists {
			// 这里只检查 PVC 是否引用了 volumeAttributesClass，
			// 无需付出解析和计算 selector 的开销。
			vacNames := getReferencedVolumeAttributesClassNames(pvc)
			return len(vacNames) != 0, nil
		}
		return pvcMatchesSelector(pvc, selector)
	}
	return false, nil
}

// pvcMatchesSelector 检查 PVC 的 VolumeAttributesClass 是否匹配给定的 label selector。
func pvcMatchesSelector(pvc *corev1.PersistentVolumeClaim, selector corev1.ScopedResourceSelectorRequirement) (bool, error) {
	labelSelector, err := helper.ScopedResourceSelectorRequirementsAsSelector(selector)
	if err != nil {
		return false, fmt.Errorf("failed to parse and convert selector: %w", err)
	}

	vacNames := getReferencedVolumeAttributesClassNames(pvc)
	if len(vacNames) == 0 {
		return labelSelector.Matches(labels.Set{}), nil
	}
	for vacName := range vacNames {
		m := labels.Set{string(corev1.ResourceQuotaScopeVolumeAttributesClass): vacName}
		if labelSelector.Matches(m) {
			return true, nil
		}
	}
	return false, nil
}

// getReferencedVolumeAttributesClassNames 获取 PVC 引用的所有 VolumeAttributesClass 名称。
// 包括 Spec、Status 和 ModifyVolumeStatus 中的名称。
func getReferencedVolumeAttributesClassNames(pvc *corev1.PersistentVolumeClaim) sets.Set[string] {
	vacNames := sets.New[string]()
	if len(ptr.Deref(pvc.Spec.VolumeAttributesClassName, "")) != 0 {
		vacNames.Insert(*pvc.Spec.VolumeAttributesClassName)
	}
	if len(ptr.Deref(pvc.Status.CurrentVolumeAttributesClassName, "")) != 0 {
		vacNames.Insert(*pvc.Status.CurrentVolumeAttributesClassName)
	}
	modifyStatus := pvc.Status.ModifyVolumeStatus
	if modifyStatus != nil && len(modifyStatus.TargetVolumeAttributesClassName) != 0 {
		vacNames.Insert(modifyStatus.TargetVolumeAttributesClassName)
	}
	return vacNames
}
