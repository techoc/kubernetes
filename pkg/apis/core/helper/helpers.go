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

package helper

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/apis/core"
)

// IsPodLevelResourcesSet 检查是否设置了 Pod 级资源（PodLevelResources）。
// 只要 Requests 或 Limits map 非空即返回 true。
// 注意：需与 k8s.io/component-helpers/resource.IsPodLevelResourcesSet 保持一致。
func IsPodLevelResourcesSet(pod *core.Pod) bool {
	if pod.Spec.Resources == nil {
		return false
	}

	if (len(pod.Spec.Resources.Requests) + len(pod.Spec.Resources.Limits)) == 0 {
		return false
	}

	// 只认可受支持的 Pod 级资源，忽略其他资源名。
	for name := range pod.Spec.Resources.Requests {
		if resourcehelper.IsSupportedPodLevelResource(v1.ResourceName(name)) {
			return true
		}
	}

	for name := range pod.Spec.Resources.Limits {
		if resourcehelper.IsSupportedPodLevelResource(v1.ResourceName(name)) {
			return true
		}
	}

	return false
}

// IsHugePageResourceName 判断资源名是否带有大页（huge page）资源前缀。
func IsHugePageResourceName(name core.ResourceName) bool {
	return strings.HasPrefix(string(name), core.ResourceHugePagesPrefix)
}

// IsHugePageResourceValueDivisible 判断存储类资源值是否为页大小的整数倍。
func IsHugePageResourceValueDivisible(name core.ResourceName, quantity resource.Quantity) bool {
	pageSize, err := HugePageSizeFromResourceName(name)
	if err != nil {
		return false
	}

	// 页大小必须为正且是整字节数，否则无法做整除判断。
	if pageSize.Sign() <= 0 || pageSize.MilliValue()%int64(1000) != int64(0) {
		return false
	}

	return quantity.Value()%pageSize.Value() == 0
}

// IsQuotaHugePageResourceName 判断资源名是否带有与大页配额相关的资源前缀。
func IsQuotaHugePageResourceName(name core.ResourceName) bool {
	return strings.HasPrefix(string(name), core.ResourceHugePagesPrefix) || strings.HasPrefix(string(name), core.ResourceRequestsHugePagesPrefix)
}

// HugePageResourceName 为指定的页大小加上规范的大页前缀，生成对应的 ResourceName。
// 页大小会先被转换为规范表示形式。
func HugePageResourceName(pageSize resource.Quantity) core.ResourceName {
	return core.ResourceName(fmt.Sprintf("%s%s", core.ResourceHugePagesPrefix, pageSize.String()))
}

// HugePageSizeFromResourceName 从大页资源名中解析出页大小。
// 输入不是合法的大页资源名时返回错误。
func HugePageSizeFromResourceName(name core.ResourceName) (resource.Quantity, error) {
	if !IsHugePageResourceName(name) {
		return resource.Quantity{}, fmt.Errorf("resource name: %s is an invalid hugepage name", name)
	}
	pageSize := strings.TrimPrefix(string(name), core.ResourceHugePagesPrefix)
	return resource.ParseQuantity(pageSize)
}

// NonConvertibleFields 遍历给定的 map，筛出所有带有 "non-convertible.kubernetes.io" 前缀的键。
func NonConvertibleFields(annotations map[string]string) map[string]string {
	nonConvertibleKeys := map[string]string{}
	for key, value := range annotations {
		if strings.HasPrefix(key, core.NonConvertibleAnnotationPrefix) {
			nonConvertibleKeys[key] = value
		}
	}
	return nonConvertibleKeys
}

// Semantic 可对 core 对象做语义层面的深度比较。
// 例如 apiequality.Semantic.DeepEqual(aPod, aPodWithNonNilButEmptyMaps) == true。
var Semantic = conversion.EqualitiesOrDie(
	func(a, b resource.Quantity) bool {
		// 只比较数值大小，忽略格式差异：不同格式（如 DecimalSI 与 BinarySI）
		// 表示的是同一个量时不应视为不同。未初始化的 Quantity 与 0 等价。
		// TODO: if we decide it's important, it should be safe to start comparing the format.
		return a.Cmp(b) == 0
	},
	func(a, b metav1.MicroTime) bool {
		return a.UTC() == b.UTC()
	},
	func(a, b metav1.Time) bool {
		return a.UTC() == b.UTC()
	},
	func(a, b metav1.FieldsV1) bool {
		return a.Equal(b)
	},
	func(a, b labels.Selector) bool {
		return a.String() == b.String()
	},
	func(a, b fields.Selector) bool {
		return a.String() == b.String()
	},
)

var standardResourceQuotaScopes = sets.New(
	core.ResourceQuotaScopeTerminating,
	core.ResourceQuotaScopeNotTerminating,
	core.ResourceQuotaScopeBestEffort,
	core.ResourceQuotaScopeNotBestEffort,
	core.ResourceQuotaScopePriorityClass,
	core.ResourceQuotaScopeVolumeAttributesClass,
)

// IsStandardResourceQuotaScope 判断给定 scope 是否为标准取值。
func IsStandardResourceQuotaScope(scope core.ResourceQuotaScope) bool {
	return standardResourceQuotaScopes.Has(scope) || scope == core.ResourceQuotaScopeCrossNamespacePodAffinity
}

var podObjectCountQuotaResources = sets.New(
	core.ResourcePods,
)

var podComputeQuotaResources = sets.New(
	core.ResourceCPU,
	core.ResourceMemory,
	core.ResourceLimitsCPU,
	core.ResourceLimitsMemory,
	core.ResourceRequestsCPU,
	core.ResourceRequestsMemory,
)

var pvcObjectCountQuotaResources = sets.New(
	core.ResourcePersistentVolumeClaims,
)

var pvcStorageQuotaResources = sets.New(
	core.ResourceRequestsStorage,
)

// IsResourceQuotaScopeValidForResource 判断某个资源是否适用于指定的配额 scope。
func IsResourceQuotaScopeValidForResource(scope core.ResourceQuotaScope, resource core.ResourceName) bool {
	switch scope {
	case core.ResourceQuotaScopeTerminating, core.ResourceQuotaScopeNotTerminating, core.ResourceQuotaScopeNotBestEffort,
		core.ResourceQuotaScopePriorityClass, core.ResourceQuotaScopeCrossNamespacePodAffinity:
		return podObjectCountQuotaResources.Has(resource) || podComputeQuotaResources.Has(resource)
	case core.ResourceQuotaScopeBestEffort:
		return podObjectCountQuotaResources.Has(resource)
	case core.ResourceQuotaScopeVolumeAttributesClass:
		return pvcObjectCountQuotaResources.Has(resource) || pvcStorageQuotaResources.Has(resource)
	default:
		return true
	}
}

var standardContainerResources = sets.New(
	core.ResourceCPU,
	core.ResourceMemory,
	core.ResourceEphemeralStorage,
)

// IsStandardContainerResourceName 判断容器是否可以针对该资源发起 request。
func IsStandardContainerResourceName(name core.ResourceName) bool {
	return standardContainerResources.Has(name) || IsHugePageResourceName(name)
}

// IsNodeAllocatableResourceName 判断资源名是否对 DRA NodeAllocatableResources 有效。
// 目前仅支持 cpu、memory 和 hugepages（alpha 阶段暂不支持 ephemeral-storage）。
func IsNodeAllocatableResourceName(name core.ResourceName) bool {
	return name == core.ResourceCPU || name == core.ResourceMemory || IsHugePageResourceName(name)
}

// IsExtendedResourceName 在以下条件全部满足时返回 true：
// 1. 资源名不在默认命名空间中；
// 2. 资源名没有 "requests." 前缀，以免与配额约定产生混淆；
// 3. 转换为配额资源名后满足 IsQualifiedName() 的规则。
func IsExtendedResourceName(name core.ResourceName) bool {
	if IsNativeResource(name) || strings.HasPrefix(string(name), core.DefaultResourceRequestsPrefix) {
		return false
	}
	// 确保转换为配额资源名后满足 IsQualifiedName() 的规则，
	// 这样扩展资源才能安全地用于配额统计。
	nameForQuota := fmt.Sprintf("%s%s", core.DefaultResourceRequestsPrefix, string(name))
	if errs := validation.IsQualifiedName(nameForQuota); len(errs) != 0 {
		return false
	}
	return true
}

// IsNativeResource 判断资源名是否位于 *kubernetes.io/ 命名空间中。
// 不带前缀（部分限定）的名称隐式属于 kubernetes.io/ 命名空间。
func IsNativeResource(name core.ResourceName) bool {
	return !strings.Contains(string(name), "/") ||
		strings.Contains(string(name), core.ResourceDefaultNamespacePrefix)
}

// IsOvercommitAllowed 判断资源是否位于默认命名空间且不是大页资源。
func IsOvercommitAllowed(name core.ResourceName) bool {
	return IsNativeResource(name) &&
		!IsHugePageResourceName(name)
}

var standardLimitRangeTypes = sets.New(
	core.LimitTypePod,
	core.LimitTypeContainer,
	core.LimitTypePersistentVolumeClaim,
)

// IsStandardLimitRangeType 判断类型是否为 Pod、Container 或 PersistentVolumeClaim。
func IsStandardLimitRangeType(value core.LimitType) bool {
	return standardLimitRangeTypes.Has(value)
}

var standardQuotaResources = sets.New(
	core.ResourceCPU,
	core.ResourceMemory,
	core.ResourceEphemeralStorage,
	core.ResourceRequestsCPU,
	core.ResourceRequestsMemory,
	core.ResourceRequestsStorage,
	core.ResourceRequestsEphemeralStorage,
	core.ResourceLimitsCPU,
	core.ResourceLimitsMemory,
	core.ResourceLimitsEphemeralStorage,
	core.ResourcePods,
	core.ResourceQuotas,
	core.ResourceServices,
	core.ResourceReplicationControllers,
	core.ResourceSecrets,
	core.ResourcePersistentVolumeClaims,
	core.ResourceConfigMaps,
	core.ResourceServicesNodePorts,
	core.ResourceServicesLoadBalancers,
)

// IsStandardQuotaResourceName 判断资源是否为配额跟踪系统已知的资源。
func IsStandardQuotaResourceName(name core.ResourceName) bool {
	return standardQuotaResources.Has(name) || IsQuotaHugePageResourceName(name)
}

var standardResources = sets.New(
	core.ResourceCPU,
	core.ResourceMemory,
	core.ResourceEphemeralStorage,
	core.ResourceRequestsCPU,
	core.ResourceRequestsMemory,
	core.ResourceRequestsEphemeralStorage,
	core.ResourceLimitsCPU,
	core.ResourceLimitsMemory,
	core.ResourceLimitsEphemeralStorage,
	core.ResourcePods,
	core.ResourceQuotas,
	core.ResourceServices,
	core.ResourceReplicationControllers,
	core.ResourceSecrets,
	core.ResourceConfigMaps,
	core.ResourcePersistentVolumeClaims,
	core.ResourceStorage,
	core.ResourceRequestsStorage,
	core.ResourceServicesNodePorts,
	core.ResourceServicesLoadBalancers,
)

// IsStandardResourceName 判断资源是否为系统已知的资源。
func IsStandardResourceName(name core.ResourceName) bool {
	return standardResources.Has(name) || IsQuotaHugePageResourceName(name)
}

var integerResources = sets.New(
	core.ResourcePods,
	core.ResourceQuotas,
	core.ResourceServices,
	core.ResourceReplicationControllers,
	core.ResourceSecrets,
	core.ResourceConfigMaps,
	core.ResourcePersistentVolumeClaims,
	core.ResourceServicesNodePorts,
	core.ResourceServicesLoadBalancers,
)

// IsIntegerResourceName 判断资源是否以整数计量。
func IsIntegerResourceName(name core.ResourceName) bool {
	return integerResources.Has(name) || IsExtendedResourceName(name)
}

// IsServiceIPSet 检查 Service 的 ClusterIP 是否已设置。
// 目的不是在此处做校验，调用方需自行保证 Service 已通过语义校验。
func IsServiceIPSet(service *core.Service) bool {
	// 本函数假定 Service 已通过语义校验，只检查 ClusterIP 是否被设置，不验证 IP 的合法性。
	return len(service.Spec.ClusterIP) > 0 &&
		service.Spec.ClusterIP != core.ClusterIPNone
}

var standardFinalizers = sets.New(
	string(core.FinalizerKubernetes),
	metav1.FinalizerOrphanDependents,
	metav1.FinalizerDeleteDependents,
)

// IsStandardFinalizerName 检查输入字符串是否为标准的 finalizer 名称。
func IsStandardFinalizerName(str string) bool {
	return standardFinalizers.Has(str)
}

// GetAccessModesAsString 把访问模式数组转为字符串表示。
// 存在时各模式始终按固定顺序输出：RWO、ROX、RWX、RWOP。
func GetAccessModesAsString(modes []core.PersistentVolumeAccessMode) string {
	modes = removeDuplicateAccessModes(modes)
	modesStr := []string{}
	if ContainsAccessMode(modes, core.ReadWriteOnce) {
		modesStr = append(modesStr, "RWO")
	}
	if ContainsAccessMode(modes, core.ReadOnlyMany) {
		modesStr = append(modesStr, "ROX")
	}
	if ContainsAccessMode(modes, core.ReadWriteMany) {
		modesStr = append(modesStr, "RWX")
	}
	if ContainsAccessMode(modes, core.ReadWriteOncePod) {
		modesStr = append(modesStr, "RWOP")
	}
	return strings.Join(modesStr, ",")
}

// GetAccessModesFromString 将 GetAccessModesAsString 生成的字符串还原为访问模式数组。
func GetAccessModesFromString(modes string) []core.PersistentVolumeAccessMode {
	strmodes := strings.Split(modes, ",")
	accessModes := []core.PersistentVolumeAccessMode{}
	for _, s := range strmodes {
		s = strings.Trim(s, " ")
		switch {
		case s == "RWO":
			accessModes = append(accessModes, core.ReadWriteOnce)
		case s == "ROX":
			accessModes = append(accessModes, core.ReadOnlyMany)
		case s == "RWX":
			accessModes = append(accessModes, core.ReadWriteMany)
		case s == "RWOP":
			accessModes = append(accessModes, core.ReadWriteOncePod)
		}
	}
	return accessModes
}

// removeDuplicateAccessModes 去除访问模式数组中的重复项。
func removeDuplicateAccessModes(modes []core.PersistentVolumeAccessMode) []core.PersistentVolumeAccessMode {
	accessModes := []core.PersistentVolumeAccessMode{}
	for _, m := range modes {
		if !ContainsAccessMode(accessModes, m) {
			accessModes = append(accessModes, m)
		}
	}
	return accessModes
}

// ContainsAccessMode 判断访问模式数组中是否包含指定模式。
func ContainsAccessMode(modes []core.PersistentVolumeAccessMode, mode core.PersistentVolumeAccessMode) bool {
	for _, m := range modes {
		if m == mode {
			return true
		}
	}
	return false
}

// ClaimContainsAllocatedResources 判断 PVC 是否已分配资源（Status.AllocatedResources 非空）。
func ClaimContainsAllocatedResources(pvc *core.PersistentVolumeClaim) bool {
	if pvc == nil {
		return false
	}

	if pvc.Status.AllocatedResources != nil {
		return true
	}
	return false
}

// ClaimContainsAllocatedResourceStatus 判断 PVC 是否记录了对等资源的状态信息。
func ClaimContainsAllocatedResourceStatus(pvc *core.PersistentVolumeClaim) bool {
	if pvc == nil {
		return false
	}

	if pvc.Status.AllocatedResourceStatuses != nil {
		return true
	}
	return false
}

// GetTolerationsFromPodAnnotations 从 Pod.Annotations 中读取 JSON 序列化的容忍数据，
// 并将其实例化为 core 包中的 []Toleration。
func GetTolerationsFromPodAnnotations(annotations map[string]string) ([]core.Toleration, error) {
	var tolerations []core.Toleration
	if len(annotations) > 0 && annotations[core.TolerationsAnnotationKey] != "" {
		err := json.Unmarshal([]byte(annotations[core.TolerationsAnnotationKey]), &tolerations)
		if err != nil {
			return tolerations, err
		}
	}
	return tolerations, nil
}

// AddOrUpdateTolerationInPod 尝试向 Pod 的容忍列表添加或更新一条容忍。
// 发生更新时返回 true，无需变更时返回 false。
func AddOrUpdateTolerationInPod(pod *core.Pod, toleration *core.Toleration) bool {
	podTolerations := pod.Spec.Tolerations

	var newTolerations []core.Toleration
	updated := false
	for i := range podTolerations {
		if toleration.MatchToleration(&podTolerations[i]) {
			// 已存在语义完全相同的容忍则无需改动，直接返回。
			if Semantic.DeepEqual(toleration, podTolerations[i]) {
				return false
			}
			newTolerations = append(newTolerations, *toleration)
			updated = true
			continue
		}

		newTolerations = append(newTolerations, podTolerations[i])
	}

	if !updated {
		newTolerations = append(newTolerations, *toleration)
	}

	pod.Spec.Tolerations = newTolerations
	return true
}

// GetTaintsFromNodeAnnotations 从注解中读取 JSON 序列化的污点数据，
// 并将其实例化为 core 包中的 []Taint。
func GetTaintsFromNodeAnnotations(annotations map[string]string) ([]core.Taint, error) {
	var taints []core.Taint
	if len(annotations) > 0 && annotations[core.TaintsAnnotationKey] != "" {
		err := json.Unmarshal([]byte(annotations[core.TaintsAnnotationKey]), &taints)
		if err != nil {
			return []core.Taint{}, err
		}
	}
	return taints, nil
}

// GetPersistentVolumeClass 返回 PV 的 StorageClassName。
func GetPersistentVolumeClass(volume *core.PersistentVolume) string {
	// 优先使用 beta 注解，兼容仍依赖它的旧对象。
	if class, found := volume.Annotations[core.BetaStorageClassAnnotation]; found {
		return class
	}

	return volume.Spec.StorageClassName
}

// GetPersistentVolumeClaimClass 返回 PVC 的 StorageClassName，未指定时返回 ""。
func GetPersistentVolumeClaimClass(claim *core.PersistentVolumeClaim) string {
	// 优先使用 beta 注解，兼容仍依赖它的旧对象。
	if class, found := claim.Annotations[core.BetaStorageClassAnnotation]; found {
		return class
	}

	if claim.Spec.StorageClassName != nil {
		return *claim.Spec.StorageClassName
	}

	return ""
}

// PersistentVolumeClaimHasClass 判断给定 PVC 是否设置了 StorageClassName。
func PersistentVolumeClaimHasClass(claim *core.PersistentVolumeClaim) bool {
	// 优先使用 beta 注解，兼容仍依赖它的旧对象。
	if _, found := claim.Annotations[core.BetaStorageClassAnnotation]; found {
		return true
	}

	if claim.Spec.StorageClassName != nil {
		return true
	}

	return false
}

// GetDeletionCostFromPodAnnotations 返回 pod-deletion-cost 注解的整数值。
// 注解未设置或值非法时返回 0。
func GetDeletionCostFromPodAnnotations(annotations map[string]string) (int32, error) {
	if value, exist := annotations[core.PodDeletionCost]; exist {
		// 拒绝带 '+' 号（如 "+10"）或前导零（如 "008"）的值，统一规范的数字表示。
		if !validFirstDigit(value) {
			return 0, fmt.Errorf("invalid value %q", value)
		}

		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			// 解析出错时默认返回 0，避免把无效值传递给调用方。
			return 0, err
		}
		return int32(i), nil
	}
	return 0, nil
}

// validFirstDigit 校验字符串首字符是否合法：只允许 '-' 或 1-9，或唯一的 "0"。
func validFirstDigit(str string) bool {
	if len(str) == 0 {
		return false
	}
	return str[0] == '-' || (str[0] == '0' && str == "0") || (str[0] >= '1' && str[0] <= '9')
}

// HasInvalidLabelValueInNodeSelectorTerms 检查 NodeSelectorTerm 的 MatchExpression 值中是否存在非法 label 值。
func HasInvalidLabelValueInNodeSelectorTerms(terms []core.NodeSelectorTerm) bool {
	for _, term := range terms {
		for _, expression := range term.MatchExpressions {
			for _, value := range expression.Values {
				if len(validation.IsValidLabelValue(value)) > 0 {
					return true
				}
			}
		}
	}
	return false
}
