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
	"fmt"
	"k8s.io/klog/v2"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/validation"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/kubernetes/pkg/features"
)

// IsExtendedResourceName 在以下条件全部满足时返回 true：
// 1. 资源名不在默认命名空间中；
// 2. 资源名没有 "requests." 前缀，以免与配额约定产生混淆；
// 3. 转换为配额资源名后满足 IsQualifiedName() 的规则。
func IsExtendedResourceName(name v1.ResourceName) bool {
	if IsNativeResource(name) || strings.HasPrefix(string(name), v1.DefaultResourceRequestsPrefix) {
		return false
	}
	// 确保转换为配额资源名后满足 IsQualifiedName() 的规则，
	// 这样扩展资源才能安全地用于配额统计。
	nameForQuota := fmt.Sprintf("%s%s", v1.DefaultResourceRequestsPrefix, string(name))
	if errs := validation.IsQualifiedName(nameForQuota); len(errs) != 0 {
		return false
	}
	return true
}

// IsPrefixedNativeResource 判断资源名是否位于 *kubernetes.io/ 命名空间。
func IsPrefixedNativeResource(name v1.ResourceName) bool {
	return strings.Contains(string(name), v1.ResourceDefaultNamespacePrefix)
}

// IsNativeResource 判断资源名是否位于 *kubernetes.io/ 命名空间。
// 不带前缀（部分限定）的名称隐式属于 kubernetes.io/ 命名空间。
func IsNativeResource(name v1.ResourceName) bool {
	return !strings.Contains(string(name), "/") ||
		IsPrefixedNativeResource(name)
}

// IsHugePageResourceName 判断资源名是否带有大页（huge page）资源前缀。
func IsHugePageResourceName(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), v1.ResourceHugePagesPrefix)
}

// HugePageResourceName 为指定的页大小加上规范的大页前缀，生成对应的 ResourceName。
// 页大小会先被转换为规范表示形式。
func HugePageResourceName(pageSize resource.Quantity) v1.ResourceName {
	return v1.ResourceName(fmt.Sprintf("%s%s", v1.ResourceHugePagesPrefix, pageSize.String()))
}

// HugePageSizeFromResourceName 从大页资源名中解析出页大小。
// 输入不是合法的大页资源名时返回错误。
func HugePageSizeFromResourceName(name v1.ResourceName) (resource.Quantity, error) {
	if !IsHugePageResourceName(name) {
		return resource.Quantity{}, fmt.Errorf("resource name: %s is an invalid hugepage name", name)
	}
	pageSize := strings.TrimPrefix(string(name), v1.ResourceHugePagesPrefix)
	return resource.ParseQuantity(pageSize)
}

// HugePageUnitSizeFromByteSize 把字节大小转换为 "<size><unit-prefix>B" 格式的大页大小字符串。
// size 必须能被表示为最大的可用单位（如 1024 = "1KB"，1048576 = "1MB"）。
func HugePageUnitSizeFromByteSize(size int64) (string, error) {
	// hugePageSizeUnitList is borrowed from opencontainers/runc/libcontainer/cgroups/utils.go
	var hugePageSizeUnitList = []string{"B", "KB", "MB", "GB", "TB", "PB"}
	idx := 0
	len := len(hugePageSizeUnitList) - 1
	for size%1024 == 0 && idx < len {
		size /= 1024
		idx++
	}
	if size > 1024 && idx < len {
		return "", fmt.Errorf("size: %d%s must be guaranteed to divisible into the largest units", size, hugePageSizeUnitList[idx])
	}
	return fmt.Sprintf("%d%s", size, hugePageSizeUnitList[idx]), nil
}

// IsHugePageMedium 判断卷介质是否为 'HugePages[-size]' 格式。
func IsHugePageMedium(medium v1.StorageMedium) bool {
	if medium == v1.StorageMediumHugePages {
		return true
	}
	return strings.HasPrefix(string(medium), string(v1.StorageMediumHugePagesPrefix))
}

// HugePageSizeFromMedium 从大页介质名中解析出页大小。
// 输入不是合法的大页介质时返回错误。
func HugePageSizeFromMedium(medium v1.StorageMedium) (resource.Quantity, error) {
	if !IsHugePageMedium(medium) {
		return resource.Quantity{}, fmt.Errorf("medium: %s is not a hugepage medium", medium)
	}
	if medium == v1.StorageMediumHugePages {
		return resource.Quantity{}, fmt.Errorf("medium: %s doesn't have size information", medium)
	}
	pageSize := strings.TrimPrefix(string(medium), string(v1.StorageMediumHugePagesPrefix))
	return resource.ParseQuantity(pageSize)
}

// IsOvercommitAllowed 判断资源是否位于默认命名空间且不是大页资源。
func IsOvercommitAllowed(name v1.ResourceName) bool {
	return IsNativeResource(name) &&
		!IsHugePageResourceName(name)
}

// IsAttachableVolumeResourceName 判断资源名是否带有可挂载卷资源前缀。
func IsAttachableVolumeResourceName(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), v1.ResourceAttachableVolumesPrefix)
}

// IsServiceIPSet 检查 Service 的 ClusterIP 是否已设置。
// 目的不是在此处做校验，调用方需自行保证 Service 已通过语义校验。
func IsServiceIPSet(service *v1.Service) bool {
	return service.Spec.ClusterIP != v1.ClusterIPNone && service.Spec.ClusterIP != ""
}

// GetAccessModesAsString 把访问模式数组转为字符串表示。
// 存在时各模式始终按固定顺序输出：RWO、ROX、RWX、RWOP。
func GetAccessModesAsString(modes []v1.PersistentVolumeAccessMode) string {
	modes = removeDuplicateAccessModes(modes)
	modesStr := []string{}
	if ContainsAccessMode(modes, v1.ReadWriteOnce) {
		modesStr = append(modesStr, "RWO")
	}
	if ContainsAccessMode(modes, v1.ReadOnlyMany) {
		modesStr = append(modesStr, "ROX")
	}
	if ContainsAccessMode(modes, v1.ReadWriteMany) {
		modesStr = append(modesStr, "RWX")
	}
	if ContainsAccessMode(modes, v1.ReadWriteOncePod) {
		modesStr = append(modesStr, "RWOP")
	}
	return strings.Join(modesStr, ",")
}

// GetAccessModesFromString 将 GetAccessModesAsString 生成的字符串还原为访问模式数组。
func GetAccessModesFromString(modes string) []v1.PersistentVolumeAccessMode {
	strmodes := strings.Split(modes, ",")
	accessModes := []v1.PersistentVolumeAccessMode{}
	for _, s := range strmodes {
		s = strings.Trim(s, " ")
		switch {
		case s == "RWO":
			accessModes = append(accessModes, v1.ReadWriteOnce)
		case s == "ROX":
			accessModes = append(accessModes, v1.ReadOnlyMany)
		case s == "RWX":
			accessModes = append(accessModes, v1.ReadWriteMany)
		case s == "RWOP":
			accessModes = append(accessModes, v1.ReadWriteOncePod)
		}
	}
	return accessModes
}

// removeDuplicateAccessModes 去除访问模式数组中的重复项。
func removeDuplicateAccessModes(modes []v1.PersistentVolumeAccessMode) []v1.PersistentVolumeAccessMode {
	accessModes := []v1.PersistentVolumeAccessMode{}
	for _, m := range modes {
		if !ContainsAccessMode(accessModes, m) {
			accessModes = append(accessModes, m)
		}
	}
	return accessModes
}

// ContainsAccessMode 判断访问模式数组中是否包含指定模式。
func ContainsAccessMode(modes []v1.PersistentVolumeAccessMode, mode v1.PersistentVolumeAccessMode) bool {
	for _, m := range modes {
		if m == mode {
			return true
		}
	}
	return false
}

// NodeSelectorRequirementKeysExistInNodeSelectorTerms 检查 terms 中是否已存在与 reqs 同 key 的 NodeSelectorTerm。
func NodeSelectorRequirementKeysExistInNodeSelectorTerms(reqs []v1.NodeSelectorRequirement, terms []v1.NodeSelectorTerm) bool {
	for _, req := range reqs {
		for _, term := range terms {
			for _, r := range term.MatchExpressions {
				if r.Key == req.Key {
					return true
				}
			}
		}
	}
	return false
}

// TopologySelectorRequirementsAsSelector 把 []TopologySelectorLabelRequirement 转换为实现 labels.Selector 的结构。
func TopologySelectorRequirementsAsSelector(tsm []v1.TopologySelectorLabelRequirement) (labels.Selector, error) {
	if len(tsm) == 0 {
		return labels.Nothing(), nil
	}

	selector := labels.NewSelector()
	for _, expr := range tsm {
		r, err := labels.NewRequirement(expr.Key, selection.In, expr.Values)
		if err != nil {
			return nil, err
		}
		selector = selector.Add(*r)
	}

	return selector, nil
}

// MatchTopologySelectorTerms 检查给定标签是否匹配任一拓扑选择器项（各项之间为 OR 关系）。
// nil 或空的项不匹配任何对象；而空的项列表则匹配所有对象。
func MatchTopologySelectorTerms(topologySelectorTerms []v1.TopologySelectorTerm, lbls labels.Set) bool {
	if len(topologySelectorTerms) == 0 {
		// 空的项列表表示不设限制，匹配所有对象。
		return true
	}

	for _, req := range topologySelectorTerms {
		// nil 或空项不匹配任何对象，跳过。
		if len(req.MatchLabelExpressions) == 0 {
			continue
		}

		labelSelector, err := TopologySelectorRequirementsAsSelector(req.MatchLabelExpressions)
		if err != nil || !labelSelector.Matches(lbls) {
			continue
		}

		return true
	}

	return false
}

// AddOrUpdateTolerationInPodSpec 尝试向 PodSpec 的容忍列表添加或更新一条容忍。
// 发生更新时返回 true，无需变更时返回 false。
func AddOrUpdateTolerationInPodSpec(spec *v1.PodSpec, toleration *v1.Toleration) bool {
	podTolerations := spec.Tolerations

	var newTolerations []v1.Toleration
	updated := false
	for i := range podTolerations {
		if toleration.MatchToleration(&podTolerations[i]) {
			if helper.Semantic.DeepEqual(toleration, podTolerations[i]) {
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

	spec.Tolerations = newTolerations
	return true
}

// GetMatchingTolerations 检查所有污点是否都被容忍：
// 若全部被容忍，返回 true 及每条污点对应的容忍；否则返回 false。
func GetMatchingTolerations(logger klog.Logger, taints []v1.Taint, tolerations []v1.Toleration) (bool, []v1.Toleration) {
	if len(taints) == 0 {
		return true, []v1.Toleration{}
	}
	if len(tolerations) == 0 && len(taints) > 0 {
		return false, []v1.Toleration{}
	}
	enableComparisonOperators := utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators)
	result := []v1.Toleration{}
	for i := range taints {
		tolerated := false
		for j := range tolerations {
			if tolerations[j].ToleratesTaint(logger, &taints[i], enableComparisonOperators) {
				result = append(result, tolerations[j])
				tolerated = true
				break
			}
		}
		if !tolerated {
			return false, []v1.Toleration{}
		}
	}
	return true, result
}

// ScopedResourceSelectorRequirementsAsSelector 把 ScopedResourceSelectorRequirement 转换为实现 labels.Selector 的结构。
func ScopedResourceSelectorRequirementsAsSelector(ssr v1.ScopedResourceSelectorRequirement) (labels.Selector, error) {
	selector := labels.NewSelector()
	var op selection.Operator
	switch ssr.Operator {
	case v1.ScopeSelectorOpIn:
		op = selection.In
	case v1.ScopeSelectorOpNotIn:
		op = selection.NotIn
	case v1.ScopeSelectorOpExists:
		op = selection.Exists
	case v1.ScopeSelectorOpDoesNotExist:
		op = selection.DoesNotExist
	default:
		return nil, fmt.Errorf("%q is not a valid scope selector operator", ssr.Operator)
	}
	r, err := labels.NewRequirement(string(ssr.ScopeName), op, ssr.Values)
	if err != nil {
		return nil, err
	}
	selector = selector.Add(*r)
	return selector, nil
}
