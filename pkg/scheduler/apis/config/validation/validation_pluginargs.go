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

package validation

import (
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
)

// supportedScoringStrategyTypes 必须是字符串集合，以便直接传给 field.NotSupported。
var supportedScoringStrategyTypes = sets.New(
	string(config.LeastAllocated),
	string(config.MostAllocated),
	string(config.RequestedToCapacityRatio),
)

// ValidateDefaultPreemptionArgs 校验 DefaultPreemptionArgs 是否合法。
// 除取值范围外，两个最小候选节点阈值不允许同时为 0。
func ValidateDefaultPreemptionArgs(path *field.Path, args *config.DefaultPreemptionArgs) error {
	var allErrs field.ErrorList
	percentagePath := path.Child("minCandidateNodesPercentage")
	absolutePath := path.Child("minCandidateNodesAbsolute")
	if err := validateMinCandidateNodesPercentage(args.MinCandidateNodesPercentage, percentagePath); err != nil {
		allErrs = append(allErrs, err)
	}
	if err := validateMinCandidateNodesAbsolute(args.MinCandidateNodesAbsolute, absolutePath); err != nil {
		allErrs = append(allErrs, err)
	}
	// 两个阈值都为 0 会让默认抢占失去候选下限，必须报错。
	if args.MinCandidateNodesPercentage == 0 && args.MinCandidateNodesAbsolute == 0 {
		allErrs = append(allErrs,
			field.Invalid(percentagePath, args.MinCandidateNodesPercentage, "cannot be zero at the same time as minCandidateNodesAbsolute"),
			field.Invalid(absolutePath, args.MinCandidateNodesAbsolute, "cannot be zero at the same time as minCandidateNodesPercentage"))
	}
	return allErrs.ToAggregate()
}

// validateMinCandidateNodesPercentage 校验 minCandidateNodesPercentage 在允许范围 [0, 100] 内。
func validateMinCandidateNodesPercentage(minCandidateNodesPercentage int32, p *field.Path) *field.Error {
	if minCandidateNodesPercentage < 0 || minCandidateNodesPercentage > 100 {
		return field.Invalid(p, minCandidateNodesPercentage, "not in valid range [0, 100]")
	}
	return nil
}

// validateMinCandidateNodesAbsolute 校验 minCandidateNodesAbsolute 非负。
func validateMinCandidateNodesAbsolute(minCandidateNodesAbsolute int32, p *field.Path) *field.Error {
	if minCandidateNodesAbsolute < 0 {
		return field.Invalid(p, minCandidateNodesAbsolute, "not in valid range [0, inf)")
	}
	return nil
}

// ValidateInterPodAffinityArgs 校验 InterPodAffinityArgs 是否合法。
func ValidateInterPodAffinityArgs(path *field.Path, args *config.InterPodAffinityArgs) error {
	return validateHardPodAffinityWeight(path.Child("hardPodAffinityWeight"), args.HardPodAffinityWeight)
}

// validateHardPodAffinityWeight 校验硬亲和权重在允许范围 [0, 100] 内。
func validateHardPodAffinityWeight(path *field.Path, w int32) error {
	const (
		minHardPodAffinityWeight = 0
		maxHardPodAffinityWeight = 100
	)

	if w < minHardPodAffinityWeight || w > maxHardPodAffinityWeight {
		msg := fmt.Sprintf("not in valid range [%d, %d]", minHardPodAffinityWeight, maxHardPodAffinityWeight)
		return field.Invalid(path, w, msg)
	}
	return nil
}

// ValidatePodTopologySpreadArgs 校验 PodTopologySpreadArgs 是否合法。
// 规则复制自 pkg/apis/core/validation.validateTopologySpreadConstraints，
// 额外要求默认约束的 .labelSelector 必须为 nil。
func ValidatePodTopologySpreadArgs(path *field.Path, args *config.PodTopologySpreadArgs) error {
	var allErrs field.ErrorList
	if err := validateDefaultingType(path.Child("defaultingType"), args.DefaultingType, args.DefaultConstraints); err != nil {
		allErrs = append(allErrs, err)
	}

	defaultConstraintsPath := path.Child("defaultConstraints")
	for i, c := range args.DefaultConstraints {
		p := defaultConstraintsPath.Index(i)
		if c.MaxSkew <= 0 {
			f := p.Child("maxSkew")
			allErrs = append(allErrs, field.Invalid(f, c.MaxSkew, "not in valid range (0, inf)"))
		}
		allErrs = append(allErrs, validateTopologyKey(p.Child("topologyKey"), c.TopologyKey)...)
		if err := validateWhenUnsatisfiable(p.Child("whenUnsatisfiable"), c.WhenUnsatisfiable); err != nil {
			allErrs = append(allErrs, err)
		}
		// 默认约束的 selector 会按每个 Pod 推导，配置里不允许显式指定。
		if c.LabelSelector != nil {
			f := field.Forbidden(p.Child("labelSelector"), "constraint must not define a selector, as they deduced for each pod")
			allErrs = append(allErrs, f)
		}
		if err := validateConstraintNotRepeat(defaultConstraintsPath, args.DefaultConstraints, i); err != nil {
			allErrs = append(allErrs, err)
		}
	}
	if len(allErrs) == 0 {
		return nil
	}
	return allErrs.ToAggregate()
}

// validateDefaultingType 校验 defaultingType 取值合法，且 SystemDefaulting 不能与显式约束共存。
func validateDefaultingType(p *field.Path, v config.PodTopologySpreadConstraintsDefaulting, constraints []v1.TopologySpreadConstraint) *field.Error {
	if v != config.SystemDefaulting && v != config.ListDefaulting {
		return field.NotSupported(p, v, []string{string(config.SystemDefaulting), string(config.ListDefaulting)})
	}
	if v == config.SystemDefaulting && len(constraints) > 0 {
		return field.Invalid(p, v, "when .defaultConstraints are not empty")
	}
	return nil
}

// validateTopologyKey 校验拓扑键非空且符合标签名规范。
func validateTopologyKey(p *field.Path, v string) field.ErrorList {
	var allErrs field.ErrorList
	if len(v) == 0 {
		allErrs = append(allErrs, field.Required(p, "can not be empty"))
	} else {
		allErrs = append(allErrs, metav1validation.ValidateLabelName(v, p)...)
	}
	return allErrs
}

// validateWhenUnsatisfiable 校验 whenUnsatisfiable 是 DoNotSchedule 或 ScheduleAnyway。
func validateWhenUnsatisfiable(p *field.Path, v v1.UnsatisfiableConstraintAction) *field.Error {
	supportedScheduleActions := sets.New(string(v1.DoNotSchedule), string(v1.ScheduleAnyway))

	if len(v) == 0 {
		return field.Required(p, "can not be empty")
	}
	if !supportedScheduleActions.Has(string(v)) {
		return field.NotSupported(p, v, sets.List(supportedScheduleActions))
	}
	return nil
}

// validateConstraintNotRepeat 校验第 idx 个约束不与前面的约束重复。
// 拓扑键和 whenUnsatisfiable 都相同的约束视为重复。
func validateConstraintNotRepeat(path *field.Path, constraints []v1.TopologySpreadConstraint, idx int) *field.Error {
	c := &constraints[idx]
	for i := range constraints[:idx] {
		other := &constraints[i]
		if c.TopologyKey == other.TopologyKey && c.WhenUnsatisfiable == other.WhenUnsatisfiable {
			return field.Duplicate(path.Index(idx), fmt.Sprintf("{%v, %v}", c.TopologyKey, c.WhenUnsatisfiable))
		}
	}
	return nil
}

// validateFunctionShape 校验自定义评分函数形状：至少一个点、利用率递增、
// 利用率在 [0, 100]、分数在 [0, MaxCustomPriorityScore]。
func validateFunctionShape(shape []config.UtilizationShapePoint, path *field.Path) field.ErrorList {
	const (
		minUtilization = 0
		maxUtilization = 100
		minScore       = 0
		maxScore       = int32(config.MaxCustomPriorityScore)
	)

	var allErrs field.ErrorList

	if len(shape) == 0 {
		allErrs = append(allErrs, field.Required(path, "at least one point must be specified"))
		return allErrs
	}

	// 利用率必须严格递增，否则折线函数无法唯一定义。
	for i := 1; i < len(shape); i++ {
		if shape[i-1].Utilization >= shape[i].Utilization {
			allErrs = append(allErrs, field.Invalid(path.Index(i).Child("utilization"), shape[i].Utilization, "values must be sorted in increasing order"))
			break
		}
	}

	for i, point := range shape {
		if point.Utilization < minUtilization || point.Utilization > maxUtilization {
			msg := fmt.Sprintf("not in valid range [%d, %d]", minUtilization, maxUtilization)
			allErrs = append(allErrs, field.Invalid(path.Index(i).Child("utilization"), point.Utilization, msg))
		}

		if point.Score < minScore || point.Score > maxScore {
			msg := fmt.Sprintf("not in valid range [%d, %d]", minScore, maxScore)
			allErrs = append(allErrs, field.Invalid(path.Index(i).Child("score"), point.Score, msg))
		}
	}

	return allErrs
}

// validateResources 校验资源权重在 (0, 100] 范围内。
func validateResources(resources []config.ResourceSpec, p *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	for i, resource := range resources {
		if resource.Weight <= 0 || resource.Weight > 100 {
			msg := fmt.Sprintf("resource weight of %v not in valid range (0, 100]", resource.Name)
			allErrs = append(allErrs, field.Invalid(p.Index(i).Child("weight"), resource.Weight, msg))
		}
	}
	return allErrs
}

// ValidateNodeResourcesBalancedAllocationArgs 校验 NodeResourcesBalancedAllocationArgs 配置正确：
// 资源名不能重复，且权重必须为 1（该插件暂不支持自定义权重）。
func ValidateNodeResourcesBalancedAllocationArgs(path *field.Path, args *config.NodeResourcesBalancedAllocationArgs) error {
	var allErrs field.ErrorList
	seenResources := sets.New[string]()
	for i, resource := range args.Resources {
		if seenResources.Has(resource.Name) {
			allErrs = append(allErrs, field.Duplicate(path.Child("resources").Index(i).Child("name"), resource.Name))
		} else {
			seenResources.Insert(resource.Name)
		}
		if resource.Weight != 1 {
			allErrs = append(allErrs, field.Invalid(path.Child("resources").Index(i).Child("weight"), resource.Weight, "must be 1"))
		}
	}
	return allErrs.ToAggregate()
}

// ValidateNodeAffinityArgs 校验 AddedAffinity 中的 required/preferred 调度项是否合法。
func ValidateNodeAffinityArgs(path *field.Path, args *config.NodeAffinityArgs) error {
	if args.AddedAffinity == nil {
		return nil
	}
	affinity := args.AddedAffinity
	var errs []error
	if ns := affinity.RequiredDuringSchedulingIgnoredDuringExecution; ns != nil {
		_, err := nodeaffinity.NewNodeSelector(ns, field.WithPath(path.Child("addedAffinity", "requiredDuringSchedulingIgnoredDuringExecution")))
		if err != nil {
			errs = append(errs, err)
		}
	}
	// TODO: Add validation for requiredDuringSchedulingRequiredDuringExecution when it gets added to the API.
	// 待该字段进入 API 后补充校验。
	if terms := affinity.PreferredDuringSchedulingIgnoredDuringExecution; len(terms) != 0 {
		_, err := nodeaffinity.NewPreferredSchedulingTerms(terms, field.WithPath(path.Child("addedAffinity", "preferredDuringSchedulingIgnoredDuringExecution")))
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Flatten(errors.NewAggregate(errs))
}

// VolumeBindingArgsValidationOptions 保存 VolumeBindingArgs 校验时依赖的可选设置，
// 主要用于注入 feature gate 状态，便于测试。
type VolumeBindingArgsValidationOptions struct {
	AllowStorageCapacityScoring bool
}

// ValidateVolumeBindingArgs 使用当前 feature gate 状态校验 VolumeBindingArgs 配置正确。
func ValidateVolumeBindingArgs(path *field.Path, args *config.VolumeBindingArgs) error {
	return ValidateVolumeBindingArgsWithOptions(path, args, VolumeBindingArgsValidationOptions{
		AllowStorageCapacityScoring: utilfeature.DefaultFeatureGate.Enabled(features.StorageCapacityScoring),
	})
}

// ValidateVolumeBindingArgsWithOptions 结合给定的校验选项校验 VolumeBindingArgs。
// BindTimeoutSeconds 不能为负；StorageCapacityScoring 关闭时 shape 字段必须为空。
func ValidateVolumeBindingArgsWithOptions(path *field.Path, args *config.VolumeBindingArgs, opts VolumeBindingArgsValidationOptions) error {
	var allErrs field.ErrorList

	if args.BindTimeoutSeconds < 0 {
		allErrs = append(allErrs, field.Invalid(path.Child("bindTimeoutSeconds"), args.BindTimeoutSeconds, "invalid BindTimeoutSeconds, should not be a negative value"))
	}

	if opts.AllowStorageCapacityScoring {
		allErrs = append(allErrs, validateFunctionShape(args.Shape, path.Child("shape"))...)
	} else if args.Shape != nil {
		// 特性关闭时若配置了 shape 就报错，防止将来开启该特性后
		// 这份休眠配置意外生效。
		allErrs = append(allErrs, field.Invalid(path.Child("shape"), args.Shape, "unexpected field `shape`, remove it or turn on the feature gate StorageCapacityScoring"))
	}
	return allErrs.ToAggregate()
}

// ValidateNodeResourcesFitArgs 校验 NodeResourcesFitArgs：忽略的资源名/组名格式、
// 评分策略类型及其资源权重、RequestedToCapacityRatio 的自定义函数形状。
func ValidateNodeResourcesFitArgs(path *field.Path, args *config.NodeResourcesFitArgs) error {
	var allErrs field.ErrorList
	resPath := path.Child("ignoredResources")
	for i, res := range args.IgnoredResources {
		path := resPath.Index(i)
		if errs := metav1validation.ValidateLabelName(res, path); len(errs) != 0 {
			allErrs = append(allErrs, errs...)
		}
	}

	groupPath := path.Child("ignoredResourceGroups")
	for i, group := range args.IgnoredResourceGroups {
		path := groupPath.Index(i)
		// 资源组名是标签名，不允许包含斜杠（斜杠属于资源名的前缀分隔）。
		if strings.Contains(group, "/") {
			allErrs = append(allErrs, field.Invalid(path, group, "resource group name can't contain '/'"))
		}
		if errs := metav1validation.ValidateLabelName(group, path); len(errs) != 0 {
			allErrs = append(allErrs, errs...)
		}
	}

	strategyPath := path.Child("scoringStrategy")
	if args.ScoringStrategy != nil {
		if !supportedScoringStrategyTypes.Has(string(args.ScoringStrategy.Type)) {
			allErrs = append(allErrs, field.NotSupported(strategyPath.Child("type"), args.ScoringStrategy.Type, sets.List(supportedScoringStrategyTypes)))
		}
		allErrs = append(allErrs, validateResources(args.ScoringStrategy.Resources, strategyPath.Child("resources"))...)
		// 只有 RequestedToCapacityRatio 类型才允许配置自定义函数形状。
		if args.ScoringStrategy.Type == config.RequestedToCapacityRatio {
			if args.ScoringStrategy.RequestedToCapacityRatio == nil {
				allErrs = append(allErrs, field.Required(strategyPath.Child("requestedToCapacityRatio"), "must be specified when type is RequestedToCapacityRatio"))
			} else {
				allErrs = append(allErrs, validateFunctionShape(args.ScoringStrategy.RequestedToCapacityRatio.Shape, strategyPath.Child("requestedToCapacityRatio").Child("shape"))...)
			}
		} else if args.ScoringStrategy.RequestedToCapacityRatio != nil {
			allErrs = append(allErrs, field.Forbidden(strategyPath.Child("requestedToCapacityRatio"), "must be nil when type is not RequestedToCapacityRatio"))
		}
	} else {
		allErrs = append(allErrs, field.Required(strategyPath, "ScoringStrategy field is required"))
	}

	if len(allErrs) == 0 {
		return nil
	}
	return allErrs.ToAggregate()
}

// ValidateDynamicResourcesArgs 校验 DynamicResourcesArgs 是否合法。
// 与 REST API 不同：因对应 feature gate 关闭而失效的字段一旦被设置，这里视为错误。
func ValidateDynamicResourcesArgs(path *field.Path, args *config.DynamicResourcesArgs, fts feature.Features) error {
	var allErrs field.ErrorList
	// 特性开启时才允许配置超时时间，并校验其非负。
	if fts.EnableDRASchedulerFilterTimeout {
		if args.FilterTimeout != nil && args.FilterTimeout.Duration < 0 {
			allErrs = append(allErrs, field.Invalid(path.Child("filterTimeout"), args.FilterTimeout, "must be zero or positive"))
		}
	} else {
		if args.FilterTimeout != nil {
			allErrs = append(allErrs, field.Forbidden(path.Child("filterTimeout"), "DRASchedulingFilterTimeout feature gate is disabled"))
		}
	}

	// 绑定超时依赖两个特性同时开启，且最短为 1 秒。
	if fts.EnableDRADeviceBindingConditions && fts.EnableDRAResourceClaimDeviceStatus {
		if args.BindingTimeout != nil && args.BindingTimeout.Duration < 1*time.Second {
			allErrs = append(allErrs, field.Invalid(
				path.Child("bindingTimeout"),
				args.BindingTimeout,
				"must be at least 1 second",
			))
		}
	} else {
		if args.BindingTimeout != nil {
			allErrs = append(allErrs, field.Forbidden(
				path.Child("bindingTimeout"),
				"DRADeviceBindingConditions or DRAResourceClaimDeviceStatus feature gate is disabled",
			))
		}
	}
	return allErrs.ToAggregate()
}
