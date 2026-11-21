/*
Copyright 2015 The Kubernetes Authors.

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

package pod

import (
	"fmt"
	"iter"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metavalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
)

// ContainerType 表示容器类型，用位掩码表示，便于组合成可同时匹配的容器集合。
type ContainerType int

const (
	// Containers 表示普通容器。
	Containers ContainerType = 1 << iota
	// InitContainers 表示初始化容器。
	InitContainers
	// EphemeralContainers 表示临时容器。
	EphemeralContainers
)

// AllContainers 指定访问所有类型的容器。
const AllContainers ContainerType = (InitContainers | Containers | EphemeralContainers)

// AllFeatureEnabledContainers 返回包含所有不受特性开关保护的容器类型的 ContainerType 掩码。
// 当前所有容器类型均已默认启用，因此直接返回 AllContainers；保留函数是为了让调用方
// 不必关心哪些类型受开关保护。
func AllFeatureEnabledContainers() ContainerType {
	return AllContainers
}

// ContainerVisitor 在每个容器 spec 上被调用，返回值表示是否继续访问后续容器。
type ContainerVisitor func(container *api.Container, containerType ContainerType) (shouldContinue bool)

// VisitContainers 用 mask 指定的容器类型过滤，对给定 pod spec 中的每个容器调用 visitor。
// 若 visitor 返回 false，则提前终止遍历。全部访问完成返回 true，被提前终止返回 false。
func VisitContainers(podSpec *api.PodSpec, mask ContainerType, visitor ContainerVisitor) bool {
	for c, t := range ContainerIter(podSpec, mask) {
		if !visitor(c, t) {
			return false
		}
	}
	return true
}

// ContainerIter 返回一个迭代器，遍历给定 pod spec 中类型被 mask 选中的所有容器。
// 遍历顺序固定为 InitContainers、普通 Containers，最后是 EphemeralContainers，
// 保证依赖该顺序的逻辑（如投递顺序）行为稳定。
func ContainerIter(podSpec *api.PodSpec, mask ContainerType) iter.Seq2[*api.Container, ContainerType] {
	return func(yield func(*api.Container, ContainerType) bool) {
		if mask&InitContainers != 0 {
			for i := range podSpec.InitContainers {
				if !yield(&podSpec.InitContainers[i], InitContainers) {
					return
				}
			}
		}
		if mask&Containers != 0 {
			for i := range podSpec.Containers {
				if !yield(&podSpec.Containers[i], Containers) {
					return
				}
			}
		}
		if mask&EphemeralContainers != 0 {
			for i := range podSpec.EphemeralContainers {
				if !yield((*api.Container)(&podSpec.EphemeralContainers[i].EphemeralContainerCommon), EphemeralContainers) {
					return
				}
			}
		}
	}
}

// Visitor 在每个对象名称上被调用，返回值表示是否继续访问后续名称。
type Visitor func(name string) (shouldContinue bool)

// skipEmptyNames 包装 visitor，过滤掉空名称。
// 空引用在 spec 中允许存在（如未指定的 secret 名），不应触发依赖方的误判。
func skipEmptyNames(visitor Visitor) Visitor {
	return func(name string) bool {
		if len(name) == 0 {
			// 跳过空名称，继续访问。
			return true
		}
		// 交给真正的 visitor 处理。
		return visitor(name)
	}
}

// VisitPodSecretNames 对 pod spec 引用的每个 secret 名称调用 visitor。
// 若 visitor 返回 false，则提前终止遍历。不递归访问传递引用（如 pod -> pvc -> pv -> secret）。
// 全部访问完成返回 true，被提前终止返回 false。
func VisitPodSecretNames(pod *api.Pod, visitor Visitor, containerType ContainerType) bool {
	// 先过滤空名称，避免镜像拉取密钥等字段为空时产生无意义的回调。
	visitor = skipEmptyNames(visitor)
	for _, reference := range pod.Spec.ImagePullSecrets {
		if !visitor(reference.Name) {
			return false
		}
	}
	VisitContainers(&pod.Spec, containerType, func(c *api.Container, containerType ContainerType) bool {
		return visitContainerSecretNames(c, visitor)
	})
	var source *api.VolumeSource
	for i := range pod.Spec.Volumes {
		source = &pod.Spec.Volumes[i].VolumeSource
		switch {
		case source.AzureFile != nil:
			if len(source.AzureFile.SecretName) > 0 && !visitor(source.AzureFile.SecretName) {
				return false
			}
		case source.CephFS != nil:
			if source.CephFS.SecretRef != nil && !visitor(source.CephFS.SecretRef.Name) {
				return false
			}
		case source.Cinder != nil:
			if source.Cinder.SecretRef != nil && !visitor(source.Cinder.SecretRef.Name) {
				return false
			}
		case source.FlexVolume != nil:
			if source.FlexVolume.SecretRef != nil && !visitor(source.FlexVolume.SecretRef.Name) {
				return false
			}
		case source.Projected != nil:
			for j := range source.Projected.Sources {
				if source.Projected.Sources[j].Secret != nil {
					if !visitor(source.Projected.Sources[j].Secret.Name) {
						return false
					}
				}
			}
		case source.RBD != nil:
			if source.RBD.SecretRef != nil && !visitor(source.RBD.SecretRef.Name) {
				return false
			}
		case source.Secret != nil:
			if !visitor(source.Secret.SecretName) {
				return false
			}
		case source.ScaleIO != nil:
			if source.ScaleIO.SecretRef != nil && !visitor(source.ScaleIO.SecretRef.Name) {
				return false
			}
		case source.ISCSI != nil:
			if source.ISCSI.SecretRef != nil && !visitor(source.ISCSI.SecretRef.Name) {
				return false
			}
		case source.StorageOS != nil:
			if source.StorageOS.SecretRef != nil && !visitor(source.StorageOS.SecretRef.Name) {
				return false
			}
		case source.CSI != nil:
			if source.CSI.NodePublishSecretRef != nil && !visitor(source.CSI.NodePublishSecretRef.Name) {
				return false
			}
		}
	}
	return true
}

// visitContainerSecretNames 访问容器通过 envFrom 和 env.valueFrom 引用的 secret 名称。
func visitContainerSecretNames(container *api.Container, visitor Visitor) bool {
	for _, env := range container.EnvFrom {
		if env.SecretRef != nil {
			if !visitor(env.SecretRef.Name) {
				return false
			}
		}
	}
	for _, envVar := range container.Env {
		if envVar.ValueFrom != nil && envVar.ValueFrom.SecretKeyRef != nil {
			if !visitor(envVar.ValueFrom.SecretKeyRef.Name) {
				return false
			}
		}
	}
	return true
}

// VisitPodConfigmapNames 对 pod spec 引用的每个 configmap 名称调用 visitor。
// 若 visitor 返回 false，则提前终止遍历。不递归访问传递引用（如 pod -> pvc -> pv -> secret）。
// 全部访问完成返回 true，被提前终止返回 false。
func VisitPodConfigmapNames(pod *api.Pod, visitor Visitor, containerType ContainerType) bool {
	visitor = skipEmptyNames(visitor)
	VisitContainers(&pod.Spec, containerType, func(c *api.Container, containerType ContainerType) bool {
		return visitContainerConfigmapNames(c, visitor)
	})
	var source *api.VolumeSource
	for i := range pod.Spec.Volumes {
		source = &pod.Spec.Volumes[i].VolumeSource
		switch {
		case source.Projected != nil:
			for j := range source.Projected.Sources {
				if source.Projected.Sources[j].ConfigMap != nil {
					if !visitor(source.Projected.Sources[j].ConfigMap.Name) {
						return false
					}
				}
			}
		case source.ConfigMap != nil:
			if !visitor(source.ConfigMap.Name) {
				return false
			}
		}
	}
	return true
}

// visitContainerConfigmapNames 访问容器通过 envFrom 和 env.valueFrom 引用的 configmap 名称。
func visitContainerConfigmapNames(container *api.Container, visitor Visitor) bool {
	for _, env := range container.EnvFrom {
		if env.ConfigMapRef != nil {
			if !visitor(env.ConfigMapRef.Name) {
				return false
			}
		}
	}
	for _, envVar := range container.Env {
		if envVar.ValueFrom != nil && envVar.ValueFrom.ConfigMapKeyRef != nil {
			if !visitor(envVar.ValueFrom.ConfigMapKeyRef.Name) {
				return false
			}
		}
	}
	return true
}

// IsPodReady 判断 pod 是否处于 Ready 状态。
func IsPodReady(pod *api.Pod) bool {
	return IsPodReadyConditionTrue(pod.Status)
}

// IsPodReadyConditionTrue 判断给定状态中的 PodReady 条件是否为 True。
func IsPodReadyConditionTrue(status api.PodStatus) bool {
	condition := GetPodReadyCondition(status)
	return condition != nil && condition.Status == api.ConditionTrue
}

// GetPodReadyCondition 从给定状态中提取 PodReady 条件，不存在时返回 nil。
func GetPodReadyCondition(status api.PodStatus) *api.PodCondition {
	_, condition := GetPodCondition(&status, api.PodReady)
	return condition
}

// GetPodCondition 从给定状态中提取指定类型的条件，返回其下标和条件指针。
// 条件不存在时返回 -1 和 nil。
func GetPodCondition(status *api.PodStatus, conditionType api.PodConditionType) (int, *api.PodCondition) {
	if status == nil {
		return -1, nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return i, &status.Conditions[i]
		}
	}
	return -1, nil
}

// UpdatePodCondition 更新已有的 pod 条件或新增一个条件。状态发生变化时把 LastTransitionTime 设为当前时间。
// 条件发生变更或新增时返回 true。
func UpdatePodCondition(status *api.PodStatus, condition *api.PodCondition) bool {
	condition.LastTransitionTime = metav1.Now()
	// 先查找同类型的已有条件，决定是新增还是更新。
	conditionIndex, oldCondition := GetPodCondition(status, condition.Type)

	if oldCondition == nil {
		// 新条件直接追加。
		status.Conditions = append(status.Conditions, *condition)
		return true
	}
	// 状态未变时沿用旧的转换时间，避免仅因为重新上报就刷新时间、扰乱事件语义。
	if condition.Status == oldCondition.Status {
		condition.LastTransitionTime = oldCondition.LastTransitionTime
	}

	isEqual := condition.Status == oldCondition.Status &&
		condition.Reason == oldCondition.Reason &&
		condition.Message == oldCondition.Message &&
		condition.LastProbeTime.Equal(&oldCondition.LastProbeTime) &&
		condition.LastTransitionTime.Equal(&oldCondition.LastTransitionTime)

	status.Conditions[conditionIndex] = *condition
	// 只要任一字段变化就认为条件发生变更。
	return !isEqual
}

// checkContainerUseIndivisibleHugePagesValues 检查容器的 limit 或 request 中是否存在非大页整数倍的大页资源值。
func checkContainerUseIndivisibleHugePagesValues(container api.Container) bool {
	for resourceName, quantity := range container.Resources.Limits {
		if helper.IsHugePageResourceName(resourceName) {
			if !helper.IsHugePageResourceValueDivisible(resourceName, quantity) {
				return true
			}
		}
	}

	for resourceName, quantity := range container.Resources.Requests {
		if helper.IsHugePageResourceName(resourceName) {
			if !helper.IsHugePageResourceValueDivisible(resourceName, quantity) {
				return true
			}
		}
	}

	return false
}

// usesIndivisibleHugePagesValues 判断是否有容器使用了非大页单位整数倍的资源值。
func usesIndivisibleHugePagesValues(podSpec *api.PodSpec) bool {
	foundIndivisibleHugePagesValue := false
	VisitContainers(podSpec, AllContainers, func(c *api.Container, containerType ContainerType) bool {
		if checkContainerUseIndivisibleHugePagesValues(*c) {
			foundIndivisibleHugePagesValue = true
		}
		// 发现非法值后即可停止，没必要继续遍历。
		return !foundIndivisibleHugePagesValue
	})

	if foundIndivisibleHugePagesValue {
		return true
	}

	// 容器检查通过后还要看 pod 级别的 Overhead，同样可能携带大页资源。
	for resourceName, quantity := range podSpec.Overhead {
		if helper.IsHugePageResourceName(resourceName) {
			if !helper.IsHugePageResourceValueDivisible(resourceName, quantity) {
				return true
			}
		}
	}

	return false
}

// hasInvalidTopologySpreadConstraintLabelSelector 判断 spec.TopologySpreadConstraints 中是否存在 labelSelector 非法的条目。
func hasInvalidTopologySpreadConstraintLabelSelector(spec *api.PodSpec) bool {
	for _, constraint := range spec.TopologySpreadConstraints {
		errs := metavalidation.ValidateLabelSelector(constraint.LabelSelector, metavalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}, nil)
		if len(errs) != 0 {
			return true
		}
	}
	return false
}

// hasInvalidTopologySpreadConstrainMatchLabelKeys 判断 spec.TopologySpreadConstraints 中是否存在 MatchLabelKeys 非法的条目。
func hasInvalidTopologySpreadConstrainMatchLabelKeys(spec *api.PodSpec) bool {
	for _, constraint := range spec.TopologySpreadConstraints {
		errs := apivalidation.ValidateMatchLabelKeysAndMismatchLabelKeys(nil, constraint.MatchLabelKeys, nil, constraint.LabelSelector)
		if len(errs) != 0 {
			return true
		}
	}
	return false
}

// hasLegacyInvalidTopologySpreadConstrainMatchLabelKeys 按旧版校验规则判断
// spec.TopologySpreadConstraints 中是否存在 MatchLabelKeys 非法的条目。
func hasLegacyInvalidTopologySpreadConstrainMatchLabelKeys(spec *api.PodSpec) bool {
	for _, constraint := range spec.TopologySpreadConstraints {
		errs := apivalidation.ValidateMatchLabelKeysInTopologySpread(nil, constraint.MatchLabelKeys, constraint.LabelSelector)
		if len(errs) != 0 {
			return true
		}
	}
	return false
}

// hasNonLocalProjectedTokenPath 判断 spec.Volumes 中是否存在路径非本地的 projected token。
func hasNonLocalProjectedTokenPath(spec *api.PodSpec) bool {
	for _, volume := range spec.Volumes {
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.ServiceAccountToken == nil {
					continue
				}
				errs := apivalidation.ValidateLocalNonReservedPath(source.ServiceAccountToken.Path, nil)
				if len(errs) != 0 {
					return true
				}
			}
		}
	}
	return false
}

// GetValidationOptionsFromPodSpecAndMeta 根据新旧 pod spec 与元数据生成校验选项。
// 核心原则是：特性开关关闭时，对旧对象已经使用过的字段必须放宽校验，避免存量对象无法更新。
func GetValidationOptionsFromPodSpecAndMeta(podSpec, oldPodSpec *api.PodSpec, podMeta, oldPodMeta *metav1.ObjectMeta) apivalidation.PodValidationOptions {
	// 先按特性开关给出默认的校验选项。
	opts := apivalidation.PodValidationOptions{
		// 关闭特性开关但允许 PodDeletionCost 字段的非法值，是为了兼容存量对象。
		AllowInvalidPodDeletionCost: !utilfeature.DefaultFeatureGate.Enabled(features.PodDeletionCost),
		// 默认不允许使用非大页单位整数倍的大页值。
		AllowIndivisibleHugePagesValues:                     false,
		AllowInvalidLabelValueInSelector:                    false,
		AllowInvalidTopologySpreadConstraintLabelSelector:   false,
		AllowNamespacedSysctlsForHostNetAndHostIPC:          false,
		AllowNonLocalProjectedTokenPath:                     false,
		PodLevelResourcesEnabled:                            utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
		AllowInvalidLabelValueInRequiredNodeAffinity:        false,
		AllowSidecarResizePolicy:                            utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling),
		AllowMatchLabelKeysInPodTopologySpread:              utilfeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodTopologySpread),
		AllowMatchLabelKeysInPodTopologySpreadSelectorMerge: utilfeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodTopologySpreadSelectorMerge),
		InPlacePodLevelResourcesVerticalScalingEnabled:      utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling),
		InPlacePodVerticalScalingMemoryBackedVolumesEnabled: utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes),
		OldPodViolatesMatchLabelKeysValidation:              false,
		OldPodViolatesLegacyMatchLabelKeysValidation:        false,
		AllowContainerRestartPolicyRules:                    utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules),
		AllowUserNamespacesWithVolumeDevices:                false,
		// 该开关同时也允许 sidecar 容器配置重启规则。
		AllowRestartAllContainers:                               utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits),
		AllowImageVolumeWithDigest:                              utilfeature.DefaultFeatureGate.Enabled(features.ImageVolumeWithDigest),
		AllowExistingRestartContainerForNonSidecarInitContainer: hasRestartContainerForNonSidecarInitContainer(oldPodSpec),
		AllowSysAdminWhenPrivilegeEscalationFalse:               false,
	}

	// 特性开关开启或旧 spec 已经使用了宽松命名规则时，必须继续允许宽松校验。
	opts.AllowRelaxedEnvironmentVariableValidation = useRelaxedEnvironmentVariableValidation(podSpec, oldPodSpec)
	opts.AllowEnvFilesValidation = useAllowEnvFilesValidation(oldPodSpec)
	opts.AllowUserNamespacesHostNetworkSupport = useAllowUserNamespacesHostNetworkSupport(oldPodSpec)

	opts.AllowOnlyRecursiveSELinuxChangePolicy = useOnlyRecursiveSELinuxChangePolicy(oldPodSpec)
	opts.AllowTaintTolerationComparisonOperators = allowTaintTolerationComparisonOperators(oldPodSpec)

	if oldPodSpec != nil {
		// 旧 spec 用了非法大页值时必须继续放行，否则更新会被新规则拒绝。
		opts.AllowIndivisibleHugePagesValues = usesIndivisibleHugePagesValues(oldPodSpec)

		opts.AllowInvalidLabelValueInSelector = hasInvalidLabelValueInAffinitySelector(oldPodSpec)
		opts.AllowInvalidLabelValueInRequiredNodeAffinity = hasInvalidLabelValueInRequiredNodeAffinity(oldPodSpec)
		// 旧 spec 中 topologySpreadConstraint 的 labelSelector 非法时同样需要放行。
		opts.AllowInvalidTopologySpreadConstraintLabelSelector = hasInvalidTopologySpreadConstraintLabelSelector(oldPodSpec)
		if opts.AllowMatchLabelKeysInPodTopologySpread {
			if opts.AllowMatchLabelKeysInPodTopologySpreadSelectorMerge {
				// 新旧校验规则不同：selector merge 开关下按新规则判断旧对象是否违规。
				opts.OldPodViolatesMatchLabelKeysValidation = hasInvalidTopologySpreadConstrainMatchLabelKeys(oldPodSpec)
			} else {
				// 否则按旧版规则判断，保证历史对象继续可更新。
				opts.OldPodViolatesLegacyMatchLabelKeysValidation = hasLegacyInvalidTopologySpreadConstrainMatchLabelKeys(oldPodSpec)
			}
		}
		// 旧 spec 的 projected token 路径非法时放行。
		opts.AllowNonLocalProjectedTokenPath = hasNonLocalProjectedTokenPath(oldPodSpec)

		// 旧 spec 在 hostNet/hostIPC 下使用了非法 sysctl 时，更新时也需要放行。
		if oldPodSpec.SecurityContext != nil && len(oldPodSpec.SecurityContext.Sysctls) != 0 {
			for _, s := range oldPodSpec.SecurityContext.Sysctls {
				err := apivalidation.ValidateHostSysctl(s.Name, oldPodSpec, nil)
				if err != nil {
					opts.AllowNamespacedSysctlsForHostNetAndHostIPC = true
					break
				}
			}
		}

		// 旧对象的 sidecar（可重启 init 容器）配置了 resize policy 时必须继续允许。
		opts.AllowSidecarResizePolicy = opts.AllowSidecarResizePolicy || hasRestartableInitContainerResizePolicy(oldPodSpec)

		opts.AllowContainerRestartPolicyRules = opts.AllowContainerRestartPolicyRules || containerRestartRulesInUse(oldPodSpec)
		opts.AllowRestartAllContainers = opts.AllowRestartAllContainers || restartAllContainersActionInUse(oldPodSpec)

		// 旧对象同时使用了 userns 和 volume devices（该组合不生效），仍允许对其进行修改。
		opts.AllowUserNamespacesWithVolumeDevices = hasUserNamespacesWithVolumeDevices(oldPodSpec)

		// 旧 spec 中 image volume 的引用为空时继续放行，避免存量对象卡住。
		opts.AllowEmptyImageVolumeReference = hasEmptyImageVolumeReference(oldPodSpec)

		opts.AllowSysAdminWhenPrivilegeEscalationFalse = useAllowSysAdminWhenPrivilegeEscalationFalse(oldPodSpec)
	}
	if oldPodMeta != nil && !opts.AllowInvalidPodDeletionCost {
		// 更新操作只对原本合法的存量对象收紧校验：旧注解已有错误则继续放行。
		_, err := helper.GetDeletionCostFromPodAnnotations(oldPodMeta.Annotations)
		opts.AllowInvalidPodDeletionCost = err != nil
	}

	return opts
}

// useRelaxedEnvironmentVariableValidation 判断是否应允许宽松的环境变量名规则。
// 开关开启时直接允许；否则仅在新对象使用了旧对象已有的宽松名称时才允许，避免放宽新对象的校验。
func useRelaxedEnvironmentVariableValidation(podSpec, oldPodSpec *api.PodSpec) bool {
	if utilfeature.DefaultFeatureGate.Enabled(features.RelaxedEnvironmentVariableValidation) {
		return true
	}

	var oldPodEnvVarNames, podEnvVarNames sets.Set[string]
	if oldPodSpec != nil {
		oldPodEnvVarNames = gatherPodEnvVarNames(oldPodSpec)
	}

	if podSpec != nil {
		podEnvVarNames = gatherPodEnvVarNames(podSpec)
	}

	for env := range podEnvVarNames {
		if relaxedEnvVarUsed(env, oldPodEnvVarNames) {
			return true
		}
	}

	return false
}

// useAllowUserNamespacesHostNetworkSupport 判断是否允许 user namespace 与 hostNetwork 同时使用。
func useAllowUserNamespacesHostNetworkSupport(oldPodSpec *api.PodSpec) bool {
	// 开关开启时直接允许。
	if utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesHostNetworkSupport) {
		return true
	}

	if oldPodSpec == nil || oldPodSpec.HostUsers == nil {
		return false
	}

	// 集群中已存在同时使用 user namespace 和 hostNetwork 的 pod 时，
	// 即使开关后续被关闭，也允许它沿用原有的校验逻辑，避免存量对象无法更新。
	userNamespaces := !*oldPodSpec.HostUsers
	return oldPodSpec.HostNetwork && userNamespaces
}

// useAllowEnvFilesValidation 判断是否允许 env.valueFrom.fileKeyRef。
// 旧对象已使用该字段时必须放行，否则开关回退后存量对象无法更新。
func useAllowEnvFilesValidation(oldPodSpec *api.PodSpec) bool {
	// 开关开启时直接允许。
	if utilfeature.DefaultFeatureGate.Enabled(features.EnvFiles) {
		return true
	}

	if oldPodSpec == nil {
		return false
	}

	for c := range ContainerIter(oldPodSpec, AllContainers) {
		if hasEnvFileKeyRef(c.Env) {
			return true
		}
	}

	return false
}

// hasEnvFileKeyRef 检查环境变量列表中是否存在 fileKeyRef 引用。
func hasEnvFileKeyRef(envs []api.EnvVar) bool {
	for _, env := range envs {
		if env.ValueFrom != nil && env.ValueFrom.FileKeyRef != nil {
			return true
		}
	}
	return false
}

// useAllowSysAdminWhenPrivilegeEscalationFalse 判断旧对象是否已经在禁用特权提升的同时申请了 CAP_SYS_ADMIN。
func useAllowSysAdminWhenPrivilegeEscalationFalse(oldPodSpec *api.PodSpec) bool {
	if oldPodSpec == nil {
		return false
	}

	for c := range ContainerIter(oldPodSpec, AllContainers) {
		if hasSysAdminAndPrivilegeEscalationFalse(c.SecurityContext) {
			return true
		}
	}

	return false
}

// hasSysAdminAndPrivilegeEscalationFalse 检查安全上下文是否同时满足：未显式开启特权提升、添加了 CAP_SYS_ADMIN。
func hasSysAdminAndPrivilegeEscalationFalse(sc *api.SecurityContext) bool {
	if sc == nil {
		return false
	}
	if sc.AllowPrivilegeEscalation != nil && *sc.AllowPrivilegeEscalation {
		return false
	}
	if sc.Capabilities == nil {
		return false
	}
	for _, c := range sc.Capabilities.Add {
		if string(c) == "CAP_SYS_ADMIN" {
			return true
		}
	}
	return false
}

// gatherPodEnvVarNames 收集 pod 中所有容器的环境变量名和 envFrom 前缀。
// envFrom 前缀也需要收集，因为宽松命名规则同样作用于注入后的变量名。
func gatherPodEnvVarNames(podSpec *api.PodSpec) sets.Set[string] {
	podEnvVarNames := sets.Set[string]{}

	for _, c := range podSpec.Containers {
		for _, env := range c.Env {
			podEnvVarNames.Insert(env.Name)
		}

		for _, env := range c.EnvFrom {
			podEnvVarNames.Insert(env.Prefix)
		}
	}

	for _, c := range podSpec.InitContainers {
		for _, env := range c.Env {
			podEnvVarNames.Insert(env.Name)
		}

		for _, env := range c.EnvFrom {
			podEnvVarNames.Insert(env.Prefix)
		}
	}

	for _, c := range podSpec.EphemeralContainers {
		for _, env := range c.Env {
			podEnvVarNames.Insert(env.Name)
		}

		for _, env := range c.EnvFrom {
			podEnvVarNames.Insert(env.Prefix)
		}
	}

	return podEnvVarNames
}

// relaxedEnvVarUsed 判断环境变量名是否属于"只在宽松规则下合法、且旧对象已在用"的情形。
func relaxedEnvVarUsed(name string, oldPodEnvVarNames sets.Set[string]) bool {
	// 集合为空表示这是创建请求或旧对象没有环境变量，此时交给特性开关决定是否宽松。
	if oldPodEnvVarNames.Len() == 0 {
		return false
	}

	if len(validation.IsEnvVarName(name)) == 0 || len(validation.IsRelaxedEnvVarName(name)) != 0 {
		// 严格规则合法，或宽松规则也不合法：两种情况都用严格规则校验即可。
		return false
	}

	// 走到这里说明该名称只在宽松规则下合法。
	if oldPodEnvVarNames.Has(name) {
		// 旧对象已经使用过该名称，必须继续允许。
		return true
	}

	return false
}

// GetValidationOptionsFromPodTemplate 返回指定 pod 模板对应的校验选项。
func GetValidationOptionsFromPodTemplate(podTemplate, oldPodTemplate *api.PodTemplateSpec) apivalidation.PodValidationOptions {
	var newPodSpec, oldPodSpec *api.PodSpec
	var newPodMeta, oldPodMeta *metav1.ObjectMeta
	// 必须小心空指针：replication controller 尤其容易传入 nil 模板。
	if podTemplate != nil {
		newPodSpec = &podTemplate.Spec
		newPodMeta = &podTemplate.ObjectMeta
	}
	if oldPodTemplate != nil {
		oldPodSpec = &oldPodTemplate.Spec
		oldPodMeta = &oldPodTemplate.ObjectMeta
	}
	return GetValidationOptionsFromPodSpecAndMeta(newPodSpec, oldPodSpec, newPodMeta, oldPodMeta)
}

// DropDisabledTemplateFields 从 pod 模板的元数据和 spec 中清除被禁用的字段。
// 所有包含 PodTemplateSpec 的资源都应在 PrepareForCreate/PrepareForUpdate 中调用它。
func DropDisabledTemplateFields(podTemplate, oldPodTemplate *api.PodTemplateSpec) {
	var (
		podSpec           *api.PodSpec
		podAnnotations    map[string]string
		oldPodSpec        *api.PodSpec
		oldPodAnnotations map[string]string
	)
	if podTemplate != nil {
		podSpec = &podTemplate.Spec
		podAnnotations = podTemplate.Annotations
	}
	if oldPodTemplate != nil {
		oldPodSpec = &oldPodTemplate.Spec
		oldPodAnnotations = oldPodTemplate.Annotations
	}
	dropDisabledFields(podSpec, podAnnotations, oldPodSpec, oldPodAnnotations)
}

// DropDisabledPodFields 从 pod 的元数据和 spec 中清除被禁用的字段。
// 所有包含 Pod 的资源都应在 PrepareForCreate/PrepareForUpdate 中调用它。
func DropDisabledPodFields(pod, oldPod *api.Pod) {
	var (
		podSpec           *api.PodSpec
		podStatus         *api.PodStatus
		podAnnotations    map[string]string
		oldPodSpec        *api.PodSpec
		oldPodStatus      *api.PodStatus
		oldPodAnnotations map[string]string
	)
	if pod != nil {
		podSpec = &pod.Spec
		podStatus = &pod.Status
		podAnnotations = pod.Annotations
	}
	if oldPod != nil {
		oldPodSpec = &oldPod.Spec
		oldPodStatus = &oldPod.Status
		oldPodAnnotations = oldPod.Annotations
	}
	dropDisabledFields(podSpec, podAnnotations, oldPodSpec, oldPodAnnotations)
	dropDisabledPodStatusFields(podStatus, oldPodStatus, podSpec, oldPodSpec)
}

// dropDisabledFields 从 pod 元数据和 spec 中清除被禁用的字段。
// 统一的判断模式：特性开关关闭且旧对象未使用该字段时才清除，避免更新时丢失存量数据。
func dropDisabledFields(
	podSpec *api.PodSpec, podAnnotations map[string]string,
	oldPodSpec *api.PodSpec, oldPodAnnotations map[string]string,
) {
	// 新 spec 必须非 nil，便于后续统一字段访问。
	if podSpec == nil {
		podSpec = &api.PodSpec{}
	}

	// 特性开关关闭且旧对象未使用时清除 hostUsers。
	if !utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) && !hostUsersInUse(oldPodSpec) {
		podSpec.HostUsers = nil
	}

	// 特性开关关闭且旧对象未使用时清除 SupplementalGroupsPolicy。
	if !utilfeature.DefaultFeatureGate.Enabled(features.SupplementalGroupsPolicy) && !supplementalGroupsPolicyInUse(oldPodSpec) {
		// 仅在 SecurityContext 非 nil 时才需要置空；为 nil 时该字段本就是 nil，无需操作。
		if podSpec.SecurityContext != nil {
			podSpec.SecurityContext.SupplementalGroupsPolicy = nil
		}
	}

	dropDisabledPodLevelResources(podSpec, oldPodSpec)
	dropDisabledProcMountField(podSpec, oldPodSpec)

	dropDisabledNodeInclusionPolicyFields(podSpec, oldPodSpec)
	dropDisabledMatchLabelKeysFieldInTopologySpread(podSpec, oldPodSpec)
	dropDisabledMatchLabelKeysFieldInPodAffinity(podSpec, oldPodSpec)
	dropDisabledDynamicResourceAllocationFields(podSpec, oldPodSpec)
	dropDisabledClusterTrustBundleProjection(podSpec, oldPodSpec)
	dropDisabledPodCertificateProjection(podSpec, oldPodSpec)
	dropDisabledAtomicWriteVolumeUserFields(podSpec, oldPodSpec)
	dropDisabledSchedulingGroup(podSpec, oldPodSpec)
	dropDisabledGRPCContainerProbeTLS(podSpec, oldPodSpec)
	dropDisabledEvictionResponders(podSpec, oldPodSpec)

	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) && !inPlacePodVerticalScalingInUse(oldPodSpec) {
		// 只清除 ResizePolicy；Resources 字段对部分控制器可变，不能在这里清除，
		// 由 ValidatePodUpdate 负责校验其合法性。
		for i := range podSpec.Containers {
			podSpec.Containers[i].ResizePolicy = nil
		}
		for i := range podSpec.InitContainers {
			podSpec.InitContainers[i].ResizePolicy = nil
		}
		for i := range podSpec.EphemeralContainers {
			podSpec.EphemeralContainers[i].ResizePolicy = nil
		}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) && !containerRestartRulesInUse(oldPodSpec) {
		dropContainerRestartRules(podSpec)
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.RecursiveReadOnlyMounts) && !rroInUse(oldPodSpec) {
		for i := range podSpec.Containers {
			for j := range podSpec.Containers[i].VolumeMounts {
				podSpec.Containers[i].VolumeMounts[j].RecursiveReadOnly = nil
			}
		}
		for i := range podSpec.InitContainers {
			for j := range podSpec.InitContainers[i].VolumeMounts {
				podSpec.InitContainers[i].VolumeMounts[j].RecursiveReadOnly = nil
			}
		}
		for i := range podSpec.EphemeralContainers {
			for j := range podSpec.EphemeralContainers[i].VolumeMounts {
				podSpec.EphemeralContainers[i].VolumeMounts[j].RecursiveReadOnly = nil
			}
		}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.EmptyDirVolumeMode) && !emptyDirVolumeModeInUse(oldPodSpec) {
		for i := range podSpec.Volumes {
			if podSpec.Volumes[i].EmptyDir != nil {
				podSpec.Volumes[i].EmptyDir.Mode = nil
			}
		}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.VolumeBindMountOptions) && !bindMountOptionsInUse(oldPodSpec) {
		for i := range podSpec.Containers {
			for j := range podSpec.Containers[i].VolumeMounts {
				podSpec.Containers[i].VolumeMounts[j].BindMountOptions = nil
			}
		}
		for i := range podSpec.InitContainers {
			for j := range podSpec.InitContainers[i].VolumeMounts {
				podSpec.InitContainers[i].VolumeMounts[j].BindMountOptions = nil
			}
		}
		for i := range podSpec.EphemeralContainers {
			for j := range podSpec.EphemeralContainers[i].VolumeMounts {
				podSpec.EphemeralContainers[i].VolumeMounts[j].BindMountOptions = nil
			}
		}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.HostnameOverride) && !setHostnameOverrideInUse(oldPodSpec) {
		// 仅在开关关闭且旧对象未使用时清空 HostnameOverride。
		podSpec.HostnameOverride = nil
	}

	dropFileKeyRefInUse(podSpec, oldPodSpec)
	dropImageVolumes(podSpec, oldPodSpec)
	dropSELinuxChangePolicy(podSpec, oldPodSpec)
	dropContainerStopSignals(podSpec, oldPodSpec)
	dropHTTPProbeProtocol(podSpec, oldPodSpec)
	DropInitContainerAnnotations(podAnnotations)
}

// setHostnameOverrideInUse 判断 pod spec 是否设置了 HostnameOverride 字段。
func setHostnameOverrideInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil || podSpec.HostnameOverride == nil {
		return false
	}
	return true
}

// dropFileKeyRefInUse 在 EnvFiles 特性关闭且旧对象未使用时清除环境变量中的 fileKeyRef。
func dropFileKeyRefInUse(podSpec, oldPodSpec *api.PodSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.EnvFiles) || podFileKeyRefInUse(oldPodSpec) {
		return
	}

	VisitContainers(podSpec, AllContainers, func(c *api.Container, _ ContainerType) bool {
		for i := range c.Env {
			if c.Env[i].ValueFrom != nil && c.Env[i].ValueFrom.FileKeyRef != nil {
				c.Env[i].ValueFrom.FileKeyRef = nil
			}
		}
		return true
	})
}

// podFileKeyRefInUse 判断 pod spec 是否有容器使用了 fileKeyRef。
func podFileKeyRefInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}

	var inUse bool
	VisitContainers(podSpec, AllContainers, func(c *api.Container, _ ContainerType) bool {
		for _, env := range c.Env {
			if env.ValueFrom != nil && env.ValueFrom.FileKeyRef != nil {
				inUse = true
				return false
			}
		}
		return true
	})
	return inUse
}

// dropContainerStopSignals 在特性关闭且旧对象未使用时清除容器生命周期中的停止信号。
func dropContainerStopSignals(podSpec, oldPodSpec *api.PodSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.ContainerStopSignals) || containerStopSignalsInUse(oldPodSpec) {
		return
	}

	// StopSignal 是 Lifecycle 中唯一受该特性保护的字段；清空后若 Lifecycle 已为空则一并移除，
	// 避免留下空对象。
	wipeLifecycle := func(ctr *api.Container) {
		if ctr.Lifecycle == nil {
			return
		}
		if ctr.Lifecycle.StopSignal != nil {
			ctr.Lifecycle.StopSignal = nil
			if *ctr.Lifecycle == (api.Lifecycle{}) {
				ctr.Lifecycle = nil
			}
		}
	}

	VisitContainers(podSpec, AllContainers, func(c *api.Container, containerType ContainerType) bool {
		if c.Lifecycle == nil {
			return true
		}
		wipeLifecycle(c)
		return true
	})
}

// containerStopSignalsInUse 判断 pod spec 是否有容器使用了停止信号。
func containerStopSignalsInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	var inUse bool
	VisitContainers(podSpec, AllContainers, func(c *api.Container, containerType ContainerType) bool {
		if c.Lifecycle == nil {
			return true
		}
		if c.Lifecycle.StopSignal != nil {
			inUse = true
			return false
		}
		return true
	})
	return inUse
}

// dropHTTPProbeProtocol 在特性关闭且旧对象未使用时清除 HTTP 探针和生命周期钩子中的协议字段。
func dropHTTPProbeProtocol(podSpec, oldPodSpec *api.PodSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.H2CContainerProbe) || httpProbeProtocolInUse(oldPodSpec) {
		return
	}

	dropProbeProtocol := func(p *api.Probe) {
		if p != nil && p.HTTPGet != nil {
			p.HTTPGet.Protocol = nil
		}
	}
	dropHandlerProtocol := func(h *api.LifecycleHandler) {
		if h != nil && h.HTTPGet != nil {
			h.HTTPGet.Protocol = nil
		}
	}

	VisitContainers(podSpec, AllContainers, func(c *api.Container, _ ContainerType) bool {
		dropProbeProtocol(c.LivenessProbe)
		dropProbeProtocol(c.ReadinessProbe)
		dropProbeProtocol(c.StartupProbe)
		if c.Lifecycle != nil {
			dropHandlerProtocol(c.Lifecycle.PostStart)
			dropHandlerProtocol(c.Lifecycle.PreStop)
		}
		return true
	})
}

// httpProbeProtocolInUse 判断 pod spec 是否有容器在 HTTP 探针或生命周期钩子中设置了协议。
func httpProbeProtocolInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	var inUse bool
	VisitContainers(podSpec, AllContainers, func(c *api.Container, _ ContainerType) bool {
		for _, probe := range []*api.Probe{c.LivenessProbe, c.ReadinessProbe, c.StartupProbe} {
			if probe != nil && probe.HTTPGet != nil && probe.HTTPGet.Protocol != nil {
				inUse = true
				return false
			}
		}
		if c.Lifecycle != nil {
			for _, handler := range []*api.LifecycleHandler{c.Lifecycle.PostStart, c.Lifecycle.PreStop} {
				if handler != nil && handler.HTTPGet != nil && handler.HTTPGet.Protocol != nil {
					inUse = true
					return false
				}
			}
		}
		return true
	})
	return inUse
}

// dropDisabledPodLevelResources 在 PodLevelResources 关闭且旧对象未使用时清除 pod 级别的 Resources。
func dropDisabledPodLevelResources(podSpec, oldPodSpec *api.PodSpec) {
	// 特性关闭且旧 spec 未使用该字段时才清除，避免更新时丢数据。
	if !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) && !podLevelResourcesInUse(oldPodSpec) {
		podSpec.Resources = nil
	}
}

// grpcProbeModeInUse 判断 pod spec 是否有容器在 gRPC 探针中设置了 Mode。
func grpcProbeModeInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	var inUse bool
	VisitContainers(podSpec, AllContainers, func(c *api.Container, _ ContainerType) bool {
		if probeHasGRPCMode(c.LivenessProbe) || probeHasGRPCMode(c.ReadinessProbe) || probeHasGRPCMode(c.StartupProbe) {
			inUse = true
			return false
		}
		return true
	})
	return inUse
}

// probeHasGRPCMode 检查探针的 Mode 是否被设置为任意值（包括 Plaintext）。
// 这样即使开关关闭，已有字段值也会被保留，不会因为值为默认值而被误清除。
func probeHasGRPCMode(probe *api.Probe) bool {
	return probe != nil && probe.GRPC != nil && probe.GRPC.Mode != nil
}

// dropDisabledGRPCContainerProbeTLS 在特性关闭且旧对象未使用时清除 gRPC 探针的 TLS Mode 字段。
func dropDisabledGRPCContainerProbeTLS(podSpec, oldPodSpec *api.PodSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.GRPCContainerProbeTLS) || grpcProbeModeInUse(oldPodSpec) {
		return
	}
	VisitContainers(podSpec, AllContainers, func(c *api.Container, _ ContainerType) bool {
		dropProbeGRPCTLS(c.LivenessProbe)
		dropProbeGRPCTLS(c.ReadinessProbe)
		dropProbeGRPCTLS(c.StartupProbe)
		return true
	})
}

// dropProbeGRPCTLS 清除单个探针 gRPC 配置中的 Mode 字段。
func dropProbeGRPCTLS(p *api.Probe) {
	if p != nil && p.GRPC != nil {
		p.GRPC.Mode = nil
	}
}

// dropDisabledPodStatusFields 从 pod status 中清除被禁用的字段。
func dropDisabledPodStatusFields(podStatus, oldPodStatus *api.PodStatus, podSpec, oldPodSpec *api.PodSpec) {
	// 新 status 必须非 nil，便于后续统一字段访问。
	if podStatus == nil {
		podStatus = &api.PodStatus{}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) && !podLevelStatusResourcesInUse(oldPodStatus) {
		// 清除 pod 级别的 Resources 和 AllocatedResources。
		podStatus.Resources = nil
		podStatus.AllocatedResources = nil
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) && !inPlacePodVerticalScalingInUse(oldPodSpec) {
		// 清除容器状态中的 Resources 字段。
		dropResourcesField := func(csl []api.ContainerStatus) {
			for i := range csl {
				csl[i].Resources = nil
			}
		}
		dropResourcesField(podStatus.ContainerStatuses)
		dropResourcesField(podStatus.InitContainerStatuses)
		dropResourcesField(podStatus.EphemeralContainerStatuses)

		// 清除容器状态中的 AllocatedResources 字段。
		dropAllocatedResourcesField := func(csl []api.ContainerStatus) {
			for i := range csl {
				csl[i].AllocatedResources = nil
			}
		}
		dropAllocatedResourcesField(podStatus.ContainerStatuses)
		dropAllocatedResourcesField(podStatus.InitContainerStatuses)
		dropAllocatedResourcesField(podStatus.EphemeralContainerStatuses)
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) && !dynamicResourceAllocationInUse(oldPodSpec) {
		podStatus.ResourceClaimStatuses = nil
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) && !draExendedResourceInUse(oldPodStatus) {
		podStatus.ExtendedResourceClaimStatus = nil
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.RecursiveReadOnlyMounts) && !rroInUse(oldPodSpec) {
		for i := range podStatus.ContainerStatuses {
			podStatus.ContainerStatuses[i].VolumeMounts = nil
		}
		for i := range podStatus.InitContainerStatuses {
			podStatus.InitContainerStatuses[i].VolumeMounts = nil
		}
		for i := range podStatus.EphemeralContainerStatuses {
			podStatus.EphemeralContainerStatuses[i].VolumeMounts = nil
		}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.ResourceHealthStatus) && !resourceHealthStatusInUse(oldPodStatus) {
		setAllocatedResourcesStatusToNil := func(csl []api.ContainerStatus) {
			for i := range csl {
				csl[i].AllocatedResourcesStatus = nil
			}
		}
		setAllocatedResourcesStatusToNil(podStatus.ContainerStatuses)
		setAllocatedResourcesStatusToNil(podStatus.InitContainerStatuses)
		setAllocatedResourcesStatusToNil(podStatus.EphemeralContainerStatuses)
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.ResourceHealthStatusMessage) && !resourceHealthStatusMessageInUse(oldPodStatus) {
		dropMessageField := func(csl []api.ContainerStatus) {
			for i := range csl {
				for j := range csl[i].AllocatedResourcesStatus {
					for k := range csl[i].AllocatedResourcesStatus[j].Resources {
						csl[i].AllocatedResourcesStatus[j].Resources[k].Message = nil
					}
				}
			}
		}
		dropMessageField(podStatus.ContainerStatuses)
		dropMessageField(podStatus.InitContainerStatuses)
		dropMessageField(podStatus.EphemeralContainerStatuses)
	}

	// 清空 ContainerStatus.User（对应禁用 SupplementalGroupsPolicy 的场景）。
	if !utilfeature.DefaultFeatureGate.Enabled(features.SupplementalGroupsPolicy) && !supplementalGroupsPolicyInUse(oldPodSpec) {
		dropUserField := func(csl []api.ContainerStatus) {
			for i := range csl {
				csl[i].User = nil
			}
		}
		dropUserField(podStatus.InitContainerStatuses)
		dropUserField(podStatus.ContainerStatuses)
		dropUserField(podStatus.EphemeralContainerStatuses)
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.PodObservedGenerationTracking) && !podObservedGenerationTrackingInUse(oldPodStatus) {
		podStatus.ObservedGeneration = 0
		for i := range podStatus.Conditions {
			podStatus.Conditions[i].ObservedGeneration = 0
		}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.ImageVolumeWithDigest) && !imageVolumeWithDigestInUse(oldPodStatus) {
		dropImageVolumeWithDigest(podStatus)
	}

	dropPodNodeAllocatableResourceStatus(podStatus, oldPodStatus)

	if !utilfeature.DefaultFeatureGate.Enabled(features.CSIVolumeHealth) && !volumeHealthInUse(oldPodStatus) {
		podStatus.VolumeHealth = nil
	}
}

// dropDisabledDynamicResourceAllocationFields 从容器 spec 和 pod 级别移除资源声明引用。
// 旧 pod spec 已使用这些字段时予以保留。
func dropDisabledDynamicResourceAllocationFields(podSpec, oldPodSpec *api.PodSpec) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) && !dynamicResourceAllocationInUse(oldPodSpec) {
		dropResourceClaimRequests(podSpec.Containers)
		dropResourceClaimRequests(podSpec.InitContainers)
		dropEphemeralResourceClaimRequests(podSpec.EphemeralContainers)
		podSpec.ResourceClaims = nil
	}
}

// draExendedResourceInUse 判断 pod status 是否使用了扩展资源声明状态。
func draExendedResourceInUse(podStatus *api.PodStatus) bool {
	if podStatus != nil && podStatus.ExtendedResourceClaimStatus != nil {
		return true
	}
	return false
}

// dropPodNodeAllocatableResourceStatus 在特性关闭且旧对象未使用时清除节点可分配资源声明状态。
func dropPodNodeAllocatableResourceStatus(podStatus, oldPodStatus *api.PodStatus) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources) || draNodeAllocatableResourceStatusInUse(oldPodStatus) {
		return
	}
	podStatus.NodeAllocatableResourceClaimStatuses = nil
}

// draNodeAllocatableResourceStatusInUse 判断 pod status 是否存在节点可分配资源声明状态。
func draNodeAllocatableResourceStatusInUse(podStatus *api.PodStatus) bool {
	if podStatus == nil {
		return false
	}
	return len(podStatus.NodeAllocatableResourceClaimStatuses) > 0
}

// volumeHealthInUse 判断 pod status 是否存在卷健康状态。
func volumeHealthInUse(podStatus *api.PodStatus) bool {
	if podStatus == nil {
		return false
	}
	return len(podStatus.VolumeHealth) > 0
}

// resourceHealthStatusInUse 判断容器状态中是否存在资源健康状态。
func resourceHealthStatusInUse(podStatus *api.PodStatus) bool {
	if podStatus == nil {
		return false
	}

	checkContainerStatuses := func(csl []api.ContainerStatus) bool {
		for _, cs := range csl {
			if len(cs.AllocatedResourcesStatus) > 0 {
				return true
			}
		}
		return false
	}

	if checkContainerStatuses(podStatus.ContainerStatuses) {
		return true
	}
	if checkContainerStatuses(podStatus.InitContainerStatuses) {
		return true
	}
	if checkContainerStatuses(podStatus.EphemeralContainerStatuses) {
		return true
	}

	return false
}

// resourceHealthStatusMessageInUse 判断容器状态的资源健康状态中是否存在 Message 字段。
func resourceHealthStatusMessageInUse(podStatus *api.PodStatus) bool {
	if podStatus == nil {
		return false
	}

	checkContainerStatuses := func(csl []api.ContainerStatus) bool {
		for _, cs := range csl {
			for _, rs := range cs.AllocatedResourcesStatus {
				for _, rh := range rs.Resources {
					if rh.Message != nil {
						return true
					}
				}
			}
		}
		return false
	}

	if checkContainerStatuses(podStatus.ContainerStatuses) {
		return true
	}
	if checkContainerStatuses(podStatus.InitContainerStatuses) {
		return true
	}
	if checkContainerStatuses(podStatus.EphemeralContainerStatuses) {
		return true
	}

	return false
}

// dynamicResourceAllocationInUse 判断 pod spec 是否使用了动态资源分配。
func dynamicResourceAllocationInUse(podSpec *api.PodSpec) bool {
	// 只需检查 pod 级别的 ResourceClaims：容器不能引用 pod 级别不存在的声明，
	// 因此该字段非空即可代表特性被使用。
	if podSpec != nil && len(podSpec.ResourceClaims) > 0 {
		return true
	}
	return false
}

// dropResourceClaimRequests 清除容器资源需求中的声明引用。
func dropResourceClaimRequests(containers []api.Container) {
	for i := range containers {
		containers[i].Resources.Claims = nil
	}
}

// dropEphemeralResourceClaimRequests 清除临时容器资源需求中的声明引用。
func dropEphemeralResourceClaimRequests(containers []api.EphemeralContainer) {
	for i := range containers {
		containers[i].Resources.Claims = nil
	}
}

// dropDisabledProcMountField 仅当旧 spec 未使用时，清除 PodSpec 中被禁用的 ProcMount 相关字段。
func dropDisabledProcMountField(podSpec, oldPodSpec *api.PodSpec) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.ProcMountType) && !procMountInUse(oldPodSpec) {
		defaultProcMount := api.DefaultProcMount
		VisitContainers(podSpec, AllContainers, func(c *api.Container, containerType ContainerType) bool {
			if c.SecurityContext != nil && c.SecurityContext.ProcMount != nil {
				// 1.12 曾错误地强制把 ProcMount 设为非 nil。当开关关闭、旧对象未使用非默认值、
				// 而新对象又带着该字段时，将其强制回默认值。
				// 注意：不能置为 nil，否则会与已持久化的数据产生 diff。
				c.SecurityContext.ProcMount = &defaultProcMount
			}
			return true
		})
	}
}

// dropDisabledNodeInclusionPolicyFields 仅当旧 spec 未使用时，清除 PodSpec 中被禁用的 NodeInclusionPolicy 相关字段。
func dropDisabledNodeInclusionPolicyFields(podSpec, oldPodSpec *api.PodSpec) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.NodeInclusionPolicyInPodTopologySpread) && podSpec != nil {
		if !nodeTaintsPolicyInUse(oldPodSpec) {
			for i := range podSpec.TopologySpreadConstraints {
				podSpec.TopologySpreadConstraints[i].NodeTaintsPolicy = nil
			}
		}
		if !nodeAffinityPolicyInUse(oldPodSpec) {
			for i := range podSpec.TopologySpreadConstraints {
				podSpec.TopologySpreadConstraints[i].NodeAffinityPolicy = nil
			}
		}
	}
}

// dropDisabledMatchLabelKeysFieldInPodAffinity 仅当旧 spec 未使用时，清除 PodSpec 中
// required/preferred PodAffinity/PodAntiAffinity 里被禁用的 MatchLabelKeys 相关字段。
func dropDisabledMatchLabelKeysFieldInPodAffinity(podSpec, oldPodSpec *api.PodSpec) {
	if podSpec == nil || podSpec.Affinity == nil || utilfeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodAffinity) || matchLabelKeysFieldInPodAffinityInUse(oldPodSpec) {
		return
	}

	if affinity := podSpec.Affinity.PodAffinity; affinity != nil {
		dropMatchLabelKeysFieldInPodAffnityTerm(affinity.RequiredDuringSchedulingIgnoredDuringExecution)
		dropMatchLabelKeysFieldInWeightedPodAffnityTerm(affinity.PreferredDuringSchedulingIgnoredDuringExecution)
	}
	if antiaffinity := podSpec.Affinity.PodAntiAffinity; antiaffinity != nil {
		dropMatchLabelKeysFieldInPodAffnityTerm(antiaffinity.RequiredDuringSchedulingIgnoredDuringExecution)
		dropMatchLabelKeysFieldInWeightedPodAffnityTerm(antiaffinity.PreferredDuringSchedulingIgnoredDuringExecution)
	}
}

// dropDisabledMatchLabelKeysFieldInTopologySpread 仅当旧 spec 未使用时，清除 PodSpec 中
// TopologySpread 里被禁用的 MatchLabelKeys 字段。
func dropDisabledMatchLabelKeysFieldInTopologySpread(podSpec, oldPodSpec *api.PodSpec) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodTopologySpread) && !matchLabelKeysInTopologySpreadInUse(oldPodSpec) {
		for i := range podSpec.TopologySpreadConstraints {
			podSpec.TopologySpreadConstraints[i].MatchLabelKeys = nil
		}
	}
}

// dropMatchLabelKeysFieldInWeightedPodAffnityTerm 清除 WeightedPodAffinityTerm 中的 MatchLabelKeys 和 MismatchLabelKeys 字段。
func dropMatchLabelKeysFieldInWeightedPodAffnityTerm(terms []api.WeightedPodAffinityTerm) {
	for i := range terms {
		terms[i].PodAffinityTerm.MatchLabelKeys = nil
		terms[i].PodAffinityTerm.MismatchLabelKeys = nil
	}
}

// dropMatchLabelKeysFieldInPodAffnityTerm 清除 PodAffinityTerm 中的 MatchLabelKeys 和 MismatchLabelKeys 字段。
func dropMatchLabelKeysFieldInPodAffnityTerm(terms []api.PodAffinityTerm) {
	for i := range terms {
		terms[i].MatchLabelKeys = nil
		terms[i].MismatchLabelKeys = nil
	}
}

// dropUserFieldsInKeyToPaths 清除每个 KeyToPath 项中的 User 字段。
func dropUserFieldsInKeyToPaths(items []api.KeyToPath) {
	for j := range items {
		items[j].User = nil
	}
}

// dropUserFieldsInDownwardAPIVolumeFiles 清除每个 DownwardAPIVolumeFile 项中的 User 字段。
func dropUserFieldsInDownwardAPIVolumeFiles(items []api.DownwardAPIVolumeFile) {
	for j := range items {
		items[j].User = nil
	}
}

// matchLabelKeysFieldInPodAffinityInUse 判断给定的亲和性条目是否设置了 MatchLabelKeys 或 MismatchLabelKeys。
func matchLabelKeysFieldInPodAffinityInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil || podSpec.Affinity == nil {
		return false
	}

	if affinity := podSpec.Affinity.PodAffinity; affinity != nil {
		for _, c := range affinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if len(c.MatchLabelKeys) > 0 || len(c.MismatchLabelKeys) > 0 {
				return true
			}
		}

		for _, c := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if len(c.PodAffinityTerm.MatchLabelKeys) > 0 || len(c.PodAffinityTerm.MismatchLabelKeys) > 0 {
				return true
			}
		}
	}

	if antiAffinity := podSpec.Affinity.PodAntiAffinity; antiAffinity != nil {
		for _, c := range antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if len(c.MatchLabelKeys) > 0 || len(c.MismatchLabelKeys) > 0 {
				return true
			}
		}

		for _, c := range antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if len(c.PodAffinityTerm.MatchLabelKeys) > 0 || len(c.PodAffinityTerm.MismatchLabelKeys) > 0 {
				return true
			}
		}
	}

	return false
}

// matchLabelKeysInTopologySpreadInUse 判断 pod spec 非 nil 且 TopologySpreadConstraints 中设置了 MatchLabelKeys。
func matchLabelKeysInTopologySpreadInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}

	for _, c := range podSpec.TopologySpreadConstraints {
		if len(c.MatchLabelKeys) > 0 {
			return true
		}
	}
	return false
}

// nodeAffinityPolicyInUse 判断 pod spec 非 nil 且 TopologySpreadConstraints 中设置了 NodeAffinityPolicy。
func nodeAffinityPolicyInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	for _, c := range podSpec.TopologySpreadConstraints {
		if c.NodeAffinityPolicy != nil {
			return true
		}
	}
	return false
}

// nodeTaintsPolicyInUse 判断 pod spec 非 nil 且 TopologySpreadConstraints 中设置了 NodeTaintsPolicy。
func nodeTaintsPolicyInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	for _, c := range podSpec.TopologySpreadConstraints {
		if c.NodeTaintsPolicy != nil {
			return true
		}
	}
	return false
}

// hostUsersInUse 判断 pod spec 是否设置了 spec.hostUsers 字段。
func hostUsersInUse(podSpec *api.PodSpec) bool {
	return podSpec != nil && podSpec.HostUsers != nil
}

// supplementalGroupsPolicyInUse 判断 pod spec 的 SecurityContext 是否设置了 SupplementalGroupsPolicy。
func supplementalGroupsPolicyInUse(podSpec *api.PodSpec) bool {
	return podSpec != nil && podSpec.SecurityContext != nil && podSpec.SecurityContext.SupplementalGroupsPolicy != nil
}

// podObservedGenerationTrackingInUse 判断 pod status 或其条件中是否记录了 ObservedGeneration。
func podObservedGenerationTrackingInUse(podStatus *api.PodStatus) bool {
	if podStatus == nil {
		return false
	}

	if podStatus.ObservedGeneration != 0 {
		return true
	}

	for _, condition := range podStatus.Conditions {
		if condition.ObservedGeneration != 0 {
			return true
		}
	}

	return false
}

// podLevelResourcesInUse 判断 pod spec 非 nil 且 pod 级别 Resources 含有非空的 Requests 或 Limits。
func podLevelResourcesInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}

	if podSpec.Resources == nil {
		return false
	}

	if len(podSpec.Resources.Requests) > 0 {
		return true
	}

	if len(podSpec.Resources.Limits) > 0 {
		return true
	}

	return false
}

// podLevelStatusResourcesInUse 检查 PodStatus 中是否设置了 AllocatedResources 或 Resources。
func podLevelStatusResourcesInUse(podStatus *api.PodStatus) bool {
	if podStatus == nil {
		return false
	}

	return podStatus.Resources != nil || podStatus.AllocatedResources != nil
}

// inPlacePodVerticalScalingInUse 判断 pod spec 非 nil 且普通或 init 容器设置了 ResizePolicy。
func inPlacePodVerticalScalingInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	var inUse bool
	containersMask := Containers | InitContainers
	VisitContainers(podSpec, containersMask, func(c *api.Container, containerType ContainerType) bool {
		if len(c.ResizePolicy) > 0 {
			inUse = true
			return false
		}
		return true
	})
	return inUse
}

// procMountInUse 判断 pod spec 非 nil 且有容器的 SecurityContext.ProcMount 被设为非默认值。
func procMountInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}

	var inUse bool
	VisitContainers(podSpec, AllContainers, func(c *api.Container, containerType ContainerType) bool {
		if c.SecurityContext == nil || c.SecurityContext.ProcMount == nil {
			return true
		}
		if *c.SecurityContext.ProcMount != api.DefaultProcMount {
			inUse = true
			return false
		}
		return true
	})

	return inUse
}

// clusterTrustBundleProjectionInUse 判断 pod spec 的卷中是否投影了 ClusterTrustBundle。
func clusterTrustBundleProjectionInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	for _, v := range podSpec.Volumes {
		if v.Projected == nil {
			continue
		}

		for _, s := range v.Projected.Sources {
			if s.ClusterTrustBundle != nil {
				return true
			}
		}
	}

	return false
}

// rroInUse 判断 pod spec 是否有容器挂载点设置了 RecursiveReadOnly。
func rroInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	var inUse bool
	VisitContainers(podSpec, AllContainers, func(c *api.Container, _ ContainerType) bool {
		for _, f := range c.VolumeMounts {
			if f.RecursiveReadOnly != nil {
				inUse = true
				return false
			}
		}
		return true
	})
	return inUse
}

// emptyDirVolumeModeInUse 判断 pod spec 的 emptyDir 卷是否设置了 Mode。
func emptyDirVolumeModeInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	for _, vol := range podSpec.Volumes {
		if vol.EmptyDir != nil && vol.EmptyDir.Mode != nil {
			return true
		}
	}
	return false
}

// bindMountOptionsInUse 判断 pod spec 是否有容器挂载点设置了 BindMountOptions。
func bindMountOptionsInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	var inUse bool
	VisitContainers(podSpec, AllContainers, func(c *api.Container, _ ContainerType) bool {
		for _, f := range c.VolumeMounts {
			if len(f.BindMountOptions) > 0 {
				inUse = true
				return false
			}
		}
		return true
	})
	return inUse
}

// dropDisabledClusterTrustBundleProjection 在特性关闭且旧对象未使用时清除 ClusterTrustBundle 投影。
func dropDisabledClusterTrustBundleProjection(podSpec, oldPodSpec *api.PodSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.ClusterTrustBundleProjection) {
		return
	}
	if podSpec == nil {
		return
	}

	// 旧 pod 已使用该字段时允许继续使用，避免更新时丢数据。
	if clusterTrustBundleProjectionInUse(oldPodSpec) {
		return
	}

	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Projected == nil {
			continue
		}

		for j := range podSpec.Volumes[i].Projected.Sources {
			podSpec.Volumes[i].Projected.Sources[j].ClusterTrustBundle = nil
		}
	}
}

// podCertificateProjectionInUse 判断 pod spec 的卷中是否投影了 PodCertificate。
func podCertificateProjectionInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	for _, v := range podSpec.Volumes {
		if v.Projected == nil {
			continue
		}

		for _, s := range v.Projected.Sources {
			if s.PodCertificate != nil {
				return true
			}
		}
	}

	return false
}

// dropDisabledPodCertificateProjection 在特性关闭且旧对象未使用时清除 PodCertificate 投影。
func dropDisabledPodCertificateProjection(podSpec, oldPodSpec *api.PodSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PodCertificateRequest) {
		return
	}
	if podSpec == nil {
		return
	}

	// 旧 pod 已使用该字段时允许继续使用。
	if podCertificateProjectionInUse(oldPodSpec) {
		return
	}

	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Projected == nil {
			continue
		}

		for j := range podSpec.Volumes[i].Projected.Sources {
			podSpec.Volumes[i].Projected.Sources[j].PodCertificate = nil
		}
	}
}

// dropDisabledAtomicWriteVolumeUserFields 在特性关闭且旧对象未使用时，
// 清除各类卷及其投影来源中的 User/DefaultUser 字段。
func dropDisabledAtomicWriteVolumeUserFields(podSpec, oldPodSpec *api.PodSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.AtomicWriteVolumeUserFields) {
		return
	}
	if podSpec == nil {
		return
	}

	// 旧 pod 已使用这些字段时允许继续使用。
	if atomicWriteVolumeUserFieldsInUse(oldPodSpec) {
		return
	}

	for i := range podSpec.Volumes {
		switch {
		case podSpec.Volumes[i].Secret != nil:
			podSpec.Volumes[i].Secret.DefaultUser = nil
			dropUserFieldsInKeyToPaths(podSpec.Volumes[i].Secret.Items)
		case podSpec.Volumes[i].ConfigMap != nil:
			podSpec.Volumes[i].ConfigMap.DefaultUser = nil
			dropUserFieldsInKeyToPaths(podSpec.Volumes[i].ConfigMap.Items)
		case podSpec.Volumes[i].DownwardAPI != nil:
			podSpec.Volumes[i].DownwardAPI.DefaultUser = nil
			dropUserFieldsInDownwardAPIVolumeFiles(podSpec.Volumes[i].DownwardAPI.Items)
		case podSpec.Volumes[i].Projected != nil:
			podSpec.Volumes[i].Projected.DefaultUser = nil

			for j := range podSpec.Volumes[i].Projected.Sources {
				switch {
				case podSpec.Volumes[i].Projected.Sources[j].Secret != nil:
					dropUserFieldsInKeyToPaths(podSpec.Volumes[i].Projected.Sources[j].Secret.Items)
				case podSpec.Volumes[i].Projected.Sources[j].DownwardAPI != nil:
					dropUserFieldsInDownwardAPIVolumeFiles(podSpec.Volumes[i].Projected.Sources[j].DownwardAPI.Items)
				case podSpec.Volumes[i].Projected.Sources[j].ConfigMap != nil:
					dropUserFieldsInKeyToPaths(podSpec.Volumes[i].Projected.Sources[j].ConfigMap.Items)
				case podSpec.Volumes[i].Projected.Sources[j].ServiceAccountToken != nil:
					podSpec.Volumes[i].Projected.Sources[j].ServiceAccountToken.User = nil
				case podSpec.Volumes[i].Projected.Sources[j].ClusterTrustBundle != nil:
					podSpec.Volumes[i].Projected.Sources[j].ClusterTrustBundle.User = nil
				case podSpec.Volumes[i].Projected.Sources[j].PodCertificate != nil:
					podSpec.Volumes[i].Projected.Sources[j].PodCertificate.User = nil
				}
			}
		}
	}
}

// atomicWriteVolumeUserFieldsInUse 判断 pod spec 的卷中是否使用了 User/DefaultUser 字段。
func atomicWriteVolumeUserFieldsInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}

	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Secret != nil {
			if podSpec.Volumes[i].Secret.DefaultUser != nil {
				return true
			}

			for j := range podSpec.Volumes[i].Secret.Items {
				if podSpec.Volumes[i].Secret.Items[j].User != nil {
					return true
				}
			}
		} else if podSpec.Volumes[i].ConfigMap != nil {
			if podSpec.Volumes[i].ConfigMap.DefaultUser != nil {
				return true
			}

			for j := range podSpec.Volumes[i].ConfigMap.Items {
				if podSpec.Volumes[i].ConfigMap.Items[j].User != nil {
					return true
				}
			}
		} else if podSpec.Volumes[i].DownwardAPI != nil {
			if podSpec.Volumes[i].DownwardAPI.DefaultUser != nil {
				return true
			}

			for j := range podSpec.Volumes[i].DownwardAPI.Items {
				if podSpec.Volumes[i].DownwardAPI.Items[j].User != nil {
					return true
				}
			}
		} else if podSpec.Volumes[i].Projected != nil {
			if podSpec.Volumes[i].Projected.DefaultUser != nil {
				return true
			}

			for j := range podSpec.Volumes[i].Projected.Sources {
				if podSpec.Volumes[i].Projected.Sources[j].Secret != nil {
					for k := range podSpec.Volumes[i].Projected.Sources[j].Secret.Items {
						if podSpec.Volumes[i].Projected.Sources[j].Secret.Items[k].User != nil {
							return true
						}
					}
				} else if podSpec.Volumes[i].Projected.Sources[j].DownwardAPI != nil {
					for k := range podSpec.Volumes[i].Projected.Sources[j].DownwardAPI.Items {
						if podSpec.Volumes[i].Projected.Sources[j].DownwardAPI.Items[k].User != nil {
							return true
						}
					}
				} else if podSpec.Volumes[i].Projected.Sources[j].ConfigMap != nil {
					for k := range podSpec.Volumes[i].Projected.Sources[j].ConfigMap.Items {
						if podSpec.Volumes[i].Projected.Sources[j].ConfigMap.Items[k].User != nil {
							return true
						}
					}
				} else if podSpec.Volumes[i].Projected.Sources[j].ServiceAccountToken != nil {
					if podSpec.Volumes[i].Projected.Sources[j].ServiceAccountToken.User != nil {
						return true
					}
				} else if podSpec.Volumes[i].Projected.Sources[j].ClusterTrustBundle != nil {
					if podSpec.Volumes[i].Projected.Sources[j].ClusterTrustBundle.User != nil {
						return true
					}
				} else if podSpec.Volumes[i].Projected.Sources[j].PodCertificate != nil {
					if podSpec.Volumes[i].Projected.Sources[j].PodCertificate.User != nil {
						return true
					}
				}
			}
		}
	}

	return false
}

// hasInvalidLabelValueInAffinitySelector 判断亲和性/反亲和性条目中的 selector 是否含非法标签值。
func hasInvalidLabelValueInAffinitySelector(spec *api.PodSpec) bool {
	if spec.Affinity != nil {
		if spec.Affinity.PodAffinity != nil {
			for _, term := range spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
				allErrs := apivalidation.ValidatePodAffinityTermSelector(term, false, nil)
				if len(allErrs) != 0 {
					return true
				}
			}
			for _, term := range spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				allErrs := apivalidation.ValidatePodAffinityTermSelector(term.PodAffinityTerm, false, nil)
				if len(allErrs) != 0 {
					return true
				}
			}
		}
		if spec.Affinity.PodAntiAffinity != nil {
			for _, term := range spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
				allErrs := apivalidation.ValidatePodAffinityTermSelector(term, false, nil)
				if len(allErrs) != 0 {
					return true
				}
			}
			for _, term := range spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				allErrs := apivalidation.ValidatePodAffinityTermSelector(term.PodAffinityTerm, false, nil)
				if len(allErrs) != 0 {
					return true
				}
			}
		}
	}
	return false
}

// IsRestartableInitContainer 判断容器的重启策略是否为 Always。
// 该函数不校验传入的容器是否真的是 init 容器，只检查重启策略字段。
func IsRestartableInitContainer(initContainer *api.Container) bool {
	if initContainer == nil || initContainer.RestartPolicy == nil {
		return false
	}
	return *initContainer.RestartPolicy == api.ContainerRestartPolicyAlways
}

// hasInvalidLabelValueInRequiredNodeAffinity 判断必需节点亲和性的 NodeSelectorTerms 是否含非法标签值。
func hasInvalidLabelValueInRequiredNodeAffinity(spec *api.PodSpec) bool {
	if spec == nil ||
		spec.Affinity == nil ||
		spec.Affinity.NodeAffinity == nil ||
		spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	return helper.HasInvalidLabelValueInNodeSelectorTerms(spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms)
}

// dropImageVolumes 在 ImageVolume 特性关闭且旧对象未使用时清除镜像卷。
// KEP: https://kep.k8s.io/4639
func dropImageVolumes(podSpec, oldPodSpec *api.PodSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.ImageVolume) || imageVolumesInUse(oldPodSpec) {
		return
	}

	// 先记录被删除的镜像卷名称，再据此清理所有容器对它们的挂载引用。
	imageVolumeNames := sets.New[string]()
	var newVolumes []api.Volume
	for _, v := range podSpec.Volumes {
		if v.Image != nil {
			imageVolumeNames.Insert(v.Name)
			continue
		}
		newVolumes = append(newVolumes, v)
	}
	podSpec.Volumes = newVolumes

	dropVolumeMounts := func(givenMounts []api.VolumeMount) (newVolumeMounts []api.VolumeMount) {
		for _, m := range givenMounts {
			if !imageVolumeNames.Has(m.Name) {
				newVolumeMounts = append(newVolumeMounts, m)
			}
		}
		return newVolumeMounts
	}

	for i, c := range podSpec.Containers {
		podSpec.Containers[i].VolumeMounts = dropVolumeMounts(c.VolumeMounts)
	}

	for i, c := range podSpec.InitContainers {
		podSpec.InitContainers[i].VolumeMounts = dropVolumeMounts(c.VolumeMounts)
	}

	for i, c := range podSpec.EphemeralContainers {
		podSpec.EphemeralContainers[i].VolumeMounts = dropVolumeMounts(c.VolumeMounts)
	}
}

// imageVolumesInUse 判断 pod spec 中是否存在镜像卷。
func imageVolumesInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}

	for _, v := range podSpec.Volumes {
		if v.Image != nil {
			return true
		}
	}

	return false
}

// dropSELinuxChangePolicy 在特性关闭且旧对象未使用时清除 pod 级别的 SELinuxChangePolicy。
func dropSELinuxChangePolicy(podSpec, oldPodSpec *api.PodSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxChangePolicy) || seLinuxChangePolicyInUse(oldPodSpec) {
		return
	}
	if podSpec == nil || podSpec.SecurityContext == nil {
		return
	}
	podSpec.SecurityContext.SELinuxChangePolicy = nil
}

// seLinuxChangePolicyInUse 判断 pod spec 的 SecurityContext 是否设置了 SELinuxChangePolicy。
func seLinuxChangePolicyInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil || podSpec.SecurityContext == nil {
		return false
	}
	return podSpec.SecurityContext.SELinuxChangePolicy != nil
}

// useOnlyRecursiveSELinuxChangePolicy 判断是否只允许 Recursive 这一种 SELinux 变更策略。
func useOnlyRecursiveSELinuxChangePolicy(oldPodSpec *api.PodSpec) bool {
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMount) {
		// 开关开启时所有策略都允许。
		return false
	}

	if seLinuxChangePolicyInUse(oldPodSpec) {
		// 旧对象已设置任意策略：必须保持其可更新性，不能收紧校验。
		return false
	}
	// 开关关闭且旧对象没有值：只允许 Recursive。
	return true
}

// taintTolerationComparisonOperatorsInUse 判断 pod spec 的容忍规则是否使用了 Lt/Gt 比较操作符。
func taintTolerationComparisonOperatorsInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	for _, toleration := range podSpec.Tolerations {
		if toleration.Operator == api.TolerationOpLt || toleration.Operator == api.TolerationOpGt {
			return true
		}
	}
	return false
}

// allowTaintTolerationComparisonOperators 判断是否允许容忍规则使用比较操作符。
func allowTaintTolerationComparisonOperators(oldPodSpec *api.PodSpec) bool {
	// 开关开启，或旧 spec 已使用比较操作符时允许。
	if utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators) ||
		taintTolerationComparisonOperatorsInUse(oldPodSpec) {
		return true
	}
	return false
}

// hasUserNamespacesWithVolumeDevices 判断 pod 是否同时启用了 user namespace 并使用了 volume devices。
func hasUserNamespacesWithVolumeDevices(podSpec *api.PodSpec) bool {
	// HostUsers 为 nil 或 true 表示未启用 user namespace。
	if podSpec.HostUsers == nil || *podSpec.HostUsers {
		return false
	}

	hasVolumeDevices := false
	VisitContainers(podSpec, AllContainers, func(c *api.Container, _ ContainerType) bool {
		if len(c.VolumeDevices) > 0 {
			hasVolumeDevices = true
			return false // 已找到，停止遍历
		}
		return true // 继续遍历
	})
	return hasVolumeDevices
}

// hasRestartableInitContainerResizePolicy 判断 pod spec 非 nil 且存在
// 同时设置了 ContainerRestartPolicyAlways 和 ResizePolicy 的 init 容器。
func hasRestartableInitContainerResizePolicy(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	for _, c := range podSpec.InitContainers {
		if IsRestartableInitContainer(&c) && len(c.ResizePolicy) > 0 {
			return true
		}
	}
	return false
}

// hasEmptyImageVolumeReference 判断 pod spec 是否存在引用为空的镜像卷。
func hasEmptyImageVolumeReference(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}
	for _, v := range podSpec.Volumes {
		if v.Image != nil && len(v.Image.Reference) == 0 {
			return true
		}
	}
	return false
}

// HasAPIObjectReference 判断 pod spec 中是否引用了 API 对象，
// 返回引用类型对应的复数资源名；遇到未知字段时返回错误。
func HasAPIObjectReference(pod *api.Pod) (bool, string, error) {
	if pod.Spec.ServiceAccountName != "" {
		return true, "serviceaccounts", nil
	}

	hasSecrets := false
	VisitPodSecretNames(pod, func(name string) (shouldContinue bool) { hasSecrets = true; return false }, AllContainers)
	if hasSecrets {
		return true, "secrets", nil
	}

	hasConfigMaps := false
	VisitPodConfigmapNames(pod, func(name string) (shouldContinue bool) { hasConfigMaps = true; return false }, AllContainers)
	if hasConfigMaps {
		return true, "configmaps", nil
	}

	if len(pod.Spec.ResourceClaims) > 0 {
		return true, "resourceclaims", nil
	}

	for _, v := range pod.Spec.Volumes {
		switch {
		case v.AWSElasticBlockStore != nil, v.AzureDisk != nil, v.CephFS != nil, v.Cinder != nil,
			v.DownwardAPI != nil, v.EmptyDir != nil, v.FC != nil, v.FlexVolume != nil, v.Flocker != nil, v.GCEPersistentDisk != nil,
			v.GitRepo != nil, v.HostPath != nil, v.Image != nil, v.ISCSI != nil, v.NFS != nil, v.PhotonPersistentDisk != nil,
			v.PortworxVolume != nil, v.Quobyte != nil, v.RBD != nil, v.ScaleIO != nil, v.StorageOS != nil, v.VsphereVolume != nil:
			// 这些卷类型不引用 Kubernetes API 对象，无需额外权限。
			continue
		case v.ConfigMap != nil:
			return true, "configmaps (via configmap volumes)", nil
		case v.Secret != nil:
			return true, "secrets (via secret volumes)", nil
		case v.CSI != nil:
			return true, "csidrivers (via CSI volumes)", nil
		case v.Glusterfs != nil:
			return true, "endpoints (via glusterFS volumes)", nil
		case v.PersistentVolumeClaim != nil:
			return true, "persistentvolumeclaims", nil
		case v.Ephemeral != nil:
			return true, "persistentvolumeclaims (via ephemeral volumes)", nil
		case v.AzureFile != nil:
			return true, "secrets (via azureFile volumes)", nil
		case v.Projected != nil:
			for _, s := range v.Projected.Sources {
				// 投影卷中需要 Kubernetes API 权限的来源在此逐一识别。
				switch {
				case s.ConfigMap != nil:
					return true, "configmaps (via projected volumes)", nil
				case s.Secret != nil:
					return true, "secrets (via projected volumes)", nil
				case s.ServiceAccountToken != nil:
					return true, "serviceaccounts (via projected volumes)", nil
				case s.ClusterTrustBundle != nil:
					return true, "clustertrustbundles", nil
				case s.PodCertificate != nil:
					return true, "podcertificates", nil
				case s.DownwardAPI != nil:
					// DownwardAPI 来源不访问 Kubernetes API，放行。
					continue
				default:
					// 未知来源一律按需要引用处理并返回错误，避免遗漏造成权限判断失效。
					return true, "", fmt.Errorf("unknown source for projected volume %q", v.Name)
				}
			}
		default:
			return true, "", fmt.Errorf("unknown volume type for volume  %q", v.Name)
		}
	}

	return false, "", nil
}

// ApparmorFieldForAnnotation 将 pod 注解转换为对应的 AppArmor profile 字段。
func ApparmorFieldForAnnotation(annotation string) *api.AppArmorProfile {
	if annotation == api.DeprecatedAppArmorAnnotationValueUnconfined {
		return &api.AppArmorProfile{Type: api.AppArmorProfileTypeUnconfined}
	}

	if annotation == api.DeprecatedAppArmorAnnotationValueRuntimeDefault {
		return &api.AppArmorProfile{Type: api.AppArmorProfileTypeRuntimeDefault}
	}

	if strings.HasPrefix(annotation, api.DeprecatedAppArmorAnnotationValueLocalhostPrefix) {
		localhostProfile := strings.TrimPrefix(annotation, api.DeprecatedAppArmorAnnotationValueLocalhostPrefix)
		if localhostProfile != "" {
			return &api.AppArmorProfile{
				Type:             api.AppArmorProfileTypeLocalhost,
				LocalhostProfile: &localhostProfile,
			}
		}
	}

	// 只有 localhostProfile 名称为空或注解值无法识别时才会走到这里。
	return nil
}

// dropContainerRestartRules 清除容器上受特性保护的重启策略与重启规则。
func dropContainerRestartRules(podSpec *api.PodSpec) {
	if podSpec == nil {
		return
	}
	// init 容器的 Always 策略早于该特性存在，需保留；其余策略和所有规则都要清除。
	for i, c := range podSpec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy != api.ContainerRestartPolicyAlways {
			podSpec.InitContainers[i].RestartPolicy = nil
		}
		podSpec.InitContainers[i].RestartPolicyRules = nil
	}
	for i := range podSpec.Containers {
		podSpec.Containers[i].RestartPolicy = nil
		podSpec.Containers[i].RestartPolicyRules = nil
	}
	for i := range podSpec.EphemeralContainers {
		podSpec.EphemeralContainers[i].RestartPolicy = nil
		podSpec.EphemeralContainers[i].RestartPolicyRules = nil
	}
}

// containerRestartRulesInUse 判断旧 pod spec 是否使用了受特性保护的重启策略或重启规则。
func containerRestartRulesInUse(oldPodSpec *api.PodSpec) bool {
	if oldPodSpec == nil {
		return false
	}
	for _, c := range oldPodSpec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy != api.ContainerRestartPolicyAlways {
			return true
		}
		if len(c.RestartPolicyRules) > 0 {
			return true
		}
	}
	for _, c := range oldPodSpec.Containers {
		if c.RestartPolicy != nil {
			return true
		}
	}
	return false
}

// dropDisabledSchedulingGroup 从 pod spec 中移除调度组，旧 spec 已使用时保留。
func dropDisabledSchedulingGroup(podSpec, oldPodSpec *api.PodSpec) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) && !schedulingGroupInUse(oldPodSpec) {
		podSpec.SchedulingGroup = nil
	}
}

// schedulingGroupInUse 判断 pod spec 是否设置了调度组。
func schedulingGroupInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}

	return podSpec.SchedulingGroup != nil
}

// restartAllContainersActionInUse 判断旧 spec 是否使用了 RestartAllContainers 动作或 sidecar 重启规则。
func restartAllContainersActionInUse(oldPodSpec *api.PodSpec) bool {
	if oldPodSpec == nil {
		return false
	}
	for _, c := range oldPodSpec.Containers {
		for _, rule := range c.RestartPolicyRules {
			if rule.Action == api.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
	}
	for _, c := range oldPodSpec.InitContainers {
		for _, rule := range c.RestartPolicyRules {
			if rule.Action == api.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
		// 该特性同时允许 sidecar 容器配置重启规则。
		if c.RestartPolicy != nil && *c.RestartPolicy == api.ContainerRestartPolicyAlways && len(c.RestartPolicyRules) > 0 {
			return true
		}
	}
	return false
}

// imageVolumeWithDigestInUse 判断容器状态的卷挂载中是否记录了镜像卷的摘要状态。
func imageVolumeWithDigestInUse(oldPodStatus *api.PodStatus) bool {
	if oldPodStatus == nil {
		return false
	}

	for _, containerStatus := range oldPodStatus.ContainerStatuses {
		for _, volumeMount := range containerStatus.VolumeMounts {
			if volumeMount.VolumeStatus != nil {
				return true
			}
		}
	}

	for _, containerStatus := range oldPodStatus.InitContainerStatuses {
		for _, volumeMount := range containerStatus.VolumeMounts {
			if volumeMount.VolumeStatus != nil {
				return true
			}
		}
	}

	for _, containerStatus := range oldPodStatus.EphemeralContainerStatuses {
		for _, volumeMount := range containerStatus.VolumeMounts {
			if volumeMount.VolumeStatus != nil {
				return true
			}
		}
	}

	return false
}

// dropImageVolumeWithDigest 清除容器状态中卷挂载的摘要状态字段。
func dropImageVolumeWithDigest(podStatus *api.PodStatus) {
	if podStatus == nil {
		return
	}

	for i := range podStatus.ContainerStatuses {
		for j := range podStatus.ContainerStatuses[i].VolumeMounts {
			podStatus.ContainerStatuses[i].VolumeMounts[j].VolumeStatus = nil
		}
	}

	for i := range podStatus.InitContainerStatuses {
		for j := range podStatus.InitContainerStatuses[i].VolumeMounts {
			podStatus.InitContainerStatuses[i].VolumeMounts[j].VolumeStatus = nil
		}
	}

	for i := range podStatus.EphemeralContainerStatuses {
		for j := range podStatus.EphemeralContainerStatuses[i].VolumeMounts {
			podStatus.EphemeralContainerStatuses[i].VolumeMounts[j].VolumeStatus = nil
		}
	}
}

// hasRestartContainerForNonSidecarInitContainer 判断是否存在配置了 RestartContainer resize policy
// 的非 sidecar init 容器。
func hasRestartContainerForNonSidecarInitContainer(spec *api.PodSpec) bool {
	if spec == nil {
		return false
	}
	for _, c := range spec.InitContainers {
		if !IsRestartableInitContainer(&c) {
			for _, p := range c.ResizePolicy {
				if p.RestartPolicy == api.RestartContainer {
					return true
				}
			}
		}
	}
	return false
}

// DefaultPodLevelResources 按依赖顺序执行所有 pod 级别资源的默认值填充。
// 将容器 spec 一次性转换为 v1.PodSpec，避免重复转换。
//
// 顺序很关键：
//  1. 用容器大页 limit 的汇总值设置 pod 级别大页 limit。
//  2. 用容器 request 的汇总值填充 pod 级别 request（同时为有 pod 级别 limit 但无 request
//     的资源补齐 request，包括第 1 步设置的大页 limit）。
//  3. 对所有容器都设置了 limit 的资源，将 pod 级别 limit 设为 max(pod 级别 request, 容器 limit 汇总)。
//     第 2 步必须先执行，确保 max() 比较时 pod 级别 request 已完整。
func DefaultPodLevelResources(pod *api.Pod) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) ||
		!utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourcesFixDefaulting) {
		return
	}
	if pod.Spec.Resources == nil {
		return
	}
	if len(pod.Spec.Resources.Requests) == 0 && len(pod.Spec.Resources.Limits) == 0 {
		return
	}

	v1PodSpec := &apiv1.PodSpec{}
	// 此处排除临时容器：它们不能声明资源，也不参与 QoS 或 pod 级别资源默认值计算。
	// 若该前提发生变化，需要同步更新此处。
	if err := corev1.Convert_core_PodSpec_To_v1_PodSpec(&api.PodSpec{
		Containers:     pod.Spec.Containers,
		InitContainers: pod.Spec.InitContainers,
	}, v1PodSpec, nil); err != nil {
		return
	}
	v1Pod := &apiv1.Pod{Spec: *v1PodSpec}

	defaultHugePagePodLevelLimits(pod, v1Pod)
	defaultPodLevelRequests(pod, v1Pod)
	defaultPodLevelLimits(pod, v1Pod)
}

// defaultHugePagePodLevelLimits 填充 pod 级别的大页 limit：
// 未指定时，将其设为容器大页 limit 的汇总值。
func defaultHugePagePodLevelLimits(pod *api.Pod, v1Pod *apiv1.Pod) {
	if pod.Spec.Resources == nil {
		return
	}
	if len(pod.Spec.Resources.Limits) == 0 && len(pod.Spec.Resources.Requests) == 0 {
		return
	}

	podLims := pod.Spec.Resources.Limits
	if podLims == nil {
		podLims = make(api.ResourceList)
	}

	aggrCtrLims := resourcehelper.AggregateContainerLimits(v1Pod, resourcehelper.PodResourcesOptions{})
	for key, aggrCtrLim := range aggrCtrLims {
		coreKey := api.ResourceName(key)
		// 这里只处理大页：CPU/内存等资源可超卖，留给后续步骤填充。
		if !resourcehelper.IsSupportedPodLevelResource(key) || !helper.IsHugePageResourceName(coreKey) {
			continue
		}

		// 若该资源已有 pod 级别 request，说明用户显式配置过，无需再补 limit。
		if _, exists := pod.Spec.Resources.Requests[coreKey]; exists {
			continue
		}

		if _, exists := podLims[coreKey]; !exists {
			podLims[coreKey] = aggrCtrLim
		}
	}

	if len(podLims) > 0 {
		pod.Spec.Resources.Limits = podLims
	}
}

// defaultPodLevelRequests 用容器 request 的汇总值（针对可超卖资源）和 pod 级别 limit
// 填充 pod 级别 request。
func defaultPodLevelRequests(pod *api.Pod, v1Pod *apiv1.Pod) {
	if pod.Spec.Resources == nil {
		return
	}
	if len(pod.Spec.Resources.Requests) == 0 && len(pod.Spec.Resources.Limits) == 0 {
		return
	}

	podReqs := pod.Spec.Resources.Requests
	if podReqs == nil {
		podReqs = make(api.ResourceList)
	}

	aggrCtrReqs := resourcehelper.AggregateContainerRequests(v1Pod, resourcehelper.PodResourcesOptions{})
	for key, aggrCtrReq := range aggrCtrReqs {
		coreKey := api.ResourceName(key)
		// 只对支持 pod 级别且允许超卖的资源，用容器 request 汇总补齐缺失的 request。
		if _, exists := podReqs[coreKey]; !exists && resourcehelper.IsSupportedPodLevelResource(key) && helper.IsOvercommitAllowed(coreKey) {
			podReqs[coreKey] = aggrCtrReq
		}
	}

	// 有 pod 级别 limit 但缺 request 的资源，直接用 limit 作为 request；
	// 这样可保证 request <= limit，同时覆盖第 1 步补出的大页 limit。
	if pod.Spec.Resources.Limits != nil {
		for key, podLim := range pod.Spec.Resources.Limits {
			if _, exists := podReqs[key]; !exists && resourcehelper.IsSupportedPodLevelResource(apiv1.ResourceName(key)) {
				podReqs[key] = podLim.DeepCopy()
			}
		}
	}

	if len(podReqs) > 0 {
		pod.Spec.Resources.Requests = podReqs
	}
}

// defaultPodLevelLimits 对候选资源（所有容器都设置了 limit）将 pod 级别 limit
// 设为 max(pod 级别 request, 容器 limit 汇总值)。
func defaultPodLevelLimits(pod *api.Pod, v1Pod *apiv1.Pod) {
	if pod.Spec.Resources == nil {
		return
	}
	if len(pod.Spec.Resources.Requests) == 0 && len(pod.Spec.Resources.Limits) == 0 {
		return
	}

	candidates := sets.New[api.ResourceName]()
	// 遍历 pod 级别 request 而非 limit：request 存在是启用 pod 级别资源管理的必要信号。
	// 现有默认值逻辑会把容器 limit 传播到容器 request，再汇总到 pod 级别 request（未显式设置时），
	// 因此所有需要补 limit 的候选资源必定已在 request map 中，遍历 request 即可完整定位候选者。
	for resName := range pod.Spec.Resources.Requests {
		if !resourcehelper.IsSupportedPodLevelResource(apiv1.ResourceName(resName)) {
			continue
		}
		// 已有 pod 级别 limit 的资源不做默认值填充。
		if _, ok := pod.Spec.Resources.Limits[resName]; !ok {
			candidates.Insert(resName)
		}
	}

	if candidates.Len() == 0 {
		return
	}

	aggrLimits := aggregateContainerLimits(&pod.Spec, candidates, v1Pod)
	if aggrLimits == nil {
		return
	}

	for resName := range candidates {
		val, ok := aggrLimits[apiv1.ResourceName(resName)]
		if !ok {
			continue
		}
		var podReq resource.Quantity
		if pod.Spec.Resources.Requests != nil {
			podReq = pod.Spec.Resources.Requests[resName]
		}

		// pod 级别 request 超过容器 limit 汇总时，把 pod 级别 limit 抬高到 request，
		// 保证 request <= limit，避免后续校验拒绝更新。
		if podReq.Cmp(val) > 0 {
			val = podReq
		}
		if pod.Spec.Resources.Limits == nil {
			pod.Spec.Resources.Limits = make(api.ResourceList)
		}
		pod.Spec.Resources.Limits[resName] = val
	}
}

// aggregateContainerLimits 返回候选资源在所有容器上的 limit 汇总值。
// 同时过滤候选集合：只要有任一容器没设置该资源的 limit，就将其从候选中移除。
func aggregateContainerLimits(spec *api.PodSpec, candidates sets.Set[api.ResourceName], v1Pod *apiv1.Pod) apiv1.ResourceList {
	// 资源要在 pod 级别补 limit，前提是包括 init 容器和 sidecar 在内的所有容器
	// 都显式设置了该资源的 limit。
	VisitContainers(spec, AllContainers, func(ctr *api.Container, _ ContainerType) bool {
		for resName := range candidates {
			if _, ok := ctr.Resources.Limits[resName]; !ok {
				candidates.Delete(resName)
			}
		}
		return candidates.Len() > 0
	})

	if candidates.Len() == 0 {
		return nil
	}

	return resourcehelper.AggregateContainerLimits(v1Pod, resourcehelper.PodResourcesOptions{})
}

var initContainerAnnotations = map[string]struct{}{
	"pod.beta.kubernetes.io/init-containers":          {},
	"pod.alpha.kubernetes.io/init-containers":         {},
	"pod.beta.kubernetes.io/init-container-statuses":  {},
	"pod.alpha.kubernetes.io/init-container-statuses": {},
}

// DropInitContainerAnnotations 就地删除 map 中遗留的 alpha/beta init 容器注解。
// 这些注解在 v1.8 已被 Spec.InitContainers 字段取代。
func DropInitContainerAnnotations(annotations map[string]string) {
	for k := range initContainerAnnotations {
		delete(annotations, k)
	}
}

// dropDisabledEvictionResponders 从 pod spec 中移除驱逐响应者配置，旧 spec 已使用时保留。
func dropDisabledEvictionResponders(podSpec, oldPodSpec *api.PodSpec) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.EvictionRequestAPI) && !evictionRespondersInUse(oldPodSpec) {
		podSpec.EvictionResponders = nil
	}
}

// evictionRespondersInUse 判断 pod spec 是否配置了驱逐响应者。
func evictionRespondersInUse(podSpec *api.PodSpec) bool {
	if podSpec == nil {
		return false
	}

	return len(podSpec.EvictionResponders) > 0
}
