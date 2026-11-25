/*
Copyright 2022 The Kubernetes Authors.

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

// Package resourceclaim 提供访问 PodResourceClaim 所引用 ResourceClaim 的常规流程支持：
// 根据 PodResourceClaim 确定 ResourceClaim 名称、取回该对象、
// 在由模板生成时校验其是否属于该 Pod，最后使用该 ResourceClaim。
package resourceclaim

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// schedulingAPIGroupPrefix 是调度 API 组名称加上斜杠的前缀，
// 用于在校验 PodGroup 所有者时忽略 API 版本部分。
const schedulingAPIGroupPrefix = schedulingapi.GroupName + "/"

var (
	// ErrAPIUnsupported 表示所有受支持的字段都未设置（输入非法或 API 已扩展），
	// 由 Name 与 NameFromPodGroup 包装进实际返回的错误中。
	ErrAPIUnsupported = errors.New("none of the supported fields are set")

	// ErrClaimNotFound 表示模板对应的 claim 尚未创建，
	// 由 Name 与 NameFromPodGroup 包装进实际返回的错误中。
	ErrClaimNotFound = errors.New("ResourceClaim not created yet")
)

// Name 根据 PodResourceClaim 解析 ResourceClaim 名称，共有三种结果。
// 一是返回错误：字段未按预期设置（输入非法，或 API 扩展后库与客户端需要更新），
// 或模板对应的 claim 尚未创建；错误中包含 Pod 与 claim 名称以及异常字段，
// 并包装本包预定义错误。
// 二是返回 nil 名称且无错误：claim 有意不创建，该 PodResourceClaim 可忽略。
// 三是返回名称且无错误：布尔值指示取回 claim 后、使用前是否必须调用 IsForPod 校验归属。
// 名称的确定依赖 Kubernetes >= 1.28 的 API 字段。
func Name(pod *v1.Pod, podClaim *v1.PodResourceClaim) (name *string, mustCheckOwner bool, err error) {
	// 按 claim 的引用方式分派。
	switch {
	// 直接引用已有 claim：返回名称，无需校验所有权。
	case podClaim.ResourceClaimName != nil:
		return podClaim.ResourceClaimName, false, nil
	// 模板生成的 claim：从 Pod 状态中查找已创建的对应条目。
	case podClaim.ResourceClaimTemplateName != nil:
		// 按 PodResourceClaim 名称匹配状态条目。
		for _, status := range pod.Status.ResourceClaimStatuses {
			if status.Name == podClaim.Name {
				// 命中后返回状态中的名称（可能为 nil，表示不需要 claim），并标记需要校验所有权。
				return status.ResourceClaimName, true, nil
			}
		}
		// 状态中找不到对应条目，说明模板 claim 尚未创建。
		return nil, false, fmt.Errorf(`pod "%s/%s": %w`, pod.Namespace, pod.Name, ErrClaimNotFound)
	default:
		// 两个字段都未设置：输入使用了不支持的 API 字段。
		return nil, false, fmt.Errorf(`pod "%s/%s", spec.resourceClaim %q: %w`, pod.Namespace, pod.Name, podClaim.Name, ErrAPIUnsupported)
	}
}

// NameFromPodGroup 根据 PodGroupResourceClaim 解析 ResourceClaim 名称，语义与 Name 一致。
// 出错时错误信息中包含 PodGroup 与 claim 名称，并包装本包预定义错误；
// 返回 nil 名称表示 claim 有意不创建；返回名称时布尔值指示是否需要调用 IsForPodGroup 校验归属。
func NameFromPodGroup(podGroup *schedulingapi.PodGroup, podGroupClaim *schedulingapi.PodGroupResourceClaim) (name *string, mustCheckOwner bool, err error) {
	// 与 Name 相同的三路分派：直接引用、模板生成、字段缺失。
	switch {
	// 直接引用：返回名称，无需校验所有权。
	case podGroupClaim.ResourceClaimName != nil:
		return podGroupClaim.ResourceClaimName, false, nil
	// 模板生成：在 PodGroup 状态中查找。
	case podGroupClaim.ResourceClaimTemplateName != nil:
		// 按 claim 名称匹配状态条目。
		for _, status := range podGroup.Status.ResourceClaimStatuses {
			if status.Name == podGroupClaim.Name {
				// 命中后返回名称并标记需要校验所有权。
				return status.ResourceClaimName, true, nil
			}
		}
		// 状态中无对应条目，说明 claim 尚未创建。
		return nil, false, fmt.Errorf("PodGroup %s/%s: %w", podGroup.Namespace, podGroup.Name, ErrClaimNotFound)
	default:
		// 两个字段都未设置：不受支持的 API 用法。
		return nil, false, fmt.Errorf("PodGroup %s/%s, spec.resourceClaim %s: %w", podGroup.Namespace, podGroup.Name, podGroupClaim.Name, ErrAPIUnsupported)
	}
}

// IsForPod 校验 ResourceClaim 是否为该 Pod 创建；当 acceptPodGroupOwner 为 true 时，
// 也接受由该 Pod 所属 PodGroup 创建的情况。acceptPodGroupOwner 仅在
// DRAWorkloadResourceClaims 特性门控开启时才应设为 true。
// 调用方不应传入其他命名空间的 claim；此处的命名空间检查仅作为防御性兜底。
// 返回的错误信息足够完整，调用方可直接透传而无需补充 Pod 或 claim 细节。
func IsForPod(pod *v1.Pod, claim *resourceapi.ResourceClaim, acceptPodGroupOwner bool) error {
	// 命名空间不一致时直接拒绝。
	if claim.Namespace != pod.Namespace {
		return fmt.Errorf("ResourceClaim %s/%s is not in the same namespace as Pod %s/%s", claim.Namespace, claim.Name, pod.Namespace, pod.Name)
	}
	// 先检查 Pod 是否为 claim 的控制器所有者。
	if !metav1.IsControlledBy(claim, pod) {
		// 不是 Pod 所有时，若允许 PodGroup 所有者且 Pod 声明了所属 PodGroup，则继续校验 PodGroup。
		if acceptPodGroupOwner && pod.Spec.SchedulingGroup != nil && pod.Spec.SchedulingGroup.PodGroupName != nil {
			// 通过 Pod 可见的 API 组、类型与名称校验 PodGroup 归属（Pod 不知道其 UID）。
			if !isForPodGroupByPod(pod, claim) {
				// 既不是 Pod 也不是对应 PodGroup 所有，返回带 PodGroup 名称的错误。
				return fmt.Errorf("ResourceClaim %s/%s was not created for Pod %s/%s (neither Pod nor PodGroup %s is the owner)", claim.Namespace, claim.Name, pod.Namespace, pod.Name, *pod.Spec.SchedulingGroup.PodGroupName)
			}
			// 由 Pod 所属 PodGroup 所有，校验通过。
			return nil
		}
		// 不允许 PodGroup 所有者或 Pod 未声明所属组：Pod 不是所有者，拒绝。
		return fmt.Errorf("ResourceClaim %s/%s was not created for Pod %s/%s (Pod is not owner)", claim.Namespace, claim.Name, pod.Namespace, pod.Name)
	}
	// Pod 是所有者，校验通过。
	return nil
}

// IsForPodGroup 校验 ResourceClaim 是否为指定 PodGroup 创建。
// 调用方不应传入其他命名空间的 claim；此处的命名空间检查仅作为防御性兜底。
// 返回的错误信息足够完整，调用方可直接透传而无需补充细节。
func IsForPodGroup(podGroup *schedulingapi.PodGroup, claim *resourceapi.ResourceClaim) error {
	// 命名空间不一致时直接拒绝。
	if claim.Namespace != podGroup.Namespace {
		return fmt.Errorf("ResourceClaim %s/%s is not in the same namespace as PodGroup %s/%s", claim.Namespace, claim.Name, podGroup.Namespace, podGroup.Name)
	}
	// 校验 PodGroup 是否为 claim 的控制器所有者。
	if !metav1.IsControlledBy(claim, podGroup) {
		// 非 PodGroup 所有，返回错误。
		return fmt.Errorf("ResourceClaim %s/%s was not created for PodGroup %s/%s (PodGroup is not owner)", claim.Namespace, claim.Name, podGroup.Namespace, podGroup.Name)
	}
	// 校验通过。
	return nil
}

// isForPodGroupByPod 的功能与 IsForPodGroup 类似，但它通过 claim 所有者引用中的
// API 组、类型与名称来校验 PodGroup，而不是比较 UID——Pod 并不知道其 PodGroup 的 UID。
// API 组只检查组名、忽略版本，因为版本可能随升级变化。
func isForPodGroupByPod(pod *v1.Pod, claim *resourceapi.ResourceClaim) bool {
	// Pod 未声明所属 PodGroup，无法匹配。
	if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
		return false
	}
	// 取 claim 的控制器所有者引用。
	controller := metav1.GetControllerOfNoCopy(claim)
	// 所有者存在、名称与 Pod 声明一致、类型为 PodGroup 且 API 组前缀匹配时才返回 true。
	return controller != nil &&
		controller.Name == *pod.Spec.SchedulingGroup.PodGroupName &&
		controller.Kind == "PodGroup" &&
		strings.HasPrefix(controller.APIVersion, schedulingAPIGroupPrefix)
}

// IsReservedForPod 判断 claim 是否把该 Pod 列为预留对象之一。
// 当 acceptPodGroupReservation 为 true 时，若预留对象是 Pod 所属的 PodGroup 也返回 true。
// 该参数仅在 DRAWorkloadResourceClaims 特性开启时才应设为 true。
func IsReservedForPod(pod *v1.Pod, claim *resourceapi.ResourceClaim, acceptPodGroupReservation bool) bool {
	// 遍历 claim 的所有预留对象。
	for _, reserved := range claim.Status.ReservedFor {
		// UID 相同即表示为该 Pod 预留。
		if reserved.UID == pod.UID {
			return true
		}
		// 允许 PodGroup 预留时，校验 API 组、资源类型以及 Pod 所声明的组名。
		if acceptPodGroupReservation && reserved.APIGroup == schedulingapi.GroupName &&
			reserved.Resource == "podgroups" &&
			pod.Spec.SchedulingGroup != nil &&
			pod.Spec.SchedulingGroup.PodGroupName != nil &&
			reserved.Name == *pod.Spec.SchedulingGroup.PodGroupName {
			return true
		}
	}
	// 没有匹配项，返回 false。
	return false
}

// CanBeReserved 判断 claim 是否还能被其他对象预留。
// 当前对共享没有任何限制，始终返回 true。
func CanBeReserved(claim *resourceapi.ResourceClaim) bool {
	return true
}

// BindTo 构造 claim 的消费者引用，指向应绑定 claim 的对象。
// 当 PodResourceClaim 的名称、ResourceClaimName 和 ResourceClaimTemplateName
// 与 PodGroup 的某条 PodGroupResourceClaim 完全一致时绑定到 PodGroup，否则绑定到 Pod。
// podGroup 仅在 DRAWorkloadResourceClaims 特性门控开启时才应传入非 nil。
func BindTo(pod *v1.Pod, podGroup *schedulingapi.PodGroup, podClaim *v1.PodResourceClaim) (resourceapi.ResourceClaimConsumerReference, error) {
	// Pod 的 claim 两个引用字段都未设置：不受支持的 API 用法。
	if podClaim.ResourceClaimName == nil &&
		podClaim.ResourceClaimTemplateName == nil {
		return resourceapi.ResourceClaimConsumerReference{}, fmt.Errorf("Pod %s/%s claim %s: %w", pod.Namespace, pod.Name, podClaim.Name, ErrAPIUnsupported)
	}
	// 只有提供了 PodGroup 时才尝试匹配其中的 claim。
	if podGroup != nil {
		// 先确认 Pod 确实属于传入的 PodGroup。
		if err := podMatchesPodGroup(pod, podGroup); err != nil {
			return resourceapi.ResourceClaimConsumerReference{}, err
		}
		// 遍历 PodGroup 声明的 claim 寻找完全匹配项。
		for _, podGroupClaim := range podGroup.Spec.ResourceClaims {
			// PodGroup 的 claim 两个引用字段都未设置：不受支持。
			if podGroupClaim.ResourceClaimName == nil &&
				podGroupClaim.ResourceClaimTemplateName == nil {
				return resourceapi.ResourceClaimConsumerReference{}, fmt.Errorf("PodGroup %s/%s claim %s: %w", podGroup.Namespace, podGroup.Name, podGroupClaim.Name, ErrAPIUnsupported)
			}
			// 名称与两个引用字段均相等时视为匹配。
			if podGroupClaim.Name == podClaim.Name &&
				ptr.Equal(podGroupClaim.ResourceClaimName, podClaim.ResourceClaimName) &&
				ptr.Equal(podGroupClaim.ResourceClaimTemplateName, podClaim.ResourceClaimTemplateName) {
				// 匹配成功：消费者引用指向 PodGroup。
				return resourceapi.ResourceClaimConsumerReference{
					APIGroup: schedulingapi.GroupName,
					Resource: "podgroups",
					Name:     podGroup.Name,
					UID:      podGroup.UID,
				}, nil
			}
		}
	}
	// 没有匹配到 PodGroup 的 claim（或未提供 PodGroup）：消费者引用指向 Pod。
	return resourceapi.ResourceClaimConsumerReference{
		APIGroup: v1.GroupName,
		Resource: "pods",
		Name:     pod.Name,
		UID:      pod.UID,
	}, nil
}

// BaseRequestRef 返回请求引用的基础名称：顶级请求返回自身，子请求返回父请求名。
func BaseRequestRef(requestRef string) string {
	// 按 '/' 切分，第一段即为父请求名或请求自身。
	segments := strings.Split(requestRef, "/")
	return segments[0]
}

// CreateSubRequestRef 将请求名与子请求名用 '/' 拼接成子请求引用。
func CreateSubRequestRef(requestName, subRequestName string) string {
	return fmt.Sprintf("%s/%s", requestName, subRequestName)
}

// IsSubRequestRef 判断引用是否指向子请求，即由 '/' 分隔的两段式名称。
func IsSubRequestRef(requestRef string) bool {
	// 切分后恰好为两段才是子请求引用。
	segments := strings.Split(requestRef, "/")
	return len(segments) == 2
}

// ConfigForResult 返回适用于给定设备分配结果的设备分配配置。
// 只保留带有 opaque 配置，且其 Requests 列表匹配结果请求或父请求的配置。
func ConfigForResult(deviceConfigurations []resourceapi.DeviceAllocationConfiguration, result resourceapi.DeviceRequestAllocationResult) []resourceapi.DeviceAllocationConfiguration {
	// 收集匹配的配置，保持原有顺序。
	var configs []resourceapi.DeviceAllocationConfiguration
	// 遍历所有设备分配配置。
	for _, deviceConfiguration := range deviceConfigurations {
		// 必须带有 opaque 配置，且请求列表与分配结果匹配。
		if deviceConfiguration.Opaque != nil &&
			isMatch(deviceConfiguration.Requests, result.Request) {
			// 记录匹配的配置。
			configs = append(configs, deviceConfiguration)
		}
	}
	return configs
}

// isMatch 判断配置的 Requests 列表是否包含给定的请求引用。
// 空列表表示匹配所有请求；此外也允许用父请求名匹配子请求。
func isMatch(requests []string, requestRef string) bool {
	// 空列表不限制请求，直接匹配。
	if len(requests) == 0 {
		return true
	}

	// 完整引用（如 foo/bar）在列表中时匹配。
	if slices.Contains(requests, requestRef) {
		return true
	}

	// 未直接命中时，取父请求名再匹配一次（如用 foo 匹配 foo/bar）。
	baseRequestRef := BaseRequestRef(requestRef)
	return slices.Contains(requests, baseRequestRef)
}

// podMatchesPodGroup 校验 Pod 是否属于给定的 PodGroup。
// 未声明所属组或声明的组与参数不一致时返回带上下文的错误。
func podMatchesPodGroup(pod *v1.Pod, podGroup *schedulingapi.PodGroup) error {
	// Pod 未声明所属 PodGroup，直接报错。
	if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
		return fmt.Errorf("Pod %s/%s does not belong to a PodGroup", pod.Namespace, pod.Name)
	}
	// 命名空间或组名不一致，报错并给出双方信息。
	if schedGroupPodGroup := *pod.Spec.SchedulingGroup.PodGroupName; pod.Namespace != podGroup.Namespace || schedGroupPodGroup != podGroup.Name {
		return fmt.Errorf("Pod %s/%s belongs to PodGroup %s/%s, not PodGroup %s/%s", pod.Namespace, pod.Name, pod.Namespace, schedGroupPodGroup, podGroup.Namespace, podGroup.Name)
	}
	// 校验通过。
	return nil
}
