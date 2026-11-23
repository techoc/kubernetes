/*
Copyright 2024 The Kubernetes Authors.

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

package resourceclaim

import (
	"iter"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// PodClaims returns an iterator over the names of all ResourceClaims currently
// referenced by or created for the Pod, along with a boolean indicating
// whether IsForPod must be called to verify ownership.
//
// This includes both explicit claims in pod.Spec.ResourceClaims (once created)
// and any DRA-backed extended resource claim in pod.Status.ExtendedResourceClaimStatus.
// Claims that have not been created yet (ErrClaimNotFound), are not needed
// (nil name), or use an unsupported API field (ErrAPIUnsupported) are skipped.
// PodClaims 返回一个迭代器，用于遍历 Pod 当前引用或为其创建的所有 ResourceClaim 名称。
// 迭代产出的布尔值表示取回 ResourceClaim 后是否必须调用 IsForPod 校验所有权。
// 遍历范围包括 pod.Spec.ResourceClaims 中显式声明且已创建的 claim，
// 以及 pod.Status.ExtendedResourceClaimStatus 中 DRA 扩展资源对应的 claim。
// 尚未创建（ErrClaimNotFound）、不再需要（名称为 nil）或使用了不支持的 API 字段
// （ErrAPIUnsupported）的 claim 会被跳过。
func PodClaims(pod *corev1.Pod) iter.Seq2[string, bool] {
	// 迭代器主体：只有调用方开始 range 时才真正执行遍历。
	return func(yield func(string, bool) bool) {
		// Pod 为 nil 时没有可遍历的 claim。
		if pod == nil {
			return
		}
		// 先遍历 spec.resourceClaims 中声明的每一项。
		for i := range pod.Spec.ResourceClaims {
			// 解析 claim 名称以及是否需要校验所有权。
			claimName, mustCheckOwner, err := Name(pod, &pod.Spec.ResourceClaims[i])
			// 解析出错或 claim 不需要创建时跳过。
			if err != nil || claimName == nil {
				// Name only returns an error when a template claim has not been
				// created yet (ErrClaimNotFound) or when neither ResourceClaimName
				// nor ResourceClaimTemplateName is set (ErrAPIUnsupported). In both
				// cases, or when claimName is nil (claim not needed), there is no
				// existing ResourceClaim to yield.
				continue
			}
			// yield 返回 false 表示调用方提前结束遍历，此时立即返回。
			if !yield(*claimName, mustCheckOwner) {
				return
			}
		}
		// 再处理状态中的 DRA 扩展资源 claim，它的所有权必须校验。
		if pod.Status.ExtendedResourceClaimStatus != nil && pod.Status.ExtendedResourceClaimStatus.ResourceClaimName != "" {
			// 名称非空时产出该 claim，并同样尊重调用方的提前终止。
			if !yield(pod.Status.ExtendedResourceClaimStatus.ResourceClaimName, true) {
				return
			}
		}
	}
}

// PodStatusEqual checks that both slices have the same number
// of entries and that the pairs of entries are semantically
// equivalent.
//
// The order of the entries matters: two slices with semantically
// equivalent entries in different order are not equal. This is
// done for the sake of performance because typically the
// order of entries doesn't change.
// PodStatusEqual 判断两个 PodResourceClaimStatus 切片是否语义等价。
// 要求条目数量相同且相同下标处的条目一致；条目顺序参与比较，
// 因为通常状态条目的顺序不会变化，按顺序比较性能更好。
func PodStatusEqual(statusA, statusB []corev1.PodResourceClaimStatus) bool {
	// 长度不同直接判定不等。
	if len(statusA) != len(statusB) {
		return false
	}
	// In most cases, status entries only get added once and not modified.
	// But this cannot be guaranteed, so for the sake of correctness in all
	// cases this code here has to check.
	// 逐项比较名称以及 ResourceClaimName 指针所指向的值。
	for i := range statusA {
		// 名称不同则不等。
		if statusA[i].Name != statusB[i].Name {
			return false
		}
		// 用 ptr.Equal 比较指针内容，nil 与 nil 视为相等。
		if !ptr.Equal(statusA[i].ResourceClaimName, statusB[i].ResourceClaimName) {
			return false
		}
	}
	return true
}

// PodExtendedStatusEqual 判断两个 PodExtendedResourceClaimStatus 是否语义等价。
// 两个 nil 视为相等，仅一侧为 nil 则不等；随后比较 ResourceClaimName
// 与 RequestMappings（顺序敏感，空切片与 nil 视为等价）。
func PodExtendedStatusEqual(statusA, statusB *corev1.PodExtendedResourceClaimStatus) bool {
	// 两侧都为 nil，视为相等。
	if statusA == nil && statusB == nil {
		return true
	}
	// 仅有一侧为 nil，视为不等。
	if (statusA == nil) != (statusB == nil) {
		return false
	}
	// 比较关联的 ResourceClaim 名称。
	if statusA.ResourceClaimName != statusB.ResourceClaimName {
		return false
	}
	// 比较请求映射的数量。
	if len(statusA.RequestMappings) != len(statusB.RequestMappings) {
		return false
	}
	// In most cases, status entries only get added once and not modified.
	// But this cannot be guaranteed, so for the sake of correctness in all
	// cases this code here has to check.
	// slices.Equal 按序比较映射内容，空切片与 nil 的比较结果为相等。
	return slices.Equal(statusA.RequestMappings, statusB.RequestMappings)
}
