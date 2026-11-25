/*
Copyright 2021 The Kubernetes Authors.

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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestResourceClaimIsFor 验证 IsForPod 与 IsForPodGroup 的所有权校验：
// 覆盖 Pod/PodGroup 所有者、非所有者、无所有者、跨命名空间、不同 API 版本
// 以及 acceptPodGroup 开关等场景，并断言精确的错误文本。
func TestResourceClaimIsFor(t *testing.T) {
	// uid 计数器与 newUID 生成稳定且互不相同的 UID。
	uid := 0
	newUID := func() types.UID {
		uid++
		return types.UID(fmt.Sprintf("%d", uid))
	}
	// isController 用于构造控制器类型的所有者引用。
	isController := true

	// podWithPodGroup 返回深拷贝并设置所属 PodGroup 的 Pod。
	podWithPodGroup := func(pod *v1.Pod, podGroupName string) *v1.Pod {
		pod = pod.DeepCopy()
		pod.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
			PodGroupName: &podGroupName,
		}
		return pod
	}

	// 非所有者的 Pod 与 PodGroup。
	podNotOwner := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podNotOwner",
			UID:       newUID(),
		},
	}
	podGroupNotOwner := &schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podGroupNotOwner",
			UID:       newUID(),
		},
	}
	// 作为所有者的 Pod 与 PodGroup。
	podOwner := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podOwner",
			UID:       newUID(),
		},
	}
	podGroupOwner := &schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podGroupOwner",
			UID:       newUID(),
		},
	}
	// 没有任何所有者的 claim。
	claimNoOwner := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimNoOwner",
			UID:       newUID(),
		},
	}
	// 由 Pod 拥有的 claim。
	claimWithOwner := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimWithOwner",
			UID:       newUID(),
			OwnerReferences: []metav1.OwnerReference{
				{
					UID:        podOwner.UID,
					Controller: &isController,
				},
			},
		},
	}
	// 由 PodGroup 拥有的 claim，所有者引用带完整 API 组、类型与名称。
	claimWithPodGroupOwner := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimWithPodGroupOwner",
			UID:       newUID(),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: schedulingapi.SchemeGroupVersion.String(),
					Kind:       "PodGroup",
					Name:       podGroupOwner.Name,
					UID:        podGroupOwner.UID,
					Controller: &isController,
				},
			},
		},
	}
	// 所有者为 PodGroup 但 API 版本不同的 claim，用于验证版本被忽略。
	claimWithOtherVersionPodGroupOwner := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimWithPodGroupOwner",
			UID:       newUID(),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "scheduling.k8s.io/v25",
					Kind:       "PodGroup",
					Name:       podGroupOwner.Name,
					UID:        podGroupOwner.UID,
					Controller: &isController,
				},
			},
		},
	}
	// 位于其他命名空间、由 Pod 拥有的 claim。
	userClaimWithOwner := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "user-namespace",
			Name:      "userClaimWithOwner",
			UID:       newUID(),
			OwnerReferences: []metav1.OwnerReference{
				{
					UID:        podOwner.UID,
					Controller: &isController,
				},
			},
		},
	}
	// 位于其他命名空间、由 PodGroup 拥有的 claim。
	userClaimWithPodGroupOwner := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "user-namespace",
			Name:      "userClaimWithPodGroupOwner",
			UID:       newUID(),
			OwnerReferences: []metav1.OwnerReference{
				{
					UID:        podGroupOwner.UID,
					Controller: &isController,
				},
			},
		},
	}

	// 子测试：IsForPod 的各种场景。
	t.Run("Pod", func(t *testing.T) {
		// expectedError 为空表示期望校验通过，否则期望错误文本精确匹配。
		testcases := map[string]struct {
			pod            *v1.Pod
			claim          *resourceapi.ResourceClaim
			acceptPodGroup bool
			expectedError  string
		}{
			// claim 由该 Pod 拥有，应通过。
			"owned": {
				pod:   podOwner,
				claim: claimWithOwner,
			},
			// claim 归另一个 Pod 所有，应报“Pod is not owner”。
			"other-pod": {
				pod:           podNotOwner,
				claim:         claimWithOwner,
				expectedError: `ResourceClaim kube-system/claimWithOwner was not created for Pod kube-system/podNotOwner (Pod is not owner)`,
			},
			// claim 无所有者，应报错。
			"no-owner": {
				pod:           podOwner,
				claim:         claimNoOwner,
				expectedError: `ResourceClaim kube-system/claimNoOwner was not created for Pod kube-system/podOwner (Pod is not owner)`,
			},
			// 允许 PodGroup 所有者但 Pod 未声明所属组，仍按普通场景报错。
			"accept-podgroup-without-podgroup": {
				pod:            podOwner,
				claim:          claimNoOwner,
				expectedError:  `ResourceClaim kube-system/claimNoOwner was not created for Pod kube-system/podOwner (Pod is not owner)`,
				acceptPodGroup: true,
			},
			// claim 与 Pod 命名空间不同，应报命名空间不一致。
			"different-namespace": {
				pod:           podOwner,
				claim:         userClaimWithOwner,
				expectedError: `ResourceClaim user-namespace/userClaimWithOwner is not in the same namespace as Pod kube-system/podOwner`,
			},
			// Pod 声明了所属组且 claim 由该组拥有，允许时应通过。
			"owned-by-podgroup": {
				pod:            podWithPodGroup(podOwner, podGroupOwner.Name),
				claim:          claimWithPodGroupOwner,
				acceptPodGroup: true,
			},
			// 所有者引用的 API 版本不同但组、类型与名称一致，仍应通过。
			"owned-by-podgroup-other-api-version": {
				pod:            podWithPodGroup(podOwner, podGroupOwner.Name),
				claim:          claimWithOtherVersionPodGroupOwner,
				acceptPodGroup: true,
			},
			// claim 由另一个 PodGroup 拥有，应报错并带上 Pod 声明的组名。
			"other-podgroup": {
				pod:            podWithPodGroup(podOwner, podGroupNotOwner.Name),
				claim:          claimWithPodGroupOwner,
				expectedError:  `ResourceClaim kube-system/claimWithPodGroupOwner was not created for Pod kube-system/podOwner (neither Pod nor PodGroup podGroupNotOwner is the owner)`,
				acceptPodGroup: true,
			},
			// Pod 声明了组但 claim 无所有者，应报错。
			"no-podgroup-owner": {
				pod:            podWithPodGroup(podOwner, podGroupOwner.Name),
				claim:          claimNoOwner,
				expectedError:  `ResourceClaim kube-system/claimNoOwner was not created for Pod kube-system/podOwner (neither Pod nor PodGroup podGroupOwner is the owner)`,
				acceptPodGroup: true,
			},
			// claim 位于其他命名空间，应报命名空间不一致。
			"different-podgroup-namespace": {
				pod:            podWithPodGroup(podOwner, podGroupOwner.Name),
				claim:          userClaimWithPodGroupOwner,
				expectedError:  `ResourceClaim user-namespace/userClaimWithPodGroupOwner is not in the same namespace as Pod kube-system/podOwner`,
				acceptPodGroup: true,
			},
		}

		// 逐个子测试执行校验，按期望断言无错误或精确的错误文本。
		for name, tc := range testcases {
			t.Run(name, func(t *testing.T) {
				err := IsForPod(tc.pod, tc.claim, tc.acceptPodGroup)
				if tc.expectedError == "" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.EqualError(t, err, tc.expectedError)
				}
			})
		}
	})

	// 子测试：IsForPodGroup 的各种场景。
	t.Run("PodGroup", func(t *testing.T) {
		// 同样以 expectedError 是否为空区分通过与失败。
		testcases := map[string]struct {
			pod           *schedulingapi.PodGroup
			claim         *resourceapi.ResourceClaim
			expectedError string
		}{
			// claim 由该 PodGroup 拥有，应通过。
			"owned": {
				pod:   podGroupOwner,
				claim: claimWithPodGroupOwner,
			},
			// claim 归另一个 PodGroup 所有，应报错（用例名沿用 other-pod）。
			"other-pod": {
				pod:           podGroupNotOwner,
				claim:         claimWithPodGroupOwner,
				expectedError: `ResourceClaim kube-system/claimWithPodGroupOwner was not created for PodGroup kube-system/podGroupNotOwner (PodGroup is not owner)`,
			},
			// claim 无所有者，应报错。
			"no-owner": {
				pod:           podGroupOwner,
				claim:         claimNoOwner,
				expectedError: `ResourceClaim kube-system/claimNoOwner was not created for PodGroup kube-system/podGroupOwner (PodGroup is not owner)`,
			},
			// claim 与 PodGroup 命名空间不同，应报错。
			"different-namespace": {
				pod:           podGroupOwner,
				claim:         userClaimWithPodGroupOwner,
				expectedError: `ResourceClaim user-namespace/userClaimWithPodGroupOwner is not in the same namespace as PodGroup kube-system/podGroupOwner`,
			},
		}

		// 执行校验并断言结果。
		for name, tc := range testcases {
			t.Run(name, func(t *testing.T) {
				err := IsForPodGroup(tc.pod, tc.claim)
				if tc.expectedError == "" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.EqualError(t, err, tc.expectedError)
				}
			})
		}
	})
}

// TestIsReservedForPod 验证 IsReservedForPod：覆盖 Pod UID 直接匹配、多个预留对象、
// PodGroup 预留（含特性开关关闭）、Pod 与 PodGroup 混合预留等场景。
func TestIsReservedForPod(t *testing.T) {
	// UID 生成器。
	uid := 0
	newUID := func() types.UID {
		uid++
		return types.UID(fmt.Sprintf("%d", uid))
	}

	// 用于预留判定的 Pod 与 PodGroup 测试对象。
	podNotReserved := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podNotReserved",
			UID:       newUID(),
		},
	}
	podGroupNotReserved := &schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podGroupNotReserved",
			UID:       newUID(),
		},
	}
	podReserved := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podReserved",
			UID:       newUID(),
		},
	}
	podGroupReserved := &schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podGroupReserved",
			UID:       newUID(),
		},
	}
	podOtherReserved := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podOtherReserved",
			UID:       newUID(),
		},
	}
	podGroupOtherReserved := &schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podGroupOtherReserved",
			UID:       newUID(),
		},
	}

	// 没有任何预留的 claim。
	claimNoReservation := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimNoReservation",
			UID:       newUID(),
		},
		Status: resourceapi.ResourceClaimStatus{},
	}

	// 为 podReserved 预留的 claim。
	claimWithReservation := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimWithReservation",
			UID:       newUID(),
		},
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{
					UID: podReserved.UID,
				},
			},
		},
	}

	// 构造带所属 PodGroup 的 Pod。
	podWithPodGroup := func(pod *v1.Pod, podGroupName string) *v1.Pod {
		pod = pod.DeepCopy()
		pod.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
			PodGroupName: &podGroupName,
		}
		return pod
	}

	// 为 podGroupReserved 预留的 claim，包含 API 组、资源类型、名称与 UID。
	claimWithPodGroupReservation := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimWithPodGroupReservation",
			UID:       newUID(),
		},
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{
					APIGroup: schedulingapi.GroupName,
					Resource: "podgroups",
					Name:     podGroupReserved.Name,
					UID:      podGroupReserved.UID,
				},
			},
		},
	}

	// 同时为两个 Pod 预留的 claim。
	claimWithMultipleReservations := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimWithMultipleReservations",
			UID:       newUID(),
		},
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{
					UID: podReserved.UID,
				},
				{
					UID: podOtherReserved.UID,
				},
			},
		},
	}

	// 同时为两个 PodGroup 预留的 claim。
	claimWithMultiplePodGroupReservations := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimWithMultiplePodGroupReservations",
			UID:       newUID(),
		},
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{
					APIGroup: schedulingapi.GroupName,
					Resource: "podgroups",
					Name:     podGroupReserved.Name,
					UID:      podGroupReserved.UID,
				},
				{
					APIGroup: schedulingapi.GroupName,
					Resource: "podgroups",
					Name:     podGroupOtherReserved.Name,
					UID:      podGroupOtherReserved.UID,
				},
			},
		},
	}

	// 同时为 Pod 与 PodGroup 预留的 claim。
	claimWithReservationsForPodAndPodGroup := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "claimWithReservationsForPodAndPodGroup",
			UID:       newUID(),
		},
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{
					UID: podReserved.UID,
				},
				{
					UID: podGroupReserved.UID,
				},
			},
		},
	}

	// acceptPodGroup 控制是否接受 PodGroup 预留，expectedResult 为期望判定结果。
	testcases := map[string]struct {
		pod            *v1.Pod
		claim          *resourceapi.ResourceClaim
		acceptPodGroup bool
		expectedResult bool
	}{
		// 无预留且 Pod 未被预留，期望 false。
		"not-reserved": {
			pod:            podNotReserved,
			claim:          claimNoReservation,
			expectedResult: false,
		},
		// UID 直接命中，期望 true。
		"reserved-for-pod": {
			pod:            podReserved,
			claim:          claimWithReservation,
			expectedResult: true,
		},
		// 预留的是其他 Pod，期望 false。
		"reserved-for-other-pod": {
			pod:            podNotReserved,
			claim:          claimWithReservation,
			expectedResult: false,
		},
		// 多个预留中包含该 Pod，期望 true。
		"multiple-reservations-including-pod": {
			pod:            podReserved,
			claim:          claimWithMultipleReservations,
			expectedResult: true,
		},
		// 多个预留中不包含该 Pod，期望 false。
		"multiple-reservations-excluding-pod": {
			pod:            podNotReserved,
			claim:          claimWithMultipleReservations,
			expectedResult: false,
		},
		// Pod 属于被预留的 PodGroup 且开关开启，期望 true。
		"reserved-for-podgroup": {
			pod:            podWithPodGroup(podNotReserved, podGroupReserved.Name),
			claim:          claimWithPodGroupReservation,
			acceptPodGroup: true,
			expectedResult: true,
		},
		// 同样的 PodGroup 预留但开关关闭，期望 false。
		"podgroup-disabled": {
			pod:            podWithPodGroup(podNotReserved, podGroupReserved.Name),
			claim:          claimWithPodGroupReservation,
			acceptPodGroup: false,
			expectedResult: false,
		},
		// Pod 自身或所属 PodGroup 任一命中即可，期望 true。
		"reserved-for-pod-and-podgroup": {
			pod:            podWithPodGroup(podReserved, podGroupReserved.Name),
			claim:          claimWithReservationsForPodAndPodGroup,
			acceptPodGroup: true,
			expectedResult: true,
		},
		// 预留的是其他 PodGroup，期望 false。
		"reserved-for-other-podgroup": {
			pod:            podWithPodGroup(podNotReserved, podGroupNotReserved.Name),
			claim:          claimWithReservation,
			acceptPodGroup: true,
			expectedResult: false,
		},
		// 多个 PodGroup 预留中包含所属组，期望 true。
		"multiple-reservations-including-podgroup": {
			pod:            podWithPodGroup(podNotReserved, podGroupReserved.Name),
			claim:          claimWithMultiplePodGroupReservations,
			acceptPodGroup: true,
			expectedResult: true,
		},
		// 多个 PodGroup 预留中不包含所属组，期望 false。
		"multiple-reservations-excluding-podgroup": {
			pod:            podWithPodGroup(podNotReserved, podGroupNotReserved.Name),
			claim:          claimWithMultiplePodGroupReservations,
			acceptPodGroup: true,
			expectedResult: false,
		},
	}

	// 调用被测函数并断言结果。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			result := IsReservedForPod(tc.pod, tc.claim, tc.acceptPodGroup)
			assert.Equal(t, tc.expectedResult, result)
		})
	}
}

// TestName 验证 Name 的三类返回：直接引用返回名称且不校验所有权；
// 模板生成在状态中找到时返回名称并要求校验所有权；
// 状态缺失或字段未设置时返回包装了预定义错误的错误。
func TestName(t *testing.T) {
	// expectedName 为期望名称，expectedCheck 为是否需要校验所有权，expectedError 为期望错误。
	testcases := map[string]struct {
		pod           *v1.Pod
		podClaim      *v1.PodResourceClaim
		expectedName  *string
		expectedCheck bool
		expectedError error
	}{
		// 直接引用：返回给定名称，无需所有权校验。
		"resource-claim-name-set": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
			},
			podClaim: &v1.PodResourceClaim{
				ResourceClaimName: func() *string { s := "existing-claim"; return &s }(),
			},
			expectedName:  func() *string { s := "existing-claim"; return &s }(),
			expectedCheck: false,
			expectedError: nil,
		},
		// 模板生成且状态中有同名条目：返回状态中的名称，需校验所有权。
		"resource-claim-template-name-set-and-status-found": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
				Status: v1.PodStatus{
					ResourceClaimStatuses: []v1.PodResourceClaimStatus{
						{
							Name:              "template-claim",
							ResourceClaimName: func() *string { s := "created-claim"; return &s }(),
						},
					},
				},
			},
			podClaim: &v1.PodResourceClaim{
				Name:                      "template-claim",
				ResourceClaimTemplateName: func() *string { s := "template-claim-template"; return &s }(),
			},
			expectedName:  func() *string { s := "created-claim"; return &s }(),
			expectedCheck: true,
			expectedError: nil,
		},
		// 状态中缺少对应条目：返回包装 ErrClaimNotFound 的错误。
		"resource-claim-template-name-set-but-status-not-found": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
				Status: v1.PodStatus{
					ResourceClaimStatuses: []v1.PodResourceClaimStatus{
						{
							Name:              "other-claim",
							ResourceClaimName: func() *string { s := "other-created-claim"; return &s }(),
						},
					},
				},
			},
			podClaim: &v1.PodResourceClaim{
				Name:                      "template-claim",
				ResourceClaimTemplateName: func() *string { s := "template-claim-template"; return &s }(),
			},
			expectedName:  nil,
			expectedCheck: false,
			expectedError: fmt.Errorf(`pod "default/test-pod": %w`, ErrClaimNotFound),
		},
		// 两个引用字段都未设置：返回包装 ErrAPIUnsupported 的错误。
		"neither-resource-claim-name-nor-template-name-set": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
			},
			podClaim: &v1.PodResourceClaim{
				Name: "invalid-claim",
			},
			expectedName:  nil,
			expectedCheck: false,
			expectedError: fmt.Errorf(`pod "default/test-pod", spec.resourceClaim "invalid-claim": %w`, ErrAPIUnsupported),
		},
	}

	// 无错误时断言名称与校验标记；有错误时断言错误文本。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			name, check, err := Name(tc.pod, tc.podClaim)
			if tc.expectedError == nil {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedName, name)
				assert.Equal(t, tc.expectedCheck, check)
			} else {
				require.EqualError(t, err, tc.expectedError.Error())
			}
		})
	}
}

// TestNameFromPodGroup 验证 NameFromPodGroup 的三类返回，语义与 TestName 一致，
// 错误中改为包含 PodGroup 信息。
func TestNameFromPodGroup(t *testing.T) {
	// 用例字段含义与 TestName 相同。
	testcases := map[string]struct {
		podGroup      *schedulingapi.PodGroup
		podGroupClaim *schedulingapi.PodGroupResourceClaim
		expectedName  *string
		expectedCheck bool
		expectedError error
	}{
		// 直接引用：返回名称，无需校验所有权。
		"resource-claim-name-set": {
			podGroup: &schedulingapi.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-podgroup",
				},
			},
			podGroupClaim: &schedulingapi.PodGroupResourceClaim{
				ResourceClaimName: new("existing-claim"),
			},
			expectedName:  new("existing-claim"),
			expectedCheck: false,
			expectedError: nil,
		},
		// 模板生成且状态命中：返回名称并要求校验所有权。
		"resource-claim-template-name-set-and-status-found": {
			podGroup: &schedulingapi.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-podgroup",
				},
				Status: schedulingapi.PodGroupStatus{
					ResourceClaimStatuses: []schedulingapi.PodGroupResourceClaimStatus{
						{
							Name:              "template-claim",
							ResourceClaimName: new("created-claim"),
						},
					},
				},
			},
			podGroupClaim: &schedulingapi.PodGroupResourceClaim{
				Name:                      "template-claim",
				ResourceClaimTemplateName: new("template-claim-template"),
			},
			expectedName:  new("created-claim"),
			expectedCheck: true,
			expectedError: nil,
		},
		// 状态中缺少条目：返回包装 ErrClaimNotFound 的错误。
		"resource-claim-template-name-set-but-status-not-found": {
			podGroup: &schedulingapi.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-podgroup",
				},
				Status: schedulingapi.PodGroupStatus{
					ResourceClaimStatuses: []schedulingapi.PodGroupResourceClaimStatus{
						{
							Name:              "other-claim",
							ResourceClaimName: new("other-created-claim"),
						},
					},
				},
			},
			podGroupClaim: &schedulingapi.PodGroupResourceClaim{
				Name:                      "template-claim",
				ResourceClaimTemplateName: new("template-claim-template"),
			},
			expectedName:  nil,
			expectedCheck: false,
			expectedError: fmt.Errorf("PodGroup default/test-podgroup: %w", ErrClaimNotFound),
		},
		// 两个引用字段都未设置：返回包装 ErrAPIUnsupported 的错误。
		"neither-resource-claim-name-nor-template-name-set": {
			podGroup: &schedulingapi.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-podgroup",
				},
			},
			podGroupClaim: &schedulingapi.PodGroupResourceClaim{
				Name: "invalid-claim",
			},
			expectedName:  nil,
			expectedCheck: false,
			expectedError: fmt.Errorf("PodGroup default/test-podgroup, spec.resourceClaim invalid-claim: %w", ErrAPIUnsupported),
		},
	}

	// 断言名称、校验标记或错误文本。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			name, check, err := NameFromPodGroup(tc.podGroup, tc.podGroupClaim)
			if tc.expectedError == nil {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedName, name)
				assert.Equal(t, tc.expectedCheck, check)
			} else {
				require.EqualError(t, err, tc.expectedError.Error())
			}
		})
	}
}

// TestBindTo 验证 BindTo：没有匹配 PodGroup 时绑定到 Pod；
// 当 Pod 的 claim 与 PodGroup 的 claim 在名称及引用字段上完全一致时绑定到 PodGroup；
// 同时覆盖 API 不支持、Pod 与组不匹配及部分字段不匹配的情况。
func TestBindTo(t *testing.T) {
	// 命名空间、Pod 名与 UID 等公共测试数据。
	namespace := "my-namespace"
	podName := "my-pod"
	podUID := types.UID("pod-uid")

	podGroupName := "my-podgroup"
	podGroupUID := types.UID("podgroup-uid")

	claimName := "my-claim"
	resourceClaimName := new("my-resource-claim")

	// 声明属于 my-podgroup 的 Pod。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			SchedulingGroup: &v1.PodSchedulingGroup{
				PodGroupName: &podGroupName,
			},
		},
	}
	// 期望得到的 Pod 消费者引用。
	podReservation := resourceapi.ResourceClaimConsumerReference{
		APIGroup: v1.GroupName,
		Resource: "pods",
		Name:     podName,
		UID:      podUID,
	}

	// 构造带指定 claim 列表的 PodGroup。
	podGroupWithClaims := func(claims ...schedulingapi.PodGroupResourceClaim) *schedulingapi.PodGroup {
		podGroup := &schedulingapi.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      podGroupName,
				UID:       podGroupUID,
			},
			Spec: schedulingapi.PodGroupSpec{
				ResourceClaims: claims,
			},
		}
		return podGroup
	}
	// 期望得到的 PodGroup 消费者引用。
	podGroupReservation := resourceapi.ResourceClaimConsumerReference{
		APIGroup: schedulingapi.GroupName,
		Resource: "podgroups",
		Name:     podGroupName,
		UID:      podGroupUID,
	}

	// expected 为期望的消费者引用，expectedError 为期望错误。
	tests := map[string]struct {
		pod           *v1.Pod
		podGroup      *schedulingapi.PodGroup
		podClaim      *v1.PodResourceClaim
		expected      resourceapi.ResourceClaimConsumerReference
		expectedError error
	}{
		// Pod 的 claim 两个引用字段都未设置，期望 ErrAPIUnsupported。
		"unsupported API in Pod": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name: claimName,
			},
			expectedError: fmt.Errorf("Pod my-namespace/my-pod claim my-claim: %w", ErrAPIUnsupported),
		},
		// 没有匹配 PodGroup（此处为 nil）时绑定到 Pod。
		"Pod claim with ResourceClaimName": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:              claimName,
				ResourceClaimName: resourceClaimName,
			},
			expected: podReservation,
		},
		// 模板引用的 claim 同样绑定到 Pod。
		"Pod claim with ResourceClaimTemplateName": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:                      claimName,
				ResourceClaimTemplateName: resourceClaimName,
			},
			expected: podReservation,
		},
		// 与 PodGroup 的 claim 完全一致，绑定到 PodGroup。
		"Pod claim with ResourceClaimName matching PodGroup": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:              claimName,
				ResourceClaimName: resourceClaimName,
			},
			podGroup: podGroupWithClaims(schedulingapi.PodGroupResourceClaim{
				Name:              claimName,
				ResourceClaimName: resourceClaimName,
			}),
			expected: podGroupReservation,
		},
		// 模板名与 PodGroup claim 一致，绑定到 PodGroup。
		"Pod claim with ResourceClaimTemplateName matching PodGroup": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:                      claimName,
				ResourceClaimTemplateName: resourceClaimName,
			},
			podGroup: podGroupWithClaims(schedulingapi.PodGroupResourceClaim{
				Name:                      claimName,
				ResourceClaimTemplateName: resourceClaimName,
			}),
			expected: podGroupReservation,
		},
		// Pod 所属组与传入 PodGroup 的命名空间不同，应报错。
		"Wrong PodGroup namespace for Pod": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:              claimName,
				ResourceClaimName: resourceClaimName,
			},
			podGroup:      &schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "not-" + namespace, Name: podGroupName}},
			expectedError: fmt.Errorf("Pod my-namespace/my-pod belongs to PodGroup my-namespace/my-podgroup, not PodGroup not-my-namespace/my-podgroup"),
		},
		// Pod 所属组与传入 PodGroup 的名称不同，应报错。
		"Wrong PodGroup name for Pod": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:              claimName,
				ResourceClaimName: resourceClaimName,
			},
			podGroup:      &schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "not-" + podGroupName}},
			expectedError: fmt.Errorf("Pod my-namespace/my-pod belongs to PodGroup my-namespace/my-podgroup, not PodGroup my-namespace/not-my-podgroup"),
		},
		// PodGroup 中没有匹配的 claim，回退绑定到 Pod。
		"No matching claim in PodGroup": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:              claimName,
				ResourceClaimName: resourceClaimName,
			},
			podGroup: podGroupWithClaims(
				schedulingapi.PodGroupResourceClaim{Name: "not-" + claimName, ResourceClaimName: resourceClaimName},
				schedulingapi.PodGroupResourceClaim{Name: "nor-" + claimName, ResourceClaimName: resourceClaimName},
			),
			expected: podReservation,
		},
		// 多条 claim 中有一条完全匹配，绑定到 PodGroup。
		"Find matching claim in PodGroup": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:              claimName,
				ResourceClaimName: resourceClaimName,
			},
			podGroup: podGroupWithClaims(
				schedulingapi.PodGroupResourceClaim{Name: "not-" + claimName, ResourceClaimName: resourceClaimName},
				schedulingapi.PodGroupResourceClaim{Name: "nor-" + claimName, ResourceClaimName: resourceClaimName},
				schedulingapi.PodGroupResourceClaim{Name: claimName, ResourceClaimName: resourceClaimName},
			),
			expected: podGroupReservation,
		},
		// claim 名称不同，不算匹配，绑定到 Pod。
		"Claim name mismatch": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:              claimName,
				ResourceClaimName: resourceClaimName,
			},
			podGroup: podGroupWithClaims(
				schedulingapi.PodGroupResourceClaim{
					Name:              "not-" + claimName,
					ResourceClaimName: resourceClaimName,
				},
			),
			expected: podReservation,
		},
		// ResourceClaimName 不同，不算匹配，绑定到 Pod。
		"Claim resourceClaimName mismatch": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:              claimName,
				ResourceClaimName: resourceClaimName,
			},
			podGroup: podGroupWithClaims(
				schedulingapi.PodGroupResourceClaim{
					Name:              claimName,
					ResourceClaimName: new("not-" + *resourceClaimName),
				},
			),
			expected: podReservation,
		},
		// ResourceClaimTemplateName 不同，不算匹配，绑定到 Pod。
		"Claim resourceClaimTemplateName mismatch": {
			pod: pod,
			podClaim: &v1.PodResourceClaim{
				Name:                      claimName,
				ResourceClaimTemplateName: resourceClaimName,
			},
			podGroup: podGroupWithClaims(
				schedulingapi.PodGroupResourceClaim{
					Name:                      claimName,
					ResourceClaimTemplateName: new("not-" + *resourceClaimName),
				},
			),
			expected: podReservation,
		},
	}

	// 执行 BindTo 并断言消费者引用或错误文本。
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := BindTo(test.pod, test.podGroup, test.podClaim)
			if test.expectedError == nil {
				require.NoError(t, err)
				assert.Equal(t, test.expected, actual)
			} else {
				assert.EqualError(t, err, test.expectedError.Error())
			}
		})
	}
}

// TestBaseRequestRef 验证 BaseRequestRef：顶级请求原样返回，子请求返回斜杠前的父请求名。
func TestBaseRequestRef(t *testing.T) {
	testcases := map[string]struct {
		requestRef             string
		expectedBaseRequestRef string
	}{
		// 输入 foo 无子请求，期望返回自身。
		"valid-no-subrequest": {
			requestRef:             "foo",
			expectedBaseRequestRef: "foo",
		},
		// 输入 foo/bar，期望返回父请求 foo。
		"valid-subrequest": {
			requestRef:             "foo/bar",
			expectedBaseRequestRef: "foo",
		},
	}

	// 断言提取的基础请求名。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			baseRequestRef := BaseRequestRef(tc.requestRef)
			assert.Equal(t, tc.expectedBaseRequestRef, baseRequestRef)
		})
	}
}

// TestConfigForResult 验证 ConfigForResult 的过滤规则：只保留 opaque 非空、
// 且 Requests 为空（匹配所有）、直接命中请求名或命中父请求名的配置；
// 同时验证不同来源的配置都能返回，而不匹配的配置被排除。
func TestConfigForResult(t *testing.T) {
	// deviceConfigurations 为候选配置，result 为分配结果，expectedConfigs 为期望保留的配置。
	testcases := map[string]struct {
		deviceConfigurations []resourceapi.DeviceAllocationConfiguration
		result               resourceapi.DeviceRequestAllocationResult
		expectedConfigs      []resourceapi.DeviceAllocationConfiguration
	}{
		// opaque 为 nil 的配置应被过滤掉，期望结果为 nil。
		"opaque-nil": {
			deviceConfigurations: []resourceapi.DeviceAllocationConfiguration{
				{
					Source:   resourceapi.AllocationConfigSourceClass,
					Requests: []string{},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: nil,
					},
				},
			},
			result: resourceapi.DeviceRequestAllocationResult{
				Request: "foo",
				Device:  "device-1",
			},
			expectedConfigs: nil,
		},
		// Requests 为空表示匹配所有请求，配置应被保留。
		"empty-requests-match-all": {
			deviceConfigurations: []resourceapi.DeviceAllocationConfiguration{
				{
					Source:   resourceapi.AllocationConfigSourceClass,
					Requests: []string{},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
			result: resourceapi.DeviceRequestAllocationResult{
				Request: "foo",
				Device:  "device-1",
			},
			expectedConfigs: []resourceapi.DeviceAllocationConfiguration{
				{
					Source:   resourceapi.AllocationConfigSourceClass,
					Requests: []string{},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
		},
		// Requests 包含完整请求名时匹配，配置应被保留。
		"match-regular-request": {
			deviceConfigurations: []resourceapi.DeviceAllocationConfiguration{
				{
					Source: resourceapi.AllocationConfigSourceClass,
					Requests: []string{
						"foo",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
			result: resourceapi.DeviceRequestAllocationResult{
				Request: "foo",
				Device:  "device-1",
			},
			expectedConfigs: []resourceapi.DeviceAllocationConfiguration{
				{
					Source: resourceapi.AllocationConfigSourceClass,
					Requests: []string{
						"foo",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
		},
		// 子请求 foo/bar 可由父请求名 foo 对应的配置匹配。
		"match-parent-request-for-subrequest": {
			deviceConfigurations: []resourceapi.DeviceAllocationConfiguration{
				{
					Source: resourceapi.AllocationConfigSourceClass,
					Requests: []string{
						"foo",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
			result: resourceapi.DeviceRequestAllocationResult{
				Request: "foo/bar",
				Device:  "device-1",
			},
			expectedConfigs: []resourceapi.DeviceAllocationConfiguration{
				{
					Source: resourceapi.AllocationConfigSourceClass,
					Requests: []string{
						"foo",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
		},
		// 完整子请求名 foo/bar 的配置匹配；foo/not-bar 的配置不匹配，被排除。
		"match-subrequest": {
			deviceConfigurations: []resourceapi.DeviceAllocationConfiguration{
				{
					Source: resourceapi.AllocationConfigSourceClass,
					Requests: []string{
						"foo/bar",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
				{
					Source: resourceapi.AllocationConfigSourceClass,
					Requests: []string{
						"foo/not-bar",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
			result: resourceapi.DeviceRequestAllocationResult{
				Request: "foo/bar",
				Device:  "device-1",
			},
			expectedConfigs: []resourceapi.DeviceAllocationConfiguration{
				{
					Source: resourceapi.AllocationConfigSourceClass,
					Requests: []string{
						"foo/bar",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
		},
		// 类来源与 claim 来源的两条配置均匹配，应按原顺序全部返回。
		"match-both-source-class-and-claim": {
			deviceConfigurations: []resourceapi.DeviceAllocationConfiguration{
				{
					Source: resourceapi.AllocationConfigSourceClass,
					Requests: []string{
						"foo",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
				{
					Source: resourceapi.AllocationConfigSourceClaim,
					Requests: []string{
						"foo/bar",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
			result: resourceapi.DeviceRequestAllocationResult{
				Request: "foo/bar",
				Device:  "device-1",
			},
			expectedConfigs: []resourceapi.DeviceAllocationConfiguration{
				{
					Source: resourceapi.AllocationConfigSourceClass,
					Requests: []string{
						"foo",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
				{
					Source: resourceapi.AllocationConfigSourceClaim,
					Requests: []string{
						"foo/bar",
					},
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{
							Driver: "driver-a",
						},
					},
				},
			},
		},
	}

	// 调用 ConfigForResult 并断言过滤结果。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			configs := ConfigForResult(tc.deviceConfigurations, tc.result)
			assert.Equal(t, tc.expectedConfigs, configs)
		})
	}
}
