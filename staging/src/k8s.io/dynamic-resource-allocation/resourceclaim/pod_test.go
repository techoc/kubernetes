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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// TestPodStatusEqual 验证 PodStatusEqual：切片与自身比较恒为 true，
// 并覆盖顺序差异、长度差异、ResourceClaimName 差异以及空切片与 nil 等价等场景。
func TestPodStatusEqual(t *testing.T) {
	// a、ay、b、empty 为可复用条目：ay 与 a 的 claim 名不同，b 的名称不同，empty 为零值条目。
	a := corev1.PodResourceClaimStatus{
		Name:              "a",
		ResourceClaimName: ptr.To("x"),
	}
	ay := corev1.PodResourceClaimStatus{
		Name:              "a",
		ResourceClaimName: ptr.To("y"),
	}
	b := corev1.PodResourceClaimStatus{
		Name:              "b",
		ResourceClaimName: ptr.To("y"),
	}
	empty := corev1.PodResourceClaimStatus{}

	// 表驱动用例：sliceA/sliceB 为待比较切片，expectEqual 为期望结果。
	testcases := map[string]struct {
		sliceA, sliceB []corev1.PodResourceClaimStatus
		expectEqual    bool
	}{
		// 内容完全相同的切片（含零值条目）应相等。
		"identical": {
			sliceA:      []corev1.PodResourceClaimStatus{a, b, empty},
			sliceB:      []corev1.PodResourceClaimStatus{a, b, empty},
			expectEqual: true,
		},
		// 条目相同但顺序不同，应判定不等。
		"different-order": {
			sliceA:      []corev1.PodResourceClaimStatus{a, b, empty},
			sliceB:      []corev1.PodResourceClaimStatus{empty, a, b},
			expectEqual: false,
		},
		// 长度不同，应判定不等。
		"different-length": {
			sliceA:      []corev1.PodResourceClaimStatus{a, b, empty},
			sliceB:      []corev1.PodResourceClaimStatus{a, b},
			expectEqual: false,
		},
		// 名称相同但 ResourceClaimName 指向的值不同，应判定不等。
		"different-resource-claim-name": {
			sliceA:      []corev1.PodResourceClaimStatus{a},
			sliceB:      []corev1.PodResourceClaimStatus{ay},
			expectEqual: false,
		},
		// 空切片与 nil 长度均为零，应视为等价。
		"semantically-equal-empty": {
			sliceA:      []corev1.PodResourceClaimStatus{},
			sliceB:      nil,
			expectEqual: true,
		},
		// 逐字段等价的两个条目应相等。
		"semantically-equivalent": {
			sliceA:      []corev1.PodResourceClaimStatus{{Name: "a", ResourceClaimName: ptr.To("x")}},
			sliceB:      []corev1.PodResourceClaimStatus{{Name: "a", ResourceClaimName: ptr.To("x")}},
			expectEqual: true,
		},
	}

	// 每个用例先验证自反性，再验证两个切片的比较结果。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// 自反性：切片与自身比较必须为 true。
			assert.True(t, PodStatusEqual(tc.sliceA, tc.sliceA), fmt.Sprintf("%v", tc.sliceA))
			assert.True(t, PodStatusEqual(tc.sliceB, tc.sliceB), fmt.Sprintf("%v", tc.sliceB))
			// 断言跨切片比较结果等于期望值。
			assert.Equal(t, tc.expectEqual, PodStatusEqual(tc.sliceA, tc.sliceB), fmt.Sprintf("%v and %v", tc.sliceA, tc.sliceB))
		})

	}
}

// TestPodExtendedStatusEqual 验证 PodExtendedStatusEqual：
// 覆盖 nil、顺序、长度、claim 名差异以及空映射切片与 nil 等价等场景。
func TestPodExtendedStatusEqual(t *testing.T) {
	// ra、rb 为两条请求映射，容器名、资源名与请求名均不同。
	ra := corev1.ContainerExtendedResourceRequest{
		ContainerName: "a",
		ResourceName:  "example.com/a",
		RequestName:   "ra",
	}
	rb := corev1.ContainerExtendedResourceRequest{
		ContainerName: "b",
		ResourceName:  "example.com/b",
		RequestName:   "rb",
	}

	// 表驱动用例：statusA/statusB 为待比较状态，expectEqual 为期望结果。
	testcases := map[string]struct {
		statusA, statusB *corev1.PodExtendedResourceClaimStatus
		expectEqual      bool
	}{
		// 完全相同的状态（含两条映射）应相等。
		"identical": {
			statusA: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{ra, rb},
				ResourceClaimName: "x",
			},
			statusB: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{ra, rb},
				ResourceClaimName: "x",
			},
			expectEqual: true,
		},
		// RequestMappings 顺序不同，应判定不等。
		"different-order": {
			statusA: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{ra, rb},
				ResourceClaimName: "x",
			},
			statusB: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{rb, ra},
				ResourceClaimName: "x",
			},
			expectEqual: false,
		},
		// 映射数量不同，应判定不等。
		"different-length": {
			statusA: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{ra, rb},
				ResourceClaimName: "x",
			},
			statusB: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{ra},
				ResourceClaimName: "x",
			},
			expectEqual: false,
		},

		// 空映射切片与 nil 等价。
		"semantically-equal-empty": {
			statusA: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{},
				ResourceClaimName: "x",
			},
			statusB: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   nil,
				ResourceClaimName: "x",
			},
			expectEqual: true,
		},
		// ResourceClaimName 不同，应判定不等。
		"different-claim-name": {
			statusA: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{ra, rb},
				ResourceClaimName: "x",
			},
			statusB: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{ra, rb},
				ResourceClaimName: "y",
			},
			expectEqual: false,
		},
		// 两个 nil 状态视为相等。
		"both-nil": {
			statusA:     nil,
			statusB:     nil,
			expectEqual: true,
		},
		// 仅一侧为 nil，应判定不等。
		"one-nil-other-not-nil": {
			statusA: nil,
			statusB: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{ra, rb},
				ResourceClaimName: "y",
			},
			expectEqual: false,
		},
		// 反方向的 nil 组合同样判定不等。
		"one-not-nil-other-nil": {
			statusA: &corev1.PodExtendedResourceClaimStatus{
				RequestMappings:   []corev1.ContainerExtendedResourceRequest{ra, rb},
				ResourceClaimName: "x",
			},
			statusB:     nil,
			expectEqual: false,
		},
	}

	// 每个用例先验证自反性，再验证两侧状态的比较结果。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// 自反性检查。
			assert.True(t, PodExtendedStatusEqual(tc.statusA, tc.statusA), "status", tc.statusA)
			assert.True(t, PodExtendedStatusEqual(tc.statusB, tc.statusB), "status", tc.statusB)
			// 断言跨状态比较结果等于期望值。
			assert.Equal(t, tc.expectEqual, PodExtendedStatusEqual(tc.statusA, tc.statusB), "status", tc.statusA, tc.statusB)
		})
	}
}

// TestPodClaims 验证 PodClaims 迭代器：从 Pod 的 spec 与 status 中收集所有可产出的 claim，
// 检查所有权标记，并验证调用方提前终止迭代时迭代器能正确响应。
func TestPodClaims(t *testing.T) {
	// claimEntry 记录一次迭代产出：claim 名称与是否需要校验所有权。
	type claimEntry struct {
		name           string
		mustCheckOwner bool
	}

	// collect 将迭代器完整消费为切片，便于直接断言。
	collect := func(pod *corev1.Pod) []claimEntry {
		var res []claimEntry
		// 遍历迭代器并逐项收集。
		for name, mustCheckOwner := range PodClaims(pod) {
			res = append(res, claimEntry{name: name, mustCheckOwner: mustCheckOwner})
		}
		return res
	}

	// nil Pod 不产出任何条目。
	assert.Empty(t, collect(nil))
	// 空 Pod 同样不产出任何条目。
	assert.Empty(t, collect(&corev1.Pod{}))

	// 构造包含直接引用、模板已创建、模板未创建、模板跳过（状态中名称为 nil）
	// 以及不支持字段的 Pod，并设置 DRA 扩展资源状态。
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			ResourceClaims: []corev1.PodResourceClaim{
				{Name: "direct", ResourceClaimName: new("claim-direct")},
				{Name: "template-created", ResourceClaimTemplateName: new("tpl-1")},
				{Name: "template-uncreated", ResourceClaimTemplateName: new("tpl-2")},
				{Name: "template-skipped", ResourceClaimTemplateName: new("tpl-3")},
				{Name: "unsupported"},
			},
		},
		Status: corev1.PodStatus{
			ResourceClaimStatuses: []corev1.PodResourceClaimStatus{
				{Name: "template-created", ResourceClaimName: new("claim-from-tpl")},
				{Name: "template-skipped", ResourceClaimName: nil},
			},
			ExtendedResourceClaimStatus: &corev1.PodExtendedResourceClaimStatus{
				ResourceClaimName: "pod-extended-resources-abc",
			},
		},
	}

	// 期望只产出直接引用、模板已创建以及扩展资源三个 claim；
	// 模板未创建与不支持字段的条目应被跳过，所有权标记分别为 false/true/true。
	expected := []claimEntry{
		{name: "claim-direct", mustCheckOwner: false},
		{name: "claim-from-tpl", mustCheckOwner: true},
		{name: "pod-extended-resources-abc", mustCheckOwner: true},
	}
	// 断言完整遍历的结果与期望一致。
	assert.Equal(t, expected, collect(pod))

	// Verify early break from iterator works properly.
	// 首次迭代后立即 break，验证迭代器支持提前终止。
	var firstOnly []claimEntry
	for name, mustCheckOwner := range PodClaims(pod) {
		firstOnly = append(firstOnly, claimEntry{name: name, mustCheckOwner: mustCheckOwner})
		break
	}
	// 断言只收集到第一项。
	assert.Equal(t, expected[:1], firstOnly)
}
