/*
Copyright The Kubernetes Authors.

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

package dra

import (
	"fmt"

	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// testOptionalNodeOperations 验证 DRA 可选节点操作。
// 注意：此测试依赖共享的测试初始化（在 dra.go 中），该初始化在该特性启用时
// 已配置 worker-0 声明支持 DRAOptionalNodeOperations。
// 其他所有节点（worker-1 到 worker-7）都没有该声明特性。
func testOptionalNodeOperations(tCtx ktesting.TContext, enabled bool) {
	if !enabled {
		return
	}
	tCtx.Parallel()

	tests := map[string]struct {
		usePodGroup     bool
		nodeSelector    map[string]string
		expectScheduled bool
		expectedNode    string
	}{
		"pod-scheduled-on-supported-node": {
			usePodGroup:     false,
			expectScheduled: true,
			expectedNode:    "worker-0",
		},
		"pod-unschedulable-on-unsupported-node": {
			usePodGroup:     false,
			nodeSelector:    map[string]string{"kubernetes.io/hostname": "worker-1"},
			expectScheduled: false,
		},
		"podgroup-scheduled-on-supported-node": {
			usePodGroup:     true,
			expectScheduled: true,
			expectedNode:    "worker-0",
		},
		"podgroup-unschedulable-on-unsupported-node": {
			usePodGroup:     true,
			nodeSelector:    map[string]string{"kubernetes.io/hostname": "worker-1"},
			expectScheduled: false,
		},
	}

	for name, tt := range tests {
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			tCtx.Parallel()
			namespace := createTestNamespace(tCtx, nil)
			startScheduler(tCtx)
			class, driverName := createTestClass(tCtx, namespace)

			slice := st.MakeResourceSliceWithAllNodes("shared-slice", driverName).
				Devices(device1).
				SkipNodeOperations(resourceapi.SkipNodeOperationAll).
				Obj()
			createSlice(tCtx, slice)

			claimObj := createClaim(tCtx, namespace, "", class, claim)

			var podGroup *schedulingapi.PodGroup
			if tt.usePodGroup {
				pg := &schedulingapi.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "podgroup",
						Namespace: namespace,
					},
					Spec: schedulingapi.PodGroupSpec{
						SchedulingPolicy: schedulingapi.PodGroupSchedulingPolicy{
							Basic: &schedulingapi.BasicSchedulingPolicy{},
						},
						ResourceClaims: []schedulingapi.PodGroupResourceClaim{
							{
								Name:              claimObj.Name,
								ResourceClaimName: &claimObj.Name,
							},
						},
					},
				}
				podGroup = must(tCtx, tCtx.Client().SchedulingV1beta1().PodGroups(namespace).Create, pg, metav1.CreateOptions{})
			}

			numPods := 1
			if tt.usePodGroup {
				numPods = 2
			}

			var pods []*v1.Pod
			for i := range numPods {
				p := podWithClaimName.DeepCopy()
				if tt.usePodGroup {
					p.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
						PodGroupName: &podGroup.Name,
					}
				}
				// 如果指定了 node selector，则将其应用到最后一个 Pod，以测试拒绝行为。
				if i == numPods-1 && tt.nodeSelector != nil {
					p.Spec.NodeSelector = tt.nodeSelector
				}
				pod := createPod(tCtx, namespace, fmt.Sprintf("-%d", i), p, claimObj)
				pods = append(pods, pod)
			}

			if tt.expectScheduled {
				// [waitForPodScheduled] 检查的 PodScheduled 条件由于竞态
				// 可能收敛为 True 或 False 两种状态，但 Pod 最终仍应被调度：
				// https://github.com/kubernetes/kubernetes/issues/139417#issuecomment-4651436886
				for _, pod := range pods {
					tCtx.Eventually(func(tCtx ktesting.TContext) (string, error) {
						p, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
						if err != nil {
							return "", err
						}
						return p.Spec.NodeName, nil
					}).WithTimeout(schedulingTimeout).Should(
						gomega.Equal(tt.expectedNode),
						"pod %s should be scheduled to %s", pod.Name, tt.expectedNode,
					)
				}
			} else {
				// 验证带有 node selector 的 Pod（最后一个）不可调度。
				unschedulablePod := pods[numPods-1]
				expectPodUnschedulable(tCtx, unschedulablePod, "")
			}
		})
	}
}
