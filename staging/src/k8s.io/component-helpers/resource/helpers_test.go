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

package resource

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/utils/ptr"
)

// 本文件把 resource 包的计算口径钉在测试里：PodRequests/PodLimits 如何处理 overhead、
// init 容器与 sidecar、pod 级资源、原地扩缩容留下的 status，以及 DRA 声明带来的分配。
// 这些计算结果参与调度与准入，口径变化等同于 API 变化，所以宁可把每种边界单独摆成一个 case。
// TestPodRequestsAndLimits 关注 overhead 的落点：请求侧把 overhead 直接累加，
// 限制侧只在容器已有对应 limit 时才累加。表里刻意放了一个只有 limit 没有 request 的 case，
// 用来确认没有 request 时不会因为 overhead 凭空生成一条 request。
// 每个 case 走 getPod 造一个普通容器加同构 init 容器的 pod，
// 这样 init 取最大值的逻辑不会干扰对 overhead 累加的观察。
func TestPodRequestsAndLimits(t *testing.T) {
	cases := []struct {
		pod              *v1.Pod
		cName            string
		expectedRequests v1.ResourceList
		expectedLimits   v1.ResourceList
	}{
		{
			// 只有 CPU limit、没有任何 request 和 overhead：requests 应为空，limits 保留 9。
			cName:            "just-limit-no-overhead",
			pod:              getPod("foo", podResources{cpuLimit: "9"}),
			expectedRequests: v1.ResourceList{},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("9"),
			},
		},
		{
			// 只有 overhead 没有容器请求：request 侧直接取开销值 5/5，limit 侧仍为空。
			cName: "just-overhead",
			pod:   getPod("foo", podResources{cpuOverhead: "5", memoryOverhead: "5"}),
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("5"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("5"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			// 请求与开销叠加：CPU 1+5=6，内存 10+5=15；没有 limit 就不生成 limit。
			cName: "req-and-overhead",
			pod:   getPod("foo", podResources{cpuRequest: "1", memoryRequest: "10", cpuOverhead: "5", memoryOverhead: "5"}),
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("6"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("15"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			// 请求与限制都声明且都有 overhead：请求侧 1+5=6 / 10+5=15，限制侧 2+5=7 / 12+5=17。
			cName: "all-req-lim-and-overhead",
			pod:   getPod("foo", podResources{cpuRequest: "1", cpuLimit: "2", memoryRequest: "10", memoryLimit: "12", cpuOverhead: "5", memoryOverhead: "5"}),
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("6"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("15"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("7"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("17"),
			},
		},
		{
			// 内存只有 request 没有 limit：请求侧照常加 overhead，限制侧只出现 CPU 的 2+5=7。
			cName: "req-some-lim-and-overhead",
			pod:   getPod("foo", podResources{cpuRequest: "1", cpuLimit: "2", memoryRequest: "10", cpuOverhead: "5", memoryOverhead: "5"}),
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("6"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("15"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("7"),
			},
		},
	}
	for idx, tc := range cases {
		resRequests := PodRequests(tc.pod, PodResourcesOptions{})
		resLimits := PodLimits(tc.pod, PodResourcesOptions{})

		if !equality.Semantic.DeepEqual(tc.expectedRequests, resRequests) {
			t.Errorf("test case failure[%d]: %v, requests:\n expected:\t%v\ngot\t\t%v", idx, tc.cName, tc.expectedRequests, resRequests)
		}

		if !equality.Semantic.DeepEqual(tc.expectedLimits, resLimits) {
			t.Errorf("test case failure[%d]: %v, limits:\n expected:\t%v\ngot\t\t%v", idx, tc.cName, tc.expectedLimits, resLimits)
		}
	}
}

// TestPodRequestsAndLimitsWithoutOverhead 锁定 ExcludeOverhead 的语义：
// 只要开关打开，pod.Spec.Overhead 必须被完全忽略，哪怕它比容器自身资源还大。
// 第三个 case 故意塞进一个巨大 init 容器，确认排除 overhead 之后仍然走 init 取最大的路径，
// 而不是把开销混进峰值里。
func TestPodRequestsAndLimitsWithoutOverhead(t *testing.T) {
	cases := []struct {
		pod              *v1.Pod
		name             string
		expectedRequests v1.ResourceList
		expectedLimits   v1.ResourceList
	}{
		{
			name: "two container no overhead - should just be sum of containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "foobar",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("5"),
								},
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("10"),
								},
							},
						},
						{
							Name: "foobar2",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("4"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("12"),
								},
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("8"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("24"),
								},
							},
						},
					},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("5"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("17"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("34"),
			},
		},
		{
			name: "two container with overhead - shouldn't consider overhead",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Overhead: v1.ResourceList{
						v1.ResourceName(v1.ResourceCPU):    resource.MustParse("3"),
						v1.ResourceName(v1.ResourceMemory): resource.MustParse("8"),
					},
					Containers: []v1.Container{
						{
							Name: "foobar",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("5"),
								},
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("10"),
								},
							},
						},
						{
							Name: "foobar2",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("4"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("12"),
								},
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("8"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("24"),
								},
							},
						},
					},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("5"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("17"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("34"),
			},
		},
		{
			// overhead 与 init 峰值同时存在；排除开关打开后，峰值只能来自 init 自身。
			name: "two container with overhead, massive init - should just be the largest init",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Overhead: v1.ResourceList{
						v1.ResourceName(v1.ResourceCPU):    resource.MustParse("3"),
						v1.ResourceName(v1.ResourceMemory): resource.MustParse("8"),
					},
					Containers: []v1.Container{
						{
							Name: "foobar",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("5"),
								},
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("10"),
								},
							},
						},
						{
							Name: "foobar2",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("4"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("12"),
								},
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("8"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("24"),
								},
							},
						},
					},
					InitContainers: []v1.Container{
						{
							Name: "small-init",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("5"),
								},
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("5"),
								},
							},
						},
						{
							Name: "big-init",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("40"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("120"),
								},
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("80"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("240"),
								},
							},
						},
					},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("40"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("120"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("80"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("240"),
			},
		},
	}
	for idx, tc := range cases {
		resRequests := PodRequests(tc.pod, PodResourcesOptions{ExcludeOverhead: true})
		resLimits := PodLimits(tc.pod, PodResourcesOptions{ExcludeOverhead: true})

		if !equality.Semantic.DeepEqual(tc.expectedRequests, resRequests) {
			t.Errorf("test case failure[%d]: %v, requests:\n expected:\t%v\ngot\t\t%v", idx, tc.name, tc.expectedRequests, resRequests)
		}

		if !equality.Semantic.DeepEqual(tc.expectedLimits, resLimits) {
			t.Errorf("test case failure[%d]: %v, limits:\n expected:\t%v\ngot\t\t%v", idx, tc.name, tc.expectedLimits, resLimits)
		}
	}
}

// TestPodResourceRequests 是 requests 聚合的主表，三套机制在这里交叉验证：
// 普通容器求和、init 容器取峰值、restartable init（sidecar）既计入总和又抬高后续 init 的峰值；
// pod overhead 的包含与排除；以及原地扩缩容时 UseStatusResources 的三方取最大
// （spec、allocated、actuated）。resize 不可行时 spec 被丢弃，只认 status 里的旧值。
// 每个 case 直接构造 Pod 而不走 getPod，是为了精确控制 Condition、AllocatedResources
// 和容器状态；populatePodStatusResourcesForTest 负责把容器状态回填成 pod 级状态，
// 模拟 Kubelet 已经写好的聚合值。
func TestPodResourceRequests(t *testing.T) {
	restartAlways := v1.ContainerRestartPolicyAlways
	testCases := []struct {
		description           string
		options               PodResourcesOptions
		overhead              v1.ResourceList
		podResizeStatus       []v1.PodCondition
		initContainers        []v1.Container
		initContainerStatuses []v1.ContainerStatus
		containers            []v1.Container
		containerStatus       []v1.ContainerStatus
		expectedRequests      v1.ResourceList
	}{
		{
			// 默认选项下取 init 与普通容器的较大者：init 4 > 普通容器 1，期望为 4。
			description: "nil options, larger init container",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"),
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("4"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
		{
			// 默认选项下普通容器求和（2+3=5）大于 init 的 2，期望取 5。
			description: "nil options, larger containers",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("5"),
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
		},
		{
			// ExcludeOverhead 打开：开销 1 被忽略，结果只来自容器聚合的 5。
			description: "pod overhead excluded",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("5"),
			},
			options: PodResourcesOptions{
				ExcludeOverhead: true,
			},
			overhead: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
		},
		{
			description: "pod overhead included",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("6"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			overhead: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
		},
		{
			// 接连几个 case 分别拆开 resize 不可行、可行但没有 resize 状态、以及三方取最大三条路径。
			// 不可行时 spec 被抛掉，只看 status，这正是原地扩缩容回退语义的关键。
			description: "resized, infeasible",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("2"),
			},
			podResizeStatus: []v1.PodCondition{{
				Type:   v1.PodResizePending,
				Status: v1.ConditionTrue,
				Reason: v1.PodReasonInfeasible,
			}},
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("4"),
						},
					},
				},
			},
			containerStatus: []v1.ContainerStatus{
				{
					Name: "container-1",
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
		{
			// resize 不可行但已在推进：allocated 4 大于 status 的 2，期望取 4。
			description: "resized, infeasible & in-progress",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"),
			},
			podResizeStatus: []v1.PodCondition{{
				Type:   v1.PodResizePending,
				Status: v1.ConditionTrue,
				Reason: v1.PodReasonInfeasible,
			}},
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("6"),
						},
					},
				},
			},
			containerStatus: []v1.ContainerStatus{
				{
					Name: "container-1",
					AllocatedResources: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("4"),
					},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
		{
			// resize 没有状态条件：回退到比较 spec 4 与 status 2，取 4。
			description: "resized, no resize status",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"),
			},
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("4"),
						},
					},
				},
			},
			containerStatus: []v1.ContainerStatus{
				{
					Name: "container-1",
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
		{
			// 每个资源独立地在三方里取最大：CPU 取 spec、Memory 取 actuated、存储取 allocated，
			// 互不影响；按整体而不是按资源取最大会把其中一项算错。
			description: "resized: per-resource 3-way maximum",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("30m"),
				v1.ResourceMemory: resource.MustParse("30M"),
				// 注意：ephemeral-storage 本身不可原地扩缩容，但这不影响本用例要验证的三方取最大口径。
				v1.ResourceEphemeralStorage: resource.MustParse("30G"),
			},
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:              resource.MustParse("30m"),
							v1.ResourceMemory:           resource.MustParse("20M"),
							v1.ResourceEphemeralStorage: resource.MustParse("10G"),
						},
					},
				},
			},
			containerStatus: []v1.ContainerStatus{
				{
					Name: "container-1",
					AllocatedResources: v1.ResourceList{
						v1.ResourceCPU:              resource.MustParse("20m"),
						v1.ResourceMemory:           resource.MustParse("10M"),
						v1.ResourceEphemeralStorage: resource.MustParse("30G"),
					},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:              resource.MustParse("10m"),
							v1.ResourceMemory:           resource.MustParse("30M"),
							v1.ResourceEphemeralStorage: resource.MustParse("20G"),
						},
					},
				},
			},
		},
		{
			description: "resized, infeasible, but don't use status",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"),
			},
			podResizeStatus: []v1.PodCondition{{
				Type:   v1.PodResizePending,
				Status: v1.ConditionTrue,
				Reason: v1.PodReasonInfeasible,
			}},
			options: PodResourcesOptions{UseStatusResources: false},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("4"),
						},
					},
				},
			},
			containerStatus: []v1.ContainerStatus{
				{
					Name: "container-1",
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
		{
			description: "resized, restartable init container, infeasible",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("2"),
			},
			podResizeStatus: []v1.PodCondition{{
				Type:   v1.PodResizePending,
				Status: v1.ConditionTrue,
				Reason: v1.PodReasonInfeasible,
			}},
			options: PodResourcesOptions{UseStatusResources: true},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("4"),
						},
					},
				},
			},
			initContainerStatuses: []v1.ContainerStatus{
				{
					Name: "restartable-init-1",
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
		{
			description: "resized, restartable init container, no resize status",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"),
			},
			options: PodResourcesOptions{UseStatusResources: true},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("4"),
						},
					},
				},
			},
			initContainerStatuses: []v1.ContainerStatus{
				{
					Name: "restartable-init-1",
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
		{
			description: "resized, restartable init container, infeasible, but don't use status",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"),
			},
			podResizeStatus: []v1.PodCondition{{
				Type:   v1.PodResizePending,
				Status: v1.ConditionTrue,
				Reason: v1.PodReasonInfeasible,
			}},
			options: PodResourcesOptions{UseStatusResources: false},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("4"),
						},
					},
				},
			},
			initContainerStatuses: []v1.ContainerStatus{
				{
					Name: "restartable-init-1",
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
		{
			description: "restartable init container",
			expectedRequests: v1.ResourceList{
				// restartable init（sidecar）与普通容器同时存在：两者请求都要计入总和。
				v1.ResourceCPU: resource.MustParse("2"),
			},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
		{
			description: "multiple restartable init containers",
			expectedRequests: v1.ResourceList{
				// 期望取峰值：max(普通 init 的 5, sidecar 之和 3+2+1 再加普通容器 1) = 7。
				v1.ResourceCPU: resource.MustParse("7"),
			},
			initContainers: []v1.Container{
				{
					Name: "init-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("5"),
						},
					},
				},
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
				{
					Name:          "restartable-init-2",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Name:          "restartable-init-3",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
		{
			description: "multiple restartable and regular init containers",
			expectedRequests: v1.ResourceList{
				// init-2 的峰值需求为自身 5 加上在它之前运行的 restartable init 之和 1+2 = 8；
				// 在它之后才启动的 sidecar 不计入这个峰值。
				v1.ResourceCPU: resource.MustParse("8"),
			},
			initContainers: []v1.Container{
				{
					Name: "init-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("5"),
						},
					},
				},
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
				{
					Name:          "restartable-init-2",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Name: "init-2",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("5"),
						},
					},
				},
				{
					Name:          "restartable-init-3",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
		{
			description: "restartable-init, init and regular",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("210"),
			},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("10"),
						},
					},
				},
				{
					Name: "init-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("200"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("100"),
						},
					},
				},
			},
		},
		{
			description: "aggregate request resolves max of sums instead of sum of maxes",
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("3"),
			},
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
				{
					Name: "c2",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			containerStatus: []v1.ContainerStatus{
				{
					Name: "c1",
					AllocatedResources: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("2"),
					},
				},
				{
					Name: "c2",
					AllocatedResources: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("1"),
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			p := &v1.Pod{
				Spec: v1.PodSpec{
					Containers:     tc.containers,
					InitContainers: tc.initContainers,
					Overhead:       tc.overhead,
				},
				Status: v1.PodStatus{
					ContainerStatuses:     tc.containerStatus,
					InitContainerStatuses: tc.initContainerStatuses,
					Conditions:            tc.podResizeStatus,
				},
			}
			populatePodStatusResourcesForTest(p)
			request := PodRequests(p, tc.options)
			if !equality.Semantic.DeepEqual(request, tc.expectedRequests) {
				t.Errorf("got=%v, want=%v, diff=%s", request, tc.expectedRequests, diff.Diff(request, tc.expectedRequests))
			}
		})
	}
}

// TestPodResourceRequestsReuse 验证 Reuse 选项真的复用了调用方传进来的 map：
// 实现若改为新建 map，传入的 Reuse 就不会被改写，下面这条断言会失败。
// 因此初始值故意给一个明显错误的数值，而不是空 map。
func TestPodResourceRequestsReuse(t *testing.T) {
	expectedRequests := v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("1"),
	}
	p := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: expectedRequests,
					},
				},
			},
		},
	}

	opts := PodResourcesOptions{
		Reuse: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("25"),
		},
	}
	requests := PodRequests(p, opts)

	if !equality.Semantic.DeepEqual(requests, expectedRequests) {
		t.Errorf("got=%v, want=%v, diff=%s", requests, expectedRequests, diff.Diff(requests, expectedRequests))
	}

	// 断言复用了调用方传入的 map：传入的 map 被改写为期望值。
	if !equality.Semantic.DeepEqual(opts.Reuse, expectedRequests) {
		t.Errorf("got=%v, want=%v, diff=%s", requests, expectedRequests, diff.Diff(opts.Reuse, expectedRequests))
	}
}

// TestPodResourceLimits 与 requests 主表对称，但多出两条 limits 特有的口径：
// 没有声明 limit 的容器视为无限制，不会因为 overhead 或 DRA 冒出一条 limit；
// overhead 只加到非零 limit 上，所以 pod overhead included 这个 case 里内存开销不会体现。
// 其余 case 继续覆盖 sidecar 累加、三方取最大，以及 spec 与 status 不一致的 resize。
func TestPodResourceLimits(t *testing.T) {
	restartAlways := v1.ContainerRestartPolicyAlways
	testCases := []struct {
		description           string
		options               PodResourcesOptions
		overhead              v1.ResourceList
		initContainers        []v1.Container
		initContainerStatuses []v1.ContainerStatus
		containers            []v1.Container
		containerStatuses     []v1.ContainerStatus
		expectedLimits        v1.ResourceList
	}{
		{
			description: "nil options, larger init container",
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"),
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("4"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
		{
			description: "nil options, larger containers",
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("5"),
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
		},
		{
			description: "pod overhead excluded",
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("5"),
			},
			options: PodResourcesOptions{
				ExcludeOverhead: true,
			},
			overhead: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"),
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
		},
		{
			// 这里内存没有声明 limit，overhead 里的 1Gi 内存开销不应出现在结果中；
			// 只有 CPU 的 limit 被抬高。
			description: "pod overhead included",
			overhead: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("6"),
				// overhead 只加到非零的 limit 上，所以这里不应出现 memory limit。
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
		},
		{
			// 所有容器都没声明 limit，pod 结果里也不该凭空出现任何 limit。
			description:    "no limited containers should result in no limits for the pod",
			expectedLimits: v1.ResourceList{},
			initContainers: []v1.Container{},
			containers: []v1.Container{
				{
					// 无限制容器：不声明任何 Resources。
				},
			},
		},
		{
			// 一个声明了 limit、一个未声明：pod 的 limit 只取前者，无限制容器不贡献 limit。
			description: "one limited and one unlimited container should result in the limited container's limits for the pod",
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("2"),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			initContainers: []v1.Container{},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("2"),
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
				{
					// 无限制容器：不声明任何 Resources。
				},
			},
		},
		{
			// init 同样适用：只有声明了 limit 的 init 参与峰值，无限制 init 被忽略。
			description: "one limited and one unlimited init container should result in the limited init container's limits for the pod",
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("2"),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("2"),
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
				{
					// 无限制 init 容器：不声明任何 Resources。
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
		{
			description: "restartable init container",
			expectedLimits: v1.ResourceList{
				// restartable init（sidecar）与普通容器同时存在：两者请求都要计入总和。
				v1.ResourceCPU: resource.MustParse("2"),
			},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
		{
			description: "multiple restartable init containers",
			expectedLimits: v1.ResourceList{
				// 期望取峰值：max(普通 init 的 5, sidecar 之和 3+2+1 再加普通容器 1) = 7。
				v1.ResourceCPU: resource.MustParse("7"),
			},
			initContainers: []v1.Container{
				{
					Name: "init-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("5"),
						},
					},
				},
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
				{
					Name:          "restartable-init-2",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Name:          "restartable-init-3",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
		{
			description: "multiple restartable and regular init containers",
			expectedLimits: v1.ResourceList{
				// init-2 的峰值需求为自身 5 加上在它之前运行的 restartable init 之和 1+2 = 8；
				// 在它之后才启动的 sidecar 不计入这个峰值。
				v1.ResourceCPU: resource.MustParse("8"),
			},
			initContainers: []v1.Container{
				{
					Name: "init-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("5"),
						},
					},
				},
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
				{
					Name:          "restartable-init-2",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
				{
					Name: "init-2",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("5"),
						},
					},
				},
				{
					Name:          "restartable-init-3",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("3"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
		{
			description: "restartable-init, init and regular",
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("210"),
			},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("10"),
						},
					},
				},
				{
					Name: "init-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("200"),
						},
					},
				},
			},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("100"),
						},
					},
				},
			},
		},
		{
			// 扩容：spec 2Gi 高于 status 的 1Gi，UseStatusResources 下应取较大的 spec 值。
			description: "pod scaled up",
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
			containerStatuses: []v1.ContainerStatus{
				{
					Name: "container-1",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
		{
			description: "pod scaled down",
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
			containerStatuses: []v1.ContainerStatus{
				{
					Name: "container-1",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
		{
			description: "pod scaled down, don't use status",
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			options: PodResourcesOptions{UseStatusResources: false},
			containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
			containerStatuses: []v1.ContainerStatus{
				{
					Name: "container-1",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
		{
			description: "pod scaled up with restartable init containers",
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			options: PodResourcesOptions{UseStatusResources: true},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
			initContainerStatuses: []v1.ContainerStatus{
				{
					Name: "restartable-init-1",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
		{
			description: "pod scaled down with restartable init containers",
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			options: PodResourcesOptions{UseStatusResources: true},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
			initContainerStatuses: []v1.ContainerStatus{
				{
					Name: "restartable-init-1",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
		{
			description: "pod scaled down with restartable init containers, don't use status",
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			options: PodResourcesOptions{UseStatusResources: false},
			initContainers: []v1.Container{
				{
					Name:          "restartable-init-1",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
			initContainerStatuses: []v1.ContainerStatus{
				{
					Name: "restartable-init-1",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
		{
			description: "aggregate limit resolves max of sums instead of sum of maxes",
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("3"),
			},
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
				{
					Name: "c2",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
				},
			},
			containerStatuses: []v1.ContainerStatus{
				{
					Name: "c1",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
				},
				{
					Name: "c2",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			p := &v1.Pod{
				Spec: v1.PodSpec{
					Containers:     tc.containers,
					InitContainers: tc.initContainers,
					Overhead:       tc.overhead,
				},
				Status: v1.PodStatus{
					ContainerStatuses:     tc.containerStatuses,
					InitContainerStatuses: tc.initContainerStatuses,
				},
			}
			populatePodStatusResourcesForTest(p)
			limits := PodLimits(p, tc.options)
			if !equality.Semantic.DeepEqual(limits, tc.expectedLimits) {
				t.Errorf("got=%v, want=%v, diff=%s", limits, tc.expectedLimits, diff.Diff(limits, tc.expectedLimits))
			}
		})
	}
}

// TestIsPodLevelResourcesSet 给 pod 级资源的判定画边界：只认受支持的资源，
// ephemeral-storage 即使填了也不算设置了 pod 级资源。
// 否则不支持的字段会误触发 pod 级覆盖，把容器级聚合结果整个替换掉。
func TestIsPodLevelResourcesSet(t *testing.T) {
	testCases := []struct {
		name         string
		podResources *v1.ResourceRequirements
		expected     bool
	}{
		{
			name:     "nil resources struct",
			expected: false,
		},
		{
			name:         "empty resources struct",
			podResources: &v1.ResourceRequirements{},
			expected:     false,
		},
		{
			name: "only unsupported resource requests set",
			podResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceEphemeralStorage: resource.MustParse("1Mi")},
			},
			expected: false,
		},
		{
			name: "only unsupported resource limits set",
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{v1.ResourceEphemeralStorage: resource.MustParse("1Mi")},
			},
			expected: false,
		},
		{
			name: "unsupported and suported resources requests set",
			podResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceEphemeralStorage: resource.MustParse("1Mi"),
					v1.ResourceCPU:              resource.MustParse("1m"),
				},
			},
			expected: true,
		},
		{
			name: "unsupported and suported resources limits set",
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceEphemeralStorage: resource.MustParse("1Mi"),
					v1.ResourceCPU:              resource.MustParse("1m"),
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testPod := &v1.Pod{Spec: v1.PodSpec{Resources: tc.podResources}}
			if got := IsPodLevelResourcesSet(testPod); got != tc.expected {
				t.Errorf("got=%t, want=%t", got, tc.expected)
			}
		})
	}

}

// TestIsPodLevelLimitsSet 是 Limits 版本的门控：只有 Requests 不算，
// 且同样要排除不支持的资源。单独列 case 是为了防止将来放宽支持集合时把这里一起带偏。
func TestIsPodLevelLimitsSet(t *testing.T) {
	testCases := []struct {
		name         string
		podResources *v1.ResourceRequirements
		expected     bool
	}{
		{
			name:     "nil resources struct",
			expected: false,
		},
		{
			name:         "empty resources struct",
			podResources: &v1.ResourceRequirements{},
			expected:     false,
		},
		{
			name: "only resource requests set",
			podResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("100Mi")},
			},
			expected: false,
		},
		{
			name: "only unsupported resource limits set",
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{v1.ResourceEphemeralStorage: resource.MustParse("1Mi")},
			},
			expected: false,
		},
		{
			name: "unsupported and suported resources limits set",
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceEphemeralStorage: resource.MustParse("1Mi"),
					v1.ResourceCPU:              resource.MustParse("1m"),
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testPod := &v1.Pod{Spec: v1.PodSpec{Resources: tc.podResources}}
			if got := IsPodLevelLimitsSet(testPod); got != tc.expected {
				t.Errorf("got=%t, want=%t", got, tc.expected)
			}
		})
	}

}

// TestPodLevelResourceRequests 固定 pod 级值与容器级值的合并规则：
// pod 级值直接覆盖同名容器级值，而不是相加；不支持的资源被忽略；
// SkipPodLevelResources 可以整体绕过这条覆盖路径。
// 表里让 init 与 sidecar 和 pod 级值同时出现，确认覆盖发生在聚合完成之后，
// 这样 sidecar 的累加不会被 pod 级值二次放大。
func TestPodLevelResourceRequests(t *testing.T) {
	restartAlways := v1.ContainerRestartPolicyAlways
	testCases := []struct {
		name             string
		opts             PodResourcesOptions
		podResources     v1.ResourceRequirements
		overhead         v1.ResourceList
		initContainers   []v1.Container
		containers       []v1.Container
		expectedRequests v1.ResourceList
	}{
		{
			name:             "nil",
			expectedRequests: v1.ResourceList{},
		},
		{
			name:             "pod level memory resource with SkipPodLevelResources true",
			podResources:     v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Mi")}},
			opts:             PodResourcesOptions{SkipPodLevelResources: true},
			expectedRequests: v1.ResourceList{},
		},
		{
			name:             "pod level memory resource with SkipPodLevelResources false",
			podResources:     v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Mi")}},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Mi")},
		},
		{
			name:         "pod level memory and container level cpu resources with SkipPodLevelResources false",
			podResources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Mi")}},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2m")}},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Mi"), v1.ResourceCPU: resource.MustParse("2m")},
		},
		{
			name:         "pod level unsupported resources set at both pod-level and container-level with SkipPodLevelResources false",
			podResources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("2Mi")}},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("3Mi")}},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("3Mi")},
		},
		{
			name:         "pod level unsupported resources set at pod-level with SkipPodLevelResources false",
			podResources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("2Mi")}},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("3Mi")}},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("3Mi")},
		},
		{
			name: "only container level resources set with SkipPodLevelResources false",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("3Mi"),
							v1.ResourceCPU:    resource.MustParse("2m"),
						},
					},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("3Mi"), v1.ResourceCPU: resource.MustParse("2m")},
		},
		{
			name: "both container-level and pod-level resources set with SkipPodLevelResources false",
			podResources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("6Mi"),
					v1.ResourceCPU:    resource.MustParse("8m"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("3Mi"),
							v1.ResourceCPU:    resource.MustParse("2m"),
						},
					},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("6Mi"), v1.ResourceCPU: resource.MustParse("8m")},
		},
		{
			name: "container-level resources and init container set with SkipPodLevelResources false",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("3Mi"),
							v1.ResourceCPU:    resource.MustParse("2m"),
						},
					},
				},
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("5Mi"),
							v1.ResourceCPU:    resource.MustParse("4m"),
						},
					},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("5Mi"), v1.ResourceCPU: resource.MustParse("4m")},
		},
		{
			name: "container-level resources and init container set with SkipPodLevelResources false",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("3Mi"),
							v1.ResourceCPU:    resource.MustParse("2m"),
						},
					},
				},
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("5Mi"),
							v1.ResourceCPU:    resource.MustParse("4m"),
						},
					},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: true},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("5Mi"), v1.ResourceCPU: resource.MustParse("4m")},
		},
		{
			name: "container-level resources and sidecar container set with SkipPodLevelResources false",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("3Mi"),
							v1.ResourceCPU:    resource.MustParse("2m"),
						},
					},
				},
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("5Mi"),
							v1.ResourceCPU:    resource.MustParse("4m"),
						},
					},
					RestartPolicy: &restartAlways,
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("8Mi"), v1.ResourceCPU: resource.MustParse("6m")},
		},
		{
			name: "container-level resources, init and sidecar container set with SkipPodLevelResources false",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("3Mi"),
							v1.ResourceCPU:    resource.MustParse("2m"),
						},
					},
				},
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("5Mi"),
							v1.ResourceCPU:    resource.MustParse("4m"),
						},
					},
					RestartPolicy: &restartAlways,
				},
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("6Mi"),
							v1.ResourceCPU:    resource.MustParse("8m"),
						},
					},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("11Mi"), v1.ResourceCPU: resource.MustParse("12m")},
		},
		{
			name: "pod-level resources, container-level resources, init and sidecar container set with SkipPodLevelResources false",
			podResources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("15Mi"),
					v1.ResourceCPU:    resource.MustParse("18m"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("3Mi"),
							v1.ResourceCPU:    resource.MustParse("2m"),
						},
					},
				},
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("5Mi"),
							v1.ResourceCPU:    resource.MustParse("4m"),
						},
					},
					RestartPolicy: &restartAlways,
				},
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("6Mi"),
							v1.ResourceCPU:    resource.MustParse("8m"),
						},
					},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("15Mi"), v1.ResourceCPU: resource.MustParse("18m")},
		},
		{
			name: "pod-level resources, hugepage request/limit single page size",
			podResources: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceMemory:                  resource.MustParse("10Mi"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("10Mi"), v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi")},
		},
		{
			name: "pod-level resources, hugepage request/limit multiple page sizes",
			podResources: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
					v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:                     resource.MustParse("1"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
					v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi"),
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"), v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi")},
		},
		{
			name: "pod-level resources, container-level resources, hugepage request/limit single page size",
			podResources: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:                     resource.MustParse("1"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("6Mi"),
						},
						Requests: v1.ResourceList{
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("6Mi"),
						},
					},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi")},
		},
		{
			name: "pod-level resources, container-level resources, hugepage request/limit multiple page sizes",
			podResources: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
					v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("2Gi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:                     resource.MustParse("1"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
					v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("2Gi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("2Gi"),
						},
						Requests: v1.ResourceList{
							v1.ResourceCPU:                     resource.MustParse("1"),
							v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("2Gi"),
						},
					},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"), v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("2Gi")},
		},
		{
			name: "pod-level resources, container-level resources, hugepage request/limit multiple page sizes between pod-level and container-level",
			podResources: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:                     resource.MustParse("1"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi"),
						},
						Requests: v1.ResourceList{
							v1.ResourceMemory:                  resource.MustParse("4Mi"),
							v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi"),
						},
					},
				},
			},
			opts:             PodResourcesOptions{SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("4Mi"), v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"), v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi")},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			podReqs := PodRequests(getPodLevelResourcesPod(tc.podResources, tc.overhead, tc.containers, tc.initContainers), tc.opts)
			if !equality.Semantic.DeepEqual(podReqs, tc.expectedRequests) {
				t.Errorf("got=%v, want=%v, diff=%s", podReqs, tc.expectedRequests, diff.Diff(podReqs, tc.expectedRequests))
			}
		})
	}
}

// TestIsSupportedPodLevelResource 单独钉住支持集合的边界：CPU、内存、hugepages 支持，
// ephemeral-storage 不支持。前面几个门控函数都依赖这个判定，
// 集合若被无意扩大，那些函数的行为会跟着漂移，所以这里单独设一道闸。
func TestIsSupportedPodLevelResource(t *testing.T) {
	testCases := []struct {
		name     string
		resource v1.ResourceName
		expected bool
	}{
		{
			name:     v1.ResourceCPU.String(),
			resource: v1.ResourceCPU,
			expected: true,
		},
		{
			name:     v1.ResourceMemory.String(),
			resource: v1.ResourceMemory,
			expected: true,
		},
		{
			name:     v1.ResourceEphemeralStorage.String(),
			resource: v1.ResourceEphemeralStorage,
			expected: false,
		},
		{
			name:     v1.ResourceHugePagesPrefix + "2Mi",
			resource: v1.ResourceHugePagesPrefix + "2Mi",
			expected: true,
		},
		{
			name:     v1.ResourceHugePagesPrefix + "1Gi",
			resource: v1.ResourceHugePagesPrefix + "1Gi",
			expected: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsSupportedPodLevelResource(tc.resource); got != tc.expected {
				t.Errorf("Supported pod level resource %s: got=%t, want=%t", tc.resource.String(), got, tc.expected)
			}
		})
	}
}

// 表里多处要用到带具体页大小的 hugepages 资源名，
// 集中定义一份避免各 case 自己拼字符串时拼出不一致的名字。
var hugePageResource1Gi = v1.ResourceName(v1.ResourceHugePagesPrefix + "1Gi")

// TestPodRequestsAndLimitsWithDRA 描述 DRA 节点可分配声明如何并入 pod 资源：
// Mapping 按资源名累加；Overhead 的 PerPod 只算一次，PerContainer 按声明覆盖的容器数放大；
// 同一个 claim 被多个容器引用时也只计一次；未被容器引用的声明仍计入 Mapping，
// 但其 PerContainer 因为容器数为零而不生效。
// 另外两条口径靠 case 固定：pod 级资源会盖过 claim 折算出来的值；
// limits 只在容器显式声明了对应 limit（hugepages 除外）时才加上 DRA。
// 每个语义点拆成独立 case，回归时能直接定位是哪一条规则坏了。
func TestPodRequestsAndLimitsWithDRA(t *testing.T) {
	testCases := []struct {
		description      string
		pod              *v1.Pod
		options          PodResourcesOptions
		expectedRequests v1.ResourceList
		expectedLimits   v1.ResourceList
	}{
		{
			description: "No claims",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("100m"),
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("100m"),
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("100m"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("100m"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
		{
			description: "Single claim on one container",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU:      resource.MustParse("100m"),
									v1.ResourceMemory:   resource.MustParse("1Gi"),
									hugePageResource1Gi: resource.MustParse("2"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU:      resource.MustParse("100m"),
									v1.ResourceMemory:   resource.MustParse("1Gi"),
									hugePageResource1Gi: resource.MustParse("2"),
								},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "claim-1",
							ResourceClaimName: ptr.To("node-allocatable-claim-1"),
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim-1",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("50m"))},
								{Name: hugePageResource1Gi, Quantity: new(resource.MustParse("1"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:      resource.MustParse("150m"), // 容器 100m 加 claim 映射 50m
				v1.ResourceMemory:   resource.MustParse("1Gi"),
				hugePageResource1Gi: resource.MustParse("3"), // 容器 2 加 claim 映射 1
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU:      resource.MustParse("150m"),
				v1.ResourceMemory:   resource.MustParse("1Gi"),
				hugePageResource1Gi: resource.MustParse("3"),
			},
		},
		{
			description: "Multiple claims on one container",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("100m"),
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
									{Name: "claim-2"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "claim-1",
							ResourceClaimName: ptr.To("node-allocatable-claim-1"),
						},
						{
							Name:              "claim-2",
							ResourceClaimName: ptr.To("node-allocatable-claim-2"),
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim-1",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("50m"))},
							},
						},
						{
							ResourceClaimName: "node-allocatable-claim-2",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("25m"))},
								{Name: v1.ResourceMemory, Quantity: new(resource.MustParse("512Mi"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("175m"),  // 容器 100m 加两个 claim 映射 50m 与 25m
				v1.ResourceMemory: resource.MustParse("1.5Gi"), // 容器 1Gi 加 claim 映射 512Mi
			},
		},
		{
			// 共享 claim 的 mapping 与 PerContainer 开销都只能计一次，
			// 不能按引用它的容器数重复累加。
			description: "Same claim referenced in multiple containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
								Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("200m")},
								Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("200m")},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "claim-1",
							ResourceClaimName: ptr.To("node-allocatable-claim-1"),
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim-1",
							Containers:        []string{"c1", "c2"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("50m"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("350m"), // 两容器 100m 与 200m，加共享 claim 映射 50m（只计一次）
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("350m"), // 两容器 100m 与 200m，加 claim 映射 50m
			},
		},
		{
			description: "Different claims on multiple containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:      resource.MustParse("100m"),
									hugePageResource1Gi: resource.MustParse("1"),
								},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:      resource.MustParse("200m"),
									v1.ResourceMemory:   resource.MustParse("1Gi"),
									hugePageResource1Gi: resource.MustParse("2"),
								},
								Claims: []v1.ResourceClaim{
									{Name: "claim-2"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "claim-1",
							ResourceClaimName: ptr.To("node-allocatable-claim-1"),
						},
						{
							Name:              "claim-2",
							ResourceClaimName: ptr.To("node-allocatable-claim-2"),
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim-1",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("50m"))},
								{Name: hugePageResource1Gi, Quantity: new(resource.MustParse("1"))},
							},
						},
						{
							ResourceClaimName: "node-allocatable-claim-2",
							Containers:        []string{"c2"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("25m"))},
								{Name: v1.ResourceMemory, Quantity: new(resource.MustParse("512Mi"))},
								{Name: hugePageResource1Gi, Quantity: new(resource.MustParse("2"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:      resource.MustParse("375m"),  // 两容器 100m 与 200m，加两个 claim 映射 50m 与 25m
				v1.ResourceMemory:   resource.MustParse("1.5Gi"), // 两容器各 1Gi，加两个 claim 映射 1Gi 与 512Mi
				hugePageResource1Gi: resource.MustParse("6"),     // 1 + 2 + 1 + 2
			},
		},
		{
			description: "Claim on init container",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Name: "ic1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m")},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "claim-1",
							ResourceClaimName: ptr.To("node-allocatable-claim"),
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim",
							Containers:        []string{"ic1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("25m"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("125m"), // 取 max(容器 100m, claim 映射 50m) 再加另一 claim 的 25m
			},
		},
		{
			description: "UseDRANodeAllocatableResourceClaimStatus flag disabled",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("100m"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("100m"),
								},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "claim-1",
							ResourceClaimName: ptr.To("node-allocatable-claim"),
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("50m"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: false},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("100m"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("100m"),
			},
		},
		{
			// pod 级资源一旦设置就盖过 claim 折算值，但容器级资源仍然保留。
			description: "Pod Level Resources overrides the claim request/limit",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("3"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true, SkipPodLevelResources: false},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("2"),   // pod 级值 2 不应被 claim 折算出的 3 覆盖
				v1.ResourceMemory: resource.MustParse("1Gi"), // 容器级资源（1Gi 内存）仍要计入
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("2"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
		{
			description: "Pod with DRA claim specifying Overhead mapping",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("100m"),
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("100m"),
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim",
							Containers:        []string{"c1", "c2"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:         v1.ResourceMemory,
									PerPod:       new(resource.MustParse("1Gi")),
									PerContainer: new(resource.MustParse("500Mi")),
								},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("200m"),
				v1.ResourceMemory: resource.MustParse("4072Mi"), // c1 1Gi + c2 1Gi + pod 级固定开销 1Gi + 每容器开销 2×500Mi = 4072Mi
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("4072Mi"), // c1 1Gi + c2 1Gi + pod 级固定开销 1Gi + 每容器开销 2×500Mi = 4072Mi
			},
		},
		{
			description: "Pod with DRA claim specifying both Mapping and Overhead mapping",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "claim-1",
							ResourceClaimName: new("node-allocatable-claim-1"),
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim-1",
							Containers:        []string{"c1", "c2"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{
									Name:     v1.ResourceMemory,
									Quantity: new(resource.MustParse("2Gi")),
								},
							},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:         v1.ResourceMemory,
									PerPod:       new(resource.MustParse("1Gi")),
									PerContainer: new(resource.MustParse("500Mi")),
								},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("6120Mi"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("6120Mi"),
			},
		},
		{
			description: "Pod with DRA claim specifying Overhead mapping, PerPod only",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim",
							Containers:        []string{"c1"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:   v1.ResourceMemory,
									PerPod: new(resource.MustParse("1Gi")),
								},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2Gi"), // c1 1Gi + pod 级固定开销 1Gi = 2Gi
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2Gi"), // c1 1Gi + pod 级固定开销 1Gi = 2Gi
			},
		},
		{
			description: "Pod with DRA claim specifying Overhead mapping, PerContainer only",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim",
							Containers:        []string{"c1"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:         v1.ResourceMemory,
									PerContainer: new(resource.MustParse("500Mi")),
								},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("1524Mi"), // c1 1Gi + 每容器开销 500Mi = 1524Mi
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("1524Mi"), // c1 1Gi + 每容器开销 500Mi = 1524Mi
			},
		},
		{
			description: "Pod with unreferenced DRA claim specifying Mapping",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "unref-direct-claim",
							Containers:        []string{},
							Mapping: []v1.NodeAllocatableMappedResources{
								{
									Name:     v1.ResourceMemory,
									Quantity: new(resource.MustParse("2Gi")),
								},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("3Gi"), // c1 1Gi + 未被容器引用的声明映射 2Gi = 3Gi
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("3Gi"), // c1 1Gi + 未被容器引用的声明映射 2Gi = 3Gi
			},
		},
		{
			description: "Pod with unreferenced DRA claim specifying Overhead mapping",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "unref-overhead-claim",
							Containers:        []string{},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:         v1.ResourceMemory,
									PerPod:       new(resource.MustParse("1Gi")),
									PerContainer: new(resource.MustParse("500Mi")),
								},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2Gi"), // c1 1Gi + PerPod 开销 1Gi + PerContainer 开销 0×500Mi = 2Gi
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("2Gi"), // c1 1Gi + PerPod 开销 1Gi + PerContainer 开销 0×500Mi = 2Gi
			},
		},
		{
			description: "Pod with unreferenced DRA claim specifying Overhead mapping, PerContainer only",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "unref-overhead-percontainer-claim",
							Containers:        []string{},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:         v1.ResourceMemory,
									PerContainer: new(resource.MustParse("500Mi")),
								},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("1Gi"), // c1 1Gi + PerContainer 开销 0×500Mi = 1Gi
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("1Gi"), // c1 1Gi + PerContainer 开销 0×500Mi = 1Gi
			},
		},
		{
			description: "shared DRA claim + memory resize (spec != status) + UseStatusResources",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")}, // 从 2Gi 缩容下来
								Claims: []v1.ResourceClaim{
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "shared-mem-claim-ref",
							ResourceClaimName: new("shared-mem-claim"),
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "c1",
							Resources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")}, // 旧的 actuated limit（实际已生效值）
							},
							AllocatedResources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")}, // 新的 allocated limit（已分配值）
						},
						{
							Name: "c2",
							Resources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
							},
							AllocatedResources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
						},
					},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("6Gi")},
					},
					AllocatedResources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("5Gi")},
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "shared-mem-claim",
							Containers:        []string{"c1", "c2"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{Name: v1.ResourceMemory, PerContainer: new(resource.MustParse("1Gi"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{
				UseDRANodeAllocatableResourceClaimStatus:       true,
				UseStatusResources:                             true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("6Gi"), // c1 三方取最大 max(spec 1Gi, allocated 1Gi, actuated 2Gi) = 2Gi，加 c2 的 2Gi，再加两个容器共享的 1Gi DRA 声明 = 6Gi
			},
		},
		{
			description: "Pod Level Resources for CPU + shared DRA claims + UseStatusResources",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")},
					},
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-cpu-claim-ref"},
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-cpu-claim-ref"},
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "shared-cpu-claim-ref",
							ResourceClaimName: new("shared-cpu-claim"),
						},
						{
							Name:              "shared-mem-claim-ref",
							ResourceClaimName: new("shared-mem-claim"),
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "c1",
							Resources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
							},
							AllocatedResources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
						},
						{
							Name: "c2",
							Resources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
							},
							AllocatedResources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
						},
					},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3"), v1.ResourceMemory: resource.MustParse("5Gi")}, // CPU 取 pod 级 spec 的 3；内存取容器 spec 加 DRA 折算的 5Gi
					},
					AllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3"), v1.ResourceMemory: resource.MustParse("5Gi")},
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "shared-cpu-claim",
							Containers:        []string{"c1", "c2"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("1"))},
							},
						},
						{
							ResourceClaimName: "shared-mem-claim",
							Containers:        []string{"c1", "c2"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{Name: v1.ResourceMemory, PerContainer: new(resource.MustParse("1Gi"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{
				UseDRANodeAllocatableResourceClaimStatus:       true,
				UseStatusResources:                             true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("5Gi"),
			},
		},
		{
			description: "Pod Level Resources + shared DRA claim during resize (spec != status) + UseStatusResources",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("4")}, // 从 3 扩容上来
					},
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-cpu-claim-ref"},
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-cpu-claim-ref"},
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "shared-cpu-claim-ref",
							ResourceClaimName: new("shared-cpu-claim"),
						},
						{
							Name:              "shared-mem-claim-ref",
							ResourceClaimName: new("shared-mem-claim"),
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "c1",
							Resources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
							},
							AllocatedResources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
						},
						{
							Name: "c2",
							Resources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
							},
							AllocatedResources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
						},
					},
					Resources: &v1.ResourceRequirements{
						// 与 spec 不一致：status 里旧的 CPU allocated 值为 3
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3"), v1.ResourceMemory: resource.MustParse("5Gi")},
					},
					AllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3"), v1.ResourceMemory: resource.MustParse("5Gi")},
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "shared-cpu-claim",
							Containers:        []string{"c1", "c2"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("1"))},
							},
						},
						{
							ResourceClaimName: "shared-mem-claim",
							Containers:        []string{"c1", "c2"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{Name: v1.ResourceMemory, PerContainer: new(resource.MustParse("1Gi"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{
				UseDRANodeAllocatableResourceClaimStatus:       true,
				UseStatusResources:                             true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("4"),
				v1.ResourceMemory: resource.MustParse("5Gi"),
			},
		},
		{
			description: "Pod Level Resources (CPU limits only) with shared DRA claim and UseStatusResources",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")},
					},
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-cpu-claim-ref"},
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-cpu-claim-ref"},
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "shared-cpu-claim-ref",
							ResourceClaimName: new("shared-cpu-claim"),
						},
						{
							Name:              "shared-mem-claim-ref",
							ResourceClaimName: new("shared-mem-claim"),
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "c1",
							Resources: &v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
							},
						},
						{
							Name: "c2",
							Resources: &v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
							},
						},
					},
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3"), v1.ResourceMemory: resource.MustParse("5Gi")},
					},
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "shared-cpu-claim",
							Containers:        []string{"c1", "c2"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("1"))},
							},
						},
						{
							ResourceClaimName: "shared-mem-claim",
							Containers:        []string{"c1", "c2"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{Name: v1.ResourceMemory, PerContainer: new(resource.MustParse("1Gi"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{
				UseDRANodeAllocatableResourceClaimStatus:       true,
				UseStatusResources:                             true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("5Gi"),
			},
		},
		{
			description: "Pod Level Resources (CPU limits only) with shared DRA claim during resize and UseStatusResources",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("4")},
					},
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-cpu-claim-ref"},
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-cpu-claim-ref"},
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "shared-cpu-claim-ref",
							ResourceClaimName: new("shared-cpu-claim"),
						},
						{
							Name:              "shared-mem-claim-ref",
							ResourceClaimName: new("shared-mem-claim"),
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "c1",
							Resources: &v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
							},
						},
						{
							Name: "c2",
							Resources: &v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
							},
						},
					},
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3"), v1.ResourceMemory: resource.MustParse("5Gi")}, // status 里旧的 CPU allocated 值为 3
					},
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "shared-cpu-claim",
							Containers:        []string{"c1", "c2"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("1"))},
							},
						},
						{
							ResourceClaimName: "shared-mem-claim",
							Containers:        []string{"c1", "c2"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{Name: v1.ResourceMemory, PerContainer: new(resource.MustParse("1Gi"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{
				UseDRANodeAllocatableResourceClaimStatus:       true,
				UseStatusResources:                             true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("4"),
				v1.ResourceMemory: resource.MustParse("5Gi"),
			},
		},
		{
			description: "Container Level Resources with shared DRA claim during memory limit resize and UseStatusResources",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("4Gi")}, // 从 2Gi 扩容上来
								Claims: []v1.ResourceClaim{
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "shared-mem-claim-ref",
							ResourceClaimName: new("shared-mem-claim"),
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "c1",
							Resources: &v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")}, // 旧的 actuated limit（实际已生效值）
							},
						},
						{
							Name: "c2",
							Resources: &v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
							},
						},
					},
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "shared-mem-claim",
							Containers:        []string{"c1", "c2"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{Name: v1.ResourceMemory, PerContainer: new(resource.MustParse("1Gi"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{
				UseDRANodeAllocatableResourceClaimStatus:       true,
				UseStatusResources:                             true,
				InPlacePodLevelResourcesVerticalScalingEnabled: false, // 关闭 pod 级垂直扩缩容：走容器级 resize 路径
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("8Gi"), // c1 取 max(spec 4Gi, actuated 2Gi) = 4Gi，加 c2 的 2Gi 得 6Gi，再加两容器各 1Gi 的 DRA 得 8Gi
			},
		},
		{
			description: "Container Level Resources + shared DRA claim during memory resize + UseStatusResources",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("4Gi")}, // 从 2Gi 扩容上来
								Claims: []v1.ResourceClaim{
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
						{
							Name: "c2",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
								Claims: []v1.ResourceClaim{
									{Name: "shared-mem-claim-ref"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "shared-mem-claim-ref",
							ResourceClaimName: new("shared-mem-claim"),
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "c1",
							Resources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")}, // 旧的 actuated request（实际生效值）
							},
							AllocatedResources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("4Gi")}, // 新的 allocated request（已分配值）
						},
						{
							Name: "c2",
							Resources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
							},
							AllocatedResources: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")},
						},
					},
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "shared-mem-claim",
							Containers:        []string{"c1", "c2"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{Name: v1.ResourceMemory, PerContainer: new(resource.MustParse("1Gi"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{
				UseDRANodeAllocatableResourceClaimStatus:       true,
				UseStatusResources:                             true,
				InPlacePodLevelResourcesVerticalScalingEnabled: false, // 关闭 pod 级垂直扩缩容：走容器级 resize 路径
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("8Gi"), // c1 三方取最大 max(spec 4Gi, allocated 4Gi, actuated 2Gi) = 4Gi，加 c2 的 2Gi 得 6Gi，再加两容器各 1Gi 的 DRA 得 8Gi
			},
		},
		{
			// 这两条区分 limits 的例外：cpu/内存没声明 limit 就不加 DRA，hugepages 则无条件加。
			description: "DRA cpu/memory not added to limits when container limits are omitted",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("100m"),
									v1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "claim-1",
							ResourceClaimName: new("node-allocatable-claim-1"),
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim-1",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("50m"))},
								{Name: v1.ResourceMemory, Quantity: new(resource.MustParse("512Mi"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("150m"),   // 容器 100m 加 DRA 映射 50m
				v1.ResourceMemory: resource.MustParse("1536Mi"), // 容器 1Gi 加 DRA 映射 512Mi
			},
			expectedLimits: v1.ResourceList{}, // 容器没声明任何 limit，DRA 折算值也不会加到 limits 上
		},
		{
			description: "DRA hugepages added to limits even when container limits are omitted",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("100m"),
								},
								Claims: []v1.ResourceClaim{
									{Name: "claim-1"},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              "claim-1",
							ResourceClaimName: new("node-allocatable-claim-1"),
						},
					},
				},
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "node-allocatable-claim-1",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{Name: hugePageResource1Gi, Quantity: new(resource.MustParse("1"))},
							},
						},
					},
				},
			},
			options: PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:      resource.MustParse("100m"),
				hugePageResource1Gi: resource.MustParse("1"),
			},
			expectedLimits: v1.ResourceList{
				hugePageResource1Gi: resource.MustParse("1"), // hugepages 即使没声明 limit 也要加进去（limits 的例外规则）
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			if tc.expectedRequests != nil {
				requests := PodRequests(tc.pod, tc.options)
				if !equality.Semantic.DeepEqual(requests, tc.expectedRequests) {
					t.Errorf("PodRequests() mismatch (-want +got):\n%s", diff.Diff(tc.expectedRequests, requests))
				}
			}
			if tc.expectedLimits != nil {
				limits := PodLimits(tc.pod, tc.options)
				if !equality.Semantic.DeepEqual(limits, tc.expectedLimits) {
					t.Errorf("PodLimits() mismatch (-want +got):\n%s", diff.Diff(tc.expectedLimits, limits))
				}
			}
		})
	}
}

// TestAggregateContainerRequestsAndLimits 直接测聚合函数本身，
// 不含 overhead 和 pod 级覆盖，因此能把 sidecar KEP 的公式单独拎出来看：
// 普通容器求和，init 容器取峰值，restartable init 既进总和又参与后续 init 的峰值计算。
// 两个 max of sums case 用来锁定 UseStatusResources 下的顺序：
// 先按来源在容器间求和，再跨来源取最大；最后几个 case 覆盖 PLR 垂直扩缩容时
// 直接读 pod 级 status，以及字段缺失时回退到容器级聚合。
func TestAggregateContainerRequestsAndLimits(t *testing.T) {
	restartAlways := v1.ContainerRestartPolicyAlways
	cases := []struct {
		options               PodResourcesOptions
		containers            []v1.Container
		containerStatuses     []v1.ContainerStatus
		initContainers        []v1.Container
		initContainerStatuses []v1.ContainerStatus
		podAllocatedResources v1.ResourceList
		podResources          *v1.ResourceRequirements
		name                  string
		expectedRequests      v1.ResourceList
		expectedLimits        v1.ResourceList
	}{
		{
			name: "one container with limits",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			expectedRequests: v1.ResourceList{},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("9"),
			},
		},
		{
			name: "two containers with limits",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			expectedRequests: v1.ResourceList{},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("18"),
			},
		},
		{
			name: "one container with requests",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("9"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			name: "two containers with requests",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("18"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			name: "regular and init containers with requests",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("9"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			name: "regular, init and sidecar containers with requests",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("8")},
					},
					RestartPolicy: &restartAlways,
				},
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("6")},
					},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("17"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			name: "regular and init containers with limits",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			expectedRequests: v1.ResourceList{},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("9"),
			},
		},
		{
			name: "regular, init and sidecar containers with limits",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("9")},
					},
				},
			},
			initContainers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("8")},
					},
					RestartPolicy: &restartAlways,
				},
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceName(v1.ResourceCPU): resource.MustParse("6")},
					},
				},
			},
			expectedRequests: v1.ResourceList{},
			expectedLimits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU): resource.MustParse("17"),
			},
		},
		{
			name:    "regularcontainers with empty requests, but status with non-empty requests",
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name:      "container-1",
					Resources: v1.ResourceRequirements{},
				},
			},
			containerStatuses: []v1.ContainerStatus{
				{
					Name: "container-1",
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("2"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			name:    "always-restart init containers with empty requests, but status with non-empty requests",
			options: PodResourcesOptions{UseStatusResources: true},
			initContainers: []v1.Container{
				{
					Name:          "container-1",
					RestartPolicy: ptr.To[v1.ContainerRestartPolicy](v1.ContainerRestartPolicyAlways),
					Resources:     v1.ResourceRequirements{},
				},
			},
			initContainerStatuses: []v1.ContainerStatus{
				{
					Name: "container-1",
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("2"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			// 聚合顺序的固定点：先求和再取最大，反过来算会得到错误的总量。
			name:    "aggregate container request resolves max of sums instead of sum of maxes",
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
				{
					Name: "c2",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
				},
			},
			containerStatuses: []v1.ContainerStatus{
				{
					Name:               "c1",
					AllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
				},
				{
					Name:               "c2",
					AllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("3"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			name:    "aggregate container limit resolves max of sums instead of sum of maxes",
			options: PodResourcesOptions{UseStatusResources: true},
			containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
				{
					Name: "c2",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
				},
			},
			containerStatuses: []v1.ContainerStatus{
				{
					Name: "c1",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
				},
				{
					Name: "c2",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
			},
			expectedRequests: v1.ResourceList{},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("3"),
			},
		},
		{
			name: "aggregate container request/limit resolves directly from pod status when PLR vertical scaling enabled",
			options: PodResourcesOptions{
				UseStatusResources: true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
				},
			},
			podAllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("4")},
			podResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")},
				Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("5")},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("5"),
			},
		},
		{
			name: "falls back to container status aggregation when PLR vertical scaling enabled but podAllocatedResources is nil",
			options: PodResourcesOptions{
				UseStatusResources: true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
				},
			},
			containerStatuses: []v1.ContainerStatus{
				{
					Name:               "c1",
					AllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("6")},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("5")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("7")},
					},
				},
			},
			podAllocatedResources: nil,
			podResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")},
				Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("7")},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("6"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("7"),
			},
		},
		{
			name: "falls back to container status aggregation when PLR vertical scaling enabled but podResources is nil",
			options: PodResourcesOptions{
				UseStatusResources: true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
				},
			},
			containerStatuses: []v1.ContainerStatus{
				{
					Name:               "c1",
					AllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("6")},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("5")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("8")},
					},
				},
			},
			podAllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("6")},
			podResources:          nil,
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("6"),
			},
			expectedLimits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("8"),
			},
		},
	}

	for idx, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testPod := &v1.Pod{
				Spec: v1.PodSpec{Containers: tc.containers, InitContainers: tc.initContainers},
				Status: v1.PodStatus{
					ContainerStatuses:     tc.containerStatuses,
					InitContainerStatuses: tc.initContainerStatuses,
					AllocatedResources:    tc.podAllocatedResources,
					Resources:             tc.podResources,
				},
			}
			resRequests := AggregateContainerRequests(testPod, tc.options)
			resLimits := AggregateContainerLimits(testPod, tc.options)

			if !equality.Semantic.DeepEqual(tc.expectedRequests, resRequests) {
				t.Errorf("test case failure[%d]: %v, requests:\n expected:\t%v\ngot\t\t%v", idx, tc.name, tc.expectedRequests, resRequests)
			}

			if !equality.Semantic.DeepEqual(tc.expectedLimits, resLimits) {
				t.Errorf("test case failure[%d]: %v, limits:\n expected:\t%v\ngot\t\t%v", idx, tc.name, tc.expectedLimits, resLimits)
			}
		})
	}
}

// getPod 用的资源速记：把 request/limit/overhead 收进一个结构，
// 表驱动时不必为每种组合写一遍嵌套的 ResourceList。
type podResources struct {
	cpuRequest, cpuLimit, memoryRequest, memoryLimit, cpuOverhead, memoryOverhead string
}

// 统一构造带 pod 级资源的 Pod，避免每个 case 重复手写 Spec.Resources 的指针。
func getPodLevelResourcesPod(podResources v1.ResourceRequirements, overhead v1.ResourceList, containers, initContainers []v1.Container) *v1.Pod {
	return &v1.Pod{
		Spec: v1.PodSpec{
			Resources:      &podResources,
			Containers:     containers,
			InitContainers: initContainers,
			Overhead:       overhead,
		},
	}
}

// TODO(ndixita): refactor to re-use getPodResourcesPod()
// 造一个普通容器加同构 init 容器的 pod：两者资源相同，
// 于是 init 取峰值的结果就等于单容器值，overhead 的累加效果不会被 init 逻辑掩盖。
func getPod(cname string, resources podResources) *v1.Pod {
	r := v1.ResourceRequirements{
		Limits:   make(v1.ResourceList),
		Requests: make(v1.ResourceList),
	}

	overhead := make(v1.ResourceList)

	if resources.cpuLimit != "" {
		r.Limits[v1.ResourceCPU] = resource.MustParse(resources.cpuLimit)
	}
	if resources.memoryLimit != "" {
		r.Limits[v1.ResourceMemory] = resource.MustParse(resources.memoryLimit)
	}
	if resources.cpuRequest != "" {
		r.Requests[v1.ResourceCPU] = resource.MustParse(resources.cpuRequest)
	}
	if resources.memoryRequest != "" {
		r.Requests[v1.ResourceMemory] = resource.MustParse(resources.memoryRequest)
	}
	if resources.cpuOverhead != "" {
		overhead[v1.ResourceCPU] = resource.MustParse(resources.cpuOverhead)
	}
	if resources.memoryOverhead != "" {
		overhead[v1.ResourceMemory] = resource.MustParse(resources.memoryOverhead)
	}

	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      cname,
					Resources: r,
				},
			},
			InitContainers: []v1.Container{
				{
					Name:      "init-" + cname,
					Resources: r,
				},
			},
			Overhead: overhead,
		},
	}
}

// TestPodResourceLimitsWithSharedDRAClaims 单独盯共享 DRA claim 的去重：
// PerContainer 开销乘以声明覆盖的容器数，但 Kubelet 写进 pod 级 status 的 limit
// 已经是去重后的值，不能再加一遍。三段对比（无 status、有 status、resize 中）
// 放在同一个测试里，是为了把这个前提和 resize 时取 spec 与 status 较大者一起固定住。
func TestPodResourceLimitsWithSharedDRAClaims(t *testing.T) {
	testPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
				{
					Name: "c2",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "c1",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("5Gi"), // spec 的 2Gi 加 DRA 的 3Gi
						},
					},
				},
				{
					Name: "c2",
					Resources: &v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("5Gi"), // spec 的 2Gi 加 DRA 的 3Gi
						},
					},
				},
			},
			NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
				{
					ResourceClaimName: "shared-claim",
					Containers:        []string{"c1", "c2"},
					Overhead: []v1.NodeAllocatableOverheadResources{
						{
							Name:         v1.ResourceMemory,
							PerContainer: new(resource.MustParse("3Gi")),
						},
					},
				},
			},
		},
	}

	// 场景 1：不读 status。
	// spec 聚合 = 2Gi (c1) + 2Gi (c2) = 4Gi；
	// 去重后的 DRA 开销 = 2 个容器 × 3Gi = 6Gi；期望 limits = 4Gi + 6Gi = 10Gi。
	limitsNoStatus := PodLimits(testPod, PodResourcesOptions{
		UseDRANodeAllocatableResourceClaimStatus: true,
		UseStatusResources:                       false,
	})
	expectedNoStatus := v1.ResourceList{
		v1.ResourceMemory: resource.MustParse("10Gi"),
	}
	if diff := diff.Diff(expectedNoStatus, limitsNoStatus); diff != "" {
		t.Errorf("PodLimits without status mismatch (-want +got):\n%s", diff)
	}

	// 场景 2：读 status，且没有 resize 进行中。
	// Kubelet 已把去重后的 limit 10Gi 写进 pod 级 status；
	// 期望取 max(spec + DRA 的 10Gi, status.Resources.Limits 的 10Gi) = 10Gi。
	testPodWithStatus := testPod.DeepCopy()
	testPodWithStatus.Status.Resources = &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("10Gi"),
		},
	}
	limitsWithStatus := PodLimits(testPodWithStatus, PodResourcesOptions{
		UseDRANodeAllocatableResourceClaimStatus:       true,
		UseStatusResources:                             true,
		InPlacePodLevelResourcesVerticalScalingEnabled: true,
	})
	expectedWithStatus := v1.ResourceList{
		v1.ResourceMemory: resource.MustParse("10Gi"),
	}
	if diff := diff.Diff(expectedWithStatus, limitsWithStatus); diff != "" {
		t.Errorf("PodLimits with status mismatch (-want +got):\n%s", diff)
	}

	// 场景 3：读 status 且 c1 正在把 limit 从 2Gi 扩到 3Gi。
	// spec 聚合 = 3Gi + 2Gi = 5Gi；去重 DRA 开销 6Gi；基准 limit = 11Gi。
	// Kubelet 写的 status limit 仍是旧值 10Gi；期望取 max(11Gi, 10Gi) = 11Gi。
	testPodResize := testPod.DeepCopy()
	testPodResize.Spec.Containers[0].Resources.Limits[v1.ResourceMemory] = resource.MustParse("3Gi")
	testPodResize.Status.Resources = &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("10Gi"),
		},
	}
	limitsResize := PodLimits(testPodResize, PodResourcesOptions{
		UseDRANodeAllocatableResourceClaimStatus:       true,
		UseStatusResources:                             true,
		InPlacePodLevelResourcesVerticalScalingEnabled: true,
	})
	expectedResize := v1.ResourceList{
		v1.ResourceMemory: resource.MustParse("11Gi"),
	}
	if diff := diff.Diff(expectedResize, limitsResize); diff != "" {
		t.Errorf("PodLimits resize mismatch (-want +got):\n%s", diff)
	}
}

// TestPodResourceRequestsWithSharedDRAClaims 是 requests 版本的去重测试，
// 比 limits 多一个 AllocatedResources：请求侧要比较 spec、allocated、actuated 三方，
// 所以三段场景里都显式写入这个字段，避免走回退分支掩盖问题。
func TestPodResourceRequestsWithSharedDRAClaims(t *testing.T) {
	testPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
						Claims: []v1.ResourceClaim{
							{Name: "shared-claim-ref"},
						},
					},
				},
				{
					Name: "c2",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
						Claims: []v1.ResourceClaim{
							{Name: "shared-claim-ref"},
						},
					},
				},
			},
			ResourceClaims: []v1.PodResourceClaim{
				{
					Name:              "shared-claim-ref",
					ResourceClaimName: new("shared-claim"),
				},
			},
		},
		Status: v1.PodStatus{
			NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
				{
					ResourceClaimName: "shared-claim",
					Containers:        []string{"c1", "c2"},
					Overhead: []v1.NodeAllocatableOverheadResources{
						{
							Name:         v1.ResourceMemory,
							PerContainer: new(resource.MustParse("3Gi")),
						},
					},
				},
			},
		},
	}

	// 场景 1：不读 status。
	// spec 聚合 = c1 2Gi + c2 2Gi = 4Gi；
	// 去重后的 DRA 开销 = 2 个容器 × 3Gi = 6Gi；期望 requests = 4Gi + 6Gi = 10Gi。
	requestsNoStatus := PodRequests(testPod, PodResourcesOptions{
		UseDRANodeAllocatableResourceClaimStatus: true,
		UseStatusResources:                       false,
	})
	expectedNoStatus := v1.ResourceList{
		v1.ResourceMemory: resource.MustParse("10Gi"),
	}
	if diff := diff.Diff(expectedNoStatus, requestsNoStatus); diff != "" {
		t.Errorf("PodRequests without status mismatch (-want +got):\n%s", diff)
	}

	// 场景 2：读 status，且没有 resize 进行中。
	// Kubelet 已把去重后的 request 10Gi 写进 pod 级 status；
	// 期望取 max(spec + DRA 的 10Gi, status.Resources.Requests 的 10Gi) = 10Gi。
	testPodWithStatus := testPod.DeepCopy()
	testPodWithStatus.Status.Resources = &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("10Gi"),
		},
	}
	testPodWithStatus.Status.AllocatedResources = v1.ResourceList{
		v1.ResourceMemory: resource.MustParse("10Gi"),
	}
	requestsWithStatus := PodRequests(testPodWithStatus, PodResourcesOptions{
		UseDRANodeAllocatableResourceClaimStatus:       true,
		UseStatusResources:                             true,
		InPlacePodLevelResourcesVerticalScalingEnabled: true,
	})
	expectedWithStatus := v1.ResourceList{
		v1.ResourceMemory: resource.MustParse("10Gi"),
	}
	if diff := diff.Diff(expectedWithStatus, requestsWithStatus); diff != "" {
		t.Errorf("PodRequests with status mismatch (-want +got):\n%s", diff)
	}

	// 场景 3：读 status 且 c1 正在把 request 从 2Gi 扩到 3Gi。
	// spec 聚合 = c1 3Gi + c2 2Gi = 5Gi；DRA 开销仍为 6Gi；
	// spec 侧基线 request = 5Gi + 6Gi = 11Gi；Kubelet 写入的 status request 还是旧值 10Gi；
	// 期望取两者较大值 max(11Gi, 10Gi) = 11Gi。
	testPodResize := testPod.DeepCopy()
	testPodResize.Spec.Containers[0].Resources.Requests[v1.ResourceMemory] = resource.MustParse("3Gi")
	testPodResize.Status.Resources = &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("10Gi"),
		},
	}
	testPodResize.Status.AllocatedResources = v1.ResourceList{
		v1.ResourceMemory: resource.MustParse("10Gi"),
	}
	requestsResize := PodRequests(testPodResize, PodResourcesOptions{
		UseDRANodeAllocatableResourceClaimStatus:       true,
		UseStatusResources:                             true,
		InPlacePodLevelResourcesVerticalScalingEnabled: true,
	})
	expectedResize := v1.ResourceList{
		v1.ResourceMemory: resource.MustParse("11Gi"),
	}
	if diff := diff.Diff(expectedResize, requestsResize); diff != "" {
		t.Errorf("PodRequests resize mismatch (-want +got):\n%s", diff)
	}
}

// 测试里手工构造的 Pod 没有 Kubelet 回填的 pod 级 status，
// 这个 helper 把容器状态聚合成 pod 级 Requests/Limits/AllocatedResources，
// 让 UseStatusResources 分支能按真实路径走到。
// 如果 case 已经显式给了 Status.Resources 就原样保留，不覆盖用例想表达的旧值。
func populatePodStatusResourcesForTest(pod *v1.Pod) {
	if pod.Status.Resources != nil {
		return
	}

	pod.Status.Resources = &v1.ResourceRequirements{
		Requests: make(v1.ResourceList),
		Limits:   make(v1.ResourceList),
	}
	pod.Status.AllocatedResources = make(v1.ResourceList)

	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Resources != nil {
			addResourceList(pod.Status.Resources.Requests, cs.Resources.Requests)
			addResourceList(pod.Status.Resources.Limits, cs.Resources.Limits)
		}
		if len(cs.AllocatedResources) > 0 {
			addResourceList(pod.Status.AllocatedResources, cs.AllocatedResources)
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Resources != nil {
			addResourceList(pod.Status.Resources.Requests, cs.Resources.Requests)
			addResourceList(pod.Status.Resources.Limits, cs.Resources.Limits)
		}
		if len(cs.AllocatedResources) > 0 {
			addResourceList(pod.Status.AllocatedResources, cs.AllocatedResources)
		}
	}
}

// TestGetContainerDRAAllocations 从单个容器视角看 DRA 分配：
// 只统计 Containers 里点名了该容器的 claim；Mapping 与 Overhead 可以同时存在，
// Overhead 里的 PerPod 与 PerContainer 会叠加；多个 claim 命中同一资源名时继续累加。
// 它与 pod 级 DRA 测试互补，这里不涉及跨容器去重，只关心这个容器拿到多少。
func TestGetContainerDRAAllocations(t *testing.T) {
	testCases := []struct {
		description   string
		pod           *v1.Pod
		containerName string
		expected      v1.ResourceList
	}{
		{
			description:   "no claims",
			containerName: "c1",
			pod: &v1.Pod{
				Status: v1.PodStatus{},
			},
			expected: make(v1.ResourceList),
		},
		{
			description:   "unreferenced claims",
			containerName: "c1",
			pod: &v1.Pod{
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "unreferenced-claim",
							Containers:        []string{"c2"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{
									Name:     v1.ResourceCPU,
									Quantity: new(resource.MustParse("2")),
								},
							},
						},
					},
				},
			},
			expected: make(v1.ResourceList),
		},
		{
			description:   "referenced direct claims",
			containerName: "c1",
			pod: &v1.Pod{
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "direct-claim",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{
									Name:     v1.ResourceCPU,
									Quantity: new(resource.MustParse("2")),
								},
							},
						},
					},
				},
			},
			expected: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("2"),
			},
		},
		{
			description:   "referenced overhead claim PerPod only",
			containerName: "c1",
			pod: &v1.Pod{
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "overhead-pod",
							Containers:        []string{"c1"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:   v1.ResourceMemory,
									PerPod: new(resource.MustParse("3Gi")),
								},
							},
						},
					},
				},
			},
			expected: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("3Gi"),
			},
		},
		{
			description:   "referenced overhead claim PerContainer only",
			containerName: "c1",
			pod: &v1.Pod{
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "overhead-container",
							Containers:        []string{"c1"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:         v1.ResourceMemory,
									PerContainer: new(resource.MustParse("1Gi")),
								},
							},
						},
					},
				},
			},
			expected: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
		{
			description:   "referenced overhead claim both PerPod and PerContainer",
			containerName: "c1",
			pod: &v1.Pod{
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "overhead-both",
							Containers:        []string{"c1"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:         v1.ResourceMemory,
									PerPod:       new(resource.MustParse("3Gi")),
									PerContainer: new(resource.MustParse("1Gi")),
								},
							},
						},
					},
				},
			},
			expected: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("4Gi"),
			},
		},
		{
			description:   "referenced claim with both mapping and overhead",
			containerName: "c1",
			pod: &v1.Pod{
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "combined-claim",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{
									Name:     v1.ResourceCPU,
									Quantity: new(resource.MustParse("2")),
								},
							},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:         v1.ResourceCPU,
									PerPod:       new(resource.MustParse("1")),
									PerContainer: new(resource.MustParse("500m")),
								},
							},
						},
					},
				},
			},
			expected: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("3500m"),
			},
		},
		{
			description:   "multiple claims referenced by same container",
			containerName: "c1",
			pod: &v1.Pod{
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "claim-1",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{
									Name:     v1.ResourceCPU,
									Quantity: new(resource.MustParse("1")),
								},
							},
						},
						{
							ResourceClaimName: "claim-2",
							Containers:        []string{"c1"},
							Overhead: []v1.NodeAllocatableOverheadResources{
								{
									Name:   v1.ResourceMemory,
									PerPod: new(resource.MustParse("2Gi")),
								},
							},
						},
					},
				},
			},
			expected: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
		},
		{
			description:   "multiple claims aggregating same resource type",
			containerName: "c1",
			pod: &v1.Pod{
				Status: v1.PodStatus{
					NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
						{
							ResourceClaimName: "claim-1",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{
									Name:     v1.ResourceCPU,
									Quantity: new(resource.MustParse("1")),
								},
							},
						},
						{
							ResourceClaimName: "claim-2",
							Containers:        []string{"c1"},
							Mapping: []v1.NodeAllocatableMappedResources{
								{
									Name:     v1.ResourceCPU,
									Quantity: new(resource.MustParse("2")),
								},
							},
						},
					},
				},
			},
			expected: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("3"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			res := GetContainerDRAAllocations(tc.pod, tc.containerName)
			if !equality.Semantic.DeepEqual(res, tc.expected) {
				t.Errorf("expected=%v, got=%v", tc.expected, res)
			}
		})
	}
}

// TestPodRequestsAndLimitsVerticalScalingWrappers 测最外层 wrapper 在 PLR
// 垂直扩缩容开关下的最终表现：pod 级 status 已填时直接采用；
// resize 不可行时丢掉 spec 只认 status；同时确认 NonMissingContainerRequests
// 对 init 容器和普通容器一视同仁，缺哪个资源补哪个。
func TestPodRequestsAndLimitsVerticalScalingWrappers(t *testing.T) {
	cases := []struct {
		name             string
		pod              *v1.Pod
		opts             PodResourcesOptions
		expectedRequests v1.ResourceList
		expectedLimits   v1.ResourceList
	}{
		{
			name: "pod level requests/limits with PLR vertical scaling enabled and status populated",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("4")},
					},
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
								Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
							},
						},
					},
				},
				Status: v1.PodStatus{
					AllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3.5")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("5")},
					},
				},
			},
			opts: PodResourcesOptions{
				UseStatusResources: true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			expectedRequests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3.5")},
			expectedLimits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("5")},
		},
		{
			name: "pod level requests/limits with PLR vertical scaling enabled and infeasible resize",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("10")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("20")},
					},
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
								Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
							},
						},
					},
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:   v1.PodResizePending,
							Status: v1.ConditionTrue,
							Reason: v1.PodReasonInfeasible,
						},
					},
					AllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("4")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("8")},
					},
				},
			},
			opts: PodResourcesOptions{
				UseStatusResources: true,
				InPlacePodLevelResourcesVerticalScalingEnabled: true,
			},
			expectedRequests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("4")},
			expectedLimits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("8")},
		},
		{
			name: "container limits aggregation with infeasible resize",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("10")},
							},
						},
					},
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:   v1.PodResizePending,
							Status: v1.ConditionTrue,
							Reason: v1.PodReasonInfeasible,
						},
					},
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "c1",
							Resources: &v1.ResourceRequirements{
								Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("6")},
							},
						},
					},
				},
			},
			opts: PodResourcesOptions{
				UseStatusResources: true,
			},
			expectedRequests: v1.ResourceList{},
			expectedLimits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("6")},
		},
		{
			name: "non missing container requests applied to init container",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Name: "i1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
							},
						},
					},
				},
			},
			opts: PodResourcesOptions{
				NonMissingContainerRequests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("100Mi"),
			},
			expectedLimits: v1.ResourceList{},
		},
		{
			name: "non missing container requests applied to regular container",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
							},
						},
					},
				},
			},
			opts: PodResourcesOptions{
				NonMissingContainerRequests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
			expectedRequests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("100Mi"),
			},
			expectedLimits: v1.ResourceList{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotReqs := PodRequests(tc.pod, tc.opts)
			if !equality.Semantic.DeepEqual(gotReqs, tc.expectedRequests) {
				t.Errorf("PodRequests() mismatch (-want +got):\n%s", diff.Diff(tc.expectedRequests, gotReqs))
			}
			gotLimits := PodLimits(tc.pod, tc.opts)
			if !equality.Semantic.DeepEqual(gotLimits, tc.expectedLimits) {
				t.Errorf("PodLimits() mismatch (-want +got):\n%s", diff.Diff(tc.expectedLimits, gotLimits))
			}
		})
	}
}

// TestResizeConditionsAndSupportedResources 是个小范围探针：
// 只看 Reason 就能区分 deferred 与 infeasible，以及 CPU 确实在支持集合里。
// 大表测试未必覆盖到条件判定的每一种取值，这里补一个直接断言防止它被改坏。
func TestResizeConditionsAndSupportedResources(t *testing.T) {
	pod := &v1.Pod{
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodResizePending,
					Status: v1.ConditionTrue,
					Reason: v1.PodReasonDeferred,
				},
			},
		},
	}
	if !IsPodResizeDeferred(pod) {
		t.Errorf("expected IsPodResizeDeferred to be true")
	}
	if IsPodResizeInfeasible(pod) {
		t.Errorf("expected IsPodResizeInfeasible to be false")
	}
	pod.Status.Conditions[0].Reason = "Other"
	if IsPodResizeDeferred(pod) {
		t.Errorf("expected IsPodResizeDeferred to be false")
	}

	if !SupportedPodLevelResources().Has(v1.ResourceCPU) {
		t.Errorf("expected SupportedPodLevelResources to contain CPU")
	}
}
