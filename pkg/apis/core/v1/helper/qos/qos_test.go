/*
Copyright 2016 The Kubernetes Authors.

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

package qos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper/qos"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/features"
)

// TestComputePodQOS 验证 ComputePodQOS 能根据容器（或 Pod 级）资源请求与限制，
// 正确地把 Pod 归类为 Guaranteed、Burstable 或 BestEffort。
//
// 为什么值得单独测试：QOS 类别直接影响 kubelet 的驱逐顺序和 cgroup 设置，
// 判定逻辑里有几处反直觉的边界，必须靠用例锁定：
//   - 瞬时存储（ephemeral-storage）虽是资源，但按设计不参与 QOS 计算；
//   - 显式声明为 0 的请求/限制，与完全不声明等价，都不计入 QOS；
//   - 普通容器与 initContainer 的归类规则一致；
//   - PodLevelResources 及其修复开关（FixKubeletQOSClass）开启/关闭时，
//     对“空资源”究竟回退到容器资源还是按 Pod 资源判定，行为不同。
//
// 每个用例同时验证外部版本和内部版本的 ComputePodQOS 结果一致，
// 防止 v1 与 core 两套实现产生漂移。
func TestComputePodQOS(t *testing.T) {
	type testCase struct {
		pod                                        *v1.Pod
		expected                                   v1.PodQOSClass
		podLevelResourcesEnabled                   bool
		podLevelResourcesFixKubeletQOSClassEnabled bool
	}

	guaranteedResources := v1.ResourceRequirements{
		// 瞬时存储请求不应影响 QOS 判定，所以这里故意给它一个大额请求，
		// 而 Limits 只含 CPU/内存；期望结果仍是 Guaranteed。
		Requests: v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("100m"),
			v1.ResourceMemory:           resource.MustParse("100Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
		},
		Limits: getResourceList("100m", "100Mi"),
	}
	// 穷举各种“看起来像有资源、实际应为 BestEffort”的写法：
	// 空资源、显式 0 值、Go 零值 Quantity，以及只设置瞬时存储（它不参与 QOS）。
	// 目的是锁定“0 == 未设置”以及“瞬时存储不计入 QOS”这两条判定规则。
	bestEffortResources := []v1.ResourceRequirements{
		{},
		{
			Requests: getResourceList("0", "0"),
		}, {
			// 请求与限制都显式为 0，仍视为没有任何有效资源。
			Requests: getResourceList("0", "0"),
			Limits:   getResourceList("0", "0"),
		}, {
			// 空的 Quantity 与未声明等价。
			Requests: v1.ResourceList{v1.ResourceCPU: resource.Quantity{}, v1.ResourceMemory: resource.Quantity{}},
		}, {
			// 只声明瞬时存储，不影响 QOS。
			Requests: v1.ResourceList{v1.ResourceEphemeralStorage: resource.MustParse("10Gi")},
		}, {
			Requests: v1.ResourceList{v1.ResourceEphemeralStorage: resource.MustParse("10Gi")},
			Limits:   v1.ResourceList{v1.ResourceEphemeralStorage: resource.MustParse("10Gi")},
		},
	}
	// Burstable 的判定要点：请求与限制不相等，或只设置其中一个。
	// 这里覆盖了 CPU/内存各自不等、单项不等、只设请求、以及只设部分限制等情况。
	burstableResources := []v1.ResourceRequirements{
		{
			Requests: getResourceList("100m", "100Mi"),
			Limits:   getResourceList("200m", "200Mi"),
		}, {
			Requests: getResourceList("100m", "100Mi"),
			Limits:   getResourceList("100m", "200Mi"),
		}, {
			Requests: getResourceList("100m", "100Mi"),
			Limits:   getResourceList("200m", "100Mi"),
		}, {
			Requests: getResourceList("100m", "100Mi"),
		}, {
			Requests: getResourceList("100m", "100Mi"),
			Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
		},
	}

	// 先为单容器 Pod 构造基线用例：每个资源模板各生成一个 Guaranteed/BestEffort/Burstable 用例。
	// 后面的 Pod 级资源、双容器等用例都会以这些基线为蓝本派生，避免重复定义。
	// 容器级别的测试用例
	singleContainerTests := []testCase{{
		pod: newPod("guaranteed", []v1.Container{
			{Resources: guaranteedResources},
		}),
		expected: v1.PodQOSGuaranteed,
	}}
	for i, res := range bestEffortResources {
		singleContainerTests = append(singleContainerTests, testCase{
			pod: newPod(fmt.Sprintf("best-effort-%d", i), []v1.Container{
				{Resources: res},
			}),
			expected: v1.PodQOSBestEffort,
		})
	}
	for i, res := range burstableResources {
		singleContainerTests = append(singleContainerTests, testCase{
			pod: newPod(fmt.Sprintf("burstable-%d", i), []v1.Container{
				{Resources: res},
			}),
			expected: v1.PodQOSBurstable,
		})
	}
	containerTests := slices.Clone(singleContainerTests)

	// 双容器用例：验证 Pod 的 QOS 取所有容器中最“宽松”的那一档
	// （Guaranteed + BestEffort 混合会降级为 Burstable）。
	// 两个容器的用例
	secondContainerTests := []testCase{{
		pod: newPod("2c-guaranteed", []v1.Container{
			{Resources: guaranteedResources},
			{Resources: guaranteedResources},
		}),
		expected: v1.PodQOSGuaranteed,
	}, {
		pod: newPod("2c-best-effort", []v1.Container{
			{Resources: bestEffortResources[0]},
			{Resources: bestEffortResources[1]},
		}),
		expected: v1.PodQOSBestEffort,
	}, {
		pod: newPod("2c-burstable", []v1.Container{
			{Resources: burstableResources[0]},
			{Resources: burstableResources[1]},
		}),
		expected: v1.PodQOSBurstable,
	}}
	// 混合用例（期望全部为 Burstable）：任取一个非 Burstable 与另一个不同档位的容器组合，
	// 只要两者档位不同，整体就应落为 Burstable；i == j 表示两容器档位相同，已由上面的用例覆盖，故跳过。
	// 混合用例（结果均为 Burstable）
	for i, res1 := range []v1.ResourceRequirements{guaranteedResources, bestEffortResources[0], burstableResources[0]} {
		for j, res2 := range []v1.ResourceRequirements{guaranteedResources, bestEffortResources[1], burstableResources[1]} {
			if i == j {
				continue
			}
			secondContainerTests = append(secondContainerTests, testCase{
				pod: newPod(fmt.Sprintf("2c-mixed-burstable-%d-%d", i, j), []v1.Container{
					{Resources: res1},
					{Resources: res2},
				}),
				expected: v1.PodQOSBurstable,
			})
		}
	}
	containerTests = append(containerTests, secondContainerTests...)

	// 派生 initContainer 变体：把双容器用例的第二个容器移到 InitContainers。
	// QOS 判定对普通容器和 initContainer 一视同仁，因此期望值保持不变，
	// 借此防止实现只对 Spec.Containers 求和而漏掉 initContainer。
	// 新增 initContainer 变体：第二个容器是否为 initContainer 不影响结果。
	for _, tc := range secondContainerTests {
		pod := tc.pod.DeepCopy()
		pod.Name += "-init"
		pod.Spec.InitContainers = pod.Spec.Containers[1:]
		pod.Spec.Containers = pod.Spec.Containers[:1]
		initTC := tc
		initTC.pod = pod
		containerTests = append(containerTests, initTC)
	}

	// 带 ephemeral container 的 Pod：临时容器没有资源字段，不应把 Guaranteed 拉低。
	// 带 ephemeral container 的 Pod：临时容器没有资源字段，不应把 Guaranteed 拉低。
	// 带临时容器的 Pod
	guaranteedECPod := newPod("guaranteed-ephemeral", []v1.Container{
		{Resources: guaranteedResources},
	})
	guaranteedECPod.Spec.EphemeralContainers = []v1.EphemeralContainer{{
		EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "ec"},
	}}
	containerTests = append(containerTests, testCase{
		pod:      guaranteedECPod,
		expected: v1.PodQOSGuaranteed,
	})

	tests := slices.Clone(containerTests)

	// 只打开 PodLevelResources 开关、却不设置 Pod 级资源字段的回归用例。
	// 没有 Pod 级资源时必须回退到按容器判定，结果应与原用例完全一致。
	// 开启 Pod 级资源但未设置 pod.spec.resources
	for _, tc := range containerTests {
		pod := tc.pod.DeepCopy()
		pod.Name += "-plr"
		plrTest := tc
		plrTest.pod = pod
		plrTest.podLevelResourcesEnabled = true
		plrTest.podLevelResourcesFixKubeletQOSClassEnabled = true
		tests = append(tests, plrTest)
	}

	// Pod 级资源（PLR）用例：把资源从容器挪到 Pod 层，验证判定以 Pod 级资源为准。
	// Pod 级资源的测试用例
	for _, tc := range singleContainerTests {
		// 变体 1：开启 PLR，容器设为 Guaranteed，但 Pod 级资源取自原容器的模板。
		// 若 IsPodLevelResourcesSet 判定 Pod 级资源为空（例如原模板是 BestEffort），
		// 则必须回退到容器资源，期望值改为 Guaranteed 并标记 -fallback。
		// 变体 1：开启 PLR，容器为 Guaranteed，同时设置 Pod 级资源
		pod := tc.pod.DeepCopy()
		pod.Name = "pod-" + pod.Name + "-plr"
		pod.Spec.Resources = pod.Spec.Containers[0].Resources.DeepCopy()
		pod.Spec.Containers[0].Resources = guaranteedResources
		expectedQOS := tc.expected
		if !resourcehelper.IsPodLevelResourcesSet(pod) {
			expectedQOS = v1.PodQOSGuaranteed
			pod.Name += "-fallback"
		}
		tests = append(tests, testCase{
			pod:                      pod,
			expected:                 expectedQOS,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: true,
		})

		// 变体 2：关闭 PLR，即使设了 Pod 级资源也必须忽略它，按容器资源判定。
		// 这是向后兼容的保证：特性关闭时行为与旧版本一致。
		// 变体 2：关闭 PLR，Pod 为 Guaranteed
		pod = tc.pod.DeepCopy()
		pod.Name += "-pod-no-plr"
		pod.Spec.Resources = &guaranteedResources
		tests = append(tests, testCase{
			pod:                      pod,
			expected:                 tc.expected,
			podLevelResourcesEnabled: false,
			podLevelResourcesFixKubeletQOSClassEnabled: false,
		})
	}

	// 显式空/零 Pod 级资源的边界用例，重点验证 FixKubeletQOSClass 开关带来的行为差异：
	// 开启修复后，ResourceList{} 或完全空的 ResourceRequirements 视为“未设置”，应回退到容器；
	// 而显式写 0 的请求/限制是有效设置，不回退，按 Pod 资源判为 BestEffort。
	// 关闭修复时则保留旧行为：即使容器是 Guaranteed，空 Pod 级资源也算 BestEffort。
	// 显式设置空/零 Pod 级资源的测试用例
	explicitPLRTests := []testCase{
		{
			pod: &v1.Pod{
				// 修复开启：Pod 级 Resources 的 Request/Limit 都是空 map，视为“未设置”，
				// 应回退到容器的 Guaranteed 资源。
				ObjectMeta: metav1.ObjectMeta{Name: "empty-pod-level-resources-should-fallback-to-containers"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "guaranteed", Resources: v1.ResourceRequirements{Requests: getResourceList("100m", "100Mi"), Limits: getResourceList("100m", "100Mi")}},
					},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{},
						Limits:   v1.ResourceList{},
					},
				},
			},
			expected:                 v1.PodQOSGuaranteed,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: true,
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "completely-empty-pod-level-resources-should-fallback-to-containers"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "guaranteed", Resources: v1.ResourceRequirements{Requests: getResourceList("100m", "100Mi"), Limits: getResourceList("100m", "100Mi")}},
					},
					Resources: &v1.ResourceRequirements{},
				},
			},
			expected:                 v1.PodQOSGuaranteed,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: true,
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "completely-empty-pod-level-resources-old-behavior"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "guaranteed", Resources: v1.ResourceRequirements{Requests: getResourceList("100m", "100Mi"), Limits: getResourceList("100m", "100Mi")}},
					},
					Resources: &v1.ResourceRequirements{},
				},
			},
			expected:                 v1.PodQOSBestEffort,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: false,
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "nil-pod-level-resources-should-fallback-to-containers"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "guaranteed", Resources: v1.ResourceRequirements{Requests: getResourceList("100m", "100Mi"), Limits: getResourceList("100m", "100Mi")}},
					},
				},
			},
			expected:                 v1.PodQOSGuaranteed,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: true,
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "zero-pod-level-resources-should-not-fallback-to-containers"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "guaranteed", Resources: v1.ResourceRequirements{Requests: getResourceList("100m", "100Mi"), Limits: getResourceList("100m", "100Mi")}},
					},
					Resources: &v1.ResourceRequirements{
						Requests: getResourceList("0", "0"),
						Limits:   getResourceList("0", "0"),
					},
				},
			},
			expected:                 v1.PodQOSBestEffort,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: true,
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "zero-pod-level-resources-should-not-fallback-to-containers-fix-disabled"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "guaranteed", Resources: v1.ResourceRequirements{Requests: getResourceList("100m", "100Mi"), Limits: getResourceList("100m", "100Mi")}},
					},
					Resources: &v1.ResourceRequirements{
						Requests: getResourceList("0", "0"),
						Limits:   getResourceList("0", "0"),
					},
				},
			},
			expected:                 v1.PodQOSBestEffort,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: false,
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "empty-pod-level-resources-old-behavior"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "guaranteed", Resources: v1.ResourceRequirements{Requests: getResourceList("100m", "100Mi"), Limits: getResourceList("100m", "100Mi")}},
					},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{},
						Limits:   v1.ResourceList{},
					},
				},
			},
			expected:                 v1.PodQOSBestEffort,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: false,
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "guaranteed-with-pod-level-resources-and-hugepages"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "best-effort"},
					},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:                   resource.MustParse("10m"),
							v1.ResourceMemory:                resource.MustParse("100Mi"),
							v1.ResourceName("hugepages-2Mi"): resource.MustParse("100Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:                   resource.MustParse("10m"),
							v1.ResourceMemory:                resource.MustParse("100Mi"),
							v1.ResourceName("hugepages-2Mi"): resource.MustParse("100Mi"),
						},
					},
				},
			},
			expected:                 v1.PodQOSGuaranteed,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: true,
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "burstable-with-pod-level-resources-and-hugepages"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "best-effort"},
					},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:                   resource.MustParse("10m"),
							v1.ResourceName("hugepages-2Mi"): resource.MustParse("100Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceName("hugepages-2Mi"): resource.MustParse("100Mi"),
						},
					},
				},
			},
			expected:                 v1.PodQOSBurstable,
			podLevelResourcesEnabled: true,
			podLevelResourcesFixKubeletQOSClassEnabled: true,
		},
	}
	tests = append(tests, explicitPLRTests...)

	for _, testCase := range tests {
		t.Run(testCase.pod.Name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, testCase.podLevelResourcesEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResourcesFixKubeletQOSClass, testCase.podLevelResourcesFixKubeletQOSClassEnabled)
			if actual := ComputePodQOS(testCase.pod); testCase.expected != actual {
				t.Errorf("ComputePodQOS error: expected: %s, actual: %s;\npod = %s", testCase.expected, actual, prettyPrintPod(testCase.pod))
			}

			// Convert v1.Pod to core.Pod, and then check against the internal version of `ComputePodQOS`.
			pod := core.Pod{}
			if err := corev1.Convert_v1_Pod_To_core_Pod(testCase.pod, &pod, nil); err != nil {
				t.Fatal(err)
			}

			if actual := v1.PodQOSClass(qos.ComputePodQOS(&pod)); testCase.expected != actual {
				t.Errorf("internal ComputePodQOS error: expected: %s, actual: %s;\npod = %s", testCase.expected, actual, prettyPrintPod(testCase.pod))
			}
		})
	}
}

func getResourceList(cpu, memory string) v1.ResourceList {
	res := v1.ResourceList{}
	if cpu != "" {
		res[v1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		res[v1.ResourceMemory] = resource.MustParse(memory)
	}
	return res
}

func newPod(name string, containers []v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
}

func prettyPrintPod(pod *v1.Pod) string {
	output, err := json.Marshal(pod)
	if err != nil {
		panic(err)
	}
	formatted := &bytes.Buffer{}
	if err := json.Indent(formatted, output, "", "  "); err != nil {
		panic(err)
	}
	return formatted.String()
}
