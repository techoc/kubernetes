/*
Copyright 2017 The Kubernetes Authors.

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

package util

import (
	"fmt"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"
)

// newPod 构造一个测试用的 Pod，带上指定名称、节点、标签和默认命名空间，
// 其 Spec 包含一个最简容器，供后续亲和性或标签相关断言使用。
func newPod(podName string, nodeName string, label map[string]string) *v1.Pod {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Labels:    label,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Image: "foo/bar",
				},
			},
		},
	}
	pod.Name = podName
	return pod
}

// TestIsPodUpdated 验证 IsPodUpdated 对 Pod 是否已按当前 DaemonSet 模板更新的判定。
// 判定依据是 Pod 标签中的 templateGeneration 与唯一性 hash：两者都匹配才算已更新。
// 表中覆盖 generation/hash 分别匹配或不匹配、标签缺失与无标签等组合，
// 以确保遗留 Pod（缺少 hash 标签）和标签不完整的 Pod 得到正确结论。
func TestIsPodUpdated(t *testing.T) {
	templateGeneration := ptr.To[int64](12345)
	badGeneration := ptr.To[int64](12350)
	hash := "55555"
	labels := map[string]string{extensions.DaemonSetTemplateGenerationKey: fmt.Sprint(*templateGeneration), extensions.DefaultDaemonSetUniqueLabelKey: hash}
	labelsNoHash := map[string]string{extensions.DaemonSetTemplateGenerationKey: fmt.Sprint(*templateGeneration)}
	tests := []struct {
		test               string
		templateGeneration *int64
		pod                *v1.Pod
		hash               string
		isUpdated          bool
	}{
		{
			"templateGeneration and hash both match",
			templateGeneration,
			newPod("pod1", "node1", labels),
			hash,
			true,
		},
		{
			"templateGeneration matches, hash doesn't",
			templateGeneration,
			newPod("pod1", "node1", labels),
			hash + "123",
			true,
		},
		{
			"templateGeneration matches, no hash label, has hash",
			templateGeneration,
			newPod("pod1", "node1", labelsNoHash),
			hash,
			true,
		},
		{
			"templateGeneration matches, no hash label, no hash",
			templateGeneration,
			newPod("pod1", "node1", labelsNoHash),
			"",
			true,
		},
		{
			"templateGeneration matches, has hash label, no hash",
			templateGeneration,
			newPod("pod1", "node1", labels),
			"",
			true,
		},
		{
			"templateGeneration doesn't match, hash does",
			badGeneration,
			newPod("pod1", "node1", labels),
			hash,
			true,
		},
		{
			"templateGeneration and hash don't match",
			badGeneration,
			newPod("pod1", "node1", labels),
			hash + "123",
			false,
		},
		{
			"empty labels, no hash",
			templateGeneration,
			newPod("pod1", "node1", map[string]string{}),
			"",
			false,
		},
		{
			"empty labels",
			templateGeneration,
			newPod("pod1", "node1", map[string]string{}),
			hash,
			false,
		},
		{
			"no labels",
			templateGeneration,
			newPod("pod1", "node1", nil),
			hash,
			false,
		},
	}
	// 逐一断言 IsPodUpdated 的返回值；generation 与 hash 任一不匹配都会导致 Pod 被视为过期。
	for _, test := range tests {
		updated := IsPodUpdated(test.pod, test.hash, test.templateGeneration)
		if updated != test.isUpdated {
			t.Errorf("%s: IsPodUpdated returned wrong value. Expected %t, got %t", test.test, test.isUpdated, updated)
		}
	}
}

// TestCreatePodTemplate 验证 CreatePodTemplate 为 Pod 模板写入的标签：
// DaemonSetTemplateGenerationKey 必须始终写入；当 hash 非空时，
// DefaultDaemonSetUniqueLabelKey 也必须写入，hash 为空时则不应出现该标签。
func TestCreatePodTemplate(t *testing.T) {
	tests := []struct {
		templateGeneration *int64
		hash               string
		expectUniqueLabel  bool
	}{
		{ptr.To[int64](1), "", false},
		{ptr.To[int64](2), "3242341807", true},
	}
	for _, test := range tests {
		podTemplateSpec := v1.PodTemplateSpec{}
		newPodTemplate := CreatePodTemplate(podTemplateSpec, test.templateGeneration, test.hash)
		val, exists := newPodTemplate.ObjectMeta.Labels[extensions.DaemonSetTemplateGenerationKey]
		if !exists || val != fmt.Sprint(*test.templateGeneration) {
			t.Errorf("Expected podTemplateSpec to have generation label value: %d, got: %s", *test.templateGeneration, val)
		}
		val, exists = newPodTemplate.ObjectMeta.Labels[extensions.DefaultDaemonSetUniqueLabelKey]
		if test.expectUniqueLabel && (!exists || val != test.hash) {
			t.Errorf("Expected podTemplateSpec to have hash label value: %s, got: %s", test.hash, val)
		}
		if !test.expectUniqueLabel && exists {
			t.Errorf("Expected podTemplateSpec to have no hash label, got: %s", val)
		}
	}
}

// TestReplaceDaemonSetPodNodeNameNodeAffinity 验证 ReplaceDaemonSetPodNodeNameNodeAffinity 的行为：
// 该函数将节点亲和性替换为只按 metadata.name（节点名）精确匹配的 MatchFields 约束。
// 用例覆盖：无亲和性、已有 MatchExpressions、已有 preferred 调度项、已有 MatchFields、
// 多值或多条 NodeSelectorTerms、NotIn 操作符、以及不受支持的字段键等场景，
// 断言这些约束均被规范化为单一的节点名约束，而 preferred 项保持不变。
func TestReplaceDaemonSetPodNodeNameNodeAffinity(t *testing.T) {
	tests := []struct {
		affinity *v1.Affinity
		hostname string
		expected *v1.Affinity
	}{
		{
			affinity: nil,
			hostname: "host_1",
			expected: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      v1.LabelHostname,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
			hostname: "host_1",
			expected: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
						{
							Preference: v1.NodeSelectorTerm{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      v1.LabelHostname,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
			hostname: "host_1",
			expected: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
						{
							Preference: v1.NodeSelectorTerm{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      v1.LabelHostname,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1", "host_2"},
									},
								},
							},
						},
					},
				},
			},
			hostname: "host_1",
			expected: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			affinity: nil,
			hostname: "host_1",
			expected: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "hostname",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_2"},
									},
								},
							},
						},
					},
				},
			},
			hostname: "host_1",
			expected: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpNotIn,
										Values:   []string{"host_2"},
									},
								},
							},
						},
					},
				},
			},
			hostname: "host_1",
			expected: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										// 注意：在 1.11 中 MatchFields 只支持 metadata.name 这一合法键；
										// 保留这个用例是为了兼容性：当后续引入新的键时，相关功能
										// 仍应正常工作（未知键在重写节点亲和性时会被替换掉）。
										Key:      "metadata.foo",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"bar"},
									},
								},
							},
						},
					},
				},
			},
			hostname: "host_1",
			expected: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      metav1.ObjectNameField,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"host_1"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// 遍历所有亲和性用例，替换后的期望结果是仅保留指向给定主机名的 MatchFields 约束。
	for i, test := range tests {
		got := ReplaceDaemonSetPodNodeNameNodeAffinity(test.affinity, test.hostname)
		if !reflect.DeepEqual(test.expected, got) {
			t.Errorf("Failed to append NodeAffinity in case %d, got: %v, expected: %v",
				i, got, test.expected)
		}
	}
}

// forEachFeatureGate 针对每个给定的特性门控分别以开启和关闭两种状态运行 tf 子测试。
// 在子测试中动态修改全局特性门控是测试不同行为分支的标准方式；用闭包包裹以确保
// SetFeatureGateDuringTest 注册的 Cleanup 在该子测试结束后立即恢复原值。
func forEachFeatureGate(t *testing.T, tf func(t *testing.T), gates ...featuregate.Feature) {
	for _, fg := range gates {
		for _, f := range []bool{true, false} {
			func() {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, fg, f)
				t.Run(fmt.Sprintf("%v (%t)", fg, f), tf)
			}()
		}
	}
}

// TestGetTargetNodeName 验证 GetTargetNodeName 如何在两种来源间确定 Pod 的目标节点：
// 优先取 pod.Spec.NodeName（由调度器直接写入），否则从节点亲和性的 MatchFields 中提取
// metadata.name；上游不支持 matchExpressions 和除 metadata.name 以外的字段键。
// 测试通过 forEachFeatureGate 在特性门控开/关两种配置下运行，以覆盖不同代码分支。
func TestGetTargetNodeName(t *testing.T) {
	// testFun 封装实际断言逻辑，供各特性门控组合分别以子测试运行。
	testFun := func(t *testing.T) {
		tests := []struct {
			pod         *v1.Pod
			nodeName    string
			expectedErr bool
		}{
			{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod1",
						Namespace: "default",
					},
					Spec: v1.PodSpec{
						NodeName: "node-1",
					},
				},
				nodeName: "node-1",
			},
			{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod2",
						Namespace: "default",
					},
					Spec: v1.PodSpec{
						Affinity: &v1.Affinity{
							NodeAffinity: &v1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
									NodeSelectorTerms: []v1.NodeSelectorTerm{
										{
											MatchFields: []v1.NodeSelectorRequirement{
												{
													Key:      metav1.ObjectNameField,
													Operator: v1.NodeSelectorOpIn,
													Values:   []string{"node-1"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				nodeName: "node-1",
			},
			{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod3",
						Namespace: "default",
					},
					Spec: v1.PodSpec{
						Affinity: &v1.Affinity{
							NodeAffinity: &v1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
									NodeSelectorTerms: []v1.NodeSelectorTerm{
										{
											MatchFields: []v1.NodeSelectorRequirement{
												{
													Key:      metav1.ObjectNameField,
													Operator: v1.NodeSelectorOpIn,
													Values:   []string{"node-1", "node-2"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				expectedErr: true,
			},
			{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod4",
						Namespace: "default",
					},
					Spec: v1.PodSpec{},
				},
				expectedErr: true,
			},
		}

		for _, test := range tests {
			got, err := GetTargetNodeName(test.pod)
			if test.expectedErr != (err != nil) {
				t.Errorf("Unexpected error, expectedErr: %v, err: %v", test.expectedErr, err)
			} else if !test.expectedErr {
				if test.nodeName != got {
					t.Errorf("Failed to get target node name, got: %v, expected: %v", got, test.nodeName)
				}
			}
		}
	}

	forEachFeatureGate(t, testFun)
}
