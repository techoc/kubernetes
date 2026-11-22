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

package antiaffinity

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// 验证硬 PodAntiAffinity 使用 kubernetes.io/hostname 以外的 TopologyKey 时会被拒绝。
// TODO: 待 RequiredDuringSchedulingRequiredDuringExecution 实现后，
// 补充该场景下非法 topologyKey 导致准入失败的测试用例。
func TestInterPodAffinityAdmission(t *testing.T) {
	handler := NewInterPodAntiAffinity()
	pod := api.Pod{
		Spec: api.PodSpec{},
	}
	tests := []struct {
		affinity      *api.Affinity
		errorExpected bool
	}{
		// 空的 affinity 应当通过。
		{
			affinity:      &api.Affinity{},
			errorExpected: false,
		},
		// 软反亲和的 topologyKey 不受限制，任意值都应通过。
		{
			affinity: &api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
						{
							Weight: 5,
							PodAffinityTerm: api.PodAffinityTerm{
								LabelSelector: &metav1.LabelSelector{
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "security",
											Operator: metav1.LabelSelectorOpIn,
											Values:   []string{"S2"},
										},
									},
								},
								TopologyKey: "az",
							},
						},
					},
				},
			},
			errorExpected: false,
		},
		// 硬反亲和的 topologyKey 合法，且软反亲和可带任意 topologyKey，整体应通过。
		{
			affinity: &api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []api.WeightedPodAffinityTerm{
						{
							Weight: 5,
							PodAffinityTerm: api.PodAffinityTerm{
								LabelSelector: &metav1.LabelSelector{
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "security",
											Operator: metav1.LabelSelectorOpIn,
											Values:   []string{"S2"},
										},
									},
								},
								TopologyKey: "az",
							},
						},
					},
					RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"S2"},
									},
								},
							},
							TopologyKey: v1.LabelHostname,
						},
					},
				},
			},
			errorExpected: false,
		},
		// 仅有硬反亲和且 topologyKey 合法时应通过。
		{
			affinity: &api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"S2"},
									},
								},
							},
							TopologyKey: v1.LabelHostname,
						},
					},
				},
			},
			errorExpected: false,
		},
		// 硬反亲和的 topologyKey 非法时应拒绝。
		{
			affinity: &api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"S2"},
									},
								},
							},
							TopologyKey: " zone ",
						},
					},
				},
			},
			errorExpected: true,
		},
		// 多个硬反亲和中夹杂一个非法 topologyKey，也应拒绝。
		{
			affinity: &api.Affinity{
				PodAntiAffinity: &api.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []api.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"S2"},
									},
								},
							},
							TopologyKey: v1.LabelHostname,
						}, {
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"S2"},
									},
								},
							},
							TopologyKey: " zone ",
						}, {
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "security",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"S2"},
									},
								},
							},
							TopologyKey: v1.LabelHostname,
						},
					},
				},
			},
			errorExpected: true,
		},
	}
	for _, test := range tests {
		pod.Spec.Affinity = test.affinity
		err := handler.Validate(context.TODO(), admission.NewAttributesRecord(&pod, nil, api.Kind("Pod").WithVersion("version"), "foo", "name", api.Resource("pods").WithVersion("version"), "", "ignored", nil, false, nil), nil)

		if test.errorExpected && err == nil {
			t.Errorf("Expected error for Anti Affinity %+v but did not get an error", test.affinity)
		}

		if !test.errorExpected && err != nil {
			t.Errorf("Unexpected error %v for AntiAffinity %+v", err, test.affinity)
		}
	}
}

// TestHandles 验证插件只对创建和更新操作生效。
func TestHandles(t *testing.T) {
	handler := NewInterPodAntiAffinity()
	tests := map[admission.Operation]bool{
		admission.Update:  true,
		admission.Create:  true,
		admission.Delete:  false,
		admission.Connect: false,
	}
	for op, expected := range tests {
		result := handler.Handles(op)
		if result != expected {
			t.Errorf("Unexpected result for operation %s: %v\n", op, result)
		}
	}
}

// TestOtherResources 验证该准入控制器对其他资源、子资源以及非 Pod 对象不产生影响。
func TestOtherResources(t *testing.T) {
	namespace := "testnamespace"
	name := "testname"
	pod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	tests := []struct {
		name        string
		kind        string
		resource    string
		subresource string
		object      runtime.Object
		expectError bool
	}{
		{
			name:     "non-pod resource",
			kind:     "Foo",
			resource: "foos",
			object:   pod,
		},
		{
			name:        "pod subresource",
			kind:        "Pod",
			resource:    "pods",
			subresource: "eviction",
			object:      pod,
		},
		{
			name:        "non-pod object",
			kind:        "Pod",
			resource:    "pods",
			object:      &api.Service{},
			expectError: true,
		},
	}

	for _, tc := range tests {
		handler := &Plugin{}

		err := handler.Validate(context.TODO(), admission.NewAttributesRecord(tc.object, nil, api.Kind(tc.kind).WithVersion("version"), namespace, name, api.Resource(tc.resource).WithVersion("version"), tc.subresource, admission.Create, &metav1.CreateOptions{}, false, nil), nil)

		if tc.expectError {
			if err == nil {
				t.Errorf("%s: unexpected nil error", tc.name)
			}
			continue
		}

		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
			continue
		}
	}
}
