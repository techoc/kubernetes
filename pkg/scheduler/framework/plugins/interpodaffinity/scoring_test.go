/*
Copyright 2019 The Kubernetes Authors.

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

package interpodaffinity

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	plugintesting "k8s.io/kubernetes/pkg/scheduler/framework/plugins/testing"
	schedruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

// 两个 team 标签，用于构造 namespace selector 的测试数据。
var nsLabelT1 = map[string]string{"team": "team1"}
var nsLabelT2 = map[string]string{"team": "team2"}

// 四个 namespace，分属两个 team，用于验证 namespace selector 的匹配行为。
var namespaces = []runtime.Object{
	&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "subteam1.team1", Labels: nsLabelT1}},
	&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "subteam2.team1", Labels: nsLabelT1}},
	&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "subteam1.team2", Labels: nsLabelT2}},
	&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "subteam2.team2", Labels: nsLabelT2}},
}

// TestPreferredAffinity 验证 Pod 亲和性/反亲和性优先项的打分行为。
func TestPreferredAffinity(t *testing.T) {
	labelRgChina := map[string]string{
		"region": "China",
	}
	labelRgIndia := map[string]string{
		"region": "India",
	}
	labelAzAz1 := map[string]string{
		"az": "az1",
	}
	labelAzAz2 := map[string]string{
		"az": "az2",
	}
	labelRgChinaAzAz1 := map[string]string{
		"region": "China",
		"az":     "az1",
	}
	podLabelSecurityS1 := map[string]string{
		"security": "S1",
	}
	podLabelSecurityS2 := map[string]string{
		"security": "S2",
	}
	// 下列亲和性定义只使用 preferredDuringSchedulingIgnoredDuringExecution 项。
	stayWithS1InRegion := &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 5,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"S1"},
								},
							},
						},
						TopologyKey: "region",
					},
				},
			},
		},
	}
	stayWithS2InRegion := &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 6,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"S2"},
								},
							},
						},
						TopologyKey: "region",
					},
				},
			},
		},
	}
	affinity3 := &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 8,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpNotIn,
									Values:   []string{"S1"},
								}, {
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"S2"},
								},
							},
						},
						TopologyKey: "region",
					},
				}, {
					Weight: 2,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpExists,
								}, {
									Key:      "wrongkey",
									Operator: metav1.LabelSelectorOpDoesNotExist,
								},
							},
						},
						TopologyKey: "region",
					},
				},
			},
		},
	}
	hardAffinity := &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "security",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{"S1", "value2"},
							},
						},
					},
					TopologyKey: "region",
				}, {
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "security",
								Operator: metav1.LabelSelectorOpExists,
							}, {
								Key:      "wrongkey",
								Operator: metav1.LabelSelectorOpDoesNotExist,
							},
						},
					},
					TopologyKey: "region",
				},
			},
		},
	}
	awayFromS1InAz := &v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 5,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"S1"},
								},
							},
						},
						TopologyKey: "az",
					},
				},
			},
		},
	}
	// 尽量避开任意 az 中 security=S2 的 Pod。
	awayFromS2InAz := &v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 5,
					PodAffinityTerm: v1.PodAffinityTerm{
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
	}
	// 尽量与同 region 的 security=S1 Pod 共处，同时避开任意 az 中 security=S2 的 Pod。
	stayWithS1InRegionAwayFromS2InAz := &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 8,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"S1"},
								},
							},
						},
						TopologyKey: "region",
					},
				},
			},
		},
		PodAntiAffinity: &v1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 5,
					PodAffinityTerm: v1.PodAffinityTerm{
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
	}

	affinityNamespaceSelector := &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 5,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"S1"},
								},
							},
						},
						TopologyKey: "region",
						Namespaces:  []string{"subteam2.team2"},
						NamespaceSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "team",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"team1"},
								},
							},
						},
					},
				},
			},
		},
	}
	antiAffinityNamespaceSelector := &v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 5,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"S1"},
								},
							},
						},
						TopologyKey: "region",
						Namespaces:  []string{"subteam2.team2"},
						NamespaceSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "team",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"team1"},
								},
							},
						},
					},
				},
			},
		},
	}
	affinityNegativeNamespaceSelector := &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 5,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"S1"},
								},
							},
						},
						TopologyKey: "region",
						NamespaceSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "team",
									Operator: metav1.LabelSelectorOpNotIn,
									Values:   []string{"team1"},
								},
							},
						},
					},
				},
			},
		},
	}
	antiAffinityNegativeNamespaceSelector := &v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 5,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"S1"},
								},
							},
						},
						TopologyKey: "region",
						NamespaceSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "team",
									Operator: metav1.LabelSelectorOpNotIn,
									Values:   []string{"team1"},
								},
							},
						},
					},
				},
			},
		},
	}
	invalidAffinityLabels := &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 8,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"{{.bad-value.}}"},
								},
							},
						},
						TopologyKey: "region",
					},
				},
			},
		},
	}
	invalidAntiAffinityLabels := &v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: 5,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "security",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"{{.bad-value.}}"},
								},
							},
						},
						TopologyKey: "az",
					},
				},
			},
		},
	}

	tests := []struct {
		pod                                *v1.Pod
		pods                               []*v1.Pod
		nodes                              []*v1.Node
		expectedList                       fwk.NodeScoreList
		name                               string
		ignorePreferredTermsOfExistingPods bool
		wantStatus                         *fwk.Status
	}{
		{
			name: "all nodes are same priority as Affinity is nil",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			wantStatus: fwk.NewStatus(fwk.Skip),
		},
		// node1 带 topology key 标签且上面有匹配 labelSelector 的 Pod，得高分。
		// node3 缺 topology key 标签，即便有匹配的 Pod 也得低分。
		// node2 带 topology key 标签，但上面的 Pod 不匹配 labelSelector，同样低分。
		{
			name: "Affinity: pod that matches topology key & pods in nodes will get high score comparing to others" +
				"which doesn't match either pods in nodes or in topology key",
			pod: &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: stayWithS1InRegion}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
				{Spec: v1.PodSpec{NodeName: "node3"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: 0}, {Name: "node3", Score: 0}},
		},
		// node1 带 topology key 标签且有匹配的 Pod，得高分。
		// node2 与 node1 的 region 标签值相同，同属一个拓扑域，因此同样得高分。
		// node3 虽带 topology key 但值不同，且没有匹配的 Pod，得低分。
		{
			name: "All the nodes that have the same topology key & label value with one of them has an existing pod that match the affinity rules, have the same score",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: stayWithS1InRegion}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgChinaAzAz1}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: fwk.MaxNodeScore}, {Name: "node3", Score: 0}},
		},
		// 两个 region 都有匹配优先项的节点，但 China region 中匹配的 Pod 更多。
		// 因此 China region 内的节点得分更高，且同一 region 内得分相同；
		// India region 内的节点则统一为较低分。
		{
			name: "Affinity: nodes in one region has more matching pods comparing to other region, so the region which has more matches will get high score",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: stayWithS2InRegion}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
				{Spec: v1.PodSpec{NodeName: "node3"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
				{Spec: v1.PodSpec{NodeName: "node4"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
				{Spec: v1.PodSpec{NodeName: "node5"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node4", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node5", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: 0}, {Name: "node3", Score: fwk.MaxNodeScore}, {Name: "node4", Score: fwk.MaxNodeScore}, {Name: "node5", Score: 0}},
		},
		// 覆盖亲和性优先项中各种 operator 和 value，包含匹配失败的情形。
		{
			name: "Affinity: different Label operators and values for pod affinity scheduling preference, including some match failures ",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: affinity3}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
				{Spec: v1.PodSpec{NodeName: "node3"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 20}, {Name: "node2", Score: fwk.MaxNodeScore}, {Name: "node3", Score: 0}},
		},
		// 亲和性的对称场景：不是待调度 Pod 声明亲和性，
		// 而是已有 Pod 声明了亲和性，待调度 Pod 恰好满足其偏好。
		{
			name: "Affinity symmetry: considered only the preferredDuringSchedulingIgnoredDuringExecution in pod affinity symmetry",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: stayWithS1InRegion}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: stayWithS2InRegion}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxNodeScore}, {Name: "node3", Score: 0}},
		},
		{
			name: "Affinity symmetry with namespace selector",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: affinityNamespaceSelector}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: stayWithS2InRegion}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: 0}, {Name: "node3", Score: 0}},
		},
		{
			name: "AntiAffinity symmetry with namespace selector",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: antiAffinityNamespaceSelector}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: stayWithS2InRegion}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxNodeScore}, {Name: "node3", Score: fwk.MaxNodeScore}},
		},
		{
			name: "Affinity symmetry: considered RequiredDuringSchedulingIgnoredDuringExecution in pod affinity symmetry",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: hardAffinity}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: hardAffinity}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: fwk.MaxNodeScore}, {Name: "node3", Score: 0}},
		},

		// 反亲和性场景：待调度 Pod 希望在节点级别避开某些已有 Pod。
		// 带 topology key 标签且上面有匹配 Pod 的节点得低分；
		// 缺 topology key 标签、或上面 Pod 不匹配的节点得高分。
		// node1 和 node2 都带 topology key 且有匹配 Pod，
		// 但 node1 上匹配的 Pod 更多，因此分数比 node2 更低。
		{
			name: "Anti Affinity: pod that does not match existing pods in node will get high score ",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: awayFromS1InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelAzAz1}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgChina}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxNodeScore}},
		},
		{
			name: "Anti Affinity: pod that does not match topology key & match the pods in nodes will get higher score comparing to others ",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: awayFromS1InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelAzAz1}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgChina}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxNodeScore}},
		},
		{
			name: "Anti Affinity: one node has more matching pods comparing to other node, so the node which has more unmatches will get high score",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: awayFromS1InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelAzAz1}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxNodeScore}},
		},
		// 反亲和性的对称场景。
		{
			name: "Anti Affinity symmetry: the existing pods in node which has anti affinity match will get high score",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: awayFromS2InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: awayFromS1InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelAzAz1}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelAzAz2}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxNodeScore}},
		},
		// 亲和性与反亲和性同时生效的场景。
		{
			name: "Affinity and Anti Affinity: considered only preferredDuringSchedulingIgnoredDuringExecution in both pod affinity & anti affinity",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: stayWithS1InRegionAwayFromS2InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelAzAz1}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: 0}},
		},
		// 组合场景：待调度 Pod 与已有 Pod 标签相同（同属一个 RC/Service）。
		// 它希望与同组 Pod 同处一个 region，但在节点级别相互分散，
		// 即同一 RC/Service 的 Pod 集中在同一 region、又尽量分布在不同节点。
		// node1、node3、node4 属于 China region，node2、node5 属于 India region。
		{
			name: "Affinity and Anti Affinity: considering both affinity and anti-affinity, the pod to schedule and existing pods have the same labels",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: stayWithS1InRegionAwayFromS2InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node3"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node3"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node4"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node5"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChinaAzAz1}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node4", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node5", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: 0}, {Name: "node3", Score: fwk.MaxNodeScore}, {Name: "node4", Score: fwk.MaxNodeScore}, {Name: "node5", Score: 0}},
		},
		// 亲和性、反亲和性及其对称场景同时生效。
		// 亲和性权重：              8,  0,  0,  0
		// 反亲和性权重：            0, -5,  0,  0
		// 亲和性对称权重：          0,  0,  8,  0
		// 反亲和性对称权重：        0,  0,  0, -5
		{
			name: "Affinity and Anti Affinity and symmetry: considered only preferredDuringSchedulingIgnoredDuringExecution in both pod affinity & anti affinity & symmetry",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: stayWithS1InRegionAwayFromS2InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
				{Spec: v1.PodSpec{NodeName: "node3", Affinity: stayWithS1InRegionAwayFromS2InAz}},
				{Spec: v1.PodSpec{NodeName: "node4", Affinity: awayFromS1InAz}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelAzAz1}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node4", Labels: labelAzAz2}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: 0}, {Name: "node3", Score: fwk.MaxNodeScore}, {Name: "node4", Score: 0}},
		},
		// 回归 https://github.com/kubernetes/kubernetes/issues/82796：
		// 同一拓扑域中部分节点有带亲和性的 Pod、部分没有，且待调度 Pod 自身无亲和性时，曾发生 panic。
		{
			name: "Avoid panic when partial nodes in a topology don't have pods with affinity",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: stayWithS1InRegionAwayFromS2InAz}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgChina}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: 0}},
		},
		{
			name:       "invalid Affinity fails PreScore",
			pod:        &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: invalidAffinityLabels}},
			wantStatus: fwk.NewStatus(fwk.Error, `Invalid value: "{{.bad-value.}}"`),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgChina}},
			},
		},
		{
			name:       "invalid AntiAffinity fails PreScore",
			pod:        &v1.Pod{Spec: v1.PodSpec{NodeName: "", Affinity: invalidAntiAffinityLabels}},
			wantStatus: fwk.NewStatus(fwk.Error, `Invalid value: "{{.bad-value.}}"`),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgChina}},
			},
		},
		{
			name: "Affinity with pods matching NamespaceSelector",
			pod:  &v1.Pod{Spec: v1.PodSpec{Affinity: affinityNamespaceSelector}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team2", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam2.team1", Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: 0}},
		},
		{
			name: "Affinity with pods matching both NamespaceSelector and Namespaces fields",
			pod:  &v1.Pod{Spec: v1.PodSpec{Affinity: affinityNamespaceSelector}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam2.team2", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam2.team1", Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: 0}},
		},
		{
			name: "Affinity with pods matching NamespaceSelector",
			pod:  &v1.Pod{Spec: v1.PodSpec{Affinity: antiAffinityNamespaceSelector}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team2", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam2.team1", Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxNodeScore}},
		},
		{
			name: "Affinity with pods matching both NamespaceSelector and Namespaces fields",
			pod:  &v1.Pod{Spec: v1.PodSpec{Affinity: antiAffinityNamespaceSelector}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam2.team2", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam2.team1", Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxNodeScore}},
		},
		{
			name: "Affinity with pods correctly enforcing negative NamespaceSelector",
			pod:  &v1.Pod{Spec: v1.PodSpec{Affinity: affinityNegativeNamespaceSelector}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team2", Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxScore}},
		},
		{
			name: "AntiAffinity with pods correctly enforcing negative NamespaceSelector",
			pod:  &v1.Pod{Spec: v1.PodSpec{Affinity: antiAffinityNegativeNamespaceSelector}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2"}, ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team2", Labels: podLabelSecurityS1}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
			},
			expectedList: []fwk.NodeScore{{Name: "node1", Score: fwk.MaxScore}, {Name: "node2", Score: 0}},
		},
		{
			name: "Ignore preferred terms of existing pods",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: stayWithS1InRegionAwayFromS2InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: stayWithS2InRegion}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
			},
			expectedList:                       []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: 0}},
			wantStatus:                         fwk.NewStatus(fwk.Skip),
			ignorePreferredTermsOfExistingPods: true,
		},
		{
			name: "Do not ignore preferred terms of existing pods",
			pod:  &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: stayWithS1InRegionAwayFromS2InAz}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS1}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: stayWithS2InRegion}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
			},
			expectedList:                       []fwk.NodeScore{{Name: "node1", Score: 0}, {Name: "node2", Score: fwk.MaxNodeScore}},
			ignorePreferredTermsOfExistingPods: false,
		},
		{
			name:       "No nodes to score",
			pod:        &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Labels: podLabelSecurityS2}},
			pods:       []*v1.Pod{},
			nodes:      []*v1.Node{},
			wantStatus: fwk.NewStatus(fwk.Skip),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			state := framework.NewCycleState()
			p := plugintesting.SetupPluginWithInformers(ctx, t, schedruntime.FactoryAdapter(feature.Features{}, New), &config.InterPodAffinityArgs{HardPodAffinityWeight: 1, IgnorePreferredTermsOfExistingPods: test.ignorePreferredTermsOfExistingPods}, cache.NewSnapshot(test.pods, test.nodes), namespaces)
			nodeInfos := tf.BuildNodeInfos(test.nodes)
			status := p.(fwk.PreScorePlugin).PreScore(ctx, state, test.pod, nodeInfos)

			if !status.IsSuccess() {
				if status.Code() != test.wantStatus.Code() {
					t.Errorf("InterPodAffinity#PreScore() returned unexpected status.Code got: %v, want: %v", status.Code(), test.wantStatus.Code())
				}

				if !strings.Contains(status.Message(), test.wantStatus.Message()) {
					t.Errorf("InterPodAffinity#PreScore() returned unexpected status.Message got: %v, want: %v", status.Message(), test.wantStatus.Message())
				}
				return
			}

			var gotList fwk.NodeScoreList
			for _, nodeInfo := range nodeInfos {
				nodeName := nodeInfo.Node().Name
				score, status := p.(fwk.ScorePlugin).Score(ctx, state, test.pod, nodeInfo)
				if !status.IsSuccess() {
					t.Errorf("unexpected error from Score: %v", status)
				}
				gotList = append(gotList, fwk.NodeScore{Name: nodeName, Score: score})
			}

			status = p.(fwk.ScorePlugin).ScoreExtensions().NormalizeScore(ctx, state, test.pod, gotList)
			if !status.IsSuccess() {
				t.Errorf("unexpected error from NormalizeScore: %v", status)
			}

			if diff := cmp.Diff(test.expectedList, gotList); diff != "" {
				t.Errorf("node score list doesn't match (-want,+got): \n %s", diff)
			}
		})
	}
}

// TestPreferredAffinityWithHardPodAffinitySymmetricWeight 验证硬亲和性对称权重对打分的影响。
func TestPreferredAffinityWithHardPodAffinitySymmetricWeight(t *testing.T) {
	podLabelServiceS1 := map[string]string{
		"service": "S1",
	}
	labelRgChina := map[string]string{
		"region": "China",
	}
	labelRgIndia := map[string]string{
		"region": "India",
	}
	labelAzAz1 := map[string]string{
		"az": "az1",
	}
	hardPodAffinity := &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "service",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{"S1"},
							},
						},
					},
					Namespaces: []string{"", "subteam2.team2"},
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "team",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{"team1"},
							},
						},
					},
					TopologyKey: "region",
				},
			},
		},
	}
	tests := []struct {
		pod                   *v1.Pod
		pods                  []*v1.Pod
		nodes                 []*v1.Node
		hardPodAffinityWeight int32
		expectedList          fwk.NodeScoreList
		name                  string
		wantStatus            *fwk.Status
	}{
		{
			name: "with default weight",
			pod:  &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: podLabelServiceS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: hardPodAffinity}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: hardPodAffinity}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			hardPodAffinityWeight: v1.DefaultHardPodAffinitySymmetricWeight,
			expectedList:          []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: fwk.MaxNodeScore}, {Name: "node3", Score: 0}},
		},
		{
			name: "with zero weight",
			pod:  &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: podLabelServiceS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: hardPodAffinity}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: hardPodAffinity}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			hardPodAffinityWeight: 0,
			wantStatus:            fwk.NewStatus(fwk.Skip),
		},
		{
			name: "with no matching namespace",
			pod:  &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team2", Labels: podLabelServiceS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: hardPodAffinity}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: hardPodAffinity}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			hardPodAffinityWeight: v1.DefaultHardPodAffinitySymmetricWeight,
			wantStatus:            fwk.NewStatus(fwk.Skip),
		},
		{
			name: "with matching NamespaceSelector",
			pod:  &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "subteam1.team1", Labels: podLabelServiceS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: hardPodAffinity}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: hardPodAffinity}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			hardPodAffinityWeight: v1.DefaultHardPodAffinitySymmetricWeight,
			expectedList:          []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: fwk.MaxNodeScore}, {Name: "node3", Score: 0}},
		},
		{
			name: "with matching Namespaces",
			pod:  &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "subteam2.team2", Labels: podLabelServiceS1}},
			pods: []*v1.Pod{
				{Spec: v1.PodSpec{NodeName: "node1", Affinity: hardPodAffinity}},
				{Spec: v1.PodSpec{NodeName: "node2", Affinity: hardPodAffinity}},
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: labelRgChina}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: labelRgIndia}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Labels: labelAzAz1}},
			},
			hardPodAffinityWeight: v1.DefaultHardPodAffinitySymmetricWeight,
			expectedList:          []fwk.NodeScore{{Name: "node1", Score: fwk.MaxNodeScore}, {Name: "node2", Score: fwk.MaxNodeScore}, {Name: "node3", Score: 0}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			state := framework.NewCycleState()
			p := plugintesting.SetupPluginWithInformers(ctx, t, schedruntime.FactoryAdapter(feature.Features{}, New), &config.InterPodAffinityArgs{HardPodAffinityWeight: test.hardPodAffinityWeight}, cache.NewSnapshot(test.pods, test.nodes), namespaces)
			nodeInfos := tf.BuildNodeInfos(test.nodes)
			status := p.(fwk.PreScorePlugin).PreScore(ctx, state, test.pod, nodeInfos)
			if !test.wantStatus.Equal(status) {
				t.Errorf("InterPodAffinity#PreScore() returned unexpected status.Code got: %v, want: %v", status.Code(), test.wantStatus.Code())
			}
			if !status.IsSuccess() {
				return
			}

			var gotList fwk.NodeScoreList
			for _, nodeInfo := range nodeInfos {
				nodeName := nodeInfo.Node().Name
				nodeInfo, err := p.(*InterPodAffinity).sharedLister.NodeInfos().Get(nodeName)
				if err != nil {
					t.Errorf("failed to get node %q from snapshot: %v", nodeName, err)
				}
				score, status := p.(fwk.ScorePlugin).Score(ctx, state, test.pod, nodeInfo)
				if !status.IsSuccess() {
					t.Errorf("unexpected error: %v", status)
				}
				gotList = append(gotList, fwk.NodeScore{Name: nodeName, Score: score})
			}

			status = p.(fwk.ScorePlugin).ScoreExtensions().NormalizeScore(ctx, state, test.pod, gotList)
			if !status.IsSuccess() {
				t.Errorf("unexpected error: %v", status)
			}

			if diff := cmp.Diff(test.expectedList, gotList); diff != "" {
				t.Errorf("unexpected NodeScoreList (-want,+got):\n%s", diff)
			}
		})
	}
}
