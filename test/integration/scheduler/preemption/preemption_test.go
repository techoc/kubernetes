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

// 本文件测试 scheduler 的抢占功能。

package preemption

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8suuid "k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	"k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultpreemption"
	plfeature "k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testfwk "k8s.io/kubernetes/test/integration/framework"
	testutils "k8s.io/kubernetes/test/integration/util"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

// 从 testutils 引入。
var (
	initPausePod        = testutils.InitPausePod
	createNode          = testutils.CreateNode
	createPausePod      = testutils.CreatePausePod
	runPausePod         = testutils.RunPausePod
	podIsGettingEvicted = testutils.PodIsGettingEvicted
)

const filterPluginName = "filter-plugin"

var lowPriority, mediumPriority, highPriority = int32(100), int32(200), int32(300)

const tokenFilterName = "token-filter"

// tokenFilter 是实现 PreFilter 和 Filter 的假插件。
// `Token` 模拟集群还能容纳的 Pod 数量：`EnablePreFilter` 为 false 或 `Token` 大于 0 时 PreFilter 通过，否则返回 Unschedulable。
// 每次 Filter() 都会消耗一个 Token：Token 大于 0 时通过，否则返回 Unschedulable
// 或 UnschedulableAndUnresolvable（`Unresolvable` 为 true 时）。
// AddPod()/RemovePod() 通过增减 Token 模拟抢占的 dryrun 过程。
type tokenFilter struct {
	Tokens          int
	Unresolvable    bool
	EnablePreFilter bool
}

// Name 返回插件名称。
func (fp *tokenFilter) Name() string {
	return tokenFilterName
}

func (fp *tokenFilter) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod,
	nodeInfo fwk.NodeInfo) *fwk.Status {
	if fp.Tokens > 0 {
		fp.Tokens--
		return nil
	}
	status := fwk.Unschedulable
	if fp.Unresolvable {
		status = fwk.UnschedulableAndUnresolvable
	}
	return fwk.NewStatus(status, fmt.Sprintf("can't fit %v", pod.Name))
}

func (fp *tokenFilter) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if !fp.EnablePreFilter || fp.Tokens > 0 {
		return nil, nil
	}
	return nil, fwk.NewStatus(fwk.Unschedulable)
}

func (fp *tokenFilter) AddPod(ctx context.Context, state fwk.CycleState, podToSchedule *v1.Pod,
	podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	fp.Tokens--
	return nil
}

func (fp *tokenFilter) RemovePod(ctx context.Context, state fwk.CycleState, podToSchedule *v1.Pod,
	podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	fp.Tokens++
	return nil
}

func (fp *tokenFilter) PreFilterExtensions() fwk.PreFilterExtensions {
	return fp
}

var _ fwk.FilterPlugin = &tokenFilter{}

// TestPreemption 覆盖若干抢占场景。
func TestPreemption(t *testing.T) {
	// 用 filter 插件初始化 scheduler。
	var filter tokenFilter
	registry := make(frameworkruntime.Registry)
	err := registry.Register(filterPluginName, func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
		return &filter, nil
	})
	if err != nil {
		t.Fatalf("Error registering a filter: %v", err)
	}
	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				Filter: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: filterPluginName},
					},
				},
				PreFilter: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: filterPluginName},
					},
				},
			},
		}},
	})

	defaultPodRes := &v1.ResourceRequirements{Requests: v1.ResourceList{
		v1.ResourceCPU:    *resource.NewMilliQuantity(100, resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
	}

	maxTokens := 1000

	tests := []struct {
		name                     string
		existingPods             []*v1.Pod
		pod                      *v1.Pod
		initTokens               int
		enablePreFilter          bool
		unresolvable             bool
		preemptedPodIndexes      map[int]struct{}
		extraNodes               []*v1.Node
		podGroups                []*schedulingv1beta1.PodGroup
		compositePodGroups       []*schedulingv1alpha3.CompositePodGroup
		genericWorkloadEnabled   bool
		compositePodGroupEnabled bool
	}{
		{
			name:       "basic pod preemption",
			initTokens: maxTokens,
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:     "victim-pod",
					Priority: &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(400, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
					},
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:     "preemptor-pod",
				Priority: &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}},
		},
		{
			name:       "basic pod preemption with filter",
			initTokens: 1,
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:     "victim-pod",
					Priority: &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
					},
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:     "preemptor-pod",
				Priority: &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}},
		},
		// 与前一个子用例相同，只是开启 enablePreFilter。假插件在 PreFilter 返回 Unschedulable，
		// 可覆盖 in-tree 插件在 PreFilter 返回 Skip 时，抢占过程中 AddPod/RemovePod 也被正确跳过的路径。
		{
			name:            "basic pod preemption with preFilter",
			initTokens:      1,
			enablePreFilter: true,
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:     "victim-pod",
					Priority: &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
					},
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:     "preemptor-pod",
				Priority: &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}},
		},
		{
			// 与前一个用例相同，但 filter 返回的结果不可恢复（unresolvable）。
			name:         "basic pod preemption with unresolvable filter",
			initTokens:   1,
			unresolvable: true,
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:     "victim-pod",
					Priority: &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
					},
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:     "preemptor-pod",
				Priority: &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{},
		},
		{
			name:       "preemption is performed to satisfy anti-affinity",
			initTokens: maxTokens,
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:      "pod-0",
					Priority:  &mediumPriority,
					Labels:    map[string]string{"pod": "p0"},
					Resources: defaultPodRes,
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "pod-1",
					Priority:  &lowPriority,
					Labels:    map[string]string{"pod": "p1"},
					Resources: defaultPodRes,
					Affinity: &v1.Affinity{
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "pod",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"preemptor"},
											},
										},
									},
									TopologyKey: "node",
								},
							},
						},
					},
				}),
			},
			// 带有反亲和性的高优先级 Pod。
			pod: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Priority:  &highPriority,
				Labels:    map[string]string{"pod": "preemptor"},
				Resources: defaultPodRes,
				Affinity: &v1.Affinity{
					PodAntiAffinity: &v1.PodAntiAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "pod",
											Operator: metav1.LabelSelectorOpIn,
											Values:   []string{"p0"},
										},
									},
								},
								TopologyKey: "node",
							},
						},
					},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}, 1: {}},
		},
		{
			// 与前一个用例类似，只是 pod-1 是高优先级。
			name:       "preemption is not performed when anti-affinity is not satisfied",
			initTokens: maxTokens,
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:      "pod-0",
					Priority:  &mediumPriority,
					Labels:    map[string]string{"pod": "p0"},
					Resources: defaultPodRes,
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "pod-1",
					Priority:  &highPriority,
					Labels:    map[string]string{"pod": "p1"},
					Resources: defaultPodRes,
					Affinity: &v1.Affinity{
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "pod",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"preemptor"},
											},
										},
									},
									TopologyKey: "node",
								},
							},
						},
					},
				}),
			},
			// 带有反亲和性的高优先级 Pod。
			pod: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Priority:  &highPriority,
				Labels:    map[string]string{"pod": "preemptor"},
				Resources: defaultPodRes,
				Affinity: &v1.Affinity{
					PodAntiAffinity: &v1.PodAntiAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "pod",
											Operator: metav1.LabelSelectorOpIn,
											Values:   []string{"p0"},
										},
									},
								},
								TopologyKey: "node",
							},
						},
					},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{},
		},
		{
			// PodGroup 被当作一个整体 victim，因此即使 pod-group-victim-2 在 node2 上，两个 Pod 也会被一并驱逐。
			name:                   "pod group victim across multiple nodes, pod-group-as-victim enabled",
			initTokens:             maxTokens,
			genericWorkloadEnabled: true,
			extraNodes: []*v1.Node{
				st.MakeNode().Name("node2").Capacity(map[v1.ResourceName]string{
					v1.ResourcePods:   "32",
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "500",
				}).Label("node", "node2").Obj(),
			},
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Priority(lowPriority).BasicPolicy().
					DisruptionModeAll().Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "pod-group-victim-1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
					PodGroupName: "pg1",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pod-group-victim-2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
					PodGroupName: "pg1",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node1"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
				}},
			}),
			// 整个 pod group 作为一个整体被驱逐：索引 0 和 1 的 Pod 都会被驱赶。
			preemptedPodIndexes: map[int]struct{}{0: {}, 1: {}},
		},
		{
			// PodGroup 所有 Pod 都在同一节点且开启 DisruptionModeAll 时，
			// 抢占其中一个成员以满足资源请求会原子化地驱逐整个组。
			name:                   "pod group occupying a single node, pod-group-as-victim enabled",
			initTokens:             maxTokens,
			genericWorkloadEnabled: true,
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg-single").Priority(lowPriority).BasicPolicy().
					DisruptionModeAll().Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "pod-group-victim-1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-single",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pod-group-victim-2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-single",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node1"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
				}},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}, 1: {}},
		},
		{
			// 跨节点存在多个 victim PodGroup 时，抢占只选择满足请求所需的最低优先级组；
			// 该组在所有节点上的成员都会被驱逐，优先级更高的组保持不动。
			name:                   "multiple victim pod groups, only subset selected for preemption",
			initTokens:             maxTokens,
			genericWorkloadEnabled: true,
			extraNodes: []*v1.Node{
				st.MakeNode().Name("node2").Capacity(map[v1.ResourceName]string{
					v1.ResourcePods:   "32",
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "500",
				}).Label("node", "node2").Obj(),
			},
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg-low").Priority(lowPriority).BasicPolicy().
					DisruptionModeAll().Obj(),
				st.MakePodGroup().Name("pg-medium").Priority(mediumPriority).BasicPolicy().
					DisruptionModeAll().Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-low-node1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-low",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-low-node2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-low",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-medium-node1",
					Priority:     &mediumPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-medium",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-medium-node2",
					Priority:     &mediumPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-medium",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node1"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
				}},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}, 1: {}},
		},
		{
			// 当节点上所有低优先级 victim PodGroup 加起来仍不足以满足请求（因为存在不可抢占的同优先级 Pod）时，
			// 抢占会整体放弃，不驱逐 PodGroup 的任何成员。
			name:                   "unsuccessful pod group preemption when freeing lower priority group is insufficient",
			initTokens:             maxTokens,
			genericWorkloadEnabled: true,
			extraNodes: []*v1.Node{
				st.MakeNode().Name("node2").Capacity(map[v1.ResourceName]string{
					v1.ResourcePods:   "32",
					v1.ResourceCPU:    "1000m",
					v1.ResourceMemory: "1000",
				}).Label("node", "node2").Obj(),
			},
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg-victim").Priority(lowPriority).BasicPolicy().
					DisruptionModeAll().Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "unpreemptible-pod",
					Priority:     &highPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-victim-1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(150, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-victim",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-victim-2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(150, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-victim",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node2"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(600, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
				}},
			}),
			preemptedPodIndexes: map[int]struct{}{},
		},
		{
			// 跨多节点的 PodGroup 使用 DisruptionModeSingle 时，驱逐目标节点上的一个成员不会触发 gang eviction，
			// 其他节点上的成员继续运行。
			name:                   "pod group victim across multiple nodes, DisruptionModeSingle enabled",
			initTokens:             maxTokens,
			genericWorkloadEnabled: true,
			extraNodes: []*v1.Node{
				st.MakeNode().Name("node2").Capacity(map[v1.ResourceName]string{
					v1.ResourcePods:   "32",
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "500",
				}).Label("node", "node2").Obj(),
			},
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg-single-multi").Priority(lowPriority).BasicPolicy().
					DisruptionModeSingle().Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-single-multi-node1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
					PodGroupName: "pg-single-multi",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-single-multi-node2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
					PodGroupName: "pg-single-multi",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node1"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
				}},
			}),
			// 只有 node1 上索引 0 的 Pod 被驱逐；node2 上索引 1 的 Pod 不会被驱逐。
			preemptedPodIndexes: map[int]struct{}{0: {}},
		},
		{
			// CompositePodGroup 被当作一个整体 victim，因此即使 pod-group-victim-2 在 node2 上，两个 Pod 也会被一并驱逐。
			name:                     "CPG victim across multiple nodes, pod-group-as-victim enabled",
			initTokens:               maxTokens,
			genericWorkloadEnabled:   true,
			compositePodGroupEnabled: true,
			extraNodes: []*v1.Node{
				st.MakeNode().Name("node2").Capacity(map[v1.ResourceName]string{
					v1.ResourcePods:   "32",
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "500",
				}).Label("node", "node2").Obj(),
			},
			compositePodGroups: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("cpg1").Priority(lowPriority).BasicPolicy().
					DisruptionModeAll().WorkloadRef("wl1", "t1").Obj(),
			},
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Priority(lowPriority).BasicPolicy().ParentCompositePodGroup("cpg1").WorkloadRef("t1", "wl1").Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "pod-group-victim-1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
					PodGroupName: "pg1",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pod-group-victim-2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
					PodGroupName: "pg1",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node1"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
				}},
			}),
			// 整个 pod group 作为一个整体被驱逐：索引 0 和 1 的 Pod 都会被驱赶。
			preemptedPodIndexes: map[int]struct{}{0: {}, 1: {}},
		},
		{
			// CPG 所有 Pod 都在同一节点且开启 DisruptionModeAll 时，
			// 抢占其中一个成员以满足资源请求会原子化地驱逐整个组。
			name:                     "CPG occupying a single node, pod-group-as-victim enabled",
			initTokens:               maxTokens,
			genericWorkloadEnabled:   true,
			compositePodGroupEnabled: true,
			compositePodGroups: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("cpg-single").Priority(lowPriority).BasicPolicy().
					DisruptionModeAll().WorkloadRef("wl1", "t1").Obj(),
			},
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg-single").Priority(lowPriority).BasicPolicy().ParentCompositePodGroup("cpg-single").WorkloadRef("t1", "wl1").Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "pod-group-victim-1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-single",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pod-group-victim-2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-single",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node1"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
				}},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}, 1: {}},
		},
		{
			// 跨节点存在多个 victim CPG 时，抢占只选择满足请求所需的最低优先级组；
			// 该组在所有节点上的成员都会被驱逐，优先级更高的组保持不动。
			name:                     "multiple victim CPGs, only subset selected for preemption",
			initTokens:               maxTokens,
			genericWorkloadEnabled:   true,
			compositePodGroupEnabled: true,
			extraNodes: []*v1.Node{
				st.MakeNode().Name("node2").Capacity(map[v1.ResourceName]string{
					v1.ResourcePods:   "32",
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "500",
				}).Label("node", "node2").Obj(),
			},
			compositePodGroups: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("cpg-low").Priority(lowPriority).BasicPolicy().
					DisruptionModeAll().WorkloadRef("wl1", "t1").Obj(),
				st.MakeCompositePodGroup().Name("cpg-medium").Priority(mediumPriority).BasicPolicy().
					DisruptionModeAll().WorkloadRef("wl2", "t2").Obj(),
			},
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg-low").Priority(lowPriority).BasicPolicy().ParentCompositePodGroup("cpg-low").WorkloadRef("t1", "wl1").Obj(),
				st.MakePodGroup().Name("pg-medium").Priority(mediumPriority).BasicPolicy().ParentCompositePodGroup("cpg-medium").WorkloadRef("t2", "wl2").Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-low-node1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-low",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-low-node2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-low",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-medium-node1",
					Priority:     &mediumPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-medium",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-medium-node2",
					Priority:     &mediumPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-medium",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node1"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
				}},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}, 1: {}},
		},
		{
			// 当节点上所有低优先级 victim CPG 加起来仍不足以满足请求（因为存在不可抢占的同优先级 Pod）时，
			// 抢占会整体放弃，不驱逐 CPG 的任何成员。
			name:                     "unsuccessful CPG preemption when freeing lower priority group is insufficient",
			initTokens:               maxTokens,
			genericWorkloadEnabled:   true,
			compositePodGroupEnabled: true,
			extraNodes: []*v1.Node{
				st.MakeNode().Name("node2").Capacity(map[v1.ResourceName]string{
					v1.ResourcePods:   "32",
					v1.ResourceCPU:    "1000m",
					v1.ResourceMemory: "1000",
				}).Label("node", "node2").Obj(),
			},
			compositePodGroups: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("cpg-victim").Priority(lowPriority).BasicPolicy().
					DisruptionModeAll().WorkloadRef("wl1", "t1").Obj(),
			},
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg-victim").Priority(lowPriority).BasicPolicy().ParentCompositePodGroup("cpg-victim").WorkloadRef("t1", "wl1").Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "unpreemptible-pod",
					Priority:     &highPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-victim-1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(150, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-victim",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-victim-2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(150, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					}},
					PodGroupName: "pg-victim",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node2"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(600, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
				}},
			}),
			preemptedPodIndexes: map[int]struct{}{},
		},
		{
			// 跨多节点的 CPG 使用 DisruptionModeSingle 时，驱逐目标节点上的一个成员不会触发 gang eviction，
			// 其他节点上的成员继续运行。
			name:                     "CPG victim across multiple nodes, DisruptionModeSingle enabled",
			initTokens:               maxTokens,
			genericWorkloadEnabled:   true,
			compositePodGroupEnabled: true,
			extraNodes: []*v1.Node{
				st.MakeNode().Name("node2").Capacity(map[v1.ResourceName]string{
					v1.ResourcePods:   "32",
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "500",
				}).Label("node", "node2").Obj(),
			},
			compositePodGroups: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("cpg-single-multi").Priority(lowPriority).BasicPolicy().
					DisruptionModeSingle().WorkloadRef("wl1", "t1").Obj(),
			},
			podGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg-single-multi").Priority(lowPriority).BasicPolicy().ParentCompositePodGroup("cpg-single-multi").WorkloadRef("t1", "wl1").Obj(),
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-single-multi-node1",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node1"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
					PodGroupName: "pg-single-multi",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:         "pg-single-multi-node2",
					Priority:     &lowPriority,
					NodeSelector: map[string]string{"node": "node2"},
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
					}},
					PodGroupName: "pg-single-multi",
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:         "preemptor-pod",
				Priority:     &highPriority,
				NodeSelector: map[string]string{"node": "node1"},
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI),
				}},
			}),
			// 只有 node1 上索引 0 的 Pod 被驱逐；node2 上索引 1 的 Pod 不会被驱逐。
			preemptedPodIndexes: map[int]struct{}{0: {}},
		},
	}

	// 创建带有资源容量和标签的节点。
	nodeRes := map[v1.ResourceName]string{
		v1.ResourcePods:   "32",
		v1.ResourceCPU:    "500m",
		v1.ResourceMemory: "500",
	}
	nodeObject := st.MakeNode().Name("node1").Capacity(nodeRes).Label("node", "node1").Obj()

	// 按 genericWorkloadEnabled 和 compositePodGroupEnabled feature gate 对用例分组，
	// 让同组用例共享按对应 feature gate 配置启动的 API server。
	type featureGateKey struct {
		genericWorkloadEnabled   bool
		compositePodGroupEnabled bool
	}

	testsByWASFeatureGates := make(map[featureGateKey][]int)
	for i, test := range tests {
		key := featureGateKey{
			genericWorkloadEnabled:   test.genericWorkloadEnabled,
			compositePodGroupEnabled: test.compositePodGroupEnabled,
		}
		testsByWASFeatureGates[key] = append(testsByWASFeatureGates[key], i)
	}

	for _, asyncPreemptionEnabled := range []bool{true, false} {
		for _, asyncAPICallsEnabled := range []bool{true, false} {
			for _, clearingNominatedNodeNameAfterBinding := range []bool{true, false} {
				for fgKey, testIndexes := range testsByWASFeatureGates {
					// 每种完整 flag 组合启动一个 API server，且 GenericWorkload、CompositePodGroup
					// 等所有 flag 在 API server 和 scheduler 之间保持一致。
					featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
						features.SchedulerAsyncPreemption:              asyncPreemptionEnabled,
						features.SchedulerAsyncAPICalls:                asyncAPICallsEnabled,
						features.ClearingNominatedNodeNameAfterBinding: clearingNominatedNodeNameAfterBinding,
						features.GenericWorkload:                       fgKey.genericWorkloadEnabled,
						features.CompositePodGroup:                     fgKey.compositePodGroupEnabled,
						features.TopologyAwareWorkloadScheduling:       fgKey.compositePodGroupEnabled,
					})
					sharedAPICtx := testutils.InitTestAPIServer(t, "preemption", nil)

					for _, i := range testIndexes {
						test := tests[i]
						t.Run(fmt.Sprintf("%s (Async preemption enabled: %v, Async API calls enabled: %v, ClearingNominatedNodeNameAfterBinding: %v)", test.name, asyncPreemptionEnabled, asyncAPICallsEnabled, clearingNominatedNodeNameAfterBinding), func(t *testing.T) {
							testCtx := testutils.InitTestSchedulerWithOptions(t,
								withNewNamespace(t, sharedAPICtx, "preemption"),
								0,
								scheduler.WithProfiles(cfg.Profiles...),
								scheduler.WithFrameworkOutOfTreeRegistry(registry))
							testutils.SyncSchedulerInformerFactory(testCtx)
							go testCtx.Scheduler.Run(testCtx.SchedulerCtx)
							defer testCtx.SchedulerCloseFn()

							if _, err := createNode(testCtx.ClientSet, nodeObject); err != nil {
								t.Fatalf("Error creating node: %v", err)
							}
							t.Cleanup(func() {
								if err := testCtx.ClientSet.CoreV1().Nodes().Delete(testCtx.Ctx, nodeObject.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
									t.Errorf("Error deleting node %s: %v", nodeObject.Name, err)
								}
							})
							for _, n := range test.extraNodes {
								if _, err := createNode(testCtx.ClientSet, n); err != nil {
									t.Fatalf("Error creating extra node %s: %v", n.Name, err)
								}
								n := n
								t.Cleanup(func() {
									if err := testCtx.ClientSet.CoreV1().Nodes().Delete(testCtx.Ctx, n.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
										t.Errorf("Error deleting node %s: %v", n.Name, err)
									}
								})
							}

							cs := testCtx.ClientSet
							ns := testCtx.NS.Name

							for _, cpg := range test.compositePodGroups {
								cpg.Namespace = ns
								if _, err := cs.SchedulingV1alpha3().CompositePodGroups(ns).Create(testCtx.Ctx, cpg, metav1.CreateOptions{}); err != nil {
									t.Fatalf("Error creating CompositePodGroup %s: %v", cpg.Name, err)
								}
							}

							for _, pg := range test.podGroups {
								pg.Namespace = ns
								if _, err := cs.SchedulingV1beta1().PodGroups(ns).Create(testCtx.Ctx, pg, metav1.CreateOptions{}); err != nil {
									t.Fatalf("Error creating PodGroup %s: %v", pg.Name, err)
								}
							}

							filter.Tokens = test.initTokens
							filter.EnablePreFilter = test.enablePreFilter
							filter.Unresolvable = test.unresolvable
							pods := make([]*v1.Pod, len(test.existingPods))
							// 创建并运行已存在的 Pod。
							for i, p := range test.existingPods {
								p.Namespace = ns
								pods[i], err = runPausePod(cs, p)
								if err != nil {
									t.Fatalf("Error running pause pod: %v", err)
								}
							}
							// 创建抢占者 Pod。
							test.pod.Namespace = ns
							preemptor, err := createPausePod(cs, test.pod)
							if err != nil {
								t.Errorf("Error while creating high priority pod: %v", err)
							}
							// 等待预期 Pod 被抢占，同时确认其余 Pod 未被抢占。
							for i, p := range pods {
								if _, found := test.preemptedPodIndexes[i]; found {
									if err = wait.PollUntilContextTimeout(testCtx.Ctx, 200*time.Millisecond, wait.ForeverTestTimeout, false,
										podIsGettingEvicted(cs, p.Namespace, p.Name)); err != nil {
										t.Errorf("Pod %v/%v is not getting evicted.", p.Namespace, p.Name)
									}
									pod, err := cs.CoreV1().Pods(p.Namespace).Get(testCtx.Ctx, p.Name, metav1.GetOptions{})
									if err != nil {
										t.Errorf("Error %v when getting the updated status for pod %v/%v ", err, p.Namespace, p.Name)
									}
									_, cond := podutil.GetPodCondition(&pod.Status, v1.DisruptionTarget)
									if cond == nil {
										t.Errorf("Pod %q does not have the expected condition: %q", klog.KObj(pod), v1.DisruptionTarget)
									}
								} else {
									// 重新获取最新状态：runPausePod 返回的 Pod 对象里 DeletionTimestamp 恒为 nil，
									// 无法据此发现意外驱逐。
									current, err := cs.CoreV1().Pods(p.Namespace).Get(testCtx.Ctx, p.Name, metav1.GetOptions{})
									if err != nil {
										t.Errorf("Error getting pod %v: %v", p.Name, err)
									} else if current.DeletionTimestamp != nil {
										t.Errorf("Pod %v was unexpectedly preempted", p.Name)
									}
								}
							}
							// 还要检查抢占者 Pod 是否设置了 NominatedNodeName 字段。
							if len(test.preemptedPodIndexes) > 0 && !clearingNominatedNodeNameAfterBinding {
								if err := testutils.WaitForNominatedNodeName(testCtx.Ctx, cs, preemptor); err != nil {
									t.Errorf("NominatedNodeName field was not set for pod %v: %v", preemptor.Name, err)
								}
							}

							// 清理。
							pods = append(pods, preemptor)
							testutils.CleanupPods(testCtx.Ctx, cs, t, pods)
						})
					}
				}
			}
		}
	}
}

func TestAsyncPreemption(t *testing.T) {
	const podBlockedInBindingName = "pod-blocked-in-binding"
	const reservingPodName = "reserving-pod"
	const blockingPodName = "blocking-pod"

	type createPod struct {
		pod *v1.Pod
		// count 是该 action 创建 Pod 的次数；若使用需配置 GenerateName，默认为 1。
		count *int
	}

	type schedulePod struct {
		podName             string
		expectSuccess       bool
		expectUnschedulable bool
	}

	type scenario struct {
		// name 是该步骤的名称，仅用于调试。
		name string

		// 以下 action 只能设置其中一个。

		// createPod 创建 Pod。
		createPod *createPod
		// createNode 额外创建一个 Node。
		createNode string
		// schedulePod 调度 activeQ 队首的 Pod，需给出预期被调度的 Pod 名称。
		schedulePod *schedulePod
		// completePreemption 完成当前正在进行的抢占，需给出 Pod 名称。
		completePreemption string
		// podGatedInQueue 检查给定 Pod 是否在调度队列中且被抢占流程 gated，需给出 Pod 名称。
		podGatedInQueue string
		// podRunningPreemption 检查给定 Pod 是否正在执行抢占，值为 Pod 创建顺序的索引（第一个创建的 Pod 用 0）。
		podRunningPreemption *int
		// activatePod 将 Pod 从 unschedulable 移回 active 或 backoff，值为要激活的 Pod 名称。
		activatePod string
		// resumeBind 恢复被阻塞的 binding 操作。
		// Pod 只有在名称匹配 podBlockedInBinding 中定义的字符串时才会被阻塞。
		resumeBind bool
		// verifyPodInUnschedulable 等待一段时间后确认给定 Pod 仍在 unschedulable 池中，值为被检查的 Pod 名称。
		verifyPodInUnschedulable string
		// flushUnschedulable 清空 unschedulable 队列。
		flushUnschedulable bool
		// waitForPodsDeleted 等待指定 Pod 从集群中删除，值为按创建顺序排列的 Pod 索引数组。
		waitForPodsDeleted []int
	}

	tests := []struct {
		name string
		// 首次调度 Pod 之后执行的场景步骤。
		scenarios []scenario
	}{
		{
			// 最基础的用例：如果它失败，说明基本抢占流程出了问题。
			name: "basic: async preemption happens expectedly",
			scenarios: []scenario{
				{
					name: "create scheduled Pod",
					createPod: &createPod{
						pod:   st.MakePod().GenerateName("victim-").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Node("node").Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
						count: ptr.To(2),
					},
				},
				{
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:            "check the pod is in the queue and gated",
					podGatedInQueue: "preemptor",
				},
				{
					name:                 "check the preemptor Pod making the preemption API calls",
					podRunningPreemption: ptr.To(2),
				},
				{
					name:               "complete the preemption API calls",
					completePreemption: "preemptor",
				},
				{
					name: "schedule the preemptor Pod after the preemption",
					schedulePod: &schedulePod{
						podName:       "preemptor",
						expectSuccess: true,
					},
				},
			},
		},
		{
			name: "basic async preemption with 1 victim, preemptor gated until preemption API call finishes",
			scenarios: []scenario{
				{
					name: "create victim",
					createPod: &createPod{
						pod: st.MakePod().GenerateName("victim-").Req(map[v1.ResourceName]string{v1.ResourceCPU: "1"}).Node("node").Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
					},
				},
				{
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:            "check the preemptor Pod is in the queue and gated",
					podGatedInQueue: "preemptor",
				},
				{
					name:                 "check the preemptor Pod making the preemption API calls",
					podRunningPreemption: ptr.To(1),
				},
				{
					name:               "complete the preemption API call",
					completePreemption: "preemptor",
				},
				{
					name: "schedule the preemptor Pod again and expect it to be scheduled",
					schedulePod: &schedulePod{
						podName:       "preemptor",
						expectSuccess: true,
					},
				},
			},
		},
		{
			name: "Lower priority Pod doesn't take over the place for higher priority Pod that is running the preemption",
			scenarios: []scenario{
				{
					name: "create scheduled Pod",
					createPod: &createPod{
						pod:   st.MakePod().GenerateName("victim-").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Node("node").Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
						count: ptr.To(2),
					},
				},
				{
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor-high-priority").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor-high-priority",
						expectUnschedulable: true,
					},
				},
				{
					name:            "check the pod is in the queue and gated",
					podGatedInQueue: "preemptor-high-priority",
				},
				{
					name:                 "check the preemptor Pod making the preemption API calls",
					podRunningPreemption: ptr.To(2),
				},
				{
					// 该 Pod 优先级低于抢占者。抢占者已被提名到该节点，因此它应当不可调度。
					name: "create a second Pod that is lower priority than the first preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("pod-mid-priority").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(50).Obj(),
					},
				},
				{
					name: "schedule the mid-priority Pod",
					schedulePod: &schedulePod{
						podName:             "pod-mid-priority",
						expectUnschedulable: true,
					},
				},
				{
					name:               "complete the preemption API calls",
					completePreemption: "preemptor-high-priority",
				},
				{
					// 抢占者 Pod 应先于中等优先级 Pod 出队。
					name: "schedule the preemptor Pod again",
					schedulePod: &schedulePod{
						podName:       "preemptor-high-priority",
						expectSuccess: true,
					},
				},
				{
					name: "schedule the mid-priority Pod again",
					schedulePod: &schedulePod{
						podName:             "pod-mid-priority",
						expectUnschedulable: true,
					},
				},
			},
		},
		{
			name: "Higher priority Pod takes over the place for lower priority Pod that is running the preemption",
			scenarios: []scenario{
				{
					name: "create scheduled Pod",
					createPod: &createPod{
						pod:   st.MakePod().GenerateName("victim-").Req(map[v1.ResourceName]string{v1.ResourceCPU: "1"}).Node("node").Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
						count: ptr.To(4),
					},
				},
				{
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor-high-priority").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor-high-priority",
						expectUnschedulable: true,
					},
				},
				{
					name:            "check the pod is in the queue and gated",
					podGatedInQueue: "preemptor-high-priority",
				},
				{
					name:                 "check the preemptor Pod making the preemption API calls",
					podRunningPreemption: ptr.To(4),
				},
				{
					// 该 Pod 优先级高于抢占者。即使抢占者已被提名到该节点，它也能接管该位置。
					name: "create a second Pod that is higher priority than the first preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor-super-high-priority").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(200).Obj(),
					},
				},
				{
					name: "schedule the super-high-priority Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor-super-high-priority",
						expectUnschedulable: true,
					},
				},
				{
					name:                 "check the super-high-priority Pod making the preemption API calls",
					podRunningPreemption: ptr.To(5),
				},
				{
					// 超高优先级抢占者也会进入抢占流程，并选择高优先级抢占者看中的位置。
					// 也就是说两个 goroutine 实际上在抢占同一批 Pod。
					name:            "check the super-high-priority pod is in the queue and gated",
					podGatedInQueue: "preemptor-super-high-priority",
				},
				{
					name:               "complete the preemption API calls of super-high-priority",
					completePreemption: "preemptor-super-high-priority",
				},
				{
					name:               "complete the preemption API calls of high-priority",
					completePreemption: "preemptor-high-priority",
				},
				{
					name: "schedule the super-high-priority Pod",
					schedulePod: &schedulePod{
						podName:       "preemptor-super-high-priority",
						expectSuccess: true,
					},
				},
				{
					name: "schedule the high-priority Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor-high-priority",
						expectUnschedulable: true,
					},
				},
			},
		},
		{
			name: "Lower priority Pod can select the same place where the higher priority Pod is preempting if the node is big enough",
			scenarios: []scenario{
				{
					name: "create scheduled Pod",
					createPod: &createPod{
						pod:   st.MakePod().GenerateName("victim-").Req(map[v1.ResourceName]string{v1.ResourceCPU: "1"}).Node("node").Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
						count: ptr.To(4),
					},
				},
				{
					// 它会抢占两个 victim。
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor-high-priority").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor-high-priority",
						expectUnschedulable: true,
					},
				},
				{
					name:            "check the pod is in the queue and gated",
					podGatedInQueue: "preemptor-high-priority",
				},
				{
					name:                 "check the preemptor Pod making the preemption API calls",
					podRunningPreemption: ptr.To(4),
				},
				{
					// 该 Pod 优先级低于抢占者。抢占者已被提名到该节点，因此它应当不可调度。
					// 该 Pod 会触发抢占，目标是第一个 Pod 未选中的两个 victim。
					name: "create a second Pod that is lower priority than the first preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor-mid-priority").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").Priority(50).Obj(),
					},
				},
				{
					name: "schedule the mid-priority Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor-mid-priority",
						expectUnschedulable: true,
					},
				},
				{
					name:            "check the mid-priority pod is in the queue and gated",
					podGatedInQueue: "preemptor-mid-priority",
				},
				{
					name:                 "check the mid-priority Pod making the preemption API calls",
					podRunningPreemption: ptr.To(5),
				},
				{
					name:               "complete the preemption API calls",
					completePreemption: "preemptor-mid-priority",
				},
				{
					name:               "complete the preemption API calls",
					completePreemption: "preemptor-high-priority",
				},
				{
					// 抢占者 Pod 应先于中等优先级 Pod 出队。
					name: "schedule the preemptor Pod again",
					schedulePod: &schedulePod{
						podName:       "preemptor-high-priority",
						expectSuccess: true,
					},
				},
				{
					name: "schedule the mid-priority Pod again",
					schedulePod: &schedulePod{
						podName:       "preemptor-mid-priority",
						expectSuccess: true,
					},
				},
			},
		},
		{
			// 该场景验证 https://github.com/kubernetes/kubernetes/issues/134217 的修复：
			// victim Pod 长时间卡在 binding，抢占者尝试抢占后进入 unschedulable，随后 victim 被删除；
			// 抢占者被 Pod/Delete 事件唤醒并开始调度，而 victim 的 binding 其实尚未结束。
			name: "victim blocked in binding, preemptor pod gets scheduled after victim-in-binding is deleted",
			scenarios: []scenario{
				{
					name: "create victim Pod that is going to be blocked in binding",
					createPod: &createPod{
						pod: st.MakePod().Name(podBlockedInBindingName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
					},
				},
				{
					name: "schedule victim Pod",
					schedulePod: &schedulePod{
						podName: podBlockedInBindingName,
					},
				},
				{
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:               "complete the preemption API call",
					completePreemption: "preemptor",
				},
				{
					name: "schedule the preemptor Pod again and expect it to be scheduled (assumed victim pod was forgotten)",
					schedulePod: &schedulePod{
						podName:       "preemptor",
						expectSuccess: true,
					},
				},
				{
					name:       "resume binding of the blocked pod",
					resumeBind: true,
				},
			},
		},
		{
			// 该场景验证 https://github.com/kubernetes/kubernetes/issues/134217 的修复，但 victim 处于优雅终止中：
			// victim Pod 长时间卡在 binding，抢占者尝试抢占后进入 unschedulable，随后 victim 开始优雅终止；
			// 抢占者被 Pod/Update 事件（作用类似 AssignedPodDeleted）唤醒并开始调度，而 victim 的 binding 尚未结束。
			name: "victim blocked in binding, preemptor pod gets scheduled when victim-in-binding is under graceful termination",
			scenarios: []scenario{
				{
					name: "create victim Pod with long termination grace period that is going to be blocked in binding",
					createPod: &createPod{
						pod: st.MakePod().Name(podBlockedInBindingName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).TerminationGracePeriodSeconds(1000).Container("image").Priority(1).Obj(),
					},
				},
				{
					name: "schedule victim Pod",
					schedulePod: &schedulePod{
						podName: podBlockedInBindingName,
					},
				},
				{
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:               "complete the preemption API call",
					completePreemption: "preemptor",
				},
				{
					name: "schedule the preemptor Pod again and expect it to be scheduled (assumed victim pod was forgotten)",
					schedulePod: &schedulePod{
						podName:       "preemptor",
						expectSuccess: true,
					},
				},
			},
		},
		{
			// 该场景验证 https://github.com/kubernetes/kubernetes/issues/134217 的修复，但 victim 处于优雅终止中：
			// victim Pod 长时间卡在 binding，抢占者尝试抢占后进入 unschedulable，随后 victim 开始优雅终止；
			// 抢占者被 Pod/Update 事件（作用类似 AssignedPodDeleted）唤醒并开始调度，而 victim 的 binding 尚未结束。
			name: "victim blocked in binding, preemptor pod gets scheduled when victim-in-binding is under graceful termination",
			scenarios: []scenario{
				{
					name: "create victim Pod with long termination grace period that is going to be blocked in binding",
					createPod: &createPod{
						pod: st.MakePod().Name(podBlockedInBindingName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").TerminationGracePeriodSeconds(1000).Priority(1).Obj(),
					},
				},
				{
					name: "schedule victim Pod",
					schedulePod: &schedulePod{
						podName: podBlockedInBindingName,
					},
				},
				{
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:               "complete the preemption API call",
					completePreemption: "preemptor",
				},
				{
					name: "schedule the preemptor Pod again and expect it to be scheduled (assumed victim pod was forgotten)",
					schedulePod: &schedulePod{
						podName:       "preemptor",
						expectSuccess: true,
					},
				},
				{
					name:       "resume binding of the blocked pod",
					resumeBind: true,
				},
			},
		},
		{
			// 该场景验证 https://github.com/kubernetes/kubernetes/issues/134217 的修复，但 victim 占用了抢占者需要的资源：
			// victim Pod 长时间卡在 binding，抢占者尝试抢占后进入 unschedulable，随后 victim 被删除；
			// 抢占者被 Pod/Update 事件（类似 AssignedPodDeleted）唤醒，但仍不可调度，因为 victim 还需释放资源；
			// 恢复 victim 的 binding 后，它在失败处理中释放资源，抢占者再次被唤醒并最终完成调度。
			name: "victim blocked in binding, preemptor pod gets scheduled after victim-in-binding is deleted and its resources are unreserved",
			scenarios: []scenario{
				{
					name: "create victim Pod that is going to be blocked in binding",
					createPod: &createPod{
						pod: st.MakePod().Name(podBlockedInBindingName + reservingPodName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
					},
				},
				{
					name: "schedule victim Pod",
					schedulePod: &schedulePod{
						podName: podBlockedInBindingName + reservingPodName,
					},
				},
				{
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:               "complete the preemption API call",
					completePreemption: "preemptor",
				},
				{
					name: "schedule the preemptor Pod again and expect it to be unschedulable (resources are still reserved by the victim)",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:       "resume binding of the blocked pod",
					resumeBind: true,
				},
				{
					name: "schedule the preemptor Pod again and expect it to be scheduled (victim pod unreserved its resources)",
					schedulePod: &schedulePod{
						podName:       "preemptor",
						expectSuccess: true,
					},
				},
			},
		},
		{
			name: "gated preemptor is eventually scheduled even if victim deletion doesn't raise queue hints",
			scenarios: []scenario{
				{
					name: "create victim pods",
					createPod: &createPod{
						pod:   st.MakePod().GenerateName(fmt.Sprintf("victim-%s-", blockingPodName)).Node("node").Priority(1).Container("image").ZeroTerminationGracePeriod().Obj(),
						count: new(2),
					},
				},
				{
					name: "create preemptor",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor").Priority(100).Container("image").Obj(),
					},
				},
				{
					name: "schedule preemptor",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:                 "verify preemptor running preemption",
					podRunningPreemption: new(2),
				},
				{
					name:            "gate preemptor",
					podGatedInQueue: "preemptor",
				},
				{
					name:               "complete preemption",
					completePreemption: "preemptor",
				},
				{
					name:               "wait for victims to be deleted",
					waitForPodsDeleted: []int{0, 1},
				},
				{
					name:                     "verify preemptor is still in unschedulable queue",
					verifyPodInUnschedulable: "preemptor",
				},
				{
					name:               "flush scheduling queue",
					flushUnschedulable: true,
				},
				{
					name: "verify preemptor scheduled",
					schedulePod: &schedulePod{
						podName:       "preemptor",
						expectSuccess: true,
					},
				},
			},
		},
		{
			// 该场景验证 https://github.com/kubernetes/kubernetes/issues/134217 的修复：victim 处于优雅终止中，且占用了抢占者所需资源；
			// victim Pod 长时间卡在 binding，抢占者尝试抢占后进入 unschedulable，随后 victim 开始优雅终止；
			// 抢占者被 Pod/Update 事件（类似 AssignedPodDeleted）唤醒，但仍不可调度，因为 victim 还需释放资源；
			// 恢复 victim 的 binding 后，它在失败处理中释放资源，抢占者再次被唤醒并最终完成调度。
			name: "victim blocked in binding, preemptor pod gets scheduled after victim-in-binding is under graceful termination and its resources are unreserved",
			scenarios: []scenario{
				{
					name: "create victim Pod that is going to be blocked in binding",
					createPod: &createPod{
						pod: st.MakePod().Name(podBlockedInBindingName + reservingPodName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").TerminationGracePeriodSeconds(1000).Priority(1).Obj(),
					},
				},
				{
					name: "schedule victim Pod",
					schedulePod: &schedulePod{
						podName: podBlockedInBindingName + reservingPodName,
					},
				},
				{
					name: "create a preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the preemptor Pod",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:               "complete the preemption API call",
					completePreemption: "preemptor",
				},
				{
					name: "schedule the preemptor Pod again and expect it to be unschedulable (resources are still reserved by the victim)",
					schedulePod: &schedulePod{
						podName:             "preemptor",
						expectUnschedulable: true,
					},
				},
				{
					name:       "resume binding of the blocked pod",
					resumeBind: true,
				},
				{
					name: "schedule the preemptor Pod again and expect it to be scheduled (victim pod unreserved its resources)",
					schedulePod: &schedulePod{
						podName:       "preemptor",
						expectSuccess: true,
					},
				},
			},
		},
		{
			// 预期结果：低优先级 Pod 转移到其他节点，不会永远卡在 unschedulable 队列中。（因测试名称长度限制写在此处。）
			name: "While lower priority Pod is waiting for preemption, higher priority Pod takes its place on the node",
			scenarios: []scenario{
				{
					name: "create N-1 victim Pods on the first node",
					createPod: &createPod{
						pod:   st.MakePod().GenerateName("victim-").Req(map[v1.ResourceName]string{v1.ResourceCPU: "1"}).Node("node").Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
						count: ptr.To(3),
					},
				},
				{
					name: "create the last victim Pod on the first node, that is going to be blocked in binding",
					createPod: &createPod{
						pod: st.MakePod().Name(podBlockedInBindingName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "1"}).Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
					},
				},
				{
					name: "schedule the last victim Pod",
					schedulePod: &schedulePod{
						podName: podBlockedInBindingName,
					},
				},
				{
					name: "create a mid-priority preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor-mid-priority").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Priority(50).Obj(),
					},
				},
				{
					name: "schedule the mid-priority preemptor Pod",
					schedulePod: &schedulePod{
						podName: "preemptor-mid-priority",
					},
				},
				{
					name:               "complete the preemption API calls",
					completePreemption: "preemptor-mid-priority",
				},
				{
					name:            "check the mid-priority preemptor Pod is gated, waiting for the last victim to be preempted",
					podGatedInQueue: "preemptor-mid-priority",
				},
				{
					name:       "create node2",
					createNode: "node2",
				},
				{
					name: "create victim Pods on node2",
					createPod: &createPod{
						pod:   st.MakePod().GenerateName("victim-").Req(map[v1.ResourceName]string{v1.ResourceCPU: "1"}).Node("node2").Container("image").ZeroTerminationGracePeriod().Priority(1).Obj(),
						count: ptr.To(4),
					},
				},
				{
					name: "create a high-priority preemptor Pod",
					createPod: &createPod{
						pod: st.MakePod().Name("preemptor-high-priority").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").Priority(100).Obj(),
					},
				},
				{
					name: "schedule the high-priority preemptor Pod and expect it to get scheduled on node1",
					// 虽然没有显式检查 Pod 是否调度到 node1，但可以据此推断：
					// 不抢占时 Pod 放不进 node2，而 node1 资源充足。
					schedulePod: &schedulePod{
						podName:       "preemptor-high-priority",
						expectSuccess: true,
					},
				},
				{
					name:       "allow the preemption of the last victim Pod on node1 to finish",
					resumeBind: true,
				},
				{
					name: "check that mid-priority preemptor Pod got activated by completed preemption and try scheduling it again",
					schedulePod: &schedulePod{
						podName: "preemptor-mid-priority",
						// Pod 已无法放进 node1，应在 node2 上触发抢占。
						expectUnschedulable: true,
					},
				},
				{
					name:               "complete the preemption API calls on node2",
					completePreemption: "preemptor-mid-priority",
				},
				{
					name: "check that mid-priority Pod got activated, schedule it on node2",
					schedulePod: &schedulePod{
						podName:       "preemptor-mid-priority",
						expectSuccess: true,
					},
				},
			},
		},
	}

	// 所有用例共享同一个节点。
	node := st.MakeNode().Name("node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj()

	sharedAPICtx := testutils.InitTestAPIServer(t, "preemption", nil)

	for _, asyncAPICallsEnabled := range []bool{true} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s (Async API calls enabled: %v)", test.name, asyncAPICallsEnabled), func(t *testing.T) {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncAPICalls, asyncAPICallsEnabled)

				// 测试异步抢占行为需要自定义 preemption 插件。
				delayedPreemptionPluginName := "delay-preemption"
				var lock sync.Mutex
				// 以 Pod 名称为 key。
				preemptionDoneChannels := make(map[string]chan struct{})
				defer func() {
					lock.Lock()
					defer lock.Unlock()
					for _, ch := range preemptionDoneChannels {
						close(ch)
					}
				}()
				registry := make(frameworkruntime.Registry)
				var preemptionPlugin *defaultpreemption.DefaultPreemption
				err := registry.Register(delayedPreemptionPluginName, func(c context.Context, r runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
					p, err := frameworkruntime.FactoryAdapter(plfeature.Features{EnableAsyncPreemption: true}, defaultpreemption.New)(c, &config.DefaultPreemptionArgs{
						// 设为默认值以通过初始化校验，与测试内容无关。
						MinCandidateNodesPercentage: 10,
						MinCandidateNodesAbsolute:   100,
					}, fh)
					if err != nil {
						return nil, fmt.Errorf("error creating default preemption plugin: %w", err)
					}

					var ok bool
					preemptionPlugin, ok = p.(*defaultpreemption.DefaultPreemption)
					if !ok {
						return nil, fmt.Errorf("unexpected plugin type %T", p)
					}

					preemptPodFn := preemptionPlugin.Executor.PreemptPod
					preemptionPlugin.Executor.PreemptPod = func(ctx context.Context, c preemption.Candidate, preemptor preemption.ExecutorPreemptor, victim *v1.Pod, pluginName string) (bool, error) {
						// 阻塞抢占 goroutine，直到用例允许它继续执行。
						lock.Lock()
						ch, ok := preemptionDoneChannels[preemptor.GetName()]
						lock.Unlock()
						if ok {
							<-ch
						}
						return preemptPodFn(ctx, c, preemptor, victim, pluginName)
					}

					return preemptionPlugin, nil
				})
				if err != nil {
					t.Fatalf("Error registering a filter: %v", err)
				}

				// 注册假 bind 插件：对指定 Pod 阻塞 binding，直到 blockBindingChannel 发来恢复信号。
				blockBindingChannel := make(chan struct{})
				defer close(blockBindingChannel)
				blockingBindPluginName := "blockingBindPlugin"
				err = registry.Register(blockingBindPluginName, func(ctx context.Context, o runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
					db, err := defaultbinder.New(ctx, o, fh)
					if err != nil {
						t.Fatalf("Error creating a default binder plugin: %v", err)
					}
					var bindPlugin = blockingBindPlugin{
						name:                blockingBindPluginName,
						nameOfPodToBlock:    podBlockedInBindingName,
						realPlugin:          db.(fwk.BindPlugin),
						blockBindingChannel: blockBindingChannel,
					}
					return &bindPlugin, nil
				})
				if err != nil {
					t.Fatalf("Error registering a bind plugin: %v", err)
				}

				// 注册假插件，为一个 Pod 预留虚拟资源，用于检查 victim 必须释放资源时 scheduler 的行为。
				reservingPluginName := "reservingPlugin"
				err = registry.Register(reservingPluginName, func(ctx context.Context, o runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
					return &reservingPlugin{
						name:               reservingPluginName,
						nameOfPodToReserve: reservingPodName,
					}, nil
				})
				if err != nil {
					t.Fatalf("Error registering a reserving plugin: %v", err)
				}

				// 注册按名称过滤节点的假插件。关键是它始终返回 QueueSkip 作为队列提示，模拟有缺陷的队列提示实现。
				queueSkipFilterPluginName := "queueSkipFilterPlugin"
				err = registry.Register(queueSkipFilterPluginName, func(ctx context.Context, o runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
					return &queueSkipFilterPlugin{
						name:              queueSkipFilterPluginName,
						nameOfBlockingPod: blockingPodName,
					}, nil
				})
				if err != nil {
					t.Fatalf("Error registering a queueSkipFilterPlugin plugin: %v", err)
				}

				cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
					Profiles: []configv1.KubeSchedulerProfile{{
						SchedulerName: ptr.To(v1.DefaultSchedulerName),
						Plugins: &configv1.Plugins{
							MultiPoint: configv1.PluginSet{
								Enabled: []configv1.Plugin{
									{Name: blockingBindPluginName},
									{Name: delayedPreemptionPluginName},
									{Name: reservingPluginName},
									{Name: queueSkipFilterPluginName},
								},
								Disabled: []configv1.Plugin{
									{Name: names.DefaultPreemption},
									{Name: names.DefaultBinder},
								},
							},
						},
					}},
				})

				// 这里只初始化 scheduler，不启动它；调度周期由测试手动触发。
				testCtx := testutils.InitTestSchedulerWithOptions(t,
					withNewNamespace(t, sharedAPICtx, "preemption"),
					0,
					scheduler.WithProfiles(cfg.Profiles...),
					scheduler.WithFrameworkOutOfTreeRegistry(registry),
					// 禁用 backoff。
					scheduler.WithPodMaxBackoffSeconds(0),
					scheduler.WithPodInitialBackoffSeconds(0),
				)
				defer testCtx.SchedulerCloseFn()
				testutils.SyncSchedulerInformerFactory(testCtx)
				cs := testCtx.ClientSet

				if preemptionPlugin == nil {
					t.Fatalf("the preemption plugin should be initialized")
				}

				logger, _ := ktesting.NewTestContext(t)
				if testCtx.Scheduler.APIDispatcher != nil {
					testCtx.Scheduler.APIDispatcher.Run(logger)
					defer testCtx.Scheduler.APIDispatcher.Close()
				}
				testCtx.Scheduler.SchedulingQueue.Run(logger)
				defer testCtx.Scheduler.SchedulingQueue.Close()

				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncPreemption, true)

				createdPods := []*v1.Pod{}
				defer func() {
					testutils.CleanupPods(testCtx.Ctx, cs, t, createdPods)
				}()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				if _, err := cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
					t.Fatalf("Failed to create an initial Node %q: %v", node.Name, err)
				}
				defer func() {
					if err := cs.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{}); err != nil {
						t.Fatalf("Failed to delete the Node %q: %v", node.Name, err)
					}
				}()

				for _, scenario := range test.scenarios {
					t.Logf("Running scenario: %s", scenario.name)
					switch {
					case scenario.createNode != "":
						newNode := st.MakeNode().Name(scenario.createNode).Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj()
						if _, err := cs.CoreV1().Nodes().Create(ctx, newNode, metav1.CreateOptions{}); err != nil {
							t.Fatalf("Failed to create an initial Node %q: %v", newNode.Name, err)
						}
						defer func() {
							if err := cs.CoreV1().Nodes().Delete(ctx, newNode.Name, metav1.DeleteOptions{}); err != nil {
								t.Fatalf("Failed to delete the Node %q: %v", newNode.Name, err)
							}
						}()
					case scenario.createPod != nil:
						if scenario.createPod.count == nil {
							scenario.createPod.count = ptr.To(1)
						}

						for i := 0; i < *scenario.createPod.count; i++ {
							pod, err := cs.CoreV1().Pods(testCtx.NS.Name).Create(ctx, scenario.createPod.pod, metav1.CreateOptions{})
							if err != nil {
								t.Fatalf("Failed to create a Pod %q: %v", pod.Name, err)
							}
							createdPods = append(createdPods, pod)
						}
					case scenario.schedulePod != nil:
						lastFailure := ""
						if err := wait.PollUntilContextTimeout(testCtx.Ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
							if len(testCtx.Scheduler.SchedulingQueue.PodsInActiveQ()) == 0 {
								lastFailure = fmt.Sprintf("Expected the pod %s to be scheduled, but no pod arrives at the activeQ", scenario.schedulePod.podName)
								return false, nil
							}

							if testCtx.Scheduler.SchedulingQueue.PodsInActiveQ()[0].Name != scenario.schedulePod.podName {
								// 继续等待：队列里可能出现比当前队首优先级更高的 Pod。
								lastFailure = fmt.Sprintf("The pod %s is expected to be scheduled, but the top Pod is %s", scenario.schedulePod.podName, testCtx.Scheduler.SchedulingQueue.PodsInActiveQ()[0].Name)
								return false, nil
							}

							return true, nil
						}); err != nil {
							t.Fatal(lastFailure)
						}

						lock.Lock()
						preemptionDoneChannels[scenario.schedulePod.podName] = make(chan struct{})
						lock.Unlock()
						testCtx.Scheduler.ScheduleOne(testCtx.Ctx)

						if scenario.schedulePod.expectSuccess {
							if err := wait.PollUntilContextTimeout(testCtx.Ctx, 200*time.Millisecond, wait.ForeverTestTimeout, false, testutils.PodScheduled(cs, testCtx.NS.Name, scenario.schedulePod.podName)); err != nil {
								t.Fatalf("Expected the pod %s to be scheduled", scenario.schedulePod.podName)
							}
						} else if scenario.schedulePod.expectUnschedulable {
							if !podInUnschedulablePodPool(t, testCtx.Scheduler.SchedulingQueue, scenario.schedulePod.podName) {
								t.Fatalf("Expected the pod %s to be in the unschedulable queue after the scheduling attempt", scenario.schedulePod.podName)
							}
						}
					case scenario.activatePod != "":
						pod := unschedulablePod(t, testCtx.Scheduler.SchedulingQueue, scenario.activatePod)
						if pod == nil {
							t.Fatalf("Expected the pod %s to be in unschedulable queue before activation phase", scenario.activatePod)
						}
						m := map[string]*v1.Pod{scenario.activatePod: pod}
						testCtx.Scheduler.SchedulingQueue.Activate(logger, m)
					case scenario.completePreemption != "":
						lock.Lock()
						if _, ok := preemptionDoneChannels[scenario.completePreemption]; !ok {
							t.Fatalf("The preemptor Pod %q is not running preemption", scenario.completePreemption)
						}

						close(preemptionDoneChannels[scenario.completePreemption])
						delete(preemptionDoneChannels, scenario.completePreemption)
						lock.Unlock()
					case scenario.podGatedInQueue != "":
						// 首先确认 Pod 确实在队列中。
						if !podInUnschedulablePodPool(t, testCtx.Scheduler.SchedulingQueue, scenario.podGatedInQueue) {
							t.Fatalf("Expected the pod %s to be in the queue", scenario.podGatedInQueue)
						}

						// 激活该 Pod 后若它仍在 unsched 池中，就说明它在 PreEnqueue 扩展点被抢占流程 gated。
						testCtx.Scheduler.SchedulingQueue.Activate(logger, map[string]*v1.Pod{scenario.podGatedInQueue: st.MakePod().Namespace(testCtx.NS.Name).Name(scenario.podGatedInQueue).Obj()})
						if !podInUnschedulablePodPool(t, testCtx.Scheduler.SchedulingQueue, scenario.podGatedInQueue) {
							t.Fatalf("Expected the pod %s to be in the queue even after the activation", scenario.podGatedInQueue)
						}
						if pInfo, _ := testCtx.Scheduler.SchedulingQueue.GetPod(scenario.podGatedInQueue, testCtx.NS.Name, nil); pInfo == nil || !pInfo.Gated() {
							t.Fatalf("Expected the pod %s to be gated", scenario.podGatedInQueue)
						}
					case scenario.podRunningPreemption != nil:
						if err := wait.PollUntilContextTimeout(testCtx.Ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
							return preemptionPlugin.Executor.IsPodRunningPreemption(createdPods[*scenario.podRunningPreemption].GetUID()), nil
						}); err != nil {
							t.Fatalf("Expected the pod %s to be running preemption", createdPods[*scenario.podRunningPreemption].Name)
						}
					case scenario.resumeBind:
						blockBindingChannel <- struct{}{}
					case scenario.verifyPodInUnschedulable != "":
						if err := wait.PollUntilContextTimeout(testCtx.Ctx, 50*time.Millisecond, 200*time.Millisecond, false, func(ctx context.Context) (bool, error) {
							if !podInUnschedulablePodPool(t, testCtx.Scheduler.SchedulingQueue, scenario.verifyPodInUnschedulable) {
								return false, fmt.Errorf("expected the pod %s to remain in the unschedulable queue after the scheduling attempt", scenario.verifyPodInUnschedulable)
							}
							// 持续轮询，确认 Pod 一直留在 unschedulable 队列且未被激活。
							return false, nil
						}); err != nil && !errors.Is(err, context.DeadlineExceeded) {
							// 若因超时或 context 取消而结束轮询，说明 Pod 始终未离开 unschedulable，符合预期；
							// 若是其他错误，则说明 Pod 被意外激活或出现了别的问题。
							t.Fatalf("Error in scenario verifyPodInUnschedulable: %v", err)
						}
					case scenario.flushUnschedulable:
						testCtx.Scheduler.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)
					case len(scenario.waitForPodsDeleted) != 0:
						for _, podIndex := range scenario.waitForPodsDeleted {
							podName := createdPods[podIndex].Name
							if err := wait.PollUntilContextTimeout(testCtx.Ctx, 50*time.Millisecond, wait.ForeverTestTimeout, false, testutils.PodDeleted(testCtx.Ctx, cs, testCtx.NS.Name, podName)); err != nil {
								t.Fatalf("Failed to wait for pod %s to be deleted: %v", podName, err)
							}
						}
					}
				}
			})
		}
	}
}

// podInUnschedulablePodPool 检查给定 Pod 是否在 unschedulable pod 池中。
func podInUnschedulablePodPool(t *testing.T, queue queue.SchedulingQueue, podName string) bool {
	t.Helper()
	// 先检查 Pod 是否在 activeQ 中。
	for _, pod := range queue.PodsInActiveQ() {
		if pod.Name == podName {
			return false
		}
	}

	pendingPods, _ := queue.PendingPods()
	for _, pod := range pendingPods {
		if pod.Name == podName {
			return true
		}
	}
	return false
}

// unschedulablePod 检查给定 Pod 是否在 unschedulable 队列中，并返回该 Pod。
func unschedulablePod(t *testing.T, queue queue.SchedulingQueue, podName string) *v1.Pod {
	t.Helper()
	unschedPods := queue.UnschedulablePods()
	for _, pod := range unschedPods {
		if pod.Name == podName {
			return pod
		}
	}
	return nil
}

type queueSkipFilterPlugin struct {
	name              string
	nameOfBlockingPod string
}

func (fp *queueSkipFilterPlugin) EventsToRegister(context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{
			Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Delete},
			QueueingHintFn: func(_ klog.Logger, _ *v1.Pod, _, _ interface{}) (fwk.QueueingHint, error) {
				return fwk.QueueSkip, nil
			},
		},
	}, nil
}

func (fp *queueSkipFilterPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	for _, scheduledPod := range nodeInfo.GetPods() {
		if strings.Contains(scheduledPod.GetPod().Name, fp.nameOfBlockingPod) {
			return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %s has blocking pod %s", nodeInfo.Node().Name, scheduledPod.GetPod().Name))
		}
	}
	return nil
}

func (fp *queueSkipFilterPlugin) Name() string {
	return fp.name
}

var _ fwk.FilterPlugin = &queueSkipFilterPlugin{}
var _ fwk.EnqueueExtensions = &queueSkipFilterPlugin{}

// blockingBindPlugin 模拟耗时很长的 binding 操作。收到解除阻塞的信号后，底层仍调用 realPlugin.Bind()。
type blockingBindPlugin struct {
	name                string
	nameOfPodToBlock    string
	realPlugin          fwk.BindPlugin
	blockBindingChannel chan struct{}
}

func (bp *blockingBindPlugin) Name() string {
	return bp.name
}

func (bp *blockingBindPlugin) Bind(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) *fwk.Status {
	if strings.Contains(p.Name, bp.nameOfPodToBlock) {
		// 阻塞 bind goroutine，直到用例允许它继续。
		select {
		case <-bp.blockBindingChannel:
		case <-ctx.Done():
		}
	}
	return bp.realPlugin.Bind(ctx, state, p, nodeName)
}

var _ fwk.BindPlugin = &blockingBindPlugin{}

// reservingPlugin 为 nameOfPodToReserve Pod 在内存中预留虚拟资源；
// 只有释放这些资源后，其他 Pod 才能被调度。
type reservingPlugin struct {
	lock               sync.Mutex
	name               string
	nameOfPodToReserve string
	reserved           bool
}

func (rp *reservingPlugin) Name() string {
	return rp.name
}

func (rp *reservingPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// 任何 Pod/Delete 事件都会唤醒该 Pod。
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Delete}},
	}, nil
}

const reservingPluginStateKey = "PreFilterReserving"

type reservingPluginState struct {
	reserved bool
}

func (s reservingPluginState) Clone() fwk.StateData {
	return reservingPluginState{
		reserved: s.reserved,
	}
}

func (rp *reservingPlugin) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	rp.lock.Lock()
	state.Write(reservingPluginStateKey, reservingPluginState{reserved: rp.reserved})
	rp.lock.Unlock()
	return nil, nil
}

func (rp *reservingPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	s, err := state.Read(reservingPluginStateKey)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if s.(reservingPluginState).reserved {
		return fwk.NewStatus(fwk.Unschedulable, "resources are reserved")
	}
	return nil
}

func (rp *reservingPlugin) Reserve(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) *fwk.Status {
	if strings.Contains(p.Name, rp.nameOfPodToReserve) {
		rp.lock.Lock()
		rp.reserved = true
		rp.lock.Unlock()
	}
	return nil
}

func (rp *reservingPlugin) Unreserve(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) {
	if strings.Contains(p.Name, rp.nameOfPodToReserve) {
		rp.lock.Lock()
		rp.reserved = false
		rp.lock.Unlock()
	}
}

func (rp *reservingPlugin) PreFilterExtensions() fwk.PreFilterExtensions {
	return rp
}

func (rp *reservingPlugin) AddPod(ctx context.Context, state fwk.CycleState, podToSchedule *v1.Pod, podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	if strings.Contains(podInfoToAdd.GetPod().Name, rp.nameOfPodToReserve) {
		state.Write(reservingPluginStateKey, reservingPluginState{reserved: true})
	}
	return nil
}

func (rp *reservingPlugin) RemovePod(ctx context.Context, state fwk.CycleState, podToSchedule *v1.Pod, podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	if strings.Contains(podInfoToRemove.GetPod().Name, rp.nameOfPodToReserve) {
		state.Write(reservingPluginStateKey, reservingPluginState{reserved: false})
	}
	return nil
}

var _ fwk.PreFilterPlugin = &reservingPlugin{}
var _ fwk.FilterPlugin = &reservingPlugin{}
var _ fwk.PreFilterExtensions = &reservingPlugin{}
var _ fwk.ReservePlugin = &reservingPlugin{}

type blockedPod struct {
	blocked chan struct{}
}

// blockingPermitPlugin 是收到信号前一直阻塞的 Permit 插件。
type blockingPermitPlugin struct {
	podsToBlock map[string]*blockedPod
}

const blockingPermitPluginName = "blocking-permit-plugin"

var _ fwk.PermitPlugin = &blockingPermitPlugin{}

func newBlockingPermitPlugin(_ context.Context, _ runtime.Object, h fwk.Handle, podsToBlock map[string]*blockedPod) fwk.Plugin {
	return &blockingPermitPlugin{
		podsToBlock: podsToBlock,
	}
}

func (pl *blockingPermitPlugin) Name() string {
	return blockingPermitPluginName
}

func (pl *blockingPermitPlugin) Permit(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (*fwk.Status, time.Duration) {
	if p, ok := pl.podsToBlock[pod.Name]; ok {
		delete(pl.podsToBlock, pod.Name)
		p.blocked <- struct{}{}
		return fwk.NewStatus(fwk.Wait, "waiting"), time.Minute
	}
	return nil, 0
}

func TestPreemptionRespectsWaitingPod(t *testing.T) {
	// 测试步骤：
	// 1. 创建 blocking permit 插件：启动时发出信号，然后等待特定信号关闭；
	// 2. 创建一个可容纳低优先级 Pod 的大节点；
	// 3. 调度命中该插件的低优先级 Pod（victim），此时它已被选到大节点上；
	// 4. victim 卡在 WaitOnPermit 时，添加一个适合重新调度 victim 的小节点；
	// 5. 调度只能放在大节点上的高优先级 Pod（preemptor）；
	// 6. 预期 preemptor 调度到大节点，victim 被抢占；
	// 7. victim 应被重新调度到小节点。

	// 创建资源仅够一个 Pod 的节点。
	nodeRes := map[v1.ResourceName]string{
		v1.ResourceCPU:    "2",
		v1.ResourceMemory: "2Gi",
	}
	node := st.MakeNode().Name("big-node").Capacity(nodeRes).Obj()

	victim := st.MakePod().Name("victim").Priority(lowPriority).Req(map[v1.ResourceName]string{v1.ResourceCPU: "1", v1.ResourceMemory: "1Gi"}).Obj()
	// 抢占者需要的资源超过小节点所能提供。
	preemptor := st.MakePod().Name("preemptor").Priority(highPriority).Req(map[v1.ResourceName]string{v1.ResourceCPU: "1.5", v1.ResourceMemory: "1.5Gi"}).Obj()

	// 注册 blocking 插件。
	victimToBlock := &blockedPod{
		blocked: make(chan struct{}),
	}
	podsToBlock := map[string]*blockedPod{
		victim.Name: victimToBlock,
	}

	registry := make(frameworkruntime.Registry)
	err := registry.Register(blockingPermitPluginName, func(ctx context.Context, obj runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
		return newBlockingPermitPlugin(ctx, obj, fh, podsToBlock), nil
	})
	if err != nil {
		t.Fatalf("Error registering plugin: %v", err)
	}

	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				Permit: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: blockingPermitPluginName},
					},
				},
			},
		}},
	})
	testCtx := testutils.InitTestSchedulerWithOptions(t,
		testutils.InitTestAPIServer(t, "preemption-waiting", nil),
		0,
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(registry))
	defer testCtx.SchedulerCloseFn()
	testutils.SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)

	cs := testCtx.ClientSet

	if _, err := createNode(cs, node); err != nil {
		t.Fatalf("Error creating node: %v", err)
	}

	t.Logf("Creating victim pod")
	victim, err = cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, victim, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating victim: %v", err)
	}

	t.Logf("Waiting for victim to reach WaitOnPermit")
	select {
	case <-victimToBlock.blocked:
		t.Logf("Victim reached WaitOnPermit")
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("Timed out waiting for victim to reach WaitOnPermit")
	}
	if err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
		return testCtx.Scheduler.Profiles[v1.DefaultSchedulerName].GetWaitingPod(victim.UID) != nil, nil
	}); err != nil {
		t.Fatalf("Timed out waiting for victim to be recorded as a waiting pod: %v", err)
	}

	smallNodeRes := map[v1.ResourceName]string{
		v1.ResourceCPU:    "1",
		v1.ResourceMemory: "1Gi",
	}
	smallNode := st.MakeNode().Name("small-node").Capacity(smallNodeRes).Obj()
	if _, err := createNode(cs, smallNode); err != nil {
		t.Fatalf("Error creating node: %v", err)
	}

	t.Logf("Creating preemptor pod")
	_, err = cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, preemptor, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating preemptor: %v", err)
	}

	// 抢占者最终应完成调度，或触发对 victim 的抢占。由于 victim 卡在 WaitingOnPermit，
	// 抢占者的 PostFilter 逻辑应找到它并对其调用 PreemptPod()。插件在抢占时返回错误，
	// 因此 victim 的调度失败，但不应从 API server 中删除，而应进入 backoff 队列并最终重新调度。
	t.Logf("Waiting for preemptor to be scheduled")
	err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 15*time.Second, false, func(ctx context.Context) (bool, error) {
		// 确认 victim 未被删除。
		_, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(ctx, victim.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, fmt.Errorf("victim pod was deleted")
			}
			return false, err
		}
		// 检查抢占者是否已调度。
		p, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(ctx, preemptor.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, fmt.Errorf("preemptor pod was deleted")
			}
			return false, err
		}
		return p.Spec.NodeName != "", nil
	})
	if err != nil {
		t.Fatalf("Failed waiting for preemptor validation: %v", err)
	}

	t.Logf("waiting for victim to be rescheduled")
	err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 15*time.Second, false, func(ctx context.Context) (bool, error) {
		v, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(ctx, victim.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return v.Spec.NodeName != "", nil
	})
	if err != nil {
		t.Fatalf("Failed waiting for victim validation: %v", err)
	}

	// 检查抢占者与 victim 是否在预期节点上：victim 在小节点，抢占者在大节点。
	v, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, victim.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error getting victim: %v", err)
	}
	if v.Spec.NodeName != "small-node" {
		t.Fatalf("Victim should be scheduled on small-node, but was scheduled on %s", v.Spec.NodeName)
	}

	p, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, preemptor.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error getting preemptor: %v", err)
	}
	if p.Spec.NodeName != "big-node" {
		t.Fatalf("Preemptor should be scheduled on big-node, but was scheduled on %s", p.Spec.NodeName)
	}
}

type perPodBlockingPlugin struct {
	shouldBlock bool
	blocked     chan struct{}
	released    chan struct{}
}

// blockingPreBindPlugin 是收到信号前一直阻塞的 PreBindPlugin。
type blockingPreBindPlugin struct {
	podToChannels map[string]*perPodBlockingPlugin
	handle        fwk.Handle
}

const blockingPreBindPluginName = "blocking-prebind-plugin"

var _ fwk.PreBindPlugin = &blockingPreBindPlugin{}

func newBlockingPreBindPlugin(_ context.Context, _ runtime.Object, h fwk.Handle, podToChannels map[string]*perPodBlockingPlugin) (fwk.Plugin, error) {
	return &blockingPreBindPlugin{
		podToChannels: podToChannels,
		handle:        h,
	}, nil
}

func (pl *blockingPreBindPlugin) Name() string {
	return blockingPreBindPluginName
}

func (pl *blockingPreBindPlugin) PreBind(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, _ string) *fwk.Status {
	podBlocks, ok := pl.podToChannels[pod.Name]
	if !ok {
		return fwk.NewStatus(fwk.Error, "pod was not prepared in test case")
	}
	if !podBlocks.shouldBlock {
		return nil
	}

	close(podBlocks.blocked)
	podBlocks.shouldBlock = false
	select {
	case <-podBlocks.released:
		return nil
	case <-ctx.Done():
		return fwk.AsStatus(ctx.Err())
	}
}

func (pl *blockingPreBindPlugin) PreBindPreFlight(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	return &fwk.PreBindPreFlightResult{}, nil
}

func TestPreemptionRespectsBindingPod(t *testing.T) {
	// 测试步骤：
	// 1. 创建 blocking prebind 插件：启动时发出信号，然后等待特定信号关闭；
	// 2. 调度命中该插件的低优先级 Pod（victim）；
	// 3. victim 卡在 PreBind 时，添加小节点，并调度只能放在大节点上的高优先级 Pod（preemptor）；
	// 4. 等待 preemptor 完成调度；
	// 5. 验证 preemptor 占据大节点，victim 未被删除而是重新调度到小节点。

	// 创建资源仅够一个 Pod 的节点。
	bigNode := st.MakeNode().Name("big-node").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "2",
		v1.ResourceMemory: "2Gi",
	}).Obj()
	// victim 需要节点的全部资源。
	victim := st.MakePod().Name("victim").Priority(lowPriority).Req(map[v1.ResourceName]string{v1.ResourceCPU: "1", v1.ResourceMemory: "1Gi"}).Obj()
	// 抢占者同样需要节点的全部资源。
	preemptor := st.MakePod().Name("preemptor").Priority(highPriority).Req(map[v1.ResourceName]string{v1.ResourceCPU: "1.5", v1.ResourceMemory: "1.5Gi"}).Obj()

	// 注册 blocking 插件。
	victimBlockingPlugin := &perPodBlockingPlugin{
		shouldBlock: true,
		blocked:     make(chan struct{}),
		released:    make(chan struct{}),
	}
	podToChannels := map[string]*perPodBlockingPlugin{
		victim.Name: victimBlockingPlugin,
		preemptor.Name: {
			shouldBlock: false,
			blocked:     make(chan struct{}),
			released:    make(chan struct{}),
		},
	}

	registry := make(frameworkruntime.Registry)
	err := registry.Register(blockingPreBindPluginName, func(ctx context.Context, obj runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
		return newBlockingPreBindPlugin(ctx, obj, fh, podToChannels)
	})
	if err != nil {
		t.Fatalf("Error registering plugin: %v", err)
	}

	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				PreBind: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: blockingPreBindPluginName},
					},
				},
			},
		}},
	})

	testCtx := testutils.InitTestSchedulerWithOptions(t,
		testutils.InitTestAPIServer(t, "preemption-binding", nil),
		0,
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(registry))
	defer testCtx.SchedulerCloseFn()
	testutils.SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)

	cs := testCtx.ClientSet

	if _, err := createNode(cs, bigNode); err != nil {
		t.Fatalf("Error creating node: %v", err)
	}

	// 1. 运行 victim。
	t.Logf("Creating victim pod")
	victim, err = cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, victim, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating victim: %v", err)
	}

	// 等待 victim 到达 PreBind。
	t.Logf("Waiting for victim to reach PreBind")
	select {
	case <-victimBlockingPlugin.blocked:
		t.Logf("Victim reached PreBind")
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("Timed out waiting for victim to reach PreBind")
	}

	// 2. 加一个小节点，抢占后正好能容纳受害者 Pod。
	smallNode := st.MakeNode().Name("small-node").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "1",
		v1.ResourceMemory: "1Gi",
	}).Obj()
	if _, err := createNode(cs, smallNode); err != nil {
		t.Fatalf("Error creating node: %v", err)
	}

	// 3. 运行抢占者 Pod。
	t.Logf("Creating preemptor pod")
	preemptor, err = cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, preemptor, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating preemptor: %v", err)
	}

	// 4. 等待 victim 被重新调度。抢占者应完成调度或触发对 victim 的抢占。
	// victim 处于 PreBind（Binding Cycle）中，抢占者的 PostFilter 逻辑应找到它，
	// 对其 BindingPod 调用 CancelPod()，使其进入 backoff 队列。
	// victim 不应从 API server 删除，而应重新调度到更小的节点上。
	err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 10*time.Second, false, func(ctx context.Context) (bool, error) {
		// 检查 victim 是否被删除。
		v, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(ctx, victim.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, fmt.Errorf("victim pod was deleted")
			}
			return false, err
		}
		// 检查 victim 是否被重新调度。
		_, cond := podutil.GetPodCondition(&v.Status, v1.PodScheduled)
		if cond != nil && cond.Status == v1.ConditionTrue {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("Failed waiting for victim validation: %v", err)
	}

	// 5. 等待抢占者完成调度。
	err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 10*time.Second, false, func(ctx context.Context) (bool, error) {
		p, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(ctx, preemptor.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		// 检查抢占者是否已调度。
		_, cond := podutil.GetPodCondition(&p.Status, v1.PodScheduled)
		if cond != nil && cond.Status == v1.ConditionTrue {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("Failed waiting for preemptor to be scheduled: %v", err)
	}

	// 6. 检查抢占者与 victim 是否在预期节点上：victim 在小节点，抢占者在大节点。
	v, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, victim.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error getting victim: %v", err)
	}

	p, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, preemptor.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error getting preemptor: %v", err)
	}
	// 验证节点分配符合预期。
	if v.Spec.NodeName != "small-node" {
		t.Errorf("victim should be scheduled on small-node, but was scheduled on %s", v.Spec.NodeName)
	}
	if p.Spec.NodeName != "big-node" {
		t.Errorf("preemptor should be scheduled on big-node, but was scheduled on %s", p.Spec.NodeName)
	}
	// 释放插件，确保能干净地收尾。
	close(victimBlockingPlugin.released)
}

var defaultPodRes = &v1.ResourceRequirements{Requests: v1.ResourceList{
	v1.ResourceCPU:    *resource.NewMilliQuantity(100, resource.DecimalSI),
	v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
}

func makePod(name string, priority int32, labelKey, labelValue string) *v1.Pod {
	pod := st.MakePod().Name(name).Priority(priority).Label(labelKey, labelValue).Container("pause").Obj()
	pod.Spec.Containers[0].Resources = *defaultPodRes
	return pod
}

func makePodWithAntiAffinityNode(name string, priority int32, labelKey, labelValue, antiAffinityVal string) *v1.Pod {
	pod := st.MakePod().Name(name).Priority(priority).Label(labelKey, labelValue).Container("pause").
		PodAntiAffinityIn("pod", "node", []string{antiAffinityVal}, st.PodAntiAffinityWithRequiredReq).Obj()
	pod.Spec.Containers[0].Resources = *defaultPodRes
	return pod
}

func makePodWithAntiAffinityHostname(name string, priority int32, labelKey, labelValue, antiAffinityVal string) *v1.Pod {
	pod := st.MakePod().Name(name).Priority(priority).Label(labelKey, labelValue).Container("pause").
		PodAntiAffinityIn("pod", v1.LabelHostname, []string{antiAffinityVal}, st.PodAntiAffinityWithRequiredReq).Obj()
	pod.Spec.Containers[0].Resources = *defaultPodRes
	return pod
}

func TestInterPodAffinityPreemption(t *testing.T) {
	type createPod struct {
		pod *v1.Pod
	}

	type schedulePod struct {
		podName             string
		expectSuccess       bool
		expectUnschedulable bool
	}

	type scenario struct {
		name                string
		createPod           *createPod
		createNode          string
		schedulePod         *schedulePod
		verifyPodDeleted    string
		verifyPodNotDeleted string
		waitForNominated    string
	}

	// 在 defaultPodRes 下，一个测试节点最多可容纳 5 个 Pod。

	maxTokens := 1000

	tests := []struct {
		name                   string
		initTokens             int
		enablePreFilter        bool
		unresolvable           bool
		scenarios              []scenario
		genericWorkloadEnabled bool
	}{
		{
			name:       "preemption is performed to satisfy anti-affinity",
			initTokens: maxTokens,
			scenarios: []scenario{
				{
					name: "create pod-0",
					createPod: &createPod{
						pod: makePod("pod-0", mediumPriority, "pod", "p0"),
					},
				},
				{
					name: "schedule pod-0",
					schedulePod: &schedulePod{
						podName:       "pod-0",
						expectSuccess: true,
					},
				},
				{
					name: "create low priority pod-1 that can be preempted by preemptor",
					createPod: &createPod{
						pod: makePodWithAntiAffinityNode("pod-1", lowPriority, "pod", "p1", "preemptor"),
					},
				},
				{
					name: "schedule pod-1",
					schedulePod: &schedulePod{
						podName:       "pod-1",
						expectSuccess: true,
					},
				},
				{
					name: "create preemptor",
					createPod: &createPod{
						pod: makePodWithAntiAffinityNode("preemptor-pod", highPriority, "pod", "preemptor", "p0"),
					},
				},
				{
					name: "schedule preemptor",
					schedulePod: &schedulePod{
						podName:             "preemptor-pod",
						expectUnschedulable: true,
					},
				},
				{
					name:             "verify pod-0 is preempted",
					verifyPodDeleted: "pod-0",
				},
				{
					name:             "verify pod-1 is preempted",
					verifyPodDeleted: "pod-1",
				},
				{
					name:             "verify nominated node name is set",
					waitForNominated: "preemptor-pod",
				},
			},
		},
		{
			name:       "preemption is not performed when anti-affinity is not satisfied",
			initTokens: maxTokens,
			scenarios: []scenario{
				{
					name: "create pod-0",
					createPod: &createPod{
						pod: makePod("pod-0", mediumPriority, "pod", "p0"),
					},
				},
				{
					name: "schedule pod-0",
					schedulePod: &schedulePod{
						podName:       "pod-0",
						expectSuccess: true,
					},
				},
				{
					name: "create high priority pod-1 that cannot be preempted by preemptor",
					createPod: &createPod{
						pod: makePodWithAntiAffinityNode("pod-1", highPriority, "pod", "p1", "preemptor"),
					},
				},
				{
					name: "schedule pod-1",
					schedulePod: &schedulePod{
						podName:       "pod-1",
						expectSuccess: true,
					},
				},
				{
					name: "create preemptor",
					createPod: &createPod{
						pod: makePodWithAntiAffinityNode("preemptor-pod", highPriority, "pod", "preemptor", "p0"),
					},
				},
				{
					name: "schedule preemptor",
					schedulePod: &schedulePod{
						podName:             "preemptor-pod",
						expectUnschedulable: true,
					},
				},
				{
					name:                "verify pod-0 is not preempted",
					verifyPodNotDeleted: "pod-0",
				},
				{
					name:                "verify pod-1 is not preempted",
					verifyPodNotDeleted: "pod-1",
				},
			},
		},
		{
			name:       "basic pod preemption with hostname anti-affinity (fast path)",
			initTokens: maxTokens,
			scenarios: []scenario{
				{
					name: "create pod-0",
					createPod: &createPod{
						pod: makePod("pod-0", mediumPriority, "pod", "p0"),
					},
				},
				{
					name: "schedule pod-0",
					schedulePod: &schedulePod{
						podName:       "pod-0",
						expectSuccess: true,
					},
				},
				{
					name: "create low priority pod-1 that can be preempted by preemptor",
					createPod: &createPod{
						pod: makePodWithAntiAffinityHostname("pod-1", lowPriority, "pod", "p1", "preemptor"),
					},
				},
				{
					name: "schedule pod-1",
					schedulePod: &schedulePod{
						podName:       "pod-1",
						expectSuccess: true,
					},
				},
				{
					name: "create preemptor",
					createPod: &createPod{
						pod: makePodWithAntiAffinityHostname("preemptor-pod", highPriority, "pod", "preemptor", "p0"),
					},
				},
				{
					name: "schedule preemptor",
					schedulePod: &schedulePod{
						podName:             "preemptor-pod",
						expectUnschedulable: true,
					},
				},
				{
					name:             "verify pod-0 is preempted",
					verifyPodDeleted: "pod-0",
				},
				{
					name:             "verify pod-1 is preempted",
					verifyPodDeleted: "pod-1",
				},
				{
					name:             "verify nominated node name is set",
					waitForNominated: "preemptor-pod",
				},
			},
		},
	}

	for _, clearingNominatedNodeNameAfterBinding := range []bool{true, false} {
		for _, genericOpts := range []struct{ genericWorkloadEnabled, cpgEnabled bool }{
			{genericWorkloadEnabled: true, cpgEnabled: true},
			{genericWorkloadEnabled: true, cpgEnabled: false},
			{genericWorkloadEnabled: false, cpgEnabled: false},
		} {
			genericWorkloadEnabled := genericOpts.genericWorkloadEnabled
			cpgEnabled := genericOpts.cpgEnabled
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:                       genericWorkloadEnabled,
				features.CompositePodGroup:                     cpgEnabled,
				features.TopologyAwareWorkloadScheduling:       cpgEnabled,
				features.ClearingNominatedNodeNameAfterBinding: clearingNominatedNodeNameAfterBinding,
			})
			sharedAPICtx := testutils.InitTestAPIServer(t, "preemption", nil)

			for _, asyncPreemptionEnabled := range []bool{true, false} {
				for _, fpEnabled := range []bool{true, false} {
					for _, test := range tests {
						nameSuffix := fmt.Sprintf("Async preemption enabled: %v, ClearingNominatedNodeNameAfterBinding: %v, fpEnabled: %v, genericWorkloadEnabled: %v, cpgEnabled: %v", asyncPreemptionEnabled, clearingNominatedNodeNameAfterBinding, fpEnabled, genericWorkloadEnabled, cpgEnabled)
						t.Run(fmt.Sprintf("%s (%s)", test.name, nameSuffix), func(t *testing.T) {
							// feature gate 与测试组合一一对应。
							featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
								features.SchedulerAsyncPreemption:              asyncPreemptionEnabled,
								features.ClearingNominatedNodeNameAfterBinding: clearingNominatedNodeNameAfterBinding,
								features.InterPodAffinityHostnameFastPath:      fpEnabled,
								features.GenericWorkload:                       genericWorkloadEnabled,
								features.CompositePodGroup:                     cpgEnabled,
								features.TopologyAwareWorkloadScheduling:       cpgEnabled,
							})

							var filter tokenFilter
							registry := make(frameworkruntime.Registry)
							err := registry.Register(filterPluginName, func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
								return &filter, nil
							})
							if err != nil {
								t.Fatalf("Error registering a filter: %v", err)
							}

							cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
								Profiles: []configv1.KubeSchedulerProfile{{
									SchedulerName: ptr.To(v1.DefaultSchedulerName),
									Plugins: &configv1.Plugins{
										Filter: configv1.PluginSet{
											Enabled: []configv1.Plugin{
												{Name: filterPluginName},
											},
										},
										PreFilter: configv1.PluginSet{
											Enabled: []configv1.Plugin{
												{Name: filterPluginName},
											},
										},
									},
								}},
							})

							testCtx := testutils.InitTestSchedulerWithOptions(t,
								withNewNamespace(t, sharedAPICtx, "preemption"),
								0,
								scheduler.WithProfiles(cfg.Profiles...),
								scheduler.WithFrameworkOutOfTreeRegistry(registry),
								// 禁用 backoff。
								scheduler.WithPodMaxBackoffSeconds(0),
								scheduler.WithPodInitialBackoffSeconds(0),
							)
							defer testCtx.SchedulerCloseFn()
							testutils.SyncSchedulerInformerFactory(testCtx)

							cs := testCtx.ClientSet

							logger, _ := ktesting.NewTestContext(t)
							if testCtx.Scheduler.APIDispatcher != nil {
								testCtx.Scheduler.APIDispatcher.Run(logger)
								defer testCtx.Scheduler.APIDispatcher.Close()
							}
							testCtx.Scheduler.SchedulingQueue.Run(logger)
							defer testCtx.Scheduler.SchedulingQueue.Close()

							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							defer testCtx.SchedulerCloseFn()

							// 创建带有资源容量和标签的节点。
							nodeRes := map[v1.ResourceName]string{
								v1.ResourcePods:   "32",
								v1.ResourceCPU:    "500m",
								v1.ResourceMemory: "500",
							}
							nodeObject := st.MakeNode().Name("node1").Capacity(nodeRes).Label("node", "node1").Label(v1.LabelHostname, "node1").Obj()

							if _, err := cs.CoreV1().Nodes().Create(ctx, nodeObject, metav1.CreateOptions{}); err != nil {
								t.Fatalf("Failed to create an initial Node %q: %v", nodeObject.Name, err)
							}
							defer func() {
								if err := cs.CoreV1().Nodes().Delete(ctx, nodeObject.Name, metav1.DeleteOptions{}); err != nil {
									t.Fatalf("Failed to delete the Node %q: %v", nodeObject.Name, err)
								}
							}()

							filter.Tokens = test.initTokens
							filter.EnablePreFilter = test.enablePreFilter
							filter.Unresolvable = test.unresolvable

							var createdPods []*v1.Pod
							defer func() {
								testutils.CleanupPods(testCtx.Ctx, cs, t, createdPods)
							}()

							for _, scenario := range test.scenarios {
								t.Logf("Running scenario: %s", scenario.name)
								switch {
								case scenario.createNode != "":
									newNode := st.MakeNode().Name(scenario.createNode).Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj()
									if _, err := cs.CoreV1().Nodes().Create(ctx, newNode, metav1.CreateOptions{}); err != nil {
										t.Fatalf("Failed to create an initial Node %q: %v", newNode.Name, err)
									}
									defer func() {
										if err := cs.CoreV1().Nodes().Delete(ctx, newNode.Name, metav1.DeleteOptions{}); err != nil {
											t.Fatalf("Failed to delete the Node %q: %v", newNode.Name, err)
										}
									}()
								case scenario.createPod != nil:
									pod, err := cs.CoreV1().Pods(testCtx.NS.Name).Create(ctx, scenario.createPod.pod, metav1.CreateOptions{})
									if err != nil {
										t.Fatalf("Failed to create a Pod %q: %v", pod.Name, err)
									}
									createdPods = append(createdPods, pod)
								case scenario.schedulePod != nil:
									lastFailure := ""
									if err := wait.PollUntilContextTimeout(testCtx.Ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
										if len(testCtx.Scheduler.SchedulingQueue.PodsInActiveQ()) == 0 {
											lastFailure = fmt.Sprintf("Expected the pod %s to be scheduled, but no pod arrives at the activeQ", scenario.schedulePod.podName)
											return false, nil
										}

										if testCtx.Scheduler.SchedulingQueue.PodsInActiveQ()[0].Name != scenario.schedulePod.podName {
											// 继续等待：队列里可能出现比当前队首优先级更高的 Pod。
											lastFailure = fmt.Sprintf("The pod %s is expected to be scheduled, but the top Pod is %s", scenario.schedulePod.podName, testCtx.Scheduler.SchedulingQueue.PodsInActiveQ()[0].Name)
											return false, nil
										}

										return true, nil
									}); err != nil {
										t.Fatal(lastFailure)
									}

									testCtx.Scheduler.ScheduleOne(testCtx.Ctx)

									if scenario.schedulePod.expectSuccess {
										if err := wait.PollUntilContextTimeout(testCtx.Ctx, 200*time.Millisecond, wait.ForeverTestTimeout, false, testutils.PodScheduled(cs, testCtx.NS.Name, scenario.schedulePod.podName)); err != nil {
											t.Fatalf("Expected the pod %s to be scheduled", scenario.schedulePod.podName)
										}
									} else if scenario.schedulePod.expectUnschedulable {
										if !podInUnschedulablePodPool(t, testCtx.Scheduler.SchedulingQueue, scenario.schedulePod.podName) {
											t.Fatalf("Expected the pod %s to be in the unschedulable queue after the scheduling attempt", scenario.schedulePod.podName)
										}
									}
								case scenario.verifyPodDeleted != "":
									if err := wait.PollUntilContextTimeout(testCtx.Ctx, 50*time.Millisecond, wait.ForeverTestTimeout, false, testutils.PodDeleted(testCtx.Ctx, cs, testCtx.NS.Name, scenario.verifyPodDeleted)); err != nil {
										t.Fatalf("Failed to wait for pod %s to be deleted: %v", scenario.verifyPodDeleted, err)
									}
								case scenario.verifyPodNotDeleted != "":
									// 稍等片刻，确认它没有被驱逐。
									time.Sleep(200 * time.Millisecond)
									p, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, scenario.verifyPodNotDeleted, metav1.GetOptions{})
									if err != nil {
										t.Errorf("Error getting pod %v: %v", scenario.verifyPodNotDeleted, err)
									} else if p.DeletionTimestamp != nil {
										t.Errorf("Pod %v was unexpectedly preempted", p.Name)
									} else {
										_, cond := podutil.GetPodCondition(&p.Status, v1.DisruptionTarget)
										if cond != nil {
											t.Errorf("Pod %q was evicted unexpectedly.", klog.KObj(p))
										}
									}
								case scenario.waitForNominated != "":
									if !clearingNominatedNodeNameAfterBinding {
										preemptor, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, scenario.waitForNominated, metav1.GetOptions{})
										if err != nil {
											t.Fatalf("Failed to get preemptor pod: %v", err)
										}
										if err := testutils.WaitForNominatedNodeName(testCtx.Ctx, cs, preemptor); err != nil {
											t.Errorf("NominatedNodeName field was not set for pod %v: %v", preemptor.Name, err)
										}
									}
								}
							}
						})
					}
				}
			}
		}
	}
}

// withNewNamespace 创建子 TestContext：复用父级的 API server，但使用全新的 Namespace。
// t.Cleanup 只删除 Namespace，API server 的生命周期由调用方管理。
// 多个子测试共享同一 API server 时，可避免每个用例重复启动的开销。
func withNewNamespace(t *testing.T, parent *testutils.TestContext, nsPrefix string) *testutils.TestContext {
	t.Helper()
	ctx, cancel := context.WithCancel(parent.Ctx)
	child := &testutils.TestContext{
		ClientSet:  parent.ClientSet,
		KubeConfig: parent.KubeConfig,
		Ctx:        ctx,
		CloseFn:    func() {}, // API server 归父级所有，此处不要关闭。
	}
	child.NS = testfwk.CreateNamespaceOrDie(child.ClientSet, nsPrefix+string(k8suuid.NewUUID()), t)
	t.Cleanup(func() {
		cancel()
		testfwk.DeleteNamespaceOrDie(child.ClientSet, child.NS, t)
	})
	return child
}
