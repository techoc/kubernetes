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

package podgrouppodscount

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func init() {
	metrics.Register()
}

// mockProposedAssignment 是测试用的 ProposedAssignment 实现，
// 只携带节点名和 PodInfo，不提供 CycleState。
type mockProposedAssignment struct {
	nodeName string
	podInfo  *framework.PodInfo
}

var _ fwk.ProposedAssignment = &mockProposedAssignment{}

// GetNodeName 返回拟分配到的节点名。
func (pa *mockProposedAssignment) GetNodeName() string {
	return pa.nodeName
}

// GetPod 返回拟分配的 Pod。
func (pa *mockProposedAssignment) GetPod() *v1.Pod {
	return pa.podInfo.GetPod()
}

// GetPodInfo 返回拟分配 Pod 的 PodInfo。
func (pa *mockProposedAssignment) GetPodInfo() fwk.PodInfo {
	return pa.podInfo
}

// GetCycleState 返回 nil：ScorePlacement 不需要 cycle state。
func (pa *mockProposedAssignment) GetCycleState() fwk.CycleState {
	return nil
}

// TestScorePlacement 验证 ScorePlacement 的得分等于快照中已分配 Pod、
// 已假定 Pod 与拟分配 Pod 数量之和，并覆盖 composite pod group 的多级层级。
func TestScorePlacement(t *testing.T) {
	type scheduledPod struct {
		pathToRoot   []string
		assignedNode string
	}

	createPodWithoutNode := func(podName, podGroupName string) *v1.Pod {
		return st.MakePod().Name(podName).Namespace("default").UID(podName).PodGroupName(podGroupName).Obj()
	}

	podInfo1, _ := framework.NewPodInfo(createPodWithoutNode("proposed-pod-1", "root"))
	podInfo2, _ := framework.NewPodInfo(createPodWithoutNode("proposed-pod-2", "root"))
	proposedAssignments := []fwk.ProposedAssignment{
		&mockProposedAssignment{
			nodeName: "node1",
			podInfo:  podInfo1,
		},
		&mockProposedAssignment{
			nodeName: "node2",
			podInfo:  podInfo2,
		},
	}

	tests := []struct {
		name          string
		podGroupInfo  fwk.PodGroupInfo
		assignedPods  []scheduledPod // 加入快照（已分配）的 Pod
		assumedPods   []scheduledPod // 在快照中假定的 Pod
		placement     *fwk.PodGroupAssignments
		expectedScore int64
	}{
		{
			name:         "existing assigned and assumed pods",
			podGroupInfo: makePodGroup(),
			assignedPods: []scheduledPod{
				{assignedNode: "node2"},
				{assignedNode: "node3"},
			},
			assumedPods: []scheduledPod{
				{assignedNode: "node1"},
			},
			placement: &fwk.PodGroupAssignments{
				ProposedAssignments: proposedAssignments,
			},
			expectedScore: 5, // 1 假定 + 2 已分配 + 2 拟分配 = 5
		},
		{
			name:         "no assumed pods",
			podGroupInfo: makePodGroup(),
			assignedPods: []scheduledPod{
				{assignedNode: "node1"},
			},
			placement: &fwk.PodGroupAssignments{
				ProposedAssignments: proposedAssignments,
			},
			expectedScore: 3, // 1 已分配 + 2 拟分配 = 3
		},
		{
			name:         "no assigned pods",
			podGroupInfo: makePodGroup(),
			assumedPods: []scheduledPod{
				{assignedNode: "node1"},
			},
			placement: &fwk.PodGroupAssignments{
				ProposedAssignments: proposedAssignments,
			},
			expectedScore: 3, // 1 假定 + 2 拟分配 = 3
		},
		{
			name:         "no assigned pods, no assumed pods",
			podGroupInfo: makePodGroup(),
			placement: &fwk.PodGroupAssignments{
				ProposedAssignments: proposedAssignments,
			},
			expectedScore: 2, // 2 拟分配
		},
		{
			name:         "for cpg existing assigned and assumed pods across multi-level hierarchies",
			podGroupInfo: makeCompositePodGroup(),
			assignedPods: []scheduledPod{
				{
					pathToRoot:   []string{"pg1", "cpg1"},
					assignedNode: "node2",
				},
				{
					pathToRoot:   []string{"pg2", "cpg2"},
					assignedNode: "node3",
				},
			},
			assumedPods: []scheduledPod{
				{
					pathToRoot:   []string{"pg1", "cpg1"},
					assignedNode: "node1",
				},
			},
			placement: &fwk.PodGroupAssignments{
				ProposedAssignments: proposedAssignments,
			},
			expectedScore: 5, // 1 假定 + 2 已分配（分属 pg1、pg2）+ 2 拟分配 = 5
		},
		{
			name:         "for cpg with no assumed pods",
			podGroupInfo: makeCompositePodGroup(),
			assignedPods: []scheduledPod{
				{
					pathToRoot:   []string{"pg1", "cpg1"},
					assignedNode: "node1",
				},
			},
			placement: &fwk.PodGroupAssignments{
				ProposedAssignments: proposedAssignments,
			},
			expectedScore: 3, // 1 已分配 + 2 拟分配 = 3
		},
		{
			name:         "for cpg with no assigned pods",
			podGroupInfo: makeCompositePodGroup(),
			assumedPods: []scheduledPod{
				{
					pathToRoot:   []string{"pg1", "cpg1"},
					assignedNode: "node1",
				},
			},
			placement: &fwk.PodGroupAssignments{
				ProposedAssignments: proposedAssignments,
			},
			expectedScore: 3, // 1 假定 + 2 拟分配 = 3
		},
		{
			name:         "for cpg with no assigned pods, no assumed pods",
			podGroupInfo: makeCompositePodGroup(),
			placement: &fwk.PodGroupAssignments{
				ProposedAssignments: proposedAssignments,
			},
			expectedScore: 2, // 2 拟分配
		},
	}

	for _, cpgEnabled := range []bool{false, true} {
		for _, tt := range tests {
			if !cpgEnabled && tt.podGroupInfo.GetCompositePodGroup() != nil {
				continue
			}
			t.Run(fmt.Sprintf("%s (cpg=%v)", tt.name, cpgEnabled), func(t *testing.T) {
				featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
					features.GenericWorkload:                 true,
					features.TopologyAwareWorkloadScheduling: true,
					features.CompositePodGroup:               cpgEnabled,
				})
				_, tCtx := ktesting.NewTestContext(t)

				pgs := []schedulingapi.PodGroup{}
				cpgs := []schedulingv1alpha3.CompositePodGroup{}
				pods := []*v1.Pod{}
				alreadyAdded := sets.New[string]()
				namespace := tt.podGroupInfo.GetNamespace()

				for i, scheduledPod := range tt.assignedPods {
					parent := tt.podGroupInfo.GetName()
					for j, entityName := range slices.Backward(scheduledPod.pathToRoot) {
						if !alreadyAdded.Has(entityName) {
							if j == 0 {
								pgs = append(pgs, *st.MakePodGroup().Name(entityName).Namespace(namespace).ParentCompositePodGroup(parent).Obj())
							} else {
								cpgs = append(cpgs, *st.MakeCompositePodGroup().Name(entityName).Namespace(namespace).ParentCompositePodGroup(parent).Obj())
							}
							alreadyAdded.Insert(entityName)
						}
						parent = entityName
					}
					name := fmt.Sprintf("assigned-pod%v", i)
					pods = append(pods, st.MakePod().Name(name).UID(name).Namespace(namespace).Node(scheduledPod.assignedNode).PodGroupName(parent).Obj())
				}

				for i, scheduledPod := range tt.assumedPods {
					parent := tt.podGroupInfo.GetName()
					for j, entityName := range slices.Backward(scheduledPod.pathToRoot) {
						if !alreadyAdded.Has(entityName) {
							if j == 0 {
								pgs = append(pgs, *st.MakePodGroup().Name(entityName).Namespace(namespace).ParentCompositePodGroup(parent).Obj())
							} else {
								cpgs = append(cpgs, *st.MakeCompositePodGroup().Name(entityName).Namespace(namespace).ParentCompositePodGroup(parent).Obj())
							}
							alreadyAdded.Insert(entityName)
						}
						parent = entityName
					}
					name := fmt.Sprintf("assumed-pod%v", i)
					pods = append(pods, st.MakePod().Name(name).UID(name).Namespace(namespace).Node(scheduledPod.assignedNode).PodGroupName(parent).Obj())
				}

				if cpg := tt.podGroupInfo.GetCompositePodGroup(); cpg != nil {
					cpgs = append(cpgs, *cpg)
				} else {
					pgs = append(pgs, *tt.podGroupInfo.GetPodGroup())
				}

				cs := clientsetfake.NewClientset(
					&schedulingapi.PodGroupList{Items: pgs},
					&schedulingv1alpha3.CompositePodGroupList{Items: cpgs},
				)
				informerFactory := informers.NewSharedInformerFactory(cs, 0)
				_ = informerFactory.Scheduling().V1beta1().PodGroups().Informer()
				_ = informerFactory.Scheduling().V1alpha3().CompositePodGroups().Informer()
				informerFactory.StartWithContext(tCtx)
				informerFactory.WaitForCacheSyncWithContext(tCtx)

				pgPtrs := make([]*schedulingapi.PodGroup, len(pgs))
				cpgPtrs := make([]*schedulingv1alpha3.CompositePodGroup, len(cpgs))
				for i := range pgs {
					pgPtrs[i] = &pgs[i]
				}
				for i := range cpgs {
					cpgPtrs[i] = &cpgs[i]
				}

				snapshot := internalcache.NewTestSnapshotWithCompositePodGroups(pods, nil, pgPtrs, cpgPtrs)

				fh, _ := frameworkruntime.NewFramework(tCtx, nil, nil,
					frameworkruntime.WithInformerFactory(informerFactory),
					frameworkruntime.WithSnapshotSharedLister(snapshot),
				)

				plugin, err := New(tCtx, nil, fh, feature.NewSchedulerFeaturesFromGates(utilfeature.DefaultFeatureGate))
				if err != nil {
					t.Fatalf("Failed to create plugin: %v", err)
				}
				pl := plugin.(*PodGroupPodsCount)

				score, status := pl.ScorePlacement(tCtx, nil, tt.podGroupInfo, tt.placement)
				if !status.IsSuccess() {
					t.Errorf("ScorePlacement failed: %v", status.Message())
				}
				if score != tt.expectedScore {
					t.Errorf("Expected score %d, got %d", tt.expectedScore, score)
				}
			})
		}
	}
}

// TestNormalizePlacementScore 验证分数按最大值线性归一化到 0-100 区间：
// 最低分得 0，最高分得 100，中间分按比例缩放；全为 0 时报错。
func TestNormalizePlacementScore(t *testing.T) {
	tests := []struct {
		name                     string
		scores                   []fwk.PlacementScore
		expectedNormalizedScores []fwk.PlacementScore
		expectedError            string
	}{
		{
			name: "distinct scores",
			scores: []fwk.PlacementScore{
				{Score: 10},
				{Score: 50},
				{Score: 110},
			},
			// 归一化公式：score * (MaxScore - MinScore) / maxCount。
			// MinScore=0、MaxScore=100、maxCount=110（整数除法）：
			// 10 * 100 / 110 = 9
			// 50 * 100 / 110 = 45
			// 110 * 100 / 110 = 100
			expectedNormalizedScores: []fwk.PlacementScore{
				{Score: 9},
				{Score: 45},
				{Score: 100},
			},
		},
		{
			name: "equal scores",
			scores: []fwk.PlacementScore{
				{Score: 50},
				{Score: 50},
			},
			expectedNormalizedScores: []fwk.PlacementScore{
				{Score: 100},
				{Score: 100},
			},
		},
		{
			name: "single score",
			scores: []fwk.PlacementScore{
				{Score: 50},
			},
			expectedNormalizedScores: []fwk.PlacementScore{
				{Score: 100},
			},
		},
		{
			name: "some minimal score that is far from a group of scores located closely", // 验证归一化不会把极端低分与聚集的高分均匀拉开
			scores: []fwk.PlacementScore{
				{Score: 11},
				{Score: 100},
				{Score: 101},
				{Score: 102},
			},
			expectedNormalizedScores: []fwk.PlacementScore{
				{Score: 10},
				{Score: 98},
				{Score: 99},
				{Score: 100},
			},
		},
		{
			name:          "zero scores",
			scores:        []fwk.PlacementScore{{Score: 0}, {Score: 0}},
			expectedError: `no pods from pod group are assigned to any of the candidate placements`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := &PodGroupPodsCount{}
			pgInfo := &framework.PodGroupInfo{Name: "pg1", Type: fwk.PodGroupKeyType}
			status := pl.NormalizePlacementScore(context.Background(), nil, pgInfo, tt.scores)
			if tt.expectedError != "" {
				if status.IsSuccess() {
					t.Fatal("Expected error, but got success")
				}
				if tt.expectedError != status.Message() {
					t.Errorf("Unexpected error message. Want %s\n, got %s", tt.expectedError, status.Message())
				}
				return
			}

			if !status.IsSuccess() {
				t.Errorf("NormalizePlacementScore failed: %v", status.Message())
			}

			if diff := cmp.Diff(tt.expectedNormalizedScores, tt.scores); diff != "" {
				t.Errorf("Unexpected scores (-want, +got):\n%s", diff)
			}
		})
	}
}

// makePodGroupInfoFromPG 由 PodGroup 对象构造测试用 PodGroupInfo。
func makePodGroupInfoFromPG(pg *schedulingapi.PodGroup) fwk.PodGroupInfo {
	return &framework.PodGroupInfo{
		Name:      pg.Name,
		Namespace: pg.Namespace,
		PodGroup:  pg,
		Type:      fwk.PodGroupKeyType,
	}
}

// makePodGroupInfoFromCPG 由 CompositePodGroup 对象构造测试用 PodGroupInfo。
func makePodGroupInfoFromCPG(cpg *schedulingv1alpha3.CompositePodGroup) fwk.PodGroupInfo {
	return &framework.PodGroupInfo{
		Name:              cpg.Name,
		Namespace:         cpg.Namespace,
		CompositePodGroup: cpg,
		Type:              fwk.CompositePodGroupKeyType,
	}
}

// makePodGroup 构造名为 "root" 的测试用 PodGroupInfo。
func makePodGroup() fwk.PodGroupInfo {
	return makePodGroupInfoFromPG(&schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "root",
			Namespace: "default",
		},
	})
}

// makeCompositePodGroup 构造名为 "root" 的测试用 composite pod group 的 PodGroupInfo。
func makeCompositePodGroup() fwk.PodGroupInfo {
	return makePodGroupInfoFromCPG(&schedulingv1alpha3.CompositePodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "root",
			Namespace: "default",
		},
	})
}
