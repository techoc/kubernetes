/*
Copyright 2025 The Kubernetes Authors.

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

package podgroup

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-helpers/storage/volume"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	stepsframework "k8s.io/kubernetes/test/integration/scheduler/podgroup/stepsframework"
	testutils "k8s.io/kubernetes/test/integration/util"
)

// createBoundRWOPPVC 创建一个 HostPath PV 和一个与之绑定的 PVC，两者都使用
// ReadWriteOncePod 访问模式。
func createBoundRWOPPVC(cs kubernetes.Interface, ns, name string) error {
	storage := v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}
	volType := v1.HostPathDirectoryOrCreate
	pv, err := testutils.CreatePV(cs, st.MakePersistentVolume().
		Name(fmt.Sprintf("pv-%s-%s", ns, name)).
		AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
		Capacity(storage.Requests).
		HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/mnt", Type: &volType}).
		Obj())
	if err != nil {
		return fmt.Errorf("cannot create pv: %w", err)
	}
	_, err = testutils.CreatePVC(cs, st.MakePersistentVolumeClaim().
		Name(name).
		Namespace(ns).
		// PVC 要被视为已绑定，需要设置 annotation 和 volume name。
		Annotation(volume.AnnBindCompleted, "true").
		VolumeName(pv.Name).
		AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
		Resources(storage).
		Obj())
	if err != nil {
		return fmt.Errorf("cannot create pvc: %w", err)
	}
	return nil
}

// TestPodGroupSchedulingWithReadWriteOncePodPVC 是针对快照
// AssumePod/ForgetPod bug 的回归测试。在 PodGroup 调度周期中，
// 组内 Pod 会被逐个假定（assume）到调度器快照中，因此 AssumePod
// 必须把被假定 Pod 的 PVC 记录到快照的 usedPVCRefCounts 中：
// 否则 VolumeRestrictions 插件看不到同一周期中较早假定的 Pod
// 已经使用了某个 ReadWriteOncePod PVC，会静默地允许第二个 Pod 挂载它。
//
// 第一个 Pod 先被创建和入队，因此 PodGroup 调度周期（按入队时间
// 对同优先级 Pod 排序）总是在第二个 Pod 之前假定它，尽管冲突是
// 对称的：无论哪个 Pod 被第二个假定，都必须观察到先被假定的那个
// Pod 的 PVC。在冲突场景中，两个 Pod 引用同一个 ReadWriteOncePod
// PVC，因此该 PodGroup 必须保持不可调度；没有修复时两个 Pod 都会
// 被调度并共享该 PVC，违反其访问模式。无冲突场景作为正向对照：
// 使用两个不同的 PVC 时，同一个 PodGroup 可以完成调度，证明测试
// 环境本身是可调度的，而冲突场景确实因预期原因失败。
func TestPodGroupSchedulingWithReadWriteOncePodPVC(t *testing.T) {
	nodeCapacity := map[v1.ResourceName]string{v1.ResourceCPU: "2"}
	podRequest := map[v1.ResourceName]string{v1.ResourceCPU: "1"}

	workload := st.MakeWorkload().Name("workload").
		PodGroupTemplate(st.MakePodGroupTemplate().Name("t").MinCount(2).Obj()).Obj()
	podGroup := st.MakePodGroup().Name("pg").WorkloadRef("t", "workload").Priority(200).MinCount(2).Obj()

	tests := []struct {
		name string
		// secondPodPVC 是调度周期中第二个被假定的
		// Pod 所引用的 PVC；第一个被假定的 Pod
		// 始终引用 pvc-1。
		secondPodPVC string
		steps        []stepsframework.Step
	}{
		{
			name:         "two pods sharing a ReadWriteOncePod PVC keep the group unschedulable",
			secondPodPVC: "pvc-1",
			steps: []stepsframework.Step{
				{
					Name:                     "Verify the group becomes unschedulable instead of sharing the PVC",
					WaitForPodsUnschedulable: []string{"pvc-pod-1", "pvc-pod-2"},
				},
				{
					Name: "Verify PodGroup condition is set to Unschedulable",
					WaitForPodGroupCondition: &stepsframework.PodGroupConditionCheck{
						PodGroupName:    "pg",
						ConditionStatus: metav1.ConditionFalse,
						Reason:          schedulingapi.PodGroupReasonUnschedulable,
					},
				},
			},
		},
		{
			name:         "two pods with distinct ReadWriteOncePod PVCs are scheduled",
			secondPodPVC: "pvc-2",
			steps: []stepsframework.Step{
				{
					Name:                 "Verify both pods are scheduled",
					WaitForPodsScheduled: []string{"pvc-pod-1", "pvc-pod-2"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			testCtx := testutils.InitTestSchedulerWithNS(t, "podgroup-pvc",
				// 禁用 backoff
				scheduler.WithPodMaxBackoffSeconds(0),
				scheduler.WithPodInitialBackoffSeconds(0))
			ns := testCtx.NS.Name

			for _, pvcName := range sets.List(sets.New("pvc-1", tt.secondPodPVC)) {
				if err := createBoundRWOPPVC(testCtx.ClientSet, ns, pvcName); err != nil {
					t.Fatal(err)
				}
			}

			// firstPod 会被优先假定（比第二个 Pod 先创建和
			// 入队），并占用 pvc-1。
			firstPod := st.MakePod().Name("pvc-pod-1").
				Req(podRequest).Container("image").
				PVC("pvc-1").
				PodGroupName("pg").Priority(200).Obj()
			// secondPod 会被第二个假定；它的 PVC
			// 是否与第一个 Pod 的冲突取决于
			// 具体测试用例。
			secondPod := st.MakePod().Name("pvc-pod-2").
				Req(podRequest).Container("image").
				PVC(tt.secondPodPVC).
				PodGroupName("pg").Priority(200).Obj()

			commonSteps := []stepsframework.Step{
				{
					Name:        "Create Nodes",
					CreateNodes: []*v1.Node{st.MakeNode().Name("node-1").Capacity(nodeCapacity).Obj()},
				},
				{
					Name:            "Create workloads",
					CreateWorkloads: []*schedulingapi.Workload{workload},
				},
				{
					Name:           "Create the PodGroup object",
					CreatePodGroup: podGroup,
				},
				{
					Name:              "Create both pods belonging to the group",
					CreatePodsInOrder: []*v1.Pod{firstPod, secondPod},
				},
			}

			if err := stepsframework.RunSteps(testCtx, t, ns, append(commonSteps, tt.steps...)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
