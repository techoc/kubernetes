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

package volumezone

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
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
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

// createPodWithVolume 构造一个在默认命名空间中引用指定 PVC 的 Pod，
// 供各测试用例复用，避免重复拼接 Pod 对象。
func createPodWithVolume(pod, pvc string) *v1.Pod {
	return st.MakePod().Name(pod).Namespace(metav1.NamespaceDefault).PVC(pvc).Obj()
}

// TestSingleZone 验证卷与节点位于单个可用区时的过滤行为：
// 覆盖 beta/GA 两种拓扑标签、zone/region 两个维度以及标签匹配与冲突的情形。
func TestSingleZone(t *testing.T) {
	// 同时准备 beta 与 GA 标签的 PV：GA 标签用 "Vol_Stable_*" 命名，
	// 用于验证插件对两套标签键的兼容处理。
	pvLister := tf.PersistentVolumeLister{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_1", Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_2", Labels: map[string]string{v1.LabelFailureDomainBetaRegion: "us-west1", "uselessLabel": "none"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_3", Labels: map[string]string{v1.LabelFailureDomainBetaRegion: "us-west1"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_Stable_1", Labels: map[string]string{v1.LabelTopologyZone: "us-west1-a"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_Stable_2", Labels: map[string]string{v1.LabelTopologyRegion: "us-west1", "uselessLabel": "none"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_Stable_3", Labels: map[string]string{v1.LabelTopologyZone: "us-west1-a", v1.LabelTopologyRegion: "us-west1-a"}},
		},
	}

	// PVC_4 引用一个不存在的 PV，用于覆盖"卷对象缺失"的容错路径。
	pvcLister := tf.PersistentVolumeClaimLister{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_2", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_3", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_3"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_4", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_not_exist"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_Stable_1", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_Stable_1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_Stable_2", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_Stable_2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_Stable_3", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_Stable_3"},
		},
	}

	// 每行声明 Pod、候选节点以及两个阶段的预期状态：
	// 无卷时 PreFilter 返回 Skip，拓扑冲突时 Filter 返回 UnschedulableAndUnresolvable。
	tests := []struct {
		name                string
		Pod                 *v1.Pod
		Node                *v1.Node
		wantPreFilterStatus *fwk.Status
		wantFilterStatus    *fwk.Status
	}{
		{
			name: "pod without volume",
			Pod:  st.MakePod().Name("pod_1").Namespace(metav1.NamespaceDefault).Obj(),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a"},
				},
			},
			wantPreFilterStatus: fwk.NewStatus(fwk.Skip),
		},
		{
			name: "node without labels",
			Pod:  createPodWithVolume("pod_1", "PVC_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "host1",
				},
			},
		},
		{
			name: "beta zone label matched",
			Pod:  createPodWithVolume("pod_1", "PVC_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a", "uselessLabel": "none"},
				},
			},
		},
		{
			name: "beta region label matched",
			Pod:  createPodWithVolume("pod_1", "PVC_2"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelFailureDomainBetaRegion: "us-west1", "uselessLabel": "none"},
				},
			},
		},
		{
			name: "beta region label doesn't match",
			Pod:  createPodWithVolume("pod_1", "PVC_2"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelFailureDomainBetaRegion: "no_us-west1", "uselessLabel": "none"},
				},
			},
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict),
		},
		{
			name: "beta zone label doesn't match",
			Pod:  createPodWithVolume("pod_1", "PVC_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "no_us-west1-a", "uselessLabel": "none"},
				},
			},
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict),
		},
		{
			name: "zone label matched",
			Pod:  createPodWithVolume("pod_1", "PVC_Stable_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelTopologyZone: "us-west1-a", "uselessLabel": "none"},
				},
			},
		},
		{
			name: "region label matched",
			Pod:  createPodWithVolume("pod_1", "PVC_Stable_2"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelTopologyRegion: "us-west1", "uselessLabel": "none"},
				},
			},
		},
		{
			name: "region label doesn't match",
			Pod:  createPodWithVolume("pod_1", "PVC_Stable_2"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelTopologyRegion: "no_us-west1", "uselessLabel": "none"},
				},
			},
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict),
		},
		{
			name: "zone label doesn't match",
			Pod:  createPodWithVolume("pod_1", "PVC_Stable_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelTopologyZone: "no_us-west1-a", "uselessLabel": "none"},
				},
			},
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict),
		},
		{
			name: "pv with zone and region, node with only zone",
			Pod:  createPodWithVolume("pod_1", "PVC_Stable_3"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "host1",
					Labels: map[string]string{
						v1.LabelTopologyZone: "us-west1-a",
					},
				},
			},
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict),
		},
		{
			name: "pv with zone,node with beta zone",
			Pod:  createPodWithVolume("pod_1", "PVC_Stable_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "host1",
					Labels: map[string]string{
						v1.LabelFailureDomainBetaZone: "us-west1-a",
					},
				},
			},
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict),
		},
		{
			name: "pv with beta label,node with ga label, matched",
			Pod:  createPodWithVolume("pod_1", "PVC_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "host1",
					Labels: map[string]string{
						v1.LabelTopologyZone: "us-west1-a",
					},
				},
			},
		},
		{
			name: "pv with beta label,node with ga label, don't match",
			Pod:  createPodWithVolume("pod_1", "PVC_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "host1",
					Labels: map[string]string{
						v1.LabelTopologyZone: "us-west1-b",
					},
				},
			},
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			// Filter 依赖 ctx 传递日志与取消信号，用 defer 保证退出时释放。
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// Filter 读取的节点信息由 CycleState 之外的 NodeInfo 提供，
			// 这里按用例构造最小化的 NodeInfo。
			state := framework.NewCycleState()
			node := &framework.NodeInfo{}
			node.SetNode(test.Node)
			p := &VolumeZone{
				pvLister:  pvLister,
				pvcLister: pvcLister,
				// 当前用例只用已绑定 PVC，无需 StorageClass lister。
				scLister: nil,
			}
			// 先跑 PreFilter 再跑 Filter，模拟真实调度周期内两个阶段共享 state 的调用顺序。
			_, preFilterStatus := p.PreFilter(ctx, state, test.Pod, nil)
			if diff := cmp.Diff(preFilterStatus, test.wantPreFilterStatus); diff != "" {
				t.Errorf("PreFilter: status does not match (-want,+got):\n%s", diff)
			}
			filterStatus := p.Filter(ctx, state, test.Pod, node)
			if diff := cmp.Diff(filterStatus, test.wantFilterStatus); diff != "" {
				t.Errorf("Filter: status does not match (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestMultiZone 验证卷标签含多个可用区（以 "__" 分隔）时的匹配语义：
// 节点区域落在任一候选区内即通过，否则冲突。
func TestMultiZone(t *testing.T) {
	pvLister := tf.PersistentVolumeLister{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_1", Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_2", Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-b", "uselessLabel": "none"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_3", Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-c__us-west1-a"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_Stable_1", Labels: map[string]string{v1.LabelTopologyZone: "us-west1-a"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_Stable_2", Labels: map[string]string{v1.LabelTopologyZone: "us-west1-c__us-west1-a"}},
		},
	}

	// PVC_3 指向带多区域标签的卷；"__" 是插件约定的多值分隔符。
	pvcLister := tf.PersistentVolumeClaimLister{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_2", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_3", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_3"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_4", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_not_exist"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_Stable_1", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_Stable_1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_Stable_2", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_Stable_2"},
		},
	}

	// 各用例覆盖：节点无标签、节点区域命中多值标签之一、以及区域不在候选集合内。
	tests := []struct {
		name                string
		Pod                 *v1.Pod
		Node                *v1.Node
		wantPreFilterStatus *fwk.Status
		wantFilterStatus    *fwk.Status
	}{
		{
			name: "node without labels",
			Pod:  createPodWithVolume("pod_1", "PVC_3"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "host1",
				},
			},
		},
		{
			name: "beta zone label matched",
			Pod:  createPodWithVolume("pod_1", "PVC_3"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a", "uselessLabel": "none"},
				},
			},
		},
		{
			name: "beta zone label doesn't match",
			Pod:  createPodWithVolume("pod_1", "PVC_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-b", "uselessLabel": "none"},
				},
			},
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict),
		},
		{
			name: "zone label matched",
			Pod:  createPodWithVolume("pod_1", "PVC_Stable_2"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelTopologyZone: "us-west1-a", "uselessLabel": "none"},
				},
			},
		},
		{
			name: "zone label doesn't match",
			Pod:  createPodWithVolume("pod_1", "PVC_Stable_1"),
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "host1",
					Labels: map[string]string{v1.LabelTopologyZone: "us-west1-b", "uselessLabel": "none"},
				},
			},
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			state := framework.NewCycleState()
			node := &framework.NodeInfo{}
			node.SetNode(test.Node)
			p := &VolumeZone{
				pvLister:  pvLister,
				pvcLister: pvcLister,
				scLister:  nil,
			}
			// 两个阶段共享同一 state，确保 PreFilter 写入的拓扑信息能被 Filter 复用。
			_, preFilterStatus := p.PreFilter(ctx, state, test.Pod, nil)
			if diff := cmp.Diff(preFilterStatus, test.wantPreFilterStatus); diff != "" {
				t.Errorf("PreFilter: status does not match (-want,+got):\n%s", diff)
			}
			filterStatus := p.Filter(ctx, state, test.Pod, node)
			if diff := cmp.Diff(filterStatus, test.wantFilterStatus); diff != "" {
				t.Errorf("Filter: status does not match (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestWithBinding 验证未绑定 PVC 根据 StorageClass 绑定模式的处理：
// 空 SC / 不存在的 SC / Immediate 模式均报错，WaitForFirstConsumer 模式则跳过检查。
func TestWithBinding(t *testing.T) {
	var (
		// 只有 WaitForFirstConsumer 模式会让插件跳过卷拓扑检查，
		// 因为该模式下卷在 Pod 调度后才创建，拓扑尚不确定。
		modeWait = storagev1.VolumeBindingWaitForFirstConsumer

		class0         = "Class_0"
		classWait      = "Class_Wait"
		classImmediate = "Class_Immediate"
	)

	// classImmediate 故意不设置 VolumeBindingMode，用于验证未设置模式时的报错路径。
	scLister := tf.StorageClassLister{
		{
			ObjectMeta: metav1.ObjectMeta{Name: classImmediate},
		},
		{
			ObjectMeta:        metav1.ObjectMeta{Name: classWait},
			VolumeBindingMode: &modeWait,
		},
	}

	pvLister := tf.PersistentVolumeLister{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "Vol_1", Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a"}},
		},
	}

	pvcLister := tf.PersistentVolumeClaimLister{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_NoSC", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{StorageClassName: &class0},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_EmptySC", Namespace: "default"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_WaitSC", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{StorageClassName: &classWait},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "PVC_ImmediateSC", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{StorageClassName: &classImmediate},
		},
	}

	// 所有未绑定用例复用同一个节点，拓扑已匹配，便于隔离 StorageClass 相关行为。
	testNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "host1",
			Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a", "uselessLabel": "none"},
		},
	}

	// 未绑定卷的错误在 PreFilter 与 Filter 两阶段都会复现，故多数用例同时断言两个状态；
	// WaitForFirstConsumer 仅在 PreFilter 返回 Skip，Filter 无状态可直接放行。
	tests := []struct {
		name                string
		Pod                 *v1.Pod
		Node                *v1.Node
		wantPreFilterStatus *fwk.Status
		wantFilterStatus    *fwk.Status
	}{
		{
			name: "label zone failure domain matched",
			Pod:  createPodWithVolume("pod_1", "PVC_1"),
			Node: testNode,
		},
		{
			name: "unbound volume empty storage class",
			Pod:  createPodWithVolume("pod_1", "PVC_EmptySC"),
			Node: testNode,
			wantPreFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
				"PersistentVolumeClaim had no pv name and storageClass name"),
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
				"PersistentVolumeClaim had no pv name and storageClass name"),
		},
		{
			name: "unbound volume no storage class",
			Pod:  createPodWithVolume("pod_1", "PVC_NoSC"),
			Node: testNode,
			wantPreFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
				`storageclasses.storage.k8s.io "Class_0" not found`),
			wantFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
				`storageclasses.storage.k8s.io "Class_0" not found`),
		},
		{
			name:                "unbound volume immediate binding mode",
			Pod:                 createPodWithVolume("pod_1", "PVC_ImmediateSC"),
			Node:                testNode,
			wantPreFilterStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "VolumeBindingMode not set for StorageClass \"Class_Immediate\""),
			wantFilterStatus:    fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "VolumeBindingMode not set for StorageClass \"Class_Immediate\""),
		},
		{
			name:                "unbound volume wait binding mode",
			Pod:                 createPodWithVolume("pod_1", "PVC_WaitSC"),
			Node:                testNode,
			wantPreFilterStatus: fwk.NewStatus(fwk.Skip),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			state := framework.NewCycleState()
			node := &framework.NodeInfo{}
			node.SetNode(test.Node)
			p := &VolumeZone{
				pvLister:  pvLister,
				pvcLister: pvcLister,
				scLister:  scLister,
			}
			// 两个阶段的预期状态按用例声明：错误类用例会在 Filter 中复现 PreFilter 的状态。
			_, preFilterStatus := p.PreFilter(ctx, state, test.Pod, nil)
			if diff := cmp.Diff(preFilterStatus, test.wantPreFilterStatus); diff != "" {
				t.Errorf("PreFilter: status does not match (-want,+got):\n%s", diff)
			}
			filterStatus := p.Filter(ctx, state, test.Pod, node)
			if diff := cmp.Diff(filterStatus, test.wantFilterStatus); diff != "" {
				t.Errorf("Filter: status does not match (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestIsSchedulableAfterPersistentVolumeClaimAdded 验证 PVC 增删事件的重入队提示：
// 只有当变更对象与 Pod 引用的是同名同命名空间 PVC 时才重新入队，否则跳过。
func TestIsSchedulableAfterPersistentVolumeClaimAdded(t *testing.T) {
	// expectedHint 为 Queue 表示事件可能让失败 Pod 变为可调度；QueueSkip 表示无关事件。
	testcases := map[string]struct {
		pod            *v1.Pod
		oldObj, newObj interface{}
		expectedHint   fwk.QueueingHint
		expectedErr    bool
	}{
		"error-wrong-new-object": {
			pod:          createPodWithVolume("pod_1", "PVC_1"),
			newObj:       "not-a-pvc",
			expectedHint: fwk.Queue,
			expectedErr:  true,
		},
		"pvc-was-added-but-pod-refers-no-pvc": {
			pod: st.MakePod().Name("pod_1").Namespace("default").Obj(),
			newObj: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
				Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_1"},
			},
			expectedHint: fwk.QueueSkip,
		},
		"pvc-was-added-and-pod-was-bound-to-different-pvc": {
			pod: createPodWithVolume("pod_1", "PVC_2"),
			newObj: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
				Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_1"},
			},
			expectedHint: fwk.QueueSkip,
		},
		"pvc-was-added-and-pod-was-bound-to-pvc-but-different-ns": {
			pod: createPodWithVolume("pod_1", "PVC_1"),
			newObj: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "ns1"},
				Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_1"},
			},
			expectedHint: fwk.QueueSkip,
		},
		"pvc-was-added-and-pod-was-bound-to-the-pvc": {
			pod: createPodWithVolume("pod_1", "PVC_1"),
			newObj: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
				Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_1"},
			},
			expectedHint: fwk.Queue,
		},
		"pvc-was-updated-and-pod-was-bound-to-the-pvc": {
			pod: createPodWithVolume("pod_1", "PVC_1"),
			oldObj: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
				Spec:       v1.PersistentVolumeClaimSpec{VolumeName: ""},
			},
			newObj: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
				Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_1"},
			},
			expectedHint: fwk.Queue,
		},
		"pvc-was-updated-but-pod-refers-no-pvc": {
			pod: st.MakePod().Name("pod_1").Namespace(metav1.NamespaceDefault).Obj(),
			oldObj: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
				Spec:       v1.PersistentVolumeClaimSpec{VolumeName: ""},
			},
			newObj: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "PVC_1", Namespace: "default"},
				Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "Vol_1"},
			},
			expectedHint: fwk.QueueSkip,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			// 事件处理函数只比较对象身份和引用的 PVC，不依赖 lister，故用空插件实例。
			p := &VolumeZone{}

			got, err := p.isSchedulableAfterPersistentVolumeClaimChange(logger, tc.pod, tc.oldObj, tc.newObj)
			if err != nil && !tc.expectedErr {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tc.expectedHint {
				t.Errorf("isSchedulableAfterPersistentVolumeClaimChange() = %v, want %v", got, tc.expectedHint)
			}
		})
	}
}

// TestIsSchedulableAfterStorageClassAdded 验证 StorageClass 新增事件的重入队提示：
// 只有新增的 SC 采用 WaitForFirstConsumer 绑定模式时，才可能让失败 Pod 变为可调度。
func TestIsSchedulableAfterStorageClassAdded(t *testing.T) {
	var modeWait = storagev1.VolumeBindingWaitForFirstConsumer

	testcases := map[string]struct {
		pod            *v1.Pod
		oldObj, newObj interface{}
		expectedHint   fwk.QueueingHint
		expectedErr    bool
	}{
		"error-wrong-new-object": {
			pod:          createPodWithVolume("pod_1", "PVC_1"),
			newObj:       "not-a-storageclass",
			expectedHint: fwk.Queue,
			expectedErr:  true,
		},
		"sc-doesn't-have-volume-binding-mode": {
			pod: createPodWithVolume("pod_1", "PVC_1"),
			newObj: &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{Name: "SC_1"},
			},
			expectedHint: fwk.QueueSkip,
		},
		"new-sc-is-wait-for-first-consumer-mode": {
			pod: createPodWithVolume("pod_1", "PVC_1"),
			newObj: &storagev1.StorageClass{
				ObjectMeta:        metav1.ObjectMeta{Name: "SC_1"},
				VolumeBindingMode: &modeWait,
			},
			expectedHint: fwk.Queue,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			// 提示计算只看传入的事件对象，无需初始化 lister。
			p := &VolumeZone{}

			got, err := p.isSchedulableAfterStorageClassAdded(logger, tc.pod, tc.oldObj, tc.newObj)
			if err != nil && !tc.expectedErr {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tc.expectedHint {
				t.Errorf("isSchedulableAfterStorageClassAdded() = %v, want %v", got, tc.expectedHint)
			}
		})
	}
}

// TestIsSchedulableAfterPersistentVolumeChange 验证 PV 变更事件的重入队提示：
// PV 拓扑标签发生变化（新增或修改）会影响卷区域约束，需重新入队；无关变化则跳过。
func TestIsSchedulableAfterPersistentVolumeChange(t *testing.T) {
	testcases := map[string]struct {
		pod            *v1.Pod
		oldObj, newObj interface{}
		expectedHint   fwk.QueueingHint
		expectedErr    bool
	}{
		"error-wrong-new-object": {
			pod:          createPodWithVolume("pod_1", "PVC_1"),
			newObj:       "not-a-pv",
			expectedHint: fwk.Queue,
			expectedErr:  true,
		},
		"error-wrong-old-object": {
			pod:          createPodWithVolume("pod_1", "PVC_1"),
			oldObj:       "not-a-pv",
			newObj:       st.MakePersistentVolume().Name("Vol_1").Obj(),
			expectedHint: fwk.Queue,
			expectedErr:  true,
		},
		"new-pv-was-added": {
			pod: createPodWithVolume("pod_1", "PVC_1"),
			newObj: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "Vol_1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-b"},
				},
			},
			expectedHint: fwk.Queue,
		},
		"pv-was-updated-and-changed-topology": {
			pod: createPodWithVolume("pod_1", "PVC_1"),
			oldObj: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "Vol_1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a"},
				},
			},
			newObj: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "Vol_1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-b"},
				},
			},
			expectedHint: fwk.Queue,
		},
		"pv-was-updated-and-added-topology-label": {
			pod: createPodWithVolume("pod_1", "PVC_1"),
			oldObj: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "Vol_1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a"},
				},
			},
			newObj: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "Vol_1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a",
						v1.LabelTopologyZone: "zone"},
				},
			},
			expectedHint: fwk.Queue,
		},
		"pv-was-updated-but-no-topology-is-changed": {
			pod: createPodWithVolume("pod_1", "PVC_1"),
			oldObj: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "Vol_1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a",
						v1.LabelTopologyZone: "zone"},
				},
			},
			newObj: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "Vol_1",
					Labels: map[string]string{v1.LabelFailureDomainBetaZone: "us-west1-a",
						v1.LabelTopologyZone: "zone"},
				},
			},
			expectedHint: fwk.QueueSkip,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			// 提示计算只比较新旧 PV 的拓扑标签，无需 lister。
			p := &VolumeZone{}

			got, err := p.isSchedulableAfterPersistentVolumeChange(logger, tc.pod, tc.oldObj, tc.newObj)
			if err != nil && !tc.expectedErr {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tc.expectedHint {
				t.Errorf("isSchedulableAfterPersistentVolumeChange() = %v, want %v", got, tc.expectedHint)
			}
		})
	}
}

// BenchmarkVolumeZone 测量插件在 1000 个 PV/PVC/节点规模下的过滤开销，
// 并对比启用与禁用 PreFilter 缓存两种路径。
func BenchmarkVolumeZone(b *testing.B) {
	// PreFilter 为 true 时先执行预处理，用于量化状态缓存对 Filter 开销的影响。
	tests := []struct {
		Name      string
		Pod       *v1.Pod
		NumPV     int
		NumPVC    int
		NumNodes  int
		PreFilter bool
	}{
		{
			Name:      "with prefilter",
			Pod:       createPodWithVolume("pod_0", "PVC_Stable_0"),
			NumPV:     1000,
			NumPVC:    1000,
			NumNodes:  1000,
			PreFilter: true,
		},
		{
			Name:      "without prefilter",
			Pod:       createPodWithVolume("pod_0", "PVC_Stable_0"),
			NumPV:     1000,
			NumPVC:    1000,
			NumNodes:  1000,
			PreFilter: false,
		},
	}

	for _, tt := range tests {
		b.Run(tt.Name, func(b *testing.B) {
			_, ctx := ktesting.NewTestContext(b)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			nodes := makeNodesWithTopologyZone(tt.NumNodes)
			pl := newPluginWithListers(ctx, b, []*v1.Pod{tt.Pod}, nodes, makePVCsWithPV(tt.NumPVC), makePVsWithZoneLabel(tt.NumPV))
			nodeInfos := make([]fwk.NodeInfo, len(nodes))
			for i := 0; i < len(nodes); i++ {
				nodeInfo := &framework.NodeInfo{}
				nodeInfo.SetNode(nodes[i])
				nodeInfos[i] = nodeInfo
			}
			p := pl.(*VolumeZone)
			state := framework.NewCycleState()

			// 预热和构造不在计时范围内，只测量 PreFilter/Filter 的调用开销。
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// 每轮重新执行 PreFilter，模拟每个调度周期的状态初始化。
				if tt.PreFilter {
					_, _ = p.PreFilter(ctx, state, tt.Pod, nil)
				}
				for _, node := range nodeInfos {
					_ = p.Filter(ctx, state, tt.Pod, node)
				}
			}
		})
	}
}

// newPluginWithListers 构造一个装配了 informer/lister 的 VolumeZone 插件实例，
// 让基准测试使用与真实调度一致的插件初始化路径。
func newPluginWithListers(ctx context.Context, tb testing.TB, pods []*v1.Pod, nodes []*v1.Node, pvcs []*v1.PersistentVolumeClaim, pvs []*v1.PersistentVolume) fwk.Plugin {
	snapshot := cache.NewSnapshot(pods, nodes)

	// SetupPluginWithInformers 需要统一的 runtime.Object 列表，PVC 与 PV 都作为 informer 初始对象。
	objects := make([]runtime.Object, 0, len(pvcs))
	for _, pvc := range pvcs {
		objects = append(objects, pvc)
	}
	for _, pv := range pvs {
		objects = append(objects, pv)
	}
	return plugintesting.SetupPluginWithInformers(ctx, tb, schedruntime.FactoryAdapter(feature.Features{}, New), &config.InterPodAffinityArgs{}, snapshot, objects)
}

// makePVsWithZoneLabel 生成 num 个带 GA zone 标签的 PV，每个 PV 使用不同的区域。
func makePVsWithZoneLabel(num int) []*v1.PersistentVolume {
	pvList := make([]*v1.PersistentVolume, num)
	for i := 0; i < len(pvList); i++ {
		pvName := fmt.Sprintf("Vol_Stable_%d", i)
		zone := fmt.Sprintf("us-west-%d", i)
		pvList[i] = &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName, Labels: map[string]string{v1.LabelTopologyZone: zone}},
		}
	}
	return pvList
}

// makePVCsWithPV 生成 num 个已绑定的 PVC，第 i 个 PVC 指向 makePVsWithZoneLabel 生成的第 i 个 PV。
func makePVCsWithPV(num int) []*v1.PersistentVolumeClaim {
	pvcList := make([]*v1.PersistentVolumeClaim, num)
	for i := 0; i < len(pvcList); i++ {
		pvcName := fmt.Sprintf("PVC_Stable_%d", i)
		pvName := fmt.Sprintf("Vol_Stable_%d", i)
		pvcList[i] = &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: pvName},
		}
	}
	return pvcList
}

// makeNodesWithTopologyZone 生成 num 个节点，全部位于区域 us-west-0，
// 与 Pod 引用的第 0 个 PV 区域一致，保证基准测试中 Filter 走匹配路径。
func makeNodesWithTopologyZone(num int) []*v1.Node {
	nodeList := make([]*v1.Node, num)
	for i := 0; i < len(nodeList); i++ {
		nodeName := fmt.Sprintf("host_%d", i)
		zone := "us-west-0"
		nodeList[i] = &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   nodeName,
				Labels: map[string]string{v1.LabelTopologyZone: zone, "uselessLabel": "none"},
			},
		}
	}
	return nodeList
}

// TestVolumeZone_DeferredResizeSkipped 验证特性开关开启时，处于延迟扩容（Deferred）状态的 Pod
// 在 PreFilter 与 Filter 阶段都被直接跳过，避免阻止其调度。
func TestVolumeZone_DeferredResizeSkipped(t *testing.T) {
	ctx := context.Background()
	// Pod 处于 ResizePending 且原因为 Deferred，是触发跳过逻辑的必要条件。
	pod := st.MakePod().Name("p").UID("p").Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(st.MakeNode().Name("node1").Obj())

	// 只有开启原地垂直扩缩容调度抢占特性，跳过逻辑才生效。
	pl := &VolumeZone{enableInPlacePodVerticalScalingSchedulerPreemption: true}

	// 无需检查卷拓扑，因此可直接传入 nil state。
	if preRes, preStatus := pl.PreFilter(ctx, nil, pod, nil); preStatus.Code() != fwk.Skip || preRes != nil {
		t.Errorf("PreFilter: got (res: %v, status: %v), want (nil, Skip)", preRes, preStatus.Code())
	}

	if filterStatus := pl.Filter(ctx, nil, pod, nodeInfo); filterStatus.Code() != fwk.Success {
		t.Errorf("Filter: got status %v, want Success (nil)", filterStatus.Code())
	}
}
