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

package pvcprotection

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/features"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/dump"
)

// reaction 描述向 fake client 注入的一个 reactor：
// verb 与 resource 用于匹配动作，reactorfn 决定返回的对象或错误。
type reaction struct {
	verb      string
	resource  string
	reactorfn clienttesting.ReactionFunc
}

// 测试使用的固定命名空间、PVC/Pod/节点名称与 UID，便于断言。
const (
	defaultNS       = "default"
	namespace2      = "namespace-2"
	defaultPVCName  = "pvc1"
	defaultPodName  = "pod1"
	defaultNodeName = "node1"
	defaultUID      = "uid1"
)

// pod 构造一个已调度到默认节点、处于 Pending 状态的测试 Pod。
func pod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultPodName,
			Namespace: defaultNS,
			UID:       defaultUID,
		},
		Spec: v1.PodSpec{
			NodeName: defaultNodeName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
}

// podWithConfig 与 pod 类似，但可指定 Pod 的名称和命名空间。
func podWithConfig(name string, namespace string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       defaultUID,
		},
		Spec: v1.PodSpec{
			NodeName: defaultNodeName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
}

// unscheduled 清空 Pod 的 nodeName，用于模拟尚未调度的 Pod。
func unscheduled(pod *v1.Pod) *v1.Pod {
	pod.Spec.NodeName = ""
	return pod
}

// withPVC 给 Pod 添加一个引用指定 PVC 的卷，模拟 Pod 正在使用该 PVC。
func withPVC(pvcName string, pod *v1.Pod) *v1.Pod {
	volume := v1.Volume{
		Name: pvcName,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		},
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
	return pod
}

// withEmptyDir 给 Pod 添加一个空目录卷，用于构造不引用任何 PVC 的 Pod。
func withEmptyDir(pod *v1.Pod) *v1.Pod {
	volume := v1.Volume{
		Name: "emptyDir",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
	return pod
}

// withStatus 设置 Pod 的运行阶段，用于区分运行中 Pod 与已终止 Pod。
func withStatus(phase v1.PodPhase, pod *v1.Pod) *v1.Pod {
	pod.Status.Phase = phase
	return pod
}

// withUID 覆盖 Pod 的 UID，用于模拟同名但不同实例的 Pod 删除后重建。
func withUID(uid types.UID, pod *v1.Pod) *v1.Pod {
	pod.ObjectMeta.UID = uid
	return pod
}

// pvc 构造默认的 PVC 对象。
func pvc() *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultPVCName,
			Namespace: defaultNS,
		},
	}
}

// pvcWithConfig 构造可指定名称和命名空间的 PVC 对象。
func pvcWithConfig(name string, namespace string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

// withProtectionFinalizer 给 PVC 加上保护 finalizer，表示控制器已接管其保护。
func withProtectionFinalizer(pvc *v1.PersistentVolumeClaim) *v1.PersistentVolumeClaim {
	pvc.Finalizers = append(pvc.Finalizers, volumeutil.PVCProtectionFinalizer)
	return pvc
}

// bound 将 PVC 状态设为 Bound，Unused 条件只对已绑定 PVC 生效。
func bound(pvc *v1.PersistentVolumeClaim) *v1.PersistentVolumeClaim {
	pvc.Status.Phase = v1.ClaimBound
	return pvc
}

// withUnusedCondition 给 PVC 附加 Unused 条件；状态为 False 时附上原因是仍有 Pod 在使用该 PVC。
func withUnusedCondition(status v1.ConditionStatus, ts metav1.Time, pvc *v1.PersistentVolumeClaim) *v1.PersistentVolumeClaim {
	pvc.Status.Conditions = append(pvc.Status.Conditions, v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimUnused,
		Status:             status,
		LastTransitionTime: ts,
		Reason:             "NoPodsUsingPVC",
		Message:            "No pods are currently referencing this PVC",
	})
	if status == v1.ConditionFalse {
		pvc.Status.Conditions[len(pvc.Status.Conditions)-1].Reason = "PodUsingPVC"
		pvc.Status.Conditions[len(pvc.Status.Conditions)-1].Message = "A pod is currently referencing this PVC"
	}
	return pvc
}

// deleted 通过设置 deletionTimestamp 模拟进入删除流程的 PVC。
func deleted(pvc *v1.PersistentVolumeClaim) *v1.PersistentVolumeClaim {
	pvc.DeletionTimestamp = &metav1.Time{}
	return pvc
}

// generateUpdateErrorFunc 返回一个 reactor：前 failures 次 update 返回 Forbidden 错误，之后放行，
// 用于验证控制器的重试行为。
func generateUpdateErrorFunc(t *testing.T, failures int) clienttesting.ReactionFunc {
	i := 0
	return func(action clienttesting.Action) (bool, runtime.Object, error) {
		i++
		if i <= failures {
			// 前几次 update 注入错误。
			update, ok := action.(clienttesting.UpdateAction)

			if !ok {
				t.Fatalf("Reactor got non-update action: %+v", action)
			}
			acc, _ := meta.Accessor(update.GetObject())
			return true, nil, apierrors.NewForbidden(update.GetResource().GroupResource(), acc.GetName(), errors.New("Mock error"))
		}
		// 之后的 update 正常放行。
		return false, nil, nil
	}
}

// generatePodListErrorFunc 与前者的思路相同，但针对 Pod list 操作注入错误。
func generatePodListErrorFunc(t *testing.T, failures int) clienttesting.ReactionFunc {
	i := 0
	return func(action clienttesting.Action) (bool, runtime.Object, error) {
		i++
		if i <= failures {
			// 前几次 Pod list 注入错误。
			list, ok := action.(clienttesting.ListAction)
			if !ok {
				t.Fatalf("Reactor got non-list action: %+v", action)
			}
			return true, nil, apierrors.NewForbidden(list.GetResource().GroupResource(), "mock pod", errors.New("Mock error"))
		}
		// 之后的 Pod list 正常放行。
		return false, nil, nil
	}
}

// TestPVCProtectionController 以表格驱动方式验证 PVC 保护控制器对 PVC 与 Pod 事件的响应：
// 包括保护 finalizer 的添加与移除、API 失败后的重试、informer 数据滞后等场景，
// 以及 PersistentVolumeClaimUnusedSinceTime 特性开启时 Unused 状态的维护。
func TestPVCProtectionController(t *testing.T) {
	pvcGVR := schema.GroupVersionResource{
		Group:    v1.GroupName,
		Version:  "v1",
		Resource: "persistentvolumeclaims",
	}
	podGVR := schema.GroupVersionResource{
		Group:    v1.GroupName,
		Version:  "v1",
		Resource: "pods",
	}
	podGVK := schema.GroupVersionKind{
		Group:   v1.GroupName,
		Version: "v1",
		Kind:    "Pod",
	}

	tests := []struct {
		name                     string
		enablePVCUnusedSinceTime bool
		// 测试开始前插入 fake client 的对象。
		initialObjects []runtime.Object
		// 是否不把 initialObjects 同步到 informer。
		// 设为 true 可模拟 informer 尚未收到某些 API 对象通知的情况。
		informersAreLate bool
		// 可选的错误注入 reactor。
		reactors []reaction
		// 要模拟的 PVC 事件，这些 PVC 会自动加入 initialObjects。
		updatedPVCs []*v1.PersistentVolumeClaim
		// 要模拟的 Pod 事件，该 Pod 会自动加入 initialObjects。
		updatedPod *v1.Pod
		// 要模拟的 Pod 事件，该 Pod 不会加入 initialObjects。
		deletedPod *v1.Pod
		// 测试期间期望 fake client 收到的动作列表，用于逐条比对。
		expectedActions []clienttesting.Action
	}{
		//
		// PVC 事件
		//
		{
			name:        "PVC without finalizer -> finalizer is added",
			updatedPVCs: []*v1.PersistentVolumeClaim{pvc(), pvcWithConfig("pvc2", "namespace-2")},
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, withProtectionFinalizer(pvc())),
				clienttesting.NewUpdateAction(pvcGVR, "namespace-2", withProtectionFinalizer(pvcWithConfig("pvc2", "namespace-2"))),
			},
		},
		{
			name:            "PVC with finalizer -> no action",
			updatedPVCs:     []*v1.PersistentVolumeClaim{withProtectionFinalizer(pvc()), withProtectionFinalizer(pvcWithConfig("pvc2", "namespace-2"))},
			expectedActions: []clienttesting.Action{},
		},
		{
			name:        "saving PVC finalizer fails -> controller retries",
			updatedPVCs: []*v1.PersistentVolumeClaim{pvc()},
			reactors: []reaction{
				{
					verb:      "update",
					resource:  "persistentvolumeclaims",
					reactorfn: generateUpdateErrorFunc(t, 2 /* 连续两次更新失败 */),
				},
			},
			expectedActions: []clienttesting.Action{
				// 第一次更新失败。
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, withProtectionFinalizer(pvc())),
				// 第二次更新仍失败。
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, withProtectionFinalizer(pvc())),
				// 第三次更新成功。
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, withProtectionFinalizer(pvc())),
			},
		},
		{
			name: "deleted PVC with finalizers across different namespaces -> finalizer is removed",
			updatedPVCs: []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc())),
				deleted(withProtectionFinalizer(pvcWithConfig("pvc2", "namespace-2")))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
				clienttesting.NewListAction(podGVR, podGVK, "namespace-2", metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, "namespace-2", deleted(pvcWithConfig("pvc2", "namespace-2"))),
			},
		},
		{
			name: "multiple PVCs with finalizer for the same namespace; no alive pods -> finalizer is removed",
			updatedPVCs: []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc())),
				deleted(withProtectionFinalizer(pvcWithConfig("pvc2", defaultNS)))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvcWithConfig("pvc2", defaultNS))),
			},
		},
		{
			name:        "finalizer removal fails -> controller retries",
			updatedPVCs: []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc()))},
			reactors: []reaction{
				{
					verb:      "update",
					resource:  "persistentvolumeclaims",
					reactorfn: generateUpdateErrorFunc(t, 2 /* 连续两次更新失败 */),
				},
			},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				// 本次更新失败。
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				// 再次更新仍然失败。
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				// 本次更新成功。
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
			},
		},
		{
			name:        "delete multiple PVCs of the same namespace; pod list fails for one PVC -> add failing PVC back to queue and continue to the next PVC",
			updatedPVCs: []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc())), deleted(withProtectionFinalizer(pvcWithConfig("pvc2", defaultNS)))},
			reactors: []reaction{
				{
					verb:      "list",
					resource:  "pods",
					reactorfn: generatePodListErrorFunc(t, 1 /* 一次列表请求失败 */),
				},
			},
			expectedActions: []clienttesting.Action{
				// 本次列表请求失败。
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				// 继续处理队列中的下一个 PVC。
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvcWithConfig("pvc2", defaultNS))),

				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
			},
		},
		{
			name: "deleted PVC with finalizer + pod with the PVC exists -> finalizer is not removed",
			initialObjects: []runtime.Object{
				withPVC(defaultPVCName, pod()),
			},
			updatedPVCs:     []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{},
		},
		{
			name: "deleted PVC with finalizer + pod with unrelated PVC and EmptyDir exists -> finalizer is removed",
			initialObjects: []runtime.Object{
				withEmptyDir(withPVC("unrelatedPVC", pod())),
			},
			updatedPVCs: []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
			},
		},
		{
			name: "deleted multiple PVCs with finalizer (same namespace) + pod with unrelated PVC and EmptyDir exists",
			initialObjects: []runtime.Object{
				withEmptyDir(withPVC("unrelatedPVC", pod())),
			},
			updatedPVCs: []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc())),
				deleted(withProtectionFinalizer(pvcWithConfig("pvc2", defaultNS))),
				deleted(withProtectionFinalizer(pvcWithConfig("pvc3", defaultNS))),
			},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvcWithConfig("pvc2", defaultNS))),
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvcWithConfig("pvc3", defaultNS))),
			},
		},
		{
			name: "deleted PVC with finalizer + pod with the PVC finished but is not deleted -> finalizer is not removed",
			initialObjects: []runtime.Object{
				withStatus(v1.PodFailed, withPVC(defaultPVCName, pod())),
			},
			updatedPVCs:     []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{},
		},
		{
			name: "deleted PVC with finalizer + pod with the PVC exists but is not in the Informer's cache yet -> finalizer is not removed",
			initialObjects: []runtime.Object{
				withPVC(defaultPVCName, pod()),
			},
			informersAreLate: true,
			updatedPVCs:      []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
			},
		},

		{
			name: "mix of deleted PVCs with some used by a pod and some unused -> finalizer is removed only for unused PVCs",
			initialObjects: []runtime.Object{
				withPVC(defaultPVCName, pod()),
				withPVC("pvc3", podWithConfig("pod2", "namespace-3")),
			},
			informersAreLate: true,
			updatedPVCs:      []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc())), deleted(withProtectionFinalizer(pvcWithConfig("pvc2", defaultNS))), deleted(withProtectionFinalizer(pvcWithConfig("pvc3", "namespace-3")))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvcWithConfig("pvc2", defaultNS))),
				clienttesting.NewListAction(podGVR, podGVK, "namespace-3", metav1.ListOptions{}),
			},
		},
		//
		// PVC UnusedSince 条件：特性门控开启时
		//
		{
			name:                     "feature enabled: unused bound PVC with no pods -> Unused=True condition set",
			enablePVCUnusedSinceTime: true,
			updatedPVCs:              []*v1.PersistentVolumeClaim{bound(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionTrue, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: pending PVC -> no Unused condition",
			enablePVCUnusedSinceTime: true,
			updatedPVCs:              []*v1.PersistentVolumeClaim{withProtectionFinalizer(pvc())},
			// PVC 尚未绑定，不会评估 Unused 条件。
			// 因此既不查询 Pod，也不更新状态。
			expectedActions: []clienttesting.Action{},
		},
		{
			name:                     "feature enabled: bound PVC already has Unused=True and is still unused -> no update",
			enablePVCUnusedSinceTime: true,
			updatedPVCs:              []*v1.PersistentVolumeClaim{bound(withUnusedCondition(v1.ConditionTrue, metav1.Unix(100, 0), withProtectionFinalizer(pvc())))},
			// PVC 已带有该条件，pvcAddedUpdated 不会将其入队。
			// 也没有 Pod 事件触发重新评估，因此不应产生任何动作。
			expectedActions: []clienttesting.Action{},
		},
		{
			name:                     "feature enabled: running pod references bound PVC with Unused=True -> condition set to False",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				bound(withUnusedCondition(v1.ConditionTrue, metav1.Unix(100, 0), withProtectionFinalizer(pvc()))),
			},
			updatedPod: withStatus(v1.PodRunning, withPVC(defaultPVCName, pod())),
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionFalse, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: bound PVC in use by running pod without condition -> Unused=False set proactively",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				withStatus(v1.PodRunning, withPVC(defaultPVCName, pod())),
			},
			updatedPVCs: []*v1.PersistentVolumeClaim{bound(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionFalse, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: deleting PVC -> condition not updated, finalizer removed",
			enablePVCUnusedSinceTime: true,
			updatedPVCs:              []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
			},
		},
		{
			name:            "feature disabled: unused PVC with no pods -> no condition action",
			updatedPVCs:     []*v1.PersistentVolumeClaim{withProtectionFinalizer(pvc())},
			expectedActions: []clienttesting.Action{},
		},
		{
			name:                     "feature enabled: unscheduled pending pod references bound PVC -> Unused=False set proactively",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				unscheduled(withPVC(defaultPVCName, pod())),
			},
			updatedPVCs: []*v1.PersistentVolumeClaim{bound(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionFalse, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: terminated (Succeeded) pod does not count as using bound PVC -> Unused=True set",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				withStatus(v1.PodSucceeded, withPVC(defaultPVCName, pod())),
			},
			updatedPVCs: []*v1.PersistentVolumeClaim{bound(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionTrue, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: terminated (Failed) pod does not count as using bound PVC -> Unused=True set",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				withStatus(v1.PodFailed, withPVC(defaultPVCName, pod())),
			},
			updatedPVCs: []*v1.PersistentVolumeClaim{bound(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionTrue, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: status update fails -> controller retries",
			enablePVCUnusedSinceTime: true,
			updatedPVCs:              []*v1.PersistentVolumeClaim{bound(withProtectionFinalizer(pvc()))},
			reactors: []reaction{
				{
					verb:      "update",
					resource:  "persistentvolumeclaims",
					reactorfn: generateUpdateErrorFunc(t, 2),
				},
			},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionTrue, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionTrue, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionTrue, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: pod not in informer cache but exists in API -> Unused=False set proactively",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				withPVC(defaultPVCName, pod()),
			},
			informersAreLate: true,
			updatedPVCs:      []*v1.PersistentVolumeClaim{bound(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionFalse, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: one of two pods deleted, remaining pod keeps bound PVC in use -> Unused=False set proactively",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				withPVC(defaultPVCName, pod()),
				bound(withProtectionFinalizer(pvc())),
			},
			deletedPod: withPVC(defaultPVCName, withUID("uid2", podWithConfig("pod2", defaultNS))),
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionFalse, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: deleting PVC that already has Unused=True condition -> condition preserved when removing finalizer",
			enablePVCUnusedSinceTime: true,
			updatedPVCs: []*v1.PersistentVolumeClaim{
				deleted(withUnusedCondition(v1.ConditionTrue, metav1.Unix(100, 0), withProtectionFinalizer(pvc()))),
			},
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(withUnusedCondition(v1.ConditionTrue, metav1.Unix(100, 0), pvc()))),
			},
		},
		{
			name:                     "feature enabled: pod deleted, bound PVC becomes unused -> Unused=True condition set",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				bound(withProtectionFinalizer(pvc())),
			},
			deletedPod: withPVC(defaultPVCName, pod()),
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionTrue, metav1.Unix(123, 0), bound(withProtectionFinalizer(pvc())))),
			},
		},
		{
			name:                     "feature enabled: used bound PVC without finalizer -> Unused=False set then finalizer added",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				withPVC(defaultPVCName, pod()),
			},
			updatedPVCs: []*v1.PersistentVolumeClaim{bound(pvc())},
			expectedActions: []clienttesting.Action{
				// 第一步：主动设置状态条件（对尚无 finalizer 的 PVC 执行 UpdateStatus）。
				clienttesting.NewUpdateSubresourceAction(pvcGVR, "status", defaultNS, withUnusedCondition(v1.ConditionFalse, metav1.Unix(123, 0), bound(pvc()))),
				// 第二步：添加 finalizer（对不含该条件的原始 PVC 执行 Update）。
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, bound(withProtectionFinalizer(pvc()))),
			},
		},
		{
			name:                     "feature enabled: deleted PVC with finalizer and pod using it -> finalizer kept (existing behavior)",
			enablePVCUnusedSinceTime: true,
			initialObjects: []runtime.Object{
				withPVC(defaultPVCName, pod()),
			},
			updatedPVCs:     []*v1.PersistentVolumeClaim{deleted(withProtectionFinalizer(pvc()))},
			expectedActions: []clienttesting.Action{},
		},
		//
		// Pod 事件
		//
		{
			name: "updated running Pod -> no action",
			initialObjects: []runtime.Object{
				deleted(withProtectionFinalizer(pvc())),
			},
			updatedPod:      withStatus(v1.PodRunning, withPVC(defaultPVCName, pod())),
			expectedActions: []clienttesting.Action{},
		},
		{
			name: "updated finished Pod -> finalizer is not removed",
			initialObjects: []runtime.Object{
				deleted(withProtectionFinalizer(pvc())),
			},
			updatedPod:      withStatus(v1.PodSucceeded, withPVC(defaultPVCName, pod())),
			expectedActions: []clienttesting.Action{},
		},
		{
			name: "updated unscheduled Pod -> finalizer is removed",
			initialObjects: []runtime.Object{
				deleted(withProtectionFinalizer(pvc())),
			},
			updatedPod: unscheduled(withPVC(defaultPVCName, pod())),
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
			},
		},
		{
			name: "deleted running Pod -> finalizer is removed",
			initialObjects: []runtime.Object{
				deleted(withProtectionFinalizer(pvc())),
			},
			deletedPod: withStatus(v1.PodRunning, withPVC(defaultPVCName, pod())),
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
			},
		},
		{
			name: "pod delete and create with same namespaced name seen as an update, old pod used deleted PVC -> finalizer is removed",
			initialObjects: []runtime.Object{
				deleted(withProtectionFinalizer(pvc())),
			},
			deletedPod: withPVC(defaultPVCName, pod()),
			updatedPod: withUID("uid2", pod()),
			expectedActions: []clienttesting.Action{
				clienttesting.NewListAction(podGVR, podGVK, defaultNS, metav1.ListOptions{}),
				clienttesting.NewUpdateAction(pvcGVR, defaultNS, deleted(pvc())),
			},
		},
		{
			name: "pod delete and create with same namespaced name seen as an update, old pod used non-deleted PVC -> finalizer is not removed",
			initialObjects: []runtime.Object{
				withProtectionFinalizer(pvc()),
			},
			deletedPod:      withPVC(defaultPVCName, pod()),
			updatedPod:      withUID("uid2", pod()),
			expectedActions: []clienttesting.Action{},
		},
		{
			name: "pod delete and create with same namespaced name seen as an update, both pods reference deleted PVC -> finalizer is not removed",
			initialObjects: []runtime.Object{
				deleted(withProtectionFinalizer(pvc())),
			},
			deletedPod:      withPVC(defaultPVCName, pod()),
			updatedPod:      withUID("uid2", withPVC(defaultPVCName, pod())),
			expectedActions: []clienttesting.Action{},
		},
		{
			name: "pod update from unscheduled to scheduled, deleted PVC is referenced -> finalizer is not removed",
			initialObjects: []runtime.Object{
				deleted(withProtectionFinalizer(pvc())),
			},
			deletedPod:      unscheduled(withPVC(defaultPVCName, pod())),
			updatedPod:      withPVC(defaultPVCName, pod()),
			expectedActions: []clienttesting.Action{},
		},
	}

	for _, test := range tests {
		// 固定仿真版本，避免不同 Kubernetes 版本的默认特性取值影响断言。
		featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.36"))
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PersistentVolumeClaimUnusedSinceTime, test.enablePVCUnusedSinceTime)

		origNowFunc := unusedSinceNowFunc
		// 固定时间函数，使 UnusedSince 时间戳可预测。
		unusedSinceNowFunc = func() metav1.Time { return metav1.Unix(123, 0) }
		t.Cleanup(func() {
			unusedSinceNowFunc = origNowFunc
		})

		// 为 client 和 informer 准备初始数据。
		var (
			clientObjs    []runtime.Object
			informersObjs []runtime.Object
		)
		if test.updatedPVCs != nil {
			for i := 0; i < len(test.updatedPVCs); i++ {
				clientObjs = append(clientObjs, test.updatedPVCs[i])
				informersObjs = append(informersObjs, test.updatedPVCs[i])
			}
		}
		if test.updatedPod != nil {
			clientObjs = append(clientObjs, test.updatedPod)
			informersObjs = append(informersObjs, test.updatedPod)
		}
		clientObjs = append(clientObjs, test.initialObjects...)
		if !test.informersAreLate {
			informersObjs = append(informersObjs, test.initialObjects...)
		}
		// 用初始数据创建 fake client。
		client := fake.NewSimpleClientset(clientObjs...)

		// 创建 informer。
		informers := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		pvcInformer := informers.Core().V1().PersistentVolumeClaims()
		podInformer := informers.Core().V1().Pods()

		// 创建控制器。
		logger, _ := ktesting.NewTestContext(t)
		ctrl, err := NewPVCProtectionController(logger, pvcInformer, podInformer, client)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// 将初始对象写入 informer 存储，使控制器的 Get() 与 List() 能读取到它们。
		for _, obj := range informersObjs {
			switch obj.(type) {
			case *v1.PersistentVolumeClaim:
				pvcInformer.Informer().GetStore().Add(obj)
			case *v1.Pod:
				podInformer.Informer().GetStore().Add(obj)
			default:
				t.Fatalf("Unknown initalObject type: %+v", obj)
			}
		}

		// 注入测试所需的错误 reactor。
		for _, reactor := range test.reactors {
			client.Fake.PrependReactor(reactor.verb, reactor.resource, reactor.reactorfn)
		}

		// 通过模拟事件启动测试。
		if test.updatedPVCs != nil {
			for i := 0; i < len(test.updatedPVCs); i++ {
				ctrl.pvcAddedUpdated(logger, test.updatedPVCs[i])
			}
		}
		switch {
		case test.deletedPod != nil && test.updatedPod != nil && test.deletedPod.Namespace == test.updatedPod.Namespace && test.deletedPod.Name == test.updatedPod.Name:
			ctrl.podAddedDeletedUpdated(logger, test.deletedPod, test.updatedPod, false)
		case test.updatedPod != nil:
			ctrl.podAddedDeletedUpdated(logger, nil, test.updatedPod, false)
		case test.deletedPod != nil:
			ctrl.podAddedDeletedUpdated(logger, nil, test.deletedPod, true)
		}

		// 持续处理控制器队列，直到得到期望的动作序列。
		timeout := time.Now().Add(10 * time.Second)
		lastReportedActionCount := 0
		for {
			if time.Now().After(timeout) {
				t.Errorf("Test %q: timed out", test.name)
				break
			}
			if ctrl.queue.Len() > 0 {
				logger.V(5).Info("Non-empty queue, processing one", "test", test.name, "queueLength", ctrl.queue.Len())
				ctx := context.TODO()
				ctrl.processNextWorkItem()
				for ctrl.pvcProcessingStore.namespaceQueue.Len() != 0 {
					ctrl.processPVCsByNamespace(ctx)
				}
			}

			if ctrl.queue.Len() > 0 {
				// 队列中仍有任务，继续处理。
				continue
			}

			currentActionCount := len(client.Actions())
			if currentActionCount < len(test.expectedActions) {
				// 只在动作数量变化时打印日志，避免每次等待都输出。
				if lastReportedActionCount < currentActionCount {
					logger.V(5).Info("Waiting for the remaining actions", "test", test.name, "currentActionCount", currentActionCount, "expectedActionCount", len(test.expectedActions))
					lastReportedActionCount = currentActionCount
				}
				// 期望的动作尚未全部出现，继续等待；
				// 通常是控制器正在进行指数退避重试。
				time.Sleep(10 * time.Millisecond)
				continue
			}
			break
		}
		actions := client.Actions()
		for i, action := range actions {
			if len(test.expectedActions) < i+1 {
				t.Errorf("Test %q: %d unexpected actions: %+v", test.name, len(actions)-len(test.expectedActions), dump.Pretty(actions[i:]))
				break
			}

			expectedAction := test.expectedActions[i]
			if !reflect.DeepEqual(expectedAction, action) {
				t.Errorf("Test %q: action %d\nExpected:\n%s\ngot:\n%s", test.name, i, dump.Pretty(expectedAction), dump.Pretty(action))
			}
		}

		if len(test.expectedActions) > len(actions) {
			t.Errorf("Test %q: %d additional expected actions", test.name, len(test.expectedActions)-len(actions))
			for _, a := range test.expectedActions[len(actions):] {
				t.Logf("    %+v", a)
			}
		}

	}
}
