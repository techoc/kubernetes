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

package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	apps "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// 测试用的标签与选择器：production/test 分别模拟匹配与不匹配的 Pod 标签，
// controllerUID 是模拟控制器的 UID。
var (
	productionLabel         = map[string]string{"type": "production"}
	testLabel               = map[string]string{"type": "testing"}
	productionLabelSelector = labels.Set{"type": "production"}.AsSelector()
	controllerUID           = "123"
)

// newPod 构造一个属于默认命名空间的 Pod 测试对象；owner 非 nil 时为其设置控制器引用。
func newPod(podName string, label map[string]string, owner metav1.Object) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Labels:    label,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Image: "foo/bar",
				},
			},
		},
	}
	// 仅当指定 owner 时才建立控制者引用，用于模拟孤儿 Pod 与已被认领的 Pod。
	if owner != nil {
		pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(owner, apps.SchemeGroupVersion.WithKind("Fake"))}
	}
	return pod
}

// TestClaimPods 验证 PodControllerRefManager.ClaimPods 对 Pod 的认领/释放规则：
// 标签选择器、命名空间、删除状态、已有属主及 finalizer 对认领结果的影响。
func TestClaimPods(t *testing.T) {
	controllerKind := schema.GroupVersionKind{}
	type test struct {
		// name 是用例名；manager 是被测管理器；pods 是输入 Pod 集合；
		// claimed 是期望认领的 Pod；patches 是期望发出的补丁数量。
		name    string
		manager *PodControllerRefManager
		pods    []*v1.Pod
		claimed []*v1.Pod
		patches int
	}
	var tests = []test{
		func() test {
			controller := v1.ReplicationController{}
			controller.Namespace = metav1.NamespaceDefault
			return test{
				// 只认领标签匹配的 Pod，并为之打补丁建立属主引用。
				name: "Claim pods with correct label",
				manager: NewPodControllerRefManager(&FakePodControl{},
					&controller,
					productionLabelSelector,
					controllerKind,
					func(ctx context.Context) error { return nil }),
				pods:    []*v1.Pod{newPod("pod1", productionLabel, nil), newPod("pod2", testLabel, nil)},
				claimed: []*v1.Pod{newPod("pod1", productionLabel, nil)},
				patches: 1,
			}
		}(),
		func() test {
			controller := v1.ReplicationController{}
			controller.Namespace = metav1.NamespaceDefault
			controller.UID = types.UID(controllerUID)
			now := metav1.Now()
			controller.DeletionTimestamp = &now
			return test{
				// 控制器已进入删除流程，不能再认领任何 Pod。
				name: "Controller marked for deletion can not claim pods",
				manager: NewPodControllerRefManager(&FakePodControl{},
					&controller,
					productionLabelSelector,
					controllerKind,
					func(ctx context.Context) error { return nil }),
				pods:    []*v1.Pod{newPod("pod1", productionLabel, nil), newPod("pod2", productionLabel, nil)},
				claimed: nil,
			}
		}(),
		func() test {
			controller := v1.ReplicationController{}
			controller.Namespace = metav1.NamespaceDefault
			controller.UID = types.UID(controllerUID)
			now := metav1.Now()
			controller.DeletionTimestamp = &now
			return test{
				// 删除中的控制器可保留已认领的 Pod，但不能认领新的孤儿 Pod。
				name: "Controller marked for deletion can not claim new pods",
				manager: NewPodControllerRefManager(&FakePodControl{},
					&controller,
					productionLabelSelector,
					controllerKind,
					func(ctx context.Context) error { return nil }),
				pods:    []*v1.Pod{newPod("pod1", productionLabel, &controller), newPod("pod2", productionLabel, nil)},
				claimed: []*v1.Pod{newPod("pod1", productionLabel, &controller)},
			}
		}(),
		func() test {
			controller := v1.ReplicationController{}
			controller2 := v1.ReplicationController{}
			controller.UID = types.UID(controllerUID)
			controller.Namespace = metav1.NamespaceDefault
			controller2.UID = types.UID("AAAAA")
			controller2.Namespace = metav1.NamespaceDefault
			return test{
				// 已被其他控制器认领的 Pod 不能被抢占。
				name: "Controller can not claim pods owned by another controller",
				manager: NewPodControllerRefManager(&FakePodControl{},
					&controller,
					productionLabelSelector,
					controllerKind,
					func(ctx context.Context) error { return nil }),
				pods:    []*v1.Pod{newPod("pod1", productionLabel, &controller), newPod("pod2", productionLabel, &controller2)},
				claimed: []*v1.Pod{newPod("pod1", productionLabel, &controller)},
			}
		}(),
		func() test {
			controller := v1.ReplicationController{}
			controller.Namespace = metav1.NamespaceDefault
			controller.UID = types.UID(controllerUID)
			return test{
				// 标签不再匹配时，控制器应释放已认领的 Pod（移除属主引用）。
				name: "Controller releases claimed pods when selector doesn't match",
				manager: NewPodControllerRefManager(&FakePodControl{},
					&controller,
					productionLabelSelector,
					controllerKind,
					func(ctx context.Context) error { return nil }),
				pods:    []*v1.Pod{newPod("pod1", productionLabel, &controller), newPod("pod2", testLabel, &controller)},
				claimed: []*v1.Pod{newPod("pod1", productionLabel, &controller)},
				patches: 1,
			}
		}(),
		func() test {
			controller := v1.ReplicationController{}
			controller.Namespace = metav1.NamespaceDefault
			controller.UID = types.UID(controllerUID)
			podToDelete1 := newPod("pod1", productionLabel, &controller)
			podToDelete2 := newPod("pod2", productionLabel, nil)
			now := metav1.Now()
			podToDelete1.DeletionTimestamp = &now
			podToDelete2.DeletionTimestamp = &now

			return test{
				// 正在删除的孤儿 Pod 不应被认领；已认领的仍保留。
				name: "Controller does not claim orphaned pods marked for deletion",
				manager: NewPodControllerRefManager(&FakePodControl{},
					&controller,
					productionLabelSelector,
					controllerKind,
					func(ctx context.Context) error { return nil }),
				pods:    []*v1.Pod{podToDelete1, podToDelete2},
				claimed: []*v1.Pod{podToDelete1},
			}
		}(),
		func() test {
			controller := v1.ReplicationController{}
			controller.Namespace = metav1.NamespaceDefault
			controller.UID = types.UID(controllerUID)
			return test{
				// 带 finalizer 时，认领与释放都要同时维护 finalizer 列表。
				name: "Controller claims or release pods according to selector with finalizers",
				manager: NewPodControllerRefManager(&FakePodControl{},
					&controller,
					productionLabelSelector,
					controllerKind,
					func(ctx context.Context) error { return nil },
					"foo-finalizer", "bar-finalizer"),
				pods:    []*v1.Pod{newPod("pod1", productionLabel, &controller), newPod("pod2", testLabel, &controller), newPod("pod3", productionLabel, nil)},
				claimed: []*v1.Pod{newPod("pod1", productionLabel, &controller), newPod("pod3", productionLabel, nil)},
				patches: 2,
			}
		}(),
		func() test {
			controller := v1.ReplicationController{}
			controller.Namespace = metav1.NamespaceDefault
			controller.UID = types.UID(controllerUID)
			pod1 := newPod("pod1", productionLabel, nil)
			pod2 := newPod("pod2", productionLabel, nil)
			pod2.Namespace = "fakens"
			return test{
				// 命名空间隔离：命名空间级控制器不能跨命名空间认领 Pod。
				name: "Controller does not claim pods of different namespace",
				manager: NewPodControllerRefManager(&FakePodControl{},
					&controller,
					productionLabelSelector,
					controllerKind,
					func(ctx context.Context) error { return nil }),
				pods:    []*v1.Pod{pod1, pod2},
				claimed: []*v1.Pod{pod1},
				patches: 1,
			}
		}(),
		func() test {
			// 以空命名空间模拟集群级控制器。
			controller := v1.ReplicationController{}
			controller.Namespace = ""
			controller.UID = types.UID(controllerUID)
			pod1 := newPod("pod1", productionLabel, nil)
			pod2 := newPod("pod2", productionLabel, nil)
			pod2.Namespace = "fakens"
			return test{
				// 集群级控制器可以认领任意命名空间中标签匹配的 Pod。
				name: "Cluster scoped controller claims pods of specified namespace",
				manager: NewPodControllerRefManager(&FakePodControl{},
					&controller,
					productionLabelSelector,
					controllerKind,
					func(ctx context.Context) error { return nil }),
				pods:    []*v1.Pod{pod1, pod2},
				claimed: []*v1.Pod{pod1, pod2},
				patches: 2,
			}
		}(),
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claimed, err := test.manager.ClaimPods(context.TODO(), test.pods)
			// 先比较认领结果，再核对补丁数量及补丁内容（UID 与 finalizer）。
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if diff := cmp.Diff(test.claimed, claimed); diff != "" {
				t.Errorf("Claimed wrong pods (-want,+got):\n%s", diff)
			}
			// 只有 FakePodControl 才能检查补丁，类型不匹配时跳过后续记录校验。
			fakePodControl, ok := test.manager.podControl.(*FakePodControl)
			if !ok {
				return
			}
			if p := len(fakePodControl.Patches); p != test.patches {
				t.Errorf("ClaimPods issues %d patches, want %d", p, test.patches)
			}
			for _, p := range fakePodControl.Patches {
				patch := string(p)
				if uid := string(test.manager.Controller.GetUID()); !strings.Contains(patch, uid) {
					t.Errorf("Patch doesn't contain controller UID %s", uid)
				}
				for _, f := range test.manager.finalizers {
					if !strings.Contains(patch, f) {
						t.Errorf("Patch doesn't contain finalizer %s, %q", patch, f)
					}
				}
			}
		})
	}
}

// TestGeneratePatchBytesForDelete 验证 GenerateDeleteOwnerRefStrategicMergeBytes 生成的
// strategic merge 补丁结构，覆盖多属主、UID 转义与 finalizer 删除等场景。
func TestGeneratePatchBytesForDelete(t *testing.T) {
	tests := []struct {
		// name 是用例名；ownerUID 是要移除的属主 UID 列表；
		// dependentUID 是被修改对象的 UID；finalizers 是要移除的 finalizer；want 是期望的补丁字节。
		name         string
		ownerUID     []types.UID
		dependentUID types.UID
		finalizers   []string
		want         []byte
	}{
		{
			// 校验单属主时补丁的基本 JSON 结构。
			name:         "check the structure of patch bytes",
			ownerUID:     []types.UID{"ss1"},
			dependentUID: "ss2",
			finalizers:   []string{},
			want:         []byte(`{"metadata":{"uid":"ss2","ownerReferences":[{"$patch":"delete","uid":"ss1"}]}}`),
		},
		{
			// 属主 UID 中的引号必须被正确转义，防止生成非法 JSON。
			name:         "check if parent uid is escaped",
			ownerUID:     []types.UID{`ss1"hello`},
			dependentUID: "ss2",
			finalizers:   []string{},
			want:         []byte(`{"metadata":{"uid":"ss2","ownerReferences":[{"$patch":"delete","uid":"ss1\"hello"}]}}`),
		},
		{
			// 依赖对象 UID 中的引号同样需要转义。
			name:         "check if revision uid uid is escaped",
			ownerUID:     []types.UID{`ss1`},
			dependentUID: `ss2"hello`,
			finalizers:   []string{},
			want:         []byte(`{"metadata":{"uid":"ss2\"hello","ownerReferences":[{"$patch":"delete","uid":"ss1"}]}}`),
		},
		{
			// 多个属主时，每个属主引用都应生成一条 delete 补丁。
			name:         "check the structure of patch bytes with multiple owners",
			ownerUID:     []types.UID{"ss1", "ss2"},
			dependentUID: "ss2",
			finalizers:   []string{},
			want:         []byte(`{"metadata":{"uid":"ss2","ownerReferences":[{"$patch":"delete","uid":"ss1"},{"$patch":"delete","uid":"ss2"}]}}`),
		},
		{
			// 指定 finalizer 时，补丁还应包含从原值列表中删除该项的操作。
			name:         "check the structure of patch bytes with a finalizer and multiple owners",
			ownerUID:     []types.UID{"ss1", "ss2"},
			dependentUID: "ss2",
			finalizers:   []string{"f1"},
			want:         []byte(`{"metadata":{"uid":"ss2","ownerReferences":[{"$patch":"delete","uid":"ss1"},{"$patch":"delete","uid":"ss2"}],"$deleteFromPrimitiveList/finalizers":["f1"]}}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := GenerateDeleteOwnerRefStrategicMergeBytes(tt.dependentUID, tt.ownerUID, tt.finalizers...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("generatePatchBytesForDelete() got = %s, want %s", got, tt.want)
			}
		})
	}
}
