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

package statefulset

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/kubernetes/pkg/apis/apps/install"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/controller/util/consistency"
	"k8s.io/utils/ptr"
)

// TestStatefulPodControlCreatesPods 验证 PVC 不存在时创建流程会同时建出 PVC 与 Pod，
// 并只发出正常类型事件。
func TestStatefulPodControlCreatesPods(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	claimIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	claimLister := corelisters.NewPersistentVolumeClaimLister(claimIndexer)
	control := NewStatefulPodControl(fakeClient, nil, claimLister, recorder, consistency.NewNoopConsistencyStore())
	// lister 与 fake client 是两套状态，需在 reactor 里同步索引，模拟 informer 观察到的创建。
	fakeClient.AddReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), action.GetResource().Resource)
	})
	fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		claimIndexer.Add(create.GetObject())
		return true, create.GetObject(), nil
	})
	fakeClient.AddReactor("create", "pods", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		return true, create.GetObject(), nil
	})
	if err := control.CreateStatefulPod(context.TODO(), set, pod); err != nil {
		t.Errorf("StatefulPodControl failed to create Pod error: %s", err)
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 2 {
		t.Errorf("Expected 2 events for successful create found %d", eventCount)
	}
	for i := range events {
		if !strings.Contains(events[i], v1.EventTypeNormal) {
			t.Errorf("Found unexpected non-normal event %s", events[i])
		}
	}
}

// TestStatefulPodControlCreatePodExists 验证 Pod 与 PVC 均已存在时不重复创建事件，
// 且 Pod 已存在的错误会向上传递。
func TestStatefulPodControlCreatePodExists(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	pvcs := getPersistentVolumeClaims(set, pod)
	pvcIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for k := range pvcs {
		pvc := pvcs[k]
		pvcIndexer.Add(&pvc)
	}
	pvcLister := corelisters.NewPersistentVolumeClaimLister(pvcIndexer)
	control := NewStatefulPodControl(fakeClient, nil, pvcLister, recorder, consistency.NewNoopConsistencyStore())
	fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		return true, create.GetObject(), nil
	})
	fakeClient.AddReactor("create", "pods", func(action core.Action) (bool, runtime.Object, error) {
		return true, pod, apierrors.NewAlreadyExists(action.GetResource().GroupResource(), pod.Name)
	})
	if err := control.CreateStatefulPod(context.TODO(), set, pod); !apierrors.IsAlreadyExists(err) {
		t.Errorf("Failed to create Pod error: %s", err)
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 0 {
		t.Errorf("Pod and PVC exist: got %d events, but want 0", eventCount)
		for i := range events {
			t.Log(events[i])
		}
	}
}

// TestStatefulPodControlCreatePodPvcCreateFailure 验证 PVC 创建失败时流程报错，
// 并以告警事件记录该失败。
func TestStatefulPodControlCreatePodPvcCreateFailure(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	pvcIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	pvcLister := corelisters.NewPersistentVolumeClaimLister(pvcIndexer)
	control := NewStatefulPodControl(fakeClient, nil, pvcLister, recorder, consistency.NewNoopConsistencyStore())
	fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("API server down"))
	})
	fakeClient.AddReactor("create", "pods", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		return true, create.GetObject(), nil
	})
	if err := control.CreateStatefulPod(context.TODO(), set, pod); err == nil {
		t.Error("Failed to produce error on PVC creation failure")
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 2 {
		t.Errorf("PVC create failure: got %d events, but want 2", eventCount)
	}
	for i := range events {
		if !strings.Contains(events[i], v1.EventTypeWarning) {
			t.Errorf("Found unexpected non-warning event %s", events[i])
		}
	}
}

// TestStatefulPodControlCreatePodPVCDeleting 验证 PVC 处于删除中时创建 Pod 会被阻止，
// 避免 Pod 绑定到即将消失的存储。
func TestStatefulPodControlCreatePodPVCDeleting(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	pvcs := getPersistentVolumeClaims(set, pod)
	pvcIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	deleteTime := time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)
	for k := range pvcs {
		pvc := pvcs[k]
		// 打上删除时间戳即可让控制器认为声明正在删除，无需真的发起删除。
		pvc.DeletionTimestamp = &metav1.Time{Time: deleteTime}
		pvcIndexer.Add(&pvc)
	}
	pvcLister := corelisters.NewPersistentVolumeClaimLister(pvcIndexer)
	control := NewStatefulPodControl(fakeClient, nil, pvcLister, recorder, consistency.NewNoopConsistencyStore())
	fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		return true, create.GetObject(), nil
	})
	fakeClient.AddReactor("create", "pods", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		return true, create.GetObject(), nil
	})
	if err := control.CreateStatefulPod(context.TODO(), set, pod); err == nil {
		t.Error("Failed to produce error on deleting PVC")
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 1 {
		t.Errorf("Deleting PVC: got %d events, but want 1", eventCount)
	}
	for i := range events {
		if !strings.Contains(events[i], v1.EventTypeWarning) {
			t.Errorf("Found unexpected non-warning event %s", events[i])
		}
	}
}

// fakeIndexer 用于模拟 lister 查询失败，只覆盖 GetByKey，其余方法由内嵌 Indexer 提供。
type fakeIndexer struct {
	cache.Indexer
	getError error
}

// GetByKey 始终返回预设错误，用来驱动调用方的失败分支。
func (f *fakeIndexer) GetByKey(key string) (interface{}, bool, error) {
	return nil, false, f.getError
}

// TestStatefulPodControlCreatePodPvcGetFailure 验证查询 PVC 失败时创建流程报错并发告警事件，
// 与 PVC 真正创建失败走同一处理路径。
func TestStatefulPodControlCreatePodPvcGetFailure(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	pvcIndexer := &fakeIndexer{getError: errors.New("API server down")}
	pvcLister := corelisters.NewPersistentVolumeClaimLister(pvcIndexer)
	control := NewStatefulPodControl(fakeClient, nil, pvcLister, recorder, consistency.NewNoopConsistencyStore())
	fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("API server down"))
	})
	fakeClient.AddReactor("create", "pods", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		return true, create.GetObject(), nil
	})
	if err := control.CreateStatefulPod(context.TODO(), set, pod); err == nil {
		t.Error("Failed to produce error on PVC creation failure")
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 2 {
		t.Errorf("PVC create failure: got %d events, but want 2", eventCount)
	}
	for i := range events {
		if !strings.Contains(events[i], v1.EventTypeWarning) {
			t.Errorf("Found unexpected non-warning event: %s", events[i])
		}
	}
}

// TestStatefulPodControlCreatePodFailed 验证 PVC 建好但 Pod 创建失败时的事件顺序：
// 先正常记录 PVC 创建，再告警记录 Pod 失败。
func TestStatefulPodControlCreatePodFailed(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	pvcIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	pvcLister := corelisters.NewPersistentVolumeClaimLister(pvcIndexer)
	control := NewStatefulPodControl(fakeClient, nil, pvcLister, recorder, consistency.NewNoopConsistencyStore())
	fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		return true, create.GetObject(), nil
	})
	fakeClient.AddReactor("create", "pods", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("API server down"))
	})
	if err := control.CreateStatefulPod(context.TODO(), set, pod); err == nil {
		t.Error("Failed to produce error on Pod creation failure")
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 2 {
		t.Errorf("Pod create failed: got %d events, but want 2", eventCount)
	} else if !strings.Contains(events[0], v1.EventTypeNormal) {
		t.Errorf("Found unexpected non-normal event %s", events[0])

	} else if !strings.Contains(events[1], v1.EventTypeWarning) {
		t.Errorf("Found unexpected non-warning event %s", events[1])
	}
}

// TestStatefulPodControlNoOpUpdate 验证身份与存储均匹配时更新是空操作，
// 不应触发任何客户端调用或事件。
func TestStatefulPodControlNoOpUpdate(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	claims := getPersistentVolumeClaims(set, pod)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for k := range claims {
		claim := claims[k]
		indexer.Add(&claim)
	}
	claimLister := corelisters.NewPersistentVolumeClaimLister(indexer)
	control := NewStatefulPodControl(fakeClient, nil, claimLister, recorder, consistency.NewNoopConsistencyStore())
	// 兜底 reactor 拦截任意调用：空操作若触碰 API，测试立即失败。
	fakeClient.AddReactor("*", "*", func(action core.Action) (bool, runtime.Object, error) {
		t.Error("no-op update should not make any client invocation")
		return true, nil, apierrors.NewInternalError(errors.New("if we are here we have a problem"))
	})
	if err := control.UpdateStatefulPod(ctx, set, pod); err != nil {
		t.Errorf("Error returned on no-op update error: %s", err)
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 0 {
		t.Errorf("no-op update: got %d events, but want 0", eventCount)
	}
}

// TestStatefulPodControlUpdatesIdentity 验证 Pod 名称变更后更新会写回，
// 且更新后的身份与 StatefulSet 期望一致。
func TestStatefulPodControlUpdatesIdentity(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := fake.NewSimpleClientset(set, pod)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	claimLister := corelisters.NewPersistentVolumeClaimLister(indexer)
	control := NewStatefulPodControl(fakeClient, nil, claimLister, recorder, consistency.NewNoopConsistencyStore())
	var updated *v1.Pod
	fakeClient.PrependReactor("update", "pods", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		updated = update.GetObject().(*v1.Pod)
		return true, update.GetObject(), nil
	})
	pod.Name = "goo-0"
	if err := control.UpdateStatefulPod(ctx, set, pod); err != nil {
		t.Errorf("Successful update returned an error: %s", err)
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 1 {
		t.Errorf("Pod update successful:got %d events,but want 1", eventCount)
	} else if !strings.Contains(events[0], v1.EventTypeNormal) {
		t.Errorf("Found unexpected non-normal event %s", events[0])
	}
	if !identityMatches(set, updated) {
		t.Error("Name update failed identity does not match")
	}
}

// TestStatefulPodControlUpdateIdentityFailure 验证更新失败时告警事件被记录，
// 且传入的 Pod 对象不会被就地改写身份。
func TestStatefulPodControlUpdateIdentityFailure(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	gooPod := newStatefulSetPod(set, 0)
	gooPod.Name = "goo-0"
	podIndexer.Add(gooPod)
	podLister := corelisters.NewPodLister(podIndexer)
	claimIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	claimLister := corelisters.NewPersistentVolumeClaimLister(claimIndexer)
	control := NewStatefulPodControl(fakeClient, podLister, claimLister, recorder, consistency.NewNoopConsistencyStore())
	// 在 reactor 内也改一次名字：若控制器复用了入参指针，失败后身份就会被污染。
	fakeClient.AddReactor("update", "pods", func(action core.Action) (bool, runtime.Object, error) {
		pod.Name = "goo-0"
		return true, nil, apierrors.NewInternalError(errors.New("API server down"))
	})
	pod.Name = "goo-0"
	if err := control.UpdateStatefulPod(ctx, set, pod); err == nil {
		t.Error("Failed update does not generate an error")
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 1 {
		t.Errorf("Pod update failed: got %d events, but want 1", eventCount)
	} else if !strings.Contains(events[0], v1.EventTypeWarning) {
		t.Errorf("Found unexpected non-warning event %s", events[0])
	}
	if identityMatches(set, pod) {
		t.Error("Failed update mutated Pod identity")
	}
}

// TestStatefulPodControlUpdatesPodStorage 验证缺失的 PVC 卷会被补回 Pod 声明，
// 并发出正常事件。
func TestStatefulPodControlUpdatesPodStorage(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	pvcIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	pvcLister := corelisters.NewPersistentVolumeClaimLister(pvcIndexer)
	control := NewStatefulPodControl(fakeClient, nil, pvcLister, recorder, consistency.NewNoopConsistencyStore())
	pvcs := getPersistentVolumeClaims(set, pod)
	volumes := make([]v1.Volume, 0, len(pod.Spec.Volumes))
	for i := range pod.Spec.Volumes {
		if _, contains := pvcs[pod.Spec.Volumes[i].Name]; !contains {
			volumes = append(volumes, pod.Spec.Volumes[i])
		}
	}
	pod.Spec.Volumes = volumes
	fakeClient.AddReactor("update", "pods", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		return true, update.GetObject(), nil
	})
	fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		return true, update.GetObject(), nil
	})
	var updated *v1.Pod
	fakeClient.PrependReactor("update", "pods", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		updated = update.GetObject().(*v1.Pod)
		return true, update.GetObject(), nil
	})
	if err := control.UpdateStatefulPod(ctx, set, pod); err != nil {
		t.Errorf("Successful update returned an error: %s", err)
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 2 {
		t.Errorf("Pod storage update successful: got %d events, but want 2", eventCount)
	}
	for i := range events {
		if !strings.Contains(events[i], v1.EventTypeNormal) {
			t.Errorf("Found unexpected non-normal event %s", events[i])
		}
	}
	if !storageMatches(set, updated) {
		t.Error("Name update failed identity does not match")
	}
}

// TestStatefulPodControlUpdatePodStorageFailure 验证补建 PVC 失败时更新流程报错，
// 并通过告警事件暴露给上层。
func TestStatefulPodControlUpdatePodStorageFailure(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	pvcIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	pvcLister := corelisters.NewPersistentVolumeClaimLister(pvcIndexer)
	control := NewStatefulPodControl(fakeClient, nil, pvcLister, recorder, consistency.NewNoopConsistencyStore())
	pvcs := getPersistentVolumeClaims(set, pod)
	volumes := make([]v1.Volume, 0, len(pod.Spec.Volumes))
	for i := range pod.Spec.Volumes {
		if _, contains := pvcs[pod.Spec.Volumes[i].Name]; !contains {
			volumes = append(volumes, pod.Spec.Volumes[i])
		}
	}
	pod.Spec.Volumes = volumes
	fakeClient.AddReactor("update", "pods", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		return true, update.GetObject(), nil
	})
	fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("API server down"))
	})
	if err := control.UpdateStatefulPod(ctx, set, pod); err == nil {
		t.Error("Failed Pod storage update did not return an error")
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 2 {
		t.Errorf("Pod storage update failed: got %d events, but want 2", eventCount)
	}
	for i := range events {
		if !strings.Contains(events[i], v1.EventTypeWarning) {
			t.Errorf("Found unexpected non-normal event %s", events[i])
		}
	}
}

// TestStatefulPodControlUpdatePodConflictSuccess 验证首次更新遭遇冲突后会自动重试，
// 重试成功后仍只记一条正常事件。
func TestStatefulPodControlUpdatePodConflictSuccess(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	podLister := corelisters.NewPodLister(podIndexer)
	claimIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	claimLister := corelisters.NewPersistentVolumeClaimLister(podIndexer)
	gooPod := newStatefulSetPod(set, 0)
	gooPod.Labels[apps.StatefulSetPodNameLabel] = "goo-starts"
	podIndexer.Add(gooPod)
	claims := getPersistentVolumeClaims(set, gooPod)
	for k := range claims {
		claim := claims[k]
		claimIndexer.Add(&claim)
	}
	control := NewStatefulPodControl(fakeClient, podLister, claimLister, recorder, consistency.NewNoopConsistencyStore())
	conflict := false
	fakeClient.AddReactor("update", "pods", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		// 只在第一次注入冲突，模拟一次性的资源版本竞争。
		if !conflict {
			conflict = true
			return true, update.GetObject(), apierrors.NewConflict(action.GetResource().GroupResource(), pod.Name, errors.New("conflict"))
		}
		return true, update.GetObject(), nil

	})
	pod.Labels[apps.StatefulSetPodNameLabel] = "goo-0"
	if err := control.UpdateStatefulPod(ctx, set, pod); err != nil {
		t.Errorf("Successful update returned an error: %s", err)
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 1 {
		t.Errorf("Pod update successful: got %d, but want 1", eventCount)
	} else if !strings.Contains(events[0], v1.EventTypeNormal) {
		t.Errorf("Found unexpected non-normal event %s", events[0])
	}
	if !identityMatches(set, pod) {
		t.Error("Name update failed identity does not match")
	}
}

// TestStatefulPodControlDeleteStatefulPod 表驱动验证删除成功、Pod 不存在与删除失败
// 三种情形下的错误返回及事件类型。
func TestStatefulPodControlDeleteStatefulPod(t *testing.T) {
	tests := []struct {
		name          string
		reactorErr    error
		wantErr       bool
		wantEventType string
	}{
		{
			name:          "success",
			wantEventType: v1.EventTypeNormal,
		},
		{
			name:          "not found",
			reactorErr:    apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "foo-0"),
			wantEventType: v1.EventTypeNormal,
		},
		{
			name:          "failure",
			reactorErr:    apierrors.NewInternalError(errors.New("API server down")),
			wantErr:       true,
			wantEventType: v1.EventTypeWarning,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := record.NewFakeRecorder(10)
			set := newStatefulSet(3)
			pod := newStatefulSetPod(set, 0)
			fakeClient := &fake.Clientset{}
			control := NewStatefulPodControl(fakeClient, nil, nil, recorder, consistency.NewNoopConsistencyStore())
			fakeClient.AddReactor("delete", "pods", func(action core.Action) (bool, runtime.Object, error) {
				return true, nil, tc.reactorErr
			})
			// NotFound 被视为成功：目标已消失即达到删除目的。
			err := control.DeleteStatefulPod(set, pod)
			if (err != nil) != tc.wantErr {
				t.Errorf("DeleteStatefulPod() error = %v, wantErr %v", err, tc.wantErr)
			}
			events := collectEvents(recorder.Events)
			if eventCount := len(events); eventCount != 1 {
				t.Errorf("got %d events, want 1", eventCount)
			} else if !strings.Contains(events[0], tc.wantEventType) {
				t.Errorf("got event %q, want %s event type", events[0], tc.wantEventType)
			}
		})
	}
}

// TestStatefulPodControlClaimsMatchDeletionPolcy 验证保留策略接线：
// Retain 时匹配，改为删除时不再匹配，具体判定逻辑不在此重复测试。
func TestStatefulPodControlClaimsMatchDeletionPolcy(t *testing.T) {
	// isClaimOwnerUpToDate 的判定逻辑已在 stateful_set_utils_test 充分覆盖；
	// 这里只验证 pod control 是否正确接到了该方法。
	_, ctx := ktesting.NewTestContext(t)
	fakeClient := &fake.Clientset{}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	claimLister := corelisters.NewPersistentVolumeClaimLister(indexer)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	claims := getPersistentVolumeClaims(set, pod)
	for k := range claims {
		claim := claims[k]
		indexer.Add(&claim)
	}
	control := NewStatefulPodControl(fakeClient, nil, claimLister, &noopRecorder{}, consistency.NewNoopConsistencyStore())
	set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	if matches, err := control.ClaimsMatchRetentionPolicy(ctx, set, pod); err != nil {
		t.Errorf("Unexpected error for ClaimsMatchRetentionPolicy (retain): %v", err)
	} else if !matches {
		t.Error("Unexpected non-match for ClaimsMatchRetentionPolicy (retain)")
	}
	set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	if matches, err := control.ClaimsMatchRetentionPolicy(ctx, set, pod); err != nil {
		t.Errorf("Unexpected error for ClaimsMatchRetentionPolicy (set deletion): %v", err)
	} else if matches {
		t.Error("Unexpected match for ClaimsMatchRetentionPolicy (set deletion)")
	}
}

// TestStatefulPodControlUpdatePodClaimForRetentionPolicy 验证删除策略下
// 无属主的 PVC 会被加上控制器引用，已有控制器的则跳过。
func TestStatefulPodControlUpdatePodClaimForRetentionPolicy(t *testing.T) {
	// 各种更新条件已在 stateful_set_utils_test 充分覆盖；
	// 这里只验证 pod control 到该方法的接线。
	_, ctx := ktesting.NewTestContext(t)
	testCases := []struct {
		name      string
		ownerRef  []metav1.OwnerReference
		expectRef bool
	}{
		{
			name:      "bare PVC",
			expectRef: true,
		},
		{
			name:      "PVC already controller",
			ownerRef:  []metav1.OwnerReference{{Controller: ptr.To(true), Name: "foobar"}},
			expectRef: false,
		},
	}

	for _, tc := range testCases {
		fakeClient := &fake.Clientset{}
		indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
		claimLister := corelisters.NewPersistentVolumeClaimLister(indexer)
		fakeClient.AddReactor("update", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
			update := action.(core.UpdateAction)
			// fake client 不会更新 lister，需手动回填以模拟 informer 同步。
			if err := indexer.Update(update.GetObject()); err != nil {
				t.Fatalf("could not update index: %v", err)
			}
			return true, update.GetObject(), nil
		})
		set := newStatefulSet(3)
		set.GetObjectMeta().SetUID("set-123")
		pod0 := newStatefulSetPod(set, 0)
		claims0 := getPersistentVolumeClaims(set, pod0)
		for k := range claims0 {
			claim := claims0[k]
			if tc.ownerRef != nil {
				claim.SetOwnerReferences(tc.ownerRef)
			}
			if err := indexer.Add(&claim); err != nil {
				t.Errorf("Could not add claim %s: %v", k, err)
			}
		}
		control := NewStatefulPodControl(fakeClient, nil, claimLister, &noopRecorder{}, consistency.NewNoopConsistencyStore())
		set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
		}
		if err := control.UpdatePodClaimForRetentionPolicy(ctx, set, pod0); err != nil {
			t.Errorf("Unexpected error for UpdatePodClaimForRetentionPolicy (retain), pod0: %v", err)
		}
		for k := range claims0 {
			claim, err := claimLister.PersistentVolumeClaims(claims0[k].Namespace).Get(claims0[k].Name)
			if err != nil {
				t.Errorf("Unexpected error getting Claim %s/%s: %v", claim.Namespace, claim.Name, err)
			}
			if hasOwnerRef(claim, set) != tc.expectRef {
				t.Errorf("%s: Claim %s/%s bad set owner ref", tc.name, claim.Namespace, claim.Name)
			}
		}
	}
}

// TestPodClaimIsStale 表驱动验证陈旧声明判定：缺失或属主 UID 匹配不算陈旧，
// 属主 UID 不匹配或缺失时才算陈旧。
func TestPodClaimIsStale(t *testing.T) {
	const missing = "missing"
	const exists = "exists"
	const stale = "stale"
	const withRef = "with-ref"
	testCases := []struct {
		name        string
		claimStates []string
		expected    bool
		skipPodUID  bool
	}{
		{
			name:        "all missing",
			claimStates: []string{missing, missing},
			expected:    false,
		},
		{
			name:        "no claims",
			claimStates: []string{},
			expected:    false,
		},
		{
			name:        "exists",
			claimStates: []string{missing, exists},
			expected:    false,
		},
		{
			name:        "all refs",
			claimStates: []string{withRef, withRef},
			expected:    false,
		},
		{
			name:        "stale & exists",
			claimStates: []string{stale, exists},
			expected:    true,
		},
		{
			name:        "stale & missing",
			claimStates: []string{stale, missing},
			expected:    true,
		},
		{
			name:        "withRef & stale",
			claimStates: []string{withRef, stale},
			expected:    true,
		},
		{
			name:        "withRef, no UID",
			claimStates: []string{withRef},
			skipPodUID:  true,
			expected:    true,
		},
	}
	for _, tc := range testCases {
		set := apps.StatefulSet{}
		set.Name = "set"
		set.Namespace = "default"
		set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
		}
		set.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"key": "value"}}
		claimIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
		for i, claimState := range tc.claimStates {
			claim := v1.PersistentVolumeClaim{}
			claim.Name = fmt.Sprintf("claim-%d", i)
			set.Spec.VolumeClaimTemplates = append(set.Spec.VolumeClaimTemplates, claim)
			claim.Name = fmt.Sprintf("%s-set-3", claim.Name)
			claim.Namespace = set.Namespace
			switch claimState {
			case missing:
			// 故意不加入索引，模拟声明在 API 中尚不存在。
			case exists:
				claimIndexer.Add(&claim)
			case stale:
				claim.SetOwnerReferences([]metav1.OwnerReference{
					{
						Name:       "set-3",
						UID:        types.UID("stale"),
						APIVersion: "v1",
						Kind:       "Pod",
					},
				})
				claimIndexer.Add(&claim)
			case withRef:
				claim.SetOwnerReferences([]metav1.OwnerReference{
					{
						Name:       "set-3",
						UID:        types.UID("123"),
						APIVersion: "v1",
						Kind:       "Pod",
					},
				})
				claimIndexer.Add(&claim)
			}
		}
		pod := v1.Pod{}
		pod.Name = "set-3"
		if !tc.skipPodUID {
			pod.SetUID("123")
		}
		claimLister := corelisters.NewPersistentVolumeClaimLister(claimIndexer)
		control := NewStatefulPodControl(&fake.Clientset{}, nil, claimLister, &noopRecorder{}, consistency.NewNoopConsistencyStore())
		expected := tc.expected
		// 查询错误的分支在此无法稳定制造，因此只校验陈旧判定结果。
		if stale, _ := control.PodClaimIsStale(&set, &pod); stale != expected {
			t.Errorf("unexpected stale for %s", tc.name)
		}
	}
}

// TestStatefulPodControlRetainDeletionPolicyUpdate 验证改为 Retain 策略后，
// 更新流程会移除 PVC 上指向 StatefulSet 的属主引用。
func TestStatefulPodControlRetainDeletionPolicyUpdate(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(1)
	set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	podLister := corelisters.NewPodLister(podIndexer)
	claimIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	claimLister := corelisters.NewPersistentVolumeClaimLister(claimIndexer)
	if err := podIndexer.Add(pod); err != nil {
		t.Errorf("couldn't add %s to index: %v", pod.GetName(), err)
	}
	claims := getPersistentVolumeClaims(set, pod)
	if len(claims) < 1 {
		t.Errorf("Unexpected missing PVCs")
	}
	for k := range claims {
		claim := claims[k]
		// 预先埋入属主引用，供更新流程将其移除。
		claim.SetOwnerReferences(addControllerRef(claim.GetOwnerReferences(), set, controllerKind))
		if err := claimIndexer.Add(&claim); err != nil {
			t.Errorf("couldn't add %s to index: %v", claim.GetName(), err)
		}
	}
	control := NewStatefulPodControl(fakeClient, podLister, claimLister, recorder, consistency.NewNoopConsistencyStore())
	if err := control.UpdateStatefulPod(ctx, set, pod); err != nil {
		t.Errorf("Successful update returned an error: %s", err)
	}
	for k := range claims {
		claim := claims[k]
		if hasOwnerRef(&claim, set) {
			t.Errorf("ownerRef not removed: %s/%s", claim.Namespace, claim.Name)
		}
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 1 {
		t.Errorf("delete failed: got %d events, but want 1", eventCount)
	}
}

// TestStatefulPodControlRetentionPolicyUpdate 验证改为删除策略后，
// 更新流程会给 PVC 补上指向 StatefulSet 的属主引用。
func TestStatefulPodControlRetentionPolicyUpdate(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(1)
	set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	claimIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	podIndexer.Add(pod)
	claims := getPersistentVolumeClaims(set, pod)
	if len(claims) != 1 {
		t.Errorf("Unexpected or missing PVCs")
	}
	var claim v1.PersistentVolumeClaim
	for k := range claims {
		claim = claims[k]
		claimIndexer.Add(&claim)
	}
	fakeClient.AddReactor("update", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		claimIndexer.Update(update.GetObject())
		return true, update.GetObject(), nil
	})
	podLister := corelisters.NewPodLister(podIndexer)
	claimLister := corelisters.NewPersistentVolumeClaimLister(claimIndexer)
	control := NewStatefulPodControl(fakeClient, podLister, claimLister, recorder, consistency.NewNoopConsistencyStore())
	if err := control.UpdateStatefulPod(ctx, set, pod); err != nil {
		t.Errorf("Successful update returned an error: %s", err)
	}
	updatedClaim, err := claimLister.PersistentVolumeClaims(claim.Namespace).Get(claim.Name)
	if err != nil {
		t.Errorf("Error retrieving claim %s/%s: %v", claim.Namespace, claim.Name, err)
	}
	if !hasOwnerRef(updatedClaim, set) {
		t.Errorf("ownerRef not added: %s/%s", claim.Namespace, claim.Name)
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 1 {
		t.Errorf("update failed: got %d events, but want 1", eventCount)
	}
}

// TestStatefulPodControlRetentionPolicyUpdateMissingClaims 验证 PVC 尚未出现时更新不报错，
// 等 PVC 出现后的下一次更新再补上属主引用。
func TestStatefulPodControlRetentionPolicyUpdateMissingClaims(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	recorder := record.NewFakeRecorder(10)
	set := newStatefulSet(1)
	set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	pod := newStatefulSetPod(set, 0)
	fakeClient := &fake.Clientset{}
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	podLister := corelisters.NewPodLister(podIndexer)
	claimIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	claimLister := corelisters.NewPersistentVolumeClaimLister(claimIndexer)
	podIndexer.Add(pod)
	fakeClient.AddReactor("update", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		claimIndexer.Update(update.GetObject())
		return true, update.GetObject(), nil
	})
	control := NewStatefulPodControl(fakeClient, podLister, claimLister, recorder, consistency.NewNoopConsistencyStore())
	if err := control.UpdateStatefulPod(ctx, set, pod); err != nil {
		t.Error("Unexpected error on pod update when PVCs are missing")
	}
	claims := getPersistentVolumeClaims(set, pod)
	if len(claims) != 1 {
		t.Errorf("Unexpected or missing PVCs")
	}
	var claim v1.PersistentVolumeClaim
	for k := range claims {
		claim = claims[k]
		claimIndexer.Add(&claim)
	}

	if err := control.UpdateStatefulPod(ctx, set, pod); err != nil {
		t.Errorf("Expected update to succeed, saw error %v", err)
	}
	updatedClaim, err := claimLister.PersistentVolumeClaims(claim.Namespace).Get(claim.Name)
	if err != nil {
		t.Errorf("Error retrieving claim %s/%s: %v", claim.Namespace, claim.Name, err)
	}
	if !hasOwnerRef(updatedClaim, set) {
		t.Errorf("ownerRef not added: %s/%s", claim.Namespace, claim.Name)
	}
	events := collectEvents(recorder.Events)
	if eventCount := len(events); eventCount != 1 {
		t.Errorf("update failed: got %d events, but want 2", eventCount)
	}
	if !strings.Contains(events[0], "SuccessfulUpdate") {
		t.Errorf("expected first event to be a successful update: %s", events[1])
	}
}

// collectEvents 把 fake recorder 的异步事件流一次性排空，
// 便于按数量与类型断言事件。
func collectEvents(source <-chan string) []string {
	done := false
	events := make([]string, 0)
	// 用非阻塞读取排空 channel，避免等待永远不来的后续事件。
	for !done {
		select {
		case event := <-source:
			events = append(events, event)
		default:
			done = true
		}
	}
	return events
}

// fakeRVGetter 提供可控的 last-sync resourceVersion，用于验证一致性存储门槛。
type fakeRVGetter struct {
	rv string
}

// LastStoreSyncResourceVersion 返回当前模拟的资源版本。
func (f *fakeRVGetter) LastStoreSyncResourceVersion() string {
	return f.rv
}

// TestStatefulPodControlConsistencyStore 验证增删改操作写入的资源版本
// 会被一致性存储记录：版本落后时拒绝就绪，追平后放行。
func TestStatefulPodControlConsistencyStore(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	testCases := []struct {
		name       string
		action     func(context.Context, *StatefulPodControl, *apps.StatefulSet, *v1.Pod) error
		resource   schema.GroupResource
		setupMocks func(*fake.Clientset, cache.Indexer, *v1.Pod, *apps.StatefulSet)
	}{
		{
			name: "CreatePod",
			action: func(ctx context.Context, control *StatefulPodControl, set *apps.StatefulSet, pod *v1.Pod) error {
				return control.CreateStatefulPod(ctx, set, pod)
			},
			resource: schema.GroupResource{Group: "", Resource: "pods"},
			setupMocks: func(fakeClient *fake.Clientset, claimIndexer cache.Indexer, pod *v1.Pod, set *apps.StatefulSet) {
				fakeClient.AddReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), action.GetResource().Resource)
				})
				fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
					create := action.(core.CreateAction)
					create.GetObject().(*v1.PersistentVolumeClaim).ResourceVersion = "2"
					require.NoError(t, claimIndexer.Add(create.GetObject()))
					return true, create.GetObject(), nil
				})
				fakeClient.AddReactor("create", "pods", func(action core.Action) (bool, runtime.Object, error) {
					create := action.(core.CreateAction)
					create.GetObject().(*v1.Pod).ResourceVersion = "2"
					return true, create.GetObject(), nil
				})
			},
		},
		{
			name: "UpdatePod",
			action: func(ctx context.Context, control *StatefulPodControl, set *apps.StatefulSet, pod *v1.Pod) error {
				pod.Name = "goo-0" // 制造身份不一致，迫使更新发生
				return control.UpdateStatefulPod(ctx, set, pod)
			},
			resource: schema.GroupResource{Group: "", Resource: "pods"},
			setupMocks: func(fakeClient *fake.Clientset, claimIndexer cache.Indexer, pod *v1.Pod, set *apps.StatefulSet) {
				fakeClient.PrependReactor("update", "pods", func(action core.Action) (bool, runtime.Object, error) {
					update := action.(core.UpdateAction)
					update.GetObject().(*v1.Pod).ResourceVersion = "2"
					return true, update.GetObject(), nil
				})
			},
		},
		{
			name: "CreateClaim",
			action: func(ctx context.Context, control *StatefulPodControl, set *apps.StatefulSet, pod *v1.Pod) error {
				return control.createPersistentVolumeClaims(set, pod)
			},
			resource: schema.GroupResource{Group: "", Resource: "persistentvolumeclaims"},
			setupMocks: func(fakeClient *fake.Clientset, claimIndexer cache.Indexer, pod *v1.Pod, set *apps.StatefulSet) {
				fakeClient.AddReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), action.GetResource().Resource)
				})
				fakeClient.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
					create := action.(core.CreateAction)
					create.GetObject().(*v1.PersistentVolumeClaim).ResourceVersion = "2"
					require.NoError(t, claimIndexer.Add(create.GetObject()))
					return true, create.GetObject(), nil
				})
			},
		},
		{
			name: "UpdateClaim",
			action: func(ctx context.Context, control *StatefulPodControl, set *apps.StatefulSet, pod *v1.Pod) error {
				return control.UpdatePodClaimForRetentionPolicy(ctx, set, pod)
			},
			resource: schema.GroupResource{Group: "", Resource: "persistentvolumeclaims"},
			setupMocks: func(fakeClient *fake.Clientset, claimIndexer cache.Indexer, pod *v1.Pod, set *apps.StatefulSet) {
				set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
					WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
				}
				claims := getPersistentVolumeClaims(set, pod)
				for k := range claims {
					claim := claims[k]
					require.NoError(t, claimIndexer.Add(&claim))
				}
				fakeClient.AddReactor("update", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
					update := action.(core.UpdateAction)
					update.GetObject().(*v1.PersistentVolumeClaim).ResourceVersion = "2"
					return true, update.GetObject(), nil
				})
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := record.NewFakeRecorder(10)
			set := newStatefulSet(3)
			set.GetObjectMeta().SetUID("set-123")
			pod := newStatefulSetPod(set, 0)
			fakeClient := &fake.Clientset{}
			podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			podLister := corelisters.NewPodLister(podIndexer)
			claimIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			claimLister := corelisters.NewPersistentVolumeClaimLister(claimIndexer)

			// 初始版本落后于写入的 "2"，制造短暂的不一致窗口。
			rvGetter := &fakeRVGetter{rv: "1"}
			consistencyStore := consistency.NewConsistencyStore(map[schema.GroupResource]consistency.LastSyncRVGetter{
				tc.resource: rvGetter,
			})

			control := NewStatefulPodControl(fakeClient, podLister, claimLister, recorder, consistencyStore)

			tc.setupMocks(fakeClient, claimIndexer, pod, set)

			if err := tc.action(ctx, control, set, pod); err != nil {
				t.Fatalf("action failed: %v", err)
			}

			// 写入尚未被消费前应拒绝就绪，否则控制器会读到过期对象。
			if err := consistencyStore.EnsureReady(types.NamespacedName{Namespace: set.Namespace, Name: set.Name}); err == nil {
				t.Error("expected consistency store to return an error, got nil")
			}

			rvGetter.rv = "2"

			if err := consistencyStore.EnsureReady(types.NamespacedName{Namespace: set.Namespace, Name: set.Name}); err != nil {
				t.Errorf("expected consistency store to be ready, got an error: %v", err)
			}
		})
	}
}
