/*
Copyright 2024 The Kubernetes Authors.

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

package vacprotection

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/util/protectionutil"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/dump"
)

var (
	vacGVR = storagev1.SchemeGroupVersion.WithResource("volumeattributesclasses")

	vac1                         = protectionutil.MakeVolumeAttributesClass().Name("vac1").Obj()
	vac1WithFinalizer            = protectionutil.MakeVolumeAttributesClass().Name("vac1").Finalizer(volumeutil.VACProtectionFinalizer).Obj()
	vac1TerminatingWithFinalizer = protectionutil.MakeVolumeAttributesClass().Name("vac1").Finalizer(volumeutil.VACProtectionFinalizer).Terminating().Obj()
	vac1Terminating              = protectionutil.MakeVolumeAttributesClass().Name("vac1").Terminating().Obj()

	pv1WithVAC1 = protectionutil.MakePersistentVolume().Name("pv1").VolumeAttributesClassName("vac1").Obj()
	pv1WithVAC2 = protectionutil.MakePersistentVolume().Name("pv1").VolumeAttributesClassName("vac2").Obj()
	pv2WithVAC1 = protectionutil.MakePersistentVolume().Name("pv2").VolumeAttributesClassName("vac1").Obj()

	pvc1WithVAC1            = protectionutil.MakePersistentVolumeClaim().Name("pvc1").VolumeAttributesClassName("vac1").Obj()
	pvc1WithVAC2            = protectionutil.MakePersistentVolumeClaim().Name("pvc1").VolumeAttributesClassName("vac2").Obj()
	pvc1WithVAC2CurrentVAC1 = protectionutil.MakePersistentVolumeClaim().Name("pvc1").VolumeAttributesClassName("vac2").CurrentVolumeAttributesClassName("vac1").Obj()
	pvc1WithVAC2TargetVAC1  = protectionutil.MakePersistentVolumeClaim().Name("pvc1").VolumeAttributesClassName("vac2").TargetVolumeAttributesClassName("vac1").Obj()
	pvc2WithVAC1            = protectionutil.MakePersistentVolumeClaim().Name("pvc2").VolumeAttributesClassName("vac1").Obj()
)

type reaction struct {
	// verb 与 resource 指定拦截的操作，reactorfn 决定注入何种错误。
	verb      string
	resource  string
	reactorfn clienttesting.ReactionFunc
}

// generateUpdateErrorFunc 返回一个 reactor：前 failures 次更新返回 Forbidden，
// 模拟 API 暂时不可用，用于验证控制器的重试行为。
func generateUpdateErrorFunc(t *testing.T, failures int) clienttesting.ReactionFunc {
	i := 0
	return func(action clienttesting.Action) (bool, runtime.Object, error) {
		i++
		if i <= failures {
			// 失败次数未满时拦截更新并返回 Forbidden。
			update, ok := action.(clienttesting.UpdateAction)

			if !ok {
				t.Fatalf("Reactor got non-update action: %+v", action)
			}
			acc, _ := meta.Accessor(update.GetObject())
			return true, nil, apierrors.NewForbidden(update.GetResource().GroupResource(), acc.GetName(), errors.New("Mock error"))
		}
		// 不拦截，交给后续 reactor 或默认处理，视为更新成功。
		return false, nil, nil
	}
}

// TestVACProtectionController 通过模拟 VAC/PV/PVC 事件验证 VAC 删除保护：
// 新 VAC 加 finalizer，终止中的 VAC 在无人引用时移除 finalizer，
// 仍被 PV/PVC 或其状态引用时保留，更新失败时持续重试。
func TestVACProtectionController(t *testing.T) {
	tests := []struct {
		name string
		// 测试开始前放入伪客户端的初始对象。
		initialObjects []runtime.Object
		// 可选的错误注入 reactor。
		reactors []reaction

		// 要模拟的 VAC 事件；该对象会自动加入 initialObjects。
		updatedVAC *storagev1.VolumeAttributesClass

		// 要模拟的 PV 事件；updatedPV 会自动加入 initialObjects。
		oldPV     *v1.PersistentVolume
		updatedPV *v1.PersistentVolume

		// 要模拟的 PVC 事件；updatedPVC 会自动加入 initialObjects。
		oldPVC     *v1.PersistentVolumeClaim
		updatedPVC *v1.PersistentVolumeClaim

		// 期望客户端在测试过程中发生的动作列表。
		expectedActions []clienttesting.Action
	}{
		// VAC 事件相关用例。
		{
			name:       "VAC without finalizer -> finalizer is added",
			updatedVAC: vac1,
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateAction(vacGVR, "", vac1WithFinalizer),
			},
		},
		{
			name:            "VAC with finalizer -> no action",
			updatedVAC:      vac1WithFinalizer,
			expectedActions: []clienttesting.Action{},
		},
		{
			name:       "saving VAC finalizer fails -> controller retries",
			updatedVAC: vac1,
			reactors: []reaction{
				{
					verb:      "update",
					resource:  "volumeattributesclasses",
					reactorfn: generateUpdateErrorFunc(t, 2 /* update fails twice*/),
				},
			},
			expectedActions: []clienttesting.Action{
				// 前两次因注入错误失败。
				clienttesting.NewUpdateAction(vacGVR, "", vac1WithFinalizer),
				clienttesting.NewUpdateAction(vacGVR, "", vac1WithFinalizer),
				// 第三次重试成功。
				clienttesting.NewUpdateAction(vacGVR, "", vac1WithFinalizer),
			},
		},
		{
			name:       "deleted VAC with finalizer -> finalizer is removed",
			updatedVAC: vac1TerminatingWithFinalizer,
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateAction(vacGVR, "", vac1Terminating),
			},
		},
		{
			name:       "finalizer removal fails -> controller retries",
			updatedVAC: vac1TerminatingWithFinalizer,
			reactors: []reaction{
				{
					verb:      "update",
					resource:  "volumeattributesclasses",
					reactorfn: generateUpdateErrorFunc(t, 2 /* update fails twice*/),
				},
			},
			expectedActions: []clienttesting.Action{
				// 前两次因注入错误失败。
				clienttesting.NewUpdateAction(vacGVR, "", vac1Terminating),
				clienttesting.NewUpdateAction(vacGVR, "", vac1Terminating),
				// 第三次重试成功。
				clienttesting.NewUpdateAction(vacGVR, "", vac1Terminating),
			},
		},
		{
			name:            "deleted VAC with finalizer but it's referenced by a PV -> finalizer is not removed",
			initialObjects:  []runtime.Object{pv1WithVAC1},
			updatedVAC:      vac1TerminatingWithFinalizer,
			expectedActions: []clienttesting.Action{},
		},
		{
			name:            "deleted VAC with finalizer but it's referenced by a PVC -> finalizer is not removed",
			initialObjects:  []runtime.Object{pvc1WithVAC1},
			updatedVAC:      vac1TerminatingWithFinalizer,
			expectedActions: []clienttesting.Action{},
		},
		// PV 事件相关用例。
		{
			name:           "pv changes vac and deleted VAC with finalizer -> finalizer is removed",
			initialObjects: []runtime.Object{vac1TerminatingWithFinalizer},
			oldPV:          pv1WithVAC1,
			updatedPV:      pv1WithVAC2,
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateAction(vacGVR, "", vac1Terminating),
			},
		},
		{
			name:           "pv is deleted and deleted VAC with finalizer -> finalizer is removed",
			initialObjects: []runtime.Object{vac1TerminatingWithFinalizer},
			oldPV:          pv1WithVAC1,
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateAction(vacGVR, "", vac1Terminating),
			},
		},
		{
			name:            "pv is deleted but other pv still holds terminating vac -> finalizer is not removed",
			initialObjects:  []runtime.Object{vac1TerminatingWithFinalizer, pv2WithVAC1},
			oldPV:           pv1WithVAC1,
			expectedActions: []clienttesting.Action{},
		},
		{
			name:            "pv is deleted but pvc still holds terminating vac -> finalizer is not removed",
			initialObjects:  []runtime.Object{vac1TerminatingWithFinalizer, pvc1WithVAC1},
			oldPV:           pv1WithVAC1,
			expectedActions: []clienttesting.Action{},
		},
		// PVC 事件相关用例。
		{
			name:           "pvc changes vac and deleted VAC with finalizer -> finalizer is removed",
			initialObjects: []runtime.Object{vac1TerminatingWithFinalizer},
			oldPVC:         pvc1WithVAC1,
			updatedPVC:     pvc1WithVAC2,
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateAction(vacGVR, "", vac1Terminating),
			},
		},
		{
			name:            "pvc changes vac but its status still holds terminating vac  -> finalizer is not removed",
			initialObjects:  []runtime.Object{vac1TerminatingWithFinalizer},
			oldPVC:          pvc1WithVAC1,
			updatedPVC:      pvc1WithVAC2CurrentVAC1,
			expectedActions: []clienttesting.Action{},
		},
		{
			name:            "pvc changes vac but its target status still holds terminating vac  -> finalizer is not removed",
			initialObjects:  []runtime.Object{vac1TerminatingWithFinalizer},
			oldPVC:          pvc1WithVAC1,
			updatedPVC:      pvc1WithVAC2TargetVAC1,
			expectedActions: []clienttesting.Action{},
		},
		{
			name:            "pvc is deleted but other pvc still holds terminating vac -> finalizer is not removed",
			initialObjects:  []runtime.Object{vac1TerminatingWithFinalizer, pvc2WithVAC1},
			oldPVC:          pvc1WithVAC1,
			expectedActions: []clienttesting.Action{},
		},
		{
			name:            "pvc is deleted but pv still holds terminating vac -> finalizer is not removed",
			initialObjects:  []runtime.Object{vac1TerminatingWithFinalizer, pv1WithVAC1},
			oldPVC:          pvc1WithVAC1,
			expectedActions: []clienttesting.Action{},
		},
	}

	for _, test := range tests {
		// 把事件相关对象并入初始数据，保证控制器能从客户端和 informer 中取到它们。
		objs := test.initialObjects
		if test.updatedVAC != nil {
			objs = append(objs, test.updatedVAC)
		}
		if test.updatedPV != nil {
			objs = append(objs, test.updatedPV)
		}
		if test.updatedPVC != nil {
			objs = append(objs, test.updatedPVC)
		}

		client := fake.NewSimpleClientset(objs...)

		// 构造 informer；控制器通过 informer 缓存读取 PV/PVC/VAC。
		informers := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		pvcInformer := informers.Core().V1().PersistentVolumeClaims()
		pvInformer := informers.Core().V1().PersistentVolumes()
		vacInformer := informers.Storage().V1().VolumeAttributesClasses()

		// 测试未启动 informer 的事件循环，需手动填充缓存供控制器 Get 使用。
		for _, obj := range objs {
			switch obj.(type) {
			case *v1.PersistentVolumeClaim:
				require.NoError(t, pvcInformer.Informer().GetStore().Add(obj), "failed to add object to PVC informer")
			case *v1.PersistentVolume:
				require.NoError(t, pvInformer.Informer().GetStore().Add(obj), "failed to add object to PV informer")
			case *storagev1.VolumeAttributesClass:
				require.NoError(t, vacInformer.Informer().GetStore().Add(obj), "failed to add object to VAC informer")
			default:
				t.Fatalf("Unknown initialObject type: %+v", obj)
			}
		}

		// 前置 reactor 优先于默认处理，用于注入更新失败。
		for _, reactor := range test.reactors {
			client.Fake.PrependReactor(reactor.verb, reactor.resource, reactor.reactorfn)
		}

		// 控制器创建成功后才会注册工作队列回调。
		logger, _ := ktesting.NewTestContext(t)
		ctrl, err := NewVACProtectionController(logger, client, pvcInformer, pvInformer, vacInformer)
		require.NoError(t, err, "failed to create controller")

		// 按用例直接调用对应事件处理函数，避免依赖真实 watch 的时序。
		if test.updatedVAC != nil {
			ctrl.vacAddedUpdated(logger, test.updatedVAC)
		}
		if test.updatedPV != nil {
			ctrl.pvUpdated(logger, test.oldPV, test.updatedPV)
		} else if test.oldPV != nil {
			ctrl.pvDeleted(logger, test.oldPV)
		}
		if test.updatedPVC != nil {
			ctrl.pvcUpdated(logger, test.oldPVC, test.updatedPVC)
		} else if test.oldPVC != nil {
			ctrl.pvcDeleted(logger, test.oldPVC)
		}

		// 控制器异步处理队列；轮询至动作数达到预期或超时，重试场景需要等待退避。
		timeout := time.Now().Add(10 * time.Second)
		lastReportedActionCount := 0
		for {
			if time.Now().After(timeout) {
				t.Errorf("Test %q: timed out", test.name)
				break
			}
			if ctrl.queue.Len() > 0 {
				logger.V(5).Info("Non-empty events queue, processing one", "test", test.name, "queueLength", ctrl.queue.Len())
				ctrl.processNextWorkItem(context.TODO())
			}
			if ctrl.queue.Len() > 0 {
				// 队列仍有待处理项，继续消化，避免提前判定动作缺失。
				continue
			}
			currentActionCount := len(client.Actions())
			if currentActionCount < len(test.expectedActions) {
				// 仅在动作数变化时打印，避免等待退避期间刷屏。
				if lastReportedActionCount < currentActionCount {
					logger.V(5).Info("Waiting for the remaining actions", "test", test.name, "currentActionCount", currentActionCount, "expectedActionCount", len(test.expectedActions))
					lastReportedActionCount = currentActionCount
				}
				// 动作数尚未达到预期，通常是重试的指数退避尚未结束，继续等待。
				time.Sleep(10 * time.Millisecond)
				continue
			}
			break
		}
		actions := client.Actions()

		if !reflect.DeepEqual(actions, test.expectedActions) {
			t.Errorf("Test %q: action not expected\nExpected:\n%s\ngot:\n%s", test.name, dump.Pretty(test.expectedActions), dump.Pretty(actions))
		}
	}
}
