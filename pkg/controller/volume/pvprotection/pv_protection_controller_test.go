/*
Copyright 2018 The Kubernetes Authors.

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

package pvprotection

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
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/dump"
)

const defaultPVName = "default-pv"

// reaction 描述注入到 fake client 的 reactor，用于模拟 API 错误。
type reaction struct {
	verb      string
	resource  string
	reactorfn clienttesting.ReactionFunc
}

// pv 构造一个未绑定、无 finalizer 的普通 PV。
func pv() *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: defaultPVName,
		},
	}
}

// boundPV 构造一个处于 Bound 相位的 PV，表示正在被使用。
func boundPV() *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: defaultPVName,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeBound,
		},
	}
}

// withProtectionFinalizer 给 PV 加上保护 finalizer；测试直接修改入参以简化构造。
func withProtectionFinalizer(pv *v1.PersistentVolume) *v1.PersistentVolume {
	pv.Finalizers = append(pv.Finalizers, volumeutil.PVProtectionFinalizer)
	return pv
}

// generateUpdateErrorFunc 生成"前 failures 次 update 失败、之后成功"的 reactor，
// 用于验证控制器的限速重试行为。
func generateUpdateErrorFunc(t *testing.T, failures int) clienttesting.ReactionFunc {
	i := 0
	return func(action clienttesting.Action) (bool, runtime.Object, error) {
		i++
		if i <= failures {
			// 前 N 次更新返回 Forbidden，模拟 API 暂时不可用
			update, ok := action.(clienttesting.UpdateAction)

			if !ok {
				t.Fatalf("Reactor got non-update action: %+v", action)
			}
			acc, _ := meta.Accessor(update.GetObject())
			return true, nil, apierrors.NewForbidden(update.GetResource().GroupResource(), acc.GetName(), errors.New("Mock error"))
		}
		// 更新成功。
		return false, nil, nil
	}
}

// deleted 给 PV 设置删除时间戳，模拟用户发起删除后的状态。
func deleted(pv *v1.PersistentVolume) *v1.PersistentVolume {
	pv.DeletionTimestamp = &metav1.Time{}
	return pv
}

// TestPVProtectionController 表驱动验证 finalizer 的增删与重试：
// 缺 finalizer 时补加、被删除且未使用时移除、仍被 Bound 时保留，
// 以及 API 更新失败后的重试次数与最终动作序列。
func TestPVProtectionController(t *testing.T) {
	pvVer := schema.GroupVersionResource{
		Group:    v1.GroupName,
		Version:  "v1",
		Resource: "persistentvolumes",
	}
	tests := []struct {
		name string
		// initialObjects 在测试开始前写入 fake client。
		initialObjects []runtime.Object
		// reactors 是可选的客户端拦截器。
		reactors []reaction
		// updatedPV 模拟 PV 事件，该对象也会自动加入 initialObjects。
		updatedPV *v1.PersistentVolume
		// expectedActions 是期望发生的 kubeclient 动作列表，用于精确断言。
		expectedActions []clienttesting.Action
	}{
		// PV 事件。
		//
		{
			// 普通 PV 缺少 finalizer 时应补加
			name:      "PV without finalizer -> finalizer is added",
			updatedPV: pv(),
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateAction(pvVer, "", withProtectionFinalizer(pv())),
			},
		},
		{
			// 已有 finalizer 的普通 PV 不触发更新
			name:            "PVC with finalizer -> no action",
			updatedPV:       withProtectionFinalizer(pv()),
			expectedActions: []clienttesting.Action{},
		},
		{
			// 更新失败两次后第三次成功，验证重试
			name:      "saving PVC finalizer fails -> controller retries",
			updatedPV: pv(),
			reactors: []reaction{
				{
					verb:      "update",
					resource:  "persistentvolumes",
					reactorfn: generateUpdateErrorFunc(t, 2 /* 更新失败两次 */),
				},
			},
			expectedActions: []clienttesting.Action{
				// 本次更新失败。
				clienttesting.NewUpdateAction(pvVer, "", withProtectionFinalizer(pv())),
				// 本次更新也失败。
				clienttesting.NewUpdateAction(pvVer, "", withProtectionFinalizer(pv())),
				// 本次更新成功。
				clienttesting.NewUpdateAction(pvVer, "", withProtectionFinalizer(pv())),
			},
		},
		{
			// 删除中的未使用 PV 应移除 finalizer 放行删除
			name:      "deleted PV with finalizer -> finalizer is removed",
			updatedPV: deleted(withProtectionFinalizer(pv())),
			expectedActions: []clienttesting.Action{
				clienttesting.NewUpdateAction(pvVer, "", deleted(pv())),
			},
		},
		{
			// 移除 finalizer 失败后重试
			name:      "finalizer removal fails -> controller retries",
			updatedPV: deleted(withProtectionFinalizer(pv())),
			reactors: []reaction{
				{
					verb:      "update",
					resource:  "persistentvolumes",
					reactorfn: generateUpdateErrorFunc(t, 2 /* 更新失败两次 */),
				},
			},
			expectedActions: []clienttesting.Action{
				// 失败。
				clienttesting.NewUpdateAction(pvVer, "", deleted(pv())),
				// 也失败。
				clienttesting.NewUpdateAction(pvVer, "", deleted(pv())),
				// 成功。
				clienttesting.NewUpdateAction(pvVer, "", deleted(pv())),
			},
		},
		{
			// 删除中但仍 Bound 的 PV 必须保留 finalizer，避免数据丢失
			name:            "deleted PVC with finalizer + PV is bound -> finalizer is not removed",
			updatedPV:       deleted(withProtectionFinalizer(boundPV())),
			expectedActions: []clienttesting.Action{},
		},
	}

	for _, test := range tests {
		// 用初始对象构造 fake client
		objs := test.initialObjects
		if test.updatedPV != nil {
			objs = append(objs, test.updatedPV)
		}

		client := fake.NewSimpleClientset(objs...)

		// 创建 informer。
		informers := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		pvInformer := informers.Core().V1().PersistentVolumes()

		// 预填充 informer 存储，使控制器的 lister 能读到对象
		for _, obj := range objs {
			switch obj.(type) {
			case *v1.PersistentVolume:
				pvInformer.Informer().GetStore().Add(obj)
			default:
				t.Fatalf("Unknown initialObject type: %+v", obj)
			}
		}

		// 注入 reactor 以模拟 API 错误
		for _, reactor := range test.reactors {
			client.Fake.PrependReactor(reactor.verb, reactor.resource, reactor.reactorfn)
		}

		// 创建控制器。
		logger, _ := ktesting.NewTestContext(t)
		ctrl, err := NewPVProtectionController(logger, pvInformer, client)
		if err != nil {
			t.Fatalf("unexpected error constructing controller: %v", err)
		}

		// 通过模拟 PV 事件启动测试流程
		if test.updatedPV != nil {
			ctrl.pvAddedUpdated(logger, test.updatedPV)
		}

		// 驱动队列直到获得预期动作；失败重试有指数退避，故设置超时与轮询
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
				// 队列仍有重试任务，立即继续处理
				continue
			}
			currentActionCount := len(client.Actions())
			if currentActionCount < len(test.expectedActions) {
				// 只在动作数变化时打印，避免退避期间刷屏
				if lastReportedActionCount < currentActionCount {
					logger.V(5).Info("Waiting for the remaining actions", "test", test.name, "currentActionCount", currentActionCount, "expectedActionCount", len(test.expectedActions))
					lastReportedActionCount = currentActionCount
				}
				// 等待重试发生，通常是限速队列的指数退避
				time.Sleep(10 * time.Millisecond)
				continue
			}
			break
		}
		actions := client.Actions()

		// 精确比较整个动作序列，确保没有多余或缺失的 API 调用
		if !reflect.DeepEqual(actions, test.expectedActions) {
			t.Errorf("Test %q: action not expected\nExpected:\n%s\ngot:\n%s", test.name, dump.Pretty(test.expectedActions), dump.Pretty(actions))
		}

	}

}
