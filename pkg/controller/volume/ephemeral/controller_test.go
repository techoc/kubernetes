/*
Copyright 2020 The Kubernetes Authors.

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

package ephemeral

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	ephemeralvolumemetrics "k8s.io/kubernetes/pkg/controller/volume/ephemeral/metrics"
)

// 测试用的固定标识，便于在表格用例间复用并精确断言资源归属。
var (
	testPodName   = "test-pod"
	testNamespace = "my-namespace"
	testPodUID    = types.UID("uidpod1")
	// otherNamespace 用于验证同名 PVC 不会跨命名空间被误用。
	otherNamespace      = "not-my-namespace"
	ephemeralVolumeName = "ephemeral-volume"

	// 共享的 Pod/PVC 夹具：部分用例需要直接修改其副本。
	testPod              = makePod(testPodName, testNamespace, testPodUID)
	testPodWithEphemeral = makePod(testPodName, testNamespace, testPodUID, *makeEphemeralVolume(ephemeralVolumeName))
	// 期望控制器创建的 PVC：名称由 Pod 与卷名拼接，属主指向该 Pod。
	testPodEphemeralClaim = makePVC(testPodName+"-"+ephemeralVolumeName, testNamespace, makeOwnerReference(testPodWithEphemeral, true))
	// 同名但无属主的 PVC，用于触发属主冲突校验。
	conflictingClaim = makePVC(testPodName+"-"+ephemeralVolumeName, testNamespace, nil)
	// 其他命名空间的同名 PVC，用于验证命名空间隔离。
	otherNamespaceClaim = makePVC(testPodName+"-"+ephemeralVolumeName, otherNamespace, nil)
)

// init 初始化 klog 标志，避免测试中使用日志接口时出现未注册标志的错误。
func init() {
	klog.InitFlags(nil)
}

// TestSyncHandler 以表格驱动方式验证 syncHandler 的各种场景：
// 创建 PVC、Pod 缺失/删除、无临时卷、跨命名空间隔离、属主冲突与创建失败。
func TestSyncHandler(t *testing.T) {
	tests := []struct {
		name   string
		podKey string
		// pvcs 与 pods 是预先放入 fake client 的对象。
		pvcs []*v1.PersistentVolumeClaim
		pods []*v1.Pod
		// expectedPVCs 是处理后期望存在的完整 PVC 列表。
		expectedPVCs []v1.PersistentVolumeClaim
		// expectedError 表示 syncHandler 是否应返回错误（进而触发重试）。
		expectedError bool
		// expectedMetrics 校验创建尝试与失败计数，确保不漏报也不误报。
		expectedMetrics expectedMetrics
	}{
		{
			name:            "create",
			pods:            []*v1.Pod{testPodWithEphemeral},
			podKey:          podKey(testPodWithEphemeral),
			expectedPVCs:    []v1.PersistentVolumeClaim{*testPodEphemeralClaim},
			expectedMetrics: expectedMetrics{1, 0},
		},
		{
			// Pod 不存在时应静默返回 nil，避免因旧事件而无限重试。
			name:   "no-such-pod",
			podKey: podKey(testPodWithEphemeral),
		},
		{
			// 已进入删除流程的 Pod 同样无需创建 PVC。
			name: "pod-deleted",
			pods: func() []*v1.Pod {
				deleted := metav1.Now()
				pods := []*v1.Pod{testPodWithEphemeral.DeepCopy()}
				pods[0].DeletionTimestamp = &deleted
				return pods
			}(),
			podKey: podKey(testPodWithEphemeral),
		},
		{
			// 没有临时卷的 Pod 不应产生任何 PVC 或指标变化。
			name:   "no-volumes",
			pods:   []*v1.Pod{testPod},
			podKey: podKey(testPod),
		},
		{
			// 其他命名空间的同名 PVC 不能阻止本命名空间创建所需 PVC。
			name:            "create-with-other-PVC",
			pods:            []*v1.Pod{testPodWithEphemeral},
			podKey:          podKey(testPodWithEphemeral),
			pvcs:            []*v1.PersistentVolumeClaim{otherNamespaceClaim},
			expectedPVCs:    []v1.PersistentVolumeClaim{*otherNamespaceClaim, *testPodEphemeralClaim},
			expectedMetrics: expectedMetrics{1, 0},
		},
		{
			// 同名 PVC 属主不匹配必须报错，不能悄悄改作他用。
			name:          "wrong-PVC-owner",
			pods:          []*v1.Pod{testPodWithEphemeral},
			podKey:        podKey(testPodWithEphemeral),
			pvcs:          []*v1.PersistentVolumeClaim{conflictingClaim},
			expectedPVCs:  []v1.PersistentVolumeClaim{*conflictingClaim},
			expectedError: true,
		},
		{
			// 模拟 API 冲突：应记录失败指标并返回错误以便重试。
			name:            "create-conflict",
			pods:            []*v1.Pod{testPodWithEphemeral},
			podKey:          podKey(testPodWithEphemeral),
			expectedMetrics: expectedMetrics{1, 1},
			expectedError:   true,
		},
	}

	for _, tc := range tests {
		// 全局日志与指标不隔离，必须串行执行用例，否则计数会互相污染。
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			var objects []runtime.Object
			for _, pod := range tc.pods {
				objects = append(objects, pod)
			}
			for _, pvc := range tc.pvcs {
				objects = append(objects, pvc)
			}

			fakeKubeClient := createTestClient(objects...)
			// 通过 reactor 注入冲突错误，覆盖真实 API 中不常见的失败路径。
			if tc.expectedMetrics.numFailures > 0 {
				fakeKubeClient.PrependReactor("create", "persistentvolumeclaims", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, apierrors.NewConflict(action.GetResource().GroupResource(), "fake name", errors.New("fake conflict"))
				})
			}
			setupMetrics()
			informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
			// defer 后进先出：先 cancel 解除 informer goroutine 的阻塞，再 Shutdown。
			defer informerFactory.Shutdown()
			defer cancel()
			podInformer := informerFactory.Core().V1().Pods()
			pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()

			c, err := NewController(ctx, fakeKubeClient, podInformer, pvcInformer)
			if err != nil {
				t.Fatalf("error creating ephemeral controller : %v", err)
			}
			ec, _ := c.(*ephemeralController)
			defer ec.queue.ShutDown()

			// 必须等缓存同步完成，否则 lister 看不到预置对象。
			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())
			cache.WaitForCacheSync(ctx.Done(), podInformer.Informer().HasSynced, pvcInformer.Informer().HasSynced)

			err = ec.syncHandler(context.TODO(), tc.podKey)
			// 错误存在性与期望完全一致才算通过：既不能漏报，也不能误报成功。
			if err != nil && !tc.expectedError {
				t.Fatalf("unexpected error while running handler: %v", err)
			}
			if err == nil && tc.expectedError {
				t.Fatalf("unexpected success")
			}

			// 用空命名空间列出全部 PVC，才能同时验证跨命名空间隔离与创建结果。
			pvcs, err := fakeKubeClient.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("unexpected error while listing PVCs: %v", err)
			}
			// 排序后再比较，消除 API 返回顺序带来的假失败。
			assert.Equal(t, sortPVCs(tc.expectedPVCs), sortPVCs(pvcs.Items))
			// 指标与 PVC 列表同等重要：失败时被跳过的路径仍可能留下错误计数。
			expectMetrics(t, tc.expectedMetrics)
		})
	}
}

// makePVC 构造测试用 PVC；owner 为 nil 时生成无属主的冲突对象。
func makePVC(name, namespace string, owner *metav1.OwnerReference) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       v1.PersistentVolumeClaimSpec{},
	}
	if owner != nil {
		pvc.OwnerReferences = []metav1.OwnerReference{*owner}
	}

	return pvc
}

// makeEphemeralVolume 构造带空模板的临时内联卷定义。
func makeEphemeralVolume(name string) *v1.Volume {
	return &v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			Ephemeral: &v1.EphemeralVolumeSource{
				VolumeClaimTemplate: &v1.PersistentVolumeClaimTemplate{},
			},
		},
	}
}

// makePod 构造测试用 Pod，可附加任意数量的卷定义。
func makePod(name, namespace string, uid types.UID, volumes ...v1.Volume) *v1.Pod {
	pvc := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: uid},
		Spec: v1.PodSpec{
			Volumes: volumes,
		},
	}

	return pvc
}

// podKey 返回 Pod 的命名空间/名称键。
// 此处固定使用共享夹具，因为所有测试 Pod 的名称与命名空间都相同。
func podKey(pod *v1.Pod) string {
	key, _ := cache.DeletionHandlingMetaNamespaceKeyFunc(testPodWithEphemeral)
	return key
}

// makeOwnerReference 构造指向给定 Pod 的属主引用，用于验证控制器属主与级联删除标志。
func makeOwnerReference(pod *v1.Pod, isController bool) *metav1.OwnerReference {
	isTrue := true
	return &metav1.OwnerReference{
		APIVersion:         "v1",
		Kind:               "Pod",
		Name:               pod.Name,
		UID:                pod.UID,
		Controller:         &isController,
		BlockOwnerDeletion: &isTrue,
	}
}

// sortPVCs 按命名空间和名称排序 PVC，使切片断言不受 API 返回顺序影响。
func sortPVCs(pvcs []v1.PersistentVolumeClaim) []v1.PersistentVolumeClaim {
	sort.Slice(pvcs, func(i, j int) bool {
		return pvcs[i].Namespace < pvcs[j].Namespace ||
			pvcs[i].Name < pvcs[j].Name
	})
	return pvcs
}

// createTestClient 用预置对象创建 fake clientset，避免依赖真实 API 服务器。
func createTestClient(objects ...runtime.Object) *fake.Clientset {
	fakeClient := fake.NewSimpleClientset(objects...)
	return fakeClient
}

// 以下是指标断言用的辅助类型与函数。

// expectedMetrics 表示一次同步后期望的创建尝试次数与失败次数。
type expectedMetrics struct {
	numCreated  int
	numFailures int
}

// expectMetrics 校验创建尝试与失败计数是否符合预期。
func expectMetrics(t *testing.T, em expectedMetrics) {
	t.Helper()

	// 同时检查两个计数器：只检查 PVC 结果无法发现多报的失败或重试。
	actualCreated, err := testutil.GetCounterMetricValue(ephemeralvolumemetrics.EphemeralVolumeCreateAttempts)
	handleErr(t, err, "ephemeralVolumeCreate")
	if actualCreated != float64(em.numCreated) {
		t.Errorf("Expected PVCs to be created %d, got %v", em.numCreated, actualCreated)
	}
	actualConflicts, err := testutil.GetCounterMetricValue(ephemeralvolumemetrics.EphemeralVolumeCreateFailures)
	handleErr(t, err, "ephemeralVolumeCreate/Conflict")
	if actualConflicts != float64(em.numFailures) {
		t.Errorf("Expected PVCs to have conflicts %d, got %v", em.numFailures, actualConflicts)
	}
}

// handleErr 在读取指标失败时记录测试错误，并标明具体失败的指标名。
func handleErr(t *testing.T, err error, metricName string) {
	if err != nil {
		t.Errorf("Failed to get %s value, err: %v", metricName, err)
	}
}

// setupMetrics 注册并清零全局计数器，保证每个用例从相同的初始状态开始。
func setupMetrics() {
	ephemeralvolumemetrics.RegisterMetrics()
	ephemeralvolumemetrics.EphemeralVolumeCreateAttempts.Reset()
	ephemeralvolumemetrics.EphemeralVolumeCreateFailures.Reset()
}
