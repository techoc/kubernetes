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

package persistentvolume

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	"k8s.io/kubernetes/pkg/controller"
	pvtesting "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/testing"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/recyclerclient"
)

func init() {
	klog.InitFlags(nil)
}

// 这是持久卷控制器的单元测试框架。它用测试用的 PVC/PV 填充控制器，可模拟以下场景：
// 1) 调用一次 syncClaim/syncVolume。
// 2) 多次调用 syncClaim/syncVolume（既模拟“PVC/PV 变更”事件，也模拟周期同步），
//    直到控制器收敛、不再修改任何对象。
// 3) 模拟近乎真实的 API server/etcd，调用卷/声明的增删改。
// 在所有场景中，测试结束后框架都会把最终的 PVC/PV 与期望列表比较并报告差异。

// controllerTest 描述单个控制器用例的输入与期望。
// 每个用例在开始前把初始卷/声明灌入控制器，并指定实际执行的函数；常见选择有：
//   - testSyncClaim：对 initialClaims 中首个声明调用 syncClaim。
//   - testSyncClaimError：调用 syncClaim 并期望返回错误。
//   - testSyncVolume：对 initialVolumes 中首个卷调用 syncVolume。
//   - 特殊场景可传入自定义函数。
//
// 用例还给出结束时期望的卷/声明列表以及期望产生的事件列表。
type controllerTest struct {
	// 用例名称，仅用于日志输出
	name string
	// 控制器卷缓存的初始内容。
	initialVolumes []*v1.PersistentVolume
	// 测试结束时控制器卷缓存的期望内容。
	expectedVolumes []*v1.PersistentVolume
	// 控制器声明缓存的初始内容。
	initialClaims []*v1.PersistentVolumeClaim
	// 测试结束时控制器声明缓存的期望内容。
	expectedClaims []*v1.PersistentVolumeClaim
	// 期望事件——只要实际事件以前缀匹配即算通过，不校验完整消息文本。
	expectedEvents []string
	// 在匹配的操作上注入的错误
	errors []pvtesting.ReactorError
	// 作为测试主体调用的函数。
	test testCall
}

// annSkipLocalStore 用于标记只加入 fake apiserver（可通过 Get 取到）、
// 而不加入本地 store 的初始 PV/PVC，借此模拟对象尚未进入 informer 缓存的时序。
const annSkipLocalStore = "pv-testing-skip-local-store"

type testCall func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error

const testNamespace = "default"
const mockPluginName = "kubernetes.io/mock-volume"

var novolumes []*v1.PersistentVolume
var noclaims []*v1.PersistentVolumeClaim
var noevents = []string{}
var noerrors = []pvtesting.ReactorError{}

type volumeReactor struct {
	*pvtesting.VolumeReactor
	ctrl *PersistentVolumeController
}

// newVolumeReactor 在通用 VolumeReactor 之上持有控制器引用，便于等待收敛时读取控制器内部状态。
func newVolumeReactor(ctx context.Context, client *fake.Clientset, ctrl *PersistentVolumeController, fakeVolumeWatch, fakeClaimWatch *watch.FakeWatcher, errors []pvtesting.ReactorError) *volumeReactor {
	return &volumeReactor{
		pvtesting.NewVolumeReactor(ctx, client, fakeVolumeWatch, fakeClaimWatch, errors),
		ctrl,
	}
}

// waitForIdle 等待所有测试、控制器及其他 goroutine 完成工作，并在连续 10 毫秒内没有新动作时返回。
func (r *volumeReactor) waitForIdle() {
	r.ctrl.runningOperations.WaitForCompletion()
	// 每 10 毫秒检查一次控制器是否仍在动作，空闲即退出。
	oldChanges := -1
	for {
		time.Sleep(10 * time.Millisecond)
		changes := r.GetChangeCount()
		if changes == oldChanges {
			// 最近 10 毫秒无变化，说明控制器已空闲。
			break
		}
		oldChanges = changes
	}
}

// waitTest 在约 10 秒超时内等待所有 goroutine 完成，并使当前卷/声明列表与期望列表一致；
// 采用指数退避是为了给异步操作留出收敛时间。
func (r *volumeReactor) waitTest(test controllerTest) error {
	// 从 10 毫秒起步、每步翻倍，10 步合计约 10.23 秒
	backoff := wait.Backoff{
		Duration: 10 * time.Millisecond,
		Jitter:   0,
		Factor:   2,
		Steps:    10,
	}
	err := wait.ExponentialBackoff(backoff, func() (done bool, err error) {
		// 先等待进行中的操作结束，避免竞态导致误判
		r.ctrl.runningOperations.WaitForCompletion()

		// 达到期望状态才返回 true，让退避循环退出
		err1 := r.CheckClaims(test.expectedClaims)
		err2 := r.CheckVolumes(test.expectedVolumes)
		if err1 == nil && err2 == nil {
			return true, nil
		}
		return false, nil
	})
	return err
}

// checkEvents 把期望事件与测试期间实际产生的事件逐一比较并报告差异，
// 采用前缀匹配是因为消息中的对象名等细节由被测逻辑拼接。
func checkEvents(t *testing.T, ctx context.Context, expectedEvents []string, ctrl *PersistentVolumeController) error {
	var err error

	// 读取记录的事件——最多等待 1 分钟，以防慢 goroutine 尚未写入。
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	logger := klog.FromContext(ctx)
	fakeRecorder := ctrl.eventRecorder.(*record.FakeRecorder)
	gotEvents := []string{}
	finished := false
	for len(gotEvents) < len(expectedEvents) && !finished {
		select {
		case event, ok := <-fakeRecorder.Events:
			if ok {
				logger.V(5).Info("Event recorder got event", "event", event)
				gotEvents = append(gotEvents, event)
			} else {
				logger.V(5).Info("Event recorder finished")
				finished = true
			}
		case _, _ = <-timer.C:
			logger.V(5).Info("Event recorder timeout")
			finished = true
		}
	}

	// 逐项校验事件数量、顺序及前缀是否匹配
	for i, expected := range expectedEvents {
		if len(gotEvents) <= i {
			t.Errorf("Event %q not emitted", expected)
			err = fmt.Errorf("events do not match")
			continue
		}
		received := gotEvents[i]
		if !strings.HasPrefix(received, expected) {
			t.Errorf("Unexpected event received, expected %q, got %q", expected, received)
			err = fmt.Errorf("events do not match")
		}
	}
	for i := len(expectedEvents); i < len(gotEvents); i++ {
		t.Errorf("Unexpected event received: %q", gotEvents[i])
		err = fmt.Errorf("events do not match")
	}
	return err
}

// alwaysReady 作为 listerSynced 的桩：测试注入的是静态对象，无需等待 informer 同步。
func alwaysReady() bool { return true }

// newTestController 构造测试用控制器：用 FakeRecorder 替换事件记录器、把 listerSynced 置为始终就绪，
// 并缩短异步创建 PV 的重试间隔，使单测能快速收敛。
func newTestController(ctx context.Context, kubeClient clientset.Interface, informerFactory informers.SharedInformerFactory, enableDynamicProvisioning bool) (*PersistentVolumeController, error) {
	if informerFactory == nil {
		informerFactory = informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
	}
	params := ControllerParameters{
		KubeClient:                kubeClient,
		SyncPeriod:                5 * time.Second,
		VolumePlugins:             []volume.VolumePlugin{},
		VolumeInformer:            informerFactory.Core().V1().PersistentVolumes(),
		ClaimInformer:             informerFactory.Core().V1().PersistentVolumeClaims(),
		ClassInformer:             informerFactory.Storage().V1().StorageClasses(),
		PodInformer:               informerFactory.Core().V1().Pods(),
		NodeInformer:              informerFactory.Core().V1().Nodes(),
		EnableDynamicProvisioning: enableDynamicProvisioning,
	}
	ctrl, err := NewController(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to construct persistentvolume controller: %v", err)
	}
	ctrl.eventRecorder = record.NewFakeRecorder(1000)
	ctrl.volumeListerSynced = alwaysReady
	ctrl.claimListerSynced = alwaysReady
	ctrl.classListerSynced = alwaysReady
	// 缩短异步创建 PV 的间隔，加快测试
	ctrl.createProvisionedPVInterval = 5 * time.Millisecond
	return ctrl, nil
}

// newVolume 按给定属性构造 PV；GCE PD 源只是占位值，多数用例只关心容量/绑定/阶段等元数据。
func newVolume(name, capacity, boundToClaimUID, boundToClaimName string, phase v1.PersistentVolumePhase, reclaimPolicy v1.PersistentVolumeReclaimPolicy, class string, annotations ...string) *v1.PersistentVolume {
	fs := v1.PersistentVolumeFilesystem
	volume := v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			ResourceVersion: "1",
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse(capacity),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
			},
			AccessModes:                   []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany},
			PersistentVolumeReclaimPolicy: reclaimPolicy,
			StorageClassName:              class,
			VolumeMode:                    &fs,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: phase,
		},
	}

	if boundToClaimName != "" {
		volume.Spec.ClaimRef = &v1.ObjectReference{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
			UID:        types.UID(boundToClaimUID),
			Namespace:  testNamespace,
			Name:       boundToClaimName,
		}
	}

	if len(annotations) > 0 {
		volume.Annotations = make(map[string]string)
		for _, a := range annotations {
			switch a {
			case storagehelpers.AnnDynamicallyProvisioned:
				volume.Annotations[a] = mockPluginName
			default:
				volume.Annotations[a] = "yes"
			}
		}
	}

	return &volume
}

// newExternalProvisionedVolume 构造 CSI 外部置备的 PV，用于覆盖 finalizer、驱动名等外部置备特有字段。
func newExternalProvisionedVolume(name, capacity, boundToClaimUID, boundToClaimName string, phase v1.PersistentVolumePhase, reclaimPolicy v1.PersistentVolumeReclaimPolicy, class string, driverName string, finalizers []string, annotations ...string) *v1.PersistentVolume {
	fs := v1.PersistentVolumeFilesystem
	volume := v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			ResourceVersion: "1",
			Finalizers:      finalizers,
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse(capacity),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       driverName,
					VolumeHandle: "527b55dc-c7db-4574-9226-2e33318b06a3",
					ReadOnly:     false,
					FSType:       "ext4",
					VolumeAttributes: map[string]string{
						"Test-Key": "Test-Value",
					},
				},
			},
			AccessModes:                   []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany},
			PersistentVolumeReclaimPolicy: reclaimPolicy,
			StorageClassName:              class,
			VolumeMode:                    &fs,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: phase,
		},
	}

	if boundToClaimName != "" {
		volume.Spec.ClaimRef = &v1.ObjectReference{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
			UID:        types.UID(boundToClaimUID),
			Namespace:  testNamespace,
			Name:       boundToClaimName,
		}
	}

	if len(annotations) > 0 {
		volume.Annotations = make(map[string]string)
		for _, a := range annotations {
			switch a {
			case storagehelpers.AnnDynamicallyProvisioned:
				volume.Annotations[a] = driverName
			default:
				volume.Annotations[a] = "yes"
			}
		}
	}

	return &volume
}

// newVolumeWithFinalizers 在 newVolume 基础上附加 finalizer，用于测试删除被阻塞的场景。
func newVolumeWithFinalizers(name, capacity, boundToClaimUID, boundToClaimName string, phase v1.PersistentVolumePhase, reclaimPolicy v1.PersistentVolumeReclaimPolicy, class string, finalizers []string, annotations ...string) *v1.PersistentVolume {
	retVolume := newVolume(name, capacity, boundToClaimUID, boundToClaimName, phase, reclaimPolicy, class, annotations...)
	retVolume.SetFinalizers(finalizers)
	return retVolume
}

// withLabels 给卷切片的首个元素设置标签，便于在测试中内联拼装卷对象。
func withLabels(labels map[string]string, volumes []*v1.PersistentVolume) []*v1.PersistentVolume {
	volumes[0].Labels = labels
	return volumes
}

// withLabelSelector 把首个声明的选择器设为给定标签的 MatchLabels，便于内联拼装对象。
func withLabelSelector(labels map[string]string, claims []*v1.PersistentVolumeClaim) []*v1.PersistentVolumeClaim {
	claims[0].Spec.Selector = &metav1.LabelSelector{
		MatchLabels: labels,
	}

	return claims
}

// withVolumeVolumeMode 设置首个卷的 VolumeMode，便于内联拼装卷对象。
func withVolumeVolumeMode(mode *v1.PersistentVolumeMode, volumes []*v1.PersistentVolume) []*v1.PersistentVolume {
	volumes[0].Spec.VolumeMode = mode
	return volumes
}

// withClaimVolumeMode 设置首个声明的 VolumeMode，便于内联拼装声明对象。
func withClaimVolumeMode(mode *v1.PersistentVolumeMode, claims []*v1.PersistentVolumeClaim) []*v1.PersistentVolumeClaim {
	claims[0].Spec.VolumeMode = mode
	return claims
}

// withExpectedCapacity 设置首个声明的 Status.Capacity，用于表达期望的绑定容量。
func withExpectedCapacity(capacity string, claims []*v1.PersistentVolumeClaim) []*v1.PersistentVolumeClaim {
	claims[0].Status.Capacity = v1.ResourceList{
		v1.ResourceName(v1.ResourceStorage): resource.MustParse(capacity),
	}

	return claims
}

// withExpectedVAC 设置首个声明当前的 VolumeAttributesClassName，用于表达期望状态。
func withExpectedVAC(vacName *string, claims []*v1.PersistentVolumeClaim) []*v1.PersistentVolumeClaim {
	claims[0].Status.CurrentVolumeAttributesClassName = vacName
	return claims
}

// withMessage 把给定消息写入首个卷的 Status.Message，便于内联构造期望对象。
func withMessage(message string, volumes []*v1.PersistentVolume) []*v1.PersistentVolume {
	volumes[0].Status.Message = message
	return volumes
}

// newVolumeArray 返回只含一个卷的切片，该卷由 newVolume() 用相同参数构造。
// TODO: 让 newVolumeArray 接受卷属性类名作为输入参数。
func newVolumeArray(name, capacity, boundToClaimUID, boundToClaimName string, phase v1.PersistentVolumePhase, reclaimPolicy v1.PersistentVolumeReclaimPolicy, class string, annotations ...string) []*v1.PersistentVolume {
	return []*v1.PersistentVolume{
		newVolume(name, capacity, boundToClaimUID, boundToClaimName, phase, reclaimPolicy, class, annotations...),
	}
}

// withVolumeDeletionTimestamp 给所有 PV 设置固定的删除时间戳。用固定时间而非 time.Now()，
// 否则期望值与实际值会因纳秒差异永远不相等。
func withVolumeDeletionTimestamp(pvs []*v1.PersistentVolume) []*v1.PersistentVolume {
	result := []*v1.PersistentVolume{}
	for _, pv := range pvs {
		// 使用 time.Now() 会导致期望值与实际值时间戳不一致
		deleteTime := metav1.Date(2020, time.February, 18, 10, 30, 30, 10, time.UTC)
		pv.SetDeletionTimestamp(&deleteTime)
		result = append(result, pv)
	}
	return result
}

// volumesWithFinalizers 批量设置 finalizer，用于构造处于删除中或被保护的 PV。
func volumesWithFinalizers(pvs []*v1.PersistentVolume, finalizers []string) []*v1.PersistentVolume {
	result := []*v1.PersistentVolume{}
	for _, pv := range pvs {
		pv.SetFinalizers(finalizers)
		result = append(result, pv)
	}
	return result
}

// newClaim 按给定属性构造 PVC；绑定态会同步填充 Status，使多数用例无需再手工设置。
func newClaim(name, claimUID, capacity, boundToVolume string, phase v1.PersistentVolumeClaimPhase, class *string, annotations ...string) *v1.PersistentVolumeClaim {
	fs := v1.PersistentVolumeFilesystem
	claim := v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       testNamespace,
			UID:             types.UID(claimUID),
			ResourceVersion: "1",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(capacity),
				},
			},
			VolumeName:       boundToVolume,
			StorageClassName: class,
			VolumeMode:       &fs,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: phase,
		},
	}

	if len(annotations) > 0 {
		claim.Annotations = make(map[string]string)
		for _, a := range annotations {
			switch a {
			case storagehelpers.AnnBetaStorageProvisioner, storagehelpers.AnnStorageProvisioner:
				claim.Annotations[a] = mockPluginName
			default:
				claim.Annotations[a] = "yes"
			}
		}
	}

	// 已绑定的声明必须带有自洽的 Status，否则会被控制器判定为状态不一致。
	if phase == v1.ClaimBound {
		claim.Status.AccessModes = claim.Spec.AccessModes
		// 多数用例直接复制请求容量即可，
		// 个别用例可用 withExpectedCapacity() 覆盖。
		claim.Status.Capacity = claim.Spec.Resources.Requests
		// 多数用例直接复制请求的 VAC 即可，
		// 个别用例可用 withExpectedVAC() 覆盖。
		claim.Status.CurrentVolumeAttributesClassName = claim.Spec.VolumeAttributesClassName
	}

	return &claim
}

// newClaimArray 返回只含一个声明的切片，该声明由 newClaim() 用相同参数构造。
// TODO: 让 newClaimArray 接受卷属性类名作为输入参数。
func newClaimArray(name, claimUID, capacity, boundToVolume string, phase v1.PersistentVolumeClaimPhase, class *string, annotations ...string) []*v1.PersistentVolumeClaim {
	return []*v1.PersistentVolumeClaim{
		newClaim(name, claimUID, capacity, boundToVolume, phase, class, annotations...),
	}
}

// claimWithVAC 设置首个声明的 VolumeAttributesClassName，用于内联构造期望对象。
func claimWithVAC(vacName *string, claims []*v1.PersistentVolumeClaim) []*v1.PersistentVolumeClaim {
	claims[0].Spec.VolumeAttributesClassName = vacName
	return claims
}

// claimWithAnnotation 把给定注解写入声明切片的首个元素，便于内联拼装声明对象。
// TODO(refactor): 清理此辅助函数及其他 claim 数组辅助函数（多数假设数组长度为一）。
// 所有调用点改用 annotateClaim；测试需要 claimArrays，但多数只操作单个 claim。
func claimWithAnnotation(name, value string, claims []*v1.PersistentVolumeClaim) []*v1.PersistentVolumeClaim {
	if claims[0].Annotations == nil {
		claims[0].Annotations = map[string]string{name: value}
	} else {
		claims[0].Annotations[name] = value
	}
	return claims
}

// claimWithDataSource 给首个声明设置数据源，用于覆盖克隆/恢复场景的测试。
func claimWithDataSource(name, kind, apiGroup string, claims []*v1.PersistentVolumeClaim) []*v1.PersistentVolumeClaim {
	claims[0].Spec.DataSource = &v1.TypedLocalObjectReference{
		Name:     name,
		Kind:     kind,
		APIGroup: &apiGroup,
	}
	return claims
}

// annotateClaim 给单个声明批量写入注解，适用于非数组形式的构造场景。
func annotateClaim(claim *v1.PersistentVolumeClaim, ann map[string]string) *v1.PersistentVolumeClaim {
	if claim.Annotations == nil {
		claim.Annotations = map[string]string{}
	}
	for key, val := range ann {
		claim.Annotations[key] = val
	}
	return claim
}

// volumeWithVAC 给单个卷设置 VolumeAttributesClassName，供内联构造使用。
func volumeWithVAC(vacName string, volume *v1.PersistentVolume) *v1.PersistentVolume {
	volume.Spec.VolumeAttributesClassName = &vacName
	return volume
}

// volumesWithVAC 批量设置卷的 VolumeAttributesClassName。
func volumesWithVAC(vacName string, volumes []*v1.PersistentVolume) []*v1.PersistentVolume {
	for _, volume := range volumes {
		volumeWithVAC(vacName, volume)
	}
	return volumes
}

// volumeWithAnnotation 给单个卷写入注解，供内联构造使用。
func volumeWithAnnotation(name, value string, volume *v1.PersistentVolume) *v1.PersistentVolume {
	if volume.Annotations == nil {
		volume.Annotations = map[string]string{name: value}
	} else {
		volume.Annotations[name] = value
	}
	return volume
}

// volumesWithAnnotation 批量给卷写入注解。
func volumesWithAnnotation(name, value string, volumes []*v1.PersistentVolume) []*v1.PersistentVolume {
	for _, volume := range volumes {
		volumeWithAnnotation(name, value, volume)
	}
	return volumes
}

// claimWithAccessMode 设置首个声明的访问模式。
func claimWithAccessMode(modes []v1.PersistentVolumeAccessMode, claims []*v1.PersistentVolumeClaim) []*v1.PersistentVolumeClaim {
	claims[0].Spec.AccessModes = modes
	return claims
}

// testSyncClaim 对首个初始声明调用一次 syncClaim，用于单步同步测试。
func testSyncClaim(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
	return ctrl.syncClaim(context.TODO(), test.initialClaims[0])
}

// testSyncClaimError 期望 syncClaim 返回错误；成功返回反而视为测试失败。
func testSyncClaimError(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
	err := ctrl.syncClaim(context.TODO(), test.initialClaims[0])

	if err != nil {
		return nil
	}
	return fmt.Errorf("syncClaim succeeded when failure was expected")
}

// testSyncVolume 对首个初始卷调用一次 syncVolume。
func testSyncVolume(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
	return ctrl.syncVolume(context.TODO(), test.initialVolumes[0])
}

type operationType string

const operationDelete = "Delete"
const operationRecycle = "Recycle"

var (
	classGold                    = "gold"
	classSilver                  = "silver"
	classCopper                  = "copper"
	classEmpty                   = ""
	classNonExisting             = "non-existing"
	classExternal                = "external"
	classExternalWait            = "external-wait"
	classUnknownInternal         = "unknown-internal"
	classUnsupportedMountOptions = "unsupported-mountoptions"
	classLarge                   = "large"
	classWait                    = "wait"
	classCSI                     = "csi"

	modeWait = storage.VolumeBindingWaitForFirstConsumer
)

// wrapTestWithPluginCalls 返回一个 testCall：
//   - 注入同时实现回收/删除/置备接口的模拟插件，按预置顺序返回结果或错误；
//   - 再调用被包装的测试函数。
func wrapTestWithPluginCalls(expectedRecycleCalls, expectedDeleteCalls []error, expectedProvisionCalls []provisionCall, toWrap testCall) testCall {
	return func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
		plugin := &mockVolumePlugin{
			recycleCalls:   expectedRecycleCalls,
			deleteCalls:    expectedDeleteCalls,
			provisionCalls: expectedProvisionCalls,
		}
		ctrl.volumePluginMgr.InitPlugins([]volume.VolumePlugin{plugin}, nil /* prober */, ctrl)
		return toWrap(ctrl, reactor, test)
	}
}

// wrapTestWithReclaimCalls 返回一个 testCall：按操作类型注入回收器或删除器，
// 使其按序返回预置错误，再调用被包装的测试函数。
func wrapTestWithReclaimCalls(operation operationType, expectedOperationCalls []error, toWrap testCall) testCall {
	if operation == operationDelete {
		return wrapTestWithPluginCalls(nil, expectedOperationCalls, nil, toWrap)
	} else {
		return wrapTestWithPluginCalls(expectedOperationCalls, nil, nil, toWrap)
	}
}

// wrapTestWithProvisionCalls 返回一个 testCall：注入按序返回预置结果的置备器，
// 再调用被包装的测试函数。
func wrapTestWithProvisionCalls(expectedProvisionCalls []provisionCall, toWrap testCall) testCall {
	return wrapTestWithPluginCalls(nil, nil, expectedProvisionCalls, toWrap)
}

// fakeCSINameTranslator 把内置插件名映射为固定的模拟 CSI 驱动名，用于迁移测试。
type fakeCSINameTranslator struct{}

// GetCSINameFromInTreeName 返回固定的驱动名，不关心传入的插件名。
func (t fakeCSINameTranslator) GetCSINameFromInTreeName(pluginName string) (string, error) {
	return "vendor.com/MockCSIDriver", nil
}

// fakeCSIMigratedPluginManager 宣称所有插件均启用 CSI 迁移，以触发迁移代码路径。
type fakeCSIMigratedPluginManager struct{}

// IsMigrationEnabledForPlugin 恒返回 true，强制走 CSI 迁移分支。
func (t fakeCSIMigratedPluginManager) IsMigrationEnabledForPlugin(pluginName string) bool {
	return true
}

// wrapTestWithCSIMigrationProvisionCalls 返回一个 testCall：
// - 注入模拟插件并替换名称转换器与迁移管理器，伪装成 CSI 迁移已启用；
// - 再调用被包装的测试函数。
func wrapTestWithCSIMigrationProvisionCalls(toWrap testCall) testCall {
	plugin := &mockVolumePlugin{}
	return func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
		ctrl.volumePluginMgr.InitPlugins([]volume.VolumePlugin{plugin}, nil /* prober */, ctrl)
		ctrl.translator = fakeCSINameTranslator{}
		ctrl.csiMigratedPluginManager = fakeCSIMigratedPluginManager{}
		return toWrap(ctrl, reactor, test)
	}
}

// wrapTestWithInjectedOperation 返回一个 testCall：在 scheduleOperation 处设钩子阻塞原测试函数，
// 趁控制器等待操作锁时调用注入函数模拟外部事件，随后放行控制器并校验其后续行为。
func wrapTestWithInjectedOperation(ctx context.Context, toWrap testCall, injectBeforeOperation func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor)) testCall {

	return func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
		// 在异步操作开始前注入钩子，以此截住 scheduleOperation
		ctrl.preOperationHook = func(operationName string) {
			// 在钩子内执行注入逻辑，此时控制器正等待操作锁
			klog.FromContext(ctx).V(4).Info("Reactor: scheduleOperation reached, injecting call")
			injectBeforeOperation(ctrl, reactor)
		}

		// 在独立 goroutine 中运行被测函数：钩子会阻塞控制器，主 goroutine 需保持响应才能完成注入。
		var testError error
		var testFinished int32

		go func() {
			testError = toWrap(ctrl, reactor, test)
			// 用原子标志通知主 goroutine 被测函数已结束，避免数据竞争。
			atomic.StoreInt32(&testFinished, 1)
		}()

		// 轮询等待被测函数结束；与上面的原子写入配对。
		for atomic.LoadInt32(&testFinished) == 0 {
			time.Sleep(time.Millisecond * 10)
		}

		return testError
	}
}

// evaluateTestResults 统一校验声明、卷和事件三方面结果，避免每个用例重复断言。
func evaluateTestResults(ctx context.Context, ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest, t *testing.T) {
	// 依次校验声明、卷、事件是否符合期望
	if err := reactor.CheckClaims(test.expectedClaims); err != nil {
		t.Errorf("Test %q: %v", test.name, err)

	}
	if err := reactor.CheckVolumes(test.expectedVolumes); err != nil {
		t.Errorf("Test %q: %v", test.name, err)
	}

	if err := checkEvents(t, ctx, test.expectedEvents, ctrl); err != nil {
		t.Errorf("Test %q: %v", test.name, err)
	}
}

// runSyncTests 执行单次同步测试：先用初始数据填充控制器，只调用一次被测函数，
// 再比较最终的卷/声明与期望值。刻意只调一次，用于验证单步行为。
func runSyncTests(t *testing.T, ctx context.Context, tests []controllerTest, storageClasses []*storage.StorageClass, pods []*v1.Pod) {
	doit := func(t *testing.T, test controllerTest) {
		// 构造测试用控制器
		client := &fake.Clientset{}
		ctrl, err := newTestController(ctx, client, nil, true)
		if err != nil {
			t.Fatalf("Test %q construct persistent volume failed: %v", test.name, err)
		}
		reactor := newVolumeReactor(ctx, client, ctrl, nil, nil, test.errors)
		for _, claim := range test.initialClaims {
			if metav1.HasAnnotation(claim.ObjectMeta, annSkipLocalStore) {
				continue
			}
			ctrl.claims.Add(claim)
		}
		for _, volume := range test.initialVolumes {
			if metav1.HasAnnotation(volume.ObjectMeta, annSkipLocalStore) {
				continue
			}
			ctrl.volumes.store.Add(volume)
		}
		reactor.AddClaims(test.initialClaims)
		reactor.AddVolumes(test.initialVolumes)

		// 用自定义 lister 注入 StorageClass，绕开 informer 同步时序。
		indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
		for _, class := range storageClasses {
			indexer.Add(class)
		}
		ctrl.classLister = storagelisters.NewStorageClassLister(indexer)

		podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
		for _, pod := range pods {
			podIndexer.Add(pod)
			ctrl.podIndexer.Add(pod)
		}
		ctrl.podLister = corelisters.NewPodLister(podIndexer)

		// 执行待测函数。
		err = test.test(ctrl, reactor.VolumeReactor, test)
		if err != nil {
			t.Errorf("Test %q failed: %v", test.name, err)
		}

		// 等待异步操作把对象推进到期望状态
		err = reactor.waitTest(test)
		if err != nil {
			t.Errorf("Test %q failed: %v", test.name, err)
		}

		evaluateTestResults(ctx, ctrl, reactor.VolumeReactor, test, t)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			doit(t, test)
		})
	}
}

// runMultisyncTests 反复调用 syncClaim/syncVolume 并穿插周期同步，直到系统收敛。流程：
//  0. 用初始数据填充控制器。
//  1. 先像 runSyncTests 一样调用一次 controllerTest.testCall()。
//  2. 对上一步发生变化的对象再次调用相应的 sync，模拟“对象变更”事件；仍有变化则重复本步。
//  3. 无新变化时对所有对象调用 sync，模拟周期同步。
//  4. 若第 3 步造成变化，回到第 2 步处理事件，并最终再次执行第 3 步。
//  5. 周期同步不再产生变化时结束，比较最终对象与期望值。
//
// 为避免逻辑错误导致死循环，循环次数设有上限。
func runMultisyncTests(t *testing.T, ctx context.Context, tests []controllerTest, storageClasses []*storage.StorageClass, defaultStorageClass string) {
	logger := klog.FromContext(ctx)
	run := func(t *testing.T, test controllerTest) {
		logger.V(4).Info("Starting multisync test", "testName", test.name)

		// 构造测试用控制器
		client := &fake.Clientset{}
		ctrl, err := newTestController(ctx, client, nil, true)
		if err != nil {
			t.Fatalf("Test %q construct persistent volume failed: %v", test.name, err)
		}

		// 用自定义 lister 注入 StorageClass，绕开 informer 同步时序。
		indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
		for _, class := range storageClasses {
			indexer.Add(class)
		}
		ctrl.classLister = storagelisters.NewStorageClassLister(indexer)

		reactor := newVolumeReactor(ctx, client, ctrl, nil, nil, test.errors)
		for _, claim := range test.initialClaims {
			ctrl.claims.Add(claim)
		}
		for _, volume := range test.initialVolumes {
			ctrl.volumes.store.Add(volume)
		}
		reactor.AddClaims(test.initialClaims)
		reactor.AddVolumes(test.initialVolumes)

		// 执行被测函数
		err = test.test(ctrl, reactor.VolumeReactor, test)
		if err != nil {
			t.Errorf("Test %q failed: %v", test.name, err)
		}

		// 交替模拟“对象变更”事件与周期同步，直到达到稳定状态。
		firstSync := true
		counter := 0
		for {
			counter++
			logger.V(4).Info("Test", "testName", test.name, "iteration", counter)

			if counter > 100 {
				t.Errorf("Test %q failed: too many iterations", test.name)
				break
			}

			// 先等待所有 goroutine 结束，再判断是否有待处理变更
			reactor.waitForIdle()

			obj := reactor.PopChange(ctx)
			if obj == nil {
				// 没有变更对象时判断是否收敛
				if firstSync || reactor.GetChangeCount() > 0 {
					// 上次周期同步之后又出现了变更，需要再跑一轮全量同步
					// 模拟所有对象的周期同步，直到不再产生变更。
					firstSync = false
					logger.V(4).Info("Test simulating periodical sync of all claims and volumes", "testName", test.name)
					reactor.SyncAll()
				} else {
					// 上一轮同步未产生任何更新，系统已稳定，结束测试。
					break
				}
			}
			// 稍作等待让 fake apiserver 的退避冷却，避免干扰下一轮
			time.Sleep(600 * time.Millisecond)

			// 有变更对象，逐个处理
			switch obj.(type) {
			case *v1.PersistentVolumeClaim:
				claim := obj.(*v1.PersistentVolumeClaim)
				// 模拟 PVC 更新事件：先更新本地缓存再同步
				ctrl.claims.Update(claim)
				err = ctrl.syncClaim(context.TODO(), claim)
				if err != nil {
					if err == pvtesting.ErrVersionConflict {
						// 版本冲突是 fake reactor 的正常现象，忽略
						logger.V(4).Info("Test intentionally ignores version error")
					} else {
						t.Errorf("Error calling syncClaim: %v", err)
						// 遇到真实错误即终止循环
						break
					}
				}
				// 继续处理本次同步产生的变更
				continue
			case *v1.PersistentVolume:
				volume := obj.(*v1.PersistentVolume)
				// 模拟 PV 更新事件：先更新本地缓存再同步
				ctrl.volumes.store.Update(volume)
				err = ctrl.syncVolume(context.TODO(), volume)
				if err != nil {
					if err == pvtesting.ErrVersionConflict {
						// 版本冲突是 fake reactor 的正常现象，忽略
						logger.V(4).Info("Test intentionally ignores version error")
					} else {
						t.Errorf("Error calling syncVolume: %v", err)
						// 遇到真实错误即终止循环
						break
					}
				}
				// 继续处理本次同步产生的变更
				continue
			}
		}
		evaluateTestResults(ctx, ctrl, reactor.VolumeReactor, test, t)
		logger.V(4).Info("Test finished after iterations", "testName", test.name, "iterations", counter)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			run(t, test)
		})
	}
}

// mockVolumePlugin 是置备/删除/回收用的桩插件，内部按序保存预置返回值，
// 用于覆盖成功与错误路径。
type mockVolumePlugin struct {
	provisionCalls       []provisionCall
	provisionCallCounter int
	deleteCalls          []error
	deleteCallCounter    int
	recycleCalls         []error
	recycleCallCounter   int
	provisionOptions     volume.VolumeOptions
}

type provisionCall struct {
	expectedParameters map[string]string
	ret                error
}

var _ volume.VolumePlugin = &mockVolumePlugin{}
var _ volume.RecyclableVolumePlugin = &mockVolumePlugin{}
var _ volume.DeletableVolumePlugin = &mockVolumePlugin{}
var _ volume.ProvisionableVolumePlugin = &mockVolumePlugin{}

// Init 是接口要求的空实现，测试无需初始化真实主机环境。
func (plugin *mockVolumePlugin) Init(host volume.VolumeHost) error {
	return nil
}

// GetPluginName 返回模拟插件名，供控制器按名称查找。
func (plugin *mockVolumePlugin) GetPluginName() string {
	return mockPluginName
}

// GetVolumeName 直接返回规格名称，避免解析真实卷源。
func (plugin *mockVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	return spec.Name(), nil
}

// CanSupport 恒为 true，让测试对象都能匹配该插件。
func (plugin *mockVolumePlugin) CanSupport(spec *volume.Spec) bool {
	return true
}

// RequiresRemount 返回 false：本插件不参与挂载流程。
func (plugin *mockVolumePlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// SupportsMountOption 返回 false。
func (plugin *mockVolumePlugin) SupportsMountOption() bool {
	return false
}

// SupportsBulkVolumeVerification 返回 false。
func (plugin *mockVolumePlugin) SupportsBulkVolumeVerification() bool {
	return false
}

// ConstructVolumeSpec 返回零值，测试不会走真实卷重建逻辑。
func (plugin *mockVolumePlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	return volume.ReconstructedVolume{}, nil
}

// SupportsSELinuxContextMount 返回 false。
func (plugin *mockVolumePlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// NewMounter 不支持挂载，直接报错以暴露误用。
func (plugin *mockVolumePlugin) NewMounter(spec *volume.Spec, podRef *v1.Pod) (volume.Mounter, error) {
	return nil, fmt.Errorf("Mounter is not supported by this plugin")
}

// NewUnmounter 不支持卸载，直接报错以暴露误用。
func (plugin *mockVolumePlugin) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	return nil, fmt.Errorf("Unmounter is not supported by this plugin")
}

// Provisioner 接口实现

// NewProvisioner 在配置了置备调用时返回插件自身（它也实现 Provisioner），否则报错。
func (plugin *mockVolumePlugin) NewProvisioner(logger klog.Logger, options volume.VolumeOptions) (volume.Provisioner, error) {
	if len(plugin.provisionCalls) > 0 {
		// 插件自身即 Provisioner，省去额外桩类型
		logger.V(4).Info("Mock plugin NewProvisioner called, returning mock provisioner")
		plugin.provisionOptions = options
		return plugin, nil
	} else {
		return nil, fmt.Errorf("Mock plugin error: no provisionCalls configured")
	}
}

// Provision 按预置顺序返回置备结果：先校验参数，再构造最小 PV 或返回预置错误。
func (plugin *mockVolumePlugin) Provision(selectedNode *v1.Node, allowedTopologies []v1.TopologySelectorTerm) (*v1.PersistentVolume, error) {
	if len(plugin.provisionCalls) <= plugin.provisionCallCounter {
		return nil, fmt.Errorf("Mock plugin error: unexpected provisioner call %d", plugin.provisionCallCounter)
	}
	var pv *v1.PersistentVolume
	call := plugin.provisionCalls[plugin.provisionCallCounter]
	if !reflect.DeepEqual(call.expectedParameters, plugin.provisionOptions.Parameters) {
		klog.TODO().Error(nil, "Invalid provisioner call", "gotOptions", plugin.provisionOptions.Parameters, "expectedOptions", call.expectedParameters)
		return nil, fmt.Errorf("Mock plugin error: invalid provisioner call")
	}
	if call.ret == nil {
		// 构造带 GCE 卷源的 PV，与期望对象保持一致
		capacity := plugin.provisionOptions.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
		accessModes := plugin.provisionOptions.PVC.Spec.AccessModes
		pv = &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: plugin.provisionOptions.PVName,
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): capacity,
				},
				AccessModes:                   accessModes,
				PersistentVolumeReclaimPolicy: plugin.provisionOptions.PersistentVolumeReclaimPolicy,
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		}
		pv.Spec.VolumeMode = plugin.provisionOptions.PVC.Spec.VolumeMode
	}

	plugin.provisionCallCounter++
	klog.TODO().V(4).Info("Mock plugin Provision call nr", "provisionCallCounter", plugin.provisionCallCounter, "pv", klog.KObj(pv), "err", call.ret)
	return pv, call.ret
}

// Deleter 接口实现

// NewDeleter 在配置了删除调用时返回插件自身，否则报错。
func (plugin *mockVolumePlugin) NewDeleter(logger klog.Logger, spec *volume.Spec) (volume.Deleter, error) {
	if len(plugin.deleteCalls) > 0 {
		// 插件自身即 Deleter
		logger.V(4).Info("Mock plugin NewDeleter called, returning mock deleter")
		return plugin, nil
	} else {
		return nil, fmt.Errorf("Mock plugin error: no deleteCalls configured")
	}
}

// Delete 按预置顺序返回删除结果，并递增调用计数以校验调用次数。
func (plugin *mockVolumePlugin) Delete() error {
	if len(plugin.deleteCalls) <= plugin.deleteCallCounter {
		return fmt.Errorf("Mock plugin error: unexpected deleter call %d", plugin.deleteCallCounter)
	}
	ret := plugin.deleteCalls[plugin.deleteCallCounter]
	plugin.deleteCallCounter++
	klog.TODO().V(4).Info("Mock plugin Delete call nr", "deleteCallCounter", plugin.deleteCallCounter, "err", ret)
	return ret
}

// Volume 接口实现

// GetPath 返回空路径，测试不涉及真实挂载路径。
func (plugin *mockVolumePlugin) GetPath() string {
	return ""
}

// GetMetrics 返回空指标。
func (plugin *mockVolumePlugin) GetMetrics() (*volume.Metrics, error) {
	return nil, nil
}

// Recycler 接口实现

// Recycle 按预置顺序返回回收结果，并递增调用计数。
func (plugin *mockVolumePlugin) Recycle(pvName string, spec *volume.Spec, eventRecorder recyclerclient.RecycleEventRecorder) error {
	if len(plugin.recycleCalls) == 0 {
		return fmt.Errorf("Mock plugin error: no recycleCalls configured")
	}

	if len(plugin.recycleCalls) <= plugin.recycleCallCounter {
		return fmt.Errorf("Mock plugin error: unexpected recycle call %d", plugin.recycleCallCounter)
	}
	ret := plugin.recycleCalls[plugin.recycleCallCounter]
	plugin.recycleCallCounter++
	klog.TODO().V(4).Info("Mock plugin Recycle call nr", "recycleCallCounter", plugin.recycleCallCounter, "err", ret)
	return ret
}
