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

package disruption

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	apps "k8s.io/api/apps/v1"
	autoscalingapi "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/meta/testrestmapper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	discoveryfake "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	scalefake "k8s.io/client-go/scale/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/test/utils/ktesting"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

// pdbStates 是 PDB 键到状态快照的内存映射，充当 fake updater 的写入目标，
// 让状态断言无需依赖 API Server。
type pdbStates map[string]policy.PodDisruptionBudget

// alwaysReady 是恒为真的同步检查函数，用于跳过 informer 缓存等待，
// 使测试注入对象后可立即驱动 sync。
var alwaysReady = func() bool { return true }

// Set 将给定的 PDB 深拷贝后按 controller.KeyFunc 计算出的键存入 ps。
// 深拷贝是为了隔离快照，避免调用方后续修改原对象污染测试断言。
func (ps *pdbStates) Set(ctx context.Context, pdb *policy.PodDisruptionBudget) error {
	key, err := controller.KeyFunc(pdb)
	if err != nil {
		return err
	}
	(*ps)[key] = *pdb.DeepCopy()
	return nil
}

// Get 返回键对应的 PDB 快照；键不存在时返回零值，
// 便于断言“该 PDB 尚未被同步/写入”的场景。
func (ps *pdbStates) Get(key string) policy.PodDisruptionBudget {
	return (*ps)[key]
}

// VerifyPdbStatus 全面校验 PDB 状态：先移除 Conditions 后比较 DisruptionsAllowed、
// CurrentHealthy、DesiredHealthy、ExpectedPods、DisruptedPods 与 ObservedGeneration，
// 再单独检查 DisruptionAllowed 条件——允许中断时为 True，否则为 False。
func (ps *pdbStates) VerifyPdbStatus(t *testing.T, key string, disruptionsAllowed, currentHealthy, desiredHealthy, expectedPods int32, disruptedPodMap map[string]metav1.Time) {
	t.Helper()
	actualPDB := ps.Get(key)
	actualConditions := actualPDB.Status.Conditions
	actualPDB.Status.Conditions = nil
	expectedStatus := policy.PodDisruptionBudgetStatus{
		DisruptionsAllowed: disruptionsAllowed,
		CurrentHealthy:     currentHealthy,
		DesiredHealthy:     desiredHealthy,
		ExpectedPods:       expectedPods,
		DisruptedPods:      disruptedPodMap,
		ObservedGeneration: actualPDB.Generation,
	}
	actualStatus := actualPDB.Status
	if diff := cmp.Diff(expectedStatus, actualStatus, cmpopts.EquateEmpty()); diff != "" {
		t.Fatalf("PDB %q status mismatch (-want,+got):\n%s", key, diff)
	}

	cond := apimeta.FindStatusCondition(actualConditions, policy.DisruptionAllowedCondition)
	if cond == nil {
		t.Fatalf("Expected condition %q, but didn't find it", policy.DisruptionAllowedCondition)
	}
	if disruptionsAllowed > 0 {
		if cond.Status != metav1.ConditionTrue {
			t.Fatalf("Expected condition %q to have status %q, but was %q",
				policy.DisruptionAllowedCondition, metav1.ConditionTrue, cond.Status)
		}
	} else {
		if cond.Status != metav1.ConditionFalse {
			t.Fatalf("Expected condition %q to have status %q, but was %q",
				policy.DisruptionAllowedCondition, metav1.ConditionFalse, cond.Status)
		}
	}
}

// VerifyDisruptionAllowed 只校验 PDB 的 DisruptionsAllowed 字段。
// 失败时打印调用栈，便于定位是哪次 sync 写入了错误值。
func (ps *pdbStates) VerifyDisruptionAllowed(t *testing.T, key string, disruptionsAllowed int32) {
	pdb := ps.Get(key)
	if pdb.Status.DisruptionsAllowed != disruptionsAllowed {
		debug.PrintStack()
		t.Fatalf("PodDisruptionAllowed mismatch for PDB %q.  Expected %v but got %v.", key, disruptionsAllowed, pdb.Status.DisruptionsAllowed)
	}
}

// VerifyNoStatusError 确认 PDB 状态中不存在因“未托管 pod（找不到 controller ref）”
// 而产生的 SyncFailed 条件，用于验证控制器不会把无属主 pod 当作同步错误。
func (ps *pdbStates) VerifyNoStatusError(t *testing.T, key string) {
	pdb := ps.Get(key)
	for _, condition := range pdb.Status.Conditions {
		if strings.Contains(condition.Message, "found no controller ref") && condition.Reason == policy.SyncFailedReason {
			t.Fatalf("PodDisruption Controller should not error when unmanaged pods are found but it failed for %q", key)
		}
	}
}

// disruptionController 在真实控制器之上暴露 fake 客户端与各对象 store，
// 便于测试直接注入 pod/PDB/工作负载并观察事件与状态。
type disruptionController struct {
	*DisruptionController

	podStore cache.Store
	pdbStore cache.Store
	rcStore  cache.Store
	rsStore  cache.Store
	dStore   cache.Store
	ssStore  cache.Store

	coreClient      *fake.Clientset
	scaleClient     *scalefake.FakeScaleClient
	discoveryClient *discoveryfake.FakeDiscovery
	informerFactory informers.SharedInformerFactory
}

// customGVK 模拟一个不受内建 finder 识别的自定义控制器 GVK，
// 用于验证“属主为未知控制器”的 pod 会让期望规模计算失败。
var customGVK = schema.GroupVersionKind{
	Group:   "custom.k8s.io",
	Version: "v1",
	Kind:    "customresource",
}

// newFakeDisruptionController 以当前时间构造测试用控制器，
// 是 newFakeDisruptionControllerWithTime 的便捷封装。
func newFakeDisruptionController(ctx context.Context) (*disruptionController, *pdbStates) {
	return newFakeDisruptionControllerWithTime(ctx, time.Now())
}

// newFakeDisruptionControllerWithTime 搭建完整的假控制器：fake 客户端与 informer、
// fake scale/discovery 客户端、可控时钟，并把状态写入替换成内存 pdbStates、
// 把 informer 同步检查置为恒真，从而让测试能直接向 store 注入对象并驱动 sync。
func newFakeDisruptionControllerWithTime(ctx context.Context, now time.Time) (*disruptionController, *pdbStates) {
	ps := &pdbStates{}

	coreClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(coreClient, controller.NoResyncPeriodFunc())

	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(customGVK, &v1.Service{})
	fakeScaleClient := &scalefake.FakeScaleClient{}
	fakeDiscovery := &discoveryfake.FakeDiscovery{
		Fake: &core.Fake{},
	}
	// 使用 fake clock，测试可精确推进逻辑时间而不必真实等待，便于验证超时与陈旧判定。
	fakeClock := clocktesting.NewFakeClock(now)

	dc := NewDisruptionControllerInternal(
		ctx,
		informerFactory.Core().V1().Pods(),
		informerFactory.Policy().V1().PodDisruptionBudgets(),
		informerFactory.Core().V1().ReplicationControllers(),
		informerFactory.Apps().V1().ReplicaSets(),
		informerFactory.Apps().V1().Deployments(),
		informerFactory.Apps().V1().StatefulSets(),
		coreClient,
		testrestmapper.TestOnlyStaticRESTMapper(scheme),
		fakeScaleClient,
		fakeDiscovery,
		fakeClock,
		stalePodDisruptionTimeout,
	)
	// 将状态写入替换为内存 pdbStates，使断言不依赖 API Server 的 ResourceVersion 冲突处理。
	dc.getUpdater = func() updater { return ps.Set }
	// informer 同步检查置为恒真，测试可直接向 store 注入对象后立即 sync，无需等待事件回调。
	dc.podListerSynced = alwaysReady
	dc.pdbListerSynced = alwaysReady
	dc.rcListerSynced = alwaysReady
	dc.rsListerSynced = alwaysReady
	dc.dListerSynced = alwaysReady
	dc.ssListerSynced = alwaysReady
	dc.recorder = record.NewFakeRecorder(100)
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	return &disruptionController{
		dc,
		informerFactory.Core().V1().Pods().Informer().GetStore(),
		informerFactory.Policy().V1().PodDisruptionBudgets().Informer().GetStore(),
		informerFactory.Core().V1().ReplicationControllers().Informer().GetStore(),
		informerFactory.Apps().V1().ReplicaSets().Informer().GetStore(),
		informerFactory.Apps().V1().Deployments().Informer().GetStore(),
		informerFactory.Apps().V1().StatefulSets().Informer().GetStore(),
		coreClient,
		fakeScaleClient,
		fakeDiscovery,
		informerFactory,
	}, ps
}

// fooBar 返回测试用的 {"foo": "bar"} 标签集，作为多数 fixture 的默认标签。
func fooBar() map[string]string {
	return map[string]string{"foo": "bar"}
}

// newSel 由给定的标签 map 构造 MatchLabels 类型的 LabelSelector。
func newSel(labels map[string]string) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: labels}
}

// newSelFooBar 返回匹配 foo=bar 的 LabelSelector，是 PDB 与工作负载 fixture 的默认选择器。
func newSelFooBar() *metav1.LabelSelector {
	return newSel(map[string]string{"foo": "bar"})
}

// newMinAvailablePodDisruptionBudget 构造指定 minAvailable、选择器为 foo=bar 的 PDB，
// 并返回其 controller 键；UID 与 ResourceVersion 固定以便结果可复现。
func newMinAvailablePodDisruptionBudget(t *testing.T, minAvailable intstr.IntOrString) (*policy.PodDisruptionBudget, string) {

	pdb := &policy.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			UID:             uuid.NewUUID(),
			Name:            "foobar",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "18",
		},
		Spec: policy.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     newSelFooBar(),
		},
	}

	pdbName, err := controller.KeyFunc(pdb)
	if err != nil {
		t.Fatalf("Unexpected error naming pdb %q: %v", pdb.Name, err)
	}

	return pdb, pdbName
}

// newMaxUnavailablePodDisruptionBudget 构造指定 maxUnavailable、选择器为 foo=bar 的 PDB，
// 并返回其 controller 键；用于验证按“不可中断上限”计算的场景。
func newMaxUnavailablePodDisruptionBudget(t *testing.T, maxUnavailable intstr.IntOrString) (*policy.PodDisruptionBudget, string) {
	pdb := &policy.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			UID:             uuid.NewUUID(),
			Name:            "foobar",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "18",
		},
		Spec: policy.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector:       newSelFooBar(),
		},
	}

	pdbName, err := controller.KeyFunc(pdb)
	if err != nil {
		t.Fatalf("Unexpected error naming pdb %q: %v", pdb.Name, err)
	}

	return pdb, pdbName
}

// updatePodOwnerToRc 将 pod 的控制器属主引用指向给定 RC，
// 使控制器能据此反查 RC 的规模。
func updatePodOwnerToRc(t *testing.T, pod *v1.Pod, rc *v1.ReplicationController) {
	var controllerReference metav1.OwnerReference
	var trueVar = true
	controllerReference = metav1.OwnerReference{UID: rc.UID, APIVersion: controllerKindRC.GroupVersion().String(), Kind: controllerKindRC.Kind, Name: rc.Name, Controller: &trueVar}
	pod.OwnerReferences = append(pod.OwnerReferences, controllerReference)
}

// updatePodOwnerToRs 将 pod 的控制器属主引用指向给定 ReplicaSet，
// 使控制器能据此反查 RS 的规模。
func updatePodOwnerToRs(t *testing.T, pod *v1.Pod, rs *apps.ReplicaSet) {
	var controllerReference metav1.OwnerReference
	var trueVar = true
	controllerReference = metav1.OwnerReference{UID: rs.UID, APIVersion: controllerKindRS.GroupVersion().String(), Kind: controllerKindRS.Kind, Name: rs.Name, Controller: &trueVar}
	pod.OwnerReferences = append(pod.OwnerReferences, controllerReference)
}

// updatePodOwnerToSs 将 pod 的控制器属主引用指向给定 StatefulSet，
// 使控制器能据此反查 SS 的规模。
func updatePodOwnerToSs(t *testing.T, pod *v1.Pod, ss *apps.StatefulSet) {
	var controllerReference metav1.OwnerReference
	var trueVar = true
	controllerReference = metav1.OwnerReference{UID: ss.UID, APIVersion: controllerKindSS.GroupVersion().String(), Kind: controllerKindSS.Kind, Name: ss.Name, Controller: &trueVar}
	pod.OwnerReferences = append(pod.OwnerReferences, controllerReference)
}

// newPod 构造一个带 foo=bar 标签且 PodReady=True 的 pod 并返回其 controller 键；
// 默认视为健康/可用，测试可通过清空 Conditions 使其变为不可用。
func newPod(t *testing.T, name string) (*v1.Pod, string) {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			UID:             uuid.NewUUID(),
			Annotations:     make(map[string]string),
			Name:            name,
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "18",
			Labels:          fooBar(),
		},
		Spec: v1.PodSpec{},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{Type: v1.PodReady, Status: v1.ConditionTrue},
			},
		},
	}

	podName, err := controller.KeyFunc(pod)
	if err != nil {
		t.Fatalf("Unexpected error naming pod %q: %v", pod.Name, err)
	}

	return pod, podName
}

// newReplicationController 构造指定副本数、选择器为 foo=bar 的 RC fixture 并返回其键。
func newReplicationController(t *testing.T, size int32) (*v1.ReplicationController, string) {
	rc := &v1.ReplicationController{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			UID:             uuid.NewUUID(),
			Name:            "foobar",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "18",
			Labels:          fooBar(),
		},
		Spec: v1.ReplicationControllerSpec{
			Replicas: &size,
			Selector: fooBar(),
		},
	}

	rcName, err := controller.KeyFunc(rc)
	if err != nil {
		t.Fatalf("Unexpected error naming RC %q", rc.Name)
	}

	return rc, rcName
}

// newDeployment 构造指定副本数、选择器为 foo=bar 的 Deployment fixture 并返回其键。
func newDeployment(t *testing.T, size int32) (*apps.Deployment, string) {
	d := &apps.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			UID:             uuid.NewUUID(),
			Name:            "foobar",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "18",
			Labels:          fooBar(),
		},
		Spec: apps.DeploymentSpec{
			Replicas: &size,
			Selector: newSelFooBar(),
		},
	}

	dName, err := controller.KeyFunc(d)
	if err != nil {
		t.Fatalf("Unexpected error naming Deployment %q: %v", d.Name, err)
	}

	return d, dName
}

// newReplicaSet 构造指定副本数、选择器为 foo=bar 的 ReplicaSet fixture 并返回其键。
func newReplicaSet(t *testing.T, size int32) (*apps.ReplicaSet, string) {
	rs := &apps.ReplicaSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			UID:             uuid.NewUUID(),
			Name:            "foobar",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "18",
			Labels:          fooBar(),
		},
		Spec: apps.ReplicaSetSpec{
			Replicas: &size,
			Selector: newSelFooBar(),
		},
	}

	rsName, err := controller.KeyFunc(rs)
	if err != nil {
		t.Fatalf("Unexpected error naming ReplicaSet %q: %v", rs.Name, err)
	}

	return rs, rsName
}

// newStatefulSet 构造指定副本数、选择器为 foo=bar 的 StatefulSet fixture 并返回其键。
func newStatefulSet(t *testing.T, size int32) (*apps.StatefulSet, string) {
	ss := &apps.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			UID:             uuid.NewUUID(),
			Name:            "foobar",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "18",
			Labels:          fooBar(),
		},
		Spec: apps.StatefulSetSpec{
			Replicas: &size,
			Selector: newSelFooBar(),
		},
	}

	ssName, err := controller.KeyFunc(ss)
	if err != nil {
		t.Fatalf("Unexpected error naming StatefulSet %q: %v", ss.Name, err)
	}

	return ss, ssName
}

// update 将对象写入 cache.Store 并断言成功，用于模拟 informer 收到更新事件。
func update(t *testing.T, store cache.Store, obj interface{}) {
	if err := store.Update(obj); err != nil {
		t.Fatalf("Could not add %+v to %+v: %v", obj, store, err)
	}
}

// add 将对象加入 cache.Store 并断言成功，用于模拟 informer 收到新增事件。
func add(t *testing.T, store cache.Store, obj interface{}) {
	if err := store.Add(obj); err != nil {
		t.Fatalf("Could not add %+v to %+v: %v", obj, store, err)
	}
}

// TestNoSelector 验证空选择器（匹配所有 pod）的 PDB：
// 起初无 pod 时不允许中断；加入一个匹配 pod 后 currentHealthy 与 expectedPods 递增，
// 但因未达到 minAvailable=3 仍禁止中断。
func TestNoSelector(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromInt32(3))
	pdb.Spec.Selector = &metav1.LabelSelector{}
	pod, _ := newPod(t, "yo-yo-yo")

	add(t, dc.pdbStore, pdb)
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 0, 0, 3, 0, map[string]metav1.Time{})

	add(t, dc.podStore, pod)
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 0, 1, 3, 1, map[string]metav1.Time{})
}

// TestUnavailable 验证可用/期望计数随 pod 增加而逐次上升，
// 并在某个 pod 变为不可用（清空 Ready 条件）后 currentHealthy 下降、
// DisruptionsAllowed 从 1 回落为 0。
func TestUnavailable(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromInt32(3))
	add(t, dc.pdbStore, pdb)
	dc.sync(ctx, pdbName)

	// 逐个加入 4 个 pod，验证每次 sync 后计数逐步上升。
	pods := []*v1.Pod{}
	for i := int32(0); i < 4; i++ {
		ps.VerifyPdbStatus(t, pdbName, 0, i, 3, i, map[string]metav1.Time{})
		pod, _ := newPod(t, fmt.Sprintf("yo-yo-yo %d", i))
		pods = append(pods, pod)
		add(t, dc.podStore, pod)
		dc.sync(ctx, pdbName)
	}
	ps.VerifyPdbStatus(t, pdbName, 1, 4, 3, 4, map[string]metav1.Time{})

	// 清空首个 pod 的 Ready 条件，模拟其变为不可用。
	pods[0].Status.Conditions = []v1.PodCondition{}
	update(t, dc.podStore, pods[0])
	dc.sync(ctx, pdbName)

	// expectedPods 不变，但 currentHealthy 减一，导致不再允许中断。
	ps.VerifyPdbStatus(t, pdbName, 0, 3, 3, 4, map[string]metav1.Time{})
}

// TestIntegerMaxUnavailable 验证整数型 maxUnavailable 对无控制器 pod 不生效：
// 零 pod 与裸 pod 场景均不允许中断，并发出 UnmanagedPods 事件。
func TestIntegerMaxUnavailable(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMaxUnavailablePodDisruptionBudget(t, intstr.FromInt32(1))
	add(t, dc.pdbStore, pdb)
	dc.sync(ctx, pdbName)
	// 先验证 PDB 匹配 0 个 pod 时不允许中断。
	ps.VerifyDisruptionAllowed(t, pdbName, 0)

	pod, _ := newPod(t, "naked")
	add(t, dc.podStore, pod)
	dc.sync(ctx, pdbName)

	ps.VerifyDisruptionAllowed(t, pdbName, 0)
	verifyEventEmitted(t, dc, "UnmanagedPods")

}

// TestIntegerMaxUnavailableWithScaling 验证整数型 maxUnavailable 会随被选 pod 控制器
// （ReplicaSet）副本数变化重算：副本数从 7 降到 5 后，desiredHealthy 与
// expectedPods 相应更新，DisruptionsAllowed 保持为 0。
func TestIntegerMaxUnavailableWithScaling(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMaxUnavailablePodDisruptionBudget(t, intstr.FromInt32(2))
	add(t, dc.pdbStore, pdb)

	rs, _ := newReplicaSet(t, 7)
	add(t, dc.rsStore, rs)

	pod, _ := newPod(t, "pod")
	updatePodOwnerToRs(t, pod, rs)
	add(t, dc.podStore, pod)
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 0, 1, 5, 7, map[string]metav1.Time{})

	// 修改 ReplicaSet 副本数，验证 PDB 会依据控制器规模重算。
	rs.Spec.Replicas = ptr.To[int32](5)
	update(t, dc.rsStore, rs)

	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 0, 1, 3, 5, map[string]metav1.Time{})
}

// TestPercentageMaxUnavailableWithScaling 验证百分比 maxUnavailable（30%）随控制器
// 副本数变化重算：副本数从 7 降到 3 后，期望规模与阈值按百分比重新计算。
func TestPercentageMaxUnavailableWithScaling(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMaxUnavailablePodDisruptionBudget(t, intstr.FromString("30%"))
	add(t, dc.pdbStore, pdb)

	rs, _ := newReplicaSet(t, 7)
	add(t, dc.rsStore, rs)

	pod, _ := newPod(t, "pod")
	updatePodOwnerToRs(t, pod, rs)
	add(t, dc.podStore, pod)
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 0, 1, 4, 7, map[string]metav1.Time{})

	// 修改 ReplicaSet 副本数，验证 PDB 会依据控制器规模重算。
	rs.Spec.Replicas = ptr.To[int32](3)
	update(t, dc.rsStore, rs)

	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 0, 1, 2, 3, map[string]metav1.Time{})
}

// TestNakedPod 验证百分比 minAvailable 的 PDB 遇到无控制器（裸）pod 时不允许中断，
// 并发出 UnmanagedPods 事件。
func TestNakedPod(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromString("28%"))
	add(t, dc.pdbStore, pdb)
	dc.sync(ctx, pdbName)
	// 先验证 PDB 匹配 0 个 pod 时不允许中断。
	ps.VerifyDisruptionAllowed(t, pdbName, 0)

	pod, _ := newPod(t, "naked")
	add(t, dc.podStore, pod)
	dc.sync(ctx, pdbName)

	ps.VerifyDisruptionAllowed(t, pdbName, 0)
	verifyEventEmitted(t, dc, "UnmanagedPods")
}

// TestUnsupportedControllerPod 验证 pod 属主为不受支持的自定义控制器时无法计算期望规模，
// PDB 保持不允许中断，并发出 CalculateExpectedPodCountFailed 事件。
func TestUnsupportedControllerPod(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromString("28%"))
	add(t, dc.pdbStore, pdb)
	dc.sync(ctx, pdbName)
	// 先验证 PDB 匹配 0 个 pod 时不允许中断。
	ps.VerifyDisruptionAllowed(t, pdbName, 0)

	pod, _ := newPod(t, "naked")
	isController := true
	pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{
		APIVersion: "apps.test.io/v1",
		Kind:       "TestWorkload",
		Name:       "fake-controller",
		UID:        "b7329742-8daa-493a-8881-6ca07139172b",
		Controller: &isController,
	})

	add(t, dc.podStore, pod)
	dc.sync(ctx, pdbName)

	ps.VerifyDisruptionAllowed(t, pdbName, 0)
	verifyEventEmitted(t, dc, "CalculateExpectedPodCountFailed")
}

// TestStatusForUnmanagedPod 验证发现未托管 pod 时控制器不会写入
// SyncFailed/“found no controller ref” 错误状态，仅记录 UnmanagedPods 事件。
func TestStatusForUnmanagedPod(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromString("28%"))
	add(t, dc.pdbStore, pdb)
	dc.sync(ctx, pdbName)
	// 先验证 PDB 匹配 0 个 pod 时不允许中断。
	ps.VerifyDisruptionAllowed(t, pdbName, 0)

	pod, _ := newPod(t, "unmanaged")
	add(t, dc.podStore, pod)
	dc.sync(ctx, pdbName)
	ps.VerifyNoStatusError(t, pdbName)
	verifyEventEmitted(t, dc, "UnmanagedPods")
}

// TestTotalUnmanagedPods 验证 getExpectedScale 能正确把无控制器 pod 计入
// unmanagedPods 列表（恰好 1 个），且不产生状态错误。
func TestTotalUnmanagedPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromString("28%"))
	add(t, dc.pdbStore, pdb)
	dc.sync(ctx, pdbName)
	// 先验证 PDB 匹配 0 个 pod 时不允许中断。
	ps.VerifyDisruptionAllowed(t, pdbName, 0)

	pod, _ := newPod(t, "unmanaged")
	add(t, dc.podStore, pod)
	dc.sync(ctx, pdbName)
	var pods []*v1.Pod
	pods = append(pods, pod)
	_, unmanagedPods, _ := dc.getExpectedScale(ctx, pods)
	if len(unmanagedPods) != 1 {
		t.Fatalf("expected one pod to be unmanaged pod but found %d", len(unmanagedPods))
	}
	ps.VerifyNoStatusError(t, pdbName)
	verifyEventEmitted(t, dc, "UnmanagedPods")
}

// TestReplicaSet 验证即使 ReplicaSet 没有 Deployment，控制器仍按其规模（10）
// 计算期望 pod 数，并把 20% 的 minAvailable 向上取整为 2。
func TestReplicaSet(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromString("20%"))
	add(t, dc.pdbStore, pdb)

	rs, _ := newReplicaSet(t, 10)
	add(t, dc.rsStore, rs)
	pod, _ := newPod(t, "pod")
	updatePodOwnerToRs(t, pod, rs)
	add(t, dc.podStore, pod)
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 0, 1, 2, 10, map[string]metav1.Time{})
}

// TestScaleResource 验证通过 scale 子资源获取自定义控制器规模的场景：
// reactor 模拟 get 返回 replicas=10，结合 maxUnavailable=5 与 4 个 pod，
// 计算允许的中断数并校验状态。
func TestScaleResource(t *testing.T) {
	customResourceUID := uuid.NewUUID()
	replicas := int32(10)
	pods := int32(4)
	maxUnavailable := int32(5)

	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	// 自定义资源没有内建 scale 控制器，需用 reactor 模拟 scale 子资源返回。
	dc.scaleClient.AddReactor("get", "customresources", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		obj := &autoscalingapi.Scale{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: metav1.NamespaceDefault,
				UID:       customResourceUID,
			},
			Spec: autoscalingapi.ScaleSpec{
				Replicas: replicas,
			},
		}
		return true, obj, nil
	})

	pdb, pdbName := newMaxUnavailablePodDisruptionBudget(t, intstr.FromInt32(maxUnavailable))
	add(t, dc.pdbStore, pdb)

	trueVal := true
	for i := 0; i < int(pods); i++ {
		pod, _ := newPod(t, fmt.Sprintf("pod-%d", i))
		pod.SetOwnerReferences([]metav1.OwnerReference{
			{
				Kind:       customGVK.Kind,
				APIVersion: customGVK.GroupVersion().String(),
				Controller: &trueVal,
				UID:        customResourceUID,
			},
		})
		add(t, dc.podStore, pod)
	}
	dc.sync(ctx, pdbName)
	disruptionsAllowed := int32(0)
	if replicas-pods < maxUnavailable {
		disruptionsAllowed = maxUnavailable - (replicas - pods)
	}
	ps.VerifyPdbStatus(t, pdbName, disruptionsAllowed, pods, replicas-maxUnavailable, replicas, map[string]metav1.Time{})
}

// TestScaleFinderNoResource 表驱动验证 getScaleController 对 discovery 中 scale 子资源
// 声明的处理：正确声明 scale 子资源时成功；仅声明同 Kind 普通资源或完全无 scale 时报错。
func TestScaleFinderNoResource(t *testing.T) {
	resourceName := "customresources"
	testCases := map[string]struct {
		apiResources []metav1.APIResource
		expectError  bool
	}{
		"resource implements scale": {
			apiResources: []metav1.APIResource{
				{
					Kind: customGVK.Kind,
					Name: resourceName + "/status",
				},
				{
					Kind:    "Scale",
					Group:   autoscalingapi.GroupName,
					Version: "v1",
					Name:    resourceName + "/scale",
				},
				{
					Kind: customGVK.Kind,
					Name: resourceName,
				},
			},
			expectError: false,
		},
		"resource implements unsupported data format for scale subresource": {
			apiResources: []metav1.APIResource{
				{
					Kind: customGVK.Kind,
					Name: resourceName,
				},
				{
					Kind: customGVK.Kind,
					Name: resourceName + "/scale",
				},
			},
			expectError: true,
		},
		"resource does not implement scale": {
			apiResources: []metav1.APIResource{
				{
					Kind: customGVK.Kind,
					Name: resourceName,
				},
			},
			expectError: true,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			customResourceUID := uuid.NewUUID()

			_, ctx := ktesting.NewTestContext(t)
			dc, _ := newFakeDisruptionController(ctx)

			// 让 get 返回 NotFound，迫使 getScaleController 依赖 discovery 判定能否找到 scale。
			dc.scaleClient.AddReactor("get", resourceName, func(action core.Action) (handled bool, ret runtime.Object, err error) {
				gr := schema.GroupResource{
					Group:    customGVK.Group,
					Resource: resourceName,
				}
				return true, nil, errors.NewNotFound(gr, "name")
			})
			// discovery 中的 scale 子资源声明是各用例能否成功的关键差异。
			dc.discoveryClient.Resources = []*metav1.APIResourceList{
				{
					GroupVersion: customGVK.GroupVersion().String(),
					APIResources: tc.apiResources,
				},
			}

			trueVal := true
			ownerRef := &metav1.OwnerReference{
				Kind:       customGVK.Kind,
				APIVersion: customGVK.GroupVersion().String(),
				Controller: &trueVal,
				UID:        customResourceUID,
			}

			_, err := dc.getScaleController(ctx, ownerRef, "default")

			if tc.expectError && err == nil {
				t.Error("expected error, but didn't get one")
			}

			if !tc.expectError && err != nil {
				t.Errorf("did not expect error, but got %v", err)
			}
		})
	}
}

// TestMultipleControllers 验证多个控制器不会让 PDB 被误判为可中断：
// 无控制器时禁止；单一 RC 时允许；出现两个 RC 时应禁止（历史断言已注释并保留 TODO）。
func TestMultipleControllers(t *testing.T) {
	const podCount = 2
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromString("1%"))
	add(t, dc.pdbStore, pdb)

	pods := []*v1.Pod{}
	for i := 0; i < podCount; i++ {
		pod, _ := newPod(t, fmt.Sprintf("pod %d", i))
		pods = append(pods, pod)
		add(t, dc.podStore, pod)
	}
	dc.sync(ctx, pdbName)

	// 此时 pod 尚无控制器，无法计算期望规模，因此不允许中断。
	ps.VerifyDisruptionAllowed(t, pdbName, 0)

	rc, _ := newReplicationController(t, 1)
	rc.Name = "rc 1"
	for i := 0; i < podCount; i++ {
		updatePodOwnerToRc(t, pods[i], rc)
	}
	add(t, dc.rcStore, rc)
	dc.sync(ctx, pdbName)
	// 单一 RC 下健康比例 200% 高于 1% 阈值，允许中断。
	ps.VerifyDisruptionAllowed(t, pdbName, 1)

	rc, _ = newReplicationController(t, 1)
	rc.Name = "rc 2"
	for i := 0; i < podCount; i++ {
		updatePodOwnerToRc(t, pods[i], rc)
	}
	add(t, dc.rcStore, rc)
	dc.sync(ctx, pdbName)

	// 健康比例 100% 仍高于 1%，但存在两个 RC（历史语义应禁止中断）。
	// TODO: 确认此断言是否仍然需要
	//ps.VerifyDisruptionAllowed(t, pdbName, 0)
}

// TestReplicationController 验证按 RC 规模计算 PDB：34% 向上取整为 2 个最小健康 pod，
// 随 3 个 pod 加入逐次校验状态；最后加入一个仅匹配标签但无控制器的 rogue pod，
// 验证允许中断数仍按受控 pod 计算。
func TestReplicationController(t *testing.T) {
	// 本测试的 PDB 匹配 foo=bar，而 RC 及其 pod 匹配 {foo=bar, baz=quux}。
	// 之后加入一个仅带 foo=bar 标签的 rogue pod：它匹配 PDB 但没有控制器，
	// 用于验证控制器不会错误地把无控制器 pod 当作同步失败。
	labels := map[string]string{
		"foo": "bar",
		"baz": "quux",
	}
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	// 34% 的 3 个副本应向上取整为 2 个最小健康 pod。
	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromString("34%"))
	add(t, dc.pdbStore, pdb)
	rc, _ := newReplicationController(t, 3)
	rc.Spec.Selector = labels
	add(t, dc.rcStore, rc)
	dc.sync(ctx, pdbName)

	// 起始 expectedPods 为 0：没有 pod 时 PDB 无法感知 RC 的存在。
	// 这是已知问题。TODO(mml): 提交 issue 跟踪。
	ps.VerifyPdbStatus(t, pdbName, 0, 0, 0, 0, map[string]metav1.Time{})

	for i := int32(0); i < 3; i++ {
		pod, _ := newPod(t, fmt.Sprintf("foobar %d", i))
		updatePodOwnerToRc(t, pod, rc)
		pod.Labels = labels
		add(t, dc.podStore, pod)
		dc.sync(ctx, pdbName)
		if i < 2 {
			ps.VerifyPdbStatus(t, pdbName, 0, i+1, 2, 3, map[string]metav1.Time{})
		} else {
			ps.VerifyPdbStatus(t, pdbName, 1, 3, 2, 3, map[string]metav1.Time{})
		}
	}

	rogue, _ := newPod(t, "rogue")
	add(t, dc.podStore, rogue)
	dc.sync(ctx, pdbName)
	ps.VerifyDisruptionAllowed(t, pdbName, 2)
}

// TestStatefulSetController 验证 StatefulSet 作为控制器时与 RC 行为一致：
// 34% 阈值向上取整为 2，随 pod 加入 currentHealthy 递增，达到阈值后允许中断。
func TestStatefulSetController(t *testing.T) {
	labels := map[string]string{
		"foo": "bar",
		"baz": "quux",
	}

	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	// 34% 的 3 个副本应向上取整为 2 个最小健康 pod。
	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromString("34%"))
	add(t, dc.pdbStore, pdb)
	ss, _ := newStatefulSet(t, 3)
	add(t, dc.ssStore, ss)
	dc.sync(ctx, pdbName)

	// 起始 expectedPods 为 0：没有 pod 时 PDB 无法感知 SS 的存在。
	// 这是已知问题。TODO(mml): 提交 issue 跟踪。
	ps.VerifyPdbStatus(t, pdbName, 0, 0, 0, 0, map[string]metav1.Time{})

	for i := int32(0); i < 3; i++ {
		pod, _ := newPod(t, fmt.Sprintf("foobar %d", i))
		updatePodOwnerToSs(t, pod, ss)
		pod.Labels = labels
		add(t, dc.podStore, pod)
		dc.sync(ctx, pdbName)
		if i < 2 {
			ps.VerifyPdbStatus(t, pdbName, 0, i+1, 2, 3, map[string]metav1.Time{})
		} else {
			ps.VerifyPdbStatus(t, pdbName, 1, 3, 2, 3, map[string]metav1.Time{})
		}
	}
}

// TestTwoControllers 验证两个控制器共同被 PDB 选中时的期望规模与阈值合并计算：
// 先仅为 RC 创建 pod 验证中间态，再加入属于 RS 的 pod，
// 最后通过逐个切换 pod 的 Ready 状态验证中断允许数的增减边界。
func TestTwoControllers(t *testing.T) {
	// 本测试的重点是在定义三个控制器并创建 pod 的过程中校验各种中间状态。
	rcLabels := map[string]string{
		"foo": "bar",
		"baz": "quux",
	}
	dLabels := map[string]string{
		"foo": "bar",
		"baz": "quuux",
	}
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	// 这些常量相互关联，故不在代码中动态计算正确值；修改任一参数都需重新推算其余值。
	// 后续循环直接依赖这些常量，保持显式取值比引入计算逻辑更易读。
	const collectionSize int32 = 11 // 每个控制器管理的集合大小
	const minimumOne int32 = 4      // 单一控制器时的整数型最小值
	const minimumTwo int32 = 7      // 两个控制器时的整数型最小值

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromString("28%"))
	add(t, dc.pdbStore, pdb)
	rc, _ := newReplicationController(t, collectionSize)
	rc.Spec.Selector = rcLabels
	add(t, dc.rcStore, rc)
	dc.sync(ctx, pdbName)

	ps.VerifyPdbStatus(t, pdbName, 0, 0, 0, 0, map[string]metav1.Time{})

	pods := []*v1.Pod{}

	unavailablePods := collectionSize - minimumOne - 1
	for i := int32(1); i <= collectionSize; i++ {
		pod, _ := newPod(t, fmt.Sprintf("quux %d", i))
		updatePodOwnerToRc(t, pod, rc)
		pods = append(pods, pod)
		pod.Labels = rcLabels
		if i <= unavailablePods {
			pod.Status.Conditions = []v1.PodCondition{}
		}
		add(t, dc.podStore, pod)
		dc.sync(ctx, pdbName)
		if i <= unavailablePods {
			ps.VerifyPdbStatus(t, pdbName, 0, 0, minimumOne, collectionSize, map[string]metav1.Time{})
		} else if i-unavailablePods <= minimumOne {
			ps.VerifyPdbStatus(t, pdbName, 0, i-unavailablePods, minimumOne, collectionSize, map[string]metav1.Time{})
		} else {
			ps.VerifyPdbStatus(t, pdbName, 1, i-unavailablePods, minimumOne, collectionSize, map[string]metav1.Time{})
		}
	}

	d, _ := newDeployment(t, collectionSize)
	d.Spec.Selector = newSel(dLabels)
	add(t, dc.dStore, d)
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 1, minimumOne+1, minimumOne, collectionSize, map[string]metav1.Time{})

	rs, _ := newReplicaSet(t, collectionSize)
	rs.Spec.Selector = newSel(dLabels)
	rs.Labels = dLabels
	add(t, dc.rsStore, rs)
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 1, minimumOne+1, minimumOne, collectionSize, map[string]metav1.Time{})

	// 循环结束时健康 pod 数应为 N+2（即 minimumTwo+2），据此推导不可用的 pod 数量。
	unavailablePods = 2*collectionSize - (minimumTwo + 2) - unavailablePods
	for i := int32(1); i <= collectionSize; i++ {
		pod, _ := newPod(t, fmt.Sprintf("quuux %d", i))
		updatePodOwnerToRs(t, pod, rs)
		pods = append(pods, pod)
		pod.Labels = dLabels
		if i <= unavailablePods {
			pod.Status.Conditions = []v1.PodCondition{}
		}
		add(t, dc.podStore, pod)
		dc.sync(ctx, pdbName)
		if i <= unavailablePods {
			ps.VerifyPdbStatus(t, pdbName, 0, minimumOne+1, minimumTwo, 2*collectionSize, map[string]metav1.Time{})
		} else if i-unavailablePods <= minimumTwo-(minimumOne+1) {
			ps.VerifyPdbStatus(t, pdbName, 0, (minimumOne+1)+(i-unavailablePods), minimumTwo, 2*collectionSize, map[string]metav1.Time{})
		} else {
			ps.VerifyPdbStatus(t, pdbName, i-unavailablePods-(minimumTwo-(minimumOne+1)),
				(minimumOne+1)+(i-unavailablePods), minimumTwo, 2*collectionSize, map[string]metav1.Time{})
		}
	}

	// 验证边界：停用 1 个 pod 仍允许中断，停用 2 个则不允许；
	// 再把其中一个恢复为 Ready 后，中断允许数应恢复。
	ps.VerifyPdbStatus(t, pdbName, 2, 2+minimumTwo, minimumTwo, 2*collectionSize, map[string]metav1.Time{})
	pods[collectionSize-1].Status.Conditions = []v1.PodCondition{}
	update(t, dc.podStore, pods[collectionSize-1])
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 1, 1+minimumTwo, minimumTwo, 2*collectionSize, map[string]metav1.Time{})

	pods[collectionSize-2].Status.Conditions = []v1.PodCondition{}
	update(t, dc.podStore, pods[collectionSize-2])
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 0, minimumTwo, minimumTwo, 2*collectionSize, map[string]metav1.Time{})

	pods[collectionSize-1].Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	update(t, dc.podStore, pods[collectionSize-1])
	dc.sync(ctx, pdbName)
	ps.VerifyPdbStatus(t, pdbName, 1, 1+minimumTwo, minimumTwo, 2*collectionSize, map[string]metav1.Time{})
}

// TestPDBNotExist 验证 sync 一个缓存中不存在的 PDB 键时返回 nil 而非报错。
func TestPDBNotExist(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, _ := newFakeDisruptionController(ctx)
	pdb, _ := newMinAvailablePodDisruptionBudget(t, intstr.FromString("67%"))
	add(t, dc.pdbStore, pdb)
	if err := dc.sync(ctx, "notExist"); err != nil {
		t.Errorf("Unexpected error: %v, expect nil", err)
	}
}

// TestUpdateDisruptedPods 验证 sync 会清理 DisruptedPods：
// 移除已开始删除的 pod、超过 stalePodDisruptionTimeout 的过期条目以及对应 pod 已消失的条目，
// 仅保留仍在中断中且未超时的条目。
func TestUpdateDisruptedPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)
	dc.recheckQueue = workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Name: "pdb_queue"})
	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromInt32(1))
	currentTime := dc.clock.Now()
	// 构造四类条目以覆盖所有清理分支：正在删除、已过期、仍在中断、pod 已消失。
	pdb.Status.DisruptedPods = map[string]metav1.Time{
		"p1":       {Time: currentTime},                       // 应被移除：pod 已开始删除。
		"p2":       {Time: currentTime.Add(-3 * time.Minute)}, // 应被移除：已超过陈旧超时。
		"p3":       {Time: currentTime.Add(-time.Minute)},     // 应保留：pod 仍在中断中且未超时。
		"notthere": {Time: currentTime},                       // 应被移除：对应 pod 已被删除。
	}
	add(t, dc.pdbStore, pdb)

	pod1, _ := newPod(t, "p1")
	pod1.DeletionTimestamp = &metav1.Time{Time: dc.clock.Now()}
	pod2, _ := newPod(t, "p2")
	pod3, _ := newPod(t, "p3")

	add(t, dc.podStore, pod1)
	add(t, dc.podStore, pod2)
	add(t, dc.podStore, pod3)

	dc.sync(ctx, pdbName)

	ps.VerifyPdbStatus(t, pdbName, 0, 1, 1, 3, map[string]metav1.Time{"p3": {Time: currentTime.Add(-time.Minute)}})
}

// TestBasicFinderFunctions 表驱动验证 ReplicaSet、ReplicationController、StatefulSet 三种 finder：
// apiVersion/Kind 正确时返回控制器及其规模，group 或 kind 不匹配时返回 nil。
func TestBasicFinderFunctions(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, _ := newFakeDisruptionController(ctx)

	rs, _ := newReplicaSet(t, 10)
	add(t, dc.rsStore, rs)
	rc, _ := newReplicationController(t, 12)
	add(t, dc.rcStore, rc)
	ss, _ := newStatefulSet(t, 14)
	add(t, dc.ssStore, ss)

	testCases := map[string]struct {
		finderFunc    podControllerFinder
		apiVersion    string
		kind          string
		name          string
		uid           types.UID
		findsScale    bool
		expectedScale int32
	}{
		"replicaset controller with apps group": {
			finderFunc:    dc.getPodReplicaSet,
			apiVersion:    "apps/v1",
			kind:          controllerKindRS.Kind,
			name:          rs.Name,
			uid:           rs.UID,
			findsScale:    true,
			expectedScale: 10,
		},
		"replicaset controller with invalid group": {
			finderFunc: dc.getPodReplicaSet,
			apiVersion: "invalid/v1",
			kind:       controllerKindRS.Kind,
			name:       rs.Name,
			uid:        rs.UID,
			findsScale: false,
		},
		"replicationcontroller with empty group": {
			finderFunc:    dc.getPodReplicationController,
			apiVersion:    "/v1",
			kind:          controllerKindRC.Kind,
			name:          rc.Name,
			uid:           rc.UID,
			findsScale:    true,
			expectedScale: 12,
		},
		"replicationcontroller with invalid group": {
			finderFunc: dc.getPodReplicationController,
			apiVersion: "apps/v1",
			kind:       controllerKindRC.Kind,
			name:       rc.Name,
			uid:        rc.UID,
			findsScale: false,
		},
		"statefulset controller with extensions group": {
			finderFunc:    dc.getPodStatefulSet,
			apiVersion:    "apps/v1",
			kind:          controllerKindSS.Kind,
			name:          ss.Name,
			uid:           ss.UID,
			findsScale:    true,
			expectedScale: 14,
		},
		"statefulset controller with invalid kind": {
			finderFunc: dc.getPodStatefulSet,
			apiVersion: "apps/v1",
			kind:       controllerKindRS.Kind,
			name:       ss.Name,
			uid:        ss.UID,
			findsScale: false,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			controllerRef := &metav1.OwnerReference{
				APIVersion: tc.apiVersion,
				Kind:       tc.kind,
				Name:       tc.name,
				UID:        tc.uid,
			}

			controllerAndScale, _ := tc.finderFunc(ctx, controllerRef, metav1.NamespaceDefault)

			if controllerAndScale == nil {
				if tc.findsScale {
					t.Error("Expected scale, but got nil")
				}
				return
			}

			if got, want := controllerAndScale.scale, tc.expectedScale; got != want {
				t.Errorf("Expected scale %d, but got %d", want, got)
			}

			if got, want := controllerAndScale.UID, tc.uid; got != want {
				t.Errorf("Expected uid %s, but got %s", want, got)
			}
		})
	}
}

// TestDeploymentFinderFunction 表驱动验证 getPodDeployment 沿 RS→Deployment 的属主链查找：
// 仅当 RS 与 Deployment 的 apiVersion/Kind 均合法时返回 Deployment 规模，否则返回 nil。
func TestDeploymentFinderFunction(t *testing.T) {
	labels := map[string]string{
		"foo": "bar",
	}

	testCases := map[string]struct {
		rsApiVersion  string
		rsKind        string
		depApiVersion string
		depKind       string
		findsScale    bool
		expectedScale int32
	}{
		"happy path": {
			rsApiVersion:  "apps/v1",
			rsKind:        controllerKindRS.Kind,
			depApiVersion: "extensions/v1",
			depKind:       controllerKindDep.Kind,
			findsScale:    true,
			expectedScale: 10,
		},
		"invalid rs apiVersion": {
			rsApiVersion:  "invalid/v1",
			rsKind:        controllerKindRS.Kind,
			depApiVersion: "apps/v1",
			depKind:       controllerKindDep.Kind,
			findsScale:    false,
		},
		"invalid rs kind": {
			rsApiVersion:  "apps/v1",
			rsKind:        "InvalidKind",
			depApiVersion: "apps/v1",
			depKind:       controllerKindDep.Kind,
			findsScale:    false,
		},
		"invalid deployment apiVersion": {
			rsApiVersion:  "extensions/v1",
			rsKind:        controllerKindRS.Kind,
			depApiVersion: "deployment/v1",
			depKind:       controllerKindDep.Kind,
			findsScale:    false,
		},
		"invalid deployment kind": {
			rsApiVersion:  "apps/v1",
			rsKind:        controllerKindRS.Kind,
			depApiVersion: "extensions/v1",
			depKind:       "InvalidKind",
			findsScale:    false,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			dc, _ := newFakeDisruptionController(ctx)

			dep, _ := newDeployment(t, 10)
			dep.Spec.Selector = newSel(labels)
			add(t, dc.dStore, dep)

			rs, _ := newReplicaSet(t, 5)
			rs.Labels = labels
			trueVal := true
			rs.OwnerReferences = append(rs.OwnerReferences, metav1.OwnerReference{
				APIVersion: tc.depApiVersion,
				Kind:       tc.depKind,
				Name:       dep.Name,
				UID:        dep.UID,
				Controller: &trueVal,
			})
			add(t, dc.rsStore, rs)

			controllerRef := &metav1.OwnerReference{
				APIVersion: tc.rsApiVersion,
				Kind:       tc.rsKind,
				Name:       rs.Name,
				UID:        rs.UID,
			}

			controllerAndScale, _ := dc.getPodDeployment(ctx, controllerRef, metav1.NamespaceDefault)

			if controllerAndScale == nil {
				if tc.findsScale {
					t.Error("Expected scale, but got nil")
				}
				return
			}

			if got, want := controllerAndScale.scale, tc.expectedScale; got != want {
				t.Errorf("Expected scale %d, but got %d", want, got)
			}

			if got, want := controllerAndScale.UID, dep.UID; got != want {
				t.Errorf("Expected uid %s, but got %s", want, got)
			}
		})
	}
}

// TestUpdatePDBStatusRetries 验证在与驱逐处理器竞态时，控制器不会把陈旧数据写回 PDB 状态；
// 具体而言，因 ResourceVersion 冲突而失败的更新不应导致旧的 DisruptionsAllowed 被写入。
// 场景中 DisruptionsAllowed 初始为 2：
// (A) 删除 1 个 pod，触发控制器尝试把 DisruptionsAllowed 写为 1；
// (B) 在该写入发生时驱逐剩余 2 个 pod，把值更新为 0（真实驱逐处理器仍看到 2，故允许）；
// (C) 若控制器在冲突后仍写入 1，即为 bug。
func TestUpdatePDBStatusRetries(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, _ := newFakeDisruptionController(ctx)
	// 注入生产实现，覆盖构造时的假 updater，使写入走真实的冲突处理路径。
	dc.getUpdater = func() updater { return dc.writePdbStatus }
	// 创建一个 PDB 与 3 个匹配它的 pod，作为竞态场景的初始状态。
	pdb, pdbKey := newMinAvailablePodDisruptionBudget(t, intstr.FromInt32(1))
	pdb, err := dc.coreClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Create(ctx, pdb, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create PDB: %v", err)
	}
	podNames := []string{"moe", "larry", "curly"}
	for _, name := range podNames {
		pod, _ := newPod(t, name)
		_, err := dc.coreClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create pod: %v", err)
		}
	}

	// 阻塞等待 fake 客户端的写入在 informer 缓存中可见。
	// 注意：这保证缓存已更新，但不保证事件处理器已全部完成；
	// 所幸 DisruptionController 主要读 informer lister，因此该保证已足够。
	if err := waitForCacheCount(dc.pdbStore, 1); err != nil {
		t.Fatalf("Failed to verify PDB in informer cache: %v", err)
	}
	if err := waitForCacheCount(dc.podStore, len(podNames)); err != nil {
		t.Fatalf("Failed to verify pods in informer cache: %v", err)
	}

	// 先同步一次，让控制器写入初始 PDB 状态（DisruptionsAllowed=2）。
	if err := dc.sync(ctx, pdbKey); err != nil {
		t.Fatalf("Failed initial sync: %v", err)
	}

	// evict 在 fake 客户端中模拟驱逐的可见副作用。
	evict := func(podNames ...string) {
		// 这些 GVR 从生成的 fake 代码复制而来，因为原处未导出。
		var (
			podsResource                 = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
			poddisruptionbudgetsResource = schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"}
		)

		// 直接写 ObjectTracker 以绕过 coreClient.Fake：调用发生时 Fake 正持有锁，
		// 若再走 Fake 接口会死锁。
		obj, err := dc.coreClient.Tracker().Get(poddisruptionbudgetsResource, pdb.Namespace, pdb.Name)
		if err != nil {
			t.Fatalf("Failed to get PDB: %v", err)
		}
		updatedPDB := obj.(*policy.PodDisruptionBudget)
		// 每次驱逐会：
		// - 递减 DisruptionsAllowed
		// - 把 pod 加入 DisruptedPods
		// - 删除该 pod
		updatedPDB.Status.DisruptionsAllowed -= int32(len(podNames))
		updatedPDB.Status.DisruptedPods = make(map[string]metav1.Time)
		for _, name := range podNames {
			updatedPDB.Status.DisruptedPods[name] = metav1.NewTime(dc.clock.Now())
		}
		if err := dc.coreClient.Tracker().Update(poddisruptionbudgetsResource, updatedPDB, updatedPDB.Namespace); err != nil {
			t.Fatalf("Eviction (PDB update) failed: %v", err)
		}
		for _, name := range podNames {
			if err := dc.coreClient.Tracker().Delete(podsResource, "default", name); err != nil {
				t.Fatalf("Eviction (pod delete) failed: %v", err)
			}
		}
	}

	// fake 客户端不会更新 ResourceVersion，也不检查冲突；
	// 因此用 reactor 让首次 PDB 更新返回冲突错误，之后正常成功，以模拟竞态。
	var failOnce sync.Once
	dc.coreClient.Fake.PrependReactor("update", "poddisruptionbudgets", func(a core.Action) (handled bool, obj runtime.Object, err error) {
		failOnce.Do(func() {
			// (B) 驱逐两个 pod，并让本次更新失败，制造冲突窗口。
			evict(podNames[1], podNames[2])
			handled = true
			err = errors.NewConflict(a.GetResource().GroupResource(), pdb.Name, fmt.Errorf("conflict"))
		})
		return handled, obj, err
	})

	// (A) 删除一个 pod，触发控制器的写入尝试。
	if err := dc.coreClient.CoreV1().Pods("default").Delete(ctx, podNames[0], metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := waitForCacheCount(dc.podStore, len(podNames)-1); err != nil {
		t.Fatalf("Failed to verify pods in informer cache: %v", err)
	}

	// sync() 要么写出已考虑驱逐结果的正确状态，
	// 要么返回错误把 PDB 重新入队等待下次同步。
	if err := dc.sync(ctx, pdbKey); err != nil {
		t.Logf("sync() returned with error: %v", err)
	} else {
		t.Logf("sync() returned with no error")
	}

	// (C) 无论 sync() 是否返回错误，最终 PDB 状态都必须反映已发生的驱逐。
	finalPDB, err := dc.coreClient.PolicyV1().PodDisruptionBudgets("default").Get(ctx, pdb.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get PDB: %v", err)
	}
	if expected, actual := int32(0), finalPDB.Status.DisruptionsAllowed; expected != actual {
		t.Errorf("DisruptionsAllowed should be %d, got %d", expected, actual)
	}
}

// TestInvalidSelectors 验证非法 LabelSelector（键含多余斜杠、非法操作符）不会导致控制器崩溃，
// 而是把 PDB 状态置为全 0 的初始值。
func TestInvalidSelectors(t *testing.T) {
	testCases := map[string]struct {
		labelSelector *metav1.LabelSelector
	}{
		"illegal value key": {
			labelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"k8s.io/too/many/slashes": "value",
				},
			},
		},
		"illegal operator": {
			labelSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "foo",
						Operator: metav1.LabelSelectorOperator("illegal"),
						Values:   []string{"bar"},
					},
				},
			},
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			dc, ps := newFakeDisruptionController(ctx)

			pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromInt32(3))
			pdb.Spec.Selector = tc.labelSelector

			add(t, dc.pdbStore, pdb)
			dc.sync(ctx, pdbName)
			ps.VerifyPdbStatus(t, pdbName, 0, 0, 0, 0, map[string]metav1.Time{})
		})
	}
}

// TestStalePodDisruption 验证 DisruptionTarget 条件超过 2 分钟陈旧阈值后被置为 False，
// 未超时保持 True；正在删除或本已是 False 的 pod 不被改写。
// 使用 fake clock 精确跨越 ±1 秒的边界，避免依赖真实时间。
func TestStalePodDisruption(t *testing.T) {
	now := time.Now()
	cases := map[string]struct {
		pod            *v1.Pod
		timePassed     time.Duration
		wantConditions []v1.PodCondition
	}{
		"stale pod disruption": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: metav1.NamespaceDefault,
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:               v1.DisruptionTarget,
							Status:             v1.ConditionTrue,
							LastTransitionTime: metav1.Time{Time: now},
						},
					},
				},
			},
			timePassed: 2*time.Minute + time.Second,
			wantConditions: []v1.PodCondition{
				{
					Type:   v1.DisruptionTarget,
					Status: v1.ConditionFalse,
				},
			},
		},
		"pod disruption in progress": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: metav1.NamespaceDefault,
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:               v1.DisruptionTarget,
							Status:             v1.ConditionTrue,
							LastTransitionTime: metav1.Time{Time: now},
						},
					},
				},
			},
			timePassed: 2*time.Minute - time.Second,
			wantConditions: []v1.PodCondition{
				{
					Type:   v1.DisruptionTarget,
					Status: v1.ConditionTrue,
				},
			},
		},
		"pod disruption actuated": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "foo",
					Namespace:         metav1.NamespaceDefault,
					DeletionTimestamp: &metav1.Time{Time: now},
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:               v1.DisruptionTarget,
							Status:             v1.ConditionTrue,
							LastTransitionTime: metav1.Time{Time: now},
						},
					},
				},
			},
			timePassed: 2*time.Minute + time.Second,
			wantConditions: []v1.PodCondition{
				{
					Type:   v1.DisruptionTarget,
					Status: v1.ConditionTrue,
				},
			},
		},
		"no pod disruption": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "foo",
					Namespace:         metav1.NamespaceDefault,
					DeletionTimestamp: &metav1.Time{Time: now},
				},
			},
			timePassed: 2*time.Minute + time.Second,
		},
		"pod disruption cleared": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "foo",
					Namespace:         metav1.NamespaceDefault,
					DeletionTimestamp: &metav1.Time{Time: now},
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:   v1.DisruptionTarget,
							Status: v1.ConditionFalse,
						},
					},
				},
			},
			timePassed: 2*time.Minute + time.Second,
			wantConditions: []v1.PodCondition{
				{
					Type:   v1.DisruptionTarget,
					Status: v1.ConditionFalse,
				},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			dc, _ := newFakeDisruptionControllerWithTime(tCtx, now)
			go dc.Run(tCtx, 1)
			if _, err := dc.coreClient.CoreV1().Pods(tc.pod.Namespace).Create(tCtx, tc.pod, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create pod: %v", err)
			}
			// fake clock 的 Sleep 只推进逻辑时间，不阻塞真实时间，可精确跨越 2 分钟阈值。
			dc.clock.Sleep(tc.timePassed)
			// 时间推进后重新把 pod 加入索引器，触发 worker 重新评估 DisruptionTarget 是否陈旧。
			if err := dc.informerFactory.Core().V1().Pods().Informer().GetIndexer().Add(tc.pod); err != nil {
				t.Fatalf("Failed adding pod to indexer: %v", err)
			}
			diff := ""
			if err := wait.Poll(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
				pod, err := dc.kubeClient.CoreV1().Pods(tc.pod.Namespace).Get(tCtx, tc.pod.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Failed getting updated pod: %v", err)
				}
				diff = cmp.Diff(tc.wantConditions, pod.Status.Conditions, cmpopts.IgnoreFields(v1.PodCondition{}, "LastTransitionTime"))
				return diff == "", nil
			}); err != nil {
				t.Fatalf("Failed waiting for worker to sync: %v, (-want,+got):\n%s", err, diff)
			}
		})
	}
}

// TestKeepExistingPDBConditionDuringSync 验证 sync 更新 PDB 状态时会保留已有的自定义 Condition，
// 不会被新条件集整体覆盖（最终应同时存在原有条件与 DisruptionAllowed 共 2 个条件）。
func TestKeepExistingPDBConditionDuringSync(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	dc, ps := newFakeDisruptionController(ctx)

	pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromInt32(3))
	pdb.Spec.Selector = &metav1.LabelSelector{}

	pdb.Status.Conditions = append(pdb.Status.Conditions, metav1.Condition{
		Type:               "ExistingTestCondition",
		Status:             metav1.ConditionTrue,
		Message:            "This is a test condition",
		Reason:             "Test",
		LastTransitionTime: metav1.Now(),
	})

	add(t, dc.pdbStore, pdb)
	if err := dc.sync(ctx, pdbName); err != nil {
		t.Fatalf("Failed to sync PDB: %v", err)
	}
	ps.VerifyPdbStatus(t, pdbName, 0, 0, 3, 0, map[string]metav1.Time{})

	actualPDB := ps.Get(pdbName)
	condition := apimeta.FindStatusCondition(actualPDB.Status.Conditions, "ExistingTestCondition")
	if len(actualPDB.Status.Conditions) != 2 {
		t.Fatalf("Expected 2 conditions, but got %d", len(actualPDB.Status.Conditions))
	}
	if condition == nil {
		t.Fatalf("Expected ExistingTestCondition condition, but didn't find it")
	}
}

// waitForCacheCount 阻塞等待给定 cache store 中的条目数达到 n，
// 10 秒超时仍未满足则返回错误，用于确定性地等待 informer 缓存同步。
func waitForCacheCount(store cache.Store, n int) error {
	return wait.Poll(10*time.Millisecond, 10*time.Second, func() (bool, error) {
		return len(store.List()) == n, nil
	})
}

// verifyEventEmitted 从 FakeRecorder 持续读取事件，直到出现包含 expectedEvent 的消息；
// 500ms ticker 超时则失败，防止事件缺失时测试永久阻塞。
func verifyEventEmitted(t *testing.T, dc *disruptionController, expectedEvent string) {
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case e := <-dc.recorder.(*record.FakeRecorder).Events:
			if strings.Contains(e, expectedEvent) {
				return
			}
		case <-ticker.C:
			t.Fatalf("Timed out: expected event not generated: %v", expectedEvent)
		}
	}
}
