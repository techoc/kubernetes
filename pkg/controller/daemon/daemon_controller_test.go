/*
Copyright 2015 The Kubernetes Authors.

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

package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/daemon/util"
	"k8s.io/kubernetes/pkg/securitycontext"
	labelsutil "k8s.io/kubernetes/pkg/util/labels"
	testingclock "k8s.io/utils/clock/testing"
)

var (
	simpleDaemonSetLabel  = map[string]string{"name": "simple-daemon", "type": "production"}
	simpleDaemonSetLabel2 = map[string]string{"name": "simple-daemon", "type": "test"}
	simpleNodeLabel       = map[string]string{"color": "blue", "speed": "fast"}
	simpleNodeLabel2      = map[string]string{"color": "red", "speed": "fast"}
	alwaysReady           = func() bool { return true }
	informerSyncTimeout   = 30 * time.Second
)

var (
	noScheduleTolerations = []v1.Toleration{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}
	noScheduleTaints      = []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}
	noExecuteTaints       = []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoExecute"}}
)

// nowPointer 返回当前时间的时间戳指针，用于构造带时间的污点。
func nowPointer() *metav1.Time {
	now := metav1.Now()
	return &now
}

var (
	nodeNotReady = []v1.Taint{{
		Key:       v1.TaintNodeNotReady,
		Effect:    v1.TaintEffectNoExecute,
		TimeAdded: nowPointer(),
	}}

	nodeUnreachable = []v1.Taint{{
		Key:       v1.TaintNodeUnreachable,
		Effect:    v1.TaintEffectNoExecute,
		TimeAdded: nowPointer(),
	}}
)

// newDaemonSet 构造测试用 DaemonSet，默认使用 OnDelete 策略和固定标签。
func newDaemonSet(name string) *apps.DaemonSet {
	two := int32(2)
	return &apps.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			UID:       uuid.NewUUID(),
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: apps.DaemonSetSpec{
			RevisionHistoryLimit: &two,
			UpdateStrategy: apps.DaemonSetUpdateStrategy{
				Type: apps.OnDeleteDaemonSetStrategyType,
			},
			Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: simpleDaemonSetLabel,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Image:                  "foo/bar",
							TerminationMessagePath: v1.TerminationMessagePathDefault,
							ImagePullPolicy:        v1.PullIfNotPresent,
							SecurityContext:        securitycontext.ValidSecurityContextWithContainerDefaults(),
						},
					},
					DNSPolicy: v1.DNSDefault,
				},
			},
		},
	}
}

// newRollingUpdateStrategy 构造 MaxUnavailable=1 的滚动更新策略。
func newRollingUpdateStrategy() *apps.DaemonSetUpdateStrategy {
	one := intstr.FromInt32(1)
	return &apps.DaemonSetUpdateStrategy{
		Type:          apps.RollingUpdateDaemonSetStrategyType,
		RollingUpdate: &apps.RollingUpdateDaemonSet{MaxUnavailable: &one},
	}
}

// newOnDeleteStrategy 构造 OnDelete 更新策略。
func newOnDeleteStrategy() *apps.DaemonSetUpdateStrategy {
	return &apps.DaemonSetUpdateStrategy{
		Type: apps.OnDeleteDaemonSetStrategyType,
	}
}

// updateStrategies 返回所有更新策略，供测试逐一覆盖两种策略的行为。
func updateStrategies() []*apps.DaemonSetUpdateStrategy {
	return []*apps.DaemonSetUpdateStrategy{newOnDeleteStrategy(), newRollingUpdateStrategy()}
}

// newNode 构造 Ready 且可分配 100 个 Pod 的测试节点。
func newNode(name string, label map[string]string) *v1.Node {
	return &v1.Node{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Labels:    label,
			Namespace: metav1.NamespaceNone,
		},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{Type: v1.NodeReady, Status: v1.ConditionTrue},
			},
			Allocatable: v1.ResourceList{
				v1.ResourcePods: resource.MustParse("100"),
			},
		},
	}
}

// addNodes 批量向节点存储写入带相同标签的节点。
func addNodes(nodeStore cache.Store, startIndex, numNodes int, label map[string]string) {
	for i := startIndex; i < startIndex+numNodes; i++ {
		nodeStore.Add(newNode(fmt.Sprintf("node-%d", i), label))
	}
}

// newPod 构造测试 Pod；传入 DaemonSet 时会带上控制器引用和模板哈希标签。
func newPod(podName string, nodeName string, label map[string]string, ds *apps.DaemonSet) *v1.Pod {
	// 加入模板哈希标签，便于区分不同版本的 Pod。
	newLabels := label
	var podSpec v1.PodSpec
	// 有 DaemonSet 时复用其模板，否则用默认规格，便于测试通用 Pod。
	if ds != nil {
		hash := controller.ComputeHash(&ds.Spec.Template, ds.Status.CollisionCount)
		newLabels = labelsutil.CloneAndAddLabel(label, apps.DefaultDaemonSetUniqueLabelKey, hash)
		podSpec = ds.Spec.Template.Spec
	} else {
		podSpec = v1.PodSpec{
			Containers: []v1.Container{
				{
					Image:                  "foo/bar",
					TerminationMessagePath: v1.TerminationMessagePathDefault,
					ImagePullPolicy:        v1.PullIfNotPresent,
					SecurityContext:        securitycontext.ValidSecurityContextWithContainerDefaults(),
				},
			},
		}
	}
	// 指定节点名以模拟已调度的 Pod。
	if len(nodeName) > 0 {
		podSpec.NodeName = nodeName
	}

	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: podName,
			Labels:       newLabels,
			Namespace:    metav1.NamespaceDefault,
		},
		Spec: podSpec,
	}
	pod.Name = names.SimpleNameGenerator.GenerateName(podName)
	if ds != nil {
		pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(ds, controllerKind)}
	}
	return pod
}

// addPods 批量向 Pod 存储写入指定节点上的 Pod。
func addPods(podStore cache.Store, nodeName string, label map[string]string, ds *apps.DaemonSet, number int) {
	for i := 0; i < number; i++ {
		pod := newPod(fmt.Sprintf("%s-", nodeName), nodeName, label, ds)
		podStore.Add(pod)
	}
}

// addFailedPods 批量写入失败状态的 Pod，用于验证失败 Pod 的处理逻辑。
func addFailedPods(podStore cache.Store, nodeName string, label map[string]string, ds *apps.DaemonSet, number int) {
	for i := 0; i < number; i++ {
		pod := newPod(fmt.Sprintf("%s-", nodeName), nodeName, label, ds)
		pod.Status = v1.PodStatus{Phase: v1.PodFailed}
		podStore.Add(pod)
	}
}

// newControllerRevision 构造测试用 ControllerRevision。
func newControllerRevision(name string, namespace string, label map[string]string,
	ownerReferences []metav1.OwnerReference) *apps.ControllerRevision {
	return &apps.ControllerRevision{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Labels:          label,
			Namespace:       namespace,
			OwnerReferences: ownerReferences,
		},
	}
}

type fakePodControl struct {
	sync.Mutex
	*controller.FakePodControl
	podStore     cache.Store
	podIDMap     map[string]*v1.Pod
	expectations controller.ControllerExpectationsInterface
}

// newFakePodControl 构造带 Pod 索引的假 PodControl，便于模拟创建与删除后的存储变更。
func newFakePodControl() *fakePodControl {
	podIDMap := make(map[string]*v1.Pod)
	return &fakePodControl{
		FakePodControl: &controller.FakePodControl{},
		podIDMap:       podIDMap,
	}
}

// CreatePods 在创建 Pod 后同步更新假存储并消费创建预期，使后续同步行为贴近真实控制器。
func (f *fakePodControl) CreatePods(ctx context.Context, namespace string, template *v1.PodTemplateSpec, object runtime.Object, controllerRef *metav1.OwnerReference) error {
	f.Lock()
	defer f.Unlock()
	if err := f.FakePodControl.CreatePods(ctx, namespace, template, object, controllerRef); err != nil {
		return fmt.Errorf("failed to create pod for DaemonSet: %w", err)
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels:    template.Labels,
			Namespace: namespace,
		},
	}

	pod.Name = names.SimpleNameGenerator.GenerateName(fmt.Sprintf("%p-", pod))

	template.Spec.DeepCopyInto(&pod.Spec)

	f.podStore.Update(pod)
	f.podIDMap[pod.Name] = pod

	ds := object.(*apps.DaemonSet)
	dsKey, _ := controller.KeyFunc(ds)
	f.expectations.CreationObserved(klog.FromContext(ctx), dsKey)

	return nil
}

// DeletePod 在删除 Pod 后同步更新假存储并消费删除预期，使后续同步行为贴近真实控制器。
func (f *fakePodControl) DeletePod(ctx context.Context, namespace string, podID string, object runtime.Object) error {
	f.Lock()
	defer f.Unlock()
	if err := f.FakePodControl.DeletePod(ctx, namespace, podID, object); err != nil {
		return fmt.Errorf("failed to delete pod %q", podID)
	}
	pod, ok := f.podIDMap[podID]
	if !ok {
		return fmt.Errorf("pod %q does not exist", podID)
	}
	f.podStore.Delete(pod)
	delete(f.podIDMap, podID)

	ds := object.(*apps.DaemonSet)
	dsKey, _ := controller.KeyFunc(ds)
	f.expectations.DeletionObserved(klog.FromContext(ctx), dsKey)

	return nil
}

type daemonSetsController struct {
	*DaemonSetsController

	dsStore      cache.Store
	historyStore cache.Store
	podStore     cache.Store
	nodeStore    cache.Store
	fakeRecorder *record.FakeRecorder
}

// newTestController 搭建带 informer 存储的测试控制器，并让同步检查始终通过。
func newTestController(ctx context.Context, initialObjects ...runtime.Object) (*daemonSetsController, *fakePodControl, *fake.Clientset, error) {
	clientset := fake.NewSimpleClientset(initialObjects...)
	informerFactory := informers.NewSharedInformerFactory(clientset, controller.NoResyncPeriodFunc())

	dsc, err := NewDaemonSetsController(
		ctx,
		informerFactory.Apps().V1().DaemonSets(),
		informerFactory.Apps().V1().ControllerRevisions(),
		informerFactory.Core().V1().Pods(),
		informerFactory.Core().V1().Nodes(),
		clientset,
		flowcontrol.NewFakeBackOff(50*time.Millisecond, 500*time.Millisecond, testingclock.NewFakeClock(time.Now())),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	fakeRecorder := record.NewFakeRecorder(100)
	dsc.eventRecorder = fakeRecorder

	dsc.podStoreSynced = alwaysReady
	dsc.nodeStoreSynced = alwaysReady
	dsc.dsStoreSynced = alwaysReady
	dsc.historyStoreSynced = alwaysReady
	podControl := newFakePodControl()
	dsc.podControl = podControl
	podControl.podStore = informerFactory.Core().V1().Pods().Informer().GetStore()

	newDsc := &daemonSetsController{
		dsc,
		informerFactory.Apps().V1().DaemonSets().Informer().GetStore(),
		informerFactory.Apps().V1().ControllerRevisions().Informer().GetStore(),
		informerFactory.Core().V1().Pods().Informer().GetStore(),
		informerFactory.Core().V1().Nodes().Informer().GetStore(),
		fakeRecorder,
	}

	podControl.expectations = newDsc.expectations

	return newDsc, podControl, clientset, nil
}

// resetCounters 清空调用计数与事件记录，便于在同一控制器上连续断言多轮同步。
func resetCounters(manager *daemonSetsController) {
	manager.podControl.(*fakePodControl).Clear()
	fakeRecorder := record.NewFakeRecorder(100)
	manager.eventRecorder = fakeRecorder
	manager.fakeRecorder = fakeRecorder
}

// validateSyncDaemonSets 校验同步结果中的创建、删除、事件数量及 ControllerRef 是否正确。
func validateSyncDaemonSets(manager *daemonSetsController, fakePodControl *fakePodControl, expectedCreates, expectedDeletes int, expectedEvents int) error {
	if len(fakePodControl.Templates) != expectedCreates {
		return fmt.Errorf("Unexpected number of creates.  Expected %d, saw %d\n", expectedCreates, len(fakePodControl.Templates))
	}
	if len(fakePodControl.DeletePodName) != expectedDeletes {
		return fmt.Errorf("Unexpected number of deletes.  Expected %d, got %v\n", expectedDeletes, fakePodControl.DeletePodName)
	}
	if len(manager.fakeRecorder.Events) != expectedEvents {
		return fmt.Errorf("Unexpected number of events.  Expected %d, saw %d\n", expectedEvents, len(manager.fakeRecorder.Events))
	}
	// 每个创建的 Pod 都必须带 ControllerRef，否则会被 GC 误删。
	if got, want := len(fakePodControl.ControllerRefs), expectedCreates; got != want {
		return fmt.Errorf("len(ControllerRefs) = %v, want %v", got, want)
	}
	// 校验 ControllerRef 指向正确的 DaemonSet。
	for _, controllerRef := range fakePodControl.ControllerRefs {
		if got, want := controllerRef.APIVersion, "apps/v1"; got != want {
			return fmt.Errorf("controllerRef.APIVersion = %q, want %q", got, want)
		}
		if got, want := controllerRef.Kind, "DaemonSet"; got != want {
			return fmt.Errorf("controllerRef.Kind = %q, want %q", got, want)
		}
		if controllerRef.Controller == nil || *controllerRef.Controller != true {
			return fmt.Errorf("controllerRef.Controller is not set to true")
		}
	}
	return nil
}

// expectSyncDaemonSets 执行一次同步并断言没有预期错误。
func expectSyncDaemonSets(t *testing.T, manager *daemonSetsController, ds *apps.DaemonSet, podControl *fakePodControl, expectedCreates, expectedDeletes int, expectedEvents int) {
	t.Helper()
	expectSyncDaemonSetsWithError(t, manager, ds, podControl, expectedCreates, expectedDeletes, expectedEvents, nil)
}

// expectSyncDaemonSetsWithError 执行一次同步并断言返回指定错误。
func expectSyncDaemonSetsWithError(t *testing.T, manager *daemonSetsController, ds *apps.DaemonSet, podControl *fakePodControl, expectedCreates, expectedDeletes int, expectedEvents int, expectedError error) {
	t.Helper()
	key, err := controller.KeyFunc(ds)
	if err != nil {
		t.Fatal("could not get key for daemon")
	}

	err = manager.syncHandler(context.TODO(), key)
	if expectedError != nil && !errors.Is(err, expectedError) {
		t.Fatalf("Unexpected error returned from syncHandler: %v", err)
	}

	if expectedError == nil && err != nil {
		t.Log(err)
	}

	err = validateSyncDaemonSets(manager, podControl, expectedCreates, expectedDeletes, expectedEvents)
	if err != nil {
		t.Fatal(err)
	}
}

// clearExpectations 清空 FakePodControl 记录并移除 DaemonSet 的创建与删除预期。
func clearExpectations(t *testing.T, manager *daemonSetsController, ds *apps.DaemonSet, fakePodControl *fakePodControl) {
	fakePodControl.Clear()
	logger, _ := ktesting.NewTestContext(t)
	key, err := controller.KeyFunc(ds)
	if err != nil {
		t.Errorf("Could not get key for daemon.")
		return
	}
	manager.expectations.DeleteExpectations(logger, key)

	now := manager.failedPodsBackoff.Clock.Now()
	hash, _ := currentDSHash(manager, ds)
	// 输出 Pod 状态便于断言失败时定位问题。
	var lines []string
	for _, obj := range manager.podStore.List() {
		pod := obj.(*v1.Pod)
		if pod.CreationTimestamp.IsZero() {
			pod.CreationTimestamp.Time = now
		}
		var readyLast time.Time
		ready := podutil.IsPodReady(pod)
		if ready {
			if c := podutil.GetPodReadyCondition(pod.Status); c != nil {
				readyLast = c.LastTransitionTime.Time.Add(time.Duration(ds.Spec.MinReadySeconds) * time.Second)
			}
		}
		nodeName, _ := util.GetTargetNodeName(pod)

		lines = append(lines, fmt.Sprintf("node=%s current=%-5t ready=%-5t age=%-4d pod=%s now=%d available=%d",
			nodeName,
			hash == pod.Labels[apps.ControllerRevisionHashLabelKey],
			ready,
			now.Unix(),
			pod.Name,
			pod.CreationTimestamp.Unix(),
			readyLast.Unix(),
		))
	}
	sort.Strings(lines)
	for _, line := range lines {
		logger.Info(line)
	}
}

// TestDeleteFinalStateUnknown 验证删除事件中的 DeletedFinalStateUnknown 会被解包并入队。
func TestDeleteFinalStateUnknown(t *testing.T) {
	for _, strategy := range updateStrategies() {
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 1, nil)
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		// 缓存删除事件携带的对象应被解包并入队。
		manager.deleteDaemonset(logger, cache.DeletedFinalStateUnknown{Key: "foo", Obj: ds})
		enqueuedKey, _ := manager.queue.Get()
		if enqueuedKey != "default/foo" {
			t.Errorf("expected delete of DeletedFinalStateUnknown to enqueue the daemonset but found: %#v", enqueuedKey)
		}
	}
}

// TestExpectationsOnRecreate 验证 DaemonSet 重建后预期被重置、删除后预期被清理。
func TestExpectationsOnRecreate(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client := fake.NewSimpleClientset()

	f := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	dsc, err := NewDaemonSetsController(
		ctx,
		f.Apps().V1().DaemonSets(),
		f.Apps().V1().ControllerRevisions(),
		f.Core().V1().Pods(),
		f.Core().V1().Nodes(),
		client,
		flowcontrol.NewFakeBackOff(50*time.Millisecond, 500*time.Millisecond, testingclock.NewFakeClock(time.Now())),
	)
	if err != nil {
		t.Fatal(err)
	}

	expectStableQueueLength := func(expected int) {
		t.Helper()
		for i := 0; i < 5; i++ {
			if actual := dsc.queue.Len(); actual != expected {
				t.Fatalf("expected queue len to remain at %d, got %d", expected, actual)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitForQueueLength := func(expected int, msg string) {
		t.Helper()
		i := 0
		err = wait.PollImmediate(100*time.Millisecond, informerSyncTimeout, func() (bool, error) {
			current := dsc.queue.Len()
			switch {
			case current == expected:
				return true, nil
			case current > expected:
				return false, fmt.Errorf("queue length %d exceeded expected length %d", current, expected)
			default:
				i++
				if i > 1 {
					t.Logf("Waiting for queue to have %d item, currently has: %d", expected, current)
				}
				return false, nil
			}
		})
		if err != nil {
			t.Fatalf("%s: %v", msg, err)
		}
		expectStableQueueLength(expected)
	}

	fakeRecorder := record.NewFakeRecorder(100)
	dsc.eventRecorder = fakeRecorder

	fakePodControl := newFakePodControl()
	fakePodControl.podStore = cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc) // 占位存储，测试中不实际读取
	fakePodControl.expectations = controller.NewControllerExpectations()                 // 占位预期，测试中不实际校验
	dsc.podControl = fakePodControl

	manager := &daemonSetsController{
		DaemonSetsController: dsc,
		fakeRecorder:         fakeRecorder,
	}

	_, err = client.CoreV1().Nodes().Create(context.Background(), newNode("master-0", nil), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	f.Start(ctx.Done())
	for ty, ok := range f.WaitForCacheSync(ctx.Done()) {
		if !ok {
			t.Fatalf("caches failed to sync: %v", ty)
		}
	}

	expectStableQueueLength(0)

	oldDS := newDaemonSet("test")
	oldDS, err = client.AppsV1().DaemonSets(oldDS.Namespace).Create(context.Background(), oldDS, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 创建 DaemonSet 会入队，这里处理该事件。
	waitForQueueLength(1, "created DS")
	ok := dsc.processNextWorkItem(context.TODO())
	if !ok {
		t.Fatal("queue is shutting down")
	}

	err = validateSyncDaemonSets(manager, fakePodControl, 1, 0, 0)
	if err != nil {
		t.Error(err)
	}
	fakePodControl.Clear()

	oldDSKey, err := controller.KeyFunc(oldDS)
	if err != nil {
		t.Fatal(err)
	}

	dsExp, exists, err := dsc.expectations.GetExpectations(oldDSKey)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("No expectations found for DaemonSet %q", oldDSKey)
	}
	if dsExp.Fulfilled() {
		t.Errorf("There should be unfulfilled expectation for creating new pods for DaemonSet %q", oldDSKey)
	}

	// DaemonSet 更新事件同样会入队。
	waitForQueueLength(1, "updated DS")
	ok = dsc.processNextWorkItem(context.TODO())
	if !ok {
		t.Fatal("queue is shutting down")
	}

	// 处理更新事件不应再次触发 DaemonSet 更新。
	expectStableQueueLength(0)

	err = client.AppsV1().DaemonSets(oldDS.Namespace).Delete(context.Background(), oldDS.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	waitForQueueLength(1, "deleted DS")

	_, exists, err = dsc.expectations.GetExpectations(oldDSKey)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Errorf("There should be no expectations for DaemonSet %q after it was deleted", oldDSKey)
	}

	// 消费删除事件但不处理，避免干扰后续对重建对象的断言。
	key, quit := dsc.queue.Get()
	if quit {
		t.Fatal("Queue is shutting down!")
	}
	dsc.queue.Done(key)
	if key != oldDSKey {
		t.Fatal("Keys should be equal!")
	}

	expectStableQueueLength(0)

	newDS := oldDS.DeepCopy()
	newDS.UID = uuid.NewUUID()
	newDS, err = client.AppsV1().DaemonSets(newDS.Namespace).Create(context.Background(), newDS, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 基本校验：重建对象必须有不同的 UID。
	if newDS.UID == oldDS.UID {
		t.Fatal("New DS has the same UID as the old one!")
	}

	waitForQueueLength(1, "recreated DS")
	ok = dsc.processNextWorkItem(context.TODO())
	if !ok {
		t.Fatal("Queue is shutting down!")
	}

	newDSKey, err := controller.KeyFunc(newDS)
	if err != nil {
		t.Fatal(err)
	}
	dsExp, exists, err = dsc.expectations.GetExpectations(newDSKey)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("No expectations found for DaemonSet %q", oldDSKey)
	}
	if dsExp.Fulfilled() {
		t.Errorf("There should be unfulfilled expectation for creating new pods for DaemonSet %q", oldDSKey)
	}

	err = validateSyncDaemonSets(manager, fakePodControl, 1, 0, 0)
	if err != nil {
		t.Error(err)
	}
	fakePodControl.Clear()
}

// markPodsReady 将存储中所有 Pod 标为 Ready，便于验证就绪相关逻辑。
func markPodsReady(store cache.Store) {
	// 将存储中的 Pod 标记为 Ready。
	for _, obj := range store.List() {
		pod := obj.(*v1.Pod)
		markPodReady(pod)
	}
}

// markPodReady 为 Pod 添加 Ready=True 条件。
func markPodReady(pod *v1.Pod) {
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue}
	podutil.UpdatePodCondition(&pod.Status, &condition)
}

// TestSimpleDaemonSetLaunchesPods 验证无节点选择器的 DaemonSet 会在每个节点上创建 Pod。
func TestSimpleDaemonSetLaunchesPods(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 5, nil)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Error(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)
	}
}

// TestSimpleDaemonSetScheduleDaemonSetPodsLaunchesPods 验证调度模式下通过 NodeAffinity 在所有节点创建 Pod。
func TestSimpleDaemonSetScheduleDaemonSetPodsLaunchesPods(t *testing.T) {
	nodeNum := 5
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, nodeNum, nil)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, nodeNum, 0, 0)

		if len(podControl.podIDMap) != nodeNum {
			t.Fatalf("failed to create pods for DaemonSet")
		}

		nodeMap := make(map[string]*v1.Node)
		for _, node := range manager.nodeStore.List() {
			n := node.(*v1.Node)
			nodeMap[n.Name] = n
		}
		if len(nodeMap) != nodeNum {
			t.Fatalf("not enough nodes in the store, expected: %v, got: %v",
				nodeNum, len(nodeMap))
		}

		for _, pod := range podControl.podIDMap {
			if len(pod.Spec.NodeName) != 0 {
				t.Fatalf("the hostname of pod %v should be empty, but got %s",
					pod.Name, pod.Spec.NodeName)
			}
			if pod.Spec.Affinity == nil {
				t.Fatalf("the Affinity of pod %s is nil.", pod.Name)
			}
			if pod.Spec.Affinity.NodeAffinity == nil {
				t.Fatalf("the NodeAffinity of pod %s is nil.", pod.Name)
			}
			nodeSelector := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			if nodeSelector == nil {
				t.Fatalf("the node selector of pod %s is nil.", pod.Name)
			}
			if len(nodeSelector.NodeSelectorTerms) != 1 {
				t.Fatalf("incorrect number of node selector terms in pod %s, expected: 1, got: %d.",
					pod.Name, len(nodeSelector.NodeSelectorTerms))
			}

			if len(nodeSelector.NodeSelectorTerms[0].MatchFields) != 1 {
				t.Fatalf("incorrect number of fields in node selector term for pod %s, expected: 1, got: %d.",
					pod.Name, len(nodeSelector.NodeSelectorTerms[0].MatchFields))
			}

			field := nodeSelector.NodeSelectorTerms[0].MatchFields[0]
			if field.Key == metav1.ObjectNameField {
				if field.Operator != v1.NodeSelectorOpIn {
					t.Fatalf("the operation of hostname NodeAffinity is not %v", v1.NodeSelectorOpIn)
				}

				if len(field.Values) != 1 {
					t.Fatalf("incorrect hostname in node affinity: expected 1, got %v", len(field.Values))
				}
				delete(nodeMap, field.Values[0])
			}
		}

		if len(nodeMap) != 0 {
			t.Fatalf("did not find pods on nodes %+v", nodeMap)
		}
	}
}

// TestSimpleDaemonSetPodCreateErrors 模拟 100 节点集群受配额限制只能创建 10 个 Pod，
// 验证单轮同步不会因创建失败而发出远超限制的创建请求。
func TestSimpleDaemonSetPodCreateErrors(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, clientset, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		podControl.FakePodControl.CreateLimit = 10
		addNodes(manager.nodeStore, 0, podControl.FakePodControl.CreateLimit*10, nil)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		var updated *apps.DaemonSet
		clientset.PrependReactor("update", "daemonsets", func(action core.Action) (handled bool, ret runtime.Object, err error) {
			if action.GetSubresource() != "status" {
				return false, nil, nil
			}
			if u, ok := action.(core.UpdateAction); ok {
				updated = u.GetObject().(*apps.DaemonSet)
			}
			return false, nil, nil
		})

		expectSyncDaemonSets(t, manager, ds, podControl, podControl.FakePodControl.CreateLimit, 0, 0)

		expectedLimit := 0
		for pass := uint8(0); expectedLimit <= podControl.FakePodControl.CreateLimit; pass++ {
			expectedLimit += controller.SlowStartInitialBatchSize << pass
		}
		if podControl.FakePodControl.CreateCallCount > expectedLimit {
			t.Errorf("Unexpected number of create calls.  Expected <= %d, saw %d\n", podControl.FakePodControl.CreateLimit*2, podControl.FakePodControl.CreateCallCount)
		}
		if updated == nil {
			t.Fatalf("Failed to get updated status")
		}
		if got, want := updated.Status.DesiredNumberScheduled, int32(podControl.FakePodControl.CreateLimit)*10; got != want {
			t.Errorf("Status.DesiredNumberScheduled = %v, want %v", got, want)
		}
		if got, want := updated.Status.CurrentNumberScheduled, int32(podControl.FakePodControl.CreateLimit); got != want {
			t.Errorf("Status.CurrentNumberScheduled = %v, want %v", got, want)
		}
		if got, want := updated.Status.UpdatedNumberScheduled, int32(podControl.FakePodControl.CreateLimit); got != want {
			t.Errorf("Status.UpdatedNumberScheduled = %v, want %v", got, want)
		}
	}
}

// TestDaemonSetPodCreateExpectationsError 验证 Pod 创建失败后预期保留，阻止同一轮内无限重试。
func TestDaemonSetPodCreateExpectationsError(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	strategies := updateStrategies()
	for _, strategy := range strategies {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		podControl.FakePodControl.CreateLimit = 10
		creationExpectations := 100
		addNodes(manager.nodeStore, 0, 100, nil)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, podControl.FakePodControl.CreateLimit, 0, 0)

		dsKey, err := controller.KeyFunc(ds)
		if err != nil {
			t.Fatalf("error get DaemonSets controller key: %v", err)
		}

		if !manager.expectations.SatisfiedExpectations(logger, dsKey) {
			t.Errorf("Unsatisfied pod creation expectations. Expected %d", creationExpectations)
		}
	}
}

// TestSimpleDaemonSetUpdatesStatusAfterLaunchingPods 验证创建 Pod 后状态中的调度与就绪数量被更新。
func TestSimpleDaemonSetUpdatesStatusAfterLaunchingPods(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, clientset, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		var updated *apps.DaemonSet
		clientset.PrependReactor("update", "daemonsets", func(action core.Action) (handled bool, ret runtime.Object, err error) {
			if action.GetSubresource() != "status" {
				return false, nil, nil
			}
			if u, ok := action.(core.UpdateAction); ok {
				updated = u.GetObject().(*apps.DaemonSet)
			}
			return false, nil, nil
		})

		manager.dsStore.Add(ds)
		addNodes(manager.nodeStore, 0, 5, nil)
		expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)

		// 确认单次同步就提交了 manage 阶段产生的状态变更。
		if got, want := updated.Status.CurrentNumberScheduled, int32(5); got != want {
			t.Errorf("Status.CurrentNumberScheduled = %v, want %v", got, want)
		}
	}
}

// TestSimpleDaemonSetUpdatesStatusError 验证状态更新失败时错误向上传播。
func TestSimpleDaemonSetUpdatesStatusError(t *testing.T) {
	var (
		syncErr   = fmt.Errorf("sync error")
		statusErr = fmt.Errorf("status error")
	)

	testCases := []struct {
		desc string

		hasSyncErr   bool
		hasStatusErr bool

		expectedErr error
	}{
		{
			desc:         "sync error",
			hasSyncErr:   true,
			hasStatusErr: false,
			expectedErr:  syncErr,
		},
		{
			desc:         "status error",
			hasSyncErr:   false,
			hasStatusErr: true,
			expectedErr:  statusErr,
		},
		{
			desc:         "sync and status error",
			hasSyncErr:   true,
			hasStatusErr: true,
			expectedErr:  syncErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			for _, strategy := range updateStrategies() {
				ds := newDaemonSet("foo")
				ds.Spec.UpdateStrategy = *strategy
				_, ctx := ktesting.NewTestContext(t)
				manager, podControl, clientset, err := newTestController(ctx, ds)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}

				if tc.hasSyncErr {
					podControl.FakePodControl.Err = syncErr
				}

				clientset.PrependReactor("update", "daemonsets", func(action core.Action) (handled bool, ret runtime.Object, err error) {
					if action.GetSubresource() != "status" {
						return false, nil, nil
					}

					if tc.hasStatusErr {
						return true, nil, statusErr
					} else {
						return false, nil, nil
					}
				})

				manager.dsStore.Add(ds)
				addNodes(manager.nodeStore, 0, 1, nil)
				expectSyncDaemonSetsWithError(t, manager, ds, podControl, 1, 0, 0, tc.expectedErr)
			}
		})
	}
}

// TestNoNodesDoesNothing 验证没有节点时 DaemonSet 不执行任何操作。
func TestNoNodesDoesNothing(t *testing.T) {
	for _, strategy := range updateStrategies() {
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// TestOneNodeDaemonLaunchesPod 验证单节点集群中 DaemonSet 会在该节点创建 Pod。
func TestOneNodeDaemonLaunchesPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		err = manager.nodeStore.Add(newNode("only-node", nil))
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// TestNotReadyNodeDaemonLaunchesPod 验证 DaemonSet 会在 NotReady 节点上创建 Pod。
func TestNotReadyNodeDaemonDoesLaunchPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		node := newNode("not-ready", nil)
		node.Status.Conditions = []v1.NodeCondition{
			{Type: v1.NodeReady, Status: v1.ConditionFalse},
		}
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}

		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// resourcePodSpec 构造带指定节点与资源请求的 PodSpec。
func resourcePodSpec(nodeName, memory, cpu string) v1.PodSpec {
	return v1.PodSpec{
		NodeName: nodeName,
		Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{
				Requests: allocatableResources(memory, cpu),
			},
		}},
	}
}

// resourceContainerSpec 构造带指定内存和 CPU 需求的容器资源配置。
func resourceContainerSpec(memory, cpu string) v1.ResourceRequirements {
	return v1.ResourceRequirements{
		Requests: allocatableResources(memory, cpu),
	}
}

// resourcePodSpecWithoutNodeName 构造未绑定节点的 PodSpec，用于测试节点选择逻辑。
func resourcePodSpecWithoutNodeName(memory, cpu string) v1.PodSpec {
	return v1.PodSpec{
		Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{
				Requests: allocatableResources(memory, cpu),
			},
		}},
	}
}

// allocatableResources 构造节点可分配资源列表。
func allocatableResources(memory, cpu string) v1.ResourceList {
	return v1.ResourceList{
		v1.ResourceMemory: resource.MustParse(memory),
		v1.ResourceCPU:    resource.MustParse(cpu),
		v1.ResourcePods:   resource.MustParse("100"),
	}
}

// TestInsufficientCapacityNodeDaemonDoesNotUnscheduleRunningPod 验证资源不足时不会驱逐已运行的 Pod。
func TestInsufficientCapacityNodeDaemonDoesNotUnscheduleRunningPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		podSpec := resourcePodSpec("too-much-mem", "75M", "75m")
		podSpec.NodeName = "too-much-mem"
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec = podSpec
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		node := newNode("too-much-mem", nil)
		node.Status.Allocatable = allocatableResources("100M", "200m")
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.podStore.Add(&v1.Pod{
			Spec: podSpec,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		switch strategy.Type {
		case apps.OnDeleteDaemonSetStrategyType:
			expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
		case apps.RollingUpdateDaemonSetStrategyType:
			expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
		default:
			t.Fatalf("unexpected UpdateStrategy %+v", strategy)
		}
	}
}

// TestInsufficientCapacityNodeSufficientCapacityWithNodeLabelDaemonLaunchPod 验证仅在资源充足且标签匹配的节点创建 Pod。
func TestInsufficientCapacityNodeSufficientCapacityWithNodeLabelDaemonLaunchPod(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	podSpec := resourcePodSpecWithoutNodeName("50M", "75m")
	ds := newDaemonSet("foo")
	ds.Spec.Template.Spec = podSpec
	ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	node1 := newNode("not-enough-resource", nil)
	node1.Status.Allocatable = allocatableResources("10M", "20m")
	node2 := newNode("enough-resource", simpleNodeLabel)
	node2.Status.Allocatable = allocatableResources("100M", "200m")
	err = manager.nodeStore.Add(node1)
	if err != nil {
		t.Fatal(err)
	}
	err = manager.nodeStore.Add(node2)
	if err != nil {
		t.Fatal(err)
	}
	err = manager.dsStore.Add(ds)
	if err != nil {
		t.Fatal(err)
	}
	expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	// 资源不足属于正常筛选，不应产生事件。
	if len(manager.fakeRecorder.Events) != 0 {
		t.Fatalf("unexpected events, got %v, expected %v: %+v", len(manager.fakeRecorder.Events), 0, manager.fakeRecorder.Events)
	}
}

// TestNetworkUnavailableNodeDaemonLaunchesPod 验证带 NetworkUnavailable 污点的节点仍会创建 Pod。
func TestNetworkUnavailableNodeDaemonLaunchesPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("simple")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		node := newNode("network-unavailable", nil)
		node.Status.Conditions = []v1.NodeCondition{
			{Type: v1.NodeNetworkUnavailable, Status: v1.ConditionTrue},
		}
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// TestDontDoAnythingIfBeingDeleted 验证删除中的 DaemonSet 不再执行任何同步操作。
func TestDontDoAnythingIfBeingDeleted(t *testing.T) {
	for _, strategy := range updateStrategies() {
		podSpec := resourcePodSpec("not-too-much-mem", "75M", "75m")
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec = podSpec
		now := metav1.Now()
		ds.DeletionTimestamp = &now
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		node := newNode("not-too-much-mem", nil)
		node.Status.Allocatable = allocatableResources("200M", "200m")
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.podStore.Add(&v1.Pod{
			Spec: podSpec,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// TestDontDoAnythingIfBeingDeletedRace 验证删除中的 DaemonSet 在删除事件竞态下也不被重新入队。
func TestDontDoAnythingIfBeingDeletedRace(t *testing.T) {
	for _, strategy := range updateStrategies() {
		// 客户端视角：DaemonSet 已被删除。
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		now := metav1.Now()
		ds.DeletionTimestamp = &now
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 5, nil)

		// 缓存视角：DaemonSet 尚未删除，模拟竞态窗口。
		ds2 := *ds
		ds2.DeletionTimestamp = nil
		err = manager.dsStore.Add(&ds2)
		if err != nil {
			t.Fatal(err)
		}

		// 该状态下匹配的孤儿 Pod 应阻止所有操作。
		pod := newPod("pod1-", "node-0", simpleDaemonSetLabel, nil)
		err = manager.podStore.Add(pod)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// TestPortConflictWithSameDaemonPodDoesNotDeletePod 验证同 DaemonSet 的 Pod 占用 hostPort 不算端口冲突。
func TestPortConflictWithSameDaemonPodDoesNotDeletePod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		podSpec := v1.PodSpec{
			NodeName: "port-conflict",
			Containers: []v1.Container{{
				Ports: []v1.ContainerPort{{
					HostPort: 666,
				}},
			}},
		}
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		node := newNode("port-conflict", nil)
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec = podSpec
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		pod := newPod(ds.Name+"-", node.Name, simpleDaemonSetLabel, ds)
		err = manager.podStore.Add(pod)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// TestNoPortConflictNodeDaemonLaunchesPod 验证 DaemonSet 会避开端口冲突的节点。
func TestNoPortConflictNodeDaemonLaunchesPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		podSpec1 := v1.PodSpec{
			NodeName: "no-port-conflict",
			Containers: []v1.Container{{
				Ports: []v1.ContainerPort{{
					HostPort: 6661,
				}},
			}},
		}
		podSpec2 := v1.PodSpec{
			NodeName: "no-port-conflict",
			Containers: []v1.Container{{
				Ports: []v1.ContainerPort{{
					HostPort: 6662,
				}},
			}},
		}
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec = podSpec2
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		node := newNode("no-port-conflict", nil)
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.podStore.Add(&v1.Pod{
			Spec: podSpec1,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// TestPodIsNotDeletedByDaemonsetWithEmptyLabelSelector 验证空标签选择器不会误删 Pod。
func TestPodIsNotDeletedByDaemonsetWithEmptyLabelSelector(t *testing.T) {
	// 构造错误配置的 DaemonSet：空选择器非法但升级兼容场景可能出现。
	//
	// 节点选择器不匹配任何节点，模拟 kubectl delete 的行为。
	//
	// 这种情况下既不应调度也不应删除已有 Pod，尽管空选择器匹配所有 Pod。
	// 控制器应识别错误配置并跳过同步，不应观察到 node1 上的 Pod 被删除。
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ls := metav1.LabelSelector{}
		ds.Spec.Selector = &ls
		ds.Spec.Template.Spec.NodeSelector = map[string]string{"foo": "bar"}

		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		err = manager.nodeStore.Add(newNode("node1", nil))
		if err != nil {
			t.Fatal(err)
		}
		// 创建不受 DaemonSet 控制的 Pod。
		err = manager.podStore.Add(&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels:    map[string]string{"bang": "boom"},
				Namespace: metav1.NamespaceDefault,
			},
			Spec: v1.PodSpec{
				NodeName: "node1",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 1)
	}
}

// TestSurgeDealsWithExistingPods 验证滚动更新中已有 Pod 的节点不重复创建，多余 Pod 会被清理。
func TestDealsWithExistingPods(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		addNodes(manager.nodeStore, 0, 5, nil)
		addPods(manager.podStore, "node-1", simpleDaemonSetLabel, ds, 1)
		addPods(manager.podStore, "node-2", simpleDaemonSetLabel, ds, 2)
		addPods(manager.podStore, "node-3", simpleDaemonSetLabel, ds, 5)
		addPods(manager.podStore, "node-4", simpleDaemonSetLabel2, ds, 2)
		expectSyncDaemonSets(t, manager, ds, podControl, 2, 5, 0)
	}
}

// TestSelectorDaemonLaunchesPods 验证带节点选择器的 DaemonSet 仅在匹配节点创建 Pod。
func TestSelectorDaemonLaunchesPods(t *testing.T) {
	for _, strategy := range updateStrategies() {
		daemon := newDaemonSet("foo")
		daemon.Spec.UpdateStrategy = *strategy
		daemon.Spec.Template.Spec.NodeSelector = simpleNodeLabel
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, daemon)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 4, nil)
		addNodes(manager.nodeStore, 4, 3, simpleNodeLabel)
		err = manager.dsStore.Add(daemon)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, daemon, podControl, 3, 0, 0)
	}
}

// TestSelectorDaemonDeletesUnselectedPods 验证节点不再匹配选择器时会删除其上的 Pod。
func TestSelectorDaemonDeletesUnselectedPods(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		addNodes(manager.nodeStore, 0, 5, nil)
		addNodes(manager.nodeStore, 5, 5, simpleNodeLabel)
		addPods(manager.podStore, "node-0", simpleDaemonSetLabel2, ds, 2)
		addPods(manager.podStore, "node-1", simpleDaemonSetLabel, ds, 3)
		addPods(manager.podStore, "node-1", simpleDaemonSetLabel2, ds, 1)
		addPods(manager.podStore, "node-4", simpleDaemonSetLabel, ds, 1)
		expectSyncDaemonSets(t, manager, ds, podControl, 5, 4, 0)
	}
}

// TestSelectorDaemonDealsWithExistingPods 验证节点选择器匹配时也能正确处理已有的多余 Pod。
func TestSelectorDaemonDealsWithExistingPods(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		addNodes(manager.nodeStore, 0, 5, nil)
		addNodes(manager.nodeStore, 5, 5, simpleNodeLabel)
		addPods(manager.podStore, "node-0", simpleDaemonSetLabel, ds, 1)
		addPods(manager.podStore, "node-1", simpleDaemonSetLabel, ds, 3)
		addPods(manager.podStore, "node-1", simpleDaemonSetLabel2, ds, 2)
		addPods(manager.podStore, "node-2", simpleDaemonSetLabel, ds, 4)
		addPods(manager.podStore, "node-6", simpleDaemonSetLabel, ds, 13)
		addPods(manager.podStore, "node-7", simpleDaemonSetLabel2, ds, 4)
		addPods(manager.podStore, "node-9", simpleDaemonSetLabel, ds, 1)
		addPods(manager.podStore, "node-9", simpleDaemonSetLabel2, ds, 1)
		expectSyncDaemonSets(t, manager, ds, podControl, 3, 20, 0)
	}
}

// TestBadSelectorDaemonDoesNothing 验证选择器不匹配任何节点时不创建 Pod。
func TestBadSelectorDaemonDoesNothing(t *testing.T) {
	for _, strategy := range updateStrategies() {
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 4, nil)
		addNodes(manager.nodeStore, 4, 3, simpleNodeLabel)
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel2
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// TestNameDaemonSetLaunchesPods 验证指定节点名的 DaemonSet 仅在对应节点创建 Pod。
func TestNameDaemonSetLaunchesPods(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec.NodeName = "node-0"
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 5, nil)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// TestBadNameDaemonSetDoesNothing 验证指定节点不存在时不创建 Pod。
func TestBadNameDaemonSetDoesNothing(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec.NodeName = "node-10"
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 5, nil)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// TestNameAndSelectorDaemonSetLaunchesPods 验证节点名与选择器同时匹配时在对应节点创建 Pod。
func TestNameAndSelectorDaemonSetLaunchesPods(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
		ds.Spec.Template.Spec.NodeName = "node-6"
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 4, nil)
		addNodes(manager.nodeStore, 4, 3, simpleNodeLabel)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// TestInconsistentNameSelectorDaemonSetDoesNothing 验证节点名与选择器指向不同节点时不创建 Pod。
func TestInconsistentNameSelectorDaemonSetDoesNothing(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
		ds.Spec.Template.Spec.NodeName = "node-0"
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 4, nil)
		addNodes(manager.nodeStore, 4, 3, simpleNodeLabel)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// TestSelectorDaemonSetLaunchesPods 验证选择器匹配多个节点时在每个匹配节点创建 Pod。
func TestSelectorDaemonSetLaunchesPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	addNodes(manager.nodeStore, 0, 4, nil)
	addNodes(manager.nodeStore, 4, 3, simpleNodeLabel)
	err = manager.dsStore.Add(ds)
	if err != nil {
		t.Fatal(err)
	}
	expectSyncDaemonSets(t, manager, ds, podControl, 3, 0, 0)
}

// TestNodeAffinityDaemonLaunchesPods 验证 DaemonSet 依据节点亲和性选择节点创建 Pod。
func TestNodeAffinityDaemonLaunchesPods(t *testing.T) {
	for _, strategy := range updateStrategies() {
		daemon := newDaemonSet("foo")
		daemon.Spec.UpdateStrategy = *strategy
		daemon.Spec.Template.Spec.Affinity = &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "color",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{simpleNodeLabel["color"]},
								},
							},
						},
					},
				},
			},
		}
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, daemon)
		if err != nil {
			t.Fatalf("error creating DaemonSetsController: %v", err)
		}
		addNodes(manager.nodeStore, 0, 4, nil)
		addNodes(manager.nodeStore, 4, 3, simpleNodeLabel)
		err = manager.dsStore.Add(daemon)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, daemon, podControl, 3, 0, 0)
	}
}

// TestNumberReadyStatus 验证状态中各就绪相关计数的统计结果。
func TestNumberReadyStatus(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, clientset, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		var updated *apps.DaemonSet
		clientset.PrependReactor("update", "daemonsets", func(action core.Action) (handled bool, ret runtime.Object, err error) {
			if action.GetSubresource() != "status" {
				return false, nil, nil
			}
			if u, ok := action.(core.UpdateAction); ok {
				updated = u.GetObject().(*apps.DaemonSet)
			}
			return false, nil, nil
		})
		addNodes(manager.nodeStore, 0, 2, simpleNodeLabel)
		addPods(manager.podStore, "node-0", simpleDaemonSetLabel, ds, 1)
		addPods(manager.podStore, "node-1", simpleDaemonSetLabel, ds, 1)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
		if updated.Status.NumberReady != 0 {
			t.Errorf("Wrong daemon %s status: %v", updated.Name, updated.Status)
		}

		selector, _ := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
		daemonPods, _ := manager.podLister.Pods(ds.Namespace).List(selector)
		for _, pod := range daemonPods {
			condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue}
			pod.Status.Conditions = append(pod.Status.Conditions, condition)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
		if updated.Status.NumberReady != 2 {
			t.Errorf("Wrong daemon %s status: %v", updated.Name, updated.Status)
		}
	}
}

// TestObservedGeneration 验证状态中的 observedGeneration 随 DaemonSet 更新而变化。
func TestObservedGeneration(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		ds.Generation = 1
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, clientset, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		var updated *apps.DaemonSet
		clientset.PrependReactor("update", "daemonsets", func(action core.Action) (handled bool, ret runtime.Object, err error) {
			if action.GetSubresource() != "status" {
				return false, nil, nil
			}
			if u, ok := action.(core.UpdateAction); ok {
				updated = u.GetObject().(*apps.DaemonSet)
			}
			return false, nil, nil
		})

		addNodes(manager.nodeStore, 0, 1, simpleNodeLabel)
		addPods(manager.podStore, "node-0", simpleDaemonSetLabel, ds, 1)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
		if updated.Status.ObservedGeneration != ds.Generation {
			t.Errorf("Wrong ObservedGeneration for daemon %s in status. Expected %d, got %d", updated.Name, ds.Generation, updated.Status.ObservedGeneration)
		}
	}
}

// TestDaemonKillFailedPods 验证控制器删除失败 Pod 且每个节点最多补建一个。
func TestDaemonKillFailedPods(t *testing.T) {
	tests := []struct {
		numFailedPods, numNormalPods, expectedCreates, expectedDeletes, expectedEvents int
		test                                                                           string
	}{
		{numFailedPods: 0, numNormalPods: 1, expectedCreates: 0, expectedDeletes: 0, expectedEvents: 0, test: "normal (do nothing)"},
		{numFailedPods: 0, numNormalPods: 0, expectedCreates: 1, expectedDeletes: 0, expectedEvents: 0, test: "no pods (create 1)"},
		{numFailedPods: 1, numNormalPods: 0, expectedCreates: 0, expectedDeletes: 1, expectedEvents: 1, test: "1 failed pod (kill 1), 0 normal pod (create 0; will create in the next sync)"},
		{numFailedPods: 1, numNormalPods: 3, expectedCreates: 0, expectedDeletes: 3, expectedEvents: 1, test: "1 failed pod (kill 1), 3 normal pods (kill 2)"},
	}

	for _, test := range tests {
		t.Run(test.test, func(t *testing.T) {
			for _, strategy := range updateStrategies() {
				ds := newDaemonSet("foo")
				ds.Spec.UpdateStrategy = *strategy
				_, ctx := ktesting.NewTestContext(t)
				manager, podControl, _, err := newTestController(ctx, ds)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				err = manager.dsStore.Add(ds)
				if err != nil {
					t.Fatal(err)
				}
				addNodes(manager.nodeStore, 0, 1, nil)
				addFailedPods(manager.podStore, "node-0", simpleDaemonSetLabel, ds, test.numFailedPods)
				addPods(manager.podStore, "node-0", simpleDaemonSetLabel, ds, test.numNormalPods)
				expectSyncDaemonSets(t, manager, ds, podControl, test.expectedCreates, test.expectedDeletes, test.expectedEvents)
			}
		})
	}
}

// TestDaemonKillFailedPodsBackoff 验证删除失败 Pod 时采用退避，避免与 kubelet 争抢导致热循环。
func TestDaemonKillFailedPodsBackoff(t *testing.T) {
	for _, strategy := range updateStrategies() {
		t.Run(string(strategy.Type), func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ds := newDaemonSet("foo")
			ds.Spec.UpdateStrategy = *strategy

			manager, podControl, _, err := newTestController(ctx, ds)
			if err != nil {
				t.Fatalf("error creating DaemonSets controller: %v", err)
			}

			err = manager.dsStore.Add(ds)
			if err != nil {
				t.Fatal(err)
			}
			addNodes(manager.nodeStore, 0, 1, nil)

			nodeName := "node-0"
			pod := newPod(fmt.Sprintf("%s-", nodeName), nodeName, simpleDaemonSetLabel, ds)

			// 加入失败 Pod 触发删除与退避逻辑。
			pod.Status.Phase = v1.PodFailed
			err = manager.podStore.Add(pod)
			if err != nil {
				t.Fatal(err)
			}

			backoffKey := failedPodsBackoffKey(ds, nodeName)

			// 首次同步删除 Pod 并初始化退避计时。
			expectSyncDaemonSets(t, manager, ds, podControl, 0, 1, 1)
			initialDelay := manager.failedPodsBackoff.Get(backoffKey)
			if initialDelay <= 0 {
				t.Fatal("Initial delay is expected to be set.")
			}

			resetCounters(manager)

			// 紧接着的同步应被退避限制。
			expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
			delay := manager.failedPodsBackoff.Get(backoffKey)
			if delay != initialDelay {
				t.Fatal("Backoff delay shouldn't be raised while waiting.")
			}

			resetCounters(manager)

			// 等待退避时间结束。
			fakeClock := manager.failedPodsBackoff.Clock

			// 将时钟拨到退避结束前。
			fakeClock.Sleep(delay - 1*time.Nanosecond)
			if !manager.failedPodsBackoff.IsInBackOffSinceUpdate(backoffKey, fakeClock.Now()) {
				t.Errorf("Backoff delay didn't last the whole waitout period.")
			}

			// 将时钟拨到退避结束点。
			fakeClock.Sleep(1 * time.Nanosecond)
			if manager.failedPodsBackoff.IsInBackOffSinceUpdate(backoffKey, fakeClock.Now()) {
				t.Fatal("Backoff delay hasn't been reset after the period has passed.")
			}

			// 退避结束后才会删除失败 Pod。
			expectSyncDaemonSets(t, manager, ds, podControl, 0, 1, 1)
		})
	}
}

// TestNoScheduleTaintedDoesntEvicitRunningIntolerantPod 验证 NoSchedule 污点节点上不耐受的运行中 Pod 不会被驱逐。
func TestNoScheduleTaintedDoesntEvicitRunningIntolerantPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("intolerant")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		node := newNode("tainted", nil)
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		setNodeTaint(node, noScheduleTaints)
		err = manager.podStore.Add(newPod("keep-running-me", "tainted", simpleDaemonSetLabel, ds))
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// TestNoExecuteTaintedDoesEvicitRunningIntolerantPod 验证 NoExecute 污点节点上不耐受的运行中 Pod 会被驱逐。
func TestNoExecuteTaintedDoesEvicitRunningIntolerantPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("intolerant")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		node := newNode("tainted", nil)
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		setNodeTaint(node, noExecuteTaints)
		err = manager.podStore.Add(newPod("stop-running-me", "tainted", simpleDaemonSetLabel, ds))
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 0, 1, 0)
	}
}

// TestTaintedNodeDaemonDoesNotLaunchIntolerantPod 验证 Pod 不耐受污点时不在该节点创建。
func TestTaintedNodeDaemonDoesNotLaunchIntolerantPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("intolerant")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		node := newNode("tainted", nil)
		setNodeTaint(node, noScheduleTaints)
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// TestTaintedNodeDaemonLaunchesToleratePod 验证 Pod 耐受污点时仍在该节点创建。
func TestTaintedNodeDaemonLaunchesToleratePod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("tolerate")
		ds.Spec.UpdateStrategy = *strategy
		setDaemonSetToleration(ds, noScheduleTolerations)
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		node := newNode("tainted", nil)
		setNodeTaint(node, noScheduleTaints)
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// TestNotReadyNodeDaemonLaunchesPod 验证带 notReady:NoExecute 污点的节点仍会创建 Pod。
func TestNotReadyNodeDaemonLaunchesPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("simple")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		node := newNode("tainted", nil)
		setNodeTaint(node, nodeNotReady)
		node.Status.Conditions = []v1.NodeCondition{
			{Type: v1.NodeReady, Status: v1.ConditionFalse},
		}
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// TestUnreachableNodeDaemonLaunchesPod 验证节点不可达且带对应污点时仍会创建 Pod。
func TestUnreachableNodeDaemonLaunchesPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("simple")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		node := newNode("tainted", nil)
		setNodeTaint(node, nodeUnreachable)
		node.Status.Conditions = []v1.NodeCondition{
			{Type: v1.NodeReady, Status: v1.ConditionUnknown},
		}
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// TestNodeDaemonLaunchesToleratePod 验证带容忍度的 Pod 可在无污点节点上创建。
func TestNodeDaemonLaunchesToleratePod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("tolerate")
		ds.Spec.UpdateStrategy = *strategy
		setDaemonSetToleration(ds, noScheduleTolerations)
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		addNodes(manager.nodeStore, 0, 1, nil)
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}

		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// TestDaemonSetRespectsTermination 验证删除中的 Pod 不会被替代。
func TestDaemonSetRespectsTermination(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		addNodes(manager.nodeStore, 0, 1, simpleNodeLabel)
		pod := newPod(fmt.Sprintf("%s-", "node-0"), "node-0", simpleDaemonSetLabel, ds)
		dt := metav1.Now()
		pod.DeletionTimestamp = &dt
		err = manager.podStore.Add(pod)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	}
}

// setNodeTaint 设置节点污点，用于验证容忍度相关行为。
func setNodeTaint(node *v1.Node, taints []v1.Taint) {
	node.Spec.Taints = taints
}

// setDaemonSetToleration 设置 DaemonSet 模板的容忍度。
func setDaemonSetToleration(ds *apps.DaemonSet, tolerations []v1.Toleration) {
	ds.Spec.Template.Spec.Tolerations = tolerations
}

// TestTaintPressureNodeDaemonLaunchesPod 验证系统关键 DaemonSet 在节点压力污点下仍创建 Pod。
func TestTaintPressureNodeDaemonLaunchesPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("critical")
		ds.Spec.UpdateStrategy = *strategy
		setDaemonSetCritical(ds)
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}

		node := newNode("resources-pressure", nil)
		node.Status.Conditions = []v1.NodeCondition{
			{Type: v1.NodeDiskPressure, Status: v1.ConditionTrue},
			{Type: v1.NodeMemoryPressure, Status: v1.ConditionTrue},
			{Type: v1.NodePIDPressure, Status: v1.ConditionTrue},
		}
		node.Spec.Taints = []v1.Taint{
			{Key: v1.TaintNodeDiskPressure, Effect: v1.TaintEffectNoSchedule},
			{Key: v1.TaintNodeMemoryPressure, Effect: v1.TaintEffectNoSchedule},
			{Key: v1.TaintNodePIDPressure, Effect: v1.TaintEffectNoSchedule},
		}
		err = manager.nodeStore.Add(node)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	}
}

// setDaemonSetCritical 把 DaemonSet 标记为系统关键，使其容忍节点压力污点。
func setDaemonSetCritical(ds *apps.DaemonSet) {
	ds.Namespace = api.NamespaceSystem
	if ds.Spec.Template.ObjectMeta.Annotations == nil {
		ds.Spec.Template.ObjectMeta.Annotations = make(map[string]string)
	}
	podPriority := scheduling.SystemCriticalPriority
	ds.Spec.Template.Spec.Priority = &podPriority
}

// TestNodeShouldRunDaemonPod 覆盖节点是否应运行 Daemon Pod 的各类判定场景。
func TestNodeShouldRunDaemonPod(t *testing.T) {
	shouldRun := true
	shouldContinueRunning := true
	cases := []struct {
		predicateName                    string
		podsOnNode                       []*v1.Pod
		nodeCondition                    []v1.NodeCondition
		nodeUnschedulable                bool
		ds                               *apps.DaemonSet
		shouldRun, shouldContinueRunning bool
	}{
		{
			predicateName: "ShouldRunDaemonPod",
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: resourcePodSpec("", "50M", "0.5"),
					},
				},
			},
			shouldRun:             true,
			shouldContinueRunning: true,
		},
		{
			predicateName: "InsufficientResourceError",
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: resourcePodSpec("", "200M", "0.5"),
					},
				},
			},
			shouldRun:             shouldRun,
			shouldContinueRunning: true,
		},
		{
			predicateName: "ErrPodNotMatchHostName",
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: resourcePodSpec("other-node", "50M", "0.5"),
					},
				},
			},
			shouldRun:             false,
			shouldContinueRunning: false,
		},
		{
			predicateName: "ErrPodNotFitsHostPorts",
			podsOnNode: []*v1.Pod{
				{
					Spec: v1.PodSpec{
						Containers: []v1.Container{{
							Ports: []v1.ContainerPort{{
								HostPort: 666,
							}},
						}},
					},
				},
			},
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{{
								Ports: []v1.ContainerPort{{
									HostPort: 666,
								}},
							}},
						},
					},
				},
			},
			shouldRun:             shouldRun,
			shouldContinueRunning: shouldContinueRunning,
		},
		{
			predicateName: "InsufficientResourceError",
			podsOnNode: []*v1.Pod{
				{
					Spec: v1.PodSpec{
						Containers: []v1.Container{{
							Ports: []v1.ContainerPort{{
								HostPort: 666,
							}},
							Resources: resourceContainerSpec("50M", "0.5"),
						}},
					},
				},
			},
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: resourcePodSpec("", "100M", "0.5"),
					},
				},
			},
			shouldRun:             shouldRun, // 资源约束交由默认调度器处理，此处只验证节点判定
			shouldContinueRunning: true,
		},
		{
			predicateName: "ShouldRunDaemonPod",
			podsOnNode: []*v1.Pod{
				{
					Spec: v1.PodSpec{
						Containers: []v1.Container{{
							Ports: []v1.ContainerPort{{
								HostPort: 666,
							}},
							Resources: resourceContainerSpec("50M", "0.5"),
						}},
					},
				},
			},
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: resourcePodSpec("", "50M", "0.5"),
					},
				},
			},
			shouldRun:             true,
			shouldContinueRunning: true,
		},
		{
			predicateName: "ErrNodeSelectorNotMatch",
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: v1.PodSpec{
							NodeSelector: simpleDaemonSetLabel2,
						},
					},
				},
			},
			shouldRun:             false,
			shouldContinueRunning: false,
		},
		{
			predicateName: "ShouldRunDaemonPod",
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: v1.PodSpec{
							NodeSelector: simpleDaemonSetLabel,
						},
					},
				},
			},
			shouldRun:             true,
			shouldContinueRunning: true,
		},
		{
			predicateName: "ErrPodAffinityNotMatch",
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: v1.PodSpec{
							Affinity: &v1.Affinity{
								NodeAffinity: &v1.NodeAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
										NodeSelectorTerms: []v1.NodeSelectorTerm{
											{
												MatchExpressions: []v1.NodeSelectorRequirement{
													{
														Key:      "type",
														Operator: v1.NodeSelectorOpIn,
														Values:   []string{"test"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			shouldRun:             false,
			shouldContinueRunning: false,
		},
		{
			predicateName: "ShouldRunDaemonPod",
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: v1.PodSpec{
							Affinity: &v1.Affinity{
								NodeAffinity: &v1.NodeAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
										NodeSelectorTerms: []v1.NodeSelectorTerm{
											{
												MatchExpressions: []v1.NodeSelectorRequirement{
													{
														Key:      "type",
														Operator: v1.NodeSelectorOpIn,
														Values:   []string{"production"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			shouldRun:             true,
			shouldContinueRunning: true,
		},
		{
			predicateName: "ShouldRunDaemonPodOnUnschedulableNode",
			ds: &apps.DaemonSet{
				Spec: apps.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: simpleDaemonSetLabel},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: simpleDaemonSetLabel,
						},
						Spec: resourcePodSpec("", "50M", "0.5"),
					},
				},
			},
			nodeUnschedulable:     true,
			shouldRun:             true,
			shouldContinueRunning: true,
		},
	}

	for i, c := range cases {
		for _, strategy := range updateStrategies() {
			node := newNode("test-node", simpleDaemonSetLabel)
			node.Status.Conditions = append(node.Status.Conditions, c.nodeCondition...)
			node.Status.Allocatable = allocatableResources("100M", "1")
			node.Spec.Unschedulable = c.nodeUnschedulable
			logger, ctx := ktesting.NewTestContext(t)
			manager, _, _, err := newTestController(ctx)
			if err != nil {
				t.Fatalf("error creating DaemonSets controller: %v", err)
			}
			manager.nodeStore.Add(node)
			for _, p := range c.podsOnNode {
				p.Spec.NodeName = "test-node"
				manager.podStore.Add(p)
			}
			c.ds.Spec.UpdateStrategy = *strategy
			shouldRun, shouldContinueRunning := NodeShouldRunDaemonPod(logger, node, c.ds)

			if shouldRun != c.shouldRun {
				t.Errorf("[%v] strategy: %v, predicateName: %v expected shouldRun: %v, got: %v", i, c.ds.Spec.UpdateStrategy.Type, c.predicateName, c.shouldRun, shouldRun)
			}
			if shouldContinueRunning != c.shouldContinueRunning {
				t.Errorf("[%v] strategy: %v, predicateName: %v expected shouldContinueRunning: %v, got: %v", i, c.ds.Spec.UpdateStrategy.Type, c.predicateName, c.shouldContinueRunning, shouldContinueRunning)
			}
		}
	}
}

// TestUpdateNode 验证节点标签或污点变化会触发 DaemonSet 重新同步。
func TestUpdateNode(t *testing.T) {
	var enqueued bool
	cases := []struct {
		test               string
		newNode            *v1.Node
		oldNode            *v1.Node
		ds                 *apps.DaemonSet
		expectedEventsFunc func(strategyType apps.DaemonSetUpdateStrategyType) int
		shouldEnqueue      bool
		expectedCreates    func() int
		// 标记节点变为 Ready 前其上是否已有 Daemon Pod，用于验证节点恢复后的处理
		preExistingPod bool
	}{
		{
			test:    "Nothing changed, should not enqueue",
			oldNode: newNode("node1", nil),
			newNode: newNode("node1", nil),
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("ds")
				ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
				return ds
			}(),
			shouldEnqueue:   false,
			expectedCreates: func() int { return 0 },
			preExistingPod:  false,
		},
		{
			test:    "Node labels changed",
			oldNode: newNode("node1", nil),
			newNode: newNode("node1", simpleNodeLabel),
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("ds")
				ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
				return ds
			}(),
			shouldEnqueue:   true,
			expectedCreates: func() int { return 0 },
			preExistingPod:  false,
		},
		{
			test: "Node taints changed",
			oldNode: func() *v1.Node {
				node := newNode("node1", nil)
				setNodeTaint(node, noScheduleTaints)
				return node
			}(),
			newNode:         newNode("node1", nil),
			ds:              newDaemonSet("ds"),
			shouldEnqueue:   true,
			expectedCreates: func() int { return 0 },
			preExistingPod:  false,
		},
		{
			test:    "Node Allocatable changed",
			oldNode: newNode("node1", nil),
			newNode: func() *v1.Node {
				node := newNode("node1", nil)
				node.Status.Allocatable = allocatableResources("200M", "200m")
				return node
			}(),
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("ds")
				ds.Spec.Template.Spec = resourcePodSpecWithoutNodeName("200M", "200m")
				return ds
			}(),
			expectedEventsFunc: func(strategyType apps.DaemonSetUpdateStrategyType) int {
				switch strategyType {
				case apps.OnDeleteDaemonSetStrategyType:
					return 0
				case apps.RollingUpdateDaemonSetStrategyType:
					return 0
				default:
					t.Fatalf("unexpected UpdateStrategy %+v", strategyType)
				}
				return 0
			},
			shouldEnqueue: false,
			expectedCreates: func() int {
				return 1
			},
			preExistingPod: false,
		},
		{
			test:    "Pod transitions from misscheduled to scheduled due to node label change",
			oldNode: newNode("node1", nil),
			newNode: newNode("node1", simpleNodeLabel),
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("ds")
				ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
				ds.Status.NumberMisscheduled = 1
				return ds
			}(),
			shouldEnqueue:   true,
			expectedCreates: func() int { return 0 },
			preExistingPod:  true,
		},
	}
	for _, c := range cases {
		for _, strategy := range updateStrategies() {
			logger, ctx := ktesting.NewTestContext(t)
			manager, podControl, _, err := newTestController(ctx)
			if err != nil {
				t.Fatalf("error creating DaemonSets controller: %v", err)
			}
			err = manager.nodeStore.Add(c.oldNode)
			if err != nil {
				t.Fatal(err)
			}
			c.ds.Spec.UpdateStrategy = *strategy
			err = manager.dsStore.Add(c.ds)
			if err != nil {
				t.Fatal(err)
			}

			manager.nodeUpdateQueue = workqueue.NewTypedRateLimitingQueueWithConfig(
				workqueue.DefaultTypedControllerRateLimiter[string](),
				workqueue.TypedRateLimitingQueueConfig[string]{
					Name: "test-daemon-node-updates",
				},
			)

			expectedEvents := 0
			if c.expectedEventsFunc != nil {
				expectedEvents = c.expectedEventsFunc(strategy.Type)
			}
			expectedCreates := 0
			if c.expectedCreates != nil {
				expectedCreates = c.expectedCreates()
			}
			expectSyncDaemonSets(t, manager, c.ds, podControl, expectedCreates, 0, expectedEvents)

			if c.preExistingPod {
				addPods(manager.podStore, c.oldNode.Name, simpleDaemonSetLabel, c.ds, 1)
			}

			manager.enqueueDaemonSet = func(ds *apps.DaemonSet) {
				if ds.Name == "ds" {
					enqueued = true
				}
			}

			err = manager.nodeStore.Add(c.newNode)
			if err != nil {
				t.Fatal(err)
			}

			enqueued = false
			manager.updateNode(logger, c.oldNode, c.newNode)

			nodeKeys := getQueuedKeys(manager.nodeUpdateQueue)
			for _, key := range nodeKeys {
				manager.syncNodeUpdate(ctx, key)
			}
			if enqueued != c.shouldEnqueue {
				t.Errorf("Test case: '%s', expected: %t, got: %t", c.test, c.shouldEnqueue, enqueued)
			}
		}
	}
}

// TestDeleteNoDaemonPod 验证非 Daemon Pod 删除不会触发 DaemonSet 同步。
func TestDeleteNoDaemonPod(t *testing.T) {
	var enqueued bool

	cases := []struct {
		test          string
		node          *v1.Node
		existPods     []*v1.Pod
		deletedPod    *v1.Pod
		ds            *apps.DaemonSet
		shouldEnqueue bool
	}{
		{
			test: "Deleted non-daemon pods to release resources",
			node: func() *v1.Node {
				node := newNode("node1", nil)
				node.Status.Conditions = []v1.NodeCondition{
					{Type: v1.NodeReady, Status: v1.ConditionTrue},
				}
				node.Status.Allocatable = allocatableResources("200M", "200m")
				return node
			}(),
			existPods: func() []*v1.Pod {
				pods := []*v1.Pod{}
				for i := 0; i < 4; i++ {
					podSpec := resourcePodSpec("node1", "50M", "50m")
					pods = append(pods, &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name: fmt.Sprintf("pod_%d", i),
						},
						Spec: podSpec,
					})
				}
				return pods
			}(),
			deletedPod: func() *v1.Pod {
				podSpec := resourcePodSpec("node1", "50M", "50m")
				return &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod_0",
					},
					Spec: podSpec,
				}
			}(),
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("ds")
				ds.Spec.Template.Spec = resourcePodSpec("", "50M", "50m")
				return ds
			}(),
			shouldEnqueue: false,
		},
		{
			test: "Deleted non-daemon pods (with controller) to release resources",
			node: func() *v1.Node {
				node := newNode("node1", nil)
				node.Status.Conditions = []v1.NodeCondition{
					{Type: v1.NodeReady, Status: v1.ConditionTrue},
				}
				node.Status.Allocatable = allocatableResources("200M", "200m")
				return node
			}(),
			existPods: func() []*v1.Pod {
				pods := []*v1.Pod{}
				for i := 0; i < 4; i++ {
					podSpec := resourcePodSpec("node1", "50M", "50m")
					pods = append(pods, &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name: fmt.Sprintf("pod_%d", i),
							OwnerReferences: []metav1.OwnerReference{
								{Controller: func() *bool { res := true; return &res }()},
							},
						},
						Spec: podSpec,
					})
				}
				return pods
			}(),
			deletedPod: func() *v1.Pod {
				podSpec := resourcePodSpec("node1", "50M", "50m")
				return &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod_0",
						OwnerReferences: []metav1.OwnerReference{
							{Controller: func() *bool { res := true; return &res }()},
						},
					},
					Spec: podSpec,
				}
			}(),
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("ds")
				ds.Spec.Template.Spec = resourcePodSpec("", "50M", "50m")
				return ds
			}(),
			shouldEnqueue: false,
		},
		{
			test: "Deleted no scheduled pods",
			node: func() *v1.Node {
				node := newNode("node1", nil)
				node.Status.Conditions = []v1.NodeCondition{
					{Type: v1.NodeReady, Status: v1.ConditionTrue},
				}
				node.Status.Allocatable = allocatableResources("200M", "200m")
				return node
			}(),
			existPods: func() []*v1.Pod {
				pods := []*v1.Pod{}
				for i := 0; i < 4; i++ {
					podSpec := resourcePodSpec("node1", "50M", "50m")
					pods = append(pods, &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name: fmt.Sprintf("pod_%d", i),
							OwnerReferences: []metav1.OwnerReference{
								{Controller: func() *bool { res := true; return &res }()},
							},
						},
						Spec: podSpec,
					})
				}
				return pods
			}(),
			deletedPod: func() *v1.Pod {
				podSpec := resourcePodSpec("", "50M", "50m")
				return &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod_5",
					},
					Spec: podSpec,
				}
			}(),
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("ds")
				ds.Spec.Template.Spec = resourcePodSpec("", "50M", "50m")
				return ds
			}(),
			shouldEnqueue: false,
		},
	}

	for _, c := range cases {
		for _, strategy := range updateStrategies() {
			logger, ctx := ktesting.NewTestContext(t)
			manager, podControl, _, err := newTestController(ctx)
			if err != nil {
				t.Fatalf("error creating DaemonSets controller: %v", err)
			}
			err = manager.nodeStore.Add(c.node)
			if err != nil {
				t.Fatal(err)
			}
			c.ds.Spec.UpdateStrategy = *strategy
			err = manager.dsStore.Add(c.ds)
			if err != nil {
				t.Fatal(err)
			}
			for _, pod := range c.existPods {
				err = manager.podStore.Add(pod)
				if err != nil {
					t.Fatal(err)
				}
			}
			switch strategy.Type {
			case apps.OnDeleteDaemonSetStrategyType, apps.RollingUpdateDaemonSetStrategyType:
				expectSyncDaemonSets(t, manager, c.ds, podControl, 1, 0, 0)
			default:
				t.Fatalf("unexpected UpdateStrategy %+v", strategy)
			}

			enqueued = false
			manager.deletePod(logger, c.deletedPod)
			if enqueued != c.shouldEnqueue {
				t.Errorf("Test case: '%s', expected: %t, got: %t", c.test, c.shouldEnqueue, enqueued)
			}
		}
	}
}

// TestDeleteUnscheduledPodForNotExistingNode 验证节点不存在时未调度 Pod 能被正确入队处理。
func TestDeleteUnscheduledPodForNotExistingNode(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		_, ctx := ktesting.NewTestContext(t)
		manager, podControl, _, err := newTestController(ctx, ds)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		err = manager.dsStore.Add(ds)
		if err != nil {
			t.Fatal(err)
		}
		addNodes(manager.nodeStore, 0, 1, nil)
		addPods(manager.podStore, "node-0", simpleDaemonSetLabel, ds, 1)
		addPods(manager.podStore, "node-1", simpleDaemonSetLabel, ds, 1)

		podScheduledUsingAffinity := newPod("pod1-node-3", "", simpleDaemonSetLabel, ds)
		podScheduledUsingAffinity.Spec.Affinity = &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchFields: []v1.NodeSelectorRequirement{
								{
									Key:      metav1.ObjectNameField,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"node-2"},
								},
							},
						},
					},
				},
			},
		}
		err = manager.podStore.Add(podScheduledUsingAffinity)
		if err != nil {
			t.Fatal(err)
		}
		expectSyncDaemonSets(t, manager, ds, podControl, 0, 1, 0)
	}
}

// TestGetNodesToDaemonPods 验证节点到 Daemon Pod 映射能按 Pod 变更及时更新。
func TestGetNodesToDaemonPods(t *testing.T) {
	ds := newDaemonSet("foo")
	ds2 := newDaemonSet("foo2")
	cases := map[string]struct {
		includeDeletedTerminal bool
		wantedPods             []*v1.Pod
		ignoredPods            []*v1.Pod
	}{
		"exclude deleted terminal pods": {
			wantedPods: []*v1.Pod{
				newPod("matching-owned-0-", "node-0", simpleDaemonSetLabel, ds),
				newPod("matching-orphan-0-", "node-0", simpleDaemonSetLabel, nil),
				newPod("matching-owned-1-", "node-1", simpleDaemonSetLabel, ds),
				newPod("matching-orphan-1-", "node-1", simpleDaemonSetLabel, nil),
				func() *v1.Pod {
					pod := newPod("matching-owned-succeeded-pod-0-", "node-0", simpleDaemonSetLabel, ds)
					pod.Status = v1.PodStatus{Phase: v1.PodSucceeded}
					return pod
				}(),
				func() *v1.Pod {
					pod := newPod("matching-owned-failed-pod-1-", "node-1", simpleDaemonSetLabel, ds)
					pod.Status = v1.PodStatus{Phase: v1.PodFailed}
					return pod
				}(),
			},
			ignoredPods: []*v1.Pod{
				newPod("non-matching-owned-0-", "node-0", simpleDaemonSetLabel2, ds),
				newPod("non-matching-orphan-1-", "node-1", simpleDaemonSetLabel2, nil),
				newPod("matching-owned-by-other-0-", "node-0", simpleDaemonSetLabel, ds2),
				func() *v1.Pod {
					pod := newPod("matching-owned-succeeded-deleted-pod-0-", "node-0", simpleDaemonSetLabel, ds)
					now := metav1.Now()
					pod.DeletionTimestamp = &now
					pod.Status = v1.PodStatus{Phase: v1.PodSucceeded}
					return pod
				}(),
				func() *v1.Pod {
					pod := newPod("matching-owned-failed-deleted-pod-1-", "node-1", simpleDaemonSetLabel, ds)
					now := metav1.Now()
					pod.DeletionTimestamp = &now
					pod.Status = v1.PodStatus{Phase: v1.PodFailed}
					return pod
				}(),
			},
		},
		"include deleted terminal pods": {
			includeDeletedTerminal: true,
			wantedPods: []*v1.Pod{
				newPod("matching-owned-0-", "node-0", simpleDaemonSetLabel, ds),
				newPod("matching-orphan-0-", "node-0", simpleDaemonSetLabel, nil),
				newPod("matching-owned-1-", "node-1", simpleDaemonSetLabel, ds),
				newPod("matching-orphan-1-", "node-1", simpleDaemonSetLabel, nil),
				func() *v1.Pod {
					pod := newPod("matching-owned-succeeded-pod-0-", "node-0", simpleDaemonSetLabel, ds)
					pod.Status = v1.PodStatus{Phase: v1.PodSucceeded}
					return pod
				}(),
				func() *v1.Pod {
					pod := newPod("matching-owned-failed-deleted-pod-1-", "node-1", simpleDaemonSetLabel, ds)
					now := metav1.Now()
					pod.DeletionTimestamp = &now
					pod.Status = v1.PodStatus{Phase: v1.PodFailed}
					return pod
				}(),
			},
			ignoredPods: []*v1.Pod{
				newPod("non-matching-owned-0-", "node-0", simpleDaemonSetLabel2, ds),
				newPod("non-matching-orphan-1-", "node-1", simpleDaemonSetLabel2, nil),
				newPod("matching-owned-by-other-0-", "node-0", simpleDaemonSetLabel, ds2),
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			manager, _, _, err := newTestController(ctx, ds, ds2)
			if err != nil {
				t.Fatalf("error creating DaemonSets controller: %v", err)
			}
			err = manager.dsStore.Add(ds)
			if err != nil {
				t.Fatal(err)
			}
			err = manager.dsStore.Add(ds2)
			if err != nil {
				t.Fatal(err)
			}
			addNodes(manager.nodeStore, 0, 2, nil)

			for _, pod := range tc.wantedPods {
				manager.podStore.Add(pod)
			}

			for _, pod := range tc.ignoredPods {
				err = manager.podStore.Add(pod)
				if err != nil {
					t.Fatal(err)
				}
			}

			nodesToDaemonPods, err := manager.getNodesToDaemonPods(context.TODO(), ds, tc.includeDeletedTerminal)
			if err != nil {
				t.Fatalf("getNodesToDaemonPods() error: %v", err)
			}
			gotPods := map[string]bool{}
			for node, pods := range nodesToDaemonPods {
				for _, pod := range pods {
					if pod.Spec.NodeName != node {
						t.Errorf("pod %v grouped into %v but belongs in %v", pod.Name, node, pod.Spec.NodeName)
					}
					gotPods[pod.Name] = true
				}
			}
			for _, pod := range tc.wantedPods {
				if !gotPods[pod.Name] {
					t.Errorf("expected pod %v but didn't get it", pod.Name)
				}
				delete(gotPods, pod.Name)
			}
			for podName := range gotPods {
				t.Errorf("unexpected pod %v was returned", podName)
			}
		})
	}
}

// TestAddNode 验证节点添加事件会触发相关 DaemonSet 入队。
func TestAddNode(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	manager, _, _, err := newTestController(ctx)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	node1 := newNode("node1", nil)
	ds := newDaemonSet("ds")
	ds.Spec.Template.Spec.NodeSelector = simpleNodeLabel
	err = manager.dsStore.Add(ds)
	if err != nil {
		t.Fatal(err)
	}
	manager.addNode(logger, node1)
	if got, want := manager.nodeUpdateQueue.Len(), 1; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
	key, done := manager.nodeUpdateQueue.Get()
	if done {
		t.Fatal("failed to get item from nodeUpdateQueue")
	}
	if key != node1.Name {
		t.Fatalf("expected node name %v, got %v", node1.Name, key)
	}
	manager.nodeUpdateQueue.Done(key)

	node2 := newNode("node2", simpleNodeLabel)
	manager.addNode(logger, node2)
	if got, want := manager.nodeUpdateQueue.Len(), 1; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
	key, done = manager.nodeUpdateQueue.Get()
	if done {
		t.Fatal("failed to get item from nodeUpdateQueue")
	}
	if key != node2.Name {
		t.Fatalf("expected node name %v, got %v", node2.Name, key)
	}
}

// TestAddPod 验证 Daemon Pod 添加事件会触发所属 DaemonSet 入队。
func TestAddPod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds1 := newDaemonSet("foo1")
		ds1.Spec.UpdateStrategy = *strategy
		ds2 := newDaemonSet("foo2")
		ds2.Spec.UpdateStrategy = *strategy
		err = manager.dsStore.Add(ds1)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds2)
		if err != nil {
			t.Fatal(err)
		}
		pod1 := newPod("pod1-", "node-0", simpleDaemonSetLabel, ds1)
		manager.addPod(logger, pod1)
		if got, want := manager.queue.Len(), 1; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
		key, done := manager.queue.Get()
		if key == "" || done {
			t.Fatalf("failed to enqueue controller for pod %v", pod1.Name)
		}
		expectedKey, _ := controller.KeyFunc(ds1)
		if got, want := key, expectedKey; got != want {
			t.Errorf("queue.Get() = %v, want %v", got, want)
		}

		pod2 := newPod("pod2-", "node-0", simpleDaemonSetLabel, ds2)
		manager.addPod(logger, pod2)
		if got, want := manager.queue.Len(), 1; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
		key, done = manager.queue.Get()
		if key == "" || done {
			t.Fatalf("failed to enqueue controller for pod %v", pod2.Name)
		}
		expectedKey, _ = controller.KeyFunc(ds2)
		if got, want := key, expectedKey; got != want {
			t.Errorf("queue.Get() = %v, want %v", got, want)
		}
	}
}

// TestAddPodOrphan 验证无主 Pod 的添加事件不会入队 DaemonSet。
func TestAddPodOrphan(t *testing.T) {
	for _, strategy := range updateStrategies() {
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds1 := newDaemonSet("foo1")
		ds1.Spec.UpdateStrategy = *strategy
		ds2 := newDaemonSet("foo2")
		ds2.Spec.UpdateStrategy = *strategy
		ds3 := newDaemonSet("foo3")
		ds3.Spec.UpdateStrategy = *strategy
		ds3.Spec.Selector.MatchLabels = simpleDaemonSetLabel2
		err = manager.dsStore.Add(ds1)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds2)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds3)
		if err != nil {
			t.Fatal(err)
		}

		// 移除 ControllerRef 使 Pod 成为孤儿，应入队匹配的 DaemonSet。
		pod := newPod("pod1-", "node-0", simpleDaemonSetLabel, nil)
		manager.addPod(logger, pod)
		if got, want := manager.queue.Len(), 2; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
		if got, want := getQueuedKeys(manager.queue), []string{"default/foo1", "default/foo2"}; !reflect.DeepEqual(got, want) {
			t.Errorf("getQueuedKeys() = %v, want %v", got, want)
		}
	}
}

// TestUpdatePod 验证 Daemon Pod 更新事件会触发所属 DaemonSet 入队。
func TestUpdatePod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds1 := newDaemonSet("foo1")
		ds1.Spec.UpdateStrategy = *strategy
		ds2 := newDaemonSet("foo2")
		ds2.Spec.UpdateStrategy = *strategy
		err = manager.dsStore.Add(ds1)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds2)
		if err != nil {
			t.Fatal(err)
		}

		pod1 := newPod("pod1-", "node-0", simpleDaemonSetLabel, ds1)
		prev := *pod1
		bumpResourceVersion(pod1)
		manager.updatePod(logger, &prev, pod1)
		if got, want := manager.queue.Len(), 1; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
		key, done := manager.queue.Get()
		if key == "" || done {
			t.Fatalf("failed to enqueue controller for pod %v", pod1.Name)
		}
		expectedKey, _ := controller.KeyFunc(ds1)
		if got, want := key, expectedKey; got != want {
			t.Errorf("queue.Get() = %v, want %v", got, want)
		}

		pod2 := newPod("pod2-", "node-0", simpleDaemonSetLabel, ds2)
		prev = *pod2
		bumpResourceVersion(pod2)
		manager.updatePod(logger, &prev, pod2)
		if got, want := manager.queue.Len(), 1; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
		key, done = manager.queue.Get()
		if key == "" || done {
			t.Fatalf("failed to enqueue controller for pod %v", pod2.Name)
		}
		expectedKey, _ = controller.KeyFunc(ds2)
		if got, want := key, expectedKey; got != want {
			t.Errorf("queue.Get() = %v, want %v", got, want)
		}
	}
}

// TestUpdatePodOrphanSameLabels 验证标签不变的无主 Pod 更新不会入队 DaemonSet。
func TestUpdatePodOrphanSameLabels(t *testing.T) {
	for _, strategy := range updateStrategies() {
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds1 := newDaemonSet("foo1")
		ds1.Spec.UpdateStrategy = *strategy
		ds2 := newDaemonSet("foo2")
		ds2.Spec.UpdateStrategy = *strategy
		err = manager.dsStore.Add(ds1)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds2)
		if err != nil {
			t.Fatal(err)
		}

		pod := newPod("pod1-", "node-0", simpleDaemonSetLabel, nil)
		prev := *pod
		bumpResourceVersion(pod)
		manager.updatePod(logger, &prev, pod)
		if got, want := manager.queue.Len(), 0; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
	}
}

// TestUpdatePodOrphanWithNewLabels 验证标签变为匹配的无主 Pod 更新会入队 DaemonSet。
func TestUpdatePodOrphanWithNewLabels(t *testing.T) {
	for _, strategy := range updateStrategies() {
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds1 := newDaemonSet("foo1")
		ds1.Spec.UpdateStrategy = *strategy
		ds2 := newDaemonSet("foo2")
		ds2.Spec.UpdateStrategy = *strategy
		err = manager.dsStore.Add(ds1)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds2)
		if err != nil {
			t.Fatal(err)
		}

		pod := newPod("pod1-", "node-0", simpleDaemonSetLabel, nil)
		prev := *pod
		prev.Labels = map[string]string{"foo2": "bar2"}
		bumpResourceVersion(pod)
		manager.updatePod(logger, &prev, pod)
		if got, want := manager.queue.Len(), 2; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
		if got, want := getQueuedKeys(manager.queue), []string{"default/foo1", "default/foo2"}; !reflect.DeepEqual(got, want) {
			t.Errorf("getQueuedKeys() = %v, want %v", got, want)
		}
	}
}

// TestUpdatePodChangeControllerRef 验证 ControllerRef 变更后会同时入队新旧 DaemonSet。
func TestUpdatePodChangeControllerRef(t *testing.T) {
	for _, strategy := range updateStrategies() {
		ds := newDaemonSet("foo")
		ds.Spec.UpdateStrategy = *strategy
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds1 := newDaemonSet("foo1")
		ds2 := newDaemonSet("foo2")
		err = manager.dsStore.Add(ds1)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds2)
		if err != nil {
			t.Fatal(err)
		}

		pod := newPod("pod1-", "node-0", simpleDaemonSetLabel, ds1)
		prev := *pod
		prev.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(ds2, controllerKind)}
		bumpResourceVersion(pod)
		manager.updatePod(logger, &prev, pod)
		if got, want := manager.queue.Len(), 2; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
	}
}

// TestUpdatePodControllerRefRemoved 验证 ControllerRef 被移除后会入队原 DaemonSet。
func TestUpdatePodControllerRefRemoved(t *testing.T) {
	for _, strategy := range updateStrategies() {
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds1 := newDaemonSet("foo1")
		ds1.Spec.UpdateStrategy = *strategy
		ds2 := newDaemonSet("foo2")
		ds2.Spec.UpdateStrategy = *strategy
		err = manager.dsStore.Add(ds1)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds2)
		if err != nil {
			t.Fatal(err)
		}

		pod := newPod("pod1-", "node-0", simpleDaemonSetLabel, ds1)
		prev := *pod
		pod.OwnerReferences = nil
		bumpResourceVersion(pod)
		manager.updatePod(logger, &prev, pod)
		if got, want := manager.queue.Len(), 2; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
	}
}

// TestDeletePod 验证 Daemon Pod 删除事件会触发所属 DaemonSet 入队。
func TestDeletePod(t *testing.T) {
	for _, strategy := range updateStrategies() {
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds1 := newDaemonSet("foo1")
		ds1.Spec.UpdateStrategy = *strategy
		ds2 := newDaemonSet("foo2")
		ds2.Spec.UpdateStrategy = *strategy
		err = manager.dsStore.Add(ds1)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds2)
		if err != nil {
			t.Fatal(err)
		}

		pod1 := newPod("pod1-", "node-0", simpleDaemonSetLabel, ds1)
		manager.deletePod(logger, pod1)
		if got, want := manager.queue.Len(), 1; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
		key, done := manager.queue.Get()
		if key == "" || done {
			t.Fatalf("failed to enqueue controller for pod %v", pod1.Name)
		}
		expectedKey, _ := controller.KeyFunc(ds1)
		if got, want := key, expectedKey; got != want {
			t.Errorf("queue.Get() = %v, want %v", got, want)
		}

		pod2 := newPod("pod2-", "node-0", simpleDaemonSetLabel, ds2)
		manager.deletePod(logger, pod2)
		if got, want := manager.queue.Len(), 1; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
		key, done = manager.queue.Get()
		if key == "" || done {
			t.Fatalf("failed to enqueue controller for pod %v", pod2.Name)
		}
		expectedKey, _ = controller.KeyFunc(ds2)
		if got, want := key, expectedKey; got != want {
			t.Errorf("queue.Get() = %v, want %v", got, want)
		}
	}
}

// TestDeletePodOrphan 验证无主 Pod 的删除事件不会入队 DaemonSet。
func TestDeletePodOrphan(t *testing.T) {
	for _, strategy := range updateStrategies() {
		logger, ctx := ktesting.NewTestContext(t)
		manager, _, _, err := newTestController(ctx)
		if err != nil {
			t.Fatalf("error creating DaemonSets controller: %v", err)
		}
		ds1 := newDaemonSet("foo1")
		ds1.Spec.UpdateStrategy = *strategy
		ds2 := newDaemonSet("foo2")
		ds2.Spec.UpdateStrategy = *strategy
		ds3 := newDaemonSet("foo3")
		ds3.Spec.UpdateStrategy = *strategy
		ds3.Spec.Selector.MatchLabels = simpleDaemonSetLabel2
		err = manager.dsStore.Add(ds1)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds2)
		if err != nil {
			t.Fatal(err)
		}
		err = manager.dsStore.Add(ds3)
		if err != nil {
			t.Fatal(err)
		}

		pod := newPod("pod1-", "node-0", simpleDaemonSetLabel, nil)
		manager.deletePod(logger, pod)
		if got, want := manager.queue.Len(), 0; got != want {
			t.Fatalf("queue.Len() = %v, want %v", got, want)
		}
	}
}

// bumpResourceVersion 递增对象版本号，使更新事件被 informer 识别。
func bumpResourceVersion(obj metav1.Object) {
	ver, _ := strconv.ParseInt(obj.GetResourceVersion(), 10, 32)
	obj.SetResourceVersion(strconv.FormatInt(ver+1, 10))
}

// getQueuedKeys 返回队列中键的有序列表。
func getQueuedKeys(queue workqueue.TypedRateLimitingInterface[string]) []string {
	var keys []string
	count := queue.Len()
	for i := 0; i < count; i++ {
		key, done := queue.Get()
		if done {
			return keys
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestSurgeDealsWithExistingPods 验证滚动更新中已有 Pod 的节点不重复创建，多余 Pod 会被清理。
func TestSurgeDealsWithExistingPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt32(1))
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	manager.dsStore.Add(ds)
	addNodes(manager.nodeStore, 0, 5, nil)
	addPods(manager.podStore, "node-1", simpleDaemonSetLabel, ds, 1)
	addPods(manager.podStore, "node-2", simpleDaemonSetLabel, ds, 2)
	addPods(manager.podStore, "node-3", simpleDaemonSetLabel, ds, 5)
	addPods(manager.podStore, "node-4", simpleDaemonSetLabel2, ds, 2)
	expectSyncDaemonSets(t, manager, ds, podControl, 2, 5, 0)
}

// TestSurgePreservesReadyOldPods 验证滚动更新中就绪的旧 Pod 在超出 maxSurge 后才会被删除。
func TestSurgePreservesReadyOldPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt32(1))
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	manager.dsStore.Add(ds)
	addNodes(manager.nodeStore, 0, 5, nil)

	// 当前哈希的 Pod 必须保留。
	pod := newPod("node-1-", "node-1", simpleDaemonSetLabel, ds)
	pod.CreationTimestamp.Time = time.Unix(100, 0)
	manager.podStore.Add(pod)

	// 最旧且就绪的 Pod 应保留。
	pod = newPod("node-1-old-", "node-1", simpleDaemonSetLabel, ds)
	delete(pod.Labels, apps.ControllerRevisionHashLabelKey)
	pod.CreationTimestamp.Time = time.Unix(50, 0)
	pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	manager.podStore.Add(pod)

	// 非最旧的 Pod 即使就绪也应删除。
	oldReadyPod := newPod("node-1-delete-", "node-1", simpleDaemonSetLabel, ds)
	delete(oldReadyPod.Labels, apps.ControllerRevisionHashLabelKey)
	oldReadyPod.CreationTimestamp.Time = time.Unix(60, 0)
	oldReadyPod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	manager.podStore.Add(oldReadyPod)

	addPods(manager.podStore, "node-2", simpleDaemonSetLabel, ds, 1)
	expectSyncDaemonSets(t, manager, ds, podControl, 3, 1, 0)

	actual := sets.NewString(podControl.DeletePodName...)
	expected := sets.NewString(oldReadyPod.Name)
	if !actual.Equal(expected) {
		t.Errorf("unexpected deletes\nexpected: %v\n  actual: %v", expected.List(), actual.List())
	}
}

// TestSurgeCreatesNewPodWhenAtMaxSurgeAndOldPodDeleted 验证旧 Pod 被删除后即使已达 maxSurge 仍会补建新 Pod。
func TestSurgeCreatesNewPodWhenAtMaxSurgeAndOldPodDeleted(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt32(1))
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	manager.dsStore.Add(ds)
	addNodes(manager.nodeStore, 0, 5, nil)

	// 最新哈希且占用 surge 配额的 Pod 应保留。
	pod := newPod("node-0-", "node-0", simpleDaemonSetLabel, ds)
	pod.CreationTimestamp.Time = time.Unix(100, 0)
	pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionFalse}}
	manager.podStore.Add(pod)

	// 就绪的旧 Pod 应保留。
	oldPodReady := newPod("node-0-old-ready-", "node-0", simpleDaemonSetLabel, ds)
	delete(oldPodReady.Labels, apps.ControllerRevisionHashLabelKey)
	oldPodReady.CreationTimestamp.Time = time.Unix(50, 0)
	oldPodReady.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	manager.podStore.Add(oldPodReady)

	// 在其他节点创建旧的就绪 Pod。
	for i := 1; i < 5; i++ {
		oldPod := newPod(fmt.Sprintf("node-%d-preserve-", i), fmt.Sprintf("node-%d", i), simpleDaemonSetLabel, ds)
		delete(oldPod.Labels, apps.ControllerRevisionHashLabelKey)
		oldPod.CreationTimestamp.Time = time.Unix(1, 0)
		oldPod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
		manager.podStore.Add(oldPod)

		// 标记最后一个旧 Pod 为删除中，应触发超出 surge 的补建。
		if i == 4 {
			thirty := int64(30)
			timestamp := metav1.Time{Time: time.Unix(1+thirty, 0)}
			oldPod.DeletionGracePeriodSeconds = &thirty
			oldPod.DeletionTimestamp = &timestamp
		}
	}

	// 控制器应识别 node-4 只剩一个删除中的 Pod。
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	clearExpectations(t, manager, ds, podControl)
}

// TestSurgeDeletesUnreadyOldPods 验证未就绪的旧 Pod 会被优先删除以尽快完成更新。
func TestSurgeDeletesUnreadyOldPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt32(1))
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	manager.dsStore.Add(ds)
	addNodes(manager.nodeStore, 0, 5, nil)

	// 最新哈希的 Pod 应保留。
	pod := newPod("node-1-", "node-1", simpleDaemonSetLabel, ds)
	pod.CreationTimestamp.Time = time.Unix(100, 0)
	manager.podStore.Add(pod)

	// 未就绪的 Pod 应被优先删除。
	oldUnreadyPod := newPod("node-1-old-unready-", "node-1", simpleDaemonSetLabel, ds)
	delete(oldUnreadyPod.Labels, apps.ControllerRevisionHashLabelKey)
	oldUnreadyPod.CreationTimestamp.Time = time.Unix(50, 0)
	oldUnreadyPod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionFalse}}
	manager.podStore.Add(oldUnreadyPod)

	// 非最旧的 Pod 应被删除。
	oldReadyPod := newPod("node-1-delete-", "node-1", simpleDaemonSetLabel, ds)
	delete(oldReadyPod.Labels, apps.ControllerRevisionHashLabelKey)
	oldReadyPod.CreationTimestamp.Time = time.Unix(60, 0)
	oldReadyPod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	manager.podStore.Add(oldReadyPod)

	addPods(manager.podStore, "node-2", simpleDaemonSetLabel, ds, 1)
	expectSyncDaemonSets(t, manager, ds, podControl, 3, 2, 0)

	actual := sets.NewString(podControl.DeletePodName...)
	expected := sets.NewString(oldReadyPod.Name, oldUnreadyPod.Name)
	if !actual.Equal(expected) {
		t.Errorf("unexpected deletes\nexpected: %v\n  actual: %v", expected.List(), actual.List())
	}
}

// TestSurgePreservesOldReadyWithUnsatisfiedMinReady 验证 minReadySeconds 未满足时旧 Pod 不会被提前删除。
func TestSurgePreservesOldReadyWithUnsatisfiedMinReady(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	ds.Spec.MinReadySeconds = 15
	ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt32(1))
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	manager.dsStore.Add(ds)
	addNodes(manager.nodeStore, 0, 5, nil)

	// 时钟设为 node-1 最新 Pod 就绪后 10 秒，尚未满足 minReadySeconds。
	manager.DaemonSetsController.failedPodsBackoff.Clock = testingclock.NewFakeClock(time.Unix(50+10, 0))

	// 最新哈希的 Pod 应保留。
	pod := newPod("node-1-", "node-1", simpleDaemonSetLabel, ds)
	pod.CreationTimestamp.Time = time.Unix(100, 0)
	pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: time.Unix(50, 0)}}}
	manager.podStore.Add(pod)

	// 最新 Pod 就绪时间不足时，就绪的旧 Pod 应保留。
	oldReadyPod := newPod("node-1-old-ready-", "node-1", simpleDaemonSetLabel, ds)
	delete(oldReadyPod.Labels, apps.ControllerRevisionHashLabelKey)
	oldReadyPod.CreationTimestamp.Time = time.Unix(50, 0)
	oldReadyPod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	manager.podStore.Add(oldReadyPod)

	// 非最旧的 Pod 应被删除。
	oldExcessReadyPod := newPod("node-1-delete-", "node-1", simpleDaemonSetLabel, ds)
	delete(oldExcessReadyPod.Labels, apps.ControllerRevisionHashLabelKey)
	oldExcessReadyPod.CreationTimestamp.Time = time.Unix(60, 0)
	oldExcessReadyPod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	manager.podStore.Add(oldExcessReadyPod)

	addPods(manager.podStore, "node-2", simpleDaemonSetLabel, ds, 1)
	expectSyncDaemonSets(t, manager, ds, podControl, 3, 1, 0)

	actual := sets.NewString(podControl.DeletePodName...)
	expected := sets.NewString(oldExcessReadyPod.Name)
	if !actual.Equal(expected) {
		t.Errorf("unexpected deletes\nexpected: %v\n  actual: %v", expected.List(), actual.List())
	}
}

// TestSurgeDeletesOldReadyWithUnsatisfiedMinReady 验证 minReadySeconds 已满足时旧 Pod 会被滚动删除。
func TestSurgeDeletesOldReadyWithUnsatisfiedMinReady(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	ds.Spec.MinReadySeconds = 15
	ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt32(1))
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	manager.dsStore.Add(ds)
	addNodes(manager.nodeStore, 0, 5, nil)

	// 时钟设为 node-1 最新 Pod 就绪后 20 秒，已满足 minReadySeconds。
	manager.DaemonSetsController.failedPodsBackoff.Clock = testingclock.NewFakeClock(time.Unix(50+20, 0))

	// 最新哈希的 Pod 应保留。
	pod := newPod("node-1-", "node-1", simpleDaemonSetLabel, ds)
	pod.CreationTimestamp.Time = time.Unix(100, 0)
	pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: time.Unix(50, 0)}}}
	manager.podStore.Add(pod)

	// 最新 Pod 就绪时间不足时，就绪的旧 Pod 应保留。
	oldReadyPod := newPod("node-1-old-ready-", "node-1", simpleDaemonSetLabel, ds)
	delete(oldReadyPod.Labels, apps.ControllerRevisionHashLabelKey)
	oldReadyPod.CreationTimestamp.Time = time.Unix(50, 0)
	oldReadyPod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	manager.podStore.Add(oldReadyPod)

	// 非最旧的 Pod 应被删除。
	oldExcessReadyPod := newPod("node-1-delete-", "node-1", simpleDaemonSetLabel, ds)
	delete(oldExcessReadyPod.Labels, apps.ControllerRevisionHashLabelKey)
	oldExcessReadyPod.CreationTimestamp.Time = time.Unix(60, 0)
	oldExcessReadyPod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	manager.podStore.Add(oldExcessReadyPod)

	addPods(manager.podStore, "node-2", simpleDaemonSetLabel, ds, 1)
	expectSyncDaemonSets(t, manager, ds, podControl, 3, 2, 0)

	actual := sets.NewString(podControl.DeletePodName...)
	expected := sets.NewString(oldExcessReadyPod.Name, oldReadyPod.Name)
	if !actual.Equal(expected) {
		t.Errorf("unexpected deletes\nexpected: %v\n  actual: %v", expected.List(), actual.List())
	}
}

// TestStoreDaemonSetStatus 验证状态更新请求仅在状态变化时发出。
func TestStoreDaemonSetStatus(t *testing.T) {
	getError := fmt.Errorf("fake get error")
	updateError := fmt.Errorf("fake update error")
	tests := []struct {
		name                 string
		updateErrorNum       int
		getErrorNum          int
		expectedUpdateCalled int
		expectedGetCalled    int
		expectedError        error
	}{
		{
			name:                 "succeed immediately",
			updateErrorNum:       0,
			getErrorNum:          0,
			expectedUpdateCalled: 1,
			expectedGetCalled:    0,
			expectedError:        nil,
		},
		{
			name:                 "succeed after one update failure",
			updateErrorNum:       1,
			getErrorNum:          0,
			expectedUpdateCalled: 2,
			expectedGetCalled:    1,
			expectedError:        nil,
		},
		{
			name:                 "fail after two update failures",
			updateErrorNum:       2,
			getErrorNum:          0,
			expectedUpdateCalled: 2,
			expectedGetCalled:    1,
			expectedError:        updateError,
		},
		{
			name:                 "fail after one update failure and one get failure",
			updateErrorNum:       1,
			getErrorNum:          1,
			expectedUpdateCalled: 1,
			expectedGetCalled:    1,
			expectedError:        getError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := newDaemonSet("foo")
			fakeClient := &fake.Clientset{}
			getCalled := 0
			fakeClient.AddReactor("get", "daemonsets", func(action core.Action) (bool, runtime.Object, error) {
				getCalled += 1
				if getCalled <= tt.getErrorNum {
					return true, nil, getError
				}
				return true, ds, nil
			})
			updateCalled := 0
			fakeClient.AddReactor("update", "daemonsets", func(action core.Action) (bool, runtime.Object, error) {
				updateCalled += 1
				if updateCalled <= tt.updateErrorNum {
					return true, nil, updateError
				}
				return true, ds, nil
			})
			if _, err := storeDaemonSetStatus(context.TODO(), fakeClient.AppsV1().DaemonSets("default"), ds, 2, 2, 2, 2, 2, 2, 2, true); !errors.Is(err, tt.expectedError) {
				t.Errorf("storeDaemonSetStatus() got %v, expected %v", err, tt.expectedError)
			}
			if getCalled != tt.expectedGetCalled {
				t.Errorf("Get() was called %v times, expected %v times", getCalled, tt.expectedGetCalled)
			}
			if updateCalled != tt.expectedUpdateCalled {
				t.Errorf("UpdateStatus() was called %v times, expected %v times", updateCalled, tt.expectedUpdateCalled)
			}
		})
	}
}

// TestShouldIgnoreNodeUpdate 验证无关的节点字段变更不会触发 DaemonSet 同步。
func TestShouldIgnoreNodeUpdate(t *testing.T) {
	cases := []struct {
		name           string
		newNode        *v1.Node
		oldNode        *v1.Node
		expectedResult bool
	}{
		{
			name:           "Nothing changed",
			oldNode:        newNode("node1", nil),
			newNode:        newNode("node1", nil),
			expectedResult: true,
		},
		{
			name:           "Node labels changed",
			oldNode:        newNode("node1", nil),
			newNode:        newNode("node1", simpleNodeLabel),
			expectedResult: false,
		},
		{
			name: "Node taints changed",
			oldNode: func() *v1.Node {
				node := newNode("node1", nil)
				setNodeTaint(node, noScheduleTaints)
				return node
			}(),
			newNode:        newNode("node1", nil),
			expectedResult: false,
		},
	}

	for _, c := range cases {
		result := shouldIgnoreNodeUpdate(*c.oldNode, *c.newNode)

		if result != c.expectedResult {
			t.Errorf("[%s] unexpected results: %v", c.name, result)
		}
	}
}
