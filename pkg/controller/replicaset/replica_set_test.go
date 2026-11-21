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

package replicaset

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	utiltesting "k8s.io/client-go/util/testing"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	. "k8s.io/kubernetes/pkg/controller/testutil"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/securitycontext"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

// 测试中使用的公共常量与变量。
var (
	informerSyncTimeout = 30 * time.Second
)

// testNewReplicaSetControllerFromClient 用给定的客户端构造 ReplicaSetController，
// 并将 informer 的同步判定替换为 alwaysReady，让测试可以直接操作索引器而无需启动 informer。
func testNewReplicaSetControllerFromClient(tb testing.TB, client clientset.Interface, stopCh chan struct{}, burstReplicas int) (*ReplicaSetController, informers.SharedInformerFactory) {
	informers := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())

	tCtx := ktesting.Init(tb)
	ret := NewReplicaSetController(
		tCtx,
		informers.Apps().V1().ReplicaSets(),
		informers.Core().V1().Pods(),
		client,
		burstReplicas,
	)

	// 直接用索引器注入对象，跳过 informer 的实际同步等待。
	ret.podListerSynced = alwaysReady
	ret.rsListerSynced = alwaysReady

	return ret, informers
}

// skipListerFunc 判断哪些请求应由 FakeHandler 放行而不记录：
// list/watch 类请求由 informer 发起，测试只关心状态更新等写请求。
func skipListerFunc(verb string, url url.URL) bool {
	if verb != "GET" {
		return false
	}
	if strings.HasSuffix(url.Path, "/pods") || strings.Contains(url.Path, "/replicasets") {
		return true
	}
	return false
}

// alwaysReady 是测试中总是返回 true 的就绪判定函数。
var alwaysReady = func() bool { return true }

// newReplicaSet 构造一个测试用的 ReplicaSet，带固定的 UID、拥有者引用、
// 标签选择器和 Pod 模板，副本数由参数指定。
func newReplicaSet(replicas int, selectorMap map[string]string) *apps.ReplicaSet {
	isController := true
	rs := &apps.ReplicaSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ReplicaSet"},
		ObjectMeta: metav1.ObjectMeta{
			UID:       uuid.NewUUID(),
			Name:      "foobar",
			Namespace: metav1.NamespaceDefault,
			OwnerReferences: []metav1.OwnerReference{
				{UID: "123", Controller: &isController},
			},
			ResourceVersion: "18",
		},
		Spec: apps.ReplicaSetSpec{
			Replicas: ptr.To[int32](int32(replicas)),
			Selector: &metav1.LabelSelector{MatchLabels: selectorMap},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"name": "foo",
						"type": "production",
					},
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
					RestartPolicy: v1.RestartPolicyAlways,
					DNSPolicy:     v1.DNSDefault,
					NodeSelector: map[string]string{
						"baz": "blah",
					},
				},
			},
		},
	}
	return rs
}

// newPod 为给定 ReplicaSet 构造一个处于指定阶段的 Pod，
// 复用 RS 的选择器与命名空间；Running 状态会附带 Ready 条件。
// properlyOwned 为 true 时设置指向该 RS 的 ControllerRef。
func newPod(name string, rs *apps.ReplicaSet, status v1.PodPhase, lastTransitionTime *metav1.Time, properlyOwned bool) *v1.Pod {
	// 只有 Running 的 Pod 才设置 Ready=True，模拟正常服务的副本。
	var conditions []v1.PodCondition
	if status == v1.PodRunning {
		condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue}
		if lastTransitionTime != nil {
			condition.LastTransitionTime = *lastTransitionTime
		}
		conditions = append(conditions, condition)
	}
	// 测试拥有者归属逻辑时，需要区分是否正确设置了 ControllerRef。
	var controllerReference metav1.OwnerReference
	if properlyOwned {
		var trueVar = true
		controllerReference = metav1.OwnerReference{UID: rs.UID, APIVersion: "v1beta1", Kind: "ReplicaSet", Name: rs.Name, Controller: &trueVar}
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:             uuid.NewUUID(),
			Name:            name,
			Namespace:       rs.Namespace,
			Labels:          rs.Spec.Selector.MatchLabels,
			OwnerReferences: []metav1.OwnerReference{controllerReference},
		},
		Status: v1.PodStatus{Phase: status, Conditions: conditions},
	}
}

// newPodList 批量构造 count 个 Pod 并可选地加入 store，
// 它们均归属给定 ReplicaSet 并使用指定的标签，用于快速填充 informer 缓存。
func newPodList(store cache.Store, count int, status v1.PodPhase, labelMap map[string]string, rs *apps.ReplicaSet, name string) *v1.PodList {
	pods := []v1.Pod{}
	var trueVar = true
	controllerReference := metav1.OwnerReference{UID: rs.UID, APIVersion: "v1beta1", Kind: "ReplicaSet", Name: rs.Name, Controller: &trueVar}
	for i := 0; i < count; i++ {
		pod := newPod(fmt.Sprintf("%s%d", name, i), rs, status, nil, false)
		pod.ObjectMeta.Labels = labelMap
		// 即使 newPod 未设置归属，这里也统一补上指向 RS 的 ControllerRef。
		pod.OwnerReferences = []metav1.OwnerReference{controllerReference}
		// store 非空时同步写入缓存，便于后续 lister 查询。
		if store != nil {
			store.Add(pod)
		}
		pods = append(pods, *pod)
	}
	return &v1.PodList{
		Items: pods,
	}
}

// processSync 通过 processNextWorkItem 触发一次同步，并捕获 syncHandler 返回的错误。
// 用于验证依赖工作队列的行为（例如同步失败后的重新入队）。
func processSync(ctx context.Context, rsc *ReplicaSetController, key string) error {
	// 临时替换 syncHandler 以捕获错误，结束后恢复，避免影响后续测试。
	oldSyncHandler := rsc.syncHandler
	defer func() {
		rsc.syncHandler = oldSyncHandler
	}()
	var syncErr error
	rsc.syncHandler = func(ctx context.Context, key string) error {
		syncErr = oldSyncHandler(ctx, key)
		return syncErr
	}
	rsc.queue.Add(key)
	rsc.processNextWorkItem(ctx)
	return syncErr
}

// validateSyncReplicaSet 校验 FakePodControl 记录的创建、删除与补丁次数是否符合期望。
func validateSyncReplicaSet(fakePodControl *controller.FakePodControl, expectedCreates, expectedDeletes, expectedPatches int) error {
	if e, a := expectedCreates, len(fakePodControl.Templates); e != a {
		return fmt.Errorf("Unexpected number of creates.  Expected %d, saw %d\n", e, a)
	}

	if e, a := expectedDeletes, len(fakePodControl.DeletePodName); e != a {
		return fmt.Errorf("Unexpected number of deletes.  Expected %d, saw %d\n", e, a)
	}

	if e, a := expectedPatches, len(fakePodControl.Patches); e != a {
		return fmt.Errorf("Unexpected number of patches.  Expected %d, saw %d\n", e, a)
	}

	return nil
}

// TestSyncReplicaSetDoesNothing 验证副本数与实际 Pod 数一致时，同步不产生任何创建、删除或补丁操作。
func TestSyncReplicaSetDoesNothing(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	client := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakePodControl := controller.FakePodControl{}
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, BurstReplicas)

	// 2 个运行中 Pod 对应 2 个期望副本，同步应为空操作。
	labelMap := map[string]string{"foo": "bar"}
	rsSpec := newReplicaSet(2, labelMap)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rsSpec)
	newPodList(informers.Core().V1().Pods().Informer().GetIndexer(), 2, v1.PodRunning, labelMap, rsSpec, "pod")

	manager.podControl = &fakePodControl
	manager.syncReplicaSet(ctx, GetKey(rsSpec, t))
	err := validateSyncReplicaSet(&fakePodControl, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
}

// TestDeleteFinalStateUnknown 验证删除事件携带 DeletedFinalStateUnknown 时，
// 控制器仍能根据 Pod 标签找到匹配的 ReplicaSet 并入队同步。
func TestDeleteFinalStateUnknown(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	client := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakePodControl := controller.FakePodControl{}
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, BurstReplicas)
	manager.podControl = &fakePodControl

	received := make(chan string)
	manager.syncHandler = func(ctx context.Context, key string) error {
		received <- key
		return nil
	}

	// DeletedFinalStateUnknown 中的对象应让控制器按被删 Pod 的选择器找到对应 RS 并入队。
	labelMap := map[string]string{"foo": "bar"}
	rsSpec := newReplicaSet(1, labelMap)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rsSpec)
	pods := newPodList(nil, 1, v1.PodRunning, labelMap, rsSpec, "pod")
	manager.deletePod(logger, cache.DeletedFinalStateUnknown{Key: "foo", Obj: &pods.Items[0]})

	go manager.worker(ctx)

	expected := GetKey(rsSpec, t)
	select {
	case key := <-received:
		if key != expected {
			t.Errorf("Unexpected sync all for ReplicaSet %v, expected %v", key, expected)
		}
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("Processing DeleteFinalStateUnknown took longer than expected")
	}
}

// TestSyncReplicaSetCreateFailures 模拟配额限制（每次最多创建 10 个）：
// 要求创建 100 个副本时，单次同步不能发起 100 次创建调用，
// 且总调用次数受慢启动批量大小限制。
func TestSyncReplicaSetCreateFailures(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	fakePodControl := controller.FakePodControl{}
	fakePodControl.CreateLimit = 10

	labelMap := map[string]string{"foo": "bar"}
	rs := newReplicaSet(fakePodControl.CreateLimit*10, labelMap)
	client := fake.NewSimpleClientset(rs)
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, BurstReplicas)

	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)

	manager.podControl = &fakePodControl
	manager.syncReplicaSet(ctx, GetKey(rs, t))
	err := validateSyncReplicaSet(&fakePodControl, fakePodControl.CreateLimit, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	expectedLimit := 0
	for pass := uint8(0); expectedLimit <= fakePodControl.CreateLimit; pass++ {
		// 计算慢启动策略下允许的累计创建调用次数上限。
		expectedLimit += controller.SlowStartInitialBatchSize << pass
	}
	if fakePodControl.CreateCallCount > expectedLimit {
		t.Errorf("Unexpected number of create calls.  Expected <= %d, saw %d\n", fakePodControl.CreateLimit*2, fakePodControl.CreateCallCount)
	}
}

// TestSyncReplicaSetDormancy 验证创建期望（expectations）对控制器休眠的影响：
// 期望未满足时只更新状态不创建 Pod；期望降低后才继续创建；
// 创建失败留下的期望不会重复降低。
func TestSyncReplicaSetDormancy(t *testing.T) {
	// 使用测试服务器拦截请求，从而伪造 Pod 的当前状态并统计状态更新次数。
	logger, ctx := ktesting.NewTestContext(t)
	fakeHandler := utiltesting.FakeHandler{
		StatusCode:    200,
		ResponseBody:  "{}",
		SkipRequestFn: skipListerFunc,
		T:             t,
	}
	testServer := httptest.NewServer(&fakeHandler)
	defer testServer.Close()
	client := clientset.NewForConfigOrDie(&restclient.Config{Host: testServer.URL, ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})

	fakePodControl := controller.FakePodControl{}
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, BurstReplicas)

	manager.podControl = &fakePodControl

	labelMap := map[string]string{"foo": "bar"}
	rsSpec := newReplicaSet(2, labelMap)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rsSpec)
	newPodList(informers.Core().V1().Pods().Informer().GetIndexer(), 1, v1.PodRunning, labelMap, rsSpec, "pod")

	// 首次同步创建一个副本并设置期望。
	rsSpec.Status.Replicas = 1
	rsSpec.Status.ReadyReplicas = 1
	rsSpec.Status.AvailableReplicas = 1
	rsSpec.Status.TerminatingReplicas = ptr.To[int32](0)
	manager.syncReplicaSet(ctx, GetKey(rsSpec, t))
	err := validateSyncReplicaSet(&fakePodControl, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 期望未满足时不创建副本，但仍应更新状态。
	rsSpec.Status.Replicas = 0
	rsSpec.Status.ReadyReplicas = 0
	rsSpec.Status.AvailableReplicas = 0
	fakePodControl.Clear()
	manager.syncReplicaSet(ctx, GetKey(rsSpec, t))
	err = validateSyncReplicaSet(&fakePodControl, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 获取 RS 键，供后续操作期望值使用。
	rsKey, err := controller.KeyFunc(rsSpec)
	if err != nil {
		t.Errorf("Couldn't get key for object %#v: %v", rsSpec, err)
	}

	// 降低期望后本应创建副本，但注入的 podControl 错误会阻止创建，期望仍处于未满足状态。
	manager.expectations.CreationObserved(logger, rsKey)
	rsSpec.Status.Replicas = 1
	rsSpec.Status.ReadyReplicas = 1
	rsSpec.Status.AvailableReplicas = 1
	fakePodControl.Clear()
	fakePodControl.Err = fmt.Errorf("fake Error")

	manager.syncReplicaSet(ctx, GetKey(rsSpec, t))
	err = validateSyncReplicaSet(&fakePodControl, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 上一次创建失败未降低期望，这次同步应直接创建副本。
	fakePodControl.Clear()
	fakePodControl.Err = nil
	manager.syncReplicaSet(ctx, GetKey(rsSpec, t))
	err = validateSyncReplicaSet(&fakePodControl, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 休眠窗口内共有 2 次状态更新（PUT）；Pod 创建走 podControl，不会被测试服务器记录。
	fakeHandler.ValidateRequestCount(t, 2)
}

// TestGetReplicaSetsWithSameController 验证 getReplicaSetsWithSameController 只返回
// 目标 RS 自身及同一控制器（相同 Owner UID）的 RS，排除无关 RS。
func TestGetReplicaSetsWithSameController(t *testing.T) {
	someRS := newReplicaSet(1, map[string]string{"foo": "bar"})
	someRS.Name = "rs1"
	relatedRS := newReplicaSet(1, map[string]string{"foo": "baz"})
	relatedRS.Name = "rs2"
	unrelatedRS := newReplicaSet(1, map[string]string{"foo": "quux"})
	unrelatedRS.Name = "rs3"
	unrelatedRS.ObjectMeta.OwnerReferences[0].UID = "456"
	pendingDeletionRS := newReplicaSet(1, map[string]string{"foo": "xyzzy"})
	pendingDeletionRS.Name = "rs4"
	pendingDeletionRS.ObjectMeta.OwnerReferences[0].UID = "789"
	now := metav1.Now()
	pendingDeletionRS.DeletionTimestamp = &now
	logger, _ := ktesting.NewTestContext(t)

	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}}), stopCh, BurstReplicas)
	testCases := []struct {
		name        string
		rss         []*apps.ReplicaSet
		rs          *apps.ReplicaSet
		expectedRSs []*apps.ReplicaSet
	}{
		{
			name:        "expect to get back a ReplicaSet that is pending deletion",
			rss:         []*apps.ReplicaSet{pendingDeletionRS, unrelatedRS},
			rs:          pendingDeletionRS,
			expectedRSs: []*apps.ReplicaSet{pendingDeletionRS},
		},
		{
			name:        "expect to get back only the given ReplicaSet if there is no related ReplicaSet",
			rss:         []*apps.ReplicaSet{someRS, unrelatedRS},
			rs:          someRS,
			expectedRSs: []*apps.ReplicaSet{someRS},
		},
		{
			name:        "expect to get back the given ReplicaSet as well as any related ReplicaSet but not an unrelated ReplicaSet",
			rss:         []*apps.ReplicaSet{someRS, relatedRS, unrelatedRS},
			rs:          someRS,
			expectedRSs: []*apps.ReplicaSet{someRS, relatedRS},
		},
	}
	for _, c := range testCases {
		// 每个用例重建索引，避免上一个用例的 RS 残留造成干扰。
		for _, r := range c.rss {
			informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(r)
		}
		actualRSs := manager.getReplicaSetsWithSameController(logger, c.rs)
		var actualRSNames, expectedRSNames []string
		for _, r := range actualRSs {
			actualRSNames = append(actualRSNames, r.Name)
		}
		for _, r := range c.expectedRSs {
			expectedRSNames = append(expectedRSNames, r.Name)
		}
		// 排序后再比较，避免索引器返回顺序影响断言。
		sort.Strings(actualRSNames)
		sort.Strings(expectedRSNames)
		if !reflect.DeepEqual(actualRSNames, expectedRSNames) {
			t.Errorf("Got [%s]; expected [%s]", strings.Join(actualRSNames, ", "), strings.Join(expectedRSNames, ", "))
		}
	}
}

// BenchmarkGetReplicaSetsWithSameController 基准测试同控制器 RS 查询：
// 缓存中放入 1 个目标、1 个相关和 100 个无关 RS，验证查询只命中相关的 2 个。
func BenchmarkGetReplicaSetsWithSameController(b *testing.B) {
	stopCh := make(chan struct{})
	defer close(stopCh)
	controller, informers := testNewReplicaSetControllerFromClient(b, clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}}), stopCh, BurstReplicas)
	logger, _ := ktesting.NewTestContext(b)

	targetRS := newReplicaSet(1, map[string]string{"foo": "bar"})
	targetRS.Name = "rs1"
	targetRS.ObjectMeta.OwnerReferences[0].UID = "123456"
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(targetRS)
	relatedRS := newReplicaSet(1, map[string]string{"foo": "bar"})
	relatedRS.Name = "rs2"
	relatedRS.ObjectMeta.OwnerReferences[0].UID = "123456"
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(relatedRS)
	// 大量无关 RS 用于衡量查询是否会退化为全量扫描。
	for i := 0; i < 100; i++ {
		unrelatedRS := newReplicaSet(1, map[string]string{"foo": fmt.Sprintf("baz-%d", i)})
		unrelatedRS.Name = fmt.Sprintf("rs-%d", i)
		unrelatedRS.ObjectMeta.OwnerReferences[0].UID = types.UID(fmt.Sprintf("%d", i))
		informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(unrelatedRS)
	}

	b.ReportAllocs()
	// 计时从查询循环开始，排除夹具构建开销。
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		gotRSs := controller.getReplicaSetsWithSameController(logger, targetRS)
		if len(gotRSs) != 2 {
			b.Errorf("Incorrect ReplicaSets number, expected 2, got: %d", len(gotRSs))
		}
	}
}

// TestPodControllerLookup 验证 getPodReplicaSets 根据标签和命名空间查找管理 Pod 的 RS：
// 无标签、标签匹配但命名空间不匹配时不返回；两者都匹配时返回对应 RS。
func TestPodControllerLookup(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}}), stopCh, BurstReplicas)
	testCases := []struct {
		inRSs     []*apps.ReplicaSet
		pod       *v1.Pod
		outRSName string
	}{
		// 无标签的 Pod 不匹配任何 RS。
		{
			inRSs: []*apps.ReplicaSet{
				{ObjectMeta: metav1.ObjectMeta{Name: "basic"}}},
			pod:       &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: metav1.NamespaceAll}},
			outRSName: "",
		},
		// 标签匹配但命名空间不匹配，同样不应找到 RS。
		{
			inRSs: []*apps.ReplicaSet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "foo"},
					Spec: apps.ReplicaSetSpec{
						Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
					},
				},
			},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo2", Namespace: "ns", Labels: map[string]string{"foo": "bar"}}},
			outRSName: "",
		},
		// 命名空间与标签都匹配时返回 RS 的键（namespace/name）。
		{
			inRSs: []*apps.ReplicaSet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "bar", Namespace: "ns"},
					Spec: apps.ReplicaSetSpec{
						Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
					},
				},
			},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo3", Namespace: "ns", Labels: map[string]string{"foo": "bar"}}},
			outRSName: "bar",
		},
	}
	for _, c := range testCases {
		for _, r := range c.inRSs {
			informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(r)
		}
		if rss := manager.getPodReplicaSets(c.pod); rss != nil {
			if len(rss) != 1 {
				t.Errorf("len(rss) = %v, want %v", len(rss), 1)
				continue
			}
			rs := rss[0]
			if c.outRSName != rs.Name {
				t.Errorf("Got replica set %+v expected %+v", rs.Name, c.outRSName)
			}
		} else if c.outRSName != "" {
			t.Errorf("Expected a replica set %v pod %v, found none", c.outRSName, c.pod.Name)
		}
	}
}

// TestRelatedPodsLookup 验证 getIndirectlyRelatedPods 返回目标 RS 自身的 Pod
// 以及同一控制器下相关 RS 的 Pod，但不包含无关 RS 的 Pod。
func TestRelatedPodsLookup(t *testing.T) {
	someRS := newReplicaSet(1, map[string]string{"foo": "bar"})
	someRS.Name = "foo1"
	relatedRS := newReplicaSet(1, map[string]string{"foo": "baz"})
	relatedRS.Name = "foo2"
	unrelatedRS := newReplicaSet(1, map[string]string{"foo": "quux"})
	unrelatedRS.Name = "bar1"
	unrelatedRS.ObjectMeta.OwnerReferences[0].UID = "456"
	pendingDeletionRS := newReplicaSet(1, map[string]string{"foo": "xyzzy"})
	pendingDeletionRS.Name = "foo3"
	pendingDeletionRS.ObjectMeta.OwnerReferences[0].UID = "789"
	now := metav1.Now()
	pendingDeletionRS.DeletionTimestamp = &now
	pod1 := newPod("pod1", someRS, v1.PodRunning, nil, true)
	pod2 := newPod("pod2", someRS, v1.PodRunning, nil, true)
	pod3 := newPod("pod3", relatedRS, v1.PodRunning, nil, true)
	pod4 := newPod("pod4", unrelatedRS, v1.PodRunning, nil, true)
	logger, _ := ktesting.NewTestContext(t)

	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}}), stopCh, BurstReplicas)
	testCases := []struct {
		name             string
		rss              []*apps.ReplicaSet
		pods             []*v1.Pod
		rs               *apps.ReplicaSet
		expectedPodNames []string
	}{
		{
			name:             "expect to get a pod even if its owning ReplicaSet is pending deletion",
			rss:              []*apps.ReplicaSet{pendingDeletionRS, unrelatedRS},
			rs:               pendingDeletionRS,
			pods:             []*v1.Pod{newPod("pod", pendingDeletionRS, v1.PodRunning, nil, true)},
			expectedPodNames: []string{"pod"},
		},
		{
			name:             "expect to get only the ReplicaSet's own pods if there is no related ReplicaSet",
			rss:              []*apps.ReplicaSet{someRS, unrelatedRS},
			rs:               someRS,
			pods:             []*v1.Pod{pod1, pod2, pod4},
			expectedPodNames: []string{"pod1", "pod2"},
		},
		{
			name:             "expect to get own pods as well as any related ReplicaSet's but not an unrelated ReplicaSet's",
			rss:              []*apps.ReplicaSet{someRS, relatedRS, unrelatedRS},
			rs:               someRS,
			pods:             []*v1.Pod{pod1, pod2, pod3, pod4},
			expectedPodNames: []string{"pod1", "pod2", "pod3"},
		},
	}
	for _, c := range testCases {
		for _, r := range c.rss {
			informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(r)
		}
		for _, pod := range c.pods {
			informers.Core().V1().Pods().Informer().GetIndexer().Add(pod)
			// addPod 确保 Pod 的拥有者关系被控制器记录，便于间接关联查询。
			manager.addPod(logger, pod)
		}
		actualPods, err := manager.getIndirectlyRelatedPods(logger, c.rs)
		if err != nil {
			t.Errorf("Unexpected error from getIndirectlyRelatedPods: %v", err)
		}
		var actualPodNames []string
		for _, pod := range actualPods {
			actualPodNames = append(actualPodNames, pod.Name)
		}
		sort.Strings(actualPodNames)
		sort.Strings(c.expectedPodNames)
		if !reflect.DeepEqual(actualPodNames, c.expectedPodNames) {
			t.Errorf("Got [%s]; expected [%s]", strings.Join(actualPodNames, ", "), strings.Join(c.expectedPodNames, ", "))
		}
	}
}

// TestWatchControllers 验证 ReplicaSet 的 watch 事件会进入工作队列，
// 并最终触发 syncHandler 同步对应的 RS。
func TestWatchControllers(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	fakeWatch := watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger})
	client := fake.NewSimpleClientset()
	client.PrependWatchReactor("replicasets", core.DefaultWatchReactor(fakeWatch, nil))
	stopCh := make(chan struct{})
	defer close(stopCh)
	informers := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	tCtx := ktesting.Init(t)
	manager := NewReplicaSetController(
		tCtx,
		informers.Apps().V1().ReplicaSets(),
		informers.Core().V1().Pods(),
		client,
		BurstReplicas,
	)
	informers.Start(stopCh)
	informers.WaitForCacheSync(stopCh)

	var testRSSpec apps.ReplicaSet
	received := make(chan string)

	// fakeWatcher 发出的更新应经由工作队列最终进入 syncHandler；
	// 在 handler 中校验收到的 RS 后关闭 received 通道表示测试完成。
	manager.syncHandler = func(ctx context.Context, key string) error {
		obj, exists, err := informers.Apps().V1().ReplicaSets().Informer().GetIndexer().GetByKey(key)
		if !exists || err != nil {
			t.Errorf("Expected to find replica set under key %v", key)
		}
		rsSpec := *obj.(*apps.ReplicaSet)
		if !apiequality.Semantic.DeepDerivative(rsSpec, testRSSpec) {
			t.Errorf("Expected %#v, but got %#v", testRSSpec, rsSpec)
		}
		close(received)
		return nil
	}
	// 仅启动工作队列，发送 watch 事件并确认其触达同步方法。
	go wait.UntilWithContext(tCtx, manager.worker, 10*time.Millisecond)

	testRSSpec.Name = "foo"
	fakeWatch.Add(&testRSSpec)

	select {
	case <-received:
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("unexpected timeout from result channel")
	}
}

// TestWatchPods 验证 Pod 的 watch 事件会被映射到管理它的 ReplicaSet，
// 并将该 RS 送入 syncHandler；此处特意把 Pod 改为 Failed 状态以触发事件。
func TestWatchPods(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	client := fake.NewSimpleClientset()

	fakeWatch := watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger})
	client.PrependWatchReactor("pods", core.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	defer close(stopCh)

	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, BurstReplicas)

	// 先在共享 informer 中放入一个 RS，供 Pod 事件关联。
	labelMap := map[string]string{"foo": "bar"}
	testRSSpec := newReplicaSet(1, labelMap)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(testRSSpec)

	received := make(chan string)
	// fakeWatcher 发出的 Pod 更新应找出管理它的 RS 并送入 syncHandler。
	manager.syncHandler = func(ctx context.Context, key string) error {
		namespace, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			t.Errorf("Error splitting key: %v", err)
		}
		rsSpec, err := manager.rsLister.ReplicaSets(namespace).Get(name)
		if err != nil {
			t.Errorf("Expected to find replica set under key %v: %v", key, err)
		}
		if !apiequality.Semantic.DeepDerivative(rsSpec, testRSSpec) {
			t.Errorf("\nExpected %#v,\nbut got %#v", testRSSpec, rsSpec)
		}
		close(received)
		return nil
	}

	// 只启动 Pod watcher 与工作队列，确认事件命中正确的 RS。
	go informers.Core().V1().Pods().Informer().RunWithContext(ctx)
	go manager.Run(ctx, 1)

	pods := newPodList(nil, 1, v1.PodRunning, labelMap, testRSSpec, "pod")
	testPod := pods.Items[0]
	testPod.Status.Phase = v1.PodFailed
	fakeWatch.Add(&testPod)

	select {
	case <-received:
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("unexpected timeout from result channel")
	}
}

// TestUpdatePods 验证 Pod 更新时对关联 RS 的重新入队：
// 保留 ControllerRef 变更标签、移除 ControllerRef 成为孤儿等情况下，
// 应同步旧拥有者或标签匹配的新 RS。
func TestUpdatePods(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, fake.NewSimpleClientset(), stopCh, BurstReplicas)

	received := make(chan string)

	manager.syncHandler = func(ctx context.Context, key string) error {
		namespace, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			t.Errorf("Error splitting key: %v", err)
		}
		rsSpec, err := manager.rsLister.ReplicaSets(namespace).Get(name)
		if err != nil {
			t.Errorf("Expected to find replica set under key %v: %v", key, err)
		}
		received <- rsSpec.Name
		return nil
	}

	go wait.UntilWithContext(ctx, manager.worker, 10*time.Millisecond)

	// 放入两个标签不同的 RS，用于验证标签匹配与拥有者关系的差异。
	labelMap1 := map[string]string{"foo": "bar"}
	testRSSpec1 := newReplicaSet(1, labelMap1)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(testRSSpec1)
	testRSSpec2 := *testRSSpec1
	labelMap2 := map[string]string{"bar": "foo"}
	testRSSpec2.Spec.Selector = &metav1.LabelSelector{MatchLabels: labelMap2}
	testRSSpec2.Name = "barfoo"
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(&testRSSpec2)

	isController := true
	controllerRef1 := metav1.OwnerReference{UID: testRSSpec1.UID, APIVersion: "v1", Kind: "ReplicaSet", Name: testRSSpec1.Name, Controller: &isController}
	controllerRef2 := metav1.OwnerReference{UID: testRSSpec2.UID, APIVersion: "v1", Kind: "ReplicaSet", Name: testRSSpec2.Name, Controller: &isController}

	// 场景 1：Pod 带有 ControllerRef，标签变化后仍同步其拥有者 RS。
	pod1 := newPodList(informers.Core().V1().Pods().Informer().GetIndexer(), 1, v1.PodRunning, labelMap1, testRSSpec1, "pod").Items[0]
	pod1.OwnerReferences = []metav1.OwnerReference{controllerRef1}
	pod1.ResourceVersion = "1"
	pod2 := pod1
	pod2.Labels = labelMap2
	pod2.ResourceVersion = "2"
	manager.updatePod(logger, &pod1, &pod2)
	expected := sets.NewString(testRSSpec1.Name)
	for _, name := range expected.List() {
		t.Logf("Expecting update for %+v", name)
		select {
		case got := <-received:
			if !expected.Has(got) {
				t.Errorf("Expected keys %#v got %v", expected, got)
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected update notifications for replica sets")
		}
	}

	// 场景 2：移除 ControllerRef 成为孤儿，期望同步标签匹配的 RS。
	pod1 = newPodList(informers.Core().V1().Pods().Informer().GetIndexer(), 1, v1.PodRunning, labelMap1, testRSSpec1, "pod").Items[0]
	pod1.ResourceVersion = "1"
	pod1.Labels = labelMap2
	pod1.OwnerReferences = []metav1.OwnerReference{controllerRef2}
	pod2 = pod1
	pod2.OwnerReferences = nil
	pod2.ResourceVersion = "2"
	manager.updatePod(logger, &pod1, &pod2)
	expected = sets.NewString(testRSSpec2.Name)
	for _, name := range expected.List() {
		t.Logf("Expecting update for %+v", name)
		select {
		case got := <-received:
			if !expected.Has(got) {
				t.Errorf("Expected keys %#v got %v", expected, got)
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected update notifications for replica sets")
		}
	}

	// 场景 3：移除 ControllerRef，应同时同步原拥有者和标签匹配的 RS。
	pod1 = newPodList(informers.Core().V1().Pods().Informer().GetIndexer(), 1, v1.PodRunning, labelMap1, testRSSpec1, "pod").Items[0]
	pod1.ResourceVersion = "1"
	pod1.Labels = labelMap2
	pod1.OwnerReferences = []metav1.OwnerReference{controllerRef1}
	pod2 = pod1
	pod2.OwnerReferences = nil
	pod2.ResourceVersion = "2"
	manager.updatePod(logger, &pod1, &pod2)
	expected = sets.NewString(testRSSpec1.Name, testRSSpec2.Name)
	for _, name := range expected.List() {
		t.Logf("Expecting update for %+v", name)
		select {
		case got := <-received:
			if !expected.Has(got) {
				t.Errorf("Expected keys %#v got %v", expected, got)
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected update notifications for replica sets")
		}
	}

	// 场景 4：保留 ControllerRef 但变更标签，只同步拥有者 RS。
	pod1 = newPodList(informers.Core().V1().Pods().Informer().GetIndexer(), 1, v1.PodRunning, labelMap1, testRSSpec1, "pod").Items[0]
	pod1.ResourceVersion = "1"
	pod1.Labels = labelMap1
	pod1.OwnerReferences = []metav1.OwnerReference{controllerRef2}
	pod2 = pod1
	pod2.Labels = labelMap2
	pod2.ResourceVersion = "2"
	manager.updatePod(logger, &pod1, &pod2)
	expected = sets.NewString(testRSSpec2.Name)
	for _, name := range expected.List() {
		t.Logf("Expecting update for %+v", name)
		select {
		case got := <-received:
			if !expected.Has(got) {
				t.Errorf("Expected keys %#v got %v", expected, got)
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected update notifications for replica sets")
		}
	}
}

// TestControllerUpdateRequeue 验证状态更新失败时控制器会重新入队：
// 通过 reactor 让 status 更新失败，处理后队列中应仍有该 RS。
func TestControllerUpdateRequeue(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 注入错误使 status.Replicas 更新失败，从而强制控制器重新入队。
	labelMap := map[string]string{"foo": "bar"}
	rs := newReplicaSet(1, labelMap)
	client := fake.NewSimpleClientset(rs)
	client.PrependReactor("update", "replicasets",
		func(action core.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() != "status" {
				return false, nil, nil
			}
			return true, nil, errors.New("failed to update status")
		})
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, BurstReplicas)

	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)
	rs.Status = apps.ReplicaSetStatus{Replicas: 2}
	newPodList(informers.Core().V1().Pods().Informer().GetIndexer(), 1, v1.PodRunning, labelMap, rs, "pod")

	fakePodControl := controller.FakePodControl{}
	manager.podControl = &fakePodControl

	// 入队一次后处理；禁用限流以便立即观察是否重新入队。
	manager.queue = workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedMaxOfRateLimiter[string]())
	manager.enqueueRS(rs)
	manager.processNextWorkItem(ctx)
	// 处理失败后应重新入队，队列长度应为 1。
	if got, want := manager.queue.Len(), 1; got != want {
		t.Errorf("queue.Len() = %v, want %v", got, want)
	}
}

// TestControllerUpdateStatusWithFailure 验证状态更新冲突后的重试：
// get 返回旧对象而 update 持续失败时，应发起 2 次 update，
// 且每次 update 都携带期望的新副本数而非旧对象的值。
func TestControllerUpdateStatusWithFailure(t *testing.T) {
	rs := newReplicaSet(1, map[string]string{"foo": "bar"})
	fakeClient := &fake.Clientset{}
	fakeClient.AddReactor("get", "replicasets", func(action core.Action) (bool, runtime.Object, error) { return true, rs, nil })
	fakeClient.AddReactor("*", "*", func(action core.Action) (bool, runtime.Object, error) {
		return true, &apps.ReplicaSet{}, fmt.Errorf("fake error")
	})
	fakeRSClient := fakeClient.AppsV1().ReplicaSets("default")
	numReplicas := int32(10)
	newStatus := apps.ReplicaSetStatus{Replicas: numReplicas}
	logger, _ := ktesting.NewTestContext(t)
	_, err := updateReplicaSetStatus(logger, fakeRSClient, rs, newStatus, DefaultReplicaSetControllerFeatures())
	if err == nil {
		t.Errorf("Expected update err")
	}
	updates, gets := 0, 0
	for _, a := range fakeClient.Actions() {
		if a.GetResource().Resource != "replicasets" {
			t.Errorf("Unexpected action %+v", a)
			continue
		}

		switch action := a.(type) {
		case core.GetAction:
			gets++
			// 即使更新失败，也要确认 get 针对的是正确的 RS。
			if action.GetName() != rs.Name {
				t.Errorf("Expected get for ReplicaSet %v, got %+v instead", rs.Name, action.GetName())
			}
		case core.UpdateAction:
			updates++
			// 即使 get 返回的 RS 副本数为 1，更新请求也必须携带期望的新副本数。
			if c, ok := action.GetObject().(*apps.ReplicaSet); !ok {
				t.Errorf("Expected a ReplicaSet as the argument to update, got %T", c)
			} else if c.Status.Replicas != numReplicas {
				t.Errorf("Expected update for ReplicaSet to contain replicas %v, got %v instead",
					numReplicas, c.Status.Replicas)
			}
		default:
			t.Errorf("Unexpected action %+v", a)
			break
		}
	}
	if gets != 1 || updates != 2 {
		t.Errorf("Expected 1 get and 2 updates, got %d gets %d updates", gets, updates)
	}
}

// TODO: 该测试逻辑过于复杂，不适合作为单元测试，应迁移到 E2E 测试套件。
// doTestControllerBurstReplicas 验证突发副本限制：扩容和缩容时，
// 控制器每轮只创建/删除 burstReplicas 个 Pod，并通过期望值等待 watch 事件确认。
func doTestControllerBurstReplicas(t *testing.T, burstReplicas, numReplicas int) {
	logger, ctx := ktesting.NewTestContext(t)
	labelMap := map[string]string{"foo": "bar"}
	rsSpec := newReplicaSet(numReplicas, labelMap)
	client := fake.NewSimpleClientset(rsSpec)
	fakePodControl := controller.FakePodControl{}
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, burstReplicas)
	manager.podControl = &fakePodControl

	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rsSpec)

	expectedPods := int32(0)
	pods := newPodList(nil, numReplicas, v1.PodPending, labelMap, rsSpec, "pod")

	rsKey, err := controller.KeyFunc(rsSpec)
	if err != nil {
		t.Errorf("Couldn't get key for object %#v: %v", rsSpec, err)
	}

	// 先扩容再缩容，验证每轮的创建/删除数量符合预期模式。
	for _, replicas := range []int32{int32(numReplicas), 0} {

		*(rsSpec.Spec.Replicas) = replicas
		informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rsSpec)

		for i := 0; i < numReplicas; i += burstReplicas {
			manager.syncReplicaSet(ctx, GetKey(rsSpec, t))

			// 缓存中累计活跃 Pod，控制器据此决定还需创建多少副本。
			activePods := int32(len(informers.Core().V1().Pods().Informer().GetIndexer().List()))
			if replicas != 0 {
				// “in flight”的 Pod 是已创建但尚未通过 watch 确认的副本，
				// 在此期间控制器会休眠，直到全部被观察到。
				expectedPods = replicas - activePods
				if expectedPods > int32(burstReplicas) {
					expectedPods = int32(burstReplicas)
				}
				// 校验本次同步确实创建了预期数量的 Pod。
				err := validateSyncReplicaSet(&fakePodControl, int(expectedPods), 0, 0)
				if err != nil {
					t.Fatal(err)
				}

				// 模拟除最后一个之外所有 Pod 的 watch 事件；
				// 期望数未清零，这些事件不应唤醒控制器。
				for i := int32(0); i < expectedPods-1; i++ {
					informers.Core().V1().Pods().Informer().GetIndexer().Add(&pods.Items[i])
					manager.addPod(logger, &pods.Items[i])
				}

				podExp, exists, err := manager.expectations.GetExpectations(rsKey)
				if !exists || err != nil {
					t.Fatalf("Did not find expectations for rs.")
				}
				if add, _ := podExp.GetExpectations(); add != 1 {
					t.Fatalf("Expectations are wrong %v", podExp)
				}
			} else {
				expectedPods = (replicas - activePods) * -1
				if expectedPods > int32(burstReplicas) {
					expectedPods = int32(burstReplicas)
				}
				err := validateSyncReplicaSet(&fakePodControl, 0, int(expectedPods), 0)
				if err != nil {
					t.Fatal(err)
				}

				// 为准确模拟 watch，必须删除 RS 正在等待其删除事件的那批 Pod。
				expectedDels := manager.expectations.GetUIDs(GetKey(rsSpec, t))
				podsToDelete := []*v1.Pod{}
				isController := true
				for _, key := range expectedDels.List() {
					nsName := strings.Split(key, "/")
					podsToDelete = append(podsToDelete, &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      nsName[1],
							Namespace: nsName[0],
							Labels:    rsSpec.Spec.Selector.MatchLabels,
							OwnerReferences: []metav1.OwnerReference{
								{UID: rsSpec.UID, APIVersion: "v1", Kind: "ReplicaSet", Name: rsSpec.Name, Controller: &isController},
							},
						},
					})
				}
				// 有意保留最后一个 Pod 不删，以便校验末尾期望值恰好为 1，防止重复删除。
				for i := range podsToDelete[1:] {
					informers.Core().V1().Pods().Informer().GetIndexer().Delete(podsToDelete[i])
					manager.deletePod(logger, podsToDelete[i])
				}
				podExp, exists, err := manager.expectations.GetExpectations(rsKey)
				if !exists || err != nil {
					t.Fatalf("Did not find expectations for ReplicaSet.")
				}
				if _, del := podExp.GetExpectations(); del != 1 {
					t.Fatalf("Expectations are wrong %v", podExp)
				}
			}

			// 期望未满足期间，上述 Pod 事件不应触发任何创建或删除。
			fakePodControl.Clear()
			manager.syncReplicaSet(ctx, GetKey(rsSpec, t))
			err := validateSyncReplicaSet(&fakePodControl, 0, 0, 0)
			if err != nil {
				t.Fatal(err)
			}

			// 处理最后一个 Pod：其事件会把期望降为 0，
			// 从而触发下一批最多 burstReplicas 个副本的创建/删除。
			if replicas != 0 {
				informers.Core().V1().Pods().Informer().GetIndexer().Add(&pods.Items[expectedPods-1])
				manager.addPod(logger, &pods.Items[expectedPods-1])
			} else {
				expectedDel := manager.expectations.GetUIDs(GetKey(rsSpec, t))
				if expectedDel.Len() != 1 {
					t.Fatalf("Waiting on unexpected number of deletes.")
				}
				nsName := strings.Split(expectedDel.List()[0], "/")
				isController := true
				lastPod := &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      nsName[1],
						Namespace: nsName[0],
						Labels:    rsSpec.Spec.Selector.MatchLabels,
						OwnerReferences: []metav1.OwnerReference{
							{UID: rsSpec.UID, APIVersion: "v1", Kind: "ReplicaSet", Name: rsSpec.Name, Controller: &isController},
						},
					},
				}
				informers.Core().V1().Pods().Informer().GetIndexer().Delete(lastPod)
				manager.deletePod(logger, lastPod)
			}
			pods.Items = pods.Items[expectedPods:]
		}

		// 确认本轮结束后活跃 Pod 数与期望副本数一致。
		activePods := int32(len(informers.Core().V1().Pods().Informer().GetIndexer().List()))
		if activePods != *(rsSpec.Spec.Replicas) {
			t.Fatalf("Unexpected number of active pods, expected %d, got %d", *(rsSpec.Spec.Replicas), activePods)
		}
		// 扩容时切片消耗了 Pod 列表，这里重新生成供缩容阶段使用。
		pods = newPodList(nil, int(replicas), v1.PodRunning, labelMap, rsSpec, "pod")
	}
}

// TestControllerBurstReplicas 用多组（burstReplicas, numReplicas）组合覆盖突发副本限制，
// 包括副本数小于突发上限的边界情况。
func TestControllerBurstReplicas(t *testing.T) {
	doTestControllerBurstReplicas(t, 5, 30)
	doTestControllerBurstReplicas(t, 5, 12)
	doTestControllerBurstReplicas(t, 3, 2)
}

// FakeRSExpectations 包装 ControllerExpectations，便于在测试中校验期望计数。
type FakeRSExpectations struct {
	*controller.ControllerExpectations
	satisfied    bool
	expSatisfied func()
}

// SatisfiedExpectations 返回预置的期望满足状态，并在调用时触发回调，
// 用于在检查期望的瞬间注入并发事件。
func (fe FakeRSExpectations) SatisfiedExpectations(logger klog.Logger, controllerKey string) bool {
	fe.expSatisfied()
	return fe.satisfied
}

// TestRSSyncExpectations 验证在统计活跃 Pod 与检查期望之间不会有 Pod “插入”：
// 通过回调在检查期望时加入一个 Pod，确认控制器不会重复创建副本。
func TestRSSyncExpectations(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	client := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	fakePodControl := controller.FakePodControl{}
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, 2)
	manager.podControl = &fakePodControl

	labelMap := map[string]string{"foo": "bar"}
	rsSpec := newReplicaSet(2, labelMap)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rsSpec)
	pods := newPodList(nil, 2, v1.PodPending, labelMap, rsSpec, "pod")
	informers.Core().V1().Pods().Informer().GetIndexer().Add(&pods.Items[0])
	postExpectationsPod := pods.Items[1]

	manager.expectations = controller.NewUIDTrackingControllerExpectations(FakeRSExpectations{
		controller.NewControllerExpectations(), true, func() {
			// 若先统计活跃 Pod 再检查期望，控制器会因看不到该 Pod 且期望已满足而重复创建副本。
			informers.Core().V1().Pods().Informer().GetIndexer().Add(&postExpectationsPod)
		},
	})
	manager.syncReplicaSet(ctx, GetKey(rsSpec, t))
	err := validateSyncReplicaSet(&fakePodControl, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
}

// TestDeleteControllerAndExpectations 验证删除 RS 时其期望值一并被清除：
// 即使之后有并发的 addPod 持有旧期望句柄，也不会再触发创建。
func TestDeleteControllerAndExpectations(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	rs := newReplicaSet(1, map[string]string{"foo": "bar"})
	client := fake.NewSimpleClientset(rs)
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, 10)

	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)

	fakePodControl := controller.FakePodControl{}
	manager.podControl = &fakePodControl

	// 首次同步创建副本并设置期望。
	manager.syncReplicaSet(ctx, GetKey(rs, t))
	err := validateSyncReplicaSet(&fakePodControl, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	fakePodControl.Clear()

	// 获取 RS 键，用于查询期望值。
	rsKey, err := controller.KeyFunc(rs)
	if err != nil {
		t.Errorf("Couldn't get key for object %#v: %v", rs, err)
	}

	// 保留期望句柄，模拟控制器删除 RS 时并发 addPod 仍持有该句柄的情形。
	podExp, exists, err := manager.expectations.GetExpectations(rsKey)
	if !exists || err != nil {
		t.Errorf("No expectations found for ReplicaSet")
	}
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Delete(rs)
	manager.deleteRS(logger, rs)
	manager.syncReplicaSet(ctx, GetKey(rs, t))

	_, exists, err = manager.expectations.GetExpectations(rsKey)
	if err != nil {
		t.Errorf("Failed to get controllee expectations: %v", err)
	}
	if exists {
		t.Errorf("Found expectations, expected none since the ReplicaSet has been deleted.")
	}

	// RS 已删除，此后对期望的修改和同步都不应产生任何动作。
	podExp.Add(-1, 0)
	informers.Core().V1().Pods().Informer().GetIndexer().Replace(make([]interface{}, 0), "0")
	manager.syncReplicaSet(ctx, GetKey(rs, t))
	err = validateSyncReplicaSet(&fakePodControl, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
}

// TestExpectationsOnRecreate 验证同名但 UID 不同的 RS 重建后，
// 旧 RS 的期望被清除，新 RS 会重新建立创建副本的期望。
func TestExpectationsOnRecreate(t *testing.T) {
	client := fake.NewSimpleClientset()
	stopCh := make(chan struct{})
	defer close(stopCh)

	f := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	tCtx := ktesting.Init(t)
	logger := tCtx.Logger()
	manager := NewReplicaSetController(
		tCtx,
		f.Apps().V1().ReplicaSets(),
		f.Core().V1().Pods(),
		client,
		100,
	)
	f.Start(stopCh)
	f.WaitForCacheSync(stopCh)
	fakePodControl := controller.FakePodControl{}
	manager.podControl = &fakePodControl

	if manager.queue.Len() != 0 {
		t.Fatal("Unexpected item in the queue")
	}

	oldRS := newReplicaSet(1, map[string]string{"foo": "bar"})
	oldRS, err := client.AppsV1().ReplicaSets(oldRS.Namespace).Create(tCtx, oldRS, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	err = wait.PollImmediate(100*time.Millisecond, informerSyncTimeout, func() (bool, error) {
		logger.V(8).Info("Waiting for queue to have 1 item", "length", manager.queue.Len())
		return manager.queue.Len() == 1, nil
	})
	if err != nil {
		t.Fatalf("initial RS didn't result in new item in the queue: %v", err)
	}

	ok := manager.processNextWorkItem(tCtx)
	if !ok {
		t.Fatal("queue is shutting down")
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.DeploymentReplicaSetTerminatingReplicas) {
		// 该特性会引发一次 terminatingReplicas nil->0 的状态更新，需要多处理一次空同步。
		ok = manager.processNextWorkItem(tCtx)
		if !ok {
			t.Fatal("queue is shutting down")
		}
	}

	err = validateSyncReplicaSet(&fakePodControl, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	fakePodControl.Clear()

	oldRSKey, err := controller.KeyFunc(oldRS)
	if err != nil {
		t.Fatal(err)
	}

	rsExp, exists, err := manager.expectations.GetExpectations(oldRSKey)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Errorf("No expectations found for ReplicaSet %q", oldRSKey)
	}
	if rsExp.Fulfilled() {
		t.Errorf("There should be unfulfilled expectations for creating new pods for ReplicaSet %q", oldRSKey)
	}

	if manager.queue.Len() != 0 {
		t.Fatal("Unexpected item in the queue")
	}

	err = client.AppsV1().ReplicaSets(oldRS.Namespace).Delete(tCtx, oldRS.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	err = wait.PollImmediate(100*time.Millisecond, informerSyncTimeout, func() (bool, error) {
		logger.V(8).Info("Waiting for queue to have 1 item", "length", manager.queue.Len())
		return manager.queue.Len() == 1, nil
	})
	if err != nil {
		t.Fatalf("Deleting RS didn't result in new item in the queue: %v", err)
	}

	_, exists, err = manager.expectations.GetExpectations(oldRSKey)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Errorf("There should be no expectations for ReplicaSet %q after it was deleted", oldRSKey)
	}

	// 跳过删除事件对应的同步，只观察新 RS 的同步。
	key, quit := manager.queue.Get()
	if quit {
		t.Fatal("Queue is shutting down!")
	}
	manager.queue.Done(key)
	if key != oldRSKey {
		t.Fatal("Keys should be equal!")
	}

	if manager.queue.Len() != 0 {
		t.Fatal("Unexpected item in the queue")
	}

	newRS := oldRS.DeepCopy()
	newRS.UID = uuid.NewUUID()
	newRS, err = client.AppsV1().ReplicaSets(newRS.Namespace).Create(tCtx, newRS, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 基本校验：重建的 RS 必须具有不同 UID。
	if newRS.UID == oldRS.UID {
		t.Fatal("New RS has the same UID as the old one!")
	}

	err = wait.PollImmediate(100*time.Millisecond, informerSyncTimeout, func() (bool, error) {
		logger.V(8).Info("Waiting for queue to have 1 item", "length", manager.queue.Len())
		return manager.queue.Len() == 1, nil
	})
	if err != nil {
		t.Fatalf("Re-creating RS didn't result in new item in the queue: %v", err)
	}

	ok = manager.processNextWorkItem(tCtx)
	if !ok {
		t.Fatal("Queue is shutting down!")
	}

	newRSKey, err := controller.KeyFunc(newRS)
	if err != nil {
		t.Fatal(err)
	}
	rsExp, exists, err = manager.expectations.GetExpectations(newRSKey)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Errorf("No expectations found for ReplicaSet %q", newRSKey)
	}
	if rsExp.Fulfilled() {
		t.Errorf("There should be unfulfilled expectations for creating new pods for ReplicaSet %q", newRSKey)
	}

	err = validateSyncReplicaSet(&fakePodControl, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	fakePodControl.Clear()
}

// shuffle 返回输入 RS 列表的随机排列副本，不修改原切片，
// 用于验证控制器选择 RS 时不依赖存储顺序。
func shuffle(controllers []*apps.ReplicaSet) []*apps.ReplicaSet {
	numControllers := len(controllers)
	randIndexes := rand.Perm(numControllers)
	shuffled := make([]*apps.ReplicaSet, numControllers)
	for i := 0; i < numControllers; i++ {
		shuffled[i] = controllers[randIndexes[i]]
	}
	return shuffled
}

// TestOverlappingRSs 验证多个 RS 使用相同选择器和创建时间时，
// 带有 ControllerRef 的 Pod 只会同步其真正拥有者，
// 而非按名称等顺序误选其他 RS。
func TestOverlappingRSs(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	client := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	labelMap := map[string]string{"foo": "bar"}

	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, 10)

	// 构造 10 个 RS，随机打乱后插入存储；
	// 它们使用相同的创建时间，以验证 ControllerRef 不依赖时间或名称排序。
	timestamp := metav1.Date(2014, time.December, 0, 0, 0, 0, 0, time.Local)
	var controllers []*apps.ReplicaSet
	for j := 1; j < 10; j++ {
		rsSpec := newReplicaSet(1, labelMap)
		rsSpec.CreationTimestamp = timestamp
		rsSpec.Name = fmt.Sprintf("rs%d", j)
		controllers = append(controllers, rsSpec)
	}
	shuffledControllers := shuffle(controllers)
	for j := range shuffledControllers {
		informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(shuffledControllers[j])
	}
	// 添加一个带 ControllerRef 的 Pod，确认只有对应 RS 被同步；
	// 特意选择中间的 RS，以暴露旧代码在时间相同时按名称排序的问题。
	rs := controllers[3]
	pods := newPodList(nil, 1, v1.PodPending, labelMap, rs, "pod")
	pod := &pods.Items[0]
	isController := true
	pod.OwnerReferences = []metav1.OwnerReference{
		{UID: rs.UID, APIVersion: "v1", Kind: "ReplicaSet", Name: rs.Name, Controller: &isController},
	}
	rsKey := GetKey(rs, t)

	manager.addPod(logger, pod)
	queueRS, _ := manager.queue.Get()
	if queueRS != rsKey {
		t.Fatalf("Expected to find key %v in queue, found %v", rsKey, queueRS)
	}
}

// TestDeletionTimestamp 验证删除时间戳相关的期望值处理：
// 带删除时间戳的新增 Pod 应减少删除期望而非创建期望；
// 从无删除时间戳到有的更新视为一次删除，且不会被重复计数。
func TestDeletionTimestamp(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	c := clientset.NewForConfigOrDie(&restclient.Config{Host: "", ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	labelMap := map[string]string{"foo": "bar"}
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, informers := testNewReplicaSetControllerFromClient(t, c, stopCh, 10)

	rs := newReplicaSet(1, labelMap)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)
	rsKey, err := controller.KeyFunc(rs)
	if err != nil {
		t.Errorf("Couldn't get key for object %#v: %v", rs, err)
	}
	pod := newPodList(nil, 1, v1.PodPending, labelMap, rs, "pod").Items[0]
	pod.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	pod.ResourceVersion = "1"
	manager.expectations.ExpectDeletions(logger, rsKey, []string{controller.PodKey(&pod)})

	// 新增即带删除时间戳的 Pod 应减少删除期望，而不是创建期望。
	manager.addPod(logger, &pod)

	queueRS, _ := manager.queue.Get()
	if queueRS != rsKey {
		t.Fatalf("Expected to find key %v in queue, found %v", rsKey, queueRS)
	}
	manager.queue.Done(rsKey)

	podExp, exists, err := manager.expectations.GetExpectations(rsKey)
	if !exists || err != nil || !podExp.Fulfilled() {
		t.Fatalf("Wrong expectations %#v", podExp)
	}

	// 从无删除时间戳更新为有删除时间戳，应视为一次删除。
	oldPod := newPodList(nil, 1, v1.PodPending, labelMap, rs, "pod").Items[0]
	oldPod.ResourceVersion = "2"
	manager.expectations.ExpectDeletions(logger, rsKey, []string{controller.PodKey(&pod)})
	manager.updatePod(logger, &oldPod, &pod)

	queueRS, _ = manager.queue.Get()
	if queueRS != rsKey {
		t.Fatalf("Expected to find key %v in queue, found %v", rsKey, queueRS)
	}
	manager.queue.Done(rsKey)

	podExp, exists, err = manager.expectations.GetExpectations(rsKey)
	if !exists || err != nil || !podExp.Fulfilled() {
		t.Fatalf("Wrong expectations %#v", podExp)
	}

	// 后续对同一 Pod 的更新（包括删除时间戳变化）不应被重复计为删除。
	isController := true
	secondPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pod.Namespace,
			Name:      "secondPod",
			Labels:    pod.Labels,
			OwnerReferences: []metav1.OwnerReference{
				{UID: rs.UID, APIVersion: "v1", Kind: "ReplicaSet", Name: rs.Name, Controller: &isController},
			},
		},
	}
	manager.expectations.ExpectDeletions(logger, rsKey, []string{controller.PodKey(secondPod)})
	oldPod.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	oldPod.ResourceVersion = "2"
	manager.updatePod(logger, &oldPod, &pod)

	podExp, exists, err = manager.expectations.GetExpectations(rsKey)
	if !exists || err != nil || podExp.Fulfilled() {
		t.Fatalf("Wrong expectations %#v", podExp)
	}

	// 已有删除时间戳的 Pod 在 delete 事件中应被忽略，因为更新时已经计数。
	manager.deletePod(logger, &pod)
	podExp, exists, err = manager.expectations.GetExpectations(rsKey)
	if !exists || err != nil || podExp.Fulfilled() {
		t.Fatalf("Wrong expectations %#v", podExp)
	}

	// 删除第二个 Pod 后，删除期望应被清零。
	manager.deletePod(logger, secondPod)

	queueRS, _ = manager.queue.Get()
	if queueRS != rsKey {
		t.Fatalf("Expected to find key %v in queue, found %v", rsKey, queueRS)
	}
	manager.queue.Done(rsKey)

	podExp, exists, err = manager.expectations.GetExpectations(rsKey)
	if !exists || err != nil || !podExp.Fulfilled() {
		t.Fatalf("Wrong expectations %#v", podExp)
	}
}

// setupManagerWithGCEnabled 构造一个使用 FakePodControl 的 RS 管理器，
// 便于验证垃圾回收（adopt/patch）相关行为。
func setupManagerWithGCEnabled(t *testing.T, stopCh chan struct{}, objs ...runtime.Object) (manager *ReplicaSetController, fakePodControl *controller.FakePodControl, informers informers.SharedInformerFactory) {
	c := fake.NewSimpleClientset(objs...)
	fakePodControl = &controller.FakePodControl{}
	manager, informers = testNewReplicaSetControllerFromClient(t, c, stopCh, BurstReplicas)

	manager.podControl = fakePodControl
	return manager, fakePodControl, informers
}

// TestDoNotPatchPodWithOtherControlRef 验证标签匹配但已被其他控制器拥有的 Pod 不会被接管，
// 因此控制器应为缺失的 2 个副本创建 Pod。
func TestDoNotPatchPodWithOtherControlRef(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	labelMap := map[string]string{"foo": "bar"}
	rs := newReplicaSet(2, labelMap)
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, fakePodControl, informers := setupManagerWithGCEnabled(t, stopCh, rs)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)
	var trueVar = true
	otherControllerReference := metav1.OwnerReference{UID: uuid.NewUUID(), APIVersion: "v1beta1", Kind: "ReplicaSet", Name: "AnotherRS", Controller: &trueVar}
	// 缓存中放入一个标签匹配但归属其他控制器的 Pod，预期不会对其打补丁。
	pod := newPod("pod", rs, v1.PodRunning, nil, true)
	pod.OwnerReferences = []metav1.OwnerReference{otherControllerReference}
	informers.Core().V1().Pods().Informer().GetIndexer().Add(pod)
	err := manager.syncReplicaSet(ctx, GetKey(rs, t))
	if err != nil {
		t.Fatal(err)
	}
	// 该 Pod 已有控制器，不算 RS 的副本，因此应创建 2 个新 Pod。
	err = validateSyncReplicaSet(fakePodControl, 2, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
}

// TestPatchPodFails 验证接管 Pod 的补丁操作失败时，
// 控制器会认为接管失败并重新入队重试。
func TestPatchPodFails(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	labelMap := map[string]string{"foo": "bar"}
	rs := newReplicaSet(2, labelMap)
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, fakePodControl, informers := setupManagerWithGCEnabled(t, stopCh, rs)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)
	// 缓存中放入两个无归属的匹配 Pod，预期发起两次接管补丁。
	informers.Core().V1().Pods().Informer().GetIndexer().Add(newPod("pod1", rs, v1.PodRunning, nil, false))
	informers.Core().V1().Pods().Informer().GetIndexer().Add(newPod("pod2", rs, v1.PodRunning, nil, false))
	// 让两次补丁都失败，控制器会认为未成功接管并将 RS 重新入队。
	fakePodControl.Err = fmt.Errorf("fake Error")
	rsKey := GetKey(rs, t)
	err := processSync(ctx, manager, rsKey)
	if err == nil || !strings.Contains(err.Error(), "fake Error") {
		t.Errorf("expected fake Error, got %+v", err)
	}
	// 共两次补丁尝试（均失败）。
	err = validateSyncReplicaSet(fakePodControl, 0, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	// RS 应重新入队，队列中可取其键。
	queueRS, _ := manager.queue.Get()
	if queueRS != rsKey {
		t.Fatalf("Expected to find key %v in queue, found %v", rsKey, queueRS)
	}
}

// TestDoNotAdoptOrCreateIfBeingDeleted 验证 RS 即将被删除（带删除时间戳）时，
// 控制器不会接管也不会创建 Pod。
func TestDoNotAdoptOrCreateIfBeingDeleted(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	labelMap := map[string]string{"foo": "bar"}
	rs := newReplicaSet(2, labelMap)
	now := metav1.Now()
	rs.DeletionTimestamp = &now
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, fakePodControl, informers := setupManagerWithGCEnabled(t, stopCh, rs)
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)
	pod1 := newPod("pod1", rs, v1.PodRunning, nil, false)
	informers.Core().V1().Pods().Informer().GetIndexer().Add(pod1)

	// 既不打补丁也不创建副本。
	err := manager.syncReplicaSet(ctx, GetKey(rs, t))
	if err != nil {
		t.Fatal(err)
	}
	err = validateSyncReplicaSet(fakePodControl, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
}

// TestDoNotAdoptOrCreateIfBeingDeletedRace 验证删除竞态：
// 直连客户端看到 RS 已删除，而 informer 缓存仍显示未删除；
// 当存在可接管的孤儿 Pod 时，控制器会二次确认并中止同步。
func TestDoNotAdoptOrCreateIfBeingDeletedRace(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	labelMap := map[string]string{"foo": "bar"}
	// 直连客户端报告 RS 已被删除。
	rs := newReplicaSet(2, labelMap)
	now := metav1.Now()
	rs.DeletionTimestamp = &now
	stopCh := make(chan struct{})
	defer close(stopCh)
	manager, fakePodControl, informers := setupManagerWithGCEnabled(t, stopCh, rs)
	// informer 缓存仍显示 RS 未删除，制造竞态窗口。
	rs2 := *rs
	rs2.DeletionTimestamp = nil
	informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(&rs2)

	// 存在匹配的孤儿 Pod 时才会触发对 RS 删除状态的二次检查。
	pod1 := newPod("pod1", rs, v1.PodRunning, nil, false)
	informers.Core().V1().Pods().Informer().GetIndexer().Add(pod1)

	// 二次确认发现已删除，同步应中止并返回错误。
	err := manager.syncReplicaSet(ctx, GetKey(rs, t))
	if err == nil {
		t.Error("syncReplicaSet() err = nil, expected non-nil")
	}
	// 中止后既不打补丁也不创建副本。
	err = validateSyncReplicaSet(fakePodControl, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
}

// TestReplicaSetAvailabilityCheck 验证 MinReadySeconds 对 AvailableReplicas 的影响：
// 1 个 Pending、1 个刚就绪、1 个就绪 2 秒、1 个就绪超过 5 秒的 Pod 中，
// 只有最后一个可计入可用副本；同时验证控制器会因等待可用数变化而重新入队。
func TestReplicaSetAvailabilityCheck(t *testing.T) {
	tCtx := ktesting.Init(t)

	labelMap := map[string]string{"foo": "bar"}
	rs := newReplicaSet(4, labelMap)
	rs.Spec.MinReadySeconds = 5
	client := fake.NewClientset(rs)
	stopCh := make(chan struct{})
	defer close(stopCh)

	manager, informers := testNewReplicaSetControllerFromClient(t, client, stopCh, BurstReplicas)
	if err := informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	pod1 := newPod("foobar-1", rs, v1.PodPending, nil, true)
	pod2 := newPod("foobar-2", rs, v1.PodRunning, &metav1.Time{Time: now}, true)
	pod3 := newPod("foobar-3", rs, v1.PodRunning, &metav1.Time{Time: now.Add(-2 * time.Second)}, true)
	pod4 := newPod("foobar-4", rs, v1.PodRunning, &metav1.Time{Time: now.Add(-4300 * time.Millisecond)}, true)
	if err := informers.Core().V1().Pods().Informer().GetIndexer().Add(pod1); err != nil {
		t.Fatal(err)
	}
	if err := informers.Core().V1().Pods().Informer().GetIndexer().Add(pod2); err != nil {
		t.Fatal(err)
	}
	if err := informers.Core().V1().Pods().Informer().GetIndexer().Add(pod3); err != nil {
		t.Fatal(err)
	}
	if err := informers.Core().V1().Pods().Informer().GetIndexer().Add(pod4); err != nil {
		t.Fatal(err)
	}
	fakePodControl := controller.FakePodControl{}
	manager.podControl = &fakePodControl

	err := manager.syncReplicaSet(tCtx, GetKey(rs, t))
	if err != nil {
		t.Fatal(err)
	}

	var updatedRs *apps.ReplicaSet
	for _, a := range client.Actions() {
		if a.GetResource().Resource != "replicasets" {
			t.Errorf("Unexpected action %+v", a)
			continue
		}

		switch action := a.(type) {
		case core.UpdateAction:
			var ok bool
			if updatedRs, ok = action.GetObject().(*apps.ReplicaSet); !ok {
				t.Errorf("Expected a ReplicaSet as the argument to update, got %T", updatedRs)
			}
		default:
			t.Errorf("Unexpected action %+v", a)
		}
	}

	// 有一个 Pod 未就绪，ReadyReplicas 为 3。
	if updatedRs.Status.ReadyReplicas != 3 {
		t.Errorf("Expected updated ReplicaSet to contain ready replicas %v, got %v instead",
			3, updatedRs.Status.ReadyReplicas)
	}
	if updatedRs.Status.AvailableReplicas != 0 {
		t.Errorf("Expected updated ReplicaSet to contain available replicas %v, got %v instead",
			0, updatedRs.Status.AvailableReplicas)
	}

	if got, want := manager.queue.Len(), 0; got != want {
		t.Errorf("queue.Len() = %v, want %v", got, want)
	}

	// RS 应在约 700ms 后重新入队以重算 availableReplicas（额外留 200ms 余量）。
	tCtx.Eventually(manager.queue.Len).WithTimeout(900*time.Millisecond).
		WithPolling(10*time.Millisecond).
		Should(gomega.Equal(1), " RS should be re-queued to recompute .status.availableReplicas")

}

// 本组变量保存测试用的条件与期望值。
var (
	// 自定义条件类型，用于与内置 ReplicaFailure 条件区分。
	imagePullBackOff apps.ReplicaSetConditionType = "ImagePullBackOff"

	// 以下工厂函数每次返回全新的条件对象，避免用例之间共享可变状态。
	condImagePullBackOff = func() apps.ReplicaSetCondition {
		return apps.ReplicaSetCondition{
			Type:   imagePullBackOff,
			Status: v1.ConditionTrue,
			Reason: "NonExistentImage",
		}
	}

	condReplicaFailure = func() apps.ReplicaSetCondition {
		return apps.ReplicaSetCondition{
			Type:   apps.ReplicaSetReplicaFailure,
			Status: v1.ConditionTrue,
			Reason: "OtherFailure",
		}
	}

	condReplicaFailure2 = func() apps.ReplicaSetCondition {
		return apps.ReplicaSetCondition{
			Type:   apps.ReplicaSetReplicaFailure,
			Status: v1.ConditionTrue,
			Reason: "AnotherFailure",
		}
	}

	status = func() *apps.ReplicaSetStatus {
		return &apps.ReplicaSetStatus{
			Conditions: []apps.ReplicaSetCondition{condReplicaFailure()},
		}
	}
)

// TestGetCondition 验证 GetCondition 能按类型找到对应条件，类型不存在时返回 nil。
func TestGetCondition(t *testing.T) {
	exampleStatus := status()

	tests := []struct {
		name string

		status   apps.ReplicaSetStatus
		condType apps.ReplicaSetConditionType

		expected bool
	}{
		{
			// 查询的类型存在于状态中。
			name: "condition exists",

			status:   *exampleStatus,
			condType: apps.ReplicaSetReplicaFailure,

			expected: true,
		},
		{
			// 查询的类型不在状态中。
			name: "condition does not exist",

			status:   *exampleStatus,
			condType: imagePullBackOff,

			expected: false,
		},
	}

	for _, test := range tests {
		cond := GetCondition(test.status, test.condType)
		exists := cond != nil
		if exists != test.expected {
			t.Errorf("%s: expected condition to exist: %t, got: %t", test.name, test.expected, exists)
		}
	}
}

// TestSetCondition 验证 SetCondition 的三种行为：首次写入、追加不同类型条件、
// 以及同类型条件被新值覆盖。
func TestSetCondition(t *testing.T) {
	tests := []struct {
		name string

		status *apps.ReplicaSetStatus
		cond   apps.ReplicaSetCondition

		expectedStatus *apps.ReplicaSetStatus
	}{
		{
			// 空状态首次写入条件。
			name: "set for the first time",

			status: &apps.ReplicaSetStatus{},
			cond:   condReplicaFailure(),

			expectedStatus: &apps.ReplicaSetStatus{Conditions: []apps.ReplicaSetCondition{condReplicaFailure()}},
		},
		{
			// 不同类型条件追加到已有条件之后。
			name: "simple set",

			status: &apps.ReplicaSetStatus{Conditions: []apps.ReplicaSetCondition{condImagePullBackOff()}},
			cond:   condReplicaFailure(),

			expectedStatus: &apps.ReplicaSetStatus{Conditions: []apps.ReplicaSetCondition{condImagePullBackOff(), condReplicaFailure()}},
		},
		{
			// 相同类型条件应被覆盖，不产生重复项。
			name: "overwrite",

			status: &apps.ReplicaSetStatus{Conditions: []apps.ReplicaSetCondition{condReplicaFailure()}},
			cond:   condReplicaFailure2(),

			expectedStatus: &apps.ReplicaSetStatus{Conditions: []apps.ReplicaSetCondition{condReplicaFailure2()}},
		},
	}

	for _, test := range tests {
		SetCondition(test.status, test.cond)
		if !reflect.DeepEqual(test.status, test.expectedStatus) {
			t.Errorf("%s: expected status: %v, got: %v", test.name, test.expectedStatus, test.status)
		}
	}
}

// TestRemoveCondition 验证 RemoveCondition：空状态无操作、删除存在的条件、
// 以及删除不存在的类型时保持状态不变。
func TestRemoveCondition(t *testing.T) {
	tests := []struct {
		name string

		status   *apps.ReplicaSetStatus
		condType apps.ReplicaSetConditionType

		expectedStatus *apps.ReplicaSetStatus
	}{
		{
			// 空状态下删除条件应是安全的空操作。
			name: "remove from empty status",

			status:   &apps.ReplicaSetStatus{},
			condType: apps.ReplicaSetReplicaFailure,

			expectedStatus: &apps.ReplicaSetStatus{},
		},
		{
			// 删除唯一存在的条件后状态清空。
			name: "simple remove",

			status:   &apps.ReplicaSetStatus{Conditions: []apps.ReplicaSetCondition{condReplicaFailure()}},
			condType: apps.ReplicaSetReplicaFailure,

			expectedStatus: &apps.ReplicaSetStatus{},
		},
		{
			// 删除不存在的类型不应影响其他条件。
			name: "doesn't remove anything",

			status:   status(),
			condType: imagePullBackOff,

			expectedStatus: status(),
		},
	}

	for _, test := range tests {
		RemoveCondition(test.status, test.condType)
		if !reflect.DeepEqual(test.status, test.expectedStatus) {
			t.Errorf("%s: expected status: %v, got: %v", test.name, test.expectedStatus, test.status)
		}
	}
}

// TestSlowStartBatch 验证 slowStartBatch 的批量放大与失败即停语义：
// 每批成功后批量翻倍，某批失败后立即停止，不改变已成功的数量。
func TestSlowStartBatch(t *testing.T) {
	fakeErr := fmt.Errorf("fake error")
	callCnt := 0
	callLimit := 0
	// slowStartBatch 可能并发调用 fn，用锁保护共享计数器。
	var lock sync.Mutex
	fn := func() error {
		lock.Lock()
		defer lock.Unlock()
		callCnt++
		// 超过调用上限即模拟失败，用于观察失败批次后的行为。
		if callCnt > callLimit {
			return fakeErr
		}
		return nil
	}

	tests := []struct {
		name              string
		count             int
		callLimit         int
		fn                func() error
		expectedSuccesses int
		expectedErr       error
		expectedCallCnt   int
	}{
		{
			name:              "callLimit = 0 (all fail)",
			count:             10,
			callLimit:         0,
			fn:                fn,
			expectedSuccesses: 0,
			expectedErr:       fakeErr,
			expectedCallCnt:   1, // 第一批至少调用一次，随后失败立即停止。
		},
		{
			name:              "callLimit = count (all succeed)",
			count:             10,
			callLimit:         10,
			fn:                fn,
			expectedSuccesses: 10,
			expectedErr:       nil,
			expectedCallCnt:   10, // 1 + 2 + 4 + 3：最后一批受剩余数量限制。
		},
		{
			name:              "callLimit < count (some succeed)",
			count:             10,
			callLimit:         5,
			fn:                fn,
			expectedSuccesses: 5,
			expectedErr:       fakeErr,
			expectedCallCnt:   7, // 1 + 2 + 4：第三批失败后不再继续。
		},
	}

	for _, test := range tests {
		// 每个用例重置计数器和上限，避免相互干扰。
		callCnt = 0
		callLimit = test.callLimit
		successes, err := slowStartBatch(test.count, 1, test.fn)
		if successes != test.expectedSuccesses {
			t.Errorf("%s: unexpected processed batch size, expected %d, got %d", test.name, test.expectedSuccesses, successes)
		}
		if err != test.expectedErr {
			t.Errorf("%s: unexpected processed batch size, expected %v, got %v", test.name, test.expectedErr, err)
		}
		// 验证某批失败后 slowStartBatch 不再发起后续调用。
		if callCnt != test.expectedCallCnt {
			t.Errorf("%s: slowStartBatch() still tries calls after a batch fails, expected %d calls, got %d", test.name, test.expectedCallCnt, callCnt)
		}
	}
}

// TestGetPodsToDelete 验证缩容时待删除 Pod 的选择顺序与结果：
// 优先未调度、Pending、未就绪、与相关 Pod 同节点、不同节点；
// diff 为 0 或超过数量时分别返回空列表或全部候选。
func TestGetPodsToDelete(t *testing.T) {
	labelMap := map[string]string{"name": "foo"}
	rs := newReplicaSet(1, labelMap)
	// 未调度的 Pending Pod，应最先被选中删除。
	unscheduledPendingPod := newPod("unscheduled-pending-pod", rs, v1.PodPending, nil, true)
	// 已调度但仍 Pending 的 Pod，优先级次之。
	scheduledPendingPod := newPod("scheduled-pending-pod", rs, v1.PodPending, nil, true)
	scheduledPendingPod.Spec.NodeName = "fake-node"
	// 已运行但未就绪的 Pod，优先级排在 Pending 之后。
	scheduledRunningNotReadyPod := newPod("scheduled-running-not-ready-pod", rs, v1.PodRunning, nil, true)
	scheduledRunningNotReadyPod.Spec.NodeName = "fake-node"
	scheduledRunningNotReadyPod.Status.Conditions = []v1.PodCondition{
		{
			Type:   v1.PodReady,
			Status: v1.ConditionFalse,
		},
	}
	// 已就绪 Pod，调度节点用于验证“与相关 Pod 同节点优先删除”。
	scheduledRunningReadyPodOnNode1 := newPod("scheduled-running-ready-pod-on-node-1", rs, v1.PodRunning, nil, true)
	scheduledRunningReadyPodOnNode1.Spec.NodeName = "fake-node-1"
	scheduledRunningReadyPodOnNode1.Status.Conditions = []v1.PodCondition{
		{
			Type:   v1.PodReady,
			Status: v1.ConditionTrue,
		},
	}
	// 另一个节点上的已就绪 Pod，与 node-1 的 Pod 形成对比。
	scheduledRunningReadyPodOnNode2 := newPod("scheduled-running-ready-pod-on-node-2", rs, v1.PodRunning, nil, true)
	scheduledRunningReadyPodOnNode2.Spec.NodeName = "fake-node-2"
	scheduledRunningReadyPodOnNode2.Status.Conditions = []v1.PodCondition{
		{
			Type:   v1.PodReady,
			Status: v1.ConditionTrue,
		},
	}

	tests := []struct {
		name string
		pods []*v1.Pod
		// related 为 nil 时默认使用 pods 本身作为相关 Pod 集合。
		related              []*v1.Pod
		diff                 int
		expectedPodsToDelete []*v1.Pod
	}{
		// 删除优先级：未调度 Pending > 已调度 Pending > 未就绪 > 与相关 Pod 同节点的就绪 Pod > 不同节点的就绪 Pod。
		// 注意 Pending Pod 不可能处于 Ready 状态。
		{
			name:                 "len(pods) = 0 (i.e., diff = 0 too)",
			pods:                 []*v1.Pod{},
			diff:                 0,
			expectedPodsToDelete: []*v1.Pod{},
		},
		{
			name: "diff = len(pods)",
			pods: []*v1.Pod{
				scheduledRunningNotReadyPod,
				scheduledRunningReadyPodOnNode1,
			},
			// diff 等于候选数，全部候选都进入删除列表。
			diff:                 2,
			expectedPodsToDelete: []*v1.Pod{scheduledRunningNotReadyPod, scheduledRunningReadyPodOnNode1},
		},
		{
			name: "diff < len(pods)",
			pods: []*v1.Pod{
				scheduledRunningReadyPodOnNode1,
				scheduledRunningNotReadyPod,
			},
			diff:                 1,
			expectedPodsToDelete: []*v1.Pod{scheduledRunningNotReadyPod},
		},
		{
			name: "various pod phases and conditions, diff = len(pods)",
			pods: []*v1.Pod{
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode2,
				scheduledRunningNotReadyPod,
				scheduledPendingPod,
				unscheduledPendingPod,
			},
			diff: 6,
			expectedPodsToDelete: []*v1.Pod{
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode2,
				scheduledRunningNotReadyPod,
				scheduledPendingPod,
				unscheduledPendingPod,
			},
		},
		{
			name: "various pod phases and conditions, diff = len(pods), relatedPods empty",
			pods: []*v1.Pod{
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode2,
				scheduledRunningNotReadyPod,
				scheduledPendingPod,
				unscheduledPendingPod,
			},
			related: []*v1.Pod{},
			diff:    6,
			expectedPodsToDelete: []*v1.Pod{
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode2,
				scheduledRunningNotReadyPod,
				scheduledPendingPod,
				unscheduledPendingPod,
			},
		},
		{
			name: "scheduled vs unscheduled, diff < len(pods)",
			pods: []*v1.Pod{
				scheduledPendingPod,
				unscheduledPendingPod,
			},
			diff: 1,
			expectedPodsToDelete: []*v1.Pod{
				unscheduledPendingPod,
			},
		},
		{
			name: "ready vs not-ready, diff < len(pods)",
			pods: []*v1.Pod{
				scheduledRunningReadyPodOnNode1,
				scheduledRunningNotReadyPod,
				scheduledRunningNotReadyPod,
			},
			diff: 2,
			expectedPodsToDelete: []*v1.Pod{
				scheduledRunningNotReadyPod,
				scheduledRunningNotReadyPod,
			},
		},
		{
			name: "ready and colocated with another ready pod vs not colocated, diff < len(pods)",
			pods: []*v1.Pod{
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode2,
			},
			related: []*v1.Pod{
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode2,
				scheduledRunningReadyPodOnNode2,
			},
			diff: 1,
			expectedPodsToDelete: []*v1.Pod{
				scheduledRunningReadyPodOnNode2,
			},
		},
		{
			name: "pending vs running, diff < len(pods)",
			pods: []*v1.Pod{
				scheduledPendingPod,
				scheduledRunningNotReadyPod,
			},
			diff: 1,
			expectedPodsToDelete: []*v1.Pod{
				scheduledPendingPod,
			},
		},
		{
			name: "various pod phases and conditions, diff < len(pods)",
			pods: []*v1.Pod{
				scheduledRunningReadyPodOnNode1,
				scheduledRunningReadyPodOnNode2,
				scheduledRunningNotReadyPod,
				scheduledPendingPod,
				unscheduledPendingPod,
			},
			diff: 3,
			expectedPodsToDelete: []*v1.Pod{
				unscheduledPendingPod,
				scheduledPendingPod,
				scheduledRunningNotReadyPod,
			},
		},
	}

	for _, test := range tests {
		// 未显式给出相关 Pod 时，默认使用待选 Pod 集合。
		related := test.related
		if related == nil {
			related = test.pods
		}
		podsToDelete := getPodsToDelete(test.pods, related, test.diff)
		if len(podsToDelete) != len(test.expectedPodsToDelete) {
			t.Errorf("%s: unexpected pods to delete, expected %v, got %v", test.name, test.expectedPodsToDelete, podsToDelete)
		}
		if !reflect.DeepEqual(podsToDelete, test.expectedPodsToDelete) {
			t.Errorf("%s: unexpected pods to delete, expected %v, got %v", test.name, test.expectedPodsToDelete, podsToDelete)
		}
	}
}

// TestGetPodKeys 验证 getPodKeys 将 Pod 列表转换为 namespace/name 形式的键：
// 空列表返回空键列表，非空列表按输入顺序返回各 Pod 的键。
func TestGetPodKeys(t *testing.T) {
	labelMap := map[string]string{"name": "foo"}
	rs := newReplicaSet(1, labelMap)
	pod1 := newPod("pod1", rs, v1.PodRunning, nil, true)
	pod2 := newPod("pod2", rs, v1.PodRunning, nil, true)

	tests := []struct {
		name            string
		pods            []*v1.Pod
		expectedPodKeys []string
	}{
		{
			"len(pods) = 0 (i.e., pods = nil)",
			[]*v1.Pod{},
			[]string{},
		},
		{
			"len(pods) > 0",
			[]*v1.Pod{
				pod1,
				pod2,
			},
			[]string{"default/pod1", "default/pod2"},
		},
	}

	for _, test := range tests {
		podKeys := getPodKeys(test.pods)
		if len(podKeys) != len(test.expectedPodKeys) {
			t.Errorf("%s: unexpected keys for pods to delete, expected %v, got %v", test.name, test.expectedPodKeys, podKeys)
		}
		for i := 0; i < len(podKeys); i++ {
			if podKeys[i] != test.expectedPodKeys[i] {
				t.Errorf("%s: unexpected keys for pods to delete, expected %v, got %v", test.name, test.expectedPodKeys, podKeys)
			}
		}
	}
}
