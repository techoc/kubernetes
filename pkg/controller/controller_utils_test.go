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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	utiltesting "k8s.io/client-go/util/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/apis/core"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/controller/testutil"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/securitycontext"
	"k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NewFakeControllerExpectationsLookup 创建带假时钟的期望 store，便于在测试中推进时间触发过期。
func NewFakeControllerExpectationsLookup(ttl time.Duration) (*ControllerExpectations, *testingclock.FakeClock) {
	fakeTime := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	fakeClock := testingclock.NewFakeClock(fakeTime)
	ttlPolicy := &cache.TTLPolicy{TTL: ttl, Clock: fakeClock}
	ttlStore := cache.NewFakeExpirationStore(
		ExpKeyFunc, nil, ttlPolicy, fakeClock)
	return &ControllerExpectations{ttlStore}, fakeClock
}

// newReplicationController 构造测试用 RC，标签和选择器与 newPodList 生成的 Pod 对齐。
func newReplicationController(replicas int) *v1.ReplicationController {
	rc := &v1.ReplicationController{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			UID:             uuid.NewUUID(),
			Name:            "foobar",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "18",
		},
		Spec: v1.ReplicationControllerSpec{
			Replicas: ptr.To[int32](int32(replicas)),
			Selector: map[string]string{"foo": "bar"},
			Template: &v1.PodTemplateSpec{
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
	return rc
}

// newPodList 为给定 RC 创建 count 个同标签、同命名空间、指定阶段的 Pod，并可加入 store。
func newPodList(store cache.Store, count int, status v1.PodPhase, rc *v1.ReplicationController) *v1.PodList {
	pods := []v1.Pod{}
	for i := 0; i < count; i++ {
		newPod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("pod%d", i),
				Labels:    rc.Spec.Selector,
				Namespace: rc.Namespace,
			},
			Status: v1.PodStatus{Phase: status},
		}
		if store != nil {
			store.Add(&newPod)
		}
		pods = append(pods, newPod)
	}
	return &v1.PodList{
		Items: pods,
	}
}

// newReplicaSet 构造测试用副本集，标签与 newPodList 生成的 Pod 对齐。
func newReplicaSet(name string, replicas int, rsUuid types.UID) *apps.ReplicaSet {
	return &apps.ReplicaSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			UID:             rsUuid,
			Name:            name,
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "18",
		},
		Spec: apps.ReplicaSetSpec{
			Replicas: ptr.To[int32](int32(replicas)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
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
}

// TestControllerExpectations 验证期望计数可被并发递减、满足或过期后控制器可同步。
func TestControllerExpectations(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	ttl := 30 * time.Second
	e, fakeClock := NewFakeControllerExpectationsLookup(ttl)
	// 实际 RC 单次同步只会创建或删除副本，这里同时设置增/删期望是为了覆盖并发递减逻辑。
	adds, dels := 10, 30
	rc := newReplicationController(1)

	// 先按 RC 的对象键登记期望，模拟向 apiserver 发出创建/删除请求。
	rcKey, err := KeyFunc(rc)
	require.NoError(t, err, "Couldn't get key for object %#v: %v", rc, err)

	e.SetExpectations(logger, rcKey, adds, dels)
	var wg sync.WaitGroup
	// 多观察一次以覆盖计数降到负数的情况：生产中失败创建或 informer 重复事件都会如此。
	for i := 0; i < adds+1; i++ {
		wg.Add(1)
		go func() {
			e.CreationObserved(logger, rcKey)
			wg.Done()
		}()
	}
	wg.Wait()

	// 新增期望已满足，但删除期望仍未满足，控制器不应同步。
	assert.False(t, e.SatisfiedExpectations(logger, rcKey), "Rc will sync before expectations are met")

	for i := 0; i < dels+1; i++ {
		wg.Add(1)
		go func() {
			e.DeletionObserved(logger, rcKey)
			wg.Done()
		}()
	}
	wg.Wait()

	tests := []struct {
		name                      string
		expectationsToSet         []int
		expireExpectations        bool
		wantPodExpectations       []int64
		wantExpectationsSatisfied bool
	}{
		{
			name:                      "Expectations have been surpassed",
			expireExpectations:        false,
			wantPodExpectations:       []int64{int64(-1), int64(-1)},
			wantExpectationsSatisfied: true,
		},
		{
			name:                      "Old expectations are cleared because of ttl",
			expectationsToSet:         []int{1, 2},
			expireExpectations:        true,
			wantPodExpectations:       []int64{int64(1), int64(2)},
			wantExpectationsSatisfied: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if len(test.expectationsToSet) > 0 {
				e.SetExpectations(logger, rcKey, test.expectationsToSet[0], test.expectationsToSet[1])
			}
			podExp, exists, err := e.GetExpectations(rcKey)
			require.NoError(t, err, "Could not get expectations for rc, exists %v and err %v", exists, err)
			assert.True(t, exists, "Could not get expectations for rc, exists %v and err %v", exists, err)

			add, del := podExp.GetExpectations()
			assert.Equal(t, test.wantPodExpectations[0], add, "Unexpected pod expectations %#v", podExp)
			assert.Equal(t, test.wantPodExpectations[1], del, "Unexpected pod expectations %#v", podExp)
			assert.Equal(t, test.wantExpectationsSatisfied, e.SatisfiedExpectations(logger, rcKey), "Expectations are met but the rc will not sync")

			if test.expireExpectations {
				// 推进假时钟越过 TTL，验证过期后期望被视为满足。
				fakeClock.Step(ttl + 1)
				assert.True(t, e.SatisfiedExpectations(logger, rcKey), "Expectations should have expired but didn't")
			}
		})
	}
}

// TestUIDExpectations 验证按 UID 跟踪的删除期望不会重复计数，且清理后不留残余 UID。
func TestUIDExpectations(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	uidExp := NewUIDTrackingControllerExpectations(NewControllerExpectations())
	type test struct {
		name        string
		numReplicas int
	}

	// 打乱用例顺序，避免测试依赖先前 RC 的残留状态。
	shuffleTests := func(tests []test) {
		for i := range tests {
			j := rand.Intn(i + 1)
			tests[i], tests[j] = tests[j], tests[i]
		}
	}

	// 为用例构造 RC 和 Pod，并登记对应 UID 的删除期望。
	getRcDataFrom := func(test test) (string, []string) {
		rc := newReplicationController(test.numReplicas)

		// 选择器加入唯一 rcName，避免不同用例的键相互冲突。
		rcName := fmt.Sprintf("rc-%v", test.numReplicas)
		rc.Name = rcName
		rc.Spec.Selector[rcName] = rcName

		podList := newPodList(nil, 5, v1.PodRunning, rc)
		rcKey, err := KeyFunc(rc)
		if err != nil {
			t.Fatalf("Couldn't get key for object %#v: %v", rc, err)
		}

		rcPodNames := []string{}
		for i := range podList.Items {
			p := &podList.Items[i]
			p.Name = fmt.Sprintf("%v-%v", p.Name, rc.Name)
			rcPodNames = append(rcPodNames, PodKey(p))
		}
		uidExp.ExpectDeletions(logger, rcKey, rcPodNames)

		return rcKey, rcPodNames
	}

	tests := []test{
		{name: "Replication controller with 2 replicas", numReplicas: 2},
		{name: "Replication controller with 1 replica", numReplicas: 1},
		{name: "Replication controller with no replicas", numReplicas: 0},
		{name: "Replication controller with 5 replicas", numReplicas: 5},
	}

	shuffleTests(tests)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			rcKey, rcPodNames := getRcDataFrom(test)
			assert.False(t, uidExp.SatisfiedExpectations(logger, rcKey),
				"Controller %v satisfied expectations before deletion", rcKey)

			for _, p := range rcPodNames {
				uidExp.DeletionObserved(logger, rcKey, p)
			}

			assert.True(t, uidExp.SatisfiedExpectations(logger, rcKey),
				"Controller %v didn't satisfy expectations after deletion", rcKey)

			uidExp.DeleteExpectations(logger, rcKey)

			assert.Nil(t, uidExp.GetUIDs(rcKey),
				"Failed to delete uid expectations for %v", rcKey)
		})
	}
}

// TestCreatePodsWithGenerateName 验证两种创建路径均以 POST 提交正确 Pod，并触发 OnWrite 回调。
func TestCreatePodsWithGenerateName(t *testing.T) {
	ns := metav1.NamespaceDefault
	generateName := "hello-"
	controllerSpec := newReplicationController(1)
	controllerRef := metav1.NewControllerRef(controllerSpec, v1.SchemeGroupVersion.WithKind("ReplicationController"))

	type test struct {
		name            string
		podCreationFunc func(podControl RealPodControl) error
		wantPod         *v1.Pod
	}
	var tests = []test{
		{
			name: "Create pod",
			podCreationFunc: func(podControl RealPodControl) error {
				return podControl.CreatePods(context.TODO(), ns, controllerSpec.Spec.Template, controllerSpec, controllerRef)
			},
			wantPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels:       controllerSpec.Spec.Template.Labels,
					GenerateName: fmt.Sprintf("%s-", controllerSpec.Name),
				},
				Spec: controllerSpec.Spec.Template.Spec,
			},
		},
		{
			name: "Create pod with generate name",
			podCreationFunc: func(podControl RealPodControl) error {
				// 断言创建请求以控制器模板中的 Pod 作为 POST 请求体。
				return podControl.CreatePodsWithGenerateName(context.TODO(), ns, controllerSpec.Spec.Template, controllerSpec, controllerRef, generateName)
			},
			wantPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels:          controllerSpec.Spec.Template.Labels,
					GenerateName:    generateName,
					OwnerReferences: []metav1.OwnerReference{*controllerRef},
				},
				Spec: controllerSpec.Spec.Template.Spec,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 假服务端返回占位 Pod，真正的校验对象是它收到的请求体。
			body := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "empty_pod"}})
			fakeHandler := utiltesting.FakeHandler{
				StatusCode:   200,
				ResponseBody: string(body),
			}
			testServer := httptest.NewServer(&fakeHandler)
			defer testServer.Close()
			clientset := clientset.NewForConfigOrDie(&restclient.Config{Host: testServer.URL, ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}, ContentType: runtime.ContentTypeJSON}})

			callbackCalled := false
			podControl := RealPodControl{
				KubeClient: clientset,
				Recorder:   &record.FakeRecorder{},
				OnWrite: func(*v1.Pod, *metav1.OwnerReference) {
					callbackCalled = true
				},
			}

			err := test.podCreationFunc(podControl)
			require.NoError(t, err, "unexpected error: %v", err)
			assert.True(t, callbackCalled, "OnWrite callback was not called")

			fakeHandler.ValidateRequest(t, "/api/v1/namespaces/default/pods", "POST", nil)
			var actualPod = &v1.Pod{}
			err = json.Unmarshal([]byte(fakeHandler.RequestBody), actualPod)
			require.NoError(t, err, "unexpected error: %v", err)
			assert.True(t, apiequality.Semantic.DeepDerivative(test.wantPod, actualPod),
				"Body: %s", fakeHandler.RequestBody)
		})
	}
}

// TestPatchPodCallbacks 验证 OnWrite 仅在补丁成功时触发，失败（如 Pod 不存在）时不触发。
func TestPatchPodCallbacks(t *testing.T) {
	fakeClient := fake.NewSimpleClientset(
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
			},
		},
	)
	wroteCallbackCalled := false
	podControl := RealPodControl{
		KubeClient: fakeClient,
		Recorder:   &record.FakeRecorder{},
		OnWrite: func(pod *v1.Pod, ownerRef *metav1.OwnerReference) {
			wroteCallbackCalled = true
		},
	}

	err := podControl.PatchPod(context.TODO(), "", "non-existing-pod", []byte("{}"))
	assert.False(t, wroteCallbackCalled, "OnWrite callback was called when not expected")
	assert.True(t, apierrors.IsNotFound(err), "Expected not found error")

	err = podControl.PatchPod(context.TODO(), "", "test-pod", []byte("{}"))
	assert.True(t, wroteCallbackCalled, "OnWrite callback was not called")
	assert.NoError(t, err, "Expected no error")
}

// TestDeletePodsAllowsMissing 验证删除不存在的 Pod 会返回 NotFound 错误。
func TestDeletePodsAllowsMissing(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	podControl := RealPodControl{
		KubeClient: fakeClient,
		Recorder:   &record.FakeRecorder{},
	}

	controllerSpec := newReplicationController(1)

	err := podControl.DeletePod(context.TODO(), "namespace-name", "podName", controllerSpec)
	assert.True(t, apierrors.IsNotFound(err))
}

// TestCountTerminatingPods 验证 CountTerminatingPods 与 FilterTerminatingPods 都只计入已设删除时间戳的 Pod。
func TestCountTerminatingPods(t *testing.T) {
	now := metav1.Now()

	// 这个 RC 本身无用，只是借 newPodList 给 Pod 加上标签和命名空间。
	rc := newReplicationController(0)
	podList := newPodList(nil, 7, v1.PodRunning, rc)
	podList.Items[0].Status.Phase = v1.PodSucceeded
	podList.Items[1].Status.Phase = v1.PodFailed
	podList.Items[2].Status.Phase = v1.PodPending
	podList.Items[2].SetDeletionTimestamp(&now)
	podList.Items[3].Status.Phase = v1.PodRunning
	podList.Items[3].SetDeletionTimestamp(&now)
	var podPointers []*v1.Pod
	for i := range podList.Items {
		podPointers = append(podPointers, &podList.Items[i])
	}

	terminatingPods := CountTerminatingPods(podPointers)

	assert.Equal(t, int32(2), terminatingPods)

	terminatingList := FilterTerminatingPods(podPointers)
	assert.Len(t, terminatingList, int(2))
}

// TestClaimedPodFiltering 验证只返回受目标控制器掌控且匹配选择器的 Pod。
func TestClaimedPodFiltering(t *testing.T) {
	rsUUID := uuid.NewUUID()

	type podData struct {
		podName         string
		ownerReferences []metav1.OwnerReference
		labels          map[string]string
	}

	type test struct {
		name         string
		pods         []podData
		wantPodNames []string
	}

	tests := []test{
		{
			name: "Filters claimed pods",
			pods: []podData{
				// 仅有一个所属者引用的场景。
				{podName: "claimed-1", labels: map[string]string{"foo": "bar"}, ownerReferences: []metav1.OwnerReference{
					{UID: rsUUID, Controller: ptr.To(true)},
				}},
				{podName: "wrong-selector-1", labels: map[string]string{"foo": "baz"}, ownerReferences: []metav1.OwnerReference{
					{UID: rsUUID, Controller: ptr.To(true)},
				}},
				{podName: "non-controller-1", labels: map[string]string{"foo": "bar"}, ownerReferences: []metav1.OwnerReference{
					{UID: rsUUID, Controller: nil},
				}},
				{podName: "other-controller-1", labels: map[string]string{"foo": "bar"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID(), Controller: ptr.To(true)},
				}},
				{podName: "other-workload-1", labels: map[string]string{"foo": "bee"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID(), Controller: ptr.To(true)},
				}},
				{podName: "standalone-pod-1", labels: map[string]string{"foo": "beetle"}, ownerReferences: []metav1.OwnerReference{}},
				// 额外所属者引用中 controller=false，不应影响真正控制器的判定。
				{podName: "claimed-2", labels: map[string]string{"foo": "bar"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID(), Controller: ptr.To(false)},
					{UID: rsUUID, Controller: ptr.To(true)},
				}},
				{podName: "wrong-selector-2", labels: map[string]string{"foo": "baz"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID(), Controller: ptr.To(false)},
					{UID: rsUUID, Controller: ptr.To(true)},
				}},
				{podName: "non-controller-2", labels: map[string]string{"foo": "bar"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID(), Controller: ptr.To(false)},
					{UID: rsUUID, Controller: ptr.To(false)},
				}},
				{podName: "other-controller-2", labels: map[string]string{"foo": "bar"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID(), Controller: ptr.To(false)},
					{UID: uuid.NewUUID(), Controller: ptr.To(true)},
				}},
				{podName: "other-workload-1", labels: map[string]string{"foo": "bee"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID(), Controller: ptr.To(false)},
					{UID: uuid.NewUUID(), Controller: ptr.To(true)},
				}},
				{podName: "standalone-pod-1", labels: map[string]string{"foo": "beetle"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID(), Controller: ptr.To(false)},
				}},
				// 额外所属者引用的 controller 为 nil，同样不应干扰判定。
				{podName: "claimed-3", labels: map[string]string{"foo": "bar"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID()},
					{UID: rsUUID, Controller: ptr.To(true)},
				}},
				{podName: "wrong-selector-3", labels: nil, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID()},
					{UID: rsUUID, Controller: ptr.To(true)},
				}},
				{podName: "non-controller-3", labels: map[string]string{"foo": "bar"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID()},
					{UID: rsUUID, Controller: nil},
				}},
				{podName: "other-controller-3", labels: map[string]string{"foo": "bar"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID()},
					{UID: uuid.NewUUID(), Controller: ptr.To(true)},
				}},
				{podName: "other-workload-1", labels: map[string]string{"foo": "bee"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID()},
				}},
				{podName: "standalone-pod-1", labels: map[string]string{"foo": "beetle"}, ownerReferences: []metav1.OwnerReference{
					{UID: uuid.NewUUID()},
				}},
			},
			wantPodNames: []string{"claimed-1", "claimed-2", "claimed-3"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 这个 RS 本身无用，只是借 newPodList 给 Pod 加上标签和命名空间。
			rs := newReplicaSet("test-claim", 3, rsUUID)
			var pods []*v1.Pod
			for _, p := range test.pods {
				pods = append(pods, &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:            p.podName,
						Namespace:       rs.Namespace,
						Labels:          p.labels,
						OwnerReferences: p.ownerReferences,
					},
					Status: v1.PodStatus{Phase: v1.PodRunning},
				})
			}

			selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
			if err != nil {
				t.Fatalf("Couldn't get selector for object %#v: %v", rs, err)
			}
			got := FilterClaimedPods(rs, selector, pods)
			gotNames := sets.NewString()
			for _, pod := range got {
				gotNames.Insert(pod.Name)
			}

			if diff := cmp.Diff(test.wantPodNames, gotNames.List()); diff != "" {
				t.Errorf("Active pod names (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestActivePodFiltering 验证只返回未终止且未进入删除的 Pod。
func TestActivePodFiltering(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	type podData struct {
		podName  string
		podPhase v1.PodPhase
	}

	type test struct {
		name         string
		pods         []podData
		wantPodNames []string
	}

	tests := []test{
		{
			name: "Filters active pods",
			pods: []podData{
				{podName: "pod-1", podPhase: v1.PodSucceeded},
				{podName: "pod-2", podPhase: v1.PodFailed},
				{podName: "pod-3"},
				{podName: "pod-4"},
				{podName: "pod-5"},
			},
			wantPodNames: []string{"pod-3", "pod-4", "pod-5"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 这个 RC 本身无用，只是借 newPodList 给 Pod 加上标签和命名空间。
			rc := newReplicationController(0)
			podList := newPodList(nil, 5, v1.PodRunning, rc)
			for idx, testPod := range test.pods {
				podList.Items[idx].Name = testPod.podName
				podList.Items[idx].Status.Phase = testPod.podPhase
			}

			var podPointers []*v1.Pod
			for i := range podList.Items {
				podPointers = append(podPointers, &podList.Items[i])
			}
			got := FilterActivePods(logger, podPointers)
			gotNames := sets.NewString()
			for _, pod := range got {
				gotNames.Insert(pod.Name)
			}

			if diff := cmp.Diff(test.wantPodNames, gotNames.List()); diff != "" {
				t.Errorf("Active pod names (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestSortingActivePods 验证 ActivePods 的删除优先级排序，并通过随机打乱输入消除顺序依赖。
func TestSortingActivePods(t *testing.T) {
	now := metav1.Now()
	then := metav1.Time{Time: now.AddDate(0, -1, 0)}

	restartAlways := v1.ContainerRestartPolicyAlways
	tests := []struct {
		name      string
		pods      []v1.Pod
		wantOrder []string
	}{
		{
			name: "Sorts by active pod",
			pods: []v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "unscheduled"},
					Spec:       v1.PodSpec{NodeName: ""},
					Status:     v1.PodStatus{Phase: v1.PodPending},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "scheduledButPending"},
					Spec:       v1.PodSpec{NodeName: "bar"},
					Status:     v1.PodStatus{Phase: v1.PodPending},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "unknownPhase"},
					Spec:       v1.PodSpec{NodeName: "foo"},
					Status:     v1.PodStatus{Phase: v1.PodUnknown},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "runningButNotReady"},
					Spec:       v1.PodSpec{NodeName: "foo"},
					Status:     v1.PodStatus{Phase: v1.PodRunning},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "runningNoLastTransitionTime"},
					Spec:       v1.PodSpec{NodeName: "foo"},
					Status: v1.PodStatus{
						Phase:             v1.PodRunning,
						Conditions:        []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
						ContainerStatuses: []v1.ContainerStatus{{RestartCount: 3}, {RestartCount: 0}},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "runningWithLastTransitionTime"},
					Spec:       v1.PodSpec{NodeName: "foo"},
					Status: v1.PodStatus{
						Phase:             v1.PodRunning,
						Conditions:        []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: now}},
						ContainerStatuses: []v1.ContainerStatus{{RestartCount: 3}, {RestartCount: 0}},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "runningLongerTime"},
					Spec:       v1.PodSpec{NodeName: "foo"},
					Status: v1.PodStatus{
						Phase:             v1.PodRunning,
						Conditions:        []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: then}},
						ContainerStatuses: []v1.ContainerStatus{{RestartCount: 3}, {RestartCount: 0}},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "lowerSidecarContainerRestartCount", CreationTimestamp: now},
					Spec: v1.PodSpec{
						NodeName: "foo",
						InitContainers: []v1.Container{{
							Name:          "sidecar",
							RestartPolicy: &restartAlways,
						}},
					},
					Status: v1.PodStatus{
						Phase:                 v1.PodRunning,
						Conditions:            []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: then}},
						ContainerStatuses:     []v1.ContainerStatus{{RestartCount: 2}, {RestartCount: 1}},
						InitContainerStatuses: []v1.ContainerStatus{{Name: "sidecar", RestartCount: 2}},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "lowerContainerRestartCount", CreationTimestamp: now},
					Spec:       v1.PodSpec{NodeName: "foo"},
					Status: v1.PodStatus{
						Phase:             v1.PodRunning,
						Conditions:        []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: then}},
						ContainerStatuses: []v1.ContainerStatus{{RestartCount: 2}, {RestartCount: 1}},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "oldest", CreationTimestamp: then},
					Spec:       v1.PodSpec{NodeName: "foo"},
					Status: v1.PodStatus{
						Phase:             v1.PodRunning,
						Conditions:        []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: then}},
						ContainerStatuses: []v1.ContainerStatus{{RestartCount: 2}, {RestartCount: 1}},
					},
				},
			},
			wantOrder: []string{
				"unscheduled",
				"scheduledButPending",
				"unknownPhase",
				"runningButNotReady",
				"runningNoLastTransitionTime",
				"runningWithLastTransitionTime",
				"runningLongerTime",
				"lowerSidecarContainerRestartCount",
				"lowerContainerRestartCount",
				"oldest",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			numPods := len(test.pods)

			// 多次随机排列后排序，确认结果不依赖输入顺序。
			for i := 0; i < 20; i++ {
				idx := rand.Perm(numPods)
				randomizedPods := make([]*v1.Pod, numPods)
				for j := 0; j < numPods; j++ {
					randomizedPods[j] = &test.pods[idx[j]]
				}

				sort.Sort(ActivePods(randomizedPods))
				gotOrder := make([]string, len(randomizedPods))
				for i := range randomizedPods {
					gotOrder[i] = randomizedPods[i].Name
				}

				if diff := cmp.Diff(test.wantOrder, gotOrder); diff != "" {
					t.Errorf("Sorted active pod names (-want,+got):\n%s", diff)
				}
			}
		})
	}
}

// TestSortingActivePodsWithRanks 验证带 rank 的 Pod 排序：等价判定、各优先级规则及特性开关行为。
func TestSortingActivePodsWithRanks(t *testing.T) {
	now := metav1.Now()
	then1Month := metav1.Time{Time: now.AddDate(0, -1, 0)}
	then2Hours := metav1.Time{Time: now.Add(-2 * time.Hour)}
	then5Hours := metav1.Time{Time: now.Add(-5 * time.Hour)}
	then8Hours := metav1.Time{Time: now.Add(-8 * time.Hour)}
	zeroTime := metav1.Time{}
	restartAlways := v1.ContainerRestartPolicyAlways
	// 构造带 sidecar、就绪时间和重启计数的 Pod，覆盖排序用到的全部字段。
	pod := func(podName, nodeName string, phase v1.PodPhase, ready bool, restarts int32, sideRestarts int32, readySince metav1.Time, created metav1.Time, annotations map[string]string) *v1.Pod {
		var conditions []v1.PodCondition
		var containerStatuses []v1.ContainerStatus
		var initContainerStatuses []v1.ContainerStatus
		if ready {
			conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: readySince}}
			containerStatuses = []v1.ContainerStatus{{RestartCount: restarts}}
			initContainerStatuses = []v1.ContainerStatus{{Name: "sidecar", RestartCount: sideRestarts}}
		}
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: created,
				Name:              podName,
				Annotations:       annotations,
			},
			Spec: v1.PodSpec{
				NodeName:       nodeName,
				InitContainers: []v1.Container{{Name: "sidecar", RestartPolicy: &restartAlways}},
			},
			Status: v1.PodStatus{
				Conditions:            conditions,
				ContainerStatuses:     containerStatuses,
				InitContainerStatuses: initContainerStatuses,
				Phase:                 phase,
			},
		}
	}
	var (
		unscheduledPod                      = pod("unscheduled", "", v1.PodPending, false, 0, 0, zeroTime, zeroTime, nil)
		scheduledPendingPod                 = pod("pending", "node", v1.PodPending, false, 0, 0, zeroTime, zeroTime, nil)
		unknownPhasePod                     = pod("unknown-phase", "node", v1.PodUnknown, false, 0, 0, zeroTime, zeroTime, nil)
		runningNotReadyPod                  = pod("not-ready", "node", v1.PodRunning, false, 0, 0, zeroTime, zeroTime, nil)
		runningReadyNoLastTransitionTimePod = pod("ready-no-last-transition-time", "node", v1.PodRunning, true, 0, 0, zeroTime, zeroTime, nil)
		runningReadyNow                     = pod("ready-now", "node", v1.PodRunning, true, 0, 0, now, now, nil)
		runningReadyThen                    = pod("ready-then", "node", v1.PodRunning, true, 0, 0, then1Month, then1Month, nil)
		runningReadyNowHighRestarts         = pod("ready-high-restarts", "node", v1.PodRunning, true, 9001, 0, now, now, nil)
		runningReadyNowHighSideRestarts     = pod("ready-high-restarts", "node", v1.PodRunning, true, 9001, 9001, now, now, nil)
		runningReadyNowCreatedThen          = pod("ready-now-created-then", "node", v1.PodRunning, true, 0, 0, now, then1Month, nil)
		lowPodDeletionCost                  = pod("low-deletion-cost", "node", v1.PodRunning, true, 0, 0, now, then1Month, map[string]string{core.PodDeletionCost: "10"})
		highPodDeletionCost                 = pod("high-deletion-cost", "node", v1.PodRunning, true, 0, 0, now, then1Month, map[string]string{core.PodDeletionCost: "100"})
		unscheduled5Hours                   = pod("unscheduled-5-hours", "", v1.PodPending, false, 0, 0, then5Hours, then5Hours, nil)
		unscheduled8Hours                   = pod("unscheduled-10-hours", "", v1.PodPending, false, 0, 0, then8Hours, then8Hours, nil)
		ready2Hours                         = pod("ready-2-hours", "", v1.PodRunning, true, 0, 0, then2Hours, then1Month, nil)
		ready5Hours                         = pod("ready-5-hours", "", v1.PodRunning, true, 0, 0, then5Hours, then1Month, nil)
		ready10Hours                        = pod("ready-10-hours", "", v1.PodRunning, true, 0, 0, then8Hours, then1Month, nil)
	)
	equalityTests := []struct {
		p1 *v1.Pod
		p2 *v1.Pod
	}{
		{p1: unscheduledPod},
		{p1: scheduledPendingPod},
		{p1: unknownPhasePod},
		{p1: runningNotReadyPod},
		{p1: runningReadyNowCreatedThen},
		{p1: runningReadyNow},
		{p1: runningReadyThen},
		{p1: runningReadyNowHighRestarts},
		{p1: runningReadyNowCreatedThen},
		{p1: unscheduled5Hours, p2: unscheduled8Hours},
		{p1: ready5Hours, p2: ready10Hours},
	}
	for i, test := range equalityTests {
		t.Run(fmt.Sprintf("Equality tests %d", i), func(t *testing.T) {
			// 未指定 p2 时用自身对比，验证同一 Pod 不被判定为小于自己。
			if test.p2 == nil {
				test.p2 = test.p1
			}
			podsWithRanks := ActivePodsWithRanks{
				Pods: []*v1.Pod{test.p1, test.p2},
				Rank: []int{1, 1},
				Now:  now,
			}
			if podsWithRanks.Less(0, 1) || podsWithRanks.Less(1, 0) {
				t.Errorf("expected pod %q to be equivalent to %q", test.p1.Name, test.p2.Name)
			}
		})
	}

	type podWithRank struct {
		pod  *v1.Pod
		rank int
	}
	inequalityTests := []struct {
		lesser, greater        podWithRank
		disablePodDeletioncost bool
	}{
		{lesser: podWithRank{unscheduledPod, 1}, greater: podWithRank{scheduledPendingPod, 2}},
		{lesser: podWithRank{unscheduledPod, 2}, greater: podWithRank{scheduledPendingPod, 1}},
		{lesser: podWithRank{scheduledPendingPod, 1}, greater: podWithRank{unknownPhasePod, 2}},
		{lesser: podWithRank{unknownPhasePod, 1}, greater: podWithRank{runningNotReadyPod, 2}},
		{lesser: podWithRank{runningNotReadyPod, 1}, greater: podWithRank{runningReadyNoLastTransitionTimePod, 1}},
		{lesser: podWithRank{runningReadyNoLastTransitionTimePod, 1}, greater: podWithRank{runningReadyNow, 1}},
		{lesser: podWithRank{runningReadyNow, 2}, greater: podWithRank{runningReadyNoLastTransitionTimePod, 1}},
		{lesser: podWithRank{runningReadyNow, 1}, greater: podWithRank{runningReadyThen, 1}},
		{lesser: podWithRank{runningReadyNow, 2}, greater: podWithRank{runningReadyThen, 1}},
		{lesser: podWithRank{runningReadyNowHighRestarts, 1}, greater: podWithRank{runningReadyNow, 1}},
		{lesser: podWithRank{runningReadyNowHighSideRestarts, 1}, greater: podWithRank{runningReadyNowHighRestarts, 1}},
		{lesser: podWithRank{runningReadyNow, 2}, greater: podWithRank{runningReadyNowHighRestarts, 1}},
		{lesser: podWithRank{runningReadyNow, 1}, greater: podWithRank{runningReadyNowCreatedThen, 1}},
		{lesser: podWithRank{runningReadyNowCreatedThen, 2}, greater: podWithRank{runningReadyNow, 1}},
		{lesser: podWithRank{lowPodDeletionCost, 2}, greater: podWithRank{highPodDeletionCost, 1}},
		{lesser: podWithRank{highPodDeletionCost, 2}, greater: podWithRank{lowPodDeletionCost, 1}, disablePodDeletioncost: true},
		{lesser: podWithRank{ready2Hours, 1}, greater: podWithRank{ready5Hours, 1}},
	}

	for i, test := range inequalityTests {
		t.Run(fmt.Sprintf("Inequality tests %d", i), func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.PodDeletionCost: !test.disablePodDeletioncost,
			})

			podsWithRanks := ActivePodsWithRanks{
				Pods: []*v1.Pod{test.lesser.pod, test.greater.pod},
				Rank: []int{test.lesser.rank, test.greater.rank},
				Now:  now,
			}
			if !podsWithRanks.Less(0, 1) {
				t.Errorf("expected pod %q with rank %v to be less than %q with rank %v", podsWithRanks.Pods[0].Name, podsWithRanks.Rank[0], podsWithRanks.Pods[1].Name, podsWithRanks.Rank[1])
			}
			if podsWithRanks.Less(1, 0) {
				t.Errorf("expected pod %q with rank %v not to be less than %v with rank %v", podsWithRanks.Pods[1].Name, podsWithRanks.Rank[1], podsWithRanks.Pods[0].Name, podsWithRanks.Rank[0])
			}
		})
	}
}

// TestNextPodAvailabilityCheck 验证距 Pod 变为可用所需时间的计算，覆盖未就绪、时间缺失和已可用等场景。
func TestNextPodAvailabilityCheck(t *testing.T) {
	// 构造在 now 前 beforeSec 秒发生就绪跳转的 Pod。
	newPodWithReadyCond := func(now metav1.Time, ready bool, beforeSec int) *v1.Pod {
		conditionStatus := v1.ConditionFalse
		if ready {
			conditionStatus = v1.ConditionTrue
		}
		return &v1.Pod{
			Status: v1.PodStatus{
				Conditions: []v1.PodCondition{
					{
						Type:               v1.PodReady,
						LastTransitionTime: metav1.NewTime(now.Add(-1 * time.Duration(beforeSec) * time.Second)),
						Status:             conditionStatus,
					},
				},
			},
		}
	}

	now := metav1.Now()
	tests := []struct {
		name            string
		pod             *v1.Pod
		minReadySeconds int32
		expected        *time.Duration
	}{
		{
			name:            "not ready",
			pod:             newPodWithReadyCond(now, false, 0),
			minReadySeconds: 0,
			expected:        nil,
		},
		{
			name:            "no minReadySeconds defined",
			pod:             newPodWithReadyCond(now, true, 0),
			minReadySeconds: 0,
			expected:        nil,
		},
		{
			name: "lastTransitionTime is zero",
			pod: func() *v1.Pod {
				pod := newPodWithReadyCond(now, true, 0)
				pod.Status.Conditions[0].LastTransitionTime = metav1.Time{}
				return pod
			}(),
			minReadySeconds: 1,
			expected:        nil,
		},
		{
			name:            "just became ready - available in 1s",
			pod:             newPodWithReadyCond(now, true, 0),
			minReadySeconds: 1,
			expected:        ptr.To(time.Second),
		},
		{
			name:            "ready for 20s - available in 10s",
			pod:             newPodWithReadyCond(now, true, 20),
			minReadySeconds: 30,
			expected:        ptr.To(10 * time.Second),
		},
		{
			name:            "available",
			pod:             newPodWithReadyCond(now, true, 51),
			minReadySeconds: 50,
			expected:        nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nextAvailable := nextPodAvailabilityCheck(test.pod, test.minReadySeconds, now.Time)
			if !ptr.Equal(nextAvailable, test.expected) {
				t.Errorf("expected next pod availability check: %v, got: %v", test.expected, nextAvailable)
			}
		})
	}
}

// TestFindMinNextPodAvailabilitySimpleCheck 验证能找出最早需重新检查可用性的 Pod，并比对两种入口的结果一致性。
func TestFindMinNextPodAvailabilitySimpleCheck(t *testing.T) {
	now := metav1.Now()

	// 构造指定就绪时长的 Pod，未就绪时直接把 Ready 条件置为 False。
	pod := func(name string, ready bool, beforeSec int) *v1.Pod {
		p := testutil.NewPod(name, "node0")
		if ready {
			p.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now.Add(-1 * time.Duration(beforeSec) * time.Second))
		} else {
			p.Status.Conditions[0].Status = v1.ConditionFalse
		}
		return p
	}

	tests := []struct {
		name            string
		pods            []*v1.Pod
		minReadySeconds int32
		expected        *time.Duration
		expectedPod     *string
	}{
		{
			name:            "no pods",
			pods:            nil,
			minReadySeconds: 0,
			expected:        nil,
			expectedPod:     nil,
		},
		{
			name: "unready pods",
			pods: []*v1.Pod{
				pod("pod1", false, 0),
				pod("pod2", false, 0),
			},
			minReadySeconds: 0,
			expected:        nil,
			expectedPod:     nil,
		},
		{
			name: "ready pods with no minReadySeconds",
			pods: []*v1.Pod{
				pod("pod1", true, 0),
				pod("pod2", true, 0),
			},
			minReadySeconds: 0,
			expected:        nil,
			expectedPod:     nil,
		},
		{
			name: "unready and ready pods should find min next availability check",
			pods: []*v1.Pod{
				pod("pod1", false, 0),
				pod("pod2", true, 2),
				pod("pod3", true, 0),
				pod("pod4", true, 4),
				pod("pod5", false, 0),
			},
			minReadySeconds: 10,
			expected:        ptr.To(6 * time.Second),
			expectedPod:     ptr.To("pod4"),
		},
		// 只有 Pod 就绪后才需要安排可用性检查，已可用的 Pod 和无序的 Pod 都不产生检查。
		{
			name: "unready and available pods do not require min next availability check",
			pods: []*v1.Pod{
				pod("pod1", false, 0),
				pod("pod2", true, 15),
				pod("pod3", true, 11),
				pod("pod4", true, 10),
				pod("pod5", false, 0),
			},
			minReadySeconds: 10,
			expected:        nil,
			expectedPod:     nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nextAvailable, checkPod := findMinNextPodAvailabilitySimpleCheck(test.pods, test.minReadySeconds, now.Time)
			var checkPodName *string
			if checkPod != nil {
				checkPodName = ptr.To(checkPod.Name)
			}
			if !ptr.Equal(nextAvailable, test.expected) {
				t.Errorf("expected next min pod availability check: %v, got: %v", test.expected, nextAvailable)
			}
			if !ptr.Equal(checkPodName, test.expectedPod) {
				t.Errorf("expected next min pod availability check for pod: %v, got: %v", test.expectedPod, checkPodName)
			}

			// 状态评估与时钟使用同一时刻时，结果应与简单检查函数一致。
			nextAvailable = FindMinNextPodAvailabilityCheck(test.pods, test.minReadySeconds, now.Time, testingclock.NewFakeClock(now.Time))

			if !ptr.Equal(nextAvailable, test.expected) {
				t.Errorf("expected next min pod availability check when status evaluation and clock is now: %v, got: %v", test.expected, nextAvailable)
			}
		})
	}
}

// TestFindMinNextPodAvailability 验证状态评估与当前时刻存在延迟时，下次可用性检查时间的计算。
func TestFindMinNextPodAvailability(t *testing.T) {
	now := metav1.Now()

	// 构造指定就绪时长的 Pod，未就绪时把 Ready 条件置为 False。
	pod := func(name string, ready bool, beforeSec int) *v1.Pod {
		p := testutil.NewPod(name, "node0")
		if ready {
			p.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now.Add(-1 * time.Duration(beforeSec) * time.Second))
		} else {
			p.Status.Conditions[0].Status = v1.ConditionFalse
		}
		return p
	}

	tests := []struct {
		name                         string
		pods                         []*v1.Pod
		minReadySeconds              int32
		statusEvaluationDelaySeconds int
		expected                     *time.Duration
	}{
		// 延迟计入后 pod4 就绪共经过 4+2 秒，因此还需等待 10-6=4 秒。
		{
			name: "unready and ready pods should find min next availability check considering status evaluation/update delay",
			pods: []*v1.Pod{
				pod("pod1", false, 0),
				pod("pod2", true, 2),
				pod("pod3", true, 0),
				pod("pod4", true, 4),
				pod("pod5", false, 0),
			},
			minReadySeconds:              10,
			statusEvaluationDelaySeconds: 2,
			expected:                     ptr.To(4 * time.Second),
		},
		// 4+7 秒已超过 minReadySeconds，此时期望的剩余等待时间为零。
		{
			name: "unready and ready pods should find min next availability check even if the status evaluation delay is longer than minReadySeconds",
			pods: []*v1.Pod{
				pod("pod1", false, 0),
				pod("pod2", true, 2),
				pod("pod3", true, 0),
				pod("pod4", true, 4),
				pod("pod5", false, 0),
			},
			minReadySeconds:              10,
			statusEvaluationDelaySeconds: 7,
			expected:                     ptr.To(0 * time.Second),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldNow := now.Time
			newNow := testingclock.NewFakePassiveClock(now.Add(time.Duration(test.statusEvaluationDelaySeconds) * time.Second))
			nextAvailable := FindMinNextPodAvailabilityCheck(test.pods, test.minReadySeconds, oldNow, newNow)

			if !ptr.Equal(nextAvailable, test.expected) {
				t.Errorf("expected next min pod availability check: %v, got: %v", test.expected, nextAvailable)
			}
		})
	}
}

// TestActiveReplicaSetsFiltering 验证只返回副本数大于零的非空副本集。
func TestActiveReplicaSetsFiltering(t *testing.T) {

	rsUuid := uuid.NewUUID()
	tests := []struct {
		name            string
		replicaSets     []*apps.ReplicaSet
		wantReplicaSets []*apps.ReplicaSet
	}{
		{
			name: "Filters active replica sets",
			replicaSets: []*apps.ReplicaSet{
				newReplicaSet("zero", 0, rsUuid),
				nil,
				newReplicaSet("foo", 1, rsUuid),
				newReplicaSet("bar", 2, rsUuid),
			},
			wantReplicaSets: []*apps.ReplicaSet{
				newReplicaSet("foo", 1, rsUuid),
				newReplicaSet("bar", 2, rsUuid),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotReplicaSets := FilterActiveReplicaSets(test.replicaSets)

			if diff := cmp.Diff(test.wantReplicaSets, gotReplicaSets); diff != "" {
				t.Errorf("Active replica set names (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestComputeHash 验证不同 collisionCount 会产生不同哈希，用于碰撞后重新生成名称。
func TestComputeHash(t *testing.T) {
	collisionCount := int32(1)
	otherCollisionCount := int32(2)
	maxCollisionCount := int32(math.MaxInt32)
	tests := []struct {
		name                string
		template            *v1.PodTemplateSpec
		collisionCount      *int32
		otherCollisionCount *int32
	}{
		{
			name:                "simple",
			template:            &v1.PodTemplateSpec{},
			collisionCount:      &collisionCount,
			otherCollisionCount: &otherCollisionCount,
		},
		{
			name:                "using math.MaxInt64",
			template:            &v1.PodTemplateSpec{},
			collisionCount:      nil,
			otherCollisionCount: &maxCollisionCount,
		},
	}

	for _, test := range tests {
		hash := ComputeHash(test.template, test.collisionCount)
		otherHash := ComputeHash(test.template, test.otherCollisionCount)

		assert.NotEqual(t, hash, otherHash, "expected different hashes but got the same: %d", hash)
	}
}

// TestRemoveTaintOffNode 验证删除节点污点的结果及 API 请求次数，覆盖污点不存在等短路场景。
func TestRemoveTaintOffNode(t *testing.T) {
	tests := []struct {
		name           string
		nodeHandler    *testutil.FakeNodeHandler
		nodeName       string
		taintsToRemove []*v1.Taint
		expectedTaints []v1.Taint
		requestCount   int
	}{
		{
			name: "remove one taint from node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
								{Key: "key2", Value: "value2", Effect: "NoExecute"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName: "node1",
			taintsToRemove: []*v1.Taint{
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: "NoSchedule"},
			},
			requestCount: 4,
		},
		{
			name: "remove multiple taints from node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
								{Key: "key2", Value: "value2", Effect: "NoExecute"},
								{Key: "key3", Value: "value3", Effect: "NoSchedule"},
								{Key: "key4", Value: "value4", Effect: "NoExecute"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName: "node1",
			taintsToRemove: []*v1.Taint{
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
				{Key: "key3", Value: "value3", Effect: "NoSchedule"},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: "NoSchedule"},
				{Key: "key4", Value: "value4", Effect: "NoExecute"},
			},
			requestCount: 4,
		},
		{
			name: "remove no-exist taints from node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
								{Key: "key2", Value: "value2", Effect: "NoExecute"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName: "node1",
			taintsToRemove: []*v1.Taint{
				{Key: "key3", Value: "value3", Effect: "NoSchedule"},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: "NoSchedule"},
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
			},
			requestCount: 2,
		},
		{
			name: "remove taint from node without taints",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName: "node1",
			taintsToRemove: []*v1.Taint{
				{Key: "key3", Value: "value3", Effect: "NoSchedule"},
			},
			expectedTaints: nil,
			requestCount:   2,
		},
		{
			name: "remove empty taint list from node without taints",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName:       "node1",
			taintsToRemove: []*v1.Taint{},
			expectedTaints: nil,
			requestCount:   2,
		},
		{
			name: "remove empty taint list from node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
								{Key: "key2", Value: "value2", Effect: "NoExecute"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName:       "node1",
			taintsToRemove: []*v1.Taint{},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: "NoSchedule"},
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
			},
			requestCount: 2,
		},
	}
	for _, test := range tests {
		// 先取回节点传入被测函数，以便其做本地短路判断。
		node, _ := test.nodeHandler.Get(context.TODO(), test.nodeName, metav1.GetOptions{})
		err := RemoveTaintOffNode(context.TODO(), test.nodeHandler, test.nodeName, node, test.taintsToRemove...)
		require.NoError(t, err, "%s: RemoveTaintOffNode() error = %v", test.name, err)

		node, _ = test.nodeHandler.Get(context.TODO(), test.nodeName, metav1.GetOptions{})
		assert.EqualValues(t, test.expectedTaints, node.Spec.Taints,
			"%s: failed to remove taint off node: expected %+v, got %+v",
			test.name, test.expectedTaints, node.Spec.Taints)

		assert.Equal(t, test.requestCount, test.nodeHandler.RequestCount,
			"%s: unexpected request count: expected %+v, got %+v",
			test.name, test.requestCount, test.nodeHandler.RequestCount)
	}
}

// TestAddOrUpdateTaintOnNode 验证新增、更新和幂等添加污点的结果及冲突重试行为。
func TestAddOrUpdateTaintOnNode(t *testing.T) {
	tests := []struct {
		name           string
		nodeHandler    *testutil.FakeNodeHandler
		nodeName       string
		taintsToAdd    []*v1.Taint
		expectedTaints []v1.Taint
		requestCount   int
		expectedErr    error
	}{
		{
			name: "add one taint on node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName: "node1",
			taintsToAdd: []*v1.Taint{
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: "NoSchedule"},
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
			},
			requestCount: 3,
		},
		{
			name: "add multiple taints to node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
								{Key: "key2", Value: "value2", Effect: "NoExecute"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName: "node1",
			taintsToAdd: []*v1.Taint{
				{Key: "key3", Value: "value3", Effect: "NoSchedule"},
				{Key: "key4", Value: "value4", Effect: "NoExecute"},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: "NoSchedule"},
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
				{Key: "key3", Value: "value3", Effect: "NoSchedule"},
				{Key: "key4", Value: "value4", Effect: "NoExecute"},
			},
			requestCount: 3,
		},
		{
			name: "add exist taints to node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
								{Key: "key2", Value: "value2", Effect: "NoExecute"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName: "node1",
			taintsToAdd: []*v1.Taint{
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
			},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: "NoSchedule"},
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
			},
			requestCount: 2,
		},
		{
			name: "add taint to node without taints",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName: "node1",
			taintsToAdd: []*v1.Taint{
				{Key: "key3", Value: "value3", Effect: "NoSchedule"},
			},
			expectedTaints: []v1.Taint{
				{Key: "key3", Value: "value3", Effect: "NoSchedule"},
			},
			requestCount: 3,
		},
		{
			name: "add empty taint list to node without taints",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName:       "node1",
			taintsToAdd:    []*v1.Taint{},
			expectedTaints: nil,
			requestCount:   1,
		},
		{
			name: "add empty taint list to node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
								{Key: "key2", Value: "value2", Effect: "NoExecute"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName:    "node1",
			taintsToAdd: []*v1.Taint{},
			expectedTaints: []v1.Taint{
				{Key: "key1", Value: "value1", Effect: "NoSchedule"},
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
			},
			requestCount: 1,
		},
		{
			name: "add taint to changed node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:            "node1",
							ResourceVersion: "1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
				AsyncCalls: []func(*testutil.FakeNodeHandler){func(m *testutil.FakeNodeHandler) {
					if len(m.UpdatedNodes) == 0 {
						m.UpdatedNodes = append(m.UpdatedNodes, &v1.Node{
							ObjectMeta: metav1.ObjectMeta{
								Name:            "node1",
								ResourceVersion: "2",
							},
							Spec: v1.NodeSpec{
								Taints: []v1.Taint{},
							}})
					}
				}},
			},
			nodeName:    "node1",
			taintsToAdd: []*v1.Taint{{Key: "key2", Value: "value2", Effect: "NoExecute"}},
			expectedTaints: []v1.Taint{
				{Key: "key2", Value: "value2", Effect: "NoExecute"},
			},
			requestCount: 5,
		},
		{
			name: "add taint to non-exist node",
			nodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:            "node1",
							ResourceVersion: "1",
						},
						Spec: v1.NodeSpec{
							Taints: []v1.Taint{
								{Key: "key1", Value: "value1", Effect: "NoSchedule"},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			nodeName:    "node2",
			taintsToAdd: []*v1.Taint{{Key: "key2", Value: "value2", Effect: "NoExecute"}},
			expectedErr: apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, "node2"),
		},
	}
	for _, test := range tests {
		err := AddOrUpdateTaintOnNode(context.TODO(), test.nodeHandler, test.nodeName, test.taintsToAdd...)
		if test.expectedErr != nil {
			// 预期出错时无需再校验污点状态。
			assert.Equal(t, test.expectedErr, err, "AddOrUpdateTaintOnNode get unexpected error")
			continue
		}
		require.NoError(t, err, "%s: AddOrUpdateTaintOnNode() error = %v", test.name, err)

		node, _ := test.nodeHandler.Get(context.TODO(), test.nodeName, metav1.GetOptions{})
		assert.EqualValues(t, test.expectedTaints, node.Spec.Taints,
			"%s: failed to add taint to node: expected %+v, got %+v",
			test.name, test.expectedTaints, node.Spec.Taints)

		assert.Equal(t, test.requestCount, test.nodeHandler.RequestCount,
			"%s: unexpected request count: expected %+v, got %+v",
			test.name, test.requestCount, test.nodeHandler.RequestCount)
	}
}

// TestFilterPodsByOwner 验证按所属者索引筛选 Pod，覆盖孤儿 Pod 包含/排除及字段不匹配等场景。
func TestFilterPodsByOwner(t *testing.T) {
	// 构造可选是否带所属者引用的 Pod。
	newPod := func(name, ns string, owner *metav1.OwnerReference) *v1.Pod {
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
			},
		}
		if owner != nil {
			pod.OwnerReferences = append(pod.OwnerReferences, *owner)
		}
		return pod
	}

	ownerKind := "OwnerKind"
	ownerName := "ownerName"
	cases := map[string]struct {
		owner        *metav1.ObjectMeta
		ownedOnly    bool
		allPods      []*v1.Pod
		wantPodsKeys sets.Set[string]
	}{
		"multiple Pods, some are owned by the owner": {
			owner: &metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      ownerName,
				UID:       "abc",
			},
			allPods: []*v1.Pod{
				newPod("a", "ns1", &metav1.OwnerReference{
					UID:        "abc",
					Kind:       ownerKind,
					Name:       ownerName,
					Controller: ptr.To(true),
				}),
				newPod("b", "ns1", &metav1.OwnerReference{
					UID:        "def",
					Kind:       ownerKind,
					Name:       ownerName,
					Controller: ptr.To(true),
				}),
				newPod("c", "ns1", &metav1.OwnerReference{
					UID:        "abc",
					Kind:       ownerKind,
					Name:       ownerName,
					Controller: ptr.To(true),
				}),
			},
			wantPodsKeys: sets.New("ns1/a", "ns1/c"),
		},
		"orphan Pods in multiple namespaces": {
			owner: &metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      ownerName,
				UID:       "abc",
			},
			allPods: []*v1.Pod{
				newPod("a", "ns1", nil),
				newPod("b", "ns2", nil),
			},
			wantPodsKeys: sets.New("ns1/a"),
		},
		"owned Pods and orphan Pods in the owner's namespace": {
			owner: &metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      ownerName,
				UID:       "abc",
			},
			allPods: []*v1.Pod{
				newPod("a", "ns1", nil),
				newPod("b", "ns2", nil),
				newPod("c", "ns1", &metav1.OwnerReference{
					UID:        "abc",
					Kind:       ownerKind,
					Name:       ownerName,
					Controller: ptr.To(true),
				}),
			},
			wantPodsKeys: sets.New("ns1/a", "ns1/c"),
		},
		"exclude orphan pods, pods in mismatched ns,uid,kind,name,controller": {
			owner: &metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      ownerName,
				UID:       "abc",
			},
			allPods: []*v1.Pod{
				newPod("a", "ns1", nil),
				newPod("other-ns-orphan", "ns2", nil),
				newPod("other-ns-owned", "ns2", &metav1.OwnerReference{
					UID:        "abc",
					Kind:       ownerKind,
					Name:       ownerName,
					Controller: ptr.To(true),
				}),
				newPod("c", "ns1", &metav1.OwnerReference{
					UID:        "abc",
					Kind:       ownerKind,
					Name:       ownerName,
					Controller: ptr.To(true),
				}),
				newPod("other-uid", "ns1", &metav1.OwnerReference{
					UID:        "other-uid",
					Kind:       ownerKind,
					Name:       ownerName,
					Controller: ptr.To(true),
				}),
				newPod("other-kind", "ns1", &metav1.OwnerReference{
					UID:        "abc",
					Kind:       "OtherKind",
					Name:       ownerName,
					Controller: ptr.To(true),
				}),
				newPod("other-name", "ns1", &metav1.OwnerReference{
					UID:        "abc",
					Kind:       ownerKind,
					Name:       "otherName",
					Controller: ptr.To(true),
				}),
				newPod("non-controller", "ns1", &metav1.OwnerReference{
					UID:        "abc",
					Kind:       ownerKind,
					Name:       ownerName,
					Controller: ptr.To(false),
				}),
			},
			ownedOnly:    true,
			wantPodsKeys: sets.New("ns1/c"),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset()
			sharedInformers := informers.NewSharedInformerFactory(fakeClient, 0)
			podInformer := sharedInformers.Core().V1().Pods()

			err := AddPodControllerIndexer(podInformer.Informer())
			if err != nil {
				t.Fatalf("failed to register indexer: %v", err)
			}
			podIndexer := podInformer.Informer().GetIndexer()
			for _, pod := range tc.allPods {
				if err := podIndexer.Add(pod); err != nil {
					t.Fatalf("failed adding Pod to indexer: %v", err)
				}
			}
			gotPods, err := FilterPodsByOwner(podIndexer, tc.owner, ownerKind, !tc.ownedOnly)
			if err != nil {
				t.Fatal(err)
			}
			gotPodKeys := sets.New[string]()
			for _, pod := range gotPods {
				gotPodKeys.Insert(pod.Namespace + "/" + pod.Name)
			}
			if diff := cmp.Diff(tc.wantPodsKeys, gotPodKeys); diff != "" {
				t.Errorf("unexpected pods returned, diff=%s", diff)
			}
		})
	}
}
