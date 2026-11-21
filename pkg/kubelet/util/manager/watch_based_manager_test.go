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

package manager

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/stretchr/testify/assert"
)

// listSecret 返回使用 fake client 列举 Secret 的 listObjectFunc。
func listSecret(ctx context.Context, fakeClient clientset.Interface) listObjectFunc {
	return func(namespace string, opts metav1.ListOptions) (runtime.Object, error) {
		return fakeClient.CoreV1().Secrets(namespace).List(ctx, opts)
	}
}

// watchSecret 返回使用 fake client watch Secret 的 watchObjectFunc。
func watchSecret(ctx context.Context, fakeClient clientset.Interface) watchObjectFunc {
	return func(namespace string, opts metav1.ListOptions) (watch.Interface, error) {
		return fakeClient.CoreV1().Secrets(namespace).Watch(ctx, opts)
	}
}

// isSecretImmutable 判断对象是否为不可变 Secret。
func isSecretImmutable(object runtime.Object) bool {
	if secret, ok := object.(*v1.Secret); ok {
		return secret.Immutable != nil && *secret.Immutable
	}
	return false
}

// newSecretCache 构造只缓存 Secret 的 objectCache，使用 fake client 与假时钟。
func newSecretCache(ctx context.Context, fakeClient clientset.Interface, fakeClock clock.Clock, maxIdleTime time.Duration) *objectCache {
	return &objectCache{
		listObject:  listSecret(ctx, fakeClient),
		watchObject: watchSecret(ctx, fakeClient),
		newObject:   func() runtime.Object { return &v1.Secret{} },
		isImmutable: isSecretImmutable,
		listWatcherWithWatchListSemanticsWrapper: func(lw *cache.ListWatch) cache.ListerWatcher {
			return cache.ToListWatcherWithWatchListSemantics(lw, fakeClient)
		},
		groupResource: corev1.Resource("secret"),
		clock:         fakeClock,
		maxIdleTime:   maxIdleTime,
		items:         make(map[objectKey]*objectCacheItem),
	}
}

// TestSecretCache 校验 Secret 缓存对增删事件的传播、引用注销与 NotFound 语义。
func TestSecretCache(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}

	listReactor := func(a core.Action) (bool, runtime.Object, error) {
		result := &v1.SecretList{
			ListMeta: metav1.ListMeta{
				ResourceVersion: "123",
			},
		}
		return true, result, nil
	}
	fakeClient.AddReactor("list", "secrets", listReactor)
	fakeWatch := watch.NewFake()
	fakeClient.AddWatchReactor("secrets", core.DefaultWatchReactor(fakeWatch, nil))

	fakeClock := testingclock.NewFakeClock(time.Now())
	store := newSecretCache(tCtx, fakeClient, fakeClock, time.Minute)

	// 引用注册会触发 List+Watch；此时 List 返回空，因此 Get 应为 NotFound。
	store.AddReference("ns", "name", "pod")
	_, err := store.Get("ns", "name")
	if !apierrors.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}

	// 最终应能读到新增的 Secret。
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "ns", ResourceVersion: "125"},
	}
	fakeWatch.Add(secret)
	getFn := func(_ context.Context) (bool, error) {
		object, err := store.Get("ns", "name")
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		secret := object.(*v1.Secret)
		if secret == nil || secret.Name != "name" || secret.Namespace != "ns" {
			return false, fmt.Errorf("unexpected secret: %v", secret)
		}
		return true, nil
	}

	if err := wait.PollUntilContextCancel(tCtx, 10*time.Millisecond, true, getFn); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 最终应能观察到 Secret 被删除。
	fakeWatch.Delete(secret)
	getFn = func(_ context.Context) (bool, error) {
		_, err := store.Get("ns", "name")
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	}
	deadlineCtx, deadlineCancel := context.WithTimeout(tCtx, time.Second)
	defer deadlineCancel()
	if err := wait.PollUntilContextCancel(deadlineCtx, 10*time.Millisecond, true, getFn); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	store.DeleteReference("ns", "name", "pod")
	_, err = store.Get("ns", "name")
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestSecretCacheMultipleRegistrations 校验同一对象的多次引用只触发一次 List/Watch。
func TestSecretCacheMultipleRegistrations(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}

	listReactor := func(a core.Action) (bool, runtime.Object, error) {
		result := &v1.SecretList{
			ListMeta: metav1.ListMeta{
				ResourceVersion: "123",
			},
		}
		return true, result, nil
	}
	fakeClient.AddReactor("list", "secrets", listReactor)
	fakeWatch := watch.NewFake()
	fakeClient.AddWatchReactor("secrets", core.DefaultWatchReactor(fakeWatch, nil))

	fakeClock := testingclock.NewFakeClock(time.Now())
	store := newSecretCache(tCtx, fakeClient, fakeClock, time.Minute)

	store.AddReference("ns", "name", "pod")
	// 该引用最终应触发一次 List 与一次 Watch。
	actionsFn := func(_ context.Context) (bool, error) {
		actions := fakeClient.Actions()
		if len(actions) > 2 {
			return false, fmt.Errorf("too many actions: %v", actions)
		}
		if len(actions) < 2 {
			return false, nil
		}
		if actions[0].GetVerb() != "list" || actions[1].GetVerb() != "watch" {
			return false, fmt.Errorf("unexpected actions: %v", actions)
		}
		return true, nil
	}
	if err := wait.PollUntilContextCancel(tCtx, 10*time.Millisecond, true, actionsFn); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 后续注册/注销同一对象不应再触发新的 List/Watch。
	for i := 0; i < 20; i++ {
		store.AddReference("ns", "name", types.UID(fmt.Sprintf("pod-%d", i)))
		store.DeleteReference("ns", "name", types.UID(fmt.Sprintf("pod-%d", i)))
	}
	actions := fakeClient.Actions()
	assert.Len(t, actions, 2, "unexpected actions")

	// 最后一次注销也不应触发任何请求。
	store.DeleteReference("ns", "name", "pod")
	_, err := store.Get("ns", "name")
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Errorf("unexpected error: %v", err)
	}
	actions = fakeClient.Actions()
	assert.Len(t, actions, 2, "unexpected actions")
}

// TestImmutableSecretStopsTheReflector 校验不可变 Secret 会让 reflector 停止，
// 同时已缓存的值仍可读取。
func TestImmutableSecretStopsTheReflector(t *testing.T) {
	secret := func(rv string, immutable bool) *v1.Secret {
		result := &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "name",
				Namespace:       "ns",
				ResourceVersion: rv,
			},
		}
		if immutable {
			trueVal := true
			result.Immutable = &trueVal
		}
		return result
	}

	tests := []struct {
		desc     string
		initial  *v1.Secret
		eventual *v1.Secret
	}{
		{
			desc:     "secret doesn't exist, created as mutable",
			initial:  nil,
			eventual: secret("200", false),
		},
		{
			desc:     "secret doesn't exist, created as immutable",
			initial:  nil,
			eventual: secret("200", true),
		},
		{
			desc:     "mutable secret modified to mutable",
			initial:  secret("100", false),
			eventual: secret("200", false),
		},
		{
			desc:     "mutable secret modified to immutable",
			initial:  secret("100", false),
			eventual: secret("200", true),
		},
		{
			desc:     "immutable secret",
			initial:  secret("100", true),
			eventual: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			fakeClient := &fake.Clientset{}
			listReactor := func(a core.Action) (bool, runtime.Object, error) {
				result := &v1.SecretList{
					ListMeta: metav1.ListMeta{
						ResourceVersion: "100",
					},
				}
				if tc.initial != nil {
					result.Items = []v1.Secret{*tc.initial}
				}
				return true, result, nil
			}
			fakeClient.AddReactor("list", "secrets", listReactor)
			fakeWatch := watch.NewFake()
			fakeClient.AddWatchReactor("secrets", core.DefaultWatchReactor(fakeWatch, nil))

			fakeClock := testingclock.NewFakeClock(time.Now())
			store := newSecretCache(tCtx, fakeClient, fakeClock, time.Minute)

			key := objectKey{namespace: "ns", name: "name"}
			itemExists := func(_ context.Context) (bool, error) {
				store.lock.Lock()
				defer store.lock.Unlock()
				_, ok := store.items[key]
				return ok, nil
			}
			reflectorRunning := func() bool {
				store.lock.Lock()
				defer store.lock.Unlock()
				item := store.items[key]

				item.lock.Lock()
				defer item.lock.Unlock()
				return !item.stopped
			}

			// AddReference 应启动 reflector。
			store.AddReference("ns", "name", "pod")
			if err := wait.PollUntilContextCancel(tCtx, 10*time.Millisecond, false, itemExists); err != nil {
				t.Errorf("item wasn't added to cache")
			}

			obj, err := store.Get("ns", "name")
			if tc.initial != nil {
				assert.True(t, apiequality.Semantic.DeepEqual(tc.initial, obj))
			} else {
				assert.True(t, apierrors.IsNotFound(err))
			}

			// 不可变 Secret 的 reflector 应已停止。
			assert.Equal(t, tc.initial == nil || !isSecretImmutable(tc.initial), reflectorRunning())

			if tc.eventual == nil {
				return
			}
			fakeWatch.Add(tc.eventual)

			// 最终 Get 应返回该 Secret。
			getFn := func(_ context.Context) (bool, error) {
				object, err := store.Get("ns", "name")
				if err != nil {
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					return false, err
				}
				secret := object.(*v1.Secret)
				return apiequality.Semantic.DeepEqual(tc.eventual, secret), nil
			}
			deadlineCtx, deadlineCancel := context.WithTimeout(tCtx, time.Second)
			defer deadlineCancel()
			if err := wait.PollUntilContextCancel(deadlineCtx, 10*time.Millisecond, true, getFn); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// 不可变 Secret 的 reflector 应已停止。
			assert.Equal(t, tc.eventual == nil || !isSecretImmutable(tc.eventual), reflectorRunning())
		})
	}
}

// TestMaxIdleTimeStopsTheReflector 校验超过 maxIdleTime 未访问时 reflector 被回收，
// 再次 Get 会重新启动。
func TestMaxIdleTimeStopsTheReflector(t *testing.T) {
	tCtx := ktesting.Init(t)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "name",
			Namespace:       "ns",
			ResourceVersion: "200",
		},
	}

	fakeClient := &fake.Clientset{}
	listReactor := func(a core.Action) (bool, runtime.Object, error) {
		result := &v1.SecretList{
			ListMeta: metav1.ListMeta{
				ResourceVersion: "200",
			},
			Items: []v1.Secret{*secret},
		}

		return true, result, nil
	}

	fakeClient.AddReactor("list", "secrets", listReactor)
	fakeWatch := watch.NewFake()
	fakeClient.AddWatchReactor("secrets", core.DefaultWatchReactor(fakeWatch, nil))
	fakeClock := testingclock.NewFakeClock(time.Now())
	store := newSecretCache(tCtx, fakeClient, fakeClock, time.Minute)

	key := objectKey{namespace: "ns", name: "name"}
	itemExists := func(_ context.Context) (bool, error) {
		store.lock.Lock()
		defer store.lock.Unlock()
		_, ok := store.items[key]
		return ok, nil
	}

	reflectorRunning := func() bool {
		store.lock.Lock()
		defer store.lock.Unlock()
		item := store.items[key]

		item.lock.Lock()
		defer item.lock.Unlock()
		return !item.stopped
	}

	// AddReference 应启动 reflector。
	store.AddReference("ns", "name", "pod")
	if err := wait.PollUntilContextCancel(tCtx, 10*time.Millisecond, false, itemExists); err != nil {
		t.Errorf("item wasn't added to cache")
	}

	obj, _ := store.Get("ns", "name")
	assert.True(t, apiequality.Semantic.DeepEqual(secret, obj))

	assert.True(t, reflectorRunning())

	fakeClock.Step(90 * time.Second)
	store.startRecycleIdleWatch()

	// 超过 maxIdleTime 后 reflector 应已停止。
	assert.False(t, reflectorRunning())

	obj, _ = store.Get("ns", "name")
	assert.True(t, apiequality.Semantic.DeepEqual(secret, obj))
	// 再次 Get 后 reflector 应重新启动。
	assert.True(t, reflectorRunning())

	fakeClock.Step(20 * time.Second)
	_, _ = store.Get("ns", "name")
	fakeClock.Step(20 * time.Second)
	_, _ = store.Get("ns", "name")
	fakeClock.Step(20 * time.Second)
	_, _ = store.Get("ns", "name")
	store.startRecycleIdleWatch()

	// 周期性调用 Get 时 reflector 应保持运行。
	assert.True(t, reflectorRunning())
}

// TestReflectorNotStoppedOnSlowInitialization 校验首次 List 很慢时 reflector
// 不会被空闲回收误杀。
func TestReflectorNotStoppedOnSlowInitialization(t *testing.T) {
	tCtx := ktesting.Init(t)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "name",
			Namespace:       "ns",
			ResourceVersion: "200",
		},
	}

	fakeClock := testingclock.NewFakeClock(time.Now())

	fakeClient := &fake.Clientset{}
	listReactor := func(a core.Action) (bool, runtime.Object, error) {
		<-fakeClock.After(120 * time.Second)

		result := &v1.SecretList{
			ListMeta: metav1.ListMeta{
				ResourceVersion: "200",
			},
			Items: []v1.Secret{*secret},
		}

		return true, result, nil
	}

	fakeClient.AddReactor("list", "secrets", listReactor)
	fakeWatch := watch.NewFake()
	fakeClient.AddWatchReactor("secrets", core.DefaultWatchReactor(fakeWatch, nil))
	store := newSecretCache(tCtx, fakeClient, fakeClock, time.Minute)

	key := objectKey{namespace: "ns", name: "name"}
	itemExists := func(_ context.Context) (bool, error) {
		store.lock.Lock()
		defer store.lock.Unlock()
		_, ok := store.items[key]
		return ok, nil
	}

	reflectorRunning := func() bool {
		store.lock.Lock()
		defer store.lock.Unlock()
		item := store.items[key]

		item.lock.Lock()
		defer item.lock.Unlock()
		return !item.stopped
	}

	reflectorInitialized := func(_ context.Context) (bool, error) {
		store.lock.Lock()
		defer store.lock.Unlock()
		item := store.items[key]

		item.lock.Lock()
		defer item.lock.Unlock()
		return item.store.hasSynced(), nil
	}

	// AddReference 应启动 reflector。
	store.AddReference("ns", "name", "pod")
	if err := wait.PollUntilContextCancel(tCtx, 10*time.Millisecond, false, itemExists); err != nil {
		t.Errorf("item wasn't added to cache")
	}

	fakeClock.Step(90 * time.Second)
	store.startRecycleIdleWatch()

	// reflector 尚未完成初始化，不应被停止；
	// 但此时 Get 仍应失败。
	assert.True(t, reflectorRunning())
	initialized, _ := reflectorInitialized(tCtx)
	assert.False(t, initialized)
	_, err := store.Get("ns", "name")
	if err == nil || !strings.Contains(err.Error(), "failed to sync") {
		t.Errorf("Expected failed to sync error, got: %v", err)
	}

	// 初始化应能成功完成。
	fakeClock.Step(30 * time.Second)
	deadlineCtx, deadlineCancel := context.WithTimeout(tCtx, time.Second)
	defer deadlineCancel()
	if err := wait.PollUntilContextCancel(deadlineCtx, 10*time.Millisecond, false, reflectorInitialized); err != nil {
		t.Errorf("reflector didn't iniailize correctly")
	}

	// 最近一分钟内访问过，回收不应停止 reflector。
	store.startRecycleIdleWatch()
	assert.True(t, reflectorRunning())

	obj, _ := store.Get("ns", "name")
	assert.True(t, apiequality.Semantic.DeepEqual(secret, obj))
}

// TestRefMapHandlesReferencesCorrectly 校验引用计数的增加、删除与幂等语义。
func TestRefMapHandlesReferencesCorrectly(t *testing.T) {
	secret1 := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "secret1",
			Namespace: "ns1",
		},
	}
	type step struct {
		action         string
		ns             string
		name           string
		referencedFrom types.UID
	}
	type expect struct {
		ns             string
		name           string
		referencedFrom types.UID
		expectCount    int
	}
	tests := []struct {
		desc    string
		steps   []step
		expects []expect
	}{
		{
			desc: "adding and deleting should works",
			steps: []step{
				{"add", "ns1", "secret1", "pod1"},
				{"add", "ns1", "secret1", "pod1"},
				{"delete", "ns1", "secret1", "pod1"},
				{"delete", "ns1", "secret1", "pod1"},
			},
			expects: []expect{
				{"ns1", "secret1", "pod1", 1},
				{"ns1", "secret1", "pod1", 2},
				{"ns1", "secret1", "pod1", 1},
				{"ns1", "secret1", "pod1", 0},
			},
		},
		{
			desc: "deleting a non-existent reference should have no effect",
			steps: []step{
				{"delete", "ns1", "secret1", "pod1"},
			},
			expects: []expect{
				{"ns1", "secret1", "pod1", 0},
			},
		},
		{
			desc: "deleting more than adding should not lead to negative refcount",
			steps: []step{
				{"add", "ns1", "secret1", "pod1"},
				{"delete", "ns1", "secret1", "pod1"},
				{"delete", "ns1", "secret1", "pod1"},
			},
			expects: []expect{
				{"ns1", "secret1", "pod1", 1},
				{"ns1", "secret1", "pod1", 0},
				{"ns1", "secret1", "pod1", 0},
			},
		},
		{
			desc: "deleting should not affect refcount of other objects or referencedFrom",
			steps: []step{
				{"add", "ns1", "secret1", "pod1"},
				{"delete", "ns1", "secret1", "pod2"},
				{"delete", "ns1", "secret2", "pod1"},
				{"delete", "ns2", "secret1", "pod1"},
			},
			expects: []expect{
				{"ns1", "secret1", "pod1", 1},
				{"ns1", "secret1", "pod1", 1},
				{"ns1", "secret1", "pod1", 1},
				{"ns1", "secret1", "pod1", 1},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			fakeClient := &fake.Clientset{}
			listReactor := func(a core.Action) (bool, runtime.Object, error) {
				result := &v1.SecretList{
					ListMeta: metav1.ListMeta{
						ResourceVersion: "200",
					},
					Items: []v1.Secret{*secret1},
				}
				return true, result, nil
			}
			fakeClient.AddReactor("list", "secrets", listReactor)
			fakeWatch := watch.NewFake()
			fakeClient.AddWatchReactor("secrets", core.DefaultWatchReactor(fakeWatch, nil))
			fakeClock := testingclock.NewFakeClock(time.Now())
			store := newSecretCache(tCtx, fakeClient, fakeClock, time.Minute)

			for i, step := range tc.steps {
				// 按步骤执行操作，并在每一步后立即检查引用计数。
				expect := tc.expects[i]
				switch step.action {
				case "add":
					store.AddReference(step.ns, step.name, step.referencedFrom)
				case "delete":
					store.DeleteReference(step.ns, step.name, step.referencedFrom)
				default:
					t.Errorf("unrecognized action of testcase %v", tc.desc)
				}

				key := objectKey{namespace: expect.ns, name: expect.name}
				item, exists := store.items[key]
				if !exists {
					if tc.expects[i].expectCount != 0 {
						t.Errorf("reference to %v/%v from %v should exists", expect.ns, expect.name, expect.referencedFrom)
					}
				} else if item.refMap[expect.referencedFrom] != expect.expectCount {
					t.Errorf("expects %v but got %v", expect.expectCount, item.refMap[expect.referencedFrom])
				}
			}
		})
	}
}

// TestUnSupportWatchListSemantics 校验客户端不支持 WatchList 语义时仍能完成同步。
func TestUnSupportWatchListSemantics(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)

	fakeClock := testingclock.NewFakeClock(time.Now())
	// fake client 不支持 WatchList 语义，因此无需准备特殊响应。
	fakeClient := fake.NewClientset()
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	target := newSecretCache(context.TODO(), fakeClient, fakeClock, time.Minute)

	ret := target.newReflectorLocked("ns", "obj")
	defer ret.stop()

	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		return ret.store.hasSynced(), nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestWatchListSemanticsSimple 校验支持 WatchList 语义时，单个 watch 响应即可完成同步。
func TestWatchListSemanticsSimple(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)

	// 用最小 HTTP 服务模拟支持 initial-events 的 watch 响应。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, ok := req.URL.Query()["watch"]; !ok {
			t.Errorf("expected a watch request, params: %v", req.URL.Query())
			http.Error(w, fmt.Errorf("unexpected request").Error(), http.StatusInternalServerError)
			return
		}

		obj := &v1.Secret{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "Secret",
			},
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					metav1.InitialEventsAnnotationKey: "true",
				},
			},
		}
		rawObj, err := json.Marshal(obj)
		if err != nil {
			t.Errorf("failed to marshal rawObj: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		watchEvent := &metav1.WatchEvent{
			Type:   string(watch.Bookmark),
			Object: runtime.RawExtension{Raw: rawObj},
		}
		rawRsp, err := json.Marshal(watchEvent)
		if err != nil {
			t.Errorf("failed to marshal watchEvent: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write(rawRsp)
		if err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	cfg := &rest.Config{Host: server.URL}
	client, err := clientset.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	target := newSecretCache(context.TODO(), client, fakeClock, time.Second)

	ret := target.newReflectorLocked("ns", "obj")
	defer ret.stop()

	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		return ret.store.hasSynced(), nil
	}); err != nil {
		t.Fatal(err)
	}
}
