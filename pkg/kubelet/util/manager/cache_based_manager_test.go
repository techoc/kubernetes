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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/stretchr/testify/assert"
)

// checkObject 断言对象存储中 ns/name 对象的存在性，以及 Get 的返回错误是否符合预期。
func checkObject(t *testing.T, store *objectStore, ns, name string, shouldExist bool) {
	_, err := store.Get(ns, name)
	if shouldExist && err != nil {
		t.Errorf("unexpected actions: %#v", err)
	}
	if !shouldExist && (err == nil || !strings.Contains(err.Error(), fmt.Sprintf("object %q/%q not registered", ns, name))) {
		t.Errorf("unexpected actions: %#v", err)
	}
}

// noObjectTTL 返回一个不启用 TTL 的 GetObjectTTLFunc（时长为 0、不存在）。
func noObjectTTL() (time.Duration, bool) {
	return time.Duration(0), false
}

// getSecret 构造一个通过 fake clientset 获取 Secret 的 GetObjectFunc。
func getSecret(ctx context.Context, fakeClient clientset.Interface) GetObjectFunc {
	return func(namespace, name string, opts metav1.GetOptions) (runtime.Object, error) {
		return fakeClient.CoreV1().Secrets(namespace).Get(ctx, name, opts)
	}
}

// newSecretStore 构造一个以 Secret 为对象的测试用 objectStore。
func newSecretStore(ctx context.Context, fakeClient clientset.Interface, clock clock.Clock, getTTL GetObjectTTLFunc, ttl time.Duration) *objectStore {
	return &objectStore{
		getObject:  getSecret(ctx, fakeClient),
		clock:      clock,
		items:      make(map[objectKey]*objectStoreItem),
		defaultTTL: ttl,
		getTTL:     getTTL,
	}
}

// getSecretNames 收集 Pod 引用到的全部 Secret 名称。
func getSecretNames(pod *v1.Pod) sets.Set[string] {
	result := sets.New[string]()
	podutil.VisitPodSecretNames(pod, func(name string) bool {
		result.Insert(name)
		return true
	})
	return result
}

// newCacheBasedSecretManager 基于给定存储构造用于测试的缓存型 Secret 管理器。
func newCacheBasedSecretManager(store Store) Manager {
	return NewCacheBasedManager(store, getSecretNames)
}

// TestSecretStore 验证对象存储的引用注册与 Get 行为：注册引用不触发 Get 请求，
// 仅对已注册对象发起 Get，未注册对象直接返回错误。
func TestSecretStore(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	store := newSecretStore(tCtx, fakeClient, clock.RealClock{}, noObjectTTL, 0)
	store.AddReference("ns1", "name1", "pod1")
	store.AddReference("ns2", "name2", "pod2")
	store.AddReference("ns1", "name1", "pod3")
	store.AddReference("ns1", "name1", "pod4")
	store.DeleteReference("ns1", "name1", "pod1")
	store.DeleteReference("ns2", "name2", "pod2")
	store.AddReference("ns3", "name3", "pod5")

	// 注册引用不会发起 Get 请求。
	actions := fakeClient.Actions()
	assert.Empty(t, actions, "unexpected actions")
	// 应发起 Get 请求
	store.Get("ns1", "name1")
	// 不应发起 Get 请求，因为 Secret 未注册
	store.Get("ns2", "name2")
	// 应发起 Get 请求
	store.Get("ns3", "name3")

	actions = fakeClient.Actions()
	assert.Len(t, actions, 2, "unexpected actions")

	for _, a := range actions {
		assert.True(t, a.Matches("get", "secrets"), "unexpected actions: %#v", a)
	}

	checkObject(t, store, "ns1", "name1", true)
	checkObject(t, store, "ns2", "name2", false)
	checkObject(t, store, "ns3", "name3", true)
	checkObject(t, store, "ns4", "name4", false)
}

// TestSecretStoreDeletingSecret 验证对象在服务端被删除后，Get 返回 NotFound 且不会把错误结果缓存下来。
func TestSecretStoreDeletingSecret(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	store := newSecretStore(tCtx, fakeClient, clock.RealClock{}, noObjectTTL, 0)
	store.AddReference("ns", "name", "pod")

	result := &v1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "name", ResourceVersion: "10"}}
	fakeClient.AddReactor("get", "secrets", func(action core.Action) (bool, runtime.Object, error) {
		return true, result, nil
	})
	secret, err := store.Get("ns", "name")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(secret, result) {
		t.Errorf("Unexpected secret: %v", secret)
	}

	fakeClient.PrependReactor("get", "secrets", func(action core.Action) (bool, runtime.Object, error) {
		return true, &v1.Secret{}, apierrors.NewNotFound(v1.Resource("secret"), "name")
	})
	secret, err = store.Get("ns", "name")
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(secret, &v1.Secret{}) {
		t.Errorf("Unexpected secret: %v", secret)
	}
}

// TestSecretStoreGetAlwaysRefresh 验证在无 TTL 配置时，并发 Get 每次都转发到后端获取最新对象。
func TestSecretStoreGetAlwaysRefresh(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	fakeClock := testingclock.NewFakeClock(time.Now())
	store := newSecretStore(tCtx, fakeClient, fakeClock, noObjectTTL, 0)

	for i := 0; i < 10; i++ {
		store.AddReference(fmt.Sprintf("ns-%d", i), fmt.Sprintf("name-%d", i), types.UID(fmt.Sprintf("pod-%d", i)))
	}
	fakeClient.ClearActions()

	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			store.Get(fmt.Sprintf("ns-%d", i%10), fmt.Sprintf("name-%d", i%10))
			wg.Done()
		}(i)
	}
	wg.Wait()
	actions := fakeClient.Actions()
	assert.Len(t, actions, 100, "unexpected actions")

	for _, a := range actions {
		assert.True(t, a.Matches("get", "secrets"), "unexpected actions: %#v", a)
	}
}

// TestSecretStoreGetNeverRefresh 验证设置 TTL 后，缓存有效期内的并发 Get 直接命中缓存，不再发起后端请求。
func TestSecretStoreGetNeverRefresh(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	fakeClock := testingclock.NewFakeClock(time.Now())
	store := newSecretStore(tCtx, fakeClient, fakeClock, noObjectTTL, time.Minute)

	for i := 0; i < 10; i++ {
		store.AddReference(fmt.Sprintf("ns-%d", i), fmt.Sprintf("name-%d", i), types.UID(fmt.Sprintf("pod-%d", i)))
	}
	fakeClient.ClearActions()

	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			store.Get(fmt.Sprintf("ns-%d", i%10), fmt.Sprintf("name-%d", i%10))
			wg.Done()
		}(i)
	}
	wg.Wait()
	actions := fakeClient.Actions()
	// 仅首次 Get 会转发到后端请求。
	assert.Len(t, actions, 10, "unexpected actions")
}

// TestCustomTTL 验证自定义 TTL 的生效、过期，以及移除自定义 TTL 后回退到默认 TTL 的行为。
func TestCustomTTL(t *testing.T) {
	ttl := time.Duration(0)
	ttlExists := false
	customTTL := func() (time.Duration, bool) {
		return ttl, ttlExists
	}

	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	fakeClock := testingclock.NewFakeClock(time.Time{})
	store := newSecretStore(tCtx, fakeClient, fakeClock, customTTL, time.Minute)

	store.AddReference("ns", "name", "pod")
	store.Get("ns", "name")
	fakeClient.ClearActions()

	// 设置 0 TTL，验证是否生效。
	ttl = time.Duration(0)
	ttlExists = true
	store.Get("ns", "name")
	actions := fakeClient.Actions()
	assert.Len(t, actions, 1, "unexpected actions")
	fakeClient.ClearActions()

	// 设置 5 分钟 TTL，验证是否生效。
	ttl = time.Duration(5) * time.Minute
	store.Get("ns", "name")
	actions = fakeClient.Actions()
	assert.Empty(t, actions, "unexpected actions")
	// 4 分钟后仍未过期。
	fakeClock.Step(4 * time.Minute)
	store.Get("ns", "name")
	actions = fakeClient.Actions()
	assert.Empty(t, actions, "unexpected actions")
	// 此时 TTL 已过期，应重新获取。
	fakeClock.Step(time.Minute)
	store.Get("ns", "name")
	actions = fakeClient.Actions()
	assert.Len(t, actions, 1, "unexpected actions")
	fakeClient.ClearActions()

	// 移除自定义 TTL，验证是否生效。
	ttlExists = false
	fakeClock.Step(55 * time.Second)
	store.Get("ns", "name")
	actions = fakeClient.Actions()
	assert.Empty(t, actions, "unexpected action")
	// 越过一分钟后应触发重新获取。
	fakeClock.Step(5 * time.Second)
	store.Get("ns", "name")
	actions = fakeClient.Actions()
	assert.Len(t, actions, 1, "unexpected actions")
}

// TestParseNodeAnnotation 表驱动验证从 Node 注解解析对象 TTL：覆盖 nil 节点、无注解、非法值、0 与有效时长等场景。
func TestParseNodeAnnotation(t *testing.T) {
	tCtx := ktesting.Init(t)
	testCases := []struct {
		node   *v1.Node
		err    error
		exists bool
		ttl    time.Duration
	}{
		{
			node:   nil,
			err:    fmt.Errorf("error"),
			exists: false,
		},
		{
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node",
				},
			},
			exists: false,
		},
		{
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node",
					Annotations: map[string]string{},
				},
			},
			exists: false,
		},
		{
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node",
					Annotations: map[string]string{v1.ObjectTTLAnnotationKey: "bad"},
				},
			},
			exists: false,
		},
		{
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node",
					Annotations: map[string]string{v1.ObjectTTLAnnotationKey: "0"},
				},
			},
			exists: true,
			ttl:    time.Duration(0),
		},
		{
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node",
					Annotations: map[string]string{v1.ObjectTTLAnnotationKey: "60"},
				},
			},
			exists: true,
			ttl:    time.Minute,
		},
	}
	for i, testCase := range testCases {
		getNode := func(context.Context) (*v1.Node, error) { return testCase.node, testCase.err }
		ttl, exists := GetObjectTTLFromNodeFunc(tCtx, getNode)()
		if exists != testCase.exists {
			t.Errorf("%d: incorrect parsing: %t", i, exists)
			continue
		}
		if exists && ttl != testCase.ttl {
			t.Errorf("%d: incorrect ttl: %v", i, ttl)
		}
	}
}

// envSecrets 描述构造 Pod 时容器引用的 Secret：envFrom 引用与 env 变量引用两类。
type envSecrets struct {
	envVarNames  []string
	envFromNames []string
}

// secretsToAttach 描述测试 Pod 要引用的 Secret 集合：镜像拉取 Secret 与容器环境 Secret。
type secretsToAttach struct {
	imagePullSecretNames []string
	containerEnvSecrets  []envSecrets
}

// podWithSecrets 构造引用指定 Secret 的 Pod，PodUID 取 "ns/name" 形式。
func podWithSecrets(ns, podName string, toAttach secretsToAttach) *v1.Pod {
	return podWithSecretsAndUID(ns, podName, fmt.Sprintf("%s/%s", ns, podName), toAttach)
}

// podWithSecretsAndUID 构造引用指定 Secret 且可指定 UID 的 Pod，用于区分同名但 UID 不同的 Pod。
func podWithSecretsAndUID(ns, podName, podUID string, toAttach secretsToAttach) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Spec: v1.PodSpec{},
	}
	for _, name := range toAttach.imagePullSecretNames {
		pod.Spec.ImagePullSecrets = append(
			pod.Spec.ImagePullSecrets, v1.LocalObjectReference{Name: name})
	}
	for i, secrets := range toAttach.containerEnvSecrets {
		container := v1.Container{
			Name: fmt.Sprintf("container-%d", i),
		}
		for _, name := range secrets.envFromNames {
			envFrom := v1.EnvFromSource{
				SecretRef: &v1.SecretEnvSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: name,
					},
				},
			}
			container.EnvFrom = append(container.EnvFrom, envFrom)
		}

		for _, name := range secrets.envVarNames {
			envSource := &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: name,
					},
				},
			}
			container.Env = append(container.Env, v1.EnvVar{ValueFrom: envSource})
		}
		pod.Spec.Containers = append(pod.Spec.Containers, container)
	}
	return pod
}

// TestCacheInvalidation 验证 Pod 注册、更新与注销时缓存对象的失效与重新获取行为。
func TestCacheInvalidation(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	fakeClock := testingclock.NewFakeClock(time.Now())
	store := newSecretStore(tCtx, fakeClient, fakeClock, noObjectTTL, time.Minute)
	manager := newCacheBasedSecretManager(store)

	// 构造一个引用若干 Secret 的 Pod。
	s1 := secretsToAttach{
		imagePullSecretNames: []string{"s1"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s1"}, envFromNames: []string{"s10"}},
			{envVarNames: []string{"s2"}},
		},
	}
	manager.RegisterPod(podWithSecrets("ns1", "name1", s1))
	// 获取这些 Secret，应触发后端 Get 请求。
	store.Get("ns1", "s1")
	store.Get("ns1", "s10")
	store.Get("ns1", "s2")
	actions := fakeClient.Actions()
	assert.Len(t, actions, 3, "unexpected number of actions")
	fakeClient.ClearActions()

	// 用新的 Secret 集合更新该 Pod。
	s2 := secretsToAttach{
		imagePullSecretNames: []string{"s1"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s1"}},
			{envVarNames: []string{"s2"}, envFromNames: []string{"s20"}},
			{envVarNames: []string{"s3"}},
		},
	}
	manager.RegisterPod(podWithSecrets("ns1", "name1", s2))
	// 仅获取 s3 与 s20，应触发后端 Get 请求。
	store.Get("ns1", "s1")
	store.Get("ns1", "s2")
	store.Get("ns1", "s20")
	store.Get("ns1", "s3")
	actions = fakeClient.Actions()
	assert.Len(t, actions, 2, "unexpected actions")
	fakeClient.ClearActions()

	// 新 Pod 引用前三组 Secret，其缓存应被失效并重新获取。
	manager.RegisterPod(podWithSecrets("ns1", "name2", s1))
	store.Get("ns1", "s1")
	store.Get("ns1", "s10")
	store.Get("ns1", "s2")
	store.Get("ns1", "s20")
	store.Get("ns1", "s3")
	actions = fakeClient.Actions()
	assert.Len(t, actions, 3, "unexpected actions")
	fakeClient.ClearActions()
}

// TestResourceContentExpired 验证后端返回 ResourceExpired 时，GetObject 会返回错误。
func TestResourceContentExpired(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	fakeClock := testingclock.NewFakeClock(time.Now())
	store := newSecretStore(tCtx, fakeClient, fakeClock, noObjectTTL, time.Minute)
	manager := newCacheBasedSecretManager(store)

	// 构造一个引用若干 Secret 的 Pod。
	s1 := secretsToAttach{
		imagePullSecretNames: []string{"s1"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s1"}, envFromNames: []string{"s10"}},
			{envVarNames: []string{"s2"}},
		},
	}

	// 模拟请求的资源内容在服务端已过期
	manager.RegisterPod(podWithSecrets("dummy-ns", "dummy-name", s1))
	fakeClient.PrependReactor("get", "secrets", func(action core.Action) (bool, runtime.Object, error) {
		return true, &v1.Secret{}, apierrors.NewResourceExpired("expired")
	})
	// 应无法获取最新对象
	_, err := manager.GetObject("dummy-ns", "s1")
	assert.Error(t, err)
}

// TestRegisterIdempotence 验证 RegisterPod 与 UnregisterPod 对引用计数的幂等性：重复注册同一 Pod 不会重复计数。
func TestRegisterIdempotence(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	fakeClock := testingclock.NewFakeClock(time.Now())
	store := newSecretStore(tCtx, fakeClient, fakeClock, noObjectTTL, time.Minute)
	manager := newCacheBasedSecretManager(store)

	s1 := secretsToAttach{
		imagePullSecretNames: []string{"s1"},
	}

	refs := func(ns, name string) int {
		store.lock.Lock()
		defer store.lock.Unlock()
		item, ok := store.items[objectKey{namespace: ns, name: name}]
		if !ok {
			return 0
		}
		return item.refCount
	}

	manager.RegisterPod(podWithSecrets("ns1", "name1", s1))
	assert.Equal(t, 1, refs("ns1", "s1"))
	manager.RegisterPod(podWithSecrets("ns1", "name1", s1))
	assert.Equal(t, 1, refs("ns1", "s1"))
	manager.RegisterPod(podWithSecrets("ns1", "name2", s1))
	assert.Equal(t, 2, refs("ns1", "s1"))

	manager.UnregisterPod(podWithSecrets("ns1", "name1", s1))
	assert.Equal(t, 1, refs("ns1", "s1"))
	manager.UnregisterPod(podWithSecrets("ns1", "name1", s1))
	assert.Equal(t, 1, refs("ns1", "s1"))
	manager.UnregisterPod(podWithSecrets("ns1", "name2", s1))
	assert.Equal(t, 0, refs("ns1", "s1"))
}

// TestCacheRefcounts 验证多个 Pod 共享 Secret 时的引用计数，以及注册、更新、注销交织场景下的计数正确性。
func TestCacheRefcounts(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	fakeClock := testingclock.NewFakeClock(time.Now())
	store := newSecretStore(tCtx, fakeClient, fakeClock, noObjectTTL, time.Minute)
	manager := newCacheBasedSecretManager(store)

	s1 := secretsToAttach{
		imagePullSecretNames: []string{"s1"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s1"}, envFromNames: []string{"s10"}},
			{envVarNames: []string{"s2"}},
			{envVarNames: []string{"s3"}},
		},
	}
	manager.RegisterPod(podWithSecrets("ns1", "name1", s1))
	manager.RegisterPod(podWithSecrets("ns1", "name2", s1))
	s2 := secretsToAttach{
		imagePullSecretNames: []string{"s2"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s4"}},
			{envVarNames: []string{"s5"}, envFromNames: []string{"s50"}},
		},
	}
	manager.RegisterPod(podWithSecrets("ns1", "name2", s2))
	manager.RegisterPod(podWithSecrets("ns1", "name3", s2))
	manager.RegisterPod(podWithSecrets("ns1", "name4", s2))
	manager.UnregisterPod(podWithSecrets("ns1", "name3", s2))
	s3 := secretsToAttach{
		imagePullSecretNames: []string{"s1"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s3"}, envFromNames: []string{"s30"}},
			{envVarNames: []string{"s5"}},
		},
	}
	manager.RegisterPod(podWithSecrets("ns1", "name5", s3))
	manager.RegisterPod(podWithSecrets("ns1", "name6", s3))
	s4 := secretsToAttach{
		imagePullSecretNames: []string{"s3"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s6"}},
			{envFromNames: []string{"s60"}},
		},
	}
	manager.RegisterPod(podWithSecrets("ns1", "name7", s4))
	manager.UnregisterPod(podWithSecrets("ns1", "name7", s4))

	// 同时验证 添加 + 更新 + 删除 的场景。
	manager.RegisterPod(podWithSecrets("ns1", "other-name", s1))
	manager.RegisterPod(podWithSecrets("ns1", "other-name", s2))
	manager.UnregisterPod(podWithSecrets("ns1", "other-name", s2))

	s5 := secretsToAttach{
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s7"}},
			{envFromNames: []string{"s70"}},
		},
	}
	// 验证无操作更新场景：重复注册相同 Secret 集合不应改变引用计数
	manager.RegisterPod(podWithSecrets("ns1", "noop-pod", s5))
	manager.RegisterPod(podWithSecrets("ns1", "noop-pod", s5))

	// 此时应有：3 个 Pod 引用 s1，2 个 Pod 引用 s2，2 个 Pod 引用 s3，s4 无引用。
	refs := func(ns, name string) int {
		store.lock.Lock()
		defer store.lock.Unlock()
		item, ok := store.items[objectKey{namespace: ns, name: name}]
		if !ok {
			return 0
		}
		return item.refCount
	}
	assert.Equal(t, 3, refs("ns1", "s1"))
	assert.Equal(t, 1, refs("ns1", "s10"))
	assert.Equal(t, 3, refs("ns1", "s2"))
	assert.Equal(t, 3, refs("ns1", "s3"))
	assert.Equal(t, 2, refs("ns1", "s30"))
	assert.Equal(t, 2, refs("ns1", "s4"))
	assert.Equal(t, 4, refs("ns1", "s5"))
	assert.Equal(t, 2, refs("ns1", "s50"))
	assert.Equal(t, 0, refs("ns1", "s6"))
	assert.Equal(t, 0, refs("ns1", "s60"))
	assert.Equal(t, 1, refs("ns1", "s7"))
	assert.Equal(t, 1, refs("ns1", "s70"))

	// 验证同名但 UID 不同的 Pod 交替注册/注销场景
	secret1 := secretsToAttach{
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"secret1"}},
		},
	}
	secret2 := secretsToAttach{
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"secret2"}},
		},
	}

	// 前置条件：无任何引用
	assert.Equal(t, 0, refs("nsinterleaved", "secret1"))
	assert.Equal(t, 0, refs("nsinterleaved", "secret2"))

	// 注册仅引用 secret1 的第一个 Pod
	manager.RegisterPod(podWithSecretsAndUID("nsinterleaved", "pod", "poduid1", secret1))
	assert.Equal(t, 1, refs("nsinterleaved", "secret1"))
	assert.Equal(t, 0, refs("nsinterleaved", "secret2"))

	// 注册仅引用 secret2 的第二个 Pod，保留对 secret1 的引用
	manager.RegisterPod(podWithSecretsAndUID("nsinterleaved", "pod", "poduid2", secret2))
	assert.Equal(t, 1, refs("nsinterleaved", "secret1"))
	assert.Equal(t, 1, refs("nsinterleaved", "secret2"))

	// 注销引用 secret1 的第一个 Pod，保留对 secret2 的引用
	manager.UnregisterPod(podWithSecretsAndUID("nsinterleaved", "pod", "poduid1", secretsToAttach{}))
	assert.Equal(t, 0, refs("nsinterleaved", "secret1"))
	assert.Equal(t, 1, refs("nsinterleaved", "secret2"))

	// 注销引用 secret2 的第二个 Pod
	manager.UnregisterPod(podWithSecretsAndUID("nsinterleaved", "pod", "poduid2", secretsToAttach{}))
	assert.Equal(t, 0, refs("nsinterleaved", "secret1"))
	assert.Equal(t, 0, refs("nsinterleaved", "secret2"))
}

// TestCacheBasedSecretManager 验证缓存管理器的注册、更新与注销流程后，各命名空间中对象的最终留存情况。
func TestCacheBasedSecretManager(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeClient := &fake.Clientset{}
	store := newSecretStore(tCtx, fakeClient, clock.RealClock{}, noObjectTTL, 0)
	manager := newCacheBasedSecretManager(store)

	// 构造一个引用若干 Secret 的 Pod。
	s1 := secretsToAttach{
		imagePullSecretNames: []string{"s1"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s1"}},
			{envVarNames: []string{"s2"}},
			{envFromNames: []string{"s20"}},
		},
	}
	manager.RegisterPod(podWithSecrets("ns1", "name1", s1))
	// 用不同的 Secret 集合更新该 Pod。
	s2 := secretsToAttach{
		imagePullSecretNames: []string{"s1"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s3"}},
			{envVarNames: []string{"s4"}},
			{envFromNames: []string{"s40"}},
		},
	}
	manager.RegisterPod(podWithSecrets("ns1", "name1", s2))
	// 创建另一个 Pod，使用相同 Secret 但位于不同命名空间。
	manager.RegisterPod(podWithSecrets("ns2", "name2", s2))
	// 创建并删除一个引用其他 Secret 的 Pod。
	s3 := secretsToAttach{
		imagePullSecretNames: []string{"s5"},
		containerEnvSecrets: []envSecrets{
			{envVarNames: []string{"s6"}},
			{envFromNames: []string{"s60"}},
		},
	}
	manager.RegisterPod(podWithSecrets("ns3", "name", s3))
	manager.UnregisterPod(podWithSecrets("ns3", "name", s3))

	// 最终应仅保留 ns1、ns2 中的 s1、s3、s4 三个 Secret。
	for _, ns := range []string{"ns1", "ns2", "ns3"} {
		for _, secret := range []string{"s1", "s2", "s3", "s4", "s5", "s6", "s20", "s40", "s50"} {
			shouldExist :=
				(secret == "s1" || secret == "s3" || secret == "s4" || secret == "s40") && (ns == "ns1" || ns == "ns2")
			checkObject(t, store, ns, secret, shouldExist)
		}
	}
}
