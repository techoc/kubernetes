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

package cacher

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"reflect"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/internalversion/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/sharding"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/delegator"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	etcdfeature "k8s.io/apiserver/pkg/storage/feature"
	storagemetrics "k8s.io/apiserver/pkg/storage/metrics"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	cachertesting "k8s.io/apiserver/pkg/storage/cacher/testing"
)

// newTestCacherWithoutSyncing 用给定的存储与时钟构造 cacher，但不等待其完成
// 初始同步，便于测试未就绪状态下的行为。
func newTestCacherWithoutSyncing(s storage.Interface, c clock.WithTicker) (*Cacher, storage.Versioner, error) {
	prefix := "/pods/"
	config := Config{
		Storage:             s,
		Versioner:           storage.APIObjectVersioner{},
		GroupResource:       schema.GroupResource{Resource: "pods"},
		EventsHistoryWindow: DefaultEventFreshDuration,
		ResourcePrefix:      prefix,
		KeyFunc:             func(obj runtime.Object) (string, error) { return storage.NamespaceKeyFunc(prefix, obj) },
		GetAttrsFunc: func(obj runtime.Object) (labels.Set, fields.Set, error) {
			pod, ok := obj.(*example.Pod)
			if !ok {
				return storage.DefaultNamespaceScopedAttr(obj)
			}
			labelsSet, fieldsSet, err := storage.DefaultNamespaceScopedAttr(obj)
			if err != nil {
				return nil, nil, err
			}
			fieldsSet["spec.nodeName"] = pod.Spec.NodeName
			return labelsSet, fieldsSet, nil
		},
		NewFunc:     func() runtime.Object { return &example.Pod{} },
		NewListFunc: func() runtime.Object { return &example.PodList{} },
		Codec:       codecs.LegacyCodec(examplev1.SchemeGroupVersion),
		Clock:       c,
	}
	cacher, err := NewCacherFromConfig(config)

	return cacher, storage.APIObjectVersioner{}, err
}

// newTestCacher 构造并等待 cacher 就绪。多数测试假设 Get/GetList/Watch 不会
// 因 watch cache 初始化返回 429，因此这里统一等待。
func newTestCacher(s storage.Interface) (*Cacher, storage.Versioner, error) {
	cacher, versioner, err := newTestCacherWithoutSyncing(s, clock.RealClock{})
	if err != nil {
		return nil, versioner, err
	}

	// 这些测试假定 Get/GetList/Watch 调用不应失败。
	// 然而，当 watchcache 处于初始化中时，现在可能返回 429 错误。
	// 为避免重写全部测试，我们等待 watchcache 完成初始化。
	if err := cacher.Wait(context.Background()); err != nil {
		return nil, storage.APIObjectVersioner{}, err
	}
	return cacher, versioner, nil
}

// TestShouldDelegateList 穷举 ListOptions 组合，验证哪些请求应委托给底层存储
// （bypass cache），哪些可以由 watch cache 直接服务。
//
// 覆盖 ResourceVersion（空/0/旧/新）、ResourceVersionMatch、continue token、
// limit 与递归与否，并针对 ListFromCacheSnapshot 开关与快照是否可用做覆盖修正。
func TestShouldDelegateList(t *testing.T) {
	type opts struct {
		Recursive            bool
		ResourceVersion      string
		ResourceVersionMatch metav1.ResourceVersionMatch
		Limit                int64
		Continue             string
	}
	toInternalOpts := func(opt opts) *internalversion.ListOptions {
		return &internalversion.ListOptions{
			ResourceVersion:      opt.ResourceVersion,
			ResourceVersionMatch: opt.ResourceVersionMatch,
			Limit:                opt.Limit,
			Continue:             opt.Continue,
		}
	}

	toStorageOpts := func(opt opts) storage.ListOptions {
		return storage.ListOptions{
			Recursive:            opt.Recursive,
			ResourceVersion:      opt.ResourceVersion,
			ResourceVersionMatch: opt.ResourceVersionMatch,
			Predicate: storage.SelectionPredicate{
				Continue: opt.Continue,
				Limit:    opt.Limit,
			},
		}
	}
	oldRV := "80"
	cacheRV := "100"
	etcdRV := "120"

	keyPrefix := "/pods/"
	continueOnOldRV, err := storage.EncodeContinue(keyPrefix+"foo", keyPrefix, int64(mustAtoi(oldRV)))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	continueOnCacheRV, err := storage.EncodeContinue(keyPrefix+"foo", keyPrefix, int64(mustAtoi(cacheRV)))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// 来自 RV 更靠前的另一个 apiserver 的 continue token
	continueOnEtcdRV, err := storage.EncodeContinue(keyPrefix+"foo", keyPrefix, int64(mustAtoi(etcdRV)))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	continueOnNegativeRV, err := storage.EncodeContinue(keyPrefix+"foo", keyPrefix, -1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	testCases := map[opts]bool{}
	testCases[opts{}] = true
	testCases[opts{Limit: 100}] = true
	testCases[opts{ResourceVersion: "0"}] = false
	testCases[opts{ResourceVersion: "0", Limit: 100}] = false
	testCases[opts{ResourceVersion: "0", ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan}] = false
	testCases[opts{ResourceVersion: "0", ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan, Limit: 100}] = false
	// Continue 相关用例
	for _, continueToken := range []string{continueOnOldRV, continueOnCacheRV, continueOnEtcdRV, continueOnNegativeRV} {
		testCases[opts{Continue: continueToken}] = true
		testCases[opts{Limit: 100, Continue: continueToken}] = true
		testCases[opts{ResourceVersion: "0", Continue: continueToken}] = true
		testCases[opts{ResourceVersion: "0", Limit: 100, Continue: continueToken}] = true
	}
	// 带 RV 的用例
	for _, rv := range []string{oldRV, cacheRV, etcdRV} {
		testCases[opts{ResourceVersion: rv}] = false
		testCases[opts{ResourceVersion: rv, Limit: 100}] = true
		testCases[opts{ResourceVersion: rv, ResourceVersionMatch: metav1.ResourceVersionMatchExact}] = true
		testCases[opts{ResourceVersion: rv, ResourceVersionMatch: metav1.ResourceVersionMatchExact, Limit: 100}] = true
		testCases[opts{ResourceVersion: rv, ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan}] = false
		testCases[opts{ResourceVersion: rv, ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan, Limit: 100}] = false
	}

	// 对多数请求而言，是否 bypass 与 Recursive 无关
	for opts, expectBypass := range testCases {
		opts.Recursive = true
		testCases[opts] = expectBypass
	}
	// 非递归 LIST 上 Continue 会被忽略
	for _, rv := range []string{oldRV, etcdRV} {
		for _, continueToken := range []string{continueOnOldRV, continueOnCacheRV, continueOnEtcdRV, continueOnNegativeRV} {
			testCases[opts{ResourceVersion: rv, Continue: continueToken}] = true
			testCases[opts{ResourceVersion: rv, Continue: continueToken, Limit: 100}] = true
		}
	}

	for _, rv := range []string{"", "0", oldRV, etcdRV} {
		for _, match := range []metav1.ResourceVersionMatch{"", metav1.ResourceVersionMatchExact, metav1.ResourceVersionMatchNotOlderThan} {
			for _, continueKey := range []string{"", continueOnOldRV, continueOnCacheRV, continueOnEtcdRV, continueOnNegativeRV} {
				for _, limit := range []int64{0, 100} {
					for _, recursive := range []bool{true, false} {
						opt := opts{
							Recursive:            recursive,
							ResourceVersion:      rv,
							ResourceVersionMatch: match,
							Limit:                limit,
							Continue:             continueKey,
						}
						if errs := validation.ValidateListOptions(toInternalOpts(opt), false); len(errs) != 0 {
							continue
						}
						if _, _, err = storage.ValidateListOptions(keyPrefix, storage.APIObjectVersioner{}, toStorageOpts(opt)); err != nil {
							continue
						}
						_, found := testCases[opt]
						if !found {
							t.Errorf("Test case not covered, but passes validation: %+v", opt)
						}
					}

				}
			}
		}
	}

	for opt := range testCases {
		if errs := validation.ValidateListOptions(toInternalOpts(opt), false); len(errs) != 0 {
			t.Errorf("Invalid LIST request that should not be tested %+v", opt)
			continue
		}
		if _, _, err = storage.ValidateListOptions(keyPrefix, storage.APIObjectVersioner{}, toStorageOpts(opt)); err != nil {
			t.Errorf("Invalid LIST request that should not be tested %+v", opt)
			continue
		}
	}

	runTestCases := func(t *testing.T, snapshotAvailable bool, testcases map[opts]bool, overrides ...map[opts]bool) {
		for opt, expectBypass := range testCases {
			for _, override := range overrides {
				if bypass, ok := override[opt]; ok {
					expectBypass = bypass
				}
			}
			backingStorage := &cachertesting.MockStorage{}
			backingStorage.GetListFn = func(_ context.Context, _ string, _ storage.ListOptions, listObj runtime.Object) error {
				podList := listObj.(*example.PodList)
				podList.ListMeta = metav1.ListMeta{ResourceVersion: cacheRV}
				return nil
			}
			cacher, _, err := newTestCacher(backingStorage)
			if err != nil {
				t.Fatalf("Couldn't create cacher: %v", err)
			}
			defer cacher.Stop()
			oldPod := &example.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:       "ns",
					Name:            "pod1",
					ResourceVersion: oldRV,
				},
			}
			latestPod := &example.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:       "ns",
					Name:            "pod1",
					ResourceVersion: cacheRV,
				},
			}
			if snapshotAvailable {
				if err := cacher.watchCache.Add(oldPod); err != nil {
					t.Fatal(err)
				}
				if err := cacher.watchCache.Update(latestPod); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := cacher.watchCache.Add(latestPod); err != nil {
					t.Fatal(err)
				}
			}
			result, err := delegator.ShouldDelegateList(toStorageOpts(opt), cacher)
			if err != nil {
				t.Fatal(err)
			}
			if result.ShouldDelegate != expectBypass {
				t.Errorf("Unexpected bypass result for List request with options %+v, bypass expected: %v, got: %v", opt, expectBypass, result.ShouldDelegate)
			}
		}
	}

	// Exact 匹配及在当前 cache RV 上的 continue。
	listFromSnapshotEnabledOverrides := map[opts]bool{}
	listFromSnapshotEnabledOverrides[opts{Recursive: true, ResourceVersion: cacheRV, Limit: 100}] = false
	listFromSnapshotEnabledOverrides[opts{Recursive: true, ResourceVersion: cacheRV, ResourceVersionMatch: metav1.ResourceVersionMatchExact}] = false
	listFromSnapshotEnabledOverrides[opts{Recursive: true, ResourceVersion: cacheRV, Limit: 100, ResourceVersionMatch: metav1.ResourceVersionMatchExact}] = false
	listFromSnapshotEnabledOverrides[opts{Recursive: true, ResourceVersion: "0", Limit: 100, Continue: continueOnCacheRV}] = false
	listFromSnapshotEnabledOverrides[opts{Recursive: true, ResourceVersion: "0", Continue: continueOnCacheRV}] = false
	listFromSnapshotEnabledOverrides[opts{Recursive: true, Limit: 100, Continue: continueOnCacheRV}] = false
	listFromSnapshotEnabledOverrides[opts{Recursive: true, Continue: continueOnCacheRV}] = false

	// Exact 匹配及带快照的 continue RV。
	snapshotAvailableOverrides := map[opts]bool{}
	snapshotAvailableOverrides[opts{Recursive: true, Continue: continueOnOldRV}] = false
	snapshotAvailableOverrides[opts{Recursive: true, Limit: 100, Continue: continueOnOldRV}] = false
	snapshotAvailableOverrides[opts{Recursive: true, ResourceVersion: "0", Continue: continueOnOldRV}] = false
	snapshotAvailableOverrides[opts{Recursive: true, ResourceVersion: "0", Limit: 100, Continue: continueOnOldRV}] = false
	snapshotAvailableOverrides[opts{Recursive: true, ResourceVersion: oldRV, Limit: 100}] = false
	snapshotAvailableOverrides[opts{Recursive: true, ResourceVersion: oldRV, ResourceVersionMatch: metav1.ResourceVersionMatchExact}] = false
	snapshotAvailableOverrides[opts{Recursive: true, ResourceVersion: oldRV, ResourceVersionMatch: metav1.ResourceVersionMatchExact, Limit: 100}] = false

	// TODO(p0lyn0mial): 以下测试假定 etcdfeature.DefaultFeatureSupportChecker.Supports(storage.RequestWatchProgress)
	// 求值为 true。否则缓存会被 bypass，测试将失败。
	//
	// 如果只运行 TestGetListCacheBypass，会看到该测试失败。
	// 但在 CI 中所有测试一起运行，其中必有测试会正确地
	// 初始化存储层，使上述方法求值为 true
	forceRequestWatchProgressSupport(t)

	consistentListFromCacheOverrides := map[opts]bool{}
	for _, recursive := range []bool{true, false} {
		consistentListFromCacheOverrides[opts{Recursive: recursive}] = false
		consistentListFromCacheOverrides[opts{Limit: 100, Recursive: recursive}] = false
	}

	t.Run("ListFromCacheSnapshot=false", func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, false)
		runTestCases(t, false, testCases, consistentListFromCacheOverrides)
	})
	t.Run("ListFromCacheSnapshot=true", func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, true)

		consistentReadWithSnapshotOverrides := map[opts]bool{}
		// 在负数 RV 上 continue 等同于一致性读取。
		consistentReadWithSnapshotOverrides[opts{Recursive: true, Limit: 0, Continue: continueOnNegativeRV}] = false
		consistentReadWithSnapshotOverrides[opts{Recursive: true, Limit: 100, Continue: continueOnNegativeRV}] = false
		consistentReadWithSnapshotOverrides[opts{Recursive: true, ResourceVersion: "0", Limit: 0, Continue: continueOnNegativeRV}] = false
		consistentReadWithSnapshotOverrides[opts{Recursive: true, ResourceVersion: "0", Limit: 100, Continue: continueOnNegativeRV}] = false
		// Exact 匹配一个缓存尚未观测到的 RV
		consistentReadWithSnapshotOverrides[opts{Recursive: true, ResourceVersion: etcdRV, Limit: 100}] = false
		consistentReadWithSnapshotOverrides[opts{Recursive: true, ResourceVersion: etcdRV, ResourceVersionMatch: metav1.ResourceVersionMatchExact}] = false
		consistentReadWithSnapshotOverrides[opts{Recursive: true, ResourceVersion: etcdRV, ResourceVersionMatch: metav1.ResourceVersionMatchExact, Limit: 100}] = false
		consistentReadWithSnapshotOverrides[opts{Recursive: true, Continue: continueOnEtcdRV}] = false
		consistentReadWithSnapshotOverrides[opts{Recursive: true, ResourceVersion: "0", Continue: continueOnEtcdRV}] = false
		consistentReadWithSnapshotOverrides[opts{Recursive: true, Continue: continueOnEtcdRV, Limit: 100}] = false
		consistentReadWithSnapshotOverrides[opts{Recursive: true, ResourceVersion: "0", Continue: continueOnEtcdRV, Limit: 100}] = false
		t.Run("SnapshotAvailable=false", func(t *testing.T) {
			runTestCases(t, false, testCases, listFromSnapshotEnabledOverrides, consistentListFromCacheOverrides, consistentReadWithSnapshotOverrides)
		})
		t.Run("SnapshotAvailable=true", func(t *testing.T) {
			runTestCases(t, true, testCases, listFromSnapshotEnabledOverrides, consistentListFromCacheOverrides, consistentReadWithSnapshotOverrides, snapshotAvailableOverrides)
		})
	})
}

// mustAtoi 是测试辅助函数，解析失败直接 panic。
func mustAtoi(s string) int {
	value, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return value
}

// TestConsistentReadFallback 验证一致性读（RV="" 的递归 List）的三种路径：
// 缓存已追平则直接返回；落后则等待 watch 进度通知并回退到存储；回退失败则报错。
// 同时校验 ConsistentReadTotal 指标的 fallback/success 标签。
func TestConsistentReadFallback(t *testing.T) {
	tcs := []struct {
		name                string
		skipStorageFallback bool
		watchCacheRV        string
		storageRV           string
		fallbackError       bool

		expectError             bool
		expectTooManyRequests   bool
		expectRV                string
		expectBlock             bool
		expectRequestsToStorage int
		expectMetric            string
	}{
		{
			name:                    "Success",
			watchCacheRV:            "42",
			storageRV:               "42",
			expectRV:                "42",
			expectRequestsToStorage: 1,
			expectMetric: `
# HELP apiserver_watch_cache_consistent_read_total [ALPHA] Counter for consistent reads from cache.
# TYPE apiserver_watch_cache_consistent_read_total counter
apiserver_watch_cache_consistent_read_total{fallback="false", group="", resource="pods", success="true"} 1
`,
		},
		{
			name:                    "Fallback",
			watchCacheRV:            "2",
			storageRV:               "42",
			expectRV:                "42",
			expectBlock:             true,
			expectRequestsToStorage: 2,
			expectMetric: `
# HELP apiserver_watch_cache_consistent_read_total [ALPHA] Counter for consistent reads from cache.
# TYPE apiserver_watch_cache_consistent_read_total counter
apiserver_watch_cache_consistent_read_total{fallback="true", group="", resource="pods", success="true"} 1
`,
		},
		{
			name:                    "Fallback Failure",
			watchCacheRV:            "2",
			storageRV:               "42",
			fallbackError:           true,
			expectError:             true,
			expectBlock:             true,
			expectRequestsToStorage: 2,
			expectMetric: `
# HELP apiserver_watch_cache_consistent_read_total [ALPHA] Counter for consistent reads from cache.
# TYPE apiserver_watch_cache_consistent_read_total counter
apiserver_watch_cache_consistent_read_total{fallback="true", group="", resource="pods", success="false"} 1
`,
		},
		{
			name:                    "Skip Storage Fallback",
			skipStorageFallback:     true,
			watchCacheRV:            "2",
			storageRV:               "42",
			expectError:             true,
			expectTooManyRequests:   true,
			expectBlock:             true,
			expectRequestsToStorage: 1,
			expectMetric: `
# HELP apiserver_watch_cache_consistent_read_total [ALPHA] Counter for consistent reads from cache.
# TYPE apiserver_watch_cache_consistent_read_total counter
apiserver_watch_cache_consistent_read_total{fallback="skipped", group="", resource="pods", success="false"} 1
`,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			forceRequestWatchProgressSupport(t)
			if tc.skipStorageFallback {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ConsistentListFromCacheSkipTimeoutFallback, true)
			}

			registry := k8smetrics.NewKubeRegistry()
			metrics.ConsistentReadTotal.Reset()
			if err := registry.Register(metrics.ConsistentReadTotal); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			backingStorage := &cachertesting.MockStorage{}
			backingStorage.GetListFn = func(_ context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
				podList := listObj.(*example.PodList)
				podList.ResourceVersion = tc.watchCacheRV
				return nil
			}
			c := testingclock.NewFakeClock(time.Now())
			cacher, _, err := newTestCacherWithoutSyncing(backingStorage, c)
			if err != nil {
				t.Fatalf("Couldn't create cacher: %v", err)
			}
			defer cacher.Stop()
			delegator := NewCacheDelegator(cacher, backingStorage)
			defer delegator.Stop()
			if err := cacher.ready.wait(context.Background()); err != nil {
				t.Fatalf("unexpected error waiting for the cache to be ready")
			}

			if fmt.Sprintf("%d", cacher.watchCache.resourceVersion) != tc.watchCacheRV {
				t.Fatalf("Expected watch cache RV to equal watchCacheRV, got: %d, want: %s", cacher.watchCache.resourceVersion, tc.watchCacheRV)
			}
			requestToStorageCount := 0
			backingStorage.GetListFn = func(_ context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
				requestToStorageCount += 1
				podList := listObj.(*example.PodList)
				if key == cacher.resourcePrefix {
					podList.ResourceVersion = tc.storageRV
					return nil
				}
				if tc.fallbackError {
					return errDummy
				}
				podList.ResourceVersion = tc.storageRV
				return nil
			}
			backingStorage.GetRVFn = func(_ context.Context) (uint64, error) {
				requestToStorageCount += 1
				rv, err := strconv.Atoi(tc.storageRV)
				if err != nil {
					t.Fatalf("failed to parse RV: %s", err)
				}
				return uint64(rv), nil
			}
			result := &example.PodList{}

			ctx, clockStepCancelFn := context.WithTimeout(context.TODO(), time.Minute)
			defer clockStepCancelFn()
			if tc.expectBlock {
				go func(ctx context.Context) {
					for {
						select {
						case <-ctx.Done():
							return
						default:
							if c.HasWaiters() {
								c.Step(blockTimeout)
							}
							time.Sleep(time.Millisecond)
						}
					}
				}(ctx)
			}

			start := cacher.clock.Now()
			err = delegator.GetList(context.TODO(), "/pods/ns", storage.ListOptions{ResourceVersion: "", Recursive: true}, result)
			clockStepCancelFn()
			duration := cacher.clock.Since(start)
			if (err != nil) != tc.expectError {
				t.Fatalf("Unexpected error err: %v", err)
			}
			if tc.expectTooManyRequests && !apierrors.IsTooManyRequests(err) {
				t.Fatalf("Unexpected error, got: %v, want TooManyRequests", err)
			}
			if result.ResourceVersion != tc.expectRV {
				t.Fatalf("Unexpected List response RV, got: %q, want: %q", result.ResourceVersion, tc.expectRV)
			}
			if requestToStorageCount != tc.expectRequestsToStorage {
				t.Fatalf("Unexpected number of requests to storage, got: %d, want: %d", requestToStorageCount, tc.expectRequestsToStorage)
			}
			blocked := duration >= blockTimeout
			if blocked != tc.expectBlock {
				t.Fatalf("Unexpected block, got: %v, want: %v", blocked, tc.expectBlock)
			}

			if err := testutil.GatherAndCompare(registry, strings.NewReader(tc.expectMetric), "apiserver_watch_cache_consistent_read_total"); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestMatchExactResourceVersionFallback 验证 Exact 语义：缓存中不存在该 RV 的
// 快照时必须回退到存储，快照可用时则直接由缓存服务。
func TestMatchExactResourceVersionFallback(t *testing.T) {
	for _, snapshotAvailable := range []bool{false, true} {
		t.Run(fmt.Sprintf("SnapshotAvailable=%t", snapshotAvailable), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, true)
			backingStorage := &cachertesting.MockStorage{}
			expectStoreRequests := 0
			backingStorage.GetListFn = func(_ context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
				expectStoreRequests++
				podList := listObj.(*example.PodList)
				switch opts.ResourceVersionMatch {
				case "":
					podList.ResourceVersion = "42"
				case metav1.ResourceVersionMatchExact:
					podList.ResourceVersion = opts.ResourceVersion
				}
				return nil
			}

			cacher, _, err := newTestCacherWithoutSyncing(backingStorage, clock.RealClock{})
			if err != nil {
				t.Fatalf("Couldn't create cacher: %v", err)
			}
			defer cacher.Stop()

			if err := cacher.ready.wait(context.Background()); err != nil {
				t.Fatalf("unexpected error waiting for the cache to be ready")
			}
			delegator := NewCacheDelegator(cacher, backingStorage)

			if snapshotAvailable {
				oldPod := &example.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace:       "ns",
						Name:            "pod1",
						ResourceVersion: "20",
					},
				}
				if err := cacher.watchCache.Add(oldPod); err != nil {
					t.Fatal(err)
				}
			}

			result := &example.PodList{}
			err = delegator.GetList(context.TODO(), "/pods/ns", storage.ListOptions{ResourceVersion: "20", ResourceVersionMatch: metav1.ResourceVersionMatchExact, Recursive: true, Predicate: storage.Everything}, result)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if result.ResourceVersion != "20" {
				t.Fatalf("Unexpected List response RV, got: %q, want: %d", result.ResourceVersion, 20)
			}
			expectStoreRequestsVal := 2
			if snapshotAvailable {
				expectStoreRequestsVal = 1
			}
			if expectStoreRequests != expectStoreRequestsVal {
				t.Fatalf("Unexpected number of requests to storage, got: %d, want: %d", expectStoreRequests, expectStoreRequestsVal)
			}
		})
	}
}

// TestGetListNonRecursiveCacheWithConsistentListFromCache 验证在
// ListFromCacheSnapshot=false 时，RV 为空的递归 List 仍由缓存服务，并通过
// GetCurrentResourceVersion 获取当前 RV。
func TestGetListNonRecursiveCacheWithConsistentListFromCache(t *testing.T) {
	// 我们只关心 ListFromCacheSnapshot=false，所以在开头一次性设置特性开关
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.ListFromCacheSnapshot: false,
	})
	forceRequestWatchProgressSupport(t)

	tests := []struct {
		name                    string
		consistentListFromCache bool
		expectGetListCallCount  int
		expectGetCurrentRV      bool
		injectRVError           bool
		expectedError           error
	}{
		{
			name:                    "ConsistentListFromCache enabled - served from cache",
			consistentListFromCache: true,
			expectGetListCallCount:  1,
			expectGetCurrentRV:      true,
			injectRVError:           false,
			expectedError:           nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var getListCount, getCurrentRVCount int
			backingStorage := &cachertesting.MockStorage{}

			backingStorage.GetListFn = func(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
				getListCount++
				if tc.injectRVError {
					return errDummy
				}
				podList := listObj.(*example.PodList)
				podList.ListMeta = metav1.ListMeta{ResourceVersion: "100"}
				return nil
			}

			backingStorage.GetRVFn = func(ctx context.Context) (uint64, error) {
				getCurrentRVCount++
				rv := uint64(100)
				err := error(nil)
				if tc.injectRVError {
					err = errDummy
					return 0, err
				}
				return rv, nil
			}

			cacher, v, err := newTestCacher(backingStorage)
			if err != nil {
				t.Fatalf("Couldn't create cacher: %v", err)
			}
			defer cacher.Stop()

			// 在注入错误之前，等待 cacher 就绪
			if err := cacher.ready.wait(context.Background()); err != nil {
				t.Fatalf("unexpected error waiting for the cache to be ready: %v", err)
			}
			delegator := NewCacheDelegator(cacher, backingStorage)
			defer delegator.Stop()

			// 准备测试对象
			key := "/pods/ns"
			input := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"}}
			if err := v.UpdateObject(input, 100); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// 将对象放入存储
			if err := cacher.watchCache.Add(input); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			pred := storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 500,
			}
			result := &example.PodList{}

			// 以空 RV 发起 list 调用——delegator 会获取当前 RV 并使用它
			err = delegator.GetList(context.TODO(), key, storage.ListOptions{
				ResourceVersion: "",
				Predicate:       pred,
				Recursive:       true,
			}, result)

			// 校验错误是否符合预期
			if !errors.Is(err, tc.expectedError) {
				t.Errorf("Expected error %v, got: %v", tc.expectedError, err)
			}

			// 校验调用的是正确的存储方法
			if getListCount != tc.expectGetListCallCount {
				t.Errorf("Expected GetList to be called %d times, but it was called %d times", tc.expectGetListCallCount, getListCount)
			}
			if tc.expectGetCurrentRV && getCurrentRVCount == 0 {
				t.Error("Expected GetCurrentResourceVersion to be called, but it wasn't")
			}
			if !tc.expectGetCurrentRV && getCurrentRVCount > 0 {
				t.Errorf("Expected GetCurrentResourceVersion not to be called, but it was called %d times", getCurrentRVCount)
			}

			// 对于成功的缓存读取，校验 resource version
			if err == nil {
				resultRV, err := cacher.versioner.ParseResourceVersion(result.ResourceVersion)
				if err != nil {
					t.Fatalf("Failed to parse result resource version: %v", err)
				}
				expectedRV := uint64(100)
				if resultRV != expectedRV {
					t.Errorf("Expected RV %d but got %d", expectedRV, resultRV)
				}
			}
		})
	}
}

// TestGetCacheBypass 验证 Get 的委托规则：RV=0 由缓存服务，未指定 RV 时
// 必须回退到存储（一致性读）。
func TestGetCacheBypass(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()
	delegator := NewCacheDelegator(cacher, backingStorage)
	defer delegator.Stop()

	result := &example.Pod{}

	// 向底层注入错误，检查 cacher 是否被 bypass。
	backingStorage.InjectGetListError(errDummy)
	err = delegator.Get(context.TODO(), "/pods/ns/pod-0", storage.GetOptions{
		IgnoreNotFound:  true,
		ResourceVersion: "0",
	}, result)
	if err != nil {
		t.Errorf("Get with RV=0 should be served from cache: %v", err)
	}

	err = delegator.Get(context.TODO(), "/pods/ns/pod-0", storage.GetOptions{
		IgnoreNotFound:  true,
		ResourceVersion: "",
	}, result)
	if !errors.Is(err, errDummy) {
		t.Errorf("Get without RV=0 should bypass cacher: %v", err)
	}
}

// TestListMetrics 验证由 watch cache 服务的 List 会记录 fetched/evaluated/returned
// 三类对象计数，并带上索引标签。
func TestListMetrics(t *testing.T) {
	ctx, delegator, terminate := testSetup(t, withNodeNameAndNamespaceIndex)
	t.Cleanup(terminate)

	storagemetrics.Register()
	legacyregistry.Reset()
	t.Cleanup(legacyregistry.Reset)

	pod := &example.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns"},
		Spec:       example.PodSpec{NodeName: "node"},
	}
	if err := (storage.APIObjectVersioner{}).UpdateObject(pod, 100); err != nil {
		t.Fatal(err)
	}
	if err := delegator.cacher.watchCache.Add(pod); err != nil {
		t.Fatal(err)
	}

	pred := storagetesting.CreatePodPredicate(fields.ParseSelectorOrDie("spec.nodeName=node"), true, []string{"spec.nodeName"})
	result := &example.PodList{}
	if err := delegator.GetList(ctx, "/pods/ns", storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       pred,
		Recursive:       true,
	}, result); err != nil {
		t.Fatal(err)
	}

	expected := `# HELP apiserver_storage_list_evaluated_objects_total [ALPHA] Number of objects tested in the course of serving a LIST request from storage
# TYPE apiserver_storage_list_evaluated_objects_total counter
apiserver_storage_list_evaluated_objects_total{group="",resource="pods",storage="watchcache"} 0
# HELP apiserver_storage_list_fetched_objects_total [ALPHA] Number of objects read from storage in the course of serving a LIST request
# TYPE apiserver_storage_list_fetched_objects_total counter
apiserver_storage_list_fetched_objects_total{group="",index="f:spec.nodeName",resource="pods",storage="watchcache"} 1
# HELP apiserver_storage_list_returned_objects_total [ALPHA] Number of objects returned for a LIST request from storage
# TYPE apiserver_storage_list_returned_objects_total counter
apiserver_storage_list_returned_objects_total{group="",resource="pods",storage="watchcache"} 1
# HELP apiserver_storage_list_total [ALPHA] Number of LIST requests served from storage
# TYPE apiserver_storage_list_total counter
apiserver_storage_list_total{group="",index="f:spec.nodeName",resource="pods",storage="watchcache"} 1
`
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected),
		"apiserver_storage_list_total",
		"apiserver_storage_list_fetched_objects_total",
		"apiserver_storage_list_evaluated_objects_total",
		"apiserver_storage_list_returned_objects_total",
	); err != nil {
		t.Fatal(err)
	}
}

// TestWatchCacheBypass 验证 Watch 无论是否指定 RV=0 都由缓存服务（与 List
// 不同，watch 可以安全地从任意历史点重放）。
func TestWatchCacheBypass(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()
	delegator := NewCacheDelegator(cacher, backingStorage)
	defer delegator.Stop()

	_, err = delegator.Watch(context.TODO(), "/pods/ns", storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
	})
	if err != nil {
		t.Errorf("Watch with RV=0 should be served from cache: %v", err)
	}

	_, err = delegator.Watch(context.TODO(), "/pods/ns", storage.ListOptions{
		ResourceVersion: "",
		Predicate:       storage.Everything,
	})
	if err != nil {
		t.Errorf("Watch without RV=0 should be served from cache: %v", err)
	}
}

// TestEmptyWatchEventCache 验证缓存从 etcd 当前版本初始化、事件历史为空时：
// 从当前 RV 或更高版本建立 watch 可以成功，更早的 RV 则返回 ResourceExpired。
func TestEmptyWatchEventCache(t *testing.T) {
	server, etcdStorage := newEtcdTestStorage(t, etcd3testing.PathPrefix())
	defer server.Terminate(t)

	// 添加若干对象
	v := storage.APIObjectVersioner{}
	lastRV := uint64(0)
	for i := 0; i < 5; i++ {
		pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("foo-%d", i), Namespace: "test-ns"}}
		out := &example.Pod{}
		key := computePodKey(pod)
		if err := etcdStorage.Create(context.Background(), key, pod, out, 0); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		var err error
		if lastRV, err = v.ParseResourceVersion(out.ResourceVersion); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	cacher, _, err := newTestCacher(etcdStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	// 由于 cacher 总是从 etcd 的"当前"版本初始化，
	// 现在我们得到一个 watch 事件缓存为空、resourceVersion 为 rv 的 cacher。
	// 它应当支持从 rv 及更高版本建立 watch，但不支持更旧的版本。

	expectedResourceExpiredError := apierrors.NewResourceExpired("").ErrStatus
	tests := []struct {
		name            string
		resourceVersion uint64
		expectedEvent   *watch.Event
	}{
		{
			name:            "RV-1",
			resourceVersion: lastRV - 1,
			expectedEvent:   &watch.Event{Type: watch.Error, Object: &expectedResourceExpiredError},
		},
		{
			name:            "RV",
			resourceVersion: lastRV,
		},
		{
			name:            "RV+1",
			resourceVersion: lastRV + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := storage.ListOptions{
				ResourceVersion: strconv.Itoa(int(tt.resourceVersion)),
				Predicate:       storage.Everything,
			}
			watcher, err := cacher.Watch(context.Background(), "/pods/test-ns", opts)
			if err != nil {
				t.Fatalf("Failed to create watch: %v", err)
			}
			defer watcher.Stop()
			select {
			case event := <-watcher.ResultChan():
				if tt.expectedEvent == nil {
					t.Errorf("Unexpected event: type=%#v, object=%#v", event.Type, event.Object)
					break
				}
				if e, a := tt.expectedEvent.Type, event.Type; e != a {
					t.Errorf("Expected: %s, got: %s", e, a)
				}
				if e, a := tt.expectedEvent.Object, event.Object; !apiequality.Semantic.DeepDerivative(e, a) {
					t.Errorf("Expected: %#v, got: %#v", e, a)
				}
			case <-time.After(1 * time.Second):
				// watch 已经建立成功，
				// 否则我们会阻塞在 cache.Watch(...) 上。
				// 另外，这些测试本质上是串行的，
				// 意味着没有其他参与者
				// 会向数据库添加条目，
				// 从而导致收到新条目。
				// 因此等待 1s 是合理的。
				if tt.expectedEvent != nil {
					t.Errorf("Failed to get an event")
				}
				// watch 保持建立成功
			}
		})
	}
}

// TestWatchNotHangingOnStartupFailure 验证缓存无法初始化时，Watch 会阻塞等待
// 就绪，但在请求上下文取消后能及时返回错误而不是永久挂起。
func TestWatchNotHangingOnStartupFailure(t *testing.T) {
	// 配置 cacher 使其无法初始化，因为
	// 对底层存储的 list 持续失败。
	dummyErr := fmt.Errorf("dummy")
	backingStorage := &cachertesting.MockStorage{WatchErr: dummyErr}
	cacher, _, err := newTestCacherWithoutSyncing(backingStorage, testingclock.NewFakeClock(time.Now()))
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	// 一段时间后取消 watch，检查它能正确终止，
	// 而不是永久挂起。
	go func() {
		defer cancel()
		cacher.clock.Sleep(1 * time.Second)
	}()

	// Watch 会阻塞等待 watchcache 初始化。
	// 确保当其上下文被取消时（例如请求因任何原因被终止），
	// 它能及时终止。
	_, err = cacher.Watch(ctx, "/pods/ns", storage.ListOptions{ResourceVersion: "0"})
	if err == nil || !strings.Contains(err.Error(), "storage is (re)initializing") {
		t.Errorf("Unexpected error: %#v", err)
	}
}

// TestWatcherNotGoingBackInTime 验证慢速 watcher 不会拖慢分发线程，且快速
// watcher 收到的事件 resourceVersion 单调递增、不重复。
func TestWatcherNotGoingBackInTime(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, v, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	// 确保留有一些用于放慢处理的预算。
	cacher.dispatchTimeoutBudget.returnUnused(100 * time.Millisecond)

	makePod := func(i int) *examplev1.Pod {
		return &examplev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("pod-%d", 1000+i),
				Namespace:       "ns",
				ResourceVersion: fmt.Sprintf("%d", 1000+i),
			},
		}
	}
	if err := cacher.watchCache.Add(makePod(0)); err != nil {
		t.Errorf("error: %v", err)
	}

	totalPods := 100

	// 创建一个读取会逐渐变慢的 watcher。
	w1, err := cacher.Watch(context.TODO(), "/pods/ns", storage.ListOptions{
		ResourceVersion: "999",
		Predicate:       storage.Everything,
	})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	defer w1.Stop()
	go func() {
		a := 0
		for range w1.ResultChan() {
			time.Sleep(time.Millisecond)
			a++
			if a == 100 {
				break
			}
		}
	}()

	// 现在向缓存推入大量对象。
	for i := 1; i < totalPods; i++ {
		cacher.watchCache.Add(makePod(i))
	}

	// 创建快速 watcher，确保它每个对象恰好收到一次。
	w2, err := cacher.Watch(context.TODO(), "/pods/ns", storage.ListOptions{ResourceVersion: "999", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	defer w2.Stop()

	shouldContinue := true
	currentRV := uint64(0)
	for shouldContinue {
		select {
		case event, ok := <-w2.ResultChan():
			if !ok {
				shouldContinue = false
				break
			}
			rv, err := v.ParseResourceVersion(event.Object.(metaRuntimeInterface).GetResourceVersion())
			if err != nil {
				t.Errorf("unexpected parsing error: %v", err)
			} else {
				if rv < currentRV {
					t.Errorf("watcher going back in time")
				}
				currentRV = rv
			}
		case <-time.After(time.Second):
			w2.Stop()
		}
	}
}

// TestCacherDontAcceptRequestsStopped 验证 Stop 后不再接受新 watch，但已有的
// watch 会被关闭，Get/GetList 仍可回退到底层存储。
func TestCacherDontAcceptRequestsStopped(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	delegator := NewCacheDelegator(cacher, backingStorage)
	defer delegator.Stop()

	w, err := delegator.Watch(context.Background(), "/pods/ns", storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}

	watchClosed := make(chan struct{})
	go func() {
		defer close(watchClosed)
		for event := range w.ResultChan() {
			switch event.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				// 符合预期
			default:
				t.Errorf("unexpected event %#v", event)
			}
		}
	}()

	cacher.Stop()

	_, err = delegator.Watch(context.Background(), "/pods/ns", storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
	if err == nil {
		t.Fatalf("Success to create Watch: %v", err)
	}

	result := &example.Pod{}
	err = delegator.Get(context.TODO(), "/pods/ns/pod-0", storage.GetOptions{
		IgnoreNotFound:  true,
		ResourceVersion: "1",
	}, result)
	if err != nil {
		t.Fatalf("Failed to get object: %v:", err)
	}

	listResult := &example.PodList{}
	err = delegator.GetList(context.TODO(), "/pods/ns", storage.ListOptions{
		ResourceVersion: "1",
		Recursive:       true,
		Predicate: storage.SelectionPredicate{
			Limit: 500,
		},
	}, listResult)
	if err != nil {
		t.Fatalf("Failed to list objects: %v", err)
	}

	select {
	case <-watchClosed:
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("timed out waiting for watch to close")
	}
}

// TestCacherDontMissEventsOnReinitialization 验证重新初始化（relist）期间建立
// 的并发 watch 不会漏事件，且每个 watcher 看到的 RV 连续递增。
func TestCacherDontMissEventsOnReinitialization(t *testing.T) {
	makePod := func(i int) *example.Pod {
		return &example.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("pod-%d", i),
				Namespace:       "ns",
				ResourceVersion: fmt.Sprintf("%d", i),
			},
		}
	}

	listCalls, watchCalls := 0, 0
	backingStorage := &cachertesting.MockStorage{
		GetListFn: func(_ context.Context, _ string, _ storage.ListOptions, listObj runtime.Object) error {
			podList := listObj.(*example.PodList)
			var err error
			switch listCalls {
			case 0:
				podList.ListMeta = metav1.ListMeta{ResourceVersion: "1"}
			case 1:
				podList.ListMeta = metav1.ListMeta{ResourceVersion: "10"}
			default:
				t.Errorf("unexpected list call: %d", listCalls)
				err = fmt.Errorf("unexpected list call")
			}
			listCalls++
			return err
		},
		WatchFn: func(_ context.Context, _ string, _ storage.ListOptions) (watch.Interface, error) {
			var w *watch.FakeWatcher
			var err error
			switch watchCalls {
			case 0:
				w = watch.NewFakeWithChanSize(10, false)
				for i := 2; i < 8; i++ {
					w.Add(makePod(i))
				}
				// 发出一个错误以强制重新 list。
				w.Error(nil)
				w.Stop()
			case 1:
				w = watch.NewFakeWithChanSize(10, false)
				for i := 12; i < 18; i++ {
					w.Add(makePod(i))
				}
				// 保持该 watch 打开以避免再次重新初始化，
				// 但将其注册到清理回调。
				t.Cleanup(func() { w.Stop() })
			default:
				t.Errorf("unexpected watch call: %d", watchCalls)
				err = fmt.Errorf("unexpected watch call")
			}
			watchCalls++
			return w, err
		},
	}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	concurrency := 1000
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	// 确保当 cacher 已处理完所有内容、在某些 watch 建立之前就回到
	// Pending 状态时，测试不会死锁。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			w, err := cacher.Watch(ctx, "/pods/", storage.ListOptions{ResourceVersion: "1", Predicate: storage.Everything})
			if err != nil {
				// Watch 初始化失败（这很可能是因为在 watch 初始化之前
				// cacher 已回到 Pending 状态。
				// 忽略这种情况。
				return
			}
			defer w.Stop()

			prevRV := -1
			for event := range w.ResultChan() {
				if event.Type == watch.Error {
					break
				}
				object := event.Object
				if co, ok := object.(runtime.CacheableObject); ok {
					object = co.GetObject()
				}
				rv, err := strconv.Atoi(object.(*example.Pod).ResourceVersion)
				if err != nil {
					t.Errorf("incorrect resource version: %v", err)
					return
				}
				if prevRV != -1 && prevRV+1 != rv {
					t.Errorf("unexpected event received, prevRV=%d, rv=%d", prevRV, rv)
					return
				}
				prevRV = rv
			}

		}()
	}
	wg.Wait()
}

// TestCacherNoLeakWithMultipleWatchers 验证 watch 的创建/停止与过期 bookmark
// watcher 清理并发进行时不会泄漏，运行数秒以覆盖多个时间桶。
func TestCacherNoLeakWithMultipleWatchers(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	pred := storage.Everything
	pred.AllowWatchBookmarks = true

	// 运行碰撞测试 3 秒，让约 2 个时间桶过期
	stopCh := make(chan struct{})
	var watchErr error
	time.AfterFunc(3*time.Second, func() { close(stopCh) })

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				w, err := cacher.Watch(ctx, "/pods/ns", storage.ListOptions{ResourceVersion: "0", Predicate: pred})
				if err != nil {
					watchErr = fmt.Errorf("Failed to create watch: %v", err)
					return
				}
				w.Stop()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				cacher.Lock()
				cacher.bookmarkWatchers.popExpiredWatchersThreadUnsafe()
				cacher.Unlock()
			}
		}
	}()

	// 等待添加/移除 watcher 的 goroutine 结束
	wg.Wait()

	if watchErr != nil {
		t.Fatal(watchErr)
	}

	// 等满过期周期后再弹出已过期的 watcher
	time.Sleep(2 * time.Second)
	cacher.Lock()
	defer cacher.Unlock()
	cacher.bookmarkWatchers.popExpiredWatchersThreadUnsafe()
	if len(cacher.bookmarkWatchers.watchersBuckets) != 0 {
		numWatchers := 0
		for bucketID, v := range cacher.bookmarkWatchers.watchersBuckets {
			numWatchers += len(v)
			t.Errorf("there are %v watchers at bucket Id %v with start Id %v", len(v), bucketID, cacher.bookmarkWatchers.startBucketID)
		}
		t.Errorf("unexpected bookmark watchers %v", numWatchers)
	}
}

// testCacherSendBookmarkEvents 是 bookmark 事件的公共测试体：
// allowWatchBookmarks 为 true 时应在事件流之后收到 bookmark，否则不应收到。
func testCacherSendBookmarkEvents(t *testing.T, allowWatchBookmarks, expectedBookmarks bool) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	pred := storage.Everything
	pred.AllowWatchBookmarks = allowWatchBookmarks

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	w, err := cacher.Watch(ctx, "/pods/ns", storage.ListOptions{ResourceVersion: "0", Predicate: pred})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}

	resourceVersion := uint64(1000)
	errc := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(time.Second)
		for i := 0; time.Now().Before(deadline); i++ {
			err := cacher.watchCache.Add(&examplev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:            fmt.Sprintf("pod-%d", i),
					Namespace:       "ns",
					ResourceVersion: fmt.Sprintf("%v", resourceVersion+uint64(i)),
				}})
			if err != nil {
				errc <- fmt.Errorf("failed to add a pod: %v", err)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	timeoutCh := time.After(2 * time.Second)
	lastObservedRV := uint64(0)
	for {
		select {
		case err := <-errc:
			t.Fatal(err)
			return
		case event, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("Unexpected closed")
			}
			rv, err := cacher.versioner.ObjectResourceVersion(event.Object)
			if err != nil {
				t.Errorf("failed to parse resource version from %#v: %v", event.Object, err)
			}
			if event.Type == watch.Bookmark {
				if !expectedBookmarks {
					t.Fatalf("Unexpected bookmark events received")
				}

				if rv < lastObservedRV {
					t.Errorf("Unexpected bookmark event resource version %v (last %v)", rv, lastObservedRV)
				}
				return
			}
			lastObservedRV = rv
		case <-timeoutCh:
			if expectedBookmarks {
				t.Fatal("Unexpected timeout to receive a bookmark event")
			}
			return
		}
	}
}

// TestCacherSendBookmarkEvents 分别验证允许与不允许 bookmark 的两种配置。
func TestCacherSendBookmarkEvents(t *testing.T) {
	testCases := []struct {
		allowWatchBookmarks bool
		expectedBookmarks   bool
	}{
		{
			allowWatchBookmarks: true,
			expectedBookmarks:   true,
		},
		{
			allowWatchBookmarks: false,
			expectedBookmarks:   false,
		},
	}

	for _, tc := range testCases {
		testCacherSendBookmarkEvents(t, tc.allowWatchBookmarks, tc.expectedBookmarks)
	}
}

// TestInitialEventsEndBookmark 验证 WatchList 语义：SendInitialEvents 时先发送
// 初始对象事件，随后发送带 InitialEventsAnnotationKey 的 bookmark 表示同步完成。
// 该 bookmark 只在 allowWatchBookmarks 与 sendInitialEvents 同时为真时出现。
func TestInitialEventsEndBookmark(t *testing.T) {
	for _, listFromCacheSnapshot := range []bool{true, false} {
		t.Run(fmt.Sprintf("ListFromCacheSnapshot=%v", listFromCacheSnapshot), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchList, true)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, listFromCacheSnapshot)
			forceRequestWatchProgressSupport(t)

			backingStorage := &cachertesting.MockStorage{}
			cacher, _, err := newTestCacher(backingStorage)
			if err != nil {
				t.Fatalf("Couldn't create cacher: %v", err)
			}
			defer cacher.Stop()

			makePod := func(index uint64) *example.Pod {
				return &example.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:            fmt.Sprintf("pod-%d", index),
						Namespace:       "ns",
						ResourceVersion: fmt.Sprintf("%v", 100+index),
					},
				}
			}

			numberOfPods := 3
			var expectedPodEvents []watch.Event
			for i := 1; i <= numberOfPods; i++ {
				pod := makePod(uint64(i))
				if err := cacher.watchCache.Add(pod); err != nil {
					t.Fatalf("failed to add a pod: %v", err)
				}
				expectedPodEvents = append(expectedPodEvents, watch.Event{Type: watch.Added, Object: pod})
			}
			var currentResourceVersion uint64 = 100 + 3

			trueVal, falseVal := true, false

			scenarios := []struct {
				name                string
				allowWatchBookmarks bool
				sendInitialEvents   *bool
			}{
				{
					name:                "allowWatchBookmarks=false, sendInitialEvents=false",
					allowWatchBookmarks: false,
					sendInitialEvents:   &falseVal,
				},
				{
					name:                "allowWatchBookmarks=false, sendInitialEvents=true",
					allowWatchBookmarks: false,
					sendInitialEvents:   &trueVal,
				},
				{
					name:                "allowWatchBookmarks=true, sendInitialEvents=true",
					allowWatchBookmarks: true,
					sendInitialEvents:   &trueVal,
				},
				{
					name:                "allowWatchBookmarks=true, sendInitialEvents=false",
					allowWatchBookmarks: true,
					sendInitialEvents:   &falseVal,
				},
				{
					name:                "allowWatchBookmarks=false, sendInitialEvents=nil",
					allowWatchBookmarks: true,
				},
			}

			for _, scenario := range scenarios {
				t.Run(scenario.name, func(t *testing.T) {
					expectedWatchEvents := expectedPodEvents
					if scenario.allowWatchBookmarks && scenario.sendInitialEvents != nil && *scenario.sendInitialEvents {
						expectedWatchEvents = append(expectedWatchEvents, watch.Event{
							Type: watch.Bookmark,
							Object: &example.Pod{
								ObjectMeta: metav1.ObjectMeta{
									ResourceVersion: strconv.FormatUint(currentResourceVersion, 10),
									Annotations:     map[string]string{metav1.InitialEventsAnnotationKey: "true"},
								},
							},
						})
					}

					pred := storage.Everything
					pred.AllowWatchBookmarks = scenario.allowWatchBookmarks
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
					defer cancel()
					w, err := cacher.Watch(ctx, "/pods/ns", storage.ListOptions{ResourceVersion: "100", SendInitialEvents: scenario.sendInitialEvents, Predicate: pred})
					if err != nil {
						t.Fatalf("Failed to create watch: %v", err)
					}
					storagetesting.TestCheckResultsInStrictOrder(t, w, expectedWatchEvents)
					storagetesting.TestCheckNoMoreResultsWithIgnoreFunc(t, w, nil)
				})
			}
		})
	}
}

// TestCacherSendsMultipleWatchBookmarks 验证周期性 bookmark：只要缓存 RV 推进，
// watcher 应能连续收到多个 bookmark 且 RV 不回退。
func TestCacherSendsMultipleWatchBookmarks(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()
	// 调整 bookmarkFrequency 以加快测试。
	// 注意：由于重算频率的精度所限，低于 1s 的取值差别不大。
	cacher.bookmarkWatchers.bookmarkFrequency = time.Second

	pred := storage.Everything
	pred.AllowWatchBookmarks = true

	makePod := func(index int) *examplev1.Pod {
		return &examplev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("pod-%d", index),
				Namespace:       "ns",
				ResourceVersion: fmt.Sprintf("%v", 100+index),
			},
		}
	}

	// 创建 pod 以初始化 watch cache。
	if err := cacher.watchCache.Add(makePod(0)); err != nil {
		t.Fatalf("failed to add a pod: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := cacher.Watch(ctx, "/pods/ns", storage.ListOptions{ResourceVersion: "100", Predicate: pred})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}

	// 再创建一个 pod，确保当前 RV 更高，
	// 从而 bookmark 会被投递（事件只在 RV 高于
	// init 事件中的最大 RV 时才会投递）。
	if err := cacher.watchCache.Add(makePod(1)); err != nil {
		t.Fatalf("failed to add a pod: %v", err)
	}

	timeoutCh := time.After(5 * time.Second)
	lastObservedRV := uint64(0)
	// 确保 watcher 收到两个 bookmark。
	for observedBookmarks := 0; observedBookmarks < 2; {
		select {
		case event, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("Unexpected closed")
			}
			rv, err := cacher.versioner.ObjectResourceVersion(event.Object)
			if err != nil {
				t.Errorf("failed to parse resource version from %#v: %v", event.Object, err)
			}
			if event.Type == watch.Bookmark {
				observedBookmarks++
				if rv < lastObservedRV {
					t.Errorf("Unexpected bookmark event resource version %v (last %v)", rv, lastObservedRV)
				}
			}
			lastObservedRV = rv
		case <-timeoutCh:
			t.Fatal("Unexpected timeout to receive bookmark events")
		}
	}
}

// TestDispatchingBookmarkEventsWithConcurrentStop 反复在分发 bookmark 的同时
// 停止 watcher，验证不会 panic、不会死锁（典型的数据竞争/关闭顺序回归测试）。
func TestDispatchingBookmarkEventsWithConcurrentStop(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	// 确保留有一些用于放慢处理的预算。
	cacher.dispatchTimeoutBudget.returnUnused(100 * time.Millisecond)

	resourceVersion := uint64(1000)
	err = cacher.watchCache.Add(&examplev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pod-0",
			Namespace:       "ns",
			ResourceVersion: fmt.Sprintf("%v", resourceVersion),
		}})
	if err != nil {
		t.Fatalf("failed to add a pod: %v", err)
	}

	for i := 0; i < 1000; i++ {
		pred := storage.Everything
		pred.AllowWatchBookmarks = true
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		w, err := cacher.Watch(ctx, "/pods/ns", storage.ListOptions{ResourceVersion: "999", Predicate: pred})
		if err != nil {
			t.Fatalf("Failed to create watch: %v", err)
		}
		bookmark := &watchCacheEvent{
			Type:            watch.Bookmark,
			ResourceVersion: uint64(i),
			Object:          cacher.newFunc(),
		}
		err = cacher.versioner.UpdateObject(bookmark.Object, bookmark.ResourceVersion)
		if err != nil {
			t.Fatalf("failure to update version of object (%d) %#v", bookmark.ResourceVersion, bookmark.Object)
		}

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			cacher.processEvent(bookmark)
			wg.Done()
		}()

		go func() {
			w.Stop()
			wg.Done()
		}()

		done := make(chan struct{})
		go func() {
			for range w.ResultChan() {
			}
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("receive result timeout")
		}
		w.Stop()
		wg.Wait()
	}
}

// TestBookmarksOnResourceVersionUpdates 验证缓存 RV 因 progress notify 推进时，
// 会向允许 bookmark 的 watcher 推送新 RV 的 bookmark。
func TestBookmarksOnResourceVersionUpdates(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	// 确保 bookmark 的发送频率高于每 1 分钟一次。
	cacher.bookmarkWatchers = newTimeBucketWatchers(clock.RealClock{}, 2*time.Second)

	makePod := func(i int) *examplev1.Pod {
		return &examplev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("pod-%d", i),
				Namespace:       "ns",
				ResourceVersion: fmt.Sprintf("%d", i),
			},
		}
	}
	if err := cacher.watchCache.Add(makePod(1000)); err != nil {
		t.Errorf("error: %v", err)
	}

	pred := storage.Everything
	pred.AllowWatchBookmarks = true

	w, err := cacher.Watch(context.TODO(), "/pods/ns", storage.ListOptions{
		ResourceVersion: "1000",
		Predicate:       pred,
	})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}

	expectedRV := 2000

	var rcErr error

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			event, ok := <-w.ResultChan()
			if !ok {
				rcErr = errors.New("Unexpected closed channel")
				return
			}
			rv, err := cacher.versioner.ObjectResourceVersion(event.Object)
			if err != nil {
				t.Errorf("failed to parse resource version from %#v: %v", event.Object, err)
			}
			if event.Type == watch.Bookmark && rv == uint64(expectedRV) {
				return
			}
		}
	}()

	// 模拟 progress notify 事件。
	cacher.watchCache.UpdateResourceVersion(strconv.Itoa(expectedRV))

	wg.Wait()
	if rcErr != nil {
		t.Fatal(rcErr)
	}
}

// fakeTimeBudget 是恒定的分发时间预算，避免测试因 CPU 饥饿而误判超时。
type fakeTimeBudget struct{}

func (f *fakeTimeBudget) takeAvailable() time.Duration {
	return 2 * time.Second
}

func (f *fakeTimeBudget) returnUnused(_ time.Duration) {}

// TestStartingResourceVersion 验证 watch 从指定 RV 开始时，收到的事件 RV 一定
// 严格大于起始 RV。
func TestStartingResourceVersion(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	// 确保留有一些用于放慢处理的预算。
	// 使用 fakeTimeBudget 是为了避免本测试在以下情况下偶发失败：
	// 1) 我们总共创建 11 个事件，需要由该 watcher 处理
	// 2) watcher 的通道大小设为 10
	// 3) 如果测试因 CPU 饥饿导致内部 goroutine 没有从通道中取走
	//    这些事件，在把全部时间预算（默认为 100ms）消耗在等待上之后，
	//    我们会直接关闭 watch，从而导致测试失败
	// 使用 fakeTimeBudget 让我们始终有预算可等待，让测试
	// 期间从 ResultCh 取走一些事件。
	//
	// 生产中也可能出现同样的情况，但那种情形下客户端可以恢复 watch。
	// 在本测试中则行不通，因为我们明确要测试的是：对象变更发生在
	// watch 建立之后。
	cacher.dispatchTimeoutBudget = &fakeTimeBudget{}

	makePod := func(i int) *examplev1.Pod {
		return &examplev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "foo",
				Namespace:       "ns",
				Labels:          map[string]string{"foo": strconv.Itoa(i)},
				ResourceVersion: fmt.Sprintf("%d", i),
			},
		}
	}

	if err := cacher.watchCache.Add(makePod(1000)); err != nil {
		t.Errorf("error: %v", err)
	}
	// 将 RV 推进 10。
	startVersion := uint64(1010)

	watcher, err := cacher.Watch(context.TODO(), "/pods/ns/foo", storage.ListOptions{ResourceVersion: strconv.FormatUint(startVersion, 10), Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer watcher.Stop()

	for i := 1; i <= 11; i++ {
		if err := cacher.watchCache.Update(makePod(1000 + i)); err != nil {
			t.Errorf("error: %v", err)
		}
	}

	e, ok := <-watcher.ResultChan()
	if !ok {
		t.Errorf("unexpectedly closed watch")
	}
	object := e.Object
	if co, ok := object.(runtime.CacheableObject); ok {
		object = co.GetObject()
	}
	pod := object.(*examplev1.Pod)
	podRV, err := cacher.versioner.ParseResourceVersion(pod.ResourceVersion)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 事件的 RV 至少应为 rv + 1，因为我们从 rv 开始建立 watch
	if podRV <= startVersion {
		t.Errorf("expected event with resourceVersion of at least %d, got %d", startVersion+1, podRV)
	}
}

// TestDispatchEventWillNotBeBlockedByTimedOutWatcher 验证一个阻塞/超时的慢
// watcher 不会阻塞其他 watcher 的事件分发。
func TestDispatchEventWillNotBeBlockedByTimedOutWatcher(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	// 确保留有一些用于放慢处理的预算。
	// 使用 fakeTimeBudget 是为了避免本测试在以下情况下偶发失败：
	// 1) watch w1 处于阻塞状态，因此一旦它的缓冲区被填满
	//    （10 个条目），我们就会耗尽整个预算
	// 2) 预算每秒才刷新一次，所以在测试中基本不会发生刷新
	// 3) 如果测试因 CPU 饥饿无法从 w2 的 ResultCh 消费事件，
	//    它的缓冲区也可能被填满；而此时我们已没有时间预算
	//    （已在第 1 点耗尽），放入下一个条目就会直接断开该 watch
	// 使用 fakeTimeBudget 让我们始终有预算可等待，让测试
	// 期间从 ResultCh 取走一些事件。
	cacher.dispatchTimeoutBudget = &fakeTimeBudget{}

	makePod := func(i int) *examplev1.Pod {
		return &examplev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("pod-%d", 1000+i),
				Namespace:       "ns",
				ResourceVersion: fmt.Sprintf("%d", 1000+i),
			},
		}
	}
	if err := cacher.watchCache.Add(makePod(0)); err != nil {
		t.Errorf("error: %v", err)
	}

	totalPods := 50

	// 创建一个会被阻塞的 watcher。
	w1, err := cacher.Watch(context.TODO(), "/pods/ns", storage.ListOptions{ResourceVersion: "999", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	defer w1.Stop()

	// 创建快速 watcher，确保它收到全部对象。
	w2, err := cacher.Watch(context.TODO(), "/pods/ns", storage.ListOptions{ResourceVersion: "999", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	defer w2.Stop()

	// 现在向缓存推入大量对象。
	for i := 1; i < totalPods; i++ {
		cacher.watchCache.Add(makePod(i))
	}

	shouldContinue := true
	eventsCount := 0
	for shouldContinue {
		select {
		case event, ok := <-w2.ResultChan():
			if !ok {
				shouldContinue = false
				break
			}
			if event.Type == watch.Added {
				eventsCount++
				if eventsCount == totalPods {
					shouldContinue = false
				}
			}
		case <-time.After(wait.ForeverTestTimeout):
			shouldContinue = false
			w2.Stop()
		}
	}
	if eventsCount != totalPods {
		t.Errorf("watcher is blocked by slower one (count: %d)", eventsCount)
	}
}

// verifyEvents 等待并校验 watch 事件。strictOrder 为 true 时要求顺序完全一致，
// 否则只要求集合匹配。比较前会剥掉 CacheableObject 包装。
func verifyEvents(t *testing.T, w watch.Interface, events []watch.Event, strictOrder bool) {
	_, _, line, _ := goruntime.Caller(1)
	actualEvents := make([]watch.Event, len(events))
	for idx := range events {
		select {
		case event := <-w.ResultChan():
			actualEvents[idx] = event
		case <-time.After(wait.ForeverTestTimeout):
			t.Logf("(called from line %d)", line)
			t.Errorf("Timed out waiting for an event")
		}
	}
	validateEvents := func(expected, actual watch.Event) (bool, []string) {
		errors := []string{}
		if e, a := expected.Type, actual.Type; e != a {
			errors = append(errors, fmt.Sprintf("Expected: %s, got: %s", e, a))
		}
		actualObject := actual.Object
		if co, ok := actualObject.(runtime.CacheableObject); ok {
			actualObject = co.GetObject()
		}
		if e, a := expected.Object, actualObject; !apiequality.Semantic.DeepEqual(e, a) {
			errors = append(errors, fmt.Sprintf("Expected: %#v, got: %#v", e, a))
		}
		return len(errors) == 0, errors
	}

	if len(events) != len(actualEvents) {
		t.Fatalf("unexpected number of events: %d, expected: %d, acutalEvents: %#v, expectedEvents:%#v", len(actualEvents), len(events), actualEvents, events)
	}

	if strictOrder {
		for idx, expectedEvent := range events {
			valid, errors := validateEvents(expectedEvent, actualEvents[idx])
			if !valid {
				t.Logf("(called from line %d)", line)
				for _, err := range errors {
					t.Error(err)
				}
			}
		}
	}
	for _, expectedEvent := range events {
		validated := false
		for _, actualEvent := range actualEvents {
			if validated, _ = validateEvents(expectedEvent, actualEvent); validated {
				break
			}
		}
		if !validated {
			t.Fatalf("Expected: %#v but didn't find", expectedEvent)
		}
	}
}

// TestCachingDeleteEvents 验证 Delete 事件的过滤语义：标签选择器使用对象删除
// 前的标签，因此从匹配集合中移除的对象应收到 Deleted 事件。
func TestCachingDeleteEvents(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	fooPredicate := storage.SelectionPredicate{
		Label: labels.SelectorFromSet(map[string]string{"foo": "true"}),
		Field: fields.Everything(),
	}
	barPredicate := storage.SelectionPredicate{
		Label: labels.SelectorFromSet(map[string]string{"bar": "true"}),
		Field: fields.Everything(),
	}

	createWatch := func(pred storage.SelectionPredicate) watch.Interface {
		w, err := cacher.Watch(context.TODO(), "/pods/ns", storage.ListOptions{ResourceVersion: "999", Predicate: pred})
		if err != nil {
			t.Fatalf("Failed to create watch: %v", err)
		}
		return w
	}

	allEventsWatcher := createWatch(storage.Everything)
	defer allEventsWatcher.Stop()
	fooEventsWatcher := createWatch(fooPredicate)
	defer fooEventsWatcher.Stop()
	barEventsWatcher := createWatch(barPredicate)
	defer barEventsWatcher.Stop()

	makePod := func(labels map[string]string, rv string) *examplev1.Pod {
		return &examplev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "pod",
				Namespace:       "ns",
				Labels:          labels,
				ResourceVersion: rv,
			},
		}
	}
	pod1 := makePod(map[string]string{"foo": "true", "bar": "true"}, "1001")
	pod2 := makePod(map[string]string{"foo": "true"}, "1002")
	pod3 := makePod(map[string]string{}, "1003")
	pod4 := makePod(map[string]string{}, "1004")
	pod1DeletedAt2 := pod1.DeepCopyObject().(*examplev1.Pod)
	pod1DeletedAt2.ResourceVersion = "1002"
	pod2DeletedAt3 := pod2.DeepCopyObject().(*examplev1.Pod)
	pod2DeletedAt3.ResourceVersion = "1003"

	allEvents := []watch.Event{
		{Type: watch.Added, Object: pod1.DeepCopy()},
		{Type: watch.Modified, Object: pod2.DeepCopy()},
		{Type: watch.Modified, Object: pod3.DeepCopy()},
		{Type: watch.Deleted, Object: pod4.DeepCopy()},
	}
	fooEvents := []watch.Event{
		{Type: watch.Added, Object: pod1.DeepCopy()},
		{Type: watch.Modified, Object: pod2.DeepCopy()},
		{Type: watch.Deleted, Object: pod2DeletedAt3.DeepCopy()},
	}
	barEvents := []watch.Event{
		{Type: watch.Added, Object: pod1.DeepCopy()},
		{Type: watch.Deleted, Object: pod1DeletedAt2.DeepCopy()},
	}

	cacher.watchCache.Add(pod1)
	cacher.watchCache.Update(pod2)
	cacher.watchCache.Update(pod3)
	cacher.watchCache.Delete(pod4)

	verifyEvents(t, allEventsWatcher, allEvents, true)
	verifyEvents(t, fooEventsWatcher, fooEvents, true)
	verifyEvents(t, barEventsWatcher, barEvents, true)
}

// testCachingObjects 验证分发出去的对象是 CacheableObject 包装，且多个 watcher
// 看到的内容与 watch cache 历史记录一致。
func testCachingObjects(t *testing.T, watchersCount int) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	dispatchedEvents := []*watchCacheEvent{}
	cacher.watchCache.config.eventHandler = func(event *watchCacheEvent) {
		dispatchedEvents = append(dispatchedEvents, event)
		cacher.processEvent(event)
	}

	watchers := make([]watch.Interface, 0, watchersCount)
	for i := 0; i < watchersCount; i++ {
		w, err := cacher.Watch(context.TODO(), "/pods/ns", storage.ListOptions{ResourceVersion: "1000", Predicate: storage.Everything})
		if err != nil {
			t.Fatalf("Failed to create watch: %v", err)
		}
		defer w.Stop()
		watchers = append(watchers, w)
	}

	makePod := func(name, rv string) *examplev1.Pod {
		return &examplev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				Namespace:       "ns",
				ResourceVersion: rv,
			},
		}
	}
	pod1 := makePod("pod", "1001")
	pod2 := makePod("pod", "1002")
	pod3 := makePod("pod", "1003")

	cacher.watchCache.Add(pod1)
	cacher.watchCache.Update(pod2)
	cacher.watchCache.Delete(pod3)

	// 此时，dispatchedEvents 已完全传播到位。

	verifyEvents := func(w watch.Interface) {
		var event watch.Event
		for index := range dispatchedEvents {
			select {
			case event = <-w.ResultChan():
			case <-time.After(wait.ForeverTestTimeout):
				t.Fatalf("timeout watiching for the event")
			}

			var object runtime.Object
			if _, ok := event.Object.(runtime.CacheableObject); !ok {
				t.Fatalf("Object in %s event should support caching: %#v", event.Type, event.Object)
			}
			object = event.Object.(runtime.CacheableObject).GetObject()

			if event.Type == watch.Deleted {
				resourceVersion, err := cacher.versioner.ObjectResourceVersion(cacher.watchCache.history.cache[index].PrevObject)
				if err != nil {
					t.Fatalf("Failed to parse resource version: %v", err)
				}
				updateResourceVersion(object, cacher.versioner, resourceVersion)
			}

			var e runtime.Object
			switch event.Type {
			case watch.Added, watch.Modified:
				e = cacher.watchCache.history.cache[index].Object
			case watch.Deleted:
				e = cacher.watchCache.history.cache[index].PrevObject
			default:
				t.Errorf("unexpected watch event: %#v", event)
			}
			if a := object; !reflect.DeepEqual(a, e) {
				t.Errorf("event object messed up for %s: %#v, expected: %#v", event.Type, a, e)
			}
		}
	}

	for i := range watchers {
		verifyEvents(watchers[i])
	}
}

// TestCachingObjects 分别覆盖单 watcher 与多 watcher 的对象缓存行为。
func TestCachingObjects(t *testing.T) {
	t.Run("single watcher", func(t *testing.T) { testCachingObjects(t, 1) })
	t.Run("many watcher", func(t *testing.T) { testCachingObjects(t, 3) })
}

// TestCacheIntervalInvalidationStopsWatch 验证历史区间失效（例如因压缩导致
// 索引回退）时，正在重放该区间的 watch 会被关闭，而不是读到不一致的数据。
func TestCacheIntervalInvalidationStopsWatch(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	// 确保留有足够预算应对慢速处理，因为
	// 整个 watch cache 都将通过该区间对外服务，
	// 而事件要到很晚才会从 cacheWatcher 的
	// 输入通道中被取出。
	cacher.dispatchTimeoutBudget.returnUnused(100 * time.Millisecond)

	// 我们自定义一个 index validator：该区间能够成功提供前
	// bufferSize 个元素，但当尝试再次填充缓冲区时，indexValidator
	// 模拟一次失效（invalidation），导致 watch 被关闭，最终我们
	// 实际处理的事件数为 bufferSize，且每个事件类型均为
	// watch.Added。
	valid := true
	invalidateCacheInterval := func() {
		valid = false
	}
	once := sync.Once{}
	indexValidator := func(index int) bool {
		isValid := valid && (index >= cacher.watchCache.history.startIndex)
		once.Do(invalidateCacheInterval)
		return isValid
	}
	cacher.watchCache.config.indexValidator = indexValidator

	makePod := func(i int) *examplev1.Pod {
		return &examplev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("pod-%d", 1000+i),
				Namespace:       "ns",
				ResourceVersion: fmt.Sprintf("%d", 1000+i),
			},
		}
	}

	// 250 是任意取值，关键是元素数量要足够多，
	// 使产生的事件数超过 bufferSize，从而让我们
	// 能够模拟缓存区间失效。
	totalPods := 250
	for i := 0; i < totalPods; i++ {
		err := cacher.watchCache.Add(makePod(i))
		if err != nil {
			t.Errorf("error: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := cacher.Watch(ctx, "/pods/ns", storage.ListOptions{
		ResourceVersion: "999",
		Predicate:       storage.Everything,
	})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	defer w.Stop()

	received := 0
	resChan := w.ResultChan()
	for event := range resChan {
		received++
		t.Logf("event type: %v, events received so far: %d", event.Type, received)
		if event.Type != watch.Added {
			t.Errorf("unexpected event type, expected: %s, got: %s, event: %v", watch.Added, event.Type, event)
		}
	}
	// 由于区间失效后 watch 即被停止，
	// 我们应当恰好处理了 bufferSize 个元素。
	if received != bufferSize {
		t.Errorf("unexpected number of events received, expected: %d, got: %d", bufferSize+1, received)
	}
}

// TestWaitUntilWatchCacheFreshAndForceAllEvents 验证 SendInitialEvents 请求在
// 缓存尚未追平所需 RV 时会主动请求 watch progress；追平后再重试即可拿到初始
// 事件。覆盖指定 RV 与未指定 RV 两种形态。
func TestWaitUntilWatchCacheFreshAndForceAllEvents(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.WatchList: true,
	})
	forceRequestWatchProgressSupport(t)

	scenarios := []struct {
		name               string
		opts               storage.ListOptions
		backingStorage     *cachertesting.MockStorage
		verifyBackingStore func(t *testing.T, s *cachertesting.MockStorage)
	}{
		{
			name: "allowWatchBookmarks=true, sendInitialEvents=true, RV=105",
			opts: storage.ListOptions{
				Predicate: func() storage.SelectionPredicate {
					p := storage.Everything
					p.AllowWatchBookmarks = true
					return p
				}(),
				SendInitialEvents: ptr.To(true),
				ResourceVersion:   "105",
			},
			verifyBackingStore: func(t *testing.T, s *cachertesting.MockStorage) {
				require.NotEqual(t, 0, s.GetRequestWatchProgressCounter(), "expected store.RequestWatchProgressCounter to be > 0. It looks like watch progress wasn't requested!")
			},
		},

		{
			name: "legacy: allowWatchBookmarks=false, sendInitialEvents=true, RV=unset",
			opts: storage.ListOptions{
				Predicate: func() storage.SelectionPredicate {
					p := storage.Everything
					p.AllowWatchBookmarks = false
					return p
				}(),
				SendInitialEvents: ptr.To(true),
			},
			backingStorage: func() *cachertesting.MockStorage {
				hasBeenPrimed := false
				s := &cachertesting.MockStorage{}
				s.GetListFn = func(_ context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
					listAccessor, err := meta.ListAccessor(listObj)
					if err != nil {
						return err
					}
					// 本函数的第一次调用
					// 用于预热 cacher
					if !hasBeenPrimed {
						listAccessor.SetResourceVersion("100")
						hasBeenPrimed = true
						return nil
					}
					listAccessor.SetResourceVersion("105")
					return nil
				}
				s.GetRVFn = func(_ context.Context) (uint64, error) {
					// 本函数的第一次调用
					// 用于预热 cacher
					if !hasBeenPrimed {
						hasBeenPrimed = true
						return 100, nil
					}
					return 105, nil
				}
				return s
			}(),
			verifyBackingStore: func(t *testing.T, s *cachertesting.MockStorage) {
				require.NotEqual(t, 0, s.GetRequestWatchProgressCounter(), "expected store.RequestWatchProgressCounter to be > 0. It looks like watch progress wasn't requested!")
			},
		},

		{
			name: "allowWatchBookmarks=true, sendInitialEvents=true, RV=unset",
			opts: storage.ListOptions{
				Predicate: func() storage.SelectionPredicate {
					p := storage.Everything
					p.AllowWatchBookmarks = true
					return p
				}(),
				SendInitialEvents: ptr.To(true),
			},
			backingStorage: func() *cachertesting.MockStorage {
				hasBeenPrimed := false
				s := &cachertesting.MockStorage{}
				s.GetListFn = func(_ context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
					listAccessor, err := meta.ListAccessor(listObj)
					if err != nil {
						return err
					}
					// 本函数的第一次调用
					// 用于预热 cacher
					if !hasBeenPrimed {
						listAccessor.SetResourceVersion("100")
						hasBeenPrimed = true
						return nil
					}
					listAccessor.SetResourceVersion("105")
					return nil
				}
				s.GetRVFn = func(_ context.Context) (uint64, error) {
					// 本函数的第一次调用
					// 用于预热 cacher
					if !hasBeenPrimed {
						hasBeenPrimed = true
						return 100, nil
					}
					return 105, nil
				}
				return s
			}(),
			verifyBackingStore: func(t *testing.T, s *cachertesting.MockStorage) {
				require.NotEqual(t, 0, s.GetRequestWatchProgressCounter(), "expected store.RequestWatchProgressCounter to be > 0. It looks like watch progress wasn't requested!")
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			var backingStorage *cachertesting.MockStorage
			if scenario.backingStorage != nil {
				backingStorage = scenario.backingStorage
			} else {
				backingStorage = &cachertesting.MockStorage{}
			}
			cacher, _, err := newTestCacher(backingStorage)
			if err != nil {
				t.Fatalf("Couldn't create cacher: %v", err)
			}
			defer cacher.Stop()

			w, err := cacher.Watch(context.Background(), "/pods/ns", scenario.opts)
			require.NoError(t, err, "failed to create watch: %v")
			defer w.Stop()
			var expectedErr *apierrors.StatusError
			if !errors.As(storage.NewTooLargeResourceVersionError(105, 100, resourceVersionTooHighRetrySeconds), &expectedErr) {
				t.Fatalf("Unable to convert NewTooLargeResourceVersionError to apierrors.StatusError")
			}
			verifyEvents(t, w, []watch.Event{
				{
					Type: watch.Error,
					Object: &metav1.Status{
						Status:  metav1.StatusFailure,
						Message: expectedErr.Error(),
						Details: expectedErr.ErrStatus.Details,
						Reason:  metav1.StatusReasonTimeout,
						Code:    504,
					},
				},
			}, true)

			go func(t *testing.T) {
				err := cacher.watchCache.Add(makeTestPodDetails("pod1", 105, "node1", map[string]string{"label": "value1"}))
				if err != nil {
					t.Errorf("failed adding a pod to the watchCache %v", err)
				}
			}(t)
			w, err = cacher.Watch(context.Background(), "/pods/ns", scenario.opts)
			require.NoError(t, err, "failed to create watch: %v")
			defer w.Stop()
			verifyEvents(t, w, []watch.Event{
				{
					Type:   watch.Added,
					Object: makeTestPodDetails("pod1", 105, "node1", map[string]string{"label": "value1"}),
				},
			}, true)
			if scenario.verifyBackingStore != nil {
				scenario.verifyBackingStore(t, backingStorage)
			}
		})
	}
}

// fakeStorage 是只实现 GetList/Watch 的最小存储，用于 benchmark。
type fakeStorage struct {
	pods []example.Pod
	storage.Interface
}

// newObjectStorage 用预置 Pod 构造 fakeStorage。
func newObjectStorage(fakePods []example.Pod) *fakeStorage {
	return &fakeStorage{
		pods: fakePods,
	}
}

// GetList 直接返回预置 Pod 并把 RV 固定为 12345。
func (m fakeStorage) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	podList := listObj.(*example.PodList)
	podList.ListMeta = metav1.ListMeta{ResourceVersion: "12345"}
	podList.Items = m.pods
	return nil
}

// Watch 返回一个永不产生事件的假 watch。
func (m fakeStorage) Watch(_ context.Context, _ string, _ storage.ListOptions) (watch.Interface, error) {
	return cachertesting.NewMockWatch(), nil
}

// BenchmarkCacher_GetList 测量不同选择率下由缓存服务 List 的耗时，对象带有
// 约 2KB 的随机负载以接近真实大小。
func BenchmarkCacher_GetList(b *testing.B) {
	testCases := []struct {
		totalObjectNum  int
		expectObjectNum int
	}{
		{
			totalObjectNum:  5000,
			expectObjectNum: 50,
		},
		{
			totalObjectNum:  5000,
			expectObjectNum: 500,
		},
		{
			totalObjectNum:  5000,
			expectObjectNum: 1000,
		},
		{
			totalObjectNum:  5000,
			expectObjectNum: 2500,
		},
		{
			totalObjectNum:  5000,
			expectObjectNum: 5000,
		},
	}
	for _, tc := range testCases {
		b.Run(
			fmt.Sprintf("totalObjectNum=%d, expectObjectNum=%d", tc.totalObjectNum, tc.expectObjectNum),
			func(b *testing.B) {
				// 创建示例 pod
				fakePods := make([]example.Pod, tc.totalObjectNum, tc.totalObjectNum)
				for i := range fakePods {
					fakePods[i].Namespace = "default"
					fakePods[i].Name = fmt.Sprintf("pod-%d", i)
					fakePods[i].ResourceVersion = strconv.Itoa(i)
					if i%(tc.totalObjectNum/tc.expectObjectNum) == 0 {
						fakePods[i].Spec.NodeName = "node-0"
					}
					data := make([]byte, 1024*2, 1024*2) // 2k 的标签数据
					rand.Read(data)
					fakePods[i].Spec.NodeSelector = map[string]string{
						"key": string(data),
					}
				}

				// 构建测试用 cacher
				store := newObjectStorage(fakePods)
				cacher, _, err := newTestCacher(store)
				if err != nil {
					b.Fatalf("new cacher: %v", err)
				}
				defer cacher.Stop()
				delegator := NewCacheDelegator(cacher, store)
				defer delegator.Stop()

				// 准备 result 和 pred
				parsedField, err := fields.ParseSelector("spec.nodeName=node-0")
				if err != nil {
					b.Fatalf("parse selector: %v", err)
				}
				pred := storage.SelectionPredicate{
					Label: labels.Everything(),
					Field: parsedField,
				}

				// 现在开始基准测试
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result := &example.PodList{}
					err = delegator.GetList(context.TODO(), "/pods/", storage.ListOptions{
						Predicate:       pred,
						Recursive:       true,
						ResourceVersion: "12345",
					}, result)
					if err != nil {
						b.Fatalf("GetList cache: %v", err)
					}
					if len(result.Items) != tc.expectObjectNum {
						b.Fatalf("expect %d but got %d", tc.expectObjectNum, len(result.Items))
					}
				}
			})
	}
}

// benchmarkPods 生成 n 个带约 2KB 随机负载的 Pod，供 benchmark 使用。
func benchmarkPods(n int) []example.Pod {
	pods := make([]example.Pod, n)
	for i := range pods {
		pods[i].Namespace = "default"
		pods[i].Name = fmt.Sprintf("pod-%d", i)
		pods[i].ResourceVersion = strconv.Itoa(i)
		// 给每个 pod 约 2KB 的唯一负载，使 benchmark
		// 处理的对象更接近真实大小。
		data := make([]byte, 1024*2)
		rand.Read(data)
		pods[i].Spec.NodeSelector = map[string]string{
			"key": string(data),
		}
	}
	return pods
}

// newDelegatorWithPods 构造一个已就绪、底层存储返回给定 Pod 的 CacheDelegator。
func newDelegatorWithPods(tb testing.TB, pods []example.Pod) *CacheDelegator {
	store := &cachertesting.MockStorage{
		GetListFn: func(_ context.Context, _ string, _ storage.ListOptions, listObj runtime.Object) error {
			podList := listObj.(*example.PodList)
			podList.ListMeta = metav1.ListMeta{ResourceVersion: "12345"}
			podList.Items = pods
			return nil
		},
		GetRVFn: func(_ context.Context) (uint64, error) { return 12345, nil },
	}
	cacher, _, err := newTestCacher(store)
	if err != nil {
		tb.Fatalf("new cacher: %v", err)
	}
	tb.Cleanup(cacher.Stop)
	delegator := NewCacheDelegator(cacher, store)
	tb.Cleanup(delegator.Stop)
	return delegator
}

// BenchmarkCacher_GetList_AllPods 测量返回全部 1 万个对象的 List 耗时。
func BenchmarkCacher_GetList_AllPods(b *testing.B) {
	totalObjectNum := 10_000
	delegator := newDelegatorWithPods(b, benchmarkPods(totalObjectNum))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := &example.PodList{}
		err := delegator.GetList(context.TODO(), "/pods/", storage.ListOptions{
			Predicate:       storage.Everything,
			Recursive:       true,
			ResourceVersion: "12345",
		}, result)
		if err != nil {
			b.Fatalf("GetList cache: %v", err)
		}
		if len(result.Items) != totalObjectNum {
			b.Fatalf("expect %d but got %d", totalObjectNum, len(result.Items))
		}
	}
}

// TestWatchListIsSynchronisedWhenNoEventsFromStoreReceived 确保即使 cacher
// 没有收到任何事件，bookmark 事件也会被投递。
func TestWatchListIsSynchronisedWhenNoEventsFromStoreReceived(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchList, true)
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	require.NoError(t, err, "failed to create cacher")
	defer cacher.Stop()

	pred := storage.Everything
	pred.AllowWatchBookmarks = true
	opts := storage.ListOptions{
		Predicate:         pred,
		SendInitialEvents: ptr.To(true),
	}
	w, err := cacher.Watch(context.Background(), "/pods/ns", opts)
	require.NoError(t, err, "failed to create watch: %v")
	defer w.Stop()

	verifyEvents(t, w, []watch.Event{
		{Type: watch.Bookmark, Object: &example.Pod{
			ObjectMeta: metav1.ObjectMeta{
				ResourceVersion: "100",
				Annotations:     map[string]string{metav1.InitialEventsAnnotationKey: "true"},
			},
		}},
	}, true)
}

// TestForgetWatcher 验证 watcher 注销的幂等性：重复调用 forgetWatcher 不会
// 破坏内部状态，且不会导致计数器重复减少以外的副作用。
func TestForgetWatcher(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	require.NoError(t, err)
	defer cacher.Stop()

	assertCacherInternalState := func(expectedWatchersCounter, expectedValueWatchersCounter int) {
		cacher.Lock()
		defer cacher.Unlock()

		require.Len(t, cacher.watchers.allWatchers, expectedWatchersCounter)
		require.Len(t, cacher.watchers.valueWatchers, expectedValueWatchersCounter)
	}
	assertCacherInternalState(0, 0)

	var forgetWatcherFn func(bool)
	var forgetCounter int
	forgetWatcherWrapped := func(drainWatcher bool) {
		forgetCounter++
		forgetWatcherFn(drainWatcher)
	}
	w := newCacheWatcher(
		0,
		func(_ string, _ labels.Set, _ fields.Set, _ runtime.Object) bool { return true },
		nil,
		storage.APIObjectVersioner{},
		testingclock.NewFakeClock(time.Now()).Now().Add(2*time.Minute),
		true,
		schema.GroupResource{Resource: "pods"},
		metrics.NewNoopWatcherMetricsObservers(),
		testingclock.NewFakeClock(time.Now()),
		"1",
	)
	forgetWatcherFn = forgetWatcher(cacher, w, 0, namespacedName{}, "", false)
	addWatcher := func(w *cacheWatcher) {
		cacher.Lock()
		defer cacher.Unlock()

		cacher.watchers.addWatcher(w, 0, namespacedName{}, "", false)
	}

	addWatcher(w)
	assertCacherInternalState(1, 0)

	forgetWatcherWrapped(false)
	assertCacherInternalState(0, 0)
	require.Equal(t, 1, forgetCounter)

	forgetWatcherWrapped(false)
	assertCacherInternalState(0, 0)
	require.Equal(t, 2, forgetCounter)
}

// TestGetWatchCacheResourceVersion 测试以下场景：
//
// +-----------------+---------------------+-----------------------+
// | ResourceVersion | AllowWatchBookmarks | SendInitialEvents     |
// +=================+=====================+=======================+
// | Unset           | true/false          | nil/true/false        |
// | 0               | true/false          | nil/true/false        |
// | 95             | true/false          | nil/true/false         |
// +-----------------+---------------------+-----------------------+
// 其中：
// - false 表示某个测试用例把该参数设为 "false"
// - true  表示某个测试用例把该参数设为 "true"
func TestGetWatchCacheResourceVersion(t *testing.T) {
	listOptions := func(allowBookmarks bool, sendInitialEvents *bool, rv string) storage.ListOptions {
		p := storage.Everything
		p.AllowWatchBookmarks = allowBookmarks

		opts := storage.ListOptions{}
		opts.Predicate = p
		opts.SendInitialEvents = sendInitialEvents
		opts.ResourceVersion = rv
		return opts
	}

	scenarios := []struct {
		name string
		opts storage.ListOptions

		expectedWatchResourceVersion int
	}{
		// +-----------------+---------------------+-----------------------+
		// | ResourceVersion | AllowWatchBookmarks | SendInitialEvents     |
		// +=================+=====================+=======================+
		// | Unset           | true/false          | nil/true/false        |
		// +-----------------+---------------------+-----------------------+
		{
			name: "RV=unset, allowWatchBookmarks=true, sendInitialEvents=nil",
			opts: listOptions(true, nil, ""),
			// 预期 RV 为 0，因为 https://github.com/kubernetes/kubernetes/pull/123935 已将此类请求恢复为从 watch cache 服务。
			// 当 WatchFromStorageWithoutResourceVersion 设为 true 时，此值应改为 100。
			expectedWatchResourceVersion: 0,
		},
		{
			name:                         "RV=unset, allowWatchBookmarks=true, sendInitialEvents=true",
			opts:                         listOptions(true, ptr.To(true), ""),
			expectedWatchResourceVersion: 100,
		},
		{
			name:                         "RV=unset, allowWatchBookmarks=true, sendInitialEvents=false",
			opts:                         listOptions(true, ptr.To(false), ""),
			expectedWatchResourceVersion: 100,
		},
		{
			name: "RV=unset, allowWatchBookmarks=false, sendInitialEvents=nil",
			opts: listOptions(false, nil, ""),
			// 预期 RV 为 0，因为 https://github.com/kubernetes/kubernetes/pull/123935 已将此类请求恢复为从 watch cache 服务。
			// 当 WatchFromStorageWithoutResourceVersion 设为 true 时，此值应改为 100。
			expectedWatchResourceVersion: 0,
		},
		{
			name:                         "RV=unset, allowWatchBookmarks=false, sendInitialEvents=true, legacy",
			opts:                         listOptions(false, ptr.To(true), ""),
			expectedWatchResourceVersion: 100,
		},
		{
			name:                         "RV=unset, allowWatchBookmarks=false, sendInitialEvents=false",
			opts:                         listOptions(false, ptr.To(false), ""),
			expectedWatchResourceVersion: 100,
		},
		// +-----------------+---------------------+-----------------------+
		// | ResourceVersion | AllowWatchBookmarks | SendInitialEvents     |
		// +=================+=====================+=======================+
		// | 0               | true/false          | nil/true/false        |
		// +-----------------+---------------------+-----------------------+
		{
			name:                         "RV=0, allowWatchBookmarks=true, sendInitialEvents=nil",
			opts:                         listOptions(true, nil, "0"),
			expectedWatchResourceVersion: 0,
		},
		{
			name:                         "RV=0, allowWatchBookmarks=true, sendInitialEvents=true",
			opts:                         listOptions(true, ptr.To(true), "0"),
			expectedWatchResourceVersion: 0,
		},
		{
			name:                         "RV=0, allowWatchBookmarks=true, sendInitialEvents=false",
			opts:                         listOptions(true, ptr.To(false), "0"),
			expectedWatchResourceVersion: 0,
		},
		{
			name:                         "RV=0, allowWatchBookmarks=false, sendInitialEvents=nil",
			opts:                         listOptions(false, nil, "0"),
			expectedWatchResourceVersion: 0,
		},
		{
			name:                         "RV=0, allowWatchBookmarks=false, sendInitialEvents=true",
			opts:                         listOptions(false, ptr.To(true), "0"),
			expectedWatchResourceVersion: 0,
		},
		{
			name:                         "RV=0, allowWatchBookmarks=false, sendInitialEvents=false",
			opts:                         listOptions(false, ptr.To(false), "0"),
			expectedWatchResourceVersion: 0,
		},
		// +-----------------+---------------------+-----------------------+
		// | ResourceVersion | AllowWatchBookmarks | SendInitialEvents     |
		// +=================+=====================+=======================+
		// | 95             | true/false          | nil/true/false         |
		// +-----------------+---------------------+-----------------------+
		{
			name:                         "RV=95, allowWatchBookmarks=true, sendInitialEvents=nil",
			opts:                         listOptions(true, nil, "95"),
			expectedWatchResourceVersion: 95,
		},
		{
			name:                         "RV=95, allowWatchBookmarks=true, sendInitialEvents=true",
			opts:                         listOptions(true, ptr.To(true), "95"),
			expectedWatchResourceVersion: 95,
		},
		{
			name:                         "RV=95, allowWatchBookmarks=true, sendInitialEvents=false",
			opts:                         listOptions(true, ptr.To(false), "95"),
			expectedWatchResourceVersion: 95,
		},
		{
			name:                         "RV=95, allowWatchBookmarks=false, sendInitialEvents=nil",
			opts:                         listOptions(false, nil, "95"),
			expectedWatchResourceVersion: 95,
		},
		{
			name:                         "RV=95, allowWatchBookmarks=false, sendInitialEvents=true",
			opts:                         listOptions(false, ptr.To(true), "95"),
			expectedWatchResourceVersion: 95,
		},
		{
			name:                         "RV=95, allowWatchBookmarks=false, sendInitialEvents=false",
			opts:                         listOptions(false, ptr.To(false), "95"),
			expectedWatchResourceVersion: 95,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			backingStorage := &cachertesting.MockStorage{}
			cacher, _, err := newTestCacher(backingStorage)
			require.NoError(t, err, "couldn't create cacher")
			defer cacher.Stop()

			parsedResourceVersion := 0
			if len(scenario.opts.ResourceVersion) > 0 {
				parsedResourceVersion, err = strconv.Atoi(scenario.opts.ResourceVersion)
				require.NoError(t, err)
			}

			actualResourceVersion, err := cacher.getWatchCacheResourceVersion(context.TODO(), uint64(parsedResourceVersion), scenario.opts)
			require.NoError(t, err)
			require.Equal(t, uint64(scenario.expectedWatchResourceVersion), actualResourceVersion, "received unexpected ResourceVersion")
		})
	}
}

// TestGetBookmarkAfterResourceVersionLockedFunc 测试以下场景：
//
// +-----------------+---------------------+-----------------------+
// | ResourceVersion | AllowWatchBookmarks | SendInitialEvents     |
// +=================+=====================+=======================+
// | Unset           | true/false          | nil/true/false        |
// | 0               | true/false          | nil/true/false        |
// | 95             | true/false          | nil/true/false         |
// +-----------------+---------------------+-----------------------+
// 其中：
// - false 表示某个测试用例把该参数设为 "false"
// - true  表示某个测试用例把该参数设为 "true"
func TestGetBookmarkAfterResourceVersionLockedFunc(t *testing.T) {
	listOptions := func(allowBookmarks bool, sendInitialEvents *bool, rv string) storage.ListOptions {
		p := storage.Everything
		p.AllowWatchBookmarks = allowBookmarks

		opts := storage.ListOptions{}
		opts.Predicate = p
		opts.SendInitialEvents = sendInitialEvents
		opts.ResourceVersion = rv
		return opts
	}

	scenarios := []struct {
		name                      string
		opts                      storage.ListOptions
		requiredResourceVersion   int
		watchCacheResourceVersion int

		expectedBookmarkResourceVersion int
	}{
		// +-----------------+---------------------+-----------------------+
		// | ResourceVersion | AllowWatchBookmarks | SendInitialEvents     |
		// +=================+=====================+=======================+
		// | Unset           | true/false          | nil/true/false        |
		// +-----------------+---------------------+-----------------------+
		{
			name:                            "RV=unset, allowWatchBookmarks=true, sendInitialEvents=nil",
			opts:                            listOptions(true, nil, ""),
			requiredResourceVersion:         100,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=unset, allowWatchBookmarks=true, sendInitialEvents=true",
			opts:                            listOptions(true, ptr.To(true), ""),
			requiredResourceVersion:         100,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 100,
		},
		{
			name:                            "RV=unset, allowWatchBookmarks=true, sendInitialEvents=false",
			opts:                            listOptions(true, ptr.To(false), ""),
			requiredResourceVersion:         100,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=unset, allowWatchBookmarks=false, sendInitialEvents=nil",
			opts:                            listOptions(false, nil, ""),
			requiredResourceVersion:         100,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=unset, allowWatchBookmarks=false, sendInitialEvents=true",
			opts:                            listOptions(false, ptr.To(true), ""),
			requiredResourceVersion:         100,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=unset, allowWatchBookmarks=false, sendInitialEvents=false",
			opts:                            listOptions(false, ptr.To(false), ""),
			requiredResourceVersion:         100,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		// +-----------------+---------------------+-----------------------+
		// | ResourceVersion | AllowWatchBookmarks | SendInitialEvents     |
		// +=================+=====================+=======================+
		// | 0               | true/false          | nil/true/false        |
		// +-----------------+---------------------+-----------------------+
		{
			name:                            "RV=0, allowWatchBookmarks=true, sendInitialEvents=nil",
			opts:                            listOptions(true, nil, "0"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=0, allowWatchBookmarks=true, sendInitialEvents=true",
			opts:                            listOptions(true, ptr.To(true), "0"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 99,
		},
		{
			name:                            "RV=0, allowWatchBookmarks=true, sendInitialEvents=false",
			opts:                            listOptions(true, ptr.To(false), "0"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=0, allowWatchBookmarks=false, sendInitialEvents=nil",
			opts:                            listOptions(false, nil, "0"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=0, allowWatchBookmarks=false, sendInitialEvents=true",
			opts:                            listOptions(false, ptr.To(true), "0"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=0, allowWatchBookmarks=false, sendInitialEvents=false",
			opts:                            listOptions(false, ptr.To(false), "0"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		// +-----------------+---------------------+-----------------------+
		// | ResourceVersion | AllowWatchBookmarks | SendInitialEvents     |
		// +=================+=====================+=======================+
		// | 95             | true/false          | nil/true/false         |
		// +-----------------+---------------------+-----------------------+
		{
			name:                            "RV=95, allowWatchBookmarks=true, sendInitialEvents=nil",
			opts:                            listOptions(true, nil, "95"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=95, allowWatchBookmarks=true, sendInitialEvents=true",
			opts:                            listOptions(true, ptr.To(true), "95"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 95,
		},
		{
			name:                            "RV=95, allowWatchBookmarks=true, sendInitialEvents=false",
			opts:                            listOptions(true, ptr.To(false), "95"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=95, allowWatchBookmarks=false, sendInitialEvents=nil",
			opts:                            listOptions(false, nil, "95"),
			requiredResourceVersion:         100,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=95, allowWatchBookmarks=false, sendInitialEvents=true",
			opts:                            listOptions(false, ptr.To(true), "95"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
		{
			name:                            "RV=95, allowWatchBookmarks=false, sendInitialEvents=false",
			opts:                            listOptions(false, ptr.To(false), "95"),
			requiredResourceVersion:         0,
			watchCacheResourceVersion:       99,
			expectedBookmarkResourceVersion: 0,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			backingStorage := &cachertesting.MockStorage{}
			cacher, _, err := newTestCacher(backingStorage)
			require.NoError(t, err, "couldn't create cacher")

			defer cacher.Stop()

			cacher.watchCache.UpdateResourceVersion(fmt.Sprintf("%d", scenario.watchCacheResourceVersion))
			parsedResourceVersion := 0
			if len(scenario.opts.ResourceVersion) > 0 {
				parsedResourceVersion, err = strconv.Atoi(scenario.opts.ResourceVersion)
				require.NoError(t, err)
			}

			getBookMarkFn, err := cacher.getBookmarkAfterResourceVersionLockedFunc(uint64(parsedResourceVersion), uint64(scenario.requiredResourceVersion), scenario.opts)
			require.NoError(t, err)
			cacher.watchCache.RLock()
			defer cacher.watchCache.RUnlock()
			getBookMarkResourceVersion := getBookMarkFn()
			require.Equal(t, uint64(scenario.expectedBookmarkResourceVersion), getBookMarkResourceVersion, "received unexpected ResourceVersion")
		})
	}
}

// TestComputeListLimit 验证计算实际分页上限的规则：RV=0 属于旧式语义，
// 不应用 limit（返回 0），其余情况沿用请求的 limit。
func TestComputeListLimit(t *testing.T) {
	scenarios := []struct {
		name          string
		opts          storage.ListOptions
		expectedLimit int64
	}{
		{
			name: "limit is zero",
			opts: storage.ListOptions{
				Predicate: storage.SelectionPredicate{
					Limit: 0,
				},
			},
			expectedLimit: 0,
		},
		{
			name: "limit is positive, RV is unset",
			opts: storage.ListOptions{
				Predicate: storage.SelectionPredicate{
					Limit: 1,
				},
				ResourceVersion: "",
			},
			expectedLimit: 1,
		},
		{
			name: "limit is positive, RV = 100",
			opts: storage.ListOptions{
				Predicate: storage.SelectionPredicate{
					Limit: 1,
				},
				ResourceVersion: "100",
			},
			expectedLimit: 1,
		},
		{
			name: "legacy case: limit is positive, RV = 0",
			opts: storage.ListOptions{
				Predicate: storage.SelectionPredicate{
					Limit: 1,
				},
				ResourceVersion: "0",
			},
			expectedLimit: 0,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			actualLimit := computeListLimit(scenario.opts)
			if actualLimit != scenario.expectedLimit {
				t.Errorf("computeListLimit returned = %v, expected %v", actualLimit, scenario.expectedLimit)
			}
		})
	}
}

// TODO(p0lyn0mial): forceRequestWatchProgressSupport 初始化存储层，
// 使依赖 storage.RequestWatchProgress 的测试能够通过
//
// 将来我们可以提供一个只在该测试持续期间内设置该特性的函数
//
// 该辅助函数会启动一个真实的 etcd 并等待特性探测完成，因此只应在确实需要
// watch progress 的测试中调用。
func forceRequestWatchProgressSupport(t *testing.T) {
	if etcdfeature.DefaultFeatureSupportChecker.Supports(storage.RequestWatchProgress) {
		return
	}

	server, _ := newEtcdTestStorage(t, etcd3testing.PathPrefix())
	defer server.Terminate(t)
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, wait.ForeverTestTimeout, true, func(_ context.Context) (bool, error) {
		return etcdfeature.DefaultFeatureSupportChecker.Supports(storage.RequestWatchProgress), nil
	}); err != nil {
		t.Fatalf("failed to wait for required %v storage feature to initialize", storage.RequestWatchProgress)
	}
}

// TestListIndexer 验证 List 的索引选择：当字段选择器命中已建立的索引字段时
// 应使用该索引（f:<field>），否则不使用索引。
func TestListIndexer(t *testing.T) {
	ctx, cacher, terminate := testSetup(t, withNodeNameAndNamespaceIndex)
	t.Cleanup(terminate)
	tests := []struct {
		name               string
		requestedNamespace string
		recursive          bool
		fieldSelector      fields.Selector
		indexFields        []string
		expectIndex        string
	}{
		{
			name:          "request without namespace, without field selector",
			recursive:     true,
			fieldSelector: fields.Everything(),
		},
		{
			name:          "request without namespace, field selector with metadata.namespace",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("metadata.namespace=namespace"),
		},
		{
			name:          "request without namespace, field selector with spec.nodename",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("spec.nodeName=node"),
			indexFields:   []string{"spec.nodeName"},
			expectIndex:   "f:spec.nodeName",
		},
		{
			name:          "request without namespace, field selector with spec.nodename to filter out",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("spec.nodeName!=node"),
			indexFields:   []string{"spec.nodeName"},
		},
		{
			name:               "request with namespace, without field selector",
			requestedNamespace: "namespace",
			recursive:          true,
			fieldSelector:      fields.Everything(),
		},
		{
			name:               "request with namespace, field selector with matched metadata.namespace",
			requestedNamespace: "namespace",
			recursive:          true,
			fieldSelector:      fields.ParseSelectorOrDie("metadata.namespace=namespace"),
		},
		{
			name:               "request with namespace, field selector with non-matched metadata.namespace",
			requestedNamespace: "namespace",
			recursive:          true,
			fieldSelector:      fields.ParseSelectorOrDie("metadata.namespace=namespace"),
		},
		{
			name:               "request with namespace, field selector with spec.nodename",
			requestedNamespace: "namespace",
			recursive:          true,
			fieldSelector:      fields.ParseSelectorOrDie("spec.nodeName=node"),
			indexFields:        []string{"spec.nodeName"},
			expectIndex:        "f:spec.nodeName",
		},
		{
			name:               "request with namespace, field selector with spec.nodename to filter out",
			requestedNamespace: "namespace",
			recursive:          true,
			fieldSelector:      fields.ParseSelectorOrDie("spec.nodeName!=node"),
			indexFields:        []string{"spec.nodeName"},
		},
		{
			name:          "request without namespace, field selector with metadata.name",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("metadata.name=name"),
		},
		{
			name:      "request without namespace, field selector with metadata.name and metadata.namespace",
			recursive: true,
			fieldSelector: fields.SelectorFromSet(fields.Set{
				"metadata.name":      "name",
				"metadata.namespace": "namespace",
			}),
		},
		{
			name:      "request without namespace, field selector with metadata.name and spec.nodeName",
			recursive: true,
			fieldSelector: fields.SelectorFromSet(fields.Set{
				"metadata.name": "name",
				"spec.nodeName": "node",
			}),
			indexFields: []string{"spec.nodeName"},
			expectIndex: "f:spec.nodeName",
		},
		{
			name:      "request without namespace, field selector with metadata.name, and with spec.nodeName to filter out watch",
			recursive: true,
			fieldSelector: fields.AndSelectors(
				fields.ParseSelectorOrDie("spec.nodeName!=node"),
				fields.SelectorFromSet(fields.Set{"metadata.name": "name"}),
			),
			indexFields: []string{"spec.nodeName"},
		},
		{
			name:               "request with namespace, with field selector metadata.name",
			requestedNamespace: "namespace",
			fieldSelector:      fields.ParseSelectorOrDie("metadata.name=name"),
		},
		{
			name:               "request with namespace, with field selector metadata.name and metadata.namespace",
			requestedNamespace: "namespace",
			fieldSelector: fields.SelectorFromSet(fields.Set{
				"metadata.name":      "name",
				"metadata.namespace": "namespace",
			}),
		},
		{
			name:               "request with namespace, with field selector metadata.name, metadata.namespace and spec.nodename",
			requestedNamespace: "namespace",
			fieldSelector: fields.SelectorFromSet(fields.Set{
				"metadata.name":      "name",
				"metadata.namespace": "namespace",
				"spec.nodeName":      "node",
			}),
			indexFields: []string{"spec.nodeName"},
		},
		{
			name:               "request with namespace, with field selector metadata.name, metadata.namespace, and with spec.nodename to filter out",
			requestedNamespace: "namespace",
			fieldSelector: fields.AndSelectors(
				fields.ParseSelectorOrDie("spec.nodeName!=node"),
				fields.SelectorFromSet(fields.Set{"metadata.name": "name", "metadata.namespace": "namespace"}),
			),
			indexFields: []string{"spec.nodeName"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred := storagetesting.CreatePodPredicate(tt.fieldSelector, true, tt.indexFields)
			_, usedIndex, err := cacher.cacher.watchCache.WaitUntilFreshAndGetList(ctx, "/pods/"+tt.requestedNamespace, storage.ListOptions{Predicate: pred, Recursive: tt.recursive})
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if usedIndex != tt.expectIndex {
				t.Errorf("Index doesn't match, expected: %q, got: %q", tt.expectIndex, usedIndex)
			}
		})
	}
}

// TestRetryAfterForUnreadyCache 验证缓存未就绪时返回 429，并携带按未就绪
// 时长计算的 RetryAfterSeconds，指导客户端退避重试。
func TestRetryAfterForUnreadyCache(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	clock := testingclock.NewFakeClock(time.Now())
	cacher, _, err := newTestCacherWithoutSyncing(backingStorage, clock)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()
	if err = cacher.ready.wait(context.Background()); err != nil {
		t.Fatalf("Unexpected error waiting for the cache to be ready")
	}

	cacher.ready.setError(nil)
	clock.Step(14 * time.Second)

	opts := storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
	}
	result := &example.PodList{}
	delegator := NewCacheDelegator(cacher, backingStorage)
	defer delegator.Stop()
	err = delegator.GetList(context.TODO(), "/pods/ns", opts, result)

	if !apierrors.IsTooManyRequests(err) {
		t.Fatalf("Unexpected GetList error: %v", err)
	}
	var statusError apierrors.APIStatus
	if !errors.As(err, &statusError) {
		t.Fatalf("Unexpected error: %v, expected apierrors.APIStatus", err)
	}
	if statusError.Status().Details == nil {
		t.Fatalf("Expected to get status details, got none")
	}
	if statusError.Status().Details.RetryAfterSeconds != 2 {
		t.Fatalf("Unexpected retry after: %v", statusError.Status().Details.RetryAfterSeconds)
	}
}

// TestWatchListSemanticsSimple 是 WatchList 语义的冒烟测试：开启特性门控后
// cacher 能正常就绪；dummy store 不支持 WatchList，因此不构造响应。
func TestWatchListSemanticsSimple(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchList, true)
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)

	// dummyStore 不支持 WatchList 语义，
	// 因此无需准备响应。
	backingStorage := &cachertesting.MockStorage{}
	clock := testingclock.NewFakeClock(time.Now())
	cacher, _, err := newTestCacherWithoutSyncing(backingStorage, clock)
	if err != nil {
		t.Fatalf("couldn't create cacher: %v", err)
	}
	defer cacher.Stop()
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	if err = cacher.ready.wait(ctx); err != nil {
		t.Fatalf("error waiting for the cache to be ready, err: %v", err)
	}
}

// selectorIncludingUID 构造一个分片 selector，其范围包含 uid 的哈希值。
func selectorIncludingUID(uid string) sharding.Selector {
	hash := "0x" + sharding.HashField(uid)
	return sharding.NewSelector(sharding.ShardRangeRequirement{
		Key:   "object.metadata.uid",
		Start: hash,
		End:   incrementHex(hash),
	})
}

// selectorExcludingUID 构造一个分片 selector，其范围不包含 uid 的哈希值。
func selectorExcludingUID(uid string) sharding.Selector {
	hash := "0x" + sharding.HashField(uid)
	return sharding.NewSelector(sharding.ShardRangeRequirement{
		Key:   "object.metadata.uid",
		Start: "0x0000000000000000",
		End:   hash,
	})
}

// incrementHex 把十六进制字符串加一（保留 "0x" 前缀），用于构造紧邻的 shard
// 范围上界。
func incrementHex(hex string) string {
	b := []byte(hex)
	for i := len(b) - 1; i >= 2; i-- {
		if b[i] < '9' {
			b[i]++
			return string(b)
		} else if b[i] == '9' {
			b[i] = 'a'
			return string(b)
		} else if b[i] < 'f' {
			b[i]++
			return string(b)
		}
		b[i] = '0'
	}
	return "0x10000000000000000"
}

// makeExamplePod 构造带 UID 与标签的测试 Pod。
func makeExamplePod(name, namespace, uid string, podLabels map[string]string) *example.Pod {
	return &example.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(uid),
			Labels:    podLabels,
		},
	}
}

// TestFilterWithAttrsAndPrefixFunction_ShardMatchAndLabels 验证分片与标签同时
// 匹配时事件通过过滤。
func TestFilterWithAttrsAndPrefixFunction_ShardMatchAndLabels(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)
	uid := "test-uid-1"
	pod := makeExamplePod("pod1", "default", uid, map[string]string{"app": "web"})
	gr := schema.GroupResource{Resource: "pods"}

	pred := storage.SelectionPredicate{
		Label:         labels.SelectorFromSet(labels.Set{"app": "web"}),
		Field:         fields.Everything(),
		ShardSelector: selectorIncludingUID(uid),
		GetAttrs:      storage.DefaultNamespaceScopedAttr,
	}
	filter := filterWithAttrsAndPrefixFunction("/pods/", pred, gr)

	if !filter("/pods/default/pod1", labels.Set{"app": "web"}, fields.Set{"metadata.name": "pod1", "metadata.namespace": "default"}, pod) {
		t.Error("expected filter to match: shard matches and labels match")
	}
}

// TestFilterWithAttrsAndPrefixFunction_ShardMatchButLabelMismatch 验证分片匹配
// 但标签不匹配时事件被拒绝。
func TestFilterWithAttrsAndPrefixFunction_ShardMatchButLabelMismatch(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)
	uid := "test-uid-2"
	pod := makeExamplePod("pod2", "default", uid, map[string]string{"app": "api"})
	gr := schema.GroupResource{Resource: "pods"}

	pred := storage.SelectionPredicate{
		Label:         labels.SelectorFromSet(labels.Set{"app": "web"}),
		Field:         fields.Everything(),
		ShardSelector: selectorIncludingUID(uid),
		GetAttrs:      storage.DefaultNamespaceScopedAttr,
	}
	filter := filterWithAttrsAndPrefixFunction("/pods/", pred, gr)

	if filter("/pods/default/pod2", labels.Set{"app": "api"}, fields.Set{"metadata.name": "pod2", "metadata.namespace": "default"}, pod) {
		t.Error("expected filter to reject: shard matches but labels don't")
	}
}

// TestFilterWithAttrsAndPrefixFunction_ShardMismatch 验证对象不在分片范围内时
// 即使标签匹配也会被拒绝。
func TestFilterWithAttrsAndPrefixFunction_ShardMismatch(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)
	uid := "test-uid-3"
	pod := makeExamplePod("pod3", "default", uid, map[string]string{"app": "web"})
	gr := schema.GroupResource{Resource: "pods"}

	pred := storage.SelectionPredicate{
		Label:         labels.Everything(),
		Field:         fields.Everything(),
		ShardSelector: selectorExcludingUID(uid),
		GetAttrs:      storage.DefaultNamespaceScopedAttr,
	}
	filter := filterWithAttrsAndPrefixFunction("/pods/", pred, gr)

	if filter("/pods/default/pod3", labels.Set{"app": "web"}, fields.Set{"metadata.name": "pod3", "metadata.namespace": "default"}, pod) {
		t.Error("expected filter to reject: shard does not match")
	}
}

// TestFilterWithAttrsAndPrefixFunction_NilShardSelector 验证未配置分片选择器时
// 不做分片过滤，全部放行。
func TestFilterWithAttrsAndPrefixFunction_NilShardSelector(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)
	pod := makeExamplePod("pod4", "default", "uid-4", map[string]string{"app": "web"})
	gr := schema.GroupResource{Resource: "pods"}

	pred := storage.SelectionPredicate{
		Label:    labels.SelectorFromSet(labels.Set{"app": "web"}),
		Field:    fields.Everything(),
		GetAttrs: storage.DefaultNamespaceScopedAttr,
	}
	filter := filterWithAttrsAndPrefixFunction("/pods/", pred, gr)

	if !filter("/pods/default/pod4", labels.Set{"app": "web"}, fields.Set{"metadata.name": "pod4", "metadata.namespace": "default"}, pod) {
		t.Error("expected filter to match: nil ShardSelector should pass everything through")
	}
}

// TestFilterWithAttrsAndPrefixFunction_EverythingSelector 验证 Everything
// 选择器覆盖整个哈希空间，所有对象都通过。
func TestFilterWithAttrsAndPrefixFunction_EverythingSelector(t *testing.T) {
	pod := makeExamplePod("pod5", "default", "uid-5", nil)
	gr := schema.GroupResource{Resource: "pods"}

	pred := storage.SelectionPredicate{
		Label:         labels.Everything(),
		Field:         fields.Everything(),
		ShardSelector: sharding.Everything(),
		GetAttrs:      storage.DefaultNamespaceScopedAttr,
	}
	filter := filterWithAttrsAndPrefixFunction("/pods/", pred, gr)

	if !filter("/pods/default/pod5", labels.Set{}, fields.Set{"metadata.name": "pod5", "metadata.namespace": "default"}, pod) {
		t.Error("expected filter to match: Everything() selector should pass all objects")
	}
}

// TestFilterWithAttrsAndPrefixFunction_WrongPrefix 验证键前缀不匹配的对象直接
// 被拒绝，避免把其他资源的事件误投递给该 watcher。
func TestFilterWithAttrsAndPrefixFunction_WrongPrefix(t *testing.T) {
	uid := "test-uid-6"
	pod := makeExamplePod("pod6", "default", uid, nil)
	gr := schema.GroupResource{Resource: "pods"}

	pred := storage.SelectionPredicate{
		Label:         labels.Everything(),
		Field:         fields.Everything(),
		ShardSelector: selectorIncludingUID(uid),
		GetAttrs:      storage.DefaultNamespaceScopedAttr,
	}
	filter := filterWithAttrsAndPrefixFunction("/pods/", pred, gr)

	if filter("/configmaps/default/cm1", nil, nil, pod) {
		t.Error("expected filter to reject: key prefix doesn't match")
	}
}

// TestFilterWithAttrsAndPrefixFunction_ShardErrorDropsEvent 验证无法解析分片
// 字段（如 runtime.Unknown）时事件被丢弃（fail closed），而不是放行。
func TestFilterWithAttrsAndPrefixFunction_ShardErrorDropsEvent(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)
	obj := &runtime.Unknown{}
	gr := schema.GroupResource{Resource: "pods"}

	pred := storage.SelectionPredicate{
		Label: labels.Everything(),
		Field: fields.Everything(),
		ShardSelector: sharding.NewSelector(sharding.ShardRangeRequirement{
			Key:   "object.metadata.uid",
			Start: "0x0000000000000000",
			End:   "0x10000000000000000",
		}),
		GetAttrs: storage.DefaultNamespaceScopedAttr,
	}
	filter := filterWithAttrsAndPrefixFunction("/pods/", pred, gr)

	if filter("/pods/default/pod-err", nil, nil, obj) {
		t.Error("expected filter to reject: shard matching should fail on unstructured object")
	}
}

// TestFilterWithAttrsAndPrefixFunction_NamespaceSharding 验证按 namespace 字段
// 哈希的分片选择器能正确匹配对象。
func TestFilterWithAttrsAndPrefixFunction_NamespaceSharding(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)
	ns := "my-namespace"
	pod := makeExamplePod("pod-ns", ns, "some-uid", nil)

	nsHash := "0x" + sharding.HashField(ns)
	sel := sharding.NewSelector(sharding.ShardRangeRequirement{
		Key:   "object.metadata.namespace",
		Start: nsHash,
		End:   incrementHex(nsHash),
	})

	gr := schema.GroupResource{Resource: "pods"}
	pred := storage.SelectionPredicate{
		Label:         labels.Everything(),
		Field:         fields.Everything(),
		ShardSelector: sel,
		GetAttrs:      storage.DefaultNamespaceScopedAttr,
	}
	filter := filterWithAttrsAndPrefixFunction("/pods/", pred, gr)

	if !filter("/pods/my-namespace/pod-ns", labels.Set{}, fields.Set{"metadata.name": "pod-ns", "metadata.namespace": ns}, pod) {
		t.Error("expected filter to match: namespace-based shard selector should match")
	}
}

// TestFilterWithAttrsAndPrefixFunction_NamespaceShardingMismatch 验证 namespace
// 哈希不落在分片范围内时对象被拒绝。
func TestFilterWithAttrsAndPrefixFunction_NamespaceShardingMismatch(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)
	ns := "other-namespace"
	pod := makeExamplePod("pod-ns2", ns, "some-uid-2", nil)

	targetHash := "0x" + sharding.HashField("my-namespace")
	sel := sharding.NewSelector(sharding.ShardRangeRequirement{
		Key:   "object.metadata.namespace",
		Start: targetHash,
		End:   incrementHex(targetHash),
	})

	gr := schema.GroupResource{Resource: "pods"}
	pred := storage.SelectionPredicate{
		Label:         labels.Everything(),
		Field:         fields.Everything(),
		ShardSelector: sel,
		GetAttrs:      storage.DefaultNamespaceScopedAttr,
	}
	filter := filterWithAttrsAndPrefixFunction("/pods/", pred, gr)

	if filter("/pods/other-namespace/pod-ns2", labels.Set{}, fields.Set{"metadata.name": "pod-ns2", "metadata.namespace": ns}, pod) {
		t.Error("expected filter to reject: namespace hash doesn't fall in shard range")
	}
}

// --- Cacher.GetList 的 sharding 单元测试 ---

// selectorExcludingOnlyUID 构造一个分片 selector，覆盖除 uid 哈希之外的
// 整个哈希空间，因此它匹配除该对象之外的所有对象。
func selectorExcludingOnlyUID(uid string) sharding.Selector {
	hash := "0x" + sharding.HashField(uid)
	return sharding.NewSelector(
		sharding.ShardRangeRequirement{
			Key:   "object.metadata.uid",
			Start: "0x0000000000000000",
			End:   hash,
		},
		sharding.ShardRangeRequirement{
			Key:   "object.metadata.uid",
			Start: incrementHex(hash),
			End:   "0x10000000000000000",
		},
	)
}

// shardTestPods 生成 n 个 UID 连续、RV 递增的 Pod，供分片 List 测试使用。
func shardTestPods(n int) []example.Pod {
	pods := make([]example.Pod, n)
	for i := range pods {
		pods[i].Namespace = "default"
		pods[i].Name = fmt.Sprintf("pod-%d", i)
		pods[i].UID = types.UID(fmt.Sprintf("uid-%d", i))
		pods[i].ResourceVersion = strconv.Itoa(i + 1)
	}
	return pods
}

// TestGetListWithShardedListAndWatch 验证 Cacher.GetList 的分片语义：nil 或
// Everything 选择器返回全部；命中分片时只返回范围内的对象，并在列表上设置
// ShardInfo 以便调用方感知结果不完整。
func TestGetListWithShardedListAndWatch(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)
	delegator := newDelegatorWithPods(t, shardTestPods(3))

	testCases := map[string]struct {
		shardSelector   sharding.Selector
		expectPodNames  []string
		expectShardInfo bool
	}{
		"nil shard selector returns all items": {
			shardSelector:  nil,
			expectPodNames: []string{"pod-0", "pod-1", "pod-2"},
		},
		"empty shard selector returns all items": {
			shardSelector:  sharding.Everything(),
			expectPodNames: []string{"pod-0", "pod-1", "pod-2"},
		},
		"shard selector returns only the matching shard": {
			shardSelector:   selectorIncludingUID("uid-1"),
			expectPodNames:  []string{"pod-1"},
			expectShardInfo: true,
		},
		"shard selector excludes items outside the shard": {
			shardSelector:   selectorExcludingOnlyUID("uid-1"),
			expectPodNames:  []string{"pod-0", "pod-2"},
			expectShardInfo: true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := &example.PodList{}
			err := delegator.GetList(context.TODO(), "/pods/", storage.ListOptions{
				Predicate: storage.SelectionPredicate{
					Label:         labels.Everything(),
					Field:         fields.Everything(),
					ShardSelector: tc.shardSelector,
				},
				Recursive:       true,
				ResourceVersion: "12345",
			}, result)
			if err != nil {
				t.Fatalf("GetList failed: %v", err)
			}
			gotNames := make([]string, 0, len(result.Items))
			for _, pod := range result.Items {
				gotNames = append(gotNames, pod.Name)
			}
			if !reflect.DeepEqual(tc.expectPodNames, gotNames) {
				t.Errorf("expected pods %v, got %v", tc.expectPodNames, gotNames)
			}
			if tc.expectShardInfo {
				if result.ShardInfo == nil {
					t.Error("expected ShardInfo to be set on the list")
				} else if want := tc.shardSelector.String(); result.ShardInfo.Selector != want {
					t.Errorf("expected ShardInfo selector %q, got %q", want, result.ShardInfo.Selector)
				}
			} else if result.ShardInfo != nil {
				t.Errorf("expected no ShardInfo on the list, got %+v", result.ShardInfo)
			}
		})
	}
}
