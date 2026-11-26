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

package resourcequota

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/utils/lru"
)

// TestLRUCacheLookup 验证 GetQuotas 的两级查找：先查 liveLookupCache，
// 未命中或已过期时回退到 informer/client。用负 TTL 模拟过期条目。
func TestLRUCacheLookup(t *testing.T) {
	namespace := "foo"
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: namespace,
		},
	}

	testcases := []struct {
		description   string
		cacheInput    []*corev1.ResourceQuota
		clientInput   []runtime.Object
		ttl           time.Duration
		namespace     string
		expectedQuota *corev1.ResourceQuota
	}{
		{
			description:   "object is found via cache",
			cacheInput:    []*corev1.ResourceQuota{resourceQuota},
			ttl:           30 * time.Second,
			namespace:     namespace,
			expectedQuota: resourceQuota,
		},
		{
			description:   "object is outdated and not found with client",
			cacheInput:    []*corev1.ResourceQuota{resourceQuota},
			ttl:           -30 * time.Second,
			namespace:     namespace,
			expectedQuota: nil,
		},
		{
			description:   "object is outdated but is found with client",
			cacheInput:    []*corev1.ResourceQuota{resourceQuota},
			clientInput:   []runtime.Object{resourceQuota},
			ttl:           -30 * time.Second,
			namespace:     namespace,
			expectedQuota: resourceQuota,
		},
		{
			description:   "object does not exist in cache and is not found with client",
			cacheInput:    []*corev1.ResourceQuota{resourceQuota},
			ttl:           30 * time.Second,
			expectedQuota: nil,
		},
		{
			description:   "object does not exist in cache and is found with client",
			cacheInput:    []*corev1.ResourceQuota{},
			clientInput:   []runtime.Object{resourceQuota},
			namespace:     namespace,
			expectedQuota: resourceQuota,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.description, func(t *testing.T) {
			liveLookupCache := lru.New(1)
			kubeClient := fake.NewSimpleClientset(tc.clientInput...)
			informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

			accessor, _ := newQuotaAccessor()
			accessor.client = kubeClient
			accessor.lister = informerFactory.Core().V1().ResourceQuotas().Lister()
			accessor.hasSynced = func() bool { return false }
			accessor.liveLookupCache = liveLookupCache

			for _, q := range tc.cacheInput {
				quota := q
				liveLookupCache.Add(quota.Namespace, liveLookupEntry{expiry: time.Now().Add(tc.ttl), items: []*corev1.ResourceQuota{quota}})
			}

			quotas, err := accessor.GetQuotas(tc.namespace)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if tc.expectedQuota != nil {
				if count := len(quotas); count != 1 {
					t.Fatalf("Expected 1 object but got %d", count)
				}

				if !reflect.DeepEqual(quotas[0], *tc.expectedQuota) {
					t.Errorf("Retrieved object does not match")
				}
				return
			}

			if count := len(quotas); count > 0 {
				t.Errorf("Expected 0 objects but got %d", count)
			}
		})
	}
}

// TestGetQuotas 确保任意时刻至多有一个到 apiserver 的 LIST 调用正在进行，
// 防止 #22422 描述的问题重现。
//
// 并发压测：同命名空间的 10 个并发调用应被 singleflight 合并为 1 次 LIST；
// 不同命名空间互不共享，因此各 1 次。删除缓存后再调用应再触发一次 LIST。
func TestGetQuotas(t *testing.T) {
	var (
		testNamespace1              = "test-a"
		testNamespace2              = "test-b"
		listCallCountTestNamespace1 int64
		listCallCountTestNamespace2 int64
	)
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
	}

	resourceQuotas := []*corev1.ResourceQuota{resourceQuota}

	kubeClient := &fake.Clientset{}
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	accessor, _ := newQuotaAccessor()
	accessor.client = kubeClient
	accessor.lister = informerFactory.Core().V1().ResourceQuotas().Lister()
	accessor.hasSynced = func() bool { return false }

	kubeClient.AddReactor("list", "resourcequotas", func(action core.Action) (bool, runtime.Object, error) {
		switch action.GetNamespace() {
		case testNamespace1:
			atomic.AddInt64(&listCallCountTestNamespace1, 1)
		case testNamespace2:
			atomic.AddInt64(&listCallCountTestNamespace2, 1)
		default:
			t.Error("unexpected namespace")
		}

		resourceQuotaList := &corev1.ResourceQuotaList{
			ListMeta: metav1.ListMeta{
				ResourceVersion: fmt.Sprintf("%d", len(resourceQuotas)),
			},
		}
		for i, quota := range resourceQuotas {
			quota.ResourceVersion = fmt.Sprintf("%d", i)
			quota.Namespace = action.GetNamespace()
			resourceQuotaList.Items = append(resourceQuotaList.Items, *quota)
		}
		// 让 handler 变慢，使并发调用真正走 singleflight 合并
		time.Sleep(time.Second)
		return true, resourceQuotaList, nil
	})

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(2)
		// 模拟缓存失效后的并发调用
		go func() {
			defer wg.Done()
			quotas, err := accessor.GetQuotas(testNamespace1)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if len(quotas) != len(resourceQuotas) {
				t.Errorf("Expected %d resource quotas, got %d", len(resourceQuotas), len(quotas))
			}
			for _, q := range quotas {
				if q.Namespace != testNamespace1 {
					t.Errorf("Expected %s namespace, got %s", testNamespace1, q.Namespace)
				}
			}
		}()

		// 不同命名空间对应不同的 group key，不与第一个命名空间共享结果
		go func() {
			defer wg.Done()
			quotas, err := accessor.GetQuotas(testNamespace2)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if len(quotas) != len(resourceQuotas) {
				t.Errorf("Expected %d resource quotas, got %d", len(resourceQuotas), len(quotas))
			}
			for _, q := range quotas {
				if q.Namespace != testNamespace2 {
					t.Errorf("Expected %s namespace, got %s", testNamespace2, q.Namespace)
				}
			}
		}()
	}

	// 此处等待所有 goroutine 完成
	wg.Wait()
	// 同命名空间的调用会被阻塞并汇聚到 singleflight group；
	// 共两组不同命名空间的调用，因此只有 2 次。
	if listCallCountTestNamespace1 != 1 {
		t.Errorf("Expected 1 resource quota call, got %d", listCallCountTestNamespace1)
	}
	if listCallCountTestNamespace2 != 1 {
		t.Errorf("Expected 1 resource quota call, got %d", listCallCountTestNamespace2)
	}

	// 使缓存失效
	accessor.liveLookupCache.Remove(testNamespace1)
	quotas, err := accessor.GetQuotas(testNamespace1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(quotas) != len(resourceQuotas) {
		t.Errorf("Expected %d resource quotas, got %d", len(resourceQuotas), len(quotas))
	}

	if listCallCountTestNamespace1 != 2 {
		t.Errorf("Expected 2 resource quota call, got %d", listCallCountTestNamespace1)
	}
	if listCallCountTestNamespace2 != 1 {
		t.Errorf("Expected 1 resource quota call, got %d", listCallCountTestNamespace2)
	}
}
