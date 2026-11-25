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

package resourcequota

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/singleflight"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/utils/lru"
)

// QuotaAccessor 把 get/set 逻辑从评估器其余部分抽象出来：可以是测试桩、直通封装，
// 或最常见的一串串扰缓存，使 evaluator 无需感知 informer、LRU、singleflight 等细节。
type QuotaAccessor interface {
	// UpdateQuotaStatus 用于持久化最终状态，应写入持久存储；
	// 返回错误表示写入未成功完成。
	UpdateQuotaStatus(newQuota *corev1.ResourceQuota) error

	// GetQuotas 返回给定 namespace 下所有可能的配额
	GetQuotas(namespace string) ([]corev1.ResourceQuota, error)
}

// quotaAccessor 是 QuotaAccessor 的默认实现，组合 informer 缓存与若干层
// “防冲突”缓存来降低对 apiserver 的读写压力。
type quotaAccessor struct {
	client kubernetes.Interface

	// lister 从共享 informer 缓存中列取配额对象
	lister corev1listers.ResourceQuotaLister

	// hasSynced 表示 lister 是否已完成初始同步
	hasSynced func() bool

	// liveLookups 缓存最近几次实时查询结果，摊薄反复查询失败的成本。
	// 以应对缓存滞后：缓存未命中或无结果时实时查询该 namespace 的真实结果。
	// 记录查询结果，重复请求时无需频繁实时查询。
	liveLookupCache *lru.Cache
	group           singleflight.Group
	liveTTL         time.Duration
	// updatedQuotas 缓存我们更新过的配额，用于在连续评估同一配额文档时
	// 读到“真正最新”的版本；这之所以可行，是因为同一资源的 etcd
	// resourceVersion 可按整数比较。改动前：22 次更新、12 次冲突；
	updatedQuotas *lru.Cache
}

// newQuotaAccessor 创建符合 QuotaAccessor 接口的对象，用于获取配额。
//
// client/lister 在此处故意留空，等 SetInternalKubeClientSet 与
// SetInternalKubeInformerFactory 注入后再使用，符合准入插件的初始化顺序。
func newQuotaAccessor() (*quotaAccessor, error) {
	liveLookupCache := lru.New(100)
	updatedCache := lru.New(100)

	// client 与 lister 将在 SetInternalKubeClientSet 和 SetInternalKubeInformerFactory 被调用时注入
	return &quotaAccessor{
		liveLookupCache: liveLookupCache,
		liveTTL:         time.Duration(30 * time.Second),
		updatedQuotas:   updatedCache,
	}, nil
}

// UpdateQuotaStatus 通过 status 子资源持久化配额状态，并把服务端返回的最新对象
// 写入 updatedQuotas，供后续评估读到“真正最新”的 resourceVersion。
func (e *quotaAccessor) UpdateQuotaStatus(newQuota *corev1.ResourceQuota) error {
	updatedQuota, err := e.client.CoreV1().ResourceQuotas(newQuota.Namespace).UpdateStatus(context.TODO(), newQuota, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	// 以 namespace/name 为键缓存写回结果；连续的评估可以据此避免重复写冲突。
	key := newQuota.Namespace + "/" + newQuota.Name
	e.updatedQuotas.Add(key, updatedQuota)
	return nil
}

var etcdVersioner = storage.APIObjectVersioner{}

// checkCache 将传入的配额与旁路缓存中的值比较并返回较新者；缓存过期时删除旧条目。
// 这之所以可行，是因为 etcd resourceVersion 是单调递增的整数 可能滞后于刚写入的结果，因此读取时以 resourceVersion
// 较大者为准；缓存落后于调用方时立即淘汰，避免脏数据长期滞留。
func (e *quotaAccessor) checkCache(quota *corev1.ResourceQuota) *corev1.ResourceQuota {
	key := quota.Namespace + "/" + quota.Name
	uncastCachedQuota, ok := e.updatedQuotas.Get(key)
	if !ok {
		return quota
	}
	cachedQuota := uncastCachedQuota.(*corev1.ResourceQuota)

	// 相等说明缓存并不更新，删除以免后续继续走这条无效路径。
	if etcdVersioner.CompareResourceVersion(quota, cachedQuota) >= 0 {
		e.updatedQuotas.Remove(key)
		return quota
	}
	return cachedQuota
}

// GetQuotas 返回指定 namespace 下所有配额；优先使用 informer 缓存，缓存尚未
// 同步时回退到实时查询，并用 singleflight 合并同一 namespace 的并发请求。
func (e *quotaAccessor) GetQuotas(namespace string) ([]corev1.ResourceQuota, error) {
	// 判断该 namespace 是否存在配额
	// 没有配额就无需做任何事
	items, err := e.lister.ResourceQuotas(namespace).List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("error resolving quota: %v", err)
	}

	// 未同步的 lister 无数据时查实时 LRU；仍未命中则实时查询并预热缓存。
	//
	// 缓存未同步时“空结果”不能当作没有配额，否则会漏掉配额校验；
	// 实时查询结果带 TTL 缓存，避免每次请求都直连 apiserver。
	if len(items) == 0 && !e.hasSynced() {
		lruItemObj, ok := e.liveLookupCache.Get(namespace)
		if !ok || lruItemObj.(liveLookupEntry).expiry.Before(time.Now()) {
			// 使用 singleflight.Group 避免重复请求淹没 apiserver，
			// 详见 #22422。
			lruItemObj, err, _ = e.group.Do(namespace, func() (interface{}, error) {
				liveList, err := e.client.CoreV1().ResourceQuotas(namespace).List(context.TODO(), metav1.ListOptions{})
				if err != nil {
					return nil, err
				}
				// 只拷贝指针切片，避免在缓存中持有整个 List 结构。
				newEntry := liveLookupEntry{expiry: time.Now().Add(e.liveTTL)}
				for i := range liveList.Items {
					newEntry.items = append(newEntry.items, &liveList.Items[i])
				}
				e.liveLookupCache.Add(namespace, newEntry)
				return newEntry, nil
			})
			if err != nil {
				return nil, err
			}
		}
		lruEntry := lruItemObj.(liveLookupEntry)
		for i := range lruEntry.items {
			items = append(items, lruEntry.items[i])
		}
	}

	resourceQuotas := []corev1.ResourceQuota{}
	for i := range items {
		quota := items[i]
		// 用最近一次写回结果覆盖可能滞后的 informer 副本。
		quota = e.checkCache(quota)
		// 始终复制一份：接下来会随意改动它，绝不能修改原始对象
		resourceQuotas = append(resourceQuotas, *quota)
	}

	return resourceQuotas, nil
}
