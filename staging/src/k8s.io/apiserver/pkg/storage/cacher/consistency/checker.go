/*
Copyright The Kubernetes Authors.

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

package consistency

import (
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	"k8s.io/klog/v2"
)

var (
	// CheckPeriod is the period of checking consistency between etcd and cache.
	// 5 minutes were proposed to match the default compaction period. It's magnitute higher than
	// List latency SLO (30 seconds) and timeout (1 minute).
	CheckPeriod = 5 * time.Minute
	// PanicOnCacheInconsistency enables the consistency checking mechanism for cache.
	// Based on KUBE_WATCHCACHE_CONSISTENCY_CHECKER environment variable.
	PanicOnCacheInconsistency = false
	// checkerListPageSize is the checker's request chunk size of list operations.
	// 分页可避免一次性拉取超大资源集合；该值与 etcd 的默认页大小对齐。
	checkerListPageSize = int64(10000)
)

// init 从环境变量读取开关，便于在集群里临时启用而不必改配置。
// 解析失败时保持默认 false——一致性检查是诊断手段，不应因配置笔误阻断启动。
func init() {
	PanicOnCacheInconsistency, _ = strconv.ParseBool(os.Getenv("KUBE_WATCHCACHE_CONSISTENCY_CHECKER"))
}

// NewChecker 构造检查器；cacher 与 etcd 都只需 GetList 能力，便于测试替换。
func NewChecker(resourcePrefix string, groupResource schema.GroupResource, newListFunc func() runtime.Object, cacher cacher, etcd getLister) *Checker {
	return &Checker{
		groupResource:  groupResource,
		resourcePrefix: resourcePrefix,
		newListFunc:    newListFunc,
		cacher:         cacher,
		etcd:           etcd,
	}
}

// Checker 周期性比对 watch cache 与 etcd 的全量内容，用于发现缓存丢事件或乱序。
type Checker struct {
	resourcePrefix string
	groupResource  schema.GroupResource
	newListFunc    func() runtime.Object

	cacher cacher
	etcd   getLister
}

// cacher 是检查器对 watch cache 的最小依赖：既能 list，也能上报就绪与一致性状态。
type cacher interface {
	getLister
	Ready() bool
	MarkConsistent(bool)
}

// getLister 抽象"按 ListOptions 列出某前缀下对象"的能力，cache 与 etcd 都实现它。
type getLister interface {
	GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error
}

// Run 启动周期检查。
//
// 首次检查前先随机等待一个周期：所有 apiserver 实例若同时启动检查，会在同一时刻
// 对 etcd 发起全量 list，形成尖峰。jitter 让检查自然错开。
func (c Checker) Run(stopCh <-chan struct{}) {
	klog.V(3).InfoS("Cache consistency check start", "group", c.groupResource.Group, "resource", c.groupResource.Resource)
	jitter := 0.5 // Period between [interval, interval * (1.0 + jitter)]
	sliding := true
	// wait.JitterUntilWithContext starts work immediately, so wait first.
	select {
	case <-time.After(wait.Jitter(CheckPeriod, jitter)):
	case <-stopCh:
	}
	wait.JitterUntilWithContext(wait.ContextForChannel(stopCh), c.check, CheckPeriod, jitter, sliding)
}

// check 执行一次比对并上报结果。
//
// 不一致时把 cacher 标记为不可信，让后续 list 退回 etcd 路径；
// 只有在显式开启开关时才 panic，默认保持进程存活并依赖指标告警。
func (c *Checker) check(ctx context.Context) {
	digests, err := c.CalculateDigests(ctx)
	if err != nil {
		klog.ErrorS(err, "Cache consistency check error", "group", c.groupResource.Group, "resource", c.groupResource.Resource)
		metrics.StorageConsistencyCheckTotal.WithLabelValues(c.groupResource.Group, c.groupResource.Resource, "error").Inc()
		return
	}
	if digests.CacheDigest == digests.EtcdDigest {
		klog.V(3).InfoS("Cache consistency check passed", "group", c.groupResource.Group, "resource", c.groupResource.Resource, "resourceVersion", digests.ResourceVersion, "digest", digests.CacheDigest)
		metrics.StorageConsistencyCheckTotal.WithLabelValues(c.groupResource.Group, c.groupResource.Resource, "success").Inc()
		c.cacher.MarkConsistent(true)
		return
	}
	klog.ErrorS(nil, "Cache consistency check failed", "group", c.groupResource.Group, "resource", c.groupResource.Resource, "resourceVersion", digests.ResourceVersion, "etcdDigest", digests.EtcdDigest, "cacheDigest", digests.CacheDigest, "diffDetail", digests.DiffDetail)
	metrics.StorageConsistencyCheckTotal.WithLabelValues(c.groupResource.Group, c.groupResource.Resource, "failure").Inc()
	if PanicOnCacheInconsistency {
		panic(fmt.Sprintf("Cache consistency check failed, group: %q, resource: %q, resourceVersion: %q, etcdDigest: %q, cacheDigest: %q, diffDetail: %v", c.groupResource.Group, c.groupResource.Resource, digests.ResourceVersion, digests.EtcdDigest, digests.CacheDigest, digests.DiffDetail))
	}
	c.cacher.MarkConsistent(false)
}

// CalculateDigests 计算 cache 与 etcd 两侧的内容摘要，并顺带定位第一处差异。
//
// 关键点：先从 cache 取一个 resourceVersion，再让 etcd 用精确版本读取。
// 否则两次读取跨越不同时间点，正常写入也会表现为"不一致"，产生大量误报。
func (c *Checker) CalculateDigests(ctx context.Context) (*Digest, error) {
	if !c.cacher.Ready() {
		return nil, fmt.Errorf("cache is not ready")
	}
	// variables for tracking the diff in consistency checker
	cacheItems := []namespaceNameRV{}
	var foundDiff *diffDetail
	var etcdIndex int

	// 缓存侧一次性读取，同时记录顺序，供后续逐项比对使用。
	cacheDigest, cacheResourceVersion, err := c.calculateStoreDigest(ctx, c.cacher, "0", 0, func(obj metav1.Object) {
		// collect items from cacher's list
		cacheItems = append(cacheItems, namespaceNameRV{Namespace: obj.GetNamespace(), Name: obj.GetName(), RV: obj.GetResourceVersion()})
	})
	if err != nil {
		return nil, fmt.Errorf("failed calculating cache digest: %w", err)
	}
	etcdDigest, etcdResourceVersion, err := c.calculateStoreDigest(ctx, c.etcd, cacheResourceVersion, checkerListPageSize, func(obj metav1.Object) {
		// compare the item in etcd's list against cacheItems which is cacher's list
		// 找到第一处差异即停止记录，并释放 cacheItems 以便 GC 回收。
		if foundDiff != nil {
			return
		}
		etcdItem := namespaceNameRV{Namespace: obj.GetNamespace(), Name: obj.GetName(), RV: obj.GetResourceVersion()}
		if len(cacheItems) <= etcdIndex {
			foundDiff = &diffDetail{Index: etcdIndex, EtcdItem: &etcdItem}
			cacheItems = nil // don't need it any more and nil it to allow GC to collect it
			return
		}
		if cacheItem := cacheItems[etcdIndex]; cacheItem != etcdItem {
			foundDiff = &diffDetail{Index: etcdIndex, EtcdItem: &etcdItem, CacheItem: &cacheItem}
			cacheItems = nil // don't need it any more and nil it to allow GC to collect it
			return
		}
		etcdIndex += 1
	})
	if err != nil {
		return nil, fmt.Errorf("failed calculating etcd digest: %w", err)
	}
	// etcd 已遍历完但 cache 还有剩余项，说明 cache 多出了对象。
	if len(cacheItems) > etcdIndex {
		cacheItem := cacheItems[etcdIndex]
		foundDiff = &diffDetail{Index: etcdIndex, CacheItem: &cacheItem}
	}
	// 两侧读取的版本必须一致，否则摘要比较没有意义，直接报错而不是误判为不一致。
	if cacheResourceVersion != etcdResourceVersion {
		return nil, fmt.Errorf("etcd returned different resource version then expected, cache: %q, etcd: %q", cacheResourceVersion, etcdResourceVersion)
	}
	return &Digest{
		ResourceVersion: cacheResourceVersion,
		CacheDigest:     cacheDigest,
		EtcdDigest:      etcdDigest,
		DiffDetail:      foundDiff,
	}, nil
}

// namespaceNameRV 是比对用的最小对象标识：命名空间、名称、资源版本。
type namespaceNameRV struct {
	Namespace string
	Name      string
	RV        string
}

// diffDetail 描述第一处差异的位置与两侧取值；某一侧为 nil 表示该侧缺少元素。
type diffDetail struct {
	Index     int
	CacheItem *namespaceNameRV
	EtcdItem  *namespaceNameRV
}

// Digest 汇总一次比对的结果。
type Digest struct {
	ResourceVersion string
	CacheDigest     string
	EtcdDigest      string
	DiffDetail      *diffDetail
}

// calculateStoreDigest 遍历 store 的全部对象并计算 FNV 摘要。
//
// resourceVersion 为 "0" 表示"从当前最新开始"，用于缓存侧；否则用 Exact 匹配，
// 保证 etcd 读到的是与缓存相同版本的数据。分页续读时清空 RV 约束，
// 因为续传 token 已隐含了版本。
func (c *Checker) calculateStoreDigest(ctx context.Context, store getLister, resourceVersion string, limit int64, metaVisitor func(objectMeta metav1.Object)) (digest, rv string, err error) {
	opts := storage.ListOptions{
		Recursive:       true,
		Predicate:       storage.Everything,
		ResourceVersion: resourceVersion,
	}
	opts.Predicate.Limit = limit
	if resourceVersion == "0" {
		opts.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan
	} else {
		opts.ResourceVersionMatch = metav1.ResourceVersionMatchExact
	}
	h := fnv.New64()
	for {
		resp := c.newListFunc()
		err = store.GetList(ctx, c.resourcePrefix, opts, resp)
		if err != nil {
			return "", "", err
		}
		err = addListToDigest(h, resp, metaVisitor)
		if err != nil {
			return "", "", err
		}
		list, err := meta.ListAccessor(resp)
		if err != nil {
			return "", "", err
		}
		// 首页的 resourceVersion 即为本次快照版本，后续页沿用同一快照。
		if resourceVersion == "0" {
			resourceVersion = list.GetResourceVersion()
		}
		continueToken := list.GetContinue()
		if continueToken == "" {
			return fmt.Sprintf("%x", h.Sum64()), resourceVersion, nil
		}
		opts.Predicate.Continue = continueToken
		opts.ResourceVersion = ""
		opts.ResourceVersionMatch = ""
	}
}

// addListToDigest 把列表中的每个对象依次写入摘要，并回调 metaVisitor。
func addListToDigest(h hash.Hash64, list runtime.Object, metaVisitor func(objectMeta metav1.Object)) error {
	return meta.EachListItem(list, func(obj runtime.Object) error {
		objectMeta, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		err = addObjectToDigest(h, objectMeta)
		if err != nil {
			return err
		}
		metaVisitor(objectMeta)
		return nil
	})
}

// addObjectToDigest 以 namespace/name/resourceVersion 拼接后写入摘要。
// 用 "/" 分隔可避免不同字段拼接产生碰撞（如 ns="a/b" 与 name 的组合歧义）。
func addObjectToDigest(h hash.Hash64, objectMeta metav1.Object) error {
	_, err := h.Write([]byte(objectMeta.GetNamespace()))
	if err != nil {
		return err
	}
	_, err = h.Write([]byte("/"))
	if err != nil {
		return err
	}
	_, err = h.Write([]byte(objectMeta.GetName()))
	if err != nil {
		return err
	}
	_, err = h.Write([]byte("/"))
	if err != nil {
		return err
	}
	_, err = h.Write([]byte(objectMeta.GetResourceVersion()))
	if err != nil {
		return err
	}
	return nil
}
