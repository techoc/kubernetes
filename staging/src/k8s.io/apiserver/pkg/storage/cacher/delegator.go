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

// Package cacher 实现基于 watch 缓存的 storage.Interface：读请求在缓存就绪且满足一致性
// 要求时走缓存，否则回退到底层 storage；写请求始终转交底层 storage。
package cacher

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/consistency"
	"k8s.io/apiserver/pkg/storage/cacher/delegator"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	"k8s.io/apiserver/pkg/storage/cacher/store"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
)

// NewCacheDelegator 把 Cacher 与底层 storage 组合成一个 storage.Interface，由它负责读写路径分流。
// 启用 DetectCacheInconsistency 或全局 panic 开关时，同时启动后台一致性检查器。
func NewCacheDelegator(cacher *Cacher, storage storage.Interface) *CacheDelegator {
	d := &CacheDelegator{
		cacher:  cacher,
		storage: storage,
		stopCh:  make(chan struct{}),
	}
	// 仅在需要检测缓存不一致时启动 checker，避免常态下的额外读写开销。
	if utilfeature.DefaultFeatureGate.Enabled(features.DetectCacheInconsistency) || consistency.PanicOnCacheInconsistency {
		d.checker = consistency.NewChecker(cacher.resourcePrefix, cacher.groupResource, cacher.newListFunc, cacher, storage)
		d.wg.Add(1)
		// checker 的生命周期由 stopCh 控制，wg 保证 Stop 返回时协程已退出。
		go func() {
			defer d.wg.Done()
			d.checker.Run(d.stopCh)
		}()
	}
	return d
}

// CacheDelegator 持有缓存与底层存储，并按请求语义决定读缓存的路径。
type CacheDelegator struct {
	cacher  *Cacher              // 提供缓存读取与 watch 能力。
	storage storage.Interface    // 缓存背后的权威存储，写操作与部分读回退都走这里。
	checker *consistency.Checker // 可选，仅在启用一致性检测时非空。

	wg       sync.WaitGroup // 跟踪 checker 协程。
	stopOnce sync.Once      // 保证 stopCh 只关闭一次。
	stopCh   chan struct{}
}

// 编译期断言：CacheDelegator 实现 storage.Interface。
var _ storage.Interface = (*CacheDelegator)(nil)

// Versioner 直接透传底层存储的版本器。
func (c *CacheDelegator) Versioner() storage.Versioner {
	return c.storage.Versioner()
}

// Create 是写操作，不经过缓存，直接交给底层存储。
func (c *CacheDelegator) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	return c.storage.Create(ctx, key, obj, out, ttl)
}

// GetCurrentResourceVersion 透传底层存储；缓存本身不掌握全局资源版本。
func (c *CacheDelegator) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	return c.storage.GetCurrentResourceVersion(ctx)
}

// EnableResourceSizeEstimation 透传底层存储的容量估算能力。
func (c *CacheDelegator) EnableResourceSizeEstimation(keys storage.KeysFunc) error {
	return c.storage.EnableResourceSizeEstimation(keys)
}

// Delete 转交底层存储执行删除，并尽量把缓存中的最新对象作为“当前版本”传入，
// 供底层做前置条件校验；缓存不可用时回退为不带建议对象的路径。
func (c *CacheDelegator) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	// 忽略传入的建议对象，尝试把当前从缓存读出的对象版本传递下去。
	// 缓存读取失败不应阻塞写；记录日志后回退到不带建议对象的删除。
	if elem, exists, err := c.cacher.watchCache.storage.GetByKey(key); err != nil {
		klog.Errorf("GetByKey returned error: %v", err)
	} else if exists {
		// 对对象做 DeepCopy，因为我们在序列化当前对象时会修改 resource version。
		// 序列化会修改 resourceVersion，必须深拷贝，避免污染缓存中共享的对象。
		currObj := elem.(*store.Element).Object.DeepCopyObject()
		return c.storage.Delete(ctx, key, out, preconditions, validateDeletion, currObj, opts)
	}
	// If we couldn't get the object, fallback to no-suggestion.
	return c.storage.Delete(ctx, key, out, preconditions, validateDeletion, nil, opts)
}

// Watch 把 watch 请求交给 Cacher，由它在缓存与底层存储之间选择事件来源。
func (c *CacheDelegator) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	// if the watch-list feature wasn't set and the resourceVersion is unset
	// ensure that the rv from which the watch is being served, is the latest
	// one. "latest" is ensured by serving the watch from
	// the underlying storage.
	//
	// 按我们的校验逻辑这不该发生，但这里还是做得绝对安全一些：
	// 特性未开启时把 sendingInitialEvents 关掉。
	// 未启用 WatchList 时不能发送初始事件；这里做兜底，防止上游漏校验。
	if !utilfeature.DefaultFeatureGate.Enabled(features.WatchList) && opts.SendInitialEvents != nil {
		opts.SendInitialEvents = nil
	}
	return c.cacher.Watch(ctx, key, opts)
}

// Get 按 resourceVersion 语义在缓存与底层存储之间分流。
func (c *CacheDelegator) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	if opts.ResourceVersion == "" {
		// 未指定 resourceVersion 时，由底层存储提供服务（保持向后兼容）。
		// 未指定 RV 时无法判断缓存是否足够新，按历史行为走底层存储。
		return c.storage.Get(ctx, key, opts, objPtr)
	}

	if !c.cacher.Ready() {
		// Cache 尚未初始化时，按 https://kep.k8s.io/4568 的描述把 delegator 的 Get
		// 请求委派给存储。
		// 缓存尚未完成初始同步，读取必须回退底层存储。
		return c.storage.Get(ctx, key, opts, objPtr)
	}
	// 指定了 resourceVersion 时由缓存提供服务，并保证返回的值至少与该
	// resourceVersion 一样新。
	// 先校验 RV 格式，避免把非法值带入缓存路径。
	if _, err := c.cacher.versioner.ParseResourceVersion(opts.ResourceVersion); err != nil {
		return err
	}
	return c.cacher.Get(ctx, key, opts, objPtr)
}

// GetList 是最复杂的分流路径：先判断是否必须委托底层，再尝试从缓存出列表，
// 并在缓存已压缩或 RV 过高时按 feature gate 选择回退或拒绝。
func (c *CacheDelegator) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	_, _, err := storage.ValidateListOptions(c.cacher.resourcePrefix, c.cacher.versioner, opts)
	if err != nil {
		// 选项非法则立即失败，避免不同后端给出不一致的行为。
		return err
	}
	result, err := delegator.ShouldDelegateList(opts, c.cacher)
	if err != nil {
		return err
	}
	if result.ShouldDelegate {
		// 分页、快照等要求必须由底层存储满足，不能由缓存代劳。
		return c.storage.GetList(ctx, key, opts, listObj)
	}

	if _, err := c.cacher.versioner.ParseResourceVersion(opts.ResourceVersion); err != nil {
		return err
	}

	if !c.cacher.Ready() && shouldDelegateListOnNotReadyCache(opts) {
		// Cacher 尚未初始化时，按 https://kep.k8s.io/4568 的描述把 delegator 的 List
		// 请求委派给存储。
		// 缓存未就绪且请求是简单的 limit 查询，回退底层以免阻塞。
		return c.storage.GetList(ctx, key, opts, listObj)
	}
	err = c.cacher.GetList(ctx, key, opts, listObj)
	success := "true"
	fallback := "false"
	if err != nil {
		if errors.IsResourceExpired(err) && utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
			// 缓存已压缩到请求 RV 之后，从底层存储重读；缓存列表允许这种回退。
			return c.storage.GetList(ctx, key, opts, listObj)
		}
		if result.ConsistentRead {
			// IsTooLargeResourceVersion 出现在请求的 RV 高于缓存当前 RV、
			// 且缓存在超时时间内未能追平时。
			// 请求 RV 超过缓存进度且缓存未在超时内追上时：
			// 要么明确拒绝让客户端重试，要么回退底层存储，由 feature gate 决定。
			if storage.IsTooLargeResourceVersion(err) {
				if utilfeature.DefaultFeatureGate.Enabled(features.ConsistentListFromCacheSkipTimeoutFallback) {
					// 跳过回退可避免 etcd 压力大时把所有请求都压到底层。
					fallback = "skipped"
					err = errors.NewTooManyRequests(err.Error(), resourceVersionTooHighRetrySeconds)
				} else {
					fallback = "true"
					err = c.storage.GetList(ctx, key, opts, listObj)
				}
			}
			// 回退成功时 err 已被清空；这里只统计最终仍然失败的请求。
			if err != nil {
				success = "false"
			}
			metrics.ConsistentReadTotal.WithLabelValues(c.cacher.groupResource.Group, c.cacher.groupResource.Resource, success, fallback).Add(1)
		}
		return err
	}
	if result.ConsistentRead {
		// 成功的缓存一致性读也要计数，便于和回退、失败对比。
		metrics.ConsistentReadTotal.WithLabelValues(c.cacher.groupResource.Group, c.cacher.groupResource.Resource, success, fallback).Add(1)
	}
	return nil
}

// shouldDelegateListOnNotReadyCache 判断缓存未就绪时能否安全下沉到存储：
// 仅当没有 label/field 选择器且带 limit 时，底层存储的列表语义才与缓存一致。
func shouldDelegateListOnNotReadyCache(opts storage.ListOptions) bool {
	pred := opts.Predicate
	noLabelSelector := pred.Label == nil || pred.Label.Empty()
	noFieldSelector := pred.Field == nil || pred.Field.Empty()
	hasLimit := pred.Limit > 0
	return noLabelSelector && noFieldSelector && hasLimit
}

// GuaranteedUpdate 转交底层存储执行更新，并尽量传入缓存中的最新对象作为前置条件建议；
// 缓存不可用时回退为不带建议对象的路径。
func (c *CacheDelegator) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	// Ignore the suggestion and try to pass down the current version of the object
	// read from cache.
	if elem, exists, err := c.cacher.watchCache.storage.GetByKey(key); err != nil {
		klog.Errorf("GetByKey returned error: %v", err)
	} else if exists {
		// 对对象做 DeepCopy，因为我们在序列化当前对象时会修改 resource version。
		// 序列化会修改 resourceVersion，必须深拷贝，避免污染缓存中共享的对象。
		currObj := elem.(*store.Element).Object.DeepCopyObject()
		return c.storage.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, preconditions, tryUpdate, currObj)
	}
	// If we couldn't get the object, fallback to no-suggestion.
	return c.storage.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, preconditions, tryUpdate, nil)
}

// Stats 透传底层存储的统计；缓存命中情况由 Cacher 自己的指标体现。
func (c *CacheDelegator) Stats(ctx context.Context) (storage.Stats, error) {
	return c.storage.Stats(ctx)
}

// ReadinessCheck 以缓存是否完成初始同步作为就绪条件。
func (c *CacheDelegator) ReadinessCheck() error {
	if !c.cacher.Ready() {
		return storage.ErrStorageNotReady
	}
	return nil
}

// RequestWatchProgress 需要底层存储广播进度通知，缓存自身不发起进度请求。
func (c *CacheDelegator) RequestWatchProgress(ctx context.Context) error {
	return c.storage.RequestWatchProgress(ctx)
}

// CompactRevision 优先返回缓存 compactor 的 revision；未启用 compactor 时回退底层存储的压缩版本。
func (c *CacheDelegator) CompactRevision() int64 {
	if c.cacher.compactor == nil {
		// 没有 compactor 说明压缩由底层存储负责。
		return c.storage.CompactRevision()
	}
	return c.cacher.compactor.Revision()
}

// Stop 幂等关闭 stopCh 并等待 checker 协程退出；未启用 checker 时 wg 为空，立即返回。
func (c *CacheDelegator) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	c.wg.Wait()
}
