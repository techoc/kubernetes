/*
Copyright 2019 The Kubernetes Authors.

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
	"bytes"
	"fmt"
	"io"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

var _ runtime.CacheableObject = &cachingObject{}

// metaRuntimeInterface implements runtime.Object and
// metav1.Object interfaces.
type metaRuntimeInterface interface {
	runtime.Object
	metav1.Object
}

// serializationResult captures a result of serialization.
//
// once 保证同一编码格式只序列化一次：并发 watch 请求会共享同一个结果，
// 这是 watch cache 能承受高并发 fan-out 的关键。
type serializationResult struct {
	// once should be used to ensure serialization is computed once.
	once sync.Once

	// raw is serialized object.
	raw []byte
	// err is error from serialization.
	err error
}

// serializationsCache is a type for caching serialization results.
//
// 以编码格式标识为键缓存序列化结果。
// 用 map + copy-on-write 而不是加锁的 map，是为了让读路径完全无锁。
type serializationsCache map[runtime.Identifier]*serializationResult

// cachingObject is an object that is able to cache its serializations
// so that each of those is computed exactly once.
//
// cachingObject implements the metav1.Object interface (accessors for
// all metadata fields).
//
// 同一对象通常要被成百上千个 watch 连接序列化，缓存结果把 CPU 成本从 O(连接数)
// 降到 O(编码格式数)。
type cachingObject struct {
	lock sync.RWMutex

	// deepCopied defines whether the object below has already been
	// deep copied. The operation is performed lazily on the first
	// setXxx operation.
	//
	// The lazy deep-copy make is useful, as effectively the only
	// case when we are setting some fields are ResourceVersion for
	// DELETE events, so in all other cases we can effectively avoid
	// performing any deep copies.
	deepCopied bool

	// Object for which serializations are cached.
	object metaRuntimeInterface

	// serializations is a cache containing object`s serializations.
	// The value stored in atomic.Value is of type serializationsCache.
	// The atomic.Value type is used to allow fast-path.
	//
	// 写入时整体替换 map（配合 lock），读取时只做一次原子 Load，避免读锁竞争。
	serializations atomic.Value
}

// newCachingObject performs a deep copy of the given object and wraps it
// into a cachingObject.
// An error is returned if it's not possible to cast the object to
// metav1.Object type.
func newCachingObject(object runtime.Object) (*cachingObject, error) {
	if obj, ok := object.(metaRuntimeInterface); ok {
		result := &cachingObject{
			object:     obj,
			deepCopied: false,
		}
		result.serializations.Store(make(serializationsCache))
		return result, nil
	}
	return nil, fmt.Errorf("can't cast object to metav1.Object: %#v", object)
}

// getSerializationResult 取出（必要时创建）指定编码格式的结果占位符。
func (o *cachingObject) getSerializationResult(id runtime.Identifier) *serializationResult {
	// Fast-path for getting from cache.
	serializations := o.serializations.Load().(serializationsCache)
	if result, exists := serializations[id]; exists {
		return result
	}

	// Slow-path (that may require insert).
	o.lock.Lock()
	defer o.lock.Unlock()

	// 拿到写锁后必须重新 Load：期间可能已被其他 goroutine 插入。
	serializations = o.serializations.Load().(serializationsCache)
	// Check if in the meantime it wasn't inserted.
	if result, exists := serializations[id]; exists {
		return result
	}

	// Insert an entry for <id>. This requires copy of existing map.
	// 复制而非原地插入，保证已经拿到旧 map 的读者不会看到并发写。
	newSerializations := make(serializationsCache)
	for k, v := range serializations {
		newSerializations[k] = v
	}
	result := &serializationResult{}
	newSerializations[id] = result
	o.serializations.Store(newSerializations)
	return result
}

// CacheEncode implements runtime.CacheableObject interface.
// It serializes the object and writes the result to given io.Writer trying
// to first use the already cached result and falls back to a given encode
// function in case of cache miss.
// It assumes that for a given identifier, the encode function always encodes
// each input object into the same output format.
//
// 契约：同一 id 必须对应确定的编码方式，否则缓存会返回错误格式的字节。
func (o *cachingObject) CacheEncode(id runtime.Identifier, encode func(runtime.Object, io.Writer) error, w io.Writer) error {
	result := o.getSerializationResult(id)
	result.once.Do(func() {
		buffer := bytes.NewBuffer(nil)
		// TODO(wojtek-t): This is currently making a copy to avoid races
		//   in cases where encoding is making subtle object modifications,
		//   e.g. #82497
		//   Figure out if we can somehow avoid this under some conditions.
		//
		// 传 GetObject() 的深拷贝而不是原对象：某些 encoder 会就地修改对象，
		// 直接编码会污染缓存中的共享对象。
		result.err = encode(o.GetObject(), buffer)
		result.raw = buffer.Bytes()
	})
	// Once invoked, fields of serialization will not change.
	if result.err != nil {
		return result.err
	}
	// 支持 splice 的 writer 可直接拼接字节，省一次拷贝（流式响应的热路径）。
	if b, support := w.(runtime.Splice); support {
		b.Splice(result.raw)
		return nil
	}
	_, err := w.Write(result.raw)
	return err
}

// GetObject implements runtime.CacheableObject interface.
// It returns deep-copy of the wrapped object to return ownership of it
// to the called according to the contract of the interface.
func (o *cachingObject) GetObject() runtime.Object {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.DeepCopyObject().(metaRuntimeInterface)
}

// GetObjectKind implements runtime.Object interface.
func (o *cachingObject) GetObjectKind() schema.ObjectKind {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetObjectKind()
}

// DeepCopyObject implements runtime.Object interface.
func (o *cachingObject) DeepCopyObject() runtime.Object {
	// DeepCopyObject on cachingObject is not expected to be called anywhere.
	// However, to be on the safe-side, we implement it, though given the
	// cache is only an optimization we ignore copying it.
	//
	// 新副本的 deepCopied 置 true：底层对象已经是独立副本，无需再拷贝一次。
	result := &cachingObject{
		deepCopied: true,
	}
	result.serializations.Store(make(serializationsCache))

	o.lock.RLock()
	defer o.lock.RUnlock()
	result.object = o.object.DeepCopyObject().(metaRuntimeInterface)
	return result
}

var (
	invalidationCacheTimestampLock sync.Mutex
	invalidationCacheTimestamp     time.Time
)

// shouldLogCacheInvalidation allows for logging cache-invalidation
// at most once per second (to avoid spamming logs in case of issues).
func shouldLogCacheInvalidation(now time.Time) bool {
	invalidationCacheTimestampLock.Lock()
	defer invalidationCacheTimestampLock.Unlock()
	if invalidationCacheTimestamp.Add(time.Second).Before(now) {
		invalidationCacheTimestamp = now
		return true
	}
	return false
}

// invalidateCacheLocked 清空序列化缓存；调用方必须已持有写锁。
//
// 正常情况下缓存不应失效（对象进缓存后视为不可变），因此这里打 warning 并附调用栈，
// 便于发现意外的写路径。
func (o *cachingObject) invalidateCacheLocked() {
	if cache, ok := o.serializations.Load().(serializationsCache); ok && len(cache) == 0 {
		return
	}
	// We don't expect cache invalidation to happen - so we want
	// to log the stacktrace to allow debugging if that will happen.
	// OTOH, we don't want to spam logs with it.
	// So we try to log it at most once per second.
	if shouldLogCacheInvalidation(time.Now()) {
		klog.Warningf("Unexpected cache invalidation for %#v\n%s", o.object, string(debug.Stack()))
	}
	o.serializations.Store(make(serializationsCache))
}

// The following functions implement metav1.Object interface:
// - getters simply delegate for the underlying object
// - setters check if operations isn't noop and if so,
//   invalidate the cache and delegate for the underlying object

// conditionalSet 是所有 setter 的共用骨架：先无锁快速判断是否 no-op，
// 真正需要修改时再加写锁、二次确认、惰性深拷贝并失效缓存。
//
// 两次 isNoop 判断是典型的 double-checked locking：第一次避免无谓加锁，
// 第二次防止两个 setter 同时通过快速路径后重复拷贝。
func (o *cachingObject) conditionalSet(isNoop func() bool, set func()) {
	if fastPath := func() bool {
		o.lock.RLock()
		defer o.lock.RUnlock()
		return isNoop()
	}(); fastPath {
		return
	}
	o.lock.Lock()
	defer o.lock.Unlock()
	if isNoop() {
		return
	}
	// 首次修改才深拷贝：缓存对象默认与缓存内部共享，写前复制避免污染其他读者。
	if !o.deepCopied {
		o.object = o.object.DeepCopyObject().(metaRuntimeInterface)
		o.deepCopied = true
	}
	o.invalidateCacheLocked()
	set()
}

func (o *cachingObject) GetNamespace() string {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetNamespace()
}
func (o *cachingObject) SetNamespace(namespace string) {
	o.conditionalSet(
		func() bool { return o.object.GetNamespace() == namespace },
		func() { o.object.SetNamespace(namespace) },
	)
}
func (o *cachingObject) GetName() string {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetName()
}
func (o *cachingObject) SetName(name string) {
	o.conditionalSet(
		func() bool { return o.object.GetName() == name },
		func() { o.object.SetName(name) },
	)
}
func (o *cachingObject) GetGenerateName() string {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetGenerateName()
}
func (o *cachingObject) SetGenerateName(name string) {
	o.conditionalSet(
		func() bool { return o.object.GetGenerateName() == name },
		func() { o.object.SetGenerateName(name) },
	)
}
func (o *cachingObject) GetUID() types.UID {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetUID()
}
func (o *cachingObject) SetUID(uid types.UID) {
	o.conditionalSet(
		func() bool { return o.object.GetUID() == uid },
		func() { o.object.SetUID(uid) },
	)
}
func (o *cachingObject) GetResourceVersion() string {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetResourceVersion()
}
func (o *cachingObject) SetResourceVersion(version string) {
	o.conditionalSet(
		func() bool { return o.object.GetResourceVersion() == version },
		func() { o.object.SetResourceVersion(version) },
	)
}
func (o *cachingObject) GetGeneration() int64 {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetGeneration()
}
func (o *cachingObject) SetGeneration(generation int64) {
	o.conditionalSet(
		func() bool { return o.object.GetGeneration() == generation },
		func() { o.object.SetGeneration(generation) },
	)
}
func (o *cachingObject) GetSelfLink() string {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetSelfLink()
}
func (o *cachingObject) SetSelfLink(selfLink string) {
	o.conditionalSet(
		func() bool { return o.object.GetSelfLink() == selfLink },
		func() { o.object.SetSelfLink(selfLink) },
	)
}
func (o *cachingObject) GetCreationTimestamp() metav1.Time {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetCreationTimestamp()
}
func (o *cachingObject) SetCreationTimestamp(timestamp metav1.Time) {
	o.conditionalSet(
		func() bool { return o.object.GetCreationTimestamp() == timestamp },
		func() { o.object.SetCreationTimestamp(timestamp) },
	)
}
func (o *cachingObject) GetDeletionTimestamp() *metav1.Time {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetDeletionTimestamp()
}
func (o *cachingObject) SetDeletionTimestamp(timestamp *metav1.Time) {
	o.conditionalSet(
		func() bool { return o.object.GetDeletionTimestamp() == timestamp },
		func() { o.object.SetDeletionTimestamp(timestamp) },
	)
}
func (o *cachingObject) GetDeletionGracePeriodSeconds() *int64 {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetDeletionGracePeriodSeconds()
}
func (o *cachingObject) SetDeletionGracePeriodSeconds(gracePeriodSeconds *int64) {
	o.conditionalSet(
		func() bool { return o.object.GetDeletionGracePeriodSeconds() == gracePeriodSeconds },
		func() { o.object.SetDeletionGracePeriodSeconds(gracePeriodSeconds) },
	)
}
func (o *cachingObject) GetLabels() map[string]string {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetLabels()
}
func (o *cachingObject) SetLabels(labels map[string]string) {
	o.conditionalSet(
		func() bool { return reflect.DeepEqual(o.object.GetLabels(), labels) },
		func() { o.object.SetLabels(labels) },
	)
}
func (o *cachingObject) GetAnnotations() map[string]string {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetAnnotations()
}
func (o *cachingObject) SetAnnotations(annotations map[string]string) {
	o.conditionalSet(
		func() bool { return reflect.DeepEqual(o.object.GetAnnotations(), annotations) },
		func() { o.object.SetAnnotations(annotations) },
	)
}
func (o *cachingObject) GetFinalizers() []string {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetFinalizers()
}
func (o *cachingObject) SetFinalizers(finalizers []string) {
	o.conditionalSet(
		func() bool { return reflect.DeepEqual(o.object.GetFinalizers(), finalizers) },
		func() { o.object.SetFinalizers(finalizers) },
	)
}
func (o *cachingObject) GetOwnerReferences() []metav1.OwnerReference {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetOwnerReferences()
}
func (o *cachingObject) SetOwnerReferences(references []metav1.OwnerReference) {
	o.conditionalSet(
		func() bool { return reflect.DeepEqual(o.object.GetOwnerReferences(), references) },
		func() { o.object.SetOwnerReferences(references) },
	)
}
func (o *cachingObject) GetManagedFields() []metav1.ManagedFieldsEntry {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetManagedFields()
}
func (o *cachingObject) SetManagedFields(managedFields []metav1.ManagedFieldsEntry) {
	o.conditionalSet(
		func() bool { return reflect.DeepEqual(o.object.GetManagedFields(), managedFields) },
		func() { o.object.SetManagedFields(managedFields) },
	)
}
