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

// metaRuntimeInterface 同时实现 runtime.Object 与
// metav1.Object 接口。
type metaRuntimeInterface interface {
	runtime.Object
	metav1.Object
}

// serializationResult 保存一次序列化的结果。
//
// once 保证同一编码格式只序列化一次：并发 watch 请求会共享同一个结果，
// 这是 watch cache 能承受高并发 fan-out 的关键。
type serializationResult struct {
	// once 用于保证序列化只计算一次。
	once sync.Once

	// raw 是序列化后的对象。
	raw []byte
	// err 是序列化产生的错误。
	err error
}

// serializationsCache 用于缓存序列化结果。
//
// 以编码格式标识为键缓存序列化结果。
// 用 map + copy-on-write 而不是加锁的 map，是为了让读路径完全无锁。
type serializationsCache map[runtime.Identifier]*serializationResult

// cachingObject 能缓存自身的各种序列化结果，
// 保证每种序列化只计算一次。
//
// cachingObject 实现 metav1.Object 接口（所有元数据字段的
// 访问器）。
//
// 同一对象通常要被成百上千个 watch 连接序列化，缓存结果把 CPU 成本从 O(连接数)
// 降到 O(编码格式数)。
type cachingObject struct {
	lock sync.RWMutex

	// deepCopied 表示内部对象是否已深拷贝。
	//
	// 深拷贝在首次 setXxx 时惰性执行。
	//
	// 惰性深拷贝很有用：实际只有 DELETE 事件设置 ResourceVersion
	//
	deepCopied bool

	// 被缓存序列化结果的对象。
	object metaRuntimeInterface

	// serializations 是保存对象序列化结果的缓存。
	// atomic.Value 中存的值是 serializationsCache 类型。
	// 用 atomic.Value 是为了支持快速路径。
	//
	// 写入时整体替换 map（配合 lock），读取时只做一次原子 Load，避免读锁竞争。
	serializations atomic.Value
}

// newCachingObject 深拷贝给定对象并包装成 cachingObject。
//
// 对象无法转换为 metav1.Object 时返回错误。
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
	// 从缓存读取的快速路径。
	serializations := o.serializations.Load().(serializationsCache)
	if result, exists := serializations[id]; exists {
		return result
	}

	// 慢路径（可能需要插入）。
	o.lock.Lock()
	defer o.lock.Unlock()

	// 拿到写锁后必须重新 Load：期间可能已被其他 goroutine 插入。
	serializations = o.serializations.Load().(serializationsCache)
	// 检查期间是否已被其他 goroutine 插入。
	if result, exists := serializations[id]; exists {
		return result
	}

	// 为 <id> 插入条目，需要复制现有 map。
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

// CacheEncode 实现 runtime.CacheableObject 接口。
// 它序列化对象并写入给定 io.Writer，优先用缓存结果，
// 未命中时回退到给定的 encode 函数。
//
// 它假定相同标识符下，encode 函数总把同一输入编码为相同输出。
//
// 契约：同一 id 必须对应确定的编码方式，否则缓存会返回错误格式的字节。
func (o *cachingObject) CacheEncode(id runtime.Identifier, encode func(runtime.Object, io.Writer) error, w io.Writer) error {
	result := o.getSerializationResult(id)
	result.once.Do(func() {
		buffer := bytes.NewBuffer(nil)
		// TODO(wojtek-t): 这里当前会复制对象，以避免编码过程中
		// 对对象做微妙修改时产生竞态，例如 #82497。
		//
		// 研究是否可在某些条件下避免复制。
		//
		// 传 GetObject() 的深拷贝而不是原对象：某些 encoder 会就地修改对象，
		// 直接编码会污染缓存中的共享对象。
		result.err = encode(o.GetObject(), buffer)
		result.raw = buffer.Bytes()
	})
	// 一旦调用，serialization 的字段不再变化。
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

// GetObject 实现 runtime.CacheableObject 接口。
// 按接口契约返回内部对象的深拷贝，把所有权交给
// 调用方。
func (o *cachingObject) GetObject() runtime.Object {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.DeepCopyObject().(metaRuntimeInterface)
}

// GetObjectKind 实现 runtime.Object 接口。
func (o *cachingObject) GetObjectKind() schema.ObjectKind {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.object.GetObjectKind()
}

// DeepCopyObject 实现 runtime.Object 接口。
func (o *cachingObject) DeepCopyObject() runtime.Object {
	// cachingObject 的 DeepCopyObject 预期不会被调用。
	// 但为安全起见仍予以实现；鉴于缓存只是优化，
	// 这里不复制缓存。
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

// shouldLogCacheInvalidation 限制缓存失效日志最多每秒一次，
// 避免出问题时刷屏。
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
	// 预期不会发生缓存失效，因此记录堆栈以便排查；
	//
	if shouldLogCacheInvalidation(time.Now()) {
		klog.Warningf("Unexpected cache invalidation for %#v\n%s", o.object, string(debug.Stack()))
	}
	o.serializations.Store(make(serializationsCache))
}

// 以下函数实现 metav1.Object 接口：
// - getter 直接委托给内部对象
// - setter 先判断是否为无操作，若不是则
// 使缓存失效并委托给内部对象

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
