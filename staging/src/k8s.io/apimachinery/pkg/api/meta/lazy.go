/*
Copyright 2017 The Kubernetes Authors.

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

package meta

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// lazyObject defers loading the mapper and typer until necessary.
//
// lazyObject 把 RESTMapper 的构造推迟到第一次真正使用时，避免客户端初始化
// 阶段就发起 discovery 请求。一旦加载完成（无论成功失败），结果都被缓存。
type lazyObject struct {
	loader func() (RESTMapper, error)

	// lock 保护以下字段，保证并发首次调用只触发一次 loader。
	lock   sync.Mutex
	loaded bool
	// err 缓存初始化错误：后续调用直接返回同一错误，不重试。
	err    error
	mapper RESTMapper
}

// NewLazyRESTMapperLoader handles unrecoverable errors when creating a RESTMapper / ObjectTyper by
// returning those initialization errors when the interface methods are invoked. This defers the
// initialization and any server calls until a client actually needs to perform the action.
//
// 把“构造失败”从 New 阶段挪到方法调用阶段，使客户端可以先构造、再在实际
// 使用时统一处理错误。
func NewLazyRESTMapperLoader(fn func() (RESTMapper, error)) RESTMapper {
	obj := &lazyObject{loader: fn}
	return obj
}

// init lazily loads the mapper and typer, returning an error if initialization has failed.
//
// 在锁内检查 loaded 并缓存结果，保证 loader 只执行一次；失败也缓存，
// 后续调用直接返回同一错误。
func (o *lazyObject) init() error {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.loaded {
		return o.err
	}
	o.mapper, o.err = o.loader()
	o.loaded = true
	return o.err
}

var _ ResettableRESTMapper = &lazyObject{}

// KindFor 先确保 mapper 已加载，再委托给底层实现。
func (o *lazyObject) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	if err := o.init(); err != nil {
		return schema.GroupVersionKind{}, err
	}
	return o.mapper.KindFor(resource)
}

// KindsFor 先确保 mapper 已加载，再委托给底层实现。
func (o *lazyObject) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	if err := o.init(); err != nil {
		return []schema.GroupVersionKind{}, err
	}
	return o.mapper.KindsFor(resource)
}

// ResourceFor 先确保 mapper 已加载，再委托给底层实现。
func (o *lazyObject) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	if err := o.init(); err != nil {
		return schema.GroupVersionResource{}, err
	}
	return o.mapper.ResourceFor(input)
}

// ResourcesFor 先确保 mapper 已加载，再委托给底层实现。
func (o *lazyObject) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	if err := o.init(); err != nil {
		return []schema.GroupVersionResource{}, err
	}
	return o.mapper.ResourcesFor(input)
}

// RESTMapping 先确保 mapper 已加载，再委托给底层实现。
func (o *lazyObject) RESTMapping(gk schema.GroupKind, versions ...string) (*RESTMapping, error) {
	if err := o.init(); err != nil {
		return nil, err
	}
	return o.mapper.RESTMapping(gk, versions...)
}

// RESTMappings 先确保 mapper 已加载，再委托给底层实现。
func (o *lazyObject) RESTMappings(gk schema.GroupKind, versions ...string) ([]*RESTMapping, error) {
	if err := o.init(); err != nil {
		return nil, err
	}
	return o.mapper.RESTMappings(gk, versions...)
}

// ResourceSingularizer 先确保 mapper 已加载，再委托给底层实现。
func (o *lazyObject) ResourceSingularizer(resource string) (singular string, err error) {
	if err := o.init(); err != nil {
		return "", err
	}
	return o.mapper.ResourceSingularizer(resource)
}

// Reset 在已成功加载的前提下把重置传递给底层 mapper；初始化失败的缓存保持
// 不变，避免 Reset 意外触发一次新的 loader 调用。
func (o *lazyObject) Reset() {
	o.lock.Lock()
	defer o.lock.Unlock()
	if o.loaded && o.err == nil {
		MaybeResetRESTMapper(o.mapper)
	}
}
