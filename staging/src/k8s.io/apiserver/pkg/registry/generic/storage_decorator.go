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

package generic

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
)

// StorageDecorator 是根据给定参数生成 storage.Interface
// 及对应 DestroyFunc 的函数签名。cacheKeyFunc 供缓存层
// 从对象计算资源键。
//
// 装饰器模式让同一套 etcd 后端可以按需包上 cacher 等能力；registry 只依赖此签名，
// 因此是否启用缓存属于装配期决策，不影响上层逻辑。
type StorageDecorator func(
	config *storagebackend.ConfigForResource,
	resourcePrefix string,
	cacheKeyFunc func(obj runtime.Object) (string, error),
	newFunc func() runtime.Object,
	newListFunc func() runtime.Object,
	getAttrsFunc storage.AttrFunc,
	trigger storage.IndexerFuncs,
	indexers *cache.Indexers) (storage.Interface, factory.DestroyFunc, error)

// UndecoratedStorage 根据给定配置创建不带任何装饰的存储。
//
// 仅创建底层 KV 存储，不包缓存；需要直连 etcd 语义的调用方使用。
func UndecoratedStorage(
	config *storagebackend.ConfigForResource,
	resourcePrefix string,
	cacheKeyFunc func(obj runtime.Object) (string, error),
	newFunc func() runtime.Object,
	newListFunc func() runtime.Object,
	getAttrsFunc storage.AttrFunc,
	trigger storage.IndexerFuncs,
	indexers *cache.Indexers) (storage.Interface, factory.DestroyFunc, error) {
	return NewRawStorage(config, newFunc, newListFunc, resourcePrefix)
}

// NewRawStorage 创建底层 kv 存储，用于绕开当前
// 两层相同存储接口的现状。
// TODO: 待所有 registry（event registry 特殊）都启用 cacher 后，移除此方法。
//
// 直接返回 etcd 客户端实现，绕过装饰器参数，供内部装配代码使用。
func NewRawStorage(config *storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, resourcePrefix string) (storage.Interface, factory.DestroyFunc, error) {
	return factory.Create(*config, newFunc, newListFunc, resourcePrefix)
}
