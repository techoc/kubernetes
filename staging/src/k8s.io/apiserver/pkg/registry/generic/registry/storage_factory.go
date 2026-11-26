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

package registry

import (
	"fmt"
	"sync"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	cacherstorage "k8s.io/apiserver/pkg/storage/cacher"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
)

// 根据给定的 storageConfig 创建 cacher。
//
// 装饰器模式：在原始存储之上叠加 watch cache 与 delegator。delegator 让
// 读请求走缓存、写请求直连存储，从而同时获得缓存收益与强一致的写入。
func StorageWithCacher() generic.StorageDecorator {
	return func(
		storageConfig *storagebackend.ConfigForResource,
		resourcePrefix string,
		cacheKeyFunc func(obj runtime.Object) (string, error),
		newFunc func() runtime.Object,
		newListFunc func() runtime.Object,
		getAttrsFunc storage.AttrFunc,
		triggerFuncs storage.IndexerFuncs,
		indexers *cache.Indexers) (storage.Interface, factory.DestroyFunc, error) {

		s, d, err := generic.NewRawStorage(storageConfig, newFunc, newListFunc, resourcePrefix)
		if err != nil {
			return s, d, err
		}
		if klogV := klog.V(5); klogV.Enabled() {
			//nolint:logcheck // It complains about the key/value pairs because it cannot check them.
			klogV.InfoS("Storage caching is enabled", objectTypeToArgs(newFunc())...)
		}

		cacherConfig := cacherstorage.Config{
			Storage: s,
			// APIObjectVersioner 使用 resourceVersion 做 CAS，是所有内置资源的默认语义。
			Versioner:           storage.APIObjectVersioner{},
			GroupResource:       storageConfig.GroupResource,
			EventsHistoryWindow: storageConfig.EventsHistoryWindow,
			ResourcePrefix:      resourcePrefix,
			KeyFunc:             cacheKeyFunc,
			NewFunc:             newFunc,
			NewListFunc:         newListFunc,
			GetAttrsFunc:        getAttrsFunc,
			IndexerFuncs:        triggerFuncs,
			Indexers:            indexers,
			Codec:               storageConfig.Codec,
		}
		cacher, err := cacherstorage.NewCacherFromConfig(cacherConfig)
		if err != nil {
			return nil, func() {}, err
		}
		delegator := cacherstorage.NewCacheDelegator(cacher, s)
		// 用 sync.Once 包裹销毁，防止调用方重复调用 destroy 导致底层存储被二次关闭。
		var once sync.Once
		destroyFunc := func() {
			once.Do(func() {
				delegator.Stop()
				cacher.Stop()
				d()
			})
		}

		return delegator, destroyFunc, nil
	}
}

// objectTypeToArgs 为日志生成对象标识：unstructured 对象输出 apiVersion/kind，
// 其余输出 Go 类型名，便于在海量资源中快速定位是哪一类存储启用了缓存。
func objectTypeToArgs(obj runtime.Object) []interface{} {
	// 非结构化对象自带 apiVersion/kind，单独处理。
	if u, isUnstructured := obj.(*unstructured.Unstructured); isUnstructured {
		if apiVersion, kind := u.GetAPIVersion(), u.GetKind(); len(apiVersion) > 0 && len(kind) > 0 {
			return []interface{}{"apiVersion", apiVersion, "kind", kind}
		}
	}

	// 其它情况直接返回类型。
	return []interface{}{"type", fmt.Sprintf("%T", obj)}
}
