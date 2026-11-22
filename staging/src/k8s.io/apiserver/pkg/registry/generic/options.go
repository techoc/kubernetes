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

package generic

import (
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/client-go/tools/cache"
)

// RESTOptions is set of resource-specific configuration options to generic registries.
//
// 这些选项决定某个资源的存储后端、删除/回收行为、指标采集周期，
// 以及向流量控制上报对象数量的追踪器。
type RESTOptions struct {
	// StorageConfig 是已经按资源限定好的存储配置（backend + 编解码器等）。
	StorageConfig *storagebackend.ConfigForResource
	// Decorator 包装底层存储接口，例如加入缓存、计数或错误转换。
	Decorator StorageDecorator

	// EnableGarbageCollection 控制删除时是否保留对象的删除标记（finalizer 语义）。
	EnableGarbageCollection   bool
	DeleteCollectionWorkers   int
	ResourcePrefix            string
	CountMetricPollPeriod     time.Duration
	StorageObjectCountTracker flowcontrolrequest.StorageObjectCountTracker
}

// Implement RESTOptionsGetter so that RESTOptions can directly be used when available (i.e. tests)
func (opts RESTOptions) GetRESTOptions(schema.GroupResource, runtime.Object) (RESTOptions, error) {
	return opts, nil
}

// RESTOptionsGetter 根据资源（以及可选的示例对象）解析出存储选项。
// 示例对象用于确定该资源的存储版本；为 nil 时使用资源默认存储版本。
type RESTOptionsGetter interface {
	// GetRESTOptions returns the RESTOptions for the given resource and example object.
	// The example object is used to determine the storage version for the resource.
	// If the example object is nil, the storage version will be determined by the resource's default storage version.
	GetRESTOptions(resource schema.GroupResource, example runtime.Object) (RESTOptions, error)
}

// StoreOptions is set of configuration options used to complete generic registries.
//
// TriggerFunc/AttrFunc/Indexers 把领域逻辑注入通用 Store：索引键、属性提取
// 与附加索引，使同一套 Store 实现适配不同资源。
type StoreOptions struct {
	RESTOptions RESTOptionsGetter
	TriggerFunc storage.IndexerFuncs
	AttrFunc    storage.AttrFunc
	Indexers    *cache.Indexers
}
