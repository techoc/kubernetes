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

// RESTOptions 是传给 generic registry 的一组资源专属配置选项。
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

// 实现 RESTOptionsGetter，使测试等场景下可直接使用 RESTOptions。
func (opts RESTOptions) GetRESTOptions(schema.GroupResource, runtime.Object) (RESTOptions, error) {
	return opts, nil
}

// RESTOptionsGetter 根据资源（以及可选的示例对象）解析出存储选项。
// 示例对象用于确定该资源的存储版本；为 nil 时使用资源默认存储版本。
type RESTOptionsGetter interface {
	// GetRESTOptions 返回给定资源和示例对象的 RESTOptions。
	// 示例对象用于确定资源的存储版本。
	// 若示例对象为 nil，则使用资源的默认存储版本。
	GetRESTOptions(resource schema.GroupResource, example runtime.Object) (RESTOptions, error)
}

// StoreOptions 是用于补全 generic registry 的一组配置选项。
//
// TriggerFunc/AttrFunc/Indexers 把领域逻辑注入通用 Store：索引键、属性提取
// 与附加索引，使同一套 Store 实现适配不同资源。
type StoreOptions struct {
	RESTOptions RESTOptionsGetter
	TriggerFunc storage.IndexerFuncs
	AttrFunc    storage.AttrFunc
	Indexers    *cache.Indexers
}
