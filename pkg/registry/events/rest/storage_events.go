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

package rest

import (
	"time"

	eventsapiv1 "k8s.io/api/events/v1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/events"
	eventstore "k8s.io/kubernetes/pkg/registry/core/event/storage"
)

// RESTStorageProvider 负责装配 Events API Group 的 REST 存储；TTL 控制 Event 在存储中的保留时长。
type RESTStorageProvider struct {
	TTL time.Duration
}

// NewRESTStorage 初始化 Events API Group，并注册当前已启用 API 版本的资源存储。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(events.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 若在此处增加 API 版本，还必须在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将版本优先级信息直接提供给 APIGroupInfo。

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[eventsapiv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 根据资源启用配置创建 Event 存储，并将配置的 TTL 秒数传递给底层存储实现。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// 注册 events.k8s.io/v1 的 Event 资源。
	if resource := "events"; apiResourceConfigSource.ResourceEnabled(eventsapiv1.SchemeGroupVersion.WithResource(resource)) {
		eventsStorage, err := eventstore.NewREST(restOptionsGetter, uint64(p.TTL.Seconds()))
		if err != nil {
			return storage, err
		}
		storage[resource] = eventsStorage
	}

	return storage, nil
}

// GroupName 返回 Events API Group 的名称。
func (p RESTStorageProvider) GroupName() string {
	return events.GroupName
}
