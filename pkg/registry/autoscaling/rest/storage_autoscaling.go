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

// 本文件负责装配 rest API Group 的版本资源映射，并按资源启用配置注册 REST Storage。
package rest

import (
	"sync"

	autoscalingapiv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	autoscalingapiv2 "k8s.io/kubernetes/pkg/apis/autoscaling/v2"
	horizontalpodautoscalerstore "k8s.io/kubernetes/pkg/registry/autoscaling/horizontalpodautoscaler/storage"
)

type RESTStorageProvider struct{}

// NewRESTStorage 初始化 API Group 元信息，按版本构建资源存储映射，并遵循 API 资源启用配置。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(autoscaling.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 若在此处增加 API 版本，还必须在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将版本优先级信息直接提供给 APIGroupInfo。

	var hpaOnce sync.Once
	var hpaStorage *horizontalpodautoscalerstore.REST
	var hpaStatusStorage *horizontalpodautoscalerstore.StatusREST
	var hpaStorageErr error
	var storageGetter hpaStorageGetter = func() (*horizontalpodautoscalerstore.REST, *horizontalpodautoscalerstore.StatusREST, error) {
		hpaOnce.Do(func() {
			hpaStorage, hpaStatusStorage, hpaStorageErr = horizontalpodautoscalerstore.NewREST(restOptionsGetter)
		})
		return hpaStorage, hpaStatusStorage, hpaStorageErr
	}

	if storageMap, err := p.v2Storage(apiResourceConfigSource, storageGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[autoscalingapiv2.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1Storage(apiResourceConfigSource, storageGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[autoscalingapiv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 构建当前 API Group v1 版本的资源到 REST Storage 映射，仅注册启用的资源及其子资源。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, storageGetter hpaStorageGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// 注册 HorizontalPodAutoscaler 主资源及其 Status 子资源。
	if resource := "horizontalpodautoscalers"; apiResourceConfigSource.ResourceEnabled(autoscalingapiv1.SchemeGroupVersion.WithResource(resource)) {
		hpaStorage, hpaStatusStorage, err := storageGetter()
		if err != nil {
			return storage, err
		}
		storage[resource] = hpaStorage
		storage[resource+"/status"] = hpaStatusStorage
	}

	return storage, nil
}

// v2Storage 构建 autoscaling/v2 的 REST Storage 映射，并注册 HPA 主资源及 status 子资源。
func (p RESTStorageProvider) v2Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, storageGetter hpaStorageGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// 注册 HorizontalPodAutoscaler 主资源及其子资源。
	if resource := "horizontalpodautoscalers"; apiResourceConfigSource.ResourceEnabled(autoscalingapiv2.SchemeGroupVersion.WithResource(resource)) {
		hpaStorage, hpaStatusStorage, err := storageGetter()
		if err != nil {
			return storage, err
		}
		storage[resource] = hpaStorage
		storage[resource+"/status"] = hpaStatusStorage
	}

	return storage, nil
}

type hpaStorageGetter func() (*horizontalpodautoscalerstore.REST, *horizontalpodautoscalerstore.StatusREST, error)

// GroupName 返回 API Group 名称，供 API Server 注册资源组和版本路由使用。
func (p RESTStorageProvider) GroupName() string {
	return autoscaling.GroupName
}
