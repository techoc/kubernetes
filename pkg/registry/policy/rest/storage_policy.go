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
	policyapiv1 "k8s.io/api/policy/v1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/policy"
	poddisruptionbudgetstore "k8s.io/kubernetes/pkg/registry/policy/poddisruptionbudget/storage"
)

type RESTStorageProvider struct{}

// NewRESTStorage 初始化 API Group 元信息，按版本构建资源存储映射，并遵循 API 资源启用配置。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(policy.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 如果在此处增加 API 版本，务必同时在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将优先级信息直接提供给 APIGroupInfo。

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[policyapiv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 构建当前 API Group v1 版本的资源到 REST Storage 映射，仅注册启用的资源及其子资源。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "poddisruptionbudgets"; apiResourceConfigSource.ResourceEnabled(policyapiv1.SchemeGroupVersion.WithResource(resource)) {
		poddisruptionbudgetStorage, poddisruptionbudgetStatusStorage, err := poddisruptionbudgetstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = poddisruptionbudgetStorage
		storage[resource+"/status"] = poddisruptionbudgetStatusStorage
	}

	return storage, nil
}

// GroupName 返回 API Group 名称，供 API Server 注册资源组和版本路由使用。
func (p RESTStorageProvider) GroupName() string {
	return policy.GroupName
}
