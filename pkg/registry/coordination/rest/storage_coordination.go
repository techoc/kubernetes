/*
Copyright 2018 The Kubernetes Authors.

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
	coordinationv1 "k8s.io/api/coordination/v1"
	coordinationv1alpha2 "k8s.io/api/coordination/v1alpha2"
	coordinationv1beta1 "k8s.io/api/coordination/v1beta1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/coordination"
	leasestorage "k8s.io/kubernetes/pkg/registry/coordination/lease/storage"
	leasecandidatestorage "k8s.io/kubernetes/pkg/registry/coordination/leasecandidate/storage"
)

// RESTStorageProvider 负责按 API 版本和资源启用配置装配 Coordination API Group 的 REST 存储。
type RESTStorageProvider struct{}

// NewRESTStorage 初始化 Coordination API Group，并依次装配 v1、v1beta1 和 v1alpha2 的资源存储。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(coordination.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 若在此处增加 API 版本，还必须在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将版本优先级信息直接提供给 APIGroupInfo。

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[coordinationv1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1beta1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[coordinationv1beta1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1alpha2Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[coordinationv1alpha2.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 根据资源启用配置注册 coordination/v1 的 Lease 存储。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// 注册 coordination/v1 的 Lease 资源。
	if resource := "leases"; apiResourceConfigSource.ResourceEnabled(coordinationv1.SchemeGroupVersion.WithResource(resource)) {
		leaseStorage, err := leasestorage.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = leaseStorage
	}
	return storage, nil
}

// v1beta1Storage 根据资源启用配置注册 coordination/v1beta1 的 LeaseCandidate 存储。
func (p RESTStorageProvider) v1beta1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// 注册当前 API 版本中的 LeaseCandidate 资源。
	if resource := "leasecandidates"; apiResourceConfigSource.ResourceEnabled(coordinationv1beta1.SchemeGroupVersion.WithResource(resource)) {
		leaseCandidateStorage, err := leasecandidatestorage.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = leaseCandidateStorage
	}
	return storage, nil
}

// v1alpha2Storage 根据资源启用配置注册 coordination/v1alpha2 的 LeaseCandidate 存储。
func (p RESTStorageProvider) v1alpha2Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// 注册当前 API 版本中的 LeaseCandidate 资源。
	if resource := "leasecandidates"; apiResourceConfigSource.ResourceEnabled(coordinationv1alpha2.SchemeGroupVersion.WithResource(resource)) {
		leaseCandidateStorage, err := leasecandidatestorage.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = leaseCandidateStorage
	}
	return storage, nil
}

// GroupName 返回 Coordination API Group 的名称。
func (p RESTStorageProvider) GroupName() string {
	return coordination.GroupName
}
