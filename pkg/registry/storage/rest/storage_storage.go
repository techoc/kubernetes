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

// 本文件负责装配 storage.k8s.io API Group 的各版本 REST Storage，并按资源启用配置注册对应资源与子资源。
package rest

import (
	storageapiv1 "k8s.io/api/storage/v1"
	storageapiv1beta1 "k8s.io/api/storage/v1beta1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	storageapi "k8s.io/kubernetes/pkg/apis/storage"
	csidriverstore "k8s.io/kubernetes/pkg/registry/storage/csidriver/storage"
	csinodestore "k8s.io/kubernetes/pkg/registry/storage/csinode/storage"
	csistoragecapacitystore "k8s.io/kubernetes/pkg/registry/storage/csistoragecapacity/storage"
	storageclassstore "k8s.io/kubernetes/pkg/registry/storage/storageclass/storage"
	volumeattachmentstore "k8s.io/kubernetes/pkg/registry/storage/volumeattachment/storage"
	volumeattributesclassstore "k8s.io/kubernetes/pkg/registry/storage/volumeattributesclass/storage"
)

type RESTStorageProvider struct {
}

// NewRESTStorage 创建 storage.k8s.io API Group 元信息，按版本构建资源存储映射，并只注册 ResourceConfig 明确启用的资源。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(storageapi.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// If you add a version here, be sure to add an entry in `k8s.io/kubernetes/cmd/kube-apiserver/app/aggregator.go with specific priorities.
	// TODO refactor the plumbing to provide the information in the APIGroupInfo

	if storageMap, err := p.v1beta1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[storageapiv1beta1.SchemeGroupVersion.Version] = storageMap
	}
	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[storageapiv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1beta1Storage 构建 storage.k8s.io/v1beta1 的资源映射；该版本只公开当前 API 定义支持的资源集合。
func (p RESTStorageProvider) v1beta1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// register volumeattributesclasses
	if resource := "volumeattributesclasses"; apiResourceConfigSource.ResourceEnabled(storageapiv1beta1.SchemeGroupVersion.WithResource(resource)) {
		volumeAttributesClassStorage, err := volumeattributesclassstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = volumeAttributesClassStorage
	}

	return storage, nil
}

// v1Storage 构建 storage.k8s.io/v1 的资源到 REST Storage 映射；每个资源启用后才创建对应 Store，子资源需单独注册路径。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storageClassStorage, err := storageclassstore.NewREST(restOptionsGetter)
	if err != nil {
		return nil, err
	}
	volumeAttachmentStorage, err := volumeattachmentstore.NewStorage(restOptionsGetter)
	if err != nil {
		return nil, err
	}

	storage := map[string]rest.Storage{}

	// storageclasses
	if resource := "storageclasses"; apiResourceConfigSource.ResourceEnabled(storageapiv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = storageClassStorage
	}

	// volumeattachments
	if resource := "volumeattachments"; apiResourceConfigSource.ResourceEnabled(storageapiv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = volumeAttachmentStorage.VolumeAttachment
		storage[resource+"/status"] = volumeAttachmentStorage.Status
	}

	// register volumeattributesclasses
	if resource := "volumeattributesclasses"; apiResourceConfigSource.ResourceEnabled(storageapiv1.SchemeGroupVersion.WithResource(resource)) {
		volumeAttributesClassStorage, err := volumeattributesclassstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = volumeAttributesClassStorage
	}

	// register csinodes
	if resource := "csinodes"; apiResourceConfigSource.ResourceEnabled(storageapiv1.SchemeGroupVersion.WithResource(resource)) {
		csiNodeStorage, err := csinodestore.NewStorage(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = csiNodeStorage.CSINode
		storage[resource+"/status"] = csiNodeStorage.Status
	}

	// register csidrivers
	if resource := "csidrivers"; apiResourceConfigSource.ResourceEnabled(storageapiv1.SchemeGroupVersion.WithResource(resource)) {
		csiDriverStorage, err := csidriverstore.NewStorage(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = csiDriverStorage.CSIDriver
	}

	// register csistoragecapacities
	if resource := "csistoragecapacities"; apiResourceConfigSource.ResourceEnabled(storageapiv1.SchemeGroupVersion.WithResource(resource)) {
		csiStorageStorage, err := csistoragecapacitystore.NewStorage(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = csiStorageStorage.CSIStorageCapacity
	}

	return storage, nil
}

// GroupName 返回 storage.k8s.io API Group 名称，用于 APIGroupInfo 注册和版本路由。
func (p RESTStorageProvider) GroupName() string {
	return storageapi.GroupName
}
