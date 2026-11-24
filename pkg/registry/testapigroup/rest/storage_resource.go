/*
Copyright 2022 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/apis/testapigroup"
	testapigroupv1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	carpstore "k8s.io/kubernetes/pkg/registry/testapigroup/carp/storage"
)

// RESTStorageProvider 负责装配测试 API Group 的 REST 存储，并提供 Carp 所需的 Namespace 客户端。
type RESTStorageProvider struct {
	NamespaceClient v1.NamespaceInterface
}

// NewRESTStorage 初始化测试 API Group，并按资源启用配置注册 v1 版本的 Carp 主资源和 /status 子资源。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(testapigroup.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 如果在此处增加 API 版本，务必同时在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将优先级信息直接提供给 APIGroupInfo。

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter, p.NamespaceClient); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[testapigroupv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 根据资源启用配置装配 Carp 主资源和 /status 子资源。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter, nsClient v1.NamespaceInterface) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "carps"; apiResourceConfigSource.ResourceEnabled(testapigroupv1.SchemeGroupVersion.WithResource(resource)) {
		resourceClaimStorage, resourceClaimStatusStorage, err := carpstore.NewREST(restOptionsGetter, nsClient)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceClaimStorage
		storage[resource+"/status"] = resourceClaimStatusStorage
	}

	return storage, nil
}

// GroupName 返回测试 API Group 的名称。
func (p RESTStorageProvider) GroupName() string {
	return testapigroup.GroupName
}
