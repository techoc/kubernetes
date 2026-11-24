/*
Copyright 2019 The Kubernetes Authors.

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
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	nodeinternal "k8s.io/kubernetes/pkg/apis/node"
	runtimeclassstorage "k8s.io/kubernetes/pkg/registry/node/runtimeclass/storage"
)

// RESTStorageProvider 负责装配 node.k8s.io API Group 的 REST 存储。
type RESTStorageProvider struct{}

// NewRESTStorage 初始化 Node API Group，并注册当前已启用版本的 RuntimeClass 存储。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(nodeinternal.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[nodev1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "runtimeclasses"; apiResourceConfigSource.ResourceEnabled(nodev1.SchemeGroupVersion.WithResource(resource)) {
		s, err := runtimeclassstorage.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = s
	}

	return storage, nil
}

// GroupName 返回 Node API Group 的名称。
func (p RESTStorageProvider) GroupName() string {
	return nodeinternal.GroupName
}
