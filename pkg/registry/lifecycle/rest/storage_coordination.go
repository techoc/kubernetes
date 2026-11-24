/*
Copyright The Kubernetes Authors.

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
	lifecyclev1alpha1 "k8s.io/api/lifecycle/v1alpha1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	"k8s.io/kubernetes/pkg/features"
	evictionstorage "k8s.io/kubernetes/pkg/registry/lifecycle/eviction/storage"
	evictionrequeststorage "k8s.io/kubernetes/pkg/registry/lifecycle/evictionrequest/storage"
	"k8s.io/utils/clock"
)

// RESTStorageProvider 负责装配 lifecycle.k8s.io API Group 的 REST 存储。
type RESTStorageProvider struct {
}

// NewRESTStorage 初始化 Lifecycle API Group，并注册 v1alpha1 版本的 Eviction 和 EvictionRequest 存储。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(lifecycle.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 如果在此处增加 API 版本，务必同时在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将优先级信息直接提供给 APIGroupInfo。

	if storageMap, err := p.v1alpha1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[lifecyclev1alpha1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1alpha1Storage 按资源启用配置和 EvictionRequestAPI 特性门控装配 Eviction、EvictionRequest 及其 /status 子资源。
func (p RESTStorageProvider) v1alpha1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "evictions"; apiResourceConfigSource.ResourceEnabled(lifecyclev1alpha1.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.EvictionRequestAPI) {
			evictionStorage, evictionStatusStorage, err := evictionstorage.NewREST(restOptionsGetter, clock.RealClock{})
			if err != nil {
				return storage, err
			}
			storage[resource] = evictionStorage
			storage[resource+"/status"] = evictionStatusStorage
		} else {
			klog.Warning("Eviction storage is disabled because the EvictionRequestAPI feature gate is disabled")
		}
	}
	if resource := "evictionrequests"; apiResourceConfigSource.ResourceEnabled(lifecyclev1alpha1.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.EvictionRequestAPI) {
			evictionRequestStorage, evictionRequestStatusStorage, err := evictionrequeststorage.NewREST(restOptionsGetter)
			if err != nil {
				return storage, err
			}
			storage[resource] = evictionRequestStorage
			storage[resource+"/status"] = evictionRequestStatusStorage
		} else {
			klog.Warning("EvictionRequest storage is disabled because the EvictionRequestAPI feature gate is disabled")
		}
	}

	return storage, nil
}

// GroupName 返回 Lifecycle API Group 的名称。
func (p RESTStorageProvider) GroupName() string {
	return lifecycle.GroupName
}
