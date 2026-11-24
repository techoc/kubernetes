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

package rest

import (
	networkingapiv1 "k8s.io/api/networking/v1"
	networkingapiv1beta1 "k8s.io/api/networking/v1beta1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/networking"
	ingressstore "k8s.io/kubernetes/pkg/registry/networking/ingress/storage"
	ingressclassstore "k8s.io/kubernetes/pkg/registry/networking/ingressclass/storage"
	ipaddressstore "k8s.io/kubernetes/pkg/registry/networking/ipaddress/storage"
	networkpolicystore "k8s.io/kubernetes/pkg/registry/networking/networkpolicy/storage"
	servicecidrstore "k8s.io/kubernetes/pkg/registry/networking/servicecidr/storage"
)

// RESTStorageProvider 负责装配 networking.k8s.io API Group 的 v1 与 v1beta1 资源存储。
type RESTStorageProvider struct{}

// NewRESTStorage 注册 API Group 与版本 StorageMap；版本优先级还需在 kube-apiserver aggregator 配置中同步维护。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(networking.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// If you add a version here, be sure to add an entry in `k8s.io/kubernetes/cmd/kube-apiserver/app/aggregator.go with specific priorities.
	// TODO refactor the plumbing to provide the information in the APIGroupInfo

	if storageMap, err := p.v1beta1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[networkingapiv1beta1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[networkingapiv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 按资源开关逐项装配 NetworkPolicy、Ingress、IngressClass、IPAddress 与 ServiceCIDR，并为支持状态的资源注册 /status。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// networkpolicies
	if resource := "networkpolicies"; apiResourceConfigSource.ResourceEnabled(networkingapiv1.SchemeGroupVersion.WithResource(resource)) {
		networkPolicyStorage, err := networkpolicystore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = networkPolicyStorage
	}

	// ingresses
	if resource := "ingresses"; apiResourceConfigSource.ResourceEnabled(networkingapiv1.SchemeGroupVersion.WithResource(resource)) {
		ingressStorage, ingressStatusStorage, err := ingressstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = ingressStorage
		storage[resource+"/status"] = ingressStatusStorage
	}

	// ingressclasses
	if resource := "ingressclasses"; apiResourceConfigSource.ResourceEnabled(networkingapiv1.SchemeGroupVersion.WithResource(resource)) {
		ingressClassStorage, err := ingressclassstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = ingressClassStorage
	}

	// ipaddress
	if resource := "ipaddresses"; apiResourceConfigSource.ResourceEnabled(networkingapiv1.SchemeGroupVersion.WithResource(resource)) {
		ipAddressStorage, err := ipaddressstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = ipAddressStorage
	}

	// servicecidrs
	if resource := "servicecidrs"; apiResourceConfigSource.ResourceEnabled(networkingapiv1.SchemeGroupVersion.WithResource(resource)) {
		serviceCIDRStorage, serviceCIDRStatusStorage, err := servicecidrstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = serviceCIDRStorage
		storage[resource+"/status"] = serviceCIDRStatusStorage
	}

	return storage, nil
}

// v1beta1Storage 只注册该版本仍对外提供的 IPAddress 和 ServiceCIDR，避免将仅 v1 资源错误暴露到旧版本。
func (p RESTStorageProvider) v1beta1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// ipaddress
	if resource := "ipaddresses"; apiResourceConfigSource.ResourceEnabled(networkingapiv1beta1.SchemeGroupVersion.WithResource(resource)) {
		ipAddressStorage, err := ipaddressstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = ipAddressStorage
	}

	// servicecidrs
	if resource := "servicecidrs"; apiResourceConfigSource.ResourceEnabled(networkingapiv1beta1.SchemeGroupVersion.WithResource(resource)) {
		serviceCIDRStorage, serviceCIDRStatusStorage, err := servicecidrstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = serviceCIDRStorage
		storage[resource+"/status"] = serviceCIDRStatusStorage
	}

	return storage, nil
}

func (p RESTStorageProvider) GroupName() string {
	return networking.GroupName
}
