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
	resourcev1 "k8s.io/api/resource/v1"
	resourcev1alpha3 "k8s.io/api/resource/v1alpha3"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	resourcev1beta2 "k8s.io/api/resource/v1beta2"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/resource"
	deviceclassstore "k8s.io/kubernetes/pkg/registry/resource/deviceclass/storage"
	devicetaintrulestore "k8s.io/kubernetes/pkg/registry/resource/devicetaintrule/storage"
	resourceclaimstore "k8s.io/kubernetes/pkg/registry/resource/resourceclaim/storage"
	resourceclaimtemplatestore "k8s.io/kubernetes/pkg/registry/resource/resourceclaimtemplate/storage"
	resourcepoolstatusrequeststore "k8s.io/kubernetes/pkg/registry/resource/resourcepoolstatusrequest/storage"
	resourceslicestore "k8s.io/kubernetes/pkg/registry/resource/resourceslice/storage"
)

// 即使对应 Feature Gate 未启用，也注册相关资源类型的 REST 存储；
// 这样管理员仍可访问并清理功能关闭前遗留的对象。

// RESTStorageProvider 持有 Namespace 客户端和授权器，负责装配 resource.k8s.io API Group 的 REST 存储。
type RESTStorageProvider struct {
	NamespaceClient v1.NamespaceInterface
	Authorizer      authorizer.UnconditionalAuthorizer
}

// NewRESTStorage 初始化 Resource API Group，并分别注册 v1、v1alpha3、v1beta1 和 v1beta2 版本的资源存储。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(resource.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 如果在此处增加 API 版本，务必同时在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将优先级信息直接提供给 APIGroupInfo。

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter, p.NamespaceClient); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[resourcev1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1alpha3Storage(apiResourceConfigSource, restOptionsGetter, p.NamespaceClient); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[resourcev1alpha3.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1beta1Storage(apiResourceConfigSource, restOptionsGetter, p.NamespaceClient); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[resourcev1beta1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1beta2Storage(apiResourceConfigSource, restOptionsGetter, p.NamespaceClient); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[resourcev1beta2.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 装配当前稳定版的 DeviceClass、DeviceTaintRule、ResourceClaim、ResourceClaimTemplate 和 ResourceSlice。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter, nsClient v1.NamespaceInterface) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "deviceclasses"; apiResourceConfigSource.ResourceEnabled(resourcev1.SchemeGroupVersion.WithResource(resource)) {
		deviceclassStorage, err := deviceclassstore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = deviceclassStorage
	}

	if resource := "devicetaintrules"; apiResourceConfigSource.ResourceEnabled(resourcev1.SchemeGroupVersion.WithResource(resource)) {
		deviceTaintRuleStorage, deviceTaintRuleStatusStorage, err := devicetaintrulestore.NewRESTv1(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = deviceTaintRuleStorage
		storage[resource+"/status"] = deviceTaintRuleStatusStorage
	}

	if resource := "resourceclaims"; apiResourceConfigSource.ResourceEnabled(resourcev1.SchemeGroupVersion.WithResource(resource)) {
		resourceClaimStorage, resourceClaimStatusStorage, err := resourceclaimstore.NewREST(restOptionsGetter, nsClient, p.Authorizer)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceClaimStorage
		storage[resource+"/status"] = resourceClaimStatusStorage
	}

	if resource := "resourceclaimtemplates"; apiResourceConfigSource.ResourceEnabled(resourcev1.SchemeGroupVersion.WithResource(resource)) {
		resourceClaimTemplateStorage, err := resourceclaimtemplatestore.NewREST(restOptionsGetter, nsClient)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceClaimTemplateStorage
	}

	if resource := "resourceslices"; apiResourceConfigSource.ResourceEnabled(resourcev1.SchemeGroupVersion.WithResource(resource)) {
		resourceSliceStorage, err := resourceslicestore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceSliceStorage
	}

	return storage, nil
}

// v1alpha3Storage 装配旧版 DeviceTaintRule 和 ResourcePoolStatusRequest 资源及其 Status 子资源。
func (p RESTStorageProvider) v1alpha3Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter, nsClient v1.NamespaceInterface) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "devicetaintrules"; apiResourceConfigSource.ResourceEnabled(resourcev1alpha3.SchemeGroupVersion.WithResource(resource)) {
		deviceTaintStorage, deviceTaintStatusStorage, err := devicetaintrulestore.NewRESTLegacy(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = deviceTaintStorage
		storage[resource+"/status"] = deviceTaintStatusStorage
	}

	if resource := "resourcepoolstatusrequests"; apiResourceConfigSource.ResourceEnabled(resourcev1alpha3.SchemeGroupVersion.WithResource(resource)) {
		resourcePoolStatusRequestStorage, resourcePoolStatusRequestStatusStorage, err := resourcepoolstatusrequeststore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourcePoolStatusRequestStorage
		storage[resource+"/status"] = resourcePoolStatusRequestStatusStorage
	}

	return storage, nil
}

// v1beta1Storage 装配 v1beta1 仍提供的 DeviceClass、ResourceClaim、ResourceClaimTemplate 和 ResourceSlice。
func (p RESTStorageProvider) v1beta1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter, nsClient v1.NamespaceInterface) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "deviceclasses"; apiResourceConfigSource.ResourceEnabled(resourcev1beta1.SchemeGroupVersion.WithResource(resource)) {
		deviceclassStorage, err := deviceclassstore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = deviceclassStorage
	}

	if resource := "resourceclaims"; apiResourceConfigSource.ResourceEnabled(resourcev1beta1.SchemeGroupVersion.WithResource(resource)) {
		resourceClaimStorage, resourceClaimStatusStorage, err := resourceclaimstore.NewREST(restOptionsGetter, nsClient, p.Authorizer)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceClaimStorage
		storage[resource+"/status"] = resourceClaimStatusStorage
	}

	if resource := "resourceclaimtemplates"; apiResourceConfigSource.ResourceEnabled(resourcev1beta1.SchemeGroupVersion.WithResource(resource)) {
		resourceClaimTemplateStorage, err := resourceclaimtemplatestore.NewREST(restOptionsGetter, nsClient)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceClaimTemplateStorage
	}

	if resource := "resourceslices"; apiResourceConfigSource.ResourceEnabled(resourcev1beta1.SchemeGroupVersion.WithResource(resource)) {
		resourceSliceStorage, err := resourceslicestore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceSliceStorage
	}

	return storage, nil
}

// v1beta2Storage 装配 v1beta2 的 DeviceClass、DeviceTaintRule、ResourceClaim、ResourceClaimTemplate 和 ResourceSlice。
func (p RESTStorageProvider) v1beta2Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter, nsClient v1.NamespaceInterface) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "deviceclasses"; apiResourceConfigSource.ResourceEnabled(resourcev1beta2.SchemeGroupVersion.WithResource(resource)) {
		deviceclassStorage, err := deviceclassstore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = deviceclassStorage
	}

	if resource := "devicetaintrules"; apiResourceConfigSource.ResourceEnabled(resourcev1beta2.SchemeGroupVersion.WithResource(resource)) {
		deviceTaintRuleStorage, deviceTaintRuleStatusStorage, err := devicetaintrulestore.NewRESTLegacy(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = deviceTaintRuleStorage
		storage[resource+"/status"] = deviceTaintRuleStatusStorage
	}

	if resource := "resourceclaims"; apiResourceConfigSource.ResourceEnabled(resourcev1beta2.SchemeGroupVersion.WithResource(resource)) {
		resourceClaimStorage, resourceClaimStatusStorage, err := resourceclaimstore.NewREST(restOptionsGetter, nsClient, p.Authorizer)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceClaimStorage
		storage[resource+"/status"] = resourceClaimStatusStorage
	}

	if resource := "resourceclaimtemplates"; apiResourceConfigSource.ResourceEnabled(resourcev1beta2.SchemeGroupVersion.WithResource(resource)) {
		resourceClaimTemplateStorage, err := resourceclaimtemplatestore.NewREST(restOptionsGetter, nsClient)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceClaimTemplateStorage
	}

	if resource := "resourceslices"; apiResourceConfigSource.ResourceEnabled(resourcev1beta2.SchemeGroupVersion.WithResource(resource)) {
		resourceSliceStorage, err := resourceslicestore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = resourceSliceStorage
	}

	return storage, nil
}

// GroupName 返回 Resource API Group 的名称。
func (p RESTStorageProvider) GroupName() string {
	return resource.GroupName
}
