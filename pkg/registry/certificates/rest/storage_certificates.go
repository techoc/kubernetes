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
	"fmt"

	certificatesapiv1 "k8s.io/api/certificates/v1"
	certificatesapiv1alpha1 "k8s.io/api/certificates/v1alpha1"
	certificatesapiv1beta1 "k8s.io/api/certificates/v1beta1"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/features"
	certificatestore "k8s.io/kubernetes/pkg/registry/certificates/certificates/storage"
	clustertrustbundlestore "k8s.io/kubernetes/pkg/registry/certificates/clustertrustbundle/storage"
	podcertificaterequeststore "k8s.io/kubernetes/pkg/registry/certificates/podcertificaterequest/storage"
	"k8s.io/utils/clock"
)

// RESTStorageProvider 根据 API 资源启用配置和特性门控，装配 Certificates API Group 的 REST 存储。
type RESTStorageProvider struct {
	Authorizer authorizer.UnconditionalAuthorizer
	Generic    bool
}

// NewRESTStorage 初始化 Certificates API Group，并按 v1、v1beta1、v1alpha1 的顺序注册已启用资源。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	if p.Authorizer == nil {
		return genericapiserver.APIGroupInfo{}, fmt.Errorf("certificates REST storage requires an authorizer")
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(certificates.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 若在此处增加 API 版本，还必须在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将版本优先级信息直接提供给 APIGroupInfo。

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[certificatesapiv1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1beta1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[certificatesapiv1beta1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1alpha1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[certificatesapiv1alpha1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 构建 certificates/v1 的 REST Storage，并分别注册资源及其支持的子资源。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "certificatesigningrequests"; apiResourceConfigSource.ResourceEnabled(certificatesapiv1.SchemeGroupVersion.WithResource(resource)) {
		csrStorage, csrStatusStorage, csrApprovalStorage, err := certificatestore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = csrStorage
		storage[resource+"/status"] = csrStatusStorage
		storage[resource+"/approval"] = csrApprovalStorage
	}

	if resource := "clustertrustbundles"; apiResourceConfigSource.ResourceEnabled(certificatesapiv1.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.ClusterTrustBundle) {
			bundleStorage, err := clustertrustbundlestore.NewREST(restOptionsGetter)
			if err != nil {
				return nil, err
			}
			storage[resource] = bundleStorage
		} else {
			klog.Warning("ClusterTrustBundle storage is disabled because the ClusterTrustBundle feature gate is disabled")
		}
	}

	if resource := "podcertificaterequests"; apiResourceConfigSource.ResourceEnabled(certificatesapiv1.SchemeGroupVersion.WithResource(resource)) && !p.Generic {
		if utilfeature.DefaultFeatureGate.Enabled(features.PodCertificateRequest) {
			pcrStorage, pcrStatusStorage, err := podcertificaterequeststore.NewREST(restOptionsGetter, p.Authorizer, clock.RealClock{})
			if err != nil {
				return nil, err
			}
			storage[resource] = pcrStorage
			storage[resource+"/status"] = pcrStatusStorage
		} else {
			klog.Warning("PodCertificateRequest storage is disabled because the PodCertificateRequest feature gate is disabled")
		}
	}

	return storage, nil
}

// v1beta1Storage 构建 certificates/v1beta1 中受支持且已启用的资源存储。
func (p RESTStorageProvider) v1beta1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "clustertrustbundles"; apiResourceConfigSource.ResourceEnabled(certificatesapiv1beta1.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.ClusterTrustBundle) {
			bundleStorage, err := clustertrustbundlestore.NewREST(restOptionsGetter)
			if err != nil {
				return nil, err
			}
			storage[resource] = bundleStorage
		} else {
			klog.Warning("ClusterTrustBundle storage is disabled because the ClusterTrustBundle feature gate is disabled")
		}
	}

	if resource := "podcertificaterequests"; apiResourceConfigSource.ResourceEnabled(certificatesapiv1beta1.SchemeGroupVersion.WithResource(resource)) && !p.Generic {
		if utilfeature.DefaultFeatureGate.Enabled(features.PodCertificateRequest) {
			pcrStorage, pcrStatusStorage, err := podcertificaterequeststore.NewREST(restOptionsGetter, p.Authorizer, clock.RealClock{})
			if err != nil {
				return nil, err
			}
			storage[resource] = pcrStorage
			storage[resource+"/status"] = pcrStatusStorage
		} else {
			klog.Warning("PodCertificateRequest storage is disabled because the PodCertificateRequest feature gate is disabled")
		}
	}

	return storage, nil
}

// v1alpha1Storage 构建 certificates/v1alpha1 中受支持且已启用的资源存储。
func (p RESTStorageProvider) v1alpha1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "clustertrustbundles"; apiResourceConfigSource.ResourceEnabled(certificatesapiv1alpha1.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.ClusterTrustBundle) {
			bundleStorage, err := clustertrustbundlestore.NewREST(restOptionsGetter)
			if err != nil {
				return nil, err
			}
			storage[resource] = bundleStorage
		} else {
			klog.Warning("ClusterTrustBundle storage is disabled because the ClusterTrustBundle feature gate is disabled")
		}
	}

	return storage, nil
}

// GroupName 返回 Certificates API Group 的名称。
func (p RESTStorageProvider) GroupName() string {
	return certificates.GroupName
}
