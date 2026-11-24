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
	authenticationv1 "k8s.io/api/authentication/v1"
	authenticationv1beta1 "k8s.io/api/authentication/v1beta1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/authentication"
	"k8s.io/kubernetes/pkg/registry/authentication/selfsubjectreview"
	"k8s.io/kubernetes/pkg/registry/authentication/tokenreview"
)

// RESTStorageProvider 持有 TokenAuthenticator 和 API Audiences，负责装配 Authentication API 组的 REST 存储。
type RESTStorageProvider struct {
	Authenticator authenticator.Request
	APIAudiences  authenticator.Audiences
}

// NewRESTStorage 创建 Authentication API 组的存储映射，并注册当前支持的 v1beta1 与 v1 资源。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	// TODO：研究如何在允许禁用该端点的同时，保证 Swagger 文档生成结果稳定。
	// if p.Authenticator == nil {
	// 	return genericapiserver.APIGroupInfo{}, false
	// }

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(authentication.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 如果新增 API 版本，必须同时在 `k8s.io/kubernetes/cmd/kube-apiserver/app/aggregator.go` 中配置对应的聚合优先级。
	// TODO：重构装配流程，将版本优先级信息直接提供给 APIGroupInfo。

	if storageMap := p.v1beta1Storage(apiResourceConfigSource, restOptionsGetter); len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[authenticationv1beta1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap := p.v1Storage(apiResourceConfigSource, restOptionsGetter); len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[authenticationv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 按资源启用配置注册 TokenReview 和 SelfSubjectReview 的 v1 REST 处理器。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) map[string]rest.Storage {
	storage := map[string]rest.Storage{}

	// TokenReview 使用注入的请求认证器验证客户端提交的 Bearer Token。
	if resource := "tokenreviews"; apiResourceConfigSource.ResourceEnabled(authenticationv1.SchemeGroupVersion.WithResource(resource)) {
		tokenReviewStorage := tokenreview.NewREST(p.Authenticator, p.APIAudiences)
		storage[resource] = tokenReviewStorage
	}
	if resource := "selfsubjectreviews"; apiResourceConfigSource.ResourceEnabled(authenticationv1.SchemeGroupVersion.WithResource(resource)) {
		selfSRStorage := selfsubjectreview.NewREST()
		storage[resource] = selfSRStorage
	}

	return storage
}

// v1beta1Storage 注册 v1beta1 版本仍提供的 SelfSubjectReview 资源。
func (p RESTStorageProvider) v1beta1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) map[string]rest.Storage {
	storage := map[string]rest.Storage{}

	// SelfSubjectReview 返回当前请求用户的身份信息，不执行额外的凭据认证。
	if resource := "selfsubjectreviews"; apiResourceConfigSource.ResourceEnabled(authenticationv1beta1.SchemeGroupVersion.WithResource(resource)) {
		selfSRStorage := selfsubjectreview.NewREST()
		storage[resource] = selfSRStorage
	}
	return storage
}

// GroupName 返回 Authentication API 组名，供 kube-apiserver 注册 APIGroup。
func (p RESTStorageProvider) GroupName() string {
	return authentication.GroupName
}
