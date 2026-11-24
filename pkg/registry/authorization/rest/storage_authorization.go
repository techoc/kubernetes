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
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/authorization"
	"k8s.io/kubernetes/pkg/registry/authorization/localsubjectaccessreview"
	"k8s.io/kubernetes/pkg/registry/authorization/selfsubjectaccessreview"
	"k8s.io/kubernetes/pkg/registry/authorization/selfsubjectrulesreview"
	"k8s.io/kubernetes/pkg/registry/authorization/subjectaccessreview"
)

// RESTStorageProvider 持有授权决策器和规则解析器，负责装配 Authorization API 组的 REST 存储。
type RESTStorageProvider struct {
	Authorizer   authorizer.UnconditionalAuthorizer
	RuleResolver authorizer.RuleResolver
}

// NewRESTStorage 创建 Authorization API 组的存储映射；未配置 Authorizer 时不注册该 API 组。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	if p.Authorizer == nil {
		return genericapiserver.APIGroupInfo{}, nil
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(authorization.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 如果新增 API 版本，必须同时在 `k8s.io/kubernetes/cmd/kube-apiserver/app/aggregator.go` 中配置对应的聚合优先级。
	// TODO：重构装配流程，将版本优先级信息直接提供给 APIGroupInfo。

	if storageMap := p.v1Storage(apiResourceConfigSource, restOptionsGetter); len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[authorizationv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 根据资源启用配置装配 SubjectAccessReview、SelfSubjectAccessReview、LocalSubjectAccessReview 和 SelfSubjectRulesReview。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) map[string]rest.Storage {
	storage := map[string]rest.Storage{}

	// 注册可由调用者指定目标用户的 SubjectAccessReview。
	if resource := "subjectaccessreviews"; apiResourceConfigSource.ResourceEnabled(authorizationv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = subjectaccessreview.NewREST(p.Authorizer, legacyscheme.Scheme)
	}

	// 注册针对当前请求用户执行授权检查的 SelfSubjectAccessReview。
	if resource := "selfsubjectaccessreviews"; apiResourceConfigSource.ResourceEnabled(authorizationv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = selfsubjectaccessreview.NewREST(p.Authorizer, legacyscheme.Scheme)
	}

	// 注册受命名空间约束、用于检查指定用户资源访问权限的 LocalSubjectAccessReview。
	if resource := "localsubjectaccessreviews"; apiResourceConfigSource.ResourceEnabled(authorizationv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = localsubjectaccessreview.NewREST(p.Authorizer, legacyscheme.Scheme)
	}

	// 注册用于查询当前用户在指定命名空间内可见授权规则的 SelfSubjectRulesReview。
	if resource := "selfsubjectrulesreviews"; apiResourceConfigSource.ResourceEnabled(authorizationv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = selfsubjectrulesreview.NewREST(p.RuleResolver)
	}

	return storage
}

// GroupName 返回 Authorization API 组名，供 kube-apiserver 注册 APIGroup。
func (p RESTStorageProvider) GroupName() string {
	return authorization.GroupName
}
