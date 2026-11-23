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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"

	"k8s.io/kube-aggregator/pkg/apis/apiregistration"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	apiservicestorage "k8s.io/kube-aggregator/pkg/registry/apiservice/etcd"
)

// NewRESTStorage returns an APIGroupInfo object that will work against apiservice.
// APIGroupInfo 是通用 apiserver 认识的资源装配格式：把"有哪些资源、各自用哪个 storage"
// 包在一起交出去，路由和版本协商就由通用层统一处理。
// 注意 shouldServeBeta 参数目前没有被用到：这里只往 v1 下挂资源。
// 参数先留着，是为了不改动调用方的函数签名，日后若真要恢复 beta 入口不必再改一遍。
func NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter, shouldServeBeta bool) genericapiserver.APIGroupInfo {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(apiregistration.GroupName, aggregatorscheme.Scheme, metav1.ParameterCodec, aggregatorscheme.Codecs)

	storage := map[string]rest.Storage{}

	// status 是独立子资源：读写走各自的 storage，但共用同一份 etcd 数据，
	// 这样控制器回写状态时不会触发 Spec 的校验规则。
	if resource := "apiservices"; apiResourceConfigSource.ResourceEnabled(v1.SchemeGroupVersion.WithResource(resource)) {
		apiServiceREST := apiservicestorage.NewREST(aggregatorscheme.Scheme, restOptionsGetter)
		storage[resource] = apiServiceREST
		storage[resource+"/status"] = apiservicestorage.NewStatusREST(aggregatorscheme.Scheme, apiServiceREST)
	}

	// 一个资源都没启用时干脆不挂版本：让通用层把这个组当成未提供，
	// 比挂一个空 map 更干净，discovery 里也不会出现空壳版本。
	if len(storage) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap["v1"] = storage
	}

	return apiGroupInfo
}
