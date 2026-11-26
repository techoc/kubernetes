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

package apiserver

import (
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"

	apiregistrationv1api "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregistrationv1apihelper "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper"
	apiregistrationv1beta1api "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1"
	listers "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1"
)

// apisHandler 服务 `/apis` 端点。
// 注册为 filter 而不是普通路由，避免与显式注册的其他端点冲突。
//
// 中文补充：/apis 的组列表是动态的，来自 APIService 对象而不是静态路由表；
// 用 filter（UnlistedHandle）注册，就不会和某个具体组显式注册的 handler 抢路径。
type apisHandler struct {
	codecs         serializer.CodecFactory
	lister         listers.APIServiceLister
	discoveryGroup metav1.APIGroup
}

func discoveryGroup(enabledVersions sets.String) metav1.APIGroup {
	// 中文补充：聚合层自己的组必须出现在发现列表里，而且 v1 恒为首选版本；
	// v1beta1 是否列出取决于它被启用了没有，不能凭空宣称支持。
	retval := metav1.APIGroup{
		Name: apiregistrationv1api.GroupName,
		Versions: []metav1.GroupVersionForDiscovery{
			{
				GroupVersion: apiregistrationv1api.SchemeGroupVersion.String(),
				Version:      apiregistrationv1api.SchemeGroupVersion.Version,
			},
		},
		PreferredVersion: metav1.GroupVersionForDiscovery{
			GroupVersion: apiregistrationv1api.SchemeGroupVersion.String(),
			Version:      apiregistrationv1api.SchemeGroupVersion.Version,
		},
	}

	if enabledVersions.Has(apiregistrationv1beta1api.SchemeGroupVersion.Version) {
		retval.Versions = append(retval.Versions, metav1.GroupVersionForDiscovery{
			GroupVersion: apiregistrationv1beta1api.SchemeGroupVersion.String(),
			Version:      apiregistrationv1beta1api.SchemeGroupVersion.Version,
		})
	}

	return retval
}

func (r *apisHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	discoveryGroupList := &metav1.APIGroupList{
		// 始终把自己的 API 组放在列表首位：它不会有对应的 APIService，
		// 而聚合 API 是核心入口，排在前面能让我们的名字优先被匹配。
		Groups: []metav1.APIGroup{r.discoveryGroup},
	}

	apiServices, err := r.lister.List(labels.Everything())
	if err != nil {
		// 中文补充：lister 读的是本地缓存，失败几乎只可能是缓存损坏，
		// 这时给 500 比返回半截发现文档更诚实，客户端可以稍后重试。
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	apiServicesByGroup := apiregistrationv1apihelper.SortedByGroupAndVersion(apiServices)
	for _, apiGroupServers := range apiServicesByGroup {
		// 跳过 legacy 组。
		if len(apiGroupServers[0].Spec.Group) == 0 {
			continue
		}
		discoveryGroup := convertToDiscoveryAPIGroup(apiGroupServers)
		if discoveryGroup != nil {
			discoveryGroupList.Groups = append(discoveryGroupList.Groups, *discoveryGroup)
		}
	}

	responsewriters.WriteObjectNegotiated(r.codecs, negotiation.DefaultEndpointRestrictions, schema.GroupVersion{}, w, req, http.StatusOK, discoveryGroupList, false)
}

// convertToDiscoveryAPIGroup 把同一组内的 APIService 转成 discovery 可用的 APIGroup。
// 若没有可用的 Service 则返回 nil。
//
// 中文补充：输入理论上只含一个组，但再去一次排序是为了直接复用"组内版本优先级"
// 的排序规则；取 [0] 即该组的全部版本，排好序后第一个自然成为 PreferredVersion。
func convertToDiscoveryAPIGroup(apiServices []*apiregistrationv1api.APIService) *metav1.APIGroup {
	apiServicesByGroup := apiregistrationv1apihelper.SortedByGroupAndVersion(apiServices)[0]

	var discoveryGroup *metav1.APIGroup

	for _, apiService := range apiServicesByGroup {
		// 首个有效的 APIService 作为默认版本。
		if discoveryGroup == nil {
			discoveryGroup = &metav1.APIGroup{
				Name: apiService.Spec.Group,
				PreferredVersion: metav1.GroupVersionForDiscovery{
					GroupVersion: apiService.Spec.Group + "/" + apiService.Spec.Version,
					Version:      apiService.Spec.Version,
				},
			}
		}

		discoveryGroup.Versions = append(discoveryGroup.Versions,
			metav1.GroupVersionForDiscovery{
				GroupVersion: apiService.Spec.Group + "/" + apiService.Spec.Version,
				Version:      apiService.Spec.Version,
			},
		)
	}

	return discoveryGroup
}

// apiGroupHandler 服务 `/apis/<group>` 端点。
//
// 中文补充：和 /apis 不同，这里针对单个组；如果本组没有任何 APIService，
// 就把请求委托给 delegate——因为该组可能由底层 apiserver 直接提供，
// 聚合层只是路过，不该擅自返回 404。
type apiGroupHandler struct {
	codecs    serializer.CodecFactory
	groupName string

	lister listers.APIServiceLister

	delegate http.Handler
}

func (r *apiGroupHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	apiServices, err := r.lister.List(labels.Everything())
	// 中文补充：lister 偶尔会返回带 HTTP 语义的 StatusError，
	// 原样透传状态码比统一压成 500 更贴近客户端预期（例如 403）。
	if statusErr, ok := err.(*apierrors.StatusError); ok {
		responsewriters.WriteRawJSON(int(statusErr.Status().Code), statusErr.Status(), w)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	apiServicesForGroup := []*apiregistrationv1api.APIService{}
	for _, apiService := range apiServices {
		if apiService.Spec.Group == r.groupName {
			apiServicesForGroup = append(apiServicesForGroup, apiService)
		}
	}

	if len(apiServicesForGroup) == 0 {
		// 中文补充：没有对应的 APIService，说明这个组不属于聚合层管辖，
		// 交回 delegate 处理；这覆盖了 APIService 刚被删除、路由还没撤销的窗口。
		r.delegate.ServeHTTP(w, req)
		return
	}

	discoveryGroup := convertToDiscoveryAPIGroup(apiServicesForGroup)
	if discoveryGroup == nil {
		// 中文补充：走到这里说明有 APIService 但没有可展示的版本，
		// 返回 404 而非 500，表示"这个组当前没有发现信息"。
		http.Error(w, "", http.StatusNotFound)
		return
	}
	responsewriters.WriteObjectNegotiated(r.codecs, negotiation.DefaultEndpointRestrictions, schema.GroupVersion{}, w, req, http.StatusOK, discoveryGroup, false)
}
