/*
Copyright 2015 The Kubernetes Authors.

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

package tests

import (
	"net/http/httptest"
	"net/url"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	. "k8s.io/client-go/tools/cache"
	utiltesting "k8s.io/client-go/util/testing"
)

// parseSelectorOrDie 解析字段选择器；用于构造测试用例中的静态选择器，
// 因此解析失败属于编程错误，直接 panic 让问题尽早暴露。
func parseSelectorOrDie(s string) fields.Selector {
	selector, err := fields.ParseSelector(s)
	if err != nil {
		panic(err)
	}
	return selector
}

// buildQueryValues 复制一份 URL 查询参数。通过副本构造期望值，
// 可以避免原始 url.Values 被 url.Values.Encode 等过程意外改动而污染其他用例。
func buildQueryValues(query url.Values) url.Values {
	v := url.Values{}
	for key, values := range query {
		for _, value := range values {
			v.Add(key, value)
		}
	}

	return v
}

// buildLocation 把资源路径与查询参数拼成完整的 URL，供 FakeHandler 校验请求。
func buildLocation(resourcePath string, query url.Values) string {
	return resourcePath + "?" + query.Encode()
}

// TestListWatchesCanList 验证 ListWatch.List 会针对不同的资源、命名空间与字段选择器
// 生成正确的请求地址。 FakeHandler 返回 500，故这里只关心请求是否正确发出。
func TestListWatchesCanList(t *testing.T) {
	fieldSelectorQueryParamName := metav1.FieldSelectorQueryParam("v1")
	table := []struct {
		desc          string
		location      string
		resource      string
		namespace     string
		fieldSelector fields.Selector
	}{
		{
			desc:          "node",
			location:      "/api/v1/nodes",
			resource:      "nodes",
			namespace:     metav1.NamespaceAll,
			fieldSelector: parseSelectorOrDie(""),
		},
		{
			desc: "pod with 'assigned' field selector",
			location: buildLocation(
				"/api/v1/pods",
				buildQueryValues(url.Values{fieldSelectorQueryParamName: []string{"spec.host="}})),
			resource:      "pods",
			namespace:     metav1.NamespaceAll,
			fieldSelector: fields.Set{"spec.host": ""}.AsSelector(),
		},
		{
			desc: "pod in namespace 'foo'",
			location: buildLocation(
				"/api/v1/namespaces/foo/pods",
				buildQueryValues(url.Values{fieldSelectorQueryParamName: []string{"spec.host="}})),
			resource:      "pods",
			namespace:     "foo",
			fieldSelector: fields.Set{"spec.host": ""}.AsSelector(),
		},
	}
	for _, item := range table {
		t.Run(item.desc, func(t *testing.T) {
			// 返回 500 表明这里不依赖真实响应，仅让 FakeHandler 记录请求。
			handler := utiltesting.FakeHandler{
				StatusCode:   500,
				ResponseBody: "",
				T:            t,
			}
			server := httptest.NewServer(&handler)
			defer server.Close()
			client := clientset.NewForConfigOrDie(&restclient.Config{Host: server.URL, ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			lw := NewListWatchFromClient(client.CoreV1().RESTClient(), item.resource, item.namespace, item.fieldSelector)
			// 禁用分块列表，避免分页参数干扰本用例对请求 URL 的精确校验。
			lw.DisableChunking = true
			// 本测试只验证发出的请求是否正确，因此忽略返回值与错误。
			_, _ = lw.List(metav1.ListOptions{})
			handler.ValidateRequest(t, item.location, "GET", nil)
		})
	}
}

// TestListWatchesCanWatch 验证 ListWatch.Watch 会针对不同的资源、命名空间、
// 字段选择器与 resourceVersion 生成正确的 watch 请求地址，
// 尤其关注 watch=true 与 resourceVersion 参数是否按预期出现。
func TestListWatchesCanWatch(t *testing.T) {
	fieldSelectorQueryParamName := metav1.FieldSelectorQueryParam("v1")
	table := []struct {
		desc          string
		rv            string
		location      string
		resource      string
		namespace     string
		fieldSelector fields.Selector
	}{
		{
			desc: "node without rv",
			location: buildLocation(
				"/api/v1/nodes",
				buildQueryValues(url.Values{"watch": []string{"true"}})),
			rv:            "",
			resource:      "nodes",
			namespace:     metav1.NamespaceAll,
			fieldSelector: parseSelectorOrDie(""),
		},
		{
			desc: "node with rv",
			location: buildLocation(
				"/api/v1/nodes",
				buildQueryValues(url.Values{"resourceVersion": []string{"42"}, "watch": []string{"true"}})),
			rv:            "42",
			resource:      "nodes",
			namespace:     metav1.NamespaceAll,
			fieldSelector: parseSelectorOrDie(""),
		},
		{
			desc: "pod with 'assigned' field selector",
			location: buildLocation(
				"/api/v1/pods",
				buildQueryValues(url.Values{fieldSelectorQueryParamName: []string{"spec.host="}, "resourceVersion": []string{"0"}, "watch": []string{"true"}})),
			rv:            "0",
			resource:      "pods",
			namespace:     metav1.NamespaceAll,
			fieldSelector: fields.Set{"spec.host": ""}.AsSelector(),
		},
		{
			desc: "pod with namespace foo and assigned field selector",
			location: buildLocation(
				"/api/v1/namespaces/foo/pods",
				buildQueryValues(url.Values{fieldSelectorQueryParamName: []string{"spec.host="}, "resourceVersion": []string{"0"}, "watch": []string{"true"}})),
			rv:            "0",
			resource:      "pods",
			namespace:     "foo",
			fieldSelector: fields.Set{"spec.host": ""}.AsSelector(),
		},
	}

	for _, item := range table {
		t.Run(item.desc, func(t *testing.T) {
			// 同样返回 500：watch 建立本身会失败，但请求已被 FakeHandler 记录，
			// 足以断言 URL 的构造是否符合预期。
			handler := utiltesting.FakeHandler{
				StatusCode:   500,
				ResponseBody: "",
				T:            t,
			}
			server := httptest.NewServer(&handler)
			defer server.Close()
			client := clientset.NewForConfigOrDie(&restclient.Config{Host: server.URL, ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			lw := NewListWatchFromClient(client.CoreV1().RESTClient(), item.resource, item.namespace, item.fieldSelector)
			// 本测试只验证发出的请求是否正确，因此忽略 watch 接口与错误。
			_, _ = lw.Watch(metav1.ListOptions{ResourceVersion: item.rv})
			handler.ValidateRequest(t, item.location, "GET", nil)
		})
	}
}
