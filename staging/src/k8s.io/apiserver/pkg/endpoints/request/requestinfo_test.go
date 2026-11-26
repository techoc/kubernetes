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

package request

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// TestGetAPIRequestInfo 表驱动地验证资源请求的 RequestInfo 解析：
// 动词、API 前缀/组/版本、命名空间、资源、子资源与名称都必须从路径和查询参数正确推导。
func TestGetAPIRequestInfo(t *testing.T) {
	namespaceAll := metav1.NamespaceAll
	successCases := []struct {
		method              string
		url                 string
		expectedVerb        string
		expectedAPIPrefix   string
		expectedAPIGroup    string
		expectedAPIVersion  string
		expectedNamespace   string
		expectedResource    string
		expectedSubresource string
		expectedName        string
		expectedParts       []string
	}{

		// 资源路径
		{MethodGet, "/api/v1/namespaces", "list", "api", "", "v1", "", "namespaces", "", "", []string{"namespaces"}},
		{MethodGet, "/api/v1/namespaces/other", "get", "api", "", "v1", "other", "namespaces", "", "other", []string{"namespaces", "other"}},

		{MethodGet, "/api/v1/namespaces/other/pods", "list", "api", "", "v1", "other", "pods", "", "", []string{"pods"}},
		{MethodGet, "/api/v1/namespaces/other/pods/foo", "get", "api", "", "v1", "other", "pods", "", "foo", []string{"pods", "foo"}},
		{MethodHead, "/api/v1/namespaces/other/pods/foo", "get", "api", "", "v1", "other", "pods", "", "foo", []string{"pods", "foo"}},
		{MethodGet, "/api/v1/pods", "list", "api", "", "v1", namespaceAll, "pods", "", "", []string{"pods"}},
		{MethodHead, "/api/v1/pods", "list", "api", "", "v1", namespaceAll, "pods", "", "", []string{"pods"}},
		{MethodGet, "/api/v1/namespaces/other/pods/foo", "get", "api", "", "v1", "other", "pods", "", "foo", []string{"pods", "foo"}},
		{MethodGet, "/api/v1/namespaces/other/pods", "list", "api", "", "v1", "other", "pods", "", "", []string{"pods"}},

		// 特殊动词
		{MethodGet, "/api/v1/proxy/namespaces/other/pods/foo", "proxy", "api", "", "v1", "other", "pods", "", "foo", []string{"pods", "foo"}},
		{MethodGet, "/api/v1/proxy/namespaces/other/pods/foo/subpath/not/a/subresource", "proxy", "api", "", "v1", "other", "pods", "", "foo", []string{"pods", "foo", "subpath", "not", "a", "subresource"}},
		{MethodGet, "/api/v1/watch/pods", "watch", "api", "", "v1", namespaceAll, "pods", "", "", []string{"pods"}},
		// watch 查询参数遵循布尔语义：1/true 为 watch，0/false 退化为 list。
		{MethodGet, "/api/v1/pods?watch=true", "watch", "api", "", "v1", namespaceAll, "pods", "", "", []string{"pods"}},
		{MethodGet, "/api/v1/pods?watch=false", "list", "api", "", "v1", namespaceAll, "pods", "", "", []string{"pods"}},
		{MethodGet, "/api/v1/watch/namespaces/other/pods", "watch", "api", "", "v1", "other", "pods", "", "", []string{"pods"}},
		{MethodGet, "/api/v1/namespaces/other/pods?watch=1", "watch", "api", "", "v1", "other", "pods", "", "", []string{"pods"}},
		{MethodGet, "/api/v1/namespaces/other/pods?watch=0", "list", "api", "", "v1", "other", "pods", "", "", []string{"pods"}},

		// 子资源识别
		{MethodGet, "/api/v1/namespaces/other/pods/foo/status", "get", "api", "", "v1", "other", "pods", "status", "foo", []string{"pods", "foo", "status"}},
		{MethodGet, "/api/v1/namespaces/other/pods/foo/proxy/subpath", "get", "api", "", "v1", "other", "pods", "proxy", "foo", []string{"pods", "foo", "proxy", "subpath"}},
		{MethodPut, "/api/v1/namespaces/other/finalize", "update", "api", "", "v1", "other", "namespaces", "finalize", "other", []string{"namespaces", "other", "finalize"}},
		{MethodPut, "/api/v1/namespaces/other/status", "update", "api", "", "v1", "other", "namespaces", "status", "other", []string{"namespaces", "other", "status"}},

		// 动词识别
		{MethodPatch, "/api/v1/namespaces/other/pods/foo", "patch", "api", "", "v1", "other", "pods", "", "foo", []string{"pods", "foo"}},
		{MethodDelete, "/api/v1/namespaces/other/pods/foo", "delete", "api", "", "v1", "other", "pods", "", "foo", []string{"pods", "foo"}},
		{MethodPost, "/api/v1/namespaces/other/pods", "create", "api", "", "v1", "other", "pods", "", "", []string{"pods"}},

		// deletecollection 动词识别
		// 集合路径上的 DELETE 是 deletecollection，而不是 delete。
		{MethodDelete, "/api/v1/nodes", "deletecollection", "api", "", "v1", "", "nodes", "", "", []string{"nodes"}},
		{MethodDelete, "/api/v1/namespaces", "deletecollection", "api", "", "v1", "", "namespaces", "", "", []string{"namespaces"}},
		{MethodDelete, "/api/v1/namespaces/other/pods", "deletecollection", "api", "", "v1", "other", "pods", "", "", []string{"pods"}},
		{MethodDelete, "/apis/extensions/v1/namespaces/other/pods", "deletecollection", "api", "extensions", "v1", "other", "pods", "", "", []string{"pods"}},

		// API 组识别
		{MethodPost, "/apis/extensions/v1/namespaces/other/pods", "create", "api", "extensions", "v1", "other", "pods", "", "", []string{"pods"}},

		// API 版本识别
		{MethodPost, "/apis/extensions/v1beta3/namespaces/other/pods", "create", "api", "extensions", "v1beta3", "other", "pods", "", "", []string{"pods"}},
	}

	resolver := newTestRequestInfoResolver()

	for _, successCase := range successCases {
		ctx := t.Context()
		req, _ := http.NewRequestWithContext(ctx, successCase.method, successCase.url, nil)

		apiRequestInfo, err := resolver.NewRequestInfo(req)
		if err != nil {
			t.Errorf("Unexpected error for url: %s %v", successCase.url, err)
		}
		if !apiRequestInfo.IsResourceRequest {
			t.Errorf("Expected resource request")
		}
		if successCase.expectedVerb != apiRequestInfo.Verb {
			t.Errorf("Unexpected verb for url: %s, expected: %s, actual: %s", successCase.url, successCase.expectedVerb, apiRequestInfo.Verb)
		}
		if successCase.expectedAPIVersion != apiRequestInfo.APIVersion {
			t.Errorf("Unexpected apiVersion for url: %s, expected: %s, actual: %s", successCase.url, successCase.expectedAPIVersion, apiRequestInfo.APIVersion)
		}
		if successCase.expectedNamespace != apiRequestInfo.Namespace {
			t.Errorf("Unexpected namespace for url: %s, expected: %s, actual: %s", successCase.url, successCase.expectedNamespace, apiRequestInfo.Namespace)
		}
		if successCase.expectedResource != apiRequestInfo.Resource {
			t.Errorf("Unexpected resource for url: %s, expected: %s, actual: %s", successCase.url, successCase.expectedResource, apiRequestInfo.Resource)
		}
		if successCase.expectedSubresource != apiRequestInfo.Subresource {
			t.Errorf("Unexpected resource for url: %s, expected: %s, actual: %s", successCase.url, successCase.expectedSubresource, apiRequestInfo.Subresource)
		}
		if successCase.expectedName != apiRequestInfo.Name {
			t.Errorf("Unexpected name for url: %s, expected: %s, actual: %s", successCase.url, successCase.expectedName, apiRequestInfo.Name)
		}
		if !reflect.DeepEqual(successCase.expectedParts, apiRequestInfo.Parts) {
			t.Errorf("Unexpected parts for url: %s, expected: %v, actual: %v", successCase.url, successCase.expectedParts, apiRequestInfo.Parts)
		}
	}

	// 结构不完整的 API 路径不应被误判为资源请求。
	errorCases := map[string]string{
		"no resource path":            "/",
		"just apiversion":             "/api/version/",
		"just prefix, group, version": "/apis/group/version/",
		"apiversion with no resource": "/api/version/",
		"bad prefix":                  "/badprefix/version/resource",
		"missing api group":           "/apis/version/resource",
	}
	for k, v := range errorCases {
		ctx := t.Context()
		req, err := http.NewRequestWithContext(ctx, MethodGet, v, nil)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		apiRequestInfo, err := resolver.NewRequestInfo(req)
		if err != nil {
			t.Errorf("%s: Unexpected error %v", k, err)
		}
		if apiRequestInfo.IsResourceRequest {
			t.Errorf("%s: expected non-resource request", k)
		}
	}
}

// TestGetNonAPIRequestInfo 验证 IsResourceRequest 判定：只有路径段足够且前缀合法时
// 才视为资源请求。
func TestGetNonAPIRequestInfo(t *testing.T) {
	tests := map[string]struct {
		url      string
		expected bool
	}{
		"simple groupless":  {"/api/version/resource", true},
		"simple group":      {"/apis/group/version/resource/name/subresource", true},
		"more steps":        {"/api/version/resource/name/subresource", true},
		"group list":        {"/apis/batch/v1/job", true},
		"group get":         {"/apis/batch/v1/job/foo", true},
		"group subresource": {"/apis/batch/v1/job/foo/scale", true},

		// 以下路径缺少资源段或前缀不合法。
		"bad root":                     {"/not-api/version/resource", false},
		"group without enough steps":   {"/apis/extensions/v1beta1", false},
		"group without enough steps 2": {"/apis/extensions/v1beta1/", false},
		"not enough steps":             {"/api/version", false},
		"one step":                     {"/api", false},
		"zero step":                    {"/", false},
		"empty":                        {"", false},
	}

	resolver := newTestRequestInfoResolver()

	for testName, tc := range tests {
		ctx := t.Context()
		req, _ := http.NewRequestWithContext(ctx, MethodGet, tc.url, nil)

		apiRequestInfo, err := resolver.NewRequestInfo(req)
		if err != nil {
			t.Errorf("%s: Unexpected error %v", testName, err)
		}
		if e, a := tc.expected, apiRequestInfo.IsResourceRequest; e != a {
			t.Errorf("%s: expected %v, actual %v", testName, e, a)
		}
	}
}

// newTestRequestInfoResolver 构造最小化的解析器：仅识别 api/apis 前缀，
// 其中 api 为无组前缀。
func newTestRequestInfoResolver() *RequestInfoFactory {
	return &RequestInfoFactory{
		APIPrefixes:          sets.NewString("api", "apis"),
		GrouplessAPIPrefixes: sets.NewString("api"),
	}
}

// TestSelectorParsing 验证列表/删除集合类请求的选择器解析与名称提取：
// 只有 list/watch/deletecollection 会解析选择器；重复参数以第一个为准；
// 选择器非法时保留原始字符串并返回错误，但名称不再提取。
func TestSelectorParsing(t *testing.T) {
	tests := []struct {
		name                  string
		method                string
		url                   string
		expectedName          string
		expectedErr           error
		expectedVerb          string
		expectedFieldSelector string
		expectedLabelSelector string
	}{
		{
			name:                  "no selector",
			method:                MethodGet,
			url:                   "/apis/group/version/resource",
			expectedVerb:          "list",
			expectedFieldSelector: "",
		},
		{
			// metadata.name 精确匹配会被提升为 RequestInfo.Name。
			name:                  "metadata.name selector",
			method:                MethodGet,
			url:                   "/apis/group/version/resource?fieldSelector=metadata.name=name1",
			expectedName:          "name1",
			expectedVerb:          "list",
			expectedFieldSelector: "metadata.name=name1",
		},
		{
			name:                  "metadata.name selector with watch",
			method:                MethodGet,
			url:                   "/apis/group/version/resource?watch=true&fieldSelector=metadata.name=name1",
			expectedName:          "name1",
			expectedVerb:          "watch",
			expectedFieldSelector: "metadata.name=name1",
		},
		{
			name:                  "random selector",
			method:                MethodGet,
			url:                   "/apis/group/version/resource?fieldSelector=foo=bar&labelSelector=baz=qux",
			expectedName:          "",
			expectedVerb:          "list",
			expectedFieldSelector: "foo=bar",
			expectedLabelSelector: "baz=qux",
		},
		{
			// 语法错误的选择器不得被静默丢弃，必须返回错误供调用方拒绝请求。
			name:                  "invalid selector with metadata.name",
			method:                MethodGet,
			url:                   "/apis/group/version/resource?fieldSelector=metadata.name=name1,foo",
			expectedName:          "",
			expectedErr:           fmt.Errorf("invalid selector"),
			expectedVerb:          "list",
			expectedFieldSelector: "metadata.name=name1,foo",
		},
		{
			name:                  "invalid selector with metadata.name with watch",
			method:                MethodGet,
			url:                   "/apis/group/version/resource?fieldSelector=metadata.name=name1,foo&watch=true",
			expectedName:          "",
			expectedErr:           fmt.Errorf("invalid selector"),
			expectedVerb:          "watch",
			expectedFieldSelector: "metadata.name=name1,foo",
		},
		{
			name:                  "invalid selector with metadata.name with watch false",
			method:                MethodGet,
			url:                   "/apis/group/version/resource?fieldSelector=metadata.name=name1,foo&watch=false",
			expectedName:          "",
			expectedErr:           fmt.Errorf("invalid selector"),
			expectedVerb:          "list",
			expectedFieldSelector: "metadata.name=name1,foo",
		},
		{
			// deletecollection 同样需要选择器来界定删除范围。
			name:                  "selector on deletecollection is honored",
			method:                MethodDelete,
			url:                   "/apis/group/version/resource?fieldSelector=foo=bar&labelSelector=baz=qux",
			expectedName:          "",
			expectedVerb:          "deletecollection",
			expectedFieldSelector: "foo=bar",
			expectedLabelSelector: "baz=qux",
		},
		{
			// 重复参数取第一个，避免结果依赖 Go map 遍历顺序。
			name:                  "selector on repeated param matches parsed param",
			method:                MethodGet,
			url:                   "/apis/group/version/resource?fieldSelector=metadata.name=foo&fieldSelector=metadata.name=bar&labelSelector=foo=bar&labelSelector=foo=baz",
			expectedName:          "foo",
			expectedVerb:          "list",
			expectedFieldSelector: "metadata.name=foo",
			expectedLabelSelector: "foo=bar",
		},
		{
			// 单对象 GET 上的选择器无意义，直接忽略。
			name:                  "selector on other verb is ignored",
			method:                MethodGet,
			url:                   "/apis/group/version/resource/name?fieldSelector=foo=bar&labelSelector=foo=bar",
			expectedName:          "name",
			expectedVerb:          "get",
			expectedFieldSelector: "",
			expectedLabelSelector: "",
		},
		{
			// 已弃用的根路径 watch 形式不解析选择器，仅为兼容旧客户端保留。
			name:                  "selector on deprecated root type watch is not parsed",
			method:                MethodGet,
			url:                   "/apis/group/version/watch/resource?fieldSelector=metadata.name=foo&labelSelector=foo=bar",
			expectedName:          "",
			expectedVerb:          "watch",
			expectedFieldSelector: "",
			expectedLabelSelector: "",
		},
		{
			name:                  "selector on deprecated root item watch is not parsed",
			method:                MethodGet,
			url:                   "/apis/group/version/watch/resource/name?fieldSelector=metadata.name=foo&labelSelector=foo=bar",
			expectedName:          "name",
			expectedVerb:          "watch",
			expectedFieldSelector: "",
			expectedLabelSelector: "",
		},
	}

	resolver := newTestRequestInfoResolver()

	for _, tc := range tests {
		ctx := t.Context()
		req, _ := http.NewRequestWithContext(ctx, tc.method, tc.url, nil)

		apiRequestInfo, err := resolver.NewRequestInfo(req)
		if err != nil {
			if tc.expectedErr == nil || !strings.Contains(err.Error(), tc.expectedErr.Error()) {
				t.Errorf("%s: Unexpected error %v", tc.name, err)
			}
		}
		if e, a := tc.expectedName, apiRequestInfo.Name; e != a {
			t.Errorf("%s: expected %v, actual %v", tc.name, e, a)
		}
		if e, a := tc.expectedVerb, apiRequestInfo.Verb; e != a {
			t.Errorf("%s: expected verb %v, actual %v", tc.name, e, a)
		}
		if e, a := tc.expectedFieldSelector, apiRequestInfo.FieldSelector; e != a {
			t.Errorf("%s: expected fieldSelector %v, actual %v", tc.name, e, a)
		}
		if e, a := tc.expectedLabelSelector, apiRequestInfo.LabelSelector; e != a {
			t.Errorf("%s: expected labelSelector %v, actual %v", tc.name, e, a)
		}
	}
}
