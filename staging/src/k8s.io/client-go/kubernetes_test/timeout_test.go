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

package kubernetes_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	manualfake "k8s.io/client-go/rest/fake"
	"k8s.io/utils/dump"
)

// TestListTimeout 验证 List 与 Watch 都会把 ListOptions.TimeoutSeconds 翻译成查询参数 "timeout"。
// 这两个操作共享参数编码逻辑，因此用同一个假 REST 客户端分别触发，确保不会有分支漏掉超时参数。
func TestListTimeout(t *testing.T) {
	// 手写一个最小 RESTClient：只要收到 timeout!=21s 的请求就直接让测试失败。
	fakeClient := &manualfake.RESTClient{
		GroupVersion:         appsv1.SchemeGroupVersion,
		NegotiatedSerializer: scheme.Codecs,
		Client: manualfake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
			if req.URL.Query().Get("timeout") != "21s" {
				t.Fatal(dump.Pretty(req.URL.Query()))
			}
			// 返回 404 即可：本测试只关心请求构造，不关心响应内容。
			return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(&bytes.Buffer{})}, nil
		}),
	}
	clientConfig := &rest.Config{
		APIPath: "/apis",
		ContentConfig: rest.ContentConfig{
			NegotiatedSerializer: scheme.Codecs,
			GroupVersion:         &appsv1.SchemeGroupVersion,
		},
	}
	restClient, _ := rest.RESTClientFor(clientConfig)
	// 用假 HTTP 客户端替换默认传输，以拦截实际请求。
	restClient.Client = fakeClient.Client
	realClient := kubernetes.New(restClient)

	timeout := int64(21)
	realClient.AppsV1().DaemonSets("").List(context.TODO(), metav1.ListOptions{TimeoutSeconds: &timeout})
	realClient.AppsV1().DaemonSets("").Watch(context.TODO(), metav1.ListOptions{TimeoutSeconds: &timeout})
}
