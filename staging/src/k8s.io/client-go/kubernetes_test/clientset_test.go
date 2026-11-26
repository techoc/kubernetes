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

// Package kubernetes_test 端到端验证 kubernetes clientset 的构造与 informer 行为，
// 特别是 watch-list（SendInitialEvents）语义的支持判定与回退。
package kubernetes_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/watchlist"
)

// TestDoesClientSupportWatchListSemanticsForKubeClient 确认类型化 clientset 被标记为支持
// watch-list 语义（其底层 REST 客户端能正确处理 SendInitialEvents）。
func TestDoesClientSupportWatchListSemanticsForKubeClient(t *testing.T) {
	target, err := kubernetes.NewForConfig(&rest.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if watchlist.DoesClientNotSupportWatchListSemantics(target) {
		t.Fatalf("Kubernetes client should support WatchList semantics")
	}
}

// TestWatchListSemanticsSimple 用最小 watch 端点验证：开启 WatchListClient 后，
// informer 会先发起带 watch 参数的流式请求，并在收到 initial-events-end Bookmark 后完成同步。
func TestWatchListSemanticsSimple(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)

	// 服务端只允许一次 watch 请求，并立即返回结束 Bookmark；
	// 若客户端改走 List，则测试失败。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, ok := req.URL.Query()["watch"]; !ok {
			t.Errorf("expected a watch request, params: %v", req.URL.Query())
			http.Error(w, fmt.Errorf("unexpected request").Error(), http.StatusInternalServerError)
			return
		}

		obj := &appsv1.Deployment{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
			},
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					metav1.InitialEventsAnnotationKey: "true",
				},
			},
		}
		rawObj, err := json.Marshal(obj)
		if err != nil {
			t.Errorf("failed to marshal rawObj: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		watchEvent := &metav1.WatchEvent{
			Type:   string(watch.Bookmark),
			Object: runtime.RawExtension{Raw: rawObj},
		}
		rawRsp, err := json.Marshal(watchEvent)
		if err != nil {
			t.Errorf("failed to marshal watchEvent: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write(rawRsp)
		if err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	cfg := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	factory := informers.NewSharedInformerFactory(client, 0)
	target := factory.Apps().V1().Deployments().Informer()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), target.HasSynced) {
		t.Fatalf("failed to wait for caches to sync")
	}
}

func TestUnSupportWatchListSemantics(t *testing.T) {
	// TestUnSupportWatchListSemantics 验证 fake clientset 不支持 watch-list 时能自动回退，
	// informer 仍可正常同步（fake 客户端只实现普通 List/Watch）。
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
	// fake 客户端不支持 WatchList 语义，无需准备响应。
	fakeClient := fakeclientset.NewClientset()
	factory := informers.NewSharedInformerFactory(fakeClient, 0)

	// 注册 Deployment Informer。
	target := factory.Apps().V1().Deployments().Informer()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), target.HasSynced) {
		t.Fatalf("failed to wait for caches to sync")
	}
}

// TestClientUserAgent 验证 User-Agent 的默认值与应用自定义值两种情况：
// 空值时使用 client-go 默认 UA，自定义值时原样发送。
func TestClientUserAgent(t *testing.T) {
	tests := []struct {
		name      string
		userAgent string
		expect    string
	}{
		{
			name:   "empty",
			expect: rest.DefaultKubernetesUserAgent(),
		},
		{
			name:      "custom",
			userAgent: "test-agent",
			expect:    "test-agent",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 服务端在收到请求时断言 UA；同时覆盖核心组与另一资源，确认 UA 对所有请求一致。
			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				userAgent := r.Header.Get("User-Agent")
				if userAgent != tc.expect {
					t.Errorf("User Agent expected: %s got: %s", tc.expect, userAgent)
					http.Error(w, "Unexpected user agent", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte("{}"))
			}))
			ts.Start()
			defer ts.Close()

			gv := v1.SchemeGroupVersion
			config := &rest.Config{
				Host: ts.URL,
			}
			config.GroupVersion = &gv
			config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
			config.UserAgent = tc.userAgent
			config.ContentType = "application/json"

			client, err := kubernetes.NewForConfig(config)
			if err != nil {
				t.Fatalf("failed to create REST client: %v", err)
			}
			_, err = client.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				t.Error(err)
			}
			_, err = client.CoreV1().Secrets("").List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				t.Error(err)
			}
		})
	}

}
