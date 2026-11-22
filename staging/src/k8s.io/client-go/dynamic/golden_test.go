/*
Copyright 2024 The Kubernetes Authors.

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

package dynamic_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// TestGoldenRequest 是请求侧的“黄金文件”测试：每个子测试用动态客户端发起一种
// 操作，把服务端实际收到的原始 HTTP 请求与 testdata 下的快照逐字节比对。
// 这样任何会改变线上协议（路径、查询参数、头部、body）的客户端改动都会被发现。
func TestGoldenRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		do   func(context.Context, dynamic.Interface) error
	}{
		{
			// 创建时带 fieldValidation 与 dryRun（后一个位置参数 "fin"）。
			name: "create",
			do: func(ctx context.Context, client dynamic.Interface) error {
				_, err := client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").Create(
					ctx,
					&unstructured.Unstructured{Object: map[string]interface{}{
						"metadata": map[string]interface{}{"name": "mips"},
					}},
					metav1.CreateOptions{FieldValidation: "warn"},
					"fin",
				)
				return err
			},
		},
		{
			name: "update",
			do: func(ctx context.Context, client dynamic.Interface) error {
				_, err := client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").Update(
					ctx,
					&unstructured.Unstructured{Object: map[string]interface{}{
						"metadata": map[string]interface{}{"name": "mips"},
					}},
					metav1.UpdateOptions{FieldValidation: "warn"},
					"fin",
				)
				return err
			},
		},
		{
			name: "updatestatus",
			// 走 status 子资源，用于确认客户端把更新目标正确地映射到 /status。
			do: func(ctx context.Context, client dynamic.Interface) error {
				_, err := client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").UpdateStatus(
					ctx,
					&unstructured.Unstructured{Object: map[string]interface{}{
						"metadata": map[string]interface{}{"name": "mips"},
					}},
					metav1.UpdateOptions{FieldValidation: "warn"},
				)
				return err
			},
		},
		{
			name: "delete",
			do: func(ctx context.Context, client dynamic.Interface) error {
				return client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").Delete(
					ctx,
					"mips",
					metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}},
					"fin",
				)
			},
		},
		{
			name: "deletecollection",
			// 集合删除需同时携带 DryRun 与 resourceVersion，验证两者都进入查询参数。
			do: func(ctx context.Context, client dynamic.Interface) error {
				return client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").DeleteCollection(
					ctx,
					metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}},
					metav1.ListOptions{ResourceVersion: "42"},
				)
			},
		},
		{
			name: "get",
			do: func(ctx context.Context, client dynamic.Interface) error {
				_, err := client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").Get(
					ctx,
					"mips",
					metav1.GetOptions{ResourceVersion: "42"},
					"fin",
				)
				return err
			},
		},
		{
			name: "list",
			do: func(ctx context.Context, client dynamic.Interface) error {
				_, err := client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").List(
					ctx,
					metav1.ListOptions{ResourceVersion: "42"},
				)
				return err
			},
		},
		{
			name: "watch",
			// Watch 需要服务端保持连接，这里不关心结果，只看请求本身。
			do: func(ctx context.Context, client dynamic.Interface) error {
				_, err := client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").Watch(
					ctx,
					metav1.ListOptions{ResourceVersion: "42"},
				)
				return err
			},
		},
		{
			name: "patch",
			do: func(ctx context.Context, client dynamic.Interface) error {
				_, err := client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").Patch(
					ctx,
					"mips",
					types.StrategicMergePatchType,
					[]byte("{\"foo\":\"bar\"}\n"),
					metav1.PatchOptions{FieldManager: "baz"},
					"fin",
				)
				return err
			},
		},
		{
			name: "apply",
			// Apply 的 Content-Type 与 fieldManager 是 SSA 协议的关键，必须体现在快照中。
			do: func(ctx context.Context, client dynamic.Interface) error {
				_, err := client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").Apply(
					ctx,
					"mips",
					&unstructured.Unstructured{Object: map[string]interface{}{
						"metadata": map[string]interface{}{"name": "mips"},
					}},
					metav1.ApplyOptions{Force: true},
					"fin",
				)
				return err
			},
		},
		{
			name: "applystatus",
			// 同上，但作用于 status 子资源。
			do: func(ctx context.Context, client dynamic.Interface) error {
				_, err := client.Resource(schema.GroupVersionResource{Group: "flops", Version: "v1alpha1", Resource: "flips"}).Namespace("mops").ApplyStatus(
					ctx,
					"mips",
					&unstructured.Unstructured{Object: map[string]interface{}{
						"metadata": map[string]interface{}{"name": "mips"},
					}},
					metav1.ApplyOptions{Force: true},
				)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// handled 用于确认服务端确实收到请求，避免客户端提前返回却被误判为通过。
			handled := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(handled)

				// DumpRequest 的第二个参数为 true，表示连 body 一起快照。
				got, err := httputil.DumpRequest(r, true)
				if err != nil {
					t.Fatal(err)
				}

				path := filepath.Join("testdata", filepath.FromSlash(t.Name()))

				// 设置环境变量即可重新生成快照；日常 CI 中只读取不写入。
				if os.Getenv("UPDATE_DYNAMIC_CLIENT_FIXTURES") == "true" {
					err := os.WriteFile(path, got, os.FileMode(0755))
					if err != nil {
						t.Fatalf("failed to update fixture: %v", err)
					}
				}

				want, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("failed to load fixture: %v", err)
				}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("unexpected difference from expected bytes:\n%s", diff)
				}
			}))
			defer srv.Close()

			client, err := dynamic.NewForConfig(&rest.Config{
				Host:      "example.com",
				UserAgent: "TestGoldenRequest",
				Transport: &http.Transport{
					// The client will send a static Host header while always
					// connecting to the test server.
					DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
						u, err := url.Parse(srv.URL)
						if err != nil {
							return nil, fmt.Errorf("failed to parse test server url: %w", err)
						}
						return (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := tc.do(ctx, client); err != nil {
				// This test detects server-perceptible changes to the request. As
				// long as the server receives the expected request, a non-nil error
				// returned from a client method is not a failure.
				t.Logf("client returned non-nil error: %v", err)
			}

			select {
			case <-handled:
			default:
				t.Fatal("no request received")
			}
		})
	}
}

// RoundTripperFunc 让普通函数可以充当 http.RoundTripper，方便在测试中伪造传输层。
type RoundTripperFunc func(*http.Request) (*http.Response, error)

func (f RoundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// TestGoldenResponse tests that the objects returned from dynamic client methods, given a fixed
// HTTP response, are not changed unintentionally by changes to the client.
//
// 与请求侧对称：用固定的 HTTP 响应文件喂给真实的客户端，再比较解析出的结构化结果，
// 防止反序列化路径（尤其 list 与 watch）被无意改变。
func TestGoldenResponse(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response string // name of fixture containing a serialized HTTP1.1 response
		do       func(t *testing.T, client dynamic.ResourceInterface) interface{}
	}{
		{
			name:     "create",
			response: "nonlist",
			do: func(t *testing.T, client dynamic.ResourceInterface) interface{} {
				got, err := client.Create(context.Background(), &unstructured.Unstructured{}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}

				return got.UnstructuredContent()
			},
		},
		{
			name:     "update",
			response: "nonlist",
			do: func(t *testing.T, client dynamic.ResourceInterface) interface{} {
				got, err := client.Update(context.Background(), &unstructured.Unstructured{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "name"}}}, metav1.UpdateOptions{})
				if err != nil {
					t.Fatal(err)
				}

				return got.UnstructuredContent()
			},
		},
		{
			name:     "updatestatus",
			response: "nonlist",
			do: func(t *testing.T, client dynamic.ResourceInterface) interface{} {
				got, err := client.UpdateStatus(context.Background(), &unstructured.Unstructured{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "name"}}}, metav1.UpdateOptions{})

				if err != nil {
					t.Fatal(err)
				}

				return got.UnstructuredContent()
			},
		},
		{
			name:     "get",
			response: "nonlist",
			do: func(t *testing.T, client dynamic.ResourceInterface) interface{} {
				got, err := client.Get(context.Background(), "name", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				return got.UnstructuredContent()
			},
		},
		{
			name:     "list",
			response: "list",
			// List 返回的是 UnstructuredList，比较其内容可确认列表的包装结构未变。
			do: func(t *testing.T, client dynamic.ResourceInterface) interface{} {
				got, err := client.List(context.Background(), metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}

				return got.UnstructuredContent()

			},
		},
		{
			name:     "watch",
			response: "events",
			// 用 watch 事件流文件驱动解码器，把事件重新序列化后与快照比较。
			do: func(t *testing.T, client dynamic.ResourceInterface) interface{} {
				w, err := client.Watch(context.Background(), metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer w.Stop()

				var got []interface{}
				for e := range w.ResultChan() {
					u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&metav1.WatchEvent{
						Type:   string(e.Type),
						Object: runtime.RawExtension{Object: e.Object},
					})
					if err != nil {
						t.Fatalf("failed to convert watch event to unstructured content: %v", err)
					}
					got = append(got, u)
				}

				return got
			},
		},
	} {
		parentTestName := t.Name()
		t.Run(tc.name, func(t *testing.T) {
			client, err := dynamic.NewForConfig(&rest.Config{

				// 传输层直接返回预先录制的 HTTP 响应，不经过网络。
				Transport: RoundTripperFunc(func(request *http.Request) (*http.Response, error) {
					fd, err := os.Open(filepath.Join("testdata", filepath.FromSlash(parentTestName), "responses", tc.response))
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := fd.Close(); err != nil {
							t.Fatal(err)
						}
					}()

					response, err := http.ReadResponse(bufio.NewReader(fd), request)
					if err != nil {
						t.Fatal(err)
					}
					return response, nil
				}),
			})
			if err != nil {
				t.Fatal(err)
			}

			// 空的 GVR 即可：请求 URL 由测试桩忽略，这里只关心响应解码。
			got := tc.do(t, client.Resource(schema.GroupVersionResource{}))

			path := filepath.Join("testdata", filepath.FromSlash(t.Name()))
			if os.Getenv("UPDATE_DYNAMIC_CLIENT_FIXTURES") == "true" {
				fixture, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, fixture, os.FileMode(0644)); err != nil {
					t.Fatalf("failed to update fixture: %v", err)
				}
			}
			fixture, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			var want interface{}
			if err := json.Unmarshal(fixture, &want); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("unexpected diff:\n%s", diff)
			}
		})
	}
}
