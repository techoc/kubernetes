//go:build !windows

/*
Copyright 2021 The Kubernetes Authors.

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

package tracing

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
	traceservice "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/watch"
	kmsv2mock "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/testing/v2"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	utiltesting "k8s.io/client-go/util/testing"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"
)

func TestAPIServerTracingWithKMSv2(t *testing.T) {
	// 在启动 API Server 之前先监听其 trace，
	// 这样测试期间 API Server 就能立即成功连接。
	listener, err := net.Listen("tcp", "localhost:")
	if err != nil {
		t.Fatal(err)
	}

	encryptionConfigFile, err := os.CreateTemp("", "encryption-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(encryptionConfigFile.Name())

	if err := os.WriteFile(encryptionConfigFile.Name(), []byte(`
apiVersion: apiserver.config.k8s.io/v1
kind: EncryptionConfiguration
resources:
  - resources:
    - secrets
    providers:
    - kms:
       apiVersion: v2
       name: kms-provider
       endpoint: unix:///@kms-provider.sock`), os.FileMode(0755)); err != nil {
		t.Fatal(err)
	}

	// 将 tracing 配置写入文件
	tracingConfigFile, err := os.CreateTemp("", "tracing-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tracingConfigFile.Name())

	if err := os.WriteFile(tracingConfigFile.Name(), []byte(fmt.Sprintf(`
apiVersion: apiserver.config.k8s.io/v1
kind: TracingConfiguration
endpoint: %s`, listener.Addr().String())), os.FileMode(0755)); err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer()
	fakeServer := &traceServer{t: t}
	fakeServer.resetExpectations([]*spanExpectation{}, trace.TraceID{})
	traceservice.RegisterTraceServiceServer(srv, fakeServer)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil {
			t.Error(serveErr)
			return
		}
	}()
	defer srv.Stop()

	_ = kmsv2mock.NewBase64Plugin(t, "@kms-provider.sock")

	// 使用我们的 tracing 配置启动 API Server
	testServer := kubeapiservertesting.StartTestServerOrDie(t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{
			"--tracing-config-file=" + tracingConfigFile.Name(),
			"--encryption-provider-config=" + encryptionConfigFile.Name(),
		},
		framework.SharedEtcd(),
	)
	defer testServer.TearDownFn()
	clientSet, err := client.NewForConfig(testServer.ClientConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		desc          string
		apiCall       func(context.Context, client.Interface) error
		expectedTrace []*spanExpectation
	}{
		{
			desc: "create secret",
			apiCall: func(ctx context.Context, c client.Interface) error {
				_, err = clientSet.CoreV1().Secrets(v1.NamespaceDefault).Create(ctx,
					&v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fake"}, Data: map[string][]byte{"foo": []byte("bar")}}, metav1.CreateOptions{})
				return err
			},
			expectedTrace: []*spanExpectation{
				{
					name: "TransformToStorage with envelopeTransformer",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"transformer.provider.name": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "kms-provider"
						},
					},
					events: []string{
						"About to encrypt data using DEK",
						"Data encryption succeeded",
						"About to encode encrypted object",
						"Encoded encrypted object",
					},
				},
			},
		},
		{
			desc: "get secret",
			apiCall: func(ctx context.Context, c client.Interface) error {
				// 这依赖于 "create secret" 步骤已成功完成
				_, err = clientSet.CoreV1().Secrets(v1.NamespaceDefault).Get(ctx, "fake", metav1.GetOptions{})
				return err
			},
			expectedTrace: []*spanExpectation{
				{
					name: "TransformFromStorage with envelopeTransformer",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"transformer.provider.name": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "kms-provider"
						},
					},
					events: []string{
						"About to decode encrypted object",
						"Decoded encrypted object",
						"About to decrypt data using DEK",
						"Data decryption succeeded",
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, traceID := sampledContext()
			fakeServer.resetExpectations(tc.expectedTrace, traceID)

			// 向 API server 发起调用
			if err := tc.apiCall(ctx, clientSet); err != nil {
				t.Fatal(err)
			}

			// 等待从我们的请求中记录到 span
			select {
			case <-fakeServer.traceFound:
			case <-time.After(30 * time.Second):
				t.Fatal("Timed out waiting for trace")
			}
		})
	}
}

func TestAPIServerTracingWithEgressSelector(t *testing.T) {
	// 在启动 API Server 之前先监听其 trace，
	// 这样测试期间 API Server 就能立即成功连接。
	listener, err := net.Listen("tcp", "localhost:")
	if err != nil {
		t.Fatal(err)
	}
	// 使用没有 controlplane 配置的 egress selector，以确保在该场景下 tracing 正常工作。
	// 将 egress selector 配置写入文件。
	egressSelectorConfigFile, err := os.CreateTemp("", "egress_selector_configuration.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(egressSelectorConfigFile.Name())

	if err := os.WriteFile(egressSelectorConfigFile.Name(), []byte(`
apiVersion: apiserver.k8s.io/v1beta1
kind: EgressSelectorConfiguration
egressSelections:
- name: cluster
  connection:
    proxyProtocol: Direct
    transport:`), os.FileMode(0755)); err != nil {
		t.Fatal(err)
	}

	// 将 tracing 配置写入文件
	tracingConfigFile, err := os.CreateTemp("", "tracing-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer utiltesting.CloseAndRemove(t, tracingConfigFile)

	if err := os.WriteFile(tracingConfigFile.Name(), []byte(fmt.Sprintf(`
apiVersion: apiserver.config.k8s.io/v1
kind: TracingConfiguration
endpoint: %s`, listener.Addr().String())), os.FileMode(0755)); err != nil {
		t.Fatal(err)
	}

	// 使用我们的 tracing 配置启动 API Server
	testServer := kubeapiservertesting.StartTestServerOrDie(t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{
			"--tracing-config-file=" + tracingConfigFile.Name(),
			"--egress-selector-config-file=" + egressSelectorConfigFile.Name(),
		},
		framework.SharedEtcd(),
	)
	defer testServer.TearDownFn()
	clientSet, err := client.NewForConfig(testServer.ClientConfig)
	if err != nil {
		t.Fatal(err)
	}
	// 确保 API Server 没有崩溃。
	_, err = clientSet.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnauthenticatedAPIServerTracing(t *testing.T) {
	// 在启动 API Server 之前先监听其 trace，
	// 这样测试期间 API Server 就能立即成功连接。
	listener, err := net.Listen("tcp", "localhost:")
	if err != nil {
		t.Fatal(err)
	}
	// 将 tracing 配置写入文件
	tracingConfigFile, err := os.CreateTemp("", "tracing-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tracingConfigFile.Name())

	if err := os.WriteFile(tracingConfigFile.Name(), []byte(fmt.Sprintf(`
apiVersion: apiserver.config.k8s.io/v1
kind: TracingConfiguration
endpoint: %s`, listener.Addr().String())), os.FileMode(0755)); err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer()
	fakeServer := &traceServer{t: t}
	fakeServer.resetExpectations([]*spanExpectation{{}}, trace.TraceID{})
	traceservice.RegisterTraceServiceServer(srv, fakeServer)

	go srv.Serve(listener)
	defer srv.Stop()

	// 使用我们的 tracing 配置启动 API Server
	testServer := kubeapiservertesting.StartTestServerOrDie(t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{"--tracing-config-file=" + tracingConfigFile.Name()},
		framework.SharedEtcd(),
	)
	defer testServer.TearDownFn()

	ctx, testTraceID := sampledContext()
	// 匹配任何具有测试 Trace ID 的 span
	fakeServer.resetExpectations([]*spanExpectation{{}}, testTraceID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, testServer.ClientConfig.Host+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	unauthenticatedConfig := rest.CopyConfig(testServer.ClientConfig)
	// 从请求中移除 bearer token，使其成为未认证请求。
	unauthenticatedConfig.BearerToken = ""
	transport, err := rest.TransportFor(unauthenticatedConfig)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: otelhttp.NewTransport(transport)}
	if _, err = client.Do(req); err != nil {
		t.Fatal(err)
	}

	// 由于请求未通过认证，确保找不到任何匹配的 trace
	select {
	case <-fakeServer.traceFound:
		t.Fatal("Found a trace when none was expected")
	case <-time.After(10 * time.Second):
	}
}

func TestAPIServerTracing(t *testing.T) {
	// 在启动 API Server 之前先监听其 trace，
	// 这样测试期间 API Server 就能立即成功连接。
	listener, err := net.Listen("tcp", "localhost:")
	if err != nil {
		t.Fatal(err)
	}
	// 将 tracing 配置写入文件
	tracingConfigFile, err := os.CreateTemp("", "tracing-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = os.Remove(tracingConfigFile.Name()); err != nil {
			t.Error(err)
		}
	}()

	if err := os.WriteFile(tracingConfigFile.Name(), []byte(fmt.Sprintf(`
apiVersion: apiserver.config.k8s.io/v1
kind: TracingConfiguration
endpoint: %s`, listener.Addr().String())), os.FileMode(0755)); err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer()
	fakeServer := &traceServer{t: t}
	fakeServer.resetExpectations([]*spanExpectation{}, trace.TraceID{})
	traceservice.RegisterTraceServiceServer(srv, fakeServer)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil {
			t.Error(serveErr)
		}
	}()
	defer srv.Stop()

	// 使用我们的 tracing 配置启动 API Server
	testServer := kubeapiservertesting.StartTestServerOrDie(t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{"--tracing-config-file=" + tracingConfigFile.Name()},
		framework.SharedEtcd(),
	)
	defer testServer.TearDownFn()
	clientSet, err := client.NewForConfig(testServer.ClientConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		desc          string
		apiCall       func(context.Context) error
		expectedTrace []*spanExpectation
	}{
		{
			desc: "create node",
			apiCall: func(ctx context.Context) error {
				_, err = clientSet.CoreV1().Nodes().Create(ctx,
					&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "fake"}}, metav1.CreateOptions{})
				return err
			},
			expectedTrace: []*spanExpectation{
				{
					name: "POST /api/v1/nodes",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"user_agent.original": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"url.path": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes"
						},
						"http.request.method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "POST"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
					},
				},
				{
					name: "Create",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes")
						},
						"user-agent": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"client": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "127.0.0.1"
						},
						"accept": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf, */*"
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
					},
					events: []string{
						"limitedReadBody succeeded",
						"About to convert to expected version",
						"Conversion done",
						"About to store object in database",
						"Write to database call succeeded",
						"About to write a response",
						"Writing http response done",
					},
				},
				{
					name: "Create etcd3",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"key": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/minions/fake"
						},
						"type": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "*core.Node"
						},
						"resource": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "nodes"
						},
					},
					events: []string{
						"About to Encode",
						"Encode succeeded",
						"TransformToStorage succeeded",
						"Txn call succeeded",
						"Decode succeeded",
					},
				},
				{
					name: "etcdserverpb.KV/Txn",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"rpc.system.name": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "grpc"
						},
					},
				},
				{
					name: "SerializeObject",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
						"method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "POST"
						},
						"mediaType": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf"
						},
						"encoder": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "{\"encodeGV\":\"v1\",\"encoder\":\"protobuf\",\"name\":\"versioning\"}"
						},
					},
					events: []string{
						"About to start writing response",
						"Write call succeeded",
					},
				},
			},
		},
		{
			desc: "get node",
			apiCall: func(ctx context.Context) error {
				// 这依赖于 "create node" 步骤已成功完成
				_, err = clientSet.CoreV1().Nodes().Get(ctx, "fake", metav1.GetOptions{})
				return err
			},
			expectedTrace: []*spanExpectation{
				{
					name: "GET /api/v1/nodes/{:name}",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"user_agent.original": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"url.path": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes/fake"
						},
						"http.request.method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "GET"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
					},
				},
				{
					name: "Get",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes/fake")
						},
						"user-agent": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"client": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "127.0.0.1"
						},
						"accept": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf, */*"
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
					},
					events: []string{
						"About to Get from storage",
						"About to write a response",
						"Writing http response done",
					},
				},
				{
					name: "Get etcd3",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"key": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/minions/fake"
						},
						"resource": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "nodes"
						},
					},
					events: []string{
						"Get call succeeded",
						"TransformFromStorage succeeded",
						"Decode succeeded",
					},
				},
				{
					name: "etcdserverpb.KV/Range",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"rpc.system.name": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "grpc"
						},
					},
				},
				{
					name: "SerializeObject",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes/fake"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
						"method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "GET"
						},
						"mediaType": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf"
						},
						"encoder": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "{\"encodeGV\":\"v1\",\"encoder\":\"protobuf\",\"name\":\"versioning\"}"
						},
					},
					events: []string{
						"About to start writing response",
						"Write call succeeded",
					},
				},
			},
		},
		{
			desc: "get node from cache",
			apiCall: func(ctx context.Context) error {
				_, err = clientSet.CoreV1().Nodes().Get(ctx, "fake", metav1.GetOptions{ResourceVersion: "0"})
				return err
			},
			expectedTrace: []*spanExpectation{
				{
					name: "GET /api/v1/nodes/{:name}",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"user_agent.original": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"url.path": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes/fake"
						},
						"http.request.method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "GET"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
					},
				},
				{
					name: "Get",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes/fake")
						},
						"user-agent": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"client": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "127.0.0.1"
						},
						"accept": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf, */*"
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
					},
					events: []string{
						"About to Get from storage",
						"About to write a response",
						"Writing http response done",
					},
				},
				{
					name: "cacher.Get",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"key": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/minions/fake"
						},
						"resource-version": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "0"
						},
					},
					events: []string{
						"watchCache locked acquired",
						"watchCache fresh enough",
						"GetByKey success",
					},
				},
				{
					name: "SerializeObject",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes/fake"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
						"method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "GET"
						},
						"mediaType": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf"
						},
						"encoder": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "{\"encodeGV\":\"v1\",\"encoder\":\"protobuf\",\"name\":\"versioning\"}"
						},
					},
					events: []string{
						"About to start writing response",
						"Write call succeeded",
					},
				},
			},
		},
		{
			desc: "WatchList nodes",
			apiCall: func(ctx context.Context) error {
				sendInitialEvents := true
				w, err := clientSet.CoreV1().Nodes().Watch(ctx, metav1.ListOptions{SendInitialEvents: &sendInitialEvents, AllowWatchBookmarks: true, ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan})
				if err != nil {
					return err
				}
				defer w.Stop()
				for e := range w.ResultChan() {
					switch e.Type {
					case watch.Bookmark:
						return nil
					case watch.Error:
						return fmt.Errorf("watch error: %v", e.Object)
					}
				}
				return nil
			},
			expectedTrace: []*spanExpectation{
				{
					name: "GET /api/v1/nodes",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"user_agent.original": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"url.path": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes"
						},
						"http.request.method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "GET"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
					},
				},
				{
					name: "WatchList",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes"
						},
						"user-agent": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"client": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "127.0.0.1"
						},
						"accept": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf, */*"
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
					},
					events: []string{},
				},
				{
					name: "cacher.Watch",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"type": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "nodes"
						},
					},
					events: []string{
						"watchCache locked acquired",
						"watchCache fresh enough",
					},
				},
				{
					name: "WatchServer.HandleHTTP",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
						"method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "GET"
						},
						"mediaType": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf;stream=watch"
						},
						"encoder": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "{\"encodeGV\":\"v1\",\"encoder\":\"raw-protobuf\",\"name\":\"versioning\"}"
						},
					},
					events: []string{},
				},
			},
		},
		{
			desc: "List nodes",
			apiCall: func(ctx context.Context) error {
				_, err = clientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				return err
			},
			expectedTrace: []*spanExpectation{
				{
					name: "GET /api/v1/nodes",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"user_agent.original": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"url.path": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes"
						},
						"http.request.method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "GET"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
					},
				},
				{
					name: "List",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes"
						},
						"user-agent": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"client": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "127.0.0.1"
						},
						"accept": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf, */*"
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
					},
					events: []string{},
				},
				{
					name: "cacher.GetList",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"type": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "nodes"
						},
					},
					events: []string{
						"Ready",
						"getCurrentRV success",
						"watchCache locked acquired",
						"watchCache fresh enough",
						"GetLatestSnapshotOrBuildLocked success",
						"Listed items from cache",
						"Filtered items",
					},
				},
				{
					name: "etcdserverpb.KV/Range",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"rpc.system.name": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "grpc"
						},
					},
				},
				{
					name: "SerializeObject",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
						"method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "GET"
						},
						"mediaType": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf"
						},
						"encoder": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "{\"encodeGV\":\"v1\",\"encoder\":\"protobuf\",\"name\":\"versioning\"}"
						},
					},
					events: []string{
						"About to start writing response",
						"Write call succeeded",
					},
				},
			},
		},
		{
			desc: "update node",
			apiCall: func(ctx context.Context) error {
				// 这依赖于 "create node" 步骤已成功完成
				_, err = clientSet.CoreV1().Nodes().Update(ctx,
					&v1.Node{ObjectMeta: metav1.ObjectMeta{
						Name:        "fake",
						Annotations: map[string]string{"foo": "bar"},
					}}, metav1.UpdateOptions{})
				return err
			},
			expectedTrace: []*spanExpectation{
				{
					name: "PUT /api/v1/nodes/{:name}",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"user_agent.original": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"url.path": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes/fake"
						},
						"http.request.method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "PUT"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
					},
				},
				{
					name: "Update",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes/fake")
						},
						"user-agent": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"client": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "127.0.0.1"
						},
						"accept": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf, */*"
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
					},
					events: []string{
						"limitedReadBody succeeded",
						"About to convert to expected version",
						"Conversion done",
						"About to store object in database",
						"Write to database call succeeded",
						"About to write a response",
						"Writing http response done",
					},
				},
				{
					name: "GuaranteedUpdate etcd3",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"key": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/minions/fake"
						},
						"type": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "*core.Node"
						},
						"resource": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "nodes"
						},
					},
					events: []string{
						"initial value restored",
						"About to Encode",
						"Encode succeeded",
						"TransformToStorage succeeded",
						"Transaction prepared",
						"Txn call completed",
						"Transaction committed",
						"Decode succeeded",
					},
				},
				{
					name: "etcdserverpb.KV/Txn",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"rpc.system.name": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "grpc"
						},
					},
				},
				{
					name: "SerializeObject",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes/fake")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
						"method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "PUT"
						},
						"mediaType": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf"
						},
						"encoder": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "{\"encodeGV\":\"v1\",\"encoder\":\"protobuf\",\"name\":\"versioning\"}"
						},
					},
					events: []string{
						"About to start writing response",
						"Write call succeeded",
					},
				},
			},
		},
		{
			desc: "patch node",
			apiCall: func(ctx context.Context) error {
				// 这依赖于 "create node" 步骤已成功完成
				oldNode := &v1.Node{ObjectMeta: metav1.ObjectMeta{
					Name:        "fake",
					Annotations: map[string]string{"foo": "bar"},
				}}
				oldData, err := json.Marshal(oldNode)
				if err != nil {
					return err
				}
				newNode := &v1.Node{ObjectMeta: metav1.ObjectMeta{
					Name:        "fake",
					Annotations: map[string]string{"foo": "bar"},
					Labels:      map[string]string{"hello": "world"},
				}}
				newData, err := json.Marshal(newNode)
				if err != nil {
					return err
				}

				patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Node{})
				if err != nil {
					return err
				}
				_, err = clientSet.CoreV1().Nodes().Patch(ctx, "fake", types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
				return err
			},
			expectedTrace: []*spanExpectation{
				{
					name: "PATCH /api/v1/nodes/{:name}",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"user_agent.original": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"url.path": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes/fake"
						},
						"http.request.method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "PATCH"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
					},
				},
				{
					name: "Patch",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes/fake")
						},
						"user-agent": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"client": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "127.0.0.1"
						},
						"accept": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf, */*"
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
					},
					events: []string{
						"limitedReadBody succeeded",
						"Recorded the audit event",
						"About to apply patch",
						"About to check admission control",
						"Object stored in database",
						"About to write a response",
						"Writing http response done",
					},
				},
				{
					name: "GuaranteedUpdate etcd3",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"key": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/minions/fake"
						},
						"type": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "*core.Node"
						},
						"resource": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "nodes"
						},
					},
					events: []string{
						"initial value restored",
						"About to Encode",
						"Encode succeeded",
						"TransformToStorage succeeded",
						"Transaction prepared",
						"Txn call completed",
						"Transaction committed",
						"Decode succeeded",
					},
				},
				{
					name: "etcdserverpb.KV/Txn",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"rpc.system.name": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "grpc"
						},
					},
				},
				{
					name: "SerializeObject",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes/fake")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
						"method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "PATCH"
						},
						"mediaType": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf"
						},
						"encoder": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "{\"encodeGV\":\"v1\",\"encoder\":\"protobuf\",\"name\":\"versioning\"}"
						},
					},
					events: []string{
						"About to start writing response",
						"Write call succeeded",
					},
				},
			},
		},
		{
			desc: "delete node",
			apiCall: func(ctx context.Context) error {
				// 这依赖于 "create node" 步骤已成功完成
				return clientSet.CoreV1().Nodes().Delete(ctx, "fake", metav1.DeleteOptions{})
			},
			expectedTrace: []*spanExpectation{
				{
					name: "DELETE /api/v1/nodes/{:name}",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"user_agent.original": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"url.path": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "/api/v1/nodes/fake"
						},
						"http.request.method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "DELETE"
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
					},
				},
				{
					name: "Delete",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes/fake")
						},
						"user-agent": func(v *commonv1.AnyValue) bool {
							return strings.HasPrefix(v.GetStringValue(), "tracing.test")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"client": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "127.0.0.1"
						},
						"accept": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf, */*"
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
					},
					events: []string{
						"limitedReadBody succeeded",
						"Decoded delete options",
						"Recorded the audit event",
						"About to delete object from database",
						"Object deleted from database",
						"About to write a response",
						"Writing http response done",
					},
				},
				{
					name: "etcdserverpb.KV/Txn",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"rpc.system.name": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "grpc"
						},
					},
				},
				{
					name: "SerializeObject",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"url": func(v *commonv1.AnyValue) bool {
							return strings.HasSuffix(v.GetStringValue(), "/api/v1/nodes/fake")
						},
						"audit-id": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() != ""
						},
						"protocol": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "HTTP/2.0"
						},
						"method": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "DELETE"
						},
						"mediaType": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "application/vnd.kubernetes.protobuf"
						},
						"encoder": func(v *commonv1.AnyValue) bool {
							return v.GetStringValue() == "{\"encodeGV\":\"v1\",\"encoder\":\"protobuf\",\"name\":\"versioning\"}"
						},
					},
					events: []string{
						"About to start writing response",
						"Write call succeeded",
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, testTraceID := sampledContext()
			fakeServer.resetExpectations(tc.expectedTrace, testTraceID)

			// 向 API server 发起调用
			if err := tc.apiCall(ctx); err != nil {
				t.Fatal(err)
			}

			// 等待从我们的请求中记录到 span
			select {
			case <-fakeServer.traceFound:
			case <-time.After(30 * time.Second):
				for _, spanExpectation := range fakeServer.expectations {
					if !spanExpectation.met {
						t.Logf("Unmet expectation: %s", spanExpectation.name)
					}
				}
				t.Fatal("Timed out waiting for trace")
			}
		})
	}
}

// traceServer 实现了 TracesServiceServer，可通过 OTLP
// 接收来自 API Server 的 span。
type traceServer struct {
	t *testing.T
	traceservice.UnimplementedTraceServiceServer
	// 该锁保护下面的每个场景的状态
	lock         sync.Mutex
	traceFound   chan struct{}
	expectations traceExpectation
	testTraceID  trace.TraceID
}

func (t *traceServer) Export(ctx context.Context, req *traceservice.ExportTraceServiceRequest) (*traceservice.ExportTraceServiceResponse, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.expectations.update(req, t.testTraceID)
	// 如果所有期望都已满足，则关闭 traceFound 以通知测试场景
	if t.expectations.met() {
		select {
		case <-t.traceFound:
			// traceFound 已经关闭
		default:
			close(t.traceFound)
		}
	}
	return &traceservice.ExportTraceServiceResponse{}, nil
}

// resetExpectations 由新的测试场景使用，
// 用于为测试服务器设置新的期望。
func (t *traceServer) resetExpectations(newExpectations traceExpectation, traceID trace.TraceID) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.traceFound = make(chan struct{})
	t.expectations = newExpectations
	t.testTraceID = traceID
}

// traceExpectation 表示对整条 trace 的期望
type traceExpectation []*spanExpectation

// met 在服务器寻找的所有 span 期望
// 都已满足时返回 true。
func (t traceExpectation) met() bool {
	for _, se := range t {
		if !se.met {
			return false
		}
	}
	return true
}

// update 查找传入请求中
// 被某个 span 满足的所有期望。
func (t traceExpectation) update(req *traceservice.ExportTraceServiceRequest, traceID trace.TraceID) {
	for _, resourceSpans := range req.GetResourceSpans() {
		for _, instrumentationSpans := range resourceSpans.GetScopeSpans() {
			for _, span := range instrumentationSpans.GetSpans() {
				t.updateForSpan(span, traceID)
			}
		}
	}
}

// updateForSpan 根据单个传入的 span 更新期望。
func (t traceExpectation) updateForSpan(span *tracev1.Span, traceID trace.TraceID) {
	if hex.EncodeToString(span.TraceId) != traceID.String() {
		return
	}
	for i, spanExpectation := range t {
		if spanExpectation.name != "" && span.Name != spanExpectation.name {
			continue
		}
		if !spanExpectation.attributes.matches(span.GetAttributes()) {
			continue
		}
		if !spanExpectation.events.matches(span.GetEvents()) {
			continue
		}
		t[i].met = true
	}

}

// spanExpectation 表示对单个 span 的期望
type spanExpectation struct {
	name       string
	attributes attributeExpectation
	events     eventExpectation
	met        bool
}

// eventExpectation 表示对附加到 span 上的事件的期望。
// 它由事件名称组成。
type eventExpectation []string

// matches 在输入事件列表中存在所有期望的事件时返回 true。
func (e eventExpectation) matches(events []*tracev1.Span_Event) bool {
	eventMap := map[string]struct{}{}
	for _, event := range events {
		eventMap[event.Name] = struct{}{}
	}
	for _, wantEvent := range e {
		if _, ok := eventMap[wantEvent]; !ok {
			return false
		}
	}
	return true
}

// eventExpectation 表示对附加到 span 上的事件的期望。
// 它是从属性键到值匹配函数的映射。
type attributeExpectation map[string]func(*commonv1.AnyValue) bool

// matches 在输入的属性列表中存在所有期望的属性时返回 true。
func (a attributeExpectation) matches(attrs []*commonv1.KeyValue) bool {
	attrsMap := map[string]*commonv1.AnyValue{}
	for _, attr := range attrs {
		attrsMap[attr.GetKey()] = attr.GetValue()
	}
	for key, checkVal := range a {
		if val, ok := attrsMap[key]; !ok || !checkVal(val) {
			return false
		}
	}
	return true
}

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func sampledContext() (context.Context, trace.TraceID) {
	tid := trace.TraceID{}
	_, _ = r.Read(tid[:])
	sid := trace.SpanID{}
	_, _ = r.Read(sid[:])
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(context.Background(), sc), tid
}
