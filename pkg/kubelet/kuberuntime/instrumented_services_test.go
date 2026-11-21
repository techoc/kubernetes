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

package kuberuntime

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	compbasemetrics "k8s.io/component-base/metrics"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestRecordOperation 验证记录一次运行时操作后指标可通过 HTTP 抓取到。
func TestRecordOperation(t *testing.T) {
	// 使用独立的本地 registry，避免污染全局指标。
	var registry = compbasemetrics.NewKubeRegistry()
	var gather compbasemetrics.Gatherer = registry

	registry.MustRegister(metrics.RuntimeOperations)
	registry.MustRegister(metrics.RuntimeOperationsDuration)
	registry.MustRegister(metrics.RuntimeOperationsErrors)

	registry.Reset()

	// 启动本地 HTTP 服务用于暴露指标。
	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer l.Close()

	prometheusURL := "http://" + l.Addr().String() + "/metrics"
	mux := http.NewServeMux()
	handler := compbasemetrics.HandlerFor(gather, compbasemetrics.HandlerOpts{})
	mux.Handle("/metrics", handler)
	server := &http.Server{
		Addr:    l.Addr().String(),
		Handler: mux,
	}
	go func() {
		server.Serve(l)
	}()

	// 记录一次操作，随后通过 HTTP 抓取指标。
	recordOperation("create_container", time.Now())
	runtimeOperationsCounterExpected := "kubelet_runtime_operations_total{operation_type=\"create_container\"} 1"
	runtimeOperationsDurationExpected := "kubelet_runtime_operations_duration_seconds_count{operation_type=\"create_container\"} 1"

	assert.HTTPBodyContains(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	}), "GET", prometheusURL, nil, runtimeOperationsCounterExpected)

	assert.HTTPBodyContains(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	}), "GET", prometheusURL, nil, runtimeOperationsDurationExpected)
}

// TestInstrumentedVersion 验证 instrumented 包装层返回底层运行时的 API 版本。
func TestInstrumentedVersion(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, _, _ := createTestRuntimeManager(tCtx)
	irs := newInstrumentedRuntimeService(fakeRuntime)
	vr, err := irs.Version(tCtx, "1")
	assert.NoError(t, err)
	assert.Equal(t, kubeRuntimeAPIVersion, vr.Version)
}

// TestStatus 验证 instrumented 包装层原样返回运行时状态。
func TestStatus(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, _, _ := createTestRuntimeManager(tCtx)
	// 构造运行时状态，验证包装层不会修改内容。
	fakeRuntime.FakeStatus = &runtimeapi.RuntimeStatus{
		Conditions: []*runtimeapi.RuntimeCondition{
			{Type: runtimeapi.RuntimeReady, Status: false},
			{Type: runtimeapi.NetworkReady, Status: true},
		},
	}
	irs := newInstrumentedRuntimeService(fakeRuntime)
	actural, err := irs.Status(tCtx, false)
	assert.NoError(t, err)
	expected := &runtimeapi.RuntimeStatus{
		Conditions: []*runtimeapi.RuntimeCondition{
			{Type: runtimeapi.RuntimeReady, Status: false},
			{Type: runtimeapi.NetworkReady, Status: true},
		},
	}
	assert.Equal(t, expected, actural.Status)
}
