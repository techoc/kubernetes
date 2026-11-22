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

package filters

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/routine"
	"k8s.io/klog/v2"
)

// TestPropogatingPanic 验证 WithPanicRecovery 不会吞掉 panic 的日志信号：
// handler 主动 panic 后，客户端应收到连接错误，同时日志里应同时出现原始 panic 信息
// 和 "apiserver panic'd" 标记。后者是排障时的固定锚点，改动会导致运维脚本失配。
func TestPropogatingPanic(t *testing.T) {
	// 劫持 klog 输出，避免测试日志污染真实 stderr，并便于断言。
	var buf bytes.Buffer
	klog.SetOutput(&buf)
	klog.LogToStderr(false)
	defer klog.LogToStderr(true)

	panicMsg := "panic as designed"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(panicMsg)
	})
	resolver := &request.RequestInfoFactory{
		APIPrefixes:          sets.NewString("api", "apis"),
		GrouplessAPIPrefixes: sets.NewString("api"),
	}
	// 用 WithRoutine 包一层，模拟真实 apiserver 中每个请求独立 goroutine 的执行模型。
	ts := httptest.NewServer(routine.WithRoutine(WithPanicRecovery(handler, resolver), func(_ *http.Request, _ *request.RequestInfo) bool { return true }))
	defer ts.Close()
	_, err := http.Get(ts.URL)
	if err == nil {
		t.Error("expected to receive an error")
	}

	// 先 flush 再读缓冲区，否则可能读到不完整的日志。
	klog.Flush()
	klog.SetOutput(&bytes.Buffer{}) // prevent further writes into buf
	capturedOutput := buf.String()

	if !strings.Contains(capturedOutput, panicMsg) || !strings.Contains(capturedOutput, "apiserver panic'd") {
		t.Errorf("unexpected out captured actual = %v", capturedOutput)
	}
}
