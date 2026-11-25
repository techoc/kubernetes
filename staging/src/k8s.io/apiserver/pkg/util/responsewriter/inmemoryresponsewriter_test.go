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

package responsewriter

import (
	"net/http"
	"testing"
)

// TestInMemoryResponseWriter 验证内存响应写入器的基本契约：
// Header 可写、WriteHeader 记录状态码、Write 追加响应体，且 String() 的输出格式稳定。
// String() 的格式被测试固定，是因为它常用于调试输出与测试断言；
// 格式一旦变化会让大量依赖它的测试失败，因此显式锁定。
func TestInMemoryResponseWriter(t *testing.T) {
	w := NewInMemoryResponseWriter()

	h := w.Header()
	h.Set("Content-Type", "application/json")

	w.WriteHeader(http.StatusCreated)

	_, err := w.Write([]byte(`{"message": "hello"}`))
	if err != nil {
		t.Errorf("Write() returned an error: %v", err)
	}

	if w.RespCode() != http.StatusCreated {
		t.Errorf("RespCode() returned unexpected code: %d, want %d", w.RespCode(), http.StatusCreated)
	}

	if string(w.Data()) != `{"message": "hello"}` {
		t.Errorf("Data() returned unexpected body: %s, want %s", string(w.Data()), `{"message": "hello"}`)
	}

	expectedString := "ResponseCode: 201, Body: {\"message\": \"hello\"}, Header: map[Content-Type:[application/json]]"
	if w.String() != expectedString {
		t.Errorf("String() returned unexpected output: %s, want %s", w.String(), expectedString)
	}
}

// TestInMemoryResponseWriter_DefaultHeader 验证未显式调用 WriteHeader 时，
// 首次 Write 会隐式返回 200，与 net/http 的隐式状态码语义保持一致。
// 若不模拟该行为，被测代码中「忘记写状态码」的缺陷会被掩盖。
func TestInMemoryResponseWriter_DefaultHeader(t *testing.T) {
	w := NewInMemoryResponseWriter()

	_, err := w.Write([]byte(`{"message": "hello"}`))
	if err != nil {
		t.Errorf("Write() returned an error: %v", err)
	}

	// 默认应为 StatusOK（200）：对应 net/http 在首次 Write 时隐式提交 200 的行为。
	if w.RespCode() != http.StatusOK {
		t.Errorf("RespCode() returned unexpected code: %d, want %d", w.RespCode(), http.StatusOK)
	}
}
