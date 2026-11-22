/*
Copyright 2014 The Kubernetes Authors.

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

package testing

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestFakeHandlerPath 验证 FakeHandler 能记录并校验请求的 path、method 与 body。
func TestFakeHandlerPath(t *testing.T) {
	handler := FakeHandler{StatusCode: http.StatusOK}
	server := httptest.NewServer(&handler)
	defer server.Close()
	method := "GET"
	path := "/foo/bar"
	body := "somebody"

	req, err := http.NewRequest(method, server.URL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	client := http.Client{}
	_, err = client.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	handler.ValidateRequest(t, path, method, &body)
}

// TestFakeHandlerPathNoBody 覆盖请求不带 body 的情况，校验时 body 期望传 nil。
func TestFakeHandlerPathNoBody(t *testing.T) {
	handler := FakeHandler{StatusCode: http.StatusOK}
	server := httptest.NewServer(&handler)
	defer server.Close()
	method := "GET"
	path := "/foo/bar"

	req, err := http.NewRequest(method, server.URL+path, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	client := http.Client{}
	_, err = client.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	handler.ValidateRequest(t, path, method, nil)
}

// fakeError 实现 testing.TB 的最小子集，用来捕获 ValidateRequest 报告的失败次数，
// 以便断言反向用例确实报错。
type fakeError struct {
	errors []string
}

func (f *fakeError) Errorf(format string, args ...interface{}) {
	f.errors = append(f.errors, format)
}

func (f *fakeError) Logf(format string, args ...interface{}) {}

// TestFakeHandlerWrongPath 路径不匹配时必须且只报告一次错误。
func TestFakeHandlerWrongPath(t *testing.T) {
	handler := FakeHandler{StatusCode: http.StatusOK}
	server := httptest.NewServer(&handler)
	defer server.Close()
	method := "GET"
	path := "/foo/bar"
	fakeT := fakeError{}

	req, err := http.NewRequest(method, server.URL+"/foo/baz", nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	client := http.Client{}
	_, err = client.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	handler.ValidateRequest(&fakeT, path, method, nil)
	if len(fakeT.errors) != 1 {
		t.Errorf("Unexpected error set: %#v", fakeT.errors)
	}
}

// TestFakeHandlerWrongMethod 方法不匹配时必须报告错误。
func TestFakeHandlerWrongMethod(t *testing.T) {
	handler := FakeHandler{StatusCode: http.StatusOK}
	server := httptest.NewServer(&handler)
	defer server.Close()
	method := "GET"
	path := "/foo/bar"
	fakeT := fakeError{}

	req, err := http.NewRequest("PUT", server.URL+path, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	client := http.Client{}
	_, err = client.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	handler.ValidateRequest(&fakeT, path, method, nil)
	if len(fakeT.errors) != 1 {
		t.Errorf("Unexpected error set: %#v", fakeT.errors)
	}
}

// TestFakeHandlerWrongBody 请求体与期望不一致时必须报告错误。
func TestFakeHandlerWrongBody(t *testing.T) {
	handler := FakeHandler{StatusCode: http.StatusOK}
	server := httptest.NewServer(&handler)
	defer server.Close()
	method := "GET"
	path := "/foo/bar"
	body := "somebody"
	fakeT := fakeError{}

	req, err := http.NewRequest(method, server.URL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	client := http.Client{}
	_, err = client.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	otherbody := "otherbody"
	handler.ValidateRequest(&fakeT, path, method, &otherbody)
	if len(fakeT.errors) != 1 {
		t.Errorf("Unexpected error set: %#v", fakeT.errors)
	}
}

// TestFakeHandlerNilBody 实际无 body 但期望非空 body 时必须报告错误。
func TestFakeHandlerNilBody(t *testing.T) {
	handler := FakeHandler{StatusCode: http.StatusOK}
	server := httptest.NewServer(&handler)
	defer server.Close()
	method := "GET"
	path := "/foo/bar"
	body := "somebody"
	fakeT := fakeError{}

	req, err := http.NewRequest(method, server.URL+path, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	client := http.Client{}
	_, err = client.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	handler.ValidateRequest(&fakeT, path, method, &body)
	if len(fakeT.errors) != 1 {
		t.Errorf("Unexpected error set: %#v", fakeT.errors)
	}
}
