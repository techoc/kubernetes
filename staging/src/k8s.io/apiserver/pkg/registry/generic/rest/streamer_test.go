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

package rest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestInputStreamReader 端到端验证 LocationStreamer 直接读取远程流内容。
func TestInputStreamReader(t *testing.T) {
	resultString := "Test output"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte(resultString))
	}))
	defer s.Close()
	u, err := url.Parse(s.URL)
	if err != nil {
		t.Errorf("Error parsing server URL: %v", err)
		return
	}
	streamer := &LocationStreamer{
		Location: u,
	}
	readCloser, _, _, err := streamer.InputStream(context.Background(), "", "")
	if err != nil {
		t.Errorf("Unexpected error when getting stream: %v", err)
		return
	}
	defer readCloser.Close()
	result, _ := io.ReadAll(readCloser)
	if string(result) != resultString {
		t.Errorf("Stream content does not match. Got: %s. Expected: %s.", string(result), resultString)
	}
}

// TestInputStreamNullLocation 验证 Location 为空时返回 nil 流而非错误，
// 调用方据此判断“无重定向”而不是当作失败。
func TestInputStreamNullLocation(t *testing.T) {
	streamer := &LocationStreamer{
		Location: nil,
	}
	readCloser, _, _, err := streamer.InputStream(context.Background(), "", "")
	if err != nil {
		t.Errorf("Unexpected error when getting stream with null location: %v", err)
	}
	if readCloser != nil {
		t.Errorf("Expected stream to be nil. Got: %#v", readCloser)
	}
}

// testTransport 用预置的 HTTP 响应文本伪造 RoundTripper，避免真实网络。
type testTransport struct {
	body string
}

func (tt *testTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := bufio.NewReader(bytes.NewBufferString(tt.body))
	return http.ReadResponse(r, req)
}

func fakeTransport(mime, message string) http.RoundTripper {
	content := fmt.Sprintf("HTTP/1.1 200 OK\nContent-Type: %s\n\n%s", mime, message)
	return &testTransport{body: content}
}

// TestInputStreamContentType 验证上游响应的 Content-Type 被原样透传，
// 供调用方决定如何解释流。
func TestInputStreamContentType(t *testing.T) {
	location, _ := url.Parse("http://www.example.com")
	streamer := &LocationStreamer{
		Location:  location,
		Transport: fakeTransport("application/json", "hello world"),
	}
	readCloser, _, contentType, err := streamer.InputStream(context.Background(), "", "")
	if err != nil {
		t.Errorf("Unexpected error when getting stream: %v", err)
		return
	}
	defer readCloser.Close()
	if contentType != "application/json" {
		t.Errorf("Unexpected content type. Got: %s. Expected: application/json", contentType)
	}
}

// TestInputStreamTransport 验证自定义 Transport 被用于拉取流。
func TestInputStreamTransport(t *testing.T) {
	message := "hello world"
	location, _ := url.Parse("http://www.example.com")
	streamer := &LocationStreamer{
		Location:  location,
		Transport: fakeTransport("text/plain", message),
	}
	readCloser, _, _, err := streamer.InputStream(context.Background(), "", "")
	if err != nil {
		t.Errorf("Unexpected error when getting stream: %v", err)
		return
	}
	defer readCloser.Close()
	result, _ := io.ReadAll(readCloser)
	if string(result) != message {
		t.Errorf("Stream content does not match. Got: %s. Expected: %s.", string(result), message)
	}
}

func fakeInternalServerErrorTransport(mime, message string) http.RoundTripper {
	content := fmt.Sprintf("HTTP/1.1 500 \"Internal Server Error\"\nContent-Type: %s\n\n%s", mime, message)
	return &testTransport{body: content}
}

// TestInputStreamInternalServerErrorTransport 验证 5xx 响应经 ResponseChecker 转换
// 为 InternalError，且保留上游给的消息作为原因。
func TestInputStreamInternalServerErrorTransport(t *testing.T) {
	message := "Pod is in PodPending"
	location, _ := url.Parse("http://www.example.com")
	streamer := &LocationStreamer{
		Location:        location,
		Transport:       fakeInternalServerErrorTransport("text/plain", message),
		ResponseChecker: NewGenericHttpResponseChecker(schema.GroupResource{}, ""),
	}
	expectedError := errors.NewInternalError(fmt.Errorf("%s", message))

	_, _, _, err := streamer.InputStream(context.Background(), "", "")
	if err == nil {
		t.Errorf("unexpected non-error")
		return
	}

	if !reflect.DeepEqual(err, expectedError) {
		t.Errorf("StreamInternalServerError does not match. Got: %s. Expected: %s.", err, expectedError)
	}
}

// TestInputStreamRedirects 验证 PreventRedirects 生效：重定向必须报错而不是被跟随，
// 否则 apiserver 会被诱导去访问任意地址（SSRF 风险）。
func TestInputStreamRedirects(t *testing.T) {
	const redirectPath = "/redirect"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == redirectPath {
			t.Fatal("Redirects should not be followed")
		} else {
			http.Redirect(w, req, redirectPath, http.StatusFound)
		}
	}))
	loc, err := url.Parse(s.URL)
	require.NoError(t, err, "Error parsing server URL")

	streamer := &LocationStreamer{
		Location:        loc,
		RedirectChecker: PreventRedirects,
	}
	_, _, _, err = streamer.InputStream(context.Background(), "", "")
	assert.Error(t, err, "Redirect should trigger an error")
}
