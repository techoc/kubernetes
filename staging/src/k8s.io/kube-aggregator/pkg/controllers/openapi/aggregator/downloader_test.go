/*
Copyright 2017 The Kubernetes Authors.

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

package aggregator

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/kube-openapi/pkg/validation/spec"
)

// 用最小 handler 模拟后端：只实现 ETag/304 协议，把下载逻辑和真实 apiserver
// 的路由、认证隔离开，让 ETag 相关分支可以单独验证。
type handlerTest struct {
	etag string
	data []byte
}

var _ http.Handler = handlerTest{}

func (h handlerTest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if len(h.etag) > 0 {
		w.Header().Add("Etag", h.etag)
	}
	ifNoneMatches := r.Header["If-None-Match"]
	for _, match := range ifNoneMatches {
		if match == h.etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	w.Write(h.data)
}

// 老版本 apiserver 不认识 /openapi/v2，会以 403 回应，而不是标准的 404；
// 保留这个模拟实现是为了以后能复现并覆盖这种回退场景，避免适配逻辑被无意改坏。
type handlerDeprecatedTest struct {
	etag string
	data []byte
}

var _ http.Handler = handlerDeprecatedTest{}

func (h handlerDeprecatedTest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// old server returns 403 on new endpoint
	if r.URL.Path == "/openapi/v2" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if len(h.etag) > 0 {
		w.Header().Add("Etag", h.etag)
	}
	ifNoneMatches := r.Header["If-None-Match"]
	for _, match := range ifNoneMatches {
		if match == h.etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	w.Write(h.data)
}

// 把 spec、ETag 和 error 一起断言，是因为下载结果的三者组合才有意义：
// 304 时不应返回 spec 但应保留 ETag，否则缓存命中判定会出错。
func assertDownloadedSpec(actualSpec *spec.Swagger, actualEtag string, err error,
	expectedSpecID string, expectedEtag string) error {
	if err != nil {
		return fmt.Errorf("downloadOpenAPISpec failed : %s", err)
	}
	if expectedSpecID == "" && actualSpec != nil {
		return fmt.Errorf("expected Not Modified, actual ID %s", actualSpec.ID)
	}
	if actualSpec != nil && actualSpec.ID != expectedSpecID {
		return fmt.Errorf("expected ID %s, actual ID %s", expectedSpecID, actualSpec.ID)
	}
	if actualEtag != expectedEtag {
		return fmt.Errorf("expected ETag '%s', actual ETag '%s'", expectedEtag, actualEtag)
	}
	return nil
}

func TestDownloadOpenAPISpec(t *testing.T) {
	s := Downloader{}

	// Test with no eTag
	actualSpec, actualEtag, _, err := s.Download(handlerTest{data: []byte("{\"id\": \"test\"}")}, "")
	assert.NoError(t, assertDownloadedSpec(actualSpec, actualEtag, err, "test", "\"6E8F849B434D4B98A569B9D7718876E9-356ECAB19D7FBE1336BABB1E70F8F3025050DE218BE78256BE81620681CFC9A268508E542B8B55974E17B2184BBFC8FFFAA577E51BE195D32B3CA2547818ABE4\""))

	// Test with eTag
	actualSpec, actualEtag, _, err = s.Download(
		handlerTest{data: []byte("{\"id\": \"test\"}"), etag: "etag_test"}, "")
	assert.NoError(t, assertDownloadedSpec(actualSpec, actualEtag, err, "test", "etag_test"))

	// Test not modified
	actualSpec, actualEtag, _, err = s.Download(
		handlerTest{data: []byte("{\"id\": \"test\"}"), etag: "etag_test"}, "etag_test")
	assert.NoError(t, assertDownloadedSpec(actualSpec, actualEtag, err, "", "etag_test"))

	// Test different eTags
	actualSpec, actualEtag, _, err = s.Download(
		handlerTest{data: []byte("{\"id\": \"test\"}"), etag: "etag_test1"}, "etag_test2")
	assert.NoError(t, assertDownloadedSpec(actualSpec, actualEtag, err, "test", "etag_test1"))
}
