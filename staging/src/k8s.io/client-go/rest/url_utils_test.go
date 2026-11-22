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

package rest

import (
	"path"
	"testing"

	"k8s.io/api/core/v1"
)

// TestValidatesHostParameter 验证 DefaultServerURL 对 Host/APIPath 的规范化：
// 裸主机名补 http://，带 scheme 的主机保留原样，API 路径统一拼到版本前缀之前。
func TestValidatesHostParameter(t *testing.T) {
	// 每个用例给出 Host、APIPath，以及拼接 versionedAPIPath 后的期望 URL。
	testCases := []struct {
		Host    string
		APIPath string

		URL string
		Err bool
	}{
		{"127.0.0.1", "", "http://127.0.0.1/" + v1.SchemeGroupVersion.Version, false},
		{"127.0.0.1:8080", "", "http://127.0.0.1:8080/" + v1.SchemeGroupVersion.Version, false},
		{"foo.bar.com", "", "http://foo.bar.com/" + v1.SchemeGroupVersion.Version, false},
		{"http://host/prefix", "", "http://host/prefix/" + v1.SchemeGroupVersion.Version, false},
		{"http://host", "", "http://host/" + v1.SchemeGroupVersion.Version, false},
		{"http://host", "/", "http://host/" + v1.SchemeGroupVersion.Version, false},
		{"http://host", "/other", "http://host/other/" + v1.SchemeGroupVersion.Version, false},
		// 主机名中带路径段（"host/server"）无法解析为合法 URL，必须报错。
		{"host/server", "", "", true},
	}
	for i, testCase := range testCases {
		// DefaultServerURL 返回基础 URL 和相对于 API 根路径的版本段。
		u, versionedAPIPath, err := DefaultServerURL(testCase.Host, testCase.APIPath, v1.SchemeGroupVersion, false)
		// 先按用例期望核对错误有无，避免对 nil URL 做后续操作。
		switch {
		case err == nil && testCase.Err:
			t.Errorf("expected error but was nil")
			continue
		case err != nil && !testCase.Err:
			t.Errorf("unexpected error %v", err)
			continue
		case err != nil:
			continue
		}
		// 版本段需要手动拼到路径上，才能得到客户端实际请求的 URL。
		u.Path = path.Join(u.Path, versionedAPIPath)
		if e, a := testCase.URL, u.String(); e != a {
			t.Errorf("%d: expected host %s, got %s", i, e, a)
			continue
		}
	}
}
