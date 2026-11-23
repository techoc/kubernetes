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

package util

import (
	"strings"
	"testing"
)

// TestParseEndpoint 验证 ParseEndpoint 能正确提取普通 socket 路径与 Linux 抽象命名空间地址。
func TestParseEndpoint(t *testing.T) {
	// 表驱动：desc 用例名，endpoint 输入，want 期望返回的路径。
	testCases := []struct {
		desc     string
		endpoint string
		want     string
	}{
		{
			desc:     "path with prefix",
			endpoint: "unix:///@path",
			want:     "@path",
		},
		{
			desc:     "path without prefix",
			endpoint: "unix:///path",
			want:     "/path",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			got, err := ParseEndpoint(tt.endpoint)
			if err != nil {
				t.Errorf("ParseEndpoint(%q) error: %v", tt.endpoint, err)
			}
			if got != tt.want {
				t.Errorf("ParseEndpoint(%q) = %q, want %q", tt.endpoint, got, tt.want)
			}
		})
	}
}

// TestParseEndpointError 验证非法端点（空字符串、非 unix scheme）会被拒绝并返回预期错误。
func TestParseEndpointError(t *testing.T) {
	// 表驱动：desc 用例名，endpoint 输入，wantErr 期望错误文本片段。
	testCases := []struct {
		desc     string
		endpoint string
		wantErr  string
	}{
		{
			desc:     "empty endpoint",
			endpoint: "",
			wantErr:  "remote KMS provider can't use empty string as endpoint",
		},
		{
			desc:     "invalid scheme",
			endpoint: "http:///path",
			wantErr:  "unsupported scheme \"http\" for remote KMS provider",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := ParseEndpoint(tt.endpoint)
			if err == nil {
				t.Errorf("ParseEndpoint(%q) error: %v", tt.endpoint, err)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("ParseEndpoint(%q) = %q, want %q", tt.endpoint, err, tt.wantErr)
			}
		})
	}
}
