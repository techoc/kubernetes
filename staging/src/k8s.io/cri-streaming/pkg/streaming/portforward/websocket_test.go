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

package portforward

import (
	"net/http"
	"reflect"
	"testing"
)

// TestV4Options 验证 NewV4Options 对 URL 中 port 查询参数的解析：
// 覆盖非 WebSocket 请求、参数缺失/非法，以及单个端口、逗号分隔、重复参数三种合法输入。
func TestV4Options(t *testing.T) {
	// 表驱动用例：url 构造请求地址；websocket 决定是否添加 Upgrade 头伪装 WebSocket 请求；
	// expectedOpts 是期望解析出的端口列表；expectedError 非空表示期望解析失败。
	tests := map[string]struct {
		url           string
		websocket     bool
		expectedOpts  *V4Options
		expectedError string
	}{
		// 非 WebSocket 请求无需端口，应返回空选项。
		"non-ws request": {
			url:          "http://example.com",
			expectedOpts: &V4Options{},
		},
		// WebSocket 请求缺少 port 参数应报错。
		"missing port": {
			url:           "http://example.com",
			websocket:     true,
			expectedError: `query parameter "port" is required`,
		},
		// 非数字端口无法解析为整数。
		"unable to parse port": {
			url:           "http://example.com?port=abc",
			websocket:     true,
			expectedError: `unable to parse "abc" as a port: strconv.ParseUint: parsing "abc": invalid syntax`,
		},
		// 负数同样无法通过无符号整数解析。
		"negative port": {
			url:           "http://example.com?port=-1",
			websocket:     true,
			expectedError: `unable to parse "-1" as a port: strconv.ParseUint: parsing "-1": invalid syntax`,
		},
		// 单个合法端口。
		"one port": {
			url:       "http://example.com?port=80",
			websocket: true,
			expectedOpts: &V4Options{
				Ports: []int32{80},
			},
		},
		// 逗号分隔可一次传入多个端口。
		"multiple ports": {
			url:       "http://example.com?port=80,90,100",
			websocket: true,
			expectedOpts: &V4Options{
				Ports: []int32{80, 90, 100},
			},
		},
		// 重复出现的 port 参数应全部收集。
		"multiple port": {
			url:       "http://example.com?port=80&port=90",
			websocket: true,
			expectedOpts: &V4Options{
				Ports: []int32{80, 90},
			},
		},
	}
	// 逐用例构造请求、按需添加 WebSocket 头，再调用 NewV4Options 并断言结果。
	for name, test := range tests {
		req, err := http.NewRequest(http.MethodGet, test.url, nil)
		if err != nil {
			t.Errorf("%s: invalid url %q err=%q", name, test.url, err)
			continue
		}
		if test.websocket {
			// 通过 Connection/Upgrade 头让请求被识别为 WebSocket 请求。
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
		}
		opts, err := NewV4Options(req)
		if len(test.expectedError) > 0 {
			// 期望失败的用例必须返回错误，且错误文本与预期一致。
			if err == nil {
				t.Errorf("%s: expected err=%q, but it was nil", name, test.expectedError)
			}
			if e, a := test.expectedError, err.Error(); e != a {
				t.Errorf("%s: expected err=%q, got %q", name, e, a)
			}
			continue
		}
		// 期望成功的用例不应返回错误。
		if err != nil {
			t.Errorf("%s: unexpected error %v", name, err)
			continue
		}
		// 用深比较校验解析出的端口列表与期望完全一致。
		if !reflect.DeepEqual(test.expectedOpts, opts) {
			t.Errorf("%s: expected options %#v, got %#v", name, test.expectedOpts, err)
		}
	}
}
