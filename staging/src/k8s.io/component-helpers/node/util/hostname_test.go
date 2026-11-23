/*
Copyright 2019 The Kubernetes Authors.

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
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestGetHostname 验证 GetHostname 的覆盖优先级与规范化规则。
// “空参数”这一用例直接拿 os.Hostname() 的返回值做期望，是为了不依赖测试机上的具体主机名：
// 环境无关地断言“空覆盖值会回退到系统主机名”。
func TestGetHostname(t *testing.T) {
	// 取系统主机名作为环境基准，供“空参数回退”用例构造期望值。
	hostname, err := os.Hostname()

	// testCases 各字段含义：desc 是用例描述（同时作为子测试名），hostname 是传给
	// GetHostname 的覆盖值，result 是期望的规范化结果，expectedErr 是期望的错误（nil 表示应成功）。
	testCases := []struct {
		desc        string
		hostname    string
		result      string
		expectedErr error
	}{
		{
			// 显式覆盖值原样返回。
			desc:        "overridden hostname",
			hostname:    "overridden",
			result:      "overridden",
			expectedErr: nil,
		},
		{
			// 大写覆盖值要转小写，保证同一节点无论怎么写都得到同一个名字。
			desc:        "overridden hostname uppercase",
			hostname:    "OVERRIDDEN",
			result:      "overridden",
			expectedErr: nil,
		},
		{
			// 首尾空白要被修剪，结果也要转成小写。
			desc:        "hostname contains spaces",
			hostname:    " OVERRIDDEN ",
			result:      "overridden",
			expectedErr: nil,
		},
		{
			// 去空白发生在返回值校验之前。全空白输入会被修剪成空串，必须报错，
			// 否则会悄悄产生一个名字为空的节点身份，排查起来极其困难。
			desc:        "hostname contains only spaces",
			hostname:    " ",
			result:      "",
			expectedErr: fmt.Errorf("empty hostname is invalid"),
		},
		{
			// 空覆盖值回退到系统主机名；期望值用 os.Hostname() 动态生成，避免绑定具体环境。
			desc:        "empty parameter",
			hostname:    "",
			result:      strings.ToLower(hostname),
			expectedErr: err,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := GetHostname(tc.hostname)

			// 分别检查“不该报错却报错”和“该报错却没报错”两个方向。
			if err != nil && tc.expectedErr == nil {
				t.Errorf("unexpected error: %v", err)
			}

			if err == nil && tc.expectedErr != nil {
				t.Errorf("expected error %v, got nil", tc.expectedErr)
			}

			// 最后比对规范化后的主机名；错误方向的检查不替代结果检查。
			if tc.result != result {
				t.Errorf("unexpected result: %s, expected: %s", result, tc.result)
			}
		})
	}
}
