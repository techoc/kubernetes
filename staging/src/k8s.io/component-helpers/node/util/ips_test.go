/*
Copyright 2023 The Kubernetes Authors.

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
	"net"
	"reflect"
	"strings"
	"testing"

	netutils "k8s.io/utils/net"
)

// TestParseNodeIPArgument 固定 kubelet 命令行参数的宽容契约：无法解析的片段
// 放进 invalids 返回而不是让整个解析失败，因为 kubelet 仍需要用其余合法 IP 启动，
// 调用方自行决定如何告警。真正的错误只留给“结构不成立”的输入，比如两个同族地址、
// 超过两个地址、双栈里出现 unspecified 地址。
// 每个用例还要在多种 cloudProvider 配置下跑一遍，因为双栈是否允许取决于 provider。
func TestParseNodeIPArgument(t *testing.T) {
	// testCases 各字段含义：desc 是用例描述（子测试名的一部分），in 是原始参数字符串，
	// out 是期望解析出的 IP 列表，invalids 是期望单列的无效片段，err 是常规配置下期望的
	// 错误子串，ssErr 是禁止双栈的配置下期望的错误子串（为空表示该配置下也应成功）。
	testCases := []struct {
		desc     string
		in       string
		out      []net.IP
		invalids []string
		err      string
		ssErr    string
	}{
		{
			// 空参数在宽松模式下视为“未配置”，返回空列表且不报错。
			desc: "empty --node-ip",
			in:   "",
			out:  nil,
		},
		{
			// 纯空白被修剪成空串，作为无效项返回，但不让整体失败。
			desc:     "just whitespace (ignored)",
			in:       " ",
			out:      nil,
			invalids: []string{""},
		},
		{
			// 完全无法解析的文本进入 invalids，交给调用方决定是否告警。
			desc:     "garbage (ignored)",
			in:       "blah",
			out:      nil,
			invalids: []string{"blah"},
		},
		{
			// 单个合法 IPv4 正常解析。
			desc: "single IPv4",
			in:   "1.2.3.4",
			out: []net.IP{
				netutils.ParseIPSloppy("1.2.3.4"),
			},
		},
		{
			// 宽松模式会修剪首尾空白。
			desc: "single IPv4 with whitespace",
			in:   " 1.2.3.4   ",
			out: []net.IP{
				netutils.ParseIPSloppy("1.2.3.4"),
			},
		},
		{
			// 前导零写法由宽松解析器接受并规范化。
			desc: "single IPv4 non-canonical",
			in:   "01.2.3.004",
			out: []net.IP{
				netutils.ParseIPSloppy("1.2.3.4"),
			},
		},
		{
			// 格式不合法的 IPv4 进入 invalids，而不是报错。
			desc:     "single IPv4 invalid (ignored)",
			in:       "1.2.3",
			out:      nil,
			invalids: []string{"1.2.3"},
		},
		{
			// 带掩码的 CIDR 不是节点 IP，视为无效项。
			desc:     "single IPv4 CIDR (ignored)",
			in:       "1.2.3.0/24",
			out:      nil,
			invalids: []string{"1.2.3.0/24"},
		},
		{
			// 单地址场景下 unspecified 地址本身是允许的。
			desc: "single IPv4 unspecified",
			in:   "0.0.0.0",
			out: []net.IP{
				net.IPv4zero,
			},
		},
		{
			// 合法地址与坏片段混用时，保留好地址并单列坏片段。
			desc: "single IPv4 plus ignored garbage",
			in:   "1.2.3.4,not-an-IPv6-address",
			out: []net.IP{
				netutils.ParseIPSloppy("1.2.3.4"),
			},
			invalids: []string{"not-an-IPv6-address"},
		},
		{
			// 单个合法 IPv6 正常解析。
			desc: "single IPv6",
			in:   "abcd::ef01",
			out: []net.IP{
				netutils.ParseIPSloppy("abcd::ef01"),
			},
		},
		{
			// 非规范的 IPv6 写法被规范化。
			desc: "single IPv6 non-canonical",
			in:   "abcd:0abc:00ab:0000:0000::1",
			out: []net.IP{
				netutils.ParseIPSloppy("abcd:abc:ab::1"),
			},
		},
		{
			// IPv4+IPv6 是合法双栈；ssErr 供不支持双栈的 provider 配置改写期望。
			desc: "simple dual-stack",
			in:   "1.2.3.4,abcd::ef01",
			out: []net.IP{
				netutils.ParseIPSloppy("1.2.3.4"),
				netutils.ParseIPSloppy("abcd::ef01"),
			},
			ssErr: "not supported in this configuration",
		},
		{
			// 双栈地址两侧的空白会被修剪。
			desc: "dual-stack with whitespace",
			in:   "abcd::ef01 , 1.2.3.4",
			out: []net.IP{
				netutils.ParseIPSloppy("abcd::ef01"),
				netutils.ParseIPSloppy("1.2.3.4"),
			},
			ssErr: "not supported in this configuration",
		},
		{
			// 两个同族地址不构成双栈，属于结构性错误。
			desc: "double IPv4",
			in:   "1.2.3.4,5.6.7.8",
			err:  "either a single IP or a dual-stack pair of IPs",
		},
		{
			// 同上，两个 IPv6 也应报错。
			desc: "double IPv6",
			in:   "abcd::1,abcd::2",
			err:  "either a single IP or a dual-stack pair of IPs",
		},
		{
			// 双栈里出现 :: 会导致分配出不可路由的地址，必须拒绝。
			desc:  "dual-stack with unspecified",
			in:    "1.2.3.4,::",
			err:   "cannot include '0.0.0.0' or '::'",
			ssErr: "not supported in this configuration",
		},
		{
			// 同上，换成 IPv4 的 0.0.0.0 同样拒绝。
			desc:  "dual-stack with unspecified",
			in:    "0.0.0.0,abcd::1",
			err:   "cannot include '0.0.0.0' or '::'",
			ssErr: "not supported in this configuration",
		},
		{
			// 双栈夹杂坏片段时，坏片段进 invalids，不影响双栈结果。
			desc: "dual-stack plus ignored garbage",
			in:   "abcd::ef01 , 1.2.3.4, something else",
			out: []net.IP{
				netutils.ParseIPSloppy("abcd::ef01"),
				netutils.ParseIPSloppy("1.2.3.4"),
			},
			invalids: []string{"something else"},
			ssErr:    "not supported in this configuration",
		},
		{
			// 超过两个地址属于结构性错误。
			desc: "triple stack!",
			in:   "1.2.3.4,abcd::1,5.6.7.8",
			err:  "either a single IP or a dual-stack pair of IPs",
		},
	}

	// 未指定 provider 和外部 provider 都允许双栈；内置云 provider 的
	// 网络模型未必支持双栈，因此这里用 gce 代表“不支持”的那一支。
	// configurations 中的 cloudProvider 是 provider 名称，dualStackSupported 表示
	// 该配置下是否允许双栈，用于在断言前改写期望值。
	configurations := []struct {
		cloudProvider      string
		dualStackSupported bool
	}{
		// 未指定 provider，允许双栈。
		{cloudProviderNone, true},
		// 外部 provider，允许双栈。
		{cloudProviderExternal, true},
		// 用 gce 代表不支持双栈的内置 provider。
		{"gce", false},
	}

	for _, tc := range testCases {
		for _, conf := range configurations {
			desc := fmt.Sprintf("%s, cloudProvider=%q", tc.desc, conf.cloudProvider)
			t.Run(desc, func(t *testing.T) {
				parsed, invalidIPs, err := ParseNodeIPArgument(tc.in, conf.cloudProvider)

				expectedOut := tc.out
				expectedInvalids := tc.invalids
				expectedErr := tc.err

				// 同一份输入在不同 provider 下期望值不同：双栈被禁时，
				// 即使原本能解析出两个地址，也要按“无地址 + 明确错误”处理。
				if !conf.dualStackSupported {
					if len(tc.out) == 2 {
						expectedOut = nil
					}
					if tc.ssErr != "" {
						expectedErr = tc.ssErr
					}
				}

				// 先比对解析出的地址列表。
				if !reflect.DeepEqual(parsed, expectedOut) {
					t.Errorf("expected %#v, got %#v", expectedOut, parsed)
				}
				// 再比对被单列的无效片段。
				if !reflect.DeepEqual(invalidIPs, expectedInvalids) {
					t.Errorf("[invalidIps] expected %#v, got %#v", expectedInvalids, invalidIPs)
				}
				// 错误信息按子串匹配，避免绑定完整文案；同时覆盖报错与未报错两个方向。
				if err != nil {
					if expectedErr == "" {
						t.Errorf("unexpected error %v", err)
					} else if !strings.Contains(err.Error(), expectedErr) {
						t.Errorf("expected error with %q, got %v", expectedErr, err)
					}
				} else if expectedErr != "" {
					t.Errorf("expected error with %q, got no error", expectedErr)
				}
			})
		}
	}
}

// TestParseNodeIPAnnotation 固定与命令行参数相反的严格契约：这是来自云 provider
// 的机器可读字段，任何格式问题都直接报错，不再走“忽略坏片段”的宽容分支。
// 注意这里没有 configurations 循环：注解解析不感知 cloudProvider，双栈始终允许。
func TestParseNodeIPAnnotation(t *testing.T) {
	// testCases 各字段含义：desc 是用例描述，in 是注解原文，out 是期望解析出的 IP 列表，
	// err 是期望的错误子串；ssErr 为保留字段，严格模式下双栈始终允许，目前不参与断言。
	testCases := []struct {
		desc  string
		in    string
		out   []net.IP
		err   string
		ssErr string
	}{
		{
			// 注解为空属于错误输入，直接解析失败。
			desc: "empty --node-ip",
			in:   "",
			err:  "could not parse",
		},
		{
			// 严格模式不修剪空白，纯空白输入无法解析。
			desc: "just whitespace",
			in:   " ",
			err:  "could not parse",
		},
		{
			// 非 IP 文本直接判错。
			desc: "garbage",
			in:   "blah",
			err:  "could not parse",
		},
		{
			// 单个 IPv4 正常解析。
			desc: "single IPv4",
			in:   "1.2.3.4",
			out: []net.IP{
				netutils.ParseIPSloppy("1.2.3.4"),
			},
		},
		{
			// 严格模式不修剪空白，带空格的地址视为无效。
			desc: "single IPv4 with whitespace",
			in:   " 1.2.3.4   ",
			err:  "could not parse",
		},
		{
			// 宽松解析器仍接受前导零写法。
			desc: "single IPv4 non-canonical",
			in:   "01.2.3.004",
			out: []net.IP{
				netutils.ParseIPSloppy("1.2.3.4"),
			},
		},
		{
			// 格式不合法的 IPv4 报错。
			desc: "single IPv4 invalid",
			in:   "1.2.3",
			err:  "could not parse",
		},
		{
			// CIDR 不是节点 IP，属于无效注解。
			desc: "single IPv4 CIDR",
			in:   "1.2.3.0/24",
			err:  "could not parse",
		},
		{
			// 单个 unspecified 地址允许。
			desc: "single IPv4 unspecified",
			in:   "0.0.0.0",
			out: []net.IP{
				net.IPv4zero,
			},
		},
		{
			// 混有坏片段时整体失败，不像参数解析那样单列无效项。
			desc: "single IPv4 plus garbage",
			in:   "1.2.3.4,not-an-IPv6-address",
			err:  "could not parse",
		},
		{
			// 单个 IPv6 正常解析。
			desc: "single IPv6",
			in:   "abcd::ef01",
			out: []net.IP{
				netutils.ParseIPSloppy("abcd::ef01"),
			},
		},
		{
			// 非规范 IPv6 写法被规范化。
			desc: "single IPv6 non-canonical",
			in:   "abcd:0abc:00ab:0000:0000::1",
			out: []net.IP{
				netutils.ParseIPSloppy("abcd:abc:ab::1"),
			},
		},
		{
			// 注解解析始终允许双栈，ssErr 在此不会生效。
			desc: "simple dual-stack",
			in:   "1.2.3.4,abcd::ef01",
			out: []net.IP{
				netutils.ParseIPSloppy("1.2.3.4"),
				netutils.ParseIPSloppy("abcd::ef01"),
			},
			ssErr: "not supported in this configuration",
		},
		{
			// 逗号后的空格不被修剪，导致解析失败。
			desc: "dual-stack with whitespace",
			in:   "abcd::ef01 , 1.2.3.4",
			err:  "could not parse",
		},
		{
			// 两个同族地址不构成双栈。
			desc: "double IPv4",
			in:   "1.2.3.4,5.6.7.8",
			err:  "either a single IP or a dual-stack pair of IPs",
		},
		{
			// 同上，两个 IPv6 报错。
			desc: "double IPv6",
			in:   "abcd::1,abcd::2",
			err:  "either a single IP or a dual-stack pair of IPs",
		},
		{
			// 双栈里出现 :: 必须拒绝。
			desc:  "dual-stack with unspecified",
			in:    "1.2.3.4,::",
			err:   "cannot include '0.0.0.0' or '::'",
			ssErr: "not supported in this configuration",
		},
		{
			// 双栈里出现 0.0.0.0 同样拒绝。
			desc:  "dual-stack with unspecified",
			in:    "0.0.0.0,abcd::1",
			err:   "cannot include '0.0.0.0' or '::'",
			ssErr: "not supported in this configuration",
		},
		{
			// 带空白或坏片段的双栈输入整体失败。
			desc: "dual-stack plus garbage",
			in:   "abcd::ef01 , 1.2.3.4, something else",
			err:  "could not parse",
		},
		{
			// 超过两个地址报错。
			desc: "triple stack!",
			in:   "1.2.3.4,abcd::1,5.6.7.8",
			err:  "either a single IP or a dual-stack pair of IPs",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			parsed, err := ParseNodeIPAnnotation(tc.in)

			expectedOut := tc.out
			expectedErr := tc.err

			// 先比对解析出的地址列表。
			if !reflect.DeepEqual(parsed, expectedOut) {
				t.Errorf("expected %#v, got %#v", expectedOut, parsed)
			}
			// 错误信息按子串匹配，并覆盖“报错 / 未报错”两个方向。
			if err != nil {
				if expectedErr == "" {
					t.Errorf("unexpected error %v", err)
				} else if !strings.Contains(err.Error(), expectedErr) {
					t.Errorf("expected error with %q, got %v", expectedErr, err)
				}
			} else if expectedErr != "" {
				t.Errorf("expected error with %q, got no error", expectedErr)
			}
		})
	}
}
