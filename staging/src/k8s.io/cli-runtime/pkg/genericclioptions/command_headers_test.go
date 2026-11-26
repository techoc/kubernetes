/*
Copyright 2021 The Kubernetes Authors.

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

package genericclioptions

import (
	"net/http"
	"testing"

	"github.com/spf13/cobra"
)

// 测试用的命令节点；TestParseCommandHeaders 会按顺序把它们串成父子链。
var kubectlCmd = &cobra.Command{Use: "kubectl"}
var applyCmd = &cobra.Command{Use: "apply"}
var createCmd = &cobra.Command{Use: "create"}
var secretCmd = &cobra.Command{Use: "secret"}
var genericCmd = &cobra.Command{Use: "generic"}
var authCmd = &cobra.Command{Use: "auth"}
var reconcileCmd = &cobra.Command{Use: "reconcile"}

// TestParseCommandHeaders 验证从任意层级的 cobra 命令都能还原出以空格分隔的完整命令路径。
func TestParseCommandHeaders(t *testing.T) {
	tests := map[string]struct {
		// 顺序很关键：后一个命令都作为前一个命令的子命令挂载。
		commands []*cobra.Command
		// 以下头必须存在，但允许其他头同时存在。
		expectedHeaders map[string]string
	}{
		"Single kubectl command example": {
			commands: []*cobra.Command{kubectlCmd},
			expectedHeaders: map[string]string{
				kubectlCommandHeader: "kubectl",
			},
		},
		"Simple kubectl apply example": {
			commands: []*cobra.Command{kubectlCmd, applyCmd},
			expectedHeaders: map[string]string{
				kubectlCommandHeader: "kubectl apply",
			},
		},
		"Kubectl auth reconcile example": {
			commands: []*cobra.Command{kubectlCmd, authCmd, reconcileCmd},
			expectedHeaders: map[string]string{
				kubectlCommandHeader: "kubectl auth reconcile",
			},
		},
		"Long kubectl create secret generic example": {
			commands: []*cobra.Command{kubectlCmd, createCmd, secretCmd, genericCmd},
			expectedHeaders: map[string]string{
				kubectlCommandHeader: "kubectl create secret generic",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rootCmd := buildCommandChain(tc.commands)
			ch := &CommandHeaderRoundTripper{}
			ch.ParseCommandHeaders(rootCmd, []string{})
			// 唯一会话 ID 头必须存在：无论命令层级如何，会话 ID 都必须生成，测试不作具体值断言。
			if _, found := ch.Headers[kubectlSessionHeader]; !found {
				t.Errorf("expected kubectl session header (%s) is missing", kubectlSessionHeader)
			}
			// 所有期望的头都必须出现，但允许多余的头。
			for key, expectedValue := range tc.expectedHeaders {
				actualValue, found := ch.Headers[key]
				if found {
					if expectedValue != actualValue {
						t.Errorf("expected header value (%s), got (%s)", expectedValue, actualValue)
					}
				} else {
					t.Errorf("expected header (%s) not found", key)
				}
			}
		})
	}
}

// 按传入顺序把每个命令挂为前一个命令的子命令，构建命令层级，
// 并返回最末端的叶子命令。
//
// buildCommandChain 按切片顺序构造父子链，并返回最末端的叶子命令。
func buildCommandChain(commands []*cobra.Command) *cobra.Command {
	var currCmd *cobra.Command
	if len(commands) > 0 {
		currCmd = commands[0]
	}
	for i := 1; i < len(commands); i++ {
		cmd := commands[i]
		currCmd.AddCommand(cmd)
		currCmd = cmd
	}
	return currCmd
}

// 验证 CancelRequest 会转发给被包装的 Delegate，
// 但前提是 Delegate 自身实现了该方法。
//
// TestCancelRequest 覆盖“Delegate 实现了 CancelRequest”和“未实现”两种情况。
func TestCancelRequest(t *testing.T) {
	tests := map[string]struct {
		delegate  http.RoundTripper
		cancelled bool
	}{
		"CancelRequest propagated to delegate": {
			delegate:  &cancellableRoundTripper{},
			cancelled: true,
		},
		"CancelRequest not propagated to delegate": {
			delegate:  &nonCancellableRoundTripper{},
			cancelled: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rt := &CommandHeaderRoundTripper{
				Delegate: tc.delegate,
			}
			req := http.Request{}
			rt.CancelRequest(&req)
			if tc.cancelled != req.Close {
				t.Errorf("expected RoundTripper cancel (%v), got (%v)", tc.cancelled, req.Close)
			}
		})
	}
}

// cancellableRoundTripper 实现 CancelRequest，通过把 req.Close 置为 true 来模拟取消已生效，
// 用于测试带 CancelRequest 的 RoundTripper。
type cancellableRoundTripper struct{}

func (rtc *cancellableRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, nil
}

func (rtc *cancellableRoundTripper) CancelRequest(req *http.Request) {
	req.Close = true
}

// nonCancellableRoundTripper 故意不实现 CancelRequest，用于测试不带 CancelRequest 的
// RoundTripper，验证不会误调用。
type nonCancellableRoundTripper struct{}

func (rtc *nonCancellableRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, nil
}
