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
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

const (
	// 请求头名遵循 SIG CLI KEP 859：服务端据此识别请求来自哪条 kubectl 命令。
	kubectlCommandHeader = "Kubectl-Command"
	kubectlSessionHeader = "Kubectl-Session"
)

// CommandHeaderRoundTripper adds a layer around the standard
// round tripper to add Request headers before delegation. Implements
// the go standard library "http.RoundTripper" interface.
//
// CommandHeaderRoundTripper 是一个 http.RoundTripper 装饰器，在真正发送请求前
// 注入 kubectl 命令与会话标识，方便服务端关联同一条命令产生的多个请求。
type CommandHeaderRoundTripper struct {
	// Delegate 是实际执行请求的 RoundTripper。
	Delegate http.RoundTripper
	// Headers 由 ParseCommandHeaders 填充，随每个请求一起发送。
	Headers map[string]string
	// SkipHeaders 非 nil 且为 true 时跳过注入，用于服务端或用户显式关闭该特性。
	SkipHeaders *atomic.Bool
}

// CommandHeaderRoundTripper adds Request headers before delegating to standard
// round tripper. These headers are kubectl command headers which
// detail the kubectl command. See SIG CLI KEP 859:
//
//	https://github.com/kubernetes/enhancements/tree/master/keps/sig-cli/859-kubectl-headers
//
// RoundTrip 先注入头部再委托，保证每条出站请求都带上命令信息。
func (c *CommandHeaderRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// 关闭开关时完全透传，避免对 Delegate 行为产生任何影响。
	if c.shouldSkipHeaders() {
		return c.Delegate.RoundTrip(req)
	}

	for header, value := range c.Headers {
		req.Header.Set(header, value)
	}

	return c.Delegate.RoundTrip(req)
}

// ParseCommandHeaders fills in a map of custom headers into the CommandHeaderRoundTripper. These
// headers are then filled into each request. For details on the custom headers see:
//
//	https://github.com/kubernetes/enhancements/tree/master/keps/sig-cli/859-kubectl-headers
//
// Each call overwrites the previously parsed command headers (not additive).
// TODO(seans3): Parse/add flags removing PII from flag values.
//
// ParseCommandHeaders 根据 cobra 命令链重建“kubectl create secret generic”这类
// 完整命令串，并生成一次性会话 ID；重复调用会整体替换而非累加。
func (c *CommandHeaderRoundTripper) ParseCommandHeaders(cmd *cobra.Command, args []string) {
	if cmd == nil {
		return
	}
	// Overwrites previously parsed command headers (headers not additive).
	c.Headers = map[string]string{}
	// Session identifier to aggregate multiple Requests from single kubectl command.
	// 会话 ID 让服务端把同一条命令触发的多次请求归为一组。
	uid := uuid.New().String()
	c.Headers[kubectlSessionHeader] = uid
	// Iterate up the hierarchy of commands from the leaf command to create
	// the full command string. Example: kubectl create secret generic
	cmdStrs := []string{}
	for cmd.HasParent() {
		parent := cmd.Parent()
		currName := strings.TrimSpace(cmd.Name())
		cmdStrs = append([]string{currName}, cmdStrs...)
		cmd = parent
	}
	currName := strings.TrimSpace(cmd.Name())
	cmdStrs = append([]string{currName}, cmdStrs...)
	if len(cmdStrs) > 0 {
		c.Headers[kubectlCommandHeader] = strings.Join(cmdStrs, " ")
	}
}

// CancelRequest is propagated to the Delegate RoundTripper within
// if the wrapped RoundTripper implements this function.
//
// 这是旧版 http.RoundTripper 的可选接口；仅当 Delegate 也实现时才转发。
func (c *CommandHeaderRoundTripper) CancelRequest(req *http.Request) {
	type canceler interface {
		CancelRequest(*http.Request)
	}
	// If possible, call "CancelRequest" on the wrapped Delegate RoundTripper.
	if cr, ok := c.Delegate.(canceler); ok {
		cr.CancelRequest(req)
	}
}

// shouldSkipHeaders 在 SkipHeaders 未设置（nil）时默认不跳过，保持向后兼容。
func (c *CommandHeaderRoundTripper) shouldSkipHeaders() bool {
	if c.SkipHeaders == nil {
		return false
	}

	return c.SkipHeaders.Load()
}
