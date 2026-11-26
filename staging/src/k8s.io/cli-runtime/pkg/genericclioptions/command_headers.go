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

// CommandHeaderRoundTripper 包装标准 RoundTripper，在委托前注入请求头；
// 实现 Go 标准库的 "http.RoundTripper" 接口。
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

// RoundTrip 在委托给标准 RoundTripper 前注入请求头；
// 这些头描述当前 kubectl 命令，详见 SIG CLI KEP 859：
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

// ParseCommandHeaders 填充 CommandHeaderRoundTripper 的自定义头，
// 后续每个请求都会携带这些头。头字段含义见：
//
//	https://github.com/kubernetes/enhancements/tree/master/keps/sig-cli/859-kubectl-headers
//
// 每次调用整体覆盖上次解析出的命令头，而非追加。
// TODO(seans3): 解析/添加标志时剔除标志值中的个人信息。
//
// ParseCommandHeaders 根据 cobra 命令链重建“kubectl create secret generic”这类
// 完整命令串，并生成一次性会话 ID；重复调用会整体替换而非累加。
func (c *CommandHeaderRoundTripper) ParseCommandHeaders(cmd *cobra.Command, args []string) {
	if cmd == nil {
		return
	}
	// 覆盖上次解析的命令头，不追加。
	c.Headers = map[string]string{}
	// 会话 ID 用于聚合同一条 kubectl 命令发出的多个请求。
	// 会话 ID 让服务端把同一条命令触发的多次请求归为一组。
	uid := uuid.New().String()
	c.Headers[kubectlSessionHeader] = uid
	// 从叶子命令向上遍历命令层级，拼出完整命令串。
	// 例如：kubectl create secret generic
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

// 仅在被包装的 RoundTripper 实现 CancelRequest 时，
// 才把该调用转发给 Delegate。
//
// 这是旧版 http.RoundTripper 的可选接口；仅当 Delegate 也实现时才转发。
func (c *CommandHeaderRoundTripper) CancelRequest(req *http.Request) {
	type canceler interface {
		CancelRequest(*http.Request)
	}
	// 如果 Delegate 支持，就调用其 "CancelRequest"。
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
