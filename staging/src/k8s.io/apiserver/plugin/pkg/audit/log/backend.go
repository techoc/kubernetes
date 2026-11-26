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

package log

import (
	"fmt"
	"io"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
)

const (
	// 以单行文本格式保存事件。
	FormatLegacy = "legacy"
	// 以结构化 JSON 格式保存事件。
	FormatJson = "json"

	// 插件名，用于帮助信息与日志。
	PluginName = "log"
)

// 日志后端支持的格式。
var AllowedFormats = []string{
	FormatLegacy,
	FormatJson,
}

// backend 把审计事件写到给定的 io.Writer（通常是文件或 stdout）。
type backend struct {
	out     io.Writer
	format  string
	encoder runtime.Encoder
}

// 编译期断言：实现 audit.Backend 接口。
var _ audit.Backend = &backend{}

// NewBackend 按指定格式创建日志审计后端；encoder 用于 json 格式的序列化。
func NewBackend(out io.Writer, format string, groupVersion schema.GroupVersion) audit.Backend {
	return &backend{
		out:     out,
		format:  format,
		encoder: audit.Codecs.LegacyCodec(groupVersion),
	}
}

// ProcessEvents 逐条写出事件；任一条失败都不影响其余事件，但整体返回 false。
func (b *backend) ProcessEvents(events ...*auditinternal.Event) bool {
	success := true
	for _, ev := range events {
		success = b.logEvent(ev) && success
	}
	return success
}

// logEvent 按配置格式写出一条事件；失败时通过插件错误通道上报，不向调用方返回错误。
func (b *backend) logEvent(ev *auditinternal.Event) bool {
	line := ""
	switch b.format {
	case FormatLegacy:
		line = audit.EventString(ev) + "\n"
	case FormatJson:
		bs, err := runtime.Encode(b.encoder, ev)
		if err != nil {
			audit.HandlePluginError(PluginName, err, ev)
			return false
		}
		line = string(bs[:])
	default:
		// 配置错误在运行期才暴露，交给审计插件错误回调而非静默丢弃。
		audit.HandlePluginError(PluginName, fmt.Errorf("log format %q is not in list of known formats (%s)",
			b.format, strings.Join(AllowedFormats, ",")), ev)
		return false
	}
	if _, err := fmt.Fprint(b.out, line); err != nil {
		audit.HandlePluginError(PluginName, err, ev)
		return false
	}
	return true
}

// Run 无后台工作，立即返回。
func (b *backend) Run(stopCh <-chan struct{}) error {
	return nil
}

// Shutdown 无需清理资源。
func (b *backend) Shutdown() {
	// 无需清理资源。
}

// String 返回插件名，用于日志与帮助信息。
func (b *backend) String() string {
	return PluginName
}
