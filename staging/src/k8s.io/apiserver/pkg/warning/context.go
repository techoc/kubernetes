/*
Copyright 2020 The Kubernetes Authors.

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

// Package warning 提供把警告写入 HTTP Warning 响应头的 context 机制。
// 插件通过 context 拿到 Recorder，从而与具体的响应写入方式解耦；
// 警告是 RFC 7234 定义的带外信息，不应影响响应状态码或正文。
package warning

import (
	"context"
)

// key 类型未导出，避免与其他包定义的 context key 冲突。
// 与 request 包采用同样的未导出 key 约定，防止跨包覆盖 context 值。
type key int

const (
	// warningRecorderKey 是存放 warning recorder 的 context key。
	warningRecorderKey key = iota
)

// Recorder 提供记录警告的方法。
//
// 实现方负责把警告写入 HTTP Warning 头；输入校验（UTF-8、无控制字符）
// 是契约的一部分，调用方不能假设 Recorder 会宽容处理非法内容。
type Recorder interface {
	// AddWarning 向响应中添加指定告警。
	// agent 必须是合法 UTF-8，且不得包含空格、引号、反斜杠或控制字符。
	// text 必须是合法 UTF-8，且不得包含控制字符。
	AddWarning(agent, text string)
}

// WithWarningRecorder 返回包装了给定 context 的新 context，其中携带给定的 Recorder 实现。
// 返回的 context 可传给 AddWarning() 使用。
func WithWarningRecorder(ctx context.Context, recorder Recorder) context.Context {
	return context.WithValue(ctx, warningRecorderKey, recorder)
}

func warningRecorderFrom(ctx context.Context) (Recorder, bool) {
	recorder, ok := ctx.Value(warningRecorderKey).(Recorder)
	return recorder, ok
}

// AddWarning 把指定 agent 与 text 的警告记录到通过 WithWarningRecorder() 绑定在 context 上的 Recorder。
// 若给定 context 中不存在 Recorder，则不做任何事。
// agent 必须是合法 UTF-8，且不得包含空格、引号、反斜杠或控制字符。
// text 必须是合法 UTF-8，且不得包含控制字符。
//
// 无 Recorder 时静默忽略，使插件可以在测试或非 HTTP 场景下无条件调用。
func AddWarning(ctx context.Context, agent string, text string) {
	recorder, ok := warningRecorderFrom(ctx)
	if !ok {
		return
	}
	recorder.AddWarning(agent, text)
}
