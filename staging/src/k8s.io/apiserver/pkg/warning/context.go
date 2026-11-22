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
//
// 中文说明：插件通过 context 拿到 Recorder，从而与具体的响应写入方式解耦；
// 警告是 RFC 7234 定义的带外信息，不应影响响应状态码或正文。
package warning

import (
	"context"
)

// The key type is unexported to prevent collisions
// 中文说明：与 request 包同样的未导出 key 约定，避免跨包覆盖 context 值。
type key int

const (
	// warningRecorderKey is the context key for the warning recorder.
	warningRecorderKey key = iota
)

// Recorder provides a method for recording warnings
//
// 中文说明：实现方负责把警告写入 HTTP Warning 头；输入校验（UTF-8、无控制字符）
// 是契约的一部分，调用方不能假设 Recorder 会宽容处理非法内容。
type Recorder interface {
	// AddWarning adds the specified warning to the response.
	// agent must be valid UTF-8, and must not contain spaces, quotes, backslashes, or control characters.
	// text must be valid UTF-8, and must not contain control characters.
	AddWarning(agent, text string)
}

// WithWarningRecorder returns a new context that wraps the provided context and contains the provided Recorder implementation.
// The returned context can be passed to AddWarning().
func WithWarningRecorder(ctx context.Context, recorder Recorder) context.Context {
	return context.WithValue(ctx, warningRecorderKey, recorder)
}

func warningRecorderFrom(ctx context.Context) (Recorder, bool) {
	recorder, ok := ctx.Value(warningRecorderKey).(Recorder)
	return recorder, ok
}

// AddWarning records a warning for the specified agent and text to the Recorder added to the provided context using WithWarningRecorder().
// If no Recorder exists in the provided context, this is a no-op.
// agent must be valid UTF-8, and must not contain spaces, quotes, backslashes, or control characters.
// text must be valid UTF-8, and must not contain control characters.
//
// 中文说明：无 Recorder 时静默忽略，使插件可以在测试或非 HTTP 场景下无条件调用。
func AddWarning(ctx context.Context, agent string, text string) {
	recorder, ok := warningRecorderFrom(ctx)
	if !ok {
		return
	}
	recorder.AddWarning(agent, text)
}
