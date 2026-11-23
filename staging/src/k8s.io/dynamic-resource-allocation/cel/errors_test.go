/*
Copyright The Kubernetes Authors.

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

package cel

import (
	"errors"
	"testing"
)

// TestEnhanceRuntimeError 以表驱动方式覆盖 nil、缺键错误、普通错误、
// 带前缀的包装错误和已含提示文本的错误，逐一校验增强后的错误文本。
func TestEnhanceRuntimeError(t *testing.T) {
	// 表结构字段：name 为用例名，err 为输入错误，want 为期望文本（空表示期望 nil）。
	tests := []struct {
		name string
		err  error
		want string
	}{
		// 输入 nil，期望返回 nil。
		{
			name: "nil error returns nil",
			err:  nil,
			want: "",
		},
		// 输入缺键错误，期望追加可选字段访问提示。
		{
			name: "no such key error gets enhanced with usage hint",
			err:  errors.New("no such key: sharingStrategy"),
			want: "no such key: sharingStrategy. consider using CEL optional chaining (.? followed by orValue()) or guarding the check with has() for optional fields",
		},
		// 输入普通错误，期望原样返回。
		{
			name: "other error is returned unchanged",
			err:  errors.New("some other error"),
			want: "some other error",
		},
		// 输入带上下文前缀的缺键错误，期望同样被增强并保留前缀。
		{
			name: "wrapped no such key error gets enhanced",
			err:  errors.New("context: no such key: fieldY"),
			want: "context: no such key: fieldY. consider using CEL optional chaining (.? followed by orValue()) or guarding the check with has() for optional fields",
		},
		// 输入已含提示文本但类型不是 enhancedError 的错误，期望再追加一次提示。
		{
			name: "error with hint text but not enhancedError type still gets enhanced",
			err:  errors.New("no such key: fieldZ. consider using CEL optional chaining"),
			want: "no such key: fieldZ. consider using CEL optional chaining. consider using CEL optional chaining (.? followed by orValue()) or guarding the check with has() for optional fields",
		},
	}
	for _, tt := range tests {
		// 每个用例作为子测试独立执行，便于定位失败场景。
		t.Run(tt.name, func(t *testing.T) {
			// 调用被测函数，得到增强后的错误。
			got := EnhanceRuntimeError(tt.err)
			// 期望文本为空时，只断言返回值为 nil。
			if tt.want == "" {
				if got != nil {
					t.Errorf("EnhanceRuntimeError() = %v, want nil", got)
				}
				return
			}
			// 期望非空却得到 nil，直接报告失败。
			if got == nil {
				t.Errorf("EnhanceRuntimeError() = nil, want %q", tt.want)
				return
			}
			// 对比实际错误文本与期望文本。
			if got.Error() != tt.want {
				t.Errorf("EnhanceRuntimeError() = %q, want %q", got.Error(), tt.want)
			}
		})
	}
}

// TestEnhanceRuntimeErrorIdempotent 验证对同一错误连续两次调用
// EnhanceRuntimeError 得到的文本完全一致，即增强操作是幂等的。
func TestEnhanceRuntimeErrorIdempotent(t *testing.T) {
	// 构造典型的缺键错误作为输入。
	originalErr := errors.New("no such key: fieldA")
	// 第一次增强应追加提示。
	enhancedOnce := EnhanceRuntimeError(originalErr)
	// 第二次增强应识别已包装的错误并原样返回。
	enhancedTwice := EnhanceRuntimeError(enhancedOnce)

	// 断言两次结果文本相同。
	if enhancedOnce.Error() != enhancedTwice.Error() {
		t.Errorf("EnhanceRuntimeError is not idempotent: first=%q, second=%q", enhancedOnce.Error(), enhancedTwice.Error())
	}
}
