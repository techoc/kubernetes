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

package wait

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// errWrapper 实现 Unwrap，errors.Is 可以穿透它识别内部错误。
type errWrapper struct {
	wrapped error
}

func (w errWrapper) Unwrap() error {
	return w.wrapped
}
func (w errWrapper) Error() string {
	return fmt.Sprintf("wrapped: %v", w.wrapped)
}

// errNotWrapper 不实现 Unwrap，errors.Is 无法穿透，用于验证 Interrupted
// 不会误判包装错误。
type errNotWrapper struct {
	wrapped error
}

func (w errNotWrapper) Error() string {
	return fmt.Sprintf("wrapped: %v", w.wrapped)
}

// TestInterrupted 验证 Interrupted 的判定：ErrWaitTimeout、context 取消/
// 超时以及它们的 Unwrap 链都算中断；普通错误和不可穿透的包装不算。
func TestInterrupted(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			err:  ErrWaitTimeout,
			want: true,
		},
		{
			err:  context.Canceled,
			want: true,
		}, {
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			err:  errWrapper{ErrWaitTimeout},
			want: true,
		},
		{
			err:  errWrapper{context.Canceled},
			want: true,
		},
		{
			err:  errWrapper{context.DeadlineExceeded},
			want: true,
		},
		{
			err:  ErrorInterrupted(nil),
			want: true,
		},
		{
			err:  ErrorInterrupted(errors.New("unknown")),
			want: true,
		},
		{
			err:  ErrorInterrupted(context.Canceled),
			want: true,
		},
		{
			err:  ErrorInterrupted(ErrWaitTimeout),
			want: true,
		},

		{
			err: nil,
		},
		{
			err: errors.New("not a cancellation"),
		},
		{
			err: errNotWrapper{ErrWaitTimeout},
		},
		{
			err: errNotWrapper{context.Canceled},
		},
		{
			err: errNotWrapper{context.DeadlineExceeded},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Interrupted(tt.err); got != tt.want {
				t.Errorf("Interrupted() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestErrorInterrupted 验证 ErrorInterrupted 的包装契约：幂等（已包装的
// 不再重复包装）、满足 errors.Is(err, errWaitTimeout)、保留原始错误文本。
func TestErrorInterrupted(t *testing.T) {
	internalErr := errInterrupted{}
	if ErrorInterrupted(internalErr) != internalErr {
		t.Fatalf("error should not be wrapped twice")
	}

	// 嵌套的 errInterrupted 也必须原样返回（幂等性不只看最外层）。
	internalErr = errInterrupted{errInterrupted{}}
	if ErrorInterrupted(internalErr) != internalErr {
		t.Fatalf("object should be identical")
	}

	in := errors.New("test")
	actual, expected := ErrorInterrupted(in), (errInterrupted{in})
	if actual != expected {
		t.Fatalf("did not wrap error")
	}
	if !errors.Is(actual, errWaitTimeout) {
		t.Fatalf("does not obey errors.Is contract")
	}
	// 包装后对外展示的错误文本必须与原始错误一致。
	if actual.Error() != in.Error() {
		t.Fatalf("unexpected error output")
	}
	if !Interrupted(actual) {
		t.Fatalf("is not Interrupted")
	}
	if Interrupted(in) {
		t.Fatalf("should not be Interrupted")
	}
}
