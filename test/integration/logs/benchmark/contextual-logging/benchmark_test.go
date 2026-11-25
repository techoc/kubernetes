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

package contextuallogging

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"k8s.io/klog/v2"
)

func init() {
	klog.InitFlags(nil)
}

// BenchmarkRecursion 测量仅使用 depth 参数递归调用函数
// 所带来的开销。
func BenchmarkRecursion(b *testing.B) {
	for depth := 10; depth <= 100000; depth *= 10 {
		b.Run(fmt.Sprintf("%d", depth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				recurse(depth)
			}
		})
	}
}

//go:noinline
func recurse(depth int) {
	if depth == 0 {
		logr.Discard().Info("hello world")
		return
	}
	recurse(depth - 1)
}

// BenchmarkRecursionWithLogger 测量增加 logr.Logger 参数
// 所带来的开销。
func BenchmarkRecursionWithLogger(b *testing.B) {
	logger := logr.Discard()

	for depth := 10; depth <= 100000; depth *= 10 {
		b.Run(fmt.Sprintf("%d", depth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				recurseWithLogger(logger, depth)
			}
		})
	}
}

//go:noinline
func recurseWithLogger(logger logr.Logger, depth int) {
	if depth == 0 {
		return
	}
	recurseWithLogger(logger, depth-1)
}

// BenchmarkRecursionWithContext 测量增加 context 参数
// 所带来的开销。
func BenchmarkRecursionWithContext(b *testing.B) {
	logger := logr.Discard()
	// nolint:logcheck // Intentionally using NewContext unconditionally here.
	ctx := logr.NewContext(context.Background(), logger)

	for depth := 10; depth <= 100000; depth *= 10 {
		b.Run(fmt.Sprintf("%d", depth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				recurseWithContext(ctx, depth)
			}
		})
	}
}

//go:noinline
func recurseWithContext(ctx context.Context, depth int) {
	if depth == 0 {
		return
	}
	recurseWithContext(ctx, depth-1)
}

// BenchmarkRecursionWithLoggerAndLog 测量增加 logr.Logger 参数
// 并使用它一次所带来的开销。
func BenchmarkRecursionWithLoggerAndLog(b *testing.B) {
	logger := logr.Discard()

	for depth := 10; depth <= 100000; depth *= 10 {
		b.Run(fmt.Sprintf("%d", depth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				recurseWithLoggerAndLog(logger, depth)
			}
		})
	}
}

//go:noinline
func recurseWithLoggerAndLog(logger logr.Logger, depth int) {
	if depth == 0 {
		logger.Info("hello world")
		return
	}
	recurseWithLoggerAndLog(logger, depth-1)
}

// BenchmarkRecursionWithContextAndLog 测量增加 context 参数
// 并用它获取及调用一次 logger 所带来的开销。
func BenchmarkRecursionWithContextAndLog(b *testing.B) {
	logger := logr.Discard()
	// nolint:logcheck // Intentionally using NewContext unconditionally here.
	ctx := logr.NewContext(context.Background(), logger)

	for depth := 10; depth <= 100000; depth *= 10 {
		b.Run(fmt.Sprintf("%d", depth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				recurseWithContextAndLog(ctx, depth)
			}
		})
	}
}

//go:noinline
func recurseWithContextAndLog(ctx context.Context, depth int) {
	if depth == 0 {
		logger := logr.FromContextOrDiscard(ctx)
		logger.Info("hello world")
		return
	}
	recurseWithContextAndLog(ctx, depth-1)
}

// BenchmarkNestedContextWithTimeouts 测量调用一个在每次调用时
// 都用 context.WithTimeout 创建新 context 的函数的速度。
func BenchmarkNestedContextWithTimeouts(b *testing.B) {
	ctx := context.Background()

	for depth := 1; depth <= 10000; depth *= 10 {
		b.Run(fmt.Sprintf("%d", depth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				nestedContextWithTimeout(ctx, depth)
			}
		})
	}
}

//go:noinline
func nestedContextWithTimeout(ctx context.Context, depth int) {
	if depth == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	nestedContextWithTimeout(ctx, depth-1)
}

// BenchmarkNestedContextWithTimeoutsAndLookup 测量调用一个在每次调用时
// 都用 context.WithTimeout 创建新 context、随后再查找 logger 的
// 函数的速度。
func BenchmarkNestedContextWithTimeoutsAndLookup(b *testing.B) {
	// nolint:logcheck // Intentionally using NewContext unconditionally here.
	ctx := logr.NewContext(context.Background(), logr.Discard())

	for depth := 1; depth <= 10000; depth *= 10 {
		b.Run(fmt.Sprintf("%d", depth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				nestedContextWithTimeoutAndLookup(ctx, depth)
			}
		})
	}
}

//go:noinline
func nestedContextWithTimeoutAndLookup(ctx context.Context, depth int) {
	if depth == 0 {
		logr.FromContextOrDiscard(ctx)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	nestedContextWithTimeoutAndLookup(ctx, depth-1)
}

var logger logr.Logger

// BenchmarkLookupWithTimeouts 测量 FromContextOrDiscard 在使用
// WithTimeout 创建的嵌套 context 中查找 logger 的速度。
func BenchmarkLookupWithTimeouts(b *testing.B) {
	for depth := 1; depth <= 10000; depth *= 10 {
		b.Run(fmt.Sprintf("%d", depth), func(b *testing.B) {
			// nolint:logcheck // Intentionally using NewContext unconditionally here.
			ctx := logr.NewContext(context.Background(), logr.Discard())
			for i := 0; i < depth; i++ {
				ctx2, cancel := context.WithTimeout(ctx, time.Hour)
				defer cancel()
				ctx = ctx2
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				logger = logr.FromContextOrDiscard(ctx)
			}
		})
	}
}

type keyT struct{}

var key keyT

// BenchmarkLookupWithValues 测量 FromContextOrDiscard 在使用
// WithValue 创建的嵌套 context 中查找 logger 的速度。
func BenchmarkLookupWithValues(b *testing.B) {
	for depth := 1; depth <= 10000; depth *= 10 {
		b.Run(fmt.Sprintf("%d", depth), func(b *testing.B) {
			// nolint:logcheck // Intentionally using NewContext unconditionally here.
			ctx := logr.NewContext(context.Background(), logr.Discard())
			for i := 0; i < depth; i++ {
				ctx = context.WithValue(ctx, key, depth)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				logger = logr.FromContextOrDiscard(ctx)
			}
		})
	}
}
