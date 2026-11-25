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

package framework

import (
	"runtime"
	"testing"
	"time"

	"go.uber.org/goleak"

	"k8s.io/apiserver/pkg/server/healthz"
)

// IgnoreBackgroundGoroutines 返回 goleak.Find 的选项，
// 用于忽略由 "go test" 和 init 函数创建的 goroutine，
// 例如来自 go.opencensus.io/stats/view/worker.go 的那个。
//
// 通常在使用 apiserver 时较晚才会创建的 goroutine，
// 在调用此函数时就已经创建好了，因此它们也会被忽略。
func IgnoreBackgroundGoroutines() []goleak.Option {
	// 确保按需创建的 goroutine 正在运行。
	_ = healthz.LogHealthz.Check(nil)

	return []goleak.Option{goleak.IgnoreCurrent()}
}

// GoleakCheck 为测试或基准测试设置 goroutine 泄漏检查。
// 该检查作为清理操作运行，当发生 goroutine 泄漏时会记录错误。
func GoleakCheck(tb testing.TB, opts ...goleak.Option) {
	// 必须在创建新 goroutine *之前* 调用。
	opts = append(opts, IgnoreBackgroundGoroutines()...)

	tb.Cleanup(func() {
		if err := goleakFindRetry(opts...); err != nil {
			tb.Error(err.Error())
		}
	})
}

func goleakFindRetry(opts ...goleak.Option) error {
	// 有些测试不会等待 goroutine 停止。goleak.Find 内部会重试，
	// 但时间不够长。600 秒对大多数测试来说似乎足够，
	// 即使是在 CI 中运行测试时也是如此。
	timeout := 600 * time.Second
	start := time.Now()
	for {
		err := goleak.Find(opts...)
		if err == nil {
			return nil
		}
		runtime.GC() // 让清理操作得以运行，例如与 client-go 中 tlsTransportCache 关联的清理
		if time.Now().Sub(start) >= timeout {
			return err
		}
	}
}
