//go:build example

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

package withktesting

// 下面的测试会失败，因此通过 "example" 构建标签将其排除在
// 常规的 "make test" 之外。要运行这些测试并查看输出，请使用
// "go test -tags example ."

import (
	"context"
	"testing"
	"time"

	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestTimeout(t *testing.T) {
	tCtx := ktesting.Init(t)
	tmp := t.TempDir()
	tCtx.Logf("Using %q as temporary directory.", tmp)
	tCtx.Cleanup(func() {
		t.Log("Cleaning up...")
	})
	if deadline, ok := t.Deadline(); ok {
		t.Logf("Will fail shortly before the test suite deadline at %s.", deadline)
	}

	// Ginkgo 和 ktesting 通过这种方式告知 Gomega，在卡住时（例如
	// gomega.Eventually 中）如何提供进度报告。
	// 这里用它在该示例收到 SIGUSR1 时提供一些额外的输出。
	remove := tCtx.Value("GINKGO_SPEC_CONTEXT").(interface {
		AttachProgressReporter(func() string) func()
	}).AttachProgressReporter(func() string { return "waiting for timeout or interrupt" })
	defer remove()

	select {
	case <-time.After(1000 * time.Hour):
		// 不应执行到这里。
		tCtx.Log("Huh?! I shouldn't be that old.")
	case <-tCtx.Done():
		// 但这一分支会在测试套件超时之前触发。
		tCtx.Errorf("need to stop: %v", context.Cause(tCtx))
	}
}
