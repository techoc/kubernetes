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

package filesystem

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestWatchUntilPanicsOnInvalidArgs 验证契约违规（pollInterval <= 0 或
// eventHandler 为 nil）会立即 panic，而不是在后台悄悄空转。
func TestWatchUntilPanicsOnInvalidArgs(t *testing.T) {
	t.Run("zero pollInterval panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for zero pollInterval")
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		WatchUntil(ctx, 0, "/tmp", func() {}, nil)
	})

	t.Run("negative pollInterval panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for negative pollInterval")
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		WatchUntil(ctx, -1*time.Second, "/tmp", func() {}, nil)
	})

	t.Run("nil eventHandler panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for nil eventHandler")
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		WatchUntil(ctx, time.Second, "/tmp", nil, nil)
	})
}

// TestWatchUntilContextCancellation 验证 ctx 取消后 WatchUntil 能及时返回，
// 不留下泄漏的 goroutine 或文件监听。
func TestWatchUntilContextCancellation(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	var callCount atomic.Int32

	done := make(chan struct{})
	go func() {
		defer close(done)
		WatchUntil(ctx, 50*time.Millisecond, dir, func() {
			callCount.Add(1)
		}, nil)
	}()

	// Wait for at least one handler invocation
	waitForCondition(t, 2*time.Second, func() bool {
		return callCount.Load() > 0
	})

	cancel()

	select {
	case <-done:
		// WatchUntil returned after context cancellation
	case <-time.After(5 * time.Second):
		t.Fatal("WatchUntil did not return after context cancellation")
	}
}

// TestWatchUntilCallsHandlerOnPoll 验证即使没有文件事件，轮询也会周期触发 handler。
func TestWatchUntilCallsHandlerOnPoll(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount atomic.Int32

	go func() {
		WatchUntil(ctx, 50*time.Millisecond, dir, func() {
			callCount.Add(1)
		}, nil)
	}()

	// Should get called multiple times via polling
	waitForCondition(t, 2*time.Second, func() bool {
		return callCount.Load() >= 3
	})
}

// TestWatchUntilCallsHandlerOnFileChange 验证文件内容变化会通过 fsnotify
// 立即触发 handler（这里故意设置很长的轮询间隔以排除轮询干扰）。
func TestWatchUntilCallsHandlerOnFileChange(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.yaml")
	if err := os.WriteFile(testFile, []byte("initial"), 0644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount atomic.Int32

	go func() {
		WatchUntil(ctx, 10*time.Minute, dir, func() {
			callCount.Add(1)
		}, nil)
	}()

	// Wait for initial handler call
	waitForCondition(t, 2*time.Second, func() bool {
		return callCount.Load() >= 1
	})

	initialCount := callCount.Load()

	// Modify the file to trigger a filesystem event
	if err := os.WriteFile(testFile, []byte("modified"), 0644); err != nil {
		t.Fatal(err)
	}

	// Should get called again due to file change
	waitForCondition(t, 5*time.Second, func() bool {
		return callCount.Load() > initialCount
	})
}

// TestWatchUntilNonExistentPathCallsErrorHandler 验证路径不存在时的降级：
// 错误上报给 errorHandler，同时 handler 仍通过轮询被调用。
func TestWatchUntilNonExistentPathCallsErrorHandler(t *testing.T) {
	nonExistent := filepath.Join(t.TempDir(), "does-not-exist")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var errorCount atomic.Int32
	var eventCount atomic.Int32

	go func() {
		WatchUntil(ctx, 50*time.Millisecond, nonExistent, func() {
			eventCount.Add(1)
		}, func(err error) {
			errorCount.Add(1)
		})
	}()

	// Error handler should be called for the non-existent path
	waitForCondition(t, 2*time.Second, func() bool {
		return errorCount.Load() > 0
	})

	// Event handler should still be called via polling
	waitForCondition(t, 2*time.Second, func() bool {
		return eventCount.Load() > 0
	})
}

// TestWatchUntilNilErrorHandler 验证 errorHandler 为 nil 时不会 panic，
// 错误被静默丢弃，轮询仍正常工作。
func TestWatchUntilNilErrorHandler(t *testing.T) {
	nonExistent := filepath.Join(t.TempDir(), "does-not-exist")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var eventCount atomic.Int32

	// nil errorHandler should not panic
	done := make(chan struct{})
	go func() {
		defer close(done)
		WatchUntil(ctx, 50*time.Millisecond, nonExistent, func() {
			eventCount.Add(1)
		}, nil)
	}()

	// Should still work with polling
	waitForCondition(t, 2*time.Second, func() bool {
		return eventCount.Load() > 0
	})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WatchUntil did not return")
	}
}

// TestWatchUntilDetectsAtomicRename 验证“写临时文件 + rename”这种原子替换
// 也能被检测到。这是 KEP 推荐的配置更新方式，若不重新 Add 监听会丢失后续事件。
func TestWatchUntilDetectsAtomicRename(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(testFile, []byte("version: 1"), 0644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount atomic.Int32

	go func() {
		// Use a long poll interval so we rely on fsnotify, not polling
		WatchUntil(ctx, 10*time.Minute, dir, func() {
			callCount.Add(1)
		}, nil)
	}()

	// Wait for initial handler call
	waitForCondition(t, 2*time.Second, func() bool {
		return callCount.Load() >= 1
	})

	countBefore := callCount.Load()

	// Atomic file replacement: write temp, then rename (recommended KEP pattern)
	tmpFile := filepath.Join(dir, ".config.yaml.tmp")
	if err := os.WriteFile(tmpFile, []byte("version: 2"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpFile, testFile); err != nil {
		t.Fatal(err)
	}

	// Should detect the rename event
	waitForCondition(t, 5*time.Second, func() bool {
		return callCount.Load() > countBefore
	})
}

// waitForCondition 轮询等待条件成立，超时则使测试失败；避免依赖固定 sleep。
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}
