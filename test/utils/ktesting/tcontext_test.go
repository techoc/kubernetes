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

package ktesting_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/onsi/gomega"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestCancelManual(t *testing.T) {
	tCtx := ktesting.Init(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 阻塞直到下面调用 tCtx.Cancel。
		<-tCtx.Done()
	}()
	tCtx.Cancel("manually canceled")
	wg.Wait()
}

func TestCancelAutomatic(t *testing.T) {
	var wg sync.WaitGroup
	// 这个回调最先注册，因此最后被调用。
	t.Cleanup(wg.Wait)
	tCtx := ktesting.Init(t)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 阻塞直到 context 被自动取消。
		<-tCtx.Done()
	}()
}

func TestSyncTestInit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 这必须能在 synctest bubble 中工作，尽管 Deadline 在那里会 panic。
		// 这样的话我们就没有 deadline。
		tCtx := ktesting.Init(t)
		deadline, ok := tCtx.Deadline()
		if ok {
			tCtx.Errorf("Expected no deadline, got %s", deadline)
		}
		if !tCtx.IsSyncTest() {
			tCtx.Errorf("Expected to run as synctest")
		}
	})
}

func TestNormalInit(t *testing.T) {
	// 结果取决于单元测试是如何启动的。
	// 关于确定性的有/无 deadline 测试，见下文。
	expectDeadline, expectOK := t.Deadline()
	expectDeadline = expectDeadline.Add(-ktesting.DefaultCleanupGracePeriod)
	tCtx := ktesting.Init(t)
	actualDeadline, actualOK := tCtx.Deadline()
	tCtx.Expect(actualOK).To(gomega.Equal(expectOK), "have deadline")
	if expectOK {
		tCtx.Expect(actualDeadline).To(gomega.BeTemporally("~", expectDeadline, 2*time.Second), "deadline")
	}
	if tCtx.IsSyncTest() {
		tCtx.Errorf("Expected to not run as synctest")
	}
}

func TestNoDeadline(t *testing.T) {
	mockT := &deadlineT{T: t, deadline: nil}
	tCtx := ktesting.Init(mockT)
	deadline, ok := tCtx.Deadline()
	if ok {
		tCtx.Errorf("Expected no deadline, got %s", deadline)
	}
}

func TestDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 在 synctest bubble 中，这个时间总是在未来。
		mockDeadline := time.Date(2000, 01, 01, 0, 0, 0, 0, time.UTC)
		mockT := &deadlineT{T: t, deadline: &mockDeadline}
		tCtx := ktesting.Init(mockT)
		actualDeadline, ok := tCtx.Deadline()
		if ok {
			expectDeadline := mockDeadline.Add(-ktesting.DefaultCleanupGracePeriod)
			tCtx.Expect(actualDeadline).To(gomega.BeTemporally("==", expectDeadline), "deadline")
		} else {
			tCtx.Error("Expected a deadline, got none")
		}
	})
}

// deadlineT 覆盖 Deadline：如果没有配置 deadline 则返回 false，
// 否则返回配置好的 deadline。
type deadlineT struct {
	*testing.T
	deadline *time.Time
}

func (t *deadlineT) Deadline() (time.Time, bool) {
	if t.deadline == nil {
		return time.Time{}, false
	}
	return *t.deadline, true
}

func TestCancelCtx(t *testing.T) {
	tCtx := ktesting.Init(t)
	var discardLogger klog.Logger
	tCtx = tCtx.WithLogger(discardLogger)
	baseCtx := tCtx

	tCtx.Cleanup(func() {
		if tCtx.Err() == nil {
			t.Error("context should be canceled but isn't")
		}
	})
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		tCtx.Assert(tCtx.Err()).To(gomega.Succeed(), "context should not be canceled but is")
		tCtx.Assert(tCtx.Logger()).To(gomega.Equal(baseCtx.Logger()), "Logger()")
	})

	// 先取消，然后让 testing.T 执行测试清理。
	tCtx.Cancel("test is complete")
}

func TestParallel(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(3)

	tCtx := ktesting.Init(t)

	// 每个子测试都与其他子测试并行运行，并等待另外两个子测试。
	test := func(tCtx ktesting.TContext) {
		tCtx.Parallel()
		wg.Done()
		wg.Wait()
	}
	tCtx.Run("one", test)
	tCtx.Run("two", test)
	tCtx.Run("three", test)
}

func TestRun(t *testing.T) {
	tCtx := ktesting.Init(t)

	key := 42
	value := "fish"
	tCtx = tCtx.WithValue(key, value)

	tCtx.Run("sub", func(tCtx ktesting.TContext) {
		tCtx.Assert(tCtx.Value(key)).To(gomega.Equal(value))

		tCtx.Cancel("test is complete")
		<-tCtx.Done()
	})

	if err := tCtx.Err(); err != nil {
		t.Errorf("parent TContext should not have been cancelled: %v", err)
	}
}

func TestWithNamespace(t *testing.T) {
	tCtx := ktesting.Init(t)
	namespace := "foo"
	tCtxWithNamespace := tCtx.WithNamespace(namespace)
	tCtx.Expect(tCtxWithNamespace.Namespace()).To(gomega.Equal(namespace))
}

func TestWithContext(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.Cancel("done")
	tCtx = tCtx.WithValue("foo", "bar")
	deadline := time.Now().Add(-time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	newCtx := tCtx.WithContext(ctx)
	tCtx.Expect(context.Cause(tCtx)).To(gomega.MatchError(gomega.ContainSubstring("done")))
	tCtx.Expect(newCtx.Err()).To(gomega.MatchError(context.DeadlineExceeded))
	tCtx.Expect(newCtx.Value("foo")).To(gomega.Equal("bar"))
}
