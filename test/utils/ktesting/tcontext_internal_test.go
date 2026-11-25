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

// 本文件属于 ktesting 包（而非 ktesting_test），以便访问
// TContext 的未导出字段。
package ktesting

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/onsi/gomega"

	"k8s.io/kubernetes/test/utils/ktesting/initoption"
)

// deadlineT2 对应于 ktesting_test 中的 deadlineT 辅助类型。
type deadlineT2 struct {
	TB
	deadline *time.Time
}

func (t *deadlineT2) Deadline() (time.Time, bool) {
	if t.deadline == nil {
		return time.Time{}, false
	}
	return *t.deadline, true
}

// TestDefaultCleanupGracePeriod 验证在未指定覆盖值时，
// cleanupGracePeriod 会被设为 DefaultCleanupGracePeriod。
func TestDefaultCleanupGracePeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mockDeadline := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		mockT := &deadlineT2{TB: t, deadline: &mockDeadline}
		tCtx := Init(mockT)
		if tCtx.cleanupGracePeriod != DefaultCleanupGracePeriod {
			t.Errorf("expected cleanupGracePeriod %v, got %v", DefaultCleanupGracePeriod, tCtx.cleanupGracePeriod)
		}
	})
}

// TestCustomCleanupGracePeriod 验证 WithCleanupGracePeriod 会将
// 覆盖值存入 cleanupGracePeriod，并相应调整有效的截止时间。
func TestCustomCleanupGracePeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mockDeadline := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		mockT := &deadlineT2{TB: t, deadline: &mockDeadline}
		custom := 30 * time.Second
		tCtx := Init(mockT, initoption.WithCleanupGracePeriod(custom))

		if tCtx.cleanupGracePeriod != custom {
			t.Errorf("expected cleanupGracePeriod %v, got %v", custom, tCtx.cleanupGracePeriod)
		}

		actualDeadline, ok := tCtx.Deadline()
		if !ok {
			t.Fatal("expected a deadline but got none")
		}
		expect := mockDeadline.Add(-custom)
		tCtx.Expect(actualDeadline).To(
			gomega.BeTemporally("==", expect),
			"context deadline should be shifted by the custom grace period")
	})
}

// TestInitCtxCleanupGracePeriod 验证 InitCtx 会通过此前被忽略的
// opts 参数应用 WithCleanupGracePeriod。
func TestInitCtxCleanupGracePeriod(t *testing.T) {
	custom := 42 * time.Second
	tCtx := InitCtx(context.Background(), t, initoption.WithCleanupGracePeriod(custom))
	if tCtx.cleanupGracePeriod != custom {
		t.Errorf("expected cleanupGracePeriod %v, got %v", custom, tCtx.cleanupGracePeriod)
	}
}

// TestInitCtxDefaultCleanupGracePeriod 验证在未给定选项时，
// InitCtx 会回退到 DefaultCleanupGracePeriod。
func TestInitCtxDefaultCleanupGracePeriod(t *testing.T) {
	tCtx := InitCtx(context.Background(), t)
	if tCtx.cleanupGracePeriod != DefaultCleanupGracePeriod {
		t.Errorf("expected cleanupGracePeriod %v, got %v", DefaultCleanupGracePeriod, tCtx.cleanupGracePeriod)
	}
}
