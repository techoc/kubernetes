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

package audit

import (
	"strconv"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
)

// fakeBackend 记录收到的事件，用于验证 Union 的分发行为。
type fakeBackend struct {
	events []*auditinternal.Event
}

// ProcessEvents 保存事件并始终报告成功。
func (f *fakeBackend) ProcessEvents(events ...*auditinternal.Event) bool {
	f.events = append(f.events, events...)
	return true
}

// Run 立即返回，fake 后端没有后台循环。
func (f *fakeBackend) Run(stopCh <-chan struct{}) error {
	return nil
}

// Shutdown 无资源需要释放。
func (f *fakeBackend) Shutdown() {
	// Nothing to do here.
}

// String 返回空字符串即可满足 Backend 接口。
func (f *fakeBackend) String() string {
	return ""
}

// TestUnion 验证 Union 把每个事件按原顺序广播给所有后端。
func TestUnion(t *testing.T) {
	backends := []Backend{
		new(fakeBackend),
		new(fakeBackend),
		new(fakeBackend),
	}

	b := Union(backends...)

	n := 5

	var events []*auditinternal.Event
	for i := 0; i < n; i++ {
		events = append(events, &auditinternal.Event{
			AuditID: types.UID(strconv.Itoa(i)),
		})
	}
	b.ProcessEvents(events...)

	for i, b := range backends {
		// so we can inspect the underlying events.
		backend := b.(*fakeBackend)

		if got := len(backend.events); got != n {
			t.Errorf("backend %d wanted %d events, got %d", i, n, got)
			continue
		}
		for j, event := range backend.events {
			wantID := types.UID(strconv.Itoa(j))
			if event.AuditID != wantID {
				t.Errorf("backend %d event %d wanted id %s, got %s", i, j, wantID, event.AuditID)
			}
		}
	}
}

// cannotMultipleRunBackend 用于验证 Union.Run 不会对每个后端启动多次。
// 如果 Run 被重复调用，close(started) 会 panic，测试因此失败。
type cannotMultipleRunBackend struct {
	started chan struct{}
}

// ProcessEvents 忽略事件。
func (b *cannotMultipleRunBackend) ProcessEvents(events ...*auditinternal.Event) bool {
	return true
}

// Run 关闭 started 作为"已启动一次"的标记。
func (b *cannotMultipleRunBackend) Run(stopCh <-chan struct{}) error {
	close(b.started)
	return nil
}

// Shutdown 无资源需要释放。
func (b *cannotMultipleRunBackend) Shutdown() {}

// String 用于错误信息中的后端标识。
func (b *cannotMultipleRunBackend) String() string {
	return "cannotMultipleRunBackend"
}

// TestUnionRun 验证 Union 只对每个后端调用一次 Run（重复 close 会 panic）。
func TestUnionRun(t *testing.T) {
	backends := []Backend{
		&cannotMultipleRunBackend{started: make(chan struct{})},
		&cannotMultipleRunBackend{started: make(chan struct{})},
		&cannotMultipleRunBackend{started: make(chan struct{})},
	}

	b := Union(backends...)

	if err := b.Run(make(chan struct{})); err != nil {
		t.Errorf("union backend run: %v", err)
	}
}
