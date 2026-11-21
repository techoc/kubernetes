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

package ipam

import (
	"errors"
	"testing"
	"time"
)

// TestTimeout 验证 Timeout 在成功与失败交替场景下的退避行为。
// 关键断言语义：成功后立即回归固定 Resync 周期，失败则按 1s→2s→4s 指数退避，
// 并在 MaxBackoff 处封顶，避免故障期间对 IPAM 依赖组件造成请求风暴。
func TestTimeout(t *testing.T) {
	time10s := 10 * time.Second
	time5s := 5 * time.Second
	timeout := &Timeout{
		Resync:       time10s,
		MaxBackoff:   time5s,
		InitialRetry: time.Second,
	}

	// 每个步骤用 err 是否为 nil 表示本轮同步成功与否，want 是下一轮应等待的时长。
	// 序列特意包含连续的失败、成功重置、再次失败，以覆盖退避的封顶与重置两条路径。
	for _, testStep := range []struct {
		err  error
		want time.Duration
	}{
		{nil, time10s},
		{nil, time10s},
		{errors.New("x"), time.Second},
		{errors.New("x"), 2 * time.Second},
		{errors.New("x"), 4 * time.Second},
		{errors.New("x"), 5 * time.Second},
		{errors.New("x"), 5 * time.Second},
		{nil, time10s},
		{nil, time10s},
		{errors.New("x"), time.Second},
		{errors.New("x"), 2 * time.Second},
		{nil, time10s},
	} {
		timeout.Update(testStep.err == nil)
		next := timeout.Next()
		if next != testStep.want {
			t.Errorf("timeout.next(%v) = %v, want %v", testStep.err, next, testStep.want)
		}
	}
}
