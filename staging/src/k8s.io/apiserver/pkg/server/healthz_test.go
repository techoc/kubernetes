/*
Copyright 2019 The Kubernetes Authors.

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

package server

import (
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// TestDelayedHealthCheck 验证 delayedHealthCheck 的启动宽限语义：
// 延迟期内始终健康，延迟后才反映 post-start hook 是否完成。
func TestDelayedHealthCheck(t *testing.T) {
	t.Run("test that liveness check returns true until the delay has elapsed", func(t *testing.T) {
		t0 := time.Unix(0, 0)
		c := testingclock.NewFakeClock(t0)
		doneCh := make(chan struct{})

		healthCheck := delayedHealthCheck(postStartHookHealthz{"test", doneCh}, c, time.Duration(10)*time.Second)
		// 刚启动，hook 未完成也应返回健康。
		err := healthCheck.Check(nil)
		if err != nil {
			t.Errorf("Got %v, expected no error", err)
		}
		// 恰好到达延迟边界时仍视为宽限期内。
		c.Step(10 * time.Second)
		err = healthCheck.Check(nil)
		if err != nil {
			t.Errorf("Got %v, expected no error", err)
		}
		// 越过边界后开始如实报告 hook 未完成。
		c.Step(1 * time.Millisecond)
		err = healthCheck.Check(nil)
		if err == nil || err.Error() != "not finished" {
			t.Errorf("Got '%v', but expected error to be 'not finished'", err)
		}
		// hook 完成后恢复健康。
		close(doneCh)
		err = healthCheck.Check(nil)
		if err != nil {
			t.Errorf("Got %v, expected no error", err)
		}
	})
	t.Run("test that liveness check does not toggle false even if done channel is closed early", func(t *testing.T) {
		t0 := time.Unix(0, 0)
		c := testingclock.NewFakeClock(t0)

		doneCh := make(chan struct{})

		healthCheck := delayedHealthCheck(postStartHookHealthz{"test", doneCh}, c, time.Duration(10)*time.Second)
		err := healthCheck.Check(nil)
		if err != nil {
			t.Errorf("Got %v, expected no error", err)
		}
		// hook 在延迟期内提前完成，之后越过延迟边界也应持续健康。
		close(doneCh)
		c.Step(10 * time.Second)
		err = healthCheck.Check(nil)
		if err != nil {
			t.Errorf("Got %v, expected no error", err)
		}
		c.Step(1 * time.Millisecond)
		err = healthCheck.Check(nil)
		if err != nil {
			t.Errorf("Got %v, expected no error", err)
		}
	})

}
