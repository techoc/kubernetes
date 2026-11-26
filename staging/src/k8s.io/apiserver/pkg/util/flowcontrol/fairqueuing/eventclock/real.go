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

// Package eventclock 抽象“在某个时间点触发回调”的能力，使公平队列
// 的实现可以在测试中替换为假时钟，从而无需真实等待。
package eventclock

import (
	"time"

	"k8s.io/utils/clock"
)

// RealEventClock 按真实世界时间触发事件
// 嵌入 clock.RealClock 复用 Now/Since 等时间读取方法。
type Real struct {
	clock.RealClock
}

var _ Interface = Real{}

// EventAfterDuration 调度一个 EventFunc
// 每个事件一个 goroutine；调用方通过 ctx 取消队列时，未触发的回调
// 仍会执行一次（接口未提供取消），因此 EventFunc 必须能容忍过期触发。
func (Real) EventAfterDuration(f EventFunc, d time.Duration) {
	ch := time.After(d)
	go func() {
		t := <-ch
		f(t)
	}()
}

// EventAfterTime 调度一个 EventFunc
// 若 t 已过去，time.Until 返回负值，time.After 会立即触发。
func (r Real) EventAfterTime(f EventFunc, t time.Time) {
	r.EventAfterDuration(f, time.Until(t))
}
