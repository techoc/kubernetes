/*
Copyright 2014 The Kubernetes Authors.

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

package async

import (
	"sync"
)

// Runner 封装一组可重复启停的循环任务：每个任务都是一个函数，
// 通过监听传入的 stop 通道关闭来退出。
type Runner struct {
	lock      sync.Mutex
	loopFuncs []func(stop chan struct{})
	stop      *chan struct{}
}

// NewRunner 用给定的循环函数创建 Runner；
// 这些函数应在 stop 通道关闭后退出。
func NewRunner(f ...func(stop chan struct{})) *Runner {
	return &Runner{loopFuncs: f}
}

// Start 启动所有循环任务；重复调用无效果。
func (r *Runner) Start() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.stop == nil {
		c := make(chan struct{})
		r.stop = &c
		for i := range r.loopFuncs {
			go r.loopFuncs[i](*r.stop)
		}
	}
}

// Stop 关闭 stop 通道以停止所有任务；未运行时调用无效果。
func (r *Runner) Stop() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.stop != nil {
		close(*r.stop)
		r.stop = nil
	}
}
