/*
Copyright 2025 The Kubernetes Authors.

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

package app

import (
	"context"
	"sync"
)

// 本文件提供将控制器运行逻辑包装为 Controller 的工具函数。

type runFunc func(ctx context.Context)

type runFuncSlice []runFunc

func (rx runFuncSlice) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(len(rx))
	for _, fnc := range rx {
		go func() {
			defer wg.Done()
			fnc(ctx)
		}()
	}
	wg.Wait()
}

// concurrentRun 将给定函数包装为并发执行且等待全部完成的 runFunc。
func concurrentRun(rx ...runFunc) runFunc {
	return runFuncSlice(rx).Run
}

// controllerLoop 实现 Controller 接口，将单个运行函数适配为控制器。
type controllerLoop struct {
	name string
	run  runFunc
}

func newControllerLoop(run runFunc, controllerName string) *controllerLoop {
	return &controllerLoop{
		name: controllerName,
		run:  run,
	}
}

// Name 返回包装器关联的规范控制器名称。
func (loop *controllerLoop) Name() string {
	return loop.name
}

// Run 委托给底层运行函数，由调用方保证其阻塞至工作完成或上下文取消。
func (loop *controllerLoop) Run(ctx context.Context) {
	loop.run(ctx)
}
