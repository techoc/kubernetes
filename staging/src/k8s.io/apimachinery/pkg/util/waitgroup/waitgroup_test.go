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

// Package waitgroup test cases reference golang sync.WaitGroup https://golang.org/src/sync/waitgroup_test.go.
package waitgroup

import (
	"testing"
)

// TestWaitGroup 复刻标准库 WaitGroup 的屏障语义，并验证 SafeWaitGroup
// 的 Wait 会阻塞直到所有 Done 被调用。先让 wg1 归零、再逐个释放 wg2，
// 每一步都检查没有任何 goroutine 提前越过屏障。
func TestWaitGroup(t *testing.T) {
	wg1 := &SafeWaitGroup{}
	wg2 := &SafeWaitGroup{}
	n := 16
	wg1.Add(n)
	wg2.Add(n)
	exited := make(chan bool, n)
	for i := 0; i != n; i++ {
		go func(i int) {
			wg1.Done()
			wg2.Wait()
			exited <- true
		}(i)
	}
	wg1.Wait()
	for i := 0; i != n; i++ {
		select {
		case <-exited:
			t.Fatal("SafeWaitGroup released group too soon")
		default:
		}
		wg2.Done()
	}
	for i := 0; i != n; i++ {
		<-exited // Will block if barrier fails to unlock someone.
	}
}

// TestWaitGroupAddFail 固定 SafeWaitGroup 的关键差异：Wait 返回后再 Add
// 必须报错，防止复用一个已关闭的组造成计数错乱。
func TestWaitGroupAddFail(t *testing.T) {
	wg := &SafeWaitGroup{}
	wg.Add(1)
	wg.Done()
	wg.Wait()
	if err := wg.Add(1); err == nil {
		t.Errorf("Should return error when add positive after Wait")
	}
}
