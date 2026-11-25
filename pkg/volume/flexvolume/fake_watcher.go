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

package flexvolume

import (
	"context"

	"github.com/fsnotify/fsnotify"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
)

// fakeWatcher 是测试用的模拟文件系统监视器。
type fakeWatcher struct {
	watches      []string // 探测器添加的监视路径，按加入顺序由旧到新排列。
	eventHandler utilfs.FSEventHandler
}

var _ utilfs.FSWatcher = &fakeWatcher{}

// newFakeWatcher 创建一个空的模拟监视器。
func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{
		watches: nil,
	}
}

// Init 记录事件回调；测试无需错误回调。
func (w *fakeWatcher) Init(eventHandler utilfs.FSEventHandler, _ utilfs.FSErrorHandler) error {
	w.eventHandler = eventHandler
	return nil
}

// Run 空实现：测试直接调用 TriggerEvent，无需后台循环。
func (w *fakeWatcher) Run(_ context.Context) { /* 空操作 */ }

// AddWatch 把路径记入监视列表，不实际监听文件系统。
func (w *fakeWatcher) AddWatch(path string) error {
	w.watches = append(w.watches, path)
	return nil
}

// TriggerEvent 触发一次模拟文件系统事件。
func (w *fakeWatcher) TriggerEvent(op fsnotify.Op, filename string) {
	w.eventHandler(fsnotify.Event{Op: op, Name: filename})
}
