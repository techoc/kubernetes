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

package filesystem

import (
	"context"
	"fmt"

	"github.com/fsnotify/fsnotify"
)

// FSWatcher 是基于回调的文件系统监听抽象，底层为 fsnotify。
type FSWatcher interface {
	// Init 用给定的事件和错误处理器初始化监听器，须在所有其他方法之前调用。
	Init(FSEventHandler, FSErrorHandler) error

	// Run 开始监听事件和错误，发生事件或错误时调用相应的处理器。
	// ctx 被取消时监听器停止并释放资源。
	Run(ctx context.Context)

	// AddWatch 添加一个待监听的文件系统路径。
	AddWatch(path string) error
}

// FSEventHandler 在 fsnotify 事件发生时被调用。
type FSEventHandler func(event fsnotify.Event)

// FSErrorHandler 在 fsnotify 错误发生时被调用。
type FSErrorHandler func(err error)

type fsnotifyWatcher struct {
	watcher      *fsnotify.Watcher
	eventHandler FSEventHandler
	errorHandler FSErrorHandler
}

var _ FSWatcher = &fsnotifyWatcher{}

// NewFsnotifyWatcher 返回一个 FSWatcher 实现：持续监听 fsnotify 事件，收到事件后立即调用事件处理器。
func NewFsnotifyWatcher() FSWatcher {
	return &fsnotifyWatcher{}
}

// AddWatch 实现 FSWatcher 的 AddWatch 方法。
func (w *fsnotifyWatcher) AddWatch(path string) error {
	return w.watcher.Add(path)
}

// Init 实现 FSWatcher 的 Init 方法。
func (w *fsnotifyWatcher) Init(eventHandler FSEventHandler, errorHandler FSErrorHandler) error {
	var err error
	w.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("fsnotify watcher init: %w", err)
	}

	w.eventHandler = eventHandler
	w.errorHandler = errorHandler
	return nil
}

// Run 实现 FSWatcher 的 Run 方法。
func (w *fsnotifyWatcher) Run(ctx context.Context) {
	go func() {
		// ctx 取消时退出循环，同时关闭 watcher 释放资源。
		defer w.watcher.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-w.watcher.Events:
				if w.eventHandler != nil {
					w.eventHandler(event)
				}
			case err := <-w.watcher.Errors:
				if w.errorHandler != nil {
					w.errorHandler(err)
				}
			}
		}
	}()
}
