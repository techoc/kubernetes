/*
Copyright 2018 The Kubernetes Authors.

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

package pluginwatcher

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/fsnotify/fsnotify"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/util"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
)

// Watcher 是插件监听器，监听插件 socket 的创建与删除。
type Watcher struct {
	path                string
	fs                  utilfs.Filesystem
	fsWatcher           *fsnotify.Watcher
	desiredStateOfWorld cache.DesiredStateOfWorld
}

// NewWatcher 返回用于 socket 注册的新监听器。
func NewWatcher(sockDir string, desiredStateOfWorld cache.DesiredStateOfWorld) *Watcher {
	return &Watcher{
		path:                sockDir,
		fs:                  &utilfs.DefaultFs{},
		desiredStateOfWorld: desiredStateOfWorld,
	}
}

// Start 监听指定路径下插件 socket 的创建与删除。
func (w *Watcher) Start(ctx context.Context, stopCh <-chan struct{}) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Plugin Watcher Start", "path", w.path)

	// 先确保被监听的目录存在，再遍历目录以发现已存在的插件。
	if err := w.init(ctx); err != nil {
		return err
	}

	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to start plugin fsWatcher, err: %v", err)
	}
	w.fsWatcher = fsWatcher

	// 在启动插件处理 goroutine 之前，先遍历插件目录并添加文件系统监听。
	if err := w.traversePluginDir(ctx, w.path); err != nil {
		logger.Error(err, "Failed to traverse plugin socket path", "path", w.path)
	}

	// 在后台消费 fsnotify 事件，处理插件 socket 的创建与删除。
	go func(fsWatcher *fsnotify.Watcher) {
		for {
			select {
			case event := <-fsWatcher.Events:
				// TODO: 处理错误并采取纠正措施。
				if event.Has(fsnotify.Create) {
					err := w.handleCreateEvent(ctx, event)
					if err != nil {
						logger.Error(err, "Error when handling create event", "event", event)
					}
				} else if event.Has(fsnotify.Remove) {
					w.handleDeleteEvent(ctx, event)
				}
				continue
			case err := <-fsWatcher.Errors:
				if err != nil {
					logger.Error(err, "FsWatcher received error")
				}
				continue
			// stopCh 关闭时关闭 fsWatcher 并退出循环。
			case <-stopCh:
				w.fsWatcher.Close()
				return
			}
		}
	}(fsWatcher)

	return nil
}

// init 确保插件 socket 目录存在。
func (w *Watcher) init(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Ensuring Plugin directory", "path", w.path)

	if err := w.fs.MkdirAll(w.path, 0755); err != nil {
		return fmt.Errorf("error (re-)creating root %s: %v", w.path, err)
	}

	return nil
}

// 遍历插件目录，发现已存在的插件 socket。除根目录无法遍历外，
// 忽略其他错误。
func (w *Watcher) traversePluginDir(ctx context.Context, dir string) error {
	logger := klog.FromContext(ctx)

	// 监听目标目录。
	err := w.fsWatcher.Add(dir)
	if err != nil {
		return fmt.Errorf("failed to watch %s, err: %v", w.path, err)
	}
	// 遍历目录中已有的子项。
	return w.fs.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if path == dir {
				return fmt.Errorf("error accessing path: %s error: %v", path, err)
			}

			logger.Error(err, "Error accessing path", "path", path)
			return nil
		}

		// 避免对根目录重复调用 fsWatcher.Add，防止潜在问题。
		if path == dir {
			return nil
		}

		// 目录加入监听；socket 文件按创建事件处理；其他文件忽略。
		mode := info.Mode()
		if mode.IsDir() {
			if err := w.fsWatcher.Add(path); err != nil {
				return fmt.Errorf("failed to watch %s, err: %v", path, err)
			}
		} else if isSocket, _ := util.IsUnixDomainSocket(path); isSocket {
			event := fsnotify.Event{
				Name: path,
				Op:   fsnotify.Create,
			}
			// TODO: 处理错误并采取纠正措施。
			if err := w.handleCreateEvent(ctx, event); err != nil {
				logger.Error(err, "Error when handling create", "event", event)
			}
		} else {
			logger.V(5).Info("Ignoring file", "path", path, "mode", mode)
		}

		return nil
	})
}

// 处理文件系统通知事件。文件名不得以 '.' 开头。
func (w *Watcher) handleCreateEvent(ctx context.Context, event fsnotify.Event) error {
	logger := klog.FromContext(ctx)
	logger.V(6).Info("Handling create event", "event", event)

	fi, err := getStat(event)
	if err != nil {
		return fmt.Errorf("stat file %s failed: %v", event.Name, err)
	}

	// 以 '.' 开头的文件是临时文件或隐藏文件，不视为插件。
	if strings.HasPrefix(fi.Name(), ".") {
		logger.V(5).Info("Ignoring file (starts with '.')", "path", fi.Name())
		return nil
	}

	// 普通文件必须是 unix domain socket 才会触发注册。
	if !fi.IsDir() {
		isSocket, err := util.IsUnixDomainSocket(util.NormalizePath(event.Name))
		if err != nil {
			return fmt.Errorf("failed to determine if file: %s is a unix domain socket: %v", event.Name, err)
		}
		if !isSocket {
			logger.V(5).Info("Ignoring non socket file", "path", fi.Name())
			return nil
		}

		return w.handlePluginRegistration(ctx, event.Name)
	}

	return w.traversePluginDir(ctx, event.Name)
}

// handlePluginRegistration 将发现的插件 socket 加入期望状态缓存，或刷新其时间戳。
func (w *Watcher) handlePluginRegistration(ctx context.Context, socketPath string) error {
	logger := klog.FromContext(ctx)
	socketPath = getSocketPath(socketPath)
	// 更新期望状态中的插件列表。即使 socket 路径已在缓存中，
	// 它也可能在从缓存移除之前被删除并重新创建，因此仍需调用
	// AddOrUpdatePlugin 刷新时间戳。
	logger.V(2).Info("Adding socket path or updating timestamp to desired state cache", "path", socketPath)
	err := w.desiredStateOfWorld.AddOrUpdatePlugin(ctx, socketPath)
	if err != nil {
		return fmt.Errorf("error adding socket path %s or updating timestamp to desired state cache: %v", socketPath, err)
	}
	return nil
}

// handleDeleteEvent 从期望状态缓存中移除被删除的插件 socket。
func (w *Watcher) handleDeleteEvent(ctx context.Context, event fsnotify.Event) {
	logger := klog.FromContext(ctx)
	logger.V(6).Info("Handling delete event", "event", event)

	socketPath := event.Name
	logger.V(2).Info("Removing socket path from desired state cache", "path", socketPath)
	w.desiredStateOfWorld.RemovePlugin(socketPath)
}
