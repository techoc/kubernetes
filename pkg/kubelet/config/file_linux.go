//go:build linux

/*
Copyright 2016 The Kubernetes Authors.

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

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/flowcontrol"
)

// 重试周期：监听失败后从 1s 起步、最长 20s 的指数退避。
const (
	retryPeriod    = 1 * time.Second
	maxRetryPeriod = 20 * time.Second
)

// retryableError 表示可重试的错误：不推进退避，下一轮立即重试。
type retryableError struct {
	message string
}

// Error 返回错误信息。
func (e *retryableError) Error() string {
	return e.message
}

// startWatch 启动后台协程监听配置文件变化。
func (s *sourceFile) startWatch(logger klog.Logger) {
	// 使用指数退避，避免路径长期不可用时反复打日志、空转。
	backOff := flowcontrol.NewBackOff(retryPeriod, maxRetryPeriod)
	backOffID := "watch"

	go wait.Forever(func() {
		// 处于退避窗口内则跳过本轮，等待下一次 tick。
		if backOff.IsInBackOffSinceUpdate(backOffID, time.Now()) {
			return
		}

		if err := s.doWatch(logger); err != nil {
			logger.Error(err, "Unable to read config path", "path", s.path)
			// 只有非可重试错误才推进退避；"路径不存在"等可恢复情况不应拉长退避。
			if _, retryable := err.(*retryableError); !retryable {
				backOff.Next(backOffID, time.Now())
			}
		}
	}, retryPeriod)
}

// doWatch 执行实际的监听逻辑：先确认路径存在，再注册 inotify 并循环处理事件。
func (s *sourceFile) doWatch(logger klog.Logger) error {
	_, err := os.Stat(s.path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// 发送空 Pod 列表，让 FileSource 即使路径尚不存在也被标记为已就绪。
		s.updates <- sourceUpdate{Pods: []*v1.Pod{}}
		return &retryableError{"path does not exist, ignoring"}
	}

	// 每次调用都新建 watcher，并在退出时关闭，避免文件描述符泄漏。
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("unable to create inotify: %v", err)
	}
	defer w.Close()

	err = w.Add(s.path)
	if err != nil {
		return fmt.Errorf("unable to create inotify for path %q: %v", s.path, err)
	}

	// 事件循环：处理文件变化与 watcher 错误；任一错误都会终止本次监听，由外层重试。
	for {
		select {
		case event := <-w.Events:
			if err = s.produceWatchEvent(logger, &event); err != nil {
				return fmt.Errorf("error while processing inotify event (%+v): %v", event, err)
			}
		case err = <-w.Errors:
			return fmt.Errorf("error while watching %q: %v", s.path, err)
		}
	}
}

// produceWatchEvent 把 inotify 事件转换为 Pod 事件并投递到 watchEvents。
func (s *sourceFile) produceWatchEvent(logger klog.Logger, e *fsnotify.Event) error {
	// 忽略以点开头的文件（编辑器临时文件、隐藏文件），避免误创建 Pod。
	if strings.HasPrefix(filepath.Base(e.Name), ".") {
		logger.V(4).Info("Ignored pod manifest, because it starts with dots", "eventName", e.Name)
		return nil
	}
	// 创建/写入/权限变化都按修改处理；删除/重命名按删除处理。
	var eventType podEventType
	switch {
	case (e.Op & fsnotify.Create) > 0:
		eventType = podAdd
	case (e.Op & fsnotify.Write) > 0:
		eventType = podModify
	case (e.Op & fsnotify.Chmod) > 0:
		eventType = podModify
	case (e.Op & fsnotify.Remove) > 0:
		eventType = podDelete
	case (e.Op & fsnotify.Rename) > 0:
		eventType = podDelete
	default:
		// 其余事件类型忽略。
		return nil
	}

	s.watchEvents <- &watchEvent{e.Name, eventType}
	return nil
}

// consumeWatchEvent 消费 Pod 事件：新增/修改走 Add，删除则从缓存移除并清理映射。
func (s *sourceFile) consumeWatchEvent(logger klog.Logger, e *watchEvent) error {
	switch e.eventType {
	case podAdd, podModify:
		pod, err := s.extractFromFile(logger, e.fileName)
		if err != nil {
			return fmt.Errorf("can't process config file %q: %v", e.fileName, err)
		}
		return s.store.Add(pod)
	// 删除事件先按文件映射找到缓存键，再从 store 与映射中移除。
	case podDelete:
		// 映射不存在说明该文件从未成功加载，直接忽略即可。
		if objKey, keyExist := s.fileKeyMapping[e.fileName]; keyExist {
			pod, podExist, err := s.store.GetByKey(objKey)
			if err != nil {
				return err
			}
			if !podExist {
				return fmt.Errorf("the pod with key %s doesn't exist in cache", objKey)
			}
			if err = s.store.Delete(pod); err != nil {
				return fmt.Errorf("failed to remove deleted pod from cache: %v", err)
			}
			delete(s.fileKeyMapping, e.fileName)
		}
	}
	return nil
}
