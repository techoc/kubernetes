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
	"sort"
	"strings"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	api "k8s.io/kubernetes/pkg/apis/core"
	utilio "k8s.io/utils/io"
)

// podEventType 表示配置文件监听产生的事件类型。
type podEventType int

const (
	// Pod 事件类型：新增、修改、删除；eventBufferLen 是事件通道的缓冲长度。
	podAdd podEventType = iota
	podModify
	podDelete

	eventBufferLen = 10
)

// watchEvent 记录一次文件监听事件及其来源文件。
type watchEvent struct {
	fileName  string
	eventType podEventType
}

// sourceFile 表示以单个文件或目录为来源的静态 Pod 配置源。
type sourceFile struct {
	path           string
	nodeName       types.NodeName
	period         time.Duration
	store          cache.Store
	fileKeyMapping map[string]string
	updates        chan<- sourceUpdate
	watchEvents    chan *watchEvent
}

// NewSourceFile 监听配置文件的变化。
func NewSourceFile(logger klog.Logger, path string, nodeName types.NodeName, period time.Duration, updates chan<- sourceUpdate) {
	// go-inotify 要求路径末尾不带分隔符，先去掉尾部的 "/"。
	path = strings.TrimRight(path, string(os.PathSeparator))

	config := newSourceFile(path, nodeName, period, updates)
	logger.V(1).Info("Watching path", "path", path)
	config.run(logger)
}

// newSourceFile 构造配置源，并用 UndeltaStore 把全量 Pod 列表推送到 updates。
func newSourceFile(path string, nodeName types.NodeName, period time.Duration, updates chan<- sourceUpdate) *sourceFile {
	// send 把存储中的全部对象转换为 Pod 列表并发送全量更新。
	send := func(objs []interface{}) {
		var pods []*v1.Pod
		for _, o := range objs {
			pods = append(pods, o.(*v1.Pod))
		}
		updates <- sourceUpdate{Pods: pods}
	}
	store := cache.NewUndeltaStore(send, cache.MetaNamespaceKeyFunc)
	return &sourceFile{
		path:           path,
		nodeName:       nodeName,
		period:         period,
		store:          store,
		fileKeyMapping: map[string]string{},
		updates:        updates,
		watchEvents:    make(chan *watchEvent, eventBufferLen),
	}
}

// run 启动周期轮询与文件监听，并在后台 goroutine 中处理配置更新。
func (s *sourceFile) run(logger klog.Logger) {
	listTicker := time.NewTicker(s.period)

	go func() {
		// 启动时立即读取一次，加快首次同步。
		if err := s.listConfig(logger); err != nil {
			logger.Error(err, "Unable to read config path", "path", s.path)
		}
		for {
			select {
			case <-listTicker.C:
				if err := s.listConfig(logger); err != nil {
					logger.Error(err, "Unable to read config path", "path", s.path)
				}
			case e := <-s.watchEvents:
				if err := s.consumeWatchEvent(logger, e); err != nil {
					logger.Error(err, "Unable to process watch event")
				}
			}
		}
	}()

	s.startWatch(logger)
}

// applyDefaults 为来自文件的 Pod 应用默认值。
func (s *sourceFile) applyDefaults(logger klog.Logger, pod *api.Pod, source string) error {
	return applyDefaults(logger, pod, source, true, s.nodeName)
}

// listConfig 读取配置路径，按文件或目录解析出 Pod 列表并替换存储内容。
func (s *sourceFile) listConfig(logger klog.Logger) error {
	path := s.path
	statInfo, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// 发送空 PodList 更新，使 FileSource 仍被标记为已同步。
		s.updates <- sourceUpdate{Pods: []*v1.Pod{}}
		return fmt.Errorf("path does not exist, ignoring")
	}

	switch {
	case statInfo.Mode().IsDir():
		pods, err := s.extractFromDir(logger, path)
		if err != nil {
			return err
		}
		if len(pods) == 0 {
			// 发送空 PodList 更新，保持与文件来源一致的行为。
			s.updates <- sourceUpdate{Pods: pods}
			return nil
		}
		return s.replaceStore(pods...)

	case statInfo.Mode().IsRegular():
		pod, err := s.extractFromFile(logger, path)
		if err != nil {
			return err
		}
		return s.replaceStore(pod)

	default:
		return fmt.Errorf("path is not a directory or file")
	}
}

// 从目录中尽可能多地读取 Pod 清单。只有在完全无法读取任何内容时才返回错误；
// 个别文件有问题不返回错误。
func (s *sourceFile) extractFromDir(logger klog.Logger, name string) ([]*v1.Pod, error) {
	dirents, err := filepath.Glob(filepath.Join(name, "[^.]*"))
	if err != nil {
		return nil, fmt.Errorf("glob failed: %v", err)
	}

	pods := make([]*v1.Pod, 0, len(dirents))
	if len(dirents) == 0 {
		return pods, nil
	}

	// 按名称排序，保证解析顺序稳定。
	sort.Strings(dirents)
	for _, path := range dirents {
		statInfo, err := os.Stat(path)
		if err != nil {
			// 文件元数据读取失败时跳过该文件，不影响其余文件。
			logger.Error(err, "Could not get metadata", "path", path)
			continue
		}

		switch {
		case statInfo.Mode().IsDir():
			// 不递归处理子目录，只记录错误。
			logger.Error(nil, "Provided manifest path is a directory, not recursing into manifest path", "path", path)
		case statInfo.Mode().IsRegular():
			pod, err := s.extractFromFile(logger, path)
			if err != nil {
				if !os.IsNotExist(err) {
					logger.Error(err, "Could not process manifest file", "path", path)
				}
			} else {
				pods = append(pods, pod)
			}
		default:
			logger.Error(nil, "Manifest path is not a directory or file", "path", path, "mode", statInfo.Mode())
		}
	}
	return pods, nil
}

// extractFromFile 从文件中解析 Pod 配置信息。
func (s *sourceFile) extractFromFile(logger klog.Logger, filename string) (pod *v1.Pod, err error) {
	logger.V(3).Info("Reading config file", "path", filename)
	// 解析成功后记录文件到对象键的映射，供后续按文件删除时使用。
	defer func() {
		if err == nil && pod != nil {
			objKey, keyErr := cache.MetaNamespaceKeyFunc(pod)
			if keyErr != nil {
				err = keyErr
				return
			}
			s.fileKeyMapping[filename] = objKey
		}
	}()

	file, err := os.Open(filename)
	if err != nil {
		return pod, err
	}
	defer file.Close()

	// 限制单个配置文件的最大读取长度，避免异常文件耗尽内存。
	data, err := utilio.ReadAtMost(file, maxConfigLength)
	if err != nil {
		return pod, err
	}

	defaultFn := func(logger klog.Logger, pod *api.Pod) error {
		return s.applyDefaults(logger, pod, filename)
	}

	// 优先按单个 Pod 解析；解析失败时返回带文件名的错误提示。
	parsed, pod, podErr := tryDecodeSinglePod(logger, data, defaultFn)
	if parsed {
		if podErr != nil {
			return pod, podErr
		}
		return pod, nil
	}

	return pod, fmt.Errorf("%v: couldn't parse as pod(%v), please check config file", filename, podErr)
}

// replaceStore 用给定 Pod 列表整体替换存储内容。
func (s *sourceFile) replaceStore(pods ...*v1.Pod) (err error) {
	objs := []interface{}{}
	for _, pod := range pods {
		objs = append(objs, pod)
	}
	return s.store.Replace(objs, "")
}
