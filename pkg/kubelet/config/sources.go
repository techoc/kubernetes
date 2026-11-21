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

// Package config 实现 Pod 配置读取器。
package config

import (
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

// SourcesReadyFn 在指定数据源都已就绪时返回 true。
type SourcesReadyFn func(sourcesSeen sets.Set[string]) bool

// SourcesReady 跟踪 kubelet 已见到的配置数据源集合。
type SourcesReady interface {
	// AddSource 把指定数据源加入受管集合。
	AddSource(source string)
	// AllReady 在当前配置的数据源都已就绪时返回 true。
	AllReady() bool
}

// NewSourcesReady 使用指定的就绪判定函数创建 SourcesReady。
func NewSourcesReady(sourcesReadyFn SourcesReadyFn) SourcesReady {
	return &sourcesImpl{
		sourcesSeen:    sets.New[string](),
		sourcesReadyFn: sourcesReadyFn,
	}
}

// sourcesImpl 实现 SourcesReady，且是线程安全的。
type sourcesImpl struct {
	// lock 保护 sourcesSeen 的并发访问。
	lock sync.RWMutex
	// sourcesSeen 是已见数据源集合。
	sourcesSeen sets.Set[string]
	// sourcesReadyFn 判断数据源是否全部就绪。
	sourcesReadyFn SourcesReadyFn
}

// AddSource 在写锁保护下记录已见的数据源。
func (s *sourcesImpl) AddSource(source string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.sourcesSeen.Insert(source)
}

// AllReady 在读锁保护下调用就绪判定函数。
func (s *sourcesImpl) AllReady() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.sourcesReadyFn(s.sourcesSeen)
}
