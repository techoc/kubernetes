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

package config

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// merger 抽象把单个来源的配置更新合并进整体配置的能力。
type merger interface {
	// Merge 在收到某个来源的变更时调用。
	// 也可以实现为增量合并；定义多个来源时必须可重入。
	Merge(ctx context.Context, source string, update sourceUpdate) error
}

// mux 负责合并多个来源的配置。
// 变更通过通道推送，并交给 merger 合并。
type mux struct {
	// merger 在收到某个来源的更新时被调用。
	merger merger

	// sourceLock 保护 sources 映射。
	sourceLock sync.RWMutex
	// sources 把来源名称映射到对应通道。
	sources map[string]chan sourceUpdate
}

// newMux 创建可合并多个来源变更的 mux。
func newMux(merger merger) *mux {
	mux := &mux{
		sources: make(map[string]chan sourceUpdate),
		merger:  merger,
	}
	return mux
}

// ChannelWithContext 返回配置来源发送更新的通道。
// 相同来源多次调用会返回同一个通道，便于变更型与状态型来源共用；
// 不同来源名称则视为并集。
func (m *mux) ChannelWithContext(ctx context.Context, source string) chan sourceUpdate {
	// 空名称无法区分来源，直接 panic。
	if len(source) == 0 {
		panic("Channel given an empty name")
	}
	m.sourceLock.Lock()
	defer m.sourceLock.Unlock()
	// 同名来源复用同一个通道。
	channel, exists := m.sources[source]
	if exists {
		return channel
	}
	newChannel := make(chan sourceUpdate)
	m.sources[source] = newChannel

	// 每个来源启动一个监听协程。
	go wait.UntilWithContext(ctx, func(ctx context.Context) { m.listen(ctx, source, newChannel) }, 0)
	return newChannel
}

// listen 持续消费来源通道中的更新并交给 merger。
func (m *mux) listen(ctx context.Context, source string, listenChannel <-chan sourceUpdate) {
	logger := klog.FromContext(ctx)
	for update := range listenChannel {
		if err := m.merger.Merge(ctx, source, update); err != nil {
			logger.Info("failed merging update", "err", err)
		}
	}
}
