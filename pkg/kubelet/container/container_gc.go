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

package container

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog/v2"
)

// GCPolicy 定义容器垃圾回收策略。
type GCPolicy struct {
	// MinAge 是容器可被回收的最小存活时长，零表示不限制。
	MinAge time.Duration

	// MaxPerPodContainer 是每个 (Pod UID, 容器名) 允许保留的已死亡容器上限，
	// 小于零表示不限制。
	MaxPerPodContainer int

	// MaxContainers 是已死亡容器总数上限，小于零表示不限制。
	MaxContainers int
}

// GC 管理已死亡容器的垃圾回收。
//
// 实现是线程兼容的。
type GC interface {
	// GarbageCollect 按策略回收容器。
	GarbageCollect(ctx context.Context) error
	// DeleteAllUnusedContainers 删除所有未使用的容器，包括已终止但未删除的 Pod 的容器。
	DeleteAllUnusedContainers(ctx context.Context) error
}

// SourcesReadyProvider 用于判断配置源是否已就绪。
type SourcesReadyProvider interface {
	// AllReady 返回当前配置的源是否都已读取到。
	AllReady() bool
}

// TODO(vmarmol): Preferentially remove pod infra containers.
type realContainerGC struct {
	// runtime 是容器运行时。
	runtime Runtime

	// policy 是垃圾回收策略。
	policy GCPolicy

	// sourcesReadyProvider 提供 kubelet 配置源的就绪状态。
	sourcesReadyProvider SourcesReadyProvider
}

// NewContainerGC 按指定策略创建 GC 实例。
func NewContainerGC(runtime Runtime, policy GCPolicy, sourcesReadyProvider SourcesReadyProvider) (GC, error) {
	// 负的 MinAge 没有意义，直接拒绝。
	if policy.MinAge < 0 {
		return nil, fmt.Errorf("invalid minimum garbage collection age: %v", policy.MinAge)
	}

	return &realContainerGC{
		runtime:              runtime,
		policy:               policy,
		sourcesReadyProvider: sourcesReadyProvider,
	}, nil
}

// GarbageCollect 按策略回收容器。
func (cgc *realContainerGC) GarbageCollect(ctx context.Context) error {
	// false 表示只按策略回收，不删除全部未使用容器。
	return cgc.runtime.GarbageCollect(ctx, cgc.policy, cgc.sourcesReadyProvider.AllReady(), false)
}

// DeleteAllUnusedContainers 删除所有未使用的容器。
func (cgc *realContainerGC) DeleteAllUnusedContainers(ctx context.Context) error {
	klog.FromContext(ctx).Info("Attempting to delete unused containers")
	// true 表示删除所有未使用容器（含已终止 Pod 的容器）。
	return cgc.runtime.GarbageCollect(ctx, cgc.policy, cgc.sourcesReadyProvider.AllReady(), true)
}
