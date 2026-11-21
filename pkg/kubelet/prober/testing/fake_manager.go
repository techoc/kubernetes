/*
Copyright 2015 The Kubernetes Authors.

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

package testing

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

// FakeManager 是测试用的 prober.Manager 模拟实现。
type FakeManager struct{}

// 以下为未使用的方法。

// AddPod 模拟添加 Pod。
func (FakeManager) AddPod(_ context.Context, _ *v1.Pod) {}

// RemovePod 模拟移除 Pod。
func (FakeManager) RemovePod(_ *v1.Pod) {}

// StopLivenessAndStartup 模拟停止存活与启动探针。
func (FakeManager) StopLivenessAndStartup(_ *v1.Pod) {}

// CleanupPods 模拟清理 Pod。
func (FakeManager) CleanupPods(_ map[types.UID]sets.Empty) {}

// Start 模拟开始同步探针状态。
func (FakeManager) Start() {}

// UpdatePodStatus 模拟更新 Pod 状态，把所有容器标记为 Ready。
func (FakeManager) UpdatePodStatus(_ context.Context, _ *v1.Pod, podStatus *v1.PodStatus) {
	for i := range podStatus.ContainerStatuses {
		podStatus.ContainerStatuses[i].Ready = true
	}
}
