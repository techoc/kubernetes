/*
Copyright 2023 The Kubernetes Authors.

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

package util

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/utils/clock"
)

// NodeStartupLatencyTracker 记录节点启动各阶段的时间点并上报指标。
type NodeStartupLatencyTracker interface {
	// 该函数可能在 kubelet 重启后再次被调用。
	RecordAttemptRegisterNode()
	// 该函数不应在 kubelet 重启后被调用。
	RecordRegisteredNewNode()
	// 该函数可能在 kubelet 重启后再次被调用。
	RecordNodeReady()
}

// basicNodeStartupLatencyTracker 是 NodeStartupLatencyTracker 的默认实现。
type basicNodeStartupLatencyTracker struct {
	// lock 保护以下时间戳字段。
	lock sync.Mutex

	// bootTime 是节点启动时间，取不到时为零值。
	bootTime time.Time
	// kubeletStartTime 是 kubelet 启动时间。
	kubeletStartTime time.Time
	// firstRegistrationAttemptTime 是首次尝试注册节点的时间。
	firstRegistrationAttemptTime time.Time
	// firstRegisteredNewNodeTime 是首次成功注册节点的时间。
	firstRegisteredNewNodeTime time.Time
	// firstNodeReadyTime 是节点首次进入 Ready 的时间。
	firstNodeReadyTime time.Time

	// clock 便于测试替换时钟。
	clock clock.Clock
}

// NewNodeStartupLatencyTracker 创建延迟跟踪器，并记录 kubelet 启动时间。
func NewNodeStartupLatencyTracker(logger klog.Logger) NodeStartupLatencyTracker {
	bootTime, err := GetBootTime(logger)
	if err != nil {
		// 取不到启动时间时保持零值，相关指标会跳过计算。
		bootTime = time.Time{}
	}
	return &basicNodeStartupLatencyTracker{
		bootTime:         bootTime,
		kubeletStartTime: time.Now(),
		clock:            clock.RealClock{},
	}
}

// RecordAttemptRegisterNode 记录首次尝试注册节点的时间。
func (n *basicNodeStartupLatencyTracker) RecordAttemptRegisterNode() {
	n.lock.Lock()
	defer n.lock.Unlock()

	// 只记录第一次注册尝试。
	if !n.firstRegistrationAttemptTime.IsZero() {
		return
	}

	n.firstRegistrationAttemptTime = n.clock.Now()
}

// RecordRegisteredNewNode 记录首次成功注册节点的时间并上报启动阶段指标。
func (n *basicNodeStartupLatencyTracker) RecordRegisteredNewNode() {
	n.lock.Lock()
	defer n.lock.Unlock()

	// 必须先有注册尝试，且只记录第一次成功注册。
	if n.firstRegistrationAttemptTime.IsZero() || !n.firstRegisteredNewNodeTime.IsZero() {
		return
	}

	n.firstRegisteredNewNodeTime = n.clock.Now()

	if !n.bootTime.IsZero() {
		// 只有拿到启动时间时才能计算 kubelet 启动前的耗时。
		metrics.NodeStartupPreKubeletDuration.Set(n.kubeletStartTime.Sub(n.bootTime).Seconds())
	}
	metrics.NodeStartupPreRegistrationDuration.Set(n.firstRegistrationAttemptTime.Sub(n.kubeletStartTime).Seconds())
	metrics.NodeStartupRegistrationDuration.Set(n.firstRegisteredNewNodeTime.Sub(n.firstRegistrationAttemptTime).Seconds())
}

// RecordNodeReady 记录节点首次 Ready 的时间并上报后续阶段指标。
func (n *basicNodeStartupLatencyTracker) RecordNodeReady() {
	n.lock.Lock()
	defer n.lock.Unlock()

	// 必须先完成注册，且只记录第一次 Ready。
	if n.firstRegisteredNewNodeTime.IsZero() || !n.firstNodeReadyTime.IsZero() {
		return
	}

	n.firstNodeReadyTime = n.clock.Now()

	// 记录注册完成到 NodeReady 的耗时。
	metrics.NodeStartupPostRegistrationDuration.Set(n.firstNodeReadyTime.Sub(n.firstRegisteredNewNodeTime).Seconds())
	if !n.bootTime.IsZero() {
		metrics.NodeStartupDuration.Set(n.firstNodeReadyTime.Sub(n.bootTime).Seconds())
	}
}
