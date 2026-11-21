/*
Copyright 2020 The Kubernetes Authors.

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

package memorymanager

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// policyTypeNone 是 none 策略的类型名。
const policyTypeNone policyType = "None"

// none 是 none 策略的实现：使用 none 策略等同于关闭内存管理。
type none struct{}

var _ Policy = &none{}

// NewPolicyNone 返回 none 策略实例。
func NewPolicyNone(logger klog.Logger) Policy {
	return &none{}
}

// Name 返回策略名称。
func (p *none) Name() string {
	return string(policyTypeNone)
}

// Start 对 none 策略为空操作。
func (p *none) Start(logger klog.Logger, s state.State) error {
	logger.Info("Start")
	return nil
}

// Allocate 调用是幂等的；none 策略不做任何分配。
func (p *none) Allocate(_ context.Context, s state.State, pod *v1.Pod, container *v1.Container, _ lifecycle.Operation) error {
	return nil
}

// RemoveContainer 调用是幂等的；none 策略无需清理。
func (p *none) RemoveContainer(_ klog.Logger, s state.State, podUID string, containerName string) {
	// AllocatePod 对 none 策略为空操作。
}

// GetTopologyHints 实现 topologymanager.HintProvider 接口；
// none 策略不提供任何 hint。
func (p *none) GetTopologyHints(_ klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, _ lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	return nil
}

// GetPodTopologyHints 实现 topologymanager.HintProvider 接口；
// none 策略不提供任何 Pod 级 hint。
func (p *none) GetPodTopologyHints(_ klog.Logger, s state.State, pod *v1.Pod, _ lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	return nil
}

// AllocatePod 对 none 策略为空操作：不进行 Pod 级内存分配。
func (p *none) AllocatePod(_ klog.Logger, s state.State, pod *v1.Pod, _ lifecycle.Operation) error {
	return nil
}

// GetAllocatableMemory 对 none 策略返回空列表，表示不管理可分配内存。
func (p *none) GetAllocatableMemory(s state.State) []state.Block {
	return []state.Block{}
}
