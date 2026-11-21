/*
Copyright 2019 The Kubernetes Authors.

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

package topologymanager

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/admission"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// fakeManager 是供测试使用的 Topology Manager 实现。
type fakeManager struct {
	// hint 是构造时指定的固定拓扑提示。
	hint *TopologyHint
	// policy 是构造时指定的策略。
	policy Policy
	// scope 是作用域名称。
	scope string
}

// NewFakeManager 创建空的 fake Topology Manager。
func NewFakeManager(logger klog.Logger) Manager {
	logger.Info("NewFakeManager")
	return &fakeManager{}
}

// NewFakeManagerWithScope 创建指定作用域的 fake Topology Manager。
func NewFakeManagerWithScope(scope string) Manager {
	return &fakeManager{
		scope: scope,
	}
}

// NewFakeManagerWithHint 创建返回指定拓扑提示的 fake Topology Manager。
func NewFakeManagerWithHint(logger klog.Logger, hint *TopologyHint) Manager {
	logger.Info("NewFakeManagerWithHint")
	return &fakeManager{
		hint:   hint,
		policy: NewNonePolicy(),
	}
}

// NewFakeManagerWithPolicy 创建使用指定策略的 fake Topology Manager。
func NewFakeManagerWithPolicy(logger klog.Logger, policy Policy) Manager {
	logger.Info("NewFakeManagerWithPolicy", "policy", policy.Name())
	return &fakeManager{
		policy: policy,
	}
}

// GetAffinity 返回构造时指定的拓扑提示。
func (m *fakeManager) GetAffinity(logger klog.Logger, podUID string, containerName string) TopologyHint {
	logger.Info("GetAffinity", "podUID", podUID, "containerName", containerName)
	// 未设置提示时返回空提示。
	if m.hint == nil {
		return TopologyHint{}
	}

	return *m.hint
}

// GetPolicy 返回构造时指定的策略。
func (m *fakeManager) GetPolicy() Policy {
	return m.policy
}

// Name 返回作用域名称。
func (m *fakeManager) Name() string {
	return m.scope
}

// AddHintProvider 记录调用，不实际注册提供者。
func (m *fakeManager) AddHintProvider(logger klog.Logger, h HintProvider) {
	logger.Info("AddHintProvider", "hintProvider", h)
}

// AddContainer 记录新增容器的调用。
func (m *fakeManager) AddContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) {
	logger.Info("AddContainer", "pod", klog.KObj(pod), "containerName", container.Name, "containerID", containerID)
}

// RemoveContainer 记录删除容器的调用并返回成功。
func (m *fakeManager) RemoveContainer(logger klog.Logger, containerID string) error {
	logger.Info("RemoveContainer", "containerID", containerID)
	return nil
}

// Admit 始终允许准入。
func (m *fakeManager) Admit(ctx context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	logger := klog.FromContext(ctx)
	logger.Info("Topology Admit Handler")
	return admission.GetPodAdmitResult(nil)
}
