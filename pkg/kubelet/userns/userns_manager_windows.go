/*
Copyright 2025 The Kubernetes Authors.

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

package userns

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// UsernsManager 在 Windows 上不支持用户命名空间，仅为接口完整性保留空实现。
type UsernsManager struct{}

// MakeUserNsManager 在 Windows 上返回 nil，表示不支持用户命名空间管理。
func MakeUserNsManager(klog.Logger, userNsPodsManager, *int64) (*UsernsManager, error) {
	return nil, nil
}

// Release 释放分配给指定 Pod 的用户命名空间；Windows 上为空操作。
func (m *UsernsManager) Release(klog.Logger, types.UID) {
	return
}

// GetOrCreateUserNamespaceMappings 在 Windows 上始终返回 nil。
func (m *UsernsManager) GetOrCreateUserNamespaceMappings(klog.Logger, *v1.Pod, string) (*runtimeapi.UserNamespace, error) {
	return nil, nil
}

// CleanupOrphanedPodUsernsAllocations 对账用户命名空间分配与实际运行 Pod 的状态，
// 释放孤儿 Pod 的分配；Windows 上为空操作。
func (m *UsernsManager) CleanupOrphanedPodUsernsAllocations(context.Context, []*v1.Pod, []*kubecontainer.Pod) error {
	return nil
}

// EnabledUserNamespacesSupport 在 Windows 上始终返回 false。
func EnabledUserNamespacesSupport() bool {
	return false
}
