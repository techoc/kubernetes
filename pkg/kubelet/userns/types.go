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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// 以下类型在支持的操作系统（Windows、Linux）间通用。

// userNsPodsManager 抽象用户命名空间管理器所需的 Pod 信息与映射计算能力。
type userNsPodsManager interface {
	// HandlerSupportsUserNamespaces 判断运行时 handler 是否支持用户命名空间。
	HandlerSupportsUserNamespaces(runtimeHandler string) (bool, error)
	// GetPodDir 返回指定 Pod 在磁盘上的目录。
	GetPodDir(podUID types.UID) string
	// ListPodsFromDisk 列出磁盘上记录的所有 Pod UID。
	ListPodsFromDisk() ([]types.UID, error)
	// GetKubeletMappings 计算 kubelet 使用的 UID/GID 映射范围（每个 Pod 分配 idsPerPod 个 ID）。
	GetKubeletMappings(logger klog.Logger, idsPerPod uint32) (uint32, uint32, error)
	// GetMaxPods 返回节点允许的最大 Pod 数量。
	GetMaxPods() int
}
