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

package lifecycle

import (
	"context"

	v1 "k8s.io/api/core/v1"
)

// PodAdmitAttributes 是 Pod 准入决策的上下文。
// 该结构的字段绝不应被修改。
type PodAdmitAttributes struct {
	// 待评估准入的 Pod。
	Pod *v1.Pod
	// 已绑定到 kubelet 的所有其他 Pod（不含待评估 Pod）。
	OtherPods []*v1.Pod
	// 当前执行的操作，取值为 "add" 或 "resize"。
	Operation Operation
}

// Operation 表示对 Pod 执行的操作类型。
type Operation string

const (
	// AddOperation 表示 Pod 正被添加到 kubelet。
	AddOperation Operation = "add"
	// ResizeOperation 表示 Pod 正在调整资源。
	ResizeOperation Operation = "resize"
)

// PodAdmitResult 是 Pod 准入决策的结果。
type PodAdmitResult struct {
	// 为 true 时表示应准入该 Pod。
	Admit bool
	// 无法准入时的简短原因（单个词）。
	Reason string
	// 解释无法准入原因的简短消息。
	Message string
}

// PodAdmitHandler 在 Pod 准入过程中被调用。
type PodAdmitHandler interface {
	// Admit 评估 Pod 是否可以准入。
	Admit(ctx context.Context, attrs *PodAdmitAttributes) PodAdmitResult
}

// PodAdmitTarget 维护待调用的准入处理器列表。
type PodAdmitTarget interface {
	// AddPodAdmitHandler 添加指定的准入处理器。
	AddPodAdmitHandler(a PodAdmitHandler)
}

// PodSyncLoopHandler 在每次同步循环迭代时被调用。
type PodSyncLoopHandler interface {
	// ShouldSync 返回 Pod 是否需要同步。
	// 该操作对每个 Pod 都会调用，必须立即返回。
	// 传入的 Pod 绝不应被修改。
	ShouldSync(pod *v1.Pod) bool
}

// PodSyncLoopTarget 维护同步循环使用的处理器列表。
type PodSyncLoopTarget interface {
	// AddPodSyncLoopHandler 添加指定的同步循环处理器。
	AddPodSyncLoopHandler(a PodSyncLoopHandler)
}

// ShouldEvictResponse 是是否驱逐判断的结果。
type ShouldEvictResponse struct {
	// 为 true 时表示应驱逐该 Pod。
	Evict bool
	// 应驱逐的简短 CamelCase 原因。
	Reason string
	// 解释应驱逐原因的简短消息。
	Message string
}

// PodSyncHandler 在每次同步 Pod 时被调用。
type PodSyncHandler interface {
	// ShouldEvict 在每次同步 Pod 时判断是否应将其从 kubelet 驱逐。
	// 若应驱逐，Pod 状态会被标记为 Failed（携带给出的原因和消息），
	// 并立即被杀死。
	// 该操作对每个 Pod 都会调用，必须立即返回。
	// 传入的 Pod 绝不应被修改。
	ShouldEvict(pod *v1.Pod) ShouldEvictResponse
}

// PodSyncTarget 维护 Pod 同步使用的处理器列表。
type PodSyncTarget interface {
	// AddPodSyncHandler 添加指定的同步处理器。
	AddPodSyncHandler(a PodSyncHandler)
}

// PodLifecycleTarget 把一组生命周期接口聚合在一起，方便使用。
type PodLifecycleTarget interface {
	PodAdmitTarget
	PodSyncLoopTarget
	PodSyncTarget
}

// PodAdmitHandlers 是准入处理器列表。
type PodAdmitHandlers []PodAdmitHandler

// AddPodAdmitHandler 添加指定的处理器。
func (handlers *PodAdmitHandlers) AddPodAdmitHandler(a PodAdmitHandler) {
	*handlers = append(*handlers, a)
}

// PodSyncLoopHandlers 是同步循环处理器列表。
type PodSyncLoopHandlers []PodSyncLoopHandler

// AddPodSyncLoopHandler 添加指定的处理器。
func (handlers *PodSyncLoopHandlers) AddPodSyncLoopHandler(a PodSyncLoopHandler) {
	*handlers = append(*handlers, a)
}

// PodSyncHandlers 是 Pod 同步处理器列表。
type PodSyncHandlers []PodSyncHandler

// AddPodSyncHandler 添加指定的同步处理器。.
func (handlers *PodSyncHandlers) AddPodSyncHandler(a PodSyncHandler) {
	*handlers = append(*handlers, a)
}
