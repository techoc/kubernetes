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

package framework

import (
	"context"

	v1 "k8s.io/api/core/v1"
)

// 这个接口把「先改本地缓存、再异步发请求」拆成两步返回：调用方能立刻看到期望状态，
// 又握着 channel 决定何时等待真正的 API 结果。若没有这个抽象，插件就得自己在缓存和 API 之间做双写，
// 顺序和一致性都很难收口。
type APICacher interface {
	// 返回值拆成 (channel, error)：error 只表示「请求没能发起」，channel 才承载实际 API 调用的结果，
	// 这样发起失败与执行失败不会混在一个返回值里。
	PatchPodStatus(pod *v1.Pod, conditions []*v1.PodCondition, nominatingInfo *NominatingInfo) (<-chan error, error)

	// 绑定走同一套缓存先行、异步落地的路径，是为了让后续调度周期能立即看到 Assume 语义，
	// 不必等 apiserver 往返。
	BindPod(binding *v1.Binding) (<-chan error, error)

	// 把被跳过/覆盖也视为成功，是因为这两种情况都意味着状态已被更新的意图作废，
	// 调用方关心的是「最终状态是否符合预期」，而不是「请求是否真的发到了 API」。
	WaitOnFinish(ctx context.Context, onFinish <-chan error) error
}

// 用泛型构造函数结构体而非固定实现，是为了让调度器和外部调用方（如 Cluster Autoscaler）
// 可以注入各自的 APICall 类型：T、K 只需满足 APICall 约束，框架不关心具体怎么发请求。
type APICallImplementations[T, K APICall] struct {
	// PodStatusPatch 是创建用于 patch Pod 状态的 APICall 的构造函数。
	PodStatusPatch func(pod *v1.Pod, conditions []*v1.PodCondition, nominatingInfo *NominatingInfo) T
	// PodBinding 是创建用于绑定 Pod 的 APICall 的构造函数。
	PodBinding func(binding *v1.Binding) K
}
