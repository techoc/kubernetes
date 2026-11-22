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

// Package apicache 通过调度队列或缓存转发 Pod 状态更新与绑定等 API 调用，
// 使调用方无需直接访问 apiserver，并支持异步执行。
package apicache

import (
	"context"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
)

// APICache 负责把 API 调用请求经由调度队列或缓存转发出去。
type APICache struct {
	schedulingQueue internalqueue.SchedulingQueue
	cache           internalcache.Cache
}

// New 创建一个使用给定调度队列与缓存的 APICache。
func New(schedulingQueue internalqueue.SchedulingQueue, cache internalcache.Cache) *APICache {
	return &APICache{
		schedulingQueue: schedulingQueue,
		cache:           cache,
	}
}

// PatchPodStatus 通过调度队列发送 Pod 状态的 patch 请求。
// patch 可能先应用到缓存中的 Pod 对象，再异步执行真正的 API 调用。
// 返回的 channel 可用于等待调用完成。
func (c *APICache) PatchPodStatus(pod *v1.Pod, conditions []*v1.PodCondition, nominatingInfo *fwk.NominatingInfo) (<-chan error, error) {
	return c.schedulingQueue.PatchPodStatus(pod, conditions, nominatingInfo)
}

// BindPod 通过缓存发送绑定请求。绑定可能先应用到缓存中的 Pod 对象，
// 再异步执行真正的 API 调用。返回的 channel 可用于等待调用完成。
func (c *APICache) BindPod(binding *v1.Binding) (<-chan error, error) {
	return c.cache.BindPod(binding)
}

// WaitOnFinish 阻塞等待 API 调用的结果被写入 onFinish channel（由 BindPod 或 PreemptPod 等方法返回）。
// 它返回从 channel 收到的错误；若调用被跳过或被覆盖，也返回 nil，
// 因为这些都属于生命周期的正常结果。
func (c *APICache) WaitOnFinish(ctx context.Context, onFinish <-chan error) error {
	select {
	case err := <-onFinish:
		// 只把意料之外的错误交给调用方，可预期的错误视为正常的生命周期结束，不向上抛。
		if fwk.IsUnexpectedError(err) {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
