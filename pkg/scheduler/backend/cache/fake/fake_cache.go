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

package fake

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
)

// Cache 是用于测试的 Cache 实现：可选地用 Func 字段覆盖各方法，避免依赖真实缓存行为。
type Cache struct {
	internalcache.Cache
	// AssumeFunc 覆盖 AssumePod；为 nil 时使用嵌入的 Cache。
	AssumeFunc func(*v1.Pod)
	// ForgetFunc 覆盖 ForgetPod；为 nil 时使用嵌入的 Cache。
	ForgetFunc func(*v1.Pod)
	// IsAssumedPodFunc 覆盖 IsAssumedPod；为 nil 时使用嵌入的 Cache。
	IsAssumedPodFunc func(*v1.Pod) bool
	// GetPodFunc 覆盖 GetPod；为 nil 时使用嵌入的 Cache。
	GetPodFunc func(*v1.Pod) *v1.Pod
	// UpdateSnapshotFunc 覆盖 UpdateSnapshot；为 nil 时使用嵌入的 Cache。
	UpdateSnapshotFunc func(nodeSnapshot *internalcache.Snapshot) error
}

// AssumePod 允许在测试中通过 AssumeFunc 模拟该方法；未设置时回落到真实缓存。
func (c *Cache) AssumePod(logger klog.Logger, pod *v1.Pod) error {
	if c.AssumeFunc != nil {
		c.AssumeFunc(pod)
		return nil
	}
	return c.Cache.AssumePod(logger, pod)
}

// ForgetPod 允许在测试中通过 ForgetFunc 模拟该方法；未设置时回落到真实缓存。
func (c *Cache) ForgetPod(logger klog.Logger, pod *v1.Pod) error {
	if c.ForgetFunc != nil {
		c.ForgetFunc(pod)
		return nil
	}
	return c.Cache.ForgetPod(logger, pod)
}

// IsAssumedPod 允许在测试中通过 IsAssumedPodFunc 模拟该方法；未设置时回落到真实缓存。
func (c *Cache) IsAssumedPod(pod *v1.Pod) (bool, error) {
	if c.IsAssumedPodFunc != nil {
		return c.IsAssumedPodFunc(pod), nil
	}
	return c.Cache.IsAssumedPod(pod)
}

// GetPod 允许在测试中通过 GetPodFunc 模拟该方法；未设置时回落到真实缓存。
func (c *Cache) GetPod(pod *v1.Pod) (*v1.Pod, error) {
	if c.GetPodFunc != nil {
		return c.GetPodFunc(pod), nil
	}
	return c.Cache.GetPod(pod)
}

// UpdateSnapshot 允许在测试中通过 UpdateSnapshotFunc 模拟该方法；未设置时回落到真实缓存。
func (c *Cache) UpdateSnapshot(logger klog.Logger, nodeSnapshot *internalcache.Snapshot) error {
	if c.UpdateSnapshotFunc != nil {
		return c.UpdateSnapshotFunc(nodeSnapshot)
	}
	return c.Cache.UpdateSnapshot(logger, nodeSnapshot)
}
