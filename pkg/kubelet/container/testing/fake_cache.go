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

package testing

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/kubelet/container"
)

// fakeCache 是 container.Cache 的假实现，直接查询底层运行时。
type fakeCache struct {
	runtime container.Runtime
}

// NewFakeCache 创建基于指定运行时的假缓存。
func NewFakeCache(runtime container.Runtime) container.Cache {
	return &fakeCache{runtime: runtime}
}

// Get 通过底层运行时获取 Pod 状态；Pod 不存在时使用仅含 ID 的空 Pod。
func (c *fakeCache) Get(id types.UID) (*container.PodStatus, error) {
	pod, err := c.runtime.GetPod(context.TODO(), id)
	if err != nil {
		pod = &container.Pod{ID: id}
	}
	return c.runtime.GetPodStatus(context.TODO(), pod)
}

// GetNewerThan 忽略时间条件，直接返回当前状态。
func (c *fakeCache) GetNewerThan(id types.UID, minTime time.Time) (*container.PodStatus, error) {
	return c.Get(id)
}

// Set 为空实现，始终返回 true 表示已更新。
func (c *fakeCache) Set(id types.UID, status *container.PodStatus, err error, timestamp time.Time) (updated bool) {
	return true
}

// Delete 为空实现，假缓存不做删除。
func (c *fakeCache) Delete(id types.UID) {
}

// UpdateTime 为空实现，假缓存不维护时间。
func (c *fakeCache) UpdateTime(_ time.Time) {
}

// SetObservedTime 为空实现，假缓存不记录观测时间。
func (c *fakeCache) SetObservedTime(id types.UID, timestamp time.Time) {
}
