/*
Copyright 2024 The Kubernetes Authors.

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

package endpoint

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// staleEndpointsTracker 记录 Endpoints 及其过期资源版本，
// 用于判断某个 Endpoints 是否已过期。
type staleEndpointsTracker struct {
	// lock 保护 staleResourceVersionByEndpoints 的并发访问。
	lock sync.RWMutex
	// staleResourceVersionByEndpoints 记录各 Endpoints 的过期资源版本。
	staleResourceVersionByEndpoints map[types.NamespacedName]string
}

// newStaleEndpointsTracker 创建一个空的 staleEndpointsTracker。
func newStaleEndpointsTracker() *staleEndpointsTracker {
	return &staleEndpointsTracker{
		staleResourceVersionByEndpoints: map[types.NamespacedName]string{},
	}
}

// Stale 将指定 Endpoints 的当前资源版本标记为过期版本。
func (t *staleEndpointsTracker) Stale(endpoints *v1.Endpoints) {
	t.lock.Lock()
	defer t.lock.Unlock()
	nn := types.NamespacedName{Name: endpoints.Name, Namespace: endpoints.Namespace}
	t.staleResourceVersionByEndpoints[nn] = endpoints.ResourceVersion
}

// IsStale 判断 Endpoints 的资源版本是否与记录的过期版本一致。
func (t *staleEndpointsTracker) IsStale(endpoints *v1.Endpoints) bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	nn := types.NamespacedName{Name: endpoints.Name, Namespace: endpoints.Namespace}
	staleResourceVersion, exists := t.staleResourceVersionByEndpoints[nn]
	if exists && staleResourceVersion == endpoints.ResourceVersion {
		return true
	}
	return false
}

// Delete 删除指定命名空间下 Endpoints 的过期版本记录。
func (t *staleEndpointsTracker) Delete(namespace, name string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	nn := types.NamespacedName{Namespace: namespace, Name: name}
	delete(t.staleResourceVersionByEndpoints, nn)
}
