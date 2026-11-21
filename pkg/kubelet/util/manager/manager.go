/*
Copyright 2018 The Kubernetes Authors.

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

package manager

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// Manager 定义在底层缓存中注册/注销 Pod 引用对象的接口，
// 并在需要时从缓存中取出这些对象。
type Manager interface {
	// GetObject 按 namespace 和 name 获取对象。
	GetObject(namespace, name string) (runtime.Object, error)

	// 警告：RegisterPod/UnregisterPod 必须高效，不能阻塞在网络操作上。

	// RegisterPod 注册给定 Pod 引用的所有对象。
	//
	// 注意：所有 RegisterPod 实现都必须是幂等的。
	RegisterPod(pod *v1.Pod)

	// UnregisterPod 注销给定 Pod 引用、且不再被其他已注册 Pod 使用的对象。
	//
	// 注意：所有 UnregisterPod 实现都必须是幂等的。
	UnregisterPod(pod *v1.Pod)
}

// Store 是 cacheBasedManager 可使用的对象缓存接口。
type Store interface {
	// AddReference 增加 referencedFrom 对对象的引用。
	// 实现必须允许多次添加，并按引用计数语义处理。
	AddReference(namespace, name string, referencedFrom types.UID)
	// DeleteReference 删除 referencedFrom 对对象的引用。
	// 只有当每次 Add 都有对应的 Delete、引用计数归零时，对象才应被删除。
	DeleteReference(namespace, name string, referencedFrom types.UID)
	// Get 从 store 中获取对象。
	Get(namespace, name string) (runtime.Object, error)
}
