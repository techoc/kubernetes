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

package secret

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/util/manager"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/clock"
)

// Manager 管理 Kubernetes Secret，包括获取 Secret 以及按 Pod 注册/注销。
type Manager interface {
	// 按命名空间与名称获取 Secret。
	GetSecret(namespace, name string) (*v1.Secret, error)

	// 警告：RegisterPod/UnregisterPod 必须高效，不能阻塞在网络操作上。

	// 注册给定 Pod 引用的所有 Secret。
	RegisterPod(pod *v1.Pod)

	// 注销给定 Pod 引用、且不再被其他已注册 Pod 使用的 Secret。
	UnregisterPod(pod *v1.Pod)
}

// simpleSecretManager 直接调用 apiserver 实现 SecretManager。
type simpleSecretManager struct {
	kubeClient clientset.Interface
}

// NewSimpleSecretManager 创建简单的 SecretManager 实例。
func NewSimpleSecretManager(kubeClient clientset.Interface) Manager {
	return &simpleSecretManager{kubeClient: kubeClient}
}

// GetSecret 直接从 apiserver 读取 Secret。
func (s *simpleSecretManager) GetSecret(namespace, name string) (*v1.Secret, error) {
	return s.kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// RegisterPod 为空操作：该实现不缓存。
func (s *simpleSecretManager) RegisterPod(pod *v1.Pod) {
}

// UnregisterPod 为空操作：该实现不缓存。
func (s *simpleSecretManager) UnregisterPod(pod *v1.Pod) {
}

// secretManager 缓存已注册 Pod 所需的 Secret。
// 不同 store 实现会带来不同的新鲜度语义（如基于 TTL 与基于 watch）。
type secretManager struct {
	manager manager.Manager
}

// GetSecret 从缓存管理器读取对象并做类型断言。
func (s *secretManager) GetSecret(namespace, name string) (*v1.Secret, error) {
	object, err := s.manager.GetObject(namespace, name)
	if err != nil {
		return nil, err
	}
	if secret, ok := object.(*v1.Secret); ok {
		return secret, nil
	}
	return nil, fmt.Errorf("unexpected object type: %v", object)
}

// RegisterPod 把 Pod 引用的 Secret 注册到缓存管理器。
func (s *secretManager) RegisterPod(pod *v1.Pod) {
	s.manager.RegisterPod(pod)
}

// UnregisterPod 从缓存管理器注销 Pod 引用。
func (s *secretManager) UnregisterPod(pod *v1.Pod) {
	s.manager.UnregisterPod(pod)
}

// getSecretNames 收集 Pod 引用的所有 Secret 名称（去重）。
func getSecretNames(pod *v1.Pod) sets.Set[string] {
	result := sets.New[string]()
	podutil.VisitPodSecretNames(pod, func(name string) bool {
		result.Insert(name)
		return true
	})
	return result
}

// 默认 TTL：缓存对象超过该时间视为过期。
const (
	defaultTTL = time.Minute
)

// NewCachingSecretManager 创建缓存型 Secret 管理器。
// 逻辑如下：
//   - Pod 创建或更新时，所有 Secret 的缓存副本失效；
//   - 每次 GetObject() 先查本地缓存；缺失、失效或过旧时从 apiserver 拉取并刷新缓存，
//     否则直接使用缓存。
func NewCachingSecretManager(kubeClient clientset.Interface, getTTL manager.GetObjectTTLFunc) Manager {
	getSecret := func(namespace, name string, opts metav1.GetOptions) (runtime.Object, error) {
		return kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, opts)
	}
	// 基于 TTL 的对象存储，过期后回源 apiserver。
	secretStore := manager.NewObjectStore(getSecret, clock.RealClock{}, getTTL, defaultTTL)
	return &secretManager{
		manager: manager.NewCacheBasedManager(secretStore, getSecretNames),
	}
}

// NewWatchingSecretManager 创建基于 watch 的 Secret 管理器。
// 逻辑如下：
//   - Pod 创建或更新时，为不再被其他 Pod 引用的对象启动独立 watch；
//   - 每次 GetObject() 直接返回通过 watch 传播到本地缓存的值。
func NewWatchingSecretManager(kubeClient clientset.Interface, resyncInterval time.Duration) Manager {
	// 构造 List/Watch/NewObject 回调，供 watch 型管理器使用。
	listSecret := func(namespace string, opts metav1.ListOptions) (runtime.Object, error) {
		return kubeClient.CoreV1().Secrets(namespace).List(context.TODO(), opts)
	}
	watchSecret := func(namespace string, opts metav1.ListOptions) (watch.Interface, error) {
		return kubeClient.CoreV1().Secrets(namespace).Watch(context.TODO(), opts)
	}
	newSecret := func() runtime.Object {
		return &v1.Secret{}
	}
	// 不可变 Secret 无需 watch 更新。
	isImmutable := func(object runtime.Object) bool {
		if secret, ok := object.(*v1.Secret); ok {
			return secret.Immutable != nil && *secret.Immutable
		}
		return false
	}
	listWatcherWithWatchListSemanticsWrapper := func(lw *cache.ListWatch) cache.ListerWatcher {
		return cache.ToListWatcherWithWatchListSemantics(lw, kubeClient)
	}
	// 资源组标识用于构造 ListWatch。
	gr := corev1.Resource("secret")
	return &secretManager{
		manager: manager.NewWatchBasedManager(listSecret, watchSecret, newSecret, isImmutable, listWatcherWithWatchListSemanticsWrapper, gr, resyncInterval, getSecretNames),
	}
}
