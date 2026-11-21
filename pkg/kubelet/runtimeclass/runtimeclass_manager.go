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

package runtimeclass

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	nodev1 "k8s.io/client-go/listers/node/v1"
)

// Manager 缓存 RuntimeClass API 对象，并向 kubelet 提供访问方法。
type Manager struct {
	informerFactory informers.SharedInformerFactory
	lister          nodev1.RuntimeClassLister
}

// NewManager 返回新的 RuntimeClass Manager；使用前必须先调用 Start。
func NewManager(client clientset.Interface) *Manager {
	const resyncPeriod = 0

	factory := informers.NewSharedInformerFactory(client, resyncPeriod)
	lister := factory.Node().V1().RuntimeClasses().Lister()

	return &Manager{
		informerFactory: factory,
		lister:          lister,
	}
}

// Start 开始与 apiserver 同步 RuntimeClass 缓存。
func (m *Manager) Start(stopCh <-chan struct{}) {
	m.informerFactory.Start(stopCh)
}

// WaitForCacheSync 暴露 informer factory 的 WaitForCacheSync，便于测试。
func (m *Manager) WaitForCacheSync(stopCh <-chan struct{}) {
	m.informerFactory.WaitForCacheSync(stopCh)
}

// LookupRuntimeHandler 返回指定 RuntimeClass 名称关联的 RuntimeHandler；
// nil 或空名称返回默认值 ""；RuntimeClass 不存在时返回 NotFound 错误。
func (m *Manager) LookupRuntimeHandler(runtimeClassName *string) (string, error) {
	if runtimeClassName == nil || *runtimeClassName == "" {
		// 默认 RuntimeClass 始终解析为空 runtime handler。
		return "", nil
	}

	name := *runtimeClassName

	rc, err := m.lister.Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			return "", err
		}
		return "", fmt.Errorf("failed to lookup RuntimeClass %s: %v", name, err)
	}

	return rc.Handler, nil
}
