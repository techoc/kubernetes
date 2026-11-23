/*
Copyright 2021 The Kubernetes Authors.

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

package admission

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
)

// NamespaceGetterFromClient 只用客户端查询，适用于不需要 informer 的场景。
func NamespaceGetterFromClient(client kubernetes.Interface) NamespaceGetter {
	return &namespaceGetter{client: client}
}

// NamespaceGetterFromListerAndClient 先查本地缓存再回源，减轻 apiserver 压力。
func NamespaceGetterFromListerAndClient(lister corev1listers.NamespaceLister, client kubernetes.Interface) NamespaceGetter {
	return &namespaceGetter{lister: lister, client: client}
}

// namespaceGetter 组合缓存与直连两种查询路径，构造方式决定查询顺序。
type namespaceGetter struct {
	lister corev1listers.NamespaceLister
	client kubernetes.Interface
}

// GetNamespace 优先读 informer 缓存；只有缓存里查不到该对象时才回源到 apiserver。
func (n *namespaceGetter) GetNamespace(ctx context.Context, name string) (namespace *corev1.Namespace, err error) {
	if n.lister != nil {
		namespace, err := n.lister.Get(name)
		// 只有 NotFound 才回源，其他错误（例如缓存未同步）原样返回，避免掩盖故障。
		if err == nil || !apierrors.IsNotFound(err) {
			return namespace, err
		}
	}
	return n.client.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
}
