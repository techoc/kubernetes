/*
Copyright 2017 The Kubernetes Authors.

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

package namespace

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/admission"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// NamespaceSelectorProvider 由具体的 webhook 配置实现，提供已解析的 namespaceSelector。
type NamespaceSelectorProvider interface {
	// 返回 webhook 的 NamespaceSelector 字段，即解析后的选择器；解析失败说明配置非法，调用方应拒绝该 webhook。
	GetParsedNamespaceSelector() (labels.Selector, error)
}

// Matcher 判断请求是否被 webhook 配置的 NamespaceSelector 豁免（跳过调用）。
// 通过读取目标命名空间的标签判断请求是否命中 webhook 的 namespaceSelector。
type Matcher struct {
	NamespaceLister corelisters.NamespaceLister
	Client          clientset.Interface
}

// GetNamespace 优先从 informer 缓存读取命名空间，缓存未命中时回源到存储。
func (m *Matcher) GetNamespace(ctx context.Context, name string) (*v1.Namespace, error) {
	ns, err := m.NamespaceLister.Get(name)
	if apierrors.IsNotFound(err) && len(name) > 0 {
		// 缓存可能滞后，直接回源确认命名空间是否真的不存在，避免误判。
		ns, err = m.Client.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	}
	return ns, err
}

// Validate 检查 Matcher 是否配置了 NamespaceLister 与 Client。
func (m *Matcher) Validate() error {
	var errs []error
	if m.NamespaceLister == nil {
		errs = append(errs, fmt.Errorf("the namespace matcher requires a namespaceLister"))
	}
	if m.Client == nil {
		errs = append(errs, fmt.Errorf("the namespace matcher requires a client"))
	}
	return utilerrors.NewAggregate(errs)
}

// GetNamespaceLabels 返回与 attr 相关的命名空间的标签。
func (m *Matcher) GetNamespaceLabels(attr admission.Attributes) (map[string]string, error) {
	// 若请求本身在创建或更新命名空间，从 attr.Object 取标签，
	// 因为 namespaceLister 还没有最新的命名空间；
	// 创建/更新命名空间时必须使用请求体中的对象，缓存里还是旧版本；
	// 删除时 attr.Object 是 DeleteOptions，只能回退到缓存中的命名空间标签。
	if attr.GetResource().Resource == "namespaces" &&
		len(attr.GetSubresource()) == 0 &&
		(attr.GetOperation() == admission.Create || attr.GetOperation() == admission.Update) {
		accessor, err := meta.Accessor(attr.GetObject())
		if err != nil {
			return nil, err
		}
		return accessor.GetLabels(), nil
	}

	namespaceName := attr.GetNamespace()
	namespace, err := m.NamespaceLister.Get(namespaceName)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	if apierrors.IsNotFound(err) {
		// 缓存可能滞后，直接回源确认命名空间是否真的不存在。
		namespace, err = m.Client.CoreV1().Namespaces().Get(context.TODO(), namespaceName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
	}
	return namespace.Labels, nil
}

// MatchNamespaceSelector 判断请求是否命中 webhook 的 namespaceSelector，
// 只有命中时才调用该 webhook。
// 返回 true 表示 webhook 应被调用；第二个返回值非空时表示无法完成匹配。
func (m *Matcher) MatchNamespaceSelector(p NamespaceSelectorProvider, attr admission.Attributes) (bool, *apierrors.StatusError) {
	namespaceName := attr.GetNamespace()
	if len(namespaceName) == 0 && attr.GetResource().Resource != "namespaces" {
		// 请求针对集群级资源且不是 namespace 时，永不豁免。
		// TODO: 想办法有选择地豁免集群级资源。
		// 同时更新 types.go 中的注释
		return true, nil
	}
	selector, err := p.GetParsedNamespaceSelector()
	if err != nil {
		return false, apierrors.NewInternalError(err)
	}
	// 空选择器表示匹配所有命名空间。
	if selector.Empty() {
		return true, nil
	}

	namespaceLabels, err := m.GetNamespaceLabels(attr)
	// 到这里说明命名空间不存在，为保持向后兼容，
	// 返回 404；命名空间不存在时原样透传状态。
	if apierrors.IsNotFound(err) {
		status, ok := err.(apierrors.APIStatus)
		if !ok {
			return false, apierrors.NewInternalError(err)
		}
		return false, &apierrors.StatusError{ErrStatus: status.Status()}
	}
	if err != nil {
		return false, apierrors.NewInternalError(err)
	}
	return selector.Matches(labels.Set(namespaceLabels)), nil
}
