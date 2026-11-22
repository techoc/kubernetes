/*
Copyright 2014 The Kubernetes Authors.

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

package exists

import (
	"context"
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	informers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// PluginName 表示准入插件的名称。
const PluginName = "NamespaceExists"

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewExists(), nil
	})
}

// Exists 是 admission.Interface 的实现。
// 若命名空间不存在，它拒绝所有带命名空间上下文的传入请求。
// 适用于要求预先声明 Namespace 资源的部署场景。
type Exists struct {
	*admission.Handler
	client          kubernetes.Interface
	namespaceLister corev1listers.NamespaceLister
}

var _ admission.ValidationInterface = &Exists{}
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&Exists{})
var _ = genericadmissioninitializer.WantsExternalKubeClientSet(&Exists{})

// Validate 根据请求属性做出准入决定。
func (e *Exists) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	// 能走到这里说明已经通过认证，且 API Server 已找到路由；因此若命名空间非空，
	// 说明这是命名空间级资源，需要继续校验其命名空间是否存在。
	if len(a.GetNamespace()) == 0 || a.GetKind().GroupKind() == api.Kind("Namespace") {
		return nil
	}

	// 缓存未预热前不能做判断，否则会把存在的命名空间误判为不存在。
	if !e.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}
	_, err := e.namespaceLister.Get(a.GetNamespace())
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return errors.NewInternalError(err)
	}

	// 缓存可能有延迟，直接查一次存储，确认命名空间确实不存在。
	_, err = e.client.CoreV1().Namespaces().Get(context.TODO(), a.GetNamespace(), metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return err
		}
		return errors.NewInternalError(err)
	}

	return nil
}

// NewExists 创建一个新的命名空间存在性准入处理器。
func NewExists() *Exists {
	return &Exists{
		Handler: admission.NewHandler(admission.Create, admission.Update, admission.Delete),
	}
}

// SetExternalKubeClientSet 实现 WantsExternalKubeClientSet 接口。
func (e *Exists) SetExternalKubeClientSet(client kubernetes.Interface) {
	e.client = client
}

// SetExternalKubeInformerFactory 实现 WantsExternalKubeInformerFactory 接口。
func (e *Exists) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	namespaceInformer := f.Core().V1().Namespaces()
	e.namespaceLister = namespaceInformer.Lister()
	e.SetReadyFunc(namespaceInformer.Informer().HasSynced)
}

// ValidateInitialization 实现 InitializationValidator 接口，校验依赖已注入。
func (e *Exists) ValidateInitialization() error {
	if e.namespaceLister == nil {
		return fmt.Errorf("missing namespaceLister")
	}
	if e.client == nil {
		return fmt.Errorf("missing client")
	}
	return nil
}
