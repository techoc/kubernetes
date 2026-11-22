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

package autoprovision

import (
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// PluginName 表示该准入插件的名称。
const PluginName = "NamespaceAutoProvision"

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewProvision(), nil
	})
}

// Provision 是 admission.Interface 的一个实现。
// 它检查所有带有命名空间的请求，若该 Namespace 不存在则自动创建。
// 适用于那些不希望在使用前显式限制 Namespace 创建的部署环境。
type Provision struct {
	*admission.Handler
	client          kubernetes.Interface
	namespaceLister corev1listers.NamespaceLister
}

var _ admission.MutationInterface = &Provision{}
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&Provision{})
var _ = genericadmissioninitializer.WantsExternalKubeClientSet(&Provision{})

// Admit 根据请求属性作出准入决策：缺少 Namespace 时自动创建。
func (p *Provision) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	// dry-run 不应产生任何副作用，故不创建 Namespace。
	if a.IsDryRun() {
		return nil
	}

	// 走到这里说明请求已通过认证，且 API server 已找到路由；
	// 因此非空命名空间代表的是 namespaced 资源。
	if len(a.GetNamespace()) == 0 || a.GetKind().GroupKind() == api.Kind("Namespace") {
		return nil
	}
	// 等待缓存同步，避免基于陈旧数据重复创建 Namespace。
	if !p.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}

	_, err := p.namespaceLister.Get(a.GetNamespace())
	if err == nil {
		return nil
	}

	if !errors.IsNotFound(err) {
		return admission.NewForbidden(a, err)
	}

	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      a.GetNamespace(),
			Namespace: "",
		},
		Status: corev1.NamespaceStatus{},
	}

	_, err = p.client.CoreV1().Namespaces().Create(context.TODO(), namespace, metav1.CreateOptions{})
	// 并发请求可能已创建同名 Namespace，此时视为成功。
	if err != nil && !errors.IsAlreadyExists(err) {
		return admission.NewForbidden(a, err)
	}

	return nil
}

// NewProvision 创建一个新的 Namespace 自动供给准入处理器。
func NewProvision() *Provision {
	return &Provision{
		Handler: admission.NewHandler(admission.Create),
	}
}

// SetExternalKubeClientSet 实现 WantsExternalKubeClientSet 接口。
func (p *Provision) SetExternalKubeClientSet(client kubernetes.Interface) {
	p.client = client
}

// SetExternalKubeInformerFactory 实现 WantsExternalKubeInformerFactory 接口。
func (p *Provision) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	namespaceInformer := f.Core().V1().Namespaces()
	p.namespaceLister = namespaceInformer.Lister()
	p.SetReadyFunc(namespaceInformer.Informer().HasSynced)
}

// ValidateInitialization 实现 InitializationValidator 接口，检查依赖的 lister 和 client 均已注入。
func (p *Provision) ValidateInitialization() error {
	if p.namespaceLister == nil {
		return fmt.Errorf("missing namespaceLister")
	}
	if p.client == nil {
		return fmt.Errorf("missing client")
	}
	return nil
}
