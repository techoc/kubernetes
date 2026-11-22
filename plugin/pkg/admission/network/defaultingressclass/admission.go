/*
Copyright 2020 The Kubernetes Authors.

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

// Package defaultingressclass 提供 DefaultIngressClass 准入控制器：
// 为未指定 class 的 Ingress 设置集群默认 IngressClass。
package defaultingressclass

import (
	"context"
	"fmt"
	"io"
	"sort"

	networkingv1 "k8s.io/api/networking/v1"
	networkingv1beta1 "k8s.io/api/networking/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	networkingv1listers "k8s.io/client-go/listers/networking/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/networking"
)

const (
	// PluginName 是本准入控制器的名称。
	PluginName = "DefaultIngressClass"
)

// Register 向 apiserver 注册本插件的工厂函数。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		plugin := newPlugin()
		return plugin, nil
	})
}

// classDefaulterPlugin 持有 IngressClass lister，实现默认 IngressClass 的准入逻辑。
type classDefaulterPlugin struct {
	*admission.Handler
	lister networkingv1listers.IngressClassLister
}

var _ admission.Interface = &classDefaulterPlugin{}
var _ admission.MutationInterface = &classDefaulterPlugin{}
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&classDefaulterPlugin{})

// newPlugin 创建一个默认 IngressClass 准入插件实例。
func newPlugin() *classDefaulterPlugin {
	return &classDefaulterPlugin{
		Handler: admission.NewHandler(admission.Create),
	}
}

// SetExternalKubeInformerFactory 用给定的 SharedInformerFactory 为插件设置
// IngressClass lister，并把缓存同步就绪作为 readyFunc。
func (a *classDefaulterPlugin) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	informer := f.Networking().V1().IngressClasses()
	a.lister = informer.Lister()
	a.SetReadyFunc(informer.Informer().HasSynced)
}

// ValidateInitialization 检查 lister 已正确注入。
func (a *classDefaulterPlugin) ValidateInitialization() error {
	if a.lister == nil {
		return fmt.Errorf("missing lister")
	}
	return nil
}

// Admit 为用户未指定 class 的 Ingress 设置默认 IngressClass。
func (a *classDefaulterPlugin) Admit(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	if attr.GetResource().GroupResource() != networkingv1.Resource("ingresses") {
		return nil
	}

	if len(attr.GetSubresource()) != 0 {
		return nil
	}

	ingress, ok := attr.GetObject().(*networking.Ingress)
	// 类型转换失败说明不是本插件处理的对象，直接放行。
	if !ok {
		klog.V(3).Infof("Expected Ingress resource, got: %v", attr.GetKind())
		return errors.NewInternalError(fmt.Errorf("Expected Ingress resource, got: %v", attr.GetKind()))
	}

	// 已显式指定 IngressClassName，不再覆盖用户的选择。
	if ingress.Spec.IngressClassName != nil {
		return nil
	}

	// 已通过旧注解指定 class，保持兼容，不再设置默认值。
	if _, ok := ingress.Annotations[networkingv1beta1.AnnotationIngressClass]; ok {
		return nil
	}

	klog.V(4).Infof("No class specified on Ingress %s", ingress.Name)

	defaultClass, err := getDefaultClass(a.lister)
	if err != nil {
		return admission.NewForbidden(attr, err)
	}

	// 集群里没有默认 IngressClass，保持原样。
	if defaultClass == nil {
		return nil
	}

	klog.V(4).Infof("Defaulting class for Ingress %s to %s", ingress.Name, defaultClass.Name)
	ingress.Spec.IngressClassName = &defaultClass.Name
	return nil
}

// getDefaultClass 从 lister 中返回默认 IngressClass；不存在时返回 nil。
// 存在多个默认 class 时取创建时间最新的，时间相同则按名称排序取确定结果。
func getDefaultClass(lister networkingv1listers.IngressClassLister) (*networkingv1.IngressClass, error) {
	list, err := lister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	defaultClasses := []*networkingv1.IngressClass{}
	for _, class := range list {
		if class.Annotations[networkingv1.AnnotationIsDefaultIngressClass] == "true" {
			defaultClasses = append(defaultClasses, class)
		}
	}

	if len(defaultClasses) == 0 {
		return nil, nil
	}
	sort.Slice(defaultClasses, func(i, j int) bool {
		if defaultClasses[i].CreationTimestamp.UnixNano() == defaultClasses[j].CreationTimestamp.UnixNano() {
			return defaultClasses[i].Name < defaultClasses[j].Name
		}
		return defaultClasses[i].CreationTimestamp.UnixNano() > defaultClasses[j].CreationTimestamp.UnixNano()
	})
	if len(defaultClasses) > 1 {
		klog.V(4).Infof("%d default IngressClasses were found, choosing the newest: %s", len(defaultClasses), defaultClasses[0].Name)
	}

	return defaultClasses[0], nil
}
