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

package podnodeselector

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// NamespaceNodeSelectors 是用于给命名空间指定节点选择器标签的注解键，
// 默认为 scheduler.alpha.kubernetes.io/node-selector。
var NamespaceNodeSelectors = []string{"scheduler.alpha.kubernetes.io/node-selector"}

// PluginName 是插件名称字符串。
const PluginName = "PodNodeSelector"

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		// TODO 改用带版本的配置文件格式。
		pluginConfig := readConfig(config)
		plugin := NewPodNodeSelector(pluginConfig.PodNodeSelectorPluginConfig)
		return plugin, nil
	})
}

// Plugin 是 admission.Interface 的实现。
type Plugin struct {
	*admission.Handler
	client          kubernetes.Interface
	namespaceLister corev1listers.NamespaceLister
	// 集群级的默认节点选择器与各命名空间的白名单。
	clusterNodeSelectors map[string]string
}

var _ = genericadmissioninitializer.WantsExternalKubeClientSet(&Plugin{})
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&Plugin{})

type pluginConfig struct {
	PodNodeSelectorPluginConfig map[string]string
}

// readConfig 从 --admission-control-config-file 指定的文件中读取 clusterDefaultNodeSelector 的默认值。
// 未提供文件时使用空字符串。
// 文件格式：
// podNodeSelectorPluginConfig:
//
//	clusterDefaultNodeSelector: <node-selectors-labels>
//	namespace1: <node-selectors-labels>
//	namespace2: <node-selectors-labels>
func readConfig(config io.Reader) *pluginConfig {
	defaultConfig := &pluginConfig{}
	if config == nil || reflect.ValueOf(config).IsNil() {
		return defaultConfig
	}
	d := yaml.NewYAMLOrJSONDecoder(config, 4096)
	for {
		if err := d.Decode(defaultConfig); err != nil {
			if err != io.EOF {
				continue
			}
		}
		break
	}
	return defaultConfig
}

// Admit 强制要求 Pod 与其命名空间的节点标签选择器至少能匹配集群中的一个节点。
func (p *Plugin) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	if shouldIgnore(a) {
		return nil
	}
	if !p.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}

	resource := a.GetResource().GroupResource()
	pod := a.GetObject().(*api.Pod)
	namespaceNodeSelector, err := p.getNamespaceNodeSelectorMap(a.GetNamespace())
	if err != nil {
		return err
	}

	if labels.Conflicts(namespaceNodeSelector, labels.Set(pod.Spec.NodeSelector)) {
		return errors.NewForbidden(resource, pod.Name, fmt.Errorf("pod node label selector conflicts with its namespace node label selector"))
	}

	// 合并选择器：命名空间选择器 + Pod 自身选择器，后者优先（同名标签以 Pod 为准）。
	podNodeSelectorLabels := labels.Merge(namespaceNodeSelector, pod.Spec.NodeSelector)
	pod.Spec.NodeSelector = map[string]string(podNodeSelectorLabels)
	return p.Validate(ctx, a, o)
}

// Validate 确保 Pod 的节点选择器是被允许的。
func (p *Plugin) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	if shouldIgnore(a) {
		return nil
	}
	if !p.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}

	resource := a.GetResource().GroupResource()
	pod := a.GetObject().(*api.Pod)

	namespaceNodeSelector, err := p.getNamespaceNodeSelectorMap(a.GetNamespace())
	if err != nil {
		return err
	}
	if labels.Conflicts(namespaceNodeSelector, labels.Set(pod.Spec.NodeSelector)) {
		return errors.NewForbidden(resource, pod.Name, fmt.Errorf("pod node label selector conflicts with its namespace node label selector"))
	}

	// 校验命名空间白名单：Pod 的选择器必须是白名单的子集。
	whitelist, err := labels.ConvertSelectorToLabelsMap(p.clusterNodeSelectors[a.GetNamespace()])
	if err != nil {
		return err
	}
	if !isSubset(pod.Spec.NodeSelector, whitelist) {
		return errors.NewForbidden(resource, pod.Name, fmt.Errorf("pod node label selector labels conflict with its namespace whitelist"))
	}

	return nil
}

// getNamespaceNodeSelectorMap 获取命名空间对应的节点选择器，缓存未命中时回退到直接查询 API。
func (p *Plugin) getNamespaceNodeSelectorMap(namespaceName string) (labels.Set, error) {
	namespace, err := p.namespaceLister.Get(namespaceName)
	// informer 缓存可能尚未同步该命名空间，直接查 API 兜底。
	if errors.IsNotFound(err) {
		namespace, err = p.defaultGetNamespace(namespaceName)
		if err != nil {
			if errors.IsNotFound(err) {
				return nil, err
			}
			return nil, errors.NewInternalError(err)
		}
	} else if err != nil {
		return nil, errors.NewInternalError(err)
	}

	return p.getNodeSelectorMap(namespace)
}

// shouldIgnore 判断请求是否无需本插件处理：仅处理 Pod 本体，不处理子资源和其他类型。
func shouldIgnore(a admission.Attributes) bool {
	resource := a.GetResource().GroupResource()
	if resource != api.Resource("pods") {
		return true
	}
	if a.GetSubresource() != "" {
		// 只对 Pod 本体做检查，子资源不受节点选择器约束。
		return true
	}

	_, ok := a.GetObject().(*api.Pod)
	if !ok {
		klog.Errorf("expected pod but got %s", a.GetKind().Kind)
		return true
	}

	return false
}

// NewPodNodeSelector 初始化一个 podNodeSelector 插件。
func NewPodNodeSelector(clusterNodeSelectors map[string]string) *Plugin {
	return &Plugin{
		Handler:              admission.NewHandler(admission.Create),
		clusterNodeSelectors: clusterNodeSelectors,
	}
}

// SetExternalKubeClientSet 设置插件使用的客户端。
func (p *Plugin) SetExternalKubeClientSet(client kubernetes.Interface) {
	p.client = client
}

// SetExternalKubeInformerFactory 配置插件的 informer 工厂。
func (p *Plugin) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	namespaceInformer := f.Core().V1().Namespaces()
	p.namespaceLister = namespaceInformer.Lister()
	p.SetReadyFunc(namespaceInformer.Informer().HasSynced)
}

// ValidateInitialization 校验对象是否已正确初始化。
func (p *Plugin) ValidateInitialization() error {
	if p.namespaceLister == nil {
		return fmt.Errorf("missing namespaceLister")
	}
	if p.client == nil {
		return fmt.Errorf("missing client")
	}
	return nil
}

// defaultGetNamespace 直接从 API 查询命名空间，用于缓存未命中时兜底。
func (p *Plugin) defaultGetNamespace(name string) (*corev1.Namespace, error) {
	namespace, err := p.client.CoreV1().Namespaces().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("namespace %s does not exist", name)
	}
	return namespace, nil
}

// getNodeSelectorMap 从命名空间的注解中提取节点选择器；注解缺失时回退到集群默认选择器。
func (p *Plugin) getNodeSelectorMap(namespace *corev1.Namespace) (labels.Set, error) {
	selector := labels.Set{}
	var err error
	found := false
	if len(namespace.ObjectMeta.Annotations) > 0 {
		for _, annotation := range NamespaceNodeSelectors {
			if ns, ok := namespace.ObjectMeta.Annotations[annotation]; ok {
				// 多个注解的选择器不能互相冲突，否则无法确定最终节点约束。
				labelsMap, err := labels.ConvertSelectorToLabelsMap(ns)
				if err != nil {
					return labels.Set{}, err
				}

				if labels.Conflicts(selector, labelsMap) {
					nsName := namespace.ObjectMeta.Name
					return labels.Set{}, fmt.Errorf("%s annotations' node label selectors conflict", nsName)
				}
				selector = labels.Merge(selector, labelsMap)
				found = true
			}
		}
	}
	if !found {
		selector, err = labels.ConvertSelectorToLabelsMap(p.clusterNodeSelectors["clusterDefaultNodeSelector"])
		if err != nil {
			return labels.Set{}, err
		}
	}
	return selector, nil
}

// isSubset 判断 subSet 是否为 superSet 的子集；superSet 为空时视为放行。
func isSubset(subSet, superSet labels.Set) bool {
	if len(superSet) == 0 {
		return true
	}

	for k, v := range subSet {
		value, ok := superSet[k]
		if !ok {
			return false
		}
		if value != v {
			return false
		}
	}
	return true
}
