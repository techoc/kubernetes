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

package podtopologylabels

import (
	"context"
	"fmt"
	"io"

	"k8s.io/klog/v2"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/component-base/featuregate"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
)

// PluginName 是本插件的名称。
const PluginName = "PodTopologyLabels"

// defaultConfig 是 kube-apiserver 使用的默认插件配置。
// 不导出以避免变量被意外修改。
var defaultConfig = Config{
	Labels: []string{"topology.kubernetes.io/zone", "topology.kubernetes.io/region"},
}

// Register 向 apiserver 注册本插件的工厂函数。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(_ io.Reader) (admission.Interface, error) {
		plugin := NewPodTopologyPlugin(defaultConfig)
		return plugin, nil
	})
}

// Config 是 podtopologylabels 准入插件实例的配置。
// 它不是面向用户的 APIServer 配置，但平台运维方可以用它构建自定义的拓扑标签插件。
type Config struct {
	// Labels 是准入时要从 Node 对象复制到 Pod Binding 的标签键集合。
	Labels []string
}

// NewPodTopologyPlugin 根据配置创建一个 Plugin 实例。
func NewPodTopologyPlugin(c Config) *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Create),
		labels:  sets.New(c.Labels...),
	}
}

// Plugin 是 PodTopologyLabels 准入控制器，负责把节点的拓扑标签复制到 Pod 或 Binding。
type Plugin struct {
	*admission.Handler

	nodeLister corev1listers.NodeLister

	// 需要复制到被绑定 Pod 上的显式标签键。
	labels sets.Set[string]

	enabled, inspectedFeatureGates bool
}

var _ admission.MutationInterface = &Plugin{}
var _ genericadmissioninitializer.WantsExternalKubeInformerFactory = &Plugin{}
var _ genericadmissioninitializer.WantsFeatures = &Plugin{}

// InspectFeatureGates 实现 WantsFeatures，读取 PodTopologyLabelsAdmission 特性开关。
func (p *Plugin) InspectFeatureGates(featureGates featuregate.FeatureGate) {
	p.enabled = featureGates.Enabled(features.PodTopologyLabelsAdmission)
	p.inspectedFeatureGates = true
}

// SetExternalKubeInformerFactory 注入 Node informer，并用其 lister 与同步状态初始化插件。
func (p *Plugin) SetExternalKubeInformerFactory(factory informers.SharedInformerFactory) {
	nodeInformer := factory.Core().V1().Nodes()
	p.nodeLister = nodeInformer.Lister()
	p.SetReadyFunc(nodeInformer.Informer().HasSynced)
}

// ValidateInitialization 检查 Node lister 已注入且特性开关已被读取。
func (p *Plugin) ValidateInitialization() error {
	if p.nodeLister == nil {
		return fmt.Errorf("nodeLister not set")
	}
	if !p.inspectedFeatureGates {
		return fmt.Errorf("feature gates not inspected")
	}
	return nil
}

// Admit 在特性开关启用时，按请求类型（Pod 创建或 Binding）执行对应的准入逻辑。
func (p *Plugin) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if !p.enabled {
		return nil
	}
	// 先判断请求是否是本插件关心的 Pod 或 Binding。
	shouldAdmit, doAdmit, err := p.shouldAdmit(a)
	if !shouldAdmit || err != nil {
		// 不处理时 err 为 nil；若需报错则 err 非 nil，直接返回即可。
		return err
	}
	// Node 缓存未同步前拿不到完整标签，不能冒然处理请求。
	if !p.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}
	// 由 shouldAdmit 选定的类型专属逻辑处理。
	return doAdmit(ctx, a, o)
}

// admitBinding 把目标节点的拓扑标签写入 Binding 的 labels。
func (p *Plugin) admitBinding(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	binding := a.GetObject().(*api.Binding)
	// 默认调度器不会完整填写 binding target 的其他字段，只能依据 Kind 判断目标类型。
	if binding.Target.Kind != "Node" {
		klog.V(6).Info("Skipping Pod being bound to non-Node object type", "target", binding.Target.GroupVersionKind())
		return nil
	}

	labelsToCopy, err := p.topologyLabelsForNodeName(binding.Target.Name)
	if err != nil {
		return err
	}
	if len(labelsToCopy) == 0 {
		// 节点没有拓扑标签，无需继续处理。
		return nil
	}

	// podBinding 存储会把 Binding 的 labels 复制到被绑定的 Pod，因此写在这里即可。
	if binding.Labels == nil {
		binding.Labels = make(map[string]string)
	}
	mergeLabels(binding.Labels, labelsToCopy)

	return nil
}

// admitPod 把已调度 Pod 所在节点的拓扑标签写入 Pod 自身的 labels。
func (p *Plugin) admitPod(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	pod := a.GetObject().(*api.Pod)
	if pod.Spec.NodeName == "" {
		// 尚未调度，没有节点可查。
		return nil
	}

	// 从已分配的节点上取出需要复制的拓扑标签。
	labelsToCopy, err := p.topologyLabelsForNodeName(pod.Spec.NodeName)
	if err != nil {
		return err
	}
	if len(labelsToCopy) == 0 {
		// 节点没有拓扑标签，无需继续处理。
		return nil
	}

	// 把拓扑标签合并进 Pod 的 labels。
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	// 同名标签以节点上的值为准，覆盖 Pod 已有的值。
	mergeLabels(pod.Labels, labelsToCopy)

	return nil
}

// topologyLabelsForNodeName 返回指定节点上属于拓扑标签的键值对；节点不存在时返回空集合。
func (p *Plugin) topologyLabelsForNodeName(nodeName string) (map[string]string, error) {
	labels := make(map[string]string)
	node, err := p.nodeLister.Get(nodeName)
	if err != nil {
		// 节点刚被删除等情况属于正常竞态，忽略 NotFound 以免破坏既有行为。
		if apierrors.IsNotFound(err) {
			return labels, nil
		}
		return nil, err
	}

	for k, v := range node.Labels {
		if !p.isTopologyLabel(k) {
			continue
		}
		labels[k] = v
	}

	return labels, nil
}

// mergeLabels 把 new 中的键值对合并进 existing，同名键以 new 为准。
func mergeLabels(existing, new map[string]string) {
	for k, v := range new {
		existing[k] = v
	}
}

// isTopologyLabel 判断标签键是否在配置要求的显式拓扑标签集合中。
func (p *Plugin) isTopologyLabel(key string) bool {
	// 只需匹配配置里显式列出的标签键。
	if p.labels.Has(key) {
		return true
	}
	return false
}

// admitFunc 是针对特定资源类型执行准入处理的函数签名。
type admitFunc func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error)

// shouldAdmit 检查准入属性，判断请求是否需要本插件处理，并返回对应的处理函数。
func (p *Plugin) shouldAdmit(a admission.Attributes) (bool, admitFunc, error) {
	if a.GetResource().GroupResource() != api.Resource("pods") {
		return false, nil, nil
	}

	switch a.GetSubresource() {
	case "": // 普通 Pod 创建请求
		_, ok := a.GetObject().(*api.Pod)
		if !ok {
			return false, nil, fmt.Errorf("expected Pod but got %T", a.GetObject())
		}
		return true, p.admitPod, nil
	case "binding":
		_, ok := a.GetObject().(*api.Binding)
		if !ok {
			return false, nil, fmt.Errorf("expected Binding but got %s", a.GetKind().Kind)
		}
		return true, p.admitBinding, nil
	default:
		// 其他子资源一概忽略。
		return false, nil, nil
	}
}
