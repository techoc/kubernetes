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

package podtolerationrestriction

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
	qoshelper "k8s.io/kubernetes/pkg/apis/core/helper/qos"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/util/tolerations"
	pluginapi "k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction/apis/podtolerationrestriction"
)

// PluginName 是该插件的名称。
const PluginName = "PodTolerationRestriction"

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		pluginConfig, err := loadConfiguration(config)
		if err != nil {
			return nil, err
		}
		return NewPodTolerationsPlugin(pluginConfig), nil
	})
}

// 命名空间注解中默认容忍与容忍白名单的键名。
const (
	// NSDefaultTolerations 是命名空间默认容忍的注解键。
	NSDefaultTolerations string = "scheduler.alpha.kubernetes.io/defaultTolerations"
	// NSWLTolerations 是命名空间容忍白名单的注解键。
	NSWLTolerations string = "scheduler.alpha.kubernetes.io/tolerationsWhitelist"
)

var _ admission.MutationInterface = &Plugin{}
var _ admission.ValidationInterface = &Plugin{}
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&Plugin{})
var _ = genericadmissioninitializer.WantsExternalKubeClientSet(&Plugin{})

// Plugin 持有准入控制器所需的客户端、namespace lister 与插件配置。
type Plugin struct {
	*admission.Handler
	client          kubernetes.Interface
	namespaceLister corev1listers.NamespaceLister
	pluginConfig    *pluginapi.Configuration
}

// Admit 为 Pod 合并命名空间/集群默认容忍，并校验最终容忍是否满足白名单。
func (p *Plugin) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	if shouldIgnore(a) {
		return nil
	}

	if !p.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}

	pod := a.GetObject().(*api.Pod)
	var extraTolerations []api.Toleration
	if a.GetOperation() == admission.Create {
		ts, err := p.getNamespaceDefaultTolerations(a.GetNamespace())
		if err != nil {
			return err
		}

		// 命名空间未配置默认容忍时，退回集群级默认容忍。
		if ts == nil {
			ts = p.pluginConfig.Default
		}

		extraTolerations = ts
	}

	if qoshelper.GetPodQOS(pod) != api.PodQOSBestEffort {
		extraTolerations = append(extraTolerations, api.Toleration{
			Key:      corev1.TaintNodeMemoryPressure,
			Operator: api.TolerationOpExists,
			Effect:   api.TaintEffectNoSchedule,
		})
	}
	// 无论 Pod 类型如何，最终都合并一次容忍列表。
	if len(extraTolerations) > 0 {
		pod.Spec.Tolerations = tolerations.MergeTolerations(pod.Spec.Tolerations, extraTolerations)
	}
	return p.Validate(ctx, a, o)
}

// Validate 校验 Pod 的容忍列表是否满足命名空间或集群级白名单。
func (p *Plugin) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	if shouldIgnore(a) {
		return nil
	}

	if !p.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}

	// 白名单校验。
	pod := a.GetObject().(*api.Pod)
	if len(pod.Spec.Tolerations) > 0 {
		whitelist, err := p.getNamespaceTolerationsWhitelist(a.GetNamespace())
		whitelistScope := "namespace"
		if err != nil {
			return err
		}

		// 命名空间未配置白名单时，退回集群级白名单。
		if whitelist == nil {
			whitelist = p.pluginConfig.Whitelist
			whitelistScope = "cluster"
		}

		if len(whitelist) > 0 {
			// 检查合并后的 Pod 容忍是否满足对应命名空间的白名单。
			if !tolerations.VerifyAgainstWhitelist(pod.Spec.Tolerations, whitelist) {
				return fmt.Errorf("pod tolerations (possibly merged with namespace default tolerations) conflict with its %s whitelist", whitelistScope)
			}
		}
	}

	return nil
}

// shouldIgnore 判断请求是否无需本插件处理：非 Pod、Pod 子资源或类型不匹配都跳过。
func shouldIgnore(a admission.Attributes) bool {
	resource := a.GetResource().GroupResource()
	if resource != api.Resource("pods") {
		return true
	}
	if a.GetSubresource() != "" {
		// 只检查 Pod 本体，不检查子资源。
		return true
	}

	obj := a.GetObject()
	_, ok := obj.(*api.Pod)
	if !ok {
		klog.Errorf("expected pod but got %s", a.GetKind().Kind)
		return true
	}

	return false
}

// NewPodTolerationsPlugin 用给定配置创建并初始化一个 Plugin。
func NewPodTolerationsPlugin(pluginConfig *pluginapi.Configuration) *Plugin {
	return &Plugin{
		Handler:      admission.NewHandler(admission.Create, admission.Update),
		pluginConfig: pluginConfig,
	}
}

// SetExternalKubeClientSet 注入 Kubernetes 客户端，用于缓存未命中时直查存储。
func (p *Plugin) SetExternalKubeClientSet(client kubernetes.Interface) {
	p.client = client
}

// SetExternalKubeInformerFactory 初始化 namespace informer，并在其同步完成后标记插件就绪。
func (p *Plugin) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	namespaceInformer := f.Core().V1().Namespaces()
	p.namespaceLister = namespaceInformer.Lister()
	p.SetReadyFunc(namespaceInformer.Informer().HasSynced)

}

// ValidateInitialization 检查所需依赖均已注入。
func (p *Plugin) ValidateInitialization() error {
	if p.namespaceLister == nil {
		return fmt.Errorf("missing namespaceLister")
	}
	if p.client == nil {
		return fmt.Errorf("missing client")
	}
	return nil
}

// getNamespace 先从缓存读取命名空间，未命中时直查存储。
// 极端情况下可能产生两次实时查询，但缓存追上后即会停止。
func (p *Plugin) getNamespace(nsName string) (*corev1.Namespace, error) {
	namespace, err := p.namespaceLister.Get(nsName)
	if errors.IsNotFound(err) {
		// 缓存可能存在延迟，直接查存储确认命名空间是否真的不存在。
		namespace, err = p.client.CoreV1().Namespaces().Get(context.TODO(), nsName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil, err
			}
			return nil, errors.NewInternalError(err)
		}
	} else if err != nil {
		return nil, errors.NewInternalError(err)
	}

	return namespace, nil
}

// getNamespaceDefaultTolerations 读取命名空间的默认容忍注解。
func (p *Plugin) getNamespaceDefaultTolerations(nsName string) ([]api.Toleration, error) {
	ns, err := p.getNamespace(nsName)
	if err != nil {
		return nil, err
	}
	return extractNSTolerations(ns, NSDefaultTolerations)
}

// getNamespaceTolerationsWhitelist 读取命名空间的容忍白名单注解。
func (p *Plugin) getNamespaceTolerationsWhitelist(nsName string) ([]api.Toleration, error) {
	ns, err := p.getNamespace(nsName)
	if err != nil {
		return nil, err
	}
	return extractNSTolerations(ns, NSWLTolerations)
}

// extractNSTolerations 从命名空间的注解中提取默认容忍或白名单。
// 注解键包括 "scheduler.alpha.kubernetes.io/defaultTolerations"
// 和 "scheduler.alpha.kubernetes.io/tolerationsWhitelist"。
// 键未设置时返回 nil；值为空时返回空列表；否则返回解析后的容忍列表。
// 区分“未设置”和“空列表”很关键：前者表示回退到集群配置，后者表示显式禁用。
func extractNSTolerations(ns *corev1.Namespace, key string) ([]api.Toleration, error) {
	// 命名空间没有任何注解。
	if len(ns.Annotations) == 0 {
		return nil, nil
	}

	// 注解键不存在。
	if _, ok := ns.Annotations[key]; !ok {
		return nil, nil
	}

	// 注解值为空，视为显式配置的空列表。
	if len(ns.Annotations[key]) == 0 {
		return []api.Toleration{}, nil
	}

	var v1Tolerations []corev1.Toleration
	err := json.Unmarshal([]byte(ns.Annotations[key]), &v1Tolerations)
	if err != nil {
		return nil, err
	}

	ts := make([]api.Toleration, len(v1Tolerations))
	for i := range v1Tolerations {
		if err := k8s_api_v1.Convert_v1_Toleration_To_core_Toleration(&v1Tolerations[i], &ts[i], nil); err != nil {
			return nil, err
		}
	}

	return ts, nil
}
