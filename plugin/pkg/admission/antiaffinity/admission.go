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

package antiaffinity

import (
	"context"
	"fmt"
	"io"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/admission"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// PluginName 是本插件的名称。
const PluginName = "LimitPodHardAntiAffinityTopology"

// Register 向 apiserver 注册本插件的工厂函数。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewInterPodAntiAffinity(), nil
	})
}

// Plugin 是 LimitPodHardAntiAffinityTopology 准入控制器，持有准入处理所需的 handler。
type Plugin struct {
	*admission.Handler
}

var _ admission.ValidationInterface = &Plugin{}

// NewInterPodAntiAffinity 创建一个 LimitPodHardAntiAffinityTopology 准入控制器实例。
func NewInterPodAntiAffinity() *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Create, admission.Update),
	}
}

// Validate 拒绝在 requiredDuringSchedulingRequiredDuringExecution 或
// requiredDuringSchedulingIgnoredDuringExecution 中使用 v1.LabelHostname（即 "kubernetes.io/hostname"）
// 以外 topologyKey 的 Pod。
func (p *Plugin) Validate(ctx context.Context, attributes admission.Attributes, o admission.ObjectInterfaces) (err error) {
	// 子资源请求和非 Pod 资源都不在本插件管辖范围内。
	if len(attributes.GetSubresource()) != 0 || attributes.GetResource().GroupResource() != api.Resource("pods") {
		return nil
	}
	pod, ok := attributes.GetObject().(*api.Pod)
	if !ok {
		return apierrors.NewBadRequest("Resource was marked with kind Pod but was unable to be converted")
	}
	affinity := pod.Spec.Affinity
	if affinity != nil && affinity.PodAntiAffinity != nil {
		var podAntiAffinityTerms []api.PodAffinityTerm
		if len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			podAntiAffinityTerms = affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		}
		// TODO: 实现 RequiredDuringSchedulingRequiredDuringExecution 后再启用下面这段代码。
		//if len(affinity.PodAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution) != 0 {
		//        podAntiAffinityTerms = append(podAntiAffinityTerms, affinity.PodAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution...)
		//}
		for _, v := range podAntiAffinityTerms {
			if v.TopologyKey != v1.LabelHostname {
				return apierrors.NewForbidden(attributes.GetResource().GroupResource(), pod.Name, fmt.Errorf("affinity.PodAntiAffinity.RequiredDuringScheduling has TopologyKey %v but only key %v is allowed", v.TopologyKey, v1.LabelHostname))
			}
		}
	}
	return nil
}
