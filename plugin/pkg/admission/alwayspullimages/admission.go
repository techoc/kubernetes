/*
Copyright 2015 The Kubernetes Authors.

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

// Package alwayspullimages 包含一个准入控制器，强制把每个新 Pod 的镜像拉取策略改为 Always。
// 在多租户集群中，这能保证私有镜像只能被持有拉取凭证的用户使用。
// 若没有该控制器，镜像一旦被拉到节点上，任何用户的 Pod 只要知道镜像名即可使用
// （前提是 Pod 被调度到该节点），无需对镜像本身做任何鉴权。
// 启用该控制器后，启动容器前总会重新拉取镜像，因此必须提供有效凭证。
package alwayspullimages

import (
	"context"
	"io"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/pods"
)

// PluginName 是准入插件的名称。
const PluginName = "AlwaysPullImages"

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewAlwaysPullImages(), nil
	})
}

// AlwaysPullImages 实现 admission.Interface。
// 它检查所有新 Pod，并把每个容器的镜像拉取策略覆盖为 Always。
type AlwaysPullImages struct {
	*admission.Handler
}

var _ admission.MutationInterface = &AlwaysPullImages{}
var _ admission.ValidationInterface = &AlwaysPullImages{}

// Admit 根据请求属性作出准入决策，强制镜像拉取策略为 Always。
func (a *AlwaysPullImages) Admit(ctx context.Context, attributes admission.Attributes, o admission.ObjectInterfaces) (err error) {
	// 忽略子资源请求以及非 Pod 资源。
	if shouldIgnore(attributes) {
		return nil
	}
	pod, ok := attributes.GetObject().(*api.Pod)
	if !ok {
		return apierrors.NewBadRequest("Resource was marked with kind Pod but was unable to be converted")
	}

	pods.VisitContainersWithPath(&pod.Spec, field.NewPath("spec"), func(c *api.Container, _ *field.Path) bool {
		c.ImagePullPolicy = api.PullAlways
		return true
	})

	// 镜像卷同样需要强制 Always，见 https://kep.k8s.io/4639。
	for _, v := range pod.Spec.Volumes {
		if v.Image != nil {
			v.Image.PullPolicy = api.PullAlways
		}
	}

	return nil
}

// Validate 确保所有容器和镜像卷的拉取策略均为 Always。
func (*AlwaysPullImages) Validate(ctx context.Context, attributes admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if shouldIgnore(attributes) {
		return nil
	}

	pod, ok := attributes.GetObject().(*api.Pod)
	if !ok {
		return apierrors.NewBadRequest("Resource was marked with kind Pod but was unable to be converted")
	}

	var allErrs []error
	pods.VisitContainersWithPath(&pod.Spec, field.NewPath("spec"), func(c *api.Container, p *field.Path) bool {
		if c.ImagePullPolicy != api.PullAlways {
			allErrs = append(allErrs, admission.NewForbidden(attributes,
				field.NotSupported(p.Child("imagePullPolicy"), c.ImagePullPolicy, []string{string(api.PullAlways)}),
			))
		}
		return true
	})

	// 镜像卷的拉取策略也要校验，见 https://kep.k8s.io/4639。
	for i, v := range pod.Spec.Volumes {
		if v.Image != nil && v.Image.PullPolicy != api.PullAlways {
			allErrs = append(allErrs, admission.NewForbidden(attributes,
				field.NotSupported(
					field.NewPath("spec").Child("volumes").Index(i).Child("image").Child("pullPolicy"),
					v.Image.PullPolicy,
					[]string{string(api.PullAlways)},
				),
			))
		}
	}

	if len(allErrs) > 0 {
		return utilerrors.NewAggregate(allErrs)
	}

	return nil
}

// isUpdateWithNoNewImages 判断本次更新是否未引入新镜像。
func isUpdateWithNoNewImages(attributes admission.Attributes) bool {
	if attributes.GetOperation() != admission.Update {
		return false
	}

	pod, ok := attributes.GetObject().(*api.Pod)
	if !ok {
		klog.Warningf("Resource was marked with kind Pod but pod was unable to be converted.")
		return false
	}

	oldPod, ok := attributes.GetOldObject().(*api.Pod)
	if !ok {
		klog.Warningf("Resource was marked with kind Pod but old pod was unable to be converted.")
		return false
	}

	// 先收集旧 Pod 引用的镜像集合。
	oldImages := sets.NewString()
	pods.VisitContainersWithPath(&oldPod.Spec, field.NewPath("spec"), func(c *api.Container, _ *field.Path) bool {
		oldImages.Insert(c.Image)
		return true
	})

	hasNewImage := false
	// 一旦发现新镜像就停止遍历，回调返回 false 即可提前终止。
	pods.VisitContainersWithPath(&pod.Spec, field.NewPath("spec"), func(c *api.Container, _ *field.Path) bool {
		if !oldImages.Has(c.Image) {
			hasNewImage = true
		}
		return !hasNewImage
	})
	return !hasNewImage
}

// shouldIgnore 判断请求是否无需处理。
func shouldIgnore(attributes admission.Attributes) bool {
	// 忽略子资源请求以及非 Pod 资源。
	if len(attributes.GetSubresource()) != 0 || attributes.GetResource().GroupResource() != api.Resource("pods") {
		return true
	}

	// 未引入新镜像的更新无需强制拉取，避免干扰原地升级等场景。
	if isUpdateWithNoNewImages(attributes) {
		return true
	}
	return false
}

// NewAlwaysPullImages 创建一个新的强制拉取镜像准入处理器。
func NewAlwaysPullImages() *AlwaysPullImages {
	return &AlwaysPullImages{
		Handler: admission.NewHandler(admission.Create, admission.Update),
	}
}
