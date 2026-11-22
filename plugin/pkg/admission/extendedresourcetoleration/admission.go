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

// Package extendedresourcetoleration 提供 ExtendedResourceToleration 准入控制器：
// 为请求扩展资源的 Pod 自动添加对应的 toleration。
package extendedresourcetoleration

import (
	"context"
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
)

// PluginName 是本准入插件的名称。
const PluginName = "ExtendedResourceToleration"

// Register 由 apiserver 调用，注册本插件的工厂函数。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return newExtendedResourceToleration(), nil
	})
}

// newExtendedResourceToleration 创建一个 ExtendedResourceToleration 准入控制器实例。
func newExtendedResourceToleration() *plugin {
	return &plugin{
		Handler: admission.NewHandler(admission.Create, admission.Update),
	}
}

// 编译期断言：plugin 实现 MutationInterface。
var _ admission.MutationInterface = &plugin{}

// plugin 是 ExtendedResourceToleration 准入控制器的内部实现。
type plugin struct {
	*admission.Handler
}

// Admit 根据 Pod 请求的资源更新其 toleration。
// 若请求了名为 "example.com/device" 的扩展资源，则添加 key 为 "example.com/device"、
// operator 为 "Exists"、effect 为 "NoSchedule" 的 toleration。
// 设计理由见：https://github.com/kubernetes/kubernetes/issues/55080
func (p *plugin) Admit(ctx context.Context, attributes admission.Attributes, o admission.ObjectInterfaces) error {
	// 子资源请求和非 Pod 资源都不在本插件管辖范围内。
	if len(attributes.GetSubresource()) != 0 || attributes.GetResource().GroupResource() != core.Resource("pods") {
		return nil
	}

	pod, ok := attributes.GetObject().(*core.Pod)
	if !ok {
		return errors.NewBadRequest(fmt.Sprintf("expected *core.Pod but got %T", attributes.GetObject()))
	}

	resources := sets.String{}
	for _, container := range pod.Spec.Containers {
		for resourceName := range container.Resources.Requests {
			if helper.IsExtendedResourceName(resourceName) {
				resources.Insert(string(resourceName))
			}
		}
	}
	for _, container := range pod.Spec.InitContainers {
		for resourceName := range container.Resources.Requests {
			if helper.IsExtendedResourceName(resourceName) {
				resources.Insert(string(resourceName))
			}
		}
	}

	// 用 .List() 取得稳定有序的资源名，保证多扩展资源场景下添加 toleration 的顺序可预测、可测试。
	for _, resource := range resources.List() {
		helper.AddOrUpdateTolerationInPod(pod, &core.Toleration{
			Key:      resource,
			Operator: core.TolerationOpExists,
			Effect:   core.TaintEffectNoSchedule,
		})
	}

	return nil
}
