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

package validating

import (
	"context"
	"io"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/configuration"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/generic"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/manifest/source"
)

const (
	// PluginName 是准入插件注册名；必须与配置中启用的插件名一致。
	PluginName = "ValidatingAdmissionWebhook"
)

// Register 把 ValidatingAdmissionWebhook 插件注册到插件注册表。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(configFile io.Reader) (admission.Interface, error) {
		plugin, err := NewValidatingAdmissionWebhook(configFile)
		if err != nil {
			return nil, err
		}

		return plugin, nil
	})
}

// Plugin 是 admission.Interface 的校验型实现，实际逻辑复用通用 webhook 插件。
type Plugin struct {
	*generic.Webhook
}

var _ admission.ValidationInterface = &Plugin{}
var _ initializer.WantsManifestLoaders = &Plugin{}

// SetManifestLoaders 提供基于 manifest 的默认化与校验所需的加载函数。
// 未配置 manifest 加载器时静默返回，保持静态清单特性可选，
// 不影响原有 webhook 配置路径。
func (a *Plugin) SetManifestLoaders(loaders *initializer.ManifestLoaders) {
	if loaders == nil || loaders.LoadValidatingWebhookManifests == nil {
		return
	}
	loadFunc := loaders.LoadValidatingWebhookManifests
	a.Webhook.SetStaticSourceFactory(func(manifestsDir string) (generic.ReloadableSource, error) {
		src := source.NewValidatingSource(manifestsDir, a.Webhook.GetAPIServerID(), source.ValidatingWebhookLoadFunc(loadFunc))
		// 启动时立即加载一次，让配置错误在启动阶段就暴露，而不是首个请求时才失败。
		if err := src.LoadInitial(); err != nil {
			return nil, err
		}
		return src, nil
	})
}

// NewValidatingAdmissionWebhook 返回通用准入 webhook 插件实例。
func NewValidatingAdmissionWebhook(configFile io.Reader) (*Plugin, error) {
	// 校验型 webhook 只关心会改变对象或触发删除语义的操作；Connect 也纳入是因为
	// 子资源连接请求同样可能被策略审查。
	handler := admission.NewHandler(admission.Connect, admission.Create, admission.Delete, admission.Update)
	p := &Plugin{}
	var err error
	p.Webhook, err = generic.NewWebhook(handler, configFile, configuration.NewValidatingWebhookConfigurationManager, newValidatingDispatcher(p))
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Validate 基于请求属性做准入决策；实际分发由通用 webhook 逻辑完成。
func (a *Plugin) Validate(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	return a.Webhook.Dispatch(ctx, attr, o)
}
