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

package mutating

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
	// PluginName 表示准入插件的名称。
	// 该名称同时是 --enable-admission-plugins 中使用的标识，修改会破坏兼容性。
	PluginName = "MutatingAdmissionWebhook"
)

// Register 注册一个插件。
// 注册工厂只负责构造插件；真正的初始化（客户端、informer 等）由 initializer 接口在后续阶段注入。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(configFile io.Reader) (admission.Interface, error) {
		plugin, err := NewMutatingWebhook(configFile)
		if err != nil {
			return nil, err
		}

		return plugin, nil
	})
}

// Plugin 是 admission.Interface 的一种实现。
// 通过嵌入 generic.Webhook 复用配置发现、匹配与调用逻辑，本类型只提供 mutating 特有的装配。
type Plugin struct {
	*generic.Webhook
}

var _ admission.MutationInterface = &Plugin{}
var _ initializer.WantsManifestLoaders = &Plugin{}

// SetManifestLoaders 提供用于基于 scheme 的默认化与校验的 manifest 加载函数。
// 静态配置（--admission-control-config-file 指向的 manifests 目录）模式下，用该函数
// 构造可重载的 source；loaders 为空表示未启用静态模式，直接返回保持原有行为。
func (a *Plugin) SetManifestLoaders(loaders *initializer.ManifestLoaders) {
	if loaders == nil || loaders.LoadMutatingWebhookManifests == nil {
		return
	}
	loadFunc := loaders.LoadMutatingWebhookManifests
	a.Webhook.SetStaticSourceFactory(func(manifestsDir string) (generic.ReloadableSource, error) {
		src := source.NewMutatingSource(manifestsDir, a.Webhook.GetAPIServerID(), source.MutatingWebhookLoadFunc(loadFunc))
		// 首次加载失败必须让启动失败，而不是带着空配置继续运行。
		if err := src.LoadInitial(); err != nil {
			return nil, err
		}
		return src, nil
	})
}

// NewMutatingWebhook 返回一个通用准入 webhook 插件。
// 处理 Connect/Create/Delete/Update 四类操作；mutating webhook 不参与纯粹的读取请求。
func NewMutatingWebhook(configFile io.Reader) (*Plugin, error) {
	handler := admission.NewHandler(admission.Connect, admission.Create, admission.Delete, admission.Update)
	p := &Plugin{}
	var err error
	// 注意 dispatcher 在 Webhook 尚未构造完成时就引用 p：它只捕获指针，
	// 直到真正 dispatch 时才会解引用，因此顺序是安全的。
	p.Webhook, err = generic.NewWebhook(handler, configFile, configuration.NewMutatingWebhookConfigurationManager, newMutatingDispatcher(p))
	if err != nil {
		return nil, err
	}
	return p, nil
}

// ValidateInitialization 实现 InitializationValidator 接口。
// 目前完全委托给 generic.Webhook；保留这一层是为了将来加入 mutating 特有的校验。
func (a *Plugin) ValidateInitialization() error {
	if err := a.Webhook.ValidateInitialization(); err != nil {
		return err
	}
	return nil
}

// Admit 根据请求属性做出准入决策。
// 直接转交给通用的 dispatch 流程：匹配 webhook、并发调用、按 failurePolicy 与
// reinvocationPolicy 处理结果。
func (a *Plugin) Admit(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	return a.Webhook.Dispatch(ctx, attr, o)
}
