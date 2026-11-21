/*
Copyright 2018 The Kubernetes Authors.

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

package options

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/informers"
	"k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
)

// AdmissionOptions 保存准入相关选项，是对通用 AdmissionOptions 的包装。
type AdmissionOptions struct {
	// GenericAdmission 保存通用的准入选项。
	GenericAdmission *genericoptions.AdmissionOptions
	// 已废弃的标志，应改用 EnabledAdmissionPlugins 和 DisabledAdmissionPlugins。
	// 两者互斥，同时指定会报错。
	PluginNames []string
}

// NewAdmissionOptions 创建一个新的 AdmissionOptions 实例。
// 注意：
//
//	它会调用 RegisterAllAdmissionPlugins 注册所有 kube-apiserver 准入插件，
//	并提供一组合理的 RecommendedPluginOrder，适用于不关心准入链的 server；
//	关心准入链的 server 可在创建后覆盖或追加该字段。
func NewAdmissionOptions() *AdmissionOptions {
	options := genericoptions.NewAdmissionOptions()
	// register all admission plugins
	RegisterAllAdmissionPlugins(options.Plugins)
	// set RecommendedPluginOrder
	options.RecommendedPluginOrder = AllOrderedPlugins
	// set DefaultOffPlugins
	options.DefaultOffPlugins = DefaultOffAdmissionPlugins()

	return &AdmissionOptions{
		GenericAdmission: options,
	}
}

// AddFlags 将 kube-apiserver 准入相关的标志添加到指定的 FlagSet。
func (a *AdmissionOptions) AddFlags(fs *pflag.FlagSet) {
	if a == nil {
		return
	}
	registerAllAdmissionPluginFlags(fs)
	fs.StringSliceVar(&a.PluginNames, "admission-control", a.PluginNames, ""+
		"Admission is divided into two phases. "+
		"In the first phase, only mutating admission plugins run. "+
		"In the second phase, only validating admission plugins run. "+
		"The names in the below list may represent a validating plugin, a mutating plugin, or both. "+
		"The order of plugins in which they are passed to this flag does not matter. "+
		"Comma-delimited list of: "+strings.Join(a.GenericAdmission.Plugins.Registered(), ", ")+".")
	fs.MarkDeprecated("admission-control", "Use --enable-admission-plugins or --disable-admission-plugins instead. Will be removed in a future version.")
	fs.Lookup("admission-control").Hidden = false

	a.GenericAdmission.AddFlags(fs)
}

// Validate 校验传给 kube-apiserver AdmissionOptions 的标志。
// kube-apiserver 先校验 PluginNames，再调用通用 AdmissionOptions.Validate。
func (a *AdmissionOptions) Validate() []error {
	if a == nil {
		return nil
	}
	var errs []error
	if a.PluginNames != nil &&
		(a.GenericAdmission.EnablePlugins != nil || a.GenericAdmission.DisablePlugins != nil) {
		errs = append(errs, fmt.Errorf("admission-control and enable-admission-plugins/disable-admission-plugins flags are mutually exclusive"))
	}

	registeredPlugins := sets.NewString(a.GenericAdmission.Plugins.Registered()...)
	for _, name := range a.PluginNames {
		if !registeredPlugins.Has(name) {
			errs = append(errs, fmt.Errorf("admission-control plugin %q is unknown", name))
		}
	}

	errs = append(errs, a.GenericAdmission.Validate()...)

	return errs
}

// ApplyTo 将准入链添加到 server 配置中。
// kube-apiserver 只需调用通用的 AdmissionOptions.ApplyTo。
func (a *AdmissionOptions) ApplyTo(
	c *server.Config,
	informers informers.SharedInformerFactory,
	kubeClient kubernetes.Interface,
	dynamicClient dynamic.Interface,
	features featuregate.FeatureGate,
	effectiveVersion compatibility.EffectiveVersion,
	pluginInitializers ...admission.PluginInitializer,
) error {
	if a == nil {
		return nil
	}

	if a.PluginNames != nil {
		// 将废弃的 PluginNames 转换为通用选项的启用/禁用插件列表。
		a.GenericAdmission.EnablePlugins, a.GenericAdmission.DisablePlugins = computePluginNames(a.PluginNames, a.GenericAdmission.RecommendedPluginOrder)
	}

	return a.GenericAdmission.ApplyTo(c, informers, kubeClient, dynamicClient, features, effectiveVersion, pluginInitializers...)
}

// computePluginNames 显式禁用所有不在启用列表中的插件。
func computePluginNames(explicitlyEnabled []string, all []string) (enabled []string, disabled []string) {
	return explicitlyEnabled, sets.NewString(all...).Difference(sets.NewString(explicitlyEnabled...)).List()
}
