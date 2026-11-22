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

package options

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilwait "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	admissionmetrics "k8s.io/apiserver/pkg/admission/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle"
	mutatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/mutating"
	validatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	mutatingwebhook "k8s.io/apiserver/pkg/admission/plugin/webhook/mutating"
	validatingwebhook "k8s.io/apiserver/pkg/admission/plugin/webhook/validating"
	apiserverapi "k8s.io/apiserver/pkg/apis/apiserver"
	apiserverapiv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	apiserverapiv1alpha1 "k8s.io/apiserver/pkg/apis/apiserver/v1alpha1"
	"k8s.io/apiserver/pkg/server"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
	"k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
)

// configScheme 是可解码准入配置文件（AdmissionConfiguration）的 scheme。
// 在 init 中注册内置与 v1/v1alpha1 版本，保证配置文件可以带版本前缀。
var configScheme = runtime.NewScheme()

func init() {
	utilruntime.Must(apiserverapi.AddToScheme(configScheme))
	utilruntime.Must(apiserverapiv1alpha1.AddToScheme(configScheme))
	utilruntime.Must(apiserverapiv1.AddToScheme(configScheme))
}

// AdmissionOptions holds the admission options
//
// AdmissionOptions 汇总准入链的注册表、推荐顺序、默认关闭集合以及
// 命令行/配置文件输入，供 ApplyTo 构建最终的准入链。
type AdmissionOptions struct {
	// RecommendedPluginOrder holds an ordered list of plugin names we recommend to use by default
	//
	// 推荐顺序是所有插件都必须出现且仅出现一次的权威顺序；实际执行
	// 顺序始终以此为准，与 --enable/--disable 标志中的书写顺序无关。
	RecommendedPluginOrder []string
	// DefaultOffPlugins is a set of plugin names that is disabled by default
	DefaultOffPlugins sets.Set[string]

	// EnablePlugins indicates plugins to be enabled passed through `--enable-admission-plugins`.
	EnablePlugins []string
	// DisablePlugins indicates plugins to be disabled passed through `--disable-admission-plugins`.
	DisablePlugins []string
	// ConfigFile is the file path with admission control configuration.
	ConfigFile string
	// Plugins contains all registered plugins.
	Plugins *admission.Plugins
	// Decorators is a list of admission decorator to wrap around the admission plugins
	Decorators admission.Decorators
}

// NewAdmissionOptions creates a new instance of AdmissionOptions
// Note:
//
//	In addition it calls RegisterAllAdmissionPlugins to register
//	all generic admission plugins.
//
//	Provides the list of RecommendedPluginOrder that holds sane values
//	that can be used by servers that don't care about admission chain.
//	Servers that do care can overwrite/append that field after creation.
//
// NewAdmissionOptions 构造默认选项：注册全部通用插件，并带上一层
// admissionmetrics 装饰器。默认顺序把所有变更型插件放在校验型之前，
// 因为 apiserver 总是先完成全部变更再执行校验。
func NewAdmissionOptions() *AdmissionOptions {
	options := &AdmissionOptions{
		Plugins:    admission.NewPlugins(),
		Decorators: admission.Decorators{admission.DecoratorFunc(admissionmetrics.WithControllerMetrics)},
		// This list is mix of mutating admission plugins and validating
		// admission plugins. The apiserver always runs the validating ones
		// after all the mutating ones, so their relative order in this list
		// doesn't matter.
		RecommendedPluginOrder: []string{lifecycle.PluginName, mutatingadmissionpolicy.PluginName, mutatingwebhook.PluginName, validatingadmissionpolicy.PluginName, validatingwebhook.PluginName},
		DefaultOffPlugins:      sets.Set[string]{},
	}
	server.RegisterAllAdmissionPlugins(options.Plugins)
	return options
}

// AddFlags adds flags related to admission for a specific APIServer to the specified FlagSet
//
// AddFlags 把启用/禁用插件与配置文件路径暴露为命令行参数。帮助文本中
// 动态列出默认启用插件与全部已注册插件，避免文档与代码漂移。
func (a *AdmissionOptions) AddFlags(fs *pflag.FlagSet) {
	if a == nil {
		return
	}

	fs.StringSliceVar(&a.EnablePlugins, "enable-admission-plugins", a.EnablePlugins, ""+
		"admission plugins that should be enabled in addition to default enabled ones ("+
		strings.Join(a.defaultEnabledPluginNames(), ", ")+"). "+
		"Comma-delimited list of admission plugins: "+strings.Join(a.Plugins.Registered(), ", ")+". "+
		"The order of plugins in this flag does not matter.")
	fs.StringSliceVar(&a.DisablePlugins, "disable-admission-plugins", a.DisablePlugins, ""+
		"admission plugins that should be disabled although they are in the default enabled plugins list ("+
		strings.Join(a.defaultEnabledPluginNames(), ", ")+"). "+
		"Comma-delimited list of admission plugins: "+strings.Join(a.Plugins.Registered(), ", ")+". "+
		"The order of plugins in this flag does not matter.")
	fs.StringVar(&a.ConfigFile, "admission-control-config-file", a.ConfigFile,
		"File with admission control configuration.")
}

// ApplyTo adds the admission chain to the server configuration.
// In case admission plugin names were not provided by a cluster-admin they will be prepared from the recommended/default values.
// In addition the method lazily initializes a generic plugin that is appended to the list of pluginInitializers
// note this method uses:
//
//	genericconfig.Authorizer
//
// ApplyTo 解析配置文件、构建初始化器链并创建准入链，最后写入
// c.AdmissionControl。准入插件依赖 core API 的 informer、kubeClient 与
// dynamicClient，因此这里显式校验非 nil，否则延迟到运行时才失败会
// 非常难排查。
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

	// Admission depends on CoreAPI to set SharedInformerFactory and ClientConfig.
	if informers == nil {
		return fmt.Errorf("admission depends on a Kubernetes core API shared informer, it cannot be nil")
	}
	if kubeClient == nil || dynamicClient == nil {
		return fmt.Errorf("admission depends on a Kubernetes core API client, it cannot be nil")
	}

	pluginNames := a.enabledPluginNames()

	pluginsConfigProvider, err := admission.ReadAdmissionConfiguration(pluginNames, a.ConfigFile, configScheme)
	if err != nil {
		return fmt.Errorf("failed to read plugin config: %v", err)
	}

	// RESTMapper 使用内存缓存发现结果，并由 post-start hook 周期刷新：
	// CRD 可能在 apiserver 启动后才注册，静态映射会缺少这些资源。
	discoveryClient := cacheddiscovery.NewMemCacheClient(kubeClient.Discovery())
	discoveryRESTMapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryClient)
	genericInitializer := initializer.New(kubeClient, dynamicClient, informers, c.Authorization.Authorizer, features,
		effectiveVersion, c.DrainedNotify(), discoveryRESTMapper)
	initializersChain := admission.PluginInitializers{initializer.NewAPIServerIDInitializer(c.APIServerID), genericInitializer}
	initializersChain = append(initializersChain, pluginInitializers...)

	// 启动后定期刷新发现缓存，使策略中的参数与匹配规则跟随集群
	// API 的增删而变化。
	admissionPostStartHook := func(hookContext server.PostStartHookContext) error {
		discoveryRESTMapper.Reset()
		go utilwait.Until(discoveryRESTMapper.Reset, 30*time.Second, hookContext.Done())
		return nil
	}

	err = c.AddPostStartHook("start-apiserver-admission-initializer", admissionPostStartHook)
	if err != nil {
		return fmt.Errorf("failed to add post start hook for policy admission: %w", err)
	}

	admissionChain, err := a.Plugins.NewFromPlugins(pluginNames, pluginsConfigProvider, initializersChain, a.Decorators)
	if err != nil {
		return err
	}

	// 在链外层再加步骤级指标，覆盖每个插件的耗时，而不仅是单个 webhook。
	c.AdmissionControl = admissionmetrics.WithStepMetrics(admissionChain)
	return nil
}

// Validate verifies flags passed to AdmissionOptions.
//
// Validate 检查启用/禁用列表中的插件名均已注册、两者无交集，并校验
// 推荐顺序与实际注册集合完全一致（后者属于开发者错误，正常部署不会
// 触发）。
func (a *AdmissionOptions) Validate() []error {
	if a == nil {
		return nil
	}

	errs := []error{}

	registeredPlugins := sets.NewString(a.Plugins.Registered()...)
	for _, name := range a.EnablePlugins {
		if !registeredPlugins.Has(name) {
			errs = append(errs, fmt.Errorf("enable-admission-plugins plugin %q is unknown", name))
		}
	}

	for _, name := range a.DisablePlugins {
		if !registeredPlugins.Has(name) {
			errs = append(errs, fmt.Errorf("disable-admission-plugins plugin %q is unknown", name))
		}
	}

	enablePlugins := sets.NewString(a.EnablePlugins...)
	disablePlugins := sets.NewString(a.DisablePlugins...)
	if len(enablePlugins.Intersection(disablePlugins).List()) > 0 {
		errs = append(errs, fmt.Errorf("%v in enable-admission-plugins and disable-admission-plugins "+
			"overlapped", enablePlugins.Intersection(disablePlugins).List()))
	}

	// Verify RecommendedPluginOrder.
	recommendPlugins := sets.NewString(a.RecommendedPluginOrder...)
	intersections := registeredPlugins.Intersection(recommendPlugins)
	if !intersections.Equal(recommendPlugins) {
		// Developer error, this should never run in.
		errs = append(errs, fmt.Errorf("plugins %v in RecommendedPluginOrder are not registered",
			recommendPlugins.Difference(intersections).List()))
	}
	if !intersections.Equal(registeredPlugins) {
		// Developer error, this should never run in.
		errs = append(errs, fmt.Errorf("plugins %v registered are not in RecommendedPluginOrder",
			registeredPlugins.Difference(intersections).List()))
	}

	return errs
}

// enabledPluginNames makes use of RecommendedPluginOrder, DefaultOffPlugins,
// EnablePlugins, DisablePlugins fields
// to prepare a list of ordered plugin names that are enabled.
//
// enabledPluginNames 的计算规则：默认关闭集合与 --disable 取并集，再
// 减去 --enable，即显式启用优先于默认关闭；最终顺序严格取自推荐顺序。
func (a *AdmissionOptions) enabledPluginNames() []string {
	allOffPlugins := append(sets.List[string](a.DefaultOffPlugins), a.DisablePlugins...)
	disabledPlugins := sets.NewString(allOffPlugins...)
	enabledPlugins := sets.NewString(a.EnablePlugins...)
	disabledPlugins = disabledPlugins.Difference(enabledPlugins)

	orderedPlugins := []string{}
	for _, plugin := range a.RecommendedPluginOrder {
		if !disabledPlugins.Has(plugin) {
			orderedPlugins = append(orderedPlugins, plugin)
		}
	}

	return orderedPlugins
}

// Return names of plugins which are enabled by default
//
// defaultEnabledPluginNames 返回推荐顺序中未被默认关闭的插件，仅用于
// 生成帮助文本。
func (a *AdmissionOptions) defaultEnabledPluginNames() []string {
	defaultOnPluginNames := []string{}
	for _, pluginName := range a.RecommendedPluginOrder {
		if !a.DefaultOffPlugins.Has(pluginName) {
			defaultOnPluginNames = append(defaultOnPluginNames, pluginName)
		}
	}

	return defaultOnPluginNames
}
