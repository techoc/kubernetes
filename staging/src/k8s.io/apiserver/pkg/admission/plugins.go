/*
Copyright 2014 The Kubernetes Authors.

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

package admission

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"

	"k8s.io/klog/v2"
)

// Factory is a function that returns an Interface for admission decisions.
// The config parameter provides an io.Reader handler to the factory in
// order to load specific configurations. If no configuration is provided
// the parameter is nil.
type Factory func(config io.Reader) (Interface, error)

// Plugins 维护准入插件名到工厂的注册表；并发安全，注册与查找都在锁内完成。
type Plugins struct {
	lock     sync.Mutex
	registry map[string]Factory
}

// NewPlugins 返回空的插件注册表，registry 在首次注册时惰性初始化。
func NewPlugins() *Plugins {
	return &Plugins{}
}

// All registered admission options.
var (
	// PluginEnabledFn checks whether a plugin is enabled.  By default, if you ask about it, it's enabled.
	//
	// 该钩子由各发行版（如 kube-apiserver 的 feature gate 校验）覆盖，
	// 用于在插件创建前做外部条件检查；默认全放行以保持库的通用性。
	PluginEnabledFn = func(name string, config io.Reader) bool {
		return true
	}
)

// PluginEnabledFunc is a function type that can provide an external check on whether an admission plugin may be enabled
type PluginEnabledFunc func(name string, config io.Reader) bool

// Registered enumerates the names of all registered plugins.
//
// 返回排序后的名称列表，保证日志与错误输出稳定。
func (ps *Plugins) Registered() []string {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	keys := []string{}
	for k := range ps.registry {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Register registers a plugin Factory by name. This
// is expected to happen during app startup.
//
// 重复注册是编程错误，直接 Fatal 终止进程：静默覆盖会让实际生效的插件难以追踪。
func (ps *Plugins) Register(name string, plugin Factory) {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	if ps.registry != nil {
		_, found := ps.registry[name]
		if found {
			klog.Fatalf("Admission plugin %q was registered twice", name)
		}
	} else {
		ps.registry = map[string]Factory{}
	}

	klog.V(1).InfoS("Registered admission plugin", "plugin", name)
	ps.registry[name] = plugin
}

// getPlugin creates an instance of the named plugin.  It returns `false` if
// the name is not known. The error is returned only when the named provider was
// known but failed to initialize.  The config parameter specifies the io.Reader
// handler of the configuration file for the cloud provider, or nil for no configuration.
//
// 返回的 bool 区分“未知插件”与“已知但初始化失败”：前者由调用方决定是否报错，
// 后者一定是错误。
func (ps *Plugins) getPlugin(name string, config io.Reader) (Interface, bool, error) {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	f, found := ps.registry[name]
	if !found {
		return nil, false, nil
	}

	config1, config2, err := splitStream(config)
	if err != nil {
		return nil, true, err
	}
	if !PluginEnabledFn(name, config1) {
		return nil, true, nil
	}

	ret, err := f(config2)
	return ret, true, err
}

// splitStream reads the stream bytes and constructs two copies of it.
//
// 配置流只能读一次，但既要给 enabled 检查又要给工厂，因此先整体读入再复制两份。
// 对 typed nil（如 (*bytes.Buffer)(nil)）也做判空，避免后续 ReadAll 返回误导性错误。
func splitStream(config io.Reader) (io.Reader, io.Reader, error) {
	if config == nil || reflect.ValueOf(config).IsNil() {
		return nil, nil, nil
	}

	configBytes, err := io.ReadAll(config)
	if err != nil {
		return nil, nil, err
	}

	return bytes.NewBuffer(configBytes), bytes.NewBuffer(configBytes), nil
}

// NewFromPlugins returns an admission.Interface that will enforce admission control decisions of all
// the given plugins.
//
// 顺序即执行顺序：调用方传入的 pluginNames 顺序决定了 mutating 与 validating 链的顺序，
// 因此配置解析阶段必须保持用户声明的次序。最终包一层 reinvocation handler，
// 支持 mutating 插件在对象被后续插件修改后重新执行。
func (ps *Plugins) NewFromPlugins(pluginNames []string, configProvider ConfigProvider, pluginInitializer PluginInitializer, decorator Decorator) (Interface, error) {
	handlers := []Interface{}
	mutationPlugins := []string{}
	validationPlugins := []string{}
	for _, pluginName := range pluginNames {
		pluginConfig, err := configProvider.ConfigFor(pluginName)
		if err != nil {
			return nil, err
		}

		plugin, err := ps.InitPlugin(pluginName, pluginConfig, pluginInitializer)
		if err != nil {
			return nil, err
		}
		if plugin != nil {
			if decorator != nil {
				handlers = append(handlers, decorator.Decorate(plugin, pluginName))
			} else {
				handlers = append(handlers, plugin)
			}

			if _, ok := plugin.(MutationInterface); ok {
				mutationPlugins = append(mutationPlugins, pluginName)
			}
			if _, ok := plugin.(ValidationInterface); ok {
				validationPlugins = append(validationPlugins, pluginName)
			}
		}
	}
	if len(mutationPlugins) != 0 {
		klog.Infof("Loaded %d mutating admission controller(s) successfully in the following order: %s.", len(mutationPlugins), strings.Join(mutationPlugins, ","))
	}
	if len(validationPlugins) != 0 {
		klog.Infof("Loaded %d validating admission controller(s) successfully in the following order: %s.", len(validationPlugins), strings.Join(validationPlugins, ","))
	}
	return newReinvocationHandler(chainAdmissionHandler(handlers)), nil
}

// InitPlugin creates an instance of the named interface.
//
// 空名字表示“不启用任何插件”，返回 (nil, nil) 而非错误，便于配置层统一处理。
func (ps *Plugins) InitPlugin(name string, config io.Reader, pluginInitializer PluginInitializer) (Interface, error) {
	if name == "" {
		klog.Info("No admission plugin specified.")
		return nil, nil
	}

	plugin, found, err := ps.getPlugin(name, config)
	if err != nil {
		return nil, fmt.Errorf("couldn't init admission plugin %q: %v", name, err)
	}
	if !found {
		return nil, fmt.Errorf("unknown admission plugin: %s", name)
	}

	pluginInitializer.Initialize(plugin)
	// ensure that plugins have been properly initialized
	if err := ValidateInitialization(plugin); err != nil {
		return nil, fmt.Errorf("failed to initialize admission plugin %q: %v", name, err)
	}

	return plugin, nil
}

// ValidateInitialization will call the InitializationValidate function in each plugin if they implement
// the InitializationValidator interface.
func ValidateInitialization(plugin Interface) error {
	if validater, ok := plugin.(InitializationValidator); ok {
		err := validater.ValidateInitialization()
		if err != nil {
			return err
		}
	}
	return nil
}

// PluginInitializers 是初始化器的有序集合，按声明顺序依次注入依赖。
type PluginInitializers []PluginInitializer

// Initialize 依次调用每个初始化器；顺序保留是因为部分初始化器依赖前一个注入的结果。
func (pp PluginInitializers) Initialize(plugin Interface) {
	for _, p := range pp {
		p.Initialize(plugin)
	}
}
