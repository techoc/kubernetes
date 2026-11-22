/*
Copyright 2019 The Kubernetes Authors.

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

package runtime

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
	fwk "k8s.io/kube-scheduler/framework"
	plfeature "k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"sigs.k8s.io/yaml"
)

// PluginFactory 是构建插件的工厂函数。
type PluginFactory = func(ctx context.Context, configuration runtime.Object, f fwk.Handle) (fwk.Plugin, error)

// PluginFactoryWithFts 是带特性门控参数构建插件的工厂函数。
// 泛型参数限定返回值必须实现 fwk.Plugin，便于类型安全地使用具体插件实例。
type PluginFactoryWithFts[T fwk.Plugin] func(context.Context, runtime.Object, fwk.Handle, plfeature.Features) (T, error)

// FactoryAdapter 将带特性门控的工厂适配为旧的 PluginFactory 签名。
// 当调用方只接受 PluginFactory、而插件实现又依赖特性门控时，用它注入 fts。
func FactoryAdapter[T fwk.Plugin](fts plfeature.Features, withFts PluginFactoryWithFts[T]) PluginFactory {
	return func(ctx context.Context, plArgs runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
		return withFts(ctx, plArgs, fh, fts)
	}
}

// DecodeInto 将 *runtime.Unknown 类型的配置按 ContentType 反序列化到 into 中。
// obj 为 nil 或 Raw 为空时直接返回，不做任何事。
func DecodeInto(obj runtime.Object, into interface{}) error {
	if obj == nil {
		return nil
	}
	configuration, ok := obj.(*runtime.Unknown)
	if !ok {
		return fmt.Errorf("want args of type runtime.Unknown, got %T", obj)
	}
	if configuration.Raw == nil {
		return nil
	}

	switch configuration.ContentType {
	// ContentType 为空时按 JSON 处理，兼容未显式设置该字段的旧配置。
	case runtime.ContentTypeJSON, "":
		return json.Unmarshal(configuration.Raw, into)
	case runtime.ContentTypeYAML:
		return yaml.Unmarshal(configuration.Raw, into)
	default:
		return fmt.Errorf("not supported content type %s", configuration.ContentType)
	}
}

// Registry 是所有可用插件的集合，框架据此查找并初始化配置中启用的插件。
// 所有插件必须在框架初始化之前注册完毕。
type Registry map[string]PluginFactory

// Register 向注册表中添加一个插件。同名插件已存在时返回错误，
// 避免静默覆盖导致配置行为出乎意料。
func (r Registry) Register(name string, factory PluginFactory) error {
	if _, ok := r[name]; ok {
		return fmt.Errorf("a plugin named %v already exists", name)
	}
	r[name] = factory
	return nil
}

// Unregister 从注册表中移除指定插件。插件不存在时返回错误，
// 让调用方及时发现名字拼错或重复注销。
func (r Registry) Unregister(name string) error {
	if _, ok := r[name]; !ok {
		return fmt.Errorf("no plugin named %v exists", name)
	}
	delete(r, name)
	return nil
}

// Merge 将另一个注册表中的所有插件合并到当前注册表。
// 遇到重名会直接返回错误，整个合并过程不保证原子性。
func (r Registry) Merge(in Registry) error {
	for name, factory := range in {
		if err := r.Register(name, factory); err != nil {
			return err
		}
	}
	return nil
}
