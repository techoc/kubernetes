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

package framework

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubeschedulerconfigv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

var configDecoder = scheme.Codecs.UniversalDecoder()

// NewFramework 根据注册函数与选项创建 Framework。
func NewFramework(ctx context.Context, fns []RegisterPluginFunc, profileName string, opts ...runtime.Option) (framework.Framework, error) {
	registry := runtime.Registry{}
	profile := &schedulerapi.KubeSchedulerProfile{
		SchedulerName: profileName,
		Plugins:       &schedulerapi.Plugins{},
	}
	for _, f := range fns {
		f(&registry, profile)
	}
	return runtime.NewFramework(ctx, registry, profile, opts...)
}

// RegisterPluginFunc 是 RegisterFilterPlugin() 等函数使用的函数签名，
// 用于把插件注册到给定的 registry。
type RegisterPluginFunc func(reg *runtime.Registry, profile *schedulerapi.KubeSchedulerProfile)

// RegisterQueueSortPlugin 返回一个函数，用于向给定 registry 注册 QueueSort 插件。
func RegisterQueueSortPlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "QueueSort")
}

// RegisterPreFilterPlugin 返回一个函数，用于向给定 registry 注册 PreFilter 插件。
func RegisterPreFilterPlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "PreFilter")
}

// RegisterFilterPlugin 返回一个函数，用于向给定 registry 注册 Filter 插件。
func RegisterFilterPlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "Filter")
}

// RegisterPostFilterPlugin 返回一个函数，用于向给定 registry 注册 PostFilter 插件。
func RegisterPostFilterPlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "PostFilter")
}

// RegisterPodGroupPostFilterPlugin 返回一个函数，用于向给定 registry 注册 PodGroupPostFilter 插件。
func RegisterPodGroupPostFilterPlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "PodGroupPostFilter")
}

// RegisterReservePlugin 返回一个函数，用于向给定 registry 注册 Reserve 插件。
func RegisterReservePlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "Reserve")
}

// RegisterPermitPlugin 返回一个函数，用于向给定 registry 注册 Permit 插件。
func RegisterPermitPlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "Permit")
}

// RegisterPreBindPlugin 返回一个函数，用于向给定 registry 注册 PreBind 插件。
func RegisterPreBindPlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "PreBind")
}

// RegisterScorePlugin 返回一个函数，用于向给定 registry 注册带权重的 Score 插件。
func RegisterScorePlugin(pluginName string, pluginNewFunc runtime.PluginFactory, weight int32) RegisterPluginFunc {
	return RegisterPluginAsExtensionsWithWeight(pluginName, weight, pluginNewFunc, "Score")
}

// RegisterPreScorePlugin 返回一个函数，用于向给定 registry 注册 PreScore 插件。
func RegisterPreScorePlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "PreScore")
}

// RegisterBindPlugin 返回一个函数，用于向给定 registry 注册 Bind 插件。
func RegisterBindPlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "Bind")
}

// RegisterPlacementGeneratePlugin 返回一个函数，用于向给定 registry 注册 PlacementGenerate 插件。
func RegisterPlacementGeneratePlugin(pluginName string, pluginNewFunc runtime.PluginFactory) RegisterPluginFunc {
	return RegisterPluginAsExtensions(pluginName, pluginNewFunc, "PlacementGenerate")
}

// RegisterPlacementScorePlugin 返回一个函数，用于向给定 registry 注册带权重的 PlacementScore 插件。
func RegisterPlacementScorePlugin(pluginName string, pluginNewFunc runtime.PluginFactory, weight int32) RegisterPluginFunc {
	return RegisterPluginAsExtensionsWithWeight(pluginName, weight, pluginNewFunc, "PlacementScore")
}

// RegisterPluginAsExtensions 返回一个函数，把插件按默认权重 1 注册到给定的多个扩展点。
func RegisterPluginAsExtensions(pluginName string, pluginNewFunc runtime.PluginFactory, extensions ...string) RegisterPluginFunc {
	return RegisterPluginAsExtensionsWithWeight(pluginName, 1, pluginNewFunc, extensions...)
}

// RegisterPluginAsExtensionsWithWeight 返回一个函数，把插件按指定权重注册到给定的多个扩展点。
func RegisterPluginAsExtensionsWithWeight(pluginName string, weight int32, pluginNewFunc runtime.PluginFactory, extensions ...string) RegisterPluginFunc {
	return func(reg *runtime.Registry, profile *schedulerapi.KubeSchedulerProfile) {
		reg.Register(pluginName, pluginNewFunc)
		for _, extension := range extensions {
			ps := getPluginSetByExtension(profile.Plugins, extension)
			if ps == nil {
				continue
			}
			ps.Enabled = append(ps.Enabled, schedulerapi.Plugin{Name: pluginName, Weight: weight})
		}
		// 使用最新配置 API 版本的默认值，确保测试 profile 与真实配置一致。
		var gvk schema.GroupVersionKind
		gvk = kubeschedulerconfigv1.SchemeGroupVersion.WithKind(pluginName + "Args")
		if args, _, err := configDecoder.Decode(nil, &gvk, nil); err == nil {
			profile.PluginConfig = append(profile.PluginConfig, schedulerapi.PluginConfig{
				Name: pluginName,
				Args: args,
			})
		}
	}
}

// getPluginSetByExtension 按扩展点名称返回 profile 中对应的插件集合；未知扩展点返回 nil。
func getPluginSetByExtension(plugins *schedulerapi.Plugins, extension string) *schedulerapi.PluginSet {
	switch extension {
	case "QueueSort":
		return &plugins.QueueSort
	case "Filter":
		return &plugins.Filter
	case "PostFilter":
		return &plugins.PostFilter
	case "PreFilter":
		return &plugins.PreFilter
	case "PreScore":
		return &plugins.PreScore
	case "Score":
		return &plugins.Score
	case "Bind":
		return &plugins.Bind
	case "Reserve":
		return &plugins.Reserve
	case "Permit":
		return &plugins.Permit
	case "PreBind":
		return &plugins.PreBind
	case "PostBind":
		return &plugins.PostBind
	case "PlacementGenerate":
		return &plugins.PlacementGenerate
	case "PlacementScore":
		return &plugins.PlacementScore
	default:
		return nil
	}
}

// BuildNodeInfos 从 v1.Node 切片构建 NodeInfo 切片。
func BuildNodeInfos(nodes []*v1.Node) []fwk.NodeInfo {
	res := make([]fwk.NodeInfo, len(nodes))
	for i := 0; i < len(nodes); i++ {
		res[i] = framework.NewNodeInfo()
		res[i].SetNode(nodes[i])
	}
	return res
}
