/*
Copyright 2022 The Kubernetes Authors.

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

package v1

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	v1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
)

var (
	// pluginArgConversionScheme 是注册了 internal 与 v1 类型的 scheme，
	// 用于对带类型的 PluginConfig.Args 做默认值填充和版本转换。
	// 只能通过 GetPluginArgConversionScheme() 访问，以保证延迟初始化只执行一次。
	pluginArgConversionScheme     *runtime.Scheme
	initPluginArgConversionScheme sync.Once
)

// GetPluginArgConversionScheme 返回用于插件参数转换的 scheme，首次调用时才初始化。
// 延迟初始化可以避免包初始化阶段的循环依赖，并保证并发调用安全。
func GetPluginArgConversionScheme() *runtime.Scheme {
	initPluginArgConversionScheme.Do(func() {
		// 注册 internal 与 v1 两套类型，才能跨版本转换 Args。
		pluginArgConversionScheme = runtime.NewScheme()
		utilruntime.Must(AddToScheme(pluginArgConversionScheme))
		utilruntime.Must(config.AddToScheme(pluginArgConversionScheme))
	})
	return pluginArgConversionScheme
}

// Convert_v1_KubeSchedulerConfiguration_To_config_KubeSchedulerConfiguration 将外部 v1 配置转换为内部配置。
// 自动字段转换完成后，还需手动将 PluginConfig.Args 从版本化类型转为内部类型。
func Convert_v1_KubeSchedulerConfiguration_To_config_KubeSchedulerConfiguration(in *v1.KubeSchedulerConfiguration, out *config.KubeSchedulerConfiguration, s conversion.Scope) error {
	if err := autoConvert_v1_KubeSchedulerConfiguration_To_config_KubeSchedulerConfiguration(in, out, s); err != nil {
		return err
	}
	return convertToInternalPluginConfigArgs(out)
}

// convertToInternalPluginConfigArgs 在应用默认值之后，
// 借助 scheme 将 PluginConfig.Args 转换为内部类型。
// Args 为 nil 或仍是 runtime.Unknown（未解析的原始配置）时跳过。
func convertToInternalPluginConfigArgs(out *config.KubeSchedulerConfiguration) error {
	scheme := GetPluginArgConversionScheme()
	for i := range out.Profiles {
		prof := &out.Profiles[i]
		for j := range prof.PluginConfig {
			args := prof.PluginConfig[j].Args
			if args == nil {
				continue
			}
			if _, isUnknown := args.(*runtime.Unknown); isUnknown {
				continue
			}
			internalArgs, err := scheme.ConvertToVersion(args, config.SchemeGroupVersion)
			if err != nil {
				return fmt.Errorf("converting .Profiles[%d].PluginConfig[%d].Args into internal type: %w", i, j, err)
			}
			prof.PluginConfig[j].Args = internalArgs
		}
	}
	return nil
}

// Convert_config_KubeSchedulerConfiguration_To_v1_KubeSchedulerConfiguration 将内部配置转换为外部 v1 配置。
// 自动字段转换完成后，还需手动将 PluginConfig.Args 从内部类型转回版本化类型。
func Convert_config_KubeSchedulerConfiguration_To_v1_KubeSchedulerConfiguration(in *config.KubeSchedulerConfiguration, out *v1.KubeSchedulerConfiguration, s conversion.Scope) error {
	if err := autoConvert_config_KubeSchedulerConfiguration_To_v1_KubeSchedulerConfiguration(in, out, s); err != nil {
		return err
	}
	return convertToExternalPluginConfigArgs(out)
}

// convertToExternalPluginConfigArgs 借助 scheme 将 PluginConfig.Args
// 转换为外部（带版本）类型。
// Args.Object 为 nil 或仍是 runtime.Unknown 时跳过。
func convertToExternalPluginConfigArgs(out *v1.KubeSchedulerConfiguration) error {
	scheme := GetPluginArgConversionScheme()
	for i := range out.Profiles {
		for j := range out.Profiles[i].PluginConfig {
			args := out.Profiles[i].PluginConfig[j].Args
			if args.Object == nil {
				continue
			}
			if _, isUnknown := args.Object.(*runtime.Unknown); isUnknown {
				continue
			}
			externalArgs, err := scheme.ConvertToVersion(args.Object, SchemeGroupVersion)
			if err != nil {
				return err
			}
			out.Profiles[i].PluginConfig[j].Args.Object = externalArgs
		}
	}
	return nil
}
