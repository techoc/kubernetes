/*
Copyright 2025 The Kubernetes Authors.

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

package compatibility

import (
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	basecompatibility "k8s.io/component-base/compatibility"
)

// DefaultComponentGlobalsRegistry 是存储所有组件的有效版本与特性门控的全局变量，便于访问。
// 使用示例：
// // 首先注册组件的有效版本与特性门控
// wardleEffectiveVersion := basecompatibility.NewEffectiveVersion("1.2")
// wardleFeatureGate := featuregate.NewFeatureGate()
// utilruntime.Must(compatibility.DefaultComponentGlobalsRegistry.Register(apiserver.WardleComponentName, wardleEffectiveVersion, wardleFeatureGate, false))
//
//	cmd := &cobra.Command{
//	 ...
//		// 在 PersistentPreRunE 中调用 DefaultComponentGlobalsRegistry.Set()，确保解析 flag 后立即根据模拟版本设置特性门控。
//		PersistentPreRunE: func(*cobra.Command, []string) error {
//			if err := compatibility.DefaultComponentGlobalsRegistry.Set(); err != nil {
//				return err
//			}
//	 ...
//		},
//		RunE: func(c *cobra.Command, args []string) error {
//			// 在别处调用 compatibility.DefaultComponentGlobalsRegistry.Validate()
//		},
//	}
//
// flags := cmd.Flags()
// // 添加 flag
// compatibility.DefaultComponentGlobalsRegistry.AddFlags(flags)
//
// 该注册表把"组件有效版本（emulation version）"与"特性门控"绑在一起：
// 版本仿真要求特性门控按仿真版本而非二进制版本生效，因此必须由同一份注册表
// 统一驱动，否则各组件会看到互相矛盾的版本视图。
var DefaultComponentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry = basecompatibility.NewComponentGlobalsRegistry()

// init 注册 kube 默认组件，使任何导入本包的程序都能获得可用的全局注册表。
// 这里用 Must 是因为注册失败属于编程错误，应在启动阶段立即暴露而非静默降级。
func init() {
	utilruntime.Must(DefaultComponentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, DefaultBuildEffectiveVersion(), utilfeature.DefaultMutableFeatureGate))
}
