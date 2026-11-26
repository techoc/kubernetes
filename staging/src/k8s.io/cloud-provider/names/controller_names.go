/*
Copyright 2023 The Kubernetes Authors.

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

// Package names 定义 cloud-controller-manager 各控制器的规范名称常量与历史别名。
// 控制器名会出现在命令行 flag、事件与日志中，属于对外契约，必须保持稳定。
package names

// 规范控制器名。命名约束、变更策略与使用场景见下方说明；名称一旦发布即视为 ID，
// 只能新增别名，不能删除旧别名。
//
// 命名约定
// 1. 各控制器命名保持一致；
// 2. 避免使用缩写，除非是公认的非 Kubernetes 缩写；
// 3. Kubernetes 资源名连写，不加连字符（"-"）。
//
// 变更策略
// 控制器名应视为 ID，只有在绝对必要时才可更改，例如过去选了不合适的名字，
// 或控制器的职责范围发生变化。改名时须在 CCMControllerAliases 中为旧名保留别名，
// 且保留所有既有别名，以维持向后兼容。
//
// 使用场景
// 以下位置应使用控制器名常量：
//  1. 在 app.DefaultInitFuncConstructors 或 sample main.controllerInitializers 中注册控制器时：
//     1.1. 在 app.ControllersDisabledByDefault 中默认禁用某控制器
//     1.2. 检查 IsControllerEnabled
//     1.3. 在 CCMControllerAliases 中定义别名（仅用于向后兼容）
//  2. 控制器内部任何位置：
//     2.1. [TODO] 通过 LoggerWithName 用控制器名配置 logger component
//     2.2. [TODO] 日志中引用控制器时用规范名（如 Starting X、Shutting down X）
//     2.3. [TODO] 事件中的 EventSource.Component 设为控制器名（通常在初始化 EventRecorder 时）
//     2.4. [TODO] 用 ControllerStarted 和 ControllerStopped 注册 ControllerManagerMetrics
//     2.5. [TODO] 调用 WaitForNamedCacheSync
//  3. 为 "--help" 命令或生成的文档定义控制器选项时：
//     3.1. 注册控制器选项时用控制器名创建 pflag.FlagSet（该名字会显示在控制器 flag 组标题里）
//     3.2. flag 的 help 文本提到控制器名时
//  4. 为新控制器定义新的 service account 时（旧控制器为兼容可能沿用不一致的 service account）
//  5. 本模块之外使用这些控制器的任何位置（kube-controller-manager、cloud-provider sample）
const (
	CloudNodeController          = "cloud-node-controller"           // 负责节点初始化，并补充云厂商相关的标签与地址。
	ServiceLBController          = "service-lb-controller"           // 负责为 LoadBalancer 类型 Service 创建/更新/删除云负载均衡。
	NodeRouteController          = "node-route-controller"           // 负责在云上同步节点路由（如 VPC 路由表）。
	CloudNodeLifecycleController = "cloud-node-lifecycle-controller" // 负责节点生命周期监控，如节点失联后的删除。
)

// CCMControllerAliases 返回别名到规范控制器名的映射。
//
// 这些别名用于向后兼容，绝不能删除；只有规范名变更时才允许新增别名（参见上方变更策略）。
func CCMControllerAliases() map[string]string {
	// 每次返回新的 map，防止调用方修改后影响其他使用方。
	return map[string]string{
		"cloud-node":           CloudNodeController,
		"service":              ServiceLBController,
		"route":                NodeRouteController,
		"cloud-node-lifecycle": CloudNodeLifecycleController,
	}

}
