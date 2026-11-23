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

// Package restartallcontainers 实现“重启全部容器”这一节点声明特性：
// 检查 Pod 是否请求了整Pod重启，并推断节点是否具备相应运行时能力。
package restartallcontainers

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期断言把“方法签名是否还符合 Feature 接口”的检查前移到本包编译阶段，
// 不必等到框架注册该 feature 时才报错。
var _ types.Feature = &restartAllContainersFeature{}

const (
	// RestartRulesFeatureGate 是容器重启规则的总开关名，这里保留作为背景信息：
	// 本特性依赖其子能力开关，而非这个总开关，避免误报能力。
	RestartRulesFeatureGate = "ContainerRestartRules"
	// RestartAllContainersOnContainerExits 是“容器退出时重启全部容器”的开关名，
	// 同时作为本特性对外声明的名字。
	RestartAllContainersOnContainerExits = "RestartAllContainersOnContainerExits"
)

// Feature 是“容器退出时重启全部容器”特性的实现单例。
// 单例指针供框架注册使用：feature 本身无状态，重复注册拿到的是同一份声明，
// 也避免各处各自构造导致 Name 与 Requirements 不一致。
var Feature = &restartAllContainersFeature{}

// restartAllContainersFeature 为空结构体，判断所需数据都从入参读取。
type restartAllContainersFeature struct{}

// Name 返回对外声明的特性名，与 ContainerRestartRules 子开关一致。
func (f *restartAllContainersFeature) Name() string {
	return RestartAllContainersOnContainerExits
}

// Discover 根据节点 feature gate 判断能力是否已启用：
// 名字里带 RestartRules 的 gate 是上层总开关，真正对外声明的能力只看
// RestartAllContainersOnContainerExits，避免把子能力误报为已启用。
func (f *restartAllContainersFeature) Discover(cfg *types.NodeConfiguration) bool {
	return cfg.FeatureGates.Enabled(RestartAllContainersOnContainerExits)
}

// Requirements 声明该特性的必要前提：只需打开
// RestartAllContainersOnContainerExits 这个特性门。
func (f *restartAllContainersFeature) Requirements() *types.FeatureRequirements {
	return &types.FeatureRequirements{
		EnabledFeatureGates: []string{RestartAllContainersOnContainerExits},
	}
}

// InferForScheduling 扫描 Pod 的普通容器与 init 容器：
// 调度前必须检查两侧，任一容器的 RestartPolicyRules 中声明了
// RestartAllContainers 动作，该 Pod 就要求节点支持“重启全部容器”语义。
func (f *restartAllContainersFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	for _, c := range podInfo.Spec.Containers {
		for _, rule := range c.RestartPolicyRules {
			if rule.Action == v1.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
	}
	for _, c := range podInfo.Spec.InitContainers {
		for _, rule := range c.RestartPolicyRules {
			if rule.Action == v1.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
	}
	return false
}

// InferForUpdate 恒返回 false：container.restartPolicy 与
// container.restartPolicyRules 都是不可变字段，更新时无需比对新旧对象；
// 该需求只可能在调度阶段从 PodSpec 中产生。
func (f *restartAllContainersFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	return false
}

// MaxVersion 返回 nil，表示没有版本上界：该能力仍在演进，
// 不能因为集群版本较高就停止声明。
func (f *restartAllContainersFeature) MaxVersion() *version.Version {
	return nil
}
