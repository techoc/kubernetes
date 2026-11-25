/*
Copyright 2020 The Kubernetes Authors.

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
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
)

// PerTestConfig 表示控制测试执行的参数。每个测试都会分配一个实例，
// 并以指针形式传给测试涉及的函数。
type PerTestConfig struct {
	// 该测试使用的测试驱动。
	Driver TestDriver

	// 一个简短字符串，作为动态生成的实体（Pod、路径）名称的第一部分，
	// 便于调试。同一测试套件中的不同测试可以使用相同值。
	Prefix string

	// 为当前测试分配的 framework 实例。
	Framework *framework.Framework

	// 非空时，使用卷的 Pod 将按 NodeSelection 调度；否则由 Kubernetes 选择节点。
	ClientNodeSelection e2epod.NodeSelection

	// 某些测试驱动会初始化存储服务器，此时必须用这里的配置运行测试；
	// 这类测试会忽略上面的取值。
	ServerConfig *e2evolume.TestConfig

	// 某些驱动运行在自己的命名空间中
	DriverNamespace *v1.Namespace
}

// GetUniqueDriverName 返回可在测试中并行使用的唯一驱动名
func (config *PerTestConfig) GetUniqueDriverName() string {
	return config.Driver.GetDriverInfo().Name + "-" + config.Framework.UniqueName
}

// ConvertTestConfig 返回一个 framework 测试配置，取值来自测试套件指定的参数，
// 或（若可用）为卷服务器动态创建的配置。
//
// 之所以这样转换，是因为 TestConfig 是 testsuites 包的公共 API，
// 而 volume.TestConfig 只是实现细节，其中包含不起作用的字段。
func ConvertTestConfig(in *PerTestConfig) e2evolume.TestConfig {
	if in.ServerConfig != nil {
		return *in.ServerConfig
	}

	return e2evolume.TestConfig{
		Namespace:           in.Framework.Namespace.Name,
		Prefix:              in.Prefix,
		ClientNodeSelection: in.ClientNodeSelection,
	}
}
