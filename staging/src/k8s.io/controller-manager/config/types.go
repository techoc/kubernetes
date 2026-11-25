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

// Package config 只应包含通用配置：本包只定义通用配置结构；具体 controller-manager 的专有配置应放在各自组件中。
package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	componentbaseconfig "k8s.io/component-base/config"
)

// GenericControllerManagerConfiguration 保存通用 controller-manager 的配置，是所有 controller-manager 共享的基础配置。
// 注意字段含义与命令行选项一一对应，应用选项时由 options 包填充。
type GenericControllerManagerConfiguration struct {
	// port 是 controller-manager HTTP 服务的监听端口。
	Port int32
	// address 是提供服务的 IP 地址（设为 0.0.0.0 表示监听所有网卡）。
	Address string
	// minResyncPeriod 是 reflector 的 resync 周期下限；实际值在 [minResyncPeriod, 2*minResyncPeriod) 之间随机。
	MinResyncPeriod metav1.Duration
	// ClientConnection 指定与 apiserver 通信时使用的 kubeconfig 文件与客户端连接设置（QPS、Burst、ContentType 等）。
	ClientConnection componentbaseconfig.ClientConnectionConfiguration
	// ControllerStartInterval 是启动各控制器管理器之间的等待间隔，用于平滑启动、降低瞬时压力。
	ControllerStartInterval metav1.Duration
	// leaderElection 定义领导者选举客户端的配置：是否开启、使用的资源锁类型、租约时长等。
	LeaderElection componentbaseconfig.LeaderElectionConfiguration
	// Controllers 是要启用或禁用的控制器列表：
	// '*' 表示所有默认启用的控制器，'foo' 表示启用 'foo'，'-foo' 表示禁用 'foo'；
	// 同一个名字以第一条匹配的规则为准。
	Controllers []string
	// DebuggingConfiguration 保存 Debugging 相关功能的配置（profiling、锁竞争分析）。
	Debugging componentbaseconfig.DebuggingConfiguration
	// LeaderMigrationEnabled 表示是否为该 controller-manager 启用领导者迁移。
	LeaderMigrationEnabled bool
	// LeaderMigration 保存领导者迁移的配置：迁移锁名称、锁类型以及各控制器的归属。
	LeaderMigration LeaderMigrationConfiguration
}

// LeaderMigrationConfiguration 提供所有迁移中领导者锁的版本化配置。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// 领导者迁移配置的“内部版本”，由外部版本（v1/v1beta1/v1alpha1）转换而来。
// +k8s:deepcopy-gen:interfaces 标记让生成器为其生成 runtime.Object 所需的 DeepCopyObject 方法。
type LeaderMigrationConfiguration struct {
	metav1.TypeMeta

	// LeaderName 是保护迁移过程的领导者选举资源名称，例如 1-20-KCM-to-1-21-CCM。
	LeaderName string

	// ResourceLock 指定用来加锁的资源对象类型，合法值为 "leases" 或 "endpoints"。
	ResourceLock string

	// ControllerLeaders 是参与迁移的各控制器领导者锁配置列表。
	ControllerLeaders []ControllerLeaderConfiguration
}

// ControllerLeaderConfiguration 提供一个迁移中领导者锁的配置，描述该控制器应由哪个组件负责运行。
type ControllerLeaderConfiguration struct {
	// Name 是被迁移控制器的名称，如 service-controller、route-controller、cloud-node-controller 等；
	// 即控制器的规范名称，用于与 IsControllerEnabled / FilterFunc 中的名字对应。
	Name string

	// Component 是控制器应运行的组件名称，如 kube-controller-manager、cloud-controller-manager 等；
	// 也可以是 '*'，表示该控制器可由参与迁移的任一组件运行，迁移期间两边都允许接管。
	Component string
}
