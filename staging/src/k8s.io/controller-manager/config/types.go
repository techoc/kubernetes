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

// Package config should only include generic configurations
// 本包只定义通用配置结构；具体 controller-manager 的专有配置应放在各自组件中。
package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	componentbaseconfig "k8s.io/component-base/config"
)

// GenericControllerManagerConfiguration holds configuration for a generic controller-manager
// 所有 controller-manager 共享的基础配置。
// 注意字段含义与命令行选项一一对应，应用选项时由 options 包填充。
type GenericControllerManagerConfiguration struct {
	// port is the port that the controller-manager's http service runs on.
	// HTTP 服务监听端口。
	Port int32
	// address is the IP address to serve on (set to 0.0.0.0 for all interfaces).
	// HTTP 服务监听地址；设为 0.0.0.0 表示监听所有网卡。
	Address string
	// minResyncPeriod is the resync period in reflectors; will be random between
	// minResyncPeriod and 2*minResyncPeriod.
	// reflector 的最小 resync 周期；实际值在 [MinResyncPeriod, 2*MinResyncPeriod) 间随机。
	MinResyncPeriod metav1.Duration
	// ClientConnection specifies the kubeconfig file and client connection
	// settings for the proxy server to use when communicating with the apiserver.
	// 与 apiserver 通信所用的 kubeconfig 与连接参数（QPS、Burst、ContentType 等）。
	ClientConnection componentbaseconfig.ClientConnectionConfiguration
	// How long to wait between starting controller managers
	// 逐个启动控制器之间的间隔，用于平滑启动、降低瞬时压力。
	ControllerStartInterval metav1.Duration
	// leaderElection defines the configuration of leader election client.
	// 领导者选举客户端配置：是否开启、使用的资源锁类型、租约时长等。
	LeaderElection componentbaseconfig.LeaderElectionConfiguration
	// Controllers is the list of controllers to enable or disable
	// '*' means "all enabled by default controllers"
	// 'foo' means "enable 'foo'"
	// '-foo' means "disable 'foo'"
	// first item for a particular name wins
	// 要启用/禁用的控制器列表：'*' 表示所有默认启用的，'foo' 启用，'-foo' 禁用；
	// 同一个名字以第一条规则为准。
	Controllers []string
	// DebuggingConfiguration holds configuration for Debugging related features.
	// 调试相关配置（profiling、锁竞争分析）。
	Debugging componentbaseconfig.DebuggingConfiguration
	// LeaderMigrationEnabled indicates whether Leader Migration should be enabled for the controller manager.
	// 是否开启领导者迁移。
	LeaderMigrationEnabled bool
	// LeaderMigration holds the configuration for Leader Migration.
	// 领导者迁移的详细配置：迁移锁名称、锁类型以及各控制器的归属。
	LeaderMigration LeaderMigrationConfiguration
}

// LeaderMigrationConfiguration provides versioned configuration for all migrating leader locks.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// 领导者迁移配置的“内部版本”，由外部版本（v1/v1beta1/v1alpha1）转换而来。
// +k8s:deepcopy-gen:interfaces 标记让生成器为其生成 runtime.Object 所需的 DeepCopyObject 方法。
type LeaderMigrationConfiguration struct {
	metav1.TypeMeta

	// LeaderName is the name of the leader election resource that protects the migration
	// E.g. 1-20-KCM-to-1-21-CCM
	// 保护迁移过程的领导者选举资源名称，例如 1-20-KCM-to-1-21-CCM。
	LeaderName string

	// ResourceLock indicates the resource object type that will be used to lock
	// Should be "leases" or "endpoints"
	// 用作锁的资源类型，合法值为 "leases" 或 "endpoints"。
	ResourceLock string

	// ControllerLeaders contains a list of migrating leader lock configurations
	// 参与迁移的各个控制器的锁配置列表。
	ControllerLeaders []ControllerLeaderConfiguration
}

// ControllerLeaderConfiguration provides the configuration for a migrating leader lock.
// 描述一个正在迁移的控制器应由哪个组件负责运行。
type ControllerLeaderConfiguration struct {
	// Name is the name of the controller being migrated
	// E.g. service-controller, route-controller, cloud-node-controller, etc
	// 即控制器的规范名称，用于与 IsControllerEnabled / FilterFunc 中的名字对应。
	Name string

	// Component is the name of the component in which the controller should be running.
	// E.g. kube-controller-manager, cloud-controller-manager, etc
	// Or '*' meaning the controller can be run under any component that participates in the migration
	// "*" 表示该控制器可由参与迁移的任一组件运行，迁移期间两边都允许接管。
	Component string
}
