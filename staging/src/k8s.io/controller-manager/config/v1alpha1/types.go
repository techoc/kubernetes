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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	componentbaseconfigv1alpha1 "k8s.io/component-base/config/v1alpha1"
)

// GenericControllerManagerConfiguration 保存通用 controller-manager 的配置。
// v1alpha1 版本的通用 controller-manager 配置；
// 与内部版本字段基本一致，连接与选举配置使用 component-base 的 v1alpha1 类型。
type GenericControllerManagerConfiguration struct {
	// port 是 controller-manager HTTP 服务的监听端口。
	Port int32
	// address 是提供服务的 IP 地址（设为 0.0.0.0 表示监听所有网卡）。
	Address string
	// minResyncPeriod 是 reflector 重同步周期的下限；实际值在 minResyncPeriod 与 2*minResyncPeriod 之间随机。
	MinResyncPeriod metav1.Duration
	// ClientConnection 指定与 apiserver 通信时使用的 kubeconfig 文件与客户端连接设置（QPS、Burst 等）。
	ClientConnection componentbaseconfigv1alpha1.ClientConnectionConfiguration
	// ControllerStartInterval 是启动各控制器管理器之间的等待时长。
	ControllerStartInterval metav1.Duration
	// leaderElection 定义领导者选举客户端的配置。
	LeaderElection componentbaseconfigv1alpha1.LeaderElectionConfiguration
	// Controllers 是要启用或禁用的控制器列表：
	// '*' 表示所有默认启用的控制器，'foo' 表示启用 'foo'，'-foo' 表示禁用 'foo'；
	// 同一个名字以第一条匹配的规则为准，语义与内部版本相同。
	Controllers []string
	// Debugging 保存 Debugging 相关功能（调试）的配置。
	Debugging componentbaseconfigv1alpha1.DebuggingConfiguration
	// LeaderMigrationEnabled 表示是否为该 controller manager 启用领导者迁移。
	LeaderMigrationEnabled bool
	// LeaderMigration 保存领导者迁移的详细配置。
	LeaderMigration LeaderMigrationConfiguration
}

// LeaderMigrationConfiguration 提供所有迁移中领导者锁的版本化配置。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// v1alpha1 版本的迁移配置，保留了 ResourceLock 字段（v1 中已移除）。
type LeaderMigrationConfiguration struct {
	metav1.TypeMeta `json:""`

	// LeaderName 是保护迁移过程的 leader election 资源名，
	// 例如 1-20-KCM-to-1-21-CCM。
	LeaderName string `json:"leaderName"`

	// ResourceLock 指定用来加锁的资源对象类型，应为 "leases" 或 "endpoints"。
	// 锁类型，v1alpha1/v1beta1 允许 leases 或 endpoints。
	ResourceLock string `json:"resourceLock"`

	// ControllerLeaders 列出各迁移 leader lock 的配置。
	ControllerLeaders []ControllerLeaderConfiguration `json:"controllerLeaders"`
}

// ControllerLeaderConfiguration 描述一个迁移中 leader lock 的配置。
type ControllerLeaderConfiguration struct {
	// Name 是被迁移 controller 的名字，
	// 例如 service-controller、route-controller、cloud-node-controller 等。
	Name string `json:"name"`

	// Component 指定该 controller 应运行的组件，
	// 例如 kube-controller-manager、cloud-controller-manager 等。
	// 取 '*' 时表示它可运行在参与迁移的任意组件下。
	Component string `json:"component"`
}
