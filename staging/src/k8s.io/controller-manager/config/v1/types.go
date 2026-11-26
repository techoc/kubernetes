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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LeaderMigrationConfiguration 为所有迁移中的 leader lock 提供版本化配置。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// v1 版本的领导者迁移配置。
// 相比内部版本，这里没有 ResourceLock 字段——v1 起锁类型固定为 leases。
type LeaderMigrationConfiguration struct {
	// 内嵌类型元信息，用于反序列化时识别 apiVersion / kind。
	metav1.TypeMeta `json:""`

	// LeaderName 是保护迁移过程的 leader election 资源名，
	// 例如 1-20-KCM-to-1-21-CCM。
	LeaderName string `json:"leaderName"`

	// ControllerLeaders 列出各迁移 leader lock 的配置。
	// listType=atomic 告诉序列化/合并逻辑该列表应作为整体替换，而不是逐项合并。
	// +listType=atomic
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
