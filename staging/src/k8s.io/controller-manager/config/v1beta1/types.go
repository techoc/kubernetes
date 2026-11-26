/*
Copyright 2021 The Kubernetes Authors.

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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LeaderMigrationConfiguration 为所有迁移中的 leader lock 提供版本化配置。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// v1beta1 版本的迁移配置；与 v1alpha1 类似，仍保留 ResourceLock 字段。
type LeaderMigrationConfiguration struct {
	metav1.TypeMeta `json:""`

	// LeaderName 是保护迁移过程的 leader election 资源名，
	// 例如 1-20-KCM-to-1-21-CCM。
	LeaderName string `json:"leaderName"`

	// ResourceLock 指定用于加锁的资源类型，
	// 合法值为 "leases" 或 "endpoints"。
	// 锁类型，合法值为 leases 或 endpoints。
	ResourceLock string `json:"resourceLock"`

	// ControllerLeaders 列出各迁移 leader lock 的配置。
	// 列表整体替换，不做逐项合并。
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
