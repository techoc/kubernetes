/*
Copyright 2017 The Kubernetes Authors.

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

// Package reconcilers 提供 apiserver 的 Endpoint Reconciler。
package reconcilers

import (
	"net"

	corev1 "k8s.io/api/core/v1"
)

// EndpointReconciler 负责协调 apiserver Service 的 endpoints。
type EndpointReconciler interface {
	// ValidateIP 确保给定 IP 满足该 reconciler 类型成功调用 ReconcileEndpoints 的要求。
	ValidateIP(ip net.IP) error

	// ReconcileEndpoints 设置给定 apiserver Service（ro 或 rw）的 endpoints。
	// 它假定所管理的 endpoints 对象只由自己管理，因此理解这些要求即可。
	//
	// 要求：
	//  * 所有 apiserver 必须使用相同的 {rw, ro} Service 端口。
	//  * 所有 apiserver 必须且只能通过 ReconcileEndpoints 管理其 endpoints。
	//  * 所有 apiserver 都会周期性调用 ReconcileEndpoints。
	ReconcileEndpoints(serviceName string, ip net.IP, endpointPorts []corev1.EndpointPort, reconcilePorts bool) error
	// RemoveEndpoints 移除本 apiserver 的租约。
	RemoveEndpoints(serviceName string, ip net.IP, endpointPorts []corev1.EndpointPort) error
	// StopReconciling 使之后的 ReconcileEndpoints 调用变为空操作。
	StopReconciling()
	// Destroy 关闭所有内部资源，必须线程安全且支持重复调用。
	Destroy()
}

// Type 表示 reconciler 的类型。
type Type string

const (
	// MasterCountReconcilerType 选择原始的基于 master 数量的 reconciler。
	MasterCountReconcilerType Type = "master-count"
	// LeaseEndpointReconcilerType 选择基于存储的 reconciler。
	LeaseEndpointReconcilerType Type = "lease"
	// NoneEndpointReconcilerType 关闭 endpoint reconciler。
	NoneEndpointReconcilerType Type = "none"
)

// Types 是 reconciler 类型数组。
type Types []Type

// AllTypes 导出所有 reconciler 类型。
var AllTypes = Types{
	MasterCountReconcilerType,
	LeaseEndpointReconcilerType,
	NoneEndpointReconcilerType,
}

// Names 返回所有 reconciler 名称的切片。
func (t Types) Names() []string {
	strs := make([]string, len(t))
	for i, v := range t {
		strs[i] = string(v)
	}
	return strs
}
