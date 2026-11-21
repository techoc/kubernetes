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

// Package reconcilers 提供空操作（noop）reconciler。
package reconcilers

import (
	"net"

	corev1 "k8s.io/api/core/v1"
)

// noneEndpointReconciler 用于禁用 endpoint reconciler。
type noneEndpointReconciler struct{}

// NewNoneEndpointReconciler 创建一个不做任何事的 EndpointReconciler（空操作）。
func NewNoneEndpointReconciler() EndpointReconciler {
	return &noneEndpointReconciler{}
}

// ValidateIP 空操作，始终返回 nil。
func (r *noneEndpointReconciler) ValidateIP(ip net.IP) error {
	return nil
}

// ReconcileEndpoints 空操作。
func (r *noneEndpointReconciler) ReconcileEndpoints(serviceName string, ip net.IP, endpointPorts []corev1.EndpointPort, reconcilePorts bool) error {
	return nil
}

// RemoveEndpoints 空操作。
func (r *noneEndpointReconciler) RemoveEndpoints(serviceName string, ip net.IP, endpointPorts []corev1.EndpointPort) error {
	return nil
}

// StopReconciling 空操作。
func (r *noneEndpointReconciler) StopReconciling() {
}

// Destroy 空操作。
func (r *noneEndpointReconciler) Destroy() {
}
