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

// Package reconcilers 提供基于 master 数量的 reconciler。
package reconcilers

import (
	"net"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	endpointsv1 "k8s.io/kubernetes/pkg/api/v1/endpoints"
	"k8s.io/kubernetes/pkg/apis/core/validation"
)

// masterCountEndpointReconciler 按预期的 master 数量协调 endpoints，实现 EndpointReconciler。
type masterCountEndpointReconciler struct {
	masterCount           int
	epAdapter             EndpointsAdapter
	stopReconcilingCalled bool
	reconcilingLock       sync.Mutex
}

// NewMasterCountEndpointReconciler 创建按预期 master 数量协调的 EndpointReconciler。
func NewMasterCountEndpointReconciler(masterCount int, epAdapter EndpointsAdapter) EndpointReconciler {
	return &masterCountEndpointReconciler{
		masterCount: masterCount,
		epAdapter:   epAdapter,
	}
}

// ValidateIP 校验 IP 是否为合法的 endpoint 地址。
func (r *masterCountEndpointReconciler) ValidateIP(ip net.IP) error {
	return validation.ValidateEndpointIP(ip.String(), nil).ToAggregate()
}

// ReconcileEndpoints 设置给定 apiserver Service（ro 或 rw）的 endpoints。
// 它假定 endpoints 只由自己管理。要求：所有 apiserver 使用相同端口、只通过本方法管理 endpoints、
// 对预期的 apiserver 数量（masterCount）达成一致，并周期性调用本方法。
func (r *masterCountEndpointReconciler) ReconcileEndpoints(serviceName string, ip net.IP, endpointPorts []corev1.EndpointPort, reconcilePorts bool) error {
	r.reconcilingLock.Lock()
	defer r.reconcilingLock.Unlock()

	if r.stopReconcilingCalled {
		return nil
	}

	e, err := r.epAdapter.Get(metav1.NamespaceDefault, serviceName, metav1.GetOptions{})
	if err != nil {
		e = &corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: metav1.NamespaceDefault,
			},
		}
	}

	// 禁止 EndpointSliceMirroring 控制器镜像该 Endpoints，未来可能改变。
	skipMirrorChanged := setSkipMirrorTrue(e)

	if errors.IsNotFound(err) {
		// Endpoints 不存在时直接创建。
		e.Subsets = []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: ip.String()}},
			Ports:     endpointPorts,
		}}
		_, err = r.epAdapter.Create(metav1.NamespaceDefault, e)
		return err
	}

	// 先判断 endpoints 是否符合预期格式（单个 subset、端口匹配、N 个 IP）。
	formatCorrect, ipCorrect, portsCorrect := checkEndpointSubsetFormat(e, ip.String(), endpointPorts, r.masterCount, reconcilePorts)
	if !formatCorrect {
		// 格式严重错误，直接重建 endpoints 记录。
		e.Subsets = []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: ip.String()}},
			Ports:     endpointPorts,
		}}
		klog.Warningf("Resetting endpoints for master service %q to %#v", serviceName, e)
		_, err = r.epAdapter.Update(metav1.NamespaceDefault, e)
		return err
	}

	if !skipMirrorChanged && ipCorrect && portsCorrect {
		return r.epAdapter.EnsureEndpointSliceFromEndpoints(metav1.NamespaceDefault, e)
	}
	if !ipCorrect {
		// 总是先加入自己的 IP。
		e.Subsets[0].Addresses = append(e.Subsets[0].Addresses, corev1.EndpointAddress{IP: ip.String()})

		// 该步骤保持字典序。
		e.Subsets = endpointsv1.RepackSubsets(e.Subsets)

		// IP 过多时移除字典序在自己之后的项；基于前述要求，最终 IP 列表会收敛到正确状态。
		if addrs := &e.Subsets[0].Addresses; len(*addrs) > r.masterCount {
			// 使用指针是因为需要原地修改。
			for i, addr := range *addrs {
				if addr.IP == ip.String() {
					for len(*addrs) > r.masterCount {
						// 必要时回绕到开头。
						remove := (i + 1) % len(*addrs)
						*addrs = append((*addrs)[:remove], (*addrs)[remove+1:]...)
					}
					break
				}
			}
		}
	}
	if !portsCorrect {
		// 重置端口。
		e.Subsets[0].Ports = endpointPorts
	}
	klog.Warningf("Resetting endpoints for master service %q to %v", serviceName, e)
	_, err = r.epAdapter.Update(metav1.NamespaceDefault, e)
	return err
}

// RemoveEndpoints 从 endpoints 中移除本 apiserver 的 IP。
func (r *masterCountEndpointReconciler) RemoveEndpoints(serviceName string, ip net.IP, endpointPorts []corev1.EndpointPort) error {
	r.reconcilingLock.Lock()
	defer r.reconcilingLock.Unlock()

	e, err := r.epAdapter.Get(metav1.NamespaceDefault, serviceName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// Endpoints 不存在，无需处理。
			return nil
		}
		return err
	}

	if len(e.Subsets) == 0 {
		// 没有 subset，无需处理。
		return nil
	}
	// 从地址列表中移除自己的 IP。
	new := []corev1.EndpointAddress{}
	for _, addr := range e.Subsets[0].Addresses {
		if addr.IP != ip.String() {
			new = append(new, addr)
		}
	}
	e.Subsets[0].Addresses = new
	e.Subsets = endpointsv1.RepackSubsets(e.Subsets)
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		_, err := r.epAdapter.Update(metav1.NamespaceDefault, e)
		return err
	})
	return err
}

// StopReconciling 停止后续协调操作。
func (r *masterCountEndpointReconciler) StopReconciling() {
	r.reconcilingLock.Lock()
	defer r.reconcilingLock.Unlock()
	r.stopReconcilingCalled = true
}

// Destroy 空实现，无资源需要释放。
func (r *masterCountEndpointReconciler) Destroy() {
}

// checkEndpointSubsetFormat 判断 endpoints 是否符合 ReconcileEndpoints 期望的格式。
// formatCorrect 表示恰好一个 subset；ipCorrect 表示找到当前 master 的 IP 且地址数不超过 masterCount；
// portsCorrect 表示端口完全匹配，仅在 reconcilePorts 为 true 时计算。
func checkEndpointSubsetFormat(e *corev1.Endpoints, ip string, ports []corev1.EndpointPort, count int, reconcilePorts bool) (formatCorrect bool, ipCorrect bool, portsCorrect bool) {
	if len(e.Subsets) != 1 {
		return false, false, false
	}
	sub := &e.Subsets[0]
	portsCorrect = true
	if reconcilePorts {
		if len(sub.Ports) != len(ports) {
			portsCorrect = false
		}
		for i, port := range ports {
			if len(sub.Ports) <= i || port != sub.Ports[i] {
				portsCorrect = false
				break
			}
		}
	}
	for _, addr := range sub.Addresses {
		if addr.IP == ip {
			ipCorrect = len(sub.Addresses) <= count
			break
		}
	}
	return true, ipCorrect, portsCorrect
}
