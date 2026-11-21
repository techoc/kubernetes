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

package reconcilers

// 原始来源：
// https://github.com/openshift/origin/blob/bb340c5dd5ff72718be86fb194dedc0faed7f4c7/pkg/cmd/server/election/lease_endpoint_reconciler.go

import (
	"fmt"
	"net"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	storagefactory "k8s.io/apiserver/pkg/storage/storagebackend/factory"
	endpointsv1 "k8s.io/kubernetes/pkg/api/v1/endpoints"
	"k8s.io/kubernetes/pkg/apis/core/validation"
)

// Leases 接口用于管理活跃 master 集合。
type Leases interface {
	// ListLeases 列出当前 master 的 IP。
	ListLeases() ([]string, error)

	// UpdateLease 添加或刷新 master 的租约。
	UpdateLease(ip string) error

	// RemoveLease 移除 master 的租约。
	RemoveLease(ip string) error

	// Destroy 在关闭时清理所有资源。
	Destroy()
}

// storageLeases 基于存储后端实现 Leases。
type storageLeases struct {
	storage   storage.Interface
	destroyFn func()
	baseKey   string
	leaseTime time.Duration
}

var _ Leases = &storageLeases{}

// ListLeases 从存储中列出当前 master 的 IP。
func (s *storageLeases) ListLeases() ([]string, error) {
	ipInfoList := &corev1.EndpointsList{}
	storageOpts := storage.ListOptions{
		ResourceVersion:      "0",
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		Predicate:            storage.Everything,
		Recursive:            true,
	}
	if err := s.storage.GetList(apirequest.NewDefaultContext(), s.baseKey, storageOpts, ipInfoList); err != nil {
		return nil, err
	}

	ipList := make([]string, 0, len(ipInfoList.Items))
	for _, ip := range ipInfoList.Items {
		if len(ip.Subsets) > 0 && len(ip.Subsets[0].Addresses) > 0 && len(ip.Subsets[0].Addresses[0].IP) > 0 {
			ipList = append(ipList, ip.Subsets[0].Addresses[0].IP)
		}
	}

	klog.V(6).Infof("Current master IPs listed in storage are %v", ipList)

	return ipList, nil
}

// UpdateLease 重置存储中 master IP 的 TTL，若 key 不存在则创建。
func (s *storageLeases) UpdateLease(ip string) error {
	key := path.Join(s.baseKey, ip)
	return s.storage.GuaranteedUpdate(apirequest.NewDefaultContext(), key, &corev1.Endpoints{}, true, nil, func(input kruntime.Object, respMeta storage.ResponseMeta) (kruntime.Object, *uint64, error) {
		// 确保 IP 设置正确，然后刷新 TTL。
		existing := input.(*corev1.Endpoints)
		existing.Subsets = []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{{IP: ip}},
			},
		}

		// TTL 必须以秒为单位。
		leaseTime := uint64(s.leaseTime / time.Second)

		// GuaranteedUpdate 仅在加载后对象发生变化时才执行写入，因此必须实际修改字段才能刷新 TTL。
		existing.Generation++

		klog.V(6).Infof("Resetting TTL on master IP %q listed in storage to %v", ip, leaseTime)

		return existing, &leaseTime, nil
	}, nil)
}

// RemoveLease 从存储中移除 master IP 的租约。
func (s *storageLeases) RemoveLease(ip string) error {
	key := path.Join(s.baseKey, ip)
	return s.storage.Delete(apirequest.NewDefaultContext(), key, &corev1.Endpoints{}, nil, rest.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
}

// Destroy 调用底层清理函数（通过 once 保证只执行一次）。
func (s *storageLeases) Destroy() {
	s.destroyFn()
}

// NewLeases 创建一个新的基于 etcd 的 Leases 实现。
func NewLeases(config *storagebackend.ConfigForResource, baseKey string, leaseTime time.Duration) (Leases, error) {
	// 不使用 storage.Watch 时 newFunc/newListFunc 可留空。
	leaseStorage, destroyFn, err := storagefactory.Create(*config, nil, nil, baseKey)
	if err != nil {
		return nil, fmt.Errorf("error creating storage factory: %v", err)
	}
	var once sync.Once
	return &storageLeases{
		storage:   leaseStorage,
		destroyFn: func() { once.Do(destroyFn) },
		baseKey:   baseKey,
		leaseTime: leaseTime,
	}, nil
}

// leaseEndpointReconciler 基于租约列表协调 endpoints。
type leaseEndpointReconciler struct {
	epAdapter             EndpointsAdapter
	masterLeases          Leases
	stopReconcilingCalled atomic.Bool
	reconcilingLock       sync.Mutex
}

// NewLeaseEndpointReconciler 创建一个新的基于租约的 EndpointReconciler。
func NewLeaseEndpointReconciler(epAdapter EndpointsAdapter, masterLeases Leases) EndpointReconciler {
	return &leaseEndpointReconciler{
		epAdapter:    epAdapter,
		masterLeases: masterLeases,
	}
}

// ValidateIP 校验 IP 是否为合法的 endpoint 地址。
func (r *leaseEndpointReconciler) ValidateIP(ip net.IP) error {
	return validation.ValidateEndpointIP(ip.String(), nil).ToAggregate()
}

// ReconcileEndpoints 列出特殊 etcd 目录中的 key。每个 key 的 TTL 为 R+n（R 为本函数调用间隔，n 为小余量）；
// apiserver 宕机后无法续租，key 过期，ReconcileEndpoints 会据此更新 endpoints 对象。
func (r *leaseEndpointReconciler) ReconcileEndpoints(serviceName string, ip net.IP, endpointPorts []corev1.EndpointPort, reconcilePorts bool) error {
	// apiserver 关闭后不再协调。
	if r.stopReconcilingCalled.Load() {
		return nil
	}

	// 加锁避免与 RemoveEndpoints 竞争。
	r.reconcilingLock.Lock()
	defer r.reconcilingLock.Unlock()

	// 无论后续是否出错或冲突，先刷新自己的租约 TTL，确保至少部分 master 会保留本端点。
	if err := r.masterLeases.UpdateLease(ip.String()); err != nil {
		return err
	}

	return r.doReconcile(serviceName, endpointPorts, reconcilePorts)
}

// doReconcile 由 ReconcileEndpoints 或 RemoveEndpoints 调用，非并发安全（调用方需持锁）。
func (r *leaseEndpointReconciler) doReconcile(serviceName string, endpointPorts []corev1.EndpointPort, reconcilePorts bool) error {
	e, err := r.epAdapter.Get(corev1.NamespaceDefault, serviceName, metav1.GetOptions{})
	shouldCreate := false
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		// endpoints 不存在且已停止协调，无需处理。
		if r.stopReconcilingCalled.Load() {
			return nil
		}

		shouldCreate = true
		e = &corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: corev1.NamespaceDefault,
			},
		}
	}

	// 读取 etcd 中的 master IP 租约列表。
	masterIPs, err := r.masterLeases.ListLeases()
	if err != nil {
		return err
	}

	// 刚刷新过自己的租约，若存储返回零个 master IP 说明状态异常，拒绝清空 endpoints。
	// 但若已被要求停止且这是最后一个 apiserver，则仍需继续以移除自身端点。
	if !r.stopReconcilingCalled.Load() && len(masterIPs) == 0 {
		return fmt.Errorf("no API server IP addresses were listed in storage, refusing to erase all endpoints for the kubernetes Service")
	}

	// 禁止 EndpointSliceMirroring 控制器镜像该 Endpoints，未来可能改变。
	skipMirrorChanged := setSkipMirrorTrue(e)

	// 将当前 endpoints 与 master IP 列表比较。
	formatCorrect, ipCorrect, portsCorrect := checkEndpointSubsetFormatWithLease(e, masterIPs, endpointPorts, reconcilePorts)
	if !skipMirrorChanged && formatCorrect && ipCorrect && portsCorrect {
		return r.epAdapter.EnsureEndpointSliceFromEndpoints(corev1.NamespaceDefault, e)
	}

	if !formatCorrect {
		// 格式严重错误，直接重建 endpoints 记录。
		e.Subsets = []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{},
			Ports:     endpointPorts,
		}}
	}

	if !formatCorrect || !ipCorrect {
		// 按 etcd 中的预期 IP 重新填充地址。
		e.Subsets[0].Addresses = make([]corev1.EndpointAddress, len(masterIPs))
		for ind, ip := range masterIPs {
			e.Subsets[0].Addresses[ind] = corev1.EndpointAddress{IP: ip}
		}

		// 该步骤保持字典序。
		e.Subsets = endpointsv1.RepackSubsets(e.Subsets)
	}

	if len(e.Subsets) != 0 && !portsCorrect {
		// 重置端口。
		e.Subsets[0].Ports = endpointPorts
	}

	klog.Warningf("Resetting endpoints for master service %q to %v", serviceName, masterIPs)
	if shouldCreate {
		if _, err = r.epAdapter.Create(corev1.NamespaceDefault, e); errors.IsAlreadyExists(err) {
			err = nil
		}
	} else {
		_, err = r.epAdapter.Update(corev1.NamespaceDefault, e)
	}
	return err
}

// checkEndpointSubsetFormatWithLease 判断租约模式下 endpoints 是否符合预期格式：
// formatCorrect 表示恰好一个 subset；ipsCorrect 表示地址与预期列表一致；
// portsCorrect 表示端口完全匹配，仅在 reconcilePorts 为 true 时计算。
func checkEndpointSubsetFormatWithLease(e *corev1.Endpoints, expectedIPs []string, ports []corev1.EndpointPort, reconcilePorts bool) (formatCorrect bool, ipsCorrect bool, portsCorrect bool) {
	if len(e.Subsets) != 1 {
		return false, false, false
	}
	sub := &e.Subsets[0]
	portsCorrect = true
	if reconcilePorts {
		if len(sub.Ports) != len(ports) {
			portsCorrect = false
		} else {
			for i, port := range ports {
				if port != sub.Ports[i] {
					portsCorrect = false
					break
				}
			}
		}
	}

	ipsCorrect = true
	if len(sub.Addresses) != len(expectedIPs) {
		ipsCorrect = false
	} else {
		// 检查地址内容：presentAddrs 的 key 作为集合，value 标记是否已出现。
		presentAddrs := make(map[string]bool, len(expectedIPs))
		for _, ip := range expectedIPs {
			presentAddrs[ip] = false
		}

		// 假定地址本身唯一。
		for _, addr := range sub.Addresses {
			if alreadySeen, ok := presentAddrs[addr.IP]; alreadySeen || !ok {
				ipsCorrect = false
				break
			}

			presentAddrs[addr.IP] = true
		}
	}

	return true, ipsCorrect, portsCorrect
}

// RemoveEndpoints 移除本 apiserver 的租约并重新协调 endpoints。
func (r *leaseEndpointReconciler) RemoveEndpoints(serviceName string, ip net.IP, endpointPorts []corev1.EndpointPort) error {
	// 加锁避免与 ReconcileEndpoints 竞争。
	r.reconcilingLock.Lock()
	defer r.reconcilingLock.Unlock()

	if err := r.masterLeases.RemoveLease(ip.String()); err != nil {
		return err
	}

	return r.doReconcile(serviceName, endpointPorts, true)
}

// StopReconciling 标记停止协调。
func (r *leaseEndpointReconciler) StopReconciling() {
	r.stopReconcilingCalled.Store(true)
}

// Destroy 释放底层租约存储。
func (r *leaseEndpointReconciler) Destroy() {
	r.masterLeases.Destroy()
}
