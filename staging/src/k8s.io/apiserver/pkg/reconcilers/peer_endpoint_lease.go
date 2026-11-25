/*
Copyright 2023 The Kubernetes Authors.

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

// Package reconcilers 提供 apiserver 级别的后台协调器，例如把本实例的
// peer 访问地址以租约形式写入共享存储，供其它 apiserver 在版本偏斜时转发请求。
// 协调器只依赖存储与 client-go，不依赖具体资源实现，便于在 apiserver 内复用。
package reconcilers

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	storagefactory "k8s.io/apiserver/pkg/storage/storagebackend/factory"
)

const (
	APIServerIdentityLabel = "apiserverIdentity"
)

// PeerAdvertiseAddress 是本 apiserver 对外声明的地址，用于版本偏斜时被 peer 反代。
type PeerAdvertiseAddress struct {
	PeerAdvertiseIP   string
	PeerAdvertisePort string
}

// peerEndpointLeases 封装底层 lease 存储；baseKey 下每个 serverId 对应一个 key。
type peerEndpointLeases struct {
	storage   storage.Interface
	destroyFn func()
	baseKey   string
	leaseTime time.Duration
}

// PeerEndpointLeaseReconciler 维护“本 apiserver 的 peer 地址”在共享存储中的租约。
// 用 lease（TTL）而非静态配置，才能让实例崩溃后在有限时间内自动过期，
// 避免 peer 一直把流量转发到不存在的地址。
type PeerEndpointLeaseReconciler interface {
	// GetEndpoint 返回指定 apiserverId 的端点。
	GetEndpoint(serverId string) (string, error)
	// UpdateLease 更新对端服务器的 IP 和端口。
	UpdateLease(serverId string, ip string, endpointPorts []corev1.EndpointPort) error
	// RemoveLease 删除此 apiserver 的对端端点租约。
	RemoveLease(serverId string) error
	// Destroy 清理关停时占用的所有资源。
	Destroy()
	// StopReconciling 使后续 ReconcileEndpoints 调用变为空操作。
	StopReconciling()
}

type peerEndpointLeaseReconciler struct {
	serverLeases          *peerEndpointLeases
	stopReconcilingCalled atomic.Bool
}

// NewPeerEndpointLeaseReconciler 创建新的对端端点租约协调器。
// 这里只做存储读写，不使用 watch，因此 newFunc/newListFunc 留空；
// destroyFn 用 sync.Once 包裹，保证 Stop 与 Destroy 重复调用安全。
func NewPeerEndpointLeaseReconciler(config *storagebackend.ConfigForResource, baseKey string, leaseTime time.Duration) (PeerEndpointLeaseReconciler, error) {
	// 不使用 storage.Watch 时，newFunc 和 newListFunc 可留空。
	leaseStorage, destroyFn, err := storagefactory.Create(*config, nil, nil, baseKey)
	if err != nil {
		return nil, fmt.Errorf("error creating storage factory: %v", err)
	}
	var once sync.Once
	return &peerEndpointLeaseReconciler{
		serverLeases: &peerEndpointLeases{
			storage:   leaseStorage,
			destroyFn: func() { once.Do(destroyFn) },
			baseKey:   baseKey,
			leaseTime: leaseTime,
		},
	}, nil
}

// PeerEndpointLeaseController 是更新对端端点租约的控制器管理器。
// 它为对端端点租约提供独立调谐循环，确保当某个 peer kube-apiserver 因版本不匹配
// 无法处理请求、需要代理到目标 apiserver 时，能够获取目标实例的最新端点信息。
// controller 与 reconciler 分离，便于把“调度节奏/生命周期”和“存储交互”分开测试。
type PeerEndpointLeaseController struct {
	reconciler       PeerEndpointLeaseReconciler
	endpointInterval time.Duration
	serverId         string
	// peeraddress 保存此 kube-apiserver 的 IP 和端口；版本偏斜时，peer 据此将请求路由到本实例。
	peeraddress string

	client kubernetes.Interface

	lock   sync.Mutex
	stopCh chan struct{} // 由 Stop() 关闭
}

func New(serverId string, peeraddress string,
	reconciler PeerEndpointLeaseReconciler, endpointInterval time.Duration, client kubernetes.Interface) *PeerEndpointLeaseController {
	return &PeerEndpointLeaseController{
		reconciler: reconciler,
		serverId:   serverId,
		// peeraddress 保存此 kube-apiserver 的 IP 和端口；版本偏斜时，peer 据此将请求路由到本实例。
		peeraddress:      peeraddress,
		endpointInterval: endpointInterval,
		client:           client,
		stopCh:           make(chan struct{}),
	}
}

// Start 启动 peer endpoint lease 调谐循环，这是集群引导所必需的。
func (c *PeerEndpointLeaseController) Start(stopCh <-chan struct{}) {
	localStopCh := make(chan struct{})
	go func() {
		defer close(localStopCh)
		select {
		case <-stopCh: // 来自 Start
		case <-c.stopCh: // 来自 Stop
		}
	}()
	go c.Run(localStopCh)
}

// Run 周期性地更新 peer endpoint leases。
//
// 先轮询 /readyz，确保本实例真正可服务后再声明自己；
// 否则 peer 可能把请求转发到一个尚未就绪的实例。
func (c *PeerEndpointLeaseController) Run(stopCh <-chan struct{}) {
	// 等待进程就绪
	wait.PollImmediateUntil(100*time.Millisecond, func() (bool, error) {
		var code int
		c.client.CoreV1().RESTClient().Get().AbsPath("/readyz").Do(context.TODO()).StatusCode(&code)
		return code == http.StatusOK, nil
	}, stopCh)

	wait.NonSlidingUntil(func() {
		if err := c.UpdatePeerEndpointLeases(); err != nil {
			runtime.HandleError(fmt.Errorf("unable to update peer endpoint leases: %v", err))
		}
	}, c.endpointInterval, stopCh)
}

// Stop 清理本 apiserver 的 peer endpoint leases。
//
// 先 StopReconciling 再 RemoveLease，避免删除后又有一个并发
// Reconcile 把 key 写回来；最后按 2*interval 超时兜底，etcd 不可达时也要能完成关停。
func (c *PeerEndpointLeaseController) Stop() {
	c.lock.Lock()
	defer c.lock.Unlock()

	select {
	case <-c.stopCh:
		return // 只关闭一次
	default:
		close(c.stopCh)
	}
	finishedReconciling := make(chan struct{})
	go func() {
		defer close(finishedReconciling)
		klog.Infof("Shutting down peer endpoint lease reconciler")
		// 停止调谐
		c.reconciler.StopReconciling()

		// 确保不会与 ReconcileEndpointLeases 产生竞态。
		if err := c.reconciler.RemoveLease(c.serverId); err != nil {
			klog.Errorf("Unable to remove peer endpoint leases: %v", err)
		}
		c.reconciler.Destroy()
	}()

	select {
	case <-finishedReconciling:
		// 完成
	case <-time.After(2 * c.endpointInterval):
		// 若无法访问 etcd 摘除自己，也不要永远阻塞 server 关停
		klog.Warning("peer_endpoint_controller's RemoveEndpoints() timed out")
	}
}

// UpdatePeerEndpointLeases 尝试更新 peer endpoint leases。
func (c *PeerEndpointLeaseController) UpdatePeerEndpointLeases() error {
	host, port, err := net.SplitHostPort(c.peeraddress)
	if err != nil {
		return err
	}

	p, err := strconv.Atoi(port)
	if err != nil {
		return err
	}
	endpointPorts := createEndpointPortSpec(p, "https")

	// 确保不会与 RemoveEndpointLeases 产生竞态。
	c.lock.Lock()
	defer c.lock.Unlock()

	// 无论下面的更新是否出错或发生冲突，都要刷新我们自己 key 的 TTL。
	// 这样至少能保证部分 server 会登记我们的 endpoint lease。
	if err := c.reconciler.UpdateLease(c.serverId, host, endpointPorts); err != nil {
		return err
	}
	return nil
}

// UpdateLease 重置存储中某个 server IP 的 TTL，key 不存在时会新建。
// 这里把 endpointPorts 的第一个元素作为 lease base key 的一部分，
// 以便支持“两个 apiserver 使用同一 IP 但不同端口”的测试场景。
//
// 若启用 UnknownVersionInteroperabilityProxy 特性，还会：
//  1. 把 apiserverId 存为 label；
//  2. 把 kube-apiserver 的 --peer-advertise-ip 与 --peer-advertise-port 取值
//     以 <ip:port> 格式的 annotation 存下来。
//
// key 用 serverId 而非 ip:port，是因为同一 IP 上可能跑多个实例；
// 代价是不校验 ip/port 的唯一性，冲突时以最后一次写入为准。
func (r *peerEndpointLeaseReconciler) UpdateLease(serverId string, ip string, endpointPorts []corev1.EndpointPort) error {
	// 仅在 apiserver 尚未关停时才调谐 endpoint
	if r.stopReconcilingCalled.Load() {
		return nil
	}

	// 用 serverID 作为 key，避免以 server 的 IP、端口作为 key。
	// 注意：这意味着该 lease 不会在各 apiserver 之间强制 ip/port 的互斥使用。
	key := path.Join(r.serverLeases.baseKey, serverId)
	return r.serverLeases.storage.GuaranteedUpdate(apirequest.NewDefaultContext(), key, &corev1.Endpoints{}, true, nil, func(input kruntime.Object, respMeta storage.ResponseMeta) (kruntime.Object, *uint64, error) {
		existing := input.(*corev1.Endpoints)
		existing.Subsets = []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{{IP: ip}},
				Ports:     endpointPorts,
			},
		}

		// 把本 server 的身份（serverId）存为 label。当 peer 因版本偏斜无法
		// 处理请求时，需要用该 label 找到本 server 的 IP。
		if existing.Labels == nil {
			existing.Labels = map[string]string{}
		}
		existing.Labels[APIServerIdentityLabel] = serverId

		// leaseTime 需要换算成秒
		leaseTime := uint64(r.serverLeases.leaseTime / time.Second)

		// 注意：GuaranteedUpdate 只有在 load 与 store 之间发生变化（resource
		// version 除外）时才会真正写入，因此不改动任何字段就无法刷新 TTL。
		// 递增 Generation 正是为了“制造变化”，否则 TTL 不会刷新。
		existing.Generation++

		klog.V(6).Infof("Resetting TTL on server IP %q listed in storage to %v", ip, leaseTime)
		return existing, &leaseTime, nil
	}, nil)
}

// ListLeases 从存储中获取当前 server IP 列表。
// 采用 Recursive + RV "0"（NotOlderThan）读本地/最新缓存，避免为发现请求
// 付出线性一致读的成本；租约数据的时效性由 TTL 保证。
func (r *peerEndpointLeaseReconciler) ListLeases() ([]string, error) {
	storageOpts := storage.ListOptions{
		ResourceVersion:      "0",
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		Predicate:            storage.Everything,
		Recursive:            true,
	}
	ipInfoList, err := r.getIpInfoList(storageOpts)
	if err != nil {
		return nil, err
	}
	ipList := make([]string, 0, len(ipInfoList.Items))
	for _, ip := range ipInfoList.Items {
		if len(ip.Subsets) > 0 && len(ip.Subsets[0].Addresses) > 0 && len(ip.Subsets[0].Addresses[0].IP) > 0 {
			ipList = append(ipList, ip.Subsets[0].Addresses[0].IP)
		}
	}
	klog.V(6).Infof("Current server IPs listed in storage are %v", ipList)
	return ipList, nil
}

// GetLease 获取指定 server id 对应的 server IP 与端口。
// 空 serverId 直接报错，避免 key 退化为 baseKey 而匹配到任意租约。
func (r *peerEndpointLeaseReconciler) GetLease(serverId string) (string, error) {
	var fullAddr string
	if serverId == "" {
		return "", fmt.Errorf("error getting endpoint for serverId: empty serverId")
	}
	storageOpts := storage.ListOptions{
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		Predicate:            storage.Everything,
		Recursive:            true,
	}
	ipInfoList, err := r.getIpInfoList(storageOpts)
	if err != nil {
		return "", err
	}

	for _, ip := range ipInfoList.Items {
		if ip.Labels[APIServerIdentityLabel] == serverId {
			if len(ip.Subsets) > 0 {
				var ipStr, portStr string
				if len(ip.Subsets[0].Addresses) > 0 {
					if len(ip.Subsets[0].Addresses[0].IP) > 0 {
						ipStr = ip.Subsets[0].Addresses[0].IP
					}
				}
				if len(ip.Subsets[0].Ports) > 0 {
					portStr = fmt.Sprint(ip.Subsets[0].Ports[0].Port)
				}
				fullAddr = net.JoinHostPort(ipStr, portStr)
				break
			}
		}
	}
	klog.V(6).Infof("Fetched this server IP for the specified apiserverId %v, %v", serverId, fullAddr)
	return fullAddr, nil
}

func (r *peerEndpointLeaseReconciler) StopReconciling() {
	r.stopReconcilingCalled.Store(true)
}

// RemoveLease 删除存储中某个 server IP 上的 lease。
// 与 UpdateLease 一致，把 endpointPorts 的第一个元素作为 lease base key 的一部分，
// 以支持“两个 apiserver 使用同一 IP 但不同端口”的测试场景。
func (r *peerEndpointLeaseReconciler) RemoveLease(serverId string) error {
	key := path.Join(r.serverLeases.baseKey, serverId)
	return r.serverLeases.storage.Delete(apirequest.NewDefaultContext(), key, &corev1.Endpoints{}, nil, rest.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
}

func (r *peerEndpointLeaseReconciler) Destroy() {
	r.serverLeases.destroyFn()
}

func (r *peerEndpointLeaseReconciler) GetEndpoint(serverId string) (string, error) {
	return r.GetLease(serverId)
}

func (r *peerEndpointLeaseReconciler) getIpInfoList(storageOpts storage.ListOptions) (*corev1.EndpointsList, error) {
	ipInfoList := &corev1.EndpointsList{}
	if err := r.serverLeases.storage.GetList(apirequest.NewDefaultContext(), r.serverLeases.baseKey, storageOpts, ipInfoList); err != nil {
		return nil, err
	}
	return ipInfoList, nil
}

// createEndpointPortSpec 构造 endpoint 的端口列表。
func createEndpointPortSpec(endpointPort int, endpointPortName string) []corev1.EndpointPort {
	return []corev1.EndpointPort{{
		Protocol: corev1.ProtocolTCP,
		Port:     int32(endpointPort),
		Name:     endpointPortName,
	}}
}
