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

package controller

import (
	"context"
	"fmt"
	"net"
	"time"

	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	networkinginformers "k8s.io/client-go/informers/networking/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	networkinglisters "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/servicecidr"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/controlplane/controller/defaultservicecidr"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	"k8s.io/utils/clock"
	netutils "k8s.io/utils/net"
)

const (
	// maxRetries 表示 Service 从队列中被放弃前允许重试的次数。当前限速器采用 5ms * 2^(重试次数-1)
	// 的指数退避策略，连续重试间隔依次为：5ms、10ms、20ms、40ms、80ms、160ms、320ms、640ms、
	// 1.3s、2.6s、5.1s、10.2s、20.4s、41s、82s。
	maxRetries = 15
	workers    = 5
)

// Repair 是一个控制器循环，它会检查所有 Service 的 ClusterIP 分配情况并记录错误，
// 然后根据当前已分配的 ClusterIP 重建准确的 IPAddress 对象列表。
//
// 处理以下问题：
// * 由运维操作或未被检测到的竞态导致的 ClusterIP 重复分配
// * 因宕机或断电而未真正创建成功、但仍保留了分配的 Service
// * 自动把旧版本 Kubernetes 创建的 Service 迁移到新的 ipallocator，
//   并创建对应的 IPAddress 对象
// * 引用（reference）或标签（labels）错误的 IPAddress 对象
//
// 仅记录日志的情况：
// * ClusterIP 不在当前配置的地址段范围内
//
// Service 的 ClusterIP 与 IPAddress 之间是一对一关系，
// 双向关系通过以下字段建立：
// Service.Spec.Cluster == IPAddress.Name AND IPAddress.ParentRef == Service
//
// 控制器使用两个调和（reconcile）循环，一个处理 Service，另一个处理 IPAddress。
// Service 循环校验双向关系是否存在且正确。
// 1. Service_X [ClusterIP_X]  <------>  IPAddress_X [Ref:Service_X]   ok
// 2. Service_Y [ClusterIP_Y]  <------>  IPAddress_Y [Ref:GatewayA]    !ok, wrong reference
// 3. Service_Z [ClusterIP_Z]  <------>  							   !ok, missing IPAddress
// 4. Service_A [ClusterIP_A]  <------>  IPAddress_A [Ref:Service_B]   !ok, duplicate IPAddress
//    Service_B [ClusterIP_A]  <------> 								only one service can verify the relation
// IPAddress 循环检查是否存在游离（orphan）的 IPAddress，其余情况
// 都由 Service 循环覆盖。
// 1.                          <------>  IPAddress_Z [Ref:Service_C]   !ok, orphan IPAddress

// RepairIPAddress 使用 Service/IPAddress Informer 双向核对 ClusterIP 所有权，修复缺失对象并清理孤立或错误引用对象。
type RepairIPAddress struct {
	client   kubernetes.Interface
	interval time.Duration

	serviceLister  corelisters.ServiceLister
	servicesSynced cache.InformerSynced

	serviceCIDRLister networkinglisters.ServiceCIDRLister
	serviceCIDRSynced cache.InformerSynced

	ipAddressLister networkinglisters.IPAddressLister
	ipAddressSynced cache.InformerSynced

	svcQueue         workqueue.TypedRateLimitingInterface[string]
	ipQueue          workqueue.TypedRateLimitingInterface[string]
	workerLoopPeriod time.Duration

	broadcaster events.EventBroadcaster
	recorder    events.EventRecorder
	clock       clock.Clock
}

// NewRepairIPAddress 创建 IPAddress 修复控制器，监听 Service、ServiceCIDR 和 IPAddress 变化并构建带限速重试的队列。
func NewRepairIPAddress(interval time.Duration,
	client kubernetes.Interface,
	serviceInformer coreinformers.ServiceInformer,
	serviceCIDRInformer networkinginformers.ServiceCIDRInformer,
	ipAddressInformer networkinginformers.IPAddressInformer) *RepairIPAddress {
	return newRepairIPAddress(interval, client, serviceInformer, serviceCIDRInformer, ipAddressInformer, clock.RealClock{})
}

// newRepairIPAddress 实现 NewRepairIPAddress，额外接收一个 clock.Clock 参数。
func newRepairIPAddress(interval time.Duration,
	client kubernetes.Interface,
	serviceInformer coreinformers.ServiceInformer,
	serviceCIDRInformer networkinginformers.ServiceCIDRInformer,
	ipAddressInformer networkinginformers.IPAddressInformer,
	c clock.Clock) *RepairIPAddress {
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})
	recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, "ipallocator-repair-controller")

	r := &RepairIPAddress{
		interval:          interval,
		client:            client,
		serviceLister:     serviceInformer.Lister(),
		servicesSynced:    serviceInformer.Informer().HasSynced,
		serviceCIDRLister: serviceCIDRInformer.Lister(),
		serviceCIDRSynced: serviceCIDRInformer.Informer().HasSynced,
		ipAddressLister:   ipAddressInformer.Lister(),
		ipAddressSynced:   ipAddressInformer.Informer().HasSynced,
		svcQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "services"},
		),
		ipQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "ipaddresses"},
		),
		workerLoopPeriod: time.Second,
		broadcaster:      eventBroadcaster,
		recorder:         recorder,
		clock:            c,
	}

	_, _ = serviceInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				r.svcQueue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				r.svcQueue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// IndexerInformer 使用 delta 队列，因此删除时必须用这个
			// key 函数。
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				r.svcQueue.Add(key)
			}
		},
	}, interval)

	ipAddressInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				r.ipQueue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				r.ipQueue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// IndexerInformer 使用 delta 队列，因此删除时必须用这个
			// key 函数。
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				r.ipQueue.Add(key)
			}
		},
	}, interval)

	return r
}

// RunUntil 等待相关缓存与默认 ServiceCIDR 就绪，先完成一次全量核对，再启动 Service/IPAddress 两组增量 worker。
func (r *RepairIPAddress) RunUntil(onFirstSuccess func(), stopCh chan struct{}) {
	defer r.ipQueue.ShutDown()
	defer r.svcQueue.ShutDown()
	r.broadcaster.StartRecordingToSink(stopCh)
	defer r.broadcaster.Shutdown()

	klog.Info("Starting ipallocator-repair-controller")
	defer klog.Info("Shutting down ipallocator-repair-controller")

	if !cache.WaitForNamedCacheSync("ipallocator-repair-controller", stopCh, r.ipAddressSynced, r.servicesSynced, r.serviceCIDRSynced) {
		return
	}

	// 等待默认 ServiceCIDR 就绪
	ctx := wait.ContextForChannel(stopCh)
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(context.Context) (bool, error) {
		_, err := r.serviceCIDRLister.Get(defaultservicecidr.DefaultServiceCIDRName)
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		runtime.HandleError(err)
		return
	}

	// 首次同步会遍历缓存中所有的 Service 和 IPAddress，
	// 同步完成后通知主循环，此后由事件处理器继续工作，
	// 这样开销更小、也更高效。
	if err := r.runOnce(); err != nil {
		runtime.HandleError(err)
		return
	}
	onFirstSuccess()

	for i := 0; i < workers; i++ {
		go wait.Until(r.ipWorker, r.workerLoopPeriod, stopCh)
		go wait.Until(r.svcWorker, r.workerLoopPeriod, stopCh)
	}

	<-stopCh
}

// runOnce 检查 ClusterIP 分配的状态，若出现无法恢复的问题则返回错误。
func (r *RepairIPAddress) runOnce() error {
	return retry.OnError(retry.DefaultBackoff, func(err error) bool {
		// 修复 ClusterIP 分配时可能收到 conflict 或 forbidden 错误。
		// IsForbidden 依赖 admission 链就绪，而它又可能依赖 Namespace informer 就绪。
		// Ref: https://issues.k8s.io/136288
		if apierrors.IsConflict(err) || apierrors.IsForbidden(err) {
			klog.ErrorS(err, "Running ipallocator repair failed ... retrying")
			return true
		}
		klog.ErrorS(err, "Running ipallocator repair failed with not retryable error")
		return false
	}, r.doRunOnce)
}

// doRunOnce 先遍历 Service 确保每个 ClusterIP 有正确 IPAddress，再反向检查控制器管理的 IPAddress 是否仍有有效 Service 所有者。
func (r *RepairIPAddress) doRunOnce() error {
	services, err := r.serviceLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("unable to refresh the service IP block: %v", err)
	}

	// 检查每个 Service 的 ClusterIP，并按预期重建状态。
	for _, svc := range services {
		key, err := cache.MetaNamespaceKeyFunc(svc)
		if err != nil {
			return err
		}
		err = r.syncService(key)
		if err != nil {
			return err
		}
	}

	// 上面已确认每个 Service 都有对应的 IP，
	// 这里再检查分配器创建的 IP 中，没有缺失对应 Service 的情况。
	ipLabelSelector := labels.Set(map[string]string{
		networkingv1.LabelManagedBy: ipallocator.ControllerName,
	}).AsSelectorPreValidated()
	ipAddresses, err := r.ipAddressLister.List(ipLabelSelector)
	if err != nil {
		return fmt.Errorf("unable to refresh the IPAddress block: %v", err)
	}
	// 检查每个 IPAddress 与其对应的 Service 是否匹配，并按预期重建状态。
	for _, ipAddress := range ipAddresses {
		key, err := cache.MetaNamespaceKeyFunc(ipAddress)
		if err != nil {
			return err
		}
		err = r.syncIPAddress(key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RepairIPAddress) svcWorker() {
	for r.processNextWorkSvc() {
	}
}

func (r *RepairIPAddress) processNextWorkSvc() bool {
	eKey, quit := r.svcQueue.Get()
	if quit {
		return false
	}
	defer r.svcQueue.Done(eKey)

	err := r.syncService(eKey)
	r.handleSvcErr(err, eKey)

	return true
}

func (r *RepairIPAddress) handleSvcErr(err error, key string) {
	if err == nil {
		r.svcQueue.Forget(key)
		return
	}

	if r.svcQueue.NumRequeues(key) < maxRetries {
		klog.V(2).InfoS("Error syncing Service, retrying", "service", key, "err", err)
		r.svcQueue.AddRateLimited(key)
		return
	}

	klog.Warningf("Dropping Service %q out of the queue: %v", key, err)
	r.svcQueue.Forget(key)
	runtime.HandleError(err)
}

// syncService 逐个校验 Service.ClusterIPs：缺少 IPAddress 时创建，引用/标签错误时重建，并拒绝越界或重复地址。
func (r *RepairIPAddress) syncService(key string) error {
	var syncError error
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	svc, err := r.serviceLister.Services(namespace).Get(name)
	if err != nil {
		// 无需处理
		return nil
	}
	if !helper.IsServiceIPSet(svc) {
		// 该 Service 不需要 ClusterIP
		return nil
	}

	for _, clusterIP := range svc.Spec.ClusterIPs {
		ip := netutils.ParseIPSloppy(clusterIP)
		if ip == nil {
			// ClusterIP 已损坏。ClusterIP 本来在校验阶段就已检查过，
			// 这里再确认一次，以防解析逻辑存在不一致。
			r.recorder.Eventf(svc, nil, v1.EventTypeWarning, "ClusterIPNotValid", "ClusterIPValidation", "Cluster IP %s is not a valid IP; please recreate Service", ip)
			runtime.HandleError(fmt.Errorf("the ClusterIP %s for Service %s/%s is not a valid IP; please recreate Service", ip, svc.Namespace, svc.Name))
			continue
		}
		// TODO(aojea) 重构这部分，把 IP 检查抽象出来
		family := getFamilyByIP(ip)
		if r.isIPOutOfRange(ip) {
			// ClusterIP 超出地址段范围
			r.recorder.Eventf(svc, nil, v1.EventTypeWarning, "ClusterIPOutOfRange", "ClusterIPAllocation", "Cluster IP [%v]: %s is not within any configured Service CIDR; please recreate service", family, ip)
			runtime.HandleError(fmt.Errorf("the ClusterIP [%v]: %s for Service %s/%s is not within any service CIDR; please recreate", family, ip, svc.Namespace, svc.Name))
			continue
		}

		// 取出与该 ClusterIP 关联的 IPAddress 对象
		ipAddress, err := r.ipAddressLister.Get(ip.String())
		if apierrors.IsNotFound(err) {
			// 该 ClusterIP 似乎没有被分配，创建对应的对象。
			r.recorder.Eventf(svc, nil, v1.EventTypeWarning, "ClusterIPNotAllocated", "ClusterIPAllocation", "Cluster IP [%v]: %s is not allocated; repairing", family, ip)
			runtime.HandleError(fmt.Errorf("the ClusterIP [%v]: %s for Service %s/%s is not allocated; repairing", family, ip, svc.Namespace, svc.Name))
			_, err := r.client.NetworkingV1().IPAddresses().Create(context.Background(), newIPAddress(ip.String(), svc), metav1.CreateOptions{})
			if err != nil {
				return err
			}
			continue
		}
		if err != nil {
			r.recorder.Eventf(svc, nil, v1.EventTypeWarning, "UnknownError", "ClusterIPAllocation", "Unable to allocate ClusterIP [%v]: %s due to an unknown error", family, ip)
			return fmt.Errorf("unable to allocate ClusterIP [%v]: %s for Service %s/%s due to an unknown error, will retry later: %v", family, ip, svc.Namespace, svc.Name, err)
		}

		// 属于某个 Service 的 IPAddress 必须引用一个 Service
		if ipAddress.Spec.ParentRef.Group != "" ||
			ipAddress.Spec.ParentRef.Resource != "services" {
			r.recorder.Eventf(svc, nil, v1.EventTypeWarning, "ClusterIPNotAllocated", "ClusterIPAllocation", "the ClusterIP [%v]: %s for Service %s/%s has a wrong reference; repairing", family, ip, svc.Namespace, svc.Name)
			if err := r.recreateIPAddress(ipAddress.Name, svc); err != nil {
				return err
			}
			continue
		}

		// 属于某个 Service 的 IPAddress 必须引用当前这个 Service
		if ipAddress.Spec.ParentRef.Namespace != svc.Namespace ||
			ipAddress.Spec.ParentRef.Name != svc.Name {
			// 确认不存在两个 Service 使用同一个 IP，否则
			// 会不断删除并重建同一个 IPAddress、反复修改其引用。
			refService, err := r.serviceLister.Services(ipAddress.Spec.ParentRef.Namespace).Get(ipAddress.Spec.ParentRef.Name)
			if err != nil {
				r.recorder.Eventf(svc, nil, v1.EventTypeWarning, "ClusterIPNotAllocated", "ClusterIPAllocation", "the ClusterIP [%v]: %s for Service %s/%s has a wrong reference; repairing", family, ip, svc.Namespace, svc.Name)
				if err := r.recreateIPAddress(ipAddress.Name, svc); err != nil {
					return err
				}
				continue
			}
			// 该 IPAddress 重复，且当前 Service 并非其引用的对象，需要重新创建
			for _, clusterIP := range refService.Spec.ClusterIPs {
				if ipAddress.Name == clusterIP {
					r.recorder.Eventf(svc, nil, v1.EventTypeWarning, "ClusterIPAlreadyAllocated", "ClusterIPAllocation", "Cluster IP [%v]:%s was assigned to multiple services; please recreate service", family, ip)
					runtime.HandleError(fmt.Errorf("the cluster IP [%v]:%s for service %s/%s was assigned to other services %s/%s; please recreate", family, ip, svc.Namespace, svc.Name, refService.Namespace, refService.Name))
					break
				}
			}
		}

		// IPAddress 必须带有分配器写入的对应 labels
		if !verifyIPAddressLabels(ipAddress) {
			if err := r.recreateIPAddress(ipAddress.Name, svc); err != nil {
				return err
			}
			continue
		}

	}
	return syncError
}

func (r *RepairIPAddress) recreateIPAddress(name string, svc *v1.Service) error {
	err := r.client.NetworkingV1().IPAddresses().Delete(context.Background(), name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	_, err = r.client.NetworkingV1().IPAddresses().Create(context.Background(), newIPAddress(name, svc), metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (r *RepairIPAddress) ipWorker() {
	for r.processNextWorkIp() {
	}
}

func (r *RepairIPAddress) processNextWorkIp() bool {
	eKey, quit := r.ipQueue.Get()
	if quit {
		return false
	}
	defer r.ipQueue.Done(eKey)

	err := r.syncIPAddress(eKey)
	r.handleIPErr(err, eKey)

	return true
}

func (r *RepairIPAddress) handleIPErr(err error, key string) {
	if err == nil {
		r.ipQueue.Forget(key)
		return
	}

	if r.ipQueue.NumRequeues(key) < maxRetries {
		klog.V(2).InfoS("Error syncing Service, retrying", "service", key, "err", err)
		r.ipQueue.AddRateLimited(key)
		return
	}

	klog.Warningf("Dropping Service %q out of the queue: %v", key, err)
	r.ipQueue.Forget(key)
	runtime.HandleError(err)
}

// 本函数反向核验 IPAddress 的 Service ParentRef：由 ipallocator 控制器拥有的 IPAddress
// 必须引用一个已存在的 Service，以避免 IPAddress 泄漏；由其他控制器拥有的 IPAddress
// syncIPAddress 反向核验 IPAddress 的 Service ParentRef；对创建事务中的短暂孤儿对象使用宽限期，超时后才清理。
func (r *RepairIPAddress) syncIPAddress(key string) error {
	ipAddress, err := r.ipAddressLister.Get(key)
	if err != nil {
		// 无需处理
		return nil
	}

	// 不由本控制器管理
	if !managedByController(ipAddress) {
		return nil
	}

	// 由 service allocator 创建但没有引用 Service，说明被其他组件改过，删除它
	if ipAddress.Spec.ParentRef.Group != "" || ipAddress.Spec.ParentRef.Resource != "services" {
		runtime.HandleError(fmt.Errorf("IPAddress %s appears to have been modified, not referencing a Service %v: cleaning up", ipAddress.Name, ipAddress.Spec.ParentRef))
		r.recorder.Eventf(ipAddress, nil, v1.EventTypeWarning, "IPAddressNotAllocated", "IPAddressAllocation", "IPAddress %s appears to have been modified, not referencing a Service %v: cleaning up", ipAddress.Name, ipAddress.Spec.ParentRef)
		err := r.client.NetworkingV1().IPAddresses().Delete(context.Background(), ipAddress.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	// 创建 Service 时，IP 分配器会先为它创建 IPAddress 对象，
	// 此时 Service 对象尚未创建成功，因此需要等待它出现。
	// 另外，如果 apiserver 在创建 Service 的过程中崩溃，IPAddress 就会变成游离对象。
	// 如果它的创建时间超过 60 秒（kube-apiserver 的默认超时时间），就删除该 IPAddress。
	svc, err := r.serviceLister.Services(ipAddress.Spec.ParentRef.Namespace).Get(ipAddress.Spec.ParentRef.Name)
	if apierrors.IsNotFound(err) {
		// 清理所有没有 ownerReference、且创建时间超过 60 秒（kube-apiserver 默认超时）的 IPAddress。
		// 之所以要这样做，是因为在 Service 创建过程中，会存在 IPAddress 已创建、Service 仍在创建的窗口期。
		// 这里假定 CreationTimestamp 一定存在。
		ipLifetime := r.clock.Now().Sub(ipAddress.CreationTimestamp.Time)
		gracePeriod := 60 * time.Second
		if ipLifetime > gracePeriod {
			runtime.HandleError(fmt.Errorf("IPAddress %s appears to have leaked: cleaning up", ipAddress.Name))
			r.recorder.Eventf(ipAddress, nil, v1.EventTypeWarning, "IPAddressNotAllocated", "IPAddressAllocation", "IPAddress: %s for Service %s/%s appears to have leaked: cleaning up", ipAddress.Name, ipAddress.Spec.ParentRef.Namespace, ipAddress.Spec.ParentRef.Name)
			err := r.client.NetworkingV1().IPAddresses().Delete(context.Background(), ipAddress.Name, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		// 宽限期结束后重新入队
		r.ipQueue.AddAfter(key, gracePeriod-ipLifetime)
		return nil
	}
	if err != nil {
		runtime.HandleError(fmt.Errorf("unable to get parent Service for IPAddress %s due to an unknown error: %v", ipAddress, err))
		r.recorder.Eventf(ipAddress, nil, v1.EventTypeWarning, "UnknownError", "IPAddressAllocation", "Unable to get parent Service for IPAddress %s due to an unknown error", ipAddress)
		return err
	}
	// Service 已存在。前面的循环已确认所有 Service → IPAddress 的关系是正确的，
	// 但还必须反过来检查 IPAddress → Service 的关系是否正确。
	for _, clusterIP := range svc.Spec.ClusterIPs {
		if ipAddress.Name == clusterIP {
			return nil
		}
	}

	// 有一个特殊情况：我们会"打破" Service ClusterIP 的不可变性，
	// 在 Update 时分配 IP —— 即 Service 的类型从 ExternalName
	// 变更为需要创建 ClusterIP 的类型。和处理"Service 晚于 IPAddress 创建"
	// 所用的逻辑类似，这里也把新建 IPAddress 对象的延迟处理时间
	// 设为 kube-apiserver 的 60 秒超时。
	if svc.Spec.Type == v1.ServiceTypeExternalName {
		ipLifetime := r.clock.Now().Sub(ipAddress.CreationTimestamp.Time)
		gracePeriod := 60 * time.Second
		if ipLifetime < gracePeriod {
			// 宽限期结束后重新入队
			r.ipQueue.AddAfter(key, gracePeriod-ipLifetime)
			return nil
		}
	}

	// Service 存储实现了事务：先创建 IPAddress 对象，再创建 Service 对象，
	// 如果 Service 对象已存在，整个事务会被回滚。当调和循环在事务回滚之前
	// 就拿到了新的 IPAddress 对象时，就可能发生竞态，
	// 产生虚假的 IPAddressWrongReference 告警。为了抑制这类告警，
	// 我们把新 IPAddress 对象的处理延迟 5 秒。Service 分配流程会先创建 IPAddress
	// 再创建 Service，因此当 IPAddress 的创建时间晚于 Service 的创建时间时，
	// 就能识别出这种场景。我们只在 IPAddress 是最近创建时才这样做，
	// 以免 Service 事务回滚失败时，IPAddress 的清理被无限期推迟。
	if ipAddress.CreationTimestamp.After(svc.CreationTimestamp.Time) &&
		r.clock.Now().Sub(ipAddress.CreationTimestamp.Time) < 5*time.Second {
		// 宽限期结束后重新入队
		r.ipQueue.AddAfter(key, 5*time.Second)
		return nil
	}
	runtime.HandleError(fmt.Errorf("the IPAddress: %s for Service %s/%s has a wrong reference %#v; cleaning up", ipAddress.Name, svc.Name, svc.Namespace, ipAddress.Spec.ParentRef))
	r.recorder.Eventf(ipAddress, nil, v1.EventTypeWarning, "IPAddressWrongReference", "IPAddressAllocation", "IPAddress: %s for Service %s/%s has a wrong reference; cleaning up", ipAddress.Name, svc.Namespace, svc.Name)
	err = r.client.NetworkingV1().IPAddresses().Delete(context.Background(), ipAddress.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil

}

// 如果该 IP 不在任何 ServiceCIDR 范围内，isIPOutOfRange 返回 true。
func (r *RepairIPAddress) isIPOutOfRange(ip net.IP) bool {
	return len(servicecidr.ContainsIP(r.serviceCIDRLister, ip)) == 0
}

// newIPAddress 构建由 Service IP allocator 管理的 IPAddress，并写入地址族、管理者标签和父 Service 引用。
func newIPAddress(name string, svc *v1.Service) *networkingv1.IPAddress {
	family := string(v1.IPv4Protocol)
	if netutils.IsIPv6String(name) {
		family = string(v1.IPv6Protocol)
	}
	return &networkingv1.IPAddress{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				networkingv1.LabelIPAddressFamily: family,
				networkingv1.LabelManagedBy:       ipallocator.ControllerName,
			},
		},
		Spec: networkingv1.IPAddressSpec{
			ParentRef: serviceToRef(svc),
		},
	}
}

func serviceToRef(svc *v1.Service) *networkingv1.ParentReference {
	if svc == nil {
		return nil
	}

	return &networkingv1.ParentReference{
		Group:     "",
		Resource:  "services",
		Namespace: svc.Namespace,
		Name:      svc.Name,
	}
}

func getFamilyByIP(ip net.IP) v1.IPFamily {
	if netutils.IsIPv6(ip) {
		return v1.IPv6Protocol
	}
	return v1.IPv4Protocol
}

// managedByController 返回给定 IPAddress 的 manager 标签是否为
// ipallocator 控制器名。
func managedByController(ip *networkingv1.IPAddress) bool {
	managedBy, ok := ip.Labels[networkingv1.LabelManagedBy]
	if !ok {
		return false
	}
	return managedBy == ipallocator.ControllerName
}

func verifyIPAddressLabels(ip *networkingv1.IPAddress) bool {
	labelFamily, ok := ip.Labels[networkingv1.LabelIPAddressFamily]
	if !ok {
		return false
	}

	family := string(v1.IPv4Protocol)
	if netutils.IsIPv6String(ip.Name) {
		family = string(v1.IPv6Protocol)
	}
	if family != labelFamily {
		return false
	}
	return managedByController(ip)
}
