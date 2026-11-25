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

package ipallocator

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	networkingv1informers "k8s.io/client-go/informers/networking/v1"
	networkingv1client "k8s.io/client-go/kubernetes/typed/networking/v1"
	networkingv1listers "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/servicecidr"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
	netutils "k8s.io/utils/net"
)

// MetaAllocator 为每个 ServiceCIDR 管理独立 IP 分配器；重叠 CIDR 共用地址引用，只有 Ready 的分配器才可分配新 IP。
// MetaAllocator 为每个 ServiceCIDR 管理独立 IP 分配器；CIDR 允许重叠，因此必须考虑多个分配器覆盖同一地址的情况。
// 分配器本身不保存 IPAddress 对象，而是从 Informer 缓存读取，因此创建和删除分配器的成本较低。
// 自动分配时，可以使用任意一个已就绪且仍有可用地址的分配器。
// MetaAllocator 基于 ServiceCIDR 和 IPAddress API 对象实现 IP 地址分配接口。
type MetaAllocator struct {
	client            networkingv1client.NetworkingV1Interface
	serviceCIDRLister networkingv1listers.ServiceCIDRLister
	serviceCIDRSynced cache.InformerSynced
	ipAddressLister   networkingv1listers.IPAddressLister
	ipAddressSynced   cache.InformerSynced
	ipAddressInformer networkingv1informers.IPAddressInformer
	queue             workqueue.TypedRateLimitingInterface[string]

	internalStopCh chan struct{}

	// allocators 以网络前缀为键保存分配器。多个 ServiceCIDR 可能包含相同前缀，
	// 因此每个分配器还需要记录引用它的 ServiceCIDR 集合。
	mu         sync.Mutex
	allocators map[string]*item

	ipFamily api.IPFamily
	metrics  bool // 启用指标采集

	// TODO(aojea)：DisableAllocatorDualWrite 特性门控完成迁移后，移除此旧位图分配器兼容字段。
	bitmapAllocator Interface
}

type item struct {
	allocator    *Allocator
	serviceCIDRs sets.Set[string] // 引用正在使用该 Allocator 的 serviceCIDRs
}

var _ Interface = &MetaAllocator{}

// NewMetaAllocator 创建基于 IPAddress 和 ServiceCIDR 对象跟踪已分配 IP 的分配器，并通过 Informer 缓存读取存储状态。
// NewMetaAllocator 以 ServiceCIDR/IPAddress Informer 构建多 CIDR 分配器，并启动负责同步各 CIDR 地址池的后台控制循环。
func NewMetaAllocator(
	client networkingv1client.NetworkingV1Interface,
	serviceCIDRInformer networkingv1informers.ServiceCIDRInformer,
	ipAddressInformer networkingv1informers.IPAddressInformer,
	isIPv6 bool,
	bitmapAllocator Interface,
) (*MetaAllocator, error) {

	c := newMetaAllocator(client, serviceCIDRInformer, ipAddressInformer, isIPv6, bitmapAllocator)
	go c.run()
	return c, nil
}

// newMetaAllocator 构建 IP 分配器，主要供测试使用。
func newMetaAllocator(client networkingv1client.NetworkingV1Interface,
	serviceCIDRInformer networkingv1informers.ServiceCIDRInformer,
	ipAddressInformer networkingv1informers.IPAddressInformer,
	isIPv6 bool,
	bitmapAllocator Interface,
) *MetaAllocator {
	// TODO：让 NewMetaAllocator 不再依赖调用方指定 IP 协议族。
	family := api.IPv4Protocol
	if isIPv6 {
		family = api.IPv6Protocol
	}

	c := &MetaAllocator{
		client:            client,
		serviceCIDRLister: serviceCIDRInformer.Lister(),
		serviceCIDRSynced: serviceCIDRInformer.Informer().HasSynced,
		ipAddressLister:   ipAddressInformer.Lister(),
		ipAddressSynced:   ipAddressInformer.Informer().HasSynced,
		ipAddressInformer: ipAddressInformer,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: ControllerName},
		),
		internalStopCh:  make(chan struct{}),
		allocators:      make(map[string]*item),
		ipFamily:        family,
		metrics:         false,
		bitmapAllocator: bitmapAllocator,
	}

	_, _ = serviceCIDRInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueueServiceCIDR,
		UpdateFunc: func(old, new interface{}) {
			c.enqueueServiceCIDR(new)
		},
		// 在事件处理器中直接处理删除事件，以便读取对象字段，而不必再次从缓存获取。
		// ServiceCIDR 受 Finalizer 保护，因此“开始删除”的后续逻辑仍由协调循环处理。
		DeleteFunc: c.deleteServiceCIDR,
	})

	return c
}

func (c *MetaAllocator) enqueueServiceCIDR(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil {
		c.queue.Add(key)
	}
}

func (c *MetaAllocator) deleteServiceCIDR(obj interface{}) {
	serviceCIDR, ok := obj.(*networkingv1.ServiceCIDR)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		serviceCIDR, ok = tombstone.Obj.(*networkingv1.ServiceCIDR)
		if !ok {
			return
		}
	}
	klog.Infof("deleting ClusterIP allocator for Service CIDR %v", serviceCIDR)

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cidr := range serviceCIDR.Spec.CIDRs {
		// 跳过当前 MetaAllocator 不支持的 IP 协议族。
		if c.ipFamily != api.IPFamily(convertToV1IPFamily(netutils.IPFamilyOfCIDRString(cidr))) {
			continue
		}
		// 获取该 ServiceCIDR 使用的分配器。
		v, ok := c.allocators[cidr]
		if !ok {
			continue
		}
		// 从分配器的引用集合中移除此 ServiceCIDR。
		v.serviceCIDRs.Delete(serviceCIDR.Name)
		if v.serviceCIDRs.Len() > 0 {
			klog.V(2).Infof("deleted Service CIDR from allocator %s, remaining %v", cidr, v.serviceCIDRs)
		} else {
			// 如果该分配器已没有任何 ServiceCIDR 引用，则销毁分配器并从映射中删除。
			v.allocator.Destroy()
			delete(c.allocators, cidr)
			klog.Infof("deleted ClusterIP allocator for Service CIDR %s", cidr)
		}
	}
}

// run 等待 ServiceCIDR 与 IPAddress 缓存同步后启动单 worker，按队列顺序重建/更新 CIDR 分配器。
func (c *MetaAllocator) run() {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()
	klog.Info("starting ServiceCIDR Allocator Controller")
	defer klog.Info("stopping ServiceCIDR Allocator Controller")

	// 等待所有相关 Informer 缓存同步完成，再开始处理工作队列中的事件。
	if !cache.WaitForCacheSync(c.internalStopCh, c.serviceCIDRSynced, c.ipAddressSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	// 使用单 worker 串行处理，每次只协调一个 ServiceCIDR。
	go wait.Until(c.runWorker, time.Second, c.internalStopCh)

	<-c.internalStopCh
}

func (c *MetaAllocator) runWorker() {
	for c.processNextItem() {
	}
}

func (c *MetaAllocator) processNextItem() bool {
	// 等待工作队列中出现新任务。
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	err := c.syncAllocators()
	// 处理业务逻辑执行过程中发生的错误，并按限速策略决定是否重新入队。
	if err != nil {
		if c.queue.NumRequeues(key) < 5 {
			klog.Infof("error syncing cidr %v: %v", key, err)
			c.queue.AddRateLimited(key)
			return true
		}
	}
	c.queue.Forget(key)
	return true
}

// syncAllocators 根据 ServiceCIDR 创建分配器并同步其就绪状态；分配器删除由事件处理器直接完成。
// syncAllocators 根据 ServiceCIDR 列表建立或更新按 CIDR 索引的分配器；同一前缀可由多个 ServiceCIDR 引用。
func (c *MetaAllocator) syncAllocators() error {
	start := time.Now()
	klog.V(2).Info("syncing ServiceCIDR allocators")
	defer func() {
		klog.V(2).Infof("syncing ServiceCIDR allocators took: %v", time.Since(start))
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	serviceCIDRs, err := c.serviceCIDRLister.List(labels.Everything())
	if err != nil {
		return err
	}

	for _, serviceCIDR := range serviceCIDRs {
		for _, cidr := range serviceCIDR.Spec.CIDRs {
			// 跳过当前 MetaAllocator 不支持的 IP 协议族。
			if c.ipFamily != api.IPFamily(convertToV1IPFamily(netutils.IPFamilyOfCIDRString(cidr))) {
				continue
			}
			// 只有 ServiceCIDR 已就绪且未进入删除流程时，对应分配器才可用于新分配。
			ready := false
			if isReady(serviceCIDR) && serviceCIDR.DeletionTimestamp.IsZero() {
				ready = true
			}

			// 检查当前 CIDR 是否已有对应分配器。
			v, ok := c.allocators[cidr]
			// 将当前 ServiceCIDR 加入已有分配器的引用集合。
			if ok {
				v.serviceCIDRs.Insert(serviceCIDR.Name)
				// 只要引用该分配器的任意一个 ServiceCIDR 就绪，分配器就可以用于新分配。
				if ready {
					v.allocator.ready.Store(true)
				} else if v.serviceCIDRs.Has(serviceCIDR.Name) && len(v.serviceCIDRs) == 1 {
					v.allocator.ready.Store(false)
				}
				klog.Infof("updated ClusterIP allocator for Service CIDR %s", cidr)
				continue
			}

			// 为该 ServiceCIDR 创建新的分配器。
			_, ipnet, err := netutils.ParseCIDRSloppy(cidr) // 此前已经校验过
			if err != nil {
				klog.Infof("error parsing cidr %s: %v", cidr, err)
				continue
			}
			// 遇到新的 ServiceCIDR，创建对应的分配器。
			allocator, err := NewIPAllocator(ipnet, c.client, c.ipAddressInformer)
			if err != nil {
				klog.Infof("error creating new IPAllocator for Service CIDR %s: %v", cidr, err)
				continue
			}
			if c.metrics {
				allocator.EnableMetrics()
			}
			allocator.ready.Store(ready)
			c.allocators[cidr] = &item{
				allocator:    allocator,
				serviceCIDRs: sets.New[string](serviceCIDR.Name),
			}
			klog.Infof("created ClusterIP allocator for Service CIDR %s", cidr)
		}
	}
	return nil
}

// getAllocator 查找包含目标 IP 的任意分配器。ready 为 true 时，只返回已就绪的分配器。
// 分配操作必须使用已就绪的分配器；由于各分配器通过缓存看到相同的 IPAddress，选择哪个覆盖该地址的分配器并不重要。
// 释放操作则可以使用任意匹配的分配器，不受其就绪状态限制。
// getAllocator 查找包含目标 IP 的 CIDR 分配器；分配时要求 Ready，释放时允许使用非 Ready 分配器。
func (c *MetaAllocator) getAllocator(ip net.IP, ready bool) (*Allocator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	address := servicecidr.IPToAddr(ip)
	// 使用第一个包含目标地址的分配器。
	for cidr, item := range c.allocators {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, err
		}
		if servicecidr.PrefixContainsIP(prefix, address) {
			if !ready {
				return item.allocator, nil
			}
			if item.allocator.ready.Load() {
				return item.allocator, nil
			}
		}
	}
	klog.V(2).Infof("Could not get allocator for IP %s", ip.String())
	return nil, ErrMismatchedNetwork
}

func (c *MetaAllocator) AllocateService(service *api.Service, ip net.IP) error {
	allocator, err := c.getAllocator(ip, true)
	if err != nil {
		return err
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.DisableAllocatorDualWrite) {
		cidr := c.bitmapAllocator.CIDR()
		if cidr.Contains(ip) {
			err := c.bitmapAllocator.Allocate(ip)
			if err != nil {
				return err
			}
		}
	}
	return allocator.AllocateService(service, ip)
}

// Allocate 尝试预留指定 IP。地址不在范围内时返回 ErrNotInRange，已被占用时返回 ErrAllocated，
// 地址池耗尽时返回 ErrFull。该方法主要供测试使用；由于创建 IPAddress 需要关联 Service，
// 未提供 Service 引用时可能无法成功创建对应对象。
func (c *MetaAllocator) Allocate(ip net.IP) error {
	return c.AllocateService(nil, ip)

}

// AllocateNextService 在双写迁移期先尝试旧位图池，再回退到新 ServiceCIDR/IPAddress 分配器，以保持升级兼容。
func (c *MetaAllocator) AllocateNextService(service *api.Service) (net.IP, error) {
	// 在双写迁移阶段，如果集群仍启用旧分配器，则优先尝试从旧分配器获取 IP，以保持向后兼容。
	if !utilfeature.DefaultFeatureGate.Enabled(features.DisableAllocatorDualWrite) {
		ip, err := c.bitmapAllocator.AllocateNext()
		if err == nil {
			allocator, err := c.getAllocator(ip, true)
			if err != nil {
				return nil, err
			}
			return ip, allocator.AllocateService(service, ip)
		} else {
			klog.Infof("no IP address available on the old ClusterIP allocator, trying to get a new address using the new allocators")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// TODO(aojea)：增加随机选择分配器的策略，并根据各分配器的地址容量调整权重。
	// 例如，分配器 A 和 B 分别有 256、1024 个可用地址时，选择 B 的概率应为选择 A 的 4 倍，
	// 从而让地址分配负载按容量随机分散。该策略需在进入 Beta 前进一步验证。
	isIPv6 := c.ipFamily == api.IPFamily(v1.IPv6Protocol)
	for cidr, item := range c.allocators {
		if netutils.IsIPv6CIDRString(cidr) != isIPv6 {
			continue
		}
		ip, err := item.allocator.AllocateNextService(service)
		if err == nil {
			return ip, nil
		}
		// 只有当前分配器已满或尚未就绪时，才继续尝试下一个分配器。
		if !errors.Is(err, ErrFull) && !errors.Is(err, ErrNotReady) {
			return nil, err
		}
	}
	return nil, ErrFull
}

// AllocateNext 返回一个尚未分配的 IP。该方法主要供测试使用；由于创建 IPAddress 需要 Service 引用，
// 未提供 Service 时可能无法创建对应对象。
func (c *MetaAllocator) AllocateNext() (net.IP, error) {
	return c.AllocateNextService(nil)
}

func (c *MetaAllocator) Release(ip net.IP) error {
	allocator, err := c.getAllocator(ip, false)
	if err != nil {
		return err
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.DisableAllocatorDualWrite) {
		cidr := c.bitmapAllocator.CIDR()
		if cidr.Contains(ip) {
			_ = c.bitmapAllocator.Release(ip)
		}
	}
	return allocator.Release(ip)

}
func (c *MetaAllocator) ForEach(f func(ip net.IP)) {
	ipLabelSelector := labels.Set(map[string]string{
		networkingv1.LabelIPAddressFamily: string(c.IPFamily()),
		networkingv1.LabelManagedBy:       ControllerName,
	}).AsSelectorPreValidated()
	ips, err := c.ipAddressLister.List(ipLabelSelector)
	if err != nil {
		return
	}
	for _, ip := range ips {
		f(netutils.ParseIPSloppy(ip.Name))
	}
}

func (c *MetaAllocator) CIDR() net.IPNet {
	return net.IPNet{}

}
func (c *MetaAllocator) IPFamily() api.IPFamily {
	return c.ipFamily
}
func (c *MetaAllocator) Has(ip net.IP) bool {
	allocator, err := c.getAllocator(ip, true)
	if err != nil {
		return false
	}
	return allocator.Has(ip)
}
func (c *MetaAllocator) Destroy() {
	select {
	case <-c.internalStopCh:
	default:
		if !utilfeature.DefaultFeatureGate.Enabled(features.DisableAllocatorDualWrite) {
			c.bitmapAllocator.Destroy()
		}
		close(c.internalStopCh)
	}
}

// 仅供测试使用。
func (c *MetaAllocator) Used() int {
	ipLabelSelector := labels.Set(map[string]string{
		networkingv1.LabelIPAddressFamily: string(c.IPFamily()),
		networkingv1.LabelManagedBy:       ControllerName,
	}).AsSelectorPreValidated()
	ips, err := c.ipAddressLister.List(ipLabelSelector)
	if err != nil {
		return 0
	}
	return len(ips)
}

// 仅供测试使用。
func (c *MetaAllocator) Free() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := 0
	prefixes := []netip.Prefix{}
	// 获取所有现存的网络前缀。
	for cidr := range c.allocators {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			continue
		}
		prefixes = append(prefixes, prefix)
	}
	// 只统计最顶层前缀，避免重叠 CIDR 导致重复计数。
	for _, prefix := range prefixes {
		if !isNotContained(prefix, prefixes) {
			continue
		}
		v, ok := c.allocators[prefix.String()]
		if !ok {
			continue
		}
		size += int(v.allocator.size)
	}
	return size - c.Used()
}

func (c *MetaAllocator) EnableMetrics() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = true
	for _, item := range c.allocators {
		item.allocator.EnableMetrics()
	}
}

// DryRun 返回一个随机的分配器
func (c *MetaAllocator) DryRun() Interface {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, item := range c.allocators {
		return item.allocator.DryRun()
	}
	return &Allocator{}
}

func isReady(serviceCIDR *networkingv1.ServiceCIDR) bool {
	if serviceCIDR == nil {
		return false
	}

	for _, condition := range serviceCIDR.Status.Conditions {
		if condition.Type == networkingv1.ServiceCIDRConditionReady {
			return condition.Status == metav1.ConditionStatus(metav1.ConditionTrue)
		}
	}
	// 假定 ServiceCIDR 已就绪，以便在 kube-controller-manager 未运行时仍能处理相关场景。
	return true
}

// 将 netutils.IPFamily 转换为 v1.IPFamily。
// TODO：统一此类辅助函数，避免与 pkg/proxy/util/utils.go 中的实现重复。
func convertToV1IPFamily(ipFamily netutils.IPFamily) v1.IPFamily {
	switch ipFamily {
	case netutils.IPv4:
		return v1.IPv4Protocol
	case netutils.IPv6:
		return v1.IPv6Protocol
	}

	return v1.IPFamilyUnknown
}

// 如果该前缀不被任何一个传入的前缀包含，
// isNotContained 返回 true。
func isNotContained(prefix netip.Prefix, prefixes []netip.Prefix) bool {
	for _, p := range prefixes {
		// 跳过与目标前缀完全相同的前缀。
		if prefix == p {
			continue
		}
		// 例如，192.168.0.0/24 位于 192.168.0.0/16 的地址范围内。
		if prefix.Overlaps(p) && prefix.Bits() >= p.Bits() {
			return false
		}
	}
	return true
}
