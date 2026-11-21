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

package servicecidrs

import (
	"context"
	"encoding/json"
	"net/netip"
	"slices"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	networkingapiv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	metav1apply "k8s.io/client-go/applyconfigurations/meta/v1"
	networkingapiv1apply "k8s.io/client-go/applyconfigurations/networking/v1"
	networkinginformers "k8s.io/client-go/informers/networking/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	networkinglisters "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/servicecidr"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	netutils "k8s.io/utils/net"
)

const (
	// maxRetries 是 Service 对象在从队列中丢弃之前可重试的最大次数。
	// 以当前的限速器（5ms*2^(maxRetries-1)）计算，以下数字表示
	// 同一 Service 连续入队之间的延迟序列：
	//
	// 5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.3s, 2.6s, 5.1s, 10.2s, 20.4s, 41s, 82s
	maxRetries     = 15
	controllerName = "service-cidr-controller"

	// ServiceCIDRProtectionFinalizer 是控制器添加到 ServiceCIDR 上的保护 finalizer，
	// 确保删除前先完成状态传播与依赖检查。
	ServiceCIDRProtectionFinalizer = "networking.k8s.io/service-cidr-finalizer"

	// deletionGracePeriod 是移除 ServiceCIDR finalizer 前等待的秒数，用于确保删除信息
	// 已传播到 apiserver 的分配器，避免在完成 ServiceCIDR 删除前又分配出新的 IP 地址。
	deletionGracePeriod = 10 * time.Second
)

// NewController 返回一个新的 *Controller。
func NewController(
	ctx context.Context,
	serviceCIDRInformer networkinginformers.ServiceCIDRInformer,
	ipAddressInformer networkinginformers.IPAddressInformer,
	client clientset.Interface,
) *Controller {
	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: controllerName})
	c := &Controller{
		client: client,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "ipaddresses"},
		),
		workerLoopPeriod: time.Second,
	}

	_, _ = serviceCIDRInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addServiceCIDR,
		UpdateFunc: c.updateServiceCIDR,
		DeleteFunc: c.deleteServiceCIDR,
	})
	c.serviceCIDRLister = serviceCIDRInformer.Lister()
	c.serviceCIDRsSynced = serviceCIDRInformer.Informer().HasSynced

	_, _ = ipAddressInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addIPAddress,
		DeleteFunc: c.deleteIPAddress,
	})

	c.ipAddressLister = ipAddressInformer.Lister()
	c.ipAddressSynced = ipAddressInformer.Informer().HasSynced

	c.eventBroadcaster = broadcaster
	c.eventRecorder = recorder

	return c
}

// Controller 管理 Service IP 地址段（ServiceCIDR）的生命周期，
// 包括保护 finalizer 的添加/移除以及 Ready 状态的维护。
type Controller struct {
	client           clientset.Interface
	eventBroadcaster record.EventBroadcaster
	eventRecorder    record.EventRecorder

	serviceCIDRLister  networkinglisters.ServiceCIDRLister
	serviceCIDRsSynced cache.InformerSynced

	ipAddressLister networkinglisters.IPAddressLister
	ipAddressSynced cache.InformerSynced

	queue workqueue.TypedRateLimitingInterface[string]

	// workerLoopPeriod 是 worker 两次运行之间的间隔。worker 处理 Service 和 IP 地址段变更队列。
	workerLoopPeriod time.Duration
}

// Run 在 ctx 结束前不会返回。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)

	c.eventBroadcaster.StartStructuredLogging(3)
	c.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.client.CoreV1().Events("")})
	defer c.eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting", "controller", controllerName)

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down", "controller", controllerName)
		c.queue.ShutDown()
		wg.Wait()
	}()

	// 等缓存同步后再启动 worker，确保 lister 能看到完整的对象。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.serviceCIDRsSynced, c.ipAddressSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.worker, c.workerLoopPeriod)
		})
	}
	<-ctx.Done()
}

// addServiceCIDR 响应 ServiceCIDR 添加事件，入队自身及所有重叠的 ServiceCIDR。
func (c *Controller) addServiceCIDR(obj interface{}) {
	cidr, ok := obj.(*networkingapiv1.ServiceCIDR)
	if !ok {
		return
	}
	c.queue.Add(cidr.Name)
	// CIDR 范围的增删会改变重叠关系，需重新评估其他 ServiceCIDR 的删除阻塞状态。
	for _, key := range c.overlappingServiceCIDRs(cidr) {
		c.queue.Add(key)
	}
}

// updateServiceCIDR 响应 ServiceCIDR 更新事件，将其加入工作队列。
func (c *Controller) updateServiceCIDR(oldObj, obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil {
		c.queue.Add(key)
	}
}

// deleteServiceCIDR 响应 ServiceCIDR 删除事件。
func (c *Controller) deleteServiceCIDR(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err == nil {
		c.queue.Add(key)
	}
}

// addIPAddress 响应 IPAddress 添加事件；新增 IP 可能阻塞某个 ServiceCIDR 的删除。
func (c *Controller) addIPAddress(obj interface{}) {
	ip, ok := obj.(*networkingapiv1.IPAddress)
	if !ok {
		return
	}

	for _, cidr := range c.containingServiceCIDRs(ip) {
		c.queue.Add(cidr)
	}
}

// deleteIPAddress 响应 IPAddress 删除事件；删除 IP 可能解除对某个 ServiceCIDR 删除的阻塞。
func (c *Controller) deleteIPAddress(obj interface{}) {
	ip, ok := obj.(*networkingapiv1.IPAddress)
	if !ok {
		// 删除事件可能携带 tombstone，需先解包出真正的 IPAddress。
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		ip, ok = tombstone.Obj.(*networkingapiv1.IPAddress)
		if !ok {
			return
		}
	}

	for _, cidr := range c.containingServiceCIDRs(ip) {
		c.queue.Add(cidr)
	}
}

// overlappingServiceCIDRs 返回与给定 ServiceCIDR 存在包含或被包含关系的所有 ServiceCIDR 名称。
// 增删 CIDR 时需要重新计算每个相关 ServiceCIDR 的状态，以判断是否可解除删除阻塞。
func (c *Controller) overlappingServiceCIDRs(serviceCIDR *networkingapiv1.ServiceCIDR) []string {
	result := sets.New[string]()
	for _, cidr := range serviceCIDR.Spec.CIDRs {
		// 若 CIDR 字符串为空，ParsePrefix 会返回错误。
		if prefix, err := netip.ParsePrefix(cidr); err == nil {
			serviceCIDRs := servicecidr.OverlapsPrefix(c.serviceCIDRLister, prefix)
			for _, v := range serviceCIDRs {
				result.Insert(v.Name)
			}
		}
	}

	return result.UnsortedList()
}

// containingServiceCIDRs 返回包含给定 IPAddress 的所有 ServiceCIDR 名称，
// 因为这些 ServiceCIDR 可能阻塞该 IP，也可能被该 IP 阻塞删除。
func (c *Controller) containingServiceCIDRs(ip *networkingapiv1.IPAddress) []string {
	// 只处理由 kube-apiserver 管理的 IP。
	managedBy, ok := ip.Labels[networkingapiv1.LabelManagedBy]
	if !ok || managedBy != ipallocator.ControllerName {
		return []string{}
	}

	address, err := netip.ParseAddr(ip.Name)
	if err != nil {
		// 理论上不会出现：IPAddress 对象已校验名称是合法 IP。
		return []string{}
	}

	result := sets.New[string]()
	serviceCIDRs := servicecidr.ContainsAddress(c.serviceCIDRLister, address)
	for _, v := range serviceCIDRs {
		result.Insert(v.Name)
	}

	return result.UnsortedList()
}

// worker 持续处理工作队列，直到队列关闭。
func (c *Controller) worker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

// processNext 从队列取出一个键并同步，失败时按重试次数决定限流重试或丢弃。
func (c *Controller) processNext(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.sync(ctx, key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}
	logger := klog.FromContext(ctx)
	if c.queue.NumRequeues(key) < maxRetries {
		logger.V(2).Info("Error syncing ServiceCIDR, retrying", "ServiceCIDR", key, "err", err)
		c.queue.AddRateLimited(key)
	} else {
		// 超过最大重试次数则丢弃，避免错误项无限占用队列。
		logger.Info("Dropping ServiceCIDR out of the queue", "ServiceCIDR", key, "err", err)
		c.queue.Forget(key)
		utilruntime.HandleErrorWithContext(ctx, err, "ServiceCIDR sync failed after max retries")
	}
	return true
}

// sync 同步单个 ServiceCIDR：处理删除流程、维护 finalizer 与 Ready 状态。
func (c *Controller) sync(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished syncing ServiceCIDR)", "ServiceCIDR", key, "elapsed", time.Since(startTime))
	}()

	logger.V(4).Info("syncing ServiceCIDR", "ServiceCIDR", key)
	cidr, err := c.serviceCIDRLister.Get(key)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// ServiceCIDR 已不存在，无需处理。
			logger.V(4).Info("ServiceCIDR no longer exist", "ServiceCIDR", key)
			return nil
		}
		return err
	}

	// 删除流程。
	if !cidr.GetDeletionTimestamp().IsZero() {
		// 移除 finalizer 前先确认该 ServiceCIDR 可以安全删除。
		ok, err := c.canDeleteCIDR(ctx, cidr)
		if err != nil {
			return err
		}
		if !ok {
			// 更新状态说明无法删除的原因；任何相关 ServiceCIDR 或 IPAddress 对象
			// 的事件都会重新评估，可能最终解除该条件。
			condition := metav1.Condition{
				Type:               networkingapiv1.ServiceCIDRConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             networkingapiv1.ServiceCIDRReasonTerminating,
				Message:            "There are still IPAddresses referencing the ServiceCIDR, please remove them or create a new ServiceCIDR",
				LastTransitionTime: metav1.Now(),
			}
			return c.updateConditionIfNeeded(ctx, cidr, condition)
		}
		// 若没有 IPAddress 依赖该 ServiceCIDR，删除是安全的。但分配器可能仍认为
		// 它处于 Ready 状态并从中分配新 IP；为避免竞态，需等待一段宽限期，
		// 确保删除变更已传播到分配器、不会再有新 IP 被分配。
		timeUntilDeleted := deletionGracePeriod - time.Since(cidr.GetDeletionTimestamp().Time)
		if timeUntilDeleted > 0 {
			c.queue.AddAfter(key, timeUntilDeleted)
			return nil
		}
		return c.removeServiceCIDRFinalizerIfNeeded(ctx, cidr)
	}

	// 创建或更新：ServiceCIDR 必须带有 finalizer。
	err = c.addServiceCIDRFinalizerIfNeeded(ctx, cidr)
	if err != nil {
		return err
	}

	condition := metav1.Condition{
		Type:               networkingapiv1.ServiceCIDRConditionReady,
		Status:             metav1.ConditionTrue,
		Message:            "Kubernetes Service CIDR is ready",
		LastTransitionTime: metav1.Now(),
	}
	if err := c.updateConditionIfNeeded(ctx, cidr, condition); err != nil {
		logger.Info("error updating default ServiceCIDR status", "error", err)
		c.eventRecorder.Eventf(cidr, v1.EventTypeWarning, "KubernetesServiceCIDRError", "The ServiceCIDR Status can not be set to Ready=True")
		return err
	}

	return nil
}

// canDeleteCIDR 检查 ServiceCIDR 是否可以安全删除而不会遗留孤立 IPAddress。
func (c *Controller) canDeleteCIDR(ctx context.Context, serviceCIDR *networkingapiv1.ServiceCIDR) (bool, error) {
	logger := klog.FromContext(ctx)
	// 检查是否存在已经包含待删除 ServiceCIDR 的上级子网。
	hasParent := true
	for _, cidr := range serviceCIDR.Spec.CIDRs {
		// 查找是否有更大的子网包含当前子网，或存在子网完全相同的其他 ServiceCIDR。
		if prefix, err := netip.ParsePrefix(cidr); err == nil {
			serviceCIDRs := servicecidr.ContainsPrefix(c.serviceCIDRLister, prefix)
			if len(serviceCIDRs) == 0 ||
				len(serviceCIDRs) == 1 && serviceCIDRs[0].Name == serviceCIDR.Name {
				hasParent = false
			}
		}
	}

	// 所有已分配的 IP 都会被上级 ServiceCIDR 包含，可安全删除并移除 finalizer。
	if hasParent {
		logger.Info("Deleting ServiceCIDR contained in other ServiceCIDR", "ServiceCIDR", serviceCIDR.String())
		return true, nil
	}

	// TODO: 后续优化此处。
	// 当前 ServiceCIDR 没有其他 ServiceCIDR 包含它，
	// 需验证不存在仅引用它、删除后会成为孤立的 IPAddress。
	for _, cidr := range serviceCIDR.Spec.CIDRs {
		// 通过标签筛选获取该地址族下所有由 apiserver 分配的 IP。
		ipLabelSelector := labels.Set(map[string]string{
			networkingapiv1.LabelIPAddressFamily: string(convertToV1IPFamily(netutils.IPFamilyOfCIDRString(cidr))),
			networkingapiv1.LabelManagedBy:       ipallocator.ControllerName,
		}).AsSelectorPreValidated()
		ips, err := c.ipAddressLister.List(ipLabelSelector)
		if err != nil {
			return false, err
		}
		for _, ip := range ips {
			// 若最长前缀匹配到的正是待删除 ServiceCIDR 且它是唯一匹配项，
			// 删除后至少会遗留一个孤立 IPAddress，须阻塞删除。
			address, err := netip.ParseAddr(ip.Name)
			if err != nil {
				// IPAddress 对象已校验名称合法，实际上不应发生。
				logger.Info("[SHOULD NOT HAPPEN] unexpected error parsing IPAddress", "IPAddress", ip.Name, "error", err)
				continue
			}
			// 查找所有包含该 IP 的 ServiceCIDR。
			serviceCIDRs := servicecidr.ContainsAddress(c.serviceCIDRLister, address)
			if len(serviceCIDRs) == 1 && serviceCIDRs[0].Name == serviceCIDR.Name {
				logger.Info("Deleting ServiceCIDR blocked by IP address", "IPAddress", address.String())
				return false, nil
			}
		}
	}

	// 不存在仅依赖该 ServiceCIDR 的 IPAddress，可安全删除并移除 finalizer。
	logger.Info("Deleting ServiceCIDR no longer have orphan IPs", "ServiceCIDR", serviceCIDR.String())
	return true, nil
}

// addServiceCIDRFinalizerIfNeeded 在 ServiceCIDR 尚无保护 finalizer 时添加它。
func (c *Controller) addServiceCIDRFinalizerIfNeeded(ctx context.Context, cidr *networkingapiv1.ServiceCIDR) error {
	if slices.Contains(cidr.GetFinalizers(), ServiceCIDRProtectionFinalizer) {
		return nil
	}

	// 使用 Strategic Merge Patch 添加 finalizer，避免整对象更新覆盖其他字段。
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"finalizers": []string{ServiceCIDRProtectionFinalizer},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = c.client.NetworkingV1().ServiceCIDRs().Patch(ctx, cidr.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	// 对象可能已被删除，NotFound 属于正常情况。
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	klog.FromContext(ctx).V(4).Info("Added protection finalizer to ServiceCIDR", "ServiceCIDR", cidr.Name)
	return nil

}

// removeServiceCIDRFinalizerIfNeeded 在 ServiceCIDR 仍带有保护 finalizer 时将其移除。
func (c *Controller) removeServiceCIDRFinalizerIfNeeded(ctx context.Context, cidr *networkingapiv1.ServiceCIDR) error {
	if !slices.Contains(cidr.GetFinalizers(), ServiceCIDRProtectionFinalizer) {
		return nil
	}
	// 通过 strategic merge patch 的删除指令移除 finalizer 列表中的指定项。
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"$deleteFromPrimitiveList/finalizers": []string{ServiceCIDRProtectionFinalizer},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = c.client.NetworkingV1().ServiceCIDRs().Patch(ctx, cidr.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	klog.FromContext(ctx).V(4).Info("Removed protection finalizer from ServiceCIDRs", "ServiceCIDR", cidr.Name)
	return nil
}

// updateConditionIfNeeded 在必要时更新 ServiceCIDR 的状态条件。
func (c *Controller) updateConditionIfNeeded(ctx context.Context, cidr *networkingapiv1.ServiceCIDR, newCondition metav1.Condition) error {
	logger := klog.FromContext(ctx)
	currentCondition := apimeta.FindStatusCondition(cidr.Status.Conditions, newCondition.Type)
	// 条件已存在且完全一致，无需更新。
	if currentCondition != nil &&
		currentCondition.Status == newCondition.Status &&
		currentCondition.Reason == newCondition.Reason &&
		currentCondition.Message == newCondition.Message {
		logger.V(4).Info("ServiceCIDR condition already up to date", "ServiceCIDR", cidr.Name, "conditionType", newCondition.Type)
		return nil
	}

	logger.V(2).Info("Updating ServiceCIDR condition", "ServiceCIDR", cidr.Name, "conditionType", newCondition.Type, "newStatus", newCondition.Status, "newReason", newCondition.Reason)

	svcApplyStatus := networkingapiv1apply.ServiceCIDRStatus().WithConditions(
		metav1apply.Condition().
			WithType(newCondition.Type).
			WithStatus(newCondition.Status).
			WithReason(newCondition.Reason).
			WithMessage(newCondition.Message).
			// 使用新条件自带的时间戳。
			WithLastTransitionTime(newCondition.LastTransitionTime))

	svcApply := networkingapiv1apply.ServiceCIDR(cidr.Name).WithStatus(svcApplyStatus)
	// 以 server-side apply 更新状态，Force=true 保证控制器可覆盖自身管理的字段。
	_, err := c.client.NetworkingV1().ServiceCIDRs().ApplyStatus(ctx, svcApply, metav1.ApplyOptions{FieldManager: controllerName, Force: true})
	return err
}

// convertToV1IPFamily 将 netutils.IPFamily 转换为 v1.IPFamily。
// TODO: 合并重复的辅助函数。
// 复制自 pkg/proxy/util/utils.go。
func convertToV1IPFamily(ipFamily netutils.IPFamily) v1.IPFamily {
	switch ipFamily {
	case netutils.IPv4:
		return v1.IPv4Protocol
	case netutils.IPv6:
		return v1.IPv6Protocol
	}

	return v1.IPFamilyUnknown
}
