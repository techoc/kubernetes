/*
Copyright The Kubernetes Authors.

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

package resourcepoolstatusrequest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	resourcev1 "k8s.io/api/resource/v1"
	resourcev1alpha3 "k8s.io/api/resource/v1alpha3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcev1informers "k8s.io/client-go/informers/resource/v1"
	resourcev1alpha3informers "k8s.io/client-go/informers/resource/v1alpha3"
	clientset "k8s.io/client-go/kubernetes"
	resourcev1listers "k8s.io/client-go/listers/resource/v1"
	resourcev1alpha3listers "k8s.io/client-go/listers/resource/v1alpha3"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/resourcepoolstatusrequest/metrics"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// ControllerName 是本控制器的名称。
	ControllerName = "resourcepoolstatusrequest-controller"

	// maxRetries 是请求被丢弃前允许重试的次数，防止永久失败的请求无限占用队列。
	maxRetries = 5

	// cleanupPollingInterval 是控制器扫描并清理过期请求的时间间隔。
	cleanupPollingInterval = 10 * time.Minute

	// completedRequestTTL 是已完成请求（带 Complete 或 Failed 条件）在自动删除前的保留时长，
	// 给用户留出读取结果的时间。
	completedRequestTTL = 1 * time.Hour

	// pendingRequestTTL 是未完成请求（无 status）在自动删除前的保留时长，
	// 用于兜底清理卡住的请求，避免它们永远留在集群里。
	pendingRequestTTL = 24 * time.Hour
)

// Controller 负责处理 ResourcePoolStatusRequest：按需从各 lister 计算池状态并写回 status。
type Controller struct {
	client clientset.Interface

	// requestLister 从共享 informer 的存储中列举/获取 ResourcePoolStatusRequest。
	requestLister resourcev1alpha3listers.ResourcePoolStatusRequestLister

	// sliceLister 从共享 informer 的存储中列举/获取 ResourceSlice。
	sliceLister resourcev1listers.ResourceSliceLister

	// claimLister 从共享 informer 的存储中列举/获取 ResourceClaim。
	claimLister resourcev1listers.ResourceClaimLister

	// taintRuleLister 从共享 informer 的存储中列举/获取 DeviceTaintRule。
	// 仅在 DRADeviceTaintRules 特性开关打开时才使用，避免依赖未必提供的 API。
	taintRuleLister resourcev1listers.DeviceTaintRuleLister

	// requestSynced 在 ResourcePoolStatusRequest 存储完成同步后返回 true。
	requestSynced cache.InformerSynced

	// sliceSynced 在 ResourceSlice 存储完成同步后返回 true。
	sliceSynced cache.InformerSynced

	// claimSynced 在 ResourceClaim 存储完成同步后返回 true。
	claimSynced cache.InformerSynced

	// taintRuleSynced 在 DeviceTaintRule 存储完成同步后返回 true。
	taintRuleSynced cache.InformerSynced

	// workqueue 是处理 ResourcePoolStatusRequest 的限流工作队列，重试走指数退避。
	workqueue workqueue.TypedRateLimitingInterface[string]
}

// NewController 构造 ResourcePoolStatusRequest 控制器：装配各 informer 的 lister 与同步函数、
// 注册指标，并为请求对象注册入队事件处理器。
func NewController(
	ctx context.Context,
	client clientset.Interface,
	requestInformer resourcev1alpha3informers.ResourcePoolStatusRequestInformer,
	sliceInformer resourcev1informers.ResourceSliceInformer,
	claimInformer resourcev1informers.ResourceClaimInformer,
	taintRuleInformer resourcev1informers.DeviceTaintRuleInformer,
) (*Controller, error) {
	logger := klog.FromContext(ctx)

	c := &Controller{
		client:        client,
		requestLister: requestInformer.Lister(),
		sliceLister:   sliceInformer.Lister(),
		claimLister:   claimInformer.Lister(),
		requestSynced: requestInformer.Informer().HasSynced,
		sliceSynced:   sliceInformer.Informer().HasSynced,
		claimSynced:   claimInformer.Informer().HasSynced,
		workqueue:     workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
	}

	// 仅在开关打开时才消费 DeviceTaintRule informer；否则未提供该 API 的集群
	// 会一直卡在它的缓存同步上，导致控制器无法启动。
	if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaintRules) {
		c.taintRuleLister = taintRuleInformer.Lister()
		c.taintRuleSynced = taintRuleInformer.Informer().HasSynced
	}

	// 注册指标（幂等，重复注册不会报错）。
	metrics.Register()

	// 只为请求注册 Add/Update 处理器：已完成请求会在 enqueueRequest 中被过滤掉，
	// Delete 无需处理，因为删除后没有对象可供同步。
	_, err := requestInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueRequest(logger, obj)
		},
		UpdateFunc: func(old, new interface{}) {
			c.enqueueRequest(logger, new)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("failed to add request event handler: %w", err)
	}

	logger.Info("ResourcePoolStatusRequest controller initialized")
	return c, nil
}

// Run 启动控制器的工作协程与 TTL 清理协程，并阻塞直到 ctx 结束；
// ctx 取消后先关闭工作队列再等待协程退出，避免残留 goroutine。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting ResourcePoolStatusRequest controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down ResourcePoolStatusRequest controller")
		c.workqueue.ShutDown()
		wg.Wait()
	}()

	// 必须等缓存同步完再启动 worker，否则 lister 看到的是空数据，
	// 会把尚未加载完成的 slice/claim 误判为池不完整。
	logger.Info("Waiting for informer caches to sync")
	syncs := []cache.InformerSynced{c.requestSynced, c.sliceSynced, c.claimSynced}
	if c.taintRuleSynced != nil {
		syncs = append(syncs, c.taintRuleSynced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), syncs...) {
		logger.Error(nil, "Failed to wait for caches to sync")
		return
	}

	logger.Info("Starting workers", "count", workers)
	for range workers {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runWorker, time.Second)
		})
	}

	// 启动独立的 TTL 垃圾回收协程，与请求处理解耦，避免清理阻塞正常同步。
	wg.Go(func() {
		wait.UntilWithContext(ctx, c.cleanupExpiredRequests, cleanupPollingInterval)
	})

	<-ctx.Done()
}

// runWorker 是长驻循环，不断调用 processNextWorkItem 从工作队列取出并处理条目，
// 直到队列关闭（processNextWorkItem 返回 false）为止。
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 取出并处理一个工作项：成功则 Forget 清除失败计数，
// 失败且未超过 maxRetries 则限流重入队，超过上限则丢弃并记录错误，防止坏请求无限重试。
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}

	defer c.workqueue.Done(key)

	logger := klog.FromContext(ctx)
	err := c.syncRequest(ctx, key)
	if err == nil {
		c.workqueue.Forget(key)
		return true
	}

	if c.workqueue.NumRequeues(key) < maxRetries {
		// 错误意味着状态可能还没算完，重入队让驱动有时间补齐 slice；限流避免热循环。
		logger.Error(err, "Error syncing request, requeuing", "request", key)
		c.workqueue.AddRateLimited(key)
		return true
	}

	// 重试耗尽后 Forget 释放队列条目的引用计数，然后上报错误供全局错误通道观测。
	logger.Error(err, "Dropping request out of the queue after max retries", "request", key, "retries", maxRetries)
	c.workqueue.Forget(key)
	utilruntime.HandleError(err)

	return true
}

// syncRequest 处理单个请求：若已有 status 则跳过；否则按需计算池状态并写回。
// 仅在指标最终确定（成功或写回失败）时才记录耗时，避免重入队的中间态污染指标。
func (c *Controller) syncRequest(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := time.Now()
	var driverName string
	var recordDuration bool

	defer func() {
		if driverName != "" && recordDuration {
			metrics.RequestProcessingDuration.WithLabelValues(driverName).Observe(time.Since(startTime).Seconds())
		}
	}()

	// 获取请求。
	request, err := c.requestLister.Get(key)
	if err != nil {
		// 对象已从缓存消失（通常已被删除），返回 nil 不重试；事件可能滞后于删除。
		return nil
	}

	driverName = request.Spec.Driver

	// 已有 status 表示结果已经写定，重复更新只会制造无意义的写请求和事件。
	if request.Status != nil {
		logger.V(4).Info("Request already processed, skipping", "request", key)
		return nil
	}

	logger.V(2).Info("Processing ResourcePoolStatusRequest", "request", key, "driver", request.Spec.Driver)

	// 状态完全按需从 lister 计算，不维护跨请求的缓存，保证结果反映当前快照。
	status := c.calculatePoolStatus(ctx, request)

	// 只对未完成的池重新入队：驱动可能还在发布 slice，需要时间收敛。
	// 视图结构错误是永久性的，直接写入 status 即可，重试没有意义。
	hasIncomplete := false
	for _, p := range status.Pools {
		if p.ValidationError != nil && strings.HasPrefix(*p.ValidationError, prefixPoolIncomplete) {
			hasIncomplete = true
			break
		}
	}
	if hasIncomplete {
		// 返回错误而非写 status，是为了不把“驱动尚未发布完”的中间态固化成最终答案。
		return fmt.Errorf("incomplete pools detected, requeueing")
	}

	// 写回前深拷贝，避免修改 informer 缓存中的共享对象。
	requestCopy := request.DeepCopy()
	requestCopy.Status = &status

	_, err = c.client.ResourceV1alpha3().ResourcePoolStatusRequests().UpdateStatus(ctx, requestCopy, metav1.UpdateOptions{})
	if err != nil {
		// 只有实际到达 API 写入阶段才计入错误与耗时指标，重入队的中间态不计。
		recordDuration = true
		metrics.RequestProcessingErrors.WithLabelValues(driverName).Inc()
		metrics.RequestsProcessed.WithLabelValues(driverName).Inc()
		return fmt.Errorf("failed to update status for request %s: %w", key, err)
	}

	recordDuration = true
	metrics.RequestsProcessed.WithLabelValues(driverName).Inc()
	logger.V(2).Info("Successfully processed ResourcePoolStatusRequest", "request", key, "poolCount", len(status.Pools))
	return nil
}

// deviceUnavailable 判断设备是否应计为不可用：既要检查设备自带的 taint，
// 也要检查管理员下发的、能匹配到该设备的 DeviceTaintRule。
func deviceUnavailable(driver, pool string, device *resourcev1.Device, rules []*resourcev1.DeviceTaintRule) bool {
	if hasUnavailableTaint(device) {
		return true
	}
	for _, rule := range rules {
		if ruleMakesDeviceUnavailable(rule, driver, pool, device.Name) {
			return true
		}
	}
	return false
}

// hasUnavailableTaint 判断设备是否携带使其不可调度的内嵌 taint（NoSchedule 或 NoExecute）。
func hasUnavailableTaint(device *resourcev1.Device) bool {
	for _, taint := range device.Taints {
		switch taint.Effect {
		case resourcev1.DeviceTaintEffectNoSchedule, resourcev1.DeviceTaintEffectNoExecute:
			return true
		}
	}
	return false
}

// ruleMakesDeviceUnavailable 判断某条 DeviceTaintRule 是否选中给定设备并施加
// NoSchedule/NoExecute taint。选择器为 nil 表示不匹配任何设备。
func ruleMakesDeviceUnavailable(rule *resourcev1.DeviceTaintRule, driver, pool, device string) bool {
	// 先看效果：只有影响调度的 taint 才会让设备不可用，其余效果直接排除。
	switch rule.Spec.Taint.Effect {
	case resourcev1.DeviceTaintEffectNoSchedule, resourcev1.DeviceTaintEffectNoExecute:
	default:
		return false
	}
	sel := rule.Spec.DeviceSelector
	// 空选择器不匹配任何对象，避免把规则误当成“匹配所有设备”。
	if sel == nil {
		return false
	}
	if sel.Driver != nil && *sel.Driver != driver {
		return false
	}
	if sel.Pool != nil && *sel.Pool != pool {
		return false
	}
	if sel.Device != nil && *sel.Device != device {
		return false
	}
	return true
}

// calculatePoolStatus 直接读取共享 informer 的 lister 按需计算池状态，
// 不在请求之间维护缓存，因此每次结果都对应当前缓存快照。
func (c *Controller) calculatePoolStatus(ctx context.Context, request *resourcev1alpha3.ResourcePoolStatusRequest) resourcev1alpha3.ResourcePoolStatusRequestStatus {
	logger := klog.FromContext(ctx)

	driver := request.Spec.Driver
	var poolNameFilter string
	if request.Spec.PoolName != nil {
		poolNameFilter = *request.Spec.PoolName
	}

	// 步骤 1：从 ResourceSlice 聚合池数据。采用两遍扫描：先求每个池的最大 generation，
	// 再只统计该 generation 的 slice。按 KEP-5677，旧 generation 的 slice 必须忽略，
	// 否则会把滚动更新中的新旧数据混在一起。
	type poolInfo struct {
		driver             string
		poolName           string
		nodeName           string
		nodeNameMixed      bool // slice 的 NodeNames 是否不同
		totalDevices       int32
		unavailableDevices int32 // 携带 NoSchedule/NoExecute taint 的设备
		sliceCount         int32
		expectedSliceCount int64
		generation         int64
		// partition/counter/shareable 视图的数据（Alpha 1.37）。
		devices        []deviceRecord
		sharedCounters []resourcev1.CounterSet
	}

	slices, err := c.sliceLister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "Failed to list ResourceSlices")
		return errorStatus("Failed to list ResourceSlices: " + err.Error())
	}

	// DeviceTaintRule 由管理员外部下发，与驱动内嵌的 taint 相互独立；
	// 仅开关打开时才读取，避免对未提供该 API 的集群发起 List。
	var taintRules []*resourcev1.DeviceTaintRule
	if c.taintRuleLister != nil && utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaintRules) {
		taintRules, err = c.taintRuleLister.List(labels.Everything())
		if err != nil {
			logger.Error(err, "Failed to list DeviceTaintRules")
			return errorStatus("Failed to list DeviceTaintRules: " + err.Error())
		}
	}

	// 第一遍：按 driver/pool 记录最大 generation，作为第二遍的过滤基准。
	maxGeneration := make(map[string]int64)
	for _, slice := range slices {
		if slice.Spec.Driver != driver {
			continue
		}
		slicePoolName := slice.Spec.Pool.Name
		if poolNameFilter != "" && slicePoolName != poolNameFilter {
			continue
		}
		key := slice.Spec.Driver + "/" + slicePoolName
		if gen, exists := maxGeneration[key]; !exists || slice.Spec.Pool.Generation > gen {
			maxGeneration[key] = slice.Spec.Pool.Generation
		}
	}

	// 第二遍：只聚合 generation 最大的 slice。
	poolData := make(map[string]*poolInfo)
	for _, slice := range slices {
		if slice.Spec.Driver != driver {
			continue
		}
		slicePoolName := slice.Spec.Pool.Name
		if poolNameFilter != "" && slicePoolName != poolNameFilter {
			continue
		}

		key := slice.Spec.Driver + "/" + slicePoolName

		// 忽略旧 generation 的 slice：驱动升级期间可能同时存在多代数据，
		// 混算会重复计数并得到错误的总设备数。
		if slice.Spec.Pool.Generation < maxGeneration[key] {
			continue
		}

		deviceCount := int32(len(slice.Spec.Devices))
		unavailCount := int32(0)
		for i := range slice.Spec.Devices {
			if deviceUnavailable(slice.Spec.Driver, slicePoolName, &slice.Spec.Devices[i], taintRules) {
				unavailCount++
			}
		}
		info, exists := poolData[key]
		if !exists {
			var nodeName string
			if slice.Spec.NodeName != nil {
				nodeName = *slice.Spec.NodeName
			}
			info = &poolInfo{
				driver:             slice.Spec.Driver,
				poolName:           slicePoolName,
				nodeName:           nodeName,
				totalDevices:       deviceCount,
				unavailableDevices: unavailCount,
				sliceCount:         1,
				expectedSliceCount: slice.Spec.Pool.ResourceSliceCount,
				generation:         maxGeneration[key],
			}
			poolData[key] = info
		} else {
			info.totalDevices += deviceCount
			info.unavailableDevices += unavailCount
			info.sliceCount++
			// 记录 NodeName 是否一致：只有所有 slice 都属于同一节点时才能上报 NodeName。
			sliceNodeName := ""
			if slice.Spec.NodeName != nil {
				sliceNodeName = *slice.Spec.NodeName
			}
			if sliceNodeName != info.nodeName {
				info.nodeNameMixed = true
			}
		}

		// 为分区/可共享视图收集每台设备和池级计数器；设备记录要带上其所属 slice 的
		// PartitionTypeAttribute，因为不同 slice 可能使用不同的分区属性。
		info.devices = append(info.devices, sliceDeviceRecords(slice)...)
		info.sharedCounters = append(info.sharedCounters, slice.Spec.SharedCounters...)
	}

	// 步骤 2：从 ResourceClaim 统计已分配设备。以 driver/pool 加设备名为键去重，
	// 因为 allowMultipleAllocations 下同一设备可能被多个 claim 共享，但物理设备只算一次。
	// AdminAccess 只是观察而非占用，必须跳过。
	allocatedDevices := make(map[string]map[string]struct{})
	// 按池累计非 AdminAccess claim 的已消费容量，供 shareableSummary 视图使用。
	consumedCapacity := make(map[string]map[resourcev1.QualifiedName]resource.Quantity)

	claims, err := c.claimLister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "Failed to list ResourceClaims")
		return errorStatus("Failed to list ResourceClaims: " + err.Error())
	}

	for _, claim := range claims {
		if claim.Status.Allocation == nil {
			continue
		}
		for _, result := range claim.Status.Allocation.Devices.Results {
			// AdminAccess 不占用设备，计入会高估已分配数量。
			if result.AdminAccess != nil && *result.AdminAccess {
				continue
			}
			key := result.Driver + "/" + result.Pool
			devices, ok := allocatedDevices[key]
			if !ok {
				devices = make(map[string]struct{})
				allocatedDevices[key] = devices
			}
			devices[result.Device] = struct{}{}

			for capacityName, quantity := range result.ConsumedCapacity {
				m := consumedCapacity[key]
				if m == nil {
					m = make(map[resourcev1.QualifiedName]resource.Quantity)
					consumedCapacity[key] = m
				}
				cur := m[capacityName].DeepCopy()
				cur.Add(quantity)
				m[capacityName] = cur
			}
		}
	}

	// 步骤 3：构造池状态列表；未集齐预期 slice 数的池打上校验错误，交由上层决定是否重试。
	var pools []resourcev1alpha3.PoolStatus
	for key, info := range poolData {
		pool := resourcev1alpha3.PoolStatus{
			Driver:     info.driver,
			PoolName:   info.poolName,
			Generation: info.generation,
		}
		// 节点名混杂时留空，上报一个不确定的 NodeName 会误导调用方。
		if info.nodeName != "" && !info.nodeNameMixed {
			nodeName := info.nodeName
			pool.NodeName = &nodeName
		}

		if int64(info.sliceCount) < info.expectedSliceCount {
			// 池未集齐 slice：只写校验错误，数量字段保持 nil，避免调用方误用部分数据。
			errMsg := fmt.Sprintf("%s pool %s/%s is incomplete: observed %d/%d slices at generation %d",
				prefixPoolIncomplete, info.driver, info.poolName, info.sliceCount, info.expectedSliceCount, info.generation)
			// 按字节截断到 256，满足 API 字段的 +k8s:maxBytes=256 约束（按 rune 截断会超限）。
			if len(errMsg) > 256 {
				errMsg = errMsg[:256]
			}
			pool.ValidationError = &errMsg
		} else {
			// 池数据完整，才填充各设备计数与 slice 数。
			inUse := allocatedDevices[key]
			allocDeviceCount := int32(len(inUse))
			unavailDevices := info.unavailableDevices
			// 用 max 兜底：异常数据（如不可用设备多于总数）不应产生负数容量。
			availableDevices := max(0, info.totalDevices-allocDeviceCount-unavailDevices)

			totalDevices := info.totalDevices
			allocDevices := allocDeviceCount
			availDevices := availableDevices
			sliceCount := info.sliceCount
			pool.ResourceSliceCount = &sliceCount
			pool.TotalDevices = &totalDevices
			pool.AllocatedDevices = &allocDevices
			pool.AvailableDevices = &availDevices
			pool.UnavailableDevices = &unavailDevices

			// 高级视图失败只影响视图字段，上面的基础计数依然有效，需照常上报。
			viewInput := poolViewInput{
				driver:           info.driver,
				poolName:         info.poolName,
				devices:          info.devices,
				sharedCounters:   info.sharedCounters,
				inUse:            inUse,
				consumedCapacity: consumedCapacity[key],
			}
			resolveDevicePartitions(request.Spec.DefaultPartitionTypeAttribute, &viewInput)

			partitionSummary, shareable, viewErr := computePoolViews(viewInput)
			pool.PartitionSummary = partitionSummary
			pool.ShareableSummary = shareable
			if viewErr != "" {
				// 同样按字节截断，避免超出字段长度限制。
				if len(viewErr) > 256 {
					viewErr = viewErr[:256]
				}
				pool.ValidationError = &viewErr
			}
		}
		pools = append(pools, pool)
	}

	// 按 driver、池名排序，保证结果稳定，便于比较和截断。
	sort.Slice(pools, func(i, j int) bool {
		if pools[i].Driver != pools[j].Driver {
			return pools[i].Driver < pools[j].Driver
		}
		return pools[i].PoolName < pools[j].PoolName
	})

	poolCount := int32(len(pools))

	// 先排序再截断，limit 才能保证返回的是确定性的前 N 个池。
	limit := resourcev1alpha3.ResourcePoolStatusRequestLimitDefault
	if request.Spec.Limit != nil {
		limit = *request.Spec.Limit
	}
	if int32(len(pools)) > limit {
		pools = pools[:limit]
	}

	// 统计不完整池的数量，写入条件消息，让调用方一眼看出结果是否完全可用。
	incompletePoolCount := 0
	for _, p := range pools {
		if p.ValidationError != nil {
			incompletePoolCount++
		}
	}

	conditionMessage := fmt.Sprintf("Calculated status for %d pools", len(pools))
	if incompletePoolCount > 0 {
		conditionMessage = fmt.Sprintf("Calculated status for %d pools (%d incomplete)", len(pools), incompletePoolCount)
	}

	now := metav1.Now()
	status := resourcev1alpha3.ResourcePoolStatusRequestStatus{
		PoolCount: &poolCount,
		Pools:     pools,
		Conditions: []metav1.Condition{
			{
				Type:               resourcev1alpha3.ResourcePoolStatusRequestConditionComplete,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "CalculationComplete",
				Message:            conditionMessage,
			},
		},
	}

	logger.V(4).Info("Calculated pool status",
		"request", request.Name,
		"poolCount", poolCount,
		"returned", len(pools))

	return status
}

// errorStatus 构造表示计算失败的状态：PoolCount 为 0，并带上 Failed 条件与具体原因，
// 让调用方能区分“算完了”和“算失败”。
func errorStatus(message string) resourcev1alpha3.ResourcePoolStatusRequestStatus {
	now := metav1.Now()
	zero := int32(0)
	return resourcev1alpha3.ResourcePoolStatusRequestStatus{
		PoolCount: &zero,
		Conditions: []metav1.Condition{
			{
				Type:               resourcev1alpha3.ResourcePoolStatusRequestConditionFailed,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "CalculationFailed",
				Message:            message,
			},
		},
	}
}

// enqueueRequest 处理 Add/Update 事件，把尚未处理的请求按名称入队；
// 类型断言失败和已完成对象都被忽略，避免无效条目进入队列。
func (c *Controller) enqueueRequest(logger klog.Logger, obj interface{}) {
	request, ok := obj.(*resourcev1alpha3.ResourcePoolStatusRequest)
	if !ok {
		logger.Error(nil, "Failed to cast object to ResourcePoolStatusRequest")
		return
	}

	// status 已设置说明结果已写定，更新事件不应再触发重复计算。
	if request.Status != nil {
		return
	}

	c.workqueue.Add(request.Name)
}

// cleanupExpiredRequests 周期性清理超过 TTL 的请求：已完成的按完成时间使用
// completedRequestTTL，未完成的按创建时间使用 pendingRequestTTL（兜底清理卡住的请求）。
// 删除带 UID 前置条件，避免误删同名的新对象。
func (c *Controller) cleanupExpiredRequests(ctx context.Context) {
	logger := klog.FromContext(ctx)

	requests, err := c.requestLister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "Failed to list ResourcePoolStatusRequests for cleanup")
		return
	}

	var deletedCount int
	for _, request := range requests {
		if c.shouldDeleteRequest(request) {
			if err := c.client.ResourceV1alpha3().ResourcePoolStatusRequests().Delete(ctx, request.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &request.UID}}); err != nil {
				// NotFound 说明已被其他协程清理，属于竞态的正常结果，不必报错。
				if !apierrors.IsNotFound(err) {
					logger.Error(err, "Failed to delete expired ResourcePoolStatusRequest", "request", request.Name)
				}
				continue
			}
			deletedCount++
			logger.V(2).Info("Deleted expired ResourcePoolStatusRequest", "request", request.Name)
		}
	}

	if deletedCount > 0 {
		logger.Info("Cleanup completed", "deletedCount", deletedCount)
	}
}

// shouldDeleteRequest 按 TTL 判断请求是否该删除：优先用 Complete/Failed 条件的
// 变更时间判断已完成请求；状态存在但条件缺失时退化为创建时间；未完成请求则用创建时间。
func (c *Controller) shouldDeleteRequest(request *resourcev1alpha3.ResourcePoolStatusRequest) bool {
	if request.Status != nil {
		// 已完成请求以条件变更时间为准，因为它比创建时间更接近“结果生成”时刻。
		for _, cond := range request.Status.Conditions {
			if cond.Type == resourcev1alpha3.ResourcePoolStatusRequestConditionComplete ||
				cond.Type == resourcev1alpha3.ResourcePoolStatusRequestConditionFailed {
				return isOlderThan(cond.LastTransitionTime.Time, completedRequestTTL)
			}
		}
		// 边界情况：有 status 却无终态条件，只能退化为创建时间，防止对象永远不被清理。
		return isOlderThan(request.CreationTimestamp.Time, completedRequestTTL)
	}
	// 未完成请求按创建时间计算，长 TTL 给驱动充分时间发布 slice。
	return isOlderThan(request.CreationTimestamp.Time, pendingRequestTTL)
}

// isOlderThan 判断给定时刻是否早于“当前时间减去 d”；零值时间视为未初始化，不参与过期判断。
func isOlderThan(t time.Time, d time.Duration) bool {
	return !t.IsZero() && time.Since(t) > d
}
