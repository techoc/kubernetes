/*
Copyright 2025 The Kubernetes Authors.

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

package tracker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/diff"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	resourceinformers "k8s.io/client-go/informers/resource/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/utils/buffer"
	"k8s.io/utils/ptr"
)

const (
	// driverPoolDeviceIndexName 是用于按“驱动/资源池/设备”三元组检索 ResourceSlice 的索引名。
	driverPoolDeviceIndexName = "driverPoolDevice"

	// 以下三个常量是索引键中的通配符，分别代表任意驱动、任意资源池、任意设备。
	anyDriver = "*"
	anyPool   = "*"
	anyDevice = "*"
)

// Tracker 维护应用了匹配的 DeviceTaintRule 之后的 ResourceSlice 视图；底层由 informer 驱动，异步处理潜在变更。
type Tracker struct {
	// enableDeviceTaintRules 标记是否启用污点规则处理；为 false 时退化为底层 informer 的薄封装。
	enableDeviceTaintRules bool

	// resourceSliceLister 在未启用污点规则时用于直接列出原始 ResourceSlice。
	// resourceSlices 是带索引的 ResourceSlice informer，提供索引器与事件注册能力。
	// resourceSlicesHandle 是注册事件处理器后返回的句柄，停止时据此注销。
	// deviceTaints 是 DeviceTaintRule 的 informer，用于监听规则变更。
	// deviceTaintsHandle 是其事件处理器注册句柄。
	// patchedResourceSlices 缓存应用规则后的 ResourceSlice，以对象名称为键。
	// broadcaster 把 CEL 运行时错误等事件广播并写入 API Server。
	// recorder 用于生成 Kubernetes Event 对象。
	resourceSliceLister   resourcelisters.ResourceSliceLister
	resourceSlices        resourceinformers.ResourceSliceIndexInformer
	resourceSlicesHandle  cache.ResourceEventHandlerRegistration
	deviceTaints          resourceinformers.DeviceTaintRuleIndexInformer
	deviceTaintsHandle    cache.ResourceEventHandlerRegistration
	patchedResourceSlices cache.Store
	broadcaster           record.EventBroadcaster
	recorder              record.EventRecorder
	// handleError 默认指向 utilruntime.HandleErrorWithContext，测试中可替换。
	handleError func(context.Context, error, string, ...any)

	// wg 跟踪后台协程，cancel 用于终止它们。
	wg     sync.WaitGroup
	cancel func(error)

	// rwMutex 同步对事件处理器相关字段的更新。
	rwMutex sync.RWMutex

	// synced 在所有上游事件处理器同步完成后被关闭。
	synced chan struct{}

	// eventHandlers 保存所有已注册的事件处理器。
	eventHandlers []cache.ResourceEventHandler
	// eventQueue 保存待投递给单个事件处理器的函数。
	// 投递时不得持有 rwMutex，因为处理器允许访问缓存，持锁会导致死锁。
	// 新函数在处理缓存更新（持锁）时入队，每个入队方返回前都会排空队列，从而保证事件被立即通知。
	// 不能用有界 channel：写者持锁等待容量、读者又在处理器中等待同一把锁时会死锁；
	// 不持锁写入又会破坏事件顺序，因此采用无界函数队列。
	// 这里用环形缓冲区而非 channel：channel 无法拥有无界容量，容量耗尽时持锁写入会死锁。
	eventQueue buffer.Ring[func()]
}

// Options 用于配置 Tracker，控制是否启用污点规则以及 informer 与事件接收器的来源。
type Options struct {
	// EnableDeviceTaintRules 控制是否把 DeviceTaintRule 反映到 Tracker 报告的 ResourceSlice 中；为 false 时无需 TaintInformer。
	EnableDeviceTaintRules bool
	// EnableConsumableCapacity 决定 CEL 编译器是否支持 DRAConsumableCapacity 特性。
	EnableConsumableCapacity bool

	// SliceInformer 提供 ResourceSlice 的 informer 与 lister。
	// TaintInformer 提供 DeviceTaintRule 的 informer。
	SliceInformer resourceinformers.TypedResourceSliceInformer
	TaintInformer resourceinformers.TypedDeviceTaintRuleInformer

	// KubeClient 用于在 CEL 表达式遇到运行时错误时生成 Event。
	KubeClient kubernetes.Interface
}

// StartTracker 为新建的 Tracker 创建并初始化 informer；未启用污点规则时返回仅透传底层 informer 的最小包装。
func StartTracker(ctx context.Context, opts Options) (finalT *Tracker, finalErr error) {
	// 未启用污点规则：直接返回最小包装，所有公开方法都短路到底层 informer。
	if !opts.EnableDeviceTaintRules {
		return &Tracker{
			resourceSliceLister: opts.SliceInformer.Lister(),
			resourceSlices:      opts.SliceInformer.TypedInformer(),
		}, nil
	}

	// 启用规则时构造完整 Tracker，随后注册事件处理器。
	t, err := newTracker(ctx, opts)
	if err != nil {
		return nil, err
	}
	// 用 defer 保证初始化失败时清理已部分构造的实例。
	defer func() {
		// 若最终没有返回 Tracker（发生错误），则停止它。
		if finalErr != nil {
			t.Stop()
		}
	}()
	// 注册上游 informer 的事件处理器。
	if err := t.initInformers(ctx); err != nil {
		return nil, fmt.Errorf("initialize informers: %w", err)
	}
	return t, nil
}

// newTracker 构造一个尚未注册事件处理器的 Tracker，主要供测试使用。
func newTracker(ctx context.Context, opts Options) (finalT *Tracker, finalErr error) {
	// 初始化各字段：索引器、补丁缓存、错误处理器与事件队列。
	// cancel 初始为空实现，真正的取消函数在 initInformers 中设置。
	t := &Tracker{
		enableDeviceTaintRules: opts.EnableDeviceTaintRules,
		resourceSliceLister:    opts.SliceInformer.Lister(),
		resourceSlices:         opts.SliceInformer.TypedInformer(),
		deviceTaints:           opts.TaintInformer.TypedInformer(),
		patchedResourceSlices:  cache.NewStore(cache.MetaNamespaceKeyFunc),
		handleError:            utilruntime.HandleErrorWithContext,
		synced:                 make(chan struct{}),
		cancel:                 func(error) {}, // 真实函数在 initInformers 中设置。
		eventQueue:             *buffer.NewRing[func()](buffer.RingOptions{InitialSize: 0, NormalSize: 4}),
	}
	// 构造失败时同样停止已部分初始化的实例。
	defer func() {
		// 出错时停止实例，避免泄漏 informer 注册与协程。
		if finalErr != nil {
			t.Stop()
		}
	}()
	// 为 ResourceSlice informer 注册“驱动/资源池/设备”索引，供规则反查受影响的 slice。
	err := t.resourceSlices.AddTypedIndexers(resourceinformers.ResourceSliceIndexers{driverPoolDeviceIndexName: sliceDriverPoolDeviceIndexFunc})
	if err != nil {
		// 索引注册失败则直接返回。
		return nil, fmt.Errorf("failed to add %s index to ResourceSlice informer: %w", driverPoolDeviceIndexName, err)
	}
	// 单测中可能未提供 KubeClient，此时不创建事件广播器。
	if opts.KubeClient != nil {
		// 提供 KubeClient 时，创建广播器并把事件输出到日志与 API Server。
		t.broadcaster = record.NewBroadcaster(record.WithContext(ctx))
		t.broadcaster.StartLogging(klog.Infof)
		t.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: opts.KubeClient.CoreV1().Events("")})
		// 创建 Event recorder，组件名为 resource_slice_tracker。
		t.recorder = t.broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "resource_slice_tracker"})
	}

	return t, nil
}

// initInformers 为 newTracker 构造的实例注册上游事件处理器并启动同步监控。
func (t *Tracker) initInformers(ctx context.Context) error {
	// err 承接事件处理器注册结果。
	var err error
	// 取 logger 并通过 HandlerOptions 传给 informer 处理器。
	logger := klog.FromContext(ctx)
	options := cache.HandlerOptions{Logger: &logger}

	// 注册 ResourceSlice 增/改/删处理器，统一触发 syncSlice 重算。
	sliceHandler := resourceinformers.ResourceSliceHandlerFuncs{
		AddFunc:    t.resourceSliceAdd(ctx),
		UpdateFunc: t.resourceSliceUpdate(ctx),
		DeleteFunc: t.resourceSliceDelete(ctx),
	}
	// 保存注册句柄，停止时据此注销。
	t.resourceSlicesHandle, err = t.resourceSlices.AddTypedEventHandler(sliceHandler, options)
	if err != nil {
		return fmt.Errorf("add event handler for ResourceSlices: %w", err)
	}

	// 注册 DeviceTaintRule 增/改/删处理器，用于重算受影响的 slice。
	taintHandler := resourceinformers.DeviceTaintRuleHandlerFuncs{
		AddFunc:    t.deviceTaintAdd(ctx),
		UpdateFunc: t.deviceTaintUpdate(ctx),
		DeleteFunc: t.deviceTaintDelete(ctx),
	}
	// 保存污点规则处理器的注册句柄。
	t.deviceTaintsHandle, err = t.deviceTaints.AddTypedEventHandler(taintHandler, options)
	if err != nil {
		return fmt.Errorf("add event handler for DeviceTaintRules: %w", err)
	}

	// 启动短生命周期协程：上游处理完成后关闭本 Tracker 的 synced 通道。
	monitorCtx, cancel := context.WithCancelCause(ctx)
	t.cancel = cancel
	// 纳入 wg 管理，保证 Stop 能等待该协程退出。
	t.wg.Go(func() {
		// 依次等待 ResourceSlice 与 DeviceTaintRule 两个处理器同步。
		for _, handle := range []cache.ResourceEventHandlerRegistration{t.resourceSlicesHandle, t.deviceTaintsHandle} {
			// 同步完成或监控被取消，二者取其一。
			select {
			case <-handle.HasSyncedChecker().Done():
			case <-monitorCtx.Done():
				// 取消时直接返回，不关闭 synced。
				return
			}
		}
		// 两个处理器都同步后关闭 synced，对外宣告 Tracker 就绪。
		close(t.synced)
	})

	return nil
}

// HasSynced 判断 Tracker 是否已处理完当前所有输入对象；此时注册新处理器可拿到最新状态。
func (t *Tracker) HasSynced() bool {
	// 通过 Done 通道是否已关闭来非阻塞地判断同步状态。
	select {
	case <-t.HasSyncedChecker().Done():
		return true
	default:
		return false
	}
}

// HasSyncedChecker 返回同步状态检查器；未启用规则时直接透传底层 informer。
func (t *Tracker) HasSyncedChecker() cache.DoneChecker {
	if !t.enableDeviceTaintRules {
		return t.resourceSlices.HasSyncedChecker()
	}

	// 启用规则时用包装类型暴露本 Tracker 的 synced 通道。
	return trackerHasSynced{t}
}

// trackerHasSynced 把 Tracker 包装成 cache.DoneChecker，用于对外暴露同步状态。
type trackerHasSynced struct{ t *Tracker }

// Name 返回该检查器的名称。
func (s trackerHasSynced) Name() string { return "ResourceSlice tracker" }

// Done 返回 Tracker 的 synced 通道。
func (s trackerHasSynced) Done() <-chan struct{} {
	return s.t.synced
}

// Stop 终止所有后台活动并阻塞至清理完成；未启用规则时无需清理。
func (t *Tracker) Stop() {
	// 最小包装模式下没有后台活动，直接返回。
	if !t.enableDeviceTaintRules {
		return
	}

	// 取消监控协程，使其不再关闭 synced。
	t.cancel(errors.New("stopped"))

	// 关闭事件广播器，flush 所有待发送事件。
	if t.broadcaster != nil {
		t.broadcaster.Shutdown()
	}
	// 注销两个上游事件处理器，忽略“未注册”错误。
	_ = t.resourceSlices.RemoveEventHandler(t.resourceSlicesHandle)
	_ = t.deviceTaints.RemoveEventHandler(t.deviceTaintsHandle)

	// 等待监控协程退出。
	t.wg.Wait()
}

// ListPatchedResourceSlices 返回应用污点规则后的全部 ResourceSlice；未启用规则时直接列出原始对象。
func (t *Tracker) ListPatchedResourceSlices() ([]*resourceapi.ResourceSlice, error) {
	// 最小包装：直接返回底层 lister 的结果。
	if !t.enableDeviceTaintRules {
		return t.resourceSliceLister.List(labels.Everything())
	}

	// 把缓存中的 any 切片转换为强类型的 ResourceSlice 切片。
	return typedSlice[*resourceapi.ResourceSlice](t.patchedResourceSlices.List()), nil
}

// AddEventHandler 注册事件处理器，并以 Add 事件补发当前全部 ResourceSlice；返回句柄可用于等待缓存同步。
// 同一处理器内的事件顺序投递，不同处理器之间不做协调；处理器可以回调 Tracker。
// 注册期间会先同步当前已有对象，之后新增处理器也能收到最新状态。
func (t *Tracker) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	// 最小包装：直接委托给底层 informer。
	if !t.enableDeviceTaintRules {
		return t.resourceSlices.AddEventHandler(handler)
	}

	// 返回前排空队列，保证新处理器立即收到补发的 Add 事件。
	defer t.emitEvents()
	// 持锁更新处理器列表与事件队列。
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	// 将新处理器加入注册表。
	t.eventHandlers = append(t.eventHandlers, handler)
	// 读取当前已打补丁的全部 slice，用于补发 Add。
	allObjs, _ := t.ListPatchedResourceSlices()
	// 为每个现有对象安排一次 OnAdd(obj, true)。
	for _, obj := range allObjs {
		t.eventQueue.WriteOne(func() {
			handler.OnAdd(obj, true)
		})
	}

	// 由 Tracker 统一提供 HasSynced；不支持移除，因此所有处理器返回同一句柄。
	return t, nil
}

// emitEvents 按 FIFO 顺序排空并投递队列中的函数。
func (t *Tracker) emitEvents() {
	// 循环直到队列为空。
	for {
		// 仅在取事件时短暂持锁，投递时不持锁以避免死锁。
		t.rwMutex.Lock()
		// 从队头取出一个待投递函数。
		deliver, ok := t.eventQueue.ReadOne()
		t.rwMutex.Unlock()

		// 队列已空，返回。
		if !ok {
			return
		}
		// 用闭包包裹投递，以便捕获处理器可能产生的 panic。
		func() {
			// 处理器 panic 时由 HandleCrash 记录并恢复，避免影响其余事件。
			defer utilruntime.HandleCrash()
			deliver()
		}()
	}
}

// pushEvent 为所有已注册处理器安排一次事件投递，实际投递由随后的 emitEvents 完成。
func (t *Tracker) pushEvent(oldObj, newObj any) {
	// 持锁遍历处理器列表并写入队列。
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()
	// 为每个处理器写入一个投递函数。
	for _, handler := range t.eventHandlers {
		// oldObj 为 nil 表示新增；newObj 为 nil 表示删除；两者都非 nil 表示更新。
		if oldObj == nil {
			t.eventQueue.WriteOne(func() {
				handler.OnAdd(newObj, false)
			})
		} else if newObj == nil {
			t.eventQueue.WriteOne(func() {
				handler.OnDelete(oldObj)
			})
		} else {
			t.eventQueue.WriteOne(func() {
				handler.OnUpdate(oldObj, newObj)
			})
		}
	}
}

// sliceDriverPoolDeviceIndexFunc 为一个 slice 生成多组索引键：驱动/资源池/设备的各种通配符组合，便于按规则反查。
func sliceDriverPoolDeviceIndexFunc(slice *resourceapi.ResourceSlice) ([]string, error) {
	// 驱动维度包含“任意”通配符与真实驱动名。
	drivers := []string{
		anyDriver,
		slice.Spec.Driver,
	}
	// 资源池维度包含“任意”通配符与真实池名。
	pools := []string{
		anyPool,
		slice.Spec.Pool.Name,
	}
	// 预分配容量：驱动数 × 池数 × (1 + 设备数)。
	indexValues := make([]string, 0, len(drivers)*len(pools)*(1+len(slice.Spec.Devices)))
	// 对每个驱动×池组合，先生成“任意设备”键，再逐个生成具体设备键。
	for _, driver := range drivers {
		for _, pool := range pools {
			// 写入代表“任意设备”的索引键，随后写入每个具体设备。
			indexValues = append(indexValues, deviceID(driver, pool, anyDevice))
			for _, device := range slice.Spec.Devices {
				indexValues = append(indexValues, deviceID(driver, pool, device.Name))
			}
		}
	}
	return indexValues, nil
}

// driverPoolDeviceIndexPatchKey 根据规则的 DeviceSelector 计算索引键，未指定字段用通配符补齐。
func driverPoolDeviceIndexPatchKey(patch *resourceapi.DeviceTaintRule) string {
	// 取规则的设备选择器。
	deviceSelector := patch.Spec.DeviceSelector
	// 无选择器不匹配任何设备，返回空键。
	if deviceSelector == nil {
		return ""
	}
	// 选择器各字段缺省时回退为通配符。
	driverKey := ptr.Deref(deviceSelector.Driver, anyDriver)
	poolKey := ptr.Deref(deviceSelector.Pool, anyPool)
	deviceKey := ptr.Deref(deviceSelector.Device, anyDevice)
	// 拼成“驱动/池/设备”形式的索引键。
	return deviceID(driverKey, poolKey, deviceKey)
}

// sliceNamesForRule 通过索引查找可能与规则匹配的 slice 名称；查询失败则记录错误并返回空。
func (t *Tracker) sliceNamesForRule(ctx context.Context, rule *resourceapi.DeviceTaintRule) []string {
	// 先算出规则对应的索引键。
	key := driverPoolDeviceIndexPatchKey(rule)
	// 从 ResourceSlice 索引器中取出匹配该键的对象名称。
	sliceNames, err := t.resourceSlices.GetIndexer().IndexKeys(driverPoolDeviceIndexName, key)
	// 查询失败：记录错误并放弃本次同步。
	if err != nil {
		t.handleError(ctx, err, "failed listing ResourceSlices for driver/pool/device key", "key", key)
		return nil
	}
	// 返回命中的 slice 名称列表。
	return sliceNames
}

// resourceSliceAdd 返回处理 slice 新增的回调：记录日志并强制重新同步该 slice。
func (t *Tracker) resourceSliceAdd(ctx context.Context) func(*resourceapi.ResourceSlice) {
	// 从上下文取 logger。
	logger := klog.FromContext(ctx)
	return func(slice *resourceapi.ResourceSlice) {
		// V(6) 输出完整对象，否则 V(5) 只输出对象引用。
		if loggerV := logger.V(6); loggerV.Enabled() {
			loggerV.Info("ResourceSlice added", "slice", klog.Format(slice))
		} else {
			logger.V(5).Info("ResourceSlice added", "slice", klog.KObj(slice))
		}
		// slice 自身变更必须发送事件，sendEvent 为 true。
		t.syncSlice(ctx, slice.Name, true)
	}
}

// resourceSliceUpdate 返回处理 slice 更新的回调，必要时输出新旧差异并重新同步。
func (t *Tracker) resourceSliceUpdate(ctx context.Context) func(_, _ *resourceapi.ResourceSlice) {
	logger := klog.FromContext(ctx)
	return func(oldSlice, newSlice *resourceapi.ResourceSlice) {
		// 调试级别同时输出 diff 与完整对象，便于发现细微改动。
		if loggerV := logger.V(6); loggerV.Enabled() {
			loggerV.Info("ResourceSlice updated", "diff", diff.Diff(oldSlice, newSlice), "slice", klog.Format(newSlice))
		} else {
			logger.V(5).Info("ResourceSlice updated", "slice", klog.KObj(newSlice))
		}
		// 始终强制发送事件。
		t.syncSlice(ctx, newSlice.Name, true)
	}
}

// resourceSliceDelete 返回处理 slice 删除的回调，按名称重新同步以清除缓存。
func (t *Tracker) resourceSliceDelete(ctx context.Context) func(resourceinformers.DeletedResourceSlice) {
	logger := klog.FromContext(ctx)
	return func(deletedSlice resourceinformers.DeletedResourceSlice) {
		// 记录删除日志。
		logger.V(5).Info("ResourceSlice deleted", "slice", klog.KObj(deletedSlice))
		// 强制发送删除事件。
		t.syncSlice(ctx, deletedSlice.GetName(), true)
	}
}

// deviceTaintAdd 返回处理规则新增的回调：找出可能匹配的 slice 并逐个重算。
func (t *Tracker) deviceTaintAdd(ctx context.Context) func(*resourceapi.DeviceTaintRule) {
	logger := klog.FromContext(ctx)
	return func(rule *resourceapi.DeviceTaintRule) {
		if loggerV := logger.V(6); loggerV.Enabled() {
			loggerV.Info("DeviceTaintRule added", "deviceTaintRule", klog.Format(rule))
		} else {
			logger.V(5).Info("DeviceTaintRule added", "deviceTaintRule", klog.KObj(rule))
		}
		// 规则新增可能改变已有 slice 的污点，故对其命中的 slice 重算。
		for _, sliceName := range t.sliceNamesForRule(ctx, rule) {
			// 非 slice 自身变更，靠设备污点差异检测决定是否发事件。
			t.syncSlice(ctx, sliceName, false)
		}
	}
}

// deviceTaintUpdate 返回处理规则更新的回调：新旧规则命中的 slice 都需重算。
func (t *Tracker) deviceTaintUpdate(ctx context.Context) func(_, _ *resourceapi.DeviceTaintRule) {
	logger := klog.FromContext(ctx)
	return func(oldRule, newRule *resourceapi.DeviceTaintRule) {
		if loggerV := logger.V(6); loggerV.Enabled() {
			loggerV.Info("DeviceTaintRule updated", "diff", diff.Diff(oldRule, newRule), "deviceTaintRule", klog.KObj(newRule))
		} else {
			logger.V(5).Info("DeviceTaintRule updated", "deviceTaintRule", klog.KObj(newRule))
		}

		// 收集新旧规则命中的 slice 并集，旧匹配者可能需要撤销旧污点。
		slicesToSync := sets.New[string]()
		// 用集合去重，避免重复同步同一 slice。
		slicesToSync.Insert(t.sliceNamesForRule(ctx, oldRule)...)
		// 分别加入旧规则与新规则命中的名称。
		slicesToSync.Insert(t.sliceNamesForRule(ctx, newRule)...)
		// 遍历并集逐个重算。
		for _, sliceName := range slicesToSync.UnsortedList() {
			t.syncSlice(ctx, sliceName, false)
		}
	}
}

// deviceTaintDelete 返回处理规则删除的回调：根据可用信息重算此前可能受影响的 slice。
func (t *Tracker) deviceTaintDelete(ctx context.Context) func(resourceinformers.DeletedDeviceTaintRule) {
	logger := klog.FromContext(ctx)
	return func(deletedRule resourceinformers.DeletedDeviceTaintRule) {
		logger.V(5).Info("DeviceTaintRule deleted", "deviceTaintRule", klog.KObj(deletedRule))
		// 拿不到被删规则对象时无法按索引反查，只能扫描缓存中所有已打补丁的 slice。
		if deletedRule.OptionalObj == nil {
			// 只同步缓存中已存在的 slice，其余不受影响。
			for _, sliceName := range t.patchedResourceSlices.List() {
				t.syncSlice(ctx, sliceName.(*resourceapi.ResourceSlice).Name, false)
			}
			return
		}
		// 能拿到规则对象时，按索引精确查找受影响的 slice。
		for _, sliceName := range t.sliceNamesForRule(ctx, deletedRule.OptionalObj) {
			t.syncSlice(ctx, sliceName, false)
		}
	}
}

// syncSlice 重新计算并缓存指定 slice 的补丁版本；sendEvent 强制发布事件，避免在规则触发时做昂贵的 DeepEqual。
func (t *Tracker) syncSlice(ctx context.Context, name string, sendEvent bool) {
	// 返回前排空事件队列。
	defer t.emitEvents()

	// 把 slice 名称绑定到 logger 与 ctx。
	logger := klog.FromContext(ctx)
	logger = klog.LoggerWithValues(logger, "resourceslice", name)
	ctx = klog.NewContext(ctx, logger)
	logger.V(5).Info("syncing ResourceSlice")

	// 从 informer 索引器读取原始 slice。
	obj, sliceExists, err := t.resourceSlices.GetIndexer().GetByKey(name)
	// 查询失败：记录错误并返回。
	if err != nil {
		t.handleError(ctx, err, "failed to lookup existing resource slice", "resourceslice", name)
		return
	}
	// 读取缓存中上一次的补丁版本，供变更检测使用。
	oldPatchedObj, oldSliceExists, err := t.patchedResourceSlices.GetByKey(name)
	if err != nil {
		t.handleError(ctx, err, "failed to lookup cached patched resource slice", "resourceslice", name)
		return
	}
	// 原始 slice 已删除：清理缓存并发布删除事件。
	if !sliceExists {
		// 从缓存移除旧的补丁版本。
		err := t.patchedResourceSlices.Delete(oldPatchedObj)
		if err != nil {
			t.handleError(ctx, err, "failed to delete cached patched resource slice", "resourceslice", name)
			return
		}
		// 发布删除事件（newObj 为 nil）。
		t.pushEvent(oldPatchedObj, nil)
		logger.V(5).Info("patched ResourceSlice deleted")
		return
	}
	// 准备旧版本指针，供后续变更检测。
	var oldPatchedSlice *resourceapi.ResourceSlice
	// 缓存命中时做类型断言，防止错误类型被写入缓存。
	if oldSliceExists {
		var ok bool
		oldPatchedSlice, ok = oldPatchedObj.(*resourceapi.ResourceSlice)
		if !ok {
			t.handleError(ctx, errors.New("invalid type in resource slice cache"), "expectedType", fmt.Sprintf("%T", (*resourceapi.ResourceSlice)(nil)), "gotType", fmt.Sprintf("%T", oldPatchedObj))
			return
		}
	}
	// 同样校验原始对象的类型。
	slice, ok := obj.(*resourceapi.ResourceSlice)
	if !ok {
		t.handleError(ctx, errors.New("invalid type in resource slice cache"), fmt.Sprintf("expected type to be %T, got %T", (*resourceapi.ResourceSlice)(nil), obj))
		return
	}

	// 取出当前全部污点规则，准备应用。
	patches := typedSlice[*resourceapi.DeviceTaintRule](t.deviceTaints.GetIndexer().List())
	// 计算打补丁后的 slice。
	patchedSlice, err := t.applyPatches(ctx, slice, patches)
	if err != nil {
		t.handleError(ctx, err, "failed to apply patches to ResourceSlice", "resourceslice", klog.KObj(slice))
		return
	}

	// 由规则变更触发时仅比较设备污点是否变化，避免昂贵的整体 DeepEqual。
	if !sendEvent && oldPatchedSlice != nil {
		// 尚未强制发事件且存在旧版本时，逐设备比较。
		for i := range patchedSlice.Spec.Devices {
			// 按索引遍历设备；此处假定设备顺序与旧版本一致。
			oldDevice := oldPatchedSlice.Spec.Devices[i]
			newDevice := patchedSlice.Spec.Devices[i]
			// 任一设备的污点不同即标记需要发送事件。
			sendEvent = sendEvent ||
				!slices.EqualFunc(oldDevice.Taints, newDevice.Taints, taintsEqual)
		}
	}

	// 用新版本覆盖缓存。
	err = t.patchedResourceSlices.Add(patchedSlice)
	if err != nil {
		t.handleError(ctx, err, "failed to add patched resource slice to cache", "resourceslice", klog.KObj(patchedSlice))
		return
	}
	// 需要时发布更新事件（旧版本作为 oldObj）。
	if sendEvent {
		t.pushEvent(oldPatchedObj, patchedSlice)
	}

	// 调试级别输出新旧差异。
	if loggerV := logger.V(6); loggerV.Enabled() {
		loggerV.Info("ResourceSlice synced", "diff", diff.Diff(oldPatchedObj, patchedSlice))
	} else {
		logger.V(5).Info("ResourceSlice synced")
	}
}

// applyPatches 把匹配的污点规则依次应用到 slice；仅在确有匹配时才深拷贝，返回打补丁后的对象。
func (t *Tracker) applyPatches(ctx context.Context, slice *resourceapi.ResourceSlice, taintRules []*resourceapi.DeviceTaintRule) (*resourceapi.ResourceSlice, error) {
	// 从 ctx 取 logger。
	logger := klog.FromContext(ctx)

	// patchedSlice 初始指向原对象，首次命中时才深拷贝，避免无谓复制。
	patchedSlice := slice

	// 逐条规则处理，并为每条附上自身上下文日志。
	for _, taintRule := range taintRules {
		logger := klog.LoggerWithValues(logger, "deviceTaintRule", klog.KObj(taintRule))
		logger.V(6).Info("processing DeviceTaintRule")

		// 取规则的设备选择器。
		deviceSelector := taintRule.Spec.DeviceSelector
		// 无选择器不匹配任何设备。
		if deviceSelector == nil {
			logger.V(7).Info("DeviceTaintRule does not apply, no selector")
			continue
		}
		// 驱动不匹配则跳过。
		if deviceSelector.Driver != nil && *deviceSelector.Driver != slice.Spec.Driver {
			logger.V(7).Info("DeviceTaintRule does not apply, mismatched driver", "sliceDriver", slice.Spec.Driver, "taintDriver", *deviceSelector.Driver)
			continue
		}
		// 资源池不匹配则跳过。
		if deviceSelector.Pool != nil && *deviceSelector.Pool != slice.Spec.Pool.Name {
			logger.V(7).Info("DeviceTaintRule does not apply, mismatched pool", "slicePool", slice.Spec.Pool.Name, "taintPool", *deviceSelector.Pool)
			continue
		}
		// 可选的设备名过滤条件。
		deviceName := deviceSelector.Device
		// 遍历 slice 中的每个设备。
		for dIndex, device := range slice.Spec.Devices {
			// 用“驱动/池/设备”三元组构造日志用的设备标识。
			deviceID := deviceID(slice.Spec.Driver, slice.Spec.Pool.Name, device.Name)
			logger := logger.WithValues("device", deviceID)

			// 指定了设备名但不一致则跳过。
			if deviceName != nil && *deviceName != device.Name {
				logger.V(7).Info("DeviceTaintRule does not apply, mismatched device", "sliceDevice", device.Name, "taintDevice", *deviceSelector.Device)
				continue
			}

			// 命中：准备追加污点。
			logger.V(6).Info("applying matching DeviceTaintRule")
			// 首次命中时深拷贝，后续复用同一副本。
			if patchedSlice == slice {
				patchedSlice = slice.DeepCopy()
			}
			// 把规则定义的污点追加到该设备。
			patchedSlice.Spec.Devices[dIndex].Taints = append(patchedSlice.Spec.Devices[dIndex].Taints, taintRule.Spec.Taint)
		}
	}

	return patchedSlice, nil
}

// taintsEqual 比较污点的关键字段；TimeAdded 用 Equal 以正确处理 nil。
func taintsEqual(a, b resourceapi.DeviceTaint) bool {
	return a.Key == b.Key &&
		a.Effect == b.Effect &&
		a.Value == b.Value &&
		a.TimeAdded.Equal(b.TimeAdded) // Equal 会处理 nil。
}

// deviceID 把驱动、资源池、设备名拼成“driver/pool/device”形式的索引键。
func deviceID(driver, pool, device string) string {
	return driver + "/" + pool + "/" + device
}

// typedSlice 把 []any 转换为 []T；nil 输入返回 nil。
func typedSlice[T any](objs []any) []T {
	// 保持 nil 语义。
	if objs == nil {
		return nil
	}
	// 预分配容量并逐项做类型断言。
	typed := make([]T, 0, len(objs))
	for _, obj := range objs {
		// 调用方需保证元素类型正确，否则这里会 panic。
		typed = append(typed, obj.(T))
	}
	return typed
}
