/*
Copyright 2019 The Kubernetes Authors.

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

package plugin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstats "google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	timedworkers "k8s.io/kubernetes/pkg/controller/tainteviction" // TODO (?)：将此通用辅助函数移到其他位置？
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/utils/ptr"
)

// DRAPluginManager 维护如何访问为 DRA 驱动注册的插件。
// 每个插件都有一个 gRPC endpoint，一个驱动可以对应多个插件。
//
// 为了获知可用插件，DRAPluginManager 实现 [cache.PluginHandler] 接口，
// 并需要注册到 plugin manager 中。
//
// 零值的 DRAPluginManager 不可用，请使用 NewDRAPluginManager 构造。
type DRAPluginManager struct {
	// backgroundCtx 用于 DRAPluginManager 之后的所有后台活动。
	// 之所以需要它，是因为该类实现的接口不提供 context。
	backgroundCtx context.Context
	// cancel 取消后台活动并携带取消原因。
	cancel func(err error)
	// kubeClient 用于删除本节点的 ResourceSlice。
	kubeClient kubernetes.Interface
	// getNode 获取本节点对象，用于构造 ResourceSlice 的 field selector。
	getNode func(context.Context) (*v1.Node, error)
	// wipingDelay 是驱动失联后延迟删除其 ResourceSlice 的时长。
	wipingDelay time.Duration
	// streamHandler 处理可选的资源健康流。
	streamHandler StreamHandler

	// withIdleTimeout 仅用于单元测试，<= 0 时忽略。
	withIdleTimeout time.Duration

	// wg 跟踪后台 goroutine，Stop 时等待它们退出。
	wg sync.WaitGroup
	// mutex 保护 store 以及各插件的 connected 状态。
	mutex sync.RWMutex

	// driver 名 -> 按加入顺序排列的 DRAPlugin。
	store map[string][]*monitoredPlugin

	// pendingWipes 记录 DRA 驱动的 ResourceSlice 应在何时被清除；
	// 实际删除在后台由 TimedWorkerQueue 回调完成。
	//
	// TimedWorkerQueue 使用 namespace/name 作为 key，
	// 这里用驱动名作为 name，namespace 为空。
	pendingWipes *timedworkers.TimedWorkerQueue
}

// 编译期断言：DRAPluginManager 必须实现 PluginHandler 接口。
var _ cache.PluginHandler = &DRAPluginManager{}

// monitoredPlugin 跟踪插件的 gRPC 连接当前是否已连通，
// 为此实现了 [grpcstats.Handler] 接口。
//
// 标签函数可能对上下文日志有用，但目前真正用到的只有 HandleConn。
type monitoredPlugin struct {
	*DRAPlugin
	// pm 指回所属的管理器，用于更新连通状态并触发同步。
	pm *DRAPluginManager

	// connected 由 store.mutex 保护。
	connected bool
}

// 编译期断言：monitoredPlugin 必须实现 gRPC stats Handler 接口。
var _ grpcstats.Handler = &monitoredPlugin{}

// TagRPC 原样返回 context，不附加任何标签。
func (m *monitoredPlugin) TagRPC(ctx context.Context, info *grpcstats.RPCTagInfo) context.Context {
	return ctx
}

// HandleRPC 是空实现，这里只关心连接级事件。
func (m *monitoredPlugin) HandleRPC(context.Context, grpcstats.RPCStats) {
}

// TagConn 原样返回 context，不附加任何标签。
func (m *monitoredPlugin) TagConn(ctx context.Context, info *grpcstats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn 在连接建立或断开时更新连通状态，并触发驱动可用性同步。
func (m *monitoredPlugin) HandleConn(_ context.Context, stats grpcstats.ConnStats) {
	connected := false
	switch stats.(type) {
	case *grpcstats.ConnBegin:
		connected = true
	case *grpcstats.ConnEnd:
		// 必须请求重连，否则 gRPC 不会重试，我们也无法感知插件重启。
		//
		// 这里必须放到 goroutine 中执行：在连接变为 idle 时，
		// gRPC 从 HandleConn 内直接调用会死锁（只在此时发生）。
		// 原因是 Connect 中的 cc.idlenessMgr.ExitIdleMode 会尝试获取
		// 一个已被 HandleConn 调用方持有的互斥锁。
		go m.conn.Connect()
	default:
		return
	}
	if m.pm.backgroundCtx.Err() != nil {
		// 正在关闭，不再关心连接变化。
		return
	}
	logger := klog.FromContext(m.pm.backgroundCtx)
	m.pm.mutex.Lock()
	defer m.pm.mutex.Unlock()
	logger.V(2).Info("Connection changed", "driverName", m.driverName, "endpoint", m.endpoint, "connected", connected)
	m.connected = connected
	// 连接状态变化可能改变驱动可用性，需要重新同步。
	m.pm.sync(m.driverName)
}

// NewDRAPluginManager 创建 DRAPluginManager，并支持在驱动插件长时间不可用时
// 删除其 ResourceSlice。
//
// context 可用于取消所有后台活动。也可以在取消 context 之外或代替取消
// 调用 Stop；Stop 还会等待后台活动结束。
func NewDRAPluginManager(ctx context.Context, kubeClient kubernetes.Interface, getNode func(context.Context) (*v1.Node, error), streamHandler StreamHandler, wipingDelay time.Duration) *DRAPluginManager {
	ctx, cancel := context.WithCancelCause(ctx)
	pm := &DRAPluginManager{
		backgroundCtx: klog.NewContext(ctx, klog.LoggerWithName(klog.FromContext(ctx), "DRA registration handler")),
		cancel:        cancel,
		kubeClient:    kubeClient,
		getNode:       getNode,
		wipingDelay:   wipingDelay,
		streamHandler: streamHandler,
	}
	// 定时队列到期后删除对应驱动的 ResourceSlice。
	pm.pendingWipes = timedworkers.CreateWorkerQueue(func(ctx context.Context, fireAt time.Time, args *timedworkers.WorkArgs) error {
		pm.wipeResourceSlices(ctx, args.Object.Name)
		return nil
	})

	// kubelet 启动时还没有 DRA 驱动注册，在驱动重新注册前它们都不可用，
	// 甚至可能永远不会恢复。因此不应对外通告任何本地资源，
	// 否则 Pod 可能卡在节点上等待驱动启动。
	//
	// 这段清理必须放到后台执行。
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()

		ctx := pm.backgroundCtx
		logger := klog.LoggerWithName(klog.FromContext(ctx), "startup")
		ctx = klog.NewContext(ctx, logger)
		pm.wipeResourceSlices(ctx, "" /* 所有驱动。 */)
	}()

	return pm
}

// Stop 取消所有剩余后台活动，并阻塞到所有 goroutine 停止；
// 例外是为清除 ResourceSlice 动态创建的 goroutine 未被跟踪，
// 但 context 取消后它们不会执行任何实际操作。
func (pm *DRAPluginManager) Stop() {
	defer pm.wg.Wait() // 必须先解锁再等待，避免与持锁的后台 goroutine 死锁。
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	logger := klog.FromContext(pm.backgroundCtx)
	// 先取消 context，阻止新的后台活动。
	pm.cancel(errors.New("Stop was called"))

	// 关闭所有连接，否则 gRPC 仍会在后台活动；同时取消所有待执行的清除任务。
	for driverName, plugins := range pm.store {
		workerArg := timedworkers.NewWorkArgs(driverName, "")
		pm.pendingWipes.CancelWork(logger, workerArg.KeyFromWorkArgs())
		for _, plugin := range plugins {
			if err := plugin.conn.Close(); err != nil {
				logger.Error(err, "Closing gRPC connection", "driverName", plugin.driverName, "endpoint", plugin.endpoint)
			}
		}
	}
}

// wipeResourceSlices 删除本节点的 ResourceSlice，可只针对指定驱动。
//
// 它会在 kubelet 启动时以独立 goroutine 调用，也会作为 TimedWorkersQueue
// 的回调调用。这两种调用方都无法处理错误，因此必须在内部实现重试。
//
// 可通过取消 context 中止。
func (pm *DRAPluginManager) wipeResourceSlices(ctx context.Context, driver string) {
	// 没有 kubeClient 时无法删除 ResourceSlice，直接返回。
	if pm.kubeClient == nil {
		return
	}
	logger := klog.FromContext(ctx)

	// 使用指数退避自行重试，因为调用方无法处理错误。
	backoff := wait.Backoff{
		Duration: time.Second,
		Factor:   2,
		Jitter:   0.2,
		Cap:      5 * time.Minute,
		Steps:    100,
	}

	// 错误在循环内部记录；context 取消不记录。
	_ = wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		node, err := pm.getNode(ctx)
		// 节点对象还不存在时继续重试，等待它出现。
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			logger.Error(err, "Unexpected error checking for node")
			return false, nil
		}
		// 默认只清理本节点的 ResourceSlice；指定驱动时再按驱动过滤。
		fieldSelector := fields.Set{resourceapi.ResourceSliceSelectorNodeName: node.Name}
		if driver != "" {
			fieldSelector[resourceapi.ResourceSliceSelectorDriver] = driver
		}

		err = pm.kubeClient.ResourceV1().ResourceSlices().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{FieldSelector: fieldSelector.String()})
		switch {
		case err == nil:
			logger.V(3).Info("Deleted ResourceSlices", "fieldSelector", fieldSelector)
			return true, nil
		case apierrors.IsUnauthorized(err):
			// kubelet 可能仍在获取自身凭据，稍后重试即可。
			logger.V(5).Info("Deleting ResourceSlice failed, retrying", "fieldSelector", fieldSelector, "err", err)
			return false, nil
		case apierrors.IsNotFound(err):
			logger.V(5).Info("ResourceSlices not found, nothing to delete.", "fieldSelector", fieldSelector)
			return true, nil
		default:
			// 其他错误记录日志后重试。
			logger.V(3).Info("Deleting ResourceSlice failed, retrying", "fieldSelector", fieldSelector, "err", err)
			return false, nil
		}
	})
}

// GetPlugin 返回 DRA 驱动 kubelet 插件中需要由 kubelet 调用的 gRPC 方法封装。
// 封装负责连接管理与日志，连接会在多次调用间复用。
//
// 驱动不可用时返回包含驱动名及原因的明确错误。
func (pm *DRAPluginManager) GetPlugin(driverName string) (*DRAPlugin, error) {
	// 空驱动名属于调用方错误。
	if driverName == "" {
		return nil, errors.New("DRA driver name is empty")
	}
	plugin := pm.get(driverName)
	if plugin == nil {
		return nil, fmt.Errorf("DRA driver %s is not registered", driverName)
	}
	return plugin, nil
}

// get 按驱动名获取一个 DRAPlugin。
func (pm *DRAPluginManager) get(driverName string) *DRAPlugin {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	logger := klog.FromContext(pm.backgroundCtx)

	plugins := pm.store[driverName]
	if len(plugins) == 0 {
		logger.V(5).Info("No plugin registered", "driverName", driverName)
		return nil
	}

	// 启发式规则：优先选择最近注册的插件；除 kubelet 重启后所有插件
	// 以随机顺序注册的情况外，它通常就是最新的。
	//
	// 优先选择已连接的插件，否则退回任意插件。
	for i := len(plugins) - 1; i >= 0; i-- {
		if plugin := plugins[i]; plugin.connected {
			logger.V(5).Info("Preferring connected plugin", "driverName", driverName, "endpoint", plugin.endpoint)
			return plugin.DRAPlugin
		}
	}
	plugin := plugins[len(plugins)-1]
	logger.V(5).Info("No plugin connected, using latest one", "driverName", driverName, "endpoint", plugin.endpoint)
	return plugin.DRAPlugin
}

// RegisterPlugin 实现 [cache.PluginHandler]。
// 插件就绪时由 plugin manager 调用。
//
// DRA 插件必须以驱动名注册。
//
// DRA 通过注册 API 的版本数组枚举插件提供的全部 gRPC 服务，
// 格式为 "<gRPC 包名>.<服务名>"（如 "v1beta1.DRAPlugin"）。
// 这样 kubelet 可以提前确定使用哪个版本，以及插件支持哪些可选服务。
func (pm *DRAPluginManager) RegisterPlugin(_ context.Context, driverName string, endpoint string, supportedServices []string, pluginClientTimeout *time.Duration) error {
	// 先确认双方至少有一个共同支持的 gRPC 服务版本。
	chosenService, err := pm.validateSupportedServices(driverName, supportedServices)
	if err != nil {
		return fmt.Errorf("invalid supported gRPC versions of DRA driver plugin %s at endpoint %s: %w", driverName, endpoint, err)
	}

	// DRAResourceHealth 服务是可选的：选择双方都支持的最近版本，
	// 插件未声明该服务时返回空字符串。
	chosenHealthService := pickHealthService(supportedServices)

	// 未指定调用超时时使用默认值。
	timeout := ptr.Deref(pluginClientTimeout, defaultClientCallTimeout)

	// 以驱动名为 key 保存新注册插件的 endpoint，
	// 管理器需要调用插件时据此查找。
	if err := pm.add(driverName, endpoint, chosenService, chosenHealthService, timeout); err != nil {
		// 不再包装，错误信息已包含细节。
		return err
	}

	return nil
}

// pickHealthService 返回插件与 kubelet 都支持的最新 DRAResourceHealth
// gRPC 服务；插件未声明该可选服务时返回空字符串。
func pickHealthService(supportedServices []string) string {
	for _, service := range healthServicesSupportedByKubelet {
		if slices.Contains(supportedServices, service) {
			return service
		}
	}
	return ""
}

// add 注册插件、建立 gRPC 连接，并在需要时启动健康流。
func (pm *DRAPluginManager) add(driverName string, endpoint string, chosenService string, chosenHealthService string, clientCallTimeout time.Duration) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	p := &DRAPlugin{
		driverName:          driverName,
		endpoint:            endpoint,
		chosenService:       chosenService,
		chosenHealthService: chosenHealthService,
		clientCallTimeout:   clientCallTimeout,
		backgroundCtx:       pm.backgroundCtx,
	}
	if pm.store == nil {
		pm.store = make(map[string][]*monitoredPlugin)
	}
	for _, oldP := range pm.store[driverName] {
		if oldP.endpoint == endpoint {
			// 同一个 endpoint 不能被另一个插件实例占用。
			return fmt.Errorf("endpoint %s already registered for DRA driver plugin %s", endpoint, driverName)
		}
	}

	logger := klog.FromContext(pm.backgroundCtx)

	mp := &monitoredPlugin{
		DRAPlugin: p,
		pm:        pm,
	}

	// gRPC 连接只创建一次，之后由 gRPC 按需连接服务端。
	target := "unix:" + endpoint
	logger.V(4).Info("Creating new gRPC connection", "target", target)
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(newMetricsInterceptor(driverName)),
		grpc.WithStatsHandler(mp),
	}
	if pm.withIdleTimeout > 0 {
		options = append(options, grpc.WithIdleTimeout(pm.withIdleTimeout))
	}
	conn, err := grpc.NewClient(target, options...)
	if err != nil {
		return fmt.Errorf("create gRPC connection to DRA driver %s plugin at endpoint %s: %w", driverName, endpoint, err)
	}
	p.conn = conn

	// 健康流是可选能力：仅在特性开启、注入处理器且双方支持时启动。
	if utilfeature.DefaultFeatureGate.Enabled(features.ResourceHealthStatus) && pm.streamHandler != nil && p.chosenHealthService != "" {
		pm.wg.Add(1)
		go func() {
			defer pm.wg.Done()
			streamCtx, streamCancel := context.WithCancel(p.backgroundCtx)
			p.SetHealthStream(streamCtx, streamCancel)

			// 流失效后按固定间隔重试，直到 context 被取消。
			wait.UntilWithContext(streamCtx, func(ctx context.Context) {
				logger.V(4).Info("Attempting to start WatchResources health stream")
				stream, err := p.NodeWatchResources(ctx)
				if err != nil {
					logger.V(3).Info("Failed to establish WatchResources stream, will retry", "err", err)
					return
				}

				logger.V(2).Info("Successfully started WatchResources health stream")

				err = pm.streamHandler.HandleWatchResourcesStream(ctx, stream, driverName)
				logger.V(2).Info("WatchResources health stream has ended", "error", err)
				if status.Code(err) == codes.Unimplemented {
					// 驱动声明支持健康服务但实际未实现，停止重试；
					// 若后续版本支持，会通过重新注册生效。
					logger.V(2).Info("Driver does not support WatchResources health stream, stopping")
					streamCancel()
				}
			}, 5*time.Second)
		}()
	}

	// 即使不调用任何 gRPC 方法，也要让 gRPC 尝试建立连接，
	// 以便尽早发现插件是否真的可用。
	// 这是实验性的 gRPC 方法；若被移除，需要改用其他方式，
	// 例如发送一次假的 gRPC 调用。
	// 立即触发连接建立，不等待首次业务调用。
	conn.Connect()

	pm.store[p.driverName] = append(pm.store[p.driverName], mp)
	logger.V(3).Info("Registered DRA plugin", "driverName", p.driverName, "endpoint", p.endpoint, "chosenService", p.chosenService, "numPlugins", len(pm.store[p.driverName]))
	pm.sync(p.driverName)
	return nil
}

// DeRegisterPlugin 实现 [cache.PluginHandler]。
// plugin manager 发现插件移除注册 socket、表明其不再可用后调用。
func (pm *DRAPluginManager) DeRegisterPlugin(_ context.Context, driverName, endpoint string) {
	// remove 本可以直接内联，但为了与 add 对称而保留。
	pm.remove(driverName, endpoint)
}

// remove 从 store 中删除指定插件并关闭其连接。
func (pm *DRAPluginManager) remove(driverName, endpoint string) {
	logger := klog.FromContext(pm.backgroundCtx)
	var p *monitoredPlugin
	defer func() {
		// 用 defer 保证关闭连接时不持有互斥锁；
		// 否则可能与 DRAPlugin.HandleConn 在持锁状态下互相死锁。
		if p != nil && p.conn != nil {
			// 关闭连接，既避免泄漏，也避免它继续尝试重连已注销的插件。
			if err := p.conn.Close(); err != nil {
				logger.Error(err, "Closing gRPC connection", "driverName", driverName, "endpoint", endpoint)
			}
		}
	}()
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	plugins := pm.store[driverName]
	i := slices.IndexFunc(plugins, func(mp *monitoredPlugin) bool { return mp.driverName == driverName && mp.endpoint == endpoint })
	if i == -1 {
		return
	}
	p = plugins[i]

	// 驱动只剩一个插件时删除整个 map 条目，否则原地删除该元素。
	last := len(plugins) == 1
	if last {
		delete(pm.store, driverName)
	} else {
		pm.store[driverName] = slices.Delete(plugins, i, i+1)
	}

	// 若插件的健康流仍在运行，注销时一并取消。
	// 若健康流仍在运行，注销时一并取消。
	healthCancel := p.HealthStreamCancel()
	if healthCancel != nil {
		logger.V(4).Info("Canceling health stream during deregistration")
		healthCancel()
	}

	logger.V(3).Info("Unregistered DRA plugin", "driverName", driverName, "endpoint", endpoint, "numPlugins", len(pm.store[driverName]))
	pm.sync(driverName)
}

// 插件信息每次变化都必须调用 sync；调用方必须持有写锁。
func (pm *DRAPluginManager) sync(driverName string) {
	// 没有 kubeClient，无法执行清除。
	if pm.kubeClient == nil {
		// 无法清除。
		return
	}
	ctx := pm.backgroundCtx
	logger := klog.FromContext(pm.backgroundCtx)
	workerArgs := timedworkers.NewWorkArgs(driverName, "")

	// 驱动是否已恢复可用？
	if pm.usable(driverName) {
		// 可用：取消该驱动待执行的 ResourceSlice 清除任务。
		pm.pendingWipes.CancelWork(logger, workerArgs.KeyFromWorkArgs())
		return
	}

	// 不可用：确保该驱动的 ResourceSlice 会被清除。
	// 如果此前已排入队列，则保持原有超时不变，因为状态没有变化。
	if pm.pendingWipes.GetWorkerUnsafe(workerArgs.KeyFromWorkArgs()) != nil {
		// 已经排队或可能正在执行。这里存在很小的检查/使用竞态，
		// 但可以接受：如果清除在取到指针与检查之间开始，
		// 工作依然会完成，这正是期望的结果。
		return
	}
	now := time.Now()
	fireAt := now.Add(pm.wipingDelay)
	logger = klog.LoggerWithName(logger, "driver-cleanup")
	logger = klog.LoggerWithValues(logger, "driverName", driverName)
	ctx = klog.NewContext(ctx, logger)
	pm.pendingWipes.AddWork(ctx, timedworkers.NewWorkArgs(driverName, ""), now, fireAt)
}

// usable 返回该 DRA 驱动是否至少有一个 endpoint 已就绪可处理 gRPC 调用。
// 调用方必须持有互斥锁。
func (pm *DRAPluginManager) usable(driverName string) bool {
	for _, mp := range pm.store[driverName] {
		if mp.connected {
			return true
		}
	}
	return false
}

// ValidatePlugin 实现 [cache.PluginHandler]。
// plugin manager 检测到 DRA 插件打开新的注册 socket 时调用。
func (pm *DRAPluginManager) ValidatePlugin(_ context.Context, driverName string, endpoint string, supportedServices []string) error {
	_, err := pm.validateSupportedServices(driverName, supportedServices)
	if err != nil {
		return fmt.Errorf("invalid supported gRPC versions of DRA driver plugin %s at endpoint %s: %w", driverName, endpoint, err)
	}

	return err
}

// validateSupportedServices 为 NodePrepareResources/NodeUnprepareResources
// 选出最高支持的 gRPC 服务并返回其名称（如 [drapbv1beta1.DRAPluginService]）。
// 插件不可用时返回错误。
func (pm *DRAPluginManager) validateSupportedServices(driverName string, supportedServices []string) (string, error) {
	// 插件一个服务都不声明时不可用。
	if len(supportedServices) == 0 {
		return "", errors.New("empty list of supported gRPC services (aka supported versions)")
	}

	// 选择最新且 kubelet 也支持的版本。
	chosenService := ""
	for _, service := range supportedServices {
		if slices.Contains(servicesSupportedByKubelet, service) {
			chosenService = service
			break
		}
	}

	// 没有共同支持的服务时直接报错；早期插件不声明 gRPC 服务，
	// 因此不存在回退到 alpha 的路径。
	if chosenService == "" {
		return "", fmt.Errorf("none of services supported by the plugin (%q) are supported by the kubelet (%q)", supportedServices, servicesSupportedByKubelet)
	}

	return chosenService, nil
}
