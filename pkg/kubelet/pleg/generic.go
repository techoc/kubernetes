/*
Copyright 2015 The Kubernetes Authors.

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

package pleg

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/utils/clock"
)

// GenericPLEG 是一个极其简单的通用 PLEG，仅依赖周期性列表查询来发现容器变化。
// 对于尚不支持真正事件生成器的容器运行时，它可作为临时替代。
//
// 注意：GenericPLEG 假设一个容器不会在一个 relist 周期内经历创建、终止并被垃圾回收。
// 若发生这种情况，GenericPLEG 会错过与该容器相关的所有事件。relist 失败时，
// 这个窗口可能变得更长。该假设并非独有——许多 kubelet 内部组件都依赖已终止容器
// 作为记账用的墓碑。垃圾回收器已针对这类情况实现。不过，为保证 kubelet 能处理
// 缺失的容器事件，建议把 relist 周期设短，并在 kubelet 中配置一个更长时间的
// 辅助周期性同步作为安全网。
type GenericPLEG struct {
	// 容器运行时
	runtime kubecontainer.Runtime
	// 订阅者从中监听事件的通道
	eventChannel chan *PodLifecycleEvent
	// pod/容器信息的内部缓存
	// 由 relistLock 保护。
	podRecords podRecords
	// 上一次 relist 的时间
	relistTime atomic.Value
	// 缓存同步 pod 所需的运行时状态
	cache kubecontainer.Cache
	// 用于可测试性
	clock clock.Clock
	// 本次 relist 期间状态获取失败的 pod，将在下一次 relist 时重试。
	podsToReinspect sync.Map // map: podUID -> empty
	// 通过关闭该通道来停止 Generic PLEG
	stopCh chan struct{}
	// 锁定 Generic PLEG 的 relist 操作
	relistLock sync.Mutex
	// 指示 Generic PLEG 是否正在运行
	isRunning bool
	// 锁定 Generic PLEG 的启动/停止操作
	runningMu sync.Mutex
	// 指示 relist 相关参数
	relistDuration *RelistDuration
	// relistRequests 是按需单 pod relist 请求的队列。
	relistRequests chan relistRequest
	// globalRelistTimer 控制周期性的全局 relist。
	globalRelistTimer clock.Timer
}

// empty 是 podsToReinspect 的空占位值（共享同一指针以减少分配）。
var empty = &struct{}{}

// plegContainerState 与 kubecontainer.State 一一对应，唯独多了不存在（non-existent）状态。
// 引入该状态是为了覆盖完整的状态迁移场景。
type plegContainerState string

const (
	plegContainerRunning     plegContainerState = "running"
	plegContainerExited      plegContainerState = "exited"
	plegContainerUnknown     plegContainerState = "unknown"
	plegContainerNonExistent plegContainerState = "non-existent"
)

// convertState 把 kubecontainer.State 映射为 plegContainerState。
func convertState(state kubecontainer.State) plegContainerState {
	switch state {
	case kubecontainer.ContainerStateCreated:
		// kubelet 尚未使用 "created" 状态，因此将其转换为 "unknown"。
		return plegContainerUnknown
	case kubecontainer.ContainerStateRunning:
		return plegContainerRunning
	case kubecontainer.ContainerStateExited:
		return plegContainerExited
	case kubecontainer.ContainerStateUnknown:
		return plegContainerUnknown
	default:
		panic(fmt.Sprintf("unrecognized container state: %v", state))
	}
}

// relistRequest 用于跟踪按需的单 pod relist 请求。
type relistRequest struct {
	// podUID 是要执行 relist 的 pod 的 UID。
	podUID types.UID
	// timestamp 是请求发起的时间。若该请求执行时 pod 已由全局 relist 处理过，
	// 则跳过该请求。
	timestamp time.Time
}

// podRecord 保存一个 pod 的上一次（old）与当前（current）快照，用于对比生成事件。
type podRecord struct {
	old     *kubecontainer.Pod
	current *kubecontainer.Pod
}

// podRecords 按 pod UID 索引 podRecord 的集合。
type podRecords map[types.UID]*podRecord

// NewGenericPLEG 实例化一个新的 GenericPLEG 对象并返回。
func NewGenericPLEG(runtime kubecontainer.Runtime, eventChannel chan *PodLifecycleEvent,
	relistDuration *RelistDuration, cache kubecontainer.Cache,
	clock clock.Clock) PodLifecycleEventGenerator {
	if cache == nil {
		panic("cache cannot be nil")
	}
	return &GenericPLEG{
		relistDuration: relistDuration,
		runtime:        runtime,
		eventChannel:   eventChannel,
		podRecords:     make(podRecords),
		cache:          cache,
		clock:          clock,
		relistRequests: make(chan relistRequest, 200),
	}
}

// Watch 返回一个通道，订阅者可从中接收 PodLifecycleEvent 事件。
// TODO: 支持多个订阅者。
func (g *GenericPLEG) Watch() chan *PodLifecycleEvent {
	return g.eventChannel
}

// Start 启动一个协程，周期性地执行 relist。
func (g *GenericPLEG) Start(ctx context.Context) {
	g.runningMu.Lock()
	defer g.runningMu.Unlock()
	if g.isRunning {
		return
	}

	g.isRunning = true
	g.stopCh = make(chan struct{})
	// 定时器设为 0，立即触发第一次 relist
	// RelistPeriod 在第一次 relist 之后才设置。
	g.globalRelistTimer = g.clock.NewTimer(0)

	go func() {
		for g.workerLoopIteration(ctx) {
		}
	}()
}

// Stop 停止 Generic PLEG：关闭停止通道、停止定时器并复位运行状态。
func (g *GenericPLEG) Stop() {
	g.runningMu.Lock()
	defer g.runningMu.Unlock()
	if g.isRunning {
		close(g.stopCh)
		g.isRunning = false
		g.globalRelistTimer.Stop()
	}
}

// workerLoopIteration 是 PLEG 的主要工作分发器，通过非阻塞的 channel 读取
// 按优先级选择要执行的工作：
// 1. 停止：若 stopCh 已关闭，退出 workerLoop。
// 2. 全局 relist：全局 relist 定时器优先于单 pod relist。
// 3. 单 pod relist：执行单 pod relist 队列中的请求。
func (g *GenericPLEG) workerLoopIteration(ctx context.Context) bool {
	// 使用非阻塞 select 实现优先级选择。

	// 第一优先级：stopCh
	select {
	case <-g.stopCh:
		return false
	default:
	}

	// 第二优先级：全局 relist
	select {
	case <-g.globalRelistTimer.C():
		g.Relist(ctx)
		g.globalRelistTimer.Reset(g.relistDuration.RelistPeriod)
		return true
	default:
	}

	// 兜底：阻塞式 select
	select {
	case <-g.stopCh:
		return false
	case <-g.globalRelistTimer.C():
		g.Relist(ctx)
		g.globalRelistTimer.Reset(g.relistDuration.RelistPeriod)
	case req := <-g.relistRequests:
		if req.timestamp.After(g.getRelistTime()) {
			// 该请求尚未被全局 relist 处理过。
			g.relistPod(ctx, req.podUID)
		}
	}

	return true
}

// 在 GenericPLEG 运行期间不得调用此方法。
func (g *GenericPLEG) Update(relistDuration *RelistDuration) {
	g.relistDuration = relistDuration
}

// Healthy 检查 PLEG 是否正常工作。
// relistThreshold 是两次 relist 之间允许的最大间隔。
func (g *GenericPLEG) Healthy() (bool, error) {
	relistTime := g.getRelistTime()
	if relistTime.IsZero() {
		return false, fmt.Errorf("pleg has yet to be successful")
	}
	// 作为指标暴露，便于基于 `time()-pleg_last_seen_seconds > nn` 配置告警
	metrics.PLEGLastSeen.Set(float64(relistTime.Unix()))
	elapsed := g.clock.Since(relistTime)
	if elapsed > g.relistDuration.RelistThreshold {
		return false, fmt.Errorf("pleg was last seen active %v ago; threshold is %v", elapsed, g.relistDuration.RelistThreshold)
	}
	return true, nil
}

// generateEvents 根据容器状态的新旧变化生成对应的生命周期事件；
// 状态未变化时返回 nil。
func generateEvents(logger klog.Logger, podID types.UID, cid string, oldState, newState plegContainerState) []*PodLifecycleEvent {
	if newState == oldState {
		return nil
	}

	logger.V(4).Info("GenericPLEG", "podUID", podID, "containerID", cid, "oldState", oldState, "newState", newState)
	switch newState {
	case plegContainerRunning:
		return []*PodLifecycleEvent{{ID: podID, Type: ContainerStarted, Data: cid}}
	case plegContainerExited:
		return []*PodLifecycleEvent{{ID: podID, Type: ContainerDied, Data: cid}}
	case plegContainerUnknown:
		return []*PodLifecycleEvent{{ID: podID, Type: ContainerChanged, Data: cid}}
	case plegContainerNonExistent:
		switch oldState {
		case plegContainerExited:
			// 此前已上报过该容器死亡事件。
			return []*PodLifecycleEvent{{ID: podID, Type: ContainerRemoved, Data: cid}}
		default:
			return []*PodLifecycleEvent{{ID: podID, Type: ContainerDied, Data: cid}, {ID: podID, Type: ContainerRemoved, Data: cid}}
		}
	default:
		panic(fmt.Sprintf("unrecognized container state: %v", newState))
	}
}

// getRelistTime 读取最近一次 relist 的时间；从未 relist 过则返回零值。
func (g *GenericPLEG) getRelistTime() time.Time {
	val := g.relistTime.Load()
	if val == nil {
		return time.Time{}
	}
	return val.(time.Time)
}

// updateRelistTime 记录最近一次 relist 的时间。
func (g *GenericPLEG) updateRelistTime(timestamp time.Time) {
	g.relistTime.Store(timestamp)
}

// Relist 向容器运行时查询 pod/容器列表，与内部记录的 pod/容器对比，
// 并据此生成事件。
func (g *GenericPLEG) Relist(ctx context.Context) {
	g.relistLock.Lock()
	defer g.relistLock.Unlock()
	logger := klog.FromContext(ctx)

	logger.V(5).Info("GenericPLEG: Relisting")

	if lastRelistTime := g.getRelistTime(); !lastRelistTime.IsZero() {
		metrics.PLEGRelistInterval.Observe(metrics.SinceInSeconds(lastRelistTime))
	}

	timestamp := g.clock.Now()
	defer func() {
		metrics.PLEGRelistDuration.Observe(metrics.SinceInSeconds(timestamp))
	}()

	// 获取全部 pod。
	podList, err := g.runtime.GetPods(ctx, true)
	if err != nil {
		logger.Error(err, "GenericPLEG: Unable to retrieve pods")
		return
	}

	g.updateRelistTime(timestamp)

	pods := kubecontainer.Pods(podList)
	// 更新运行中的 pod 与容器数量指标
	updateRunningPodAndContainerMetrics(pods)
	g.podRecords.setCurrent(pods)

	for pid := range g.podRecords {
		g.reconcilePodRecord(ctx, pid)
	}

	// 更新缓存时间戳。这一步必须发生在所有 pod 都已正确更新进缓存之后。
	g.cache.UpdateTime(timestamp)
}

// reconcilePodRecord 对比 pod 的旧记录与当前记录，检查值得产生事件的状态变化。
// 若产生了事件或需要重新检查，则获取更新后的状态、更新 pod 缓存并投递事件。
// 该函数只能在当前 pod 记录已更新之后调用。
func (g *GenericPLEG) reconcilePodRecord(ctx context.Context, pid types.UID) {
	logger := klog.FromContext(ctx)
	// 对比旧的与当前的 pod，生成事件。
	oldPod := g.podRecords.getOld(pid)
	pod := g.podRecords.getCurrent(pid)
	// 获取旧 pod 与新 pod 中的全部容器。
	allContainers := getContainersFromPods(oldPod, pod)
	var events []*PodLifecycleEvent
	for _, container := range allContainers {
		containerEvents := computeEvents(logger, oldPod, pod, &container.ID)
		events = append(events, containerEvents...)
	}

	_, reinspect := g.podsToReinspect.LoadAndDelete(pid)

	if len(events) == 0 && !reinspect {
		// 该 pod 无需其他处理。
		return
	}

	// updateCache() 会检查 pod 并更新缓存。若检查过程中出错，
	// 我们希望 PLEG 在下一次 relist 时重试。为此，这里不更新该 pod
	// 对应的 podRecord，使变化在下次 relist 时能被再次检测到。
	// TODO: 若同一 relist 周期内多个 pod 发生变化，逐个检查 pod 并获取
	// PodStatus 来更新缓存可能会耗时较长。需要留意这一点，必要时应并行化。
	status, err := g.updateCache(ctx, pod, pid)
	if err != nil {
		// 依赖 updateCache 调用 GetPodStatus 来记录实际的错误信息。
		logger.V(4).Info("PLEG: Ignoring events for pod", "pod", klog.KRef(pod.Namespace, pod.Name), "err", err)

		// 确保下次 relist 时重新检查该 pod
		g.podsToReinspect.Store(pid, empty)

		return
	}

	if len(events) == 0 {
		// 确保一次完整的重新检查之后始终触发一次 PodSync。
		events = append(events, &PodLifecycleEvent{ID: pid, Type: PodSync})
	}

	// 更新内部存储并发送事件。
	g.podRecords.update(pid)

	// 从容器 ID 到退出码的映射，用作临时的查询缓存
	containerExitCode := make(map[string]int)

	for i := range events {
		// 过滤掉尚不可靠、且还没有其他组件使用的事件。
		if events[i].Type == ContainerChanged {
			continue
		}
		select {
		case g.eventChannel <- events[i]:
		default:
			metrics.PLEGDiscardEvents.Inc()
			logger.Error(nil, "Event channel is full, discard this relist() cycle event")
		}
		// 在容器以特定事件结束时，记录其退出码
		if events[i].Type == ContainerDied {
			// 首次出现 ContainerDied 事件时，填充 containerExitCode 映射
			if len(containerExitCode) == 0 && pod != nil {
				if err == nil {
					for _, containerStatus := range status.ContainerStatuses {
						containerExitCode[containerStatus.ID.ID] = containerStatus.ExitCode
					}
				}
			}
			if containerID, ok := events[i].Data.(string); ok {
				if exitCode, ok := containerExitCode[containerID]; ok && pod != nil {
					logger.V(2).Info("Generic (PLEG): container finished", "podID", pod.ID, "containerID", containerID, "exitCode", exitCode)
				}
			}
		}
	}
}

// relistPod 对单个 pod 执行一次按需 relist：拉取其运行时状态、更新当前记录，
// 再走 reconcilePodRecord 生成事件并刷新缓存观察时间。
func (g *GenericPLEG) relistPod(ctx context.Context, podUID types.UID) {
	g.relistLock.Lock()
	defer g.relistLock.Unlock()
	logger := klog.FromContext(ctx)

	logger.V(5).Info("GenericPLEG: Relisting Pod", "podUID", podUID)

	startTime := g.clock.Now()
	defer func() {
		metrics.PLEGPodRelistDuration.Observe(metrics.SinceInSeconds(startTime))
	}()
	pod, err := g.runtime.GetPod(ctx, podUID)
	if err != nil {
		// 这里除记录错误外无需做其他处理，因为下一次全局 relist 会重试。
		logger.Error(err, "GenericPLEG: Unable to retrieve pod", "podUID", podUID)
		return
	}

	// 更新当前 pod 记录
	if pr, ok := g.podRecords[pod.ID]; ok {
		pr.current = pod
	} else {
		g.podRecords[pod.ID] = &podRecord{current: pod}
	}

	g.reconcilePodRecord(ctx, podUID)

	// 由于这里没有设置全局时间戳，需要把这个 pod 标记为已观察过，
	// 以备其状态并未发生变化的情况。
	g.cache.SetObservedTime(podUID, pod.Timestamp)
}

// getContainersFromPods 合并若干个 pod 中的容器与沙箱，按容器 ID 去重后返回。
func getContainersFromPods(pods ...*kubecontainer.Pod) []*kubecontainer.Container {
	cidSet := sets.New[string]()
	var containers []*kubecontainer.Container
	fillCidSet := func(cs []*kubecontainer.Container) {
		for _, c := range cs {
			cid := c.ID.ID
			if cidSet.Has(cid) {
				continue
			}
			cidSet.Insert(cid)
			containers = append(containers, c)
		}
	}

	for _, p := range pods {
		if p == nil {
			continue
		}
		fillCidSet(p.Containers)
		// 把沙箱也当作容器处理
		// TODO: 显式跟踪沙箱。
		fillCidSet(p.Sandboxes)
	}
	return containers
}

// computeEvents 获取容器在旧 pod 与新 pod 中的状态，并据此生成生命周期事件。
func computeEvents(logger klog.Logger, oldPod, newPod *kubecontainer.Pod, cid *kubecontainer.ContainerID) []*PodLifecycleEvent {
	var pid types.UID
	if oldPod != nil {
		pid = oldPod.ID
	} else if newPod != nil {
		pid = newPod.ID
	}
	oldState := getContainerState(oldPod, cid)
	newState := getContainerState(newPod, cid)
	return generateEvents(logger, pid, cid.ID, oldState, newState)
}

// getPodIPs 在新状态没有 pod IP 且其沙箱均已退出时，
// 沿用旧缓存状态中的 pod IP。
func (g *GenericPLEG) getPodIPs(pid types.UID, status *kubecontainer.PodStatus) []string {
	if len(status.IPs) != 0 {
		return status.IPs
	}

	oldStatus, err := g.cache.Get(pid)
	if err != nil || len(oldStatus.IPs) == 0 {
		return nil
	}

	for _, sandboxStatus := range status.SandboxStatuses {
		// 若至少有一个沙箱处于就绪状态，则使用本次状态更新的 pod IP
		if sandboxStatus.State == runtimeapi.PodSandboxState_SANDBOX_READY {
			return status.IPs
		}
	}

	// 对于没有就绪容器或沙箱的 pod（如已退出的 pod），
	// 使用旧状态的 pod IP
	return oldStatus.IPs
}

// updateCache 尝试把 pod 状态更新进 kubelet 缓存。
func (g *GenericPLEG) updateCache(ctx context.Context, pod *kubecontainer.Pod, pid types.UID) (*kubecontainer.PodStatus, error) {
	logger := klog.FromContext(ctx)
	if pod == nil {
		// 该 pod 在本次 relist 中缺失，意味着它没有可见的
		// （活动或非活动）容器。
		logger.V(4).Info("PLEG: Delete status for pod", "podUID", string(pid))
		g.cache.Delete(pid)
		return nil, nil
	}

	timestamp := pod.Timestamp
	status, err := g.runtime.GetPodStatus(ctx, pod)
	if err != nil {
		// nolint:logcheck // 在 if 分支内未使用 klog.V 的结果没有问题，
		// 这里只用它判断是否应添加额外的 "podStatus" 键及其值。
		if logger.V(6).Enabled() {
			logger.Error(err, "PLEG: Write status", "pod", klog.KRef(pod.Namespace, pod.Name), "podStatus", status)
		} else {
			logger.Error(err, "PLEG: Write status", "pod", klog.KRef(pod.Namespace, pod.Name))
		}
	} else {
		if klogV := logger.V(6); klogV.Enabled() {
			logger.Info("PLEG: Write status", "pod", klog.KRef(pod.Namespace, pod.Name), "podStatus", status)
		} else {
			logger.V(4).Info("PLEG: Write status", "pod", klog.KRef(pod.Namespace, pod.Name))
		}
		// 若新状态中的 IP 为空，则在缓存更新过程中保留旧的 pod IP。
		// pod 拆除时，kubelet 可能与 PLEG 竞争，在网络拆除之后才拿到 pod 状态，
		// 但 Kubernetes API 期望已完成 pod 在死亡后其 IP 仍然可用。
		status.IPs = g.getPodIPs(pid, status)
	}

	g.cache.Set(pod.ID, status, err, timestamp)
	return status, err
}

// RequestReinspect 请求在下次 relist 时重新检查指定 pod。
func (g *GenericPLEG) RequestReinspect(podUID types.UID) {
	g.podsToReinspect.Store(podUID, empty)
}

// RequestRelist 在 PLEGOnDemandRelist 特性门控启用时，把单 pod relist 请求
// 投递到请求队列；队列已满则丢弃并记录错误。
func (g *GenericPLEG) RequestRelist(logger klog.Logger, podUID types.UID) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.PLEGOnDemandRelist) {
		return
	}

	select {
	case g.relistRequests <- relistRequest{podUID, time.Now()}:
	default:
		logger.Error(nil, "Relist request channel full; dropping relist request", "podUID", podUID)
	}
}

// getContainerState 获取指定容器在 pod 中的状态；
// 容器不存在（含沙箱）时返回 non-existent。
func getContainerState(pod *kubecontainer.Pod, cid *kubecontainer.ContainerID) plegContainerState {
	// 默认取不存在状态。
	state := plegContainerNonExistent
	if pod == nil {
		return state
	}
	c := pod.FindContainerByID(*cid)
	if c != nil {
		return convertState(c.State)
	}
	// 同时在沙箱中查找。
	c = pod.FindSandboxByID(*cid)
	if c != nil {
		return convertState(c.State)
	}

	return state
}

// updateRunningPodAndContainerMetrics 根据 pod 列表更新运行中容器与
// 运行中 pod（以运行中沙箱计）的指标。
func updateRunningPodAndContainerMetrics(pods []*kubecontainer.Pod) {
	runningSandboxNum := 0
	// 中间映射，记录每种 "container_state" 的数量
	containerStateCount := make(map[string]int)

	for _, pod := range pods {
		containers := pod.Containers
		for _, container := range containers {
			// 更新映射中对应 "container_state" 的计数，用于设置 gaugeVec 指标的值
			containerStateCount[string(container.State)]++
		}

		sandboxes := pod.Sandboxes

		for _, sandbox := range sandboxes {
			if sandbox.State == kubecontainer.ContainerStateRunning {
				runningSandboxNum++
				// 每个 pod 应当只有一个运行中的沙箱
				break
			}
		}
	}
	for key, value := range containerStateCount {
		metrics.RunningContainerCount.WithLabelValues(key).Set(float64(value))
	}

	// 设置运行中 pod 数量的指标值
	metrics.RunningPodCount.Set(float64(runningSandboxNum))
}

// getOld 返回指定 pod 的旧快照，不存在时返回 nil。
func (pr podRecords) getOld(id types.UID) *kubecontainer.Pod {
	r, ok := pr[id]
	if !ok {
		return nil
	}
	return r.old
}

// getCurrent 返回指定 pod 的当前快照，不存在时返回 nil。
func (pr podRecords) getCurrent(id types.UID) *kubecontainer.Pod {
	r, ok := pr[id]
	if !ok {
		return nil
	}
	return r.current
}

// setCurrent 重置全部记录的当前快照，并把给定的 pod 列表写入各记录的 current。
func (pr podRecords) setCurrent(pods []*kubecontainer.Pod) {
	for i := range pr {
		pr[i].current = nil
	}
	for _, pod := range pods {
		if r, ok := pr[pod.ID]; ok {
			r.current = pod
		} else {
			pr[pod.ID] = &podRecord{current: pod}
		}
	}
}

// update 把指定 pod 的当前快照迁移为旧快照。
func (pr podRecords) update(id types.UID) {
	r, ok := pr[id]
	if !ok {
		return
	}
	pr.updateInternal(id, r)
}

// updateInternal 执行一次快照迁移：current 置为 nil 并成为新的 old；
// 若 pod 已不存在则直接删除该记录。
func (pr podRecords) updateInternal(id types.UID, r *podRecord) {
	if r.current == nil {
		// pod 已不存在，删除该条目。
		delete(pr, id)
		return
	}
	r.old = r.current
	r.current = nil
}
