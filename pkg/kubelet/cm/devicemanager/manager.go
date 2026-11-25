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

// Package devicemanager 实现 kubelet 的 Device Plugin 管理：注册插件、跟踪设备健康、为容器分配设备并持久化分配状态。
package devicemanager

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	errorsutil "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/server/healthz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/checkpoint"
	plugin "k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/plugin/v1beta1"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// nodeWithoutTopology 是没有 NUMA 拓扑信息的设备所使用的伪 NUMA 节点编号。
const nodeWithoutTopology = -1

// ActivePodsFunc 返回节点上当前活跃的 Pod 列表，用于识别已终止 Pod 的残留分配。
type ActivePodsFunc func() []*v1.Pod

// ManagerImpl 负责管理 Device Plugin 的注册、健康状态与设备分配。
type ManagerImpl struct {
	checkpointdir string

	endpointStore map[string]map[string]*endpointInfo // resourceName -> socketPath -> endpointInfo 的映射。

	endpoints map[string]endpointInfo // 键为 ResourceName。
	mutex     sync.Mutex

	server plugin.Server

	// activePods 用于列出节点上活跃的 Pod，
	// 以便在更新已分配设备时扣除存量 Pod 已申请的资源。
	// 该函数在 Start 时被真实实现替换，之前是返回空列表的桩函数。
	activePods ActivePodsFunc

	// sourcesReady 表示 kubelet 各配置源是否就绪（例如 apiserver 同步完成）。
	// 只有所有源就绪后才能安全地从 checkpoint 中清理已消失的 Pod。
	sourcesReady config.SourcesReady

	// allDevices 保存当前已注册的全部设备（含不健康设备）。
	allDevices ResourceDeviceInstances

	// healthyDevices 记录各资源下健康设备的 ID 集合，是分配设备的主要来源。
	healthyDevices map[string]sets.Set[string]

	// unhealthyDevices 记录不健康设备；它们计入容量但不参与分配。
	unhealthyDevices map[string]sets.Set[string]

	// allocatedDevices 是按资源名索引的已分配设备集合。
	allocatedDevices map[string]sets.Set[string]

	// podDevices 记录 Pod/容器到设备的映射，并在重启后由 checkpoint 恢复。
	podDevices        *podDevices
	checkpointManager checkpointmanager.CheckpointManager

	// numaNodes 是底层机器上可用的 NUMA 节点编号列表。
	numaNodes []int

	// topologyAffinityStore 提供 TopologyManager 计算出的 NUMA 亲和性提示。
	topologyAffinityStore topologymanager.Store

	// devicesToReuse 保存已分配给 init 容器、可被后续容器复用的设备。
	// 普通 init 容器结束后释放，重启式 init 容器则保留分配。
	devicesToReuse PodReusableDevices

	// containerMap 提供 (pod, container) -> containerID 的映射，
	// 用于识别 kubelet 重启前就已运行的容器。
	containerMap containermap.ContainerMap

	// containerRunningSet 标记 containerMap 中由运行时报告为运行态的容器，
	// 用于区分“kubelet 重启”与“整机重启”两种场景。
	// 整机重启后容器不会处于运行态，因此无需重复分配设备。
	containerRunningSet sets.Set[string]

	// update 是设备健康状态变更的通知通道，容量 100 以避免阻塞插件回调。
	update chan resourceupdates.Update
}

// endpointInfo 组合一个设备插件端点及其能力选项。
type endpointInfo struct {
	e    endpoint
	opts *pluginapi.DevicePluginOptions
}

// sourcesReadyStub 是配置源未就绪阶段的占位实现，始终认为已就绪。
type sourcesReadyStub struct{}

// PodReusableDevices 以 Pod UID 为键，记录可复用的设备（按资源名分组）。
type PodReusableDevices map[string]map[string]sets.Set[string]

// AddSource 为空实现，桩不跟踪任何配置源。
func (s *sourcesReadyStub) AddSource(source string) {}

// AllReady 始终返回 true，使初始化阶段不会因源未就绪而跳过分配。
func (s *sourcesReadyStub) AllReady() bool { return true }

// NewManagerImpl 创建 Device Plugin 管理器；Windows 上使用不同的 socket 路径。
func NewManagerImpl(logger klog.Logger, topology []cadvisorapi.Node, topologyAffinityStore topologymanager.Store) (*ManagerImpl, error) {
	socketPath := pluginapi.KubeletSocket
	if runtime.GOOS == "windows" {
		socketPath = os.Getenv("SYSTEMDRIVE") + pluginapi.KubeletSocketWindows
	}
	return newManagerImpl(logger, socketPath, topology, topologyAffinityStore)
}

// newManagerImpl 是构造函数主体，便于测试注入自定义 socket 路径。
func newManagerImpl(logger klog.Logger, socketPath string, topology []cadvisorapi.Node, topologyAffinityStore topologymanager.Store) (*ManagerImpl, error) {
	logger.V(2).Info("Creating Device Plugin manager", "path", socketPath)

	var numaNodes []int
	for _, node := range topology {
		numaNodes = append(numaNodes, node.Id)
	}

	manager := &ManagerImpl{
		endpoints:             make(map[string]endpointInfo),
		endpointStore:         make(map[string]map[string]*endpointInfo),
		allDevices:            NewResourceDeviceInstances(),
		healthyDevices:        make(map[string]sets.Set[string]),
		unhealthyDevices:      make(map[string]sets.Set[string]),
		allocatedDevices:      make(map[string]sets.Set[string]),
		podDevices:            newPodDevices(),
		numaNodes:             numaNodes,
		topologyAffinityStore: topologyAffinityStore,
		devicesToReuse:        make(PodReusableDevices),
		update:                make(chan resourceupdates.Update, 100),
	}

	server, err := plugin.NewServer(logger, socketPath, manager, manager)
	if err != nil {
		return nil, fmt.Errorf("failed to create plugin server: %v", err)
	}

	manager.server = server
	manager.checkpointdir, _ = filepath.Split(server.SocketPath())

	// 这些依赖在 Start 时才注入真实实现；在此之前用桩保证调用安全，
	// 因此下面先初始化为空操作实现。
	manager.activePods = func() []*v1.Pod { return []*v1.Pod{} }
	manager.sourcesReady = &sourcesReadyStub{}
	checkpointManager, err := checkpointmanager.NewCheckpointManager(manager.checkpointdir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize checkpoint manager: %v", err)
	}
	manager.checkpointManager = checkpointManager

	return manager, nil
}

// Updates 返回设备健康变更的通知通道，供上层（如 podresources）订阅。
func (m *ManagerImpl) Updates() <-chan resourceupdates.Update {
	return m.update
}

// CleanupPluginDirectory 在启动时清理设备插件目录中残留的 unix socket，
// 避免连接到已失效的旧插件端点。
func (m *ManagerImpl) CleanupPluginDirectory(logger klog.Logger, dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	var errs []error
	for _, name := range names {
		filePath := filepath.Join(dir, name)
		// 绝不能删除 checkpoint 文件，否则会丢失设备分配状态。
		if filePath == m.checkpointFile() {
			continue
		}
		stat, err := os.Stat(filePath)
		if err != nil {
			logger.Error(err, "Failed to stat file", "path", filePath)
			continue
		}
		// 只删除 socket 文件，其他文件可能是插件或运维人员放置的。
		if stat.IsDir() || stat.Mode()&os.ModeSocket == 0 {
			continue
		}
		err = os.RemoveAll(filePath)
		if err != nil {
			errs = append(errs, err)
			logger.Error(err, "Failed to remove file", "path", filePath)
			continue
		}
	}
	return errorsutil.NewAggregate(errs)
}

// PluginConnected 在设备插件注册时连接新的端点。
// 同一资源可以有多个端点，按 socket 路径分别保存。
func (m *ManagerImpl) PluginConnected(ctx context.Context, resourceName string, p plugin.DevicePlugin) error {
	logger := klog.FromContext(ctx)
	options, err := p.API().GetDevicePluginOptions(ctx, &pluginapi.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get device plugin options: %v", err)
	}

	e := newEndpointImpl(p)

	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.endpointStore == nil {
		m.endpointStore = make(map[string]map[string]*endpointInfo)
	}
	if m.endpointStore[resourceName] == nil {
		m.endpointStore[resourceName] = make(map[string]*endpointInfo)
	}
	if _, exists := m.endpointStore[resourceName][e.socketPath()]; exists {
		return fmt.Errorf("device plugin already connected: %s", e.socketPath())
	}
	// endpoints 保存该资源当前主用端点，endpointStore 保存全部端点。
	m.endpoints[resourceName] = endpointInfo{e, options}
	m.endpointStore[resourceName][e.socketPath()] = &endpointInfo{e, options}

	logger.V(2).Info("Device plugin connected", "resourceName", resourceName)
	return nil
}

// PluginDisconnected 在设备插件注销时移除对应端点。
// 最后一个端点断开时把该资源整体标记为不健康。
func (m *ManagerImpl) PluginDisconnected(logger klog.Logger, resourceName string, socketPath string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	endpoints, ok := m.endpointStore[resourceName]
	if !ok {
		return
	}
	ep, exists := endpoints[socketPath]
	if !exists {
		return
	}
	ep.e.setStopTime(time.Now())

	// 最后一个端点消失意味着该资源完全不可用，容量需要降为 0。
	last := len(endpoints) == 1
	if last {
		delete(m.endpointStore, resourceName)
		m.markResourceUnhealthy(logger, resourceName)
		logger.V(2).Info("Last device plugin for this resource disconnected", "resource", resourceName)
	} else {
		delete(endpoints, socketPath)
		// 仍有其他端点时，任选一个提升为主用端点，保持 endpoints 映射有效。
		for _, remainingEp := range endpoints {
			m.endpoints[resourceName] = *remainingEp
			break
		}
	}
}

// PluginListAndWatchReceiver 接收插件的 ListAndWatch 响应，
// 并据此更新设备数量与健康状态等最新信息，
// 同时把已注册设备和设备到容器的分配信息写入 checkpoint。
// 该方法由插件服务端在收到推送时调用。
func (m *ManagerImpl) PluginListAndWatchReceiver(logger klog.Logger, resourceName string, resp *pluginapi.ListAndWatchResponse) {
	m.genericDeviceUpdateCallback(logger, resourceName, resp.Devices)
}

// genericDeviceUpdateCallback 用最新的设备列表重建健康/不健康集合，并通知受影响 Pod。
func (m *ManagerImpl) genericDeviceUpdateCallback(logger klog.Logger, resourceName string, devices []*pluginapi.Device) {
	healthyCount := 0
	m.mutex.Lock()
	m.healthyDevices[resourceName] = sets.New[string]()
	m.unhealthyDevices[resourceName] = sets.New[string]()
	oldDevices := m.allDevices[resourceName]
	podsToUpdate := sets.New[string]()
	m.allDevices[resourceName] = make(map[string]*pluginapi.Device)
	for _, dev := range devices {

		// 特性门控开启时，需要把设备健康变化同步到 Pod 状态。
		if utilfeature.DefaultFeatureGate.Enabled(features.ResourceHealthStatus) {
			// 对比旧设备的健康状态，必要时把受影响 Pod 加入待更新集合。
			updatePodUIDFn := func(deviceID string) {
				podUID, _ := m.podDevices.getPodAndContainerForDevice(resourceName, deviceID)
				if podUID != "" {
					podsToUpdate.Insert(podUID)
				}
			}
			// 仅当健康状态真正变化时才通知，避免无谓的状态刷新。
			if oldDev, ok := oldDevices[dev.ID]; ok {
				if oldDev.Health != dev.Health {
					updatePodUIDFn(dev.ID)
				}
			} else {
				// 新出现的设备可能之前短暂消失过但仍分配给某个 Pod，
				// 因此也需要发送更新，让 Pod 状态重新反映其健康情况。
				updatePodUIDFn(dev.ID)
			}
		}

		m.allDevices[resourceName][dev.ID] = dev
		if dev.Health == pluginapi.Healthy {
			m.healthyDevices[resourceName].Insert(dev.ID)
			healthyCount++
		} else {
			m.unhealthyDevices[resourceName].Insert(dev.ID)
		}
	}
	m.mutex.Unlock()

	if utilfeature.DefaultFeatureGate.Enabled(features.ResourceHealthStatus) {
		// 通道非阻塞写入：满时丢弃本次通知而不是阻塞插件回调。
		if len(podsToUpdate) > 0 {
			select {
			case m.update <- resourceupdates.Update{PodUIDs: podsToUpdate.UnsortedList()}:
			default:
				logger.Error(goerrors.New("device update channel is full"), "discard pods info", "podsToUpdate", podsToUpdate.UnsortedList())
			}
		}
	}

	// 设备列表变化后立即持久化，避免重启后容量与实际不一致。
	if err := m.writeCheckpoint(logger); err != nil {
		logger.Error(err, "Writing checkpoint encountered")
	}
	logger.V(2).Info("Processed device updates for resource", "resourceName", resourceName, "totalCount", len(devices), "healthyCount", healthyCount)
}

// GetWatcherHandler 返回插件注册的 watcher 处理器。
func (m *ManagerImpl) GetWatcherHandler() cache.PluginHandler {
	return m.server
}

// GetHealthChecker 返回插件服务的健康检查器。
func (m *ManagerImpl) GetHealthChecker() healthz.HealthChecker {
	return m.server
}

// checkpointFile 返回设备管理器 checkpoint 文件路径。
func (m *ManagerImpl) checkpointFile() string {
	return filepath.Join(m.checkpointdir, kubeletDeviceManagerCheckpoint)
}

// Start 启动设备插件管理器，
// 从 checkpoint 恢复 podDevices 与 allocatedDevices，
// 并启动设备插件注册服务。
func (m *ManagerImpl) Start(logger klog.Logger, activePods ActivePodsFunc, sourcesReady config.SourcesReady, initialContainers containermap.ContainerMap, initialContainerRunningSet sets.Set[string]) error {
	logger.V(2).Info("Starting Device Plugin manager")

	m.activePods = activePods
	m.sourcesReady = sourcesReady
	m.containerMap = initialContainers
	m.containerRunningSet = initialContainerRunningSet

	// 读取失败不阻断启动：容量会暂时为 0，待插件重新注册后恢复。
	err := m.readCheckpoint(logger)
	if err != nil {
		logger.Error(err, "Continue after failing to read checkpoint file. Device allocation info may NOT be up-to-date")
	}

	return m.server.Start(logger)
}

// Stop 停止插件服务端。
// 可并发、重复调用，且无需先调用 Start。
// 因此必须保证幂等。
func (m *ManagerImpl) Stop(logger klog.Logger) error {
	return m.server.Stop(logger)
}

// Allocate 从已注册的设备插件中为容器分配设备。
// 目前只支持 Add 操作，其他操作直接忽略。
func (m *ManagerImpl) Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "pod", klog.KObj(pod), "containerName", container.Name, "operation", operation)
	if operation != lifecycle.AddOperation {
		logger.V(2).Info("Device Manager support only Allocate(add)")
		return nil
	}
	if _, ok := m.devicesToReuse[string(pod.UID)]; !ok {
		m.devicesToReuse[string(pod.UID)] = make(map[string]sets.Set[string])
	}
	// 只保留当前 Pod 的复用表，防止旧 Pod 的条目无限累积。
	for podUID := range m.devicesToReuse {
		if podUID != string(pod.UID) {
			delete(m.devicesToReuse, podUID)
		}
	}
	// 调用方保证先遍历 init 容器再遍历普通容器；
	// 若该约定改变，这里的复用逻辑必须同步调整。
	// 否则普通容器可能抢走本应留给 init 容器的设备。
	for _, initContainer := range pod.Spec.InitContainers {
		if container.Name == initContainer.Name {

			if err := m.allocateContainerResources(ctx, pod, container, m.devicesToReuse[string(pod.UID)]); err != nil {
				return err
			}
			if !podutil.IsRestartableInitContainer(&initContainer) {
				m.podDevices.addContainerAllocatedResources(string(pod.UID), container.Name, m.devicesToReuse[string(pod.UID)])
			} else {
				// 重启式 init 容器会再次启动，因此必须保留其设备分配；
				// 从复用表中移除，防止被后续容器复用。
				// 普通 init 容器的设备则在结束后可复用。
				m.podDevices.removeContainerAllocatedResources(string(pod.UID), container.Name, m.devicesToReuse[string(pod.UID)])
			}
			return nil
		}
	}
	if err := m.allocateContainerResources(ctx, pod, container, m.devicesToReuse[string(pod.UID)]); err != nil {
		return err
	}
	m.podDevices.removeContainerAllocatedResources(string(pod.UID), container.Name, m.devicesToReuse[string(pod.UID)])
	return nil
}

// UpdatePluginResources 依据已分配设备修正 NodeInfo 的可分配资源，保证已调度 Pod 能通过准入检查。
func (m *ManagerImpl) UpdatePluginResources(node *schedulerframework.NodeInfo, attrs *lifecycle.PodAdmitAttributes) error {
	pod := attrs.Pod

	// Pod 未使用任何设备插件资源时无需调整。
	if !m.podDevices.hasPod(string(pod.UID)) {
		return nil
	}

	m.sanitizeNodeAllocatable(node)
	return nil
}

// markResourceUnhealthy 把某资源的全部健康设备移入不健康集合。
func (m *ManagerImpl) markResourceUnhealthy(logger klog.Logger, resourceName string) {
	logger.V(2).Info("Mark all resources Unhealthy for resource", "resourceName", resourceName)
	healthyDevices := sets.New[string]()
	if _, ok := m.healthyDevices[resourceName]; ok {
		healthyDevices = m.healthyDevices[resourceName]
		m.healthyDevices[resourceName] = sets.New[string]()
	}
	if _, ok := m.unhealthyDevices[resourceName]; !ok {
		m.unhealthyDevices[resourceName] = sets.New[string]()
	}
	m.unhealthyDevices[resourceName] = m.unhealthyDevices[resourceName].Union(healthyDevices)
}

// GetCapacity 在 kubelet 更新节点状态时调用，返回容量、可分配量和已失效资源列表。
// 容量计入不健康设备（仍占硬件资源），可分配量只计健康设备。
// kubelet 重启后容量会暂时为 0，直到插件重新注册；这不会影响已运行的 Pod，
// 因为准入阶段 UpdatePluginResources 会为已分配 Pod 修正 nodeinfo 容量。
func (m *ManagerImpl) GetCapacity(logger klog.Logger) (v1.ResourceList, v1.ResourceList, []string) {
	var capacity = v1.ResourceList{}
	var allocatable = v1.ResourceList{}
	deletedResources := sets.New[string]()
	m.mutex.Lock()
	for resourceName, devices := range m.healthyDevices {
		eI, ok := m.endpoints[resourceName]
		if (ok && eI.e.stopGracePeriodExpired()) || !ok {
			// endpoints 与健康设备表必须保持一致，
			// 否则可能无法正确回收已不存在的资源或设备。
			// 停止宽限期已过或端点缺失的资源都视为已失效。
			if !ok {
				logger.Info("Unexpected: healthyDevices and endpoints are out of sync")
			}
			deletedResources.Insert(resourceName)
		} else {
			capacity[v1.ResourceName(resourceName)] = *resource.NewQuantity(int64(devices.Len()), resource.DecimalSI)
			allocatable[v1.ResourceName(resourceName)] = *resource.NewQuantity(int64(devices.Len()), resource.DecimalSI)
		}
	}
	for resourceName, devices := range m.unhealthyDevices {
		eI, ok := m.endpoints[resourceName]
		if (ok && eI.e.stopGracePeriodExpired()) || !ok {
			if !ok {
				logger.Info("Unexpected: unhealthyDevices and endpoints became out of sync")
			}
			deletedResources.Insert(resourceName)
		} else {
			capacityCount := capacity[v1.ResourceName(resourceName)]
			unhealthyCount := *resource.NewQuantity(int64(devices.Len()), resource.DecimalSI)
			capacityCount.Add(unhealthyCount)
			capacity[v1.ResourceName(resourceName)] = capacityCount
		}
	}

	// 从内部状态中彻底移除已失效资源，避免其容量被反复上报。
	for resourceName := range deletedResources {
		delete(m.endpoints, resourceName)
		delete(m.healthyDevices, resourceName)
		delete(m.unhealthyDevices, resourceName)
	}

	m.mutex.Unlock()

	// 状态有变化才写 checkpoint，减少磁盘 I/O。
	if deletedResources.Len() > 0 {
		if err := m.writeCheckpoint(logger); err != nil {
			logger.Error(err, "Failed to write checkpoint file")
		}
	}
	return capacity, allocatable, deletedResources.UnsortedList()
}

// writeCheckpoint 将设备到容器的分配信息持久化到磁盘。
func (m *ManagerImpl) writeCheckpoint(logger klog.Logger) error {
	m.mutex.Lock()
	registeredDevs := make(map[string][]string)
	for resource, devices := range m.healthyDevices {
		registeredDevs[resource] = devices.UnsortedList()
	}
	data := checkpoint.New(m.podDevices.toCheckpointData(logger),
		registeredDevs)
	m.mutex.Unlock()
	// 先释放锁再写盘：checkpoint 写入可能较慢，不应阻塞分配路径。
	err := m.checkpointManager.CreateCheckpoint(kubeletDeviceManagerCheckpoint, data)
	if err != nil {
		err2 := fmt.Errorf("failed to write checkpoint file %q: %v", kubeletDeviceManagerCheckpoint, err)
		logger.Error(err, "Failed to write checkpoint file")
		return err2
	}
	logger.V(4).Info("Checkpoint file written", "checkpoint", kubeletDeviceManagerCheckpoint)
	return nil
}

// readCheckpoint 从磁盘读取设备分配信息并填充内部状态。
// checkpoint 不存在属于正常情况（首次启动），直接返回。
func (m *ManagerImpl) readCheckpoint(logger klog.Logger) error {
	cp, err := m.getCheckpoint()
	if err != nil {
		if err == errors.ErrCheckpointNotFound {
			// checkpoint 不存在时没有可恢复的数据，无需继续处理。
			logger.Error(err, "Failed to read data from checkpoint", "checkpoint", kubeletDeviceManagerCheckpoint)
			return nil
		}
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	podDevices, registeredDevs := cp.GetData()
	m.podDevices.fromCheckpointData(logger, podDevices)
	m.allocatedDevices = m.podDevices.devices()
	for resource := range registeredDevs {
		// 启动时先创建空的健康设备集合，容量保持为 0，
		// 直到对应插件重新注册并上报设备。
		m.healthyDevices[resource] = sets.New[string]()
		m.unhealthyDevices[resource] = sets.New[string]()
		m.endpoints[resource] = endpointInfo{e: newStoppedEndpointImpl(resource), opts: nil}
	}

	logger.V(4).Info("Read data from checkpoint file", "checkpoint", kubeletDeviceManagerCheckpoint)
	return nil
}

// getCheckpoint 读取并反序列化 checkpoint 文件。
func (m *ManagerImpl) getCheckpoint() (checkpoint.DeviceManagerCheckpoint, error) {
	registeredDevs := make(map[string][]string)
	devEntries := make([]checkpoint.PodDevicesEntry, 0)
	cp := checkpoint.New(devEntries, registeredDevs)
	err := m.checkpointManager.GetCheckpoint(kubeletDeviceManagerCheckpoint, cp)
	return cp, err
}

// UpdateAllocatedDevices 释放绑定在已终止 Pod 上的设备。
func (m *ManagerImpl) UpdateAllocatedDevices(logger klog.Logger) {
	activePods := m.activePods()
	// 配置源未就绪时活跃 Pod 列表不可信，贸然清理会释放仍在使用的设备。
	if !m.sourcesReady.AllReady() {
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	podsToBeRemoved := m.podDevices.pods()
	for _, pod := range activePods {
		podsToBeRemoved.Delete(string(pod.UID))
	}
	if len(podsToBeRemoved) <= 0 {
		return
	}
	logger.V(3).Info("Pods to be removed", "podUIDs", sets.List(podsToBeRemoved))
	m.podDevices.delete(sets.List(podsToBeRemoved))
	// 删除 Pod 记录后重新生成 allocatedDevices，保持两者一致。
	m.allocatedDevices = m.podDevices.devices()
}

// devicesToAllocate 计算还需要通过 Allocate RPC 申请的设备 ID。
// 无需调用 RPC 时返回空列表。
func (m *ManagerImpl) devicesToAllocate(ctx context.Context, podUID, contName, resource string, required int, reusableDevices sets.Set[string]) (sets.Set[string], error) {
	logger := klog.FromContext(ctx)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	needed := required
	// 容器重启时可能已有分配，先扣除这部分。
	// 若已有分配数与请求数不符，说明 Pod 规格被改动，直接报错。
	devices := m.podDevices.containerDevices(podUID, contName, resource)
	if devices != nil {
		logger.V(3).Info("Found pre-allocated devices for resource on pod", "resourceName", resource, "containerName", contName, "podUID", podUID, "devices", sets.List(devices))
		needed = needed - devices.Len()
		// Pod 通过 API server 准入后资源不应改变，因此这里失败即意味着状态异常。
		// 如果将来允许变更，需要重新设计这部分逻辑。
		if needed != 0 {
			return nil, fmt.Errorf("pod %q container %q changed request for resource %q from %d to %d", podUID, contName, resource, devices.Len(), required)
		}
	}

	// 三种主要场景需要区分处理：
	// 1. kubelet 正常运行时的分配（最常见）。
	// 2. kubelet 重启：插件、容器和运行时都还在运行。
	// 3. 整机重启：插件可能尚未启动。
	//    能走到这里说明运行时已启动（由系统服务依赖保证）。

	// 先处理异常场景（2 和 3）：此时 kubelet 正在重新初始化，配置源尚未全部就绪。
	// 借助运行时信息区分场景 2 与 3：
	// 若容器被报告为运行中，则只可能是 kubelet 重启；整机重启时容器也已被关闭。
	// 此时容器已持有全部所需设备，无需重复分配。
	if !m.sourcesReady.AllReady() && m.isContainerAlreadyRunning(logger, podUID, contName) {
		logger.V(3).Info("Container detected running, nothing to do", "deviceNumber", needed, "resourceName", resource, "podUID", podUID, "containerName", contName)
		return nil, nil
	}

	// 场景 2 已排除，剩下场景 3 或场景 1，可走统一路径。
	logger.V(3).Info("Need devices to allocate for pod", "deviceNumber", needed, "resourceName", resource, "podUID", podUID, "containerName", contName)
	healthyDevices, hasRegistered := m.healthyDevices[resource]

	// 以下检查只可能在场景 3 失败：
	// kubelet 重新初始化时容器先于插件出现，分配请求可能早于插件注册。
	// 这种情况下必须明确报错，等待插件就绪后重试。
	// 稳态下调度器不会把 Pod 调度到设备不足的节点。
	// 注意：必须先检查注册与健康状态再检查所需数量，否则会触发 #109595。
	// 即使绕过调度器，也仍需这些检查兜底。
	if !hasRegistered {
		return nil, fmt.Errorf("cannot allocate unregistered device %s", resource)
	}

	// 资源已注册但没有健康设备时无法分配。
	if healthyDevices.Len() == 0 {
		return nil, fmt.Errorf("no healthy devices present; cannot allocate unhealthy devices %s", resource)
	}

	// 已分配设备中有不健康的，也不能继续分配，避免部分容器不可用。
	if !healthyDevices.IsSuperset(devices) {
		return nil, fmt.Errorf("previously allocated devices are no longer healthy; cannot allocate unhealthy devices %s", resource)
	}

	// 场景 3 的已知错误路径已处理完，后续走公共逻辑。
	// kubelet 稳态下的容器重启也复用同一流程。
	if needed == 0 {
		logger.V(3).Info("No devices needed, nothing to do", "deviceNumber", needed, "resourceName", resource, "podUID", podUID, "containerName", contName)
		// 无需分配，直接返回。
		return nil, nil
	}

	// allocated 收集本次分配的设备，并在返回前同步到 allocatedDevices。
	// 闭包按需填充该集合。
	allocated := sets.New[string]()

	// allocateRemainingFrom 从给定集合中补充设备，直到满足所需数量。
	// 返回 true 表示已分配足够，调用方可提前结束。
	allocateRemainingFrom := func(devices sets.Set[string]) bool {
		// 调用 GetPreferredAllocation 时会释放锁，其他路径可能重建 allocatedDevices，
		// 因此这里需要重新初始化，防止向 nil map 写入。
		// 空集合条目可能已被清理。
		// 该重建是安全的，因为设备分配信息以 podDevices 为准。
		if m.allocatedDevices[resource] == nil {
			m.allocatedDevices[resource] = sets.New[string]()
		}
		for device := range devices.Difference(allocated) {
			m.allocatedDevices[resource].Insert(device)
			allocated.Insert(device)
			needed--
			if needed == 0 {
				return true
			}
		}
		return false
	}

	// 优先使用可复用设备，避免已分配给 init 容器的设备被浪费。
	if allocateRemainingFrom(reusableDevices) {
		return allocated, nil
	}

	// 从健康设备中排除已分配设备，得到可用设备。
	devicesInUse := m.allocatedDevices[resource]
	// 可用数量不足时立即报错，避免部分分配。
	available := m.healthyDevices[resource].Difference(devicesInUse)
	if available.Len() < needed {
		return nil, fmt.Errorf("requested number of devices unavailable for %s. Requested: %d, Available: %d", resource, needed, available.Len())
	}

	// 按 NUMA 亲和性把可用设备分为对齐、非对齐和无拓扑三类。
	aligned, unaligned, noAffinity := m.filterByAffinity(logger, podUID, contName, resource, available)

	// 对齐设备足够时，让插件在候选集合中挑选具体设备。
	// 插件只能在对齐集合内挑选，以保持 TopologyManager 的保证。
	if needed < aligned.Len() {
		// 优先使用插件推荐列表与对齐集合的交集。
		preferred, err := m.callGetPreferredAllocationIfAvailable(ctx, podUID, contName, resource, aligned.Union(allocated), allocated, required)
		if err != nil {
			return nil, err
		}
		if allocateRemainingFrom(preferred.Intersection(aligned)) {
			return allocated, nil
		}
		// 插件未返回推荐或数量不足时，退回直接使用对齐集合。
		// 若对齐集合也不够，说明状态不一致，报错。
		if allocateRemainingFrom(aligned) {
			return allocated, nil
		}

		return nil, fmt.Errorf("unexpectedly allocated less resources than required. Requested: %d, Got: %d", required, required-needed)
	}

	// 对齐设备不够时，先尽量用完对齐设备，
	// 以满足 TopologyManager 的 NUMA 对齐要求。
	// 剩余设备再交给插件推荐。
	if allocateRemainingFrom(aligned) {
		return allocated, nil
	}

	// 在全部可用设备中让插件推荐剩余设备。
	// 推荐结果仍需与可用集合求交，防止选到已占用设备。
	preferred, err := m.callGetPreferredAllocationIfAvailable(ctx, podUID, contName, resource, available.Union(allocated), allocated, required)
	if err != nil {
		return nil, err
	}
	if allocateRemainingFrom(preferred.Intersection(available)) {
		return allocated, nil
	}

	// 最后从非对齐和无拓扑设备中补齐。
	// 这是尽力而为的兜底，不保证 NUMA 对齐。
	// 若仍不足则返回错误，说明健康设备数量与容量不一致。
	if allocateRemainingFrom(unaligned) {
		return allocated, nil
	}
	if allocateRemainingFrom(noAffinity) {
		return allocated, nil
	}

	return nil, fmt.Errorf("unexpectedly allocated less resources than required. Requested: %d, Got: %d", required, required-needed)
}

// filterByAffinity 按 TopologyManager 的亲和性提示把可用设备分为三类。
func (m *ManagerImpl) filterByAffinity(logger klog.Logger, podUID, contName, resource string, available sets.Set[string]) (sets.Set[string], sets.Set[string], sets.Set[string]) {
	// 无亲和性信息时全部归入 noAffinity，不影响正常分配。
	hint := m.topologyAffinityStore.GetAffinity(logger, podUID, contName)
	if !m.deviceHasTopologyAlignment(resource) || hint.NUMANodeAffinity == nil {
		return sets.New[string](), sets.New[string](), available
	}

	// 一个设备可能关联多个 NUMA 节点；
	// 没有拓扑信息的设备归入伪节点 -1。
	// 这样后续排序时不会遗漏任何设备。
	// perNodeDevices 的键是 NUMA 节点编号。
	perNodeDevices := make(map[int]sets.Set[string])
	for d := range available {
		if m.allDevices[resource][d].Topology == nil || len(m.allDevices[resource][d].Topology.Nodes) == 0 {
			if _, ok := perNodeDevices[nodeWithoutTopology]; !ok {
				perNodeDevices[nodeWithoutTopology] = sets.New[string]()
			}
			perNodeDevices[nodeWithoutTopology].Insert(d)
			continue
		}

		for _, node := range m.allDevices[resource][d].Topology.Nodes {
			if _, ok := perNodeDevices[int(node.ID)]; !ok {
				perNodeDevices[int(node.ID)] = sets.New[string]()
			}
			perNodeDevices[int(node.ID)].Insert(d)
		}
	}

	// 收集所有相关 NUMA 节点，便于后续排序。
	var nodes []int
	for node := range perNodeDevices {
		nodes = append(nodes, node)
	}

	// 排序规则：
	// 1) 在亲和性集合内的节点优先；
	// 2) 其次是不在亲和性集合内的节点；
	// 3) 最后是无拓扑的伪节点 -1。
	// 同组内按设备数量升序，尽量把设备集中到少数节点。
	sort.Slice(nodes, func(i, j int) bool {
		// 两者都在亲和集合内时，按设备数升序。
		if hint.NUMANodeAffinity.IsSet(nodes[i]) && hint.NUMANodeAffinity.IsSet(nodes[j]) {
			return perNodeDevices[nodes[i]].Len() < perNodeDevices[nodes[j]].Len()
		}
		if hint.NUMANodeAffinity.IsSet(nodes[i]) {
			return true
		}
		if hint.NUMANodeAffinity.IsSet(nodes[j]) {
			return false
		}

		// 伪节点 -1 排在真实节点之后（二者不可能同时为 -1）。
		if nodes[i] == nodeWithoutTopology {
			return false
		}
		if nodes[j] == nodeWithoutTopology {
			return true
		}

		// 两者都是不在亲和集合内的真实节点，按设备数升序。
		return perNodeDevices[nodes[i]].Len() < perNodeDevices[nodes[j]].Len()
	})

	// 生成三个有序设备列表：
	// 第一个来自亲和掩码内的 NUMA 节点，
	// 第二个来自亲和掩码外的节点，
	// 第三个来自无 NUMA 关联的设备。
	// 由于按排序后的节点顺序遍历，组内设备也按节点设备数有序。
	// 这样优先填充设备较少的节点，有利于拓扑均衡。
	// 循环顺序决定了这一性质。
	var fromAffinity []string
	var notFromAffinity []string
	var withoutTopology []string
	for d := range available {
		// 同一设备可能出现在多个 NUMA 节点下，
		// 因此遇到第一个包含它的节点后即跳出，
		// 保证每个设备只进入一个列表。
		// 分组依据是该节点是否在亲和集合内。
		for _, n := range nodes {
			if perNodeDevices[n].Has(d) {
				if n == nodeWithoutTopology {
					withoutTopology = append(withoutTopology, d)
				} else if hint.NUMANodeAffinity.IsSet(n) {
					fromAffinity = append(fromAffinity, d)
				} else {
					notFromAffinity = append(notFromAffinity, d)
				}
				break
			}
		}
	}

	// 三个列表并集即为全部可用设备，不重不漏。
	return sets.New[string](fromAffinity...), sets.New[string](notFromAffinity...), sets.New[string](withoutTopology...)
}

// allocateContainerResources 为容器申请全部所需设备插件资源：
// 对每个新资源发起 Allocate RPC，
// 处理响应并更新缓存的容器设备映射。
// 成功后统一写一次 checkpoint。
func (m *ManagerImpl) allocateContainerResources(ctx context.Context, pod *v1.Pod, container *v1.Container, devicesToReuse map[string]sets.Set[string]) error {
	logger := klog.FromContext(ctx)
	podUID := string(pod.UID)
	contName := container.Name
	allocatedDevicesUpdated := false
	needsUpdateCheckpoint := false
	// 扩展资源不允许超卖，因此 Requests 必等于 Limits，遍历 Limits 即可。
	// 这也是设备插件只能以扩展资源形式暴露的原因。
	// 因此只需要检查 Limits。
	// 资源名即为设备插件注册的资源名。
	for k, v := range container.Resources.Limits {
		resource := string(k)
		needed := int(v.Value())
		logger.V(3).Info("Looking for needed resources", "resourceName", resource, "pod", klog.KObj(pod), "containerName", container.Name, "needed", needed)
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) && isDRAExtendedResource(pod, container.Name, resource) {
			// 由 DRA 管理的扩展资源不归设备管理器分配，避免重复处理。
			logger.V(3).Info("Skipping allocation for DRA-backed extended resource", "resourceName", resource, "pod", klog.KObj(pod), "containerName", container.Name)
			continue
		}
		if !m.isDevicePluginResource(resource) {
			continue
		}
		// 分配前先回收已终止 Pod 的残留设备，
		// 否则可用设备数量会被低估。
		if !allocatedDevicesUpdated {
			m.UpdateAllocatedDevices(logger)
			allocatedDevicesUpdated = true
		}
		allocDevices, err := m.devicesToAllocate(ctx, podUID, contName, resource, needed, devicesToReuse[resource])
		if err != nil {
			return err
		}
		if allocDevices == nil || len(allocDevices) <= 0 {
			continue
		}

		needsUpdateCheckpoint = true

		startRPCTime := time.Now()
		// Allocate 涉及插件 RPC，可能较慢，必须在锁外执行。
		// 失败时可能留下部分分配，由 UpdateAllocatedDevices 后续回收；
		// 两个容器竞争同类资源且交错执行时，双方都可能失败，这是已知权衡。
		// TODO: 如果实际出现分配效率问题，再重新审视这部分实现。
		// 同时考虑并行化设备插件的 Allocate gRPC 调用。
		m.mutex.Lock()
		eI, ok := m.endpoints[resource]
		m.mutex.Unlock()
		if !ok {
			m.mutex.Lock()
			m.allocatedDevices = m.podDevices.devices()
			m.mutex.Unlock()
			return fmt.Errorf("unknown Device Plugin %s", resource)
		}

		devs := allocDevices.UnsortedList()
		// TODO: 重构为按 Pod 聚合的单个 Allocate 请求。
		// 这样可以减少 RPC 次数并简化失败回滚。
		logger.V(4).Info("Making allocation request for device plugin", "devices", devs, "resourceName", resource, "pod", klog.KObj(pod), "containerName", container.Name)
		resp, err := eI.e.allocate(ctx, devs)
		metrics.DevicePluginAllocationDuration.WithLabelValues(resource).Observe(metrics.SinceInSeconds(startRPCTime))
		if err != nil {
			// 分配失败时以 podDevices 为准重建 allocatedDevices，
			// 丢弃本次可能的部分占用。
			m.mutex.Lock()
			m.allocatedDevices = m.podDevices.devices()
			m.mutex.Unlock()
			return err
		}

		if len(resp.ContainerResponses) == 0 {
			return fmt.Errorf("no containers return in allocation response %v", resp)
		}

		allocDevicesWithNUMA := checkpoint.NewDevicesPerNUMA()
		// 按设备所属 NUMA 节点分组后写入 podDevices。
		m.mutex.Lock()
		for dev := range allocDevices {
			if m.allDevices[resource][dev].Topology == nil || len(m.allDevices[resource][dev].Topology.Nodes) == 0 {
				allocDevicesWithNUMA[nodeWithoutTopology] = append(allocDevicesWithNUMA[nodeWithoutTopology], dev)
				continue
			}
			for idx := range m.allDevices[resource][dev].Topology.Nodes {
				node := m.allDevices[resource][dev].Topology.Nodes[idx]
				allocDevicesWithNUMA[node.ID] = append(allocDevicesWithNUMA[node.ID], dev)
			}
		}
		m.mutex.Unlock()
		m.podDevices.insert(podUID, contName, resource, allocDevicesWithNUMA, resp.ContainerResponses[0])
	}

	if needsUpdateCheckpoint {
		return m.writeCheckpoint(logger)
	}

	return nil
}

// checkPodActive 判断 Pod 是否仍在活跃列表中。
func (m *ManagerImpl) checkPodActive(pod *v1.Pod) bool {
	activePods := m.activePods()
	for _, activePod := range activePods {
		if activePod.UID == pod.UID {
			return true
		}
	}

	return false
}

// GetDeviceRunContainerOptions 返回容器运行所需的设备配置（设备节点、环境变量等）。
// 若无缓存状态则返回空结构；
// 必要时会重新分配设备以覆盖重启竞态。
func (m *ManagerImpl) GetDeviceRunContainerOptions(ctx context.Context, pod *v1.Pod, container *v1.Container) (*DeviceRunContainerOptions, error) {
	logger := klog.FromContext(ctx)
	podUID := string(pod.UID)
	contName := container.Name
	needsReAllocate := false
	for k, v := range container.Resources.Limits {
		resource := string(k)
		if !m.isDevicePluginResource(resource) || v.Value() == 0 {
			continue
		}
		err := m.callPreStartContainerIfNeeded(ctx, podUID, contName, resource)
		if err != nil {
			return nil, err
		}

		if !m.checkPodActive(pod) {
			logger.V(5).Info("Pod deleted from activePods, skip to reAllocate", "pod", klog.KObj(pod), "podUID", podUID, "containerName", container.Name)
			continue
		}

		// 资源属于设备插件但缓存中没有分配记录，
		// 通常是节点重启期间的竞态，需要重新发起分配。
		// 重新分配走标准 Add 流程，因此无需特殊处理。
		if m.podDevices.containerDevices(podUID, contName, resource) == nil {
			needsReAllocate = true
		}
	}
	if needsReAllocate {
		logger.V(2).Info("Needs to re-allocate device plugin resources for pod", "pod", klog.KObj(pod), "containerName", container.Name)
		// 设备管理器只支持 Add 操作。
		if err := m.Allocate(ctx, pod, container, lifecycle.AddOperation); err != nil {
			return nil, err
		}
	}
	return m.podDevices.deviceRunContainerOptions(logger, string(pod.UID), container.Name), nil
}

// callPreStartContainerIfNeeded 对声明了 PreStartRequired 的插件资源发起 PreStartContainer RPC。
// 该调用用于让插件在容器启动前完成设备初始化。
func (m *ManagerImpl) callPreStartContainerIfNeeded(ctx context.Context, podUID, contName, resource string) error {
	logger := klog.FromContext(ctx)
	m.mutex.Lock()
	eI, ok := m.endpoints[resource]
	if !ok {
		m.mutex.Unlock()
		return fmt.Errorf("endpoint not found in cache for a registered resource: %s", resource)
	}

	if eI.opts == nil || !eI.opts.PreStartRequired {
		m.mutex.Unlock()
		// 插件未要求 PreStart 时无需调用。
		logger.V(5).Info("Plugin options indicate to skip PreStartContainer for resource", "podUID", podUID, "resourceName", resource, "containerName", contName)
		return nil
	}

	devices := m.podDevices.containerDevices(podUID, contName, resource)
	if devices == nil {
		m.mutex.Unlock()
		return fmt.Errorf("no devices found allocated in local cache for pod %s, container %s, resource %s", podUID, contName, resource)
	}

	m.mutex.Unlock()
	devs := devices.UnsortedList()
	logger.V(4).Info("Issuing a PreStartContainer call for container", "containerName", contName, "podUID", podUID)
	_, err := eI.e.preStartContainer(ctx, devs)
	if err != nil {
		return fmt.Errorf("device plugin PreStartContainer rpc failed with err: %v", err)
	}
	// TODO：为 init RPC 增加指标支持。
	return nil
}

// callGetPreferredAllocationIfAvailable 对支持推荐的插件发起 GetPreferredAllocation RPC。
// 返回空集合表示插件未提供推荐。
func (m *ManagerImpl) callGetPreferredAllocationIfAvailable(ctx context.Context, podUID, contName, resource string, available, mustInclude sets.Set[string], size int) (sets.Set[string], error) {
	logger := klog.FromContext(ctx)
	eI, ok := m.endpoints[resource]
	if !ok {
		return nil, fmt.Errorf("endpoint not found in cache for a registered resource: %s", resource)
	}

	if eI.opts == nil || !eI.opts.GetPreferredAllocationAvailable {
		logger.V(5).Info("Plugin options indicate to skip GetPreferredAllocation for resource", "resourceName", resource, "podUID", podUID, "containerName", contName)
		return nil, nil
	}

	m.mutex.Unlock()
	// 调用插件前必须释放锁，否则可能阻塞其他分配路径。
	logger.V(4).Info("Issuing a GetPreferredAllocation call for container", "resourceName", resource, "containerName", contName, "podUID", podUID)
	resp, err := eI.e.getPreferredAllocation(ctx, available.UnsortedList(), mustInclude.UnsortedList(), size)
	m.mutex.Lock()
	if err != nil {
		return nil, fmt.Errorf("device plugin GetPreferredAllocation rpc failed with err: %v", err)
	}
	if resp != nil && len(resp.ContainerResponses) > 0 {
		return sets.New[string](resp.ContainerResponses[0].DeviceIDs...), nil
	}
	return sets.New[string](), nil
}

// sanitizeNodeAllocatable 确保 NodeInfo 的可分配资源不少于已分配量，
// 这样插件故障时已调度 Pod 仍能通过准入检查。
// 只调高不调低：容量以 kubelet 上报为准。
// 该调整只影响准入，不影响调度器的容量计算。
func (m *ManagerImpl) sanitizeNodeAllocatable(node *schedulerframework.NodeInfo) {
	var newAllocatableResource *schedulerframework.Resource
	allocatableResource := node.Allocatable
	if allocatableResource.ScalarResources == nil {
		allocatableResource.ScalarResources = make(map[v1.ResourceName]int64)
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	for resource, devices := range m.allocatedDevices {
		needed := devices.Len()
		quant, ok := allocatableResource.ScalarResources[v1.ResourceName(resource)]
		if ok && int(quant) >= needed {
			continue
		}
		// 已分配量高于可分配量时上调，
		// 使用 Clone 避免修改共享的 Resource 对象。
		if newAllocatableResource == nil {
			newAllocatableResource = allocatableResource.Clone()
		}
		newAllocatableResource.ScalarResources[v1.ResourceName(resource)] = int64(needed)
	}
	if newAllocatableResource != nil {
		node.Allocatable = newAllocatableResource
	}
}

// isDevicePluginResource 判断资源是否由设备插件管理。
func (m *ManagerImpl) isDevicePluginResource(resource string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	_, registeredResource := m.healthyDevices[resource]
	_, allocatedResource := m.allocatedDevices[resource]
	// 已注册或曾分配过的资源都视为设备插件资源，
	// 这样插件消失后仍能正确清理其设备。
	if registeredResource || allocatedResource {
		return true
	}
	return false
}

// isDRAExtendedResource 判断某容器的扩展资源是否由 DRA 管理。
// 依据 Pod 状态中的 ExtendedResourceClaimStatus 判断。
func isDRAExtendedResource(pod *v1.Pod, containerName, resourceName string) bool {
	claimStatus := pod.Status.ExtendedResourceClaimStatus
	if claimStatus != nil {
		for _, resMap := range claimStatus.RequestMappings {
			if resMap.ContainerName == containerName && resMap.ResourceName == resourceName {
				return true
			}
		}
	}
	return false
}

// GetAllocatableDevices 返回所有健康设备的信息。
func (m *ManagerImpl) GetAllocatableDevices(logger klog.Logger) ResourceDeviceInstances {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	resp := m.allDevices.Filter(m.healthyDevices)
	logger.V(4).Info("GetAllocatableDevices", "known", len(m.allDevices), "allocatable", len(resp))
	return resp
}

// AllocatePod 是 Pod 级资源分配入口，设备管理器不支持该能力。
func (m *ManagerImpl) AllocatePod(logger klog.Logger, pod *v1.Pod, _ lifecycle.Operation) error {
	// 设备只能按容器分配，因此这里直接返回成功但不做任何事。
	logger.V(2).Info("Device Manager does not support pod level resource allocation")
	return nil
}

// GetDevices 返回指定容器使用的设备。
func (m *ManagerImpl) GetDevices(podUID, containerName string) ResourceDeviceInstances {
	return m.podDevices.getContainerDevices(podUID, containerName)
}

// UpdateAllocatedResourcesStatus 把设备健康状态写入 Pod 状态的 AllocatedResourcesStatus。
func (m *ManagerImpl) UpdateAllocatedResourcesStatus(logger klog.Logger, pod *v1.Pod, status *v1.PodStatus) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 暂不处理以下低概率边界情况：
	//  - spec 中存在但 status 中缺失的容器；
	//  - spec 中申请了但 podDevices 无记录的资源。
	for i, containerStatus := range status.ContainerStatuses {
		devices := m.podDevices.getContainerDevices(string(pod.UID), containerStatus.Name)

		for resourceName, deviceInstances := range devices {
			for id, d := range deviceInstances {
				health := pluginapi.Healthy
				// 设备记录理论上一定存在，这里仍做防御性检查。
				if r, ok := m.allDevices[resourceName]; ok {
					if _, ok := r[id]; ok {
						health = m.allDevices[resourceName][id].Health
					}
				}

				d.Health = health

				deviceInstances[id] = d
			}
		}

		for resourceName, dI := range devices {
			resourceStatus := v1.ResourceStatus{
				Name:      v1.ResourceName(resourceName),
				Resources: []v1.ResourceHealth{},
			}

			for id, d := range dI {
				health := v1.ResourceHealthStatusHealthy
				if d.Health != pluginapi.Healthy {
					health = v1.ResourceHealthStatusUnhealthy
				}
				resourceStatus.Resources = append(resourceStatus.Resources, v1.ResourceHealth{
					ResourceID: v1.ResourceID(id),
					Health:     health,
				})
			}

			if status.ContainerStatuses[i].AllocatedResourcesStatus == nil {
				status.ContainerStatuses[i].AllocatedResourcesStatus = []v1.ResourceStatus{}
			}

			// 按资源名更新已有状态，不存在则追加。
			found := false
			for j, rs := range status.ContainerStatuses[i].AllocatedResourcesStatus {
				if rs.Name == resourceStatus.Name {
					status.ContainerStatuses[i].AllocatedResourcesStatus[j] = resourceStatus
					found = true
					break
				}
			}

			if !found {
				status.ContainerStatuses[i].AllocatedResourcesStatus = append(status.ContainerStatuses[i].AllocatedResourcesStatus, resourceStatus)
			}
		}
	}
}

// ShouldResetExtendedResourceCapacity 判断是否需要把扩展资源容量清零。
// 没有 checkpoint 文件说明节点是新创建的，
// 此时旧的容量上报已失效，应重置为 0。
func (m *ManagerImpl) ShouldResetExtendedResourceCapacity() bool {
	checkpoints, err := m.checkpointManager.ListCheckpoints()
	if err != nil {
		return false
	}
	return len(checkpoints) == 0
}

// isContainerAlreadyRunning 判断某容器是否在 kubelet 重启前就已在运行。
func (m *ManagerImpl) isContainerAlreadyRunning(logger klog.Logger, podUID, cntName string) bool {
	// 同一 Pod/容器名可能存在多条记录（旧容器已退出、新容器运行中），
	// 因此需要遍历全部记录，只要有一条处于运行态即认为已运行。
	// 这样可避免重复分配设备。
	// 容器运行时不可用时 containerRunningSet 为空，可能导致准入失败（#118559），
	// 注意：若 kubelet 重启时容器运行时不可用，containerRunningSet 会为空，
	// 记录该已知限制，便于日后排查类似问题。
	// 如需改进，可在运行时恢复后重新触发分配。
	// 目前接受这一风险。
	foundAnyContainer := false
	foundRunningContainer := false

	m.containerMap.Visit(func(visitPodUID, visitContainerName, visitContainerID string) {
		if visitPodUID == podUID && visitContainerName == cntName {
			foundAnyContainer = true
			if m.containerRunningSet.Has(visitContainerID) {
				foundRunningContainer = true
				logger.V(4).Info("Container found in the initial running set", "podUID", podUID, "containerName", cntName, "containerID", visitContainerID)
			}
		}
	})

	if !foundAnyContainer {
		logger.V(4).Info("Container not found in the initial map, assumed NOT running", "podUID", podUID, "containerName", cntName)
		return false
	}

	if !foundRunningContainer {
		logger.V(4).Info("Container found in map but not in running set", "podUID", podUID, "containerName", cntName)
		return false
	}

	return true
}
