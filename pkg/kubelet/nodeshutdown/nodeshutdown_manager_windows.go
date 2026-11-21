//go:build windows

/*
Copyright 2024 The Kubernetes Authors.

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

// Package nodeshutdown 监听节点级关机事件，并在系统关机前触发节点上 Pod 的优雅终止。
package nodeshutdown

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	kubeletevents "k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/windows/service"

	"golang.org/x/sys/windows/registry"
	"golang.org/x/sys/windows/svc/mgr"
)

const (
	// serviceKubelet 是 kubelet 的 Windows 服务名。
	serviceKubelet = "kubelet"
	// shutdownOrderRegPath 是保存预关机顺序的注册表路径。
	shutdownOrderRegPath = `SYSTEM\CurrentControlSet\Control`
	// shutdownOrderStringValue 是注册表中预关机顺序的键名。
	shutdownOrderStringValue = "PreshutdownOrder"
)

// managerImpl 实现 Windows 上的 Node Shutdown Manager 核心逻辑。
type managerImpl struct {
	logger   klog.Logger
	recorder record.EventRecorder
	nodeRef  *v1.ObjectReference

	// getPods 获取当前活跃 Pod，用于关机时逐个终止。
	getPods        eviction.ActivePodsFunc
	syncNodeStatus func(context.Context)

	// nodeShuttingDownMutex 保护 nodeShuttingDownNow 的并发访问。
	nodeShuttingDownMutex sync.Mutex
	nodeShuttingDownNow   bool
	podManager            *podManager

	enableMetrics bool
	storage       storage
}

// preShutdownHandler 将 Windows 服务的预关机回调桥接到管理器实现。
type preShutdownHandler struct {
	manager *managerImpl
}

// ProcessShutdownEvent 响应 Windows SCM 的预关机回调。
func (h *preShutdownHandler) ProcessShutdownEvent() error {
	// Windows SCM 的 preshutdown 回调没有 context，因此在此回调边界创建根 context。
	return h.manager.ProcessShutdownEvent(context.Background())
}

// NewManager 返回一个新的节点关机管理器。
func NewManager(conf *Config) Manager {
	// WindowsGracefulNodeShutdown 门控关闭时返回空实现。
	if !utilfeature.DefaultFeatureGate.Enabled(features.WindowsGracefulNodeShutdown) {
		m := managerStub{}
		conf.Logger.Info("Node shutdown manager is disabled as the feature gate is not enabled")
		return m
	}

	podManager := newPodManager(conf)

	// 未配置任何关机宽限期时同样返回空实现。
	if len(podManager.shutdownGracePeriodByPodPriority) == 0 {
		m := managerStub{}
		conf.Logger.Info("Node shutdown manager is disabled as no shutdown grace period is configured")
		return m
	}

	manager := &managerImpl{
		logger:         conf.Logger,
		recorder:       conf.Recorder,
		nodeRef:        conf.NodeRef,
		getPods:        conf.GetPodsFunc,
		syncNodeStatus: conf.SyncNodeStatusFunc,
		podManager:     podManager,
		enableMetrics:  utilfeature.DefaultFeatureGate.Enabled(features.GracefulNodeShutdownBasedOnPodPriority),
		storage: localStorage{
			Path: filepath.Join(conf.StateDirectory, localStorageStateFile),
		},
	}
	manager.logger.Info("Creating node shutdown manager",
		"shutdownGracePeriodRequested", conf.ShutdownGracePeriodRequested,
		"shutdownGracePeriodCriticalPods", conf.ShutdownGracePeriodCriticalPods,
		"shutdownGracePeriodByPodPriority", podManager.shutdownGracePeriodByPodPriority,
	)
	return manager
}

// Admit 在节点正在关机时拒绝所有 Pod 的准入。
func (m *managerImpl) Admit(_ context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	nodeShuttingDown := m.ShutdownStatus() != nil

	if nodeShuttingDown {
		return lifecycle.PodAdmitResult{
			Admit:   false,
			Reason:  NodeShutdownNotAdmittedReason,
			Message: nodeShutdownNotAdmittedMessage,
		}
	}
	return lifecycle.PodAdmitResult{Admit: true}
}

// setMetrics 从持久化状态中恢复并设置节点关机相关指标。
func (m *managerImpl) setMetrics() {
	if m.enableMetrics && m.storage != nil {
		sta := state{}
		err := m.storage.Load(&sta)
		if err != nil {
			m.logger.Error(err, "Failed to load graceful shutdown state")
		} else {
			if !sta.StartTime.IsZero() {
				metrics.GracefulShutdownStartTime.Set(timestamp(sta.StartTime))
			}
			if !sta.EndTime.IsZero() {
				metrics.GracefulShutdownEndTime.Set(timestamp(sta.EndTime))
			}
		}
	}
}

// Start 启动节点关机管理器，并注册 Windows 服务的预关机处理回调。
func (m *managerImpl) Start(_ context.Context) error {
	m.logger.V(1).Info("Shutdown manager get started")

	_, err := m.start()

	if err != nil {
		return err
	}

	// 注册预关机回调，Windows 进入关机流程时会调用该处理器。
	service.SetPreShutdownHandler(&preShutdownHandler{manager: m})

	m.setMetrics()

	return nil
}

// start 初始化 Windows 服务与注册表配置，为预关机处理做好准备。
func (m *managerImpl) start() (chan struct{}, error) {
	// 仅在以 Windows 服务方式运行时才处理关机事件。
	isServiceInitialized := service.IsServiceInitialized()
	if !isServiceInitialized {
		return nil, fmt.Errorf("%s is NOT running as a Windows service", serviceKubelet)
	}

	// 更新注册表，把 kubelet 的依赖项加入现有的预关机顺序。
	mgr, err := mgr.Connect()
	if err != nil {
		return nil, fmt.Errorf("Could not connect to service manager: %w", err)
	}
	defer mgr.Disconnect()

	s, err := mgr.OpenService(serviceKubelet)
	if err != nil {
		return nil, fmt.Errorf("Could not access service %s: %w", serviceKubelet, err)
	}
	defer s.Close()

	preshutdownInfo, err := service.QueryPreShutdownInfo(s.Handle)
	if err != nil {
		return nil, fmt.Errorf("Could not query preshutdown info: %w", err)
	}
	m.logger.V(1).Info("Shutdown manager get current preshutdown info", "PreshutdownTimeout", preshutdownInfo.PreshutdownTimeout)

	config, err := s.Config()
	if err != nil {
		return nil, fmt.Errorf("Could not access config of service %s: %w", serviceKubelet, err)
	}

	// 打开注册表键以读写预关机顺序。
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, shutdownOrderRegPath, registry.QUERY_VALUE|registry.SET_VALUE)
	if err != nil {
		return nil, fmt.Errorf("Could not access registry: %w", err)
	}
	defer key.Close()

	// 读取现有的预关机顺序。
	existingOrders, _, err := key.GetStringsValue(shutdownOrderStringValue)
	if err != nil {
		return nil, fmt.Errorf("Could not access registry value %s: %w", shutdownOrderStringValue, err)
	}
	m.logger.V(1).Info("Shutdown manager get current service preshutdown order", "Preshutdownorder", existingOrders)

	// 将 kubelet 的依赖项合并进现有顺序。
	newOrders := addToExistingOrder(config.Dependencies, existingOrders)
	err = key.SetStringsValue("PreshutdownOrder", newOrders)
	if err != nil {
		return nil, fmt.Errorf("Could not set registry %s to be new value %s: %w", shutdownOrderStringValue, newOrders, err)
	}

	// 若预关机超时小于请求的宽限期，则尝试将其调整为请求值。
	if periodRequested := m.periodRequested().Milliseconds(); periodRequested > int64(preshutdownInfo.PreshutdownTimeout) {
		m.logger.V(1).Info("Shutdown manager override preshutdown info", "ShutdownGracePeriod", periodRequested)
		err := service.UpdatePreShutdownInfo(s.Handle, uint32(periodRequested))
		if err != nil {
			return nil, fmt.Errorf("Unable to override preshoutdown config by shutdown manager: %v", err)
		}

		// 再次读取预关机信息；若覆盖成功，该值应等于请求的关机宽限期。
		preshutdownInfo, err := service.QueryPreShutdownInfo(s.Handle)
		if err != nil {
			return nil, fmt.Errorf("Unable to get preshoutdown info after overrided by shutdown manager: %v", err)
		}

		if periodRequested > int64(preshutdownInfo.PreshutdownTimeout) {
			return nil, fmt.Errorf("Shutdown manager was unable to update preshutdown info to %v (ShutdownGracePeriod), current value of preshutdown info (%v) is less than requested ShutdownGracePeriod", periodRequested, preshutdownInfo.PreshutdownTimeout)
		}
	}

	return nil, nil
}

// ShutdownStatus 在节点正在关机时返回错误，否则返回 nil。
func (m *managerImpl) ShutdownStatus() error {
	m.nodeShuttingDownMutex.Lock()
	defer m.nodeShuttingDownMutex.Unlock()

	if m.nodeShuttingDownNow {
		return fmt.Errorf("node is shutting down")
	}
	return nil
}

// ProcessShutdownEvent 处理预关机事件：标记节点关机中、同步状态并终止所有 Pod。
func (m *managerImpl) ProcessShutdownEvent(ctx context.Context) error {
	m.logger.V(1).Info("Shutdown manager detected new preshutdown event", "event", "preshutdown")

	m.recorder.Event(m.nodeRef, v1.EventTypeNormal, kubeletevents.NodeShutdown, "Shutdown manager detected preshutdown event")

	m.nodeShuttingDownMutex.Lock()
	m.nodeShuttingDownNow = true
	m.nodeShuttingDownMutex.Unlock()

	// 异步同步节点状态与 Ready 条件，避免阻塞关机处理流程。
	nodeStatusCtx := klog.NewContext(ctx, m.logger)
	go m.syncNodeStatus(nodeStatusCtx)

	m.logger.V(1).Info("Shutdown manager processing preshutdown event")
	activePods := m.getPods()

	defer func() {
		m.logger.V(1).Info("Shutdown manager completed processing preshutdown event, node will shutdown shortly")
	}()

	// 记录优雅关机的开始与结束时间，便于重启后恢复指标。
	if m.enableMetrics && m.storage != nil {
		startTime := time.Now()
		err := m.storage.Store(state{
			StartTime: startTime,
		})
		if err != nil {
			m.logger.Error(err, "Failed to store graceful shutdown state")
		}
		metrics.GracefulShutdownStartTime.Set(timestamp(startTime))
		metrics.GracefulShutdownEndTime.Set(0)

		defer func() {
			endTime := time.Now()
			err := m.storage.Store(state{
				StartTime: startTime,
				EndTime:   endTime,
			})
			if err != nil {
				m.logger.Error(err, "Failed to store graceful shutdown state")
			}
			metrics.GracefulShutdownEndTime.Set(timestamp(endTime))
		}()
	}

	return m.podManager.killPods(ctx, activePods)
}

// periodRequested 返回按 Pod 优先级配置的关机宽限期总和。
func (m *managerImpl) periodRequested() time.Duration {
	var sum int64
	for _, period := range m.podManager.shutdownGracePeriodByPodPriority {
		sum += period.ShutdownGracePeriodSeconds
	}
	return time.Duration(sum) * time.Second
}

// removeItemFromList 从字符串列表中删除指定项的所有出现（忽略大小写）。
func removeItemFromList(stringlist []string, item string) []string {
	writeIndex := 0

	// 遍历列表，只保留与目标项不匹配（忽略大小写）的元素。
	for _, listItem := range stringlist {
		if !strings.EqualFold(listItem, item) {
			stringlist[writeIndex] = listItem
			writeIndex++
		}
	}

	// 返回裁剪到有效长度的切片。
	return stringlist[:writeIndex]
}

// insertAt 在切片的指定索引处插入一个元素。
func insertAt(slice []string, index int, value string) []string {
	// 索引大于等于长度时直接追加到末尾。
	if index >= len(slice) {
		return append(slice, value)
	}

	// 先追加一个零值元素以腾出空间，再整体后移插入位置之后的元素。
	slice = append(slice, "")
	copy(slice[index+1:], slice[index:])
	slice[index] = value

	return slice
}

// addToExistingOrder 将 kubelet 及其依赖合并到现有的预关机顺序中：
// Dependencies: ["a", "b", "c", "d"]
// ExistingOrder: ["x", "b", "y", "c", "z"]
// 输出结果为：
// Modified List: ["x", "kubelet", "b", "y", "c", "z", "a", "d"]
func addToExistingOrder(dependencies []string, existingOrder []string) []string {
	// 没有依赖项时无需修改。
	if len(dependencies) == 0 {
		return existingOrder
	}

	// 先从现有顺序中移除可能已存在的 kubelet 条目，避免重复。
	existingOrder = removeItemFromList(existingOrder, serviceKubelet)

	// 记录现有顺序中的项，用于去重。
	existingOrderMap := make(map[string]bool)
	for _, item := range existingOrder {
		existingOrderMap[item] = true
	}
	// 把尚未出现在现有顺序中的依赖项追加到末尾。
	for _, item := range dependencies {
		if !existingOrderMap[item] {
			existingOrder = append(existingOrder, item)
		}
	}

	// 在第一个公共项之前插入 kubelet，以尊重现有顺序。
	firstCommonIndex := -1
	for i, item := range existingOrder {
		for _, item1 := range dependencies {
			if item == item1 {
				firstCommonIndex = i
				break
			}
		}
		if firstCommonIndex != -1 {
			break
		}
	}

	// 找到公共项时，在其之前插入 kubelet。
	if firstCommonIndex != -1 {
		existingOrder = insertAt(existingOrder, firstCommonIndex, serviceKubelet)
	}

	return existingOrder
}
