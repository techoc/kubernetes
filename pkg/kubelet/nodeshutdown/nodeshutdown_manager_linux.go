//go:build linux

/*
Copyright 2020 The Kubernetes Authors.

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
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	kubeletevents "k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/nodeshutdown/systemd"
)

const (
	// dbusReconnectPeriod 是 dbus 连接断开后重新监听关机事件的重试间隔。
	dbusReconnectPeriod = 1 * time.Second
)

// systemDbus 是创建 dbus 连接的可替换函数，便于测试注入 mock。
var systemDbus = func() (dbusInhibiter, error) {
	return systemd.NewDBusCon()
}

// dbusInhibiter 抽象了与 logind 交互所需的能力（inhibit 锁与关机事件监听）。
type dbusInhibiter interface {
	CurrentInhibitDelay() (time.Duration, error)
	InhibitShutdown() (systemd.InhibitLock, error)
	ReleaseInhibitLock(lock systemd.InhibitLock) error
	ReloadLogindConf() error
	MonitorShutdown(klog.Logger) (<-chan bool, error)
	OverrideInhibitDelay(inhibitDelayMax time.Duration) error
	Close() error
}

// managerImpl 实现 Node Shutdown Manager 的核心逻辑。
type managerImpl struct {
	logger   klog.Logger
	recorder record.EventRecorder
	nodeRef  *v1.ObjectReference

	// getPods 获取当前活跃 Pod，用于关机时逐个终止。
	getPods        eviction.ActivePodsFunc
	syncNodeStatus func(context.Context)

	newDBusConn func() (dbusInhibiter, error)
	dbusCon     dbusInhibiter
	inhibitLock systemd.InhibitLock

	// nodeShuttingDownMutex 保护 nodeShuttingDownNow 的并发访问。
	nodeShuttingDownMutex sync.Mutex
	nodeShuttingDownNow   bool
	podManager            *podManager

	enableMetrics bool
	storage       storage
}

// NewManager 返回一个新的节点关机管理器。
func NewManager(conf *Config) Manager {
	// 特性门控关闭时返回空实现，避免引入额外开销。
	if !utilfeature.DefaultFeatureGate.Enabled(features.GracefulNodeShutdown) {
		m := managerStub{}
		return m
	}

	podManager := newPodManager(conf)

	// 配置为空时同样返回空实现。
	if len(podManager.shutdownGracePeriodByPodPriority) == 0 {
		m := managerStub{}
		return m
	}

	manager := &managerImpl{
		logger:         conf.Logger,
		recorder:       conf.Recorder,
		nodeRef:        conf.NodeRef,
		getPods:        conf.GetPodsFunc,
		syncNodeStatus: conf.SyncNodeStatusFunc,
		newDBusConn:    systemDbus,
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
func (m *managerImpl) Admit(ctx context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
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

// Start 启动节点关机管理器，开始监听节点关机事件。
func (m *managerImpl) Start(ctx context.Context) error {
	stop, err := m.start(ctx)
	if err != nil {
		return err
	}
	// 启动监听循环：一旦当前监听结束（连接断开或事件流关闭），
	// 等待一个重连周期后重新建立监听。
	go func() {
		for {
			if stop != nil {
				select {
				case <-stop:
				case <-ctx.Done():
					return
				}
			}

			t := time.NewTimer(dbusReconnectPeriod)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return
			}
			// 重新建立 dbus 连接与关机事件监听。
			m.logger.V(1).Info("Restarting watch for node shutdown events")
			stop, err = m.start(ctx)
			if err != nil {
				m.logger.Error(err, "Unable to watch the node for shutdown events")
			}
		}
	}()

	m.setMetrics()
	return nil
}

// start 建立一次 dbus 连接、获取 inhibit 锁并启动关机事件监听，返回停止通道。
func (m *managerImpl) start(ctx context.Context) (chan struct{}, error) {
	systemBus, err := m.newDBusConn()
	if err != nil {
		return nil, err
	}

	// 替换前先关闭旧的 dbus 连接，避免泄漏 goroutine 与文件描述符。参见 #120613。
	if m.dbusCon != nil {
		if err := m.dbusCon.Close(); err != nil {
			m.logger.Error(err, "Failed to close previous dbus connection, ignoring and starting a new one anyway")
		}
	}
	m.dbusCon = systemBus

	// 若后续步骤失败，则关闭新连接，避免每次重试都泄漏 dbus goroutine。
	startSucceeded := false
	defer func() {
		if !startSucceeded {
			if err := m.dbusCon.Close(); err != nil {
				m.logger.Error(err, "Failed to close dbus connection after start failure")
			}
			m.dbusCon = nil
		}
	}()

	currentInhibitDelay, err := m.dbusCon.CurrentInhibitDelay()
	if err != nil {
		return nil, err
	}

	// 若 logind.conf 中配置的 InhibitDelayMaxUSec 小于请求的宽限期，
	// 则尝试将其调整为请求值。
	if periodRequested := m.podManager.periodRequested(); periodRequested > currentInhibitDelay {
		err := m.dbusCon.OverrideInhibitDelay(periodRequested)
		if err != nil {
			return nil, fmt.Errorf("unable to override inhibit delay by shutdown manager: %v", err)
		}

		err = m.dbusCon.ReloadLogindConf()
		if err != nil {
			return nil, err
		}

		// ReloadLogindConf 是异步生效的，需以指数退避轮询直到配置更新完成。
		backoff := wait.Backoff{
			Duration: 100 * time.Millisecond,
			Factor:   2.0,
			Steps:    5,
		}
		var updatedInhibitDelay time.Duration
		attempt := 0
		err = wait.ExponentialBackoff(backoff, func() (bool, error) {
			attempt += 1
			// 再次读取当前 inhibitDelay；若覆盖成功，该值应等于请求的关机宽限期。
			updatedInhibitDelay, err = m.dbusCon.CurrentInhibitDelay()
			if err != nil {
				return false, err
			}
			if periodRequested <= updatedInhibitDelay {
				return true, nil
			}
			if attempt < backoff.Steps {
				m.logger.V(3).Info("InhibitDelayMaxSec still less than requested, retrying", "attempt", attempt, "current", updatedInhibitDelay, "requested", periodRequested)
			}
			return false, nil
		})
		if err != nil {
			if !wait.Interrupted(err) {
				return nil, err
			}
			if periodRequested > updatedInhibitDelay {
				return nil, fmt.Errorf("node shutdown manager was timed out after %d attempts waiting for logind InhibitDelayMaxSec to update to %v (ShutdownGracePeriod), current value is %v", attempt, periodRequested, updatedInhibitDelay)
			}
		}

	}

	err = m.acquireInhibitLock()
	if err != nil {
		return nil, err
	}

	// 建立关机事件监听；失败时释放已获取的 inhibit 锁，避免残留锁阻塞关机。
	events, err := m.dbusCon.MonitorShutdown(m.logger)
	if err != nil {
		releaseErr := m.dbusCon.ReleaseInhibitLock(m.inhibitLock)
		if releaseErr != nil {
			return nil, fmt.Errorf("failed releasing inhibitLock: %v and failed monitoring shutdown: %v", releaseErr, err)
		}
		return nil, fmt.Errorf("failed to monitor shutdown: %v", err)
	}

	startSucceeded = true
	stop := make(chan struct{})
	go func() {
		// 监听关机事件，遵循 https://www.freedesktop.org/wiki/Software/systemd/inhibit/
		// 中描述的 logind Inhibit Delay 模式：
		// 1. 关机管理器启动时获取一个 inhibit 锁；
		// 2. 收到 shutdown(true) 事件时处理关机并释放 inhibit 锁；
		// 3. 收到 shutdown(false) 事件表示之前的关机被取消，此时重新获取 inhibit 锁。
		for {
			select {
			case <-ctx.Done():
				close(stop)
				return
			case isShuttingDown, ok := <-events:
				if !ok {
					m.logger.Error(nil, "Ended to watching the node for shutdown events")
					close(stop)
					return
				}
				m.logger.V(1).Info("Shutdown manager detected new shutdown event, isNodeShuttingDownNow", "event", isShuttingDown)

				var shutdownType string
				if isShuttingDown {
					shutdownType = "shutdown"
				} else {
					shutdownType = "cancelled"
				}
				m.logger.V(1).Info("Shutdown manager detected new shutdown event", "event", shutdownType)
				if isShuttingDown {
					m.recorder.Event(m.nodeRef, v1.EventTypeNormal, kubeletevents.NodeShutdown, "Shutdown manager detected shutdown event")
				} else {
					m.recorder.Event(m.nodeRef, v1.EventTypeNormal, kubeletevents.NodeShutdown, "Shutdown manager detected shutdown cancellation")
				}

				m.nodeShuttingDownMutex.Lock()
				m.nodeShuttingDownNow = isShuttingDown
				m.nodeShuttingDownMutex.Unlock()

				if isShuttingDown {
					// 异步同步节点状态与 Ready 条件，避免阻塞关机事件处理。
					nodeStatusCtx := klog.NewContext(ctx, m.logger)
					go m.syncNodeStatus(nodeStatusCtx)

					if err := m.processShutdownEvent(ctx); err != nil {
						m.logger.Error(err, "Shutdown manager failed to process shutdown event")
					}
				} else {
					// 关机被取消，重新获取 inhibit 锁以便下次关机时仍能延迟。
					_ = m.acquireInhibitLock()
				}
			}
		}
	}()
	return stop, nil
}

// acquireInhibitLock 获取新的 inhibit 锁，并释放此前持有的旧锁。
func (m *managerImpl) acquireInhibitLock() error {
	lock, err := m.dbusCon.InhibitShutdown()
	if err != nil {
		return err
	}
	if m.inhibitLock != 0 {
		m.dbusCon.ReleaseInhibitLock(m.inhibitLock)
	}
	m.inhibitLock = lock
	return nil
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

// processShutdownEvent 处理关机事件：记录关机时间指标并优雅终止节点上的 Pod。
func (m *managerImpl) processShutdownEvent(ctx context.Context) error {
	m.logger.V(1).Info("Shutdown manager processing shutdown event")
	activePods := m.getPods()

	// 无论成功与否都要释放 inhibit 锁，让系统能够继续关机。
	defer func() {
		m.dbusCon.ReleaseInhibitLock(m.inhibitLock)
		m.logger.V(1).Info("Shutdown manager completed processing shutdown event, node will shutdown shortly")
	}()

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

		// 记录关机结束时间，供下一次启动时通过 setMetrics 恢复指标。
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
