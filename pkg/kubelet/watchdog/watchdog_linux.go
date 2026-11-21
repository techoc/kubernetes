//go:build linux

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

package watchdog

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/klog/v2"
)

// WatchdogClient 定义与 systemd watchdog 交互的接口。
type WatchdogClient interface {
	// 返回 systemd watchdog 超时时间。
	SdWatchdogEnabled(unsetEnvironment bool) (time.Duration, error)
	// 向 systemd 发送心跳通知。
	SdNotify(unsetEnvironment bool) (bool, error)
}

// DefaultWatchdogClient 使用真实的 systemd daemon 函数实现 WatchdogClient。
type DefaultWatchdogClient struct{}

// 编译期断言：DefaultWatchdogClient 实现 WatchdogClient 接口。
var _ WatchdogClient = &DefaultWatchdogClient{}

// SdWatchdogEnabled 返回 systemd watchdog 的超时时间。
func (d *DefaultWatchdogClient) SdWatchdogEnabled(unsetEnvironment bool) (time.Duration, error) {
	return daemon.SdWatchdogEnabled(unsetEnvironment)
}

// SdNotify 向 systemd 发送 WATCHDOG=1 心跳。
func (d *DefaultWatchdogClient) SdNotify(unsetEnvironment bool) (bool, error) {
	return daemon.SdNotify(unsetEnvironment, daemon.SdNotifyWatchdog)
}

// Option 定义初始化 healthChecker 的可选参数。
type Option func(*healthChecker)

// WithWatchdogClient 注入自定义 watchdog 客户端，便于测试。
func WithWatchdogClient(watchdog WatchdogClient) Option {
	return func(hc *healthChecker) {
		hc.watchdog = watchdog
	}
}

// healthChecker 定期检查 kubelet 健康并向 systemd watchdog 发送心跳。
type healthChecker struct {
	checkers     atomic.Value
	retryBackoff wait.Backoff
	interval     time.Duration
	watchdog     WatchdogClient
}

// 编译期断言：healthChecker 实现 HealthChecker 接口。
var _ HealthChecker = &healthChecker{}

// 最小通知间隔，防止配置过小导致忙循环。
const minimalNotifyInterval = time.Second

// NewHealthChecker 创建 HealthChecker 实例。
// 根据 systemd watchdog 状态配置行为；watchdog 未启用时返回空实例且不报错，
// 配置错误时返回错误。
func NewHealthChecker(logger klog.Logger, opts ...Option) (HealthChecker, error) {
	hc := &healthChecker{
		watchdog: &DefaultWatchdogClient{},
	}
	for _, o := range opts {
		o(hc)
	}

	// get watchdog information
	// 读取 watchdog 超时配置；配置错误直接返回。
	watchdogVal, err := hc.watchdog.SdWatchdogEnabled(false)
	if err != nil {
		// Failed to get watchdog configuration information.
		// This occurs when we want to start the watchdog but the configuration is incorrect,
		// for example, the time is not configured correctly.
		return nil, fmt.Errorf("configure watchdog: %w", err)
	}
	// 未启用 watchdog 时返回空实例，Start 不会执行检查。
	if watchdogVal == 0 {
		logger.Info("Systemd watchdog is not enabled")
		return &healthChecker{}, nil
	}
	// 超时过小无法安全发送心跳，拒绝启动。
	if watchdogVal <= minimalNotifyInterval {
		return nil, fmt.Errorf("configure watchdog timeout too small: %v", watchdogVal)
	}

	// 心跳发送失败时按指数退避重试 2 步。
	retryBackoff := wait.Backoff{
		Duration: time.Second,
		Factor:   2.0,
		Jitter:   0.1,
		Steps:    2,
	}
	hc.retryBackoff = retryBackoff
	// 以超时时间的一半作为心跳间隔，留出一半的容错窗口。
	hc.interval = watchdogVal / 2

	return hc, nil
}

// SetHealthCheckers 合并默认检查器与调用方提供的检查器，并原子替换。
func (hc *healthChecker) SetHealthCheckers(syncLoop syncLoopHealthChecker, checkers []healthz.HealthChecker) {
	// Define the default set of health checkers that should always be present
	// 默认检查器始终存在：ping、日志与同步循环健康检查。
	defaultCheckers := []healthz.HealthChecker{
		healthz.PingHealthz,
		healthz.LogHealthz,
		healthz.NamedCheck("syncloop", syncLoop.SyncLoopHealthCheck),
	}

	var combined []healthz.HealthChecker
	combined = append(combined, defaultCheckers...)
	combined = append(combined, checkers...)
	hc.checkers.Store(combined)
}

// getHealthCheckers 返回当前检查器列表；未设置时返回空列表。
func (hc *healthChecker) getHealthCheckers() []healthz.HealthChecker {
	if v := hc.checkers.Load(); v != nil {
		return v.([]healthz.HealthChecker)
	}
	return []healthz.HealthChecker{}
}

// Start 启动周期性健康检查与心跳发送。
func (hc *healthChecker) Start(ctx context.Context) {
	logger := klog.FromContext(ctx)
	// 间隔无效说明 watchdog 未启用，直接返回。
	if hc.interval <= 0 {
		logger.Info("Systemd watchdog is not enabled or the interval is invalid, so health checking will not be started.")
		return
	}
	logger.Info("Starting systemd watchdog with interval", "interval", hc.interval)

	// 周期执行：检查失败则跳过本次心跳，避免把不健康的 kubelet 报告为健康。
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := hc.doCheck(); err != nil {
			logger.Error(err, "Do not notify watchdog this iteration as the kubelet is reportedly not healthy")
			return
		}

		// 心跳发送失败按退避重试；ack 为 false 表示 NOTIFY_SOCKET 未设置。
		err := wait.ExponentialBackoffWithContext(ctx, hc.retryBackoff, func(_ context.Context) (bool, error) {
			ack, err := hc.watchdog.SdNotify(false)
			if err != nil {
				logger.V(5).Info("Failed to notify systemd watchdog, retrying", "error", err)
				return false, nil
			}
			if !ack {
				return false, fmt.Errorf("failed to notify systemd watchdog, notification not supported - (i.e. NOTIFY_SOCKET is unset)")
			}

			logger.V(5).Info("Watchdog plugin notified", "acknowledgment", ack, "state", daemon.SdNotifyWatchdog)
			return true, nil
		})
		if err != nil {
			logger.Error(err, "Failed to notify watchdog")
		}
	}, hc.interval)
}

// doCheck 依次执行所有健康检查，任一失败即返回错误。
func (hc *healthChecker) doCheck() error {
	for _, hc := range hc.getHealthCheckers() {
		if err := hc.Check(nil); err != nil {
			return fmt.Errorf("checker %s failed: %w", hc.Name(), err)
		}
	}
	return nil
}
