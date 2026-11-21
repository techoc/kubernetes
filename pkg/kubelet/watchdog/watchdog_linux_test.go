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
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/initoption"
)

// 部分测试会捕获日志输出，不要降低详细度，否则会失败。
// init 把默认详细度设为 5。
func init() {
	ktesting.SetDefaultVerbosity(5)
}

// mockSyncLoopHealthChecker 是返回预设错误的同步循环健康检查桩。
type mockSyncLoopHealthChecker struct {
	healthCheckErr error
}

// SyncLoopHealthCheck 返回预设错误。
func (m *mockSyncLoopHealthChecker) SyncLoopHealthCheck(req *http.Request) error {
	return m.healthCheckErr
}

// mockWatchdogClient 是返回预设结果的 watchdog 客户端桩。
type mockWatchdogClient struct {
	enabledVal time.Duration
	enabledErr error
	notifyAck  bool
	notifyErr  error
}

// SdWatchdogEnabled 返回预设的超时时间与错误。
func (m *mockWatchdogClient) SdWatchdogEnabled(unsetEnvironment bool) (time.Duration, error) {
	return m.enabledVal, m.enabledErr
}

// SdNotify 返回预设的 ack 与错误。
func (m *mockWatchdogClient) SdNotify(unsetEnvironment bool) (bool, error) {
	return m.notifyAck, m.notifyErr
}

// 测试用的正常与过小 watchdog 间隔。
const (
	interval      = 4 * time.Second
	intervalSmall = 1 * time.Second
)

// TestNewHealthChecker 验证 NewHealthChecker 在不同 watchdog 配置下的行为。
func TestNewHealthChecker(t *testing.T) {
	// 用例覆盖启用、未启用、配置错误与超时过小。
	tests := []struct {
		name        string
		mockEnabled time.Duration
		mockErr     error
		wantErr     bool
	}{
		{"Watchdog enabled", interval, nil, false},
		{"Watchdog not enabled", 0, nil, false},
		{"Watchdog enabled with error", interval, errors.New("mock error"), true},
		{"Watchdog timeout too small", intervalSmall, nil, true},
	}

	// 逐用例构造 mock 客户端并校验错误。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockWatchdogClient{
				enabledVal: tt.mockEnabled,
				enabledErr: tt.mockErr,
			}
			logger, _ := ktesting.NewTestContext(t)
			_, err := NewHealthChecker(logger, WithWatchdogClient(mockClient))
			if (err != nil) != tt.wantErr {
				t.Errorf("NewHealthChecker() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestHealthCheckerStart 验证 Start 的健康检查与心跳通知行为。
func TestHealthCheckerStart(t *testing.T) {
	tCtx := ktesting.Init(t)

	// 用例覆盖通知成功、通知失败、健康检查失败与未注册检查器。
	tests := []struct {
		name           string
		enabledVal     time.Duration
		noCheckers     bool
		healthCheckErr error
		notifyAck      bool
		notifyErr      error
		expectedLogs   []string
	}{
		{
			name:           "Watchdog enabled and notify succeeds",
			enabledVal:     interval,
			healthCheckErr: nil,
			notifyAck:      true,
			notifyErr:      nil,
			expectedLogs:   []string{"Starting systemd watchdog with interval", "Watchdog plugin notified"},
		},
		{
			name:           "Watchdog enabled and notify fails, notification not supported",
			enabledVal:     interval,
			healthCheckErr: nil,
			notifyAck:      false,
			notifyErr:      nil,
			expectedLogs:   []string{"Starting systemd watchdog with interval", "Failed to notify watchdog", "notification not supported"},
		},
		{
			name:           "Watchdog enabled and notify fails, transmission failed",
			enabledVal:     interval,
			healthCheckErr: nil,
			notifyAck:      false,
			notifyErr:      errors.New("mock notify error"),
			expectedLogs:   []string{"Starting systemd watchdog with interval", "Failed to notify watchdog"},
		},
		{
			name:           "Watchdog enabled and health check fails",
			enabledVal:     interval,
			healthCheckErr: errors.New("mock healthy error"),
			notifyAck:      true,
			notifyErr:      nil,
			expectedLogs:   []string{"Starting systemd watchdog with interval", "Do not notify watchdog this iteration as the kubelet is reportedly not healthy"},
		},
		{
			name:       "Watchdog enabled and no checkers added, notify succeeds",
			enabledVal: interval,
			noCheckers: true,
			notifyAck:  true,
			notifyErr:  nil,
			expectedLogs: []string{
				"Starting systemd watchdog with interval", "Watchdog plugin notified"},
		},
	}

	// 逐用例执行并检查日志。
	for _, tt := range tests {
		tCtx.SyncTest(tt.name, func(tCtx ktesting.TContext) {
			tCtx = ktesting.Init(tCtx, initoption.BufferLogs(true))
			logger := tCtx.Logger()
			defer func() {
				tCtx.Cancel("test has completed")
			}()

			// 让 SdWatchdogEnabled 返回有效值。
			mockClient := &mockWatchdogClient{
				enabledVal: tt.enabledVal,
				notifyAck:  tt.notifyAck,
				notifyErr:  tt.notifyErr,
			}

			// 创建 healthChecker。
			hc, err := NewHealthChecker(logger, WithWatchdogClient(mockClient))
			if err != nil {
				t.Fatalf("NewHealthChecker() failed: %v", err)
			}
			if !tt.noCheckers {
				// 绕过 SetHealthCheckers，因为它会加入 LogHealthz 并泄漏后台 goroutine，
				// 与 SyncTest 不兼容。
				hc.(*healthChecker).checkers.Store([]healthz.HealthChecker{
					healthz.NamedCheck("syncloop", (&mockSyncLoopHealthChecker{healthCheckErr: tt.healthCheckErr}).SyncLoopHealthCheck),
				})
			}

			// 启动健康检查。
			hc.Start(tCtx)

			// 等待足够时间让健康检查执行。
			time.Sleep(2 * interval)

			// 检查日志以确认健康检查确实运行。
			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			for _, expectedLog := range tt.expectedLogs {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log '%s' not found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}
