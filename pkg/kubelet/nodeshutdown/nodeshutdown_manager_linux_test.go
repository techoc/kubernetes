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

package nodeshutdown

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	klogtesting "k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init" // 启用 ktesting 命令行标志。
	"k8s.io/kubernetes/pkg/apis/scheduling"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	"k8s.io/kubernetes/pkg/kubelet/nodeshutdown/systemd"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// lock 保护全局 systemDbus，避免并发场景下被同时修改。
var lock sync.Mutex

// fakeDbus 模拟 logind 的 inhibit/monitor 接口，供节点关停流程测试使用。
type fakeDbus struct {
	currentInhibitDelay        time.Duration
	overrideSystemInhibitDelay time.Duration
	shutdownChan               chan bool

	didInhibitShutdown      bool
	didOverrideInhibitDelay bool
	closed                  bool
	onCloseChan             chan struct{} // 若设置，Close() 被调用时会收到信号。

	currentInhibitDelayErr error
}

// CurrentInhibitDelay 返回当前值或已覆盖的 inhibit 延迟。
func (f *fakeDbus) CurrentInhibitDelay() (time.Duration, error) {
	if f.currentInhibitDelayErr != nil {
		return 0, f.currentInhibitDelayErr
	}
	if f.didOverrideInhibitDelay {
		return f.overrideSystemInhibitDelay, nil
	}
	return f.currentInhibitDelay, nil
}

// Close 标记连接已关闭，并通过 onCloseChan 通知等待方。
func (f *fakeDbus) Close() error {
	f.closed = true
	if f.onCloseChan != nil {
		select {
		case f.onCloseChan <- struct{}{}:
		default:
		}
	}
	return nil
}

// InhibitShutdown 记录已调用并返回模拟的 inhibit 锁。
func (f *fakeDbus) InhibitShutdown() (systemd.InhibitLock, error) {
	f.didInhibitShutdown = true
	return systemd.InhibitLock(0), nil
}

// ReleaseInhibitLock 为无操作实现。
func (f *fakeDbus) ReleaseInhibitLock(lock systemd.InhibitLock) error {
	return nil
}

// ReloadLogindConf 为无操作实现。
func (f *fakeDbus) ReloadLogindConf() error {
	return nil
}

// MonitorShutdown 返回测试注入的关停事件通道。
func (f *fakeDbus) MonitorShutdown(_ klog.Logger) (<-chan bool, error) {
	return f.shutdownChan, nil
}

// OverrideInhibitDelay 记录已尝试覆盖延迟。
func (f *fakeDbus) OverrideInhibitDelay(inhibitDelayMax time.Duration) error {
	f.didOverrideInhibitDelay = true
	return nil
}

// TestManager 覆盖关停管理器在不同宽限期、inhibit 延迟与 Pod 优先级组合下的行为。
func TestManager(t *testing.T) {
	tCtx := ktesting.Init(t)
	systemDbusTmp := systemDbus
	defer func() {
		systemDbus = systemDbusTmp
	}()
	normalPodNoGracePeriod := makePod("normal-pod-nil-grace-period", scheduling.DefaultPriorityWhenNoDefaultClassExists, nil /* terminationGracePeriod */)
	criticalPodNoGracePeriod := makePod("critical-pod-nil-grace-period", scheduling.SystemCriticalPriority, nil /* terminationGracePeriod */)

	shortGracePeriod := int64(2)
	normalPodGracePeriod := makePod("normal-pod-grace-period", scheduling.DefaultPriorityWhenNoDefaultClassExists, &shortGracePeriod /* terminationGracePeriod */)
	criticalPodGracePeriod := makePod("critical-pod-grace-period", scheduling.SystemCriticalPriority, &shortGracePeriod /* terminationGracePeriod */)

	longGracePeriod := int64(1000)
	normalPodLongGracePeriod := makePod("normal-pod-long-grace-period", scheduling.DefaultPriorityWhenNoDefaultClassExists, &longGracePeriod /* terminationGracePeriod */)

	var tests = []struct {
		desc                             string
		activePods                       []*v1.Pod
		shutdownGracePeriodRequested     time.Duration
		shutdownGracePeriodCriticalPods  time.Duration
		systemInhibitDelay               time.Duration
		overrideSystemInhibitDelay       time.Duration
		expectedDidOverrideInhibitDelay  bool
		expectedPodToGracePeriodOverride map[string]int64
		expectedError                    error
		expectedPodStatuses              map[string]v1.PodStatus
	}{
		{
			desc: "verify pod status",
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "running-pod"},
					Spec:       v1.PodSpec{},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "failed-pod"},
					Spec:       v1.PodSpec{},
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "succeeded-pod"},
					Spec:       v1.PodSpec{},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
					},
				},
			},
			shutdownGracePeriodRequested:     time.Duration(30 * time.Second),
			shutdownGracePeriodCriticalPods:  time.Duration(10 * time.Second),
			systemInhibitDelay:               time.Duration(40 * time.Second),
			overrideSystemInhibitDelay:       time.Duration(40 * time.Second),
			expectedDidOverrideInhibitDelay:  false,
			expectedPodToGracePeriodOverride: map[string]int64{"running-pod": 20, "failed-pod": 20, "succeeded-pod": 20},
			expectedPodStatuses: map[string]v1.PodStatus{
				"running-pod": {
					Phase:   v1.PodFailed,
					Message: "Pod was terminated in response to imminent node shutdown.",
					Reason:  "Terminated",
					Conditions: []v1.PodCondition{
						{
							Type:    v1.DisruptionTarget,
							Status:  v1.ConditionTrue,
							Reason:  "TerminationByKubelet",
							Message: "Pod was terminated in response to imminent node shutdown.",
						},
					},
				},
				"failed-pod": {
					Phase:   v1.PodFailed,
					Message: "Pod was terminated in response to imminent node shutdown.",
					Reason:  "Terminated",
					Conditions: []v1.PodCondition{
						{
							Type:    v1.DisruptionTarget,
							Status:  v1.ConditionTrue,
							Reason:  "TerminationByKubelet",
							Message: "Pod was terminated in response to imminent node shutdown.",
						},
					},
				},
				"succeeded-pod": {
					Phase:   v1.PodSucceeded,
					Message: "Pod was terminated in response to imminent node shutdown.",
					Reason:  "Terminated",
					Conditions: []v1.PodCondition{
						{
							Type:    v1.DisruptionTarget,
							Status:  v1.ConditionTrue,
							Reason:  "TerminationByKubelet",
							Message: "Pod was terminated in response to imminent node shutdown.",
						},
					},
				},
			},
		},
		{
			desc:                             "no override (total=30s, critical=10s)",
			activePods:                       []*v1.Pod{normalPodNoGracePeriod, criticalPodNoGracePeriod},
			shutdownGracePeriodRequested:     time.Duration(30 * time.Second),
			shutdownGracePeriodCriticalPods:  time.Duration(10 * time.Second),
			systemInhibitDelay:               time.Duration(40 * time.Second),
			overrideSystemInhibitDelay:       time.Duration(40 * time.Second),
			expectedDidOverrideInhibitDelay:  false,
			expectedPodToGracePeriodOverride: map[string]int64{"normal-pod-nil-grace-period": 20, "critical-pod-nil-grace-period": 10},
			expectedPodStatuses: map[string]v1.PodStatus{
				"normal-pod-nil-grace-period": {
					Phase:   v1.PodFailed,
					Message: "Pod was terminated in response to imminent node shutdown.",
					Reason:  "Terminated",
					Conditions: []v1.PodCondition{
						{
							Type:    v1.DisruptionTarget,
							Status:  v1.ConditionTrue,
							Reason:  "TerminationByKubelet",
							Message: "Pod was terminated in response to imminent node shutdown.",
						},
					},
				},
				"critical-pod-nil-grace-period": {
					Phase:   v1.PodFailed,
					Message: "Pod was terminated in response to imminent node shutdown.",
					Reason:  "Terminated",
					Conditions: []v1.PodCondition{
						{
							Type:    v1.DisruptionTarget,
							Status:  v1.ConditionTrue,
							Reason:  "TerminationByKubelet",
							Message: "Pod was terminated in response to imminent node shutdown.",
						},
					},
				},
			},
		},
		{
			desc:                             "no override (total=30s, critical=10s) pods with terminationGracePeriod and without",
			activePods:                       []*v1.Pod{normalPodNoGracePeriod, criticalPodNoGracePeriod, normalPodGracePeriod, criticalPodGracePeriod},
			shutdownGracePeriodRequested:     time.Duration(30 * time.Second),
			shutdownGracePeriodCriticalPods:  time.Duration(10 * time.Second),
			systemInhibitDelay:               time.Duration(40 * time.Second),
			overrideSystemInhibitDelay:       time.Duration(40 * time.Second),
			expectedDidOverrideInhibitDelay:  false,
			expectedPodToGracePeriodOverride: map[string]int64{"normal-pod-nil-grace-period": 20, "critical-pod-nil-grace-period": 10, "normal-pod-grace-period": 2, "critical-pod-grace-period": 2},
		},
		{
			desc:                             "no override (total=30s, critical=10s) pod with long terminationGracePeriod is overridden",
			activePods:                       []*v1.Pod{normalPodNoGracePeriod, criticalPodNoGracePeriod, normalPodGracePeriod, criticalPodGracePeriod, normalPodLongGracePeriod},
			shutdownGracePeriodRequested:     time.Duration(30 * time.Second),
			shutdownGracePeriodCriticalPods:  time.Duration(10 * time.Second),
			systemInhibitDelay:               time.Duration(40 * time.Second),
			overrideSystemInhibitDelay:       time.Duration(40 * time.Second),
			expectedDidOverrideInhibitDelay:  false,
			expectedPodToGracePeriodOverride: map[string]int64{"normal-pod-nil-grace-period": 20, "critical-pod-nil-grace-period": 10, "normal-pod-grace-period": 2, "critical-pod-grace-period": 2, "normal-pod-long-grace-period": 20},
		},
		{
			desc:                             "no override (total=30, critical=0)",
			activePods:                       []*v1.Pod{normalPodNoGracePeriod, criticalPodNoGracePeriod},
			shutdownGracePeriodRequested:     time.Duration(30 * time.Second),
			shutdownGracePeriodCriticalPods:  time.Duration(0 * time.Second),
			systemInhibitDelay:               time.Duration(40 * time.Second),
			overrideSystemInhibitDelay:       time.Duration(40 * time.Second),
			expectedDidOverrideInhibitDelay:  false,
			expectedPodToGracePeriodOverride: map[string]int64{"normal-pod-nil-grace-period": 30, "critical-pod-nil-grace-period": 0},
		},
		{
			desc:                             "override successful (total=30, critical=10)",
			activePods:                       []*v1.Pod{normalPodNoGracePeriod, criticalPodNoGracePeriod},
			shutdownGracePeriodRequested:     time.Duration(30 * time.Second),
			shutdownGracePeriodCriticalPods:  time.Duration(10 * time.Second),
			systemInhibitDelay:               time.Duration(5 * time.Second),
			overrideSystemInhibitDelay:       time.Duration(30 * time.Second),
			expectedDidOverrideInhibitDelay:  true,
			expectedPodToGracePeriodOverride: map[string]int64{"normal-pod-nil-grace-period": 20, "critical-pod-nil-grace-period": 10},
		},
		{
			desc:                             "override unsuccessful",
			activePods:                       []*v1.Pod{normalPodNoGracePeriod, criticalPodNoGracePeriod},
			shutdownGracePeriodRequested:     time.Duration(30 * time.Second),
			shutdownGracePeriodCriticalPods:  time.Duration(10 * time.Second),
			systemInhibitDelay:               time.Duration(5 * time.Second),
			overrideSystemInhibitDelay:       time.Duration(5 * time.Second),
			expectedDidOverrideInhibitDelay:  true,
			expectedPodToGracePeriodOverride: map[string]int64{"normal-pod-nil-grace-period": 5, "critical-pod-nil-grace-period": 0},
			expectedError:                    fmt.Errorf("node shutdown manager was timed out after 5 attempts waiting for logind InhibitDelayMaxSec to update to 30s (ShutdownGracePeriod), current value is 5s"),
		},
		{
			desc:                            "override unsuccessful, zero time",
			activePods:                      []*v1.Pod{normalPodNoGracePeriod, criticalPodNoGracePeriod},
			shutdownGracePeriodRequested:    time.Duration(5 * time.Second),
			shutdownGracePeriodCriticalPods: time.Duration(5 * time.Second),
			systemInhibitDelay:              time.Duration(0 * time.Second),
			overrideSystemInhibitDelay:      time.Duration(0 * time.Second),
			expectedError:                   fmt.Errorf("node shutdown manager was timed out after 5 attempts waiting for logind InhibitDelayMaxSec to update to 5s (ShutdownGracePeriod), current value is 0s"),
		},
		{
			desc:                             "no override, all time to critical pods",
			activePods:                       []*v1.Pod{normalPodNoGracePeriod, criticalPodNoGracePeriod},
			shutdownGracePeriodRequested:     time.Duration(5 * time.Second),
			shutdownGracePeriodCriticalPods:  time.Duration(5 * time.Second),
			systemInhibitDelay:               time.Duration(5 * time.Second),
			overrideSystemInhibitDelay:       time.Duration(5 * time.Second),
			expectedDidOverrideInhibitDelay:  false,
			expectedPodToGracePeriodOverride: map[string]int64{"normal-pod-nil-grace-period": 0, "critical-pod-nil-grace-period": 5},
		},
	}

	for _, tc := range tests {
		// 逐用例在独立同步上下文中运行，隔离全局 dbus 的替换。
		tCtx.SyncTest(tc.desc, func(tCtx ktesting.TContext) {
			defer tCtx.Cancel("test completed")
			t := tCtx.TB()
			logger := tCtx.Logger()

			activePodsFunc := func() []*v1.Pod {
				return tc.activePods
			}

			type PodKillInfo struct {
				Name        string
				GracePeriod int64
			}

			// 通过 killPodsFunc 收集每个 Pod 实际使用的宽限期，验证覆盖逻辑。
			podKillChan := make(chan PodKillInfo, 1)
			killPodsFunc := func(pod *v1.Pod, evict bool, gracePeriodOverride *int64, fn func(podStatus *v1.PodStatus)) error {
				var gracePeriod int64
				if gracePeriodOverride != nil {
					gracePeriod = *gracePeriodOverride
				}
				fn(&pod.Status)
				podKillChan <- PodKillInfo{Name: pod.Name, GracePeriod: gracePeriod}
				return nil
			}

			fakeShutdownChan := make(chan bool)
			fakeDbus := &fakeDbus{currentInhibitDelay: tc.systemInhibitDelay, shutdownChan: fakeShutdownChan, overrideSystemInhibitDelay: tc.overrideSystemInhibitDelay}

			lock.Lock()
			systemDbus = func() (dbusInhibiter, error) {
				return fakeDbus, nil
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.GracefulNodeShutdown, true)

			fakeRecorder := &record.FakeRecorder{}
			fakeVolumeManager := volumemanager.NewFakeVolumeManager([]v1.UniqueVolumeName{}, 0, nil, false)
			nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}
			manager := NewManager(&Config{
				Logger:                          logger,
				VolumeManager:                   fakeVolumeManager,
				Recorder:                        fakeRecorder,
				NodeRef:                         nodeRef,
				GetPodsFunc:                     activePodsFunc,
				KillPodFunc:                     killPodsFunc,
				SyncNodeStatusFunc:              func(context context.Context) {},
				ShutdownGracePeriodRequested:    tc.shutdownGracePeriodRequested,
				ShutdownGracePeriodCriticalPods: tc.shutdownGracePeriodCriticalPods,
				Clock:                           testingclock.NewFakeClock(time.Now()),
				StateDirectory:                  os.TempDir(),
			})

			err := manager.Start(tCtx)
			lock.Unlock()

			if tc.expectedError != nil {
				if err == nil {
					t.Errorf("unexpected error message. Got: <nil> want %s", tc.expectedError.Error())
				} else if !strings.Contains(err.Error(), tc.expectedError.Error()) {
					t.Errorf("unexpected error message. Got: %s want %s", err.Error(), tc.expectedError.Error())
				}
			} else {
				assert.NoError(t, err, "expected manager.Start() to not return error")
				assert.True(t, fakeDbus.didInhibitShutdown, "expected that manager inhibited shutdown")
				assert.NoError(t, manager.ShutdownStatus(), "expected that manager does not return error since shutdown is not active")
				assert.True(t, manager.Admit(tCtx, nil).Admit)

				// 发送模拟的关停事件。
				select {
				case fakeShutdownChan <- true:
				case <-time.After(1 * time.Second):
					t.Fatal()
				}

				// 等待所有 Pod 被终止。
				killedPodsToGracePeriods := map[string]int64{}
				for i := 0; i < len(tc.activePods); i++ {
					select {
					case podKillInfo := <-podKillChan:
						killedPodsToGracePeriods[podKillInfo.Name] = podKillInfo.GracePeriod
						continue
					case <-time.After(1 * time.Second):
						t.Fatal()
					}
				}

				assert.Error(t, manager.ShutdownStatus(), "expected that manager returns error since shutdown is active")
				assert.False(t, manager.Admit(tCtx, nil).Admit)
				assert.Equal(t, tc.expectedPodToGracePeriodOverride, killedPodsToGracePeriods)
				assert.Equal(t, tc.expectedDidOverrideInhibitDelay, fakeDbus.didOverrideInhibitDelay, "override system inhibit delay differs")
				if tc.expectedPodStatuses != nil {
					for _, pod := range tc.activePods {
						if diff := cmp.Diff(tc.expectedPodStatuses[pod.Name], pod.Status, cmpopts.IgnoreFields(v1.PodCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
							t.Errorf("Unexpected PodStatus: (-want,+got):\n%s", diff)
						}
					}
				}
			}
		})
	}
}

// TestFeatureEnabled 验证特性门控与宽限期共同决定关停管理器是否启用。
func TestFeatureEnabled(t *testing.T) {
	var tests = []struct {
		desc                         string
		shutdownGracePeriodRequested time.Duration
		featureGateEnabled           bool
		expectEnabled                bool
	}{
		{
			desc:                         "shutdownGracePeriodRequested 0; disables feature",
			shutdownGracePeriodRequested: time.Duration(0 * time.Second),
			featureGateEnabled:           true,
			expectEnabled:                false,
		},
		{
			desc:                         "feature gate disabled; disables feature",
			shutdownGracePeriodRequested: time.Duration(100 * time.Second),
			featureGateEnabled:           false,
			expectEnabled:                false,
		},
		{
			desc:                         "feature gate enabled; shutdownGracePeriodRequested > 0; enables feature",
			shutdownGracePeriodRequested: time.Duration(100 * time.Second),
			featureGateEnabled:           true,
			expectEnabled:                true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			activePodsFunc := func() []*v1.Pod {
				return nil
			}
			killPodsFunc := func(pod *v1.Pod, evict bool, gracePeriodOverride *int64, fn func(*v1.PodStatus)) error {
				return nil
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.GracefulNodeShutdown, tc.featureGateEnabled)

			fakeRecorder := &record.FakeRecorder{}
			fakeVolumeManager := volumemanager.NewFakeVolumeManager([]v1.UniqueVolumeName{}, 0, nil, false)
			nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

			manager := NewManager(&Config{
				Logger:                          logger,
				VolumeManager:                   fakeVolumeManager,
				Recorder:                        fakeRecorder,
				NodeRef:                         nodeRef,
				GetPodsFunc:                     activePodsFunc,
				KillPodFunc:                     killPodsFunc,
				SyncNodeStatusFunc:              func(context.Context) {},
				ShutdownGracePeriodRequested:    tc.shutdownGracePeriodRequested,
				ShutdownGracePeriodCriticalPods: 0,
				StateDirectory:                  os.TempDir(),
			})
			assert.Equal(t, tc.expectEnabled, manager != managerStub{})
		})
	}
}

// TestRestart 验证 dbus 连接断开后管理器会持续重连。
func TestRestart(t *testing.T) {
	ktesting.Init(t).SyncTest("", testRestart)
}

// testRestart 是 TestRestart 的实际测试体。
func testRestart(tCtx ktesting.TContext) {
	defer tCtx.Cancel("test completed")
	logger := tCtx.Logger()
	t := tCtx.TB()
	systemDbusTmp := systemDbus
	defer func() {
		systemDbus = systemDbusTmp
	}()

	shutdownGracePeriodRequested := 30 * time.Second
	shutdownGracePeriodCriticalPods := 10 * time.Second
	systemInhibitDelay := 40 * time.Second
	overrideSystemInhibitDelay := 40 * time.Second
	activePodsFunc := func() []*v1.Pod {
		return nil
	}
	killPodsFunc := func(pod *v1.Pod, isEvicted bool, gracePeriodOverride *int64, fn func(*v1.PodStatus)) error {
		return nil
	}
	syncNodeStatus := func(context.Context) {}

	var shutdownChan chan bool
	var shutdownChanMut sync.Mutex
	var connChan = make(chan struct{}, 1)

	lock.Lock()
	systemDbus = func() (dbusInhibiter, error) {
		defer func() {
			connChan <- struct{}{}
		}()
		ch := make(chan bool)
		shutdownChanMut.Lock()
		shutdownChan = ch
		shutdownChanMut.Unlock()
		dbus := &fakeDbus{currentInhibitDelay: systemInhibitDelay, shutdownChan: ch, overrideSystemInhibitDelay: overrideSystemInhibitDelay}
		return dbus, nil
	}

	fakeRecorder := &record.FakeRecorder{}
	fakeVolumeManager := volumemanager.NewFakeVolumeManager([]v1.UniqueVolumeName{}, 0, nil, false)
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}
	manager := NewManager(&Config{
		Logger:                          logger,
		VolumeManager:                   fakeVolumeManager,
		Recorder:                        fakeRecorder,
		NodeRef:                         nodeRef,
		GetPodsFunc:                     activePodsFunc,
		KillPodFunc:                     killPodsFunc,
		SyncNodeStatusFunc:              syncNodeStatus,
		ShutdownGracePeriodRequested:    shutdownGracePeriodRequested,
		ShutdownGracePeriodCriticalPods: shutdownGracePeriodCriticalPods,
		StateDirectory:                  os.TempDir(),
	})

	err := manager.Start(tCtx)
	lock.Unlock()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 0; i != 3; i++ {
		select {
		case <-time.After(dbusReconnectPeriod * 5):
			t.Fatal("wait dbus connect timeout")
		case <-connChan:
		}

		shutdownChanMut.Lock()
		close(shutdownChan)
		shutdownChanMut.Unlock()
	}
}

// TestStartDoesNotReconnectAfterContextCancel 验证上下文取消后不再重连 dbus。
func TestStartDoesNotReconnectAfterContextCancel(t *testing.T) {
	ktesting.Init(t).SyncTest("", testStartDoesNotReconnectAfterContextCancel)
}

// testStartDoesNotReconnectAfterContextCancel 是该测试的实际测试体。
func testStartDoesNotReconnectAfterContextCancel(tCtx ktesting.TContext) {
	defer tCtx.Cancel("test completed")
	logger := tCtx.Logger()
	t := tCtx.TB()

	systemDbusTmp := systemDbus
	defer func() {
		systemDbus = systemDbusTmp
	}()

	shutdownGracePeriodRequested := 30 * time.Second
	shutdownGracePeriodCriticalPods := 10 * time.Second
	systemInhibitDelay := 40 * time.Second
	overrideSystemInhibitDelay := 40 * time.Second

	shutdownChans := make(chan chan bool, 2)

	lock.Lock()
	systemDbus = func() (dbusInhibiter, error) {
		ch := make(chan bool)
		shutdownChans <- ch
		return &fakeDbus{
			currentInhibitDelay:        systemInhibitDelay,
			shutdownChan:               ch,
			overrideSystemInhibitDelay: overrideSystemInhibitDelay,
		}, nil
	}

	manager := NewManager(&Config{
		Logger:                          logger,
		VolumeManager:                   volumemanager.NewFakeVolumeManager([]v1.UniqueVolumeName{}, 0, nil, false),
		Recorder:                        &record.FakeRecorder{},
		NodeRef:                         &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""},
		GetPodsFunc:                     func() []*v1.Pod { return nil },
		KillPodFunc:                     func(*v1.Pod, bool, *int64, func(*v1.PodStatus)) error { return nil },
		SyncNodeStatusFunc:              func(context.Context) {},
		ShutdownGracePeriodRequested:    shutdownGracePeriodRequested,
		ShutdownGracePeriodCriticalPods: shutdownGracePeriodCriticalPods,
		StateDirectory:                  os.TempDir(),
	})

	ctx, cancel := context.WithCancel(tCtx.Context)
	err := manager.Start(ctx)
	lock.Unlock()
	require.NoError(t, err)

	var shutdownChan chan bool
	select {
	case shutdownChan = <-shutdownChans:
	case <-time.After(dbusReconnectPeriod):
		t.Fatal("timed out waiting for initial dbus watch")
	}

	cancel()
	close(shutdownChan)

	select {
	case <-shutdownChans:
		t.Fatal("shutdown manager reconnected after context cancellation")
	case <-time.After(dbusReconnectPeriod * 5):
	}
}

// Test_managerImpl_processShutdownEvent 验证关停事件处理在 Pod 终止耗时过长时会输出超时日志。
func Test_managerImpl_processShutdownEvent(t *testing.T) {
	tCtx := ktesting.Init(t)

	var (
		fakeRecorder      = &record.FakeRecorder{}
		fakeVolumeManager = volumemanager.NewFakeVolumeManager([]v1.UniqueVolumeName{}, 0, nil, false)
		syncNodeStatus    = func(context.Context) {}
		nodeRef           = &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}
		fakeclock         = testingclock.NewFakeClock(time.Now())
	)

	type fields struct {
		recorder                         record.EventRecorder
		nodeRef                          *v1.ObjectReference
		volumeManager                    volumemanager.VolumeManager
		shutdownGracePeriodByPodPriority []kubeletconfig.ShutdownGracePeriodByPodPriority
		getPods                          eviction.ActivePodsFunc
		killPodFunc                      eviction.KillPodFunc
		syncNodeStatus                   func(context.Context)
		dbusCon                          dbusInhibiter
		inhibitLock                      systemd.InhibitLock
		nodeShuttingDownNow              bool
		clock                            clock.Clock
	}
	tests := []struct {
		name                   string
		fields                 fields
		wantErr                bool
		expectedOutputContains string
	}{
		{
			name: "kill pod func take too long",
			fields: fields{
				recorder:      fakeRecorder,
				nodeRef:       nodeRef,
				volumeManager: fakeVolumeManager,
				shutdownGracePeriodByPodPriority: []kubeletconfig.ShutdownGracePeriodByPodPriority{
					{
						Priority:                   1,
						ShutdownGracePeriodSeconds: 10,
					},
					{
						Priority:                   2,
						ShutdownGracePeriodSeconds: 20,
					},
				},
				getPods: func() []*v1.Pod {
					return []*v1.Pod{
						makePod("normal-pod", 1, nil),
						makePod("critical-pod", 2, nil),
					}
				},
				killPodFunc: func(pod *v1.Pod, isEvicted bool, gracePeriodOverride *int64, fn func(*v1.PodStatus)) error {
					fakeclock.Step(60 * time.Second)
					return nil
				},
				syncNodeStatus: syncNodeStatus,
				clock:          fakeclock,
				dbusCon:        &fakeDbus{},
			},
			wantErr:                false,
			expectedOutputContains: "Shutdown manager pod killing time out",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := klogtesting.NewLogger(t,
				klogtesting.NewConfig(
					klogtesting.BufferLogs(true),
				),
			)
			m := &managerImpl{
				logger:                logger,
				recorder:              tt.fields.recorder,
				nodeRef:               tt.fields.nodeRef,
				getPods:               tt.fields.getPods,
				syncNodeStatus:        tt.fields.syncNodeStatus,
				dbusCon:               tt.fields.dbusCon,
				inhibitLock:           tt.fields.inhibitLock,
				nodeShuttingDownMutex: sync.Mutex{},
				nodeShuttingDownNow:   tt.fields.nodeShuttingDownNow,
				podManager: &podManager{
					logger:                           logger,
					volumeManager:                    tt.fields.volumeManager,
					shutdownGracePeriodByPodPriority: tt.fields.shutdownGracePeriodByPodPriority,
					killPodFunc:                      tt.fields.killPodFunc,
					clock:                            tt.fields.clock,
				},
			}
			err := m.processShutdownEvent(tCtx)
			if tt.wantErr {
				require.Error(t, err, "managerImpl.processShutdownEvent() should return an error")
			} else {
				require.NoError(t, err, "managerImpl.processShutdownEvent() should not return an error")
			}

			underlier, ok := logger.GetSink().(klogtesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}

			log := underlier.GetBuffer().String()
			if !strings.Contains(log, tt.expectedOutputContains) {
				// 失败时日志会一并展示；若想在成功运行时查看，
				// 可使用 "go test -v"。
				t.Errorf("managerImpl.processShutdownEvent() should have logged %s, see actual output above.", tt.expectedOutputContains)
			}
		})
	}
}

// Test_processShutdownEvent_VolumeUnmountTimeout 验证卷卸载超时不会阻塞关停流程超过宽限期。
func Test_processShutdownEvent_VolumeUnmountTimeout(t *testing.T) {
	ktesting.Init(t).SyncTest("", testProcessShutdownEventVolumeUnmountTimeout)
}

// testProcessShutdownEventVolumeUnmountTimeout 是该测试的实际测试体。
func testProcessShutdownEventVolumeUnmountTimeout(tCtx ktesting.TContext) {
	t := tCtx.TB()
	var (
		fakeRecorder               = &record.FakeRecorder{}
		syncNodeStatus             = func(context.Context) {}
		nodeRef                    = &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}
		fakeclock                  = testingclock.NewFakeClock(time.Now())
		shutdownGracePeriodSeconds = 2
	)

	fakeVolumeManager := volumemanager.NewFakeVolumeManager(
		[]v1.UniqueVolumeName{},
		3*time.Second, // 故意设置得比 shutdownGracePeriodSeconds（2 秒）更长，用于验证卷卸载超过允许宽限期的行为。
		// 用于验证卷卸载操作超过允许宽限期时的情况。
		fmt.Errorf("unmount timeout"), false,
	)
	// 使用带缓冲的 logger，因为本测试要断言日志输出。
	logger := klogtesting.NewLogger(t, klogtesting.NewConfig(klogtesting.BufferLogs(true)))
	m := &managerImpl{
		logger:   logger,
		recorder: fakeRecorder,
		nodeRef:  nodeRef,
		getPods: func() []*v1.Pod {
			return []*v1.Pod{
				makePod("test-pod", 1, nil),
			}
		},
		syncNodeStatus: syncNodeStatus,
		dbusCon:        &fakeDbus{},
		podManager: &podManager{
			logger:        logger,
			volumeManager: fakeVolumeManager,
			shutdownGracePeriodByPodPriority: []kubeletconfig.ShutdownGracePeriodByPodPriority{
				{
					Priority:                   1,
					ShutdownGracePeriodSeconds: int64(shutdownGracePeriodSeconds),
				},
			},
			killPodFunc: func(pod *v1.Pod, isEvicted bool, gracePeriodOverride *int64, fn func(*v1.PodStatus)) error {
				return nil
			},
			clock: fakeclock,
		},
	}

	start := fakeclock.Now()
	err := m.processShutdownEvent(tCtx)
	end := fakeclock.Now()

	require.NoError(t, err, "managerImpl.processShutdownEvent() should not return an error")

	// 检查 processShutdownEvent 是否在期望时间内完成。
	actualDuration := int(end.Sub(start).Seconds())
	assert.LessOrEqual(t, actualDuration, shutdownGracePeriodSeconds, "processShutdownEvent took too long")

	underlier, ok := logger.GetSink().(klogtesting.Underlier)
	if !ok {
		t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
	}

	log := underlier.GetBuffer().String()
	expectedLogMessage := "Failed while waiting for all the volumes belonging to Pods in this group to unmount"
	assert.Contains(t, log, expectedLogMessage, "Expected log message not found")
}

// TestStartDbusConnectionClosedOnError 验证 start() 在创建 dbus 连接后失败时，
// 连接会被正确关闭，避免 goroutine 与文件描述符泄漏。参见 #120613。
// TestStartDbusConnectionClosedOnError 验证 start 失败时已创建的 dbus 连接会被关闭，避免泄漏 goroutine 与文件描述符。
func TestStartDbusConnectionClosedOnError(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	systemDbusTmp := systemDbus
	defer func() {
		systemDbus = systemDbusTmp
	}()

	connErr := fmt.Errorf("simulated CurrentInhibitDelay error")
	var createdConnections []*fakeDbus

	lock.Lock()
	systemDbus = func() (dbusInhibiter, error) {
		fd := &fakeDbus{
			currentInhibitDelayErr: connErr,
			shutdownChan:           make(chan bool),
		}
		createdConnections = append(createdConnections, fd)
		return fd, nil
	}
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.GracefulNodeShutdown, true)

	fakeRecorder := &record.FakeRecorder{}
	fakeVolumeManager := volumemanager.NewFakeVolumeManager([]v1.UniqueVolumeName{}, 0, nil, false)
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}
	manager := NewManager(&Config{
		Logger:        logger,
		VolumeManager: fakeVolumeManager,
		Recorder:      fakeRecorder,
		NodeRef:       nodeRef,
		GetPodsFunc:   func() []*v1.Pod { return nil },
		KillPodFunc: func(pod *v1.Pod, isEvicted bool, gracePeriodOverride *int64, fn func(*v1.PodStatus)) error {
			return nil
		},
		SyncNodeStatusFunc:              func(context.Context) {},
		ShutdownGracePeriodRequested:    30 * time.Second,
		ShutdownGracePeriodCriticalPods: 10 * time.Second,
		StateDirectory:                  os.TempDir(),
	})

	err := manager.Start(tCtx)
	lock.Unlock()

	require.Error(t, err, "expected start to fail due to CurrentInhibitDelay error")
	require.Len(t, createdConnections, 1, "expected exactly one connection to be created")
	assert.True(t, createdConnections[0].closed, "expected the dbus connection to be closed after start() failure")
}

// TestRestartClosesOldConnection 验证 Start() 的重试循环重连时，
// 会先关闭旧的 dbus 连接再创建新连接。参见 #120613。
// TestRestartClosesOldConnection 验证重连时旧的 dbus 连接会先被关闭。
func TestRestartClosesOldConnection(t *testing.T) {
	ktesting.Init(t).SyncTest("", testRestartClosesOldConnection)
}

// testRestartClosesOldConnection 是该测试的实际测试体。
func testRestartClosesOldConnection(tCtx ktesting.TContext) {
	defer tCtx.Cancel("test completed")
	logger := tCtx.Logger()
	t := tCtx.TB()

	systemDbusTmp := systemDbus
	defer func() {
		systemDbus = systemDbusTmp
	}()

	// 用 onCloseChan 可靠地获知第一个连接的 Close() 调用，
	// 避免与共享全局 systemDbus 的先前测试遗留 goroutine 产生竞态。
	firstConnClosedChan := make(chan struct{}, 1)
	var firstConn *fakeDbus

	lock.Lock()
	systemDbus = func() (dbusInhibiter, error) {
		ch := make(chan bool)
		fd := &fakeDbus{
			currentInhibitDelay:        40 * time.Second,
			overrideSystemInhibitDelay: 40 * time.Second,
			shutdownChan:               ch,
		}
		return fd, nil
	}
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.GracefulNodeShutdown, true)

	fakeRecorder := &record.FakeRecorder{}
	fakeVolumeManager := volumemanager.NewFakeVolumeManager([]v1.UniqueVolumeName{}, 0, nil, false)
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}
	manager := NewManager(&Config{
		Logger:        logger,
		VolumeManager: fakeVolumeManager,
		Recorder:      fakeRecorder,
		NodeRef:       nodeRef,
		GetPodsFunc:   func() []*v1.Pod { return nil },
		KillPodFunc: func(pod *v1.Pod, isEvicted bool, gracePeriodOverride *int64, fn func(*v1.PodStatus)) error {
			return nil
		},
		SyncNodeStatusFunc:              func(context.Context) {},
		ShutdownGracePeriodRequested:    30 * time.Second,
		ShutdownGracePeriodCriticalPods: 10 * time.Second,
		StateDirectory:                  os.TempDir(),
	})

	err := manager.Start(tCtx)

	// 保存第一个连接的引用并启用其关闭通知。
	m := manager.(*managerImpl)
	firstConn = m.dbusCon.(*fakeDbus)
	firstConn.onCloseChan = firstConnClosedChan
	lock.Unlock()

	require.NoError(t, err)

	// 关闭 shutdown 通道以触发重连，模拟 dbus 断开。
	close(firstConn.shutdownChan)

	// 等待 start() 对第一个连接调用 Close()。
	select {
	case <-firstConnClosedChan:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first connection to be closed on reconnect")
	}

	assert.True(t, firstConn.closed, "expected the first dbus connection to be closed after reconnect")
}
