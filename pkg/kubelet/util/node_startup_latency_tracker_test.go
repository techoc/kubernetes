/*
Copyright 2023 The Kubernetes Authors.

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

package util

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// 以下常量是各节点启动延迟指标的完整名称，用于按名筛选指标。
const (
	metricsNameNodeStartupPreKubelet       = "kubelet_node_startup_pre_kubelet_duration_seconds"
	metricsNameNodeStartupPreRegistration  = "kubelet_node_startup_pre_registration_duration_seconds"
	metricsNameNodeStartupRegistration     = "kubelet_node_startup_registration_duration_seconds"
	metricsNameNodeStartupPostRegistration = "kubelet_node_startup_post_registration_duration_seconds"
	metricsNameNodeStartup                 = "kubelet_node_startup_duration_seconds"
)

// TestNodeStartupLatencyNoEvents 验证没有任何事件时，所有启动延迟指标都保持为 0。
func TestNodeStartupLatencyNoEvents(t *testing.T) {
	t.Run("metrics registered; no incoming events", func(t *testing.T) {
		// 注册指标并确保测试结束后清理，避免指标状态影响其他测试。
		metrics.Register()
		defer clearMetrics()

		tracker := &basicNodeStartupLatencyTracker{
			bootTime:         frozenTime.Add(-100 * time.Millisecond),
			kubeletStartTime: frozenTime,
			clock:            clock.RealClock{},
		}

		wants := `
		# HELP kubelet_node_startup_duration_seconds [ALPHA] Duration in seconds of node startup in total.
        # TYPE kubelet_node_startup_duration_seconds gauge
        kubelet_node_startup_duration_seconds 0
        # HELP kubelet_node_startup_post_registration_duration_seconds [ALPHA] Duration in seconds of node startup after registration.
        # TYPE kubelet_node_startup_post_registration_duration_seconds gauge
        kubelet_node_startup_post_registration_duration_seconds 0
        # HELP kubelet_node_startup_pre_kubelet_duration_seconds [ALPHA] Duration in seconds of node startup before kubelet starts.
        # TYPE kubelet_node_startup_pre_kubelet_duration_seconds gauge
        kubelet_node_startup_pre_kubelet_duration_seconds 0
        # HELP kubelet_node_startup_pre_registration_duration_seconds [ALPHA] Duration in seconds of node startup before registration.
        # TYPE kubelet_node_startup_pre_registration_duration_seconds gauge
        kubelet_node_startup_pre_registration_duration_seconds 0
        # HELP kubelet_node_startup_registration_duration_seconds [ALPHA] Duration in seconds of node startup during registration.
        # TYPE kubelet_node_startup_registration_duration_seconds gauge
        kubelet_node_startup_registration_duration_seconds 0
		`
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants),
			metricsNameNodeStartupPreKubelet,
			metricsNameNodeStartupPreRegistration,
			metricsNameNodeStartupRegistration,
			metricsNameNodeStartupPostRegistration,
			metricsNameNodeStartup,
		); err != nil {
			t.Error(err)
		}

		// 未记录任何时间戳时，tracker 中的各事件时间应保持零值。
		assert.Equal(t, frozenTime.Add(-100*time.Millisecond), tracker.bootTime)
		assert.Equal(t, frozenTime, tracker.kubeletStartTime)
		assert.True(t, tracker.firstRegistrationAttemptTime.IsZero())
		assert.True(t, tracker.firstRegisteredNewNodeTime.IsZero())
		assert.True(t, tracker.firstNodeReadyTime.IsZero())
	})
}

// TestRecordAllTimestamps 验证按顺序记录全部事件后，各阶段启动延迟指标计算正确。
func TestRecordAllTimestamps(t *testing.T) {
	t.Run("all timestamps are recorded", func(t *testing.T) {
		metrics.Register()
		defer clearMetrics()

		fakeClock := testingclock.NewFakeClock(frozenTime)
		tracker := &basicNodeStartupLatencyTracker{
			bootTime:         frozenTime.Add(-100 * time.Millisecond),
			kubeletStartTime: frozenTime,
			clock:            fakeClock,
		}

		// 依次推进假时钟并记录各启动阶段事件。
		fakeClock.Step(800 * time.Millisecond)
		tracker.RecordAttemptRegisterNode()

		assert.Equal(t, frozenTime.Add(800*time.Millisecond), tracker.firstRegistrationAttemptTime)

		fakeClock.Step(400 * time.Millisecond)
		tracker.RecordRegisteredNewNode()

		assert.Equal(t, frozenTime.Add(1200*time.Millisecond), tracker.firstRegisteredNewNodeTime)

		fakeClock.Step(1100 * time.Millisecond)
		tracker.RecordNodeReady()

		assert.Equal(t, frozenTime.Add(2300*time.Millisecond), tracker.firstNodeReadyTime)

		wants := `
		# HELP kubelet_node_startup_duration_seconds [ALPHA] Duration in seconds of node startup in total.
        # TYPE kubelet_node_startup_duration_seconds gauge
        kubelet_node_startup_duration_seconds 2.4
        # HELP kubelet_node_startup_post_registration_duration_seconds [ALPHA] Duration in seconds of node startup after registration.
        # TYPE kubelet_node_startup_post_registration_duration_seconds gauge
        kubelet_node_startup_post_registration_duration_seconds 1.1
        # HELP kubelet_node_startup_pre_kubelet_duration_seconds [ALPHA] Duration in seconds of node startup before kubelet starts.
        # TYPE kubelet_node_startup_pre_kubelet_duration_seconds gauge
        kubelet_node_startup_pre_kubelet_duration_seconds 0.1
        # HELP kubelet_node_startup_pre_registration_duration_seconds [ALPHA] Duration in seconds of node startup before registration.
        # TYPE kubelet_node_startup_pre_registration_duration_seconds gauge
        kubelet_node_startup_pre_registration_duration_seconds 0.8
        # HELP kubelet_node_startup_registration_duration_seconds [ALPHA] Duration in seconds of node startup during registration.
        # TYPE kubelet_node_startup_registration_duration_seconds gauge
        kubelet_node_startup_registration_duration_seconds 0.4
		`
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants),
			metricsNameNodeStartupPreKubelet,
			metricsNameNodeStartupPreRegistration,
			metricsNameNodeStartupRegistration,
			metricsNameNodeStartupPostRegistration,
			metricsNameNodeStartup,
		); err != nil {
			t.Error(err)
		}
	})
}

// TestRecordAttemptRegister 验证只记录一次注册尝试时，仅注册尝试时间被写入。
func TestRecordAttemptRegister(t *testing.T) {
	t.Run("record attempt register node", func(t *testing.T) {
		metrics.Register()
		defer clearMetrics()

		fakeClock := testingclock.NewFakeClock(frozenTime)
		tracker := &basicNodeStartupLatencyTracker{
			bootTime:         frozenTime.Add(-100 * time.Millisecond),
			kubeletStartTime: frozenTime,
			clock:            fakeClock,
		}

		fakeClock.Step(600 * time.Millisecond)
		tracker.RecordAttemptRegisterNode()

		assert.Equal(t, frozenTime.Add(600*time.Millisecond), tracker.firstRegistrationAttemptTime)
		assert.True(t, tracker.firstRegisteredNewNodeTime.IsZero())
		assert.True(t, tracker.firstNodeReadyTime.IsZero())

		wants := `
		# HELP kubelet_node_startup_duration_seconds [ALPHA] Duration in seconds of node startup in total.
        # TYPE kubelet_node_startup_duration_seconds gauge
        kubelet_node_startup_duration_seconds 0
        # HELP kubelet_node_startup_post_registration_duration_seconds [ALPHA] Duration in seconds of node startup after registration.
        # TYPE kubelet_node_startup_post_registration_duration_seconds gauge
        kubelet_node_startup_post_registration_duration_seconds 0
        # HELP kubelet_node_startup_pre_kubelet_duration_seconds [ALPHA] Duration in seconds of node startup before kubelet starts.
        # TYPE kubelet_node_startup_pre_kubelet_duration_seconds gauge
        kubelet_node_startup_pre_kubelet_duration_seconds 0
        # HELP kubelet_node_startup_pre_registration_duration_seconds [ALPHA] Duration in seconds of node startup before registration.
        # TYPE kubelet_node_startup_pre_registration_duration_seconds gauge
        kubelet_node_startup_pre_registration_duration_seconds 0
        # HELP kubelet_node_startup_registration_duration_seconds [ALPHA] Duration in seconds of node startup during registration.
        # TYPE kubelet_node_startup_registration_duration_seconds gauge
        kubelet_node_startup_registration_duration_seconds 0
				`
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants),
			metricsNameNodeStartupPreKubelet,
			metricsNameNodeStartupPreRegistration,
			metricsNameNodeStartupRegistration,
			metricsNameNodeStartupPostRegistration,
			metricsNameNodeStartup,
		); err != nil {
			t.Error(err)
		}
	})
}

// TestRecordAttemptRegisterTwice 验证重复记录注册尝试时只保留第一次的时间。
func TestRecordAttemptRegisterTwice(t *testing.T) {
	t.Run("record attempt register node twice", func(t *testing.T) {
		metrics.Register()
		defer clearMetrics()

		fakeClock := testingclock.NewFakeClock(frozenTime)
		tracker := &basicNodeStartupLatencyTracker{
			bootTime:         frozenTime.Add(-100 * time.Millisecond),
			kubeletStartTime: frozenTime,
			clock:            fakeClock,
		}

		fakeClock.Step(600 * time.Millisecond)
		tracker.RecordAttemptRegisterNode()

		fakeClock.Step(300 * time.Millisecond)
		tracker.RecordAttemptRegisterNode()

		assert.Equal(t, frozenTime.Add(600*time.Millisecond), tracker.firstRegistrationAttemptTime)
		assert.True(t, tracker.firstRegisteredNewNodeTime.IsZero())
		assert.True(t, tracker.firstNodeReadyTime.IsZero())
	})
}

// TestSkippingRecordRegisteredNewNode 验证重复注册或缺少前置步骤时，
// 注册完成时间不会被错误记录。
func TestSkippingRecordRegisteredNewNode(t *testing.T) {
	t.Run("record register new node twice", func(t *testing.T) {
		metrics.Register()
		defer clearMetrics()

		fakeClock := testingclock.NewFakeClock(frozenTime)
		tracker := &basicNodeStartupLatencyTracker{
			bootTime:         frozenTime.Add(-100 * time.Millisecond),
			kubeletStartTime: frozenTime,
			clock:            fakeClock,
		}

		fakeClock.Step(100 * time.Millisecond)
		tracker.RecordAttemptRegisterNode()

		fakeClock.Step(500 * time.Millisecond)
		tracker.RecordRegisteredNewNode()

		fakeClock.Step(300 * time.Millisecond)
		tracker.RecordRegisteredNewNode()

		assert.Equal(t, frozenTime.Add(600*time.Millisecond), tracker.firstRegisteredNewNodeTime)

		wants := `
		# HELP kubelet_node_startup_duration_seconds [ALPHA] Duration in seconds of node startup in total.
        # TYPE kubelet_node_startup_duration_seconds gauge
        kubelet_node_startup_duration_seconds 0
        # HELP kubelet_node_startup_post_registration_duration_seconds [ALPHA] Duration in seconds of node startup after registration.
        # TYPE kubelet_node_startup_post_registration_duration_seconds gauge
        kubelet_node_startup_post_registration_duration_seconds 0
        # HELP kubelet_node_startup_pre_kubelet_duration_seconds [ALPHA] Duration in seconds of node startup before kubelet starts.
        # TYPE kubelet_node_startup_pre_kubelet_duration_seconds gauge
        kubelet_node_startup_pre_kubelet_duration_seconds 0.1
        # HELP kubelet_node_startup_pre_registration_duration_seconds [ALPHA] Duration in seconds of node startup before registration.
        # TYPE kubelet_node_startup_pre_registration_duration_seconds gauge
        kubelet_node_startup_pre_registration_duration_seconds 0.1
        # HELP kubelet_node_startup_registration_duration_seconds [ALPHA] Duration in seconds of node startup during registration.
        # TYPE kubelet_node_startup_registration_duration_seconds gauge
        kubelet_node_startup_registration_duration_seconds 0.5
		`
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants),
			metricsNameNodeStartupPreKubelet,
			metricsNameNodeStartupPreRegistration,
			metricsNameNodeStartupRegistration,
			metricsNameNodeStartupPostRegistration,
			metricsNameNodeStartup,
		); err != nil {
			t.Error(err)
		}
	})

	t.Run("record register new node without previous step", func(t *testing.T) {
		metrics.Register()
		defer clearMetrics()

		fakeClock := testingclock.NewFakeClock(frozenTime)
		tracker := &basicNodeStartupLatencyTracker{
			bootTime:         frozenTime.Add(-100 * time.Millisecond),
			kubeletStartTime: frozenTime,
			clock:            fakeClock,
		}

		fakeClock.Step(700 * time.Millisecond)
		tracker.RecordRegisteredNewNode()

		assert.True(t, tracker.firstRegisteredNewNodeTime.IsZero())

		wants := `
		# HELP kubelet_node_startup_duration_seconds [ALPHA] Duration in seconds of node startup in total.
        # TYPE kubelet_node_startup_duration_seconds gauge
        kubelet_node_startup_duration_seconds 0
        # HELP kubelet_node_startup_post_registration_duration_seconds [ALPHA] Duration in seconds of node startup after registration.
        # TYPE kubelet_node_startup_post_registration_duration_seconds gauge
        kubelet_node_startup_post_registration_duration_seconds 0
        # HELP kubelet_node_startup_pre_kubelet_duration_seconds [ALPHA] Duration in seconds of node startup before kubelet starts.
        # TYPE kubelet_node_startup_pre_kubelet_duration_seconds gauge
        kubelet_node_startup_pre_kubelet_duration_seconds 0
        # HELP kubelet_node_startup_pre_registration_duration_seconds [ALPHA] Duration in seconds of node startup before registration.
        # TYPE kubelet_node_startup_pre_registration_duration_seconds gauge
        kubelet_node_startup_pre_registration_duration_seconds 0
        # HELP kubelet_node_startup_registration_duration_seconds [ALPHA] Duration in seconds of node startup during registration.
        # TYPE kubelet_node_startup_registration_duration_seconds gauge
        kubelet_node_startup_registration_duration_seconds 0
		`
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants),
			metricsNameNodeStartupPreKubelet,
			metricsNameNodeStartupPreRegistration,
			metricsNameNodeStartupRegistration,
			metricsNameNodeStartupPostRegistration,
			metricsNameNodeStartup,
		); err != nil {
			t.Error(err)
		}
	})
}

// TestSkippingRecordNodeReady 验证重复记录节点就绪或缺少前置步骤时，
// 节点就绪时间不会被错误记录。
func TestSkippingRecordNodeReady(t *testing.T) {
	t.Run("record node ready twice", func(t *testing.T) {
		metrics.Register()
		defer clearMetrics()

		fakeClock := testingclock.NewFakeClock(frozenTime)
		tracker := &basicNodeStartupLatencyTracker{
			bootTime:         frozenTime.Add(-100 * time.Millisecond),
			kubeletStartTime: frozenTime,
			clock:            fakeClock,
		}

		fakeClock.Step(100 * time.Millisecond)
		tracker.RecordAttemptRegisterNode()

		fakeClock.Step(200 * time.Millisecond)
		tracker.RecordRegisteredNewNode()

		fakeClock.Step(300 * time.Millisecond)
		tracker.RecordNodeReady()

		fakeClock.Step(700 * time.Millisecond)
		tracker.RecordNodeReady()

		assert.Equal(t, frozenTime.Add(600*time.Millisecond), tracker.firstNodeReadyTime)

		wants := `
		# HELP kubelet_node_startup_duration_seconds [ALPHA] Duration in seconds of node startup in total.
        # TYPE kubelet_node_startup_duration_seconds gauge
        kubelet_node_startup_duration_seconds 0.7
        # HELP kubelet_node_startup_post_registration_duration_seconds [ALPHA] Duration in seconds of node startup after registration.
        # TYPE kubelet_node_startup_post_registration_duration_seconds gauge
        kubelet_node_startup_post_registration_duration_seconds 0.3
        # HELP kubelet_node_startup_pre_kubelet_duration_seconds [ALPHA] Duration in seconds of node startup before kubelet starts.
        # TYPE kubelet_node_startup_pre_kubelet_duration_seconds gauge
        kubelet_node_startup_pre_kubelet_duration_seconds 0.1
        # HELP kubelet_node_startup_pre_registration_duration_seconds [ALPHA] Duration in seconds of node startup before registration.
        # TYPE kubelet_node_startup_pre_registration_duration_seconds gauge
        kubelet_node_startup_pre_registration_duration_seconds 0.1
        # HELP kubelet_node_startup_registration_duration_seconds [ALPHA] Duration in seconds of node startup during registration.
        # TYPE kubelet_node_startup_registration_duration_seconds gauge
        kubelet_node_startup_registration_duration_seconds 0.2
		`
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants),
			metricsNameNodeStartupPreKubelet,
			metricsNameNodeStartupPreRegistration,
			metricsNameNodeStartupRegistration,
			metricsNameNodeStartupPostRegistration,
			metricsNameNodeStartup,
		); err != nil {
			t.Error(err)
		}
	})

	t.Run("record node ready without previous step", func(t *testing.T) {
		metrics.Register()
		defer clearMetrics()

		fakeClock := testingclock.NewFakeClock(frozenTime)
		tracker := &basicNodeStartupLatencyTracker{
			bootTime:         frozenTime.Add(-100 * time.Millisecond),
			kubeletStartTime: frozenTime,
			clock:            fakeClock,
		}

		fakeClock.Step(100 * time.Millisecond)
		tracker.RecordAttemptRegisterNode()

		fakeClock.Step(700 * time.Millisecond)
		tracker.RecordNodeReady()

		assert.True(t, tracker.firstNodeReadyTime.IsZero())

		wants := `
		# HELP kubelet_node_startup_duration_seconds [ALPHA] Duration in seconds of node startup in total.
        # TYPE kubelet_node_startup_duration_seconds gauge
        kubelet_node_startup_duration_seconds 0
        # HELP kubelet_node_startup_post_registration_duration_seconds [ALPHA] Duration in seconds of node startup after registration.
        # TYPE kubelet_node_startup_post_registration_duration_seconds gauge
        kubelet_node_startup_post_registration_duration_seconds 0
        # HELP kubelet_node_startup_pre_kubelet_duration_seconds [ALPHA] Duration in seconds of node startup before kubelet starts.
        # TYPE kubelet_node_startup_pre_kubelet_duration_seconds gauge
        kubelet_node_startup_pre_kubelet_duration_seconds 0
        # HELP kubelet_node_startup_pre_registration_duration_seconds [ALPHA] Duration in seconds of node startup before registration.
        # TYPE kubelet_node_startup_pre_registration_duration_seconds gauge
        kubelet_node_startup_pre_registration_duration_seconds 0
        # HELP kubelet_node_startup_registration_duration_seconds [ALPHA] Duration in seconds of node startup during registration.
        # TYPE kubelet_node_startup_registration_duration_seconds gauge
        kubelet_node_startup_registration_duration_seconds 0
		`
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants),
			metricsNameNodeStartupPreKubelet,
			metricsNameNodeStartupPreRegistration,
			metricsNameNodeStartupRegistration,
			metricsNameNodeStartupPostRegistration,
			metricsNameNodeStartup,
		); err != nil {
			t.Error(err)
		}
	})
}

// clearMetrics 将所有节点启动延迟指标重置为 0，供测试之间隔离状态。
func clearMetrics() {
	metrics.NodeStartupPreKubeletDuration.Set(0)
	metrics.NodeStartupPreRegistrationDuration.Set(0)
	metrics.NodeStartupRegistrationDuration.Set(0)
	metrics.NodeStartupPostRegistrationDuration.Set(0)
	metrics.NodeStartupDuration.Set(0)
}
