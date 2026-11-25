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

package framework

import "time"

var defaultTimeouts = TimeoutContext{
	Poll:                      2 * time.Second, // 来自原先的 e2e/framework/pod poll interval
	PodStart:                  5 * time.Minute,
	PodStartShort:             2 * time.Minute,
	PodStartSlow:              15 * time.Minute,
	PodDelete:                 5 * time.Minute,
	ClaimProvision:            5 * time.Minute,
	ClaimProvisionShort:       1 * time.Minute,
	DataSourceProvision:       5 * time.Minute,
	ClaimBound:                3 * time.Minute,
	PVReclaim:                 3 * time.Minute,
	PVBound:                   3 * time.Minute,
	PVCreate:                  3 * time.Minute,
	PVDelete:                  5 * time.Minute,
	PVDeleteSlow:              20 * time.Minute,
	SnapshotCreate:            5 * time.Minute,
	SnapshotDelete:            5 * time.Minute,
	SnapshotControllerMetrics: 5 * time.Minute,
	SystemPodsStartup:         10 * time.Minute,
	NodeSchedulable:           30 * time.Minute,
	SystemDaemonsetStartup:    5 * time.Minute,
	NodeNotReady:              3 * time.Minute,
	ClaimUnusedCondition:      3 * time.Minute,
}

// TimeoutContext 包含若干操作的超时设置。
type TimeoutContext struct {
	// Poll 是等待某个 condition 时两次 API 调用之间的间隔。
	Poll time.Duration

	// PodStart 是等待 Pod 启动的时长。
	// 此值是 gomega.Eventually 的默认值。
	PodStart time.Duration

	// PodStartShort 与 `PodStart` 相同，但更短。
	// 视情况使用，主要在确信 Pod 启动不会延迟时使用。
	// 此值是 gomega.Consistently 的默认值。
	PodStartShort time.Duration

	// PodStartSlow 与 `PodStart` 相同，但更长。
	// 视情况使用，主要在确信 Pod 启动会比平时更慢时使用。
	PodStartSlow time.Duration

	// PodDelete 是等待 Pod 被删除的时长。
	PodDelete time.Duration

	// ClaimProvision 是等待 claim 完成动态供给的时长。
	ClaimProvision time.Duration

	// DataSourceProvision 是等待 claim 从源 claim 完成动态供给的时长。
	DataSourceProvision time.Duration

	// ClaimProvisionShort 与 `ClaimProvision` 相同，但更短。
	ClaimProvisionShort time.Duration

	// ClaimBound 是等待 claim 绑定的时长。
	ClaimBound time.Duration

	// PVReclaim 是等待 PV 被回收的时长。
	PVReclaim time.Duration

	// PVBound 是等待 PV 绑定的时长。
	PVBound time.Duration

	// PVCreate 是等待 PV 被创建的时长。
	PVCreate time.Duration

	// PVDelete 是等待 PV 被删除的时长。
	PVDelete time.Duration

	// PVDeleteSlow 与 PVDelete 相同，但更长。
	PVDeleteSlow time.Duration

	// SnapshotCreate 是等待 snapshot 创建 snapshotContent 的时长。
	SnapshotCreate time.Duration

	// SnapshotDelete 是等待 snapshot 删除 snapshotContent 的时长。
	SnapshotDelete time.Duration

	// SnapshotControllerMetrics 是等待 snapshot controller metrics 的时长。
	SnapshotControllerMetrics time.Duration

	// SystemPodsStartup 是等待系统 Pod 进入运行态的时长。
	SystemPodsStartup time.Duration

	// NodeSchedulable 是等待所有节点变为可调度的时长。
	NodeSchedulable time.Duration

	// SystemDaemonsetStartup 是等待所有系统 daemonset 就绪的时长。
	SystemDaemonsetStartup time.Duration

	// NodeNotReady 是等待节点变为 NotReady 的时长。
	NodeNotReady time.Duration

	// ClaimUnusedCondition 是等待 PVC 上出现或改变 Unused condition 的时长。
	ClaimUnusedCondition time.Duration
}

// NewTimeoutContext 返回一个 TimeoutContext，所有值要么设为
// 硬编码默认值，要么设为运行 E2E 套件时配置的值。
// 套件。应在命令行解析之后调用。
func NewTimeoutContext() *TimeoutContext {
	// 复制一份，否则调用方就有可能修改
	// 原始值。
	copy := TestContext.timeouts
	return &copy
}

// PollInterval 定义等待某个 condition 时
// 等待某个 condition 时。
//
// 此值是 gomega.Eventually 和 gomega.Consistently 的默认值。
func PollInterval() time.Duration {
	return TestContext.timeouts.Poll
}
