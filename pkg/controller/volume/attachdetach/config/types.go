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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AttachDetachControllerConfiguration 描述挂载/卸载控制器的配置项。
type AttachDetachControllerConfiguration struct {
	// Reconciler 周期性运行，通过触发挂载/卸载操作来协调期望状态与实际状态。
	// 该标志用于启用或停止协调；默认为 false，即协调功能启用。
	DisableAttachDetachReconcilerSync bool
	// ReconcilerSyncLoopPeriod 是协调循环两次执行之间的等待时长，默认 60 秒。
	ReconcilerSyncLoopPeriod metav1.Duration
	// DisableForceDetachOnTimeout 控制在超过最大卸载时间时是否禁用强制卸载。
	// 默认为 false，即超时后仍会强制卸载。
	DisableForceDetachOnTimeout bool
}
