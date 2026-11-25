/*
Copyright 2025 The Kubernetes Authors.

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

package benchmark

import (
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

type SchedulerPerfOption func(options *schedulerPerfOptions)

// HookFn 是在测试执行过程中调用的函数。
// 该函数可以通过 tCtx 记录测试错误或中止当前测试运行。
// 此外，它也可以返回一个非 nil 的 error。
type HookFn func(tCtx ktesting.TContext) error

// PreRunFn hook 函数在每个 workload 的 feature gate 设置完成后、
// 调度器启动之前调用。它返回一个可选的清理函数和一个 error。
type PreRunFn func(tCtx ktesting.TContext, w *Workload) (func(), error)

// NodeUpdateFn 是在 workload 中通过 createNodesOp 创建节点后调用的函数。
type NodeUpdateFn func(tCtx ktesting.TContext, scheduler *scheduler.Scheduler, w *Workload, nodes *v1.NodeList) error

type schedulerPerfOptions struct {
	outOfTreePluginRegistry frameworkruntime.Registry
	preRunFn                PreRunFn
	prepareFn               HookFn
	nodeUpdateFn            NodeUpdateFn
	podsSchedulingTimeout   time.Duration
}

// WithPrepareFn 用于设置一个函数，该函数在每个 workload 运行前调用。
// （例如为自定义插件应用 CRD。）
// 此时 Scheduler 和 etcd 已经启动。
func WithPrepareFn(prepareFn HookFn) SchedulerPerfOption {
	return func(s *schedulerPerfOptions) {
		s.prepareFn = prepareFn
	}
}

// WithNodeUpdateFn 用于设置一个函数，该函数在 workload 执行期间
// 由 createNodesOp 创建节点之后调用。
func WithNodeUpdateFn(fn NodeUpdateFn) SchedulerPerfOption {
	return func(s *schedulerPerfOptions) {
		s.nodeUpdateFn = fn
	}
}

// WithPreRunFn 用于设置一个函数，该函数在完成进程配置之后、
// 但在运行任何代码之前调用。进程配置包括日志和 feature gate，
// 而要运行的代码包括 etcd 和 Scheduler。
func WithPreRunFn(preRunFn PreRunFn) SchedulerPerfOption {
	return func(s *schedulerPerfOptions) {
		s.preRunFn = preRunFn
	}
}

// WithPodsSchedulingTimeout 用于设置专门的自定义超时时间，
// 用于等待 Pod 完成调度。
func WithPodsSchedulingTimeout(timeout time.Duration) SchedulerPerfOption {
	return func(s *schedulerPerfOptions) {
		s.podsSchedulingTimeout = timeout
	}
}
