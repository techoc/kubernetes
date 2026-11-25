/*
Copyright 2022 The Kubernetes Authors.

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

package options

import (
	"fmt"

	"github.com/spf13/pflag"

	nodeconfig "k8s.io/cloud-provider/controllers/node/config"
)

// NodeControllerOptions 持有 NodeController 的选项（原注释误写为 ServiceController），
// 实际是 cloud-node-controller 的 flag 视图，直接嵌入内部配置结构，
// AddFlags/ApplyTo 负责在 pflag 与配置之间搬运值。
type NodeControllerOptions struct {
	*nodeconfig.NodeControllerConfiguration
}

// AddFlags 把 NodeController 相关的 flag 注册到指定的 FlagSet。
// flag 默认值取自嵌入配置；因此 ApplyTo 只需处理需要特殊推导的字段。
func (o *NodeControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.ConcurrentNodeSyncs, "concurrent-node-syncs", o.ConcurrentNodeSyncs, "Number of workers concurrently synchronizing nodes.")
	// 为向后兼容，未指定 --concurrent-node-status-updates 时将从 --concurrent-node-syncs 推导。
	// 默认写 0 表示“未设置”，ApplyTo 时再决定回退值，避免 flag 默认值掩盖用户显式输入。
	fs.Int32Var(&o.ConcurrentNodeStatusUpdates, "concurrent-node-status-updates", 0, "Number of workers concurrently updating node statuses. If unspecified or 0, --concurrent-node-syncs is used instead")
}

// ApplyTo 用选项填充 NodeController 配置。
// 状态更新 worker 数在未显式设置时回退为并发同步数，保持旧版本行为。
func (o *NodeControllerOptions) ApplyTo(cfg *nodeconfig.NodeControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.ConcurrentNodeSyncs = o.ConcurrentNodeSyncs
	// Concurrent node status updates used to be derived from the concurrent-node-syncs flag,
	// so for backwards compatibility it will default to concurrent-node-syncs if concurrent-node-status-updates is not specified
	if o.ConcurrentNodeStatusUpdates > 0 {
		cfg.ConcurrentNodeStatusUpdates = o.ConcurrentNodeStatusUpdates
	} else {
		cfg.ConcurrentNodeStatusUpdates = o.ConcurrentNodeSyncs
	}

	return nil
}

// Validate 校验 NodeControllerOptions。
// 同步数必须为正；状态更新数允许 0（表示继承），但不允许负数。
func (o *NodeControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}
	var errors []error
	if o.ConcurrentNodeSyncs <= 0 {
		errors = append(errors, fmt.Errorf("concurrent-node-syncs must be a positive number"))
	}
	if o.ConcurrentNodeStatusUpdates < 0 {
		errors = append(errors, fmt.Errorf("concurrent-node-status-updates cannot be a negative number"))
	}
	return errors
}
