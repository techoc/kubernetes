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

package topologymanager

import (
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	kubefeatures "k8s.io/kubernetes/pkg/features"
)

const (
	// PreferClosestNUMANodes 是优先选择 NUMA 距离更近节点的策略选项。
	PreferClosestNUMANodes string = "prefer-closest-numa-nodes"
	// MaxAllowableNUMANodes 是允许参与拓扑对齐的最大 NUMA 节点数选项。
	MaxAllowableNUMANodes string = "max-allowable-numa-nodes"
)

// 策略选项按稳定性分级；只有对应特性门控开启时才允许使用 alpha/beta 选项。
var (
	alphaOptions  = sets.New[string]()
	betaOptions   = sets.New[string]()
	stableOptions = sets.New[string](
		PreferClosestNUMANodes,
		MaxAllowableNUMANodes,
	)
)

// CheckPolicyOptionAvailable 校验策略选项是否已知且在当前门控下可用。
func CheckPolicyOptionAvailable(option string) error {
	if !alphaOptions.Has(option) && !betaOptions.Has(option) && !stableOptions.Has(option) {
		return fmt.Errorf("unknown Topology Manager Policy option: %q", option)
	}

	if alphaOptions.Has(option) && !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.TopologyManagerPolicyAlphaOptions) {
		return fmt.Errorf("topology manager policy alpha-level options not enabled, but option %q provided", option)
	}

	if betaOptions.Has(option) && !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.TopologyManagerPolicyBetaOptions) {
		return fmt.Errorf("topology manager policy beta-level options not enabled, but option %q provided", option)
	}

	return nil
}

// PolicyOptions 保存解析后的 Topology Manager 策略选项。
type PolicyOptions struct {
	// PreferClosestNUMA 表示是否优先选择 NUMA 距离更近的节点。
	PreferClosestNUMA bool
	// MaxAllowableNUMANodes 是允许的最大 NUMA 节点数。
	MaxAllowableNUMANodes int
}

// NewPolicyOptions 解析并校验策略选项，返回填充好的 PolicyOptions。
func NewPolicyOptions(logger klog.Logger, policyOptions map[string]string) (PolicyOptions, error) {
	opts := PolicyOptions{
		// 先把 MaxAllowableNUMANodes 设为默认值；
		// 若用户显式指定该选项，会在下面覆盖。
		MaxAllowableNUMANodes: defaultMaxAllowableNUMANodes,
	}

	for name, value := range policyOptions {
		if err := CheckPolicyOptionAvailable(name); err != nil {
			return opts, err
		}

		// 按选项名称解析对应类型的值。
		switch name {
		case PreferClosestNUMANodes:
			optValue, err := strconv.ParseBool(value)
			if err != nil {
				return opts, fmt.Errorf("bad value for option %q: %w", name, err)
			}
			opts.PreferClosestNUMA = optValue
		case MaxAllowableNUMANodes:
			optValue, err := strconv.Atoi(value)
			if err != nil {
				return opts, fmt.Errorf("unable to convert policy option to integer %q: %w", name, err)
			}

			// 低于默认值会破坏 kubelet 的预期，直接拒绝。
			if optValue < defaultMaxAllowableNUMANodes {
				return opts, fmt.Errorf("the minimum value of %q should not be less than %v", name, defaultMaxAllowableNUMANodes)
			}

			// 高于默认值仅告警，不阻止启动。
			if optValue > defaultMaxAllowableNUMANodes {
				logger.Info("WARNING: the value of max-allowable-numa-nodes is more than the default recommended value", "max-allowable-numa-nodes", optValue, "defaultMaxAllowableNUMANodes", defaultMaxAllowableNUMANodes)
			}
			opts.MaxAllowableNUMANodes = optValue
		default:
			// 正常情况下不会到达这里（未知选项已在前面拦截），
			// 保留该分支作为额外防护。
			return opts, fmt.Errorf("unsupported topologymanager option: %q (%s)", name, value)
		}
	}
	return opts, nil
}
