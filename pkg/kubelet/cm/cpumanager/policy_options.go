/*
Copyright 2021 The Kubernetes Authors.

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

package cpumanager

import (
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
)

// 选项名称，属于用户接口的一部分。
const (
	FullPCPUsOnlyOption             string = "full-pcpus-only"
	DistributeCPUsAcrossNUMAOption  string = "distribute-cpus-across-numa"
	AlignBySocketOption             string = "align-by-socket"
	DistributeCPUsAcrossCoresOption string = "distribute-cpus-across-cores"
	StrictCPUReservationOption      string = "strict-cpu-reservation"
	PreferAlignByUnCoreCacheOption  string = "prefer-align-cpus-by-uncorecache"
)

// 按特性成熟度分组的选项集合，用于特性门控校验。
var (
	alphaOptions = sets.New[string](
		AlignBySocketOption,
		DistributeCPUsAcrossCoresOption,
	)
	betaOptions = sets.New[string](
		DistributeCPUsAcrossNUMAOption,
	)
	stableOptions = sets.New[string](
		FullPCPUsOnlyOption,
		StrictCPUReservationOption,
		PreferAlignByUnCoreCacheOption,
	)
)

// CheckPolicyOptionAvailable 根据特性门控设置校验给定选项是否可用；
// 成功返回 nil，失败返回描述性错误。
func CheckPolicyOptionAvailable(option string) error {
	// 未知选项直接拒绝。
	if !alphaOptions.Has(option) && !betaOptions.Has(option) && !stableOptions.Has(option) {
		return fmt.Errorf("unknown CPU Manager Policy option: %q", option)
	}

	// alpha 选项需要 CPUManagerPolicyAlphaOptions 门控。
	if alphaOptions.Has(option) && !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CPUManagerPolicyAlphaOptions) {
		return fmt.Errorf("CPU Manager Policy Alpha-level Options not enabled, but option %q provided", option)
	}

	// beta 选项需要 CPUManagerPolicyBetaOptions 门控。
	if betaOptions.Has(option) && !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CPUManagerPolicyBetaOptions) {
		return fmt.Errorf("CPU Manager Policy Beta-level Options not enabled, but option %q provided", option)
	}

	// stable 选项无需特性门控检查。
	return nil
}

// StaticPolicyOptions 保存解析后的策略选项，供内部使用。
type StaticPolicyOptions struct {
	// 是否启用额外分配限制，避免不同容器落在同一个 core 上；
	// 这里将 "core" 与 "physical CPU" 视为同义，沿用 k8s 的术语；
	// 命名难免有歧义，选择 "pcpu" 是因为既有文档已暗示 vCPU。
	FullPhysicalCPUsOnly bool
	// 当需要多个 NUMA 节点才能满足分配时，是否将 CPU 均匀分布到各节点。
	DistributeCPUsAcrossNUMA bool
	// 是否按 socket 边界而非 NUMA 边界对齐 CPU。
	AlignBySocket bool
	// 是否把超线程 CPU 分散到不同物理 core；
	// 这是偏好型策略，无法满足时不应报错。
	DistributeCPUsAcrossCores bool
	// 是否从可用 CPU 列表中移除预留 core。
	StrictCPUReservation bool
	// 尽量把 CPU 对齐到 uncore cache 边界；
	// 只要仍有 CPU 可用，条件不满足也允许 Pod 准入。
	PreferAlignByUncoreCacheOption bool
}

// NewStaticPolicyOptions 根据用户配置创建 StaticPolicyOptions。
func NewStaticPolicyOptions(policyOptions map[string]string) (StaticPolicyOptions, error) {
	opts := StaticPolicyOptions{}
	// 逐项校验选项可用性并解析布尔值。
	for name, value := range policyOptions {
		if err := CheckPolicyOptionAvailable(name); err != nil {
			return opts, err
		}

		switch name {
		case FullPCPUsOnlyOption:
			optValue, err := strconv.ParseBool(value)
			if err != nil {
				return opts, fmt.Errorf("bad value for option %q: %w", name, err)
			}
			opts.FullPhysicalCPUsOnly = optValue
		case DistributeCPUsAcrossNUMAOption:
			optValue, err := strconv.ParseBool(value)
			if err != nil {
				return opts, fmt.Errorf("bad value for option %q: %w", name, err)
			}
			opts.DistributeCPUsAcrossNUMA = optValue
		case AlignBySocketOption:
			optValue, err := strconv.ParseBool(value)
			if err != nil {
				return opts, fmt.Errorf("bad value for option %q: %w", name, err)
			}
			opts.AlignBySocket = optValue
		case DistributeCPUsAcrossCoresOption:
			optValue, err := strconv.ParseBool(value)
			if err != nil {
				return opts, fmt.Errorf("bad value for option %q: %w", name, err)
			}
			opts.DistributeCPUsAcrossCores = optValue
		case StrictCPUReservationOption:
			optValue, err := strconv.ParseBool(value)
			if err != nil {
				return opts, fmt.Errorf("bad value for option %q: %w", name, err)
			}
			opts.StrictCPUReservation = optValue
		case PreferAlignByUnCoreCacheOption:
			optValue, err := strconv.ParseBool(value)
			if err != nil {
				return opts, fmt.Errorf("bad value for option %q: %w", name, err)
			}
			opts.PreferAlignByUncoreCacheOption = optValue
		default:
			// 这里不应到达，因为前面已经检测到未知选项；
			// 保留该分支是为了提供额外的安全保护。
			return opts, fmt.Errorf("unsupported cpumanager option: %q (%s)", name, value)
		}
	}

	// 这些选项组合语义冲突，必须拒绝。
	if opts.FullPhysicalCPUsOnly && opts.DistributeCPUsAcrossCores {
		return opts, fmt.Errorf("static policy options %s and %s can not be used at the same time", FullPCPUsOnlyOption, DistributeCPUsAcrossCoresOption)
	}

	// TODO(@Jeffwan)：完成更多兼容性测试后移除此检查。
	// 两个选项同时启用会互相冲突。
	if opts.DistributeCPUsAcrossNUMA && opts.DistributeCPUsAcrossCores {
		return opts, fmt.Errorf("static policy options %s and %s can not be used at the same time", DistributeCPUsAcrossNUMAOption, DistributeCPUsAcrossCoresOption)
	}

	// uncorecache 对齐与跨 core 分散互斥。
	if opts.PreferAlignByUncoreCacheOption && opts.DistributeCPUsAcrossCores {
		return opts, fmt.Errorf("static policy options %s and %s can not be used at the same time", PreferAlignByUnCoreCacheOption, DistributeCPUsAcrossCoresOption)
	}

	// uncorecache 对齐与跨 NUMA 分散互斥。
	if opts.PreferAlignByUncoreCacheOption && opts.DistributeCPUsAcrossNUMA {
		return opts, fmt.Errorf("static policy options %s and %s can not be used at the same time", PreferAlignByUnCoreCacheOption, DistributeCPUsAcrossNUMAOption)
	}

	return opts, nil
}

// ValidateStaticPolicyOptions 确保请求的策略选项与 CPUManager 所在机器兼容。
func ValidateStaticPolicyOptions(opts StaticPolicyOptions, topology *topology.CPUTopology, topologyManager topologymanager.Store) error {
	// 按 socket 对齐与 single-numa-node 策略、以及 socket 多于 NUMA 的硬件不兼容。
	if opts.AlignBySocket {
		// 与 topology manager 的 single-numa-node 策略选项不兼容。
		if topologyManager.GetPolicy().Name() == topologymanager.PolicySingleNumaNode {
			return fmt.Errorf("Topolgy manager %s policy is incompatible with CPUManager %s policy option", topologymanager.PolicySingleNumaNode, AlignBySocketOption)
		}
		// 当 socket 数量多于 NUMA 节点数量时，与拓扑不兼容。
		if topology.NumSockets > topology.NumNUMANodes {
			return fmt.Errorf("Align by socket is not compatible with hardware where number of sockets are more than number of NUMA")
		}
	}
	return nil
}
