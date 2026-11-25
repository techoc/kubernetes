/*
Copyright 2017 The Kubernetes Authors.

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

package validation

import (
	"fmt"
	"regexp"
	"time"
	"unicode"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/component-base/featuregate"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics"
	tracingapi "k8s.io/component-base/tracing/api/v1"
	utilsysctl "k8s.io/component-helpers/node/util/sysctl"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	imagepullmanager "k8s.io/kubernetes/pkg/kubelet/images/pullmanager"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
	utiltaints "k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/utils/cpuset"
)

var (
	defaultCFSQuota = metav1.Duration{Duration: 100 * time.Millisecond}
)

// previousDefaultMemoryThrottlingFactor 是旧版默认值（0.9）。
// 允许沿用旧默认值的 kubelet 配置文件在 MemoryQoS 关闭时仍通过校验。
const previousDefaultMemoryThrottlingFactor = 0.9

// ValidateKubeletConfiguration 校验 kubelet 配置 kc，返回聚合后的校验错误；
// 配置合法时返回 nil。
func ValidateKubeletConfiguration(kc *kubeletconfig.KubeletConfiguration, featureGate featuregate.FeatureGate) error {
	allErrors := []error{}

	// TODO(lauralorenz): 需要结合动态 KubeletConfiguration 合并的进度，重新评估
	// 此处对特性门控的解释。这里把门控复制到局部变量，并无条件地与当前门控配置
	// 合并后再做测试。defaults.go 中的假设与此相关，需一并考虑。

	// 复制一份特性门控，并与配置中声明的门控合并，
	// 从而按 kubelet 实际运行时的门控集合来校验配置。
	localFeatureGate := featureGate.DeepCopy()
	if err := localFeatureGate.SetFromMap(kc.FeatureGates); err != nil {
		return err
	}
	if errs := localFeatureGate.Validate(); len(errs) > 0 {
		allErrors = append(allErrors, errs...)
	}

	// 以下逐项校验各配置字段的取值范围及其相互约束。
	if kc.NodeLeaseDurationSeconds <= 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: nodeLeaseDurationSeconds must be greater than 0"))
	}
	if kc.SystemCgroups != "" && kc.CgroupRoot == "" {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: systemCgroups (--system-cgroups) was specified and cgroupRoot (--cgroup-root) was not specified"))
	}
	if kc.EventBurst < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: eventBurst (--event-burst) %v must not be a negative number", kc.EventBurst))
	}
	if kc.EventRecordQPS < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: eventRecordQPS (--event-qps) %v must not be a negative number", kc.EventRecordQPS))
	}
	if kc.HealthzPort != 0 && utilvalidation.IsValidPortNum(int(kc.HealthzPort)) != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: healthzPort (--healthz-port) %v must be between 1 and 65535, inclusive", kc.HealthzPort))
	}
	if !localFeatureGate.Enabled(features.CPUCFSQuotaPeriod) && kc.CPUCFSQuotaPeriod != defaultCFSQuota {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: cpuCFSQuotaPeriod (--cpu-cfs-quota-period) %v requires feature gate CustomCPUCFSQuotaPeriod", kc.CPUCFSQuotaPeriod))
	}
	if localFeatureGate.Enabled(features.CPUCFSQuotaPeriod) && utilvalidation.IsInRange(int(kc.CPUCFSQuotaPeriod.Duration), int(1*time.Millisecond), int(time.Second)) != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: cpuCFSQuotaPeriod (--cpu-cfs-quota-period) %v must be between 1ms and 1sec, inclusive", kc.CPUCFSQuotaPeriod))
	}
	if utilvalidation.IsInRange(int(kc.ImageGCHighThresholdPercent), 0, 100) != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: imageGCHighThresholdPercent (--image-gc-high-threshold) %v must be between 0 and 100, inclusive", kc.ImageGCHighThresholdPercent))
	}
	if utilvalidation.IsInRange(int(kc.ImageGCLowThresholdPercent), 0, 100) != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: imageGCLowThresholdPercent (--image-gc-low-threshold) %v must be between 0 and 100, inclusive", kc.ImageGCLowThresholdPercent))
	}
	if kc.ImageGCLowThresholdPercent >= kc.ImageGCHighThresholdPercent {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: imageGCLowThresholdPercent (--image-gc-low-threshold) %v must be less than imageGCHighThresholdPercent (--image-gc-high-threshold) %v", kc.ImageGCLowThresholdPercent, kc.ImageGCHighThresholdPercent))
	}
	if kc.ImageMaximumGCAge.Duration != 0 && !localFeatureGate.Enabled(features.ImageMaximumGCAge) {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: ImageMaximumGCAge feature gate is required for Kubelet configuration option imageMaximumGCAge"))
	}
	// 默认 sysctl 需要特性门控支持；同时校验名称合法性、命名空间已知性并检测重复。
	if len(kc.DefaultPodSysctls) > 0 {
		if !localFeatureGate.Enabled(features.DefaultPodSysctls) {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: DefaultPodSysctls feature gate is required for Kubelet configuration option defaultPodSysctls"))
		} else {
			seenSysctls := sets.New[string]()
			for k := range kc.DefaultPodSysctls {
				if !isValidSysctlName(k) {
					allErrors = append(allErrors, fmt.Errorf("invalid configuration: %q is not a valid sysctl name for defaultPodSysctls", k))
				} else {
					ns, _, _ := utilsysctl.GetNamespace(k)
					if ns == utilsysctl.UnknownNamespace {
						allErrors = append(allErrors, fmt.Errorf("invalid configuration: %q is not known to be namespaced for defaultPodSysctls", k))
					}
					normalizedKey := utilsysctl.NormalizeName(k)
					if seenSysctls.Has(normalizedKey) {
						allErrors = append(allErrors, fmt.Errorf("invalid configuration: duplicate sysctl %q found in defaultPodSysctls", normalizedKey))
					}
					seenSysctls.Insert(normalizedKey)
				}
			}
		}
	}
	if kc.ImageMinimumGCAge.Duration < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: imageMinimumGCAge %v must not be negative", kc.ImageMinimumGCAge.Duration))
	}
	if kc.ImageMaximumGCAge.Duration < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: imageMaximumGCAge %v must not be negative", kc.ImageMaximumGCAge.Duration))
	}
	if kc.ImageMaximumGCAge.Duration > 0 && kc.ImageMaximumGCAge.Duration <= kc.ImageMinimumGCAge.Duration {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: imageMaximumGCAge %v must be greater than imageMinimumGCAge %v", kc.ImageMaximumGCAge.Duration, kc.ImageMinimumGCAge.Duration))
	}
	if utilvalidation.IsInRange(int(kc.IPTablesDropBit), 0, 31) != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: iptablesDropBit (--iptables-drop-bit) %v must be between 0 and 31, inclusive", kc.IPTablesDropBit))
	}
	if utilvalidation.IsInRange(int(kc.IPTablesMasqueradeBit), 0, 31) != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: iptablesMasqueradeBit (--iptables-masquerade-bit) %v must be between 0 and 31, inclusive", kc.IPTablesMasqueradeBit))
	}
	if kc.KubeAPIBurst < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: kubeAPIBurst (--kube-api-burst) %v must not be a negative number", kc.KubeAPIBurst))
	}
	if kc.KubeAPIQPS < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: kubeAPIQPS (--kube-api-qps) %v must not be a negative number", kc.KubeAPIQPS))
	}
	if kc.NodeStatusMaxImages < -1 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: nodeStatusMaxImages (--node-status-max-images) %v must be -1 or greater", kc.NodeStatusMaxImages))
	}
	if kc.MaxOpenFiles < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: maxOpenFiles (--max-open-files) %v must not be a negative number", kc.MaxOpenFiles))
	}
	if kc.MaxPods < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: maxPods (--max-pods) %v must not be a negative number", kc.MaxPods))
	}
	if utilvalidation.IsInRange(int(kc.OOMScoreAdj), -1000, 1000) != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: oomScoreAdj (--oom-score-adj) %v must be between -1000 and 1000, inclusive", kc.OOMScoreAdj))
	}
	if kc.PodsPerCore < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: podsPerCore (--pods-per-core) %v must not be a negative number", kc.PodsPerCore))
	}
	if utilvalidation.IsValidPortNum(int(kc.Port)) != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: port (--port) %v must be between 1 and 65535, inclusive", kc.Port))
	}
	if kc.ReadOnlyPort != 0 && utilvalidation.IsValidPortNum(int(kc.ReadOnlyPort)) != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: readOnlyPort (--read-only-port) %v must be between 0 and 65535, inclusive", kc.ReadOnlyPort))
	}
	if kc.RegistryBurst < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: registryBurst (--registry-burst) %v must not be a negative number", kc.RegistryBurst))
	}
	if kc.RegistryPullQPS < 0 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: registryPullQPS (--registry-qps) %v must not be a negative number", kc.RegistryPullQPS))
	}
	if kc.MaxParallelImagePulls != nil && *kc.MaxParallelImagePulls < 1 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: maxParallelImagePulls %v must be a positive number", *kc.MaxParallelImagePulls))
	}
	if kc.SerializeImagePulls && kc.MaxParallelImagePulls != nil && *kc.MaxParallelImagePulls > 1 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: maxParallelImagePulls cannot be larger than 1 unless SerializeImagePulls (--serialize-image-pulls) is set to false"))
	}
	if kc.ServerTLSBootstrap && !localFeatureGate.Enabled(features.RotateKubeletServerCertificate) {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: serverTLSBootstrap %v requires feature gate RotateKubeletServerCertificate", kc.ServerTLSBootstrap))
	}
	if kc.RunOnce {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: runOnce (--runOnce) %v, Runonce mode has been deprecated and should not be set", kc.RunOnce))
	}

	// 注册污点本身必须合法，且不允许携带 TimeAdded——该字段由节点生命周期逻辑维护。
	for _, nodeTaint := range kc.RegisterWithTaints {
		if err := utiltaints.CheckTaintValidation(nodeTaint); err != nil {
			allErrors = append(allErrors, fmt.Errorf("invalid taint: %v", nodeTaint))
		}
		if nodeTaint.TimeAdded != nil {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: taint.TimeAdded is not nil"))
		}
	}

	// 拓扑管理器的策略与作用域必须是已知的枚举值。
	switch kc.TopologyManagerPolicy {
	case kubeletconfig.NoneTopologyManagerPolicy:
	case kubeletconfig.BestEffortTopologyManagerPolicy:
	case kubeletconfig.RestrictedTopologyManagerPolicy:
	case kubeletconfig.SingleNumaNodeTopologyManagerPolicy:
	default:
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: topologyManagerPolicy (--topology-manager-policy) %q must be one of: %q", kc.TopologyManagerPolicy, []string{kubeletconfig.NoneTopologyManagerPolicy, kubeletconfig.BestEffortTopologyManagerPolicy, kubeletconfig.RestrictedTopologyManagerPolicy, kubeletconfig.SingleNumaNodeTopologyManagerPolicy}))
	}

	switch kc.TopologyManagerScope {
	case kubeletconfig.ContainerTopologyManagerScope:
	case kubeletconfig.PodTopologyManagerScope:
	default:
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: topologyManagerScope (--topology-manager-scope) %q must be one of: %q, or %q", kc.TopologyManagerScope, kubeletconfig.ContainerTopologyManagerScope, kubeletconfig.PodTopologyManagerScope))
	}

	// 优雅节点关机的宽限期配置需要满足彼此的大小关系与最小时间下限。
	if localFeatureGate.Enabled(features.GracefulNodeShutdown) {
		if kc.ShutdownGracePeriodCriticalPods.Duration > kc.ShutdownGracePeriod.Duration {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: shutdownGracePeriodCriticalPods %v must be <= shutdownGracePeriod %v", kc.ShutdownGracePeriodCriticalPods, kc.ShutdownGracePeriod))
		}
		if kc.ShutdownGracePeriod.Duration < 0 || (kc.ShutdownGracePeriod.Duration > 0 && kc.ShutdownGracePeriod.Duration < time.Second) {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: shutdownGracePeriod %v must be either zero or otherwise >= 1 sec", kc.ShutdownGracePeriod))
		}
		if kc.ShutdownGracePeriodCriticalPods.Duration < 0 || (kc.ShutdownGracePeriodCriticalPods.Duration > 0 && kc.ShutdownGracePeriodCriticalPods.Duration < time.Second) {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: shutdownGracePeriodCriticalPods %v must be either zero or otherwise >= 1 sec", kc.ShutdownGracePeriodCriticalPods))
		}
	}
	if (kc.ShutdownGracePeriod.Duration > 0 || kc.ShutdownGracePeriodCriticalPods.Duration > 0) && !localFeatureGate.Enabled(features.GracefulNodeShutdown) {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: specifying shutdownGracePeriod or shutdownGracePeriodCriticalPods requires feature gate GracefulNodeShutdown"))
	}
	if localFeatureGate.Enabled(features.GracefulNodeShutdownBasedOnPodPriority) {
		if len(kc.ShutdownGracePeriodByPodPriority) != 0 && (kc.ShutdownGracePeriod.Duration > 0 || kc.ShutdownGracePeriodCriticalPods.Duration > 0) {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: Cannot specify both shutdownGracePeriodByPodPriority and shutdownGracePeriod at the same time"))
		}
	}
	if !localFeatureGate.Enabled(features.GracefulNodeShutdownBasedOnPodPriority) {
		if len(kc.ShutdownGracePeriodByPodPriority) != 0 {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: Specifying shutdownGracePeriodByPodPriority requires feature gate GracefulNodeShutdownBasedOnPodPriority"))
		}
	}
	// NodeSwap 门控开启时 swapBehavior 只能是已知取值；门控关闭时则禁止设置交换配置。
	if localFeatureGate.Enabled(features.NodeSwap) {
		switch kc.MemorySwap.SwapBehavior {
		case "":
		case string(kubetypes.NoSwap):
		case string(kubetypes.LimitedSwap):
		default:
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: memorySwap.swapBehavior %q must be one of: \"\", %q or %q", kc.MemorySwap.SwapBehavior, kubetypes.LimitedSwap, kubetypes.NoSwap))
		}
	}
	if !localFeatureGate.Enabled(features.NodeSwap) && kc.MemorySwap != (kubeletconfig.MemorySwapConfiguration{}) {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: memorySwap.swapBehavior cannot be set when NodeSwap feature flag is disabled"))
	}

	// KubeletCrashLoopBackOffMax 门控开启时必须设置重启周期上限，且取值范围为 1s~300s。
	if localFeatureGate.Enabled(features.KubeletCrashLoopBackOffMax) {
		if kc.CrashLoopBackOff.MaxContainerRestartPeriod == nil {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: FeatureGate KubeletCrashLoopBackOffMax is enabled, CrashLoopBackOff.MaxContainerRestartPeriod must be set"))
		}
		if kc.CrashLoopBackOff.MaxContainerRestartPeriod != nil && utilvalidation.IsInRange(int(kc.CrashLoopBackOff.MaxContainerRestartPeriod.Duration.Milliseconds()), 1000, 300000) != nil {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: CrashLoopBackOff.MaxContainerRestartPeriod (got: %v seconds) must be set between 1s and 300s", kc.CrashLoopBackOff.MaxContainerRestartPeriod.Seconds()))
		}
	} else if kc.CrashLoopBackOff.MaxContainerRestartPeriod != nil {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: FeatureGate KubeletCrashLoopBackOffMax not enabled, CrashLoopBackOff.MaxContainerRestartPeriod must not be set"))
	}

	// 在主校验循环之前，先检查 enforceNodeAllocatable 中互斥的键。
	reservedKeys := map[string]bool{
		kubetypes.SystemReservedEnforcementKey:             false,
		kubetypes.SystemReservedCompressibleEnforcementKey: false,
		kubetypes.KubeReservedEnforcementKey:               false,
		kubetypes.KubeReservedCompressibleEnforcementKey:   false,
	}

	for _, val := range kc.EnforceNodeAllocatable {
		reservedKeys[val] = true
	}

	if reservedKeys[kubetypes.SystemReservedCompressibleEnforcementKey] && reservedKeys[kubetypes.SystemReservedEnforcementKey] {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: both %q and %q cannot be specified together in enforceNodeAllocatable (--enforce-node-allocatable)", kubetypes.SystemReservedEnforcementKey, kubetypes.SystemReservedCompressibleEnforcementKey))
	}

	if reservedKeys[kubetypes.KubeReservedCompressibleEnforcementKey] && reservedKeys[kubetypes.KubeReservedEnforcementKey] {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: both %q and %q cannot be specified together in enforceNodeAllocatable (--enforce-node-allocatable)", kubetypes.KubeReservedEnforcementKey, kubetypes.KubeReservedCompressibleEnforcementKey))
	}

	// enforceNodeAllocatable 中的各项必须唯一；部分选项还要求对应的 cgroup 已配置。
	uniqueEnforcements := sets.Set[string]{}
	for _, val := range kc.EnforceNodeAllocatable {
		if uniqueEnforcements.Has(val) {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: duplicated enforcements %q in enforceNodeAllocatable (--enforce-node-allocatable)", val))
			continue
		}
		uniqueEnforcements.Insert(val)

		switch val {
		case kubetypes.NodeAllocatableEnforcementKey:
		case kubetypes.SystemReservedEnforcementKey, kubetypes.SystemReservedCompressibleEnforcementKey:
			if kc.SystemReservedCgroup == "" {
				allErrors = append(allErrors, fmt.Errorf("invalid configuration: systemReservedCgroup (--system-reserved-cgroup) must be specified when %q or %q included in enforceNodeAllocatable (--enforce-node-allocatable)", kubetypes.SystemReservedEnforcementKey, kubetypes.SystemReservedCompressibleEnforcementKey))
			}
		case kubetypes.KubeReservedEnforcementKey, kubetypes.KubeReservedCompressibleEnforcementKey:
			if kc.KubeReservedCgroup == "" {
				allErrors = append(allErrors, fmt.Errorf("invalid configuration: kubeReservedCgroup (--kube-reserved-cgroup) must be specified when %q or %q included in enforceNodeAllocatable (--enforce-node-allocatable)", kubetypes.KubeReservedEnforcementKey, kubetypes.KubeReservedCompressibleEnforcementKey))
			}
		case kubetypes.NodeAllocatableNoneKey:
			if len(kc.EnforceNodeAllocatable) > 1 {
				allErrors = append(allErrors, fmt.Errorf("invalid configuration: enforceNodeAllocatable (--enforce-node-allocatable) may not contain additional enforcements when %q is specified", kubetypes.NodeAllocatableNoneKey))
			}
			// 显式指定 "none" 时跳过后续所有检查。
			continue
		default:
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: option %q specified for enforceNodeAllocatable (--enforce-node-allocatable). Valid options are %q, %q, %q, %q, %q or %q",
				val, kubetypes.NodeAllocatableEnforcementKey, kubetypes.SystemReservedEnforcementKey, kubetypes.SystemReservedCompressibleEnforcementKey,
				kubetypes.KubeReservedEnforcementKey, kubetypes.KubeReservedCompressibleEnforcementKey, kubetypes.NodeAllocatableNoneKey))
			continue
		}

		if !kc.CgroupsPerQOS {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: cgroupsPerQOS (--cgroups-per-qos) must be set to true when %q contained in enforceNodeAllocatable (--enforce-node-allocatable)", val))
		}
	}
	// hairpinMode 必须是已知的取值之一。
	switch kc.HairpinMode {
	case kubeletconfig.HairpinNone:
	case kubeletconfig.HairpinVeth:
	case kubeletconfig.PromiscuousBridge:
	default:
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: option %q specified for hairpinMode (--hairpin-mode). Valid options are %q, %q or %q",
			kc.HairpinMode, kubeletconfig.HairpinNone, kubeletconfig.HairpinVeth, kubeletconfig.PromiscuousBridge))
	}

	// 镜像拉取凭证校验策略仅在对应特性门控开启时生效。
	if localFeatureGate.Enabled(features.KubeletEnsureSecretPulledImages) {
		switch kc.ImagePullCredentialsVerificationPolicy {
		case string(kubeletconfig.NeverVerify),
			string(kubeletconfig.NeverVerifyPreloadedImages),
			string(kubeletconfig.NeverVerifyAllowlistedImages),
			string(kubeletconfig.AlwaysVerify):
		default:
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: option %q specified for imagePullCredentialsVerificationPolicy. Valid options are %q, %q, %q or %q",
				kc.ImagePullCredentialsVerificationPolicy, kubeletconfig.NeverVerify, kubeletconfig.NeverVerifyPreloadedImages, kubeletconfig.NeverVerifyAllowlistedImages, kubeletconfig.AlwaysVerify))
		}

		if len(kc.PreloadedImagesVerificationAllowlist) > 0 && kc.ImagePullCredentialsVerificationPolicy != string(kubeletconfig.NeverVerifyAllowlistedImages) {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: can't set `preloadedImagesVerificationAllowlist` if `imagePullCredentialsVertificationPolicy` is not \"NeverVerifyAllowlistedImages\""))
		} else if err := imagepullmanager.ValidateAllowlistImagesPatterns(kc.PreloadedImagesVerificationAllowlist); err != nil {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: invalid image pattern in `preloadedImagesVerificationAllowlist`: %w", err))
		}
	} else {
		if len(kc.ImagePullCredentialsVerificationPolicy) > 0 {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: `imagePullCredentialsVerificationPolicy` must not be set if KubeletEnsureSecretPulledImages feature gate is not enabled"))
		}
		if len(kc.PreloadedImagesVerificationAllowlist) > 0 {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: `preloadedImagesVerificationAllowlist` must not be set if KubeletEnsureSecretPulledImages feature gate is not enabled"))
		}
	}

	if kc.ReservedSystemCPUs != "" {
		// --reserved-cpus 与 --system-reserved-cgroup/--kube-reserved-cgroup 互斥，
		// 同时指定会导致 CPU 预留与 cgroup 预留语义冲突。
		if kc.SystemReservedCgroup != "" || kc.KubeReservedCgroup != "" {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: can't use reservedSystemCPUs (--reserved-cpus) with systemReservedCgroup (--system-reserved-cgroup) or kubeReservedCgroup (--kube-reserved-cgroup)"))
		}
		if _, err := cpuset.Parse(kc.ReservedSystemCPUs); err != nil {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: unable to parse reservedSystemCPUs (--reserved-cpus) %v, error: %w", kc.ReservedSystemCPUs, err))
		}
	}

	// 委托子函数校验保留内存配置与操作系统相关的配置。
	allErrors = append(allErrors, validateReservedMemoryConfiguration(kc)...)

	if err := validateKubeletOSConfiguration(kc); err != nil {
		allErrors = append(allErrors, err)
	}
	// 日志、追踪与指标相关配置复用组件库的校验逻辑，避免重复实现。
	allErrors = append(allErrors, metrics.ValidateShowHiddenMetricsVersion(kc.ShowHiddenMetricsForVersion)...)

	if errs := logsapi.Validate(&kc.Logging, localFeatureGate, field.NewPath("logging")); len(errs) > 0 {
		allErrors = append(allErrors, errs.ToAggregate().Errors()...)
	}

	if errs := tracingapi.ValidateTracingConfiguration(kc.Tracing, localFeatureGate, field.NewPath("tracing")); len(errs) > 0 {
		allErrors = append(allErrors, errs.ToAggregate().Errors()...)
	}

	// 内存 QoS 相关参数依赖 MemoryQoS 特性门控：关闭时只允许旧默认值。
	if !localFeatureGate.Enabled(features.MemoryQoS) && kc.MemoryThrottlingFactor != nil && *kc.MemoryThrottlingFactor != previousDefaultMemoryThrottlingFactor {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: memoryThrottlingFactor must not be set when MemoryQoS feature gate is disabled"))
	}

	if kc.MemoryThrottlingFactor != nil && (*kc.MemoryThrottlingFactor <= 0 || *kc.MemoryThrottlingFactor > 1.0) {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: memoryThrottlingFactor %v must be greater than 0 and less than or equal to 1.0", *kc.MemoryThrottlingFactor))
	}

	if !localFeatureGate.Enabled(features.MemoryQoS) &&
		kc.MemoryReservationPolicy == kubeletconfig.TieredReservationMemoryReservationPolicy {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: memoryReservationPolicy %q requires MemoryQoS feature gate to be enabled",
			kc.MemoryReservationPolicy))
	}
	switch kc.MemoryReservationPolicy {
	case kubeletconfig.NoneMemoryReservationPolicy, kubeletconfig.TieredReservationMemoryReservationPolicy:
	default:
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: option %q specified for memoryReservationPolicy. Valid options are %q or %q", kc.MemoryReservationPolicy, kubeletconfig.NoneMemoryReservationPolicy, kubeletconfig.TieredReservationMemoryReservationPolicy))
	}

	// 容器运行时端点必填，否则 kubelet 无法与运行时通信。
	if kc.ContainerRuntimeEndpoint == "" {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: the containerRuntimeEndpoint was not specified or empty"))
	}

	// 系统日志查询需要 NodeLogQuery 门控，且依赖系统日志处理器处于开启状态。
	if kc.EnableSystemLogQuery && !localFeatureGate.Enabled(features.NodeLogQuery) {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: NodeLogQuery feature gate is required for enableSystemLogQuery"))
	}
	if kc.EnableSystemLogQuery && !kc.EnableSystemLogHandler {
		allErrors = append(allErrors,
			fmt.Errorf("invalid configuration: enableSystemLogHandler is required for enableSystemLogQuery"))
	}

	// 容器日志相关的并发度、监控间隔与文件数量都有下限要求。
	if kc.ContainerLogMaxWorkers < 1 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: containerLogMaxWorkers must be greater than or equal to 1"))
	}

	if kc.ContainerLogMonitorInterval.Duration.Seconds() < 3 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: containerLogMonitorInterval must be a positive time duration greater than or equal to 3s"))
	}

	if kc.ContainerLogMaxFiles <= 1 {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: containerLogMaxFiles must be greater than 1"))
	}

	// Pod 日志目录必须是绝对路径且已规范化，否则日志落盘行为不可预期。
	if kc.PodLogsDir == "" {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: podLogsDir was not specified"))
	}

	if !utilfs.IsAbs(kc.PodLogsDir) {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: pod logs path %q must be absolute path", kc.PodLogsDir))
	}

	if !utilfs.IsPathClean(kc.PodLogsDir) {
		allErrors = append(allErrors, fmt.Errorf("invalid configuration: pod logs path %q must be normalized", kc.PodLogsDir))
	}

	// Pod 日志路径会作为指标标签值使用，因此必须确保只包含 ASCII 字符。
	for _, c := range kc.PodLogsDir {
		if c > unicode.MaxASCII {
			allErrors = append(allErrors, fmt.Errorf("invalid configuration: pod logs path %q mut contains ASCII characters only", kc.PodLogsDir))
			break
		}
	}

	// 汇总所有校验错误一次性返回，便于使用者一次性修复全部问题。
	return utilerrors.NewAggregate(allErrors)
}

// 以下 sysctl 名称格式规则与 pkg/apis/policy/validation 中的定义保持一致。
// 参考：https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/policy/validation/validation.go
const (
	sysctlSegmentFmt      string = "[a-z0-9]([-_a-z0-9]*[a-z0-9])?"
	sysctlContainSlashFmt string = "(" + sysctlSegmentFmt + "[\\./])*" + sysctlSegmentFmt
	sysctlMaxLength       int    = 253
)

var sysctlRegexp = regexp.MustCompile("^" + sysctlContainSlashFmt + "$")

// isValidSysctlName 在配置阶段校验 sysctl 名称是否合法，
// 防止 kubelet 在运行时向沙箱容器应用非法 sysctl 而失败。
func isValidSysctlName(name string) bool {
	if len(name) > sysctlMaxLength {
		return false
	}
	return sysctlRegexp.MatchString(name)
}
