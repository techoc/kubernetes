//go:build linux

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

package cm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	libcontainercgroups "github.com/opencontainers/cgroups"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/stats/pidlimit"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

const (
	defaultNodeAllocatableCgroupName = "kubepods"
)

// createNodeAllocatableCgroups 在启用 CgroupsPerQOS 时创建节点可分配资源（kubepods）cgroup。
func (cm *containerManagerImpl) createNodeAllocatableCgroups(logger klog.Logger) error {
	nodeAllocatable := cm.internalCapacity
	// 用户要求强制节点可分配资源时，用可分配上限而不是节点总容量作为 cgroup 限制。
	nc := cm.NodeConfig.NodeAllocatableConfig
	if cm.CgroupsPerQOS && nc.EnforceNodeAllocatable.Has(kubetypes.NodeAllocatableEnforcementKey) {
		nodeAllocatable = cm.getNodeAllocatableInternalAbsolute()
	}

	cgroupConfig := &CgroupConfig{
		Name: cm.cgroupRoot,
		// cgroup 默认 CPU shares 很低，可能导致 Pod 被 CPU 饥饿，因此显式设置。
		ResourceParameters: cm.getCgroupConfig(nodeAllocatable, false),
	}
	// cgroup 已存在则无需重复创建。
	if cm.cgroupManager.Exists(cgroupConfig.Name) {
		return nil
	}
	if err := cm.cgroupManager.Create(logger, cgroupConfig); err != nil {
		logger.Error(err, "Failed to create cgroup", "cgroupName", cm.cgroupRoot)
		return err
	}
	return nil
}

// enforceNodeAllocatableCgroups 强制执行节点可分配资源的 cgroup 限制。
func (cm *containerManagerImpl) enforceNodeAllocatableCgroups(logger klog.Logger) error {
	nc := cm.NodeConfig.NodeAllocatableConfig

	// 无论是否强制节点可分配资源，都要更新 kubepods cgroup 的 CPU 限制：
	// cgroup 默认 CPU shares 太低，可能造成 CPU 饥饿。
	nodeAllocatable := cm.internalCapacity
	// 用户要求强制节点可分配资源时，用可分配上限而不是节点总容量。
	if cm.CgroupsPerQOS && nc.EnforceNodeAllocatable.Has(kubetypes.NodeAllocatableEnforcementKey) {
		nodeAllocatable = cm.getNodeAllocatableInternalAbsolute()
	}

	logger.V(4).Info("Attempting to enforce Node Allocatable", "config", nc)

	cgroupConfig := &CgroupConfig{
		Name:               cm.cgroupRoot,
		ResourceParameters: cm.getCgroupConfig(nodeAllocatable, false),
	}

	// 之前启用 MemoryQoS 时写入的 memory.min/memory.low 可能跨 kubelet 重启残留，
	// 这里重置为 0，确保回滚真正生效。
	if !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) && libcontainercgroups.IsCgroup2UnifiedMode() {
		rp := cgroupConfig.ResourceParameters
		if rp.Unified == nil {
			rp.Unified = make(map[string]string)
		}
		rp.Unified[Cgroup2MemoryMin] = "0"
		rp.Unified[Cgroup2MemoryLow] = "0"
	}

	// 事件使用 ObjectReference：节点对象可能尚未缓存，详见 #42701。
	nodeRef := nodeRefFromNode(cm.nodeInfo.Name)

	// 如果节点尚未 drain 或将 Node Allocatable 调低，现有 Pod 的内存占用可能高于新限制；
	// 预期通过 Pod 驱逐逐步降到限制以下，因此这里持续重试 cgroup 更新直到成功。
	// 非根 cgroupRoot 也要更新：cgroup 默认 CPU 限制可能过低。
	// cgroupRoot 为空表示根容器，此时跳过。
	if len(cm.cgroupRoot) > 0 {
		go func() {
			for {
				err := cm.cgroupManager.Update(logger, cgroupConfig)
				// 更新成功后上报事件并退出重试循环。
				if err == nil {
					cm.recorder.Event(nodeRef, v1.EventTypeNormal, events.SuccessfulNodeAllocatableEnforcement, "Updated Node Allocatable limit across pods")
					return
				}
				message := fmt.Sprintf("Failed to update Node Allocatable Limits %q: %v", cm.cgroupRoot, err)
				cm.recorder.Event(nodeRef, v1.EventTypeWarning, events.FailedNodeAllocatableEnforcement, message)
				time.Sleep(time.Minute)
			}
		}()
	}
	// 按需应用 system-reserved 和 kube-reserved 的 cgroup 限制。
	if nc.EnforceNodeAllocatable.Has(kubetypes.SystemReservedEnforcementKey) {
		logger.V(2).Info("Enforcing system reserved on cgroup", "cgroupName", nc.SystemReservedCgroupName, "limits", nc.SystemReserved)
		if err := cm.enforceExistingCgroup(logger, nc.SystemReservedCgroupName, nc.SystemReserved, false); err != nil {
			message := fmt.Sprintf("Failed to enforce System Reserved Cgroup Limits on %q: %v", nc.SystemReservedCgroupName, err)
			cm.recorder.Event(nodeRef, v1.EventTypeWarning, events.FailedNodeAllocatableEnforcement, message)
			return errors.New(message)
		}
		cm.recorder.Eventf(nodeRef, v1.EventTypeNormal, events.SuccessfulNodeAllocatableEnforcement, "Updated limits on system reserved cgroup %v", nc.SystemReservedCgroupName)
	}
	if nc.EnforceNodeAllocatable.Has(kubetypes.KubeReservedEnforcementKey) {
		logger.V(2).Info("Enforcing kube reserved on cgroup", "cgroupName", nc.KubeReservedCgroupName, "limits", nc.KubeReserved)
		if err := cm.enforceExistingCgroup(logger, nc.KubeReservedCgroupName, nc.KubeReserved, false); err != nil {
			message := fmt.Sprintf("Failed to enforce Kube Reserved Cgroup Limits on %q: %v", nc.KubeReservedCgroupName, err)
			cm.recorder.Event(nodeRef, v1.EventTypeWarning, events.FailedNodeAllocatableEnforcement, message)
			return errors.New(message)
		}
		cm.recorder.Eventf(nodeRef, v1.EventTypeNormal, events.SuccessfulNodeAllocatableEnforcement, "Updated limits on kube reserved cgroup %v", nc.KubeReservedCgroupName)
	}

	if nc.EnforceNodeAllocatable.Has(kubetypes.SystemReservedCompressibleEnforcementKey) {
		logger.V(2).Info("Enforcing system reserved compressible on cgroup", "cgroupName", nc.SystemReservedCgroupName, "limits", nc.SystemReserved)
		if err := cm.enforceExistingCgroup(logger, nc.SystemReservedCgroupName, nc.SystemReserved, true); err != nil {
			message := fmt.Sprintf("Failed to enforce System Reserved Compressible Cgroup Limits on %q: %v", nc.SystemReservedCgroupName, err)
			cm.recorder.Event(nodeRef, v1.EventTypeWarning, events.FailedNodeAllocatableEnforcement, message)
			return errors.New(message)
		}
		cm.recorder.Eventf(nodeRef, v1.EventTypeNormal, events.SuccessfulNodeAllocatableEnforcement, "Updated limits on system reserved cgroup %v", nc.SystemReservedCgroupName)
	}

	if nc.EnforceNodeAllocatable.Has(kubetypes.KubeReservedCompressibleEnforcementKey) {
		logger.V(2).Info("Enforcing kube reserved compressible on cgroup", "cgroupName", nc.KubeReservedCgroupName, "limits", nc.KubeReserved)
		if err := cm.enforceExistingCgroup(logger, nc.KubeReservedCgroupName, nc.KubeReserved, true); err != nil {
			message := fmt.Sprintf("Failed to enforce Kube Reserved Compressible Cgroup Limits on %q: %v", nc.KubeReservedCgroupName, err)
			cm.recorder.Event(nodeRef, v1.EventTypeWarning, events.FailedNodeAllocatableEnforcement, message)
			return errors.New(message)
		}
		cm.recorder.Eventf(nodeRef, v1.EventTypeNormal, events.SuccessfulNodeAllocatableEnforcement, "Updated limits on kube reserved cgroup %v", nc.KubeReservedCgroupName)
	}
	return nil
}

// enforceExistingCgroup 通过 cgroupManager 更新已有 cgroup 的资源限制。
func (cm *containerManagerImpl) enforceExistingCgroup(logger klog.Logger, cNameStr string, rl v1.ResourceList, compressibleResources bool) error {
	cName := cm.cgroupManager.CgroupName(cNameStr)
	rp := cm.getCgroupConfig(rl, compressibleResources)
	if rp == nil {
		return fmt.Errorf("%q cgroup is not configured properly", cName)
	}

	// 为 kube-reserved/system-reserved 的 cgroup 启用 MemoryQoS，详见
	// https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/2570-memory-qos
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) {
		if rp.Memory != nil {
			if rp.Unified == nil {
				rp.Unified = make(map[string]string)
			}
			rp.Unified[Cgroup2MemoryMin] = strconv.FormatInt(*rp.Memory, 10)
		}
	}

	cgroupConfig := &CgroupConfig{
		Name:               cName,
		ResourceParameters: rp,
	}
	logger.V(4).Info("Enforcing limits on cgroup", "cgroupName", cName, "cpuShares", cgroupConfig.ResourceParameters.CPUShares, "memory", cgroupConfig.ResourceParameters.Memory, "pidsLimit", cgroupConfig.ResourceParameters.PidsLimit)
	// 先校验 cgroup 配置，再执行更新。
	if err := cm.cgroupManager.Validate(cgroupConfig.Name); err != nil {
		return err
	}
	if err := cm.cgroupManager.Update(logger, cgroupConfig); err != nil {
		return err
	}
	return nil
}

// getCgroupConfig 构造用于创建或更新 cgroup 的 ResourceConfig。
func (cm *containerManagerImpl) getCgroupConfig(rl v1.ResourceList, compressibleResourcesOnly bool) *ResourceConfig {
	rc := getCgroupConfigInternal(rl, compressibleResourcesOnly)
	if rc == nil {
		return nil
	}

	// None 策略配合 cgroupv2 + systemd 管理器时，必须让 systemd 感知 cpuset cgroup。
	// systemd 默认不会创建它（未委托给 kubelet，Apply() 请求里也没有），
	// 这会导致 kubelet 反复重启：cgroupfs 中创建的 cpuset 被 systemd 删除，
	// kubelet 又因 cgroup 不符合预期而无法继续。
	// 把 cpuset 委托给 kubelet 需要在 libcontainer 中做额外改造，当前做法已足够。
	// 仅在 None 策略下这样做；Static 策略会自行更新 cpuset，
	// 参见 policy none 的 GetAllocatableCPUs 注释。
	// None 策略下把全部 CPU 写入 cpuset，确保 systemd 创建并管理该 cgroup。
	if cm.cpuManager.GetAllocatableCPUs().IsEmpty() {
		rc.CPUSet = cm.cpuManager.GetAllCPUs()
	}

	return rc
}

// getCgroupConfigInternal 是不依赖 containerManager 的部分，
// 便于单元测试直接调用而无需构造完整的 containerManager。
func getCgroupConfigInternal(rl v1.ResourceList, compressibleResourcesOnly bool) *ResourceConfig {
	// TODO(vishh)：必要时设置 CPU Quota。
	if rl == nil {
		return nil
	}
	var rc ResourceConfig

	setCompressibleResources := func() {
		if q, exists := rl[v1.ResourceCPU]; exists {
			// CPU 以毫核表示，换算为 shares。
			val := MilliCPUToShares(q.MilliValue())
			rc.CPUShares = &val
		}
	}

	// 只处理可压缩资源（CPU），用于 compressible 类型的限制。
	if compressibleResourcesOnly {
		setCompressibleResources()
	} else {
		if q, exists := rl[v1.ResourceMemory]; exists {
			// 内存以字节表示，直接作为限制值。
			val := q.Value()
			rc.Memory = &val
		}

		setCompressibleResources()

		if q, exists := rl[pidlimit.PIDs]; exists {
			val := q.Value()
			rc.PidsLimit = &val
		}
		rc.HugePageLimit = HugePageLimits(rl)
	}
	return &rc
}

// GetNodeAllocatableAbsolute 返回节点可分配资源的绝对值，主要用于强制执行。
// 注意返回值不包含节点上所有可用资源，仅包含可分配的那部分。
func (cm *containerManagerImpl) GetNodeAllocatableAbsolute() v1.ResourceList {
	cm.RLock()
	defer cm.RUnlock()
	return cm.getNodeAllocatableAbsoluteImpl(cm.capacity)
}

// getNodeAllocatableAbsoluteImpl 从容量中扣除 system-reserved 和 kube-reserved。
func (cm *containerManagerImpl) getNodeAllocatableAbsoluteImpl(capacity v1.ResourceList) v1.ResourceList {
	result := make(v1.ResourceList)
	for k, v := range capacity {
		value := v.DeepCopy()
		if cm.NodeConfig.SystemReserved != nil {
			value.Sub(cm.NodeConfig.SystemReserved[k])
		}
		if cm.NodeConfig.KubeReserved != nil {
			value.Sub(cm.NodeConfig.KubeReserved[k])
		}
		if value.Sign() < 0 {
			// 可分配资源为负没有意义，截断为 0。
			value.Set(0)
		}
		result[k] = value
	}
	return result
}

// getNodeAllocatableInternalAbsolute 与 getNodeAllocatableAbsolute 类似，
// 但使用包含内部资源（当前为进程 ID）的容量，仅用于设置顶层 cgroup。
func (cm *containerManagerImpl) getNodeAllocatableInternalAbsolute() v1.ResourceList {
	return cm.getNodeAllocatableAbsoluteImpl(cm.internalCapacity)
}

// GetNodeAllocatableReservation 返回调度时必须在该节点上预留的资源量。
func (cm *containerManagerImpl) GetNodeAllocatableReservation() v1.ResourceList {
	cm.RLock()
	defer cm.RUnlock()
	// 预留量 = system-reserved + kube-reserved + 硬驱逐阈值。
	evictionReservation := hardEvictionReservation(cm.HardEvictionThresholds, cm.capacity)
	result := make(v1.ResourceList)
	for k := range cm.capacity {
		value := resource.NewQuantity(0, resource.DecimalSI)
		if cm.NodeConfig.SystemReserved != nil {
			value.Add(cm.NodeConfig.SystemReserved[k])
		}
		if cm.NodeConfig.KubeReserved != nil {
			value.Add(cm.NodeConfig.KubeReserved[k])
		}
		if evictionReservation != nil {
			value.Add(evictionReservation[k])
		}
		if !value.IsZero() {
			result[k] = *value
		}
	}
	return result
}

// validateNodeAllocatable 校验用户配置的预留量不超过节点容量，
// 配置非法时返回错误。
func (cm *containerManagerImpl) validateNodeAllocatable() error {
	cm.RLock()
	defer cm.RUnlock()
	var errors []string
	nar := cm.GetNodeAllocatableReservation()
	for k, v := range nar {
		value := cm.capacity[k].DeepCopy()
		value.Sub(v)

		if value.Sign() < 0 {
			// 容量减去预留后为负，说明预留量超过了节点容量。
			errors = append(errors, fmt.Sprintf("Resource %q has a reservation of %v but capacity of %v. Expected capacity >= reservation.", k, v, cm.capacity[k]))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("invalid Node Allocatable configuration. %s", strings.Join(errors, " "))
	}
	return nil
}

// 事件使用 ObjectReference：节点对象可能尚未缓存，详见 #42701。
func nodeRefFromNode(nodeName string) *v1.ObjectReference {
	return &v1.ObjectReference{
		APIVersion: "v1",
		Kind:       "Node",
		Name:       nodeName,
		Namespace:  "",
	}
}
