/*
Copyright 2015 The Kubernetes Authors.

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

//go:generate mockery
package cm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	// TODO: Migrate kubelet to either use its own internal objects or client library.
	v1 "k8s.io/api/core/v1"
	"k8s.io/apiserver/pkg/server/healthz"
	internalapi "k8s.io/cri-api/pkg/apis"
	"k8s.io/klog/v2"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/apis/podresources"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/status"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/utils/cpuset"
)

const (
	// 仍在使用的 cgroup v1 用户的告警信息。
	CgroupV1DeprecatedWarning = "cgroup v1 detected. cgroup v1 support is deprecated and will be removed in a future release. Please migrate to cgroup v2. More information at https://git.k8s.io/enhancements/keps/sig-node/5573-remove-cgroup-v1"

	// 内核不支持根 cgroup 的 cpu.stat 但使用了 cgroup v2 时的告警。
	// cpu.stat 自内核 5.8 起才出现在根 cgroup 上，参见
	// https://github.com/torvalds/linux/commit/936f2a70f2077f64fab1dcb3eca71879e82ecd3f
	CgroupV2KernelWarning = "cgroup v2 is being used on a kernel, which doesn't support root `cpu.stat`." +
		"Kubelet will continue, but may experience instability or wrong behavior"
)

// ActivePodsFunc 返回当前节点上活跃的 Pod 列表。
type ActivePodsFunc func() []*v1.Pod

// GetNodeFunc 获取节点对象，用于事件引用和状态同步。
type GetNodeFunc func(context.Context) (*v1.Node, error)

// ContainerManager 管理节点上的容器、cgroup 与设备插件资源。
type ContainerManager interface {
	// Start 执行容器管理器的初始化：确保运行时守护进程位于容器中，
	// 并创建运行非容器化进程的 system 容器。
	Start(context.Context, *v1.Node, ActivePodsFunc, GetNodeFunc, config.SourcesReady, status.PodStatusProvider, internalapi.RuntimeService, bool) error

	// SystemCgroupsLimit 返回分配给系统 cgroup（system 与 Kubernetes 服务）的资源。
	SystemCgroupsLimit() v1.ResourceList

	// GetNodeConfig 返回容器管理器当前使用的 NodeConfig。
	GetNodeConfig() NodeConfig

	// Status 返回内部状态。
	Status() Status

	// NewPodContainerManager 返回 Pod cgroup 管理器；
	// 未启用 QoS cgroup 层级时返回 noop 实现。
	NewPodContainerManager() PodContainerManager

	// GetMountedSubsystems 返回节点上已挂载的 cgroup 子系统。
	GetMountedSubsystems() *CgroupSubsystems

	// GetQOSContainersInfo 返回顶层 QoS cgroup 的名称。
	GetQOSContainersInfo() QOSContainersInfo

	// GetNodeAllocatableReservation 返回调度时必须预留的计算资源量。
	GetNodeAllocatableReservation() v1.ResourceList

	// GetCapacity 返回容器管理器统计到的节点可用计算资源。
	GetCapacity(logger klog.Logger, localStorageCapacityIsolation bool) v1.ResourceList

	// GetDevicePluginResourceCapacity 返回节点容量（设备插件资源总量）、
	// 节点可分配量（设备插件上报的健康资源总量），以及曾注册但当前不活跃的设备插件资源。
	GetDevicePluginResourceCapacity(logger klog.Logger) (v1.ResourceList, v1.ResourceList, []string)

	// UpdateQOSCgroups 以线程安全的方式把顶层 QoS cgroup 更新到期望状态。
	UpdateQOSCgroups(logger klog.Logger) error

	// GetResources 返回填充了设备、挂载和扩展资源环境变量的 RunContainerOptions。
	GetResources(ctx context.Context, pod *v1.Pod, container *v1.Container) (*kubecontainer.RunContainerOptions, error)

	// UpdatePluginResources 为可能的设备插件资源请求调用 Allocate；
	// 成功后按需调整 nodeInfo 的可分配资源，确保不低于 Pod 请求量。
	UpdatePluginResources(*schedulerframework.NodeInfo, *lifecycle.PodAdmitAttributes) error

	// InternalContainerLifecycle 返回内部容器生命周期管理器。
	InternalContainerLifecycle(logger klog.Logger) InternalContainerLifecycle

	// GetPodCgroupRoot 返回包含所有 Pod 的 cgroup 路径。
	GetPodCgroupRoot() string

	// GetPluginRegistrationHandlers 返回插件注册处理器集合；
	// pluginwatcher 通过它统一处理各类插件的注册。
	GetPluginRegistrationHandlers() map[string]cache.PluginHandler

	// GetHealthCheckers 返回所有插件的健康检查器，
	// 它们会接入 systemd watchdog 监控服务健康。
	GetHealthCheckers() []healthz.HealthChecker

	// ShouldResetExtendedResourceCapacity 返回节点重建后是否应清零扩展资源容量。
	ShouldResetExtendedResourceCapacity() bool

	// GetAllocateResourcesPodAdmitHandler 返回负责分配 Pod 资源的准入处理器。
	GetAllocateResourcesPodAdmitHandler(logger klog.Logger) lifecycle.PodAdmitHandler

	// GetNodeAllocatableAbsolute 返回节点可分配资源的绝对值，主要用于强制执行。
	GetNodeAllocatableAbsolute() v1.ResourceList

	// PrepareDynamicResources 准备 Pod 的动态资源。
	PrepareDynamicResources(context.Context, *v1.Pod) error

	// UnprepareDynamicResources 释放 Pod 的动态资源。
	UnprepareDynamicResources(context.Context, *v1.Pod) error

	// PodMightNeedToUnprepareResources 判断指定 UID 的 Pod 是否可能需要释放资源。
	PodMightNeedToUnprepareResources(UID types.UID) bool

	// UpdateAllocatedResourcesStatus 更新 Pod 已分配资源的状态。
	UpdateAllocatedResourcesStatus(logger klog.Logger, pod *v1.Pod, status *v1.PodStatus)

	// Updates 返回设备状态变化时接收更新的 channel。
	Updates() <-chan resourceupdates.Update

	// PodHasExclusiveCPUs 在 Pod 中至少有一个 sidecar 或应用容器分配了独占 CPU 时返回 true。
	PodHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod) bool

	// ContainerHasExclusiveCPUs 判断 Pod 中的指定容器是否分配了独占 CPU。
	ContainerHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod, container *v1.Container) bool

	// 实现 PodResources Provider API。
	podresources.CPUsProvider
	podresources.DevicesProvider
	podresources.MemoryProvider
	podresources.DynamicResourcesProvider
}

// cpuAllocationReader 抽象独占 CPU 查询，便于测试替换实现。
type cpuAllocationReader interface {
	GetExclusiveCPUs(podUID, containerName string) cpuset.CPUSet
}

// NodeConfig 汇总 kubelet 节点级配置，供 cgroup 与资源管理器使用。
type NodeConfig struct {
	NodeName              types.NodeName
	RuntimeCgroupsName    string
	SystemCgroupsName     string
	KubeletCgroupsName    string
	KubeletOOMScoreAdj    int32
	ContainerRuntime      string
	CgroupsPerQOS         bool
	CgroupRoot            string
	CgroupDriver          string
	KubeletRootDir        string
	ProtectKernelDefaults bool
	NodeAllocatableConfig
	QOSReserved                  map[v1.ResourceName]int64
	CPUManagerPolicy             string
	CPUManagerPolicyOptions      map[string]string
	TopologyManagerScope         string
	CPUManagerReconcilePeriod    time.Duration
	MemoryManagerPolicy          string
	MemoryManagerReservedMemory  []kubeletconfig.MemoryReservation
	MemoryReservationPolicy      kubeletconfig.MemoryReservationPolicy
	MemoryThrottlingFactor       *float64
	PodPidsLimit                 int64
	EnforceCPULimits             bool
	CPUCFSQuotaPeriod            time.Duration
	TopologyManagerPolicy        string
	TopologyManagerPolicyOptions map[string]string
	CgroupVersion                int
}

// NodeAllocatableConfig 描述节点可分配资源的预留与强制策略。
type NodeAllocatableConfig struct {
	KubeReservedCgroupName   string
	SystemReservedCgroupName string
	ReservedSystemCPUs       cpuset.CPUSet
	EnforceNodeAllocatable   sets.Set[string]
	KubeReserved             v1.ResourceList
	SystemReserved           v1.ResourceList
	HardEvictionThresholds   []evictionapi.Threshold
}

type Status struct {
	// 任何未满足的软性要求。
	SoftRequirements error
}

// int64Slice 把 int 切片转换为 int64 切片。
func int64Slice(in []int) []int64 {
	out := make([]int64, len(in))
	for i := range in {
		out[i] = int64(in[i])
	}
	return out
}

// podHasExclusiveCPUs 检查 Pod 的 init 容器和普通容器是否分配了独占 CPU。
func podHasExclusiveCPUs(logger klog.Logger, cr cpuAllocationReader, pod *v1.Pod) bool {
	for _, container := range pod.Spec.InitContainers {
		if containerHasExclusiveCPUs(logger, cr, pod, &container) {
			return true
		}
	}
	for _, container := range pod.Spec.Containers {
		if containerHasExclusiveCPUs(logger, cr, pod, &container) {
			return true
		}
	}
	logger.V(4).Info("Pod contains no container with pinned cpus", "podName", pod.Name)
	return false
}

// containerHasExclusiveCPUs 查询指定容器的独占 CPU 集合是否非空。
func containerHasExclusiveCPUs(logger klog.Logger, cr cpuAllocationReader, pod *v1.Pod, container *v1.Container) bool {
	exclusiveCPUs := cr.GetExclusiveCPUs(string(pod.UID), container.Name)
	if !exclusiveCPUs.IsEmpty() {
		logger.V(4).Info("Container has pinned cpus", "podName", pod.Name, "containerName", container.Name)
		return true
	}
	return false
}

// parsePercentage 把百分比字符串解析为 0-100 的整数。
func parsePercentage(v string) (int64, error) {
	// 必须带百分号，且数值范围在 0 到 100 之间。
	if !strings.HasSuffix(v, "%") {
		return 0, fmt.Errorf("percentage expected, got '%s'", v)
	}
	percentage, err := strconv.ParseInt(strings.TrimRight(v, "%"), 10, 0)
	if err != nil {
		return 0, fmt.Errorf("invalid number in percentage '%s'", v)
	}
	if percentage < 0 || percentage > 100 {
		return 0, fmt.Errorf("percentage must be between 0 and 100")
	}
	return percentage, nil
}

// ParseQOSReserved 解析 --qos-reserved 选项，目前只支持 memory 资源。
func ParseQOSReserved(m map[string]string) (*map[v1.ResourceName]int64, error) {
	reservations := make(map[v1.ResourceName]int64)
	for k, v := range m {
		switch v1.ResourceName(k) {
		// 目前只支持内存资源。
		case v1.ResourceMemory:
			q, err := parsePercentage(v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse percentage %q for %q resource: %w", v, k, err)
			}
			reservations[v1.ResourceName(k)] = q
		default:
			return nil, fmt.Errorf("cannot reserve %q resource", k)
		}
	}
	return &reservations, nil
}

// containerDevicesFromResourceDeviceInstances 把设备管理器内部结构转换为
// PodResources API 的容器设备列表。
func containerDevicesFromResourceDeviceInstances(devs devicemanager.ResourceDeviceInstances) []*podresourcesapi.ContainerDevices {
	var respDevs []*podresourcesapi.ContainerDevices

	for resourceName, resourceDevs := range devs {
		for devID, dev := range resourceDevs {
			topo := dev.GetTopology()
			if topo == nil {
				// 部分设备插件不上报拓扑信息，这是合法的；照常上报设备，由调用方决定如何处理。
				respDevs = append(respDevs, &podresourcesapi.ContainerDevices{
					ResourceName: resourceName,
					DeviceIds:    []string{devID},
				})
				continue
			}

			for _, node := range topo.GetNodes() {
				respDevs = append(respDevs, &podresourcesapi.ContainerDevices{
					ResourceName: resourceName,
					DeviceIds:    []string{devID},
					Topology: &podresourcesapi.TopologyInfo{
						Nodes: []*podresourcesapi.NUMANode{
							{
								ID: node.GetID(),
							},
						},
					},
				})
			}
		}
	}

	return respDevs
}
