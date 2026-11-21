/*
Copyright 2018 The Kubernetes Authors.

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

package nodestatus

import (
	"context"
	"fmt"
	"math"
	"net"
	goruntime "runtime"
	"strings"
	"sync"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	cloudproviderapi "k8s.io/cloud-provider/api"
	"k8s.io/component-base/version"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"

	"k8s.io/klog/v2"
)

const (
	// MaxNamesPerImageInNodeStatus 是节点状态中每个镜像
	// 最多记录的镜像名称数量。
	MaxNamesPerImageInNodeStatus = 5
)

// Setter 就地修改 Node，失败时返回错误。
// 注意：返回错误前可能已部分修改节点。
type Setter func(ctx context.Context, node *v1.Node) error

// 确保重启事件只上报一次。
var rebootEvent sync.Once

// NodeAddress 返回一个更新节点地址信息的 Setter。
func NodeAddress(nodeIPs []net.IP, // 通常为 Kubelet.nodeIPs
	validateNodeIPFunc func(net.IP) error, // 通常为 Kubelet.nodeIPValidator
	hostname string, // 通常为 Kubelet.hostname
	externalCloudProvider bool, // 通常为 Kubelet.externalCloudProvider
	resolveAddressFunc func(net.IP) (net.IP, error), // 通常为 k8s.io/apimachinery/pkg/util/net.ResolveBindAddress
) Setter {
	var nodeIP, secondaryNodeIP net.IP
	if len(nodeIPs) > 0 {
		nodeIP = nodeIPs[0]
	}
	// 首个 nodeIP 决定首选 IP 族；未指定时默认 IPv4。
	preferIPv4 := nodeIP == nil || nodeIP.To4() != nil
	isPreferredIPFamily := func(ip net.IP) bool { return (ip.To4() != nil) == preferIPv4 }
	nodeIPSpecified := nodeIP != nil && !nodeIP.IsUnspecified()

	if len(nodeIPs) > 1 {
		secondaryNodeIP = nodeIPs[1]
	}
	secondaryNodeIPSpecified := secondaryNodeIP != nil && !secondaryNodeIP.IsUnspecified()

	return func(ctx context.Context, node *v1.Node) error {
		logger := klog.FromContext(ctx)
		if nodeIPSpecified {
			if err := validateNodeIPFunc(nodeIP); err != nil {
				return fmt.Errorf("failed to validate nodeIP: %v", err)
			}
			logger.V(4).Info("Using node IP", "IP", nodeIP.String())
		}
		if secondaryNodeIPSpecified {
			if err := validateNodeIPFunc(secondaryNodeIP); err != nil {
				return fmt.Errorf("failed to validate secondaryNodeIP: %v", err)
			}
			logger.V(4).Info("Using secondary node IP", "IP", secondaryNodeIP.String())
		}

		if externalCloudProvider && nodeIPSpecified {
			// 外置云厂商模式下，把 nodeIP 写入节点注解。
			//
			// 没有云控制器时不写该注解：此类集群不会迁移到外置 CCM。
			if node.ObjectMeta.Annotations == nil {
				node.ObjectMeta.Annotations = make(map[string]string)
			}
			annotation := nodeIP.String()
			if secondaryNodeIPSpecified {
				annotation += "," + secondaryNodeIP.String()
			}
			node.ObjectMeta.Annotations[cloudproviderapi.AnnotationAlphaProvidedIPAddr] = annotation
		} else if node.ObjectMeta.Annotations != nil {
			// 不再使用云厂商或不再覆盖节点 IP 时，清除过期注解。
			delete(node.ObjectMeta.Annotations, cloudproviderapi.AnnotationAlphaProvidedIPAddr)
		}

		if externalCloudProvider {
			// 若 --cloud-provider=external 且地址已由云厂商设置，则直接返回，
			// 因为云厂商设置的地址优先级更高。
			// 否则先用命令行指定的节点 IP，稍后由云厂商覆盖；
			// 这能缓解树外 provider 的大量引导问题。
			if len(node.Status.Addresses) > 0 {
				return nil
			}
			// 未设置 nodeIP 时等待外置云厂商写入节点地址。
			// 若 nodeIP 是通配地址 0.0.0.0 或 ::，则用对应 IP 族默认网关的 IP 引导节点，
			// 直到树外 provider 稍后覆盖它。
			// 否则直接使用这些地址，前提是安装者/管理员事先知道外部云厂商会使用相同地址，
			// 从而避免下面链接中描述的问题。
			// 我们已经通过 AnnotationAlphaProvidedIPAddr 注解向外部云厂商提示了地址。
			if nodeIP == nil {
				node.Status.Addresses = []v1.NodeAddress{
					{Type: v1.NodeHostName, Address: hostname},
				}
				return nil
			}
		}
		if nodeIPSpecified && secondaryNodeIPSpecified {
			node.Status.Addresses = []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: nodeIP.String()},
				{Type: v1.NodeInternalIP, Address: secondaryNodeIP.String()},
				{Type: v1.NodeHostName, Address: hostname},
			}
		} else {
			var ipAddr net.IP
			var err error

			// 1) nodeIP 已设置且不是通配地址时直接使用；
			// 2) HostnameOverride 本身是 IP 时使用它；
			// 3) 通过 DNS 按节点名解析 IP；
			// 4) 从默认网关所在网卡获取 IP。
			//
			// 步骤 3、4 中优先选择与 nodeIP 同族的地址；
			// 例如 nodeIP 为 "::" 时优先 IPv6。
			if nodeIPSpecified {
				ipAddr = nodeIP
			} else if addr := netutils.ParseIPSloppy(hostname); addr != nil {
				ipAddr = addr
			} else {
				var addrs []net.IP
				addrs, _ = net.LookupIP(node.Name)
				for _, addr := range addrs {
					if err = validateNodeIPFunc(addr); err == nil {
						if isPreferredIPFamily(addr) {
							ipAddr = addr
							break
						} else if ipAddr == nil {
							ipAddr = addr
						}
					}
				}

				if ipAddr == nil {
					ipAddr, err = resolveAddressFunc(nodeIP)
				}
			}

			if ipAddr == nil {
				// 所有途径都失败，无法确定节点 IP，直接报错。
				return fmt.Errorf("can't get ip address of node %s. error: %v", node.Name, err)
			}
			node.Status.Addresses = []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: ipAddr.String()},
				{Type: v1.NodeHostName, Address: hostname},
			}
		}
		return nil
	}
}

// MachineInfo 返回一个更新节点机器信息（容量、NodeInfo 等）的 Setter。
func MachineInfo(nodeName string,
	maxPods int,
	podsPerCore int,
	machineInfoFunc func() (*cadvisorapi.MachineInfo, error), // 通常为 Kubelet.GetCachedMachineInfo
	capacityFunc func(logger klog.Logger, localStorageCapacityIsolation bool) v1.ResourceList, // 通常为 Kubelet.containerManager.GetCapacity
	devicePluginResourceCapacityFunc func(logger klog.Logger) (v1.ResourceList, v1.ResourceList, []string), // 通常为 Kubelet.containerManager.GetDevicePluginResourceCapacity
	nodeAllocatableReservationFunc func() v1.ResourceList, // 通常为 Kubelet.containerManager.GetNodeAllocatableReservation
	recordEventFunc func(eventType, event, message string), // 通常为 Kubelet.recordEvent
	runningInUserNSFunc func() *bool, // 通常为 Kubelet.runningInUserNS
	localStorageCapacityIsolation bool,
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		logger := klog.FromContext(ctx)
		// 注意：不要盲目覆盖 capacity，以免丢失 opaque 资源等外部声明的容量。
		if node.Status.Capacity == nil {
			node.Status.Capacity = v1.ResourceList{}
		}

		var devicePluginAllocatable v1.ResourceList
		var devicePluginCapacity v1.ResourceList
		var removedDevicePlugins []string

		// TODO: Post NotReady if we cannot get MachineInfo from cAdvisor. This needs to start
		// cAdvisor locally, e.g. for test-cmd.sh, and in integration test.
		info, err := machineInfoFunc()
		if err != nil {
			node.Status.Capacity[v1.ResourceCPU] = *resource.NewMilliQuantity(0, resource.DecimalSI)
			node.Status.Capacity[v1.ResourceMemory] = resource.MustParse("0Gi")
			node.Status.Capacity[v1.ResourcePods] = *resource.NewQuantity(int64(maxPods), resource.DecimalSI)
			logger.Error(err, "Error getting machine info")
		} else {
			node.Status.NodeInfo.MachineID = info.MachineID
			node.Status.NodeInfo.SystemUUID = info.SystemUUID

			for rName, rCap := range cadvisor.CapacityFromMachineInfo(info) {
				node.Status.Capacity[rName] = rCap
			}

			// podsPerCore 为正时 Pod 容量取 min(核数*podsPerCore, maxPods)。
			if podsPerCore > 0 {
				node.Status.Capacity[v1.ResourcePods] = *resource.NewQuantity(
					int64(math.Min(float64(info.NumCores*podsPerCore), float64(maxPods))), resource.DecimalSI)
			} else {
				node.Status.Capacity[v1.ResourcePods] = *resource.NewQuantity(
					int64(maxPods), resource.DecimalSI)
			}

			if node.Status.NodeInfo.BootID != "" &&
				node.Status.NodeInfo.BootID != info.BootID {
				// TODO: This requires a transaction, either both node status is updated
				// and event is recorded or neither should happen, see issue #6055.
				//
				// 只上报一次重启事件：recordEventFunc 会把事件入队，可能产生大量重复重启事件。
				rebootEvent.Do(func() {
					recordEventFunc(v1.EventTypeWarning, events.NodeRebooted,
						fmt.Sprintf("Node %s has been rebooted, boot id: %s", nodeName, info.BootID))
				})
			}
			node.Status.NodeInfo.BootID = info.BootID

			// TODO: all the node resources should use ContainerManager.GetCapacity instead of deriving the
			// capacity for every node status request
			initialCapacity := capacityFunc(logger, localStorageCapacityIsolation)
			if initialCapacity != nil {
				if v, exists := initialCapacity[v1.ResourceEphemeralStorage]; exists {
					node.Status.Capacity[v1.ResourceEphemeralStorage] = v
				}
			}

			devicePluginCapacity, devicePluginAllocatable, removedDevicePlugins = devicePluginResourceCapacityFunc(logger)
			for k, v := range devicePluginCapacity {
				if old, ok := node.Status.Capacity[k]; !ok || old.Value() != v.Value() {
					logger.V(2).Info("Updated capacity for device plugin", "plugin", k, "capacity", v.Value())
				}
				node.Status.Capacity[k] = v
			}

			for _, removedResource := range removedDevicePlugins {
				logger.V(2).Info("Set capacity for removed resource to 0 on device removal", "device", removedResource)
				// 设备插件移除后把容量置 0 而不是删掉字段：
				// 这样既表明该资源仍由设备插件管理，
				// 又保留“曾经注册过”的信息。
				//
				// 这也是区分设备插件资源与集群级资源所必需的：
				// 后者在节点状态中通常不出现。
				node.Status.Capacity[v1.ResourceName(removedResource)] = *resource.NewQuantity(int64(0), resource.DecimalSI)
			}

			if utilfeature.DefaultFeatureGate.Enabled(features.NodeSwap) && info.SwapCapacity != 0 {
				node.Status.NodeInfo.Swap = &v1.NodeSwapStatus{
					Capacity: ptr.To(int64(info.SwapCapacity)),
				}
			}

			if utilfeature.DefaultFeatureGate.Enabled(features.KubeletInUserNamespace) {
				node.Status.NodeInfo.RunningInUserNamespace = runningInUserNSFunc()
			}
		}

		// 设置 Allocatable。
		if node.Status.Allocatable == nil {
			node.Status.Allocatable = make(v1.ResourceList)
		}
		// 从 Allocatable 中移除 Capacity 里已不存在的扩展资源。
		for k := range node.Status.Allocatable {
			_, found := node.Status.Capacity[k]
			if !found && v1helper.IsExtendedResourceName(k) {
				delete(node.Status.Allocatable, k)
			}
		}
		allocatableReservation := nodeAllocatableReservationFunc()
		for k, v := range node.Status.Capacity {
			value := v.DeepCopy()
			if res, exists := allocatableReservation[k]; exists {
				value.Sub(res)
			}
			if value.Sign() < 0 {
				// Allocatable 为负数没有意义，归零。
				value.Set(0)
			}
			node.Status.Allocatable[k] = value
		}

		for k, v := range devicePluginAllocatable {
			if old, ok := node.Status.Allocatable[k]; !ok || old.Value() != v.Value() {
				logger.V(2).Info("Updated allocatable", "device", k, "allocatable", v.Value())
			}
			node.Status.Allocatable[k] = v
		}
		// 巨页预留需要从可分配内存中扣除，否则内存会被重复计算。
		for k, v := range node.Status.Capacity {
			if v1helper.IsHugePageResourceName(k) {
				allocatableMemory := node.Status.Allocatable[v1.ResourceMemory]
				value := v.DeepCopy()
				allocatableMemory.Sub(value)
				if allocatableMemory.Sign() < 0 {
					// Allocatable 为负数没有意义，归零。
					allocatableMemory.Set(0)
				}
				node.Status.Allocatable[v1.ResourceMemory] = allocatableMemory
			}
		}
		return nil
	}
}

// VersionInfo 返回一个更新节点版本信息的 Setter。
func VersionInfo(versionInfoFunc func() (*cadvisorapi.VersionInfo, error), // 通常为 Kubelet.cadvisor.VersionInfo
	runtimeTypeFunc func() string, // 通常为 Kubelet.containerRuntime.Type
	runtimeVersionFunc func(ctx context.Context) (kubecontainer.Version, error), // 通常为 Kubelet.containerRuntime.Version
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		verinfo, err := versionInfoFunc()
		if err != nil {
			return fmt.Errorf("error getting version info: %v", err)
		}

		node.Status.NodeInfo.KernelVersion = verinfo.KernelVersion
		node.Status.NodeInfo.OSImage = verinfo.ContainerOsVersion

		runtimeVersion := "Unknown"
		if runtimeVer, err := runtimeVersionFunc(ctx); err == nil {
			runtimeVersion = runtimeVer.String()
		}
		node.Status.NodeInfo.ContainerRuntimeVersion = fmt.Sprintf("%s://%s", runtimeTypeFunc(), runtimeVersion)

		node.Status.NodeInfo.KubeletVersion = version.Get().String()

		if utilfeature.DefaultFeatureGate.Enabled(features.DisableNodeKubeProxyVersion) {
			// 该字段已弃用；若此前被设置过，应主动清空。
			node.Status.NodeInfo.KubeProxyVersion = ""
		} else {
			node.Status.NodeInfo.KubeProxyVersion = version.Get().String()
		}

		return nil
	}
}

// DaemonEndpoints 返回一个更新节点 DaemonEndpoints 的 Setter。
func DaemonEndpoints(daemonEndpoints *v1.NodeDaemonEndpoints) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		node.Status.DaemonEndpoints = *daemonEndpoints
		return nil
	}
}

// Images 返回一个更新节点镜像列表的 Setter。
// imageListFunc 应返回按镜像大小降序排列的列表。
// nodeStatusMaxImages 为 -1 时不限制上报数量。
func Images(nodeStatusMaxImages int32,
	imageListFunc func() ([]kubecontainer.Image, error), // 通常为 Kubelet.imageManager.GetImageList
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		// 更新节点的镜像列表。
		var imagesOnNode []v1.ContainerImage
		containerImages, err := imageListFunc()
		if err != nil {
			node.Status.Images = imagesOnNode
			return fmt.Errorf("error getting image list: %v", err)
		}
		// imageListFunc 已按镜像大小降序返回，这里只需截断。
		if int(nodeStatusMaxImages) > -1 &&
			int(nodeStatusMaxImages) < len(containerImages) {
			containerImages = containerImages[0:nodeStatusMaxImages]
		}

		for _, image := range containerImages {
			// 先复制名称切片，避免修改镜像列表元素本身。
			names := append([]string{}, image.RepoDigests...)
			names = append(names, image.RepoTags...)
			// 每个镜像最多上报 MaxNamesPerImageInNodeStatus 个名称。
			if len(names) > MaxNamesPerImageInNodeStatus {
				names = names[0:MaxNamesPerImageInNodeStatus]
			}
			imagesOnNode = append(imagesOnNode, v1.ContainerImage{
				Names:     names,
				SizeBytes: image.Size,
			})
		}

		node.Status.Images = imagesOnNode
		return nil
	}
}

// GoRuntime 返回一个写入 GOOS/GOARCH 的 Setter。
func GoRuntime() Setter {
	return func(ctx context.Context, node *v1.Node) error {
		node.Status.NodeInfo.OperatingSystem = goruntime.GOOS
		node.Status.NodeInfo.Architecture = goruntime.GOARCH
		return nil
	}
}

// NodeFeatures 返回一个写入节点特性（NodeFeatures）的 Setter。
func NodeFeatures(featuresGetter func() *kubecontainer.RuntimeFeatures) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		if !utilfeature.DefaultFeatureGate.Enabled(features.SupplementalGroupsPolicy) {
			return nil
		}
		features := featuresGetter()
		if features == nil {
			return nil
		}
		node.Status.Features = &v1.NodeFeatures{
			SupplementalGroupsPolicy: &features.SupplementalGroupsPolicy,
		}
		return nil
	}
}

// RuntimeHandlers 返回一个写入节点 runtime handler 信息的 Setter。
func RuntimeHandlers(fn func() []kubecontainer.RuntimeHandler) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		if !utilfeature.DefaultFeatureGate.Enabled(features.RecursiveReadOnlyMounts) && !utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) {
			return nil
		}
		handlers := fn()
		node.Status.RuntimeHandlers = make([]v1.NodeRuntimeHandler, len(handlers))
		for i, h := range handlers {
			node.Status.RuntimeHandlers[i] = v1.NodeRuntimeHandler{
				Name: h.Name,
				Features: &v1.NodeRuntimeHandlerFeatures{
					RecursiveReadOnlyMounts: &h.SupportsRecursiveReadOnlyMounts,
					UserNamespaces:          &h.SupportsUserNamespaces,
				},
			}
		}
		return nil
	}
}

// ReadyCondition 返回一个更新节点 NodeReady 条件的 Setter。
func ReadyCondition(
	nowFunc func() time.Time, // 通常为 Kubelet.clock.Now
	runtimeErrorsFunc func() error, // 通常为 Kubelet.runtimeState.runtimeErrors
	networkErrorsFunc func() error, // 通常为 Kubelet.runtimeState.networkErrors
	storageErrorsFunc func() error, // 通常为 Kubelet.runtimeState.storageErrors
	cmStatusFunc func() cm.Status, // 通常为 Kubelet.containerManager.Status
	nodeShutdownManagerErrorsFunc func() error, // 通常为 kubelet.shutdownManager.errors.
	recordEventFunc func(logger klog.Logger, eventType, event string), // 通常为 Kubelet.recordNodeStatusEvent
	localStorageCapacityIsolation bool,
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		logger := klog.FromContext(ctx)
		// NOTE(aaronlevy): NodeReady 必须位于节点条件列表的最后。
		// 这是为了兼容 kubelet 与 master 版本不一致的情况。
		currentTime := metav1.NewTime(nowFunc())
		newNodeReadyCondition := v1.NodeCondition{
			Type:              v1.NodeReady,
			Status:            v1.ConditionTrue,
			Reason:            "KubeletReady",
			Message:           "kubelet is posting ready status",
			LastHeartbeatTime: currentTime,
		}
		errs := []error{runtimeErrorsFunc(), networkErrorsFunc(), storageErrorsFunc(), nodeShutdownManagerErrorsFunc()}
		requiredCapacities := []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory, v1.ResourcePods}
		if localStorageCapacityIsolation {
			requiredCapacities = append(requiredCapacities, v1.ResourceEphemeralStorage)
		}
		missingCapacities := []string{}
		for _, resource := range requiredCapacities {
			if _, found := node.Status.Capacity[resource]; !found {
				missingCapacities = append(missingCapacities, string(resource))
			}
		}
		if len(missingCapacities) > 0 {
			errs = append(errs, fmt.Errorf("missing node capacity for resources: %s", strings.Join(missingCapacities, ", ")))
		}
		if aggregatedErr := errors.NewAggregate(errs); aggregatedErr != nil {
			newNodeReadyCondition = v1.NodeCondition{
				Type:              v1.NodeReady,
				Status:            v1.ConditionFalse,
				Reason:            "KubeletNotReady",
				Message:           aggregatedErr.Error(),
				LastHeartbeatTime: currentTime,
			}
		}

		// 记录容器管理器未满足的软性要求。
		status := cmStatusFunc()
		if status.SoftRequirements != nil {
			newNodeReadyCondition.Message = fmt.Sprintf("%s. WARNING: %s", newNodeReadyCondition.Message, status.SoftRequirements.Error())
		}

		// 状态未翻转时保留原 LastTransitionTime，避免无意义地刷新翻转时间。
		readyConditionUpdated := false
		needToRecordEvent := false
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == v1.NodeReady {
				if node.Status.Conditions[i].Status == newNodeReadyCondition.Status {
					newNodeReadyCondition.LastTransitionTime = node.Status.Conditions[i].LastTransitionTime
				} else {
					newNodeReadyCondition.LastTransitionTime = currentTime
					needToRecordEvent = true
				}
				node.Status.Conditions[i] = newNodeReadyCondition
				readyConditionUpdated = true
				break
			}
		}
		if !readyConditionUpdated {
			newNodeReadyCondition.LastTransitionTime = currentTime
			node.Status.Conditions = append(node.Status.Conditions, newNodeReadyCondition)
		}
		if needToRecordEvent {
			if newNodeReadyCondition.Status == v1.ConditionTrue {
				recordEventFunc(logger, v1.EventTypeNormal, events.NodeReady)
			} else {
				recordEventFunc(logger, v1.EventTypeNormal, events.NodeNotReady)
				logger := klog.FromContext(ctx)
				logger.Info("Node became not ready", "node", klog.KObj(node), "condition", newNodeReadyCondition)
			}
		}
		return nil
	}
}

// MemoryPressureCondition 返回一个更新 NodeMemoryPressure 条件的 Setter。
func MemoryPressureCondition(nowFunc func() time.Time, // 通常为 Kubelet.clock.Now
	pressureFunc func() bool, // 通常为 Kubelet.evictionManager.IsUnderMemoryPressure
	recordEventFunc func(logger klog.Logger, eventType, event string), // 通常为 Kubelet.recordNodeStatusEvent
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		logger := klog.FromContext(ctx)
		currentTime := metav1.NewTime(nowFunc())
		var condition *v1.NodeCondition

		// 若 NodeMemoryPressure 条件已存在，直接复用其指针进行更新。
		// 复用既有条件的指针，后续修改才能直接反映到 node.Status.Conditions。
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == v1.NodeMemoryPressure {
				condition = &node.Status.Conditions[i]
			}
		}

		newCondition := false
		// 条件不存在时先创建一个占位条件。
		if condition == nil {
			condition = &v1.NodeCondition{
				Type:   v1.NodeMemoryPressure,
				Status: v1.ConditionUnknown,
			}
			// 此处不能立即 append：condition 是局部副本，
			// 立即 append 后，下面做的修改不会反映到切片中。
			newCondition = true
		}

		// 更新心跳时间。
		condition.LastHeartbeatTime = currentTime

		// 下面逻辑同时处理“新建条件”和“已有条件”两种情况。
		// 新建条件的初始状态为 v1.ConditionUnknown，
		// 因此必然与 True/False 都不相等，
		// 从而会根据当前是否受压进入对应的分支。
		if pressureFunc() {
			if condition.Status != v1.ConditionTrue {
				condition.Status = v1.ConditionTrue
				condition.Reason = "KubeletHasInsufficientMemory"
				condition.Message = "kubelet has insufficient memory available"
				condition.LastTransitionTime = currentTime
				recordEventFunc(logger, v1.EventTypeNormal, "NodeHasInsufficientMemory")
			}
		} else if condition.Status != v1.ConditionFalse {
			condition.Status = v1.ConditionFalse
			condition.Reason = "KubeletHasSufficientMemory"
			condition.Message = "kubelet has sufficient memory available"
			condition.LastTransitionTime = currentTime
			recordEventFunc(logger, v1.EventTypeNormal, "NodeHasSufficientMemory")
		}

		if newCondition {
			node.Status.Conditions = append(node.Status.Conditions, *condition)
		}
		return nil
	}
}

// PIDPressureCondition 返回一个更新 NodePIDPressure 条件的 Setter。
func PIDPressureCondition(nowFunc func() time.Time, // 通常为 Kubelet.clock.Now
	pressureFunc func() bool, // 通常为 Kubelet.evictionManager.IsUnderPIDPressure
	recordEventFunc func(logger klog.Logger, eventType, event string), // 通常为 Kubelet.recordNodeStatusEvent
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		logger := klog.FromContext(ctx)
		currentTime := metav1.NewTime(nowFunc())
		var condition *v1.NodeCondition

		// 若 NodePIDPressure 条件已存在，直接复用其指针进行更新。
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == v1.NodePIDPressure {
				condition = &node.Status.Conditions[i]
			}
		}

		newCondition := false
		// 条件不存在时先创建一个占位条件。
		if condition == nil {
			condition = &v1.NodeCondition{
				Type:   v1.NodePIDPressure,
				Status: v1.ConditionUnknown,
			}
			// 此处不能立即 append：condition 是局部副本，
			// 立即 append 后，下面做的修改不会反映到切片中。
			newCondition = true
		}

		// 更新心跳时间。
		condition.LastHeartbeatTime = currentTime

		// 下面逻辑同时处理“新建条件”和“已有条件”两种情况。
		// 新建条件的初始状态为 v1.ConditionUnknown，
		// 因此必然与 True/False 都不相等，
		// 从而会根据当前是否受压进入对应的分支。
		if pressureFunc() {
			if condition.Status != v1.ConditionTrue {
				condition.Status = v1.ConditionTrue
				condition.Reason = "KubeletHasInsufficientPID"
				condition.Message = "kubelet has insufficient PID available"
				condition.LastTransitionTime = currentTime
				recordEventFunc(logger, v1.EventTypeNormal, "NodeHasInsufficientPID")
			}
		} else if condition.Status != v1.ConditionFalse {
			condition.Status = v1.ConditionFalse
			condition.Reason = "KubeletHasSufficientPID"
			condition.Message = "kubelet has sufficient PID available"
			condition.LastTransitionTime = currentTime
			recordEventFunc(logger, v1.EventTypeNormal, "NodeHasSufficientPID")
		}

		if newCondition {
			node.Status.Conditions = append(node.Status.Conditions, *condition)
		}
		return nil
	}
}

// DiskPressureCondition 返回一个更新 NodeDiskPressure 条件的 Setter。
func DiskPressureCondition(nowFunc func() time.Time, // 通常为 Kubelet.clock.Now
	pressureFunc func() bool, // 通常为 Kubelet.evictionManager.IsUnderDiskPressure
	recordEventFunc func(logger klog.Logger, eventType, event string), // 通常为 Kubelet.recordNodeStatusEvent
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		logger := klog.FromContext(ctx)
		currentTime := metav1.NewTime(nowFunc())
		var condition *v1.NodeCondition

		// 若 NodeDiskPressure 条件已存在，直接复用其指针进行更新。
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == v1.NodeDiskPressure {
				condition = &node.Status.Conditions[i]
			}
		}

		newCondition := false
		// 条件不存在时先创建一个占位条件。
		if condition == nil {
			condition = &v1.NodeCondition{
				Type:   v1.NodeDiskPressure,
				Status: v1.ConditionUnknown,
			}
			// 此处不能立即 append：condition 是局部副本，
			// 立即 append 后，下面做的修改不会反映到切片中。
			newCondition = true
		}

		// 更新心跳时间。
		condition.LastHeartbeatTime = currentTime

		// 下面逻辑同时处理“新建条件”和“已有条件”两种情况。
		// 新建条件的初始状态为 v1.ConditionUnknown，
		// 因此必然与 True/False 都不相等，
		// 从而会根据当前是否受压进入对应的分支。
		if pressureFunc() {
			if condition.Status != v1.ConditionTrue {
				condition.Status = v1.ConditionTrue
				condition.Reason = "KubeletHasDiskPressure"
				condition.Message = "kubelet has disk pressure"
				condition.LastTransitionTime = currentTime
				recordEventFunc(logger, v1.EventTypeNormal, "NodeHasDiskPressure")
			}
		} else if condition.Status != v1.ConditionFalse {
			condition.Status = v1.ConditionFalse
			condition.Reason = "KubeletHasNoDiskPressure"
			condition.Message = "kubelet has no disk pressure"
			condition.LastTransitionTime = currentTime
			recordEventFunc(logger, v1.EventTypeNormal, "NodeHasNoDiskPressure")
		}

		if newCondition {
			node.Status.Conditions = append(node.Status.Conditions, *condition)
		}
		return nil
	}
}

// VolumesInUse 返回一个更新节点在用卷列表的 Setter。
func VolumesInUse(syncedFunc func() bool, // 通常为 Kubelet.volumeManager.ReconcilerStatesHasBeenSynced
	volumesInUseFunc func() []v1.UniqueVolumeName, // 通常为 Kubelet.volumeManager.GetVolumesInUse
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		// 必须等 reconciler 完成状态同步后再写 VolumesInUse，否则可能写入不完整状态。
		if syncedFunc() {
			node.Status.VolumesInUse = volumesInUseFunc()
		}
		return nil
	}
}
