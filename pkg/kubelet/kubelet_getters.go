/*
Copyright 2016 The Kubernetes Authors.

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

package kubelet

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilpath "k8s.io/utils/path"
	utilstrings "k8s.io/utils/strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig"
	kubelettypes "k8s.io/kubernetes/pkg/kubelet/types"
	utilnode "k8s.io/kubernetes/pkg/util/node"
	"k8s.io/kubernetes/pkg/volume/csi"
)

// getRootDir 返回 kubelet 可用于存放数据的目录的完整路径。
// 这些函数便于把接口传递给其他模块，使它们无需持有整个 kubelet
// 实例也能知道数据应写往何处。
func (kl *Kubelet) getRootDir() string {
	return kl.rootDirectory
}

// getPodLogsDir 返回 kubelet 用于存放 Pod 日志文件的目录完整路径。
// 若配置文件中未另行指定，默认为 /var/log/pods。
func (kl *Kubelet) getPodLogsDir() string {
	return kl.podLogsDirectory
}

// getPodsDir 返回用于创建各 Pod 目录的父目录完整路径。
func (kl *Kubelet) getPodsDir() string {
	return filepath.Join(kl.getRootDir(), kubeletconfig.DefaultKubeletPodsDirName)
}

// getPluginsDir 返回用于创建各插件目录的父目录完整路径。
// 插件可将需要持久化的数据存放在这些目录中；每个插件应在
// 该目录下创建以自身名称命名的子目录。
func (kl *Kubelet) getPluginsDir() string {
	return filepath.Join(kl.getRootDir(), kubeletconfig.DefaultKubeletPluginsDirName)
}

// getPluginsRegistrationDir 返回用于放置插件 socket 以完成注册的目录完整路径。
// 有关插件注册的更多信息可参见 pluginwatcher 模块。
func (kl *Kubelet) getPluginsRegistrationDir() string {
	return filepath.Join(kl.getRootDir(), kubeletconfig.DefaultKubeletPluginsRegistrationDirName)
}

// getPluginDir 返回指定插件的数据目录名。
// 插件可将需要持久化的数据存放在这些目录中。
// 每个 Pod 的插件数据目录参见 getPodPluginDir。
func (kl *Kubelet) getPluginDir(pluginName string) string {
	return filepath.Join(kl.getPluginsDir(), pluginName)
}

// getCheckpointsDir 返回用于存放 checkpoint 的数据目录名。
// checkpoint 可保存在该目录中供后续使用。
func (kl *Kubelet) getCheckpointsDir() string {
	return filepath.Join(kl.getRootDir(), kubeletconfig.DefaultKubeletCheckpointsDirName)
}

// getVolumeDevicePluginsDir 返回用于创建各插件目录的父目录完整路径。
// 插件可将需要持久化的数据存放在这些目录中；每个插件应在
// 该目录下创建以自身名称命名的子目录。
func (kl *Kubelet) getVolumeDevicePluginsDir() string {
	return filepath.Join(kl.getRootDir(), kubeletconfig.DefaultKubeletPluginsDirName)
}

// getVolumeDevicePluginDir 返回指定插件的数据目录名。
// 插件可将需要持久化的数据存放在这些目录中。
// 每个 Pod 的卷设备插件数据目录参见 getVolumeDevicePluginsDir。
func (kl *Kubelet) getVolumeDevicePluginDir(pluginName string) string {
	return filepath.Join(kl.getVolumeDevicePluginsDir(), pluginName, kubeletconfig.DefaultKubeletVolumeDevicesDirName)
}

// GetPodDir 返回指定 Pod 的每 Pod 数据目录完整路径。
// 若 Pod 不存在，该目录可能不存在。
func (kl *Kubelet) GetPodDir(podUID types.UID) string {
	return kl.getPodDir(podUID)
}

// ListPodsFromDisk 获取在磁盘上拥有数据目录的 Pod 列表。
func (kl *Kubelet) ListPodsFromDisk() ([]types.UID, error) {
	return kl.listPodsFromDisk()
}

// HandlerSupportsUserNamespaces 检查指定的 runtime handler 是否支持用户命名空间。
func (kl *Kubelet) HandlerSupportsUserNamespaces(rtHandler string) (bool, error) {
	rtHandlers := kl.runtimeState.runtimeHandlers()
	if len(rtHandlers) == 0 {
		// 运行时较旧、不支持该消息时，返回的切片为空。
		return false, nil
	}
	for _, h := range rtHandlers {
		if h.Name == rtHandler {
			return h.SupportsUserNamespaces, nil
		}
	}
	return false, fmt.Errorf("the handler %q is not known", rtHandler)
}

// GetKubeletMappings 获取为 Kubelet 分配的额外 ID。
func (kl *Kubelet) GetKubeletMappings(logger klog.Logger, idsPerPod uint32) (uint32, uint32, error) {
	return kl.getKubeletMappings(logger, idsPerPod)
}

// GetMaxPods 返回 kubelet 允许运行的最大 Pod 数量。
func (kl *Kubelet) GetMaxPods() int {
	return kl.maxPods
}

// GetUserNamespacesIDsPerPod 返回每个 Pod 分配的用户命名空间 ID 数量；
// 未配置或配置为 0 时使用默认值。
func (kl *Kubelet) GetUserNamespacesIDsPerPod() uint32 {
	userNs := kl.kubeletConfiguration.UserNamespaces
	if userNs == nil {
		return kubeletconfig.DefaultKubeletUserNamespacesIDsPerPod
	}
	idsPerPod := userNs.IDsPerPod
	if idsPerPod == nil || *idsPerPod == 0 {
		return kubeletconfig.DefaultKubeletUserNamespacesIDsPerPod
	}
	// 该值已校验为不超过 MaxUint32，因此可以安全地丢弃高位。
	return uint32(*idsPerPod)
}

// getPodDir 返回具有给定 UID 的 Pod 的每 Pod 目录完整路径。
func (kl *Kubelet) getPodDir(podUID types.UID) string {
	return filepath.Join(kl.getPodsDir(), string(podUID))
}

// getPodVolumesSubpathsDir 返回指定 Pod 的每 Pod subpath 目录完整路径，
// 各 subpath 卷会在该目录下创建。若 Pod 不存在或未指定 subpath，该目录可能不存在。
func (kl *Kubelet) getPodVolumeSubpathsDir(podUID types.UID) string {
	return filepath.Join(kl.getPodDir(podUID), kubeletconfig.DefaultKubeletVolumeSubpathsDirName)
}

// getPodVolumesDir 返回指定 Pod 的每 Pod 数据目录完整路径，
// 各卷会在该目录下创建。若 Pod 不存在，该目录可能不存在。
func (kl *Kubelet) getPodVolumesDir(podUID types.UID) string {
	return filepath.Join(kl.getPodDir(podUID), kubeletconfig.DefaultKubeletVolumesDirName)
}

// getPodVolumeDir 返回指定 Pod 在指定插件下某个具名卷所对应目录的完整路径。
// 若 Pod 不存在，该目录可能不存在。
func (kl *Kubelet) getPodVolumeDir(podUID types.UID, pluginName string, volumeName string) string {
	return filepath.Join(kl.getPodVolumesDir(podUID), pluginName, volumeName)
}

// getPodVolumeDevicesDir 返回指定 Pod 的每 Pod 数据目录完整路径，
// 各卷会在该目录下创建。若 Pod 不存在，该目录可能不存在。
func (kl *Kubelet) getPodVolumeDevicesDir(podUID types.UID) string {
	return filepath.Join(kl.getPodDir(podUID), kubeletconfig.DefaultKubeletVolumeDevicesDirName)
}

// getPodVolumeDeviceDir 返回指定 Pod 下某个具名插件所对应目录的完整路径。
// 若 Pod 不存在，该目录可能不存在。
func (kl *Kubelet) getPodVolumeDeviceDir(podUID types.UID, pluginName string) string {
	return filepath.Join(kl.getPodVolumeDevicesDir(podUID), pluginName)
}

// getPodPluginsDir 返回指定 Pod 的每 Pod 数据目录完整路径，
// 插件可在该目录下存放该 Pod 的数据。若 Pod 不存在，该目录可能不存在。
func (kl *Kubelet) getPodPluginsDir(podUID types.UID) string {
	return filepath.Join(kl.getPodDir(podUID), kubeletconfig.DefaultKubeletPluginsDirName)
}

// getPodPluginDir 返回给定 Pod UID 下指定插件的数据目录名。
// 插件可将需要持久化的数据存放在这些目录中。
// 非每 Pod 的插件数据目录参见 getPluginDir。
func (kl *Kubelet) getPodPluginDir(podUID types.UID, pluginName string) string {
	return filepath.Join(kl.getPodPluginsDir(podUID), pluginName)
}

// getPodContainerDir 返回指定 Pod 的每 Pod 数据目录完整路径，
// 容器数据保存在该目录下。若 Pod 或容器不存在，该目录可能不存在。
func (kl *Kubelet) getPodContainerDir(podUID types.UID, ctrName string) string {
	return filepath.Join(kl.getPodDir(podUID), kubeletconfig.DefaultKubeletContainersDirName, ctrName)
}

// getPodResourcesDir 返回包含 Pod 资源 socket 的目录完整路径。
func (kl *Kubelet) getPodResourcesDir() string {
	return filepath.Join(kl.getRootDir(), kubeletconfig.DefaultKubeletPodResourcesDirName)
}

// getPodsAPIDir 返回包含 Pods API socket 的目录完整路径。
func (kl *Kubelet) getPodsAPIDir() string {
	return filepath.Join(kl.getRootDir(), kubeletconfig.DefaultKubeletPodsAPIDirName)
}

// GetPods 返回绑定到该 kubelet 的所有 Pod 及其 spec，以及 mirror Pod。
func (kl *Kubelet) GetPods() []*v1.Pod {
	pods := kl.podManager.GetPods()
	for i, p := range pods {
		// Pod 缓存不会更新静态 Pod 的状态。
		// TODO(tallclair): GetPods() 的大多数调用方不需要 Pod 状态。我们可以将其参数化，
		// 或仅向确实需要状态的调用方注入状态（也许只有 /pods HTTP 处理器需要）。
		if kubelettypes.IsStaticPod(p) {
			if status, ok := kl.statusManager.GetPodStatus(p.UID); ok {
				// 不要修改缓存
				p = p.DeepCopy()
				p.Status = status
				pods[i] = p
			}
		}
	}
	return pods
}

// GetRunningPods 通过查看容器运行时缓存返回 kubelet 上正在运行的所有 Pod。
// 该函数会把 kubecontainer.Pod 转换为 v1.Pod，因此只有两者共有的字段
// 才具有实际意义。
func (kl *Kubelet) GetRunningPods(ctx context.Context) ([]*v1.Pod, error) {
	pods, err := kl.runtimeCache.GetPods(ctx)
	if err != nil {
		return nil, err
	}

	apiPods := make([]*v1.Pod, 0, len(pods))
	for _, pod := range pods {
		apiPods = append(apiPods, pod.ToAPIPod())
	}
	return apiPods, nil
}

// GetPodByFullName 获取具有给定“完整”名称的 Pod，
// 该名称同时包含命名空间信息；同时返回是否找到该 Pod。
func (kl *Kubelet) GetPodByFullName(podFullName string) (*v1.Pod, bool) {
	return kl.podManager.GetPodByFullName(podFullName)
}

// GetPodByName 返回第一个匹配给定命名空间和名称的 Pod，
// 同时返回是否找到该 Pod。
func (kl *Kubelet) GetPodByName(namespace, name string) (*v1.Pod, bool) {
	return kl.podManager.GetPodByName(namespace, name)
}

// GetPodByCgroupfs 返回映射到指定 cgroup 的 Pod，
// 同时返回是否找到该 Pod。
func (kl *Kubelet) GetPodByCgroupfs(cgroupfs string) (*v1.Pod, bool) {
	pcm := kl.containerManager.NewPodContainerManager()
	if result, podUID := pcm.IsPodCgroup(cgroupfs); result {
		return kl.podManager.GetPodByUID(podUID)
	}
	return nil, false
}

// getRuntime 返回 kubelet 当前使用的 Runtime 实现。
func (kl *Kubelet) getRuntime() kubecontainer.Runtime {
	return kl.containerRuntime
}

// GetNode 返回本 Kubelet 所配置节点名的节点信息。
func (kl *Kubelet) GetNode(ctx context.Context) (*v1.Node, error) {
	if kl.kubeClient == nil {
		return kl.initialNode(ctx)
	}
	return kl.nodeLister.Get(string(kl.nodeName))
}

// getNodeAnyWay() 必须返回 RunGeneralPredicates() 所需的 *v1.Node。
// 获取 *v1.Node 的方式如下：
// 返回本节点的 nodeInfo；若出错或处于独立运行模式，则返回一个
// 表示无 Pod、零容量且带默认标签的节点的构造 nodeInfo。
func (kl *Kubelet) getNodeAnyWay(ctx context.Context) (*v1.Node, error) {
	if n, err := kl.GetNode(ctx); err == nil {
		return n, nil
	}
	// GetNode 失败时退回初始节点信息，保证通用谓词计算仍可继续。
	return kl.initialNode(ctx)
}

// GetNodeConfig 返回容器管理器的节点配置。
func (kl *Kubelet) GetNodeConfig() cm.NodeConfig {
	return kl.containerManager.GetNodeConfig()
}

// GetPodCgroupRoot 返回包含所有 Pod 的 cgroup 的字面 cgroupfs 值。
func (kl *Kubelet) GetPodCgroupRoot() string {
	return kl.containerManager.GetPodCgroupRoot()
}

// getHostIPsAnyWay 尝试从 kubelet 的 nodeInfo 或 initialNode 返回主机 IP。
func (kl *Kubelet) getHostIPsAnyWay(ctx context.Context) ([]net.IP, error) {
	node, err := kl.getNodeAnyWay(ctx)
	if err != nil {
		return nil, err
	}
	return utilnode.GetNodeHostIPs(node)
}

// GetExtraSupplementalGroupsForPod 返回该 Pod 的额外补充组列表。
// 这些额外补充组来自 Pod 所依赖的持久卷上的注解。
func (kl *Kubelet) GetExtraSupplementalGroupsForPod(pod *v1.Pod) []int64 {
	return kl.volumeManager.GetExtraSupplementalGroupsForPod(pod)
}

// getPodVolumePathListFromDisk 通过从磁盘读取给定 Pod 的卷目录，
// 返回卷路径列表。
func (kl *Kubelet) getPodVolumePathListFromDisk(logger klog.Logger, podUID types.UID) ([]string, error) {
	volumes := []string{}
	podVolDir := kl.getPodVolumesDir(podUID)

	if pathExists, pathErr := mount.PathExists(podVolDir); pathErr != nil {
		return volumes, fmt.Errorf("error checking if path %q exists: %v", podVolDir, pathErr)
	} else if !pathExists {
		logger.V(6).Info("Path does not exist", "path", podVolDir)
		return volumes, nil
	}

	volumePluginDirs, err := os.ReadDir(podVolDir)
	if err != nil {
		logger.Error(err, "Could not read directory", "path", podVolDir)
		return volumes, err
	}
	for _, volumePluginDir := range volumePluginDirs {
		volumePluginName := volumePluginDir.Name()
		volumePluginPath := filepath.Join(podVolDir, volumePluginName)
		volumeDirs, err := utilpath.ReadDirNoStat(volumePluginPath)
		if err != nil {
			return volumes, fmt.Errorf("could not read directory %s: %v", volumePluginPath, err)
		}
		unescapePluginName := utilstrings.UnescapeQualifiedName(volumePluginName)

		if unescapePluginName != csi.CSIPluginName {
			for _, volumeDir := range volumeDirs {
				volumes = append(volumes, filepath.Join(volumePluginPath, volumeDir))
			}
		} else {
			// CSI 卷的挂载路径会多出一层 "/mount" 子路径，因此若该挂载路径
			// 存在，也需要把它加入列表。
			for _, volumeDir := range volumeDirs {
				path := filepath.Join(volumePluginPath, volumeDir)
				csimountpath := csi.GetCSIMounterPath(path)
				if pathExists, _ := mount.PathExists(csimountpath); pathExists {
					volumes = append(volumes, csimountpath)
				}
			}
		}
	}
	return volumes, nil
}

// getMountedVolumePathListFromDisk 从磁盘上的卷路径中筛选出实际已挂载的路径。
func (kl *Kubelet) getMountedVolumePathListFromDisk(logger klog.Logger, podUID types.UID) ([]string, error) {
	mountedVolumes := []string{}
	volumePaths, err := kl.getPodVolumePathListFromDisk(logger, podUID)
	if err != nil {
		return mountedVolumes, err
	}
	// 仅用 IsLikelyNotMountPoint 判断可能无法覆盖所有情况。对于 CSI 卷，
	// 无论是 1) 不挂载还是 2) 在 rootfs 内做 bind mount，挂载检查都无法按预期工作。
	// 我们计划在删除 Pod 前移除这个挂载点检查条件，因为它并不可靠，且不同类型的卷
	// 条件可能不同。但这需要一个可靠的方法来清理未使用的卷目录，以避免 Pod 删除期间
	// 出现问题。参见 issue #74650 的讨论。
	for _, volumePath := range volumePaths {
		isNotMount, err := kl.mounter.IsLikelyNotMountPoint(volumePath)
		if err != nil {
			return mountedVolumes, fmt.Errorf("fail to check mount point %q: %v", volumePath, err)
		}
		if !isNotMount {
			mountedVolumes = append(mountedVolumes, volumePath)
		}
	}
	return mountedVolumes, nil
}

// getPodVolumeSubpathListFromDisk 通过从磁盘读取给定 Pod 的 subpath 目录，
// 返回卷 subpath 路径列表。
func (kl *Kubelet) getPodVolumeSubpathListFromDisk(logger klog.Logger, podUID types.UID) ([]string, error) {
	volumes := []string{}
	podSubpathsDir := kl.getPodVolumeSubpathsDir(podUID)

	if pathExists, pathErr := mount.PathExists(podSubpathsDir); pathErr != nil {
		return nil, fmt.Errorf("error checking if path %q exists: %v", podSubpathsDir, pathErr)
	} else if !pathExists {
		// 目录不存在说明没有 subpath，直接返回空列表。
		return volumes, nil
	}

	// 显式遍历 /<volume>/<container name>/<subPathIndex>
	volumePluginDirs, err := os.ReadDir(podSubpathsDir)
	if err != nil {
		logger.Error(err, "Could not read directory", "path", podSubpathsDir)
		return volumes, err
	}
	for _, volumePluginDir := range volumePluginDirs {
		volumePluginName := volumePluginDir.Name()
		volumePluginPath := filepath.Join(podSubpathsDir, volumePluginName)
		containerDirs, err := os.ReadDir(volumePluginPath)
		if err != nil {
			return volumes, fmt.Errorf("could not read directory %s: %v", volumePluginPath, err)
		}
		for _, containerDir := range containerDirs {
			containerName := containerDir.Name()
			containerPath := filepath.Join(volumePluginPath, containerName)
			// 在 subPathIndex 层级改用 ReadDirNoStat，避免对可能无响应的
			// 挂载点执行 stat 操作。
			subPaths, err := utilpath.ReadDirNoStat(containerPath)
			if err != nil {
				return volumes, fmt.Errorf("could not read directory %s: %v", containerPath, err)
			}
			for _, subPathDir := range subPaths {
				volumes = append(volumes, filepath.Join(containerPath, subPathDir))
			}
		}
	}
	return volumes, nil
}

// GetRequestedContainersInfo 返回容器信息。
func (kl *Kubelet) GetRequestedContainersInfo(containerName string, options cadvisorapi.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	return kl.cadvisor.GetRequestedContainersInfo(containerName, options)
}

// GetVersionInfo 返回当前使用的 cAdvisor 版本信息。
func (kl *Kubelet) GetVersionInfo() (*cadvisorapi.VersionInfo, error) {
	return kl.cadvisor.VersionInfo()
}

// GetCachedMachineInfo 返回缓存的机器信息；这里假设机器信息在重启前不会变化。
func (kl *Kubelet) GetCachedMachineInfo() (*cadvisorapi.MachineInfo, error) {
	kl.machineInfoLock.RLock()
	defer kl.machineInfoLock.RUnlock()
	return kl.machineInfo, nil
}

// setCachedMachineInfo 在加锁保护下更新缓存的机器信息。
func (kl *Kubelet) setCachedMachineInfo(info *cadvisorapi.MachineInfo) {
	kl.machineInfoLock.Lock()
	defer kl.machineInfoLock.Unlock()
	kl.machineInfo = info
}

// getLastStableNodeAddresses 返回最近一次观察到的节点地址。
func (kl *Kubelet) getLastObservedNodeAddresses(ctx context.Context) []v1.NodeAddress {
	logger := klog.FromContext(ctx)
	node, err := kl.GetNode(ctx)
	if err != nil || node == nil {
		logger.V(4).Info("fail to obtain node from local cache", "node", kl.nodeName, "error", err)
		return nil
	}
	return node.Status.Addresses
}

// GetAllocatedPods 返回所有活跃 Pod 及其已分配的资源。
func (kl *Kubelet) GetAllocatedPods() ([]*v1.Pod, error) {
	return kl.allocationManager.GetAllocatedPods(), nil
}

// GetAllocatedPodByName 返回指定命名空间和名称的已分配 Pod。
// 若 Pod 不存在或处于非活跃状态，返回 (nil, nil)。
func (kl *Kubelet) GetAllocatedPodByName(namespace, name string) (*v1.Pod, error) {
	pod, found := kl.GetPodByName(namespace, name)
	if !found {
		return nil, nil
	}
	// 过滤掉非活跃 Pod，避免对已终止的 Pod 返回分配信息。
	activePods := kl.filterOutInactivePods([]*v1.Pod{pod})
	if len(activePods) == 0 {
		return nil, nil
	}
	allocatedPod, _ := kl.allocationManager.UpdatePodFromAllocation(pod)
	return allocatedPod, nil
}
