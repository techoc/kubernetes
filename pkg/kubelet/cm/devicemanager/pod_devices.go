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

package devicemanager

import (
	"maps"
	"sync"

	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/util/sets"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/checkpoint"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// deviceAllocateInfo 记录某个容器在某个资源上的设备分配结果。
type deviceAllocateInfo struct {
	// deviceIds 保存按 NUMA 节点划分的、分配给该容器的设备 ID。
	deviceIds checkpoint.DevicesPerNUMA
	// allocResp 缓存设备插件 Allocate 调用的响应，供容器启动时复用。
	allocResp *pluginapi.ContainerAllocateResponse
}

// resourceAllocateInfo 以资源名为键，保存该资源下各容器的设备分配信息。
type resourceAllocateInfo map[string]deviceAllocateInfo

// containerDevices 以容器名为键，保存该容器的资源分配信息。
type containerDevices map[string]resourceAllocateInfo

// podDevices 以 Pod UID 为键，保存各 Pod 的设备分配缓存；内嵌 RWMutex 保护并发访问。
type podDevices struct {
	sync.RWMutex
	devs map[string]containerDevices
}

// newPodDevices 返回一个 podDevices 对象，自带读写锁，
// 内部 map 以 Pod UID 为键、以容器设备信息 containerDevices 为值。
func newPodDevices() *podDevices {
	return &podDevices{devs: make(map[string]containerDevices)}
}

// pods 返回当前被跟踪的所有 Pod UID 集合。
func (pdev *podDevices) pods() sets.Set[string] {
	pdev.RLock()
	defer pdev.RUnlock()
	ret := sets.New[string]()
	for k := range pdev.devs {
		ret.Insert(k)
	}
	return ret
}

// size 返回当前被跟踪的 Pod 数量。
func (pdev *podDevices) size() int {
	pdev.RLock()
	defer pdev.RUnlock()
	return len(pdev.devs)
}

// hasPod 判断指定 Pod 是否已被跟踪。
func (pdev *podDevices) hasPod(podUID string) bool {
	pdev.RLock()
	defer pdev.RUnlock()
	_, podExists := pdev.devs[podUID]
	return podExists
}

// insert 记录某个容器在某个资源上的设备分配信息，必要时逐层创建嵌套 map。
func (pdev *podDevices) insert(podUID, contName, resource string, devices checkpoint.DevicesPerNUMA, resp *pluginapi.ContainerAllocateResponse) {
	pdev.Lock()
	defer pdev.Unlock()
	if _, podExists := pdev.devs[podUID]; !podExists {
		pdev.devs[podUID] = make(containerDevices)
	}
	if _, contExists := pdev.devs[podUID][contName]; !contExists {
		pdev.devs[podUID][contName] = make(resourceAllocateInfo)
	}
	pdev.devs[podUID][contName][resource] = deviceAllocateInfo{
		deviceIds: devices,
		allocResp: resp,
	}
}

// delete 批量删除指定 Pod 的全部设备分配缓存。
func (pdev *podDevices) delete(pods []string) {
	pdev.Lock()
	defer pdev.Unlock()
	for _, uid := range pods {
		delete(pdev.devs, uid)
	}
}

// podDevices 返回指定 Pod 在指定资源上分配到的设备 ID 列表。
// 若不存在对应的 <podUID, resource> 缓存，则返回空集合。
func (pdev *podDevices) podDevices(podUID, resource string) sets.Set[string] {
	pdev.RLock()
	defer pdev.RUnlock()

	ret := sets.New[string]()
	for contName := range pdev.devs[podUID] {
		ret = ret.Union(pdev.containerDevices(podUID, contName, resource))
	}
	return ret
}

// containerDevices 返回指定容器在指定资源上分配到的设备 ID 列表。
// 若不存在对应的 <podUID, contName, resource> 缓存，则返回 nil。
func (pdev *podDevices) containerDevices(podUID, contName, resource string) sets.Set[string] {
	pdev.RLock()
	defer pdev.RUnlock()
	if _, podExists := pdev.devs[podUID]; !podExists {
		return nil
	}
	if _, contExists := pdev.devs[podUID][contName]; !contExists {
		return nil
	}
	devs, resourceExists := pdev.devs[podUID][contName][resource]
	if !resourceExists {
		return nil
	}
	return devs.deviceIds.Devices()
}

// addContainerAllocatedResources 将指定 <podUID, contName> 分配到的设备资源并入 allocatedResources。
func (pdev *podDevices) addContainerAllocatedResources(podUID, contName string, allocatedResources map[string]sets.Set[string]) {
	pdev.RLock()
	defer pdev.RUnlock()
	containers, exists := pdev.devs[podUID]
	if !exists {
		return
	}
	resources, exists := containers[contName]
	if !exists {
		return
	}
	for resource, devices := range resources {
		allocatedResources[resource] = allocatedResources[resource].Union(devices.deviceIds.Devices())
	}
}

// removeContainerAllocatedResources 从 allocatedResources 中移除指定 <podUID, contName> 分配到的设备资源。
func (pdev *podDevices) removeContainerAllocatedResources(podUID, contName string, allocatedResources map[string]sets.Set[string]) {
	pdev.RLock()
	defer pdev.RUnlock()
	containers, exists := pdev.devs[podUID]
	if !exists {
		return
	}
	resources, exists := containers[contName]
	if !exists {
		return
	}
	for resource, devices := range resources {
		allocatedResources[resource] = allocatedResources[resource].Difference(devices.deviceIds.Devices())
	}
}

// devices 返回所有被跟踪 Pod 已分配的设备，按资源名分组。
func (pdev *podDevices) devices() map[string]sets.Set[string] {
	ret := make(map[string]sets.Set[string])
	pdev.RLock()
	defer pdev.RUnlock()
	for _, containerDevices := range pdev.devs {
		for _, resources := range containerDevices {
			for resource, devices := range resources {
				if _, exists := ret[resource]; !exists {
					ret[resource] = sets.New[string]()
				}
				if devices.allocResp != nil {
					ret[resource] = ret[resource].Union(devices.deviceIds.Devices())
				}
			}
		}
	}
	return ret
}

// getPodAndContainerForDevice 返回指定资源下某设备所属的 podUID 与容器名。
// 查询必须按 resourceName 限定范围：设备 ID 仅在单个资源内唯一，
// 不同插件可能暴露 ID 相同的设备。
func (pdev *podDevices) getPodAndContainerForDevice(resourceName, deviceID string) (string, string) {
	pdev.RLock()
	defer pdev.RUnlock()
	for podUID, containerDevices := range pdev.devs {
		for containerName, resources := range containerDevices {
			if devices, ok := resources[resourceName]; ok {
				if devices.deviceIds.Devices().Has(deviceID) {
					return podUID, containerName
				}
			}
		}
	}
	return "", ""
}

// toCheckpointData 将内存中的设备分配状态转换为可持久化的 checkpoint 数据。
func (pdev *podDevices) toCheckpointData(logger klog.Logger) []checkpoint.PodDevicesEntry {
	var data []checkpoint.PodDevicesEntry
	pdev.RLock()
	defer pdev.RUnlock()
	for podUID, containerDevices := range pdev.devs {
		for conName, resources := range containerDevices {
			for resource, devices := range resources {
				// 缺少 Allocate 响应时无法恢复分配细节，只能跳过该条目并记录错误。
				if devices.allocResp == nil {
					logger.Error(nil, "Can't marshal allocResp, allocation response is missing", "podUID", podUID, "containerName", conName, "resourceName", resource)
					continue
				}

				allocResp, err := proto.Marshal(devices.allocResp)
				if err != nil {
					logger.Error(err, "Can't marshal allocResp", "podUID", podUID, "containerName", conName, "resourceName", resource)
					continue
				}
				data = append(data, checkpoint.PodDevicesEntry{
					PodUID:        podUID,
					ContainerName: conName,
					ResourceName:  resource,
					DeviceIDs:     devices.deviceIds,
					AllocResp:     allocResp})
			}
		}
	}
	return data
}

// fromCheckpointData 依据 checkpoint 数据重建 podDevices 中的设备分配状态。
func (pdev *podDevices) fromCheckpointData(logger klog.Logger, data []checkpoint.PodDevicesEntry) {
	for _, entry := range data {
		logger.V(2).Info("Get checkpoint entry",
			"podUID", entry.PodUID, "containerName", entry.ContainerName,
			"resourceName", entry.ResourceName, "deviceIDs", entry.DeviceIDs, "allocated", entry.AllocResp)
		allocResp := &pluginapi.ContainerAllocateResponse{}
		err := proto.Unmarshal(entry.AllocResp, allocResp)
		if err != nil {
			logger.Error(err, "Can't unmarshal allocResp", "podUID", entry.PodUID, "containerName", entry.ContainerName, "resourceName", entry.ResourceName)
			continue
		}
		pdev.insert(entry.PodUID, entry.ContainerName, entry.ResourceName, entry.DeviceIDs, allocResp)
	}
}

// deviceRunContainerOptions 汇总容器已分配设备对应的运行时配置（环境变量、设备、
// 挂载、注解与 CDI 设备），供容器创建时使用。
func (pdev *podDevices) deviceRunContainerOptions(logger klog.Logger, podUID, contName string) *DeviceRunContainerOptions {
	pdev.RLock()
	defer pdev.RUnlock()

	containers, exists := pdev.devs[podUID]
	if !exists {
		return nil
	}
	resources, exists := containers[contName]
	if !exists {
		return nil
	}
	opts := &DeviceRunContainerOptions{}
	// 用 map 检测重复设置：同一容器可能对应多个资源，各资源的响应需要合并去重。
	devsMap := make(map[string]string)
	mountsMap := make(map[string]string)
	envsMap := make(map[string]string)
	annotationsMap := make(map[string]string)
	// 记录该容器请求的全部 CDI 设备，用于跨资源去重。
	allCDIDevices := sets.New[string]()
	// 遍历该容器所有已缓存资源的 Allocate 响应。
	for _, devices := range resources {
		resp := devices.allocResp
		// 每个 Allocate 响应包含以下内容：
		// 环境变量
		// 挂载点
		// 设备文件
		// 容器注解
		// CDI 设备 ID
		// 这些内容均以“资源 + 容器”为粒度。
		// 更新 RunContainerOptions.Envs。
		for k, v := range resp.Envs {
			if e, ok := envsMap[k]; ok {
				logger.V(4).Info("Skip existing env", "envKey", k, "envValue", v)
				if e != v {
					logger.Error(nil, "Environment variable has conflicting setting", "envKey", k, "expected", v, "got", e)
				}
				continue
			}
			logger.V(4).Info("Add env", "envKey", k, "envValue", v)
			envsMap[k] = v
			opts.Envs = append(opts.Envs, kubecontainer.EnvVar{Name: k, Value: v})
		}

		// 更新 RunContainerOptions.Devices。
		for _, dev := range resp.Devices {
			if d, ok := devsMap[dev.ContainerPath]; ok {
				logger.V(4).Info("Skip existing device", "containerPath", dev.ContainerPath, "hostPath", dev.HostPath)
				if d != dev.HostPath {
					logger.Error(nil, "Container device has conflicting mapping host devices",
						"containerPath", dev.ContainerPath, "got", d, "expected", dev.HostPath)
				}
				continue
			}
			logger.V(4).Info("Add device", "containerPath", dev.ContainerPath, "hostPath", dev.HostPath)
			devsMap[dev.ContainerPath] = dev.HostPath
			opts.Devices = append(opts.Devices, kubecontainer.DeviceInfo{
				PathOnHost:      dev.HostPath,
				PathInContainer: dev.ContainerPath,
				Permissions:     dev.Permissions,
			})
		}

		// 更新 RunContainerOptions.Mounts。
		for _, mount := range resp.Mounts {
			if m, ok := mountsMap[mount.ContainerPath]; ok {
				logger.V(4).Info("Skip existing mount", "containerPath", mount.ContainerPath, "hostPath", mount.HostPath)
				if m != mount.HostPath {
					logger.Error(nil, "Container mount has conflicting mapping host mounts",
						"containerPath", mount.ContainerPath, "conflictingPath", m, "hostPath", mount.HostPath)
				}
				continue
			}
			logger.V(4).Info("Add mount", "containerPath", mount.ContainerPath, "hostPath", mount.HostPath)
			mountsMap[mount.ContainerPath] = mount.HostPath
			opts.Mounts = append(opts.Mounts, kubecontainer.Mount{
				Name:          mount.ContainerPath,
				ContainerPath: mount.ContainerPath,
				HostPath:      mount.HostPath,
				ReadOnly:      mount.ReadOnly,
				// TODO: 该字段后续可能需要纳入设备插件 API。
				SELinuxRelabel: false,
			})
		}

		// 更新注解。
		for k, v := range resp.Annotations {
			if e, ok := annotationsMap[k]; ok {
				logger.V(4).Info("Skip existing annotation", "annotationKey", k, "annotationValue", v)
				if e != v {
					logger.Error(nil, "Annotation has conflicting setting", "annotationKey", k, "expected", e, "got", v)
				}
				continue
			}
			logger.V(4).Info("Add annotation", "annotationKey", k, "annotationValue", v)
			annotationsMap[k] = v
			opts.Annotations = append(opts.Annotations, kubecontainer.Annotation{Name: k, Value: v})
		}

		// 更新 CDI 设备。
		cdiDevices := getCDIDeviceInfo(logger, resp, allCDIDevices)
		opts.CDIDevices = append(opts.CDIDevices, cdiDevices...)
	}

	return opts
}

// getCDIDeviceInfo 从 Allocate 响应中提取 CDI 设备，并跳过已知设备以实现跨资源去重。
func getCDIDeviceInfo(logger klog.Logger, resp *pluginapi.ContainerAllocateResponse, knownCDIDevices sets.Set[string]) []kubecontainer.CDIDevice {
	var cdiDevices []kubecontainer.CDIDevice
	for _, cdiDevice := range resp.CdiDevices {
		if knownCDIDevices.Has(cdiDevice.Name) {
			logger.V(4).Info("Skip existing CDI Device", "name", cdiDevice.Name)
			continue
		}
		logger.V(4).Info("Add CDI device", "name", cdiDevice.Name)
		knownCDIDevices.Insert(cdiDevice.Name)

		device := kubecontainer.CDIDevice{
			Name: cdiDevice.Name,
		}
		cdiDevices = append(cdiDevices, device)
	}

	return cdiDevices
}

// getContainerDevices 返回指定容器在所有资源上分配到的设备实例，并附带 NUMA 拓扑信息。
func (pdev *podDevices) getContainerDevices(podUID, contName string) ResourceDeviceInstances {
	pdev.RLock()
	defer pdev.RUnlock()

	if _, podExists := pdev.devs[podUID]; !podExists {
		return nil
	}
	if _, contExists := pdev.devs[podUID][contName]; !contExists {
		return nil
	}
	resDev := NewResourceDeviceInstances()
	for resource, allocateInfo := range pdev.devs[podUID][contName] {
		if len(allocateInfo.deviceIds) == 0 {
			continue
		}
		devicePluginMap := make(map[string]*pluginapi.Device)
		for numaid, devlist := range allocateInfo.deviceIds {
			for _, devID := range devlist {
				var topology *pluginapi.TopologyInfo
				if numaid != nodeWithoutTopology {
					NUMANodes := []*pluginapi.NUMANode{{ID: numaid}}
					if pDev, ok := devicePluginMap[devID]; ok && pDev.Topology != nil {
						if nodes := pDev.Topology.GetNodes(); nodes != nil {
							NUMANodes = append(NUMANodes, nodes...)
						}
					}

					// 此处只关心拓扑信息，设备的 ID 与 Healthy 字段无关紧要。
					topology = &pluginapi.TopologyInfo{Nodes: NUMANodes}
				}
				devicePluginMap[devID] = &pluginapi.Device{
					Topology: topology,
				}
			}
		}
		resDev[resource] = devicePluginMap
	}
	return resDev
}

// DeviceInstances 是设备名到插件设备数据的映射。
type DeviceInstances map[string]*pluginapi.Device

// ResourceDeviceInstances 是资源名到 DeviceInstances 的映射。
type ResourceDeviceInstances map[string]DeviceInstances

// NewResourceDeviceInstances 返回一个新的空 ResourceDeviceInstances。
func NewResourceDeviceInstances() ResourceDeviceInstances {
	return make(ResourceDeviceInstances)
}

// Clone 返回 ResourceDeviceInstances 的浅拷贝：外层 map 与各资源的 map 均被复制，
// 但设备对象本身仍共享引用。
func (rdev ResourceDeviceInstances) Clone() ResourceDeviceInstances {
	clone := NewResourceDeviceInstances()
	for resourceName, resourceDevs := range rdev {
		clone[resourceName] = maps.Clone(resourceDevs)
	}
	return clone
}

// Filter 接收以 map[string]sets.Set[string] 表达的条件集合，
// 返回仅包含匹配条件设备的新的 ResourceDeviceInstances。
func (rdev ResourceDeviceInstances) Filter(cond map[string]sets.Set[string]) ResourceDeviceInstances {
	filtered := NewResourceDeviceInstances()
	for resourceName, filterIDs := range cond {
		if _, exists := rdev[resourceName]; !exists {
			continue
		}
		filtered[resourceName] = DeviceInstances{}
		for instanceID, instance := range rdev[resourceName] {
			if filterIDs.Has(instanceID) {
				filtered[resourceName][instanceID] = instance
			}
		}
	}
	return filtered
}
