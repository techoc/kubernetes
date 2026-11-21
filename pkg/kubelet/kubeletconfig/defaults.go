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

package kubeletconfig

// 定义 kubelet 配置使用的合理默认值。
const (
	DefaultKubeletPodsDirName                = "pods"
	DefaultKubeletVolumesDirName             = "volumes"
	DefaultKubeletVolumeSubpathsDirName      = "volume-subpaths"
	DefaultKubeletVolumeDevicesDirName       = "volumeDevices"
	DefaultKubeletPluginsDirName             = "plugins"
	DefaultKubeletPluginsRegistrationDirName = "plugins_registry"
	DefaultKubeletContainersDirName          = "containers"
	DefaultKubeletPluginContainersDirName    = "plugin-containers"
	DefaultKubeletPodResourcesDirName        = "pod-resources"
	DefaultKubeletPodsAPIDirName             = "pods-api"
	// 以下为各插件目录使用的 SELinux 标签，插件与容器共享同一类型标签。
	KubeletPluginsDirSELinuxLabel       = "system_u:object_r:container_file_t:s0"
	KubeletContainersSharedSELinuxLabel = "system_u:object_r:container_file_t:s0"
	DefaultKubeletCheckpointsDirName    = "checkpoints"
	// 每个 Pod 默认分配的用户命名空间 ID 数量（65536 个 UID/GID）。
	DefaultKubeletUserNamespacesIDsPerPod = 65536
)
