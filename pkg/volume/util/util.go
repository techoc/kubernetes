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

package util

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/securitycontext"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
	"k8s.io/mount-utils"
	"k8s.io/utils/io"
	utilstrings "k8s.io/utils/strings"
)

const (
	// readyFileName 是标记卷已就绪的文件名。
	readyFileName = "ready"

	// ControllerManagedAttachAnnotation 是 Node 对象上的注解键，
	// 表示该节点的挂载/卸载操作应由 attach/detach 控制器管理。
	ControllerManagedAttachAnnotation string = "volumes.kubernetes.io/controller-managed-attach-detach"

	// MountsInGlobalPDPath 是附加在卷插件名后的目录名，
	// 用于在全局 PD 路径下存放卷挂载点。
	MountsInGlobalPDPath = "mounts"

	// VolumeGidAnnotationKey 是 PersistentVolume 对象上指定附加 GID 的注解键。
	VolumeGidAnnotationKey = "pv.beta.kubernetes.io/gid"

	// VolumeDynamicallyCreatedByKey 是动态创建的 PersistentVolume 对象上的注解键。
	VolumeDynamicallyCreatedByKey = "kubernetes.io/createdby"

	// kubernetesPluginPathPrefix 是 Kubernetes 插件挂载路径的前缀。
	kubernetesPluginPathPrefix = "/plugins/kubernetes.io/"
)

// IsReady 检查给定目录下是否存在名为 'ready' 的普通文件，存在则返回 true。
func IsReady(dir string) bool {
	readyFile := filepath.Join(dir, readyFileName)
	s, err := os.Stat(readyFile)
	if err != nil {
		return false
	}

	// 只把普通文件当作就绪标记，避免同名目录被误判。
	if !s.Mode().IsRegular() {
		klog.Errorf("ready-file is not a file: %s", readyFile)
		return false
	}

	return true
}

// SetReady 在给定目录下创建名为 'ready' 的文件，失败时记录错误日志。
func SetReady(dir string) {
	// 先建目录：调用方未必已创建它，否则文件创建会失败。
	if err := os.MkdirAll(dir, 0750); err != nil && !os.IsExist(err) {
		klog.Errorf("Can't mkdir %s: %v", dir, err)
		return
	}

	readyFile := filepath.Join(dir, readyFileName)
	file, err := os.Create(readyFile)
	if err != nil {
		klog.Errorf("Can't touch %s: %v", readyFile, err)
		return
	}
	file.Close()
}

// GetSecretForPV 按名称和命名空间查找 Secret，校验其类型后返回数据映射。
func GetSecretForPV(secretNamespace, secretName, volumePluginName string, kubeClient clientset.Interface) (map[string]string, error) {
	secret := make(map[string]string)
	if kubeClient == nil {
		return secret, fmt.Errorf("cannot get kube client")
	}
	secrets, err := kubeClient.CoreV1().Secrets(secretNamespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		return secret, err
	}
	if secrets.Type != v1.SecretType(volumePluginName) {
		return secret, fmt.Errorf("cannot get secret of type %s", volumePluginName)
	}
	for name, data := range secrets.Data {
		secret[name] = string(data)
	}
	return secret, nil
}

// LoadPodFromFile 从文件读取并解码，返回一个 Pod。
func LoadPodFromFile(filePath string) (*v1.Pod, error) {
	if filePath == "" {
		return nil, fmt.Errorf("file path not specified")
	}
	podDef, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file path %s: %+v", filePath, err)
	}
	if len(podDef) == 0 {
		return nil, fmt.Errorf("file was empty: %s", filePath)
	}
	pod := &v1.Pod{}

	codec := legacyscheme.Codecs.UniversalDecoder()
	if err := apiruntime.DecodeInto(codec, podDef, pod); err != nil {
		return nil, fmt.Errorf("failed decoding file: %v", err)
	}
	return pod, nil
}

// CalculateTimeoutForVolume 计算 Recycler Pod 完成回收操作的超时时间。
// 取 minimumTimeout 与按存储容量（每 Gi）计算的 timeoutIncrement 中的较大者。
func CalculateTimeoutForVolume(minimumTimeout, timeoutIncrement int, pv *v1.PersistentVolume) int64 {
	giQty := resource.MustParse("1Gi")
	pvQty := pv.Spec.Capacity[v1.ResourceStorage]
	giSize := giQty.Value()
	pvSize := pvQty.Value()
	timeout := (pvSize / giSize) * int64(timeoutIncrement)
	if timeout < int64(minimumTimeout) {
		return int64(minimumTimeout)
	}
	return timeout
}

// GetPath 检查 mounter 返回的路径是否为空，为空则返回错误。
func GetPath(mounter volume.Mounter) (string, error) {
	path := mounter.GetPath()
	if path == "" {
		return "", fmt.Errorf("path is empty %s", reflect.TypeOf(mounter).String())
	}
	return path, nil
}

// UnmountViaEmptyDir 将 secret、configmap、git_repo 和 downwardapi 的卸载操作委托给 empty_dir。
func UnmountViaEmptyDir(dir string, host volume.VolumeHost, volName string, volSpec volume.Spec, podUID utypes.UID) error {
	klog.V(3).Infof("Tearing down volume %v for pod %v at %v", volName, podUID, dir)

	// 包装成 EmptyDir，由它完成实际卸载。
	wrapped, err := host.NewWrapperUnmounter(volName, volSpec, podUID)
	if err != nil {
		return err
	}
	return wrapped.TearDownAt(dir)
}

// MountOptionFromSpec 从卷规格中提取挂载选项，并与传入的选项合并。
func MountOptionFromSpec(spec *volume.Spec, options ...string) []string {
	pv := spec.PersistentVolume

	if pv != nil {
		// 优先使用 beta 注解，兼容旧版行为。
		if mo, ok := pv.Annotations[v1.MountOptionAnnotation]; ok {
			moList := strings.Split(mo, ",")
			return JoinMountOptions(moList, options)
		}

		if len(pv.Spec.MountOptions) > 0 {
			return JoinMountOptions(pv.Spec.MountOptions, options)
		}
	}

	return options
}

// JoinMountOptions 合并挂载选项并去重。
func JoinMountOptions(userOptions []string, systemOptions []string) []string {
	allMountOptions := sets.New[string]()

	// 用集合去重，避免同一选项被重复传给挂载命令。
	for _, mountOption := range userOptions {
		if len(mountOption) > 0 {
			allMountOptions.Insert(mountOption)
		}
	}

	for _, mountOption := range systemOptions {
		allMountOptions.Insert(mountOption)
	}
	return sets.List(allMountOptions)
}

// ContainsAccessMode 判断 modes 中是否包含指定的访问模式。
func ContainsAccessMode(modes []v1.PersistentVolumeAccessMode, mode v1.PersistentVolumeAccessMode) bool {
	return slices.Contains(modes, mode)
}

// ContainsAllAccessModes 判断 modes 是否包含所有指定的访问模式。
func ContainsAllAccessModes(indexedModes []v1.PersistentVolumeAccessMode, requestedModes []v1.PersistentVolumeAccessMode) bool {
	for _, mode := range requestedModes {
		if !ContainsAccessMode(indexedModes, mode) {
			return false
		}
	}
	return true
}

// GetWindowsPath 将路径转换为 Windows 风格路径。
func GetWindowsPath(path string) string {
	windowsPath := strings.Replace(path, "/", "\\", -1)
	if strings.HasPrefix(windowsPath, "\\") {
		windowsPath = "c:" + windowsPath
	}
	return windowsPath
}

// GetUniquePodName 返回用于唯一标识 Pod 的标识符。
func GetUniquePodName(pod *v1.Pod) types.UniquePodName {
	return types.UniquePodName(pod.UID)
}

// GetUniqueVolumeName 返回代表卷/插件的唯一名称。
// 调用方需保证 volumeName 能唯一标识该卷背后的设备、目录或路径等。
// 返回的名称可用于唯一引用该卷，例如避免对同一卷重复触发
// 挂载/卸载或 attach/detach 操作。
func GetUniqueVolumeName(pluginName, volumeName string) v1.UniqueVolumeName {
	return v1.UniqueVolumeName(fmt.Sprintf("%s/%s", pluginName, volumeName))
}

// GetUniqueVolumeNameFromSpecWithPod 返回包含 Pod 名的唯一卷名，
// 便于为共享同一卷的不同 Pod 生成不同的名称。
func GetUniqueVolumeNameFromSpecWithPod(
	podName types.UniquePodName, volumePlugin volume.VolumePlugin, volumeSpec *volume.Spec) v1.UniqueVolumeName {
	return v1.UniqueVolumeName(
		fmt.Sprintf("%s/%v-%s", volumePlugin.GetPluginName(), podName, volumeSpec.Name()))
}

// GetUniqueVolumeNameFromSpec 使用给定 VolumePlugin 为卷规格生成唯一名称。
// 该名称可唯一引用卷规格所指向的实际设备、目录或路径等。
// 若插件不支持该卷规格，则返回错误。
func GetUniqueVolumeNameFromSpec(
	volumePlugin volume.VolumePlugin,
	volumeSpec *volume.Spec) (v1.UniqueVolumeName, error) {
	if volumePlugin == nil {
		return "", fmt.Errorf(
			"volumePlugin should not be nil. volumeSpec.Name=%q",
			volumeSpec.Name())
	}

	volumeName, err := volumePlugin.GetVolumeName(volumeSpec)
	if err != nil || volumeName == "" {
		return "", fmt.Errorf(
			"failed to GetVolumeName from volumePlugin for volumeSpec %q err=%v",
			volumeSpec.Name(),
			err)
	}

	return GetUniqueVolumeName(
			volumePlugin.GetPluginName(),
			volumeName),
		nil
}

// IsPodTerminated 判断 Pod 是否已终止。
func IsPodTerminated(pod *v1.Pod, podStatus v1.PodStatus) bool {
	// TODO: kubelet 状态的保证不足以确信可以忽略已删除的 Pod，
	// 即使所有容器都处于 notRunning（kubelet 不保证 waiting 状态时没有在尝试
	// 启动容器）。
	return podStatus.Phase == v1.PodFailed || podStatus.Phase == v1.PodSucceeded || (pod.DeletionTimestamp != nil && notRunning(podStatus.InitContainerStatuses) && notRunning(podStatus.ContainerStatuses) && notRunning(podStatus.EphemeralContainerStatuses))
}

// notRunning 在所有状态均为 terminated 或 waiting，或状态列表为空时返回 true。
func notRunning(statuses []v1.ContainerStatus) bool {
	for _, status := range statuses {
		if status.State.Terminated == nil && status.State.Waiting == nil {
			return false
		}
	}
	return true
}

// SplitUniqueName 将唯一名称拆分为插件名和卷名。
// uniqueName 需符合 plugin_name/volume_name 格式，且插件名带命名空间，即 namespace/plugin，
// 其中恰好包含一个 '/'。因此唯一名称形如 plugin_namespace/plugin/volume_name；
// 参见 k8s.io/kubernetes/pkg/volume/plugins.go 中 VolumePlugin 接口说明，
// 以及 pkg/volume/util/volumehelper/volumehelper.go 中构造唯一卷名的 GetUniqueVolumeNameFromSpec。
func SplitUniqueName(uniqueName v1.UniqueVolumeName) (string, string, error) {
	components := strings.SplitN(string(uniqueName), "/", 3)
	if len(components) != 3 {
		return "", "", fmt.Errorf("cannot split volume unique name %s to plugin/volume components", uniqueName)
	}
	pluginName := fmt.Sprintf("%s/%s", components[0], components[1])
	return pluginName, components[2], nil
}

// GetVolumeMode 从 PV 中获取 VolumeMode。
// 内联卷没有 PersistentVolume，为保持既有行为返回 filesystem。
func GetVolumeMode(volumeSpec *volume.Spec) (v1.PersistentVolumeMode, error) {
	if volumeSpec == nil || volumeSpec.PersistentVolume == nil {
		return v1.PersistentVolumeFilesystem, nil
	}
	if volumeSpec.PersistentVolume.Spec.VolumeMode != nil {
		return *volumeSpec.PersistentVolume.Spec.VolumeMode, nil
	}
	return "", fmt.Errorf("cannot get volumeMode for volume: %v", volumeSpec.Name())
}

// GetPersistentVolumeClaimQualifiedName 返回 PVC 的限定名（命名空间/名称）。
func GetPersistentVolumeClaimQualifiedName(claim *v1.PersistentVolumeClaim) string {
	return utilstrings.JoinQualifiedName(claim.GetNamespace(), claim.GetName())
}

// CheckVolumeModeFilesystem 检查 VolumeMode 是否为 Filesystem。
func CheckVolumeModeFilesystem(volumeSpec *volume.Spec) (bool, error) {
	volumeMode, err := GetVolumeMode(volumeSpec)
	if err != nil {
		return true, err
	}
	if volumeMode == v1.PersistentVolumeBlock {
		return false, nil
	}
	return true, nil
}

// CheckPersistentVolumeClaimModeBlock 检查 PVC 的 VolumeMode 是否为 Block。
func CheckPersistentVolumeClaimModeBlock(pvc *v1.PersistentVolumeClaim) bool {
	return pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode == v1.PersistentVolumeBlock
}

// IsWindowsUNCPath 判断路径是否以 \\ 开头。
// 可用于跳过指向 SMB 共享、本地命名管道和本地 UNC 路径的处理。
func IsWindowsUNCPath(goos, path string) bool {
	if goos != "windows" {
		return false
	}
	// 检查 UNC 前缀 \\
	if strings.HasPrefix(path, `\\`) {
		return true
	}
	return false
}

// IsWindowsLocalPath 判断路径是否为以 "/" 或 "\" 开头的本地路径，
// 例如 "/foo/bar" 或 "\foo\bar"。
func IsWindowsLocalPath(goos, path string) bool {
	if goos != "windows" {
		return false
	}
	if IsWindowsUNCPath(goos, path) {
		return false
	}
	if strings.Contains(path, ":") {
		return false
	}
	if !(strings.HasPrefix(path, `/`) || strings.HasPrefix(path, `\`)) {
		return false
	}
	return true
}

// MakeAbsolutePath 根据 GOOS 将路径转换为绝对路径。
func MakeAbsolutePath(goos, path string) string {
	if goos != "windows" {
		return filepath.Clean("/" + path)
	}
	// 以下均针对 Windows。
	// 含冒号说明已带盘符，直接返回。
	if strings.Contains(path, ":") {
		return path
	}
	// 以斜杠开头但没有盘符时补上 'c:'。
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "\\") {
		return "c:" + path
	}
	// 其他情况补上 'c:\'。
	return "c:\\" + path
}

// MapBlockVolume 为指定卷和 Pod 提供映射块设备路径的通用实现。
// 实现了 volume.BlockVolumeMapper.Map() 方法的插件应调用此函数。
func MapBlockVolume(
	blkUtil volumepathhandler.BlockVolumePathHandler,
	devicePath,
	globalMapPath,
	podVolumeMapPath,
	volumeMapName string,
	podUID utypes.UID,
) error {
	// 以绑定挂载方式将设备映射到节点全局路径。
	mapErr := blkUtil.MapDevice(devicePath, globalMapPath, string(podUID), true /* 绑定挂载 */)
	if mapErr != nil {
		return fmt.Errorf("blkUtil.MapDevice failed. devicePath: %s, globalMapPath:%s, podUID: %s, bindMount: %v: %v",
			devicePath, globalMapPath, string(podUID), true, mapErr)
	}

	// 将设备映射到 Pod 的卷路径。
	mapErr = blkUtil.MapDevice(devicePath, podVolumeMapPath, volumeMapName, false /* 非绑定挂载 */)
	if mapErr != nil {
		return fmt.Errorf("blkUtil.MapDevice failed. devicePath: %s, podVolumeMapPath:%s, volumeMapName: %s, bindMount: %v: %v",
			devicePath, podVolumeMapPath, volumeMapName, false, mapErr)
	}

	// 持有文件描述符锁以保持块设备打开，否则设备可能被静默移除，
	// 再挂上同名的另一设备，容器运行时无法察觉这种情况。
	// 为避免意外，必须对块设备加锁。
	_, mapErr = blkUtil.AttachFileDevice(filepath.Join(globalMapPath, string(podUID)))
	if mapErr != nil {
		return fmt.Errorf("blkUtil.AttachFileDevice failed. globalMapPath:%s, podUID: %s: %v",
			globalMapPath, string(podUID), mapErr)
	}

	return nil
}

// UnmapBlockVolume 为指定卷和 Pod 提供解除块设备映射的通用实现。
// 实现了 volume.BlockVolumeMapper.Map() 方法的插件应调用此函数。
func UnmapBlockVolume(
	blkUtil volumepathhandler.BlockVolumePathHandler,
	globalUnmapPath,
	podDeviceUnmapPath,
	volumeMapName string,
	podUID utypes.UID,
) error {
	// 释放文件描述符锁。
	err := blkUtil.DetachFileDevice(filepath.Join(globalUnmapPath, string(podUID)))
	if err != nil {
		return fmt.Errorf("blkUtil.DetachFileDevice failed. globalUnmapPath:%s, podUID: %s: %v",
			globalUnmapPath, string(podUID), err)
	}

	// 解除 Pod 卷路径上的设备映射。
	unmapDeviceErr := blkUtil.UnmapDevice(podDeviceUnmapPath, volumeMapName, false /* 非绑定挂载 */)
	if unmapDeviceErr != nil {
		return fmt.Errorf("blkUtil.DetachFileDevice failed. podDeviceUnmapPath:%s, volumeMapName: %s, bindMount: %v: %v",
			podDeviceUnmapPath, volumeMapName, false, unmapDeviceErr)
	}

	// 解除节点全局路径上的设备映射。
	unmapDeviceErr = blkUtil.UnmapDevice(globalUnmapPath, string(podUID), true /* 绑定挂载 */)
	if unmapDeviceErr != nil {
		return fmt.Errorf("blkUtil.DetachFileDevice failed. globalUnmapPath:%s, podUID: %s, bindMount: %v: %v",
			globalUnmapPath, string(podUID), true, unmapDeviceErr)
	}
	return nil
}

// IsLocalEphemeralVolume 判断参数是否为本地临时卷。
// 本地指卷使用 kubelet 管理的本地磁盘存储；临时指卷的生命周期与 Pod 相同。
func IsLocalEphemeralVolume(volume v1.Volume) bool {
	return volume.GitRepo != nil ||
		(volume.EmptyDir != nil && volume.EmptyDir.Medium == v1.StorageMediumDefault) ||
		volume.ConfigMap != nil
}

// GetPodVolumeNames 返回 Pod 使用的卷名，包括文件系统挂载和裸块设备。
// 为避免再次遍历容器，可选地一并收集 SELinux 选项。
func GetPodVolumeNames(pod *v1.Pod, collectSELinuxOptions bool) (mounts sets.Set[string], devices sets.Set[string], seLinuxContainerContexts map[string][]*v1.SELinuxOptions) {
	mounts = sets.New[string]()
	devices = sets.New[string]()
	seLinuxContainerContexts = make(map[string][]*v1.SELinuxOptions)

	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(container *v1.Container, containerType podutil.ContainerType) bool {
		var seLinuxOptions *v1.SELinuxOptions
		if collectSELinuxOptions {
			effectiveContainerSecurity := securitycontext.DetermineEffectiveSecurityContext(pod, container)
			if effectiveContainerSecurity != nil {
				seLinuxOptions = effectiveContainerSecurity.SELinuxOptions
			}
		}

		if container.VolumeMounts != nil {
			for _, mount := range container.VolumeMounts {
				mounts.Insert(mount.Name)
				if seLinuxOptions != nil && collectSELinuxOptions {
					seLinuxContainerContexts[mount.Name] = append(seLinuxContainerContexts[mount.Name], seLinuxOptions.DeepCopy())
				}
			}
		}
		if container.VolumeDevices != nil {
			for _, device := range container.VolumeDevices {
				devices.Insert(device.Name)
			}
		}
		return true
	})
	return
}

// FsUserFrom 返回 Pod 的 FsUser，由各容器的 runAsUser 属性决定。
func FsUserFrom(pod *v1.Pod) *int64 {
	var fsUser *int64
	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(container *v1.Container, containerType podutil.ContainerType) bool {
		runAsUser, ok := securitycontext.DetermineEffectiveRunAsUser(pod, container)
		// 有容器未指定用户，或存在多个不同的非 root UID 时无法确定。
		if !ok || (fsUser != nil && *fsUser != *runAsUser) {
			fsUser = nil
			return false
		}
		if fsUser == nil {
			fsUser = runAsUser
		}
		return true
	})
	return fsUser
}

// HasMountRefs 判断给定的 mountPath 是否存在挂载引用。
// TODO: 这是针对 gci mounter 导致的设备卸载问题的临时绕过方案。
// 在 GCI 集群中，若使用 gci mounter 挂载，mounter 脚本启动的容器会产生额外挂载；
// 这些挂载与原始挂载无关，检查挂载引用时应予忽略。
// 当前做法是过滤掉包含原始挂载路径中 k8s 插件后缀的那些路径。
func HasMountRefs(mountPath string, mountRefs []string) bool {
	// mountPath 通常形如
	//   /var/lib/kubelet/plugins/kubernetes.io/some-plugin/mounts/volume-XXXX
	// 挂载引用可能形如
	//   /home/somewhere/var/lib/kubelet/plugins/kubernetes.io/some-plugin/...
	// 若 /var/lib/kubelet 挂在其他设备上，引用也可能形如
	//   /mnt/some-other-place/kubelet/plugins/kubernetes.io/some-plugin/...
	// 这两种都由 kubelet 处理，不应算作挂载引用。我们关心的是类似
	//   /data/local/some/manual/mount
	// 的路径，因为卸载它可能中断该挂载点上的使用。
	//
	// 因此不查找完整的 /var/lib/... 路径，而是截取 plugins/kubernetes.io/ 之后的部分来匹配。
	//
	// 若路径中没有 /plugins/...，则使用完整的 mountPath。
	pathToFind := mountPath
	if i := strings.Index(mountPath, kubernetesPluginPathPrefix); i > -1 {
		pathToFind = mountPath[i:]
	}
	for _, ref := range mountRefs {
		if !strings.Contains(ref, pathToFind) {
			return true
		}
	}
	return false
}

// IsMultiAttachAllowed 判断该卷是否明确不允许/不可能挂载到多个节点。
// 当前实现只能可靠判断哪些卷确定被禁止多挂载；返回 false 并不代表该卷类型支持多挂载，
// 仍需依赖 attacher 在这种情况下快速失败。
// 参见 https://github.com/kubernetes/kubernetes/issues/40669 和 https://github.com/kubernetes/kubernetes/pull/40148#discussion_r98055047
func IsMultiAttachAllowed(volumeSpec *volume.Spec) bool {
	if volumeSpec == nil {
		// 无法确定是否支持，若不支持就让 attacher 后续自行失败。
		return true
	}

	if volumeSpec.Volume != nil {
		// 排查已知在多挂载时失败缓慢或易出问题的卷类型。
		if volumeSpec.Volume.AzureDisk != nil ||
			volumeSpec.Volume.Cinder != nil {
			return false
		}
	}

	// 只有持久卷才有可靠信息判断是否允许多挂载；
	// 其余情况信任各卷实现会拒绝不支持的访问模式。
	if volumeSpec.PersistentVolume != nil {
		// 排查多挂载时不会失败的持久卷类型。
		if len(volumeSpec.PersistentVolume.Spec.AccessModes) == 0 {
			// 未指定访问模式则无法确定，必要时让 attacher 失败。
			return true
		}

		// 检查该卷是否允许挂载到多个 Pod/节点，若允许则返回 false。
		for _, accessMode := range volumeSpec.PersistentVolume.Spec.AccessModes {
			if accessMode == v1.ReadWriteMany || accessMode == v1.ReadOnlyMany {
				return true
			}
		}
		return false
	}

	// 无法确定是否支持，若不支持就让 attacher 后续自行失败。
	return true
}

// IsAttachableVolume 判断给定的卷规格是否为可挂载卷。
func IsAttachableVolume(volumeSpec *volume.Spec, volumePluginMgr *volume.VolumePluginMgr) bool {
	attachableVolumePlugin, _ := volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
	if attachableVolumePlugin != nil {
		volumeAttacher, err := attachableVolumePlugin.NewAttacher()
		if err == nil && volumeAttacher != nil {
			return true
		}
	}

	return false
}

// IsDeviceMountableVolume 判断给定的卷规格是否支持设备级挂载。
func IsDeviceMountableVolume(volumeSpec *volume.Spec, volumePluginMgr *volume.VolumePluginMgr) bool {
	deviceMountableVolumePlugin, _ := volumePluginMgr.FindDeviceMountablePluginBySpec(volumeSpec)
	if deviceMountableVolumePlugin != nil {
		volumeDeviceMounter, err := deviceMountableVolumePlugin.NewDeviceMounter()
		if err == nil && volumeDeviceMounter != nil {
			return true
		}
	}

	return false
}

// GetReliableMountRefs 调用 mounter.GetMountRefs，并在遇到 IsInconsistentReadError 时重试。
// 适用于没有防止同一卷被挂到多节点机制（如 attach/detach）的插件的卷重建场景。
func GetReliableMountRefs(mounter mount.Interface, mountPath string) ([]string, error) {
	var paths []string
	var lastErr error
	// 读取挂载引用可能命中不一致快照，短间隔重试直到结果稳定。
	err := wait.PollImmediate(10*time.Millisecond, time.Minute, func() (bool, error) {
		var err error
		paths, err = mounter.GetMountRefs(mountPath)
		if io.IsInconsistentReadError(err) {
			lastErr = err
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if wait.Interrupted(err) {
		return nil, lastErr
	}
	return paths, err
}

// VolumeHealthConditionSetsEqual 判断两组卷健康条件是否等价（忽略顺序，只比较状态和原因）。
func VolumeHealthConditionSetsEqual(a, b []v1.VolumeHealthCondition) bool {
	type key struct {
		status v1.VolumeHealthStatusType
		reason string
	}
	toSet := func(conds []v1.VolumeHealthCondition) map[key]struct{} {
		s := make(map[key]struct{}, len(conds))
		for _, c := range conds {
			s[key{c.Status, c.Reason}] = struct{}{}
		}
		return s
	}
	// 顺序无关，按（状态，原因）归一化后比较集合。
	sa, sb := toSet(a), toSet(b)
	if len(sa) != len(sb) {
		return false
	}
	for k := range sa {
		if _, ok := sb[k]; !ok {
			return false
		}
	}
	return true
}
