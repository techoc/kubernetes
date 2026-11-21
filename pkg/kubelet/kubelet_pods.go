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
	"bytes"
	"context"
	goerrors "errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/cri-streaming/pkg/streaming/portforward"
	remotecommandserver "k8s.io/cri-streaming/pkg/streaming/remotecommand"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	podshelper "k8s.io/kubernetes/pkg/apis/core/pods"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/fieldpath"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/envvars"
	"k8s.io/kubernetes/pkg/kubelet/images"
	"k8s.io/kubernetes/pkg/kubelet/kuberuntime"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/status"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	envutil "k8s.io/kubernetes/pkg/kubelet/util/env"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
	utilpod "k8s.io/kubernetes/pkg/util/pod"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
	volumevalidation "k8s.io/kubernetes/pkg/volume/validation"
	"k8s.io/kubernetes/third_party/forked/golang/expansion"
	utilnet "k8s.io/utils/net"
)

// 受管 hosts 文件的固定文件头（普通网络与主机网络各一种）
const (
	managedHostsHeader                = "# Kubernetes-managed hosts file.\n"
	managedHostsHeaderWithHostNetwork = "# Kubernetes-managed hosts file (host network).\n"
)

// 容器状态原因列表
const (
	PodInitializing         = "PodInitializing"
	ContainerCreating       = "ContainerCreating"
	RestartingAllContainers = "RestartingAllContainers"

	kubeletUser = "kubelet"
)

// parseGetSubIdsOutput 解析 `getsubids` 工具的输出，该工具用于查询某个用户或组的从属 ID 范围。
// getsubids 为每条已配置的映射输出一行。
// 这里假定只存在一条映射，且从属用户 ID 与从属组 ID 使用相同的范围。
// 输出类似：
// $ getsubids kubelet
// 0: kubelet 65536 2147483648
// $ getsubids -g kubelet
// 0: kubelet 65536 2147483648
func parseGetSubIdsOutput(input string) (uint32, uint32, error) {
	lines := strings.Split(strings.Trim(input, "\n"), "\n")
	if len(lines) != 1 {
		return 0, 0, fmt.Errorf("error parsing line %q: it must contain only one line", input)
	}

	parts := strings.Fields(lines[0])
	if len(parts) != 4 {
		return 0, 0, fmt.Errorf("invalid line %q", input)
	}

	// 解析数值字段
	num1, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("error parsing line %q: %w", input, err)
	}

	num2, err := strconv.ParseUint(parts[3], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("error parsing line %q: %w", input, err)
	}

	return uint32(num1), uint32(num2), nil
}

// defaultKubeletMappings 返回在未为 kubelet 用户做专门配置时用户命名空间可使用的 ID 范围：
// 即除 idsPerPod 以下以及最后 idsPerPod 个 ID 之外的全部 ID。
func defaultKubeletMappings(idsPerPod uint32) (uint32, uint32) {
	firstID := idsPerPod
	// 最后 idsPerPod 个 ID 不映射，因为内核认为 user
	// 2^32-1 是非法用户（参见 man 7 user_namespaces）。
	// 这里先把 firstID 转成 64 位，否则任何运算（包括减法）都会触发溢出检测
	// （Go 无法推断减去一个非负数后结果仍在 32 位范围内）；
	// 随后再转回 32 位，因为函数返回值就是 32 位。
	length := uint32((1 << 32) - uint64(firstID) - uint64(idsPerPod))
	return firstID, length
}

// getKubeletMappings 返回可用于配置用户命名空间的 ID 范围。
// 若为 kubelet 用户配置了从属用户/组 ID 范围且安装了 getsubids 工具，
// 则使用同时为用户 ID 和组 ID 指定的那条映射。
// 若未安装该工具或未配置任何 ID，则返回默认映射。
// 默认映射包含除 idsPerPod 以下及最后 idsPerPod 个 ID 之外的全部 ID
// （因为该范围中的最后一个用户在内核看来是非法的）。
func (kl *Kubelet) getKubeletMappings(logger klog.Logger, idsPerPod uint32) (uint32, uint32, error) {
	// 无专门配置时返回的默认映射
	defaultFirstID, defaultLen := defaultKubeletMappings(idsPerPod)

	if !utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) {
		return defaultFirstID, defaultLen, nil
	}

	// 必须先检查用户是否存在：getsubids 可能被配置为通过远程调用获取结果，
	// 而我们无法区分“远程端点不可达”和“端点可达但该用户没有条目”两种情况。
	// 因此先检查 kubelet 用户：若用户存在且 getsubids 可用，就期望能得到配置；
	// 若用户存在但 getsubids 没有返回任何配置，则认为远程端不可用并让 kubelet 启动失败。
	_, err := user.Lookup(kubeletUser)
	if err != nil {
		var unknownUserErr user.UnknownUserError
		if goerrors.As(err, &unknownUserErr) {
			// 若用户不存在，则认为未配置用户命名空间映射
			logger.V(5).Info("user namespaces: user not found, using default mappings", "user", kubeletUser)
			return defaultFirstID, defaultLen, nil
		}
		return 0, 0, err
	}

	execName := "getsubids"
	cmd, err := exec.LookPath(execName)
	if err != nil {
		if goerrors.Is(err, exec.ErrNotFound) {
			logger.V(2).Info("user namespaces: executable not found, using default mappings", "executable", execName, "err", err)
			return defaultFirstID, defaultLen, nil
		}
		return 0, 0, err
	}
	outUids, err := exec.Command(cmd, kubeletUser).Output()
	if err != nil {
		return 0, 0, fmt.Errorf("error retrieving additional uids for user %q: %w", kubeletUser, err)
	}
	outGids, err := exec.Command(cmd, "-g", kubeletUser).Output()
	if err != nil {
		return 0, 0, fmt.Errorf("error retrieving additional gids for user %q", kubeletUser)
	}
	if string(outUids) != string(outGids) {
		return 0, 0, fmt.Errorf("mismatched subuids and subgids for user %q", kubeletUser)
	}
	logger.V(5).Info("user namespaces: user found, using mappings from getsubids", "user", kubeletUser)
	return parseGetSubIdsOutput(string(outUids))
}

// listPodsFromDisk 返回磁盘上存在数据目录的 Pod UID 列表。
func (kl *Kubelet) listPodsFromDisk() ([]types.UID, error) {
	podInfos, err := os.ReadDir(kl.getPodsDir())
	if err != nil {
		return nil, err
	}
	pods := []types.UID{}
	for i := range podInfos {
		if podInfos[i].IsDir() {
			pods = append(pods, types.UID(podInfos[i].Name()))
		}
	}
	return pods, nil
}

// GetActivePods 返回已被 kubelet 接纳且尚未完全终止的 Pod，
// 对应 kubelet 的“期望状态”——即应当处于运行状态的 Pod。
// 警告：当前该列表不包含已被强制删除但可能仍在终止中的 Pod，
// 这意味着这些 Pod 在准入阶段分配的资源可能仍被占用。参见
func (kl *Kubelet) GetActivePods() []*v1.Pod {
	allPods := kl.podManager.GetPods()
	activePods := kl.filterOutInactivePods(allPods)
	return activePods
}

// getAllocatedPods 返回活跃 Pod（见 GetActivePods），但会将其更新为已分配状态。
func (kl *Kubelet) getAllocatedPods() []*v1.Pod {
	activePods := kl.GetActivePods()
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		return activePods
	}

	allocatedPods := make([]*v1.Pod, len(activePods))
	for i, pod := range activePods {
		allocatedPods[i], _ = kl.allocationManager.UpdatePodFromAllocation(pod)
	}
	return allocatedPods
}

// makeBlockVolumes 把容器中声明的裸块设备映射为运行时的设备信息。
// 实验性功能。
func (kl *Kubelet) makeBlockVolumes(logger klog.Logger, pod *v1.Pod, container *v1.Container, podVolumes kubecontainer.VolumeMap, blkutil volumepathhandler.BlockVolumePathHandler) ([]kubecontainer.DeviceInfo, error) {
	var devices []kubecontainer.DeviceInfo
	for _, device := range container.VolumeDevices {
		// 设备路径必须是绝对路径
		if !utilfs.IsAbs(device.DevicePath) {
			return nil, fmt.Errorf("error DevicePath `%s` must be an absolute path", device.DevicePath)
		}
		vol, ok := podVolumes[device.Name]
		if !ok || vol.BlockVolumeMapper == nil {
			logger.Error(nil, "Block volume cannot be satisfied for container, because the volume is missing or the volume mapper is nil", "containerName", container.Name, "device", device)
			return nil, fmt.Errorf("cannot find volume %q to pass into container %q", device.Name, container.Name)
		}
		// 获取 Pod 设备目录下指向块设备的软链接
		dirPath, volName := vol.BlockVolumeMapper.GetPodDeviceMapPath()
		symlinkPath := filepath.Join(dirPath, volName)
		if islinkExist, checkErr := blkutil.IsSymlinkExist(symlinkPath); checkErr != nil {
			return nil, checkErr
		} else if islinkExist {
			// 根据 PVC 卷源的只读标记设置挂载权限为只读。
			permission := "mrw"
			if vol.ReadOnly {
				permission = "r"
			}
			logger.V(4).Info("Device will be attached to container in the corresponding path on host", "containerName", container.Name, "path", symlinkPath)
			devices = append(devices, kubecontainer.DeviceInfo{PathOnHost: symlinkPath, PathInContainer: device.DevicePath, Permissions: permission})
		}
	}

	return devices, nil
}

// shouldMountHostsFile 判断是否应挂载节点的 /etc/hosts。
// Kubernetes 仅在以下情况挂载 /etc/hosts：
// - 容器不是基础设施（pause）容器；
// - 容器尚未自行挂载 /etc/hosts。
// 以下情况不会挂载 /etc/hosts：
// - Pod 使用 Pod 网络但 PodIP 尚未设置（例如正在创建 Pod sandbox）；
// - Pod 运行在 Windows 上且包含 hostProcess 容器。
func shouldMountHostsFile(pod *v1.Pod, podIPs []string) bool {
	shouldMount := len(podIPs) > 0 || pod.Spec.HostNetwork
	if runtime.GOOS == "windows" {
		return shouldMount && !kubecontainer.HasWindowsHostProcessContainer(pod)
	}
	return shouldMount
}

// makeMounts 计算给定容器需要挂载的挂载点集合。
func makeMounts(logger klog.Logger, pod *v1.Pod, podDir string, container *v1.Container, hostName, hostDomain string, podIPs []string, podVolumes kubecontainer.VolumeMap, hu hostutil.HostUtils, subpather subpath.Interface, expandEnvs []kubecontainer.EnvVar, supportsRRO bool, imageVolumes kubecontainer.ImageVolumes) ([]kubecontainer.Mount, func(), error) {
	mountEtcHostsFile := shouldMountHostsFile(pod, podIPs)
	logger.V(3).Info("Creating hosts mount for container", "pod", klog.KObj(pod), "containerName", container.Name, "podIPs", podIPs, "path", mountEtcHostsFile)
	mounts := []kubecontainer.Mount{}
	var cleanupActions []func()
	cleanupAction := func() {
		// 按挂载执行顺序的逆序清理，以应对清理动作之间可能存在的依赖；
		// 这些动作本身并无层级关系，只是约定逆序执行。
		for _, fn := range slices.Backward(cleanupActions) {
			fn()
		}
	}
	for i, mount := range container.VolumeMounts {
		// 若容器已自行挂载 /etc/hosts，则不再重复挂载
		mountEtcHostsFile = mountEtcHostsFile && (mount.MountPath != etcHostsPath)
		vol, ok := podVolumes[mount.Name]
		if !ok || vol.Mounter == nil {
			logger.Error(nil, "Mount cannot be satisfied for the container, because the volume is missing or the volume mounter (vol.Mounter) is nil",
				"containerName", container.Name, "ok", ok, "volumeMounter", mount)
			return nil, cleanupAction, fmt.Errorf("cannot find volume %q to mount into container %q", mount.Name, container.Name)
		}

		relabelVolume := false
		// 若卷支持 SELinux、尚未重打标签且不是只读卷，
		// 则对其重新打标签并标记为已打标签
		if vol.Mounter.GetAttributes().Managed && vol.Mounter.GetAttributes().SELinuxRelabel && !vol.SELinuxLabeled {
			vol.SELinuxLabeled = true
			relabelVolume = true
		}

		var (
			hostPath     string
			image        *runtimeapi.ImageSpec
			imageSubPath string
			err          error
		)

		subPath := mount.SubPath
		if mount.SubPathExpr != "" {
			subPath, err = kubecontainer.ExpandContainerVolumeMounts(mount, expandEnvs)

			if err != nil {
				return nil, cleanupAction, err
			}
		}

		if subPath != "" {
			if utilfs.IsAbs(subPath) {
				return nil, cleanupAction, fmt.Errorf("error SubPath `%s` must not be an absolute path", subPath)
			}

			if err := volumevalidation.ValidatePathNoBacksteps(subPath); err != nil {
				return nil, cleanupAction, fmt.Errorf("unable to provision SubPath `%s`: %w", subPath, err)
			}
		}

		if imageVolumes != nil && utilfeature.DefaultFeatureGate.Enabled(features.ImageVolume) {
			image = imageVolumes[mount.Name]
			imageSubPath = subPath
		}

		if image == nil {
			hostPath, err = volumeutil.GetPath(vol.Mounter)
			if err != nil {
				return nil, cleanupAction, err
			}

			if subPath != "" {
				volumePath := hostPath
				hostPath = filepath.Join(volumePath, subPath)

				if subPathExists, err := hu.PathExists(hostPath); err != nil {
					logger.Error(nil, "Could not determine if subPath exists, will not attempt to change its permissions", "path", hostPath)
				} else if !subPathExists {
					// 现在主动创建 subPath，因为若等到被引用时由运行时自动创建，
					// 其属主和权限可能不正确。例如 Pod 设置 fsGroup 时 subPath 目录至少需要 g+rwx，
					// 若不在此时创建，Docker 之后会以错误的 0750 权限自动创建；
					// 同时必须防止路径逃逸出卷目录。
					perm, err := hu.GetMode(volumePath)
					if err != nil {
						return nil, cleanupAction, err
					}
					if err := subpather.SafeMakeDir(subPath, volumePath, perm); err != nil && !goerrors.Is(err, os.ErrExist) {
						// 不要把详细错误返回给用户，以免泄露主机文件系统信息
						logger.Error(nil, "Failed to create subPath directory for volumeMount of the container", "containerName", container.Name, "volumeMountName", mount.Name)
						return nil, cleanupAction, fmt.Errorf("failed to create subPath directory for volumeMount %q of container %q", mount.Name, container.Name)
					}
				}
				var subpathCleanup func()
				hostPath, subpathCleanup, err = subpather.PrepareSafeSubpath(subpath.Subpath{
					VolumeMountIndex: i,
					Path:             hostPath,
					VolumeName:       vol.InnerVolumeSpecName,
					VolumePath:       volumePath,
					PodDir:           podDir,
					ContainerName:    container.Name,
				})
				if subpathCleanup != nil {
					// 追加到清理列表，确保复合 cleanupAction 被调用时
					// 所有 subPath 清理动作都会执行
					cleanupActions = append(cleanupActions, subpathCleanup)
				}
				if err != nil {
					// 不要把详细错误返回给用户，以免泄露主机文件系统信息
					logger.Error(nil, "Failed to prepare subPath for volumeMount of the container", "containerName", container.Name, "volumeMountName", mount.Name)
					return nil, cleanupAction, fmt.Errorf("failed to prepare subPath for volumeMount %q of container %q", mount.Name, container.Name)
				}
			}

			// Windows 上若路径不是 C:/ 形式，Docker 卷挂载会失败
			if hostPath != "" && volumeutil.IsWindowsLocalPath(runtime.GOOS, hostPath) {
				hostPath = volumeutil.MakeAbsolutePath(runtime.GOOS, hostPath)
			}
		}

		containerPath := mount.MountPath
		// Windows 上 UNC 路径/SMB 共享/命名管道会被 IsAbs 判为 false，需单独判断以跳过 MakeAbsolutePath
		if !volumeutil.IsWindowsUNCPath(runtime.GOOS, containerPath) && !utilfs.IsAbs(containerPath) {
			containerPath = volumeutil.MakeAbsolutePath(runtime.GOOS, containerPath)
		}

		propagation, err := translateMountPropagation(mount.MountPropagation)
		if err != nil {
			return nil, cleanupAction, err
		}
		logger.V(5).Info("Mount has propagation", "pod", klog.KObj(pod), "containerName", container.Name, "volumeMountName", mount.Name, "propagation", propagation)
		mustMountRO := vol.Mounter.GetAttributes().ReadOnly

		rro, err := resolveRecursiveReadOnly(mount, supportsRRO)
		if err != nil {
			return nil, cleanupAction, fmt.Errorf("failed to resolve recursive read-only mode: %w", err)
		}
		if rro && !utilfeature.DefaultFeatureGate.Enabled(features.RecursiveReadOnlyMounts) {
			return nil, cleanupAction, fmt.Errorf("recursive read-only mount needs feature gate %q to be enabled", features.RecursiveReadOnlyMounts)
		}

		mounts = append(mounts, kubecontainer.Mount{
			Name:              mount.Name,
			ContainerPath:     containerPath,
			HostPath:          hostPath,
			Image:             image,
			ImageSubPath:      imageSubPath,
			ReadOnly:          mount.ReadOnly || mustMountRO,
			RecursiveReadOnly: rro,
			SELinuxRelabel:    relabelVolume,
			Propagation:       propagation,
			BindMountOptions:  mount.BindMountOptions,
		})
	}
	if mountEtcHostsFile {
		hostAliases := pod.Spec.HostAliases
		hostsMount, err := makeHostsMount(podDir, podIPs, hostName, hostDomain, hostAliases, pod.Spec.HostNetwork)
		if err != nil {
			return nil, cleanupAction, err
		}
		mounts = append(mounts, *hostsMount)
	}
	return mounts, cleanupAction, nil
}

// translateMountPropagation 把 v1.MountPropagationMode 转换为
// 即 runtimeapi.MountPropagation。
func translateMountPropagation(mountMode *v1.MountPropagationMode) (runtimeapi.MountPropagation, error) {
	if runtime.GOOS == "windows" {
		// Windows 容器不支持挂载传播，统一使用 private。
		// 参考 https://docs.docker.com/storage/bind-mounts/#configure-bind-propagation。
		return runtimeapi.MountPropagation_PROPAGATION_PRIVATE, nil
	}

	switch {
	case mountMode == nil:
		// 默认使用 PRIVATE
		return runtimeapi.MountPropagation_PROPAGATION_PRIVATE, nil
	case *mountMode == v1.MountPropagationHostToContainer:
		return runtimeapi.MountPropagation_PROPAGATION_HOST_TO_CONTAINER, nil
	case *mountMode == v1.MountPropagationBidirectional:
		return runtimeapi.MountPropagation_PROPAGATION_BIDIRECTIONAL, nil
	case *mountMode == v1.MountPropagationNone:
		return runtimeapi.MountPropagation_PROPAGATION_PRIVATE, nil
	default:
		return 0, fmt.Errorf("invalid MountPropagation mode: %q", *mountMode)
	}
}

// getEtcHostsPath 返回 Pod 生成的 /etc/hosts 文件在宿主机上的完整路径。
func getEtcHostsPath(podDir string) string {
	hostsFilePath := filepath.Join(podDir, "etc-hosts")
	// Windows 上若路径不是 C:/ 形式，卷挂载会失败
	return volumeutil.MakeAbsolutePath(runtime.GOOS, hostsFilePath)
}

// makeHostsMount 为注入 Pod 各容器的 hosts 文件构造挂载点。
// 这里使用 podIPs 而不是 podIP，因为即使未启用双栈特性 podIPs 也已填充。
func makeHostsMount(podDir string, podIPs []string, hostName, hostDomainName string, hostAliases []v1.HostAlias, useHostNetwork bool) (*kubecontainer.Mount, error) {
	hostsFilePath := getEtcHostsPath(podDir)
	if err := ensureHostsFile(hostsFilePath, podIPs, hostName, hostDomainName, hostAliases, useHostNetwork); err != nil {
		return nil, err
	}
	return &kubecontainer.Mount{
		Name:           "k8s-managed-etc-hosts",
		ContainerPath:  etcHostsPath,
		HostPath:       hostsFilePath,
		ReadOnly:       false,
		SELinuxRelabel: true,
	}, nil
}

// ensureHostsFile 确保目标 hosts 文件包含最新的 IP、主机名
// 与域名信息。
func ensureHostsFile(fileName string, hostIPs []string, hostName, hostDomainName string, hostAliases []v1.HostAlias, useHostNetwork bool) error {
	var hostsFileContent []byte
	var err error

	if useHostNetwork {
		// 若 Pod 使用主机网络，则从节点文件系统读取 hosts 文件：
		// etcHostsPath 指向节点上 hosts 文件的位置
		// （类 Unix 系统即 /etc/hosts）
		hostsFileContent, err = nodeHostsFileContent(etcHostsPath, hostAliases)
		if err != nil {
			return err
		}
	} else {
		// 若 Pod 不使用主机网络，则基于 Pod IP 等信息生成受管理的 hosts 文件
		hostsFileContent = managedHostsFileContent(hostIPs, hostName, hostDomainName, hostAliases)
	}

	hostsFilePerm := os.FileMode(0644)
	if err := os.WriteFile(fileName, hostsFileContent, hostsFilePerm); err != nil {
		return err
	}
	return os.Chmod(fileName, hostsFilePerm)
}

// nodeHostsFileContent 读取节点 hosts 文件内容，并前置受管文件头。
func nodeHostsFileContent(hostsFilePath string, hostAliases []v1.HostAlias) ([]byte, error) {
	hostsFileContent, err := os.ReadFile(hostsFilePath)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	buffer.WriteString(managedHostsHeaderWithHostNetwork)
	buffer.Write(hostsFileContent)
	buffer.Write(hostsEntriesFromHostAliases(hostAliases))
	return buffer.Bytes(), nil
}

// managedHostsFileContent 基于 Pod IP 等信息生成受管理的 etc/hosts 内容。
func managedHostsFileContent(hostIPs []string, hostName, hostDomainName string, hostAliases []v1.HostAlias) []byte {
	var buffer bytes.Buffer
	buffer.WriteString(managedHostsHeader)
	buffer.WriteString("127.0.0.1\tlocalhost\n")                      // ipv4 localhost
	buffer.WriteString("::1\tlocalhost ip6-localhost ip6-loopback\n") // ipv6 localhost
	buffer.WriteString("fe00::0\tip6-localnet\n")
	buffer.WriteString("fe00::0\tip6-mcastprefix\n")
	buffer.WriteString("fe00::1\tip6-allnodes\n")
	buffer.WriteString("fe00::2\tip6-allrouters\n")
	if len(hostDomainName) > 0 {
		// 为 podIPs 中的每个 IP 生成主机条目；
		// 即使未启用双栈特性，集群中 podIPs 字段
		// 也会被填充
		for _, hostIP := range hostIPs {
			buffer.WriteString(fmt.Sprintf("%s\t%s.%s\t%s\n", hostIP, hostName, hostDomainName, hostName))
		}
	} else {
		for _, hostIP := range hostIPs {
			buffer.WriteString(fmt.Sprintf("%s\t%s\n", hostIP, hostName))
		}
	}
	buffer.Write(hostsEntriesFromHostAliases(hostAliases))
	return buffer.Bytes()
}

// hostsEntriesFromHostAliases 把 HostAliases 渲染为 hosts 文件条目；
// 同一 IP 的多个别名写入同一行
func hostsEntriesFromHostAliases(hostAliases []v1.HostAlias) []byte {
	if len(hostAliases) == 0 {
		return []byte{}
	}

	var buffer bytes.Buffer
	buffer.WriteString("\n")
	buffer.WriteString("# Entries added by HostAliases.\n")
	// 每个 IP 的所有别名写入 hosts 文件中的同一行
	for _, hostAlias := range hostAliases {
		buffer.WriteString(fmt.Sprintf("%s\t%s\n", hostAlias.IP, strings.Join(hostAlias.Hostnames, "\t")))
	}
	return buffer.Bytes()
}

// truncatePodHostnameIfNeeded 在 Pod 主机名超过 63 个字符时进行截断。
func truncatePodHostnameIfNeeded(logger klog.Logger, podName, hostname string) (string, error) {
	// 主机名上限为 63 个字符（规范为 64 字节，即 63 个字符加一个 NUL 终止符）。
	const hostnameMaxLen = 63
	if len(hostname) <= hostnameMaxLen {
		return hostname, nil
	}
	truncated := hostname[:hostnameMaxLen]
	logger.Error(nil, "Hostname for pod was too long, truncated it", "podName", podName, "hostnameMaxLen", hostnameMaxLen, "truncatedHostname", truncated)
	// 主机名不能以 '-' 或 '.' 结尾
	truncated = strings.TrimRight(truncated, "-.")
	if len(truncated) == 0 {
		// 正常情况下不会走到这里。
		return "", fmt.Errorf("hostname for pod %q was invalid: %q", podName, hostname)
	}
	return truncated, nil
}

// GetOrCreateUserNamespaceMappings 返回 sandbox 用户命名空间的配置。
func (kl *Kubelet) GetOrCreateUserNamespaceMappings(logger klog.Logger, pod *v1.Pod, runtimeHandler string) (*runtimeapi.UserNamespace, error) {
	return kl.usernsManager.GetOrCreateUserNamespaceMappings(logger, pod, runtimeHandler)
}

// GeneratePodHostNameAndDomain 根据 Pod 的 spec 和注解生成主机名与域名，
// 校验失败时返回错误。
func (kl *Kubelet) GeneratePodHostNameAndDomain(logger klog.Logger, pod *v1.Pod) (string, string, error) {
	clusterDomain := kl.dnsConfigurer.ClusterDomain

	if pod.Spec.HostnameOverride != nil {
		hostname := *pod.Spec.HostnameOverride
		if msgs := utilvalidation.IsDNS1123Subdomain(hostname); len(msgs) != 0 {
			return "", "", fmt.Errorf("pod HostnameOverride %q is not a valid DNS subdomain: %s", hostname, strings.Join(msgs, ";"))
		}
		truncatedHostname, err := truncatePodHostnameIfNeeded(logger, pod.Name, hostname)
		if err != nil {
			return "", "", err
		}
		return truncatedHostname, "", nil
	}

	hostname := pod.Name
	if len(pod.Spec.Hostname) > 0 {
		if msgs := utilvalidation.IsDNS1123Label(pod.Spec.Hostname); len(msgs) != 0 {
			return "", "", fmt.Errorf("pod Hostname %q is not a valid DNS label: %s", pod.Spec.Hostname, strings.Join(msgs, ";"))
		}
		hostname = pod.Spec.Hostname
	}

	hostname, err := truncatePodHostnameIfNeeded(logger, pod.Name, hostname)
	if err != nil {
		return "", "", err
	}

	hostDomain := ""
	if len(pod.Spec.Subdomain) > 0 {
		if msgs := utilvalidation.IsDNS1123Label(pod.Spec.Subdomain); len(msgs) != 0 {
			return "", "", fmt.Errorf("pod Subdomain %q is not a valid DNS label: %s", pod.Spec.Subdomain, strings.Join(msgs, ";"))
		}
		hostDomain = fmt.Sprintf("%s.%s.svc.%s", pod.Spec.Subdomain, pod.Namespace, clusterDomain)
	}

	return hostname, hostDomain, nil
}

// GetPodCgroupParent 从容器管理器获取 Pod 的 cgroup 父级。
func (kl *Kubelet) GetPodCgroupParent(pod *v1.Pod) string {
	pcm := kl.containerManager.NewPodContainerManager()
	_, cgroupParent := pcm.GetPodContainerName(pod)
	return cgroupParent
}

// ResizeEphemeralVolume 直接触发指定卷的扩容。
func (kl *Kubelet) ResizeEphemeralVolume(pod *v1.Pod, volumeName string, newSize *apiresource.Quantity) error {
	return kl.volumeManager.ResizeEphemeralVolume(pod, volumeName, newSize)
}

// GenerateRunContainerOptions 生成 RunContainerOptions，
// 供容器运行时设置启动容器的参数。
func (kl *Kubelet) GenerateRunContainerOptions(ctx context.Context, pod *v1.Pod, container *v1.Container, podIP string, podIPs []string, imageVolumes kubecontainer.ImageVolumes) (*kubecontainer.RunContainerOptions, func(), error) {
	logger := klog.FromContext(ctx)
	supportsRRO := kl.runtimeClassSupportsRecursiveReadOnlyMounts(logger, pod)

	opts, err := kl.containerManager.GetResources(ctx, pod, container)
	if err != nil {
		return nil, nil, err
	}
	// hostname 为短主机名，会传给 makeMounts 用于生成 /etc/hosts 文件。
	hostname, hostDomainName, err := kl.GeneratePodHostNameAndDomain(logger, pod)
	if err != nil {
		return nil, nil, err
	}

	podName := volumeutil.GetUniquePodName(pod)
	volumes := kl.volumeManager.GetMountedVolumesForPod(podName)

	blkutil := volumepathhandler.NewBlockVolumePathHandler()
	blkVolumes, err := kl.makeBlockVolumes(logger, pod, container, volumes, blkutil)
	if err != nil {
		return nil, nil, err
	}
	opts.Devices = append(opts.Devices, blkVolumes...)

	envs, err := kl.makeEnvironmentVariables(ctx, pod, container, podIP, podIPs, volumes)
	if err != nil {
		return nil, nil, err
	}
	opts.Envs = append(opts.Envs, envs...)

	// 只把 podIPs 传给 makeMounts，因为即使未启用双栈特性 podIPs 也已填充。
	mounts, cleanupAction, err := makeMounts(logger, pod, kl.getPodDir(pod.UID), container, hostname, hostDomainName, podIPs, volumes, kl.hostutil, kl.subpather, opts.Envs, supportsRRO, imageVolumes)
	if err != nil {
		return nil, cleanupAction, err
	}
	opts.Mounts = append(opts.Mounts, mounts...)

	// Windows 上仅在使用 containerd 时才能设置 TerminationMessagePath：
	// Docker for Windows 不支持把单个文件挂载为卷。
	if len(container.TerminationMessagePath) != 0 {
		p := kl.getPodContainerDir(pod.UID, container.Name)
		if err := os.MkdirAll(p, 0750); err != nil {
			logger.Error(err, "Error on creating dir", "path", p)
		} else {
			opts.PodContainerDir = p
		}
	}

	return opts, cleanupAction, nil
}

var masterServices = sets.New[string]("kubernetes")

// getServiceEnvVarMap 为指定命名空间中的 Pod 构造可访问服务的环境变量映射。
func (kl *Kubelet) getServiceEnvVarMap(ns string, enableServiceLinks bool) (map[string]string, error) {
	var (
		serviceMap = make(map[string]*v1.Service)
		m          = make(map[string]string)
	)

	// 从 apiserver（经缓存）获取全部 Service 资源，
	// 并填充为服务相关的环境变量。
	if kl.serviceLister == nil {
		// 没有 apiserver 的 kubelet（如裸 GCE ContainerVM）不设置服务环境变量。
		return m, nil
	}
	services, err := kl.serviceLister.List(labels.Everything())
	if err != nil {
		return m, fmt.Errorf("failed to list services when setting up env vars")
	}

	// 把命名空间 ns 中的服务投影到 master 服务集合
	for i := range services {
		service := services[i]
		// 忽略 ClusterIP 为 "None" 或为空的 Service
		if !v1helper.IsServiceIPSet(service) {
			continue
		}
		serviceName := service.Name

		// master 服务（默认命名空间中的 kubernetes 服务）总是生成环境变量，
		// 即使 enableServiceLinks 为 false；
		// 当 enableServiceLinks 为 true 时，同一命名空间中的
		// 其他服务也会生成环境变量。
		if service.Namespace == metav1.NamespaceDefault && masterServices.Has(serviceName) {
			if _, exists := serviceMap[serviceName]; !exists {
				serviceMap[serviceName] = service
			}
		} else if service.Namespace == ns && enableServiceLinks {
			serviceMap[serviceName] = service
		}
	}

	mappedServices := []*v1.Service{}
	for key := range serviceMap {
		mappedServices = append(mappedServices, serviceMap[key])
	}

	for _, e := range envvars.FromServices(mappedServices) {
		m[e.Name] = e.Value
	}
	return m, nil
}

// makeEnvironmentVariables 为指定命名空间中的 Pod 构造容器环境变量。
func (kl *Kubelet) makeEnvironmentVariables(ctx context.Context, pod *v1.Pod, container *v1.Container, podIP string, podIPs []string, podVolumes kubecontainer.VolumeMap) ([]kubecontainer.EnvVar, error) {
	logger := klog.FromContext(ctx)
	if pod.Spec.EnableServiceLinks == nil {
		return nil, fmt.Errorf("nil pod.spec.enableServiceLinks encountered, cannot construct envvars")
	}

	// 若 Pod 来自 kube-apiserver，说明 apiserver 有响应且 kubelet 凭据有效，
	// 因此可以合理地在构建服务环境变量映射前，至少等待一次 service lister 同步完成。
	// 这不能完全消除下面的竞态，但能避免“服务列表尚未完成即可预期会成功”的明显失败；
	// 典型场景是 kubelet 重启后先读到 Pod 再读到 Service，
	// 导致某些 Pod 因未等待服务同步而缺少 KUBERNETES_SERVICE_HOST 注入。
	// 该链接特殊在于它会被无条件注入 Pod，并供集群内配置读取。
	if !kubetypes.IsStaticPod(pod) && !kl.serviceHasSynced() {
		return nil, fmt.Errorf("services have not yet been read at least once, cannot construct envvars")
	}

	var result []kubecontainer.EnvVar
	// 注意：这些变量会加入运行时配置，但不参与 kubecontainer.HashContainer 计算的校验和，
	// 这样仍可通过哈希判断容器是否已在运行，
	// 避免仅因某个服务变化就重启容器。
	// 注意 kubelet 看到 Pod 与看到 Service 之间存在竞态，
	// 用户可通过以下方式规避：(1) 在启动服务与启动 Pod 之间等待；(2) 检测到
	// 缺少服务环境变量后退出并重启；(3) 使用 DNS 而非环境变量（推荐）。
	serviceEnv, err := kl.getServiceEnvVarMap(pod.Namespace, *pod.Spec.EnableServiceLinks)
	if err != nil {
		return result, err
	}

	var (
		configMaps = make(map[string]*v1.ConfigMap)
		secrets    = make(map[string]*v1.Secret)
		tmpEnv     = make(map[string]string) // TODO: switch to map[string][]byte
	)

	// Env 会覆盖 EnvFrom 中的变量：
	// 先处理 EnvFrom，再允许 Env 覆盖已有值。
	for _, envFrom := range container.EnvFrom {
		switch {
		case envFrom.ConfigMapRef != nil:
			cm := envFrom.ConfigMapRef
			name := cm.Name
			configMap, ok := configMaps[name]
			if !ok {
				if kl.kubeClient == nil {
					return result, fmt.Errorf("couldn't get configMap %v/%v, no kubeClient defined", pod.Namespace, name)
				}
				optional := cm.Optional != nil && *cm.Optional
				configMap, err = kl.configMapManager.GetConfigMap(pod.Namespace, name)
				if err != nil {
					if errors.IsNotFound(err) && optional {
						// 标记为可选时忽略未找到错误
						continue
					}
					return result, err
				}
				configMaps[name] = configMap
			}

			for k, v := range configMap.Data {
				if len(envFrom.Prefix) > 0 {
					k = envFrom.Prefix + k
				}

				// TODO: 校验不含 NUL 字节
				tmpEnv[k] = v
			}
		case envFrom.SecretRef != nil:
			s := envFrom.SecretRef
			name := s.Name
			secret, ok := secrets[name]
			if !ok {
				if kl.kubeClient == nil {
					return result, fmt.Errorf("couldn't get secret %v/%v, no kubeClient defined", pod.Namespace, name)
				}
				optional := s.Optional != nil && *s.Optional
				secret, err = kl.secretManager.GetSecret(pod.Namespace, name)
				if err != nil {
					if errors.IsNotFound(err) && optional {
						// 标记为可选时忽略未找到错误
						continue
					}
					return result, err
				}
				secrets[name] = secret
			}

			for k, v := range secret.Data {
				if len(envFrom.Prefix) > 0 {
					k = envFrom.Prefix + k
				}

				// TODO: 校验不含 NUL 字节
				tmpEnv[k] = string(v)
			}
		}
	}

	// 计算变量的最终取值：
	// 1.  计算每个变量的最终值：
	//     a.  若变量设置了 Value，则展开其中指向其他变量的 `$(var)` 引用，
	//         引用来源为容器声明的变量与服务环境变量；
	//     b.  若变量定义了来源，则解析该来源；
	// 2.  按变量声明顺序构建容器环境；
	// 3.  追加剩余的服务环境变量
	var (
		mappingFunc = expansion.MappingFuncFor(tmpEnv, serviceEnv)
	)
	for _, envVar := range container.Env {
		runtimeVal := envVar.Value
		if runtimeVal != "" {
			// 步骤 1a：展开变量引用
			runtimeVal = expansion.Expand(runtimeVal, mappingFunc)
		} else if envVar.ValueFrom != nil {
			// 步骤 1b：解析其他环境变量来源
			switch {
			case envVar.ValueFrom.FieldRef != nil:
				runtimeVal, err = kl.podFieldSelectorRuntimeValue(ctx, envVar.ValueFrom.FieldRef, pod, podIP, podIPs)
				if err != nil {
					return result, err
				}
			case envVar.ValueFrom.ResourceFieldRef != nil:
				defaultedPod, defaultedContainer, err := kl.defaultPodLimitsForDownwardAPI(ctx, pod, container)
				if err != nil {
					return result, err
				}
				runtimeVal, err = containerResourceRuntimeValue(envVar.ValueFrom.ResourceFieldRef, defaultedPod, defaultedContainer)
				if err != nil {
					return result, err
				}
			case envVar.ValueFrom.ConfigMapKeyRef != nil:
				cm := envVar.ValueFrom.ConfigMapKeyRef
				name := cm.Name
				key := cm.Key
				optional := cm.Optional != nil && *cm.Optional
				configMap, ok := configMaps[name]
				if !ok {
					if kl.kubeClient == nil {
						return result, fmt.Errorf("couldn't get configMap %v/%v, no kubeClient defined", pod.Namespace, name)
					}
					configMap, err = kl.configMapManager.GetConfigMap(pod.Namespace, name)
					if err != nil {
						if errors.IsNotFound(err) && optional {
							// 标记为可选时忽略未找到错误
							continue
						}
						return result, err
					}
					configMaps[name] = configMap
				}
				runtimeVal, ok = configMap.Data[key]
				if !ok {
					if optional {
						continue
					}
					return result, fmt.Errorf("couldn't find key %v in ConfigMap %v/%v", key, pod.Namespace, name)
				}
			case envVar.ValueFrom.SecretKeyRef != nil:
				s := envVar.ValueFrom.SecretKeyRef
				name := s.Name
				key := s.Key
				optional := s.Optional != nil && *s.Optional
				secret, ok := secrets[name]
				if !ok {
					if kl.kubeClient == nil {
						return result, fmt.Errorf("couldn't get secret %v/%v, no kubeClient defined", pod.Namespace, name)
					}
					secret, err = kl.secretManager.GetSecret(pod.Namespace, name)
					if err != nil {
						if errors.IsNotFound(err) && optional {
							// 标记为可选时忽略未找到错误
							continue
						}
						return result, err
					}
					secrets[name] = secret
				}
				runtimeValBytes, ok := secret.Data[key]
				if !ok {
					if optional {
						continue
					}
					return result, fmt.Errorf("couldn't find key %v in Secret %v/%v", key, pod.Namespace, name)
				}
				// TODO: 校验不含 NUL 字节
				runtimeVal = string(runtimeValBytes)
			case utilfeature.DefaultFeatureGate.Enabled(features.EnvFiles) && envVar.ValueFrom.FileKeyRef != nil:
				f := envVar.ValueFrom.FileKeyRef
				key := f.Key
				volume := f.VolumeName
				optional := f.Optional != nil && *f.Optional
				vol, ok := podVolumes[volume]
				if !ok || vol.Mounter == nil {
					return result, fmt.Errorf("cannot find the volume %q referenced by FileKeyRef", volume)
				}

				hostPath, err := volumeutil.GetPath(vol.Mounter)
				if err != nil {
					return result, fmt.Errorf("failed to get host path for volume %q: %w", volume, err)
				}

				// 拼接环境变量文件的完整路径：
				// 由 hostPath 与 FileKeyRef 中指定的路径组合而成
				envFilePath, err := securejoin.SecureJoin(hostPath, f.Path)
				if err != nil {
					return result, err
				}

				runtimeVal, err = envutil.ParseEnv(envFilePath, key)
				if err != nil {
					logger.Error(err, "Failed to parse env file", "pod", klog.KObj(pod))
					return result, fmt.Errorf("couldn't parse env file")
				}

				// 若未找到该 key 且不是可选来源，则返回错误
				if runtimeVal == "" {
					if optional {
						// 若 key 不存在且为可选来源，则跳过该环境变量
						continue
					}
					return result, fmt.Errorf("environment variable key %q not found in file %q", key, envFilePath)
				}
			}
		}

		tmpEnv[envVar.Name] = runtimeVal
	}

	// 追加环境变量
	for k, v := range tmpEnv {
		result = append(result, kubecontainer.EnvVar{Name: k, Value: v})
	}

	// 追加剩余的服务环境变量。
	for k, v := range serviceEnv {
		// 该逻辑访问 apiserver 与 Pods；
		// master 或 kubelet 都可能设置服务环境变量，若两者都设置，
		// 则跳过 kubelet 生成的同名 key，避免环境变量重复。
		// TODO: 所有平台都改用 apiserver+Pods 后删除下一行判断
		if _, present := tmpEnv[k]; !present {
			result = append(result, kubecontainer.EnvVar{Name: k, Value: v})
		}
	}
	return result, nil
}

// podFieldSelectorRuntimeValue 返回给定向下 API 字段选择器在
// 该 Pod 上的运行时取值。
func (kl *Kubelet) podFieldSelectorRuntimeValue(ctx context.Context, fs *v1.ObjectFieldSelector, pod *v1.Pod, podIP string, podIPs []string) (string, error) {
	internalFieldPath, _, err := podshelper.ConvertDownwardAPIFieldLabel(fs.APIVersion, fs.FieldPath, "")
	if err != nil {
		return "", err
	}

	// 使 podIPs 顺序与节点 IP 族偏好一致（#97979）
	podIPs = kl.sortPodIPs(podIPs)
	if len(podIPs) > 0 {
		podIP = podIPs[0]
	}

	switch internalFieldPath {
	case "spec.nodeName":
		return pod.Spec.NodeName, nil
	case "spec.serviceAccountName":
		return pod.Spec.ServiceAccountName, nil
	case "status.hostIP":
		hostIPs, err := kl.getHostIPsAnyWay(ctx)
		if err != nil {
			return "", err
		}
		return hostIPs[0].String(), nil
	case "status.hostIPs":
		hostIPs, err := kl.getHostIPsAnyWay(ctx)
		if err != nil {
			return "", err
		}
		ips := make([]string, 0, len(hostIPs))
		for _, ip := range hostIPs {
			ips = append(ips, ip.String())
		}
		return strings.Join(ips, ","), nil
	case "status.podIP":
		return podIP, nil
	case "status.podIPs":
		return strings.Join(podIPs, ","), nil
	}
	return fieldpath.ExtractFieldPathAsString(pod, internalFieldPath)
}

// containerResourceRuntimeValue 返回指定容器资源字段选择器的值。
func containerResourceRuntimeValue(fs *v1.ResourceFieldSelector, pod *v1.Pod, container *v1.Container) (string, error) {
	containerName := fs.ContainerName
	if len(containerName) == 0 {
		return resource.ExtractContainerResourceValue(fs, container)
	}
	return resource.ExtractResourceValueByContainerName(fs, pod, containerName)
}

// killPod 通知容器运行时终止 Pod。调用前要求 Pod 状态已包含
// 最近一次 syncPod 的结果，否则可能无法终止新建的容器与 sandbox。
func (kl *Kubelet) killPod(ctx context.Context, pod *v1.Pod, p kubecontainer.Pod, gracePeriodOverride *int64) error {
	logger := klog.FromContext(ctx)
	// 调用容器运行时的 KillPod，停止该 Pod 所有已知的运行中容器
	if err := kl.containerRuntime.KillPod(ctx, pod, p, gracePeriodOverride); err != nil {
		return err
	}
	if err := kl.containerManager.UpdateQOSCgroups(logger); err != nil {
		logger.V(2).Info("Failed to update QoS cgroups while killing pod", "err", err)
	}
	return nil
}

// makePodDataDirs 创建 Pod 数据目录（Pod 目录、卷目录与插件目录）。
func (kl *Kubelet) makePodDataDirs(pod *v1.Pod) error {
	uid := pod.UID
	if err := os.MkdirAll(kl.getPodDir(uid), 0750); err != nil && !os.IsExist(err) {
		return err
	}
	if err := os.MkdirAll(kl.getPodVolumesDir(uid), 0750); err != nil && !os.IsExist(err) {
		return err
	}
	if err := os.MkdirAll(kl.getPodPluginsDir(uid), 0750); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

// getPullSecretsForPod 检查 Pod 并获取其引用的拉取镜像凭据；
// 无法找到的 Secret 名称会一并返回，以便镜像拉取失败时
// 后续发出事件。
func (kl *Kubelet) getPullSecretsForPod(logger klog.Logger, pod *v1.Pod) ([]v1.Secret, []string) {
	pullSecrets := []v1.Secret{}
	missingPullSecretNames := []string{}

	for _, secretRef := range pod.Spec.ImagePullSecrets {
		if len(secretRef.Name) == 0 {
			// API 校验曾允许名称为空的条目（https://issue.k8s.io/99454#issuecomment-787838112），
			// 这里直接忽略以避免无谓告警。
			continue
		}
		secret, err := kl.secretManager.GetSecret(pod.Namespace, secretRef.Name)
		if err != nil {
			logger.Info("Unable to retrieve pull secret, the image pull may not succeed.", "pod", klog.KObj(pod), "secret", klog.KRef(pod.Namespace, secretRef.Name), "err", err)
			missingPullSecretNames = append(missingPullSecretNames, secretRef.Name)
			continue
		}

		pullSecrets = append(pullSecrets, *secret)
	}

	return pullSecrets, missingPullSecretNames
}

// PodCouldHaveRunningContainers 在指定 UID 的 Pod 可能仍有运行中容器时返回 true；
// 若 Pod 尚未启动或未知，则返回 false。
func (kl *Kubelet) PodCouldHaveRunningContainers(pod *v1.Pod) bool {
	if kl.podWorkers.CouldHaveRunningContainers(pod.UID) {
		return true
	}

	// 检查 Pod 在终止前是否可能需要释放资源
	// 注意：这是临时方案，放在这里是为了避免改动
	// status manager 及其测试。
	// TODO: 扩展 PodDeletionSafetyProvider 接口，
	// 并在独立的 Kubelet 方法中实现。
	if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		if kl.containerManager.PodMightNeedToUnprepareResources(pod.UID) {
			return true
		}
	}
	return false
}

// PodIsFinished 在 SyncTerminatedPod 完成时返回 true，即
// Pod 占用的所有必需节点级资源都已被 kubelet 回收。
func (kl *Kubelet) PodIsFinished(pod *v1.Pod) bool {
	return kl.podWorkers.ShouldPodBeFinished(pod.UID)
}

// filterOutInactivePods 返回未处于终止阶段、或已知已完全终止的 Pod。
// 该方法仅应用于 pod worker 上游（即 pod manager 已知的 Pod），
// 其他组件应以 pod worker 为准。
func (kl *Kubelet) filterOutInactivePods(pods []*v1.Pod) []*v1.Pod {
	filteredPods := make([]*v1.Pod, 0, len(pods))
	for _, p := range pods {
		if kl.isPodInactive(p) {
			continue
		}
		filteredPods = append(filteredPods, p)
	}
	return filteredPods
}

// isPodInactive 在 Pod 处于终止阶段、或已知已完全终止时返回 true。
// 该方法仅应用于 pod worker 上游（即 pod manager 已知的 Pod）。
func (kl *Kubelet) isPodInactive(p *v1.Pod) bool {
	// 若某 UID 的 Pod 已完全终止，则应从 Pod 列表中剔除
	if kl.podWorkers.IsPodKnownTerminated(p.UID) {
		return true
	}
	// 终止阶段的 Pod 视为非活跃，除非正在主动终止
	if kl.isAdmittedPodTerminal(p) && !kl.podWorkers.IsPodTerminationRequested(p.UID) {
		return true
	}
	return false
}

// isAdmittedPodTerminal 在配置源中的 Pod 已处于终止阶段，
// 或 kubelet 已标记该 Pod 到达终止阶段但配置源尚未确认时返回 true；
// 该方法只应在通知 pod worker 的配置循环内使用，
// 其他组件应以 pod worker 为准。
func (kl *Kubelet) isAdmittedPodTerminal(pod *v1.Pod) bool {
	// 若配置源已观测到终止阶段，则 Pod 视为非活跃
	// （若 kubelet 记录了 Pod 到达终止阶段，则绝不应重启它）
	if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
		return true
	}
	// 在 kubelet 内部被标记为终止的 Pod 也视为非活跃
	// （可能已被 kubelet 准入拒绝）
	if status, ok := kl.statusManager.GetPodStatus(pod.UID); ok {
		if status.Phase == v1.PodSucceeded || status.Phase == v1.PodFailed {
			return true
		}
	}
	return false
}

// removeOrphanedPodStatuses 删除 podStatus 中已不再绑定到本节点的过期条目。
func (kl *Kubelet) removeOrphanedPodStatuses(logger klog.Logger, pods []*v1.Pod, mirrorPods []*v1.Pod) {
	podUIDs := make(map[types.UID]bool)
	for _, pod := range pods {
		podUIDs[pod.UID] = true
	}
	for _, pod := range mirrorPods {
		podUIDs[pod.UID] = true
	}
	kl.statusManager.RemoveOrphanedStatuses(logger, podUIDs)
}

// HandlePodCleanups 执行一系列清理工作：终止 pod worker、杀掉不应存在的 Pod、
// 并清理孤儿卷与 Pod 目录。执行期间不会向 pod worker 下发配置变更，
// 因此不会有新 Pod 出现。方法结束后，kubelet 的期望状态应与 pod worker
// 及其他 Pod 相关组件的实际状态完成对齐。
// 该函数由主同步循环执行，必须快速返回，内部调用都应异步化。
// 耗时的对齐操作应由其他组件（如卷管理器）完成。
// 本次调用的耗时决定了使用固定 UID 的静态 Pod 更新后的最小重启延迟
// （大多数静态 Pod 应使用动态 UID）；执行期间不会向 pod worker
// 下发任何配置更新。
func (kl *Kubelet) HandlePodCleanups(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	// kubelet 没有检查点机制，因此需要先枚举 cgroup 树中的 Pod，
	// 再检查 pod manager 中的 Pod；这样 cgroup 树的视图
	// 不会错误地把之后才加入的 Pod 也算进来。
	var (
		cgroupPods map[types.UID]cm.CgroupName
		err        error
	)
	if kl.cgroupsPerQOS {
		pcm := kl.containerManager.NewPodContainerManager()
		cgroupPods, err = pcm.GetAllPodsFromCgroups(logger)
		if err != nil {
			return fmt.Errorf("failed to get list of pods that still exist on cgroup mounts: %v", err)
		}
	}

	allPods, mirrorPods, orphanedMirrorPodFullnames := kl.podManager.GetPodsAndMirrorPods()

	// Pod 阶段单调推进：一旦到达最终状态，无论重启策略如何都不应再离开该状态；
	// 这类 Pod 的状态不应再被修改，也无需同步。
	// TODO: 这里的逻辑未处理两种情形：
	//   1. 容器死亡后立即被删除时，kubelet 可能无法生成正确的状态，
	//      更谈不上正确过滤；
	//   2. kubelet 在把 Pod 的终止状态写入 apiserver 之前重启，
	//      可能仍会重启一个已终止的 Pod（即便 apiserver 尚未认为它终止）。
	// 给 kubelet 增加检查点可缓解这两种情况。

	// 停止配置源中已不存在的终止 Pod 对应的 worker
	logger.V(3).Info("Clean up pod workers for terminated pods")
	workingPods := kl.podWorkers.SyncKnownPods(logger, allPods)

	// 对齐：此时 pod worker 已裁剪到期望的 Pod 集合；因 UID 复用需要重启的 Pod
	// 或上一轮运行遗留的 Pod，此时尚未被 pod worker 感知。

	allPodsByUID := make(sets.Set[types.UID])
	for _, pod := range allPods {
		allPodsByUID.Insert(pod.UID)
	}

	// 识别拥有 worker 的 Pod 集合：应包括配置中所有未终止的 Pod，
	// 以及已从配置中移除但正在终止的 Pod。正在终止的 Pod
	// 会加入 possiblyRunningPods，以免过早清理其 Pod cgroup。
	stringIfTrue := func(t bool) string {
		if t {
			return "true"
		}
		return ""
	}
	runningPods := make(map[types.UID]sets.Empty)
	possiblyRunningPods := make(map[types.UID]sets.Empty)
	for uid, sync := range workingPods {
		switch sync.State {
		case SyncPod:
			runningPods[uid] = struct{}{}
			possiblyRunningPods[uid] = struct{}{}
		case TerminatingPod:
			possiblyRunningPods[uid] = struct{}{}
		default:
		}
	}

	// 从运行时获取运行中容器列表以执行清理；
	// 需要最新状态，避免延迟复用同一 UID 的静态 Pod 的重启。
	if err := kl.runtimeCache.ForceUpdateIfOlder(ctx, kl.clock.Now()); err != nil {
		logger.Error(err, "Error listing containers")
		return err
	}
	runningRuntimePods, err := kl.runtimeCache.GetPods(ctx)
	if err != nil {
		logger.Error(err, "Error listing containers")
		return err
	}

	// 停止对未运行 Pod 的探测
	logger.V(3).Info("Clean up probes for terminated pods")
	kl.probeManager.CleanupPods(possiblyRunningPods)

	// 删除不在已知配置 Pod 列表中的孤儿 Pod 状态
	logger.V(3).Info("Clean up orphaned pod statuses")
	kl.removeOrphanedPodStatuses(logger, allPods, mirrorPods)
	kl.allocationManager.RemoveOrphanedPods(allPodsByUID)

	// 删除孤儿的 Pod 用户命名空间分配（如有）。
	logger.V(3).Info("Clean up orphaned pod user namespace allocations")
	if err = kl.usernsManager.CleanupOrphanedPodUsernsAllocations(ctx, allPods, runningRuntimePods); err != nil {
		logger.Error(err, "Failed cleaning up orphaned pod user namespaces allocations")
	}

	// 清理已知不再拥有容器的 Pod 的孤儿卷。注意这里传入所有 Pod
	// （包括已终止的 Pod），以免删除已终止但尚未删除的 Pod
	// 所关联的卷。
	// TODO: 未来该方法可以更积极地清理终止 Pod（卷、挂载目录、日志与
	// 容器都可以更好地分离处理）
	logger.V(3).Info("Clean up orphaned pod directories")
	err = kl.cleanupOrphanedPodDirs(logger, allPods, runningRuntimePods)
	if err != nil {
		// 即使某项清理失败，也希望其余清理任务继续执行，
		// 因此这里只记录错误并继续；其他清理任务同样遵循此原则。
		logger.Error(err, "Failed cleaning up orphaned pod directories")
	}

	// 删除所有孤儿 mirror pod（mirror pod 由 pod worker 按名称跟踪）
	logger.V(3).Info("Clean up orphaned mirror pods")
	for _, podFullname := range orphanedMirrorPodFullnames {
		if !kl.podWorkers.IsPodForMirrorPodTerminatingByFullName(podFullname) {
			_, err := kl.mirrorPodClient.DeleteMirrorPod(ctx, podFullname, nil)
			if err != nil {
				logger.Error(err, "Encountered error when deleting mirror pod", "podName", podFullname)
			} else {
				logger.V(3).Info("Deleted mirror pod", "podName", podFullname)
			}
		}
	}

	// 裁剪已终止 Pod 的 worker 后，获取活跃 Pod 列表，用于统计指标并判断重启
	activePods := kl.filterOutInactivePods(allPods)
	allRegularPods, allStaticPods := splitPodsByStatic(allPods)
	activeRegularPods, activeStaticPods := splitPodsByStatic(activePods)
	metrics.DesiredPodCount.WithLabelValues("").Set(float64(len(allRegularPods)))
	metrics.DesiredPodCount.WithLabelValues("true").Set(float64(len(allStaticPods)))
	metrics.ActivePodCount.WithLabelValues("").Set(float64(len(activeRegularPods)))
	metrics.ActivePodCount.WithLabelValues("true").Set(float64(len(activeStaticPods)))
	metrics.MirrorPodCount.Set(float64(len(mirrorPods)))

	// 此时 pod worker 已通过 SyncKnownPods 知道哪些 Pod 不再需要；
	// 接下来在活跃 Pod 中查找 pod worker 尚未感知的 Pod 并下发更新。
	// 最常见的原因是 Pod 被删除后以相同 UID 重建（对 API Pod 极罕见，
	// 对使用固定 UID 的静态 Pod 则很常见）。上一轮执行可能仍在运行的容器
	// 必须由 pod worker 的 sync 方法对齐处理。
	// 必须使用活跃 Pod，因为那才是已准入的 Pod 集合（podManager 还包含
	// 永远不会运行的 Pod，statusManager 则跟踪已被拒绝的 Pod）。
	var restartCount, restartCountStatic int
	for _, desiredPod := range activePods {
		if _, knownPod := workingPods[desiredPod.UID]; knownPod {
			continue
		}

		logger.V(3).Info("Pod will be restarted because it is in the desired set and not known to the pod workers (likely due to UID reuse)", "podUID", desiredPod.UID)
		isStatic := kubetypes.IsStaticPod(desiredPod)
		pod, mirrorPod, wasMirror := kl.podManager.GetPodAndMirrorPod(desiredPod)
		if pod == nil || wasMirror {
			logger.V(2).Info("Programmer error, restartable pod was a mirror pod but activePods should never contain a mirror pod", "podUID", desiredPod.UID)
			continue
		}
		kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
			UpdateType: kubetypes.SyncPodCreate,
			Pod:        pod,
			MirrorPod:  mirrorPod,
		})

		// 现在期望的 Pod 也已被感知
		workingPods[desiredPod.UID] = PodWorkerSync{State: SyncPod, HasConfig: true, Static: isStatic}
		if isStatic {
			// 可重启的静态 Pod 属于正常情况
			restartCountStatic++
		} else {
			// 这几乎可以确定是异常行为：API Pod 删除重建后不应保留相同 UID，
			// 除非发生了严重的 API 违规
			restartCount++
		}
	}
	metrics.RestartedPodTotal.WithLabelValues("true").Add(float64(restartCountStatic))
	metrics.RestartedPodTotal.WithLabelValues("").Add(float64(restartCount))

	// 完成对已删除但非运行时 Pod（没有运行中容器）、已终止且不被 pod worker
	// 所感知的 Pod 的终止。
	// 例如在 kubelet 准入阶段被拒绝、从未启动过的 Pod（没有孤儿 Pod）。
	// 通过向 pod worker 发送 SyncPodKill 可以推进这类 Pod 的强制删除，
	// 同时避免下一次 HandlePodCleanups 再次进入相同流程。
	for _, pod := range kl.filterTerminalPodsToDelete(allPods, runningRuntimePods, workingPods) {
		logger.V(3).Info("Handling termination and deletion of the pod to pod workers", "pod", klog.KObj(pod), "podUID", pod.UID)
		kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
			UpdateType: kubetypes.SyncPodKill,
			Pod:        pod,
		})
	}

	// 最后，终止运行时中存在但不在配置已知运行 Pod 列表中的 Pod：
	// 这类运行中运行时 Pod 的终止会异步发生，
	// 并会在下一次 HandlePodCleanups 中处理。
	var orphanCount int
	for _, runningPod := range runningRuntimePods {
		// 若 CRI 中存在 pod worker 未知的孤儿 Pod 资源，则立即终止；
		// 由于 housekeeping 与其他 pod worker 更新互斥，可以确定期间没有
		// 新 Pod 加入。注意在运行时不可见但此前已知的 Pod
		// 由 SyncKnownPods() 负责终止。
		_, knownPod := workingPods[runningPod.ID]
		if !knownPod {
			one := int64(1)
			killPodOptions := &KillPodOptions{
				PodTerminationGracePeriodSecondsOverride: &one,
			}
			logger.V(2).Info("Clean up containers for orphaned pod we had not seen before", "podUID", runningPod.ID, "killPodOptions", killPodOptions)
			kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
				UpdateType:     kubetypes.SyncPodKill,
				RunningPod:     runningPod,
				KillPodOptions: killPodOptions,
			})

			// 运行中的 Pod 现在也已被感知
			workingPods[runningPod.ID] = PodWorkerSync{State: TerminatingPod, Orphan: true}
			orphanCount++
		}
	}
	metrics.OrphanedRuntimePodTotal.Add(float64(orphanCount))

	// 记录完正在终止的 Pod、并补入应当运行的新 Pod 后，
	// 在这里记录汇总。并非所有 PodWorkerSync 取值组合都是合法的。
	counts := make(map[PodWorkerSync]int)
	for _, sync := range workingPods {
		counts[sync]++
	}
	for validSync, configState := range map[PodWorkerSync]string{
		{HasConfig: true, Static: true}:                "desired",
		{HasConfig: true, Static: false}:               "desired",
		{Orphan: true, HasConfig: true, Static: true}:  "orphan",
		{Orphan: true, HasConfig: true, Static: false}: "orphan",
		{Orphan: true, HasConfig: false}:               "runtime_only",
	} {
		for _, state := range []PodWorkerState{SyncPod, TerminatingPod, TerminatedPod} {
			validSync.State = state
			count := counts[validSync]
			delete(counts, validSync)
			staticString := stringIfTrue(validSync.Static)
			if !validSync.HasConfig {
				staticString = "unknown"
			}
			metrics.WorkingPodCount.WithLabelValues(state.String(), configState, staticString).Set(float64(count))
		}
	}
	if len(counts) > 0 {
		// 以防遗漏某种组合
		logger.V(3).Info("Programmer error, did not report a kubelet_working_pods metric for a value returned by SyncKnownPods", "counts", counts)
	}

	// 清理层级中那些确定已不再运行的 Pod（容器运行时中不存在）的 cgroup
	if kl.cgroupsPerQOS {
		pcm := kl.containerManager.NewPodContainerManager()
		logger.V(3).Info("Clean up orphaned pod cgroups")
		kl.cleanupOrphanedPodCgroups(logger, pcm, cgroupPods, possiblyRunningPods)
	}

	// 清理所有退避记录。
	kl.crashLoopBackOff.GC()
	return nil
}

// filterTerminalPodsToDelete 返回可由 status manager 删除、但不在 pod worker 中的终止 Pod。
// 首先，检查 deletionTimestamp 只是性能优化，
// 没有删除时间戳的终止 Pod 无需处理；
// 其次，检查终止状态是为了避免对尚未加入 pod worker 的 Pending Pod
// 误触发删除；
// 第三，跳过 pod worker 已知的 Pod，因为其生命周期已由 pod worker 处理；
// 最后，跳过运行时 Pod，其终止由 HandlePodCleanups 中专门的分支处理。
func (kl *Kubelet) filterTerminalPodsToDelete(allPods []*v1.Pod, runningRuntimePods []*kubecontainer.Pod, workingPods map[types.UID]PodWorkerSync) map[types.UID]*v1.Pod {
	terminalPodsToDelete := make(map[types.UID]*v1.Pod)
	for _, pod := range allPods {
		if pod.DeletionTimestamp == nil {
			// 跳过没有删除时间戳的 Pod
			continue
		}
		if !podutil.IsPodPhaseTerminal(pod.Status.Phase) {
			// 跳过非终止状态的 Pod
			continue
		}
		if _, knownPod := workingPods[pod.UID]; knownPod {
			// 跳过 pod worker 已知的 Pod
			continue
		}
		terminalPodsToDelete[pod.UID] = pod
	}
	for _, runningRuntimePod := range runningRuntimePods {
		// 跳过运行中的运行时 Pod：它们由专门的分支负责终止容器
		delete(terminalPodsToDelete, runningRuntimePod.ID)
	}
	return terminalPodsToDelete
}

// splitPodsByStatic 把 pod manager 中的期望 Pod 分为普通 Pod 与静态 Pod。
// mirror pod 不是合法配置源（创建 mirror pod 不能促使 kubelet
// 启动静态 Pod），因此会被排除。
func splitPodsByStatic(pods []*v1.Pod) (regular, static []*v1.Pod) {
	regular, static = make([]*v1.Pod, 0, len(pods)), make([]*v1.Pod, 0, len(pods))
	for _, pod := range pods {
		if kubetypes.IsMirrorPod(pod) {
			continue
		}
		if kubetypes.IsStaticPod(pod) {
			static = append(static, pod)
		} else {
			regular = append(regular, pod)
		}
	}
	return regular, static
}

// validateContainerLogStatus 根据容器状态返回用于取日志的容器 ID：
// previous 为 true 时只返回上一个已终止容器的日志，否则优先返回当前运行容器、
// 其次才回退到上一次终止的容器；容器信息不可用时会返回具体错误。
func (kl *Kubelet) validateContainerLogStatus(logger klog.Logger, podName string, podStatus *v1.PodStatus, containerName string, previous bool) (containerID kubecontainer.ContainerID, err error) {
	var cID string

	cStatus, found := podutil.GetContainerStatus(podStatus.ContainerStatuses, containerName)
	if !found {
		cStatus, found = podutil.GetContainerStatus(podStatus.InitContainerStatuses, containerName)
	}
	if !found {
		cStatus, found = podutil.GetContainerStatus(podStatus.EphemeralContainerStatuses, containerName)
	}
	if !found {
		return kubecontainer.ContainerID{}, fmt.Errorf("container %q in pod %q is not available", containerName, podName)
	}
	lastState := cStatus.LastTerminationState
	waiting, running, terminated := cStatus.State.Waiting, cStatus.State.Running, cStatus.State.Terminated

	switch {
	case previous:
		if lastState.Terminated == nil || lastState.Terminated.ContainerID == "" {
			return kubecontainer.ContainerID{}, fmt.Errorf("previous terminated container %q in pod %q not found", containerName, podName)
		}
		cID = lastState.Terminated.ContainerID

	case running != nil:
		cID = cStatus.ContainerID

	case terminated != nil:
		// 当下一个容器未启动时，terminated.ContainerID 为空，此时从 lastState.Terminated 取日志
		if terminated.ContainerID == "" {
			if lastState.Terminated != nil && lastState.Terminated.ContainerID != "" {
				cID = lastState.Terminated.ContainerID
			} else {
				return kubecontainer.ContainerID{}, fmt.Errorf("container %q in pod %q is terminated", containerName, podName)
			}
		} else {
			cID = terminated.ContainerID
		}

	case lastState.Terminated != nil:
		if lastState.Terminated.ContainerID == "" {
			return kubecontainer.ContainerID{}, fmt.Errorf("container %q in pod %q is terminated", containerName, podName)
		}
		cID = lastState.Terminated.ContainerID

	case waiting != nil:
		// 针对最常见的 pending 失败输出一些信息
		switch reason := waiting.Reason; reason {
		case images.ErrImagePull.Error():
			return kubecontainer.ContainerID{}, fmt.Errorf("container %q in pod %q is waiting to start: image can't be pulled", containerName, podName)
		case images.ErrImagePullBackOff.Error():
			return kubecontainer.ContainerID{}, fmt.Errorf("container %q in pod %q is waiting to start: trying and failing to pull image", containerName, podName)
		default:
			return kubecontainer.ContainerID{}, fmt.Errorf("container %q in pod %q is waiting to start: %v", containerName, podName, reason)
		}
	default:
		// 无法识别的状态
		return kubecontainer.ContainerID{}, fmt.Errorf("container %q in pod %q is waiting to start - no logs yet", containerName, podName)
	}

	return kubecontainer.ParseContainerID(logger, cID), nil
}

// GetKubeletContainerLogs 返回容器的日志。
// TODO: 该方法目前返回随机一次容器尝试的日志，应改为返回最近一次尝试
// 或全部尝试的日志。
func (kl *Kubelet) GetKubeletContainerLogs(ctx context.Context, podFullName, containerName string, logOptions *v1.PodLogOptions, stdout, stderr io.Writer) error {
	// Pod worker 会定期把状态写入 statusManager；若其中没有缓存，
	// 说明出了问题（或 kubelet 刚重启尚未完成同步），
	// 此时直接认为 Pod 尚未就绪。
	name, namespace, err := kubecontainer.ParsePodFullName(podFullName)
	if err != nil {
		return fmt.Errorf("unable to parse pod full name %q: %v", podFullName, err)
	}

	pod, ok := kl.GetPodByName(namespace, name)
	if !ok {
		return fmt.Errorf("pod %q cannot be found - no logs available", name)
	}

	// TODO: 应以 podWorker 的 Pod store 为准，因为 mirrorPod 可能仍存在，
	// Pod 也可能已被强制删除但仍在终止中（用户应能基于全名
	// 查看被强制删除的静态 Pod 日志）。
	var podUID types.UID
	pod, mirrorPod, wasMirror := kl.podManager.GetPodAndMirrorPod(pod)
	if wasMirror {
		if pod == nil {
			return fmt.Errorf("mirror pod %q does not have a corresponding pod", name)
		}
		podUID = mirrorPod.UID
	} else {
		podUID = pod.UID
	}

	podStatus, found := kl.statusManager.GetPodStatus(podUID)
	if !found {
		// 若没有缓存状态，则使用配置源（apiserver）中的状态，
		// 这在 kubelet 刚重启时很有用。
		podStatus = pod.Status
	}

	// TODO: 与 kuberuntime.GetContainerLogs 的逻辑合并：这里把容器名转成容器 ID，
	// 而 kuberuntime 内又把容器 ID 转回容器名与重启次数。
	// TODO: 容器日志生命周期独立管理后，应基于现有日志文件取日志，
	// 而不是依赖容器状态。
	containerID, err := kl.validateContainerLogStatus(klog.FromContext(ctx), pod.Name, &podStatus, containerName, logOptions.Previous)
	if err != nil {
		return err
	}

	// 自 v1.32 起，若未请求该输出流，stdout 可能为 nil。
	if stdout != nil {
		// 交给容器运行时前先向 stdout 写 0 字节，
		// 确保开始复制时至少发生一次 Write 调用，
		// 即使随后会阻塞等待容器输出日志。
		if _, err := stdout.Write([]byte{}); err != nil {
			return err
		}
	}

	return kl.containerRuntime.GetContainerLogs(ctx, pod, containerID, logOptions, stdout, stderr)
}

// getPhase 根据容器信息计算 Pod 的阶段。
func getPhase(logger klog.Logger, pod *v1.Pod, info []v1.ContainerStatus, podIsTerminal, podHasInitialized bool) v1.PodPhase {
	spec := pod.Spec
	pendingRestartableInitContainers := 0
	pendingRegularInitContainers := 0
	failedInitialization := 0
	failedInitializationNotRestartable := 0

	// 普通 init 容器
	for _, container := range spec.InitContainers {
		if podutil.IsRestartableInitContainer(&container) {
			// 这里跳过可重启 init 容器，因为它们在 Pod 阶段判定上
			// 与普通 init 容器略有不同，需单独处理。
			continue
		}

		containerStatus, ok := podutil.GetContainerStatus(info, container.Name)
		if !ok {
			pendingRegularInitContainers++
			continue
		}

		switch {
		case containerStatus.State.Running != nil:
			pendingRegularInitContainers++
		case containerStatus.State.Terminated != nil:
			exitCode := containerStatus.State.Terminated.ExitCode
			if exitCode != 0 {
				failedInitialization++
				if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
					restartable := podutil.ContainerShouldRestart(container, pod.Spec, exitCode)
					if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
						restartable = restartable || containerStatus.State.Terminated.Reason == RestartingAllContainers
					}
					if !restartable {
						failedInitializationNotRestartable++
					}
				}
			}
		case containerStatus.State.Waiting != nil:
			if containerStatus.LastTerminationState.Terminated != nil {
				exitCode := containerStatus.LastTerminationState.Terminated.ExitCode
				if exitCode != 0 {
					failedInitialization++
					restartable := podutil.ContainerShouldRestart(container, pod.Spec, exitCode)
					if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
						restartable = restartable || containerStatus.LastTerminationState.Terminated.Reason == RestartingAllContainers
					}
					if !restartable {
						failedInitializationNotRestartable++
					}
				}
			} else {
				pendingRegularInitContainers++
			}
		default:
			pendingRegularInitContainers++
		}
	}

	// 可重启 init 容器与普通容器的计数器
	unknown := 0
	running := 0
	waiting := 0
	stopped := 0
	stoppedNotRestartable := 0
	succeeded := 0

	// sidecar（可重启）init 容器
	for _, container := range spec.InitContainers {
		if !podutil.IsRestartableInitContainer(&container) {
			// 普通 init 容器已在上面处理，这里跳过。
			continue
		}
		containerStatus, ok := podutil.GetContainerStatus(info, container.Name)
		if !ok {
			unknown++
			continue
		}

		switch {
		case containerStatus.State.Running != nil:
			if containerStatus.Started == nil || !*containerStatus.Started {
				pendingRestartableInitContainers++
			}
			running++
		case containerStatus.State.Terminated != nil:
			// 此处不做处理：已终止的可重启 init 容器
			// 不参与 Pod 阶段判定。
		case containerStatus.State.Waiting != nil:
			if containerStatus.LastTerminationState.Terminated != nil {
				// 此处不做处理：已终止的可重启 init 容器
				// 不参与 Pod 阶段判定。
			} else {
				pendingRestartableInitContainers++
				waiting++
			}
		default:
			pendingRestartableInitContainers++
			unknown++
		}
	}

	for _, container := range spec.Containers {
		containerStatus, ok := podutil.GetContainerStatus(info, container.Name)
		if !ok {
			unknown++
			continue
		}

		switch {
		case containerStatus.State.Running != nil:
			running++
		case containerStatus.State.Terminated != nil:
			stopped++
			exitCode := containerStatus.State.Terminated.ExitCode
			if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
				restartable := podutil.ContainerShouldRestart(container, pod.Spec, exitCode)
				if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
					restartable = restartable || containerStatus.State.Terminated.Reason == RestartingAllContainers
				}
				if !restartable {
					stoppedNotRestartable++
				}
			}
			if exitCode == 0 {
				succeeded++
			}
		case containerStatus.State.Waiting != nil:
			if containerStatus.LastTerminationState.Terminated != nil {
				stopped++
				if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
					exitCode := containerStatus.LastTerminationState.Terminated.ExitCode
					restartable := podutil.ContainerShouldRestart(container, pod.Spec, exitCode)
					if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
						restartable = restartable || containerStatus.LastTerminationState.Terminated.Reason == RestartingAllContainers
					}
					if !restartable {
						stoppedNotRestartable++
					}
				}
			} else {
				waiting++
			}
		default:
			unknown++
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
		if failedInitializationNotRestartable > 0 {
			return v1.PodFailed
		}
	} else {
		if failedInitialization > 0 && spec.RestartPolicy == v1.RestartPolicyNever {
			return v1.PodFailed
		}
	}

	switch {
	case pendingRegularInitContainers > 0 ||
		(pendingRestartableInitContainers > 0 &&
			// 这样可以处理 Pod 已初始化、但可重启 init 容器正在重启的场景，
			// 此时不应因为 sidecar 重启就把 Pod 退回 Pending（普通容器已运行过）。
			!podHasInitialized):
		fallthrough
	case waiting > 0:
		logger.V(5).Info("Pod waiting > 0, pending")
		// 有一个或多个容器尚未启动
		return v1.PodPending
	case running > 0 && unknown == 0:
		// 所有容器都已启动，且至少有一个容器正在运行
		return v1.PodRunning
	case running == 0 && stopped > 0 && unknown == 0:
		// Pod 已处于终止状态，无论重启策略如何容器都不会再重启。
		if podIsTerminal {
			// TODO(#116484): 静态 Pod 也应赋予终止阶段。
			if !kubetypes.IsStaticPod(pod) {
				// 所有普通容器均成功终止，且所有可重启 init 容器均已停止，
				if stopped == succeeded {
					return v1.PodSucceeded
				}
				// 至少存在一个失败
				return v1.PodFailed
			}
		}
		// 所有容器均已终止
		if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
			if stopped != stoppedNotRestartable {
				// 至少有一个容器正在重启
				return v1.PodRunning
			}
			if stopped == succeeded {
				return v1.PodSucceeded
			}
			return v1.PodFailed
		}
		if spec.RestartPolicy == v1.RestartPolicyAlways {
			// 所有容器都在重启过程中
			return v1.PodRunning
		}
		if stopped == succeeded {
			// 重启策略不是 Always，所有容器均成功终止，
			// 且所有可重启 init 容器均已停止。
			return v1.PodSucceeded
		}
		if spec.RestartPolicy == v1.RestartPolicyNever {
			// 重启策略为 Never，所有容器均已终止且至少一个失败
			return v1.PodFailed
		}
		// 重启策略为 OnFailure，至少一个容器失败并正在重启
		return v1.PodRunning
	default:
		logger.V(5).Info("Pod default case, pending")
		return v1.PodPending
	}
}

// determinePodResizeStatus 返回该 Pod 当前的扩缩容条件；Pod 已终止时清除并返回 nil
func (kl *Kubelet) determinePodResizeStatus(allocatedPod *v1.Pod, podIsTerminal bool) []*v1.PodCondition {
	// Pod 终止时清除 resize 状态。
	if podIsTerminal {
		kl.statusManager.ClearPodResizeInProgressCondition(allocatedPod.UID)
		kl.statusManager.ClearPodResizePendingCondition(allocatedPod.UID, metrics.DeferredResizeResolutionTerminated)
		return nil
	}

	resizeStatus := kl.statusManager.GetPodResizeConditions(allocatedPod.UID)
	return resizeStatus
}

// generateAPIPodStatus 根据内部 Pod 状态生成最终的 API Pod 状态。
// 该方法只应在 sync*Pod 方法内部调用。
func (kl *Kubelet) generateAPIPodStatus(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, podIsTerminal bool) v1.PodStatus {
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Generating pod status", "podIsTerminal", podIsTerminal, "pod", klog.KObj(pod))
	// 以上一次 Pod 状态（若不存在则用 API 中的状态）作为本次生成的基础
	oldPodStatus, found := kl.statusManager.GetPodStatus(pod.UID)
	if !found {
		oldPodStatus = pod.Status
	}
	s := kl.convertStatusToAPIStatus(ctx, pod, podStatus, oldPodStatus)
	// 计算下一阶段并保留 reason
	allStatus := append(append([]v1.ContainerStatus{}, s.ContainerStatuses...), s.InitContainerStatuses...)
	s.Phase = getPhase(logger, pod, allStatus, podIsTerminal, kubecontainer.HasAnyActiveRegularContainerStarted(&pod.Spec, podStatus))
	logger.V(4).Info("Got phase for pod", "pod", klog.KObj(pod), "oldPhase", oldPodStatus.Phase, "phase", s.Phase)

	// 在 status manager、运行时和本次生成的状态之间做三方合并，
	// 确保终止状态被正确设置。
	if s.Phase != v1.PodFailed && s.Phase != v1.PodSucceeded {
		switch {
		case oldPodStatus.Phase == v1.PodFailed || oldPodStatus.Phase == v1.PodSucceeded:
			logger.V(4).Info("Status manager phase was terminal, updating phase to match", "pod", klog.KObj(pod), "phase", oldPodStatus.Phase)
			s.Phase = oldPodStatus.Phase
		case pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded:
			logger.V(4).Info("API phase was terminal, updating phase to match", "pod", klog.KObj(pod), "phase", pod.Status.Phase)
			s.Phase = pod.Status.Phase
		}
	}

	if s.Phase == oldPodStatus.Phase {
		// 阶段未变化时保留与该阶段关联的 reason 与 message
		s.Reason = oldPodStatus.Reason
		s.Message = oldPodStatus.Message
		if len(s.Reason) == 0 {
			s.Reason = pod.Status.Reason
		}
		if len(s.Message) == 0 {
			s.Message = pod.Status.Message
		}
	}

	// 检查是否有内部模块请求驱逐该 Pod，并覆盖 reason 与 message
	for _, podSyncHandler := range kl.PodSyncHandlers {
		if result := podSyncHandler.ShouldEvict(pod); result.Evict {
			s.Phase = v1.PodFailed
			s.Reason = result.Reason
			s.Message = result.Message
			break
		}
	}

	// Pod 不允许从终止阶段退出
	if pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded {
		// apiserver 显示的是终止阶段，不允许再变更
		if s.Phase != pod.Status.Phase {
			logger.Error(nil, "Pod attempted illegal phase transition", "pod", klog.KObj(pod), "originalStatusPhase", pod.Status.Phase, "apiStatusPhase", s.Phase, "apiStatus", s)
			// 强制回到 apiserver 中的阶段
			s.Phase = pod.Status.Phase
		}
	}

	kl.probeManager.UpdatePodStatus(ctx, pod, s)

	// 更新已分配资源状态
	if utilfeature.DefaultFeatureGate.Enabled(features.ResourceHealthStatus) {
		kl.containerManager.UpdateAllocatedResourcesStatus(logger, pod, s)
	}

	// 保留所有非 kubelet 拥有的 Pod 条件
	s.Conditions = make([]v1.PodCondition, 0, len(pod.Status.Conditions)+1)
	for _, c := range pod.Status.Conditions {
		if !kubetypes.PodConditionByKubelet(c.Type) {
			s.Conditions = append(s.Conditions, c)
		}
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		resizeStatus := kl.determinePodResizeStatus(pod, podIsTerminal)
		for _, c := range resizeStatus {
			// 仅当特性开关关闭且该条件的 observed generation 尚未设置时，
			// 才清零 observedGeneration；其他情况下不覆盖它，
			// 因为该条件可能反映的是更早的 podspec。
			gen := podutil.CalculatePodConditionObservedGeneration(&oldPodStatus, pod.Generation, c.Type)
			if gen == 0 {
				c.ObservedGeneration = 0
			}
			s.Conditions = append(s.Conditions, *c)
		}
	}

	// 从状态中复制 Pod 中断条件：这些条件在驱逐（节点资源压力或
	// 节点优雅关闭）期间已更新，
	// 不应根据容器状态重新生成，因为它们基于一次性事件。
	cType := v1.DisruptionTarget
	if _, condition := podutil.GetPodConditionFromList(oldPodStatus.Conditions, cType); condition != nil {
		s.Conditions = utilpod.ReplaceOrAppendPodCondition(s.Conditions, condition)
	}

	// 设置所有由 kubelet 拥有的 Pod 条件
	if utilfeature.DefaultFeatureGate.Enabled(features.PodReadyToStartContainersCondition) {
		s.Conditions = append(s.Conditions, status.GeneratePodReadyToStartContainersCondition(pod, &oldPodStatus, podStatus))
	}
	allContainerStatuses := append(s.InitContainerStatuses, s.ContainerStatuses...)
	s.Conditions = append(s.Conditions, status.GeneratePodInitializedCondition(pod, &oldPodStatus, allContainerStatuses, s.Phase))
	s.Conditions = append(s.Conditions, status.GeneratePodReadyCondition(pod, &oldPodStatus, s.Conditions, allContainerStatuses, s.Phase))
	s.Conditions = append(s.Conditions, status.GenerateContainersReadyCondition(pod, &oldPodStatus, allContainerStatuses, s.Phase))
	s.Conditions = append(s.Conditions, v1.PodCondition{
		Type:               v1.PodScheduled,
		ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(&oldPodStatus, pod.Generation, v1.PodScheduled),
		Status:             v1.ConditionTrue,
	})
	if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
		if podutil.AllContainersCouldRestart(&pod.Spec) {
			s.Conditions = append(s.Conditions, status.GenerateAllContainersRestartingCondition(pod, podStatus, &oldPodStatus, s.Phase))
		}
	}
	// 设置 HostIP/HostIPs，并为使用主机网络的 Pod 初始化 PodIP/PodIPs
	if kl.kubeClient != nil {
		hostIPs, err := kl.getHostIPsAnyWay(ctx)
		if err != nil {
			logger.V(4).Info("Cannot get host IPs", "err", err)
		} else {
			if s.HostIP != "" {
				if utilnet.IPFamilyOfString(s.HostIP) != utilnet.IPFamilyOf(hostIPs[0]) {
					kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, "HostIPsIPFamilyMismatch",
						"Kubelet detected an IPv%s node IP (%s), but the cloud provider selected an IPv%s node IP (%s); pass an explicit `--node-ip` to kubelet to fix this.",
						utilnet.IPFamilyOfString(s.HostIP), s.HostIP, utilnet.IPFamilyOf(hostIPs[0]), hostIPs[0].String())
				}
			}
			s.HostIP = hostIPs[0].String()
			s.HostIPs = []v1.HostIP{{IP: s.HostIP}}
			if len(hostIPs) == 2 {
				s.HostIPs = append(s.HostIPs, v1.HostIP{IP: hostIPs[1].String()})
			}

			// 使用主机网络的 Pod 以节点 IP 作为 PodIP，一经设置便不可变；
			// 例外是节点变为双栈时会补充第二个 IP。
			if kubecontainer.IsHostNetworkPod(pod) {
				// 主 IP 尚未设置
				if s.PodIP == "" {
					s.PodIP = hostIPs[0].String()
					s.PodIPs = []v1.PodIP{{IP: s.PodIP}}
				}
				// 第二个 IP 尚未设置（#105320）
				if len(hostIPs) == 2 && len(s.PodIPs) == 1 {
					if utilnet.IPFamilyOfString(s.PodIPs[0].IP) != utilnet.IPFamilyOf(hostIPs[1]) {
						s.PodIPs = append(s.PodIPs, v1.PodIP{IP: hostIPs[1].String()})
					}
				}
			}
		}
	}

	return *s
}

// sortPodIPs 按集群 IP 族偏好对 PodIPs 排序并截断。
// 运行时 Pod 状态可能包含任意数量、任意顺序的 IP。
// PodIPs 由 kubeGenericRuntimeManager.determinePodSandboxIPs() 获得；
// 先选取与节点 IP 同族的第一个 IP，
// 再选取相反 IP 族的第一个 IP（若有），
// 用于 Pod.Status.PodIPs 与向下 API 环境变量。
func (kl *Kubelet) sortPodIPs(podIPs []string) []string {
	ips := make([]string, 0, 2)
	var validPrimaryIP, validSecondaryIP func(ip net.IP) bool
	if len(kl.nodeIPs) == 0 || utilnet.IsIPv4(kl.nodeIPs[0]) {
		validPrimaryIP = utilnet.IsIPv4
		validSecondaryIP = utilnet.IsIPv6
	} else {
		validPrimaryIP = utilnet.IsIPv6
		validSecondaryIP = utilnet.IsIPv4
	}

	// 重新解析并格式化 IP，以防 CRI 返回值格式不规范。
	for _, ipStr := range podIPs {
		ip := utilnet.ParseIPSloppy(ipStr)
		if validPrimaryIP(ip) {
			ips = append(ips, ip.String())
			break
		}
	}
	for _, ipStr := range podIPs {
		ip := utilnet.ParseIPSloppy(ipStr)
		if validSecondaryIP(ip) {
			ips = append(ips, ip.String())
			break
		}
	}
	return ips
}

// convertStatusToAPIStatus 根据内部 Pod 状态与 API 中的历史状态，
// 初始化 API 的 PodStatus。
// 它只做转换，不会修改 kubelet 自身状态。
func (kl *Kubelet) convertStatusToAPIStatus(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, oldPodStatus v1.PodStatus) *v1.PodStatus {
	logger := klog.FromContext(ctx)
	var apiPodStatus v1.PodStatus

	// 复制 Pod 状态中的 IP，避免与 PodStatus 产生竞态（#102806）
	podIPs := make([]string, len(podStatus.IPs))
	copy(podIPs, podStatus.IPs)

	// 使 podIPs 顺序与节点 IP 族偏好一致（#97979）
	podIPs = kl.sortPodIPs(podIPs)
	for _, ip := range podIPs {
		apiPodStatus.PodIPs = append(apiPodStatus.PodIPs, v1.PodIP{IP: ip})
	}
	if len(apiPodStatus.PodIPs) > 0 {
		apiPodStatus.PodIP = apiPodStatus.PodIPs[0].IP
	}

	// 为旧版本（1.6 之前）创建的 Pod 补充状态
	apiPodStatus.QOSClass = v1qos.GetPodQOS(pod)

	podRestarting := false
	if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
		podRestarting = kubecontainer.ShouldAllContainersRestart(pod, podStatus, &oldPodStatus)
	}

	var imageVolumeNames sets.Set[string]
	if utilfeature.DefaultFeatureGate.Enabled(features.ImageVolumeWithDigest) {
		imageVolumeNames = sets.New[string]()
		for _, podVolume := range pod.Spec.Volumes {
			if podVolume.Image == nil {
				continue
			}
			imageVolumeNames.Insert(podVolume.Name)
		}
	}

	apiPodStatus.ContainerStatuses = kl.convertToAPIContainerStatuses(
		ctx,
		pod, podStatus,
		oldPodStatus.ContainerStatuses,
		pod.Spec.Containers,
		imageVolumeNames,
		len(pod.Spec.InitContainers) > 0,
		false,
		podRestarting,
	)
	apiPodStatus.InitContainerStatuses = kl.convertToAPIContainerStatuses(
		ctx,
		pod, podStatus,
		oldPodStatus.InitContainerStatuses,
		pod.Spec.InitContainers,
		imageVolumeNames,
		len(pod.Spec.InitContainers) > 0,
		true,
		podRestarting,
	)
	var ecSpecs []v1.Container
	for i := range pod.Spec.EphemeralContainers {
		ecSpecs = append(ecSpecs, v1.Container(pod.Spec.EphemeralContainers[i].EphemeralContainerCommon))
	}

	// #80875: 目前已遍历 podStatus 三次，未来可重构为
	// 单次遍历 ContainerStatuses
	apiPodStatus.EphemeralContainerStatuses = kl.convertToAPIContainerStatuses(
		ctx,
		pod, podStatus,
		oldPodStatus.EphemeralContainerStatuses,
		ecSpecs,
		imageVolumeNames,
		len(pod.Spec.InitContainers) > 0,
		false,
		podRestarting,
	)

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
		apiPodStatus.Resources = kl.convertToAPIPodLevelResourcesStatus(logger, pod, oldPodStatus)
		opts := resourcehelper.PodResourcesOptions{
			SkipPodLevelResources:                    !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
			UseDRANodeAllocatableResourceClaimStatus: utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources),
		}
		apiPodStatus.AllocatedResources = resourcehelper.PodRequests(pod, opts)
	}

	return &apiPodStatus
}

// getEffectiveAllocatedResources 基于分配后的 Pod 计算实际生效的请求与上限，
// 并保留 Pod 级资源（若已启用）
func getEffectiveAllocatedResources(allocatedPod *v1.Pod) *v1.ResourceRequirements {
	allocatedResources := allocatedPod.Spec.Resources.DeepCopy()
	if allocatedResources == nil {
		allocatedResources = &v1.ResourceRequirements{}
	}
	opts := resourcehelper.PodResourcesOptions{
		SkipPodLevelResources:                    !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
		UseDRANodeAllocatableResourceClaimStatus: utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources),
	}
	allocatedResources.Requests = resourcehelper.PodRequests(allocatedPod, opts)
	allocatedResources.Limits = resourcehelper.PodLimits(allocatedPod, opts)
	return allocatedResources
}

// convertToAPIPodLevelResourcesStatus 生成 Pod 级资源的状态值：
// 运行中的 Pod 以 cgroup 读回的实际值为准，否则回退到分配值；
// 处于最小值附近的 CPU 值会保留分配值，避免取整造成抖动
func (kl *Kubelet) convertToAPIPodLevelResourcesStatus(logger klog.Logger, allocatedPod *v1.Pod, oldPodStatus v1.PodStatus) *v1.ResourceRequirements {
	if allocatedPod.Status.Phase != v1.PodRunning {
		return getEffectiveAllocatedResources(allocatedPod)
	}

	// TODO(ndixita): 重新评估是否应在 Pod Status 中输出 cgroup 数值。
	// 内部取整逻辑会带来细微波动，
	// 导致输出不稳定；需要判断这些信息是否足够有用，
	// 以抵消一致性方面的问题。
	pcm := kl.containerManager.NewPodContainerManager()
	memoryConfig, err := pcm.GetPodCgroupConfig(allocatedPod, v1.ResourceMemory)
	if err != nil {
		logger.Error(err, "failed to read memory cgroup config for the pod", "podName", allocatedPod.Name)
	}
	memoryLimit := cm.MemoryLimitsFromConfig(memoryConfig)
	cpuConfig, err := pcm.GetPodCgroupConfig(allocatedPod, v1.ResourceCPU)
	if err != nil {
		logger.Error(err, "failed to read memory cgroup limits for the pod", "podName", allocatedPod.Name)

	}

	cpuRequest := cm.CPURequestsFromConfig(cpuConfig)
	cpuLimit := cm.CPULimitsFromConfig(cpuConfig)

	preserveOldResourcesValue := func(rName v1.ResourceName, oldStatusResource, resource v1.ResourceList) {
		if allocatedPod.Status.Phase == v1.PodRunning && oldPodStatus.Phase == v1.PodRunning && oldPodStatus.Resources != nil {
			if r, exists := oldStatusResource[rName]; exists {
				resource[rName] = r.DeepCopy()
			}
		}
	}

	resources := allocatedPod.Spec.Resources.DeepCopy()

	if oldPodStatus.Resources == nil {
		oldPodStatus.Resources = &v1.ResourceRequirements{}
	}

	if resources == nil {
		resources = &v1.ResourceRequirements{}
	}

	if resources.Requests == nil {
		resources.Requests = make(v1.ResourceList)
	}

	if resources.Limits == nil {
		resources.Limits = make(v1.ResourceList)
	}

	if cpuRequest != nil {
		// 若分配值与实际值都处于或低于 MinShares，
		// 则在 API 中保留分配值，避免混淆并简化
		// 在 cgroup 最小下限处的比较。
		if cpuRequest.MilliValue() > cm.MinShares || resources.Requests.Cpu().MilliValue() > cm.MinShares {
			allocatedMilliCPU := resources.Requests.Cpu().MilliValue()
			allocatedShares := cm.MilliCPUToShares(allocatedMilliCPU)
			// cgroup v2 的 cpu.weight 粒度较粗，shares -> weight -> shares
			// 的转换可能丢失精度。若 cgroup 回读的权重与
			// 分配 shares 往返换算后的结果一致，
			// 则在 API 中保留分配的 milliCPU；否则视为真实变化，
			// 使用实际生效的值。
			if cpuConfig != nil && cpuConfig.CPUShares != nil &&
				cm.CPUSharesEqualAfterV2RoundTrip(allocatedShares, *cpuConfig.CPUShares) {
				logger.V(4).Info("Preserving allocated CPU request due to cgroup v2 round-trip",
					"allocated", allocatedMilliCPU, "actuated", cpuRequest.MilliValue(),
					"allocatedShares", allocatedShares, "readbackShares", *cpuConfig.CPUShares)
			} else {
				resources.Requests[v1.ResourceCPU] = cpuRequest.DeepCopy()
			}
		}
	} else {
		preserveOldResourcesValue(v1.ResourceCPU, oldPodStatus.Resources.Requests, resources.Requests)
	}

	if _, found := resources.Requests[v1.ResourceMemory]; !found {
		opts := resourcehelper.PodResourcesOptions{
			SkipPodLevelResources:                    !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
			UseDRANodeAllocatableResourceClaimStatus: utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources),
		}
		aggregatedResources := resourcehelper.PodRequests(allocatedPod, opts)
		if val, ok := aggregatedResources[v1.ResourceMemory]; ok {
			resources.Requests[v1.ResourceMemory] = val
		}
	}

	if cpuLimit != nil {
		// 若分配值与实际值都处于或低于最小生效上限，
		// 则在 API 中保留分配值，
		// 避免混淆并简化比较。
		if cpuLimit.MilliValue() > cm.MinMilliCPULimit || resources.Limits.Cpu().MilliValue() > cm.MinMilliCPULimit {
			resources.Limits[v1.ResourceCPU] = cpuLimit.DeepCopy()
		}
	} else {
		preserveOldResourcesValue(v1.ResourceCPU, oldPodStatus.Resources.Limits, resources.Limits)

	}

	if memoryLimit != nil {
		resources.Limits[v1.ResourceMemory] = memoryLimit.DeepCopy()
	} else {
		preserveOldResourcesValue(v1.ResourceMemory, oldPodStatus.Resources.Limits, resources.Limits)
	}

	return resources
}

// convertToAPIContainerStatuses 把内部容器状态转换为 API 容器状态。
func (kl *Kubelet) convertToAPIContainerStatuses(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, previousStatus []v1.ContainerStatus, containers []v1.Container, imageVolumeNames sets.Set[string], hasInitContainers, isInitContainer bool, podRestarting bool) []v1.ContainerStatus {
	logger := klog.FromContext(ctx)
	convertContainerStatus := func(cs *kubecontainer.Status, oldStatus *v1.ContainerStatus) *v1.ContainerStatus {
		cid := cs.ID.String()
		status := &v1.ContainerStatus{
			Name:         cs.Name,
			RestartCount: int32(cs.RestartCount),
			Image:        cs.Image,
			// 把带 digest 的镜像引用转换为 Kubernetes 公开的
			// ContainerStatus.ImageID 是历史有意为之的行为，不应更改。
			ImageID:     cs.ImageRef,
			ContainerID: cid,
		}
		if oldStatus != nil {
			status.VolumeMounts = oldStatus.VolumeMounts // immutable
			if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
				if oldStatus.RestartCount > status.RestartCount {
					status.RestartCount = oldStatus.RestartCount
				}
				// 普通容器重启会在运行时保留旧容器，以便 kubelet 查询其终止状态；
				// 而 RestartAllContainers 会删除旧容器，
				// 因此容器 ID 变化时必须手动传递 LastTerminationState。
				if oldStatus.ContainerID != status.ContainerID && oldStatus.State.Terminated != nil {
					status.LastTerminationState.Terminated = oldStatus.State.Terminated
				} else if oldStatus.LastTerminationState.Terminated != nil {
					status.LastTerminationState.Terminated = oldStatus.LastTerminationState.Terminated
				}
			}
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.ImageVolumeWithDigest) && imageVolumeNames.Len() > 0 {
			for i := range status.VolumeMounts {
				volumeName := status.VolumeMounts[i].Name
				if !imageVolumeNames.Has(volumeName) {
					continue
				}

				var imageSpec kubecontainer.ImageSpec
				for _, curVolumeMount := range cs.Mounts {
					if curVolumeMount.Image == nil || curVolumeMount.Name != volumeName {
						continue
					}
					imageSpec = kuberuntime.ToKubeContainerImageSpec(&runtimeapi.Image{
						Id:   curVolumeMount.Image.Image,
						Spec: curVolumeMount.Image,
					})
					break
				}

				if imageSpec.Image == "" {
					logger.Error(fmt.Errorf("image was not found"), "", "pod", pod.Name, "container", cs.Name, "volume", volumeName)
					continue
				}

				imageRef, err := kl.containerRuntime.GetImageRef(ctx, imageSpec)
				if err != nil {
					logger.Error(err, "error getting image volume digest", "volume", volumeName)
					continue
				}
				if status.VolumeMounts[i].VolumeStatus == nil {
					status.VolumeMounts[i].VolumeStatus = &v1.VolumeStatus{}
				}
				if status.VolumeMounts[i].VolumeStatus.Image == nil {
					status.VolumeMounts[i].VolumeStatus.Image = &v1.ImageVolumeStatus{}
				}
				status.VolumeMounts[i].VolumeStatus.Image.ImageRef = imageRef
			}
		}

		switch {
		case cs.State == kubecontainer.ContainerStateRunning:
			status.State.Running = &v1.ContainerStateRunning{StartedAt: metav1.NewTime(cs.StartedAt)}
			if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
				if oldStatus != nil &&
					oldStatus.State.Terminated != nil &&
					oldStatus.RestartCount >= int32(cs.RestartCount) {
					status.RestartCount = oldStatus.RestartCount + 1
				}
			}
		case cs.State == kubecontainer.ContainerStateCreated:
			// 已创建但未运行的容器属于“等待运行”状态
			status.State.Waiting = &v1.ContainerStateWaiting{}
		case cs.State == kubecontainer.ContainerStateExited:
			status.State.Terminated = &v1.ContainerStateTerminated{
				ExitCode:    int32(cs.ExitCode),
				Reason:      cs.Reason,
				Message:     cs.Message,
				StartedAt:   metav1.NewTime(cs.StartedAt),
				FinishedAt:  metav1.NewTime(cs.FinishedAt),
				ContainerID: cid,
			}

		case cs.State == kubecontainer.ContainerStateUnknown &&
			oldStatus != nil && // we have an old status
			oldStatus.State.Running != nil: // our previous status was running

			reason := kubecontainer.ContainerReasonStatusUnknown
			// Pod 正在重启时，reason 应为 RestartingAllContainers
			if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) && podRestarting {
				reason = RestartingAllContainers
			}
			// 出现该情况说明容器此前在运行、现在已不在运行（假定 CRI 未漏报运行中容器）：
			// 例如容器失败后 kubelet 未及时查询到结果。此时不应立即置为 waiting，
			// 否则 runonce Pod 这类场景可能真的运行两次——“容器从未运行”与
			// “容器运行后失败”不同，kubelet 与上层逻辑（如 crashloop 检测）
			// 会区别对待这两种情况。
			status.State.Terminated = &v1.ContainerStateTerminated{
				Reason:   reason,
				Message:  "The container could not be located when the pod was terminated",
				ExitCode: 137, // this code indicates an error
			}
			// 重启次数通常来自 CRI（见方法开头），但这里是为 CRI 未返回状态的情况
			// 显式补充，因此需要手动递增以保证准确。
			status.RestartCount = oldStatus.RestartCount + 1

		default:
			// 把未知状态一律折叠为 waiting；只要有容器 waiting，Pod 状态就会变为 pending，
			// 即使容器实际在运行。也就是说任一容器状态读取失败都会导致整个 Pod pending。
			// 参见 kuberuntime_container.go 中运行时状态映射
			// 到 helpers.go 的转换逻辑，
			// 以及本文件中的解释。
			status.State.Waiting = &v1.ContainerStateWaiting{}
		}
		return status
	}

	convertContainerStatusResources := func(allocatedContainer *v1.Container, status *v1.ContainerStatus, cStatus *kubecontainer.Status, oldStatuses map[string]v1.ContainerStatus) *v1.ResourceRequirements {
		cName := allocatedContainer.Name
		// 容器运行中时旧状态应该总是存在
		oldStatus, oldStatusFound := oldStatuses[cName]

		// 若新状态缺少资源信息，且容器正在运行、此前状态也在运行
		// （且容器 ID 未变），则保留此前上报的资源值。
		preserveOldResourcesValue := func(rName v1.ResourceName, oldStatusResource, resource v1.ResourceList) {
			if cStatus.State == kubecontainer.ContainerStateRunning &&
				oldStatusFound && oldStatus.State.Running != nil &&
				status.ContainerID == oldStatus.ContainerID &&
				oldStatusResource != nil {
				if r, exists := oldStatusResource[rName]; exists {
					resource[rName] = r.DeepCopy()
				}
			}
		}

		if cStatus.State != kubecontainer.ContainerStateRunning {
			// 容器未运行时直接使用分配的资源
			return allocatedContainer.Resources.DeepCopy()
		}
		if oldStatus.Resources == nil {
			oldStatus.Resources = &v1.ResourceRequirements{}
		}

		// 状态资源默认取分配资源：
		// 对未运行容器这即上报值；
		// 对不可调整的资源也使用这些值。
		resources := allocatedContainer.Resources.DeepCopy()
		if resources.Limits != nil {
			if cStatus.Resources != nil && cStatus.Resources.CPULimit != nil {
				// 若分配值与实际值都处于或低于最小生效上限，则保留
				// 避免混淆并简化比较。
				if cStatus.Resources.CPULimit.MilliValue() > cm.MinMilliCPULimit ||
					resources.Limits.Cpu().MilliValue() > cm.MinMilliCPULimit {
					resources.Limits[v1.ResourceCPU] = cStatus.Resources.CPULimit.DeepCopy()
				}
			} else {
				preserveOldResourcesValue(v1.ResourceCPU, oldStatus.Resources.Limits, resources.Limits)
			}
			if cStatus.Resources != nil && cStatus.Resources.MemoryLimit != nil {
				resources.Limits[v1.ResourceMemory] = cStatus.Resources.MemoryLimit.DeepCopy()
			} else {
				preserveOldResourcesValue(v1.ResourceMemory, oldStatus.Resources.Limits, resources.Limits)
			}
		}
		if resources.Requests != nil {
			if cStatus.Resources != nil && cStatus.Resources.CPURequest != nil {
				// 若分配值与实际值都处于或低于 MinShares，则在 API 中保留分配值，
				// 避免混淆并简化比较。
				if cStatus.Resources.CPURequest.MilliValue() > cm.MinShares ||
					resources.Requests.Cpu().MilliValue() > cm.MinShares {
					resources.Requests[v1.ResourceCPU] = cStatus.Resources.CPURequest.DeepCopy()
				}
			} else {
				preserveOldResourcesValue(v1.ResourceCPU, oldStatus.Resources.Requests, resources.Requests)
			}
			if cStatus.Resources != nil && cStatus.Resources.MemoryRequest != nil {
				resources.Requests[v1.ResourceMemory] = cStatus.Resources.MemoryRequest.DeepCopy()
			} else {
				preserveOldResourcesValue(v1.ResourceMemory, oldStatus.Resources.Requests, resources.Requests)
			}
		}

		return resources
	}

	convertContainerStatusUser := func(cStatus *kubecontainer.Status) *v1.ContainerUser {
		if cStatus.User == nil {
			return nil
		}

		user := &v1.ContainerUser{}
		if cStatus.User.Linux != nil {
			user.Linux = &v1.LinuxContainerUser{
				UID:                cStatus.User.Linux.UID,
				GID:                cStatus.User.Linux.GID,
				SupplementalGroups: cStatus.User.Linux.SupplementalGroups,
			}
		}

		return user
	}

	// 从旧的 Pod 状态中取出旧容器状态
	oldStatuses := make(map[string]v1.ContainerStatus, len(containers))
	for _, status := range previousStatus {
		oldStatuses[status.Name] = status
	}

	// 先把所有容器状态设为默认 waiting
	statuses := make(map[string]*v1.ContainerStatus, len(containers))
	defaultWaitingState := v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: ContainerCreating}}
	if hasInitContainers {
		defaultWaitingState = v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: PodInitializing}}
	}

	supportsRRO := kl.runtimeClassSupportsRecursiveReadOnlyMounts(logger, pod)

	for _, container := range containers {
		status := &v1.ContainerStatus{
			Name:  container.Name,
			Image: container.Image,
			State: defaultWaitingState,
		}
		// status.VolumeMounts 无法从 kubecontainer.Status 传播，
		// 因为 CRI API 不知道卷名称。
		if utilfeature.DefaultFeatureGate.Enabled(features.RecursiveReadOnlyMounts) {
			for _, vol := range container.VolumeMounts {
				volStatus := v1.VolumeMountStatus{
					Name:      vol.Name,
					MountPath: vol.MountPath,
					ReadOnly:  vol.ReadOnly,
				}
				if vol.ReadOnly {
					rroMode := v1.RecursiveReadOnlyDisabled
					if b, err := resolveRecursiveReadOnly(vol, supportsRRO); err != nil {
						logger.Error(err, "failed to resolve recursive read-only mode", "mode", *vol.RecursiveReadOnly)
					} else if b {
						if utilfeature.DefaultFeatureGate.Enabled(features.RecursiveReadOnlyMounts) {
							rroMode = v1.RecursiveReadOnlyEnabled
						} else {
							logger.Error(nil, "recursive read-only mount needs feature gate to be enabled",
								"featureGate", features.RecursiveReadOnlyMounts)
						}
					}
					volStatus.RecursiveReadOnly = &rroMode // Disabled or Enabled
				}
				status.VolumeMounts = append(status.VolumeMounts, volStatus)
			}
		}
		oldStatus, found := oldStatuses[container.Name]
		if found {
			if oldStatus.State.Terminated != nil {
				status = &oldStatus
			} else {
				// 用旧状态中的部分字段作为默认值
				status.RestartCount = oldStatus.RestartCount
				status.LastTerminationState = oldStatus.LastTerminationState
			}
		}
		statuses[container.Name] = status
	}

	for _, container := range containers {
		found := false
		for _, cStatus := range podStatus.ContainerStatuses {
			if container.Name == cStatus.Name {
				found = true
				break
			}
		}
		if found {
			continue
		}
		// 若找不到容器，假定其应处于 waiting 看似合理，但状态语义要求
		// 必须存在上一次终止记录。若离线时间较长或容器被删除，该记录可能缺失。
		// 下面的代码确保：若容器此前在运行、随后状态消失，
		// 即使不知道退出码也能推断其已终止，
		// 通过设置 lastTerminationState，可以让容器保持 waiting，
		// 并通过 API 呈现更准确的信息。

		oldStatus, ok := oldStatuses[container.Name]
		if !ok {
			continue
		}

		// 若容器缺失、发生了原地 RestartAllContainers 且旧状态不是 waiting，
		// 说明容器已被运行时移除，应视为 Waiting；其 LastTerminationState 的
		// reason 应为 RestartingAllContainers，避免重启时产生混淆。
		if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) && podRestarting && oldStatus.State.Waiting == nil {
			status := statuses[container.Name]
			status.State.Waiting = &v1.ContainerStateWaiting{
				Reason:  RestartingAllContainers,
				Message: "The container is removed because RestartAllContainers in place",
			}
			status.State.Terminated = nil
			status.LastTerminationState.Terminated = &v1.ContainerStateTerminated{
				Reason:   RestartingAllContainers,
				Message:  "The container is removed because RestartAllContainers in place",
				ExitCode: 137,
			}
			status.RestartCount = oldStatus.RestartCount + 1
			statuses[container.Name] = status
			continue
		}

		if oldStatus.State.Terminated != nil {
			// 旧容器状态若为 terminated，则 lastTerminationState 已正确
			continue
		}
		if oldStatus.State.Running == nil {
			// 旧容器状态不是 running 时，waiting 就是合适的，无需处理
			continue
		}

		// 走到这里说明 Pod 此前在运行但没有终止状态，
		// 需进一步判断是否处于 pending。
		status := statuses[container.Name]
		// 若即将写入的是默认状态，waiting 会把 Pod 拉回 Pending；但这里已知它此前在运行。
		isDefaultWaitingStatus := status.State.Waiting != nil && status.State.Waiting.Reason == ContainerCreating
		if hasInitContainers {
			isDefaultWaitingStatus = status.State.Waiting != nil && status.State.Waiting.Reason == PodInitializing
		}
		if !isDefaultWaitingStatus {
			// 状态已被写入，不覆盖
			continue
		}
		if status.LastTerminationState.Terminated != nil {
			// 已有终止状态，无需处理
			continue
		}

		// 设置该值可确保这里显示为 stopped 而非 waiting，
		// 从而避免 Pod 退回 Pending。
		status.LastTerminationState.Terminated = &v1.ContainerStateTerminated{
			Reason:   kubecontainer.ContainerReasonStatusUnknown,
			Message:  "The container could not be located when the pod was deleted.  The container used to be Running",
			ExitCode: 137,
		}

		// 若 Pod 未被删除，说明容器发生了重启，递增重启次数
		if pod.DeletionTimestamp == nil {
			status.RestartCount += 1
		}

		statuses[container.Name] = status
	}

	// 排序前先复制切片
	containerStatusesCopy := make([]*kubecontainer.Status, len(podStatus.ContainerStatuses))
	copy(containerStatusesCopy, podStatus.ContainerStatuses)

	// 让最新的容器状态排在最前
	sort.Sort(sort.Reverse(kubecontainer.SortContainerStatusesByCreationTime(containerStatusesCopy)))
	// 按运行时状态设置容器状态
	containerSeen := map[string]int{}
	for _, cStatus := range containerStatusesCopy {
		cName := cStatus.Name
		if _, ok := statuses[cName]; !ok {
			// 这里同时会忽略 infra（pause）容器。
			continue
		}
		if containerSeen[cName] >= 2 {
			continue
		}
		var oldStatusPtr *v1.ContainerStatus
		if oldStatus, ok := oldStatuses[cName]; ok {
			oldStatusPtr = &oldStatus
		}
		status := convertContainerStatus(cStatus, oldStatusPtr)
		if !utilfeature.DefaultFeatureGate.Enabled(features.ChangeContainerStatusOnKubeletRestart) {
			if cStatus.State == kubecontainer.ContainerStateRunning {
				if oldStatus, ok := oldStatuses[status.Name]; ok && oldStatus.Started != nil {
					status.Started = oldStatus.Started
				}
			}
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
			allocatedContainer := kubecontainer.GetContainerSpec(pod, cName)
			if allocatedContainer != nil {
				// status.Resources 反映 cgroup 实际生效的资源；容器运行时
				// 由 CRI 状态填充。
				status.Resources = convertContainerStatusResources(allocatedContainer, status, cStatus, oldStatuses)
				// status.AllocatedResources 表示扩缩容期望的 cgroup 资源状态，
				// 严格基于 spec 中未膨胀的标准 cgroup 资源请求。
				// 容器级别的 AllocatedResources 不包含 DRA 分配，
				// 因为 claim 可在 Pod 内多个容器间共享，
				// 且分配可能不均（由 DRA 驱动保证）。
				status.AllocatedResources = allocatedContainer.Resources.Requests
			}
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.SupplementalGroupsPolicy) {
			status.User = convertContainerStatusUser(cStatus)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.ContainerStopSignals) {
			status.StopSignal = cStatus.StopSignal
		}

		if containerSeen[cName] == 0 {
			statuses[cName] = status
		} else {
			statuses[cName].LastTerminationState = status.State
		}
		containerSeen[cName] = containerSeen[cName] + 1
	}

	// 处理启动失败的容器，它们应处于 Waiting 状态。
	for _, container := range containers {
		if isInitContainer {
			// init 容器以退出码 0 终止时不会重启。
			// TODO(random-liu): 用更清晰的方式处理。
			s := podStatus.FindContainerStatusByName(container.Name)
			if s != nil && s.State == kubecontainer.ContainerStateExited && s.ExitCode == 0 {
				continue
			}
			// Pod 状态未持久化时，init 容器状态可能丢失，
			// 常见于以下场景：
			// 1. 静态 Pod；
			// 2. 普通 Pod 的 init 容器状态未能同步到 apiserver
			//    （例如控制面问题，较为罕见）。
			// 此时若 init 容器已从运行时移除，kubelet 无法获知其状态；
			// 但如果普通容器已经创建，可以推断不可重启的 init 容器
			// 已成功完成。
			// 注意：可重启 init 容器（sidecar）应保持 waiting，
			// 由 kubelet 负责重启。
			isSidecar := container.RestartPolicy != nil && *container.RestartPolicy == v1.ContainerRestartPolicyAlways
			if s == nil &&
				kuberuntime.HasAnyRegularContainerCreated(pod, podStatus) &&
				!isSidecar &&
				statuses[container.Name].State.Waiting != nil {
				statuses[container.Name].State = v1.ContainerState{
					Terminated: &v1.ContainerStateTerminated{
						Reason:   "Completed",
						Message:  "Unable to get init container status from container runtime and pod has been initialized, treat it as exited normally",
						ExitCode: 0,
					},
				}
				continue
			}
		}
		// 若容器在下一次 syncPod 中应被重启，则处于 *Waiting* 状态。
		if !kubecontainer.ShouldContainerBeRestarted(logger, &container, pod, podStatus) {
			continue
		}
		status := statuses[container.Name]
		reason, ok := kl.reasonCache.Get(pod.UID, container.Name)
		if !ok {
			// 其实这里也可以置为 Waiting，但信息量更少；
			// 由于容器很快会重启，这里优先保留原状态。
			// 注意在当前 ShouldContainerBeRestarted 实现下，原状态可能是：
			//   * Waiting：没有关联的历史容器与启动失败原因记录；
			//   * Terminated：容器已终止。
			continue
		}
		if status.State.Terminated != nil {
			status.LastTerminationState = status.State
		}
		status.State = v1.ContainerState{
			Waiting: &v1.ContainerStateWaiting{
				Reason:  reason.Err.Error(),
				Message: reason.Message,
			},
		}
		statuses[container.Name] = status
	}

	// 对容器状态排序，因为该接口的调用方期望 Pod 内容器顺序确定。
	if isInitContainer {
		return kubetypes.SortStatusesOfInitContainers(pod, statuses)
	}
	containerStatuses := make([]v1.ContainerStatus, 0, len(statuses))
	for _, status := range statuses {
		containerStatuses = append(containerStatuses, *status)
	}

	sort.Sort(kubetypes.SortedContainerStatuses(containerStatuses))
	return containerStatuses
}

// ServeLogs 提供当前机器上的日志服务。
func (kl *Kubelet) ServeLogs(w http.ResponseWriter, req *http.Request) {
	// TODO: 对允许提供的日志做白名单限制
	kl.logServer.ServeHTTP(w, req)
}

// findContainer 根据 Pod ID、全名与容器名查找并返回容器；
// 未找到时返回 nil。
func (kl *Kubelet) findContainer(ctx context.Context, podFullName string, podUID types.UID, containerName string) (*kubecontainer.Container, error) {
	pods, err := kl.containerRuntime.GetPods(ctx, false)
	if err != nil {
		return nil, err
	}
	// 进行 UID 转换；
	// 这里需要静态 Pod 的 UID，而 kubecontainer API 使用 types.UID。
	podUID = types.UID(kl.podManager.TranslatePodUID(podUID))
	pod := kubecontainer.Pods(pods).FindPod(podFullName, podUID)
	return pod.FindContainerByName(containerName), nil
}

// RunInContainer 在容器内执行命令，并以字节数组返回合并后的 stdout 与 stderr。
func (kl *Kubelet) RunInContainer(ctx context.Context, podFullName string, podUID types.UID, containerName string, cmd []string) ([]byte, error) {
	container, err := kl.findContainer(ctx, podFullName, podUID, containerName)
	if err != nil {
		return nil, err
	}
	if container == nil {
		return nil, fmt.Errorf("container not found (%q)", containerName)
	}
	// TODO(tallclair): 传入合适的超时时间。
	return kl.runner.RunInContainer(ctx, container.ID, cmd, 0)
}

// GetExec 返回 exec 请求的流式 URL；由 kubelet 自己服务时返回 nil。
func (kl *Kubelet) GetExec(ctx context.Context, podFullName string, podUID types.UID, containerName string, cmd []string, streamOpts remotecommandserver.Options) (*url.URL, error) {
	container, err := kl.findContainer(ctx, podFullName, podUID, containerName)
	if err != nil {
		return nil, err
	}
	if container == nil {
		return nil, fmt.Errorf("container not found (%q)", containerName)
	}
	return kl.streamingRuntime.GetExec(ctx, container.ID, cmd, streamOpts.Stdin, streamOpts.Stdout, streamOpts.Stderr, streamOpts.TTY)
}

// GetAttach 返回 attach 请求的流式 URL；由 kubelet 自己服务时返回 nil。
func (kl *Kubelet) GetAttach(ctx context.Context, podFullName string, podUID types.UID, containerName string, streamOpts remotecommandserver.Options) (*url.URL, error) {
	container, err := kl.findContainer(ctx, podFullName, podUID, containerName)
	if err != nil {
		return nil, err
	}
	if container == nil {
		return nil, fmt.Errorf("container %s not found in pod %s", containerName, podFullName)
	}

	// attach 的 TTY 设置必须与容器初始配置一致，
	// 因为进程启动后无法再改变是否运行在 TTY 中。
	// 因此需要通过 api.Pod 获取 TTY 状态。
	pod, found := kl.GetPodByFullName(podFullName)
	if !found || (string(podUID) != "" && pod.UID != podUID) {
		return nil, fmt.Errorf("pod %s not found", podFullName)
	}
	containerSpec := kubecontainer.GetContainerSpec(pod, containerName)
	if containerSpec == nil {
		return nil, fmt.Errorf("container %s not found in pod %s", containerName, podFullName)
	}
	tty := containerSpec.TTY

	return kl.streamingRuntime.GetAttach(ctx, container.ID, streamOpts.Stdin, streamOpts.Stdout, streamOpts.Stderr, tty)
}

// GetPortForward 返回 port-forward 请求的流式 URL；由 kubelet 自己服务时返回 nil。
func (kl *Kubelet) GetPortForward(ctx context.Context, podName, podNamespace string, podUID types.UID, portForwardOpts portforward.V4Options) (*url.URL, error) {
	pods, err := kl.containerRuntime.GetPods(ctx, false)
	if err != nil {
		return nil, err
	}
	// 进行 UID 转换；
	// 这里需要静态 Pod 的 UID，而 kubecontainer API 使用 types.UID。
	podUID = types.UID(kl.podManager.TranslatePodUID(podUID))
	podFullName := kubecontainer.BuildPodFullName(podName, podNamespace)
	pod := kubecontainer.Pods(pods).FindPod(podFullName, podUID)
	if pod.IsEmpty() {
		return nil, fmt.Errorf("pod not found (%q)", podFullName)
	}

	return kl.streamingRuntime.GetPortForward(ctx, podName, podNamespace, podUID, portForwardOpts.Ports)
}

// cleanupOrphanedPodCgroups 删除不应再存在的 cgroup：
// 将缓存的 cgroupPods 与给定的 runningPods 列表对齐。
func (kl *Kubelet) cleanupOrphanedPodCgroups(logger klog.Logger, pcm cm.PodContainerManager, cgroupPods map[types.UID]cm.CgroupName, possiblyRunningPods map[types.UID]sets.Empty) {
	// 遍历所有发现的 Pod，判断它们是否仍应运行
	for uid, val := range cgroupPods {
		// 若 Pod 在运行集合中，则不是清理候选
		if _, ok := possiblyRunningPods[uid]; ok {
			continue
		}

		// 若卷尚未卸载/分离，则不要删除 cgroup，
		// 以免内存后端卷的用量计费被传递到父 cgroup；
		// 卷仍存在时，先把 cgroup 中进程的 CPU shares 降到最低，
		// 等待卷清理完成。
		if podVolumesExist := kl.podVolumesExist(logger, uid); podVolumesExist {
			logger.V(3).Info("Orphaned pod found, but volumes not yet removed.  Reducing cpu to minimum", "podUID", uid)
			if err := pcm.ReduceCPULimits(logger, val); err != nil {
				logger.Info("Failed to reduce cpu time for pod pending volume cleanup", "podUID", uid, "err", err)
			}
			continue
		}
		logger.V(3).Info("Orphaned pod found, removing pod cgroups", "podUID", uid)
		// 销毁不应运行 Pod 的所有 cgroup：
		// 先杀掉这些 cgroup 中的全部进程。
		// Destroy 的错误会被显式检查并记录，
		// 但错误可以容忍，因为 housekeeping 循环
		// 之后还会再次尝试删除这些不需要的 Pod cgroup。
		go func() {
			if err := pcm.Destroy(logger, val); err != nil {
				logger.Info("Failed to destroy orphaned pod cgroup", "podUID", uid, "err", err)
			}
		}()
	}
}

// runtimeClassSupportsRecursiveReadOnlyMounts 判断 Pod 所使用的运行时 handler 是否支持递归只读挂载
func (kl *Kubelet) runtimeClassSupportsRecursiveReadOnlyMounts(logger klog.Logger, pod *v1.Pod) bool {
	if kl.runtimeClassManager == nil {
		return false
	}
	runtimeHandlerName, err := kl.runtimeClassManager.LookupRuntimeHandler(pod.Spec.RuntimeClassName)
	if err != nil {
		logger.Error(err, "failed to look up the runtime handler", "runtimeClassName", pod.Spec.RuntimeClassName)
		return false
	}
	runtimeHandlers := kl.runtimeState.runtimeHandlers()
	return runtimeHandlerSupportsRecursiveReadOnlyMounts(logger, runtimeHandlerName, runtimeHandlers)
}

// runtimeHandlerSupportsRecursiveReadOnlyMounts 检查运行时 handler 是否支持递归只读挂载；
// 这里不检查 kubelet 特性开关。
func runtimeHandlerSupportsRecursiveReadOnlyMounts(logger klog.Logger, runtimeHandlerName string, runtimeHandlers []kubecontainer.RuntimeHandler) bool {
	if len(runtimeHandlers) == 0 {
		// 运行时无法返回 handler 列表，
		// 无需在此告警。
		return false
	}
	for _, h := range runtimeHandlers {
		if h.Name == runtimeHandlerName {
			return h.SupportsRecursiveReadOnlyMounts
		}
	}
	logger.Error(nil, "Unknown runtime handler", "runtimeHandlerName", runtimeHandlerName)
	return false
}

// resolveRecursiveReadOnly 解析卷挂载的递归只读模式。
func resolveRecursiveReadOnly(m v1.VolumeMount, runtimeSupportsRRO bool) (bool, error) {
	if m.RecursiveReadOnly == nil || *m.RecursiveReadOnly == v1.RecursiveReadOnlyDisabled {
		return false, nil
	}
	if !m.ReadOnly {
		return false, fmt.Errorf("volume %q requested recursive read-only mode, but it is not read-only", m.Name)
	}
	if m.MountPropagation != nil && *m.MountPropagation != v1.MountPropagationNone {
		return false, fmt.Errorf("volume %q requested recursive read-only mode, but it is not compatible with propagation %q",
			m.Name, *m.MountPropagation)
	}
	switch rroMode := *m.RecursiveReadOnly; rroMode {
	case v1.RecursiveReadOnlyIfPossible:
		return runtimeSupportsRRO, nil
	case v1.RecursiveReadOnlyEnabled:
		if !runtimeSupportsRRO {
			return false, fmt.Errorf("volume %q requested recursive read-only mode, but it is not supported by the runtime", m.Name)
		}
		return true, nil
	default:
		return false, fmt.Errorf("unknown recursive read-only mode %q", rroMode)
	}
}

// recordPodLevelResourcesAdmission 按 Pod 级/容器级资源配置模式与 QoS 级别统计准入次数，
// 用于观察 PodLevelResources 特性的采用情况
func recordPodLevelResourcesAdmission(pod *v1.Pod) {
	hasResources := func(containers []v1.Container) bool {
		for _, c := range containers {
			if len(c.Resources.Requests) > 0 || len(c.Resources.Limits) > 0 {
				return true
			}
		}
		return false
	}

	hasPodLevel := resourcehelper.IsPodLevelResourcesSet(pod)
	hasContainerLevel := hasResources(pod.Spec.Containers) || hasResources(pod.Spec.InitContainers)

	var configMode string
	switch {
	case hasPodLevel && hasContainerLevel:
		configMode = "pod_and_container_level"
	case hasPodLevel:
		configMode = "pod_level"
	case hasContainerLevel:
		configMode = "container_level"
	}

	qosLabel := strings.ToLower(string(v1qos.GetPodQOS(pod)))
	metrics.PodLevelResourcesAdmissionTotal.WithLabelValues(configMode, qosLabel).Inc()
}
