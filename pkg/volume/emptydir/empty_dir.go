/*
Copyright 2014 The Kubernetes Authors.

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

package emptydir

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/util/swap"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilstrings "k8s.io/utils/strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	usernamespacefeature "k8s.io/kubernetes/pkg/kubelet/userns"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/fsquota"
)

// TODO: 将来会收紧此权限：改为通过 group 属性授权容器使用 emptyDir 卷，
// 而不是对所有人开放。
//
// https://issue.k8s.io/2630
const perm os.FileMode = 0777

// unixModeToFileMode 把 Unix 风格的权限值（0-01777）转换为 Go 的 os.FileMode。
// 粘滞位（01000）映射为 os.ModeSticky。
func unixModeToFileMode(mode int32) os.FileMode {
	fm := os.FileMode(mode & 0777)
	if mode&01000 != 0 {
		fm |= os.ModeSticky
	}
	return fm
}

// ProbeVolumePlugins 是卷插件的主要入口，返回本包实现的插件列表。
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{
		&emptyDirPlugin{nil},
	}
}

// emptyDirPlugin 实现 emptyDir 卷插件，持有访问节点目录所需的 VolumeHost。
type emptyDirPlugin struct {
	host volume.VolumeHost
}

var _ volume.ResizableEphemeralVolumePlugin = &emptyDirPlugin{}

const (
	// EmptyDirPluginName 是 emptyDir 卷插件在 VolumePluginMgr 中的注册名。
	EmptyDirPluginName = "kubernetes.io/empty-dir"
	// hugePagesPageSizeMountOption 是 hugetlbfs 挂载参数中指定页大小的键名。
	hugePagesPageSizeMountOption = "pagesize"
)

// getPath 返回 Pod 专属的卷目录；插件限定名含 '/'，转义后才能安全拼入目录名。
func getPath(uid types.UID, volName string, host volume.VolumeHost) string {
	return host.GetPodVolumeDir(uid, utilstrings.EscapeQualifiedName(EmptyDirPluginName), volName)
}

// Init 保存 VolumeHost，供后续创建卷路径和挂载器使用。
func (plugin *emptyDirPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host

	return nil
}

// GetPluginName 返回插件的注册名称。
func (plugin *emptyDirPlugin) GetPluginName() string {
	return EmptyDirPluginName
}

// GetVolumeName 返回用户在 Pod 中声明的卷名；emptyDir 是临时卷，没有独立卷对象。
func (plugin *emptyDirPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _ := getVolumeSource(spec)
	if volumeSource == nil {
		return "", fmt.Errorf("spec does not reference an emptyDir volume type")
	}

	// 临时卷没有持久化的卷对象，直接沿用用户定义的名字。
	return spec.Name(), nil
}

// CanSupport 判断 spec 是否声明了 emptyDir 卷源。
func (plugin *emptyDirPlugin) CanSupport(spec *volume.Spec) bool {
	return spec.Volume != nil && spec.Volume.EmptyDir != nil
}

// RequiresRemount 表示不需要由卷插件触发重挂。
func (plugin *emptyDirPlugin) RequiresRemount(spec *volume.Spec) bool {
	// 内存型 emptyDir 的扩缩容由 kuberuntime_manager 负责，插件侧重挂反而会干扰它。
	return false
}

// SupportsMountOption 表示不支持用户自定义挂载参数。
func (plugin *emptyDirPlugin) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount 表示不支持通过挂载参数传递 SELinux 上下文。
func (plugin *emptyDirPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// NewMounter 创建生产用挂载器，注入真实的挂载器与挂载探测器。
func (plugin *emptyDirPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	return plugin.newMounterInternal(spec, pod, plugin.host.GetMounter(), &realMountDetector{plugin.host.GetMounter()})
}

// calculateEmptyDirMemorySize 取节点可分配内存、Pod 内存 limit 与卷自身 limit 的最小值作为 tmpfs 上限。
func calculateEmptyDirMemorySize(nodeAllocatableMemory *resource.Quantity, spec *volume.Spec, pod *v1.Pod) *resource.Quantity {
	// 默认上限为节点可分配内存：单个 Pod 不可能比所有 Pod 加起来用得更多。
	sizeLimit := nodeAllocatableMemory
	zero := resource.MustParse("0")

	// 计算 Pod 的内存分配；复用 pod cgroup 的同一函数，保证两处口径一致。
	// 注意：不限制 Pod 内存的平台（如 Windows）上结果可能为 nil。
	podResourceConfig := cm.ResourceConfigForPod(pod, false, uint64(100000), false, config.NoneMemoryReservationPolicy)
	if podResourceConfig != nil && podResourceConfig.Memory != nil {
		podMemoryLimit := resource.NewQuantity(*(podResourceConfig.Memory), resource.BinarySI)
		// 只接受 0 < 值 <= 当前上限，避免非法或放大的 limit 生效。
		if podMemoryLimit.Cmp(zero) > 0 && podMemoryLimit.Cmp(*sizeLimit) < 1 {
			sizeLimit = podMemoryLimit
		}
	}

	// 卷自身的 limit 只有在比 Pod 可用内存更小时才收紧上限。
	if spec.Volume.EmptyDir.SizeLimit != nil {
		volumeSizeLimit := spec.Volume.EmptyDir.SizeLimit
		// 同样只接受 0 < 值 <= 当前上限。
		if volumeSizeLimit.Cmp(zero) > 0 && volumeSizeLimit.Cmp(*sizeLimit) < 1 {
			sizeLimit = volumeSizeLimit
		}
	}
	return sizeLimit
}

// newMounterInternal 组装 emptyDir 挂载器，便于测试时注入假挂载器与探测器。
func (plugin *emptyDirPlugin) newMounterInternal(spec *volume.Spec, pod *v1.Pod, mounter mount.Interface, mountDetector mountDetector) (volume.Mounter, error) {
	medium := v1.StorageMediumDefault
	sizeLimit := &resource.Quantity{}
	var mode *int32
	if spec.Volume.EmptyDir != nil { // 未显式指定卷源时按默认空目录处理。
		medium = spec.Volume.EmptyDir.Medium
		if medium == v1.StorageMediumMemory {
			nodeAllocatable, err := plugin.host.GetNodeAllocatable()
			if err != nil {
				return nil, err
			}
			sizeLimit = calculateEmptyDirMemorySize(nodeAllocatable.Memory(), spec, pod)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.EmptyDirVolumeMode) {
			mode = spec.Volume.EmptyDir.Mode
		}
	}
	return &emptyDir{
		pod:             pod,
		volName:         spec.Name(),
		medium:          medium,
		sizeLimit:       sizeLimit,
		mode:            mode,
		mounter:         mounter,
		mountDetector:   mountDetector,
		plugin:          plugin,
		MetricsProvider: volume.NewMetricsDu(getPath(pod.UID, spec.Name(), plugin.host)),
	}, nil
}

// NewUnmounter 创建卸载器；真实依赖在此注入，测试通过内部函数进行。
func (plugin *emptyDirPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	// 真实实现只在这一层注入，内部函数保持可测试。
	return plugin.newUnmounterInternal(volName, podUID, plugin.host.GetMounter(), &realMountDetector{plugin.host.GetMounter()})
}

// newUnmounterInternal 构造卸载器；介质此时未知，等卸载时按实际挂载状态判断。
func (plugin *emptyDirPlugin) newUnmounterInternal(volName string, podUID types.UID, mounter mount.Interface, mountDetector mountDetector) (volume.Unmounter, error) {
	ed := &emptyDir{
		pod:             &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID}},
		volName:         volName,
		medium:          v1.StorageMediumDefault, // 卸载时再根据实际挂载情况修正。
		mounter:         mounter,
		mountDetector:   mountDetector,
		plugin:          plugin,
		MetricsProvider: volume.NewMetricsDu(getPath(podUID, volName, plugin.host)),
	}
	return ed, nil
}

// ConstructVolumeSpec 从已挂载路径重建最小化的 emptyDir 卷 spec。
func (plugin *emptyDirPlugin) ConstructVolumeSpec(volName, mountPath string) (volume.ReconstructedVolume, error) {
	emptyDirVolume := &v1.Volume{
		Name: volName,
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(emptyDirVolume),
	}, nil
}

// mountDetector 抽象「如何判断路径背后是哪种挂载」，便于在测试中替换真实挂载器。
type mountDetector interface {
	// GetMountMedium 返回路径背后的介质类型、该路径本身是否为挂载点，以及挂载页大小。
	// 例如返回 (v1.StorageMediumMemory, false, nil) 表示路径位于内存文件系统
	// （Linux 上是 tmpfs）上，但并非该 tmpfs 的根挂载点。
	GetMountMedium(path string, requestedMedium v1.StorageMedium) (v1.StorageMedium, bool, *resource.Quantity, error)
}

// emptyDir 表示暴露给 Pod 的临时目录，生命周期不超过 Pod 本身。
type emptyDir struct {
	pod           *v1.Pod
	volName       string
	sizeLimit     *resource.Quantity
	medium        v1.StorageMedium
	mode          *int32
	mounter       mount.Interface
	mountDetector mountDetector
	plugin        *emptyDirPlugin
	volume.MetricsProvider
}

// GetAttributes 描述卷属性：可写、由 kubelet 管理、需要 SELinux 重打标。
func (ed *emptyDir) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       false,
		Managed:        true,
		SELinuxRelabel: true,
	}
}

// SetUp 在默认路径上创建并准备卷目录。
func (ed *emptyDir) SetUp(mounterArgs volume.MounterArgs) error {
	return ed.SetUpAt(ed.GetPath(), mounterArgs)
}

// SetUpAt 在指定目录上创建并准备卷。
func (ed *emptyDir) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	notMnt, err := ed.mounter.IsLikelyNotMountPoint(dir)
	// 目录可能还不存在，此时探测会报 IsNotExist，应继续初始化而不是直接失败。
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// 默认介质只要就绪文件存在即视为可用；内存介质还必须已经挂载。
	readyDir := ed.getMetaDir()
	if volumeutil.IsReady(readyDir) {
		if ed.medium == v1.StorageMediumMemory && !notMnt {
			return nil
		} else if ed.medium == v1.StorageMediumDefault {
			// 就绪文件在，还要确认目录本身仍在。
			if _, err := os.Stat(dir); err == nil {
				klog.V(6).InfoS("Dir exists, so check and assign quota if the underlying medium supports quotas", "dir", dir)
				err = ed.assignQuota(dir, mounterArgs.DesiredSize)
				return err
			}
			// 正常流程不会出现，说明用户手工删了卷目录；清掉就绪标记，下次 SetUp 会重建。
			klog.Warningf("volume ready file dir %s exist, but volume dir %s does not. Remove ready dir", readyDir, dir)
			if err := os.RemoveAll(readyDir); err != nil && !os.IsNotExist(err) {
				klog.Warningf("failed to remove ready dir [%s]: %v", readyDir, err)
			}
		}
	}

	switch {
	case ed.medium == v1.StorageMediumDefault:
		err = ed.setupDir(dir)
	case ed.medium == v1.StorageMediumMemory:
		err = ed.setupTmpfs(dir)
	case v1helper.IsHugePageMedium(ed.medium):
		err = ed.setupHugepages(dir)
	default:
		err = fmt.Errorf("unknown storage medium %q", ed.medium)
	}

	ownershipChanger := volume.NewVolumeOwnership(ed, dir, mounterArgs.FsGroup, nil /*fsGroupChangePolicy*/, volumeutil.FSGroupCompleteHook(ed.plugin, nil))
	_ = ownershipChanger.ChangePermissions()

	// 配额设置失败只记录、不让挂载失败：在支持强制配额之前仍可退回 du 统计。
	if err == nil {
		volumeutil.SetReady(ed.getMetaDir())
		err = ed.assignQuota(dir, mounterArgs.DesiredSize)
	}
	return err
}

// assignQuota 检查底层介质是否支持配额，支持时为卷设置配额。
func (ed *emptyDir) assignQuota(dir string, mounterSize *resource.Quantity) error {
	if mounterSize != nil {
		// 这里的 err 刻意遮蔽外层 err，不影响后续挂载结果判断。
		// hostUsers 默认为 true，Pod 默认不受 user namespace 限制；显式设为 false 才启用。
		userNamespacesEnabled := false
		if usernamespacefeature.EnabledUserNamespacesSupport() {
			userNamespacesEnabled = ed.pod.Spec.HostUsers != nil && !*ed.pod.Spec.HostUsers
		}
		hasQuotas, err := fsquota.SupportsQuotas(ed.mounter, dir, userNamespacesEnabled)
		klog.V(3).Infof("assignQuota called, hasQuotas = %t userNamespacesEnabled = %t", hasQuotas, userNamespacesEnabled)
		if err != nil {
			klog.V(3).Infof("Unable to check for quota support on %s: %s", dir, err.Error())
		} else if hasQuotas {
			klog.V(3).Infof("emptydir trying to assign quota %v on %s", mounterSize, dir)
			if err := fsquota.AssignQuota(ed.mounter, dir, ed.pod.UID, mounterSize, userNamespacesEnabled); err != nil {
				klog.V(3).Infof("Set quota on %s failed %s", dir, err.Error())
				return err
			}
			return nil
		}
	}
	return nil
}

// setupTmpfs 在指定目录上挂载 tmpfs。
func (ed *emptyDir) setupTmpfs(dir string) error {
	if ed.mounter == nil {
		return fmt.Errorf("memory storage requested, but mounter is nil")
	}
	if err := ed.setupDir(dir); err != nil {
		return err
	}
	// 先探测现状，保证重复 SetUp 不会二次挂载。
	medium, isMnt, _, err := ed.mountDetector.GetMountMedium(dir, ed.medium)
	if err != nil {
		return err
	}
	// 已挂载为内存介质时直接返回，无需重复挂载。
	if isMnt && medium == v1.StorageMediumMemory {
		return nil
	}

	options := ed.generateTmpfsMountOptions(swap.IsTmpfsNoswapOptionSupported(ed.mounter, ed.plugin.host.GetPluginDir(EmptyDirPluginName)))

	klog.V(3).Infof("pod %v: mounting tmpfs for volume %v", ed.pod.UID, ed.volName)
	return ed.mounter.MountSensitiveWithoutSystemd("tmpfs", dir, "tmpfs", options, nil)
}

// setupHugepages 在指定目录上挂载 hugetlbfs。
func (ed *emptyDir) setupHugepages(dir string) error {
	if ed.mounter == nil {
		return fmt.Errorf("memory storage requested, but mounter is nil")
	}
	if err := ed.setupDir(dir); err != nil {
		return err
	}
	// 先探测现状，保证重复 SetUp 不会二次挂载。
	medium, isMnt, mountPageSize, err := ed.mountDetector.GetMountMedium(dir, ed.medium)
	klog.V(3).Infof("pod %v: setupHugepages: medium: %s, isMnt: %v, dir: %s, err: %v", ed.pod.UID, medium, isMnt, dir, err)
	if err != nil {
		return err
	}
	// 已按相同页大小挂载 hugepages 时无需重复操作。
	if isMnt && v1helper.IsHugePageMedium(medium) {
		// 介质为 Hugepages：未指定页大小。
		if ed.medium == v1.StorageMediumHugePages {
			return nil
		}
		if mountPageSize == nil {
			return fmt.Errorf("pod %v: mounted dir %s pagesize is not determined", ed.pod.UID, dir)
		}
		// 介质为 Hugepages-<size>：已指定页大小。
		// 已挂载的页大小必须与介质声明的大小一致。
		mediumSize, err := v1helper.HugePageSizeFromMedium(ed.medium)
		if err != nil {
			return err
		}
		if mountPageSize == nil || mediumSize.Cmp(*mountPageSize) != 0 {
			return fmt.Errorf("pod %v: mounted dir %s pagesize '%s' and requested medium size '%s' differ", ed.pod.UID, dir, mountPageSize.String(), mediumSize.String())
		}
		return nil
	}

	pageSizeMountOption, err := getPageSizeMountOption(ed.medium, ed.pod)
	if err != nil {
		return err
	}

	klog.V(3).Infof("pod %v: mounting hugepages for volume %v", ed.pod.UID, ed.volName)
	return ed.mounter.MountSensitiveWithoutSystemd("nodev", dir, "hugetlbfs", []string{pageSizeMountOption}, nil)
}

// getPageSizeMountOption 根据介质和 Pod 资源请求推导页大小挂载参数，并校验各容器请求是否一致。
func getPageSizeMountOption(medium v1.StorageMedium, pod *v1.Pod) (string, error) {
	pageSizeFound := false
	pageSize := resource.Quantity{}

	var mediumPageSize resource.Quantity
	if medium != v1.StorageMediumHugePages {
		// 介质为 Hugepages-<size>，页大小由介质名给出。
		var err error
		mediumPageSize, err = v1helper.HugePageSizeFromMedium(medium)
		if err != nil {
			return "", err
		}
	}

	podLevelAndContainerLevelRequests := []v1.ResourceList{}
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) && resourcehelper.IsPodLevelResourcesSet(pod) {
		podLevelAndContainerLevelRequests = append(podLevelAndContainerLevelRequests, pod.Spec.Resources.Requests)
	}

	// init 容器在少数场景下也会申请大页。
	for _, container := range append(pod.Spec.Containers, pod.Spec.InitContainers...) {
		podLevelAndContainerLevelRequests = append(podLevelAndContainerLevelRequests, container.Resources.Requests)
	}

	// 大页的 request 与 limit 必须相等，因此只取 request 即可。
	for _, resourceList := range podLevelAndContainerLevelRequests {
		for requestName := range resourceList {
			if !v1helper.IsHugePageResourceName(requestName) {
				continue
			}
			currentPageSize, err := v1helper.HugePageSizeFromResourceName(requestName)
			if err != nil {
				return "", err
			}
			if medium == v1.StorageMediumHugePages { // 介质为 Hugepages，未指定页大小。
				// 介质为 Hugepages 时，一个 Pod 中所有卷的页大小必须一致。
				if pageSizeFound && pageSize.Cmp(currentPageSize) != 0 {
					return "", fmt.Errorf("medium: %s can't be used if container requests multiple huge page sizes", medium)
				}

				pageSizeFound = true
				pageSize = currentPageSize
			} else { // 介质为 Hugepages-<size>。
				if currentPageSize.Cmp(mediumPageSize) == 0 {
					pageSizeFound = true
					pageSize = currentPageSize
				}
			}
		}
	}

	if !pageSizeFound {
		return "", fmt.Errorf("medium %s: hugePages storage requested, but there is no resource request for huge pages", medium)
	}

	return fmt.Sprintf("%s=%s", hugePagesPageSizeMountOption, pageSize.String()), nil
}

// setupDir 创建目录并按指定 mode 设置权限；未指定 mode 时使用 0777。
func (ed *emptyDir) setupDir(dir string) error {
	// 目录不存在时创建，已存在则保留原有内容。
	if err := os.MkdirAll(dir, perm); err != nil {
		return err
	}

	if ed.mode != nil && runtime.GOOS != "windows" {
		effectivePerm := unixModeToFileMode(*ed.mode)
		if err := os.Chmod(dir, effectivePerm); err != nil {
			return err
		}
		return nil
	}

	// 读取实际权限位，判断是否需要修正。
	fileinfo, err := os.Lstat(dir)
	if err != nil {
		return err
	}

	if fileinfo.Mode().Perm() != perm.Perm() {
		// 权限不符通常说明 kubelet 进程带了 umask。与其清空整个进程的 umask，
		// 或锁住线程反复改 umask，直接用 chmod 补齐需要的权限位更简单。
		err := os.Chmod(dir, perm)
		if err != nil {
			return err
		}

		fileinfo, err = os.Lstat(dir)
		if err != nil {
			return err
		}

		if fileinfo.Mode().Perm() != perm.Perm() {
			klog.Errorf("Expected directory %q permissions to be: %s; got: %s", dir, perm.Perm(), fileinfo.Mode().Perm())
		}
	}

	return nil
}

// GetPath 返回该卷在宿主机上的目录路径。
func (ed *emptyDir) GetPath() string {
	return getPath(ed.pod.UID, ed.volName, ed.plugin.host)
}

// TearDown 清理默认路径上的卷目录。
func (ed *emptyDir) TearDown() error {
	return ed.TearDownAt(ed.GetPath())
}

// TearDownAt 卸载并删除指定目录中的内容。
func (ed *emptyDir) TearDownAt(dir string) error {
	// 先删除就绪标记，避免后续重建目录时被误判为已就绪。
	readyDir := ed.getMetaDir()
	if removeErr := os.RemoveAll(readyDir); removeErr != nil && !os.IsNotExist(removeErr) {
		return fmt.Errorf("failed to remove ready dir [%s]: %v", readyDir, removeErr)
	}

	if pathExists, pathErr := mount.PathExists(dir); pathErr != nil {
		return fmt.Errorf("error checking if path exists: %w", pathErr)
	} else if !pathExists {
		klog.Warningf("Unmount skipped because path does not exist: %v", dir)
		return nil
	}

	// 按实际挂载状态判断介质，而不是信任 spec 里的记录。
	medium, isMnt, _, err := ed.mountDetector.GetMountMedium(dir, ed.medium)
	if err != nil {
		return err
	}
	if isMnt {
		if medium == v1.StorageMediumMemory {
			ed.medium = v1.StorageMediumMemory
			return ed.teardownTmpfsOrHugetlbfs(dir)
		} else if medium == v1.StorageMediumHugePages {
			ed.medium = v1.StorageMediumHugePages
			return ed.teardownTmpfsOrHugetlbfs(dir)
		}
	}
	// 没有挂载点则按默认磁盘介质处理。
	return ed.teardownDefault(dir)
}

// teardownDefault 清理默认介质（普通磁盘目录）上的卷。
func (ed *emptyDir) teardownDefault(dir string) error {
	if utilfeature.DefaultFeatureGate.Enabled(features.LocalStorageCapacityIsolationFSQuotaMonitoring) {
		// 删除目录前先清除配额记录。
		userNamespacesEnabled := false
		if usernamespacefeature.EnabledUserNamespacesSupport() {
			userNamespacesEnabled = ed.pod.Spec.HostUsers != nil && !*ed.pod.Spec.HostUsers
		}
		err := fsquota.ClearQuota(ed.mounter, dir, userNamespacesEnabled)
		if err != nil {
			klog.Warningf("Failed to clear quota on %s: %v", dir, err)
		}
	}
	// 操作执行器已能处理同一卷上的重复操作，无需再靠重命名目录规避竞态。
	return os.RemoveAll(dir)
}

// teardownTmpfsOrHugetlbfs 先卸载 tmpfs/hugetlbfs，再删除挂载目录。
func (ed *emptyDir) teardownTmpfsOrHugetlbfs(dir string) error {
	if ed.mounter == nil {
		return fmt.Errorf("memory storage requested, but mounter is nil")
	}
	if err := ed.mounter.Unmount(dir); err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return nil
}

// getMetaDir 返回存放卷元数据（就绪标记）的插件目录。
func (ed *emptyDir) getMetaDir() string {
	return filepath.Join(ed.plugin.host.GetPodPluginDir(ed.pod.UID, utilstrings.EscapeQualifiedName(EmptyDirPluginName)), ed.volName)
}

// getVolumeSource 取出 spec 中的 emptyDir 卷源及只读标记，不支持时返回 nil。
func getVolumeSource(spec *volume.Spec) (*v1.EmptyDirVolumeSource, bool) {
	var readOnly bool
	var volumeSource *v1.EmptyDirVolumeSource

	if spec.Volume != nil && spec.Volume.EmptyDir != nil {
		volumeSource = spec.Volume.EmptyDir
		readOnly = spec.ReadOnly
	}

	return volumeSource, readOnly
}

// generateTmpfsMountOptions 生成 tmpfs 挂载参数：显式大小上限和可选的 noswap。
func (ed *emptyDir) generateTmpfsMountOptions(noswapSupported bool) (options []string) {
	// Linux 默认只给 tmpfs 50% 内存容量，有显式上限时必须写明 size。
	if ed.sizeLimit != nil && ed.sizeLimit.Value() > 0 {
		options = append(options, fmt.Sprintf("size=%d", ed.sizeLimit.Value()))
	}

	if noswapSupported {
		options = append(options, swap.TmpfsNoswapOption)
	}

	return options
}
