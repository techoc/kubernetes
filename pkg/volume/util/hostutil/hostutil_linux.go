//go:build linux

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

package hostutil

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/opencontainers/selinux/go-selinux"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilpath "k8s.io/utils/path"
)

const (
	// mountinfo 文件的位置。
	procMountInfoPath = "/proc/self/mountinfo"
)

// HostUtil 在 Linux 平台上实现 HostUtils 接口。
type HostUtil struct {
}

// NewHostUtil 返回一个在 Linux 平台上实现 HostUtils 接口的结构体。
func NewHostUtil() *HostUtil {
	return &HostUtil{}
}

// DeviceOpened 用 O_EXCL 标志调用 Open 检查块设备是否已被占用。
// 路径不是设备时记录日志并返回 false、nil；
// Open 返回 EBUSY 时返回 true、nil；打开成功时返回 false、nil；
// 其余情况返回 false 和错误。
func (hu *HostUtil) DeviceOpened(pathname string) (bool, error) {
	return ExclusiveOpenFailsOnDevice(pathname)
}

// PathIsDevice 根据 os.Stat 返回的 FileInfo 判断路径是否为设备。
func (hu *HostUtil) PathIsDevice(pathname string) (bool, error) {
	pathType, err := hu.GetFileType(pathname)
	isDevice := pathType == FileTypeCharDev || pathType == FileTypeBlockDev
	return isDevice, err
}

// ExclusiveOpenFailsOnDevice 用 O_EXCL 标志调用 Open 检查块设备是否已被占用。
func ExclusiveOpenFailsOnDevice(pathname string) (bool, error) {
	var isDevice bool
	finfo, err := os.Stat(pathname)
	if os.IsNotExist(err) {
		isDevice = false
	}
	// os.Stat 本身失败。
	if err != nil {
		return false, fmt.Errorf(
			"PathIsDevice failed for path %q: %v",
			pathname,
			err)
	}
	// 路径确实是设备。
	if finfo.Mode()&os.ModeDevice != 0 {
		isDevice = true
	}

	if !isDevice {
		klog.Errorf("Path %q is not referring to a device.", pathname)
		return false, nil
	}
	fd, errno := unix.Open(pathname, unix.O_RDONLY|unix.O_EXCL|unix.O_CLOEXEC, 0)
	// 设备被占用时 Open 返回的 fd 无效，此时 Close 失败属预期行为。
	defer unix.Close(fd)
	if errno == nil {
		// 设备未被占用。
		return false, nil
	} else if errno == unix.EBUSY {
		// 设备正在被使用。
		return true, nil
	}
	// Open 调用返回了其他错误。
	return false, errno
}

// GetDeviceNameFromMount 给定挂载点，从其全局挂载点中找出设备名。
func (hu *HostUtil) GetDeviceNameFromMount(mounter mount.Interface, mountPath, pluginMountDir string) (string, error) {
	return getDeviceNameFromMount(mounter, mountPath, pluginMountDir)
}

// getDeviceNameFromMount 从 /proc/self/mountinfo 中查找设备名，要求挂载点引用匹配给定的插件挂载目录。
// 若没有任何匹配项，则返回 mountPath 的末段作为卷名。
func getDeviceNameFromMount(mounter mount.Interface, mountPath, pluginMountDir string) (string, error) {
	refs, err := mounter.GetMountRefs(mountPath)
	if err != nil {
		klog.V(4).Infof("GetMountRefs failed for mount path %q: %v", mountPath, err)
		return "", err
	}
	if len(refs) == 0 {
		klog.V(4).Infof("Directory %s is not mounted", mountPath)
		return "", fmt.Errorf("directory %s is not mounted", mountPath)
	}
	for _, ref := range refs {
		if strings.HasPrefix(ref, pluginMountDir) {
			volumeID, err := filepath.Rel(pluginMountDir, ref)
			if err != nil {
				klog.Errorf("Failed to get volume id from mount %s - %v", mountPath, err)
				return "", err
			}
			return volumeID, nil
		}
	}

	return path.Base(mountPath), nil
}

// MakeRShared 检查路径所在挂载点是否具有 rshared 传播属性，若没有则将其绑定挂载为 rshared。
func (hu *HostUtil) MakeRShared(path string) error {
	return DoMakeRShared(path, procMountInfoPath)
}

// GetFileType 判断路径是文件、目录、socket、块设备还是字符设备。
func (hu *HostUtil) GetFileType(pathname string) (FileType, error) {
	return getFileType(pathname)
}

// PathExists 判断路径是否存在；除“文件不存在”以外的错误都会返回。
func (hu *HostUtil) PathExists(pathname string) (bool, error) {
	return utilpath.Exists(utilpath.CheckFollowSymlink, pathname)
}

// EvalHostSymlinks 返回解析符号链接后的路径名。
func (hu *HostUtil) EvalHostSymlinks(pathname string) (string, error) {
	return filepath.EvalSymlinks(pathname)
}

// FindMountInfo 返回给定路径所在的挂载点信息。
func (hu *HostUtil) FindMountInfo(path string) (mount.MountInfo, error) {
	return findMountInfo(path, procMountInfoPath)
}

// isShared 判断路径所在挂载点是否具有 shared 传播属性。
func isShared(mount string, mountInfoPath string) (bool, error) {
	info, err := findMountInfo(mount, mountInfoPath)
	if err != nil {
		return false, err
	}

	// 解析可选字段中的 shared 标记。
	for _, opt := range info.OptionalFields {
		if strings.HasPrefix(opt, "shared:") {
			return true, nil
		}
	}
	return false, nil
}

// findMountInfo 在 mountinfo 中查找路径所在的最长（最后出现的）挂载点。
func findMountInfo(path, mountInfoPath string) (mount.MountInfo, error) {
	infos, err := mount.ParseMountInfo(mountInfoPath)
	if err != nil {
		return mount.MountInfo{}, err
	}

	// 倒序扫描 mountinfo：第一个作为 path 前缀的挂载点即 path 实际所在的挂载。
	var info *mount.MountInfo
	for i := len(infos) - 1; i >= 0; i-- {
		if mount.PathWithinBase(path, infos[i].MountPoint) {
			info = &infos[i]
			break
		}
	}
	if info == nil {
		return mount.MountInfo{}, fmt.Errorf("cannot find mount point for %q", path)
	}
	return *info, nil
}

// DoMakeRShared 是 Linux 上 MakeRShared 的通用实现：检查路径是否已共享，必要时绑定挂载为 rshared。
// mountCmd 和 mountArgs 预期为类 mount 命令；DoMakeRShared 会追加
// '--bind <path> <path>' 与 '--make-rshared <path>'。
func DoMakeRShared(path string, mountInfoFilename string) error {
	shared, err := isShared(path, mountInfoFilename)
	if err != nil {
		return err
	}
	if shared {
		klog.V(4).Infof("Directory %s is already on a shared mount", path)
		return nil
	}

	klog.V(2).Infof("Bind-mounting %q with shared mount propagation", path)
	// 等价于 mount --bind /var/lib/kubelet /var/lib/kubelet
	if err := syscall.Mount(path, path, "" /*fstype*/, syscall.MS_BIND, "" /*data*/); err != nil {
		return fmt.Errorf("failed to bind-mount %s: %v", path, err)
	}

	// 等价于 mount --make-rshared /var/lib/kubelet
	if err := syscall.Mount(path, path, "" /*fstype*/, syscall.MS_SHARED|syscall.MS_REC, "" /*data*/); err != nil {
		return fmt.Errorf("failed to make %s rshared: %v", path, err)
	}

	return nil
}

// seLinuxEnabledFunc 对应 selinux.SELinuxEnabled，便于在单测中替换。
type seLinuxEnabledFunc func() bool

// GetSELinux 是 Linux 上 GetSELinuxSupport 的通用实现。
func GetSELinux(path string, mountInfoFilename string, selinuxEnabled seLinuxEnabledFunc) (bool, error) {
	// SELinux 关闭时无需解析 /proc/mounts。
	if !selinuxEnabled() {
		return false, nil
	}

	info, err := findMountInfo(path, mountInfoFilename)
	if err != nil {
		return false, err
	}

	// "seclabel" 可能出现在挂载选项或超级块选项中。
	return slices.Contains(info.SuperOptions, "seclabel") || slices.Contains(info.MountOptions, "seclabel"), nil
}

// GetSELinuxSupport 判断路径所在挂载点是否支持 SELinux。
func (hu *HostUtil) GetSELinuxSupport(pathname string) (bool, error) {
	return GetSELinux(pathname, procMountInfoPath, selinux.GetEnabled)
}

// GetOwner 返回路径所属用户和组的数字 ID；先解析符号链接再取值。
func (hu *HostUtil) GetOwner(pathname string) (int64, int64, error) {
	realpath, err := filepath.EvalSymlinks(pathname)
	if err != nil {
		return -1, -1, err
	}
	return GetOwnerLinux(realpath)
}

// GetMode 返回路径的权限位。
func (hu *HostUtil) GetMode(pathname string) (os.FileMode, error) {
	return GetModeLinux(pathname)
}

// GetOwnerLinux 返回路径所属用户和组的数字 ID。
// pathname 必须已解析过符号链接。
func GetOwnerLinux(pathname string) (int64, int64, error) {
	info, err := os.Stat(pathname)
	if err != nil {
		return -1, -1, err
	}
	stat := info.Sys().(*syscall.Stat_t)
	return int64(stat.Uid), int64(stat.Gid), nil
}

// GetModeLinux 返回路径的权限位。
func GetModeLinux(pathname string) (os.FileMode, error) {
	info, err := os.Stat(pathname)
	if err != nil {
		return 0, err
	}
	return info.Mode(), nil
}

// GetSELinuxMountContext 返回挂载点上 -o context=XYZ 挂载选项的值。
func (hu *HostUtil) GetSELinuxMountContext(pathname string) (string, error) {
	return getSELinuxMountContext(pathname, procMountInfoPath, selinux.GetEnabled)
}

// getSELinuxMountContext 是 GetSELinuxMountContext 在 Linux 上的通用实现，
// 额外拆出函数便于单测注入 selinuxEnabled。
func getSELinuxMountContext(path string, mountInfoFilename string, selinuxEnabled seLinuxEnabledFunc) (string, error) {
	// SELinux 关闭时无需解析 /proc/mounts。
	if !selinuxEnabled() {
		return "", nil
	}

	info, err := findMountInfo(path, mountInfoFilename)
	if err != nil {
		return "", err
	}

	for _, opt := range info.SuperOptions {
		if !strings.HasPrefix(opt, "context=") {
			continue
		}
		// 去掉 context= 前缀。
		context := strings.TrimPrefix(opt, "context=")
		// 去掉包裹的双引号。
		context = strings.Trim(context, "\"")
		return context, nil
	}
	return "", nil
}
