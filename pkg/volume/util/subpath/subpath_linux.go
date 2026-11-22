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

// Package subpath 在 Linux 上实现 subpath 的安全处理：解析卷内子目录，
// 通过 /proc/<pid>/fd/<fd> 做 bind mount，防止 symlink 逃逸出卷路径。
package subpath

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

const (
	// 存放 subpath 挂载点的目录
	// TODO: 改为通过 kubelet_getters 传入目录
	containerSubPathDirectoryName = "volume-subpaths"
	// 遍历目录时不跟随符号链接所用的 syscall.Openat 标志
	nofollowFlags = unix.O_RDONLY | unix.O_NOFOLLOW
	// 获取文件描述符时不跟随符号链接所用的标志
	openFDFlags = unix.O_NOFOLLOW | unix.O_PATH
)

type subpath struct {
	mounter mount.Interface
}

// New 返回适用于当前系统的 subpath.Interface。
func New(mounter mount.Interface) Interface {
	return &subpath{
		mounter: mounter,
	}
}

// CleanSubPaths 清理指定 pod 的某个卷下所有 subpath bind mount。
func (sp *subpath) CleanSubPaths(podDir string, volumeName string) error {
	return doCleanSubPaths(sp.mounter, podDir, volumeName)
}

// SafeMakeDir 在 base 范围内安全地创建子目录，防止借助符号链接逃出 base。
func (sp *subpath) SafeMakeDir(subdir string, base string, perm os.FileMode) error {
	realBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		return fmt.Errorf("error resolving symlinks in %s: %s", base, err)
	}

	realFullPath := filepath.Join(realBase, subdir)

	return doSafeMakeDir(realFullPath, realBase, perm)
}

// PrepareSafeSubpath 为 subpath 建立 bind mount，返回可供容器使用的宿主路径。
func (sp *subpath) PrepareSafeSubpath(subPath Subpath) (newHostPath string, cleanupAction func(), err error) {
	newHostPath, err = doBindSubPath(sp.mounter, subPath)

	// 容器启动时无需清理动作；容器停止时由 CleanSubPaths 负责卸载 bind mount。
	cleanupAction = nil
	return newHostPath, cleanupAction, err
}

// safeOpenSubPath 打开 subpath 并返回其文件描述符。
func safeOpenSubPath(mounter mount.Interface, subpath Subpath) (int, error) {
	if !mount.PathWithinBase(subpath.Path, subpath.VolumePath) {
		return -1, fmt.Errorf("subpath %q not within volume path %q", subpath.Path, subpath.VolumePath)
	}
	fd, err := doSafeOpen(subpath.Path, subpath.VolumePath)
	if err != nil {
		return -1, fmt.Errorf("error opening subpath %v: %v", subpath.Path, err)
	}
	return fd, nil
}

// prepareSubpathTarget 为 subpath 的 bind mount 创建挂载目标。
// 当目标已存在且其上已挂载内容时返回 true。
func prepareSubpathTarget(mounter mount.Interface, subpath Subpath) (bool, string, error) {
	// 提前检查 subpath 是否已经被 bind mount。
	bindPathTarget := getSubpathBindTarget(subpath)
	isMount, err := mounter.IsMountPoint(bindPathTarget)
	if err != nil {
		if os.IsNotExist(err) {
			// 忽略不存在的错误：下面会按需创建文件或目录。
			isMount = false
		} else if mount.IsCorruptedMnt(err) {
			// 挂载点已损坏，尝试卸载后重建。
			klog.Warningf("Detected corrupted mount at %s (error: %v), unmounting", bindPathTarget, err)
			if unmountErr := mounter.Unmount(bindPathTarget); unmountErr != nil {
				return false, "", fmt.Errorf("error unmounting corrupted mount %s: %w", bindPathTarget, unmountErr)
			}
			isMount = false
		} else {
			return false, "", fmt.Errorf("error checking path %s for mount: %w", bindPathTarget, err)
		}
	}
	if isMount {
		// 已挂载，需确认它是否 bind mount 到了同一个路径。
		samePath, err := checkSubPathFileEqual(subpath, bindPathTarget)
		if err != nil {
			return false, "", fmt.Errorf("error checking subpath mount info for %s: %s", bindPathTarget, err)
		}
		if !samePath {
			// 使用惰性卸载，避免挂起的 FUSE/GlusterFS flush 阻塞 pod 恢复。
			klog.V(4).Infof("Subpath bind mount at %s points to a different inode/device than source, will lazy-unmount and recreate", bindPathTarget)
			if err = lazyUnmountFn(bindPathTarget); err != nil {
				return false, "", fmt.Errorf("error lazy-unmounting stale subpath mount %s: %w", bindPathTarget, err)
			}
		} else {
			// 已正确挂载，无需重复操作。
			klog.V(5).Infof("Skipping bind-mounting subpath %s: already mounted", bindPathTarget)
			return true, bindPathTarget, nil
		}
	}

	// bindPathTarget 位于 /var/lib/kubelet 下，即使是容器化的 kubelet 也无需路径转换即可访问。
	bindParent := filepath.Dir(bindPathTarget)
	err = os.MkdirAll(bindParent, 0750)
	if err != nil && !os.IsExist(err) {
		return false, "", fmt.Errorf("error creating directory %s: %s", bindParent, err)
	}

	t, err := os.Lstat(subpath.Path)
	if err != nil {
		return false, "", fmt.Errorf("lstat %s failed: %s", subpath.Path, err)
	}

	if t.Mode()&os.ModeDir > 0 {
		if err = os.Mkdir(bindPathTarget, 0750); err != nil && !os.IsExist(err) {
			return false, "", fmt.Errorf("error creating directory %s: %s", bindPathTarget, err)
		}
	} else {
		// 相当于 "/bin/touch <bindPathTarget>"。
		// 普通文件即可作为所有类型目标（符号链接、设备、管道、socket 等）的挂载点，
		// bind mount 会将其正确替换为源文件的类型。
		if err = os.WriteFile(bindPathTarget, []byte{}, 0640); err != nil {
			return false, "", fmt.Errorf("error creating file %s: %s", bindPathTarget, err)
		}
	}
	return false, bindPathTarget, nil
}

// lazyUnmountFn 是包级变量，便于测试替换而无需 fork 出新二进制。
var lazyUnmountFn = func(path string) error {
	return syscall.Unmount(path, syscall.MNT_DETACH)
}

// checkSubPathFileEqual 判断 subpath 源与 bind mount 目标是否指向相同的 inode/device。
func checkSubPathFileEqual(subpath Subpath, bindMountTarget string) (bool, error) {
	s, err := os.Lstat(subpath.Path)
	if err != nil {
		return false, fmt.Errorf("stat %s failed: %s", subpath.Path, err)
	}

	t, err := os.Lstat(bindMountTarget)
	if err != nil {
		return false, fmt.Errorf("lstat %s failed: %s", bindMountTarget, err)
	}

	if !os.SameFile(s, t) {
		return false, nil
	}
	return true, nil
}

// getSubpathBindTarget 返回 subpath 的 bind mount 目标路径。
func getSubpathBindTarget(subpath Subpath) string {
	// 容器名是 DNS label，可安全用作目录名。
	return filepath.Join(subpath.PodDir, containerSubPathDirectoryName, subpath.VolumeName, subpath.ContainerName, strconv.Itoa(subpath.VolumeMountIndex))
}

// doBindSubPath 为 subpath 建立 bind mount 并返回宿主路径。
func doBindSubPath(mounter mount.Interface, subpath Subpath) (hostPath string, err error) {
	// Linux 上 kubelet 运行在宿主机：
	// - 安全地打开 subpath
	// - 将 /proc/<kubelet 的 pid>/fd/<fd> bind mount 到 subpath 目标
	// 用户无法把 /proc/<kubelet 的 pid>/fd/<fd> 改指向恶意位置。

	// 在此一次性解析所有符号链接，供后续函数使用。
	newVolumePath, err := filepath.EvalSymlinks(subpath.VolumePath)
	if err != nil {
		return "", fmt.Errorf("error resolving symlinks in %q: %v", subpath.VolumePath, err)
	}
	newPath, err := filepath.EvalSymlinks(subpath.Path)
	if err != nil {
		return "", fmt.Errorf("error resolving symlinks in %q: %v", subpath.Path, err)
	}
	klog.V(5).Infof("doBindSubPath %q (%q) for volumepath %q", subpath.Path, newPath, subpath.VolumePath)
	subpath.VolumePath = newVolumePath
	subpath.Path = newPath

	fd, err := safeOpenSubPath(mounter, subpath)
	if err != nil {
		return "", err
	}
	defer syscall.Close(fd)

	alreadyMounted, bindPathTarget, err := prepareSubpathTarget(mounter, subpath)
	if err != nil {
		return "", err
	}
	if alreadyMounted {
		return bindPathTarget, nil
	}

	success := false
	defer func() {
		// 出错时清理 subpath。
		if !success {
			klog.V(4).Infof("doBindSubPath() failed for %q, cleaning up subpath", bindPathTarget)
			if cleanErr := cleanSubPath(mounter, subpath); cleanErr != nil {
				klog.Errorf("Failed to clean subpath %q: %v", bindPathTarget, cleanErr)
			}
		}
	}()

	kubeletPid := os.Getpid()
	mountSource := fmt.Sprintf("/proc/%d/fd/%v", kubeletPid, fd)

	// 执行 bind mount。
	options := []string{"bind"}
	mountFlags := []string{"--no-canonicalize"}
	klog.V(5).Infof("bind mounting %q at %q", mountSource, bindPathTarget)
	if err = mounter.MountSensitiveWithoutSystemdWithMountFlags(mountSource, bindPathTarget, "" /*fstype*/, options, nil /* sensitiveOptions */, mountFlags); err != nil {
		return "", fmt.Errorf("error mounting %s: %s", subpath.Path, err)
	}
	success = true

	klog.V(3).Infof("Bound SubPath %s into %s", subpath.Path, bindPathTarget)
	return bindPathTarget, nil
}

// doCleanSubPaths 拆除某个 pod 的所有 subpath bind mount。
func doCleanSubPaths(mounter mount.Interface, podDir string, volumeName string) error {
	// 扫描 /var/lib/kubelet/pods/<uid>/volume-subpaths/<volume>/*
	subPathDir := filepath.Join(podDir, containerSubPathDirectoryName, volumeName)
	klog.V(4).Infof("Cleaning up subpath mounts for %s", subPathDir)

	containerDirs, err := os.ReadDir(subPathDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("error reading %s: %s", subPathDir, err)
	}

	for _, containerDir := range containerDirs {
		if !containerDir.IsDir() {
			klog.V(4).Infof("Container file is not a directory: %s", containerDir.Name())
			continue
		}
		klog.V(4).Infof("Cleaning up subpath mounts for container %s", containerDir.Name())

		// 扫描 /var/lib/kubelet/pods/<uid>/volume-subpaths/<volume>/<container name>/*
		fullContainerDirPath := filepath.Join(subPathDir, containerDir.Name())
		// 原先的遍历方式是 ReadDir，对诸如 "stale NFS file handle" 之类的错误不够健壮，
		// 后续补丁改用 filepath.Walk：错误可以向上传递并由回调 WalkFunc 处理。
		// Go 1.16 之后引入的 WalkDir 更高效：回调 WalkDirFunc 在读取目录前就被调用，
		// 当容器的 subPath 包含大量目录时可以省下一些时间。
		// 详情见 https://github.com/kubernetes/kubernetes/pull/71804 和 https://github.com/kubernetes/kubernetes/issues/107667。
		err = filepath.WalkDir(fullContainerDirPath, func(path string, info os.DirEntry, _ error) error {
			if path == fullContainerDirPath {
				// 跳过顶层目录。
				return nil
			}

			// 将错误向上传递，交由 doCleanSubPath 处理。
			if err = doCleanSubPath(mounter, fullContainerDirPath, filepath.Base(path)); err != nil {
				return err
			}

			// 需要检查 info 不为 nil：挂载失效或权限错误时传入的 err 可能非空。
			if info != nil && info.IsDir() {
				// 跳过卷的子目录：只需卸载第一层，否则会尝试卸载卷内的子目录。
				return filepath.SkipDir
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("error processing %s: %s", fullContainerDirPath, err)
		}

		// 整个容器已处理完毕，移除其目录。
		if err := os.Remove(fullContainerDirPath); err != nil {
			return fmt.Errorf("error deleting %s: %s", fullContainerDirPath, err)
		}
		klog.V(5).Infof("Removed %s", fullContainerDirPath)
	}
	// 整个 pod 卷的 subpath 已清理完毕，移除其 subpath 目录。
	if err := os.Remove(subPathDir); err != nil {
		return fmt.Errorf("error deleting %s: %s", subPathDir, err)
	}
	klog.V(5).Infof("Removed %s", subPathDir)

	// 如果已是最后一个，移除整个 subpath 目录。
	podSubPathDir := filepath.Join(podDir, containerSubPathDirectoryName)
	if err := os.Remove(podSubPathDir); err != nil && !os.IsExist(err) {
		return fmt.Errorf("error deleting %s: %s", podSubPathDir, err)
	}
	klog.V(5).Infof("Removed %s", podSubPathDir)
	return nil
}

// doCleanSubPath 拆除单个 subpath bind mount。
func doCleanSubPath(mounter mount.Interface, fullContainerDirPath, subPathIndex string) error {
	// 处理 /var/lib/kubelet/pods/<uid>/volume-subpaths/<volume>/<container name>/<subPathName>
	klog.V(4).Infof("Cleaning up subpath mounts for subpath %v", subPathIndex)
	fullSubPath := filepath.Join(fullContainerDirPath, subPathIndex)

	if err := mount.CleanupMountPoint(fullSubPath, mounter, true); err != nil {
		return fmt.Errorf("error cleaning subpath mount %s: %s", fullSubPath, err)
	}

	klog.V(4).Infof("Successfully cleaned subpath directory %s", fullSubPath)
	return nil
}

// cleanSubPath 拆除 subpath bind mount，并递归删除变空的目录。
func cleanSubPath(mounter mount.Interface, subpath Subpath) error {
	containerDir := filepath.Join(subpath.PodDir, containerSubPathDirectoryName, subpath.VolumeName, subpath.ContainerName)

	// 清理子目录的 bind mount。
	if err := doCleanSubPath(mounter, containerDir, strconv.Itoa(subpath.VolumeMountIndex)); err != nil && !os.IsNotExist(err) {
		return err
	}

	// 递归删除空目录。
	if err := removeEmptyDirs(subpath.PodDir, containerDir); err != nil {
		return err
	}

	return nil
}

// removeEmptyDirs 从 endDir 向 baseDir 反向遍历，遇到非空目录即停止，
// 途中删除所有空目录。
func removeEmptyDirs(baseDir, endDir string) error {
	if !mount.PathWithinBase(endDir, baseDir) {
		return fmt.Errorf("endDir %q is not within baseDir %q", endDir, baseDir)
	}

	for curDir := endDir; curDir != baseDir; curDir = filepath.Dir(curDir) {
		s, err := os.Stat(curDir)
		if err != nil {
			if os.IsNotExist(err) {
				klog.V(5).Infof("curDir %q doesn't exist, skipping", curDir)
				continue
			}
			return fmt.Errorf("error stat %q: %v", curDir, err)
		}
		if !s.IsDir() {
			return fmt.Errorf("path %q not a directory", curDir)
		}

		err = os.Remove(curDir)
		if os.IsExist(err) {
			klog.V(5).Infof("Directory %q not empty, not removing", curDir)
			break
		} else if err != nil {
			return fmt.Errorf("error removing directory %q: %v", curDir, err)
		}
		klog.V(5).Infof("Removed directory %q", curDir)
	}
	return nil
}

// doSafeMakeDir 在 pathname 处创建目录，但仅当它位于 base 范围内时。
func doSafeMakeDir(pathname string, base string, perm os.FileMode) error {
	klog.V(4).Infof("Creating directory %q within base %q", pathname, base)

	if !mount.PathWithinBase(pathname, base) {
		return fmt.Errorf("path %s is outside of allowed base %s", pathname, base)
	}

	// 快速检查目录是否已存在。
	s, err := os.Stat(pathname)
	if err == nil {
		// 路径已存在。
		if s.IsDir() {
			// 目录已存在。它可能位于父目录之外，但无法做无竞态的检查。
			klog.V(4).Infof("Directory %s already exists", pathname)
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: pathname, Err: syscall.ENOTDIR}
	}

	// 找出其中已存在的最长目录前缀。
	existingPath, toCreate, err := findExistingPrefix(base, pathname)
	if err != nil {
		return fmt.Errorf("error opening directory %s: %s", pathname, err)
	}
	// 确认已存在的目录确实位于允许的 base 之内。
	fullExistingPath, err := filepath.EvalSymlinks(existingPath)
	if err != nil {
		return fmt.Errorf("error opening directory %s: %s", existingPath, err)
	}
	if !mount.PathWithinBase(fullExistingPath, base) {
		return fmt.Errorf("path %s is outside of allowed base %s", fullExistingPath, err)
	}

	klog.V(4).Infof("%q already exists, %q to create", fullExistingPath, filepath.Join(toCreate...))
	parentFD, err := doSafeOpen(fullExistingPath, base)
	if err != nil {
		return fmt.Errorf("cannot open directory %s: %s", existingPath, err)
	}
	childFD := -1
	defer func() {
		if parentFD != -1 {
			if err = syscall.Close(parentFD); err != nil {
				klog.V(4).Infof("Closing FD %v failed for safemkdir(%v): %v", parentFD, pathname, err)
			}
		}
		if childFD != -1 {
			if err = syscall.Close(childFD); err != nil {
				klog.V(4).Infof("Closing FD %v failed for safemkdir(%v): %v", childFD, pathname, err)
			}
		}
	}()

	currentPath := fullExistingPath
	// 逐级创建目录，确保没人能把已创建的目录替换成符号链接。
	for _, dir := range toCreate {
		currentPath = filepath.Join(currentPath, dir)
		klog.V(4).Infof("Creating %s", dir)
		err = syscall.Mkdirat(parentFD, currentPath, uint32(perm))
		if err != nil {
			return fmt.Errorf("cannot create directory %s: %w", currentPath, err)
		}
		// 钻进刚创建的目录。
		childFD, err = syscall.Openat(parentFD, dir, nofollowFlags|unix.O_CLOEXEC, 0)
		if err != nil {
			return fmt.Errorf("cannot open %s: %s", currentPath, err)
		}
		// 可以确信 childFD 是安全的。用户可能在 Mkdirat() 与 Openat() 之间改动它，但：
		// - 无法改成符号链接——我们使用了 nofollowFlags；
		// - 即便改成文件（或设备、管道、socket 等），要么后续 Mkdirat() 失败，
		//   要么我们把该文件挂载进用户容器。两种情况下安全性都不受影响，
		//   用户要么拿到错误，要么拿到本来就可访问的文件。

		if err = syscall.Close(parentFD); err != nil {
			klog.V(4).Infof("Closing FD %v failed for safemkdir(%v): %v", parentFD, pathname, err)
		}
		parentFD = childFD
		childFD = -1

		// 目录都已创建。上面的 mkdirat(..., perm) 受当前 umask 影响，
		// 必须给所有新建目录补上正确的权限（容器将把它作为 subpath 访问），
		// 这样用户才能读写。
		// parentFD 此时指向最后创建的目录。

		// 把 perm（os.FileMode）转换为 fchmod() 需要的 uint32。
		kernelPerm := uint32(perm & os.ModePerm)
		if perm&os.ModeSetgid > 0 {
			kernelPerm |= syscall.S_ISGID
		}
		if perm&os.ModeSetuid > 0 {
			kernelPerm |= syscall.S_ISUID
		}
		if perm&os.ModeSticky > 0 {
			kernelPerm |= syscall.S_ISVTX
		}
		if err = syscall.Fchmod(parentFD, kernelPerm); err != nil {
			return fmt.Errorf("chmod %q failed: %s", currentPath, err)
		}
	}

	return nil
}

// findExistingPrefix 找出 pathname 中已存在的前缀，
// 并返回尚不存在的剩余目录列表。
func findExistingPrefix(base, pathname string) (string, []string, error) {
	rel, err := filepath.Rel(base, pathname)
	if err != nil {
		return base, nil, err
	}
	dirs := strings.Split(rel, string(filepath.Separator))

	// 循环执行 OpenAt 找到第一个不存在的目录，同时解析符号链接。
	// 这样比遍历所有目录逐个调用 os.Stat() 更快，因为符号链接只会在 OpenAt() 时解析一次。
	currentPath := base
	fd, err := syscall.Open(currentPath, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return pathname, nil, fmt.Errorf("error opening %s: %s", currentPath, err)
	}
	defer func() {
		if err = syscall.Close(fd); err != nil {
			klog.V(4).Infof("Closing FD %v failed for findExistingPrefix(%v): %v", fd, pathname, err)
		}
	}()
	for i, dir := range dirs {
		// 这里使用 O_PATH，防止用户把目录换成 fifo 时发生挂起。
		childFD, err := syscall.Openat(fd, dir, unix.O_PATH|unix.O_CLOEXEC, 0)
		if err != nil {
			if os.IsNotExist(err) {
				return currentPath, dirs[i:], nil
			}
			return base, nil, err
		}
		if err = syscall.Close(fd); err != nil {
			klog.V(4).Infof("Closing FD %v failed for findExistingPrefix(%v): %v", fd, pathname, err)
		}
		fd = childFD
		currentPath = filepath.Join(currentPath, dir)
	}
	return pathname, []string{}, nil
}

// doSafeOpen 打开 path 并返回其文件描述符。
// 不允许符号链接（pathname 必须已解析完符号链接），
// 且路径必须位于 base 目录之内。
func doSafeOpen(pathname string, base string) (int, error) {
	pathname = filepath.Clean(pathname)
	base = filepath.Clean(base)

	// 计算需要逐级跟随的路径段。
	subpath, err := filepath.Rel(base, pathname)
	if err != nil {
		return -1, err
	}
	segments := strings.Split(subpath, string(filepath.Separator))

	// 前提：base 是唯一受我们控制的目录，且不允许 base 本身是符号链接。
	parentFD, err := syscall.Open(base, nofollowFlags|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("cannot open directory %s: %s", base, err)
	}
	defer func() {
		if parentFD != -1 {
			if err = syscall.Close(parentFD); err != nil {
				klog.V(4).Infof("Closing FD %v failed for safeopen(%v): %v", parentFD, pathname, err)
			}
		}
	}()

	childFD := -1
	defer func() {
		if childFD != -1 {
			if err = syscall.Close(childFD); err != nil {
				klog.V(4).Infof("Closing FD %v failed for safeopen(%v): %v", childFD, pathname, err)
			}
		}
	}()

	currentPath := base

	// 用 openat() 逐级跟随，确保用户无法把已有目录改成符号链接。
	for _, seg := range segments {
		var deviceStat unix.Stat_t

		currentPath = filepath.Join(currentPath, seg)
		if !mount.PathWithinBase(currentPath, base) {
			return -1, fmt.Errorf("path %s is outside of allowed base %s", currentPath, base)
		}

		// 若是自动挂载目录则触发自动挂载；若不是目录则忽略错误。
		// 注意结尾斜杠是必须的，参见 openat(2) 和 open_by_handle_at(2) 中的 "automount"。
		unix.Fstatat(parentFD, seg+"/", &deviceStat, unix.AT_SYMLINK_NOFOLLOW)

		klog.V(5).Infof("Opening path %s", currentPath)
		childFD, err = syscall.Openat(parentFD, seg, openFDFlags|unix.O_CLOEXEC, 0)
		if err != nil {
			return -1, fmt.Errorf("cannot open %s: %s", currentPath, err)
		}

		err := unix.Fstat(childFD, &deviceStat)
		if err != nil {
			return -1, fmt.Errorf("error running fstat on %s with %v", currentPath, err)
		}
		fileFmt := deviceStat.Mode & syscall.S_IFMT
		if fileFmt == syscall.S_IFLNK {
			return -1, fmt.Errorf("unexpected symlink found %s", currentPath)
		}

		// 关闭 parentFD。
		if err = syscall.Close(parentFD); err != nil {
			return -1, fmt.Errorf("closing fd for %q failed: %v", filepath.Dir(currentPath), err)
		}
		// 将 child 设为新的 parent。
		parentFD = childFD
		childFD = -1
	}

	// 一路走到末尾，返回该 fd，不要关闭。
	finalFD := parentFD
	parentFD = -1

	return finalFD, nil
}
