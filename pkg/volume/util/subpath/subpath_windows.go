//go:build windows

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

package subpath

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

// MaxPathLength 是 Windows 路径的最大长度。通常为 260，启用长路径后上限为 32767。
const MaxPathLength = 32767

// subpath 是 Windows 平台上 Subpath 接口的实现。
type subpath struct{}

// New 返回当前系统使用的 subpath.Interface。
func New(mount.Interface) Interface {
	return &subpath{}
}

// isDriveLetterorEmptyPath 判断给定路径是否为空或以 ":"、":\"、":\\" 结尾（即盘符根）。
func isDriveLetterorEmptyPath(path string) bool {
	if path == "" || strings.HasSuffix(path, ":\\\\") || strings.HasSuffix(path, ":") || strings.HasSuffix(path, ":\\") {
		return true
	}
	return false
}

// isDeviceOrUncPath 判断路径是否以 "Volume"（卷 GUID 路径）、"UNC"（去掉 "\\?\" 前缀的设备形式 UNC 路径），
// 或 "\\" / "\\"（设备命名空间路径，如 "\\?\"、"\\.\" 以及 UNC 网络路径 "\\server\share"）开头。
//
// 任何以 "\\" 开头的路径都不得解析/跟随：访问远程 UNC 目标会令 Windows 开启 SMB 会话并自动发送节点凭据，
// 从而可能被用于强制 NTLM 认证（MITRE ATT&CK T1187）。旧实现只匹配 "\\?\" 与 "\\.\" 两种设备形式，
// 会放过诸如 "\\attacker\share" 的普通 UNC 路径。这里也匹配正斜杠形式，以覆盖传入未规范化路径的调用方
// （例如在 mount.NormalizeWindowsPath 运行前读取到的符号链接目标）。
func isDeviceOrUncPath(path string) bool {
	if strings.HasPrefix(path, "Volume") || strings.HasPrefix(path, "UNC") || strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, "//") {
		return true
	}
	return false
}

// getUpperPath 去掉路径的最后一级目录。
func getUpperPath(path string) string {
	sep := fmt.Sprintf("%c", filepath.Separator)
	upperpath := strings.TrimSuffix(path, sep)
	return filepath.Dir(upperpath)
}

// isLinkPath 判断目录/文件是否为链接（SymbolicLink、Junction 或 HardLink）。
func isLinkPath(path string) (bool, error) {
	cmd := exec.Command("powershell", "/c", "$ErrorActionPreference = 'Stop'; (Get-Item -Force -LiteralPath $env:linkpath).LinkType")
	cmd.Env = append(os.Environ(), fmt.Sprintf("linkpath=%s", path))
	klog.V(8).Infof("Executing command: %q", cmd.String())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(string(output)) != "" {
		return true, nil
	}
	return false, nil
}

// evalSymlink 返回解析所有符号链接后的路径。若解析结果是设备路径或网络连接，则返回原路径。
func evalSymlink(path string) (string, error) {
	path = mount.NormalizeWindowsPath(path)
	if isDeviceOrUncPath(path) || isDriveLetterorEmptyPath(path) {
		klog.V(4).Infof("Path '%s' is not a symlink, return its original form.", path)
		return path, nil
	}
	upperpath := path
	base := ""
	for i := 0; i < MaxPathLength; i++ {
		isLink, err := isLinkPath(upperpath)
		if err != nil {
			return "", err
		}
		if isLink {
			break
		}
		// 当前层不是链接，继续向上检查父目录。
		base = filepath.Join(filepath.Base(upperpath), base)
		upperpath = getUpperPath(upperpath)
		if isDriveLetterorEmptyPath(upperpath) {
			klog.V(4).Infof("Path '%s' is not a symlink, return its original form.", path)
			return path, nil
		}
	}
	// 此命令用于取得符号链接的目标路径；-Force 让 Get-Item 也能处理 AppData 等隐藏目录。
	cmd := exec.Command("powershell", "/c", "$ErrorActionPreference = 'Stop'; (Get-Item -Force -LiteralPath $env:linkpath).Target")
	cmd.Env = append(os.Environ(), fmt.Sprintf("linkpath=%s", upperpath))
	klog.V(8).Infof("Executing command: %q", cmd.String())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	klog.V(4).Infof("evaluate path %s: symlink from %s to %s", path, upperpath, string(output))
	linkedPath := strings.TrimSpace(string(output))
	if linkedPath == "" || isDeviceOrUncPath(linkedPath) {
		klog.V(4).Infof("Path '%s' has a target %s. Return its original form.", path, linkedPath)
		return path, nil
	}
	// 目标为相对路径时，相对当前链接所在目录解析。
	if !filepath.IsAbs(linkedPath) {
		linkedPath = filepath.Join(getUpperPath(upperpath), linkedPath)
	}
	nextLink, err := evalSymlink(linkedPath)
	if err != nil {
		return path, err
	}
	return filepath.Join(nextLink, base), nil
}

// lockAndCheckSubPath 先解析符号链接，再检查 hostPath 是否位于 volumePath 内。
// 该函数会锁定所有中间子目录的句柄，调用方需在容器启动后关闭这些句柄。
func lockAndCheckSubPath(volumePath, hostPath string) ([]uintptr, error) {
	if len(volumePath) == 0 || len(hostPath) == 0 {
		return []uintptr{}, nil
	}

	finalSubPath, err := evalSymlink(hostPath)
	if err != nil {
		return []uintptr{}, fmt.Errorf("cannot evaluate link %s: %s", hostPath, err)
	}

	finalVolumePath, err := evalSymlink(volumePath)
	if err != nil {
		return []uintptr{}, fmt.Errorf("cannot read link %s: %s", volumePath, err)
	}

	return lockAndCheckSubPathWithoutSymlink(finalVolumePath, finalSubPath)
}

// lockAndCheckSubPathWithoutSymlink 锁定所有中间子目录并检查其均位于 volumePath 内。
// volumePath 与 subPath 不得含符号链接，否则返回错误。
func lockAndCheckSubPathWithoutSymlink(volumePath, subPath string) ([]uintptr, error) {
	if len(volumePath) == 0 || len(subPath) == 0 {
		return []uintptr{}, nil
	}

	// 取 subPath 相对 volumePath 的路径，便于逐级校验边界。
	relSubPath, err := filepath.Rel(volumePath, subPath)
	if err != nil {
		return []uintptr{}, fmt.Errorf("Rel(%s, %s) error: %v", volumePath, subPath, err)
	}
	if mount.StartsWithBackstep(relSubPath) {
		return []uintptr{}, fmt.Errorf("SubPath %q not within volume path %q", subPath, volumePath)
	}

	if relSubPath == "." {
		// 两者相同，无需检查中间目录。
		return []uintptr{}, nil
	}

	fileHandles := []uintptr{}
	var errorResult error

	currentFullPath := volumePath
	dirs := strings.Split(relSubPath, string(os.PathSeparator))
	for _, dir := range dirs {
		// 先锁定该级子目录，防止校验期间被替换为链接。
		currentFullPath = filepath.Join(currentFullPath, dir)
		handle, err := lockPath(currentFullPath)
		if err != nil {
			errorResult = fmt.Errorf("cannot lock path %s: %s", currentFullPath, err)
			break
		}
		fileHandles = append(fileHandles, handle)

		// 锁定后再次确认中间目录不是符号链接。
		stat, err := os.Lstat(currentFullPath)
		if err != nil {
			errorResult = fmt.Errorf("Lstat(%q) error: %v", currentFullPath, err)
			break
		}
		if stat.Mode()&os.ModeSymlink != 0 {
			errorResult = fmt.Errorf("subpath %q is an unexpected symlink after EvalSymlinks", currentFullPath)
			break
		}

		// go1.23 behavior change: https://github.com/golang/go/issues/63703#issuecomment-2535941458
		if stat.Mode()&os.ModeIrregular != 0 {
			errorResult = fmt.Errorf("subpath %q is an unexpected irregular file after EvalSymlinks", currentFullPath)
			break
		}

		if !mount.PathWithinBase(currentFullPath, volumePath) {
			errorResult = fmt.Errorf("SubPath %q not within volume path %q", currentFullPath, volumePath)
			break
		}
	}

	return fileHandles, errorResult
}

// unlockPath 释放之前的锁定目录句柄。
func unlockPath(fileHandles []uintptr) {
	if fileHandles != nil {
		for _, handle := range fileHandles {
			syscall.CloseHandle(syscall.Handle(handle))
		}
	}
}

// lockPath 锁定目录或符号链接并返回句柄；调用方需执行 "syscall.CloseHandle(handle)" 解锁。
func lockPath(path string) (uintptr, error) {
	if len(path) == 0 {
		return uintptr(syscall.InvalidHandle), syscall.ERROR_FILE_NOT_FOUND
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return uintptr(syscall.InvalidHandle), err
	}
	access := uint32(syscall.GENERIC_READ)
	sharemode := uint32(syscall.FILE_SHARE_READ)
	createmode := uint32(syscall.OPEN_EXISTING)
	flags := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS | syscall.FILE_FLAG_OPEN_REPARSE_POINT)
	fd, err := syscall.CreateFile(pathp, access, sharemode, nil, createmode, flags, 0)
	return uintptr(fd), err
}

// PrepareSafeSubpath 锁定 subPath 中的所有目录并确认它们不是符号链接。
func (sp *subpath) PrepareSafeSubpath(subPath Subpath) (newHostPath string, cleanupAction func(), err error) {
	handles, err := lockAndCheckSubPath(subPath.VolumePath, subPath.Path)

	// 容器启动时才释放目录锁，以覆盖整个挂载过程。
	cleanupAction = func() {
		unlockPath(handles)
	}
	return subPath.Path, cleanupAction, err
}

// CleanSubPaths 为空实现：Windows 下子路径无需 bind mount，因此无需清理。
func (sp *subpath) CleanSubPaths(podDir string, volumeName string) error {
	return nil
}

// SafeMakeDir 确保创建的目录不会借助符号链接逃出 base 目录。
func (sp *subpath) SafeMakeDir(subdir string, base string, perm os.FileMode) error {
	realBase, err := evalSymlink(base)
	if err != nil {
		return fmt.Errorf("error resolving symlinks in %s: %s", base, err)
	}

	realFullPath := filepath.Join(realBase, subdir)
	return doSafeMakeDir(realFullPath, realBase, perm)
}

// doSafeMakeDir 逐级创建目录，并在每一步锁定并检查边界，防止通过符号链接逃逸。
func doSafeMakeDir(pathname string, base string, perm os.FileMode) error {
	klog.V(4).Infof("Creating directory %q within base %q", pathname, base)

	if !mount.PathWithinBase(pathname, base) {
		return fmt.Errorf("path %s is outside of allowed base %s", pathname, base)
	}

	// 先快速判断目录是否已存在。
	s, err := os.Stat(pathname)
	if err == nil {
		// 路径已存在。
		if s.IsDir() {
			// 目录可能位于 base 之外，但此处无法做无竞态的检查。
			klog.V(4).Infof("Directory %s already exists", pathname)
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: pathname, Err: syscall.ENOTDIR}
	}

	// 找出已有的前缀目录，剩余部分待创建。
	existingPath, toCreate, err := findExistingPrefix(base, pathname)
	if err != nil {
		return fmt.Errorf("error opening directory %s: %s", pathname, err)
	}
	if len(toCreate) == 0 {
		return nil
	}

	// 解析符号链接后，确认已有目录仍在允许的 base 内。
	fullExistingPath, err := evalSymlink(existingPath)
	if err != nil {
		return fmt.Errorf("error opening existing directory %s: %s", existingPath, err)
	}
	fullBasePath, err := evalSymlink(base)
	if err != nil {
		return fmt.Errorf("cannot read link %s: %s", base, err)
	}
	if !mount.PathWithinBase(fullExistingPath, fullBasePath) {
		return fmt.Errorf("path %s is outside of allowed base %s", fullExistingPath, err)
	}

	// 锁定 fullBasePath 到 fullExistingPath 之间的所有目录（自上而下）。
	fileHandles, err := lockAndCheckSubPathWithoutSymlink(fullBasePath, fullExistingPath)
	defer unlockPath(fileHandles)
	if err != nil {
		return err
	}

	klog.V(4).Infof("%q already exists, %q to create", fullExistingPath, filepath.Join(toCreate...))
	currentPath := fullExistingPath
	// 逐级创建目录，并立即锁定新建目录，防止其被替换为符号链接。
	for _, dir := range toCreate {
		currentPath = filepath.Join(currentPath, dir)
		klog.V(4).Infof("Creating %s", dir)
		if err := os.Mkdir(currentPath, perm); err != nil {
			return fmt.Errorf("cannot create directory %s: %w", currentPath, err)
		}
		handle, err := lockPath(currentPath)
		if err != nil {
			return fmt.Errorf("cannot lock path %s: %s", currentPath, err)
		}
		defer syscall.CloseHandle(syscall.Handle(handle))
		// 锁定后再次确认新建目录不是符号链接。
		stat, err := os.Lstat(currentPath)
		if err != nil {
			return fmt.Errorf("Lstat(%q) error: %v", currentPath, err)
		}
		if stat.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("subpath %q is an unexpected symlink after Mkdir", currentPath)
		}
		// go1.23 behavior change: https://github.com/golang/go/issues/63703#issuecomment-2535941458
		if stat.Mode()&os.ModeIrregular != 0 {
			return fmt.Errorf("subpath %q is an unexpected irregular file after Mkdir", currentPath)
		}
	}

	return nil
}

// findExistingPrefix 返回 pathname 中最长的一段已存在前缀，以及尚需创建的剩余目录列表。
func findExistingPrefix(base, pathname string) (string, []string, error) {
	rel, err := filepath.Rel(base, pathname)
	if err != nil {
		return base, nil, err
	}

	if mount.StartsWithBackstep(rel) {
		return base, nil, fmt.Errorf("pathname(%s) is not within base(%s)", pathname, base)
	}

	if rel == "." {
		// 两者相同，无需创建任何目录。
		return pathname, []string{}, nil
	}

	dirs := strings.Split(rel, string(filepath.Separator))

	var parent string
	currentPath := base
	for i, dir := range dirs {
		parent = currentPath
		currentPath = filepath.Join(parent, dir)
		if _, err := os.Lstat(currentPath); err != nil {
			if os.IsNotExist(err) {
				return parent, dirs[i:], nil
			}
			return base, nil, err
		}
	}

	return pathname, []string{}, nil
}
