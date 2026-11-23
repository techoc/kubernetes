//go:build windows

/*
Copyright 2019 The Kubernetes Authors.

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

package mount

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"k8s.io/klog/v2"
)

// following failure codes are from https://docs.microsoft.com/en-us/windows/desktop/debug/system-error-codes--1300-1699-
// ERROR_BAD_NETPATH                 = 53
// ERROR_NETWORK_BUSY                = 54
// ERROR_UNEXP_NET_ERR               = 59
// ERROR_NETNAME_DELETED             = 64
// ERROR_NETWORK_ACCESS_DENIED       = 65
// ERROR_BAD_DEV_TYPE                = 66
// ERROR_BAD_NET_NAME                = 67
// ERROR_SESSION_CREDENTIAL_CONFLICT = 1219
// ERROR_LOGON_FAILURE               = 1326
// WSAEHOSTDOWN                      = 10064
// 网络相关错误码集中于此，便于统一判断远程挂载是否已损坏。
var errorNoList = [...]int{53, 54, 59, 64, 65, 66, 67, 1219, 1326, 10064}

// IsCorruptedMnt return true if err is about corrupted mount point
// 剥开标准库包装的错误才能拿到底层 errno，进而识别损坏的远程挂载。
func IsCorruptedMnt(err error) bool {
	if err == nil {
		return false
	}

	var underlyingError error
	switch pe := err.(type) {
	case nil:
		return false
	case *os.PathError:
		underlyingError = pe.Err
	case *os.LinkError:
		underlyingError = pe.Err
	case *os.SyscallError:
		underlyingError = pe.Err
	}

	if ee, ok := underlyingError.(syscall.Errno); ok {
		for _, errno := range errorNoList {
			if int(ee) == errno {
				klog.Warningf("IsCorruptedMnt failed with error: %v, error code: %v", err, errno)
				return true
			}
		}
	}

	return false
}

// NormalizeWindowsPath makes sure the given path is a valid path on Windows
// systems by making sure all instances of `/` are replaced with `\\`, and the
// path beings with `c:`
// 上层可能传入 POSIX 风格路径，先归一化再交给系统 API，避免路径解析歧义。
func NormalizeWindowsPath(path string) string {
	normalizedPath := strings.Replace(path, "/", "\\", -1)
	if strings.HasPrefix(normalizedPath, "\\") {
		normalizedPath = "c:" + normalizedPath
	}
	return normalizedPath
}

// ValidateDiskNumber : disk number should be a number in [0, 99]
// 磁盘号会被插入 PowerShell 命令，先校验数字格式以防注入。
func ValidateDiskNumber(disk string) error {
	if _, err := strconv.Atoi(disk); err != nil {
		return fmt.Errorf("wrong disk number format: %q, err: %v", disk, err)
	}
	return nil
}

// isMountPointMatch determines if the mountpoint matches the dir
// Windows 下没有设备号语义，按路径精确比较即可。
func isMountPointMatch(mp MountPoint, dir string) bool {
	return mp.Path == dir
}

// PathExists returns true if the specified path exists.
// TODO: clean this up to use pkg/util/file/FileExists
// 损坏的挂载点虽 stat 失败，但不应被当成不存在，故单独返回 true 并带出错误。
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else if IsCorruptedMnt(err) {
		return true, err
	}
	return false, err
}
