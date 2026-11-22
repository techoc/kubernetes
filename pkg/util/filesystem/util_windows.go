//go:build windows

/*
Copyright 2023 The Kubernetes Authors.

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

package filesystem

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"golang.org/x/sys/windows"
)

const (
	// 两次尝试连接 Unix 域套接字之间的等待时长。
	// 参见 https://github.com/kubernetes/kubernetes/issues/104584：
	// 首次尝试很可能失败，因此需要重试。
	socketDialRetryPeriod = 1 * time.Second
	// 连接 Unix 域套接字的总超时时间（含重试）。
	socketDialTimeout = 4 * time.Second
)

// IsUnixDomainSocket 判断给定文件是否为 AF_UNIX 套接字文件。
// 注意内部包含重试逻辑，判断最多可能耗时 4 秒。
func IsUnixDomainSocket(filePath string) (bool, error) {
	// 通过 FSCTL_GET_REPARSE_POINT 查询重解析点（https://docs.microsoft.com/en-us/windows/win32/fileio/reparse-points）
	// 并检查 reparseTagSocket 的方式在 Windows 1809 上不可靠：
	// 若套接字文件由容器在 bind mount 目录中创建，而 kubelet 在宿主机发起 FSCTL，就会失效。

	// 文件不存在自然不可能是 Unix 域套接字。
	if info, err := os.Stat(filePath); os.IsNotExist(err) {
		return false, fmt.Errorf("File %s not found. Err: %v", filePath, err)
	} else if err == nil && info.Mode()&os.ModeSocket != 0 { // Go 1.23 起 Windows 支持 os.ModeSocket，可直接判定
		klog.V(6).InfoS("File identified as a Unix domain socket", "filePath", filePath)
		return true, nil
	}
	klog.V(6).InfoS("Function IsUnixDomainSocket starts", "filePath", filePath)
	// Go 在 Windows 上曾长期不支持 os.ModeSocket（https://github.com/golang/go/issues/33357），
	// 因此只能实际发起连接、根据是否报错来判断文件是否为 Unix 域套接字。
	// 按 https://github.com/kubernetes/kubernetes/issues/104584，首次连接可能失败，
	// 所以需要多次重试。
	var lastSocketErr error
	err := wait.PollImmediate(socketDialRetryPeriod, socketDialTimeout,
		func() (bool, error) {
			klog.V(6).InfoS("Dialing the socket", "filePath", filePath)
			var c net.Conn
			c, lastSocketErr = net.Dial("unix", filePath)
			if lastSocketErr == nil {
				c.Close()
				klog.V(6).InfoS("Socket dialed successfully", "filePath", filePath)
				return true, nil
			}
			klog.V(6).InfoS("Failed the current attempt to dial the socket, so pausing before retry",
				"filePath", filePath, "err", lastSocketErr, "socketDialRetryPeriod",
				socketDialRetryPeriod)
			return false, nil
		})

	// 若条件函数始终未返回 true，PollImmediate 会以 "timed out waiting for the condition" 失败。
	if err != nil {
		klog.V(2).InfoS("Failed all attempts to dial the socket so marking it as a non-Unix Domain socket. Last socket error along with the error from PollImmediate follow",
			"filePath", filePath, "lastSocketErr", lastSocketErr, "err", err)
		return false, nil
	}
	return true, nil
}

// MkdirAll 创建目录并设置权限。
// Windows 上 os.MkdirAll 不会真正设置权限，因此创建后还需调用 Chmod 补齐。
func MkdirAll(path string, perm os.FileMode) error {
	klog.V(6).InfoS("Function MkdirAll starts", "path", path, "perm", perm)
	if _, err := os.Stat(path); err == nil {
		// 路径已存在，无需处理。
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error checking path %s: %w", path, err)
	}

	err := os.MkdirAll(path, perm)
	if err != nil {
		return fmt.Errorf("error creating directory %s: %w", path, err)
	}

	err = Chmod(path, perm)
	if err != nil {
		return fmt.Errorf("error setting permissions for directory %s: %w", path, err)
	}

	return nil
}

const (
	// Windows 的 syscall 包里没有定义这些权限位，以下为 SDDL 中使用的访问掩码。

	USER_READ      = 0x100                                       // 所有者读取权限
	USER_WRITE     = 0x80                                        // 所有者写入权限
	USER_EXECUTE   = 0x40                                        // 所有者执行权限
	GROUP_READ     = 0x20                                        // 属组读取权限
	GROUP_WRITE    = 0x10                                        // 属组写入权限
	GROUP_EXECUTE  = 0x8                                         // 属组执行权限
	OTHERS_READ    = 0x4                                         // 其他用户读取权限
	OTHERS_WRITE   = 0x2                                         // 其他用户写入权限
	OTHERS_EXECUTE = 0x1                                         // 其他用户执行权限
	USER_ALL       = USER_READ | USER_WRITE | USER_EXECUTE       // 所有者全部权限
	GROUP_ALL      = GROUP_READ | GROUP_WRITE | GROUP_EXECUTE    // 属组全部权限
	OTHERS_ALL     = OTHERS_READ | OTHERS_WRITE | OTHERS_EXECUTE // 其他用户全部权限
)

// Chmod 设置文件或目录的访问权限。
// Windows 上 os.Chmod 只能设置只读标志，因此这里改用 Windows API 重写 DACL。
// USER 位对应所有者 SID，GROUP 位对应属组 SID，OTHERS 位对应 BUILTIN\Users。
// Windows 容器只能以 ContainerUser 或 ContainerAdministrator 运行：前者继承 BUILTIN\Users
// 的权限，后者继承 BUILTIN\Administrators 的权限。由于容器无法使用宿主机已知的自定义账户，
// OTHERS 位实际上决定了宿主机文件对 ContainerUser 的授权。
func Chmod(path string, filemode os.FileMode) error {
	klog.V(6).InfoS("Function Chmod starts", "path", path, "filemode", filemode)
	// 先获取文件的安全描述符。
	sd, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION|windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION)
	if err != nil {
		return fmt.Errorf("Error getting security descriptor for file %s: %v", path, err)
	}

	// 从安全描述符取出所有者 SID，用于设置 USER 权限。
	owner, _, err := sd.Owner()
	if err != nil {
		return fmt.Errorf("Error getting owner SID for file %s: %v", path, err)
	}
	ownerString := owner.String()

	// 从安全描述符取出属组 SID，用于设置 GROUP 权限。
	group, _, err := sd.Group()
	if err != nil {
		return fmt.Errorf("Error getting group SID for file %s: %v", path, err)
	}
	groupString := group.String()

	mask := uint32(windows.ACCESS_MASK(filemode))

	// 用 SDDL 格式按目标权限构造新的自由访问控制列表（DACL）。
	// https://learn.microsoft.com/windows/win32/secauthz/security-descriptor-definition-language
	// DACL 由若干访问控制条目（ACE）组成，每条 ACE 描述某个 SID 的允许或拒绝权限，格式为：
	//  (AceType;AceFlags;Rights;ObjectGuid;InheritObjectGuid;AccountSid)
	// 这里用不到 ObjectGuid 和 InheritObjectGuid，留空即可。

	dacl := "D:"

	// 构造所有者的 ACE
	dacl += "(A;OICI;"
	if mask&USER_ALL == USER_ALL {
		dacl += "FA"
	} else {
		if mask&USER_READ == USER_READ {
			dacl += "FR"
		}
		if mask&USER_WRITE == USER_WRITE {
			dacl += "FW"
		}
		if mask&USER_EXECUTE == USER_EXECUTE {
			dacl += "FX"
		}
	}
	dacl += ";;;" + ownerString + ")"

	// 构造属组的 ACE
	dacl += "(A;OICI;"
	if mask&GROUP_ALL == GROUP_ALL {
		dacl += "FA"
	} else {
		if mask&GROUP_READ == GROUP_READ {
			dacl += "FR"
		}
		if mask&GROUP_WRITE == GROUP_WRITE {
			dacl += "FW"
		}
		if mask&GROUP_EXECUTE == GROUP_EXECUTE {
			dacl += "FX"
		}
	}
	dacl += ";;;" + groupString + ")"

	// 构造其他用户（BUILTIN\Users）的 ACE
	dacl += "(A;OICI;"
	if mask&OTHERS_ALL == OTHERS_ALL {
		dacl += "FA"
	} else {
		if mask&OTHERS_READ == OTHERS_READ {
			dacl += "FR"
		}
		if mask&OTHERS_WRITE == OTHERS_WRITE {
			dacl += "FW"
		}
		if mask&OTHERS_EXECUTE == OTHERS_EXECUTE {
			dacl += "FX"
		}
	}
	dacl += ";;;BU)"

	klog.V(6).InfoS("Setting new DACL for path", "path", path, "dacl", dacl)

	// 由 DACL 字符串构造新的安全描述符。
	newSD, err := windows.SecurityDescriptorFromString(dacl)
	if err != nil {
		return fmt.Errorf("Error creating new security descriptor from DACL string: %v", err)
	}

	// 从新的安全描述符中取出二进制格式的 DACL。
	newDACL, _, err := newSD.DACL()
	if err != nil {
		return fmt.Errorf("Error getting DACL from new security descriptor: %v", err)
	}

	// 将新的 DACL 写回文件，只更新 DACL，不改动所有者和属组。
	return windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, // 保持所有者 SID 不变
		nil, // 保持属组 SID 不变
		newDACL,
		nil) // 不修改 SACL
}

// IsAbs 判断给定路径是否为绝对路径。
// Windows 上 filepath.IsAbs 不认为以斜杠开头的路径是绝对路径，但这类路径实际可用作绝对路径
// （https://docs.microsoft.com/en-us/dotnet/standard/io/file-path-formats）。
//
// 警告：不要用它校验会跨系统传播的 API 值，例如在 Unix 上校验、持久化后再由 Windows 消费的
// REST API 字段。
func IsAbs(path string) bool {
	return filepath.IsAbs(path) || strings.HasPrefix(path, `\`) || strings.HasPrefix(path, `/`)
}
