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
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	winio "github.com/Microsoft/go-winio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"golang.org/x/sys/windows"
)

// TestIsUnixDomainSocketPipe 验证 Windows 命名管道不会被误判为 Unix 域套接字。
func TestIsUnixDomainSocketPipe(t *testing.T) {
	generatePipeName := func(suffixLen int) string {
		letter := []rune("abcdef0123456789")
		b := make([]rune, suffixLen)
		for i := range b {
			b[i] = letter[rand.Intn(len(letter))]
		}
		return "\\\\.\\pipe\\test-pipe" + string(b)
	}
	testFile := generatePipeName(4)
	pipeln, err := winio.ListenPipe(testFile, &winio.PipeConfig{SecurityDescriptor: "D:P(A;;GA;;;BA)(A;;GA;;;SY)"})
	defer pipeln.Close()

	require.NoErrorf(t, err, "Failed to listen on named pipe for test purposes: %v", err)
	result, err := IsUnixDomainSocket(testFile)
	assert.NoError(t, err, "Unexpected error from IsUnixDomainSocket.")
	assert.False(t, result, "Unexpected result: true from IsUnixDomainSocket.")
}

// TestPendingUnixDomainSocket 验证 Windows 上套接字文件已存在但尚未就绪时，
// IsUnixDomainSocket 会重试等待其可用。
// 参见 https://github.com/kubernetes/kubernetes/issues/104584
func TestPendingUnixDomainSocket(t *testing.T) {
	// 创建一个临时文件模拟“尚未就绪”的套接字文件。
	// kubelet 会监听文件变化并立即响应，若不重试就会触发上述问题中的竞争。
	f, err := os.CreateTemp("", "test-domain-socket")
	require.NoErrorf(t, err, "Failed to create file for test purposes: %v", err)
	testFile := f.Name()
	f.Close()

	// 在文件替换为真实套接字之前启动检查，触发重试逻辑。
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		result, err := IsUnixDomainSocket(testFile)
		assert.Nil(t, err, "Unexpected error from IsUnixDomainSocket: %v", err)
		assert.True(t, result, "Unexpected result: false from IsUnixDomainSocket.")
		wg.Done()
	}()

	// 等待足够长的时间，确保重试逻辑已经介入。
	time.Sleep(socketDialRetryPeriod)

	// 用真正的 Unix 域套接字文件替换临时文件。
	os.Remove(testFile)
	ta, err := net.ResolveUnixAddr("unix", testFile)
	require.NoError(t, err, "Failed to ResolveUnixAddr.")
	unixln, err := net.ListenUnix("unix", ta)
	require.NoError(t, err, "Failed to ListenUnix.")

	// 等待检查 goroutine 结束，再关闭套接字。
	wg.Wait()
	unixln.Close()
}

// TestWindowsChmod 验证 Chmod 在 Windows 上生成的 SDDL 安全描述符是否符合预期。
func TestWindowsChmod(t *testing.T) {
	// 测试用例中的 OWNER 会在运行后替换为真实的 owner SID。
	testCases := []struct {
		fileMode           os.FileMode
		expectedDescriptor string
	}{
		{
			fileMode:           0777,
			expectedDescriptor: "O:OWNERG:BAD:PAI(A;OICI;FA;;;OWNER)(A;OICI;FA;;;BA)(A;OICI;FA;;;BU)",
		},
		{
			fileMode:           0750,
			expectedDescriptor: "O:OWNERG:BAD:PAI(A;OICI;FA;;;OWNER)(A;OICI;0x1200a9;;;BA)", // 0x1200a9 表示 GENERIC_READ | GENERIC_EXECUTE
		},
		{
			fileMode:           0664,
			expectedDescriptor: "O:OWNERG:BAD:PAI(A;OICI;0x12019f;;;OWNER)(A;OICI;0x12019f;;;BA)(A;OICI;FR;;;BU)", // 0x12019f 表示 GENERIC_READ | GENERIC_WRITE
		},
	}

	for _, testCase := range testCases {
		tempDir, err := os.MkdirTemp("", "test-dir")
		require.NoError(t, err, "Failed to create temporary directory.")
		defer os.RemoveAll(tempDir)

		// 把文件 OWNER 设为当前用户、GROUP 设为 BUILTIN\Administrators（BA），保证结果可预测。
		currentUserSID, err := getCurrentUserSID()
		require.NoError(t, err, "Failed to get current user SID")

		err = setOwnerInfo(tempDir, currentUserSID)
		require.NoError(t, err, "Failed to set current owner SID")

		err = setGroupInfo(tempDir, "S-1-5-32-544")
		require.NoError(t, err, "Failed to set group for directory.")

		err = Chmod(tempDir, testCase.fileMode)
		require.NoError(t, err, "Failed to set permissions for directory.")

		owner, _, descriptor, err := getPermissionsInfo(tempDir)
		require.NoError(t, err, "Failed to get permissions for directory.")

		expectedDescriptor := strings.ReplaceAll(testCase.expectedDescriptor, "OWNER", owner)
		// 当管理员组只有一个账户时（CI 常见情况），SDDL 会用 LA（本地管理员）
		// 代替真实 SID；这里统一替换为实际 SID，保证断言结果稳定。
		descriptor = strings.ReplaceAll(descriptor, "LA", owner)

		assert.Equal(t, expectedDescriptor, descriptor, "Unexpected DACL for directory. when setting permissions to %o", testCase.fileMode)
	}
}

// getCurrentUserSID 返回当前用户的 SID。
func getCurrentUserSID() (string, error) {
	token := windows.GetCurrentProcessToken()
	user, err := token.GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("Error getting user SID: %v", err)
	}

	return user.User.Sid.String(), nil
}

// getPermissionsInfo 以 SDDL 格式返回文件或目录的所有者、组和完整安全描述符。
// https://learn.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-definition-language
func getPermissionsInfo(path string) (string, string, string, error) {
	sd, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION)
	if err != nil {
		return "", "", "", fmt.Errorf("Error getting security descriptor for file %s: %v", path, err)
	}

	owner, _, err := sd.Owner()
	if err != nil {
		return "", "", "", fmt.Errorf("Error getting owner SID for file %s: %v", path, err)
	}
	group, _, err := sd.Group()
	if err != nil {
		return "", "", "", fmt.Errorf("Error getting group SID for file %s: %v", path, err)
	}

	sdString := sd.String()

	return owner.String(), group.String(), sdString, nil
}

// setOwnerInfo 将文件或目录的 OWNER 设置为指定 SID。
func setOwnerInfo(path, owner string) error {
	ownerSID, err := windows.StringToSid(owner)
	if err != nil {
		return fmt.Errorf("Error converting owner SID %s to SID: %v", owner, err)
	}

	err = windows.SetNamedSecurityInfo(
		path, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
		ownerSID, // 所有者 SID
		nil,      // 组 SID
		nil,      // DACL
		nil,      // SACL
	)

	if err != nil {
		return fmt.Errorf("Error setting owner SID for file %s: %v", path, err)
	}
	return nil
}

// setGroupInfo 将文件或目录的 GROUP 设置为指定组。
func setGroupInfo(path, group string) error {
	groupSID, err := windows.StringToSid(group)
	if err != nil {
		return fmt.Errorf("Error converting group name %s to SID: %v", group, err)

	}

	err = windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.GROUP_SECURITY_INFORMATION,
		nil, // 所有者 SID
		groupSID,
		nil, // DACL
		nil, // SACL
	)

	if err != nil {
		return fmt.Errorf("Error setting group SID for file %s: %v", path, err)
	}

	return nil
}

// TestDeleteFilePermissions 验证目录权限为 0660 时无法删除其中的子项，
// 而权限为 0770 时可以删除。
func TestDeleteFilePermissions(t *testing.T) {

	// SSH 会话中的 Windows 连接会获得 SeBackupPrivilege 和 SeRestorePrivilege，
	// 可绕过 ACL 删除文件，使本测试失去意义，因此直接跳过。
	if sshConn := os.Getenv("SSH_CONNECTION"); sshConn != "" {
		t.Skip("Skipping test when running over SSH connection.")
	}
	tempDir, err := os.MkdirTemp("", "test-dir")
	require.NoError(t, err, "Failed to create temporary directory.")

	err = Chmod(tempDir, 0660)
	require.NoError(t, err, "Failed to set permissions for directory to 0660.")

	filePath := filepath.Join(tempDir, "test-file")
	err = os.WriteFile(filePath, []byte("test"), 0440)
	require.NoError(t, err, "Failed to create file in directory.")

	err = os.Remove(filePath)
	require.Error(t, err, "Expected expected error when trying to remove file in directory.")

	err = Chmod(tempDir, 0770)
	require.NoError(t, err, "Failed to set permissions for directory to 0770.")

	err = os.Remove(filePath)
	require.NoError(t, err, "Failed to remove file in directory.")

	err = os.Remove(tempDir)
	require.NoError(t, err, "Failed to remove directory.")
}

// TestAbsWithSlash 验证 IsAbs 能正确识别 Windows 上以斜杠开头的绝对路径。
func TestAbsWithSlash(t *testing.T) {
	// Windows 上 filepath.IsAbs 不会将以斜杠开头的路径判为绝对路径，因此需要包装。
	assert.True(t, IsAbs("/test"))
	assert.True(t, IsAbs("\\test"))

	assert.False(t, IsAbs("./local"))
	assert.False(t, IsAbs("local"))
}
