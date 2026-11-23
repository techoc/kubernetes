/*
Copyright 2025 The Kubernetes Authors.

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

package kubeletplugin

import (
	"context"
	"errors"
	"net"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2/ktesting"
)

// TestEndpointLifecycle 验证端点的完整生命周期：监听成功时套接字文件出现，关闭后文件被清理。
// 输入为临时目录中的默认端点；断言监听/关闭无错误，且文件存在性符合预期。
func TestEndpointLifecycle(t *testing.T) {
	// 创建带测试日志的上下文。
	_, ctx := ktesting.NewTestContext(t)
	// 用临时目录作为套接字所在目录。
	tempDir := t.TempDir()
	socketname := "test.sock"
	// 构造使用默认 Unix 域套接字监听的端点。
	e := endpoint{dir: tempDir, file: socketname}
	// 执行监听。
	listener, err := e.listen(ctx)
	// 断言监听成功。
	require.NoError(t, err, "listen")
	// 断言套接字文件已创建。
	assert.FileExists(t, path.Join(tempDir, socketname))
	// 关闭监听器。
	require.NoError(t, listener.Close(), "close")
	// 断言关闭后套接字文件被删除。
	assert.NoFileExists(t, path.Join(tempDir, socketname))
}

// TestEndpointListener 验证自定义 listenFunc 完全接管监听：传入的 socketpath 正确，
// 且返回值被原样使用，不会创建真实的套接字文件。
func TestEndpointListener(t *testing.T) {
	// 创建带测试日志的上下文。
	_, ctx := ktesting.NewTestContext(t)
	// 临时目录用于构造预期路径。
	tempDir := t.TempDir()
	socketname := "test.sock"
	// 自定义监听函数：断言路径正确，并返回 (nil, nil) 以检验返回值是否原样透传。
	listen := func(ctx2 context.Context, socketpath string) (net.Listener, error) {
		// 断言传入路径是 dir 与 file 的拼接。
		assert.Equal(t, path.Join(tempDir, socketname), socketpath)
		return nil, nil
	}
	// 构造带自定义监听函数的端点。
	e := endpoint{dir: tempDir, file: socketname, listenFunc: listen}
	// 走自定义监听分支。
	listener, err := e.listen(ctx)
	// 断言无错误。
	require.NoError(t, err, "listen")
	// 断言没有创建真实套接字文件。
	assert.NoFileExists(t, path.Join(tempDir, socketname))
	// 断言返回的监听器就是自定义函数提供的 nil。
	assert.Nil(t, listener)
}

// closeErrorListener is a net.Listener whose Close returns a fixed error. Only
// Close is called here, so the embedded Listener is left nil.
// closeErrorListener 是一个 Close 固定返回指定错误的 net.Listener。
type closeErrorListener struct {
	// 内嵌监听器，测试中不使用，保持 nil。
	// closeErr 是 Close 要返回的错误。
	net.Listener
	closeErr error
}

// Close 返回预设的错误。
func (l closeErrorListener) Close() error { return l.closeErr }

// unremovableSocket puts something at the socket path that os.Remove refuses to
// delete. A directory that is not empty fails without depending on file
// permissions or on which user runs the test.
// unremovableSocket 在套接字路径上放置一个 os.Remove 无法删除的东西（非空目录）。
// 使用非空目录可避免依赖文件权限或运行测试的用户身份。
func unremovableSocket(t *testing.T, dir, file string) {
	// 标记为测试辅助函数。
	t.Helper()
	// 在套接字路径上创建一个目录。
	require.NoError(t, os.Mkdir(path.Join(dir, file), 0700))
	// 在目录中放一个文件使其成为非空目录，从而无法被 os.Remove 删除。
	require.NoError(t, os.WriteFile(path.Join(dir, file, "occupied"), nil, 0600))
}

// TestEndpointCloseReportsFailedSocketRemoval 验证关闭时若套接字删除失败，错误必须被报告出来。
// 输入：路径上放置非空目录，而监听器自身 Close 不报错；
// 断言返回错误且错误信息包含 "remove Unix domain socket"。
func TestEndpointCloseReportsFailedSocketRemoval(t *testing.T) {
	// 临时目录。
	tempDir := t.TempDir()
	socketname := "test.sock"
	// 制造无法删除的套接字路径。
	unremovableSocket(t, tempDir, socketname)
	// 构造包装监听器，内嵌的 Close 不返回错误。
	listener := &unixListener{
		Listener: closeErrorListener{},
		endpoint: endpoint{dir: tempDir, file: socketname},
	}

	// 执行关闭。
	err := listener.Close()

	// 断言残留套接字导致的失败被报告。
	require.Error(t, err, "closing must report the socket that was left behind")
	// 断言错误上下文指向套接字删除。
	assert.Contains(t, err.Error(), "remove Unix domain socket")
}

// TestEndpointCloseKeepsBothErrors 验证监听器 Close 与套接字删除同时失败时，两个错误都被保留。
// 输入：两类错误同时存在；断言返回错误既匹配监听器错误，也包含套接字删除错误。
func TestEndpointCloseKeepsBothErrors(t *testing.T) {
	// 临时目录。
	tempDir := t.TempDir()
	socketname := "test.sock"
	// 制造套接字删除失败。
	unremovableSocket(t, tempDir, socketname)
	// 构造监听器自身的关闭错误。
	closeErr := errors.New("close failed")
	// 构造同时携带两类错误的监听器。
	listener := &unixListener{
		Listener: closeErrorListener{closeErr: closeErr},
		endpoint: endpoint{dir: tempDir, file: socketname},
	}

	// 执行关闭。
	err := listener.Close()

	// 断言有错误返回。
	require.Error(t, err)
	// 断言保留了监听器自身的错误。
	require.ErrorIs(t, err, closeErr, "the listener's own error")
	// 断言保留了套接字删除错误。
	assert.Contains(t, err.Error(), "remove Unix domain socket", "the removal error")
}
