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

package util

import (
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

// npipeProtocol 是 Windows 命名管道的 URL 协议名。
const npipeProtocol = "npipe"

// LocalEndpoint 返回指定端点下命名管道的完整路径；与 unix 不同，Windows 不能使用 socket。
func LocalEndpoint(path, file string) (string, error) {
	// 从路径中提取 podresources 配置名。只有 Windows 需要这段额外逻辑，因为其管道布局不同；
	// 为避免修改函数签名，逻辑放在这里。先 Join file，确保最后一个路径分量是文件，后续操作才能正确执行。
	podResourcesDir := filepath.Base(filepath.Dir(filepath.Join(path, file)))
	if podResourcesDir == "" {
		// 正常情况下不会发生：用户可以配置根目录，且我们期望其中存在类似 "pod-resources" 的子目录。
		return "", fmt.Errorf("cannot infer the podresources directory from path %q", path)
	}
	// Windows 管道使用正斜杠，参见 https://learn.microsoft.com/windows/win32/ipc/pipe-names。
	// 因此在 unix 上使用 url 拼接在这里并无明显收益，参见 https://github.com/kubernetes/kubernetes/issues/78628，
	// 直接从头构造路径即可。
	// 格式：\\ServerName\pipe\PipeName
	// ServerName 可以是远程计算机名，或用句点表示本机。
	// 只有 PipeName 视为普通 Windows 路径，管道路径的其余部分固定，故使用常量。
	serverPart := `\\.`
	pipePart := "pipe"
	pipeName := "kubelet-" + podResourcesDir
	return npipeProtocol + "://" + filepath.Join(serverPart, pipePart, pipeName), nil
}

// tickCount 是 kernel32 的 GetTickCount64，返回系统启动以来的毫秒数。
var tickCount = syscall.NewLazyDLL("kernel32.dll").NewProc("GetTickCount64")

// GetBootTime 返回机器启动时间并截断到秒，基于 GetTickCount64 反推。
func GetBootTime(klog.Logger) (time.Time, error) {
	currentTime := time.Now()
	output, _, err := tickCount.Call()
	if errno, ok := err.(syscall.Errno); !ok || errno != 0 {
		return time.Time{}, err
	}
	return currentTime.Add(-time.Duration(output) * time.Millisecond).Truncate(time.Second), nil
}

// NormalizePath 把某些 Go 框架（如 fsnotify）返回的路径转换为
// 可传给 Windows 专有代码的原生路径。
func NormalizePath(path string) string {
	path = strings.ReplaceAll(path, "/", "\\")
	if strings.HasPrefix(path, "\\") {
		path = "c:" + path
	}
	return path
}

// IsCgroup2UnifiedMode 在 Windows 上暂为空操作，始终返回 false。
func IsCgroup2UnifiedMode() bool {
	return false
}
