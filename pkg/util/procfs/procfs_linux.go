//go:build linux

/*
Copyright 2015 The Kubernetes Authors.

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

package procfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"unicode"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
)

// ProcFS 提供通过 PID 查询容器名称的实现。
type ProcFS struct{}

// containerNameFromProcCgroup 从 /proc/<pid>/cgroup 的内容中提取 devices 子系统
// 的路径，即容器名称。只关注 devices 一行。
func containerNameFromProcCgroup(content string) (string, error) {
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		entries := strings.SplitN(line, ":", 3)
		if len(entries) == 3 && entries[1] == "devices" {
			return strings.TrimSpace(entries[2]), nil
		}
	}
	return "", fmt.Errorf("could not find devices cgroup location")
}

// GetFullContainerName 根据容器的根进程 ID 获取容器名称。
// 例如容器的 devices cgroup 位于 /sys/fs/cgroup/devices/docker/nginx 时返回 docker/nginx。
// 前提是该进程只属于一个 cgroup 层级。
func (pfs *ProcFS) GetFullContainerName(pid int) (string, error) {
	filePath := path.Join("/proc", strconv.Itoa(pid), "cgroup")
	content, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", os.ErrNotExist
		}
		return "", err
	}
	return containerNameFromProcCgroup(string(content))
}

// PKill 用正则表达式查找进程，并向每个匹配的进程发送指定信号。
// 单个进程发送失败不影响其余进程，最终汇总返回所有错误。
func PKill(name string, sig syscall.Signal) error {
	if len(name) == 0 {
		return fmt.Errorf("name should not be empty")
	}
	re, err := regexp.Compile(name)
	if err != nil {
		return err
	}
	pids := getPids(re)
	if len(pids) == 0 {
		return fmt.Errorf("unable to fetch pids for process name : %q", name)
	}
	errList := []error{}
	for _, pid := range pids {
		if err = syscall.Kill(pid, sig); err != nil {
			errList = append(errList, err)
		}
	}
	return utilerrors.NewAggregate(errList)
}

// PidOf 按指定名称（正则匹配可执行文件名的完整路径末尾段）查找进程并返回其 PID。
func PidOf(name string) ([]int, error) {
	if len(name) == 0 {
		return []int{}, fmt.Errorf("name should not be empty")
	}
	re, err := regexp.Compile("(^|/)" + name + "$")
	if err != nil {
		return []int{}, err
	}
	return getPids(re), nil
}

// getPids 遍历 /proc 下所有以数字命名的目录，按可执行文件名匹配 re 并收集 PID。
func getPids(re *regexp.Regexp) []int {
	pids := []int{}

	dirFD, err := os.Open("/proc")
	if err != nil {
		return nil
	}
	defer dirFD.Close()

	for {
		// 分批少量读取，避免 /proc 下条目过多时一次性分配大量内存。
		ls, err := dirFD.Readdir(10)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil
		}

		for _, entry := range ls {
			if !entry.IsDir() {
				continue
			}

			// 非数字目录不是 PID，跳过。
			pid, err := strconv.Atoi(entry.Name())
			if err != nil {
				continue
			}

			cmdline, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
			if err != nil {
				klog.V(4).Infof("Error reading file %s: %+v", filepath.Join("/proc", entry.Name(), "cmdline"), err)
				continue
			}

			// cmdline 中参数以 '\0' 分隔，只需第一段即可拿到可执行文件名。
			parts := bytes.SplitN(cmdline, []byte{0}, 2)
			if len(parts) == 0 {
				continue
			}
			// 按空白和 ':' 切分，兼容容器运行时的 "containerd://..." 形式
			exe := strings.FieldsFunc(string(parts[0]), func(c rune) bool {
				return unicode.IsSpace(c) || c == ':'
			})
			if len(exe) == 0 {
				continue
			}
			// 只匹配可执行文件名；匹配成功后直接用目录名作为 PID。
			if re.MatchString(exe[0]) {
				pids = append(pids, pid)
			}
		}
	}

	return pids
}
