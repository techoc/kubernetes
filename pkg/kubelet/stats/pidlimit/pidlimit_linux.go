//go:build linux

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

package pidlimit

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// Stats 返回系统最大任务数与当前进程数等基本信息。
func Stats() (*statsapi.RlimitStats, error) {
	rlimit := &statsapi.RlimitStats{}

	// -1 表示尚未读取到任何限制。
	taskMax := int64(-1)
	// 取 kernel.pid_max 与 kernel.threads-max 的较小值，
	// 两者都表示系统级的任务数量上限。
	for _, file := range []string{"/proc/sys/kernel/pid_max", "/proc/sys/kernel/threads-max"} {
		if content, err := os.ReadFile(file); err == nil {
			if limit, err := strconv.ParseInt(string(content[:len(content)-1]), 10, 64); err == nil {
				if taskMax == -1 || taskMax > limit {
					taskMax = limit
				}
			}
		}
	}
	// 两次读取都未失败时才设置上限。
	if taskMax >= 0 {
		rlimit.MaxPID = &taskMax
	}

	// 优先读取 /proc/loadavg，因为 sysinfo(2) 在进程数超过 65538 时会截断。
	// 参见 https://github.com/kubernetes/kubernetes/issues/107107
	if procs, err := runningTaskCount(); err == nil {
		rlimit.NumOfRunningProcesses = &procs
	} else {
		var info syscall.Sysinfo_t
		syscall.Sysinfo(&info)
		procs := int64(info.Procs)
		rlimit.NumOfRunningProcesses = &procs
	}

	rlimit.Time = v1.NewTime(time.Now())

	return rlimit, nil
}

// runningTaskCount 从 /proc/loadavg 读取系统任务总数。
func runningTaskCount() (int64, error) {
	// 示例：1.36 3.49 4.53 2/3518 3715089
	bytes, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, err
	}
	// 第 4 个字段形如 "running/total"。
	fields := strings.Fields(string(bytes))
	if len(fields) < 5 {
		return 0, fmt.Errorf("not enough fields in /proc/loadavg")
	}
	subfields := strings.Split(fields[3], "/")
	if len(subfields) != 2 {
		return 0, fmt.Errorf("error parsing fourth field of /proc/loadavg")
	}
	// 取分母，即系统当前任务总数。
	return strconv.ParseInt(subfields[1], 10, 64)
}
