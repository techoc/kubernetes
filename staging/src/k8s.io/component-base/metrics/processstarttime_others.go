//go:build !windows

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

package metrics

import (
	"os"

	"github.com/prometheus/procfs"
)

// GetProcessStart 返回当前进程的启动时间（Unix 秒）。
// 非 Windows 平台通过 procfs 读取 /proc/<pid>/stat；该值用于让抓取端推算进程运行时长。
// Windows 无 /proc，由 processstarttime_windows.go 提供基于 filetime 的实现。
func GetProcessStart() (float64, error) {
	pid := os.Getpid()
	// 只关心自身进程，因此无需遍历 /proc。
	p, err := procfs.NewProc(pid)
	if err != nil {
		return 0, err
	}

	// Stat 在进程已退出或权限不足时可能失败；此时返回错误值而非 0 成功值。
	if stat, err := p.Stat(); err == nil {
		return stat.StartTime()
	}
	return 0, err
}
