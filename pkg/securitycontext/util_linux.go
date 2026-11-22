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

package securitycontext

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// possibleCPUs 返回主机上所有可能的 CPU 编号列表。
// 用于确定哪些 CPU 的 thermal_throttle 路径需要 mask。
// 优先从 /sys/devices/system/cpu/possible 解析（支持 CPU 热插拔），
// 回退到 runtime.NumCPU()（仅返回在线 CPU）。
func possibleCPUs() (cpus []int) {
	if ncpu := possibleCPUsParsed(); ncpu != nil {
		return ncpu
	}

	for i := range runtime.NumCPU() {
		cpus = append(cpus, i)
	}

	return cpus
}

// possibleCPUsParsed 从 /sys/devices/system/cpu/possible 解析 CPU 编号。
// 该文件格式为 "0-7,16-23" 这样的范围表示，表示系统可能支持的 CPU（包括离线 CPU）。
// 使用 sync.OnceValue 确保只解析一次，因为 CPU 拓扑在运行时不会频繁变化。
// 如果解析失败（如文件不存在或格式错误），返回 nil，调用方会回退到 runtime.NumCPU()。
var possibleCPUsParsed = sync.OnceValue(func() (cpus []int) {
	data, err := os.ReadFile("/sys/devices/system/cpu/possible")
	if err != nil {
		return nil
	}

	ranges := strings.SplitSeq(strings.TrimSpace(string(data)), ",")

	for r := range ranges {
		if rStart, rEnd, ok := strings.Cut(r, "-"); !ok {
			cpu, err := strconv.Atoi(rStart)
			if err != nil {
				return nil
			}
			cpus = append(cpus, cpu)
		} else {
			var start, end int
			start, err := strconv.Atoi(rStart)
			if err != nil {
				return nil
			}
			end, err = strconv.Atoi(rEnd)
			if err != nil {
				return nil
			}
			for i := start; i <= end; i++ {
				cpus = append(cpus, i)
			}
		}
	}

	return cpus
})
