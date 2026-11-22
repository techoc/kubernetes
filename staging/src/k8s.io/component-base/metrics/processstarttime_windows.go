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

package metrics

import (
	"golang.org/x/sys/windows"
)

// GetProcessStart 返回当前进程的启动时间（Unix 秒）。
// Windows 通过 GetProcessTimes 读取创建时间，该时间以 100ns 为单位。
func GetProcessStart() (float64, error) {
	processHandle := windows.CurrentProcess()

	var creationTime, exitTime, kernelTime, userTime windows.Filetime
	if err := windows.GetProcessTimes(processHandle, &creationTime, &exitTime, &kernelTime, &userTime); err != nil {
		return 0, err
	}
	// Filetime.Nanoseconds() 返回自 1601 年起的纳秒数；换算成秒与 Unix 时间一致。
	return float64(creationTime.Nanoseconds() / 1e9), nil
}
