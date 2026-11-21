//go:build freebsd

/*
Copyright 2020 The Kubernetes Authors.

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
	"time"

	"unsafe"

	"golang.org/x/sys/unix"

	"k8s.io/klog/v2"
)

// GetBootTime 返回机器启动时间，并截断到秒；通过 CLOCK_UPTIME 计算得出。
func GetBootTime(klog.Logger) (time.Time, error) {
	currentTime := time.Now()
	ts := &unix.Timeval{}
	_, _, e1 := unix.Syscall(uintptr(unix.SYS_CLOCK_GETTIME), uintptr(unix.CLOCK_UPTIME), uintptr(unsafe.Pointer(ts)), 0)
	if e1 != 0 {
		return time.Time{}, fmt.Errorf("error getting system uptime")
	}

	return currentTime.Add(-time.Duration(ts.Sec) * time.Second).Truncate(time.Second), nil
}
