//go:build !linux && !darwin && !windows

/*
Copyright 2014 The Kubernetes Authors.

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

package fs

import (
	"fmt"
)

// UsageInfo 描述路径占用的字节数与 inode 数。
type UsageInfo struct {
	Bytes  int64
	Inodes int64
}

// Info 在该构建平台上不受支持：所有统计值返回 0，并附带错误。
func Info(path string) (int64, int64, int64, int64, int64, int64, error) {
	return 0, 0, 0, 0, 0, 0, fmt.Errorf("fsinfo not supported for this build")
}

// DiskUsage 获取指定路径的磁盘用量；该构建平台不支持，返回错误。
func DiskUsage(path string) (UsageInfo, error) {
	var usage UsageInfo
	return usage, fmt.Errorf("directory disk usage not supported for this build.")
}
