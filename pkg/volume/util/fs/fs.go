//go:build linux || darwin

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
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	servermetrics "k8s.io/kubernetes/pkg/kubelet/server/metrics"
	"k8s.io/kubernetes/pkg/volume/util/fsquota"
)

// UsageInfo 记录目录消耗的字节数和 inode 数。
type UsageInfo struct {
	Bytes  int64
	Inodes int64
}

// Info 返回 path 所在文件系统的可用字节数、总容量、已用字节数、
// inode 总数、空闲 inode 数、已用 inode 数及错误。
func Info(path string) (int64, int64, int64, int64, int64, int64, error) {
	statfs := &unix.Statfs_t{}
	err := unix.Statfs(path, statfs)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	// 可用空间按可用块数乘以块大小计算。
	available := int64(statfs.Bavail) * int64(statfs.Bsize)

	// 总容量按总块数乘以块大小计算。
	capacity := int64(statfs.Blocks) * int64(statfs.Bsize)

	// 已用空间按已用块数乘以块大小计算。
	usage := (int64(statfs.Blocks) - int64(statfs.Bfree)) * int64(statfs.Bsize)

	inodes := int64(statfs.Files)
	inodesFree := int64(statfs.Ffree)
	inodesUsed := inodes - inodesFree

	return available, capacity, usage, inodes, inodesFree, inodesUsed, nil
}

// DiskUsage 统计指定目录的 inode 数和磁盘用量。
func DiskUsage(path string) (UsageInfo, error) {
	var usage UsageInfo

	if path == "" {
		return usage, fmt.Errorf("invalid directory")
	}

	// 先查询配额系统是否管理该目录。返回 nil 或错误表示不支持配额或
	// 缺少 xfs_quota 工具，此时改用遍历目录的方式统计。
	startTime := time.Now()
	consumption, _ := fsquota.GetConsumption(path)
	if consumption != nil {
		usage.Bytes = consumption.Value()
		defer servermetrics.CollectVolumeStatCalDuration("fsquota", startTime)
	} else {
		defer servermetrics.CollectVolumeStatCalDuration("du", startTime)
	}

	inodes, _ := fsquota.GetInodes(path)
	if inodes != nil {
		usage.Inodes = inodes.Value()
	}

	if inodes != nil && consumption != nil {
		return usage, nil
	}

	topLevelStat := &unix.Stat_t{}
	err := unix.Stat(path, topLevelStat)
	if err != nil {
		return usage, err
	}

	// 记录已统计过的硬链接 inode，避免重复计数。
	dedupedInodes := make(map[uint64]struct{})

	err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		// 忽略读取目录后已被删除的文件。
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("unable to count inodes for %s: %s", path, err)
		}

		// 按文档 Sys 可能为 nil。
		if info.Sys() == nil {
			return fmt.Errorf("fileinfo Sys is nil")
		}

		s, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("unsupported fileinfo; could not convert to stat_t")
		}

		if s.Dev != topLevelStat.Dev {
			// 不跨越设备边界统计。
			return filepath.SkipDir
		}

		// 硬链接去重。
		if s.Nlink > 1 {
			if _, ok := dedupedInodes[s.Ino]; !ok {
				dedupedInodes[s.Ino] = struct{}{}
			} else {
				return nil
			}
		}

		if consumption == nil {
			usage.Bytes += int64(s.Blocks) * int64(512) // stat 中块大小固定为 512 字节
		}

		if inodes == nil {
			usage.Inodes++
		}

		return nil
	})

	return usage, err
}
