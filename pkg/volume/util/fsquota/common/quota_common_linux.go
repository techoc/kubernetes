//go:build linux

/*
Copyright 2018 The Kubernetes Authors.

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

package common

import (
	"regexp"
)

// QuotaType 表示要应用的配额类型。
type QuotaType int

const (
	// FSQuotaAccounting 表示仅用于统计的配额。
	FSQuotaAccounting QuotaType = 1 << iota
	// FSQuotaEnforcing 表示强制执行的配额。
	FSQuotaEnforcing QuotaType = 1 << iota
)

// FirstQuota 是我们起始的配额 ID。
// XXXXXXX 需要更好的做法...
var FirstQuota QuotaID = 1048577

// MountsFile 是系统挂载数据的位置。
var MountsFile = "/proc/self/mounts"

// MountParseRegexp 用于解析 /proc/sys/self/mounts。
var MountParseRegexp = regexp.MustCompilePOSIX("^([^ ]*)[ \t]*([^ ]*)[ \t]*([^ ]*)") // 忽略选项等字段

// LinuxVolumeQuotaProvider 在设备支持配额时返回合适的配额执行器。
type LinuxVolumeQuotaProvider interface {
	// GetQuotaApplier 返回可应用配额的对象；
	// 若该 provider 无法在此设备上支持配额则返回 nil。
	GetQuotaApplier(mountpoint string, backingDev string) LinuxVolumeQuotaApplier
}

// LinuxVolumeQuotaApplier 是 Linux 支持的任意配额机制的通用接口。
type LinuxVolumeQuotaApplier interface {
	// GetQuotaOnDir 获取作用于该目录的配额 ID（如果有）。
	GetQuotaOnDir(path string) (QuotaID, error)

	// SetQuotaOnDir 将指定的配额 ID 应用到目录。
	// bytes 为负数时表示应用非强制配额（例如设一个大到用不上的上限）。
	SetQuotaOnDir(path string, id QuotaID, bytes int64) error

	// QuotaIDIsInUse 判断配额 ID 是否已被使用。
	// 实现不应检查 /etc/project 或 /etc/projid，只检查底层机制是否已占用该 ID。
	// 返回 false 且无错误表示 ID 未被使用；true 表示已在使用。
	// 返回错误则表示任何配额 ID 都会失败。
	QuotaIDIsInUse(id QuotaID) (bool, error)

	// GetConsumption 返回目录的用量（字节），由实现的配额机制确定。
	// 若无法通过该机制获取，应返回错误，让上层自行遍历目录。
	GetConsumption(path string, id QuotaID) (int64, error)

	// GetInodes 返回目录占用的 inode 数，由实现的配额机制确定。
	// 若无法通过该机制获取，应返回错误，让上层自行遍历目录。
	GetInodes(path string, id QuotaID) (int64, error)
}
