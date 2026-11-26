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

// Package helpers 提供云提供商卷插件共用的容量换算与可用区选择工具。
//
// 容量按云厂商的分配单位向上取整，可用区按卷名稳定散列选择；
// 两者都要求可重复计算，以保证同一 PVC 的重试得到相同结果。
package helpers

import (
	"fmt"
	"math"
	"math/bits"

	"k8s.io/apimachinery/pkg/api/resource"
)

/*
云提供商的卷插件会为对应的 PersistentVolumeClaim 创建磁盘，而各家使用的分配单位
不同：AWS 允许以整数 GiB 指定大小，Portworx 则要求以字节表示。例如在 AWS 上申请
1500MiB 的卷，实际调用 AWS API 时应当申请 2GiB 的磁盘。
本文件包含按云提供商分配单位对存储申请量做舍入的函数。
*/

// 下面这组 RoundUpTo* 均返回“需要多少个分配单位”。必须向上取整——
// 少算单位会让实际磁盘小于 PVC 申请值，Pod 写满后即失败；各变体只是分配单位
// 与返回类型不同（int64 / int / int32），语义完全一致。

const (
	// GB 表示十进制千兆字节。
	GB = 1000 * 1000 * 1000
	// GiB 表示二进制千兆字节。
	GiB = 1024 * 1024 * 1024

	// MB 表示十进制兆字节。
	MB = 1000 * 1000
	// MiB 表示二进制兆字节。
	MiB = 1024 * 1024

	// KB 表示十进制千字节。
	KB = 1000
	// KiB 表示二进制千字节。
	KiB = 1024
)

// RoundUpToGiB 按 GiB 向上取整。
func RoundUpToGiB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, GiB)
}

// RoundUpToMB 按 MB 向上取整。
func RoundUpToMB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, MB)
}

// RoundUpToMiB 按 MiB 向上取整。
func RoundUpToMiB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, MiB)
}

// RoundUpToKB 按 KB 向上取整。
func RoundUpToKB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, KB)
}

// RoundUpToKiB 按 KiB 向上取整。
func RoundUpToKiB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, KiB)
}

// RoundUpToB 按字节向上取整（即原值）。
func RoundUpToB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, 1)
}

// RoundUpToGiBInt 按 GiB 向上取整，返回 int；溢出时返回错误。
func RoundUpToGiBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, GiB)
}

// RoundUpToMBInt 按 MB 向上取整，返回 int；溢出时返回错误。
func RoundUpToMBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, MB)
}

// RoundUpToMiBInt 按 MiB 向上取整，返回 int；溢出时返回错误。
func RoundUpToMiBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, MiB)
}

// RoundUpToKBInt 按 KB 向上取整，返回 int；溢出时返回错误。
func RoundUpToKBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, KB)
}

// RoundUpToKiBInt 按 KiB 向上取整，返回 int；溢出时返回错误。
func RoundUpToKiBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, KiB)
}

// RoundUpToGiBInt32 按 GiB 向上取整，返回 int32；溢出时返回错误。
func RoundUpToGiBInt32(size resource.Quantity) (int32, error) {
	return roundUpSizeInt32(size, GiB)
}

// roundUpSizeInt 计算容纳给定容量所需的分配单元数，返回 int；溢出时返回错误。
func roundUpSizeInt(size resource.Quantity, allocationUnitBytes int64) (int, error) {
	if bits.UintSize == 32 {
		res, err := roundUpSizeInt32(size, allocationUnitBytes)
		return int(res), err
	}
	res, err := roundUpSizeInt64(size, allocationUnitBytes)
	return int(res), err
}

// roundUpSizeInt32 计算容纳指定大小的卷需要多少个分配单位，
// 返回 int32，溢出时返回错误。
//
// 先按 int64 计算再检查上限，避免在 32 位平台上发生截断。
func roundUpSizeInt32(size resource.Quantity, allocationUnitBytes int64) (int32, error) {
	roundedUpInt32, err := roundUpSizeInt64(size, allocationUnitBytes)
	if err != nil {
		return 0, err
	}
	if roundedUpInt32 > math.MaxInt32 {
		return 0, fmt.Errorf("quantity %s is too great, overflows int32", size.String())
	}
	return int32(roundedUpInt32), nil
}

// roundUpSizeInt64 计算容纳给定容量所需的分配单元数，返回 int64；溢出时返回错误。
func roundUpSizeInt64(size resource.Quantity, allocationUnitBytes int64) (int64, error) {
	// 先用 CmpInt64() 判断 size 是否超出 int64，否则 Value() 的结果不可信；
	// 确认后再按 int64 取值并舍入。
	// 不用 AsScale() 等函数是因为它们不支持 BinarySI 格式；也不能直接用 AsInt64()，
	// 它只实现了 int64 缩放数（int64Amount）。

	// CmpInt64() 在被比较的量大于 MaxInt64 时实际上返回 0，因此 >= 0 同时覆盖
	// “等于 MaxInt64”与“溢出”两种情况；此时 Value()
	// 的结果不可信，必须报错而不是返回截断后的单位数。
	if size.CmpInt64(math.MaxInt64) >= 0 {
		return 0, fmt.Errorf("quantity %s is too great, overflows int64", size.String())
	}
	volumeSizeBytes := size.Value()

	// 整除后只要有余数就进位，即向上取整。
	roundedUp := volumeSizeBytes / allocationUnitBytes
	if volumeSizeBytes%allocationUnitBytes > 0 {
		roundedUp++
	}
	return roundedUp, nil
}
