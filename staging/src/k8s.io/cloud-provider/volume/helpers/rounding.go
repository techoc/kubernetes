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
	// GB - GigaByte size
	GB = 1000 * 1000 * 1000
	// GiB - GibiByte size
	GiB = 1024 * 1024 * 1024

	// MB - MegaByte size
	MB = 1000 * 1000
	// MiB - MebiByte size
	MiB = 1024 * 1024

	// KB - KiloByte size
	KB = 1000
	// KiB - KibiByte size
	KiB = 1024
)

// RoundUpToGiB rounds up given quantity upto chunks of GiB
func RoundUpToGiB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, GiB)
}

// RoundUpToMB rounds up given quantity to chunks of MB
func RoundUpToMB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, MB)
}

// RoundUpToMiB rounds up given quantity upto chunks of MiB
func RoundUpToMiB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, MiB)
}

// RoundUpToKB rounds up given quantity to chunks of KB
func RoundUpToKB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, KB)
}

// RoundUpToKiB rounds up given quantity to chunks of KiB
func RoundUpToKiB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, KiB)
}

// RoundUpToB rounds up given quantity to chunks of bytes
func RoundUpToB(size resource.Quantity) (int64, error) {
	return roundUpSizeInt64(size, 1)
}

// RoundUpToGiBInt rounds up given quantity upto chunks of GiB. It returns an
// int instead of an int64 and an error if there's overflow
func RoundUpToGiBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, GiB)
}

// RoundUpToMBInt rounds up given quantity to chunks of MB. It returns an
// int instead of an int64 and an error if there's overflow
func RoundUpToMBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, MB)
}

// RoundUpToMiBInt rounds up given quantity upto chunks of MiB. It returns an
// int instead of an int64 and an error if there's overflow
func RoundUpToMiBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, MiB)
}

// RoundUpToKBInt rounds up given quantity to chunks of KB. It returns an
// int instead of an int64 and an error if there's overflow
func RoundUpToKBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, KB)
}

// RoundUpToKiBInt rounds up given quantity upto chunks of KiB. It returns an
// int instead of an int64 and an error if there's overflow
func RoundUpToKiBInt(size resource.Quantity) (int, error) {
	return roundUpSizeInt(size, KiB)
}

// RoundUpToGiBInt32 rounds up given quantity up to chunks of GiB. It returns an
// int32 instead of an int64 and an error if there's overflow
func RoundUpToGiBInt32(size resource.Quantity) (int32, error) {
	return roundUpSizeInt32(size, GiB)
}

// roundUpSizeInt calculates how many allocation units are needed to accommodate
// a volume of a given size. It returns an int and an error if there's overflow
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

// roundUpSizeInt64 calculates how many allocation units are needed to accommodate
// a volume of a given size. It returns an int64 and an error if there's overflow
func roundUpSizeInt64(size resource.Quantity, allocationUnitBytes int64) (int64, error) {
	// Use CmpInt64() to find out if the value of "size" would overflow an
	// int64 and therefore have Value() return a wrong result. Then, retrieve
	// the value as int64 and perform the rounding.
	// It's not convenient to use AsScale() and related functions as they don't
	// support BinarySI format, nor can we use AsInt64() directly since it's
	// only implemented for int64 scaled numbers (int64Amount).

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
