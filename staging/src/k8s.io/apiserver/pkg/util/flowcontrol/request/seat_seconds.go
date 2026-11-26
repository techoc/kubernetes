/*
Copyright 2021 The Kubernetes Authors.

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

package request

import (
	"fmt"
	"math"
	"time"
)

// SeatSeconds 是以 seat-second 为单位的工作量度量，采用定点数表示。
// `SeatSeconds(n)` 表示 `n/ssScale` 个 seat-second。
// `ssScale` 常量是本实现私有的，
// 其它代码不应使用它。
//
// 使用无符号定点数而非浮点，是为了让 APF 的账务累加在长时间运行后仍然精确、可比较。
type SeatSeconds uint64

// MaxSeatsSeconds 是 SeatSeconds 可表示的最大值。
const MaxSeatSeconds = SeatSeconds(math.MaxUint64)

// MinSeatSeconds 是 SeatSeconds 可表示的最小值。
const MinSeatSeconds = SeatSeconds(0)

// SeatsTimeDuration 根据给定因子产生 SeatSeconds 值。
// 仅用于产生较小的值，即工作量的增量，
// 而非自进程启动以来完成的工作量。
//
// 参数是"并发份额 × 持续时间"：先换算成纳秒再按 ssScale 放大，四舍五入避免长期偏差。
func SeatsTimesDuration(seats float64, duration time.Duration) SeatSeconds {
	return SeatSeconds(int64(math.Round(seats * float64(duration/time.Nanosecond) / (1e9 / ssScale))))
}

// ToFloat 转换为浮点表示。
// 该转换可能损失精度。
func (ss SeatSeconds) ToFloat() float64 {
	return float64(ss) / ssScale
}

// DurationPerSeat 返回每个 seat 对应的时长。
// 该除法可能损失精度。
//
// 用于把累计工作量换算成"每份额可用的时长"，供公平性算法比较。
func (ss SeatSeconds) DurationPerSeat(seats float64) time.Duration {
	return time.Duration(float64(ss) / seats * (float64(time.Second) / ssScale))
}

// String 转换为字符串。
// 对大值和小值都适用。
//
// 手工拆分整数/小数部分，避免浮点格式化在大数值下丢失精度。
func (ss SeatSeconds) String() string {
	const div = SeatSeconds(ssScale)
	quo := ss / div
	rem := ss - quo*div
	return fmt.Sprintf("%d.%08dss", quo, rem)
}

// ssScale 是定点小数位数对应的缩放因子（1e8 即 8 位小数）。
const ssScale = 1e8
