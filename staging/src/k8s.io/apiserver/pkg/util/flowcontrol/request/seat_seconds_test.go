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

// Package request 估算请求成本（座位数、时长、对象数量），为 API 优先级与
// 公平性（APF）的排队与限流提供度量。
package request

import (
	"math"
	"testing"
	"time"
)

// TestSeatSecondsString exercises the SeatSeconds constructor and de-constructors (String, ToFloat).
//
// 中文说明：SeatSeconds 用 1e-8 秒的定点数表示工作量，测试同时覆盖
// String 的固定 8 位小数格式与 ToFloat 的精度往返。
func TestSeatSecondsString(t *testing.T) {
	testCases := []struct {
		ss          SeatSeconds
		expectFloat float64
		expectStr   string
	}{
		{ss: SeatSeconds(1), expectFloat: 1.0 / ssScale, expectStr: "0.00000001ss"},
		{ss: SeatSeconds(ssScale - 1), expectFloat: (ssScale - 1) / ssScale, expectStr: "0.99999999ss"},
		{ss: 0, expectFloat: 0, expectStr: "0.00000000ss"},
		{ss: SeatsTimesDuration(1, time.Second), expectFloat: 1, expectStr: "1.00000000ss"},
		{ss: SeatsTimesDuration(123, 100*time.Millisecond), expectFloat: 12.3, expectStr: "12.30000000ss"},
		{ss: SeatsTimesDuration(1203, 10*time.Millisecond), expectFloat: 12.03, expectStr: "12.03000000ss"},
	}
	for _, testCase := range testCases {
		actualStr := testCase.ss.String()
		if actualStr != testCase.expectStr {
			t.Errorf("SeatSeconds(%d).String() is %q but expected %q", uint64(testCase.ss), actualStr, testCase.expectStr)
		}
		// 按同一比例缩放后再比较，避免浮点尾差导致的误报。
		actualFloat := testCase.ss.ToFloat()
		if math.Round(actualFloat*ssScale) != math.Round(testCase.expectFloat*ssScale) {
			t.Errorf("SeatSeconds(%d).ToFloat() is %v but expected %v", uint64(testCase.ss), actualFloat, testCase.expectFloat)
		}
	}
}

// TestSeatSecondsPerSeat 验证按座位数折算的时长；座位数为 0 时
// 工作量已无意义，期望得到 0 时长而不是除零 panic。
func TestSeatSecondsPerSeat(t *testing.T) {
	testCases := []struct {
		ss     SeatSeconds
		seats  float64
		expect time.Duration
	}{
		{ss: SeatsTimesDuration(10, time.Second), seats: 1, expect: 10 * time.Second},
		{ss: SeatsTimesDuration(1, time.Second), seats: 10, expect: 100 * time.Millisecond},
		{ss: SeatsTimesDuration(13, 5*time.Millisecond), seats: 5, expect: 13 * time.Millisecond},
		{ss: SeatsTimesDuration(12, 0), seats: 10, expect: 0},
	}
	for _, testCase := range testCases {
		actualDuration := testCase.ss.DurationPerSeat(testCase.seats)
		if actualDuration != testCase.expect {
			t.Errorf("DurationPerSeats returned %v rather than expected %q", actualDuration, testCase.expect)
		}
	}
}
