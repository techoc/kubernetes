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

package duration

import (
	"fmt"
	"time"
)

// ShortHumanDuration 返回时长的简洁人类可读表示，精度有限，适合事件时间等场景。
// 例如 90s 显示为 "1m"，25h 显示为 "1d"，400d 显示为 "1y"。
func ShortHumanDuration(d time.Duration) string {
	// Allow deviation no more than 2 seconds(excluded) to tolerate machine time
	// inconsistence, it can be considered as almost now.
	// 小于 -1 秒视为时钟偏差导致的“未来”时间，直接标记为无效。
	if seconds := int(d.Seconds()); seconds < -1 {
		return "<invalid>"
	} else if seconds < 0 {
		return "0s"
	} else if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	} else if minutes := int(d.Minutes()); minutes < 60 {
		return fmt.Sprintf("%dm", minutes)
	} else if hours := int(d.Hours()); hours < 24 {
		return fmt.Sprintf("%dh", hours)
	} else if hours < 24*365 {
		return fmt.Sprintf("%dd", hours/24)
	}
	return fmt.Sprintf("%dy", int(d.Hours()/24/365))
}

// HumanDuration 返回时长的简洁人类可读表示，约 2-3 位有效数字，
// 在相邻量级之间保留更细的组合（如 "1m30s"、"2h10m"、"3d4h"）。
func HumanDuration(d time.Duration) string {
	// Allow deviation no more than 2 seconds(excluded) to tolerate machine time
	// inconsistence, it can be considered as almost now.
	if seconds := int(d.Seconds()); seconds < -1 {
		return "<invalid>"
	} else if seconds < 0 {
		return "0s"
	} else if seconds < 60*2 {
		return fmt.Sprintf("%ds", seconds)
	}
	// 以下按分钟、小时、天、年逐级切换，并在接近边界时输出两级组合以保留有效数字。
	minutes := int(d / time.Minute)
	if minutes < 10 {
		s := int(d/time.Second) % 60
		if s == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm%ds", minutes, s)
	} else if minutes < 60*3 {
		return fmt.Sprintf("%dm", minutes)
	}
	hours := int(d / time.Hour)
	if hours < 8 {
		m := int(d/time.Minute) % 60
		if m == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		return fmt.Sprintf("%dh%dm", hours, m)
	} else if hours < 48 {
		return fmt.Sprintf("%dh", hours)
	} else if hours < 24*8 {
		h := hours % 24
		if h == 0 {
			return fmt.Sprintf("%dd", hours/24)
		}
		return fmt.Sprintf("%dd%dh", hours/24, h)
	} else if hours < 24*365*2 {
		return fmt.Sprintf("%dd", hours/24)
	} else if hours < 24*365*8 {
		dy := int(hours/24) % 365
		if dy == 0 {
			return fmt.Sprintf("%dy", hours/24/365)
		}
		return fmt.Sprintf("%dy%dd", hours/24/365, dy)
	}
	return fmt.Sprintf("%dy", int(hours/24/365))
}
