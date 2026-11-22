/*
Copyright 2015 The Kubernetes Authors.

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

package net

import (
	"fmt"
	"strconv"
	"strings"
)

// PortRange 表示一段 TCP/UDP 端口区间；表示单个端口时 Size 为 1。
// Base 是起始端口，Size 是端口个数（而非结束端口），因此结束端口为 Base+Size-1。
type PortRange struct {
	Base int
	Size int
}

// Contains 判断给定端口是否落在区间内。
// 使用 (p-Base) < Size 而非 p <= Base+Size-1，可避免 Base+Size 溢出。
func (pr *PortRange) Contains(p int) bool {
	return (p >= pr.Base) && ((p - pr.Base) < pr.Size)
}

// String 把 PortRange 转为 "min-max" 形式，可被 PortRange.Set 或 ParsePortRange 解析回去。
// Size 为 0 时返回空串。
func (pr PortRange) String() string {
	if pr.Size == 0 {
		return ""
	}
	return fmt.Sprintf("%d-%d", pr.Base, pr.Base+pr.Size-1)
}

// Set 解析 "value"、"min-max" 或 "min+offset" 形式（两端闭区间），并据此设置 PortRange。
// 这是 flag.Value 与 pflag.Value 接口的一部分。
func (pr *PortRange) Set(value string) error {
	// 用位标志记录出现了哪种记法，便于在 switch 中集中报错。
	const (
		SinglePortNotation = 1 << iota
		HyphenNotation
		PlusNotation
	)

	value = strings.TrimSpace(value)
	hyphenIndex := strings.Index(value, "-")
	plusIndex := strings.Index(value, "+")

	// 空串表示清空区间，而不是解析失败——命令行中允许显式取消设置。
	if value == "" {
		pr.Base = 0
		pr.Size = 0
		return nil
	}

	var err error
	var low, high int
	var notation int

	if plusIndex == -1 && hyphenIndex == -1 {
		notation |= SinglePortNotation
	}
	if hyphenIndex != -1 {
		notation |= HyphenNotation
	}
	if plusIndex != -1 {
		notation |= PlusNotation
	}

	switch notation {
	case SinglePortNotation:
		var port int
		port, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
		low = port
		high = port
	case HyphenNotation:
		low, err = strconv.Atoi(value[:hyphenIndex])
		if err != nil {
			return err
		}
		high, err = strconv.Atoi(value[hyphenIndex+1:])
		if err != nil {
			return err
		}
	case PlusNotation:
		// "base+offset" 中的 offset 是相对偏移量，而非结束端口。
		var offset int
		low, err = strconv.Atoi(value[:plusIndex])
		if err != nil {
			return err
		}
		offset, err = strconv.Atoi(value[plusIndex+1:])
		if err != nil {
			return err
		}
		high = low + offset
	default:
		// 同时出现 '-' 和 '+' 等组合视为非法。
		return fmt.Errorf("unable to parse port range: %s", value)
	}

	if low > 65535 || high > 65535 {
		return fmt.Errorf("the port range cannot be greater than 65535: %s", value)
	}

	if high < low {
		return fmt.Errorf("end port cannot be less than start port: %s", value)
	}

	pr.Base = low
	pr.Size = 1 + high - low
	return nil
}

// Type 返回该类型的描述字符串，供 pflag 在帮助信息中展示。
func (*PortRange) Type() string {
	return "portRange"
}

// ParsePortRange 解析 "min-max" 形式（两端闭区间）并构造新的 PortRange。
func ParsePortRange(value string) (*PortRange, error) {
	pr := &PortRange{}
	err := pr.Set(value)
	if err != nil {
		return nil, err
	}
	return pr, nil
}

// ParsePortRangeOrDie 与 ParsePortRange 相同，但解析失败时 panic，
// 仅适用于启动阶段由代码常量构造区间的场景。
func ParsePortRangeOrDie(value string) *PortRange {
	pr, err := ParsePortRange(value)
	if err != nil {
		panic(fmt.Sprintf("couldn't parse port range %q: %v", value, err))
	}
	return pr
}
