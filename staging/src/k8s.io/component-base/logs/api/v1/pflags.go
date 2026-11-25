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

package v1

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/pflag"
)

// VModuleConfigurationPflag 为 VModuleConfiguration 实现 pflag.Value 接口，
// 值指针不能为 nil。
func VModuleConfigurationPflag(value *VModuleConfiguration) pflag.Value {
	return vmoduleConfigurationPFlag{value}
}

// vmoduleConfigurationPFlag 把 -vmodule 的 "pattern=N,..." 语法适配到 pflag。
type vmoduleConfigurationPFlag struct {
	value *VModuleConfiguration
}

// String 返回 -vmodule 参数（以逗号分隔的 pattern=N 列表）。
func (wrapper vmoduleConfigurationPFlag) String() string {
	if wrapper.value == nil {
		return ""
	}
	var patterns []string
	for _, item := range *wrapper.value {
		patterns = append(patterns, fmt.Sprintf("%s=%d", item.FilePattern, item.Verbosity))
	}
	return strings.Join(patterns, ",")
}

// Set 解析 -vmodule 参数（以逗号分隔的 pattern=N 列表）。
func (wrapper vmoduleConfigurationPFlag) Set(value string) error {
	// 此代码对应 https://github.com/kubernetes/klog/blob/9ad246211af1ed84621ee94a26fcce0038b69cd1/klog.go#L287-L313

	for _, pat := range strings.Split(value, ",") {
		if len(pat) == 0 {
			// 尾随逗号产生的空字符串等可直接忽略。
			continue
		}
		patLev := strings.Split(pat, "=")
		if len(patLev) != 2 || len(patLev[0]) == 0 || len(patLev[1]) == 0 {
			return fmt.Errorf("%q does not have the pattern=N format", pat)
		}
		pattern := patLev[0]
		// 使用 31 而非 32，以确保也能容纳在 int32 中。
		// 与 klog 保持一致，避免超出 int32 范围导致下游截断。
		v, err := strconv.ParseUint(patLev[1], 10, 31)
		if err != nil {
			return fmt.Errorf("parsing verbosity in %q: %v", pat, err)
		}
		*wrapper.value = append(*wrapper.value, VModuleItem{FilePattern: pattern, Verbosity: VerbosityLevel(v)})
	}
	return nil
}

// Type 返回 pflag 展示用的类型名。
func (wrapper vmoduleConfigurationPFlag) Type() string {
	return "pattern=N,..."
}

// VerbosityLevelPflag 为详细程度等级值实现 pflag.Value 接口。
func VerbosityLevelPflag(value *VerbosityLevel) pflag.Value {
	return verbosityLevelPflag{value}
}

// verbosityLevelPflag 把单个 verbosity 等级适配到 pflag。
type verbosityLevelPflag struct {
	value *VerbosityLevel
}

// String 输出十进制等级；nil 视为 0，避免 flag 帮助信息 panic。
func (wrapper verbosityLevelPflag) String() string {
	if wrapper.value == nil {
		return "0"
	}
	return strconv.FormatInt(int64(*wrapper.value), 10)
}

// Get 实现 pflag.Getter，供需要反射读取标志值的调用方使用。
func (wrapper verbosityLevelPflag) Get() interface{} {
	if wrapper.value == nil {
		return VerbosityLevel(0)
	}
	return *wrapper.value
}

// Set 解析十进制等级；限制为 31 位以与 klog 的 int32 语义兼容。
func (wrapper verbosityLevelPflag) Set(value string) error {
	// 为兼容 klog，限制在 int32 范围内。
	v, err := strconv.ParseUint(value, 10, 31)
	if err != nil {
		return err
	}
	*wrapper.value = VerbosityLevel(v)
	return nil
}

// Type 返回 pflag 展示用的类型名。
func (wrapper verbosityLevelPflag) Type() string {
	return "Level"
}
