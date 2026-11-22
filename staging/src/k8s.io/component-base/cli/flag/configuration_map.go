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

// Package flag 提供 Kubernetes 组件共用的 pflag.Value 实现。
// 这些类型负责把命令行上的自定义字符串语法解析成 map、slice、
// 证书键值对等结构，并保证解析失败时给出可定位的错误。
package flag

import (
	"fmt"
	"sort"
	"strings"
)

// ConfigurationMap 实现 pflag.Value，把 "k1=v1,k2=v2" 形式的参数
// 解析为 map[string]string。组件常用它承载启动时的键值配置。
type ConfigurationMap map[string]string

// String 以 "k=v" 形式序列化，并按 key 排序。
// 排序是为了让帮助输出、日志和测试断言保持稳定，避免 map 遍历顺序造成的抖动。
func (m *ConfigurationMap) String() string {
	pairs := []string{}
	for k, v := range *m {
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// Set 解析逗号分隔的键值对。缺少 "=" 的项按空字符串值处理而非报错，
// 这样开关式配置可以简写为 "--config=feature"。
func (m *ConfigurationMap) Set(value string) error {
	for _, s := range strings.Split(value, ",") {
		// 忽略空项，容忍末尾或连续的逗号。
		if len(s) == 0 {
			continue
		}
		// SplitN 限制为 2 段，使值中允许出现 "="。
		arr := strings.SplitN(s, "=", 2)
		if len(arr) == 2 {
			(*m)[strings.TrimSpace(arr[0])] = strings.TrimSpace(arr[1])
		} else {
			(*m)[strings.TrimSpace(arr[0])] = ""
		}
	}
	return nil
}

// Type 返回该 flag 在帮助信息中展示的类型名。
func (*ConfigurationMap) Type() string {
	return "mapStringString"
}
