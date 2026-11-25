/*
Copyright 2025 The Kubernetes Authors.

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

package flag

import (
	"github.com/spf13/pflag"
)

// TrackerValue 包装非布尔值，设置时把提供的布尔变量置为 true。
// 用于区分“用户显式设置”与“使用默认值”，例如只有显式给出 --v 时才覆盖配置。
type TrackerValue struct {
	value    pflag.Value
	provided *bool
}

// BoolTrackerValue 包装布尔值，设置时把提供的布尔变量置为 true。
// 布尔标志单独包装，因为 pflag 依赖 IsBoolFlag 决定是否允许省略值。
type BoolTrackerValue struct {
	boolValue
	provided *bool
}

// boolValue 保留 pflag.Value 并额外暴露 IsBoolFlag，以便包装后仍是布尔标志。
type boolValue interface {
	pflag.Value
	IsBoolFlag() bool
}

var _ pflag.Value = &TrackerValue{}
var _ boolValue = &BoolTrackerValue{}

// NewTracker 返回包装给定值的 Value，设置时把提供的布尔变量置为 true。
func NewTracker(value pflag.Value, provided *bool) pflag.Value {
	if value == nil {
		panic("value must not be nil")
	}

	if provided == nil {
		panic("provided boolean must not be nil")
	}

	// 动态判断底层类型：布尔标志需要保留 IsBoolFlag 行为。
	if boolValue, ok := value.(boolValue); ok {
		return &BoolTrackerValue{boolValue: boolValue, provided: provided}
	}
	return &TrackerValue{value: value, provided: provided}
}

// String 委托给被包装的值。
func (f *TrackerValue) String() string {
	return f.value.String()
}

// Set 仅在底层解析成功时把 provided 置真，失败时保持“未显式提供”。
func (f *TrackerValue) Set(value string) error {
	err := f.value.Set(value)
	if err == nil {
		*f.provided = true
	}
	return err
}

// Type 委托给被包装的值。
func (f *TrackerValue) Type() string {
	return f.value.Type()
}

// Set 与 TrackerValue.Set 语义相同，额外保留布尔标志接口。
func (f *BoolTrackerValue) Set(value string) error {
	err := f.boolValue.Set(value)
	if err == nil {
		*f.provided = true
	}

	return err
}
