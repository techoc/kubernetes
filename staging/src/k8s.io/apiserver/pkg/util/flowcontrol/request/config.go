/*
Copyright 2022 The Kubernetes Authors.

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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

const (
	// objectsPerSeat：多少个对象折算一个座位。数值越大，同样大小的 list
	// 占用的座位越少，默认值直接决定 list 请求的限流松紧。
	objectsPerSeat = 100.0
	// watchesPerSeat：多少个 watch 折算一个座位，用于估算对象变更的扇出成本。
	watchesPerSeat              = 10.0
	enableMutatingWorkEstimator = true
)

// eventAdditionalDuration 是每个变更事件的基础成本估计；
// 与 watchers/座位系数共同决定变更请求的工作量。
var eventAdditionalDuration = 5 * time.Millisecond

// WorkEstimatorConfig 配置 APF 的工作量估算参数。
// 指针字段允许 list/mutating 两侧独立配置为 nil（关闭该估算）。
type WorkEstimatorConfig struct {
	*ListWorkEstimatorConfig     `json:"listWorkEstimatorConfig,omitempty"`
	*MutatingWorkEstimatorConfig `json:"mutatingWorkEstimatorConfig,omitempty"`

	// 请求至少占用的 seat 数。
	MinimumSeats uint64 `json:"minimumSeats,omitempty"`

	// 修改下面的最大值时必须同步调整 work_estimate_seats_samples 的桶上界，
	// 否则指标会截断观测值。

	// list 请求可占用 seat 数的上限。
	MaximumListSeatsLimit uint64 `json:"maximumListSeatsLimit,omitempty"`
	// MaximumMutatingSeatsLimit 是变更（mutating）请求可占用的最大座位数上限。
	// （字段名沿用历史拼写 MaximumListSeatsLimit，实际语义是变更请求上限。）
	MaximumMutatingSeatsLimit uint64 `json:"maximumMutatingSeatsLimit,omitempty"`
}

// ListWorkEstimatorConfig 保存与 list 请求相关的工作量估算参数。
type ListWorkEstimatorConfig struct {
	ObjectsPerSeat float64 `json:"objectsPerSeat,omitempty"`
}

// MutatingWorkEstimatorConfig 保存监听对象变更相关的工作量估算参数。
// 与监听对象变更相关的参数。
type MutatingWorkEstimatorConfig struct {
	// TODO(wojtekt): 待算法调优到不会让可扩展性测试失败后移除该开关。
	Enabled                 bool            `json:"enable,omitempty"`
	EventAdditionalDuration metav1.Duration `json:"eventAdditionalDurationMs,omitempty"`
	WatchesPerSeat          float64         `json:"watchesPerSeat,omitempty"`
}

// DefaultWorkEstimatorConfig 创建一个使用默认值的 WorkEstimatorConfig。
// list 座位上限受 SizeBasedListCostEstimate feature gate 控制：
// 关闭时为 10（保守），开启后按对象大小估算、上限放宽到 100。
func DefaultWorkEstimatorConfig() *WorkEstimatorConfig {
	var maximumListSeatsLimit uint64 = 10
	if utilfeature.DefaultFeatureGate.Enabled(features.SizeBasedListCostEstimate) {
		maximumListSeatsLimit = 100
	}

	return &WorkEstimatorConfig{
		MinimumSeats:                1,
		MaximumListSeatsLimit:       maximumListSeatsLimit,
		MaximumMutatingSeatsLimit:   10,
		ListWorkEstimatorConfig:     defaultListWorkEstimatorConfig(),
		MutatingWorkEstimatorConfig: defaultMutatingWorkEstimatorConfig(),
	}
}

// 用默认值构造 ListWorkEstimatorConfig。
func defaultListWorkEstimatorConfig() *ListWorkEstimatorConfig {
	return &ListWorkEstimatorConfig{ObjectsPerSeat: objectsPerSeat}
}

// 用默认值构造 MutatingWorkEstimatorConfig。
func defaultMutatingWorkEstimatorConfig() *MutatingWorkEstimatorConfig {
	return &MutatingWorkEstimatorConfig{
		Enabled:                 enableMutatingWorkEstimator,
		EventAdditionalDuration: metav1.Duration{Duration: eventAdditionalDuration},
		WatchesPerSeat:          watchesPerSeat,
	}
}

// eventAdditionalDuration 将 eventAdditionalDurationMs 转换为 time.Duration 类型。
// 注意：JSON 字段名为 ...Ms，但底层是 metav1.Duration，
// 反序列化按 duration 字符串解析；这里仅做取值封装。
func (c *MutatingWorkEstimatorConfig) eventAdditionalDuration() time.Duration {
	return c.EventAdditionalDuration.Duration
}
