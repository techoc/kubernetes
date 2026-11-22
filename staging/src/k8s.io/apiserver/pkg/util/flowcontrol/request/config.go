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

// WorkEstimatorConfig holds work estimator parameters.
// WorkEstimatorConfig 配置 APF 的工作量估算参数。
// 指针字段允许 list/mutating 两侧独立配置为 nil（关闭该估算）。
type WorkEstimatorConfig struct {
	*ListWorkEstimatorConfig     `json:"listWorkEstimatorConfig,omitempty"`
	*MutatingWorkEstimatorConfig `json:"mutatingWorkEstimatorConfig,omitempty"`

	// MinimumSeats is the minimum number of seats a request must occupy.
	MinimumSeats uint64 `json:"minimumSeats,omitempty"`

	// NOTE: work_estimate_seats_samples metric uses the value of maximumSeats
	// as the upper bound, so when we change maximum seats values below we should also
	// update the buckets of the metric.
	// 修改下面的最大值时必须同步调整 work_estimate_seats_samples 的桶上界，
	// 否则指标会截断观测值。

	// MaximumListSeatsLimit is an upper limit on the max seats a list request can occupy.
	MaximumListSeatsLimit uint64 `json:"maximumListSeatsLimit,omitempty"`
	// MaximumListSeatsLimit is an upper limit on the max seats a mutating request can occupy.
	// （字段名沿用历史拼写 MaximumListSeatsLimit，实际语义是变更请求上限。）
	MaximumMutatingSeatsLimit uint64 `json:"maximumMutatingSeatsLimit,omitempty"`
}

// ListWorkEstimatorConfig holds work estimator parameters related to list requests.
type ListWorkEstimatorConfig struct {
	ObjectsPerSeat float64 `json:"objectsPerSeat,omitempty"`
}

// MutatingWorkEstimatorConfig holds work estimator
// parameters related to watches of mutating objects.
type MutatingWorkEstimatorConfig struct {
	// TODO(wojtekt): Remove it once we tune the algorithm to not fail
	// scalability tests.
	Enabled                 bool            `json:"enable,omitempty"`
	EventAdditionalDuration metav1.Duration `json:"eventAdditionalDurationMs,omitempty"`
	WatchesPerSeat          float64         `json:"watchesPerSeat,omitempty"`
}

// DefaultWorkEstimatorConfig creates a new WorkEstimatorConfig with default values.
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

// defaultListWorkEstimatorConfig creates a new ListWorkEstimatorConfig with default values.
func defaultListWorkEstimatorConfig() *ListWorkEstimatorConfig {
	return &ListWorkEstimatorConfig{ObjectsPerSeat: objectsPerSeat}
}

// defaultMutatingWorkEstimatorConfig creates a new MutatingWorkEstimatorConfig with default values.
func defaultMutatingWorkEstimatorConfig() *MutatingWorkEstimatorConfig {
	return &MutatingWorkEstimatorConfig{
		Enabled:                 enableMutatingWorkEstimator,
		EventAdditionalDuration: metav1.Duration{Duration: eventAdditionalDuration},
		WatchesPerSeat:          watchesPerSeat,
	}
}

// eventAdditionalDuration converts eventAdditionalDurationMs to a time.Duration type.
// 注意：JSON 字段名为 ...Ms，但底层是 metav1.Duration，
// 反序列化按 duration 字符串解析；这里仅做取值封装。
func (c *MutatingWorkEstimatorConfig) eventAdditionalDuration() time.Duration {
	return c.EventAdditionalDuration.Duration
}
