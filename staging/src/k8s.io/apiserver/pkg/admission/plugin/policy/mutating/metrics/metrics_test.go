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

package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// metricsObserver 是测试用例要触发的指标记录动作。
type metricsObserver func()

// TestNoUtils 验证 mutating admission policy 的 check 计数与耗时指标，
// 以及 ObserveAdmission / ObserveRejection 两条路径产出的指标完全一致。
func TestNoUtils(t *testing.T) {

	// 只比对这两个指标，避免其他全局指标的噪声干扰。
	metrics := []string{
		"apiserver_mutating_admission_policy_check_total",
		"apiserver_mutating_admission_policy_check_duration_seconds",
	}

	testCases := []struct {
		desc     string
		want     string
		observer metricsObserver
	}{
		{
			desc: "observe policy admission",
			want: `
			# HELP apiserver_mutating_admission_policy_check_duration_seconds [ALPHA] Mutation admission latency for individual mutation expressions in seconds, labeled by policy and binding.
            # TYPE apiserver_mutating_admission_policy_check_duration_seconds histogram
			apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.0000005"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.001"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.01"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.1"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="1"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="+Inf"} 1
            apiserver_mutating_admission_policy_check_duration_seconds_sum{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 10
            apiserver_mutating_admission_policy_check_duration_seconds_count{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 1
            # HELP apiserver_mutating_admission_policy_check_total [ALPHA] Mutation admission policy check total, labeled by policy and further identified by binding.
            # TYPE apiserver_mutating_admission_policy_check_total counter
            apiserver_mutating_admission_policy_check_total{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 1
			`,
			// 10 秒耗时落在 +Inf 桶内，因此除 +Inf 外各桶计数均为 0。
			observer: func() {
				Metrics.ObserveAdmission(context.TODO(), time.Duration(10)*time.Second, "policy.example.com", "binding.example.com", MutatingInvalidError)
			},
		},
		{
			desc: "observe policy rejection",
			want: `
			# HELP apiserver_mutating_admission_policy_check_duration_seconds [ALPHA] Mutation admission latency for individual mutation expressions in seconds, labeled by policy and binding.
            # TYPE apiserver_mutating_admission_policy_check_duration_seconds histogram
			apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.0000005"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.001"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.01"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.1"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="1"} 0
            apiserver_mutating_admission_policy_check_duration_seconds_bucket{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="+Inf"} 1
            apiserver_mutating_admission_policy_check_duration_seconds_sum{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 10
            apiserver_mutating_admission_policy_check_duration_seconds_count{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 1
            # HELP apiserver_mutating_admission_policy_check_total [ALPHA] Mutation admission policy check total, labeled by policy and further identified by binding.
            # TYPE apiserver_mutating_admission_policy_check_total counter
            apiserver_mutating_admission_policy_check_total{error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 1
			`,
			// 拒绝路径期望产出与 admit 路径相同的样本，证明两者共享同一组指标。
			observer: func() {
				Metrics.ObserveRejection(context.TODO(), time.Duration(10)*time.Second, "policy.example.com", "binding.example.com", MutatingInvalidError)
			},
		},
	}

	// 先清空全局指标，避免其他测试残留影响基线。
	Metrics.Reset()

	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			defer Metrics.Reset()
			tt.observer()
			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tt.want), metrics...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
