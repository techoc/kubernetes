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

package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// metricsObserver 表示一次待观测的指标写入动作。
type metricsObserver func()

// TestNoUtils 验证在没有 "utils"（组合变量等辅助特性）参与时，
// ObserveAdmission / ObserveRejection 产生的直方图与计数器标签是否符合预期。
func TestNoUtils(t *testing.T) {

	// 仅断言这两个指标，避免其他全局指标干扰比较结果。
	metrics := []string{
		"apiserver_validating_admission_policy_check_total",
		"apiserver_validating_admission_policy_check_duration_seconds",
	}

	testCases := []struct {
		desc     string
		want     string
		observer metricsObserver
	}{
		{
			// 校验允许路径：enforcement_action 应为 allow。
			desc: "observe policy admission",
			want: `
			# HELP apiserver_validating_admission_policy_check_duration_seconds [BETA] Validation admission latency for individual validation expressions in seconds, labeled by policy and further including binding and enforcement action taken.
            # TYPE apiserver_validating_admission_policy_check_duration_seconds histogram
			apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="allow",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.0000005"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="allow",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.001"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="allow",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.01"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="allow",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.1"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="allow",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="1"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="allow",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="+Inf"} 1
            apiserver_validating_admission_policy_check_duration_seconds_sum{enforcement_action="allow",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 10
            apiserver_validating_admission_policy_check_duration_seconds_count{enforcement_action="allow",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 1
            # HELP apiserver_validating_admission_policy_check_total [BETA] Validation admission policy check total, labeled by policy and further identified by binding and enforcement action taken.
            # TYPE apiserver_validating_admission_policy_check_total counter
            apiserver_validating_admission_policy_check_total{enforcement_action="allow",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 1
			`,
			observer: func() {
				Metrics.ObserveAdmission(context.TODO(), time.Duration(10)*time.Second, "policy.example.com", "binding.example.com", ValidatingInvalidError)
			},
		},
		{
			// 校验拒绝路径：同样的 error_type，但 enforcement_action 为 deny。
			desc: "observe policy rejection",
			want: `
			# HELP apiserver_validating_admission_policy_check_duration_seconds [BETA] Validation admission latency for individual validation expressions in seconds, labeled by policy and further including binding and enforcement action taken.
            # TYPE apiserver_validating_admission_policy_check_duration_seconds histogram
			apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="deny",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.0000005"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="deny",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.001"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="deny",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.01"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="deny",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="0.1"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="deny",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="1"} 0
            apiserver_validating_admission_policy_check_duration_seconds_bucket{enforcement_action="deny",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com",le="+Inf"} 1
            apiserver_validating_admission_policy_check_duration_seconds_sum{enforcement_action="deny",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 10
            apiserver_validating_admission_policy_check_duration_seconds_count{enforcement_action="deny",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 1
            # HELP apiserver_validating_admission_policy_check_total [BETA] Validation admission policy check total, labeled by policy and further identified by binding and enforcement action taken.
            # TYPE apiserver_validating_admission_policy_check_total counter
            apiserver_validating_admission_policy_check_total{enforcement_action="deny",error_type="invalid_error",policy="policy.example.com",policy_binding="binding.example.com"} 1
			`,
			observer: func() {
				Metrics.ObserveRejection(context.TODO(), time.Duration(10)*time.Second, "policy.example.com", "binding.example.com", ValidatingInvalidError)
			},
		},
	}

	// 指标为包级全局状态，测试开始前清空，避免同进程其他测试残留数据。
	Metrics.Reset()

	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			// 每个子测试结束后清理，保证用例之间互不影响。
			defer Metrics.Reset()
			tt.observer()
			// 按期望文本逐行比对，任何标签或桶值差异都会失败。
			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tt.want), metrics...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
