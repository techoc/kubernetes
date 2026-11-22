/*
Copyright The Kubernetes Authors.

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

// 本文件用 testutil.GatherAndCompare 逐字比对指标输出，
// 覆盖冒充（impersonation）请求的模式与决策两个维度，以及授权检查的独立指标。
// 之所以做全量文本比对，是为了同时锁定指标名、标签、分桶边界等对外契约。
package metrics

import (
	"strings"
	"testing"
	"time"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// TestRecordImpersonationAttempt 验证冒充请求计数与耗时直方图的模式/决策标签组合。
// mode 为空串代表请求未指定冒充模式（通常在校验阶段即被拒绝）。
func TestRecordImpersonationAttempt(t *testing.T) {
	RegisterMetrics()

	attemptMetrics := []string{
		namespace + "_" + subsystem + "_attempts_total",
		namespace + "_" + subsystem + "_attempts_duration_seconds",
	}

	testCases := []struct {
		name          string
		mode          string
		decision      string
		expectedValue string
	}{
		{
			name:     "allowed with user-info mode",
			mode:     "user-info",
			decision: "allowed",
			expectedValue: `
				# HELP apiserver_impersonation_attempts_total [ALPHA] Total number of impersonation attempts split by mode and decision.
				# TYPE apiserver_impersonation_attempts_total counter
				apiserver_impersonation_attempts_total{decision="allowed",mode="user-info"} 1
				# HELP apiserver_impersonation_attempts_duration_seconds [ALPHA] Latency of impersonation attempts in seconds split by mode and decision.
				# TYPE apiserver_impersonation_attempts_duration_seconds histogram
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.001"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.002"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.004"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.008"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.016"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.032"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.064"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.128"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.256"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.512"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="1.024"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="2.048"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="4.096"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="8.192"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="16.384"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="+Inf"} 1
				apiserver_impersonation_attempts_duration_seconds_sum{decision="allowed",mode="user-info"} 0.1
				apiserver_impersonation_attempts_duration_seconds_count{decision="allowed",mode="user-info"} 1
			`,
		},
		{
			name:     "denied attempt",
			mode:     "",
			decision: "denied",
			expectedValue: `
				# HELP apiserver_impersonation_attempts_total [ALPHA] Total number of impersonation attempts split by mode and decision.
				# TYPE apiserver_impersonation_attempts_total counter
				apiserver_impersonation_attempts_total{decision="denied",mode=""} 1
				# HELP apiserver_impersonation_attempts_duration_seconds [ALPHA] Latency of impersonation attempts in seconds split by mode and decision.
				# TYPE apiserver_impersonation_attempts_duration_seconds histogram
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.001"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.002"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.004"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.008"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.016"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.032"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.064"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.128"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.256"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.512"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="1.024"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="2.048"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="4.096"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="8.192"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="16.384"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="+Inf"} 1
				apiserver_impersonation_attempts_duration_seconds_sum{decision="denied",mode=""} 0.1
				apiserver_impersonation_attempts_duration_seconds_count{decision="denied",mode=""} 1
			`,
		},
		{
			name:     "allowed with legacy mode",
			mode:     "legacy",
			decision: "allowed",
			expectedValue: `
				# HELP apiserver_impersonation_attempts_total [ALPHA] Total number of impersonation attempts split by mode and decision.
				# TYPE apiserver_impersonation_attempts_total counter
				apiserver_impersonation_attempts_total{decision="allowed",mode="legacy"} 1
				# HELP apiserver_impersonation_attempts_duration_seconds [ALPHA] Latency of impersonation attempts in seconds split by mode and decision.
				# TYPE apiserver_impersonation_attempts_duration_seconds histogram
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.001"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.002"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.004"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.008"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.016"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.032"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.064"} 0
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.128"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.256"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.512"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="1.024"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="2.048"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="4.096"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="8.192"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="16.384"} 1
				apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="+Inf"} 1
				apiserver_impersonation_attempts_duration_seconds_sum{decision="allowed",mode="legacy"} 0.1
				apiserver_impersonation_attempts_duration_seconds_count{decision="allowed",mode="legacy"} 1
			`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子用例前清空指标，避免前一个用例的样本污染文本比对。
			resetMetricsForTest()

			RecordImpersonationAttempt(tc.mode, tc.decision, 100*time.Millisecond)

			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tc.expectedValue), attemptMetrics...); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestRecordImpersonationAuthorizationCall 验证冒充处理器内部授权检查的指标，
// 与请求级指标分开，便于区分“请求被拒”与“授权子系统故障/拒绝”。
func TestRecordImpersonationAuthorizationCall(t *testing.T) {
	RegisterMetrics()

	authorizationMetrics := []string{
		namespace + "_" + subsystem + "_authorization_attempts_total",
		namespace + "_" + subsystem + "_authorization_attempts_duration_seconds",
	}

	testCases := []struct {
		name          string
		mode          string
		decision      string
		expectedValue string
	}{
		{
			name:     "user-info allowed",
			mode:     "user-info",
			decision: "allowed",
			expectedValue: `
				# HELP apiserver_impersonation_authorization_attempts_total [ALPHA] Total number of authorization checks made by the impersonation handler split by mode and decision.
				# TYPE apiserver_impersonation_authorization_attempts_total counter
				apiserver_impersonation_authorization_attempts_total{decision="allowed",mode="user-info"} 1
				# HELP apiserver_impersonation_authorization_attempts_duration_seconds [ALPHA] Latency of authorization checks made by the impersonation handler in seconds split by mode and decision.
				# TYPE apiserver_impersonation_authorization_attempts_duration_seconds histogram
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.001"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.002"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.004"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.008"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.016"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.032"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.064"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.128"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.256"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.512"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="1.024"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="2.048"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="4.096"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="8.192"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="16.384"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="+Inf"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="allowed",mode="user-info"} 0.1
				apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="allowed",mode="user-info"} 1
			`,
		},
		{
			name:     "arbitrary-node denied",
			mode:     "arbitrary-node",
			decision: "denied",
			expectedValue: `
				# HELP apiserver_impersonation_authorization_attempts_total [ALPHA] Total number of authorization checks made by the impersonation handler split by mode and decision.
				# TYPE apiserver_impersonation_authorization_attempts_total counter
				apiserver_impersonation_authorization_attempts_total{decision="denied",mode="arbitrary-node"} 1
				# HELP apiserver_impersonation_authorization_attempts_duration_seconds [ALPHA] Latency of authorization checks made by the impersonation handler in seconds split by mode and decision.
				# TYPE apiserver_impersonation_authorization_attempts_duration_seconds histogram
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.001"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.002"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.004"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.008"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.016"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.032"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.064"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.128"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.256"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="0.512"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="1.024"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="2.048"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="4.096"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="8.192"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="16.384"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="arbitrary-node",le="+Inf"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="arbitrary-node"} 0.1
				apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="arbitrary-node"} 1
			`,
		},
		{
			name:     "legacy allowed",
			mode:     "legacy",
			decision: "allowed",
			expectedValue: `
				# HELP apiserver_impersonation_authorization_attempts_total [ALPHA] Total number of authorization checks made by the impersonation handler split by mode and decision.
				# TYPE apiserver_impersonation_authorization_attempts_total counter
				apiserver_impersonation_authorization_attempts_total{decision="allowed",mode="legacy"} 1
				# HELP apiserver_impersonation_authorization_attempts_duration_seconds [ALPHA] Latency of authorization checks made by the impersonation handler in seconds split by mode and decision.
				# TYPE apiserver_impersonation_authorization_attempts_duration_seconds histogram
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.001"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.002"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.004"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.008"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.016"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.032"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.064"} 0
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.128"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.256"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="0.512"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="1.024"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="2.048"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="4.096"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="8.192"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="16.384"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="legacy",le="+Inf"} 1
				apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="allowed",mode="legacy"} 0.1
				apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="allowed",mode="legacy"} 1
			`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetMetricsForTest()

			RecordImpersonationAuthorizationCall(tc.mode, tc.decision, 100*time.Millisecond)

			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tc.expectedValue), authorizationMetrics...); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestRecordImpersonationMetricsMultiple 验证多次记录时同一标签组合的累加行为：
// 计数递增、直方图分桶累加、不同模式/决策互不干扰。
func TestRecordImpersonationMetricsMultiple(t *testing.T) {
	RegisterMetrics()
	resetMetricsForTest()

	// 两次相同的 denied 样本用于验证同标签累加；随后混合不同模式与决策，
	// 确认各标签组合独立维护自己的计数与直方图。
	RecordImpersonationAttempt("user-info", "allowed", 100*time.Millisecond)
	RecordImpersonationAttempt("", "denied", 50*time.Millisecond)
	RecordImpersonationAttempt("", "denied", 50*time.Millisecond)
	RecordImpersonationAuthorizationCall("user-info", "allowed", 100*time.Millisecond)
	RecordImpersonationAuthorizationCall("user-info", "allowed", 100*time.Millisecond)
	RecordImpersonationAuthorizationCall("user-info", "denied", 50*time.Millisecond)
	RecordImpersonationAuthorizationCall("legacy", "denied", 50*time.Millisecond)

	expectedValue := `
		# HELP apiserver_impersonation_attempts_duration_seconds [ALPHA] Latency of impersonation attempts in seconds split by mode and decision.
		# TYPE apiserver_impersonation_attempts_duration_seconds histogram
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.001"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.002"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.004"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.008"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.016"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.032"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.064"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.128"} 1
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.256"} 1
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.512"} 1
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="1.024"} 1
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="2.048"} 1
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="4.096"} 1
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="8.192"} 1
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="16.384"} 1
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="+Inf"} 1
		apiserver_impersonation_attempts_duration_seconds_sum{decision="allowed",mode="user-info"} 0.1
		apiserver_impersonation_attempts_duration_seconds_count{decision="allowed",mode="user-info"} 1
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.001"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.002"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.004"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.008"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.016"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.032"} 0
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.064"} 2
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.128"} 2
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.256"} 2
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="0.512"} 2
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="1.024"} 2
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="2.048"} 2
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="4.096"} 2
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="8.192"} 2
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="16.384"} 2
		apiserver_impersonation_attempts_duration_seconds_bucket{decision="denied",mode="",le="+Inf"} 2
		apiserver_impersonation_attempts_duration_seconds_sum{decision="denied",mode=""} 0.1
		apiserver_impersonation_attempts_duration_seconds_count{decision="denied",mode=""} 2
		# HELP apiserver_impersonation_attempts_total [ALPHA] Total number of impersonation attempts split by mode and decision.
		# TYPE apiserver_impersonation_attempts_total counter
		apiserver_impersonation_attempts_total{decision="allowed",mode="user-info"} 1
		apiserver_impersonation_attempts_total{decision="denied",mode=""} 2
		# HELP apiserver_impersonation_authorization_attempts_duration_seconds [ALPHA] Latency of authorization checks made by the impersonation handler in seconds split by mode and decision.
		# TYPE apiserver_impersonation_authorization_attempts_duration_seconds histogram
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.001"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.002"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.004"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.008"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.016"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.032"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.064"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.128"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.256"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="0.512"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="1.024"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="2.048"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="4.096"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="8.192"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="16.384"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="allowed",mode="user-info",le="+Inf"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="allowed",mode="user-info"} 0.2
		apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="allowed",mode="user-info"} 2
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.001"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.002"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.004"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.008"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.016"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.032"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.064"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.128"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.256"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="0.512"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="1.024"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="2.048"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="4.096"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="8.192"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="16.384"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="legacy",le="+Inf"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="legacy"} 0.05
		apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="legacy"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.001"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.002"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.004"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.008"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.016"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.032"} 0
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.064"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.128"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.256"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="0.512"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="1.024"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="2.048"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="4.096"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="8.192"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="16.384"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_bucket{decision="denied",mode="user-info",le="+Inf"} 1
		apiserver_impersonation_authorization_attempts_duration_seconds_sum{decision="denied",mode="user-info"} 0.05
		apiserver_impersonation_authorization_attempts_duration_seconds_count{decision="denied",mode="user-info"} 1
		# HELP apiserver_impersonation_authorization_attempts_total [ALPHA] Total number of authorization checks made by the impersonation handler split by mode and decision.
		# TYPE apiserver_impersonation_authorization_attempts_total counter
		apiserver_impersonation_authorization_attempts_total{decision="allowed",mode="user-info"} 2
		apiserver_impersonation_authorization_attempts_total{decision="denied",mode="legacy"} 1
		apiserver_impersonation_authorization_attempts_total{decision="denied",mode="user-info"} 1
	`

	allMetrics := []string{
		namespace + "_" + subsystem + "_attempts_duration_seconds",
		namespace + "_" + subsystem + "_attempts_total",
		namespace + "_" + subsystem + "_authorization_attempts_duration_seconds",
		namespace + "_" + subsystem + "_authorization_attempts_total",
	}

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), allMetrics...); err != nil {
		t.Fatal(err)
	}
}

// resetMetricsForTest 清空本包注册的所有指标。
// 测试共用 legacyregistry 全局注册表，因此必须显式重置，否则用例之间会相互影响。
// resetMetricsForTest 清空本包注册的所有指标。
// 测试共用 legacyregistry 全局注册表，因此必须显式重置，否则用例之间会相互影响。
func resetMetricsForTest() {
	impersonationAttemptsTotal.Reset()
	impersonationAttemptsDurationSeconds.Reset()
	impersonationAuthorizationAttemptsTotal.Reset()
	impersonationAuthorizationAttemptsDurationSeconds.Reset()
}
