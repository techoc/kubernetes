/*
Copyright 2024 The Kubernetes Authors.

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

package cel

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// TestRecordAuthorizationMatchConditionEvaluationFailure 验证授权 webhook 的
// match condition 相关指标：评估失败计数、排除计数与评估耗时直方图。
// 中文说明：使用 legacyregistry 全局 gatherer，因此每个子用例前必须 ResetMetricsForTest，
// 否则上一个用例的计数会污染断言。期望值刻意包含 HELP/TYPE 行，
// 以锁定指标元数据；histogram 用 1 秒的观测落入 +Inf 桶来验证分桶与 sum/count。
func TestRecordAuthorizationMatchConditionEvaluationFailure(t *testing.T) {
	testCases := []struct {
		desc      string
		metrics   []string
		name      string
		authztype string
		want      string
	}{
		{
			desc: "evaluation failure total",
			metrics: []string{
				"apiserver_authorization_match_condition_evaluation_errors_total",
				"apiserver_authorization_match_condition_exclusions_total",
				"apiserver_authorization_match_condition_evaluation_seconds",
			},
			name:      "wh1.example.com",
			authztype: "Webhook",
			want: `
			# HELP apiserver_authorization_match_condition_evaluation_errors_total [ALPHA] Total number of errors when an authorization webhook encounters a match condition error split by authorizer type and name.
			# TYPE apiserver_authorization_match_condition_evaluation_errors_total counter
			apiserver_authorization_match_condition_evaluation_errors_total{name="wh1.example.com",type="Webhook"} 1
			# HELP apiserver_authorization_match_condition_evaluation_seconds [ALPHA] Authorization match condition evaluation time in seconds, split by authorizer type and name.
        	# TYPE apiserver_authorization_match_condition_evaluation_seconds histogram
        	apiserver_authorization_match_condition_evaluation_seconds_bucket{name="wh1.example.com",type="Webhook",le="0.001"} 0
        	apiserver_authorization_match_condition_evaluation_seconds_bucket{name="wh1.example.com",type="Webhook",le="0.005"} 0
        	apiserver_authorization_match_condition_evaluation_seconds_bucket{name="wh1.example.com",type="Webhook",le="0.01"} 0
        	apiserver_authorization_match_condition_evaluation_seconds_bucket{name="wh1.example.com",type="Webhook",le="0.025"} 0
        	apiserver_authorization_match_condition_evaluation_seconds_bucket{name="wh1.example.com",type="Webhook",le="0.1"} 0
        	apiserver_authorization_match_condition_evaluation_seconds_bucket{name="wh1.example.com",type="Webhook",le="0.2"} 0
        	apiserver_authorization_match_condition_evaluation_seconds_bucket{name="wh1.example.com",type="Webhook",le="0.25"} 0
        	apiserver_authorization_match_condition_evaluation_seconds_bucket{name="wh1.example.com",type="Webhook",le="+Inf"} 1
        	apiserver_authorization_match_condition_evaluation_seconds_sum{name="wh1.example.com",type="Webhook"} 1
        	apiserver_authorization_match_condition_evaluation_seconds_count{name="wh1.example.com",type="Webhook"} 1
			# HELP apiserver_authorization_match_condition_exclusions_total [ALPHA] Total number of exclusions when an authorization webhook is skipped because match conditions exclude it.
			# TYPE apiserver_authorization_match_condition_exclusions_total counter
			apiserver_authorization_match_condition_exclusions_total{name="wh1.example.com",type="Webhook"} 1
			`,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			// 全局 registry 状态必须重置，保证指标基线为零。
			ResetMetricsForTest()
			m := NewMatcherMetrics()
			// 依次记录失败、排除与耗时三类事件，覆盖指标的全部写入路径。
			m.RecordAuthorizationMatchConditionEvaluationFailure(context.Background(), tt.authztype, tt.name)
			m.RecordAuthorizationMatchConditionExclusion(context.Background(), tt.authztype, tt.name)
			m.RecordAuthorizationMatchConditionEvaluation(context.Background(), tt.authztype, tt.name, time.Duration(1*time.Second))
			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tt.want), tt.metrics...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
