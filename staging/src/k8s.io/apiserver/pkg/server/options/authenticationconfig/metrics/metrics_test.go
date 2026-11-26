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

// Package metrics 记录认证配置控制器自动重载的结果、时间戳与生效配置哈希。
package metrics

import (
	"strings"
	"testing"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

const (
	testAPIServerID     = "testAPIServerID"
	testAPIServerIDHash = "sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37"
	testConfigData      = `
apiVersion: apiserver.config.k8s.io/v1
kind: AuthenticationConfiguration
jwt:
- issuer:
    url: https://test-issuer
    audiences: [ "aud" ]
  claimMappings:
    username:
      claim: sub
      prefix: ""
`
)

// TestRecordAuthenticationConfigAutomaticReloadFailure 验证失败计数只带 status=failure 标签，
// 且 apiserver_id_hash 是对原始 ID 做哈希后的稳定值（避免泄露主机名）。
func TestRecordAuthenticationConfigAutomaticReloadFailure(t *testing.T) {
	expectedValue := `
	# HELP apiserver_authentication_config_controller_automatic_reloads_total [BETA] Total number of automatic reloads of authentication configuration split by status and apiserver identity.
    # TYPE apiserver_authentication_config_controller_automatic_reloads_total counter
    apiserver_authentication_config_controller_automatic_reloads_total {apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",status="failure"} 1
	`
	metrics := []string{
		namespace + "_" + subsystem + "_automatic_reloads_total",
	}

	authenticationConfigAutomaticReloadsTotal.Reset()
	RegisterMetrics()

	RecordAuthenticationConfigAutomaticReloadFailure(testAPIServerID)
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
		t.Fatal(err)
	}
}

// TestRecordAuthenticationConfigAutomaticReloadSuccess 验证成功时同时更新计数与
// last_config_info（后者以配置内容哈希为标签，值为 1，用于表达"当前生效的配置"）。
func TestRecordAuthenticationConfigAutomaticReloadSuccess(t *testing.T) {
	expectedValue := `
	# HELP apiserver_authentication_config_controller_automatic_reloads_total [BETA] Total number of automatic reloads of authentication configuration split by status and apiserver identity.
    # TYPE apiserver_authentication_config_controller_automatic_reloads_total counter
    apiserver_authentication_config_controller_automatic_reloads_total {apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",status="success"} 1
	# HELP apiserver_authentication_config_controller_last_config_info [ALPHA] Information about the last applied authentication configuration with hash as label, split by apiserver identity.
	# TYPE apiserver_authentication_config_controller_last_config_info gauge
	apiserver_authentication_config_controller_last_config_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:ccbcaf98557c273dfc779222d54a5bd3e785ea5330048f3bf4278cf3997b669c"} 1
	`
	metrics := []string{
		namespace + "_" + subsystem + "_automatic_reloads_total",
		namespace + "_" + subsystem + "_last_config_info",
	}

	ResetMetricsForTest()
	RegisterMetrics()

	RecordAuthenticationConfigAutomaticReloadSuccess(testAPIServerID, testConfigData)
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
		t.Fatal(err)
	}
}

// TestAuthenticationConfigAutomaticReloadLastTimestampSeconds 验证时间戳 gauge 按
// status 分标签暴露，且保留秒级精度。
func TestAuthenticationConfigAutomaticReloadLastTimestampSeconds(t *testing.T) {
	testCases := []struct {
		expectedValue string
		resultLabel   string
		timestamp     int64
	}{
		{
			expectedValue: `
                # HELP apiserver_authentication_config_controller_automatic_reload_last_timestamp_seconds [BETA] Timestamp of the last automatic reload of authentication configuration split by status and apiserver identity.
                # TYPE apiserver_authentication_config_controller_automatic_reload_last_timestamp_seconds gauge
                apiserver_authentication_config_controller_automatic_reload_last_timestamp_seconds{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",status="failure"} 1.689101941e+09
            `,
			resultLabel: "failure",
			timestamp:   1689101941,
		},
		{
			expectedValue: `
                # HELP apiserver_authentication_config_controller_automatic_reload_last_timestamp_seconds [BETA] Timestamp of the last automatic reload of authentication configuration split by status and apiserver identity.
                # TYPE apiserver_authentication_config_controller_automatic_reload_last_timestamp_seconds gauge
                apiserver_authentication_config_controller_automatic_reload_last_timestamp_seconds{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",status="success"} 1.689101941e+09
            `,
			resultLabel: "success",
			timestamp:   1689101941,
		},
	}

	metrics := []string{
		namespace + "_" + subsystem + "_automatic_reload_last_timestamp_seconds",
	}
	RegisterMetrics()

	for _, tc := range testCases {
		authenticationConfigAutomaticReloadLastTimestampSeconds.Reset()
		authenticationConfigAutomaticReloadLastTimestampSeconds.WithLabelValues(tc.resultLabel, testAPIServerIDHash).Set(float64(tc.timestamp))

		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tc.expectedValue), metrics...); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRecordAuthenticationConfigAutomaticReloadSuccess_StaleMetricCleanup 验证配置切换后
// 旧哈希对应的 gauge 会被清除，避免监控端误以为存在多个生效配置。
func TestRecordAuthenticationConfigAutomaticReloadSuccess_StaleMetricCleanup(t *testing.T) {
	ResetMetricsForTest()
	RegisterMetrics()

	// 记录第一份配置的首次成功
	firstConfig := "config1"
	RecordAuthenticationConfigAutomaticReloadSuccess(testAPIServerID, firstConfig)

	// 记录另一份配置的成功——应清理旧指标
	secondConfig := "config2"
	RecordAuthenticationConfigAutomaticReloadSuccess(testAPIServerID, secondConfig)

	// 校验只剩最新 hash 的指标
	expectedValue := `
	# HELP apiserver_authentication_config_controller_automatic_reloads_total [BETA] Total number of automatic reloads of authentication configuration split by status and apiserver identity.
    # TYPE apiserver_authentication_config_controller_automatic_reloads_total counter
    apiserver_authentication_config_controller_automatic_reloads_total {apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",status="success"} 2
	# HELP apiserver_authentication_config_controller_last_config_info [ALPHA] Information about the last applied authentication configuration with hash as label, split by apiserver identity.
	# TYPE apiserver_authentication_config_controller_last_config_info gauge
	apiserver_authentication_config_controller_last_config_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:f309dd9c31fe24b3e594d2f9420419c48dfe954523245d5f35dc37739970d881"} 1
	`
	metrics := []string{
		namespace + "_" + subsystem + "_automatic_reloads_total",
		namespace + "_" + subsystem + "_last_config_info",
	}

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
		t.Fatal(err)
	}
}
