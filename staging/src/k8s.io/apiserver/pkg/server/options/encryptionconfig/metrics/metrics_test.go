/*
Copyright 2023 The Kubernetes Authors.

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
	"strings"
	"testing"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

const (
	// testAPIServerID 是测试用 apiserver 标识；指标中只出现其哈希。
	testAPIServerID = "testAPIServerID"
	// testAPIServerIDHash 是 testAPIServerID 对应的 sha256，用于断言标签值。
	testAPIServerIDHash = "sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37"
	// testConfigDataHash 是测试用加密配置内容的哈希。
	testConfigDataHash = "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3"
)

// TestRecordEncryptionConfigAutomaticReloadFailure 验证自动重载失败会以
// status="failure" 计数，且 apiserver 标识只以哈希形式暴露。
func TestRecordEncryptionConfigAutomaticReloadFailure(t *testing.T) {
	expectedValue := `
	# HELP apiserver_encryption_config_controller_automatic_reloads_total [ALPHA] Total number of reload successes and failures of encryption configuration split by apiserver identity.
    # TYPE apiserver_encryption_config_controller_automatic_reloads_total counter
    apiserver_encryption_config_controller_automatic_reloads_total {apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",status="failure"} 1
	`
	metricNames := []string{
		namespace + "_" + subsystem + "_automatic_reloads_total",
	}

	encryptionConfigAutomaticReloadsTotal.Reset()
	RegisterMetrics()

	RecordEncryptionConfigAutomaticReloadFailure(testAPIServerID)
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metricNames...); err != nil {
		t.Fatal(err)
	}
}

// TestRecordEncryptionConfigAutomaticReloadSuccess 验证成功重载同时更新
// 重载计数与“最后一次生效配置”的信息指标。
func TestRecordEncryptionConfigAutomaticReloadSuccess(t *testing.T) {
	expectedValue := `
	# HELP apiserver_encryption_config_controller_automatic_reloads_total [ALPHA] Total number of reload successes and failures of encryption configuration split by apiserver identity.
    # TYPE apiserver_encryption_config_controller_automatic_reloads_total counter
    apiserver_encryption_config_controller_automatic_reloads_total {apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",status="success"} 1
	# HELP apiserver_encryption_config_controller_last_config_info [ALPHA] Information about the last applied encryption configuration with hash as label, split by apiserver identity.
	# TYPE apiserver_encryption_config_controller_last_config_info gauge
	apiserver_encryption_config_controller_last_config_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3"} 1
	`
	metricNames := []string{
		namespace + "_" + subsystem + "_automatic_reloads_total",
		namespace + "_" + subsystem + "_last_config_info",
	}

	ResetMetricsForTest()
	RegisterMetrics()

	RecordEncryptionConfigAutomaticReloadSuccess(testAPIServerID, testConfigDataHash)
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metricNames...); err != nil {
		t.Fatal(err)
	}
}

// TestEncryptionConfigAutomaticReloadLastTimestampSeconds 验证最后一次重载时间戳
// 分别按 success/failure 标签记录为秒级浮点数。
func TestEncryptionConfigAutomaticReloadLastTimestampSeconds(t *testing.T) {
	testCases := []struct {
		expectedValue string
		resultLabel   string
		timestamp     int64
	}{
		{
			expectedValue: `
                # HELP apiserver_encryption_config_controller_automatic_reload_last_timestamp_seconds [ALPHA] Timestamp of the last successful or failed automatic reload of encryption configuration split by apiserver identity.
                # TYPE apiserver_encryption_config_controller_automatic_reload_last_timestamp_seconds gauge
                apiserver_encryption_config_controller_automatic_reload_last_timestamp_seconds{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",status="failure"} 1.689101941e+09
            `,
			resultLabel: "failure",
			timestamp:   1689101941,
		},
		{
			expectedValue: `
                # HELP apiserver_encryption_config_controller_automatic_reload_last_timestamp_seconds [ALPHA] Timestamp of the last successful or failed automatic reload of encryption configuration split by apiserver identity.
                # TYPE apiserver_encryption_config_controller_automatic_reload_last_timestamp_seconds gauge
                apiserver_encryption_config_controller_automatic_reload_last_timestamp_seconds{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",status="success"} 1.689101941e+09
            `,
			resultLabel: "success",
			timestamp:   1689101941,
		},
	}

	metricNames := []string{
		namespace + "_" + subsystem + "_automatic_reload_last_timestamp_seconds",
	}
	RegisterMetrics()

	for _, tc := range testCases {
		encryptionConfigAutomaticReloadLastTimestampSeconds.Reset()
		encryptionConfigAutomaticReloadLastTimestampSeconds.WithLabelValues(tc.resultLabel, testAPIServerIDHash).Set(float64(tc.timestamp))

		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tc.expectedValue), metricNames...); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRecordEncryptionConfigAutomaticReloadSuccess_StaleMetricCleanup 验证重载成功后
// 会清理旧配置哈希对应的 last_config_info 指标，避免同一 apiserver 同时暴露多个
// “当前配置”标签，导致监控误判。
func TestRecordEncryptionConfigAutomaticReloadSuccess_StaleMetricCleanup(t *testing.T) {
	ResetMetricsForTest()
	RegisterMetrics()

	// 记录第一个配置
	firstConfigHash := "sha256:firsthash"
	RecordEncryptionConfigAutomaticReloadSuccess(testAPIServerID, firstConfigHash)

	// 确认第一个配置的指标存在
	firstExpected := `
	# HELP apiserver_encryption_config_controller_last_config_info [ALPHA] Information about the last applied encryption configuration with hash as label, split by apiserver identity.
	# TYPE apiserver_encryption_config_controller_last_config_info gauge
	apiserver_encryption_config_controller_last_config_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:firsthash"} 1
	`
	metricNames := []string{
		namespace + "_" + subsystem + "_last_config_info",
	}

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(firstExpected), metricNames...); err != nil {
		t.Fatal(err)
	}

	// 记录第二个配置——应清理第一个配置的指标
	secondConfigHash := "sha256:secondhash"
	RecordEncryptionConfigAutomaticReloadSuccess(testAPIServerID, secondConfigHash)

	// 确认只剩第二个配置的指标
	secondExpected := `
	# HELP apiserver_encryption_config_controller_last_config_info [ALPHA] Information about the last applied encryption configuration with hash as label, split by apiserver identity.
	# TYPE apiserver_encryption_config_controller_last_config_info gauge
	apiserver_encryption_config_controller_last_config_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:secondhash"} 1
	`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(secondExpected), metricNames...); err != nil {
		t.Fatal(err)
	}
}
