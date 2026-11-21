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

package plugin

import (
	"strings"
	"testing"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// TestKubeletCredentialProviderPluginErrors 验证插件错误计数器的上报。
func TestKubeletCredentialProviderPluginErrors(t *testing.T) {
	expectedValue := `
	# HELP kubelet_credential_provider_plugin_errors_total [ALPHA] Number of errors from credential provider plugin
	# TYPE kubelet_credential_provider_plugin_errors_total counter
	kubelet_credential_provider_plugin_errors_total{plugin_name="test-plugin"} 1
	`
	metricNames := []string{
		"kubelet_credential_provider_plugin_errors_total",
	}

	// 重置计数器，避免前序测试的残留值影响结果。
	kubeletCredentialProviderPluginErrors.Reset()
	registerMetrics()

	kubeletCredentialProviderPluginErrors.WithLabelValues("test-plugin").Inc()
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metricNames...); err != nil {
		t.Fatal(err)
	}
}

// TestKubeletCredentialProviderPluginDuration 验证插件耗时直方图的桶与统计量。
func TestKubeletCredentialProviderPluginDuration(t *testing.T) {
	expectedValue := `
	# HELP kubelet_credential_provider_plugin_duration [ALPHA] Duration of execution in seconds for credential provider plugin
	# TYPE kubelet_credential_provider_plugin_duration histogram
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="0.005"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="0.01"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="0.025"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="0.05"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="0.1"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="0.25"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="0.5"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="1"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="2.5"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="5"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="10"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="test-plugin",le="+Inf"} 1
	kubelet_credential_provider_plugin_duration_sum{plugin_name="test-plugin"} 0.3
	kubelet_credential_provider_plugin_duration_count{plugin_name="test-plugin"} 1
	`
	metricNames := []string{
		"kubelet_credential_provider_plugin_duration",
	}

	kubeletCredentialProviderPluginDuration.Reset()
	registerMetrics()

	kubeletCredentialProviderPluginDuration.WithLabelValues("test-plugin").Observe(0.3)
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metricNames...); err != nil {
		t.Fatal(err)
	}
}

// TestKubeletCredentialProviderConfigInfo 验证配置哈希 gauge 的上报。
func TestKubeletCredentialProviderConfigInfo(t *testing.T) {
	expectedValue := `
	# HELP kubelet_credential_provider_config_info [ALPHA] Information about the last applied credential provider configuration with hash as label
	# TYPE kubelet_credential_provider_config_info gauge
	kubelet_credential_provider_config_info{hash="sha256:abcd1234"} 1
	`
	metricNames := []string{
		"kubelet_credential_provider_config_info",
	}

	// 同时重置其他指标，避免残留的配置哈希干扰。
	kubeletCredentialProviderPluginErrors.Reset()
	kubeletCredentialProviderPluginDuration.Reset()
	registerMetrics()

	// 记录配置哈希，验证 gauge 标签值。
	recordCredentialProviderConfigHash("sha256:abcd1234")
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metricNames...); err != nil {
		t.Fatal(err)
	}
}

// TestMultiplePluginErrors 验证多个插件的错误计数按标签分别累计。
func TestMultiplePluginErrors(t *testing.T) {
	expectedValue := `
	# HELP kubelet_credential_provider_plugin_errors_total [ALPHA] Number of errors from credential provider plugin
	# TYPE kubelet_credential_provider_plugin_errors_total counter
	kubelet_credential_provider_plugin_errors_total{plugin_name="plugin-a"} 2
	kubelet_credential_provider_plugin_errors_total{plugin_name="plugin-b"} 1
	`
	metricNames := []string{
		"kubelet_credential_provider_plugin_errors_total",
	}

	kubeletCredentialProviderPluginErrors.Reset()
	registerMetrics()

	kubeletCredentialProviderPluginErrors.WithLabelValues("plugin-a").Inc()
	kubeletCredentialProviderPluginErrors.WithLabelValues("plugin-a").Inc()
	kubeletCredentialProviderPluginErrors.WithLabelValues("plugin-b").Inc()
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metricNames...); err != nil {
		t.Fatal(err)
	}
}

// TestMultiplePluginDurations 验证多个插件的耗时指标按标签分别记录。
func TestMultiplePluginDurations(t *testing.T) {
	expectedValue := `
	# HELP kubelet_credential_provider_plugin_duration [ALPHA] Duration of execution in seconds for credential provider plugin
	# TYPE kubelet_credential_provider_plugin_duration histogram
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="0.005"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="0.01"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="0.025"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="0.05"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="0.1"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="0.25"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="0.5"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="1"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="2.5"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="5"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="10"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="fast-plugin",le="+Inf"} 1
	kubelet_credential_provider_plugin_duration_sum{plugin_name="fast-plugin"} 0.001
	kubelet_credential_provider_plugin_duration_count{plugin_name="fast-plugin"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="0.005"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="0.01"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="0.025"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="0.05"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="0.1"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="0.25"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="0.5"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="1"} 0
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="2.5"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="5"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="10"} 1
	kubelet_credential_provider_plugin_duration_bucket{plugin_name="slow-plugin",le="+Inf"} 1
	kubelet_credential_provider_plugin_duration_sum{plugin_name="slow-plugin"} 2
	kubelet_credential_provider_plugin_duration_count{plugin_name="slow-plugin"} 1
	`
	metricNames := []string{
		"kubelet_credential_provider_plugin_duration",
	}

	kubeletCredentialProviderPluginDuration.Reset()
	registerMetrics()

	kubeletCredentialProviderPluginDuration.WithLabelValues("fast-plugin").Observe(0.001)
	kubeletCredentialProviderPluginDuration.WithLabelValues("slow-plugin").Observe(2.0)
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metricNames...); err != nil {
		t.Fatal(err)
	}
}

// TestCredentialProviderConfigInfoWithDifferentHashes 验证只上报最后一个配置哈希。
func TestCredentialProviderConfigInfoWithDifferentHashes(t *testing.T) {
	expectedValue := `
	# HELP kubelet_credential_provider_config_info [ALPHA] Information about the last applied credential provider configuration with hash as label
	# TYPE kubelet_credential_provider_config_info gauge
	kubelet_credential_provider_config_info{hash="sha256:config2"} 1
	`
	metricNames := []string{
		"kubelet_credential_provider_config_info",
	}

	registerMetrics()

	// 自定义收集器只保留最新哈希，因此期望值仅包含 config2。
	// 连续记录两个哈希，验证只保留最新状态。
	recordCredentialProviderConfigHash("sha256:config1")
	recordCredentialProviderConfigHash("sha256:config2")
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metricNames...); err != nil {
		t.Fatal(err)
	}
}
