/*
Copyright 2021 The Kubernetes Authors.

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
	"sync"

	"k8s.io/apiserver/pkg/util/configmetrics"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	KubeletSubsystem = "kubelet"
)

var (
	registerOnce sync.Once

	kubeletCredentialProviderPluginErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "credential_provider_plugin_errors_total",
			Help:           "Number of errors from credential provider plugin",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"plugin_name"},
	)

	kubeletCredentialProviderPluginDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "credential_provider_plugin_duration",
			Help:           "Duration of execution in seconds for credential provider plugin",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"plugin_name"},
	)

	// kubeletCredentialProviderConfigInfo 提供当前生效配置的描述信息。
	// hash 标签通常在 kubelet 进程生命周期内不变，仅在配置更新并重启后变化。
	kubeletCredentialProviderConfigInfo = metrics.NewDesc(
		metrics.BuildFQName("", KubeletSubsystem, "credential_provider_config_info"),
		"Information about the last applied credential provider configuration with hash as label",
		[]string{"hash"},
		nil,
		metrics.ALPHA,
		"",
	)
)

var configHashProvider = configmetrics.NewAtomicHashProvider()

// registerMetrics 注册凭证插件相关指标，且只注册一次。
func registerMetrics() {
	registerOnce.Do(func() {
		legacyregistry.MustRegister(kubeletCredentialProviderPluginErrors)
		legacyregistry.MustRegister(kubeletCredentialProviderPluginDuration)
		legacyregistry.CustomMustRegister(configmetrics.NewConfigInfoCustomCollector(kubeletCredentialProviderConfigInfo, configHashProvider))
	})
}

// recordCredentialProviderConfigHash 记录当前凭证插件配置的哈希。
func recordCredentialProviderConfigHash(configHash string) {
	configHashProvider.SetHashes(configHash)
}
