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

// Package metrics 提供基于清单的准入配置相关指标。
package metrics

import (
	"crypto/sha256"
	"fmt"
	"sync"

	"k8s.io/apiserver/pkg/util/configmetrics"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "apiserver"
	subsystem = "manifest_admission_config_controller"
)

var (
	admissionManifestAutomaticReloadsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "automatic_reloads_total",
			Help:           "Total number of automatic reloads of admission manifest configuration split by status, plugin, and apiserver identity.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status", "plugin", "apiserver_id_hash"},
	)

	admissionManifestAutomaticReloadLastTimestampSeconds = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "automatic_reload_last_timestamp_seconds",
			Help:           "Timestamp of the last automatic reload of admission manifest configuration split by status, plugin, and apiserver identity.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status", "plugin", "apiserver_id_hash"},
	)

	admissionManifestLastConfigInfo = metrics.NewDesc(
		metrics.BuildFQName(namespace, subsystem, "last_config_info"),
		"Information about the last applied admission manifest configuration with hash as label, split by plugin and apiserver identity.",
		[]string{"plugin", "apiserver_id_hash", "hash"},
		nil,
		metrics.ALPHA,
		"",
	)
)

// ManifestType 表示当前正在跟踪清单的准入插件名称。
type ManifestType string

const (
	// ValidatingWebhookManifestType 表示 validating webhook 配置。
	ValidatingWebhookManifestType ManifestType = "ValidatingAdmissionWebhook"
	// MutatingWebhookManifestType 表示 mutating webhook 配置。
	MutatingWebhookManifestType ManifestType = "MutatingAdmissionWebhook"
	// VAPManifestType 表示 ValidatingAdmissionPolicy 配置。
	VAPManifestType ManifestType = "ValidatingAdmissionPolicy"
	// MAPManifestType 表示 MutatingAdmissionPolicy 配置。
	MAPManifestType ManifestType = "MutatingAdmissionPolicy"
)

var registerMetrics sync.Once

// configHashProviders 按插件存储配置信息指标的 provider，
// 每个插件的 provider 各自保存 [apiserver_id_hash, config_hash]。
//
// 按插件分开存储，避免不同插件（webhook/policy）的配置哈希互相覆盖。
var configHashProviders = map[ManifestType]*configmetrics.AtomicHashProvider{
	ValidatingWebhookManifestType: configmetrics.NewAtomicHashProvider(),
	MutatingWebhookManifestType:   configmetrics.NewAtomicHashProvider(),
	VAPManifestType:               configmetrics.NewAtomicHashProvider(),
	MAPManifestType:               configmetrics.NewAtomicHashProvider(),
}

// multiTypeConfigInfoCollector 为多种清单类型输出配置信息指标。
//
// 用自定义 collector 而不是 GaugeVec，是因为配置哈希按插件分别保存在
// AtomicHashProvider 中，且只有成功加载过配置的插件才应暴露该指标。
type multiTypeConfigInfoCollector struct {
	metrics.BaseStableCollector
	desc *metrics.Desc
}

var _ metrics.StableCollector = &multiTypeConfigInfoCollector{}

// DescribeWithStability 向 registry 声明本 collector 产出的唯一指标描述。
func (c *multiTypeConfigInfoCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- c.desc
}

// CollectWithStability 为每个已上报配置的插件输出一条常量 1 的 info 指标。
// 配置内容只以哈希形式出现，避免在指标中泄漏原文。
func (c *multiTypeConfigInfoCollector) CollectWithStability(ch chan<- metrics.Metric) {
	// 为每个有数据的插件输出一条指标
	for manifestType, provider := range configHashProviders {
		hashes := provider.GetCurrentHashes()
		if len(hashes) >= 2 && len(hashes[0]) > 0 {
			// hashes 包含 [apiserver_id_hash, config_hash]
			// 前置插件名以匹配标签顺序：["plugin", "apiserver_id_hash", "hash"]
			labelValues := append([]string{string(manifestType)}, hashes...)
			ch <- metrics.NewLazyConstMetric(c.desc, metrics.GaugeValue, 1, labelValues...)
		}
	}
}

// RegisterMetrics 注册清单式准入配置相关指标；sync.Once 防止重复注册 panic。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(admissionManifestAutomaticReloadsTotal)
		legacyregistry.MustRegister(admissionManifestAutomaticReloadLastTimestampSeconds)
		// 使用自定义 collector 输出所有清单类型的指标
		legacyregistry.CustomMustRegister(&multiTypeConfigInfoCollector{desc: admissionManifestLastConfigInfo})
	})
}

// ResetMetricsForTest 清空计数器与时间戳，供测试隔离使用。
// info 指标由无状态 collector 即时计算，无需重置。
func ResetMetricsForTest() {
	admissionManifestAutomaticReloadsTotal.Reset()
	admissionManifestAutomaticReloadLastTimestampSeconds.Reset()
}

// RecordAutomaticReloadFailure 记录给定清单类型的重载失败。
func RecordAutomaticReloadFailure(manifestType ManifestType, apiServerID string) {
	apiServerIDHash := getHash(apiServerID)
	admissionManifestAutomaticReloadsTotal.WithLabelValues("failure", string(manifestType), apiServerIDHash).Inc()
	admissionManifestAutomaticReloadLastTimestampSeconds.WithLabelValues("failure", string(manifestType), apiServerIDHash).SetToCurrentTime()
}

// RecordAutomaticReloadSuccess 记录给定清单类型的重载成功。
//
// 成功时额外更新配置哈希，失败则保留上一次成功的哈希——监控上"最后生效的
// 配置"不应因为一次失败的重载而丢失。
func RecordAutomaticReloadSuccess(manifestType ManifestType, apiServerID, configHash string) {
	apiServerIDHash := getHash(apiServerID)
	admissionManifestAutomaticReloadsTotal.WithLabelValues("success", string(manifestType), apiServerIDHash).Inc()
	admissionManifestAutomaticReloadLastTimestampSeconds.WithLabelValues("success", string(manifestType), apiServerIDHash).SetToCurrentTime()

	RecordLastConfigInfo(manifestType, apiServerID, configHash)
}

// RecordLastConfigInfo 记录最近一次成功加载配置的哈希。
func RecordLastConfigInfo(manifestType ManifestType, apiServerID, configHash string) {
	if provider, ok := configHashProviders[manifestType]; ok {
		// 存储 [apiserver_id_hash, config_hash]，类型名由 multiTypeConfigInfoCollector 前置
		provider.SetHashes(getHash(apiServerID), configHash)
	}
}

// getHash 对标识类字符串取 SHA-256 前缀并加算法前缀，避免在指标标签中
// 暴露原始 apiserver 身份（可能含主机名等敏感信息）。
func getHash(data string) string {
	if len(data) == 0 {
		return ""
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(data)))
}
