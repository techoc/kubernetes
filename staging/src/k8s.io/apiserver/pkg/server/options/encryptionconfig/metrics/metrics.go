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

// Package metrics 提供加密配置控制器的指标。
// 所有标签都只包含哈希后的 apiserver 身份与配置哈希，绝不暴露原始
// 身份或配置内容，避免经指标端点泄露安全敏感信息。
package metrics

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"sync"

	"k8s.io/apiserver/pkg/util/configmetrics"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "apiserver"
	subsystem = "encryption_config_controller"
)

var (
	// encryptionConfigAutomaticReloadsTotal 统计自动重载的成功/失败次数。
	encryptionConfigAutomaticReloadsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "automatic_reloads_total",
			Help:           "Total number of reload successes and failures of encryption configuration split by apiserver identity.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status", "apiserver_id_hash"},
	)

	// encryptionConfigAutomaticReloadLastTimestampSeconds 记录最近一次重载的时间戳，
	// 便于检测配置热加载是否停滞。
	encryptionConfigAutomaticReloadLastTimestampSeconds = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "automatic_reload_last_timestamp_seconds",
			Help:           "Timestamp of the last successful or failed automatic reload of encryption configuration split by apiserver identity.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status", "apiserver_id_hash"},
	)

	// encryptionConfigLastConfigInfo 以 info 风格暴露当前生效的配置哈希。
	encryptionConfigLastConfigInfo = metrics.NewDesc(
		metrics.BuildFQName(namespace, subsystem, "last_config_info"),
		"Information about the last applied encryption configuration with hash as label, split by apiserver identity.",
		[]string{"apiserver_id_hash", "hash"},
		nil,
		metrics.ALPHA,
		"",
	)
)

// registerMetrics 保证指标只注册一次。
var registerMetrics sync.Once

// hashPool 复用 sha256 计算器，避免每次记录指标都分配。
var hashPool *sync.Pool

// configHashProvider 维护各 apiserver 当前配置哈希，由自定义 collector 读取。
var configHashProvider = configmetrics.NewAtomicHashProvider()

// RegisterMetrics 注册本包全部指标；可重复调用。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		hashPool = &sync.Pool{
			New: func() interface{} {
				return sha256.New()
			},
		}
		legacyregistry.MustRegister(encryptionConfigAutomaticReloadsTotal)
		legacyregistry.MustRegister(encryptionConfigAutomaticReloadLastTimestampSeconds)
		legacyregistry.CustomMustRegister(configmetrics.NewConfigInfoCustomCollector(encryptionConfigLastConfigInfo, configHashProvider))
	})
}

// ResetMetricsForTest 清空计数器与时间戳，仅供测试隔离使用。
func ResetMetricsForTest() {
	encryptionConfigAutomaticReloadsTotal.Reset()
	encryptionConfigAutomaticReloadLastTimestampSeconds.Reset()
}

// RecordEncryptionConfigAutomaticReloadFailure 记录一次自动重载失败。
func RecordEncryptionConfigAutomaticReloadFailure(apiServerID string) {
	apiServerIDHash := getHash(apiServerID)
	encryptionConfigAutomaticReloadsTotal.WithLabelValues("failure", apiServerIDHash).Inc()
	recordEncryptionConfigAutomaticReloadTimestamp("failure", apiServerIDHash)
}

// RecordEncryptionConfigAutomaticReloadSuccess 记录一次自动重载成功，
// 并更新当前生效配置的哈希信息。
func RecordEncryptionConfigAutomaticReloadSuccess(apiServerID, encryptionConfigDataHash string) {
	apiServerIDHash := getHash(apiServerID)
	encryptionConfigAutomaticReloadsTotal.WithLabelValues("success", apiServerIDHash).Inc()
	recordEncryptionConfigAutomaticReloadTimestamp("success", apiServerIDHash)

	RecordEncryptionConfigLastConfigInfo(apiServerID, encryptionConfigDataHash)
}

// recordEncryptionConfigAutomaticReloadTimestamp 将时间戳设为当前时刻。
func recordEncryptionConfigAutomaticReloadTimestamp(result, apiServerIDHash string) {
	encryptionConfigAutomaticReloadLastTimestampSeconds.WithLabelValues(result, apiServerIDHash).SetToCurrentTime()
}

// RecordEncryptionConfigLastConfigInfo 记录某个 apiserver 当前生效的配置哈希。
func RecordEncryptionConfigLastConfigInfo(apiServerID, encryptionConfigDataHash string) {
	configHashProvider.SetHashes(getHash(apiServerID), encryptionConfigDataHash)
}

// getHash 返回 "sha256:<hex>" 形式的摘要；空输入返回空串以表示“未知”。
func getHash(data string) string {
	if len(data) == 0 {
		return ""
	}
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	h.Write([]byte(data))
	dataHash := fmt.Sprintf("sha256:%x", h.Sum(nil))
	hashPool.Put(h)
	return dataHash
}
