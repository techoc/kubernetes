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

// Package metrics 记录授权配置控制器自动重载的结果与最近一次生效配置的哈希。
//
// 指标带 apiserver_id_hash 标签，用于在多实例部署中定位是哪个实例重载失败。
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
	subsystem = "authorization_config_controller"
)

var (
	// authorizationConfigAutomaticReloadsTotal 按成功/失败统计重载次数。
	authorizationConfigAutomaticReloadsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "automatic_reloads_total",
			Help:           "Total number of automatic reloads of authorization configuration split by status and apiserver identity.",
			StabilityLevel: metrics.BETA,
		},
		[]string{"status", "apiserver_id_hash"},
	)

	// authorizationConfigAutomaticReloadLastTimestampSeconds 记录最近一次重载的时间戳，
	// 便于发现“长时间未重载”的异常实例。
	authorizationConfigAutomaticReloadLastTimestampSeconds = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "automatic_reload_last_timestamp_seconds",
			Help:           "Timestamp of the last automatic reload of authorization configuration split by status and apiserver identity.",
			StabilityLevel: metrics.BETA,
		},
		[]string{"status", "apiserver_id_hash"},
	)

	// authorizationConfigLastConfigInfo 用哈希标识当前生效的配置内容，
	// 避免把配置原文写进指标。
	authorizationConfigLastConfigInfo = metrics.NewDesc(
		metrics.BuildFQName(namespace, subsystem, "last_config_info"),
		"Information about the last applied authorization configuration with hash as label, split by apiserver identity.",
		[]string{"apiserver_id_hash", "hash"},
		nil,
		metrics.ALPHA,
		"",
	)
)

// registerMetrics 保证重复调用只注册一次；hashPool 也在此初始化，
// 因为 RegisterMetrics 之前不应该有任何记录调用。
var registerMetrics sync.Once
var hashPool *sync.Pool
var configHashProvider = configmetrics.NewAtomicHashProvider()

// RegisterMetrics 注册本包的指标，并初始化可复用的 SHA-256 计算器池。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		hashPool = &sync.Pool{
			New: func() interface{} {
				return sha256.New()
			},
		}
		legacyregistry.MustRegister(authorizationConfigAutomaticReloadsTotal)
		legacyregistry.MustRegister(authorizationConfigAutomaticReloadLastTimestampSeconds)
		legacyregistry.CustomMustRegister(configmetrics.NewConfigInfoCustomCollector(authorizationConfigLastConfigInfo, configHashProvider))
	})
}

// ResetMetricsForTest 清空计数器与时间戳，供测试隔离用例。
func ResetMetricsForTest() {
	authorizationConfigAutomaticReloadsTotal.Reset()
	authorizationConfigAutomaticReloadLastTimestampSeconds.Reset()
}

// RecordAuthorizationConfigAutomaticReloadFailure 记录一次重载失败。
func RecordAuthorizationConfigAutomaticReloadFailure(apiServerID string) {
	apiServerIDHash := getHash(apiServerID)
	authorizationConfigAutomaticReloadsTotal.WithLabelValues("failure", apiServerIDHash).Inc()
	authorizationConfigAutomaticReloadLastTimestampSeconds.WithLabelValues("failure", apiServerIDHash).SetToCurrentTime()
}

// RecordAuthorizationConfigAutomaticReloadSuccess 记录一次重载成功，并更新当前配置哈希。
func RecordAuthorizationConfigAutomaticReloadSuccess(apiServerID, authzConfigData string) {
	apiServerIDHash := getHash(apiServerID)
	authorizationConfigAutomaticReloadsTotal.WithLabelValues("success", apiServerIDHash).Inc()
	authorizationConfigAutomaticReloadLastTimestampSeconds.WithLabelValues("success", apiServerIDHash).SetToCurrentTime()

	RecordAuthorizationConfigLastConfigInfo(apiServerID, authzConfigData)
}

// RecordAuthorizationConfigLastConfigInfo 更新“当前配置”的实例 ID 与内容哈希。
func RecordAuthorizationConfigLastConfigInfo(apiServerID, authzConfigData string) {
	configHashProvider.SetHashes(getHash(apiServerID), getHash(authzConfigData))
}

// getHash 返回 "sha256:<hex>" 形式的摘要；空输入返回空串，表示“无值”而非某个固定哈希。
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
