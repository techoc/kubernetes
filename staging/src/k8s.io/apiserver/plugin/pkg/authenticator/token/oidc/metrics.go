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

package oidc

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/utils/clock"
)

const (
	// 指标统一挂在 apiserver_authentication 下，与其他认证相关指标并列。
	namespace = "apiserver"
	subsystem = "authentication"
)

var (
	// 只统计缓存未命中的认证耗时，因为命中路径由缓存层单独观测。
	jwtAuthenticatorLatencyMetric = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "jwt_authenticator_latency_seconds",
			Help:           "Latency of jwt authentication operations in seconds. This is the time spent authenticating a token for cache miss only (i.e. when the token is not found in the cache).",
			StabilityLevel: metrics.ALPHA,
			// default histogram buckets with a 1ms starting point
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"result", "jwt_issuer_hash"},
	)

	jwksFetchLastTimestampSeconds = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "jwt_authenticator_jwks_fetch_last_timestamp_seconds",
			Help: "Timestamp of the last successful or failed JWKS fetch split by result, api server identity " +
				"and jwt issuer for the JWT authenticator.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result", "jwt_issuer_hash", "apiserver_id_hash"},
	)

	jwksFetchLastKeySetInfo = metrics.NewDesc(
		metrics.BuildFQName(namespace, subsystem, "jwt_authenticator_jwks_fetch_last_key_set_info"),
		"Information about the last JWKS fetched by the JWT authenticator with hash as label, split by api server identity and jwt issuer.",
		[]string{"jwt_issuer_hash", "apiserver_id_hash", "hash"},
		nil,
		metrics.ALPHA,
		"",
	)
)

// jwksHashKey uniquely identifies a JWKS by issuer and API server ID
//
// 中文说明：用哈希而非原始 issuer 作为键，避免指标标签泄漏内部身份配置。
type jwksHashKey struct {
	jwtIssuerHash   string
	apiServerIDHash string
}

// jwksHashProvider manages JWKS hashes for all authenticators
//
// 中文说明：多个 OIDC 认证器共享该 provider，采集时一次性汇总；sync.Map
// 适配"读多写少、键集合动态变化"的场景。
type jwksHashProvider struct {
	hashes sync.Map // map[jwksHashKey]string
}

// newJWKSHashProvider 创建空的哈希提供者。
func newJWKSHashProvider() *jwksHashProvider {
	return &jwksHashProvider{}
}

// setHash 记录某 issuer/apiserver 组合最近一次成功获取的 key set 哈希。
func (p *jwksHashProvider) setHash(jwtIssuer, apiServerID, keySet string) {
	key := jwksHashKey{
		jwtIssuerHash:   getHash(jwtIssuer),
		apiServerIDHash: getHash(apiServerID),
	}
	jwksHash := getHash(keySet)
	p.hashes.Store(key, jwksHash)
}

// getHashes 返回当前全部哈希的快照。
//
// 中文说明：返回副本而非直接暴露 sync.Map，保证采集过程中不会与写入竞争。
func (p *jwksHashProvider) getHashes() map[jwksHashKey]string {
	result := make(map[jwksHashKey]string)
	p.hashes.Range(func(k, v interface{}) bool {
		result[k.(jwksHashKey)] = v.(string)
		return true
	})
	return result
}

// reset 清空全部记录，仅供测试使用。
func (p *jwksHashProvider) reset() {
	p.hashes.Range(func(k, v interface{}) bool {
		p.hashes.Delete(k)
		return true
	})
}

// deleteHash 删除某 issuer/apiserver 组合的哈希。
func (p *jwksHashProvider) deleteHash(jwtIssuer, apiServerID string) {
	key := jwksHashKey{
		jwtIssuerHash:   getHash(jwtIssuer),
		apiServerIDHash: getHash(apiServerID),
	}
	p.hashes.Delete(key)
}

// jwksHashCollector is a custom collector that emits JWKS hash metrics
//
// 中文说明：哈希集合动态变化，无法用固定标签的 GaugeVec 表达，因此用自定义
// collector 在采集时枚举。
type jwksHashCollector struct {
	metrics.BaseStableCollector
	desc         *metrics.Desc
	hashProvider *jwksHashProvider
}

// newJWKSHashCollector 绑定指标描述与哈希来源。
func newJWKSHashCollector(desc *metrics.Desc, hashProvider *jwksHashProvider) metrics.StableCollector {
	return &jwksHashCollector{
		desc:         desc,
		hashProvider: hashProvider,
	}
}

// DescribeWithStability 声明唯一产出的指标描述。
func (c *jwksHashCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- c.desc
}

// CollectWithStability 为每个 issuer/apiserver 组合输出一条值为 1 的 info 指标。
func (c *jwksHashCollector) CollectWithStability(ch chan<- metrics.Metric) {
	hashes := c.hashProvider.getHashes()
	for key, hash := range hashes {
		ch <- metrics.NewLazyConstMetric(
			c.desc,
			metrics.GaugeValue,
			1,
			key.jwtIssuerHash,
			key.apiServerIDHash,
			hash,
		)
	}
}

var registerMetrics sync.Once
var hashProvider = newJWKSHashProvider()

// RegisterMetrics 幂等注册全部 OIDC 认证指标。
//
// 中文说明：sync.Once 防止多个认证器实例重复注册导致 MustRegister panic。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(jwtAuthenticatorLatencyMetric)
		legacyregistry.MustRegister(jwksFetchLastTimestampSeconds)
		legacyregistry.CustomMustRegister(newJWKSHashCollector(jwksFetchLastKeySetInfo, hashProvider))
	})
}

// recordAuthenticationLatency 记录一次认证耗时，result 为 success/failure。
func recordAuthenticationLatency(result, jwtIssuerHash string, duration time.Duration) {
	jwtAuthenticatorLatencyMetric.WithLabelValues(result, jwtIssuerHash).Observe(duration.Seconds())
}

// recordJWKSFetchTimestamp 把最近一次 JWKS 获取时间设为当前时刻。
func recordJWKSFetchTimestamp(result, jwtIssuer, apiServerID string) {
	jwksFetchLastTimestampSeconds.WithLabelValues(result, getHash(jwtIssuer), getHash(apiServerID)).SetToCurrentTime()
}

// recordJWKSFetchKeySetSuccess 记录一次成功的 JWKS 获取：哈希 + 时间戳。
func recordJWKSFetchKeySetSuccess(jwtIssuer, apiServerID, keySet string) {
	recordJWKSFetchKeySetHash(jwtIssuer, apiServerID, keySet)
	recordJWKSFetchTimestamp("success", jwtIssuer, apiServerID)
}

// recordJWKSFetchKeySetFailure 只更新时间戳，保留上一次成功的哈希，
// 便于判断"失败但仍在使用旧 key set"。
func recordJWKSFetchKeySetFailure(jwtIssuer, apiServerID string) {
	recordJWKSFetchTimestamp("failure", jwtIssuer, apiServerID)
}

// recordJWKSFetchKeySetHash 更新哈希提供者中的记录。
func recordJWKSFetchKeySetHash(jwtIssuer, apiServerID, keySet string) {
	hashProvider.setHash(jwtIssuer, apiServerID, keySet)
}

// DeleteJWKSFetchMetrics deletes all JWKS-related metrics for a specific issuer and API server.
// This includes the hash metric and timestamp metrics (both success and failure).
// This should be called when an issuer is removed from the configuration to clean up stale metrics.
func DeleteJWKSFetchMetrics(jwtIssuer, apiServerID string) {
	jwtIssuerHash := getHash(jwtIssuer)
	apiServerIDHash := getHash(apiServerID)

	hashProvider.deleteHash(jwtIssuer, apiServerID)

	jwksFetchLastTimestampSeconds.DeleteLabelValues("success", jwtIssuerHash, apiServerIDHash)
	jwksFetchLastTimestampSeconds.DeleteLabelValues("failure", jwtIssuerHash, apiServerIDHash)
}

// ResetMetrics 清空全部指标，仅供测试使用。
func ResetMetrics() {
	jwtAuthenticatorLatencyMetric.Reset()
	jwksFetchLastTimestampSeconds.Reset()
	hashProvider.reset()
}

// getHash 返回带算法前缀的 SHA-256 十六进制摘要；空输入返回空串，
// 以区分"未配置"与"已配置的空值"。
func getHash(data string) string {
	if len(data) > 0 {
		return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(data)))
	}
	return ""
}

// newInstrumentedAuthenticator 用真实时钟包装认证器。
func newInstrumentedAuthenticator(jwtIssuer string, delegate AuthenticatorTokenWithHealthCheck) AuthenticatorTokenWithHealthCheck {
	return newInstrumentedAuthenticatorWithClock(jwtIssuer, delegate, clock.RealClock{})
}

// newInstrumentedAuthenticatorWithClock 允许注入时钟，便于测试耗时统计。
func newInstrumentedAuthenticatorWithClock(jwtIssuer string, delegate AuthenticatorTokenWithHealthCheck, clock clock.PassiveClock) *instrumentedAuthenticator {
	return &instrumentedAuthenticator{
		jwtIssuerHash: getHash(jwtIssuer),
		delegate:      delegate,
		clock:         clock,
	}
}

// instrumentedAuthenticator 为认证调用加上耗时与结果指标。
type instrumentedAuthenticator struct {
	jwtIssuerHash string
	delegate      AuthenticatorTokenWithHealthCheck
	clock         clock.PassiveClock
}

// AuthenticateToken 调用底层认证器并记录耗时。
//
// 中文说明：(false, nil) 表示 token 不属于该 issuer，属于正常的"未命中"，
// 不应计入失败率，否则多 issuer 场景下的指标会严重失真。
func (a *instrumentedAuthenticator) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	start := a.clock.Now()
	response, ok, err := a.delegate.AuthenticateToken(ctx, token)
	// this only happens when issuer doesn't match the authenticator
	// we don't want to record metrics for this case
	if !ok && err == nil {
		return response, ok, err
	}

	duration := a.clock.Since(start)
	if err != nil {
		recordAuthenticationLatency("failure", a.jwtIssuerHash, duration)
	} else {
		recordAuthenticationLatency("success", a.jwtIssuerHash, duration)
	}
	return response, ok, err
}

// HealthCheck 直接委托，健康检查不单独计量。
func (a *instrumentedAuthenticator) HealthCheck() error {
	return a.delegate.HealthCheck()
}
