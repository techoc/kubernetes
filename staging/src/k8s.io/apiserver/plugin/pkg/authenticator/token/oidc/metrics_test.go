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
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

const (
	testIssuer      = "testIssuer"
	testAPIServerID = "testAPIServerID"
	testKeySet      = `{"keys":[{"kty":"RSA","use":"sig","kid":"test"}]}`
)

// TestRecordAuthenticationLatency 验证认证延迟直方图只在缓存未命中时记录，
// 且 result 标签区分 success/failure；未匹配 issuer 的请求不产生任何样本。
// 期望值直接以 Prometheus 文本格式给出，任何标签或桶的改动都会让测试失败。
func TestRecordAuthenticationLatency(t *testing.T) {
	tests := []struct {
		name            string
		authenticator   AuthenticatorTokenWithHealthCheck
		generateMetrics func()
		expectedValue   string
	}{
		{
			name:          "success",
			authenticator: &dummyAuthenticator{response: &authenticator.Response{}, ok: true},
			expectedValue: `
        # HELP apiserver_authentication_jwt_authenticator_latency_seconds [ALPHA] Latency of jwt authentication operations in seconds. This is the time spent authenticating a token for cache miss only (i.e. when the token is not found in the cache).
        # TYPE apiserver_authentication_jwt_authenticator_latency_seconds histogram
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="0.001"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="0.005"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="0.01"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="0.025"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="0.05"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="0.1"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="0.25"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="0.5"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="1"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="2.5"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="5"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="10"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success",le="+Inf"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_sum{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success"} 1e-09
        apiserver_authentication_jwt_authenticator_latency_seconds_count{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="success"} 1
		`,
		},
		{
			name:          "error",
			authenticator: &dummyAuthenticator{response: &authenticator.Response{}, ok: false, err: fmt.Errorf("error")},
			expectedValue: `
        # HELP apiserver_authentication_jwt_authenticator_latency_seconds [ALPHA] Latency of jwt authentication operations in seconds. This is the time spent authenticating a token for cache miss only (i.e. when the token is not found in the cache).
        # TYPE apiserver_authentication_jwt_authenticator_latency_seconds histogram
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="0.001"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="0.005"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="0.01"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="0.025"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="0.05"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="0.1"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="0.25"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="0.5"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="1"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="2.5"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="5"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="10"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_bucket{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure",le="+Inf"} 1
        apiserver_authentication_jwt_authenticator_latency_seconds_sum{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure"} 1e-09
        apiserver_authentication_jwt_authenticator_latency_seconds_count{jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad",result="failure"} 1
		`,
		},
		{
			name:          "no metrics when issuer doesn't match",
			authenticator: &dummyAuthenticator{response: &authenticator.Response{}, ok: false, err: nil},
			expectedValue: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jwtAuthenticatorLatencyMetric.Reset()
			RegisterMetrics()

			a := newInstrumentedAuthenticatorWithClock(testIssuer, tt.authenticator, dummyClock{})
			_, _, _ = a.AuthenticateToken(context.Background(), "token")

			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tt.expectedValue), "apiserver_authentication_jwt_authenticator_latency_seconds"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// dummyAuthenticator 返回预设的认证结果，用于驱动指标记录路径。
type dummyAuthenticator struct {
	response *authenticator.Response
	ok       bool
	err      error
}

// AuthenticateToken 原样返回预设结果。
func (a *dummyAuthenticator) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	return a.response, a.ok, a.err
}

// HealthCheck 不应在指标测试中被调用。
func (a *dummyAuthenticator) HealthCheck() error {
	panic("should not be called")
}

// dummyClock 提供确定的计时结果（Since 固定返回 1ns），
// 使直方图 sum 可被精确断言。
type dummyClock struct {
}

// Now 返回真实当前时间（仅用于满足接口）。
func (d dummyClock) Now() time.Time {
	return time.Now()
}

// Since 固定返回 1ns，保证期望文本中的 sum=1e-09 稳定。
func (d dummyClock) Since(t time.Time) time.Duration {
	return time.Duration(1)
}

// TestRecordJWKSFetchKeySetSuccess 验证 JWKS 成功获取后以
// apiserver_id_hash/jwt_issuer_hash/hash 三个哈希标签暴露最后一份 key set。
func TestRecordJWKSFetchKeySetSuccess(t *testing.T) {
	expectedValue := `
	# HELP apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info [ALPHA] Information about the last JWKS fetched by the JWT authenticator with hash as label, split by api server identity and jwt issuer.
	# TYPE apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info gauge
	apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:d132d414ef2da3d863abd7bf0165c00403ef1d3510faf8fdf1d7cf335c888e53",jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad"} 1
	`

	metrics := []string{
		namespace + "_" + subsystem + "_jwt_authenticator_jwks_fetch_last_key_set_info",
	}

	ResetMetrics()
	RegisterMetrics()

	recordJWKSFetchKeySetSuccess(testIssuer, testAPIServerID, testKeySet)

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
		t.Fatal(err)
	}
}

// TestRecordJWKSFetchKeySetFailure 验证失败路径仍会更新“最后获取时间戳”指标，
// 即使没有 key set 信息可暴露——运维可据此发现持续失败的 JWKS 拉取。
func TestRecordJWKSFetchKeySetFailure(t *testing.T) {
	ResetMetrics()
	RegisterMetrics()

	recordJWKSFetchKeySetFailure(testIssuer, testAPIServerID)

	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, m := range metrics {
		if m.GetName() == namespace+"_"+subsystem+"_jwt_authenticator_jwks_fetch_last_timestamp_seconds" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Expected jwt_authenticator_jwks_fetch_last_timestamp_seconds metric to be present")
	}
}

// TestJWKSHashCollector_MultipleAuthenticators 验证多个 issuer 的信息可同时存在，
// 标签组合不同即视为不同的时间序列。
func TestJWKSHashCollector_MultipleAuthenticators(t *testing.T) {
	expectedValue := `
	# HELP apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info [ALPHA] Information about the last JWKS fetched by the JWT authenticator with hash as label, split by api server identity and jwt issuer.
	# TYPE apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info gauge
	apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:d132d414ef2da3d863abd7bf0165c00403ef1d3510faf8fdf1d7cf335c888e53",jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad"} 1
	apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:1b5293c65ffc96e13f2d6fefae782190aec8cfb89957a3109419d4f47b80e3e8",jwt_issuer_hash="sha256:f10ab1bafaa1a8628d0fae41ee554948912b01957e4a2db1698fc1c3e4451682"} 1
	`

	metrics := []string{
		namespace + "_" + subsystem + "_jwt_authenticator_jwks_fetch_last_key_set_info",
	}

	ResetMetrics()
	RegisterMetrics()

	recordJWKSFetchKeySetSuccess(testIssuer, testAPIServerID, testKeySet)

	secondIssuer := "https://another-issuer.example.com"
	secondKeySet := `{"keys":[{"kty":"EC","use":"sig","kid":"test2"}]}`
	recordJWKSFetchKeySetSuccess(secondIssuer, testAPIServerID, secondKeySet)

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
		t.Fatal(err)
	}
}

// TestJWKSHashCollector_UpdateExistingHash 验证同一 issuer 重新拉取后
// 只保留最新 key set 的哈希，旧哈希不残留（info 型指标必须替换而非累加）。
func TestJWKSHashCollector_UpdateExistingHash(t *testing.T) {
	expectedValue := `
	# HELP apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info [ALPHA] Information about the last JWKS fetched by the JWT authenticator with hash as label, split by api server identity and jwt issuer.
	# TYPE apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info gauge
	apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:1b5293c65ffc96e13f2d6fefae782190aec8cfb89957a3109419d4f47b80e3e8",jwt_issuer_hash="sha256:29b34beedc55b972f2428f21bc588f9d38e5e8f7a7af825486e7bb4fd9caa2ad"} 1
	`

	metrics := []string{
		namespace + "_" + subsystem + "_jwt_authenticator_jwks_fetch_last_key_set_info",
	}

	ResetMetrics()
	RegisterMetrics()

	recordJWKSFetchKeySetSuccess(testIssuer, testAPIServerID, testKeySet)

	// Update with new JWKS - should replace old hash with new one
	updatedKeySet := `{"keys":[{"kty":"EC","use":"sig","kid":"test2"}]}`
	recordJWKSFetchKeySetSuccess(testIssuer, testAPIServerID, updatedKeySet)

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
		t.Fatal(err)
	}
}

// TestJWKSHashCollector_DeleteHash 验证删除某个 issuer 的指标后不会影响其他 issuer，
// 防止认证器被移除时留下永久残留的时间序列。
func TestJWKSHashCollector_DeleteHash(t *testing.T) {
	ResetMetrics()
	RegisterMetrics()

	recordJWKSFetchKeySetSuccess(testIssuer, testAPIServerID, testKeySet)
	secondIssuer := "https://another-issuer.example.com"
	secondKeySet := `{"keys":[{"kty":"EC","use":"sig","kid":"test2"}]}`
	recordJWKSFetchKeySetSuccess(secondIssuer, testAPIServerID, secondKeySet)

	// Delete first authenticator's metrics and verify only second authenticator's hash remains
	DeleteJWKSFetchMetrics(testIssuer, testAPIServerID)

	expectedValue := `
	# HELP apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info [ALPHA] Information about the last JWKS fetched by the JWT authenticator with hash as label, split by api server identity and jwt issuer.
	# TYPE apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info gauge
	apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info{apiserver_id_hash="sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37",hash="sha256:1b5293c65ffc96e13f2d6fefae782190aec8cfb89957a3109419d4f47b80e3e8",jwt_issuer_hash="sha256:f10ab1bafaa1a8628d0fae41ee554948912b01957e4a2db1698fc1c3e4451682"} 1
	`

	metrics := []string{
		namespace + "_" + subsystem + "_jwt_authenticator_jwks_fetch_last_key_set_info",
	}

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
		t.Fatal(err)
	}
}
