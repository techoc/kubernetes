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

package feature

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apiserver/pkg/storage"
	testingclock "k8s.io/utils/clock/testing"
)

// mockEndpointVersion 描述单个 endpoint 的版本或错误。
type mockEndpointVersion struct {
	Endpoint string
	Version  string
	Error    error
}

// MockEtcdClient is a mock implementation of the EtcdClientInterface interface.
type MockEtcdClient struct {
	EndpointVersion []mockEndpointVersion
}

// getEndpoints 汇总所有 mock endpoint 名。
func (m MockEtcdClient) getEndpoints() []string {
	var endpoints []string
	for _, ev := range m.EndpointVersion {
		endpoints = append(endpoints, ev.Endpoint)
	}
	return endpoints
}

// getVersion 按 endpoint 返回预置版本或错误。
func (m MockEtcdClient) getVersion(endpoint string) (string, error) {
	for _, ev := range m.EndpointVersion {
		if ev.Endpoint == endpoint {
			return ev.Version, ev.Error
		}
	}
	// Never should happen, unless tests having a problem.
	return "", fmt.Errorf("No version found")
}

// Endpoints 实现 client 接口。
func (m *MockEtcdClient) Endpoints() []string {
	return m.getEndpoints()
}

// Status returns a mock status response.
func (m *MockEtcdClient) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	version, err := m.getVersion(endpoint)
	if err != nil {
		return nil, err
	}
	// Return a mock status response
	return &clientv3.StatusResponse{
		Version: version,
	}, nil
}

// TestSupports 验证未探测时的默认语义：
// RequestWatchProgress 默认不支持，RangeStream 默认支持，未知特性记录错误并返回 false。
func TestSupports(t *testing.T) {
	tests := []struct {
		testName       string
		featureName    string
		expectedResult bool
	}{
		{
			testName:    "Disabled - with unknown feature",
			featureName: "some unknown feature",
		},
		{
			testName:    "Disabled - with empty feature",
			featureName: "",
		},
		{
			testName:       "Disabled - default",
			featureName:    storage.RequestWatchProgress,
			expectedResult: false,
		},
		{
			testName:       "Enabled - RangeStream defaults to supported until proven otherwise",
			featureName:    storage.RangeStream,
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			var testFeatureSupportChecker FeatureSupportChecker = newDefaultFeatureSupportChecker()

			supported := testFeatureSupportChecker.Supports(tt.featureName)

			assert.Equal(t, tt.expectedResult, supported)
		})
	}
}

// TestSupportsRequestWatchProgress 以多轮探测模拟多个 etcd client 先后初始化，
// 验证“一旦有 endpoint 不支持就永久关闭、不能被后续 client 重新打开”的粘滞语义。
func TestSupportsRequestWatchProgress(t *testing.T) {
	type testCase struct {
		endpointsVersion []mockEndpointVersion
		expectedResult   bool
		expectedError    error
	}
	tests := []struct {
		testName string
		rounds   []testCase
	}{
		{
			testName: "Disabled - default disabled",
			rounds:   []testCase{{expectedResult: false}},
		},
		{
			testName: "Enabled - supported versions bound",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.4.31", Endpoint: "localhost:2390"}},
					expectedResult: true,
				},
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.5.13", Endpoint: "localhost:2391"}},
					expectedResult: true,
				},
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.6.0", Endpoint: "localhost:2392"}},
					expectedResult: true}},
		},
		{
			testName: "Disabled - supported versions bound, 3.4.30",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.4.30", Endpoint: "localhost:2390"}},
					expectedResult: false}},
		},
		{
			testName: "Disabled - supported versions bound, 3.5.0",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.5.0", Endpoint: "localhost:2390"}},
					expectedResult: false}},
		},
		{
			testName: "Disabled - supported versions bound, 3.5.12",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.5.12", Endpoint: "localhost:2390"}},
					expectedResult: false}},
		},
		{
			testName: "Disabled - disables if called with one client doesn't support it",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.5.13", Endpoint: "localhost:2390"},
					{Version: "3.5.10", Endpoint: "localhost:2391"}},
					expectedResult: false}},
		},
		{
			testName: "Disabled - disables if called with all client doesn't support it",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.5.9", Endpoint: "localhost:2390"},
					{Version: "3.5.10", Endpoint: "localhost:2391"}},
					expectedResult: false}},
		},
		{
			testName: "Enabled - if provided client has at least one endpoint that supports it and no client that doesn't",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.4.31", Endpoint: "localhost:2390"},
					{Version: "3.5.13", Endpoint: "localhost:2391"},
					{Version: "3.5.14", Endpoint: "localhost:2392"},
					{Version: "3.6.0", Endpoint: "localhost:2393"}},
					expectedResult: true}},
		},
		{
			testName: "Disabled - cannot be re-enabled",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.4.0", Endpoint: "localhost:2390"},
					{Version: "3.4.1", Endpoint: "localhost:2391"}},
					expectedResult: false},
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.6.0", Endpoint: "localhost:2392"}},
					expectedResult: false}},
		},
		{
			testName: "Enabled - one client supports it and later disabled it with second client",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.6.0", Endpoint: "localhost:2390"},
					{Version: "3.5.14", Endpoint: "localhost:2391"}},
					expectedResult: true},
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.4.0", Endpoint: "localhost:2392"}},
					expectedResult: false}},
		},
		{
			testName: "Disabled - malformed version would disable the supported cluster and can not be re-enabled again",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.6.0", Endpoint: "localhost:2390"}},
					expectedResult: true,
				},
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.4.--aaa", Endpoint: "localhost:2392"}},
					expectedResult: false},
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.5.13", Endpoint: "localhost:2393"}},
					expectedResult: false}},
		},
		{
			testName: "Enabled - error on first client, enabled success on second client",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.6.0", Endpoint: "localhost:2390", Error: fmt.Errorf("some error")}},
					expectedResult: false,
					expectedError:  fmt.Errorf("failed checking etcd version, endpoint: %q: %w", "localhost:2390", fmt.Errorf("some error")),
				},
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.5.14", Endpoint: "localhost:2391"}},
					expectedResult: true}},
		},
		{
			testName: "Disabled - enabled success on first client, error on second client, disabled success on third client",
			rounds: []testCase{
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.6.0", Endpoint: "localhost:2390"}},
					expectedResult: true,
				},
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.6.0", Endpoint: "localhost:2391", Error: fmt.Errorf("some error")}},
					expectedResult: true,
					expectedError:  fmt.Errorf("failed checking etcd version, endpoint: %q: %w", "localhost:2391", fmt.Errorf("some error")),
				},
				{endpointsVersion: []mockEndpointVersion{
					{Version: "3.5.10", Endpoint: "localhost:2392"}},
					expectedResult: false}},
		},
		{
			testName: "Disabled - client doesn't have any endpoints",
			rounds:   []testCase{{endpointsVersion: []mockEndpointVersion{}, expectedResult: false}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			var testFeatureSupportChecker = newDefaultFeatureSupportChecker()
			for _, round := range tt.rounds {
				// Mock Etcd client
				mockClient := &MockEtcdClient{EndpointVersion: round.endpointsVersion}
				ctx := context.Background()

				for _, ep := range mockClient.Endpoints() {
					err := testFeatureSupportChecker.clientSupportsRequestWatchProgress(ctx, mockClient, ep)
					assert.Equal(t, round.expectedError, err)
				}

				supported := testFeatureSupportChecker.Supports(storage.RequestWatchProgress)
				assert.Equal(t, round.expectedResult, supported)
			}
		})
	}
}

// TestMarkUnsupported 验证 RangeStream 被标记不支持后，经过 recheck 间隔才重新报告支持。
func TestMarkUnsupported(t *testing.T) {
	f := newDefaultFeatureSupportChecker()
	fakeClock := testingclock.NewFakeClock(time.Now())
	f.clock = fakeClock

	assert.True(t, f.Supports(storage.RangeStream))

	f.MarkUnsupported(storage.RangeStream)
	assert.False(t, f.Supports(storage.RangeStream))

	fakeClock.Step(rangeStreamRecheckInterval - time.Second)
	assert.False(t, f.Supports(storage.RangeStream))

	fakeClock.Step(2 * time.Second)
	assert.True(t, f.Supports(storage.RangeStream))
}
