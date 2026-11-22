/*
Copyright 2022 The Kubernetes Authors.

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

package aggregated

import (
	"net/http"
	"net/http/httptest"

	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/endpoints/request"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

const discoveryPath = "/apis"
const jsonAccept = "application/json"
const protobufAccept = "application/vnd.kubernetes.protobuf"
const aggregatedV2Beta1AcceptSuffix = ";g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList"
const aggregatedAcceptSuffix = ";g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"

const aggregatedV2Beta1JSONAccept = jsonAccept + aggregatedV2Beta1AcceptSuffix
const aggregatedV2Beta1ProtoAccept = protobufAccept + aggregatedV2Beta1AcceptSuffix
const aggregatedJSONAccept = jsonAccept + aggregatedAcceptSuffix
const aggregatedProtoAccept = protobufAccept + aggregatedAcceptSuffix
const aggregatedNoPeerJSONAccept = jsonAccept + aggregatedAcceptSuffix + ";profile=nopeer"
const aggregatedNoPeerProtoAccept = protobufAccept + aggregatedAcceptSuffix + ";profile=nopeer"

// fetchPath 用给定 Accept 头调用 handler 并返回响应体。
//
// 中文说明：用假 handler 的固定字符串作为响应体，测试只关心路由选择，
// 不关心真实聚合发现的序列化内容。
func fetchPath(handler http.Handler, path, accept string) string {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(request.MethodGet, discoveryPath, nil)

	// Ask for JSON response
	req.Header.Set("Accept", accept)

	handler.ServeHTTP(w, req)
	return w.Body.String()
}

// fakeHTTPHandler 返回固定字符串，用于标识实际被选中的下游 handler。
type fakeHTTPHandler struct {
	data string
}

// ServeHTTP 直接把 data 写入响应体，忽略请求内容。
func (f fakeHTTPHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	io.WriteString(resp, f.data)
}

// TestAggregationEnabled 验证基于 Accept 头的聚合发现路由与 profile 语义。
//
// 中文说明：三个下游 handler 的响应体不同，因此仅凭返回内容就能判断请求被
// 路由到哪一层；用例同时覆盖畸形/空 Accept、多类型优先级与 peer/nopeer。
func TestAggregationEnabled(t *testing.T) {
	// 固定仿真版本，避免测试结果随默认版本变化。
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.34"))
	unaggregated := fakeHTTPHandler{data: "unaggregated"}
	aggregated := fakeHTTPHandler{data: "nopeer-aggregated"}
	peerAggregated := fakeHTTPHandler{data: "peer-aggregated"}
	wrapped := WrapAggregatedDiscoveryToHandler(unaggregated, aggregated, peerAggregated)

	testCases := []struct {
		accept               string
		expected             string
		enablePeerAggregated bool
	}{
		{
			// Misconstructed/incorrect accept headers should be passed to the unaggregated handler to return an error
			accept:   "application/json;foo=bar",
			expected: "unaggregated",
		}, {
			// Empty accept headers are valid and should be handled by the unaggregated handler
			accept:   "",
			expected: "unaggregated",
		}, {
			accept:   aggregatedV2Beta1JSONAccept,
			expected: "nopeer-aggregated",
		}, {
			accept:   aggregatedV2Beta1ProtoAccept,
			expected: "nopeer-aggregated",
		}, {
			accept:   aggregatedJSONAccept,
			expected: "nopeer-aggregated",
		}, {
			accept:   aggregatedProtoAccept,
			expected: "nopeer-aggregated",
		}, {
			accept:   jsonAccept,
			expected: "unaggregated",
		}, {
			accept:   protobufAccept,
			expected: "unaggregated",
		}, {
			// Server should return the first accepted type
			accept:   aggregatedJSONAccept + "," + jsonAccept,
			expected: "nopeer-aggregated",
		}, {
			// Server should return the first accepted type
			accept:   aggregatedProtoAccept + "," + protobufAccept,
			expected: "nopeer-aggregated",
		},
		// Peer Agg discovery cases.
		// profile is not set (should default to peer-aggregated)
		{
			accept:               aggregatedJSONAccept,
			expected:             "peer-aggregated",
			enablePeerAggregated: true,
		}, {
			accept:               aggregatedProtoAccept,
			expected:             "peer-aggregated",
			enablePeerAggregated: true,
		},
		// profile=nopeer (should return no-peer)
		{
			accept:               aggregatedNoPeerJSONAccept,
			expected:             "nopeer-aggregated",
			enablePeerAggregated: true,
		}, {
			accept:               aggregatedNoPeerProtoAccept,
			expected:             "nopeer-aggregated",
			enablePeerAggregated: true,
		},
		// profile is set to something other than no-peer (should default to peer-aggregated)
		{
			accept:               aggregatedJSONAccept + ";profile=foo",
			expected:             "peer-aggregated",
			enablePeerAggregated: true,
		},
	}

	for _, tc := range testCases {
		// v2beta1 相关用例需要临时关闭"移除 beta 类型"门控，否则请求会走新版本分支。
		if tc.accept == aggregatedV2Beta1JSONAccept || tc.accept == aggregatedV2Beta1ProtoAccept {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, genericfeatures.AggregatedDiscoveryRemoveBetaType, false)
		}
		// peer 聚合需要额外的门控；未开启时 profile 参数被忽略。
		if tc.enablePeerAggregated {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, genericfeatures.UnknownVersionInteroperabilityProxy, true)
		}
		body := fetchPath(wrapped, discoveryPath, tc.accept)
		assert.Equal(t, tc.expected, body)
	}
}
