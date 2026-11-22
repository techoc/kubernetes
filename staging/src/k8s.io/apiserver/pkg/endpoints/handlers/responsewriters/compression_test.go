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

package responsewriters

import (
	"net/http"
	"testing"

	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

// TestContentEncodingSupported 验证 Accept-Encoding 的协商结果：
// 只有特性门控开启且客户端接受 gzip 时才返回 "gzip"，其余情况一律返回空串（不压缩）。
func TestContentEncodingSupported(t *testing.T) {
	scenarios := []struct {
		name              string
		acceptEncoding    string
		featureGate       featuregate.Feature
		enableFeatureGate bool
		expectedEncoding  string
	}{
		{
			name:              "no Accept-Encoding header",
			acceptEncoding:    "",
			featureGate:       features.APIResponseCompression,
			enableFeatureGate: true,
			expectedEncoding:  "",
		},
		{
			name:              "gzip accepted, compression enabled",
			acceptEncoding:    "gzip",
			featureGate:       features.APIResponseCompression,
			enableFeatureGate: true,
			expectedEncoding:  "gzip",
		},
		{
			name:             "gzip accepted, compression disabled",
			acceptEncoding:   "gzip",
			featureGate:      features.APIResponseCompression,
			expectedEncoding: "",
		},
		{
			// 逗号分隔、含多个编码时仍应识别出 gzip。
			name:              "multiple encodings with gzip",
			acceptEncoding:    "deflate, gzip",
			featureGate:       features.APIResponseCompression,
			enableFeatureGate: true,
			expectedEncoding:  "gzip",
		},
		{
			// 只声明不支持的编码时不压缩，而不是回退到 deflate。
			name:              "unsupported encoding only",
			acceptEncoding:    "deflate",
			featureGate:       features.APIResponseCompression,
			enableFeatureGate: true,
			expectedEncoding:  "",
		},
		{
			name:              "gzip with whitespace",
			acceptEncoding:    " gzip ",
			featureGate:       features.APIResponseCompression,
			enableFeatureGate: true,
			expectedEncoding:  "gzip",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, scenario.featureGate, scenario.enableFeatureGate)

			req, _ := http.NewRequest(http.MethodGet, "/", nil)
			if scenario.acceptEncoding != "" {
				req.Header.Set("Accept-Encoding", scenario.acceptEncoding)
			}

			got := ContentEncodingSupported(req, scenario.featureGate)
			if got != scenario.expectedEncoding {
				t.Errorf("contentEncodingSupported returned: %q, want: %q", got, scenario.expectedEncoding)
			}
		})
	}
}

// TestResponseContentEncodingSupported 验证内部辅助函数同样受 APIResponseCompression 门控约束。
func TestResponseContentEncodingSupported(t *testing.T) {
	scenarios := []struct {
		name                          string
		acceptEncoding                string
		apiResponseCompressionEnabled bool
		expectedEncoding              string
	}{
		{
			name:                          "gzip accepted, APIResponseCompression enabled",
			acceptEncoding:                "gzip",
			apiResponseCompressionEnabled: true,
			expectedEncoding:              "gzip",
		},
		{
			name:                          "gzip accepted, APIResponseCompression disabled",
			acceptEncoding:                "gzip",
			apiResponseCompressionEnabled: false,
			expectedEncoding:              "",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.APIResponseCompression, scenario.apiResponseCompressionEnabled)

			req, _ := http.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Accept-Encoding", scenario.acceptEncoding)

			got := responseContentEncodingSupported(req)
			if got != scenario.expectedEncoding {
				t.Errorf("ResponseContentEncodingSupported returned: %q, want: %q", got, scenario.expectedEncoding)
			}
		})
	}
}
