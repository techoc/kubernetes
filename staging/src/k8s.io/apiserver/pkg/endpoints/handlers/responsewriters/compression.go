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
	"compress/gzip"
	"net/http"
	"strings"
	"sync"

	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
)

const (
	// gzipContentEncodingLevel is set to 1 which uses least CPU compared to higher levels, yet offers
	// similar compression ratios (off by at most 1.5x, but typically within 1.1x-1.3x). For further details see -
	// https://github.com/kubernetes/kubernetes/issues/112296
	gzipContentEncodingLevel = 1
)

// NewGzipWriterPoolOrDie returns a sync.Pool of gzip.Writers configured with gzipContentEncodingLevel.
//
// 复用 gzip.Writer 可以避免每次响应都分配压缩器与 32KB 窗口。
// 池中的 writer 由使用方负责 Reset/归还；级别固定为 1，因此池中实例可互换。
func NewGzipWriterPoolOrDie() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			gw, err := gzip.NewWriterLevel(nil, gzipContentEncodingLevel)
			if err != nil {
				// 级别是编译期常量，走到这里说明常量被改坏了，直接 panic 比静默降级更安全。
				panic(err)
			}
			return gw
		},
	}
}

// responseContentEncodingSupported returns a supported client-requested content encoding for the
// provided request. It will return the empty string if no supported content encoding was
// found or if the APIResponseCompression feature gate is disabled.
func responseContentEncodingSupported(req *http.Request) string {
	return ContentEncodingSupported(req, features.APIResponseCompression)
}

// ContentEncodingSupported returns a supported client-requested content encoding for the
// provided request. It will return the empty string if no supported content encoding was
// found or if the provided feature gate is disabled.
//
// 手工解析 Accept-Encoding 而不是用标准库协商，是为了让 API 响应压缩受
// 特性门控控制，并且只认 gzip 一种编码——服务端没有实现其它算法。
func ContentEncodingSupported(req *http.Request, feature featuregate.Feature) string {
	encoding := req.Header.Get("Accept-Encoding")
	if len(encoding) == 0 {
		return ""
	}
	// 门控关闭时即使客户端支持也不压缩，保证回退路径可预测。
	if !utilfeature.DefaultFeatureGate.Enabled(feature) {
		return ""
	}
	// 逐段切分逗号分隔的编码列表，忽略 q 值等参数，只看编码名。
	for len(encoding) > 0 {
		var token string
		if next := strings.Index(encoding, ","); next != -1 {
			token = encoding[:next]
			encoding = encoding[next+1:]
		} else {
			token = encoding
			encoding = ""
		}
		if strings.TrimSpace(token) == "gzip" {
			return "gzip"
		}
	}
	return ""
}
