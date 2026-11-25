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
	// gzipContentEncodingLevel 设为 1：相比更高级别最省 CPU，压缩率却相近
	// （最多偏差 1.5 倍，通常落在 1.1x-1.3x 之间）。详见 -
	// https://github.com/kubernetes/kubernetes/issues/112296
	gzipContentEncodingLevel = 1
)

// NewGzipWriterPoolOrDie 返回一个按 gzipContentEncodingLevel 配置的 gzip.Writer 的 sync.Pool。
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

// responseContentEncodingSupported 返回所提供请求中客户端请求且受支持的 content
// encoding；若未找到受支持的编码，或 APIResponseCompression 特性门控被关闭，
// 则返回空字符串。
func responseContentEncodingSupported(req *http.Request) string {
	return ContentEncodingSupported(req, features.APIResponseCompression)
}

// ContentEncodingSupported 返回所提供请求中客户端请求且受支持的 content
// encoding；若未找到受支持的编码，或所提供的特性门控被关闭，则返回空字符串。
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
