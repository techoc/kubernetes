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

package cel

import celconfig "k8s.io/apiserver/pkg/apis/cel"

const (
	// DefaultMaxRequestSizeBytes 是 apiserver 会接受的最大请求体大小。
	// 与 apiserver 的请求体上限保持一致，CEL 的 cost budget 与实际可接收
	// 请求规模挂钩，避免出现“请求能进、表达式必然超预算”的死角。
	DefaultMaxRequestSizeBytes = celconfig.MaxRequestSizeBytes

	// MaxDurationSizeJSON：OpenAPI duration 字符串遵循 RFC 3339 第 5.6 节，详见 maxDatetimeSizeJSON 的注释；
	// duration 的 JSON 表示上限含两侧引号，用于 cost 估算时按最坏长度计。
	MaxDurationSizeJSON = 32
	// MaxDatetimeSizeJSON
	// OpenAPI datetime 字符串遵循 RFC 3339 第 5.6 节，最长形式为
	// 9999-12-31T23:59:59.999999999Z，长度 30；再加 2 以容纳引号。
	MaxDatetimeSizeJSON = 32
	// MinDurationSizeJSON
	// Golang 允许把字符串 0 解析为 duration，加上两侧引号共 3 个字符。
	MinDurationSizeJSON = 3
	// JSONDateSize 是作为 JSON 对象一部分序列化的日期长度
	// RFC 3339 日期需要 YYYY-MM-DD，再加 2 以容纳引号
	JSONDateSize = 12
	// MinDatetimeSizeJSON 是 RFC 3339 格式 datetime 的最小长度
	// RFC 3339 datetime 需要完整日期（YYYY-MM-DD）与时间（HH:MM:SS），再加 3：
	// 两侧引号加分隔日期与时间的 T。
	MinDatetimeSizeJSON = 21
	// MinStringSize 是字面量 "" 的长度
	MinStringSize = 2
	// MinBoolSize 是字面量 true 的长度
	MinBoolSize = 4
	// MinNumberSize 是字面量 0 的长度
	MinNumberSize = 1

	// MaxFormatSize 是允许的 format 字符串最大长度；
	// 限制 format 参数长度，防止通过超长正则等格式串放大编译/匹配成本。
	MaxFormatSize          = 64
	MaxNameFormatRegexSize = 128
)
