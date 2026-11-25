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
	// OpenAPI datetime strings follow RFC 3339, section 5.6, and the longest possible
	// such string is 9999-12-31T23:59:59.999999999Z, which has length 30 - we add 2
	// to allow for quotation marks
	MaxDatetimeSizeJSON = 32
	// MinDurationSizeJSON
	// Golang allows a string of 0 to be parsed as a duration, so that plus 2 to account for
	// quotation marks makes 3
	MinDurationSizeJSON = 3
	// JSONDateSize is the size of a date serialized as part of a JSON object
	// RFC 3339 dates require YYYY-MM-DD, and then we add 2 to allow for quotation marks
	JSONDateSize = 12
	// MinDatetimeSizeJSON is the minimal length of a datetime formatted as RFC 3339
	// RFC 3339 datetimes require a full date (YYYY-MM-DD) and full time (HH:MM:SS), and we add 3 for
	// quotation marks like always in addition to the capital T that separates the date and time
	MinDatetimeSizeJSON = 21
	// MinStringSize is the size of literal ""
	MinStringSize = 2
	// MinBoolSize is the length of literal true
	MinBoolSize = 4
	// MinNumberSize is the length of literal 0
	MinNumberSize = 1

	// MaxFormatSize 是允许的 format 字符串最大长度；
	// 限制 format 参数长度，防止通过超长正则等格式串放大编译/匹配成本。
	MaxFormatSize          = 64
	MaxNameFormatRegexSize = 128
)
