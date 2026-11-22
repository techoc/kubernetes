/*
Copyright 2019 The Kubernetes Authors.

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

package validation

import (
	"fmt"
	"strings"
	"sync"

	"golang.org/x/sync/singleflight"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// supportedVersionedFormats 记录 CRD schema 支持的格式及其引入版本。
// 在不支持该格式的版本中，CRD schema 里的格式会被忽略。
var supportedVersionedFormats = []versionedFormats{
	{
		introducedVersion: version.MajorMinor(1, 0),
		formats: sets.New(
			"bsonobjectid", // bson 对象 ID
			"uri",          // 由 Golang net/url.ParseRequestURI 解析的 URI
			"email",        // 由 Golang net/mail.ParseAddress 解析的邮箱地址
			"hostname",     // 合法的 Internet 主机名，定义见 RFC 1034 第 3.1 节
			"ipv4",         // 由 Golang net.ParseIP 解析的 IPv4 地址
			"ipv6",         // 由 Golang net.ParseIP 解析的 IPv6 地址
			"cidr",         // 由 Golang net.ParseCIDR 解析的 CIDR
			"mac",          // 由 Golang net.ParseMAC 解析的 MAC 地址
			"uuid",         // 允许大写的 UUID，正则 (?i)^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$
			"uuid3",        // 允许大写的 UUID3，正则 (?i)^[0-9a-f]{8}-?[0-9a-f]{4}-?3[0-9a-f]{3}-?[0-9a-f]{4}-?[0-9a-f]{12}$
			"uuid4",        // 允许大写的 UUID4，正则 (?i)^[0-9a-f]{8}-?[0-9a-f]{4}-?4[0-9a-f]{3}-?[89ab][0-9a-f]{3}-?[0-9a-f]{12}$
			"uuid5",        // 允许大写的 UUID6，正则 (?i)^[0-9a-f]{8}-?[0-9a-f]{4}-?5[0-9a-f]{3}-?[89ab][0-9a-f]{3}-?[0-9a-f]{12}$
			"isbn",         // ISBN10 或 ISBN13 字符串，如 "0321751043" 或 "978-0321751041"
			"isbn10",       // ISBN10 字符串，如 "0321751043"
			"isbn13",       // ISBN13 字符串，如 "978-0321751041"
			"creditcard",   // 信用卡号，正则 ^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\\d{3})\\d{11})$，允许混入任意非数字字符
			"ssn",          // 美国社会安全号，正则 ^\\d{3}[- ]?\\d{2}[- ]?\\d{4}$
			"hexcolor",     // 十六进制颜色码，如 "#FFFFFF"，正则 ^#?([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$
			"rgbcolor",     // RGB 颜色码，如 "rgb(255,255,2559"
			"byte",         // base64 编码的二进制数据
			"password",     // 任意字符串
			"date",         // 日期字符串，如 "2006-01-02"，定义见 RFC3339 full-date
			"duration",     // 时长字符串，如 "22 ns"，由 Golang time.ParseDuration 解析或兼容 Scala duration 格式
			"datetime",     // 日期时间字符串，如 "2014-12-15T19:30:20.000Z"，定义见 RFC3339 date-time
		),
	},
	{
		introducedVersion: version.MajorMinor(1, 34),
		formats: sets.New(
			// 短的 Kubernetes 名称，最长 63 个字符，由字母数字和短横线组成，
			// 且不能以短横线开头或结尾。
			// k8s-short-name 几乎符合 DNS 标签（RFC 1123）的定义，
			// 区别是不允许大写字母。
			"k8s-short-name",

			// 长的 Kubernetes 名称，最长 253 个字符，由点分隔的段组成；
			// 每段只使用字母数字和短横线（不能以短横线开头/结尾）。
			// k8s-long-name 几乎符合 DNS 子域（RFC 1123）的定义，
			// 区别是不允许大写字母，且构成子域的各 DNS 标签没有 63 字符的长度上限。
			"k8s-long-name",
		),
	},
}

// StripUnsupportedFormatsPostProcess 将不支持的格式置为空字符串。
// 仅支持所有已知 Kubernetes 版本都支持的格式。
// Deprecated: 请改用 StripUnsupportedFormatsPostProcessorForVersion。
func StripUnsupportedFormatsPostProcess(s *spec.Schema) error {
	return legacyPostProcessor(s)
}

// StripUnsupportedFormatsPostProcessorForVersion 按给定兼容版本确定支持的格式，
// 并将不支持的格式置为空字符串。
func StripUnsupportedFormatsPostProcessorForVersion(compatibilityVersion *version.Version) func(s *spec.Schema) error {
	return func(s *spec.Schema) error {
		if len(s.Format) == 0 {
			return nil
		}

		schemaType := ""
		if len(s.Type) == 1 {
			schemaType = s.Type[0]
		}
		switch schemaType {
		case "", "string":
			// go-openapi 的默认格式名归一化处理（去掉连字符）
			normalized := strings.ReplaceAll(s.Format, "-", "")
			if !supportedFormatsAtVersion(compatibilityVersion).supported.Has(normalized) {
				s.Format = ""
			}
		case "integer":
			if s.Format != "int32" && s.Format != "int64" {
				s.Format = ""
			}
		case "number":
			if s.Format != "float" && s.Format != "double" {
				s.Format = ""
			}
		default:
			// 其他类型不支持 Format
			s.Format = ""
		}

		return nil
	}
}

// GetUnrecognizedFormats 返回给定 schema 中无法识别的格式列表。
// 它使用与 StripUnsupportedFormatsPostProcessorForVersion 相同的判定依据。
func GetUnrecognizedFormats(schema *spec.Schema, compatibilityVersion *version.Version) []string {
	var unrecognizedFormats []string
	if len(schema.Format) == 0 {
		return unrecognizedFormats
	}

	schemaType := ""
	if len(schema.Type) == 1 {
		schemaType = schema.Type[0]
	}
	switch schemaType {
	case "", "string":
		// go-openapi 的默认格式名归一化处理（去掉连字符）
		normalized := strings.ReplaceAll(schema.Format, "-", "")
		if !supportedFormatsAtVersion(compatibilityVersion).supported.Has(normalized) {
			unrecognizedFormats = append(unrecognizedFormats, schema.Format)
		}
	case "integer":
		if schema.Format != "int32" && schema.Format != "int64" {
			unrecognizedFormats = append(unrecognizedFormats, schema.Format)
		}
	case "number":
		if schema.Format != "float" && schema.Format != "double" {
			unrecognizedFormats = append(unrecognizedFormats, schema.Format)
		}
	default:
		// 其他类型不支持 Format
		unrecognizedFormats = append(unrecognizedFormats, schema.Format)
	}

	return unrecognizedFormats
}

// versionedFormats 描述一组格式及其被引入的 Kubernetes 版本。
type versionedFormats struct {
	introducedVersion *version.Version
	formats           sets.Set[string]
}

// supportedFormats 描述某个兼容版本下支持的格式集合。
type supportedFormats struct {
	compatibilityVersion *version.Version
	// supported 是在该 Kubernetes 兼容版本下被校验支持的格式集合。
	supported sets.Set[string]
}

// cacheFormatSets 控制是否缓存按版本计算的格式集合；测试中可关闭以避免污染。
var cacheFormatSets = true

// supportedFormatsAtVersion 返回（并缓存）指定兼容版本支持的格式集合。
func supportedFormatsAtVersion(ver *version.Version) *supportedFormats {
	key := fmt.Sprintf("%d.%d", ver.Major(), ver.Minor())
	var entry interface{}
	if entry, ok := baseEnvs.Load(key); ok {
		return entry.(*supportedFormats)
	}
	entry, _, _ = baseEnvsSingleflight.Do(key, func() (interface{}, error) {
		entry := newFormatsAtVersion(ver, supportedVersionedFormats)
		// 仅在允许缓存时写入 sync.Map，测试可借此观察每次重新计算的结果
		if cacheFormatSets {
			baseEnvs.Store(key, entry)
		}
		return entry, nil
	})
	return entry.(*supportedFormats)
}

// newFormatsAtVersion 计算指定版本下所有已引入格式的归一化集合。
func newFormatsAtVersion(ver *version.Version, versionedFormats []versionedFormats) *supportedFormats {
	result := &supportedFormats{
		compatibilityVersion: ver,
		supported:            sets.New[string](),
	}
	for _, vf := range versionedFormats {
		// go-openapi 的默认格式名归一化处理（去掉连字符）
		normalized := sets.Set[string]{}
		for format := range vf.formats {
			normalized.Insert(strings.ReplaceAll(format, "-", ""))
		}

		// 只有达到引入版本的格式才纳入支持集合
		if ver.AtLeast(vf.introducedVersion) {
			result.supported = result.supported.Union(normalized)

		}
	}
	return result
}

var (
	// baseEnvs 缓存按 "major.minor" 键索引的格式集合；
	// baseEnvsSingleflight 合并并发的格式集合计算，避免重复构建。
	baseEnvs             = sync.Map{}
	baseEnvsSingleflight = &singleflight.Group{}
)

// legacyPostProcessor 是固定使用 1.0 兼容版本的格式清理器。
var legacyPostProcessor = StripUnsupportedFormatsPostProcessorForVersion(version.MajorMinor(1, 0))
