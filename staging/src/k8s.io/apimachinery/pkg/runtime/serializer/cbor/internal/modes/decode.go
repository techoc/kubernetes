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

package modes

import (
	"reflect"

	"github.com/fxamacker/cbor/v2"
)

// simpleValues 注册允许的 CBOR simple value：只放行 false/true/null，
// 其余（包括规范中保留的 24-31）一律拒绝，避免非 JSON 可表达的语义混入。
var simpleValues *cbor.SimpleValueRegistry = func() *cbor.SimpleValueRegistry {
	var opts []func(*cbor.SimpleValueRegistry) error
	for sv := 0; sv <= 255; sv++ {
		// 拒绝简单值 0-19、23 和 32-255：24-31 是 CBOR 规范保留值，
		// 视为格式错误。这里只接受 false（20）、true（21）和 null（22）。
		switch sv {
		case 20: // false
		case 21: // true
		case 22: // null
		case 24, 25, 26, 27, 28, 29, 30, 31: // reserved
		default:
			opts = append(opts, cbor.WithRejectedSimpleValue(cbor.SimpleValue(sv)))
		}
	}
	simpleValues, err := cbor.NewSimpleValueRegistryFromDefaults(opts...)
	if err != nil {
		panic(err)
	}
	return simpleValues
}()

// decode 是 Decode 模式的基础，未配置 JSONUnmarshalerTranscoder。
// TranscodeToJSON 直接使用它而非 Decode，以避免两者之间的初始化循环。
// 其它场景应使用导出的 DecModes。
//
// 中文补充：这里的每个选项都以“与 JSON 序列化器行为一致”为目标；差异点（如重复键
// 直接报错）在注释中单独说明。
var decode cbor.DecMode = func() cbor.DecMode {
	decode, err := cbor.DecOptions{
		// 含重复键的 map 虽然格式正确，但按 CBOR 规范无效，必须拒绝。
		// 与 JSON 序列化器不同，这类输入会被直接拒绝，而不是作为严格解码错误上报。
		DupMapKey: cbor.DupMapKeyEnforcedAPF,

		// 为与 JSON 行为一致，RFC3339 字符串无论有无 tag 都应能解码为 time.Time；
		// 若存在 tag，其编号必须合法。
		TimeTag: cbor.DecTagOptional,

		// MaxNestedLevels 与 JSON 实现中的取值保持一致。
		MaxNestedLevels: 10000,

		// MaxArrayElements 取 cbor 库允许的最大值。依赖库的初始格式检查和
		// API 请求大小限制，防止解码时预分配超大切片。
		MaxArrayElements: 2147483647,

		// MaxMapPairs 限定 map 中允许的键值对上限。取值足够大，实践中 API Server
		// 的解码器总会先触发请求体大小限制，而不会触到这里的上限。
		MaxMapPairs: 2097152,

		// 本序列化器不会生成不定长序列，但其它实现可能会。
		IndefLength: cbor.IndefLengthAllowed,

		// 接受包含 CBOR tag 的输入。
		TagsMd: cbor.TagsAllowed,

		// 将类型 0（无符号整数）解码为 int64。
		// TODO: IntDecConvertSignedOrFail 在溢出时报错，而 JSON 会尝试回退到 float64。
		IntDec: cbor.IntDecConvertSignedOrFail,

		// 禁止生成 map[cbor.ByteString]interface{}，这种类型不适合解码到 interface{}。
		MapKeyByteString: cbor.MapKeyByteStringForbidden,

		// map 键在目标结构体中找不到对应字段时报错。
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,

		// 目标是 interface{} 时，把 map 解码为具体的 map[string]interface{} 类型。
		DefaultMapType: reflect.TypeOf(map[string]interface{}(nil)),

		// 内容不是合法 UTF-8 的 CBOR 文本串格式正确但违反 CBOR 规范，应拒绝。
		// 编码器有责任保证生成的文本串都是合法 UTF-8；未经验证的字符串
		// 可使用字节串主类型编码。
		UTF8: cbor.UTF8RejectInvalid,

		// map 键与结构体字段绝不做大小写不敏感的匹配。
		FieldNameMatching: cbor.FieldNameMatchingCaseSensitive,

		// 把 CBOR 字节串解码到 interface{} 时，生成具体的 string 值。
		DefaultByteStringType: reflect.TypeOf(""),

		// 允许把 CBOR 字节串解码到 string 类型的目标值。若字节串被包在
		// "expected later encoding" tag 中
		// （https://www.rfc-editor.org/rfc/rfc8949.html#section-3.4.5.2），
		// 则按该 tag 指定的文本编码（如 base64）处理字节串内容。
		ByteStringToString: cbor.ByteStringToStringAllowedWithExpectedLaterEncoding,

		// 允许 CBOR 字节串作为 map 键与结构体字段匹配。
		FieldNameByteString: cbor.FieldNameByteStringAllowed,

		// 把无法识别的 tag 解码到 interface{} 时，返回 tag 的内容，
		// 而不是默认的 cbor.Tag（表示 number 与 content 的键值对）。
		UnrecognizedTagToAny: cbor.UnrecognizedTagContentToAny,

		// 把 time tag 解码到 interface{} 时，生成包含 RFC 3339 时间戳的字符串。
		TimeTagToAny: cbor.TimeTagToRFC3339Nano,

		// 为与 JSON 行为一致，RFC 3339 格式的字符串可解码为 time.Time。
		ByteStringToTime: cbor.ByteStringToTimeAllowed,

		// 拒绝 NaN 与无穷浮点值：它们在 JSON 中没有表示（RFC 8259 第 6 节）。
		NaN: cbor.NaNDecodeForbidden,
		Inf: cbor.InfDecodeForbidden,

		// 把字节串反序列化到 []byte 时，默认认为其中是 base64 编码的字节，
		// 除非 "expected later encoding" tag 明确表示相反含义。这与 JSON 文本
		// 反序列化到 []byte 的行为一致。
		ByteStringExpectedFormat: cbor.ByteStringExpectedBase64,

		// 拒绝任意精度整数 tag：允许出现在 Unstructured 中的类型无法无损地表示它们。
		BignumTag: cbor.BignumTagForbidden,

		// 只接受 true、false 和 null 三种简单值，其余一律拒绝。
		SimpleValues: simpleValues,

		// 禁用对 encoding.BinaryUnmarshaler 实现类型的默认识别，JSON 解码不支持它。
		BinaryUnmarshaler: cbor.BinaryUnmarshalerNone,

		// 对实现 encoding.TextMarshaler 的类型，调用其 MarshalText 方法，
		// 并把结果编码为 CBOR 文本串。
		TextUnmarshaler: cbor.TextUnmarshalerTextString,
	}.DecMode()
	if err != nil {
		panic(err)
	}
	return decode
}()

var Decode cbor.DecMode = func() cbor.DecMode {
	opts := decode.DecOptions()
	// 解码到实现 json.Unmarshaler（但未实现 cbor.Unmarshaler）的类型时，
	// 先把输入转码为 JSON，再交给该值的 UnmarshalJSON 方法。
	// 中文补充：这让大量只实现了 UnmarshalJSON 的既有类型无需改动即可支持 CBOR。
	opts.JSONUnmarshalerTranscoder = TranscodeFunc(TranscodeToJSON)
	dm, err := opts.DecMode()
	if err != nil {
		panic(err)
	}
	return dm
}()

// DecodeLax 由 Decode 派生，但对输入中的未知字段不报错。
// 中文补充：用于非严格解码路径；严格解码会在遇到未知字段时报错，以支持字段拼写错误的提示。
var DecodeLax cbor.DecMode = func() cbor.DecMode {
	opts := Decode.DecOptions()
	// 清除“未知字段报错”这一位，其余严格性设置保持不变。
	opts.ExtraReturnErrors &^= cbor.ExtraDecErrorUnknownField // clear bit
	dm, err := opts.DecMode()
	if err != nil {
		panic(err)
	}
	return dm
}()
