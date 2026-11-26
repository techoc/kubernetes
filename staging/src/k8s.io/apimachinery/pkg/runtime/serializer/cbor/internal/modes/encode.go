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
	"io"

	"github.com/fxamacker/cbor/v2"
)

// encode 是 Encode 模式的基础，未配置 JSONMarshalerTranscoder。
// TranscodeFromJSON 直接使用它而非 Encode，以避免两者之间的初始化循环。
// 其它场景应使用导出的 EncModes。
//
// 这组选项的目标是让相同语义的值产生相同字节（确定性编码），并尽量与
// JSON 序列化器的行为对齐，使 JSON 与 CBOR 转换可逆。
var encode = EncMode{
	delegate: func() cbor.UserBufferEncMode {
		encode, err := cbor.EncOptions{
			// map 键必须排序才能得到确定性输出，排序规则遵循
			// RFC 8949 4.2.1 "Core Deterministic Encoding Requirements"。
			Sort: cbor.SortBytewiseLexical,

			// CBOR 为 IEEE-754 float16、float32 和 float64 分别定义了类型。
			// 用能保持值精度的最小宽度存储浮点数，使等价的 float32 与 float64
			// 编码出相同字节，与 JSON 编码一致。这是 "Core Deterministic
			// Encoding Requirements" 的一项要求。
			ShortestFloat: cbor.ShortestFloat16,

			// 尝试编码 NaN 或无穷值时报错，与 JSON 序列化器保持一致。
			NaNConvert: cbor.NaNConvertReject,
			InfConvert: cbor.InfConvertReject,

			// 尝试编码 math/big.Int 时报错：Unstructured 中可用的动态数值类型
			// 只有 float64 和 int64，无法无损往返。
			BigIntConvert: cbor.BigIntConvertReject,

			// time.Time 的 MarshalJSON 会写出带纳秒的 RFC3339 字符串。
			Time: cbor.TimeRFC3339Nano,

			// 解码器必须同时接受带 tag 0 和不带 tag 0 的 RFC3339 字符串
			// （经过 time.Time -> JSON -> Unstructured -> CBOR 后，编码器已无法
			// 可靠判断某个字符串是否来自 time.Time 序列化），因此生成 tag 0 意义不大。
			TimeTag: cbor.EncTagNone,

			// 不定长项有多种编码方式且这里用不到，禁用以避免引入不确定性。
			IndefLength: cbor.IndefLengthForbidden,

			// 保留 slice 和 map 的 nil 与空值之分。
			NilContainers: cbor.NilContainerAsNull,

			// 允许生成 tag。
			TagsMd: cbor.TagsAllowed,

			// 沿用 encoding/json 对“空”的定义。
			OmitEmpty: cbor.OmitEmptyGoValue,

			// CBOR 的文本串与字节串在结构上等价，区别在于内容非合法 UTF-8 的
			// 文本串本身无效。解码时已拒绝所有非法文本串，编码时不再逐一校验或
			// 清洗 Go 字符串。把 Go 字符串编码为字节串与现有 Protobuf 行为类似，
			// 也能低成本地保证输出是合法 CBOR。
			String: cbor.StringToByteString,

			// 结构体字段名编码为字节串类型，而非文本串类型。
			FieldName: cbor.FieldNameToByteString,

			// Go 字节数组编码为 CBOR 整数数组（与 JSON 一致），而非字节串。
			ByteArray: cbor.ByteArrayToArray,

			// 把 []byte 编码为包在 tag 22 中的 CBOR 字节串（期望后续 base64 编码，
			// https://www.rfc-editor.org/rfc/rfc8949.html#section-3.4.5.2），以兼容
			// 现有 JSON 行为。它告诉解码器：解码到 string（或 unstructured）时，
			// 结果应是原始字节的 base64 编码。[]byte -> CBOR -> []byte 的往返
			// 无需任何 base64 编解码。
			ByteSliceLaterFormat: cbor.ByteSliceLaterFormatBase64,

			// 禁用对 encoding.BinaryMarshaler 实现类型的默认识别，JSON 编码不支持它。
			BinaryMarshaler: cbor.BinaryMarshalerNone,

			// 对实现 encoding.TextUnmarshaler 的类型，把 CBOR 字符串的内容
			// 传给其 UnmarshalText 方法。
			TextMarshaler: cbor.TextMarshalerTextString,
		}.UserBufferEncMode()
		if err != nil {
			panic(err)
		}
		return encode
	}(),
	deterministic: true,
}

// Encode 是默认的确定性编码模式：在 encode 的基础上增加 JSONMarshaler 的
// 转码支持，使只实现了 MarshalJSON 的类型也能产出 CBOR。
var Encode = EncMode{
	delegate: func() cbor.UserBufferEncMode {
		opts := encode.options()
		// 对实现 json.Marshaler（但未实现 cbor.Marshaler）的类型，调用其
		// MarshalJSON 方法，并把结果直接转码为 CBOR。
		opts.JSONMarshalerTranscoder = TranscodeFunc(TranscodeFromJSON)
		em, err := opts.UserBufferEncMode()
		if err != nil {
			panic(err)
		}
		return em
	}(),
	deterministic: true,
}

// EncodeNondeterministic 与 Encode 相同，但 map key 采用快速打乱而非排序。
// 仅用于不要求字节稳定性的场景（如测试或性能敏感路径），不能用于生成
// 需要缓存或比较的序列化输出。
var EncodeNondeterministic = EncMode{
	delegate: func() cbor.UserBufferEncMode {
		opts := Encode.options()
		opts.Sort = cbor.SortFastShuffle
		em, err := opts.UserBufferEncMode()
		if err != nil {
			panic(err)
		}
		return em
	}(),
	deterministic: false,
}

// EncMode 封装底层的 CBOR 编码模式，并显式记录是否保证确定性输出。
// deterministic 不能从 delegate 推断，故单独保存供调用方查询。
type EncMode struct {
	delegate      cbor.UserBufferEncMode
	deterministic bool
}

// options 返回底层选项的副本，供派生模式修改而不会影响当前模式。
func (em EncMode) options() cbor.EncOptions {
	return em.delegate.EncOptions()
}

// IsDeterministic 报告该模式的输出是否与 map 迭代顺序无关。
func (em EncMode) IsDeterministic() bool {
	return em.deterministic
}

// MarshalTo 编码到 writer。若目标本身就是内部 buffer，则直接写入以省去
// 一次拷贝；否则从对象池借 buffer，编码完成后再复制到 w。
func (em EncMode) MarshalTo(v interface{}, w io.Writer) error {
	if buf, ok := w.(*buffer); ok {
		return em.delegate.MarshalToBuffer(v, &buf.Buffer)
	}

	buf := buffers.Get()
	defer buffers.Put(buf)
	if err := em.delegate.MarshalToBuffer(v, &buf.Buffer); err != nil {
		return err
	}

	if _, err := io.Copy(w, buf); err != nil {
		return err
	}

	return nil
}

// Marshal 返回独立的字节切片。对象池中的 buffer 会被复用，必须 clone，
// 否则调用方拿到的数据可能在后续编码中被覆盖。
func (em EncMode) Marshal(v interface{}) ([]byte, error) {
	buf := buffers.Get()
	defer buffers.Put(buf)

	if err := em.MarshalTo(v, &buf.Buffer); err != nil {
		return nil, err
	}

	clone := make([]byte, buf.Len())
	copy(clone, buf.Bytes())

	return clone, nil
}
