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

// encode is the basis for the Encode mode, with no JSONMarshalerTranscoder
// configured. TranscodeFromJSON uses this directly rather than Encode to avoid an initialization
// cycle between the two. Everything else should use one of the exported EncModes.
//
// 这组选项的目标是让相同语义的值产生相同字节（确定性编码），并尽量与
// JSON 序列化器的行为对齐，使 JSON 与 CBOR 转换可逆。
var encode = EncMode{
	delegate: func() cbor.UserBufferEncMode {
		encode, err := cbor.EncOptions{
			// Map keys need to be sorted to have deterministic output, and this is the order
			// defined in RFC 8949 4.2.1 "Core Deterministic Encoding Requirements".
			Sort: cbor.SortBytewiseLexical,

			// CBOR supports distinct types for IEEE-754 float16, float32, and float64. Store
			// floats in the smallest width that preserves value so that equivalent float32 and
			// float64 values encode to identical bytes, as they do in a JSON
			// encoding. Satisfies one of the "Core Deterministic Encoding Requirements".
			ShortestFloat: cbor.ShortestFloat16,

			// Error on attempt to encode NaN and infinite values. This is what the JSON
			// serializer does.
			NaNConvert: cbor.NaNConvertReject,
			InfConvert: cbor.InfConvertReject,

			// Error on attempt to encode math/big.Int values, which can't be faithfully
			// roundtripped through Unstructured in general (the dynamic numeric types allowed
			// in Unstructured are limited to float64 and int64).
			BigIntConvert: cbor.BigIntConvertReject,

			// MarshalJSON for time.Time writes RFC3339 with nanos.
			Time: cbor.TimeRFC3339Nano,

			// The decoder must be able to accept RFC3339 strings with or without tag 0 (e.g. by
			// the end of time.Time -> JSON -> Unstructured -> CBOR, the CBOR encoder has no
			// reliable way of knowing that a particular string originated from serializing a
			// time.Time), so producing tag 0 has little use.
			TimeTag: cbor.EncTagNone,

			// Indefinite-length items have multiple encodings and aren't being used anyway, so
			// disable to avoid an opportunity for nondeterminism.
			IndefLength: cbor.IndefLengthForbidden,

			// Preserve distinction between nil and empty for slices and maps.
			NilContainers: cbor.NilContainerAsNull,

			// OK to produce tags.
			TagsMd: cbor.TagsAllowed,

			// Use the same definition of "empty" as encoding/json.
			OmitEmpty: cbor.OmitEmptyGoValue,

			// The CBOR types text string and byte string are structurally equivalent, with the
			// semantic difference that a text string whose content is an invalid UTF-8 sequence
			// is itself invalid. We reject all invalid text strings at decode time and do not
			// validate or sanitize all Go strings at encode time. Encoding Go strings to the
			// byte string type is comparable to the existing Protobuf behavior and cheaply
			// ensures that the output is valid CBOR.
			String: cbor.StringToByteString,

			// Encode struct field names to the byte string type rather than the text string
			// type.
			FieldName: cbor.FieldNameToByteString,

			// Marshal Go byte arrays to CBOR arrays of integers (as in JSON) instead of byte
			// strings.
			ByteArray: cbor.ByteArrayToArray,

			// Marshal []byte to CBOR byte string enclosed in tag 22 (expected later base64
			// encoding, https://www.rfc-editor.org/rfc/rfc8949.html#section-3.4.5.2), to
			// interoperate with the existing JSON behavior. This indicates to the decoder that,
			// when decoding into a string (or unstructured), the resulting value should be the
			// base64 encoding of the original bytes. No base64 encoding or decoding needs to be
			// performed for []byte-to-CBOR-to-[]byte roundtrips.
			ByteSliceLaterFormat: cbor.ByteSliceLaterFormatBase64,

			// Disable default recognition of types implementing encoding.BinaryMarshaler, which
			// is not recognized for JSON encoding.
			BinaryMarshaler: cbor.BinaryMarshalerNone,

			// Unmarshal into types that implement encoding.TextUnmarshaler by passing
			// the contents of a CBOR string to their UnmarshalText method.
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
		// To encode a value of a type that implements json.Marshaler (and does not
		// implement cbor.Marshaler), transcode the result of calling its MarshalJSON method
		// directly to CBOR.
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
