/*
Copyright 2014 The Kubernetes Authors.

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

package runtime

import (
	"bytes"
	"errors"
	"fmt"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/apimachinery/pkg/util/json"
)

// RawExtension intentionally avoids implementing value.UnstructuredConverter for now because the
// signature of ToUnstructured does not allow returning an error value in cases where the conversion
// is not possible (content type is unrecognized or bytes don't match content type).
// rawToUnstructured 根据内容类型把 RawExtension 的原始字节解码成通用结构。
// 内容类型未知时报错而不是猜测，避免把 JSON 当 CBOR 解析产生误导性错误。
func rawToUnstructured(raw []byte, contentType string) (interface{}, error) {
	switch contentType {
	case ContentTypeJSON:
		var u interface{}
		if err := json.Unmarshal(raw, &u); err != nil {
			return nil, fmt.Errorf("failed to parse RawExtension bytes as JSON: %w", err)
		}
		return u, nil
	case ContentTypeCBOR:
		var u interface{}
		if err := cbor.Unmarshal(raw, &u); err != nil {
			return nil, fmt.Errorf("failed to parse RawExtension bytes as CBOR: %w", err)
		}
		return u, nil
	default:
		return nil, fmt.Errorf("cannot convert RawExtension with unrecognized content type to unstructured")
	}
}

// guessContentType 通过嗅探前缀判断 Raw 的格式。CBOR 使用自描述标签
// 0xd9d9f7 作为 magic number；JSON 则根据首个非空白字符判断。
func (re RawExtension) guessContentType() string {
	switch {
	case bytes.HasPrefix(re.Raw, cborSelfDescribed):
		return ContentTypeCBOR
	case len(re.Raw) > 0:
		switch re.Raw[0] {
		case '\t', '\r', '\n', ' ', '{', '[', 'n', 't', 'f', '"', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// Prefixes for the four whitespace characters, objects, arrays, strings, numbers, true, false, and null.
			return ContentTypeJSON
		}
	}
	return ""
}

// UnmarshalJSON 保存原始 JSON 字节而不解析，保留原样是为了让 RawExtension
// 可以承载任意对象并将解析推迟到知道目标类型时。null 表示没有值，Raw 保持
// 不变。append(re.Raw[0:0], ...) 复用底层数组，避免重复分配。
func (re *RawExtension) UnmarshalJSON(in []byte) error {
	if re == nil {
		return errors.New("runtime.RawExtension: UnmarshalJSON on nil pointer")
	}
	if bytes.Equal(in, []byte("null")) {
		return nil
	}
	re.Raw = append(re.Raw[0:0], in...)
	return nil
}

var (
	cborNull          = []byte{0xf6}
	cborSelfDescribed = []byte{0xd9, 0xd9, 0xf7}
)

// UnmarshalCBOR 与 UnmarshalJSON 类似，但会先补上自描述 CBOR 标签，使后续
// 编码/识别逻辑无需区分来源。CBOR null（0xf6）同样表示空值。
func (re *RawExtension) UnmarshalCBOR(in []byte) error {
	if re == nil {
		return errors.New("runtime.RawExtension: UnmarshalCBOR on nil pointer")
	}
	if !bytes.Equal(in, cborNull) {
		if !bytes.HasPrefix(in, cborSelfDescribed) {
			// The self-described CBOR tag doesn't change the interpretation of the data
			// item it encloses, but it is useful as a magic number. Its encoding is
			// also what is used to implement the CBOR RecognizingDecoder.
			re.Raw = append(re.Raw[:0], cborSelfDescribed...)
		}
		re.Raw = append(re.Raw, in...)
	}
	return nil
}

// MarshalJSON may get called on pointers or values, so implement MarshalJSON on value.
// http://stackoverflow.com/questions/21390979/custom-marshaljson-never-gets-called-in-go
//
// 若 Raw 已经是 JSON，直接原样输出以保证字节稳定性；若为 CBOR，则先转成
// 通用结构再编码为 JSON，实现跨格式兼容。
func (re RawExtension) MarshalJSON() ([]byte, error) {
	if re.Raw == nil {
		// TODO: this is to support legacy behavior of JSONPrinter and YAMLPrinter, which
		// expect to call json.Marshal on arbitrary versioned objects (even those not in
		// the scheme). pkg/kubectl/resource#AsVersionedObjects and its interaction with
		// kubectl get on objects not in the scheme needs to be updated to ensure that the
		// objects that are not part of the scheme are correctly put into the right form.
		if re.Object != nil {
			return json.Marshal(re.Object)
		}
		return []byte("null"), nil
	}

	contentType := re.guessContentType()
	if contentType == ContentTypeJSON {
		return re.Raw, nil
	}

	u, err := rawToUnstructured(re.Raw, contentType)
	if err != nil {
		return nil, err
	}
	return json.Marshal(u)
}

// MarshalCBOR 与 MarshalJSON 对称：优先原样输出 CBOR，否则把 Raw 转为
// 通用结构后用 CBOR 编码。Raw 为 nil 时退回到 Object 字段或 null。
func (re RawExtension) MarshalCBOR() ([]byte, error) {
	if re.Raw == nil {
		if re.Object != nil {
			return cbor.Marshal(re.Object)
		}
		return cbor.Marshal(nil)
	}

	contentType := re.guessContentType()
	if contentType == ContentTypeCBOR {
		return re.Raw, nil
	}

	u, err := rawToUnstructured(re.Raw, contentType)
	if err != nil {
		return nil, err
	}
	return cbor.Marshal(u)
}
