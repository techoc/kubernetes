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

// RawExtension 暂不实现 value.UnstructuredConverter，因为 ToUnstructured 的接口签名无法返回转换错误。
// 当内容类型无法识别，或原始字节与声明的内容类型不匹配时，转换可能失败；若忽略错误会丢失重要信息。
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
			// JSON/CBOR 识别器使用的前缀：四种空白字符，以及对象、数组、字符串、数字、true、false 和 null。
			return ContentTypeJSON
		}
	}
	return ""
}

// 把 JSON 原始字节保存到 Raw 字段，并清除已有 Object，确保后续访问不会混用旧对象与新内容。
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

// 把 CBOR 数据保存在 Raw 中并清除已缓存的 Object，延迟到具体使用方再决定如何解码。
func (re *RawExtension) UnmarshalCBOR(in []byte) error {
	if re == nil {
		return errors.New("runtime.RawExtension: UnmarshalCBOR on nil pointer")
	}
	if !bytes.Equal(in, cborNull) {
		if !bytes.HasPrefix(in, cborSelfDescribed) {
			// 自描述 CBOR 标签不会改变其包裹的数据项含义，但可作为识别 CBOR 数据的魔数。
			// CBOR RecognizingDecoder 利用该标签的编码形式判断输入格式。
			re.Raw = append(re.Raw[:0], cborSelfDescribed...)
		}
		re.Raw = append(re.Raw, in...)
	}
	return nil
}

// 若 Object 非空则先将对象编码为 JSON；否则返回 Raw 中保留的原始 JSON。没有可用内容时输出 JSON null。
func (re RawExtension) MarshalJSON() ([]byte, error) {
	if re.Raw == nil {
		// TODO：此逻辑用于兼容 JSONPrinter 和 YAMLPrinter 的历史行为：它们可能对任意版本化对象调用 json.Marshal，
		// 即使对象尚未注册到 Scheme。后续应调整 pkg/kubectl/resource#AsVersionedObjects 与 kubectl get，
		// 确保未注册对象也能被正确转换为预期输出形式。
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

// 优先把 Object 编码为 CBOR；若只有 Raw，则返回原始 CBOR 字节，避免不必要的解码再编码。
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
