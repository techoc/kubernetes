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

package runtime

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ClientNegotiator 无法为请求操作找到合适序列化器时返回 NegotiateError。
// Stream 用于区分流式解码（例如 watch）与普通一次性解码，帮助调用方定位格式不受支持的原因。
type NegotiateError struct {
	ContentType string
	Stream      bool
}

func (e NegotiateError) Error() string {
	if e.Stream {
		return fmt.Sprintf("no stream serializers registered for %s", e.ContentType)
	}
	return fmt.Sprintf("no serializers registered for %s", e.ContentType)
}

// clientNegotiator 按客户端请求的 Content-Type 选择 serializer，并在编码/解码
// 时把对象转换到固定目标版本。encode/decode 的 GroupVersioner 在构造时确定。
type clientNegotiator struct {
	serializer     NegotiatedSerializer
	encode, decode GroupVersioner
}

func (n *clientNegotiator) Encoder(contentType string, params map[string]string) (Encoder, error) {
	// TODO：pretty=1 当前由 NegotiateOutputMediaType 处理；如果客户端协商器也需要该参数，可考虑将逻辑迁移到此处。
	mediaTypes := n.serializer.SupportedMediaTypes()
	info, ok := SerializerInfoForMediaType(mediaTypes, contentType)
	if !ok {
		// 调用方未指定类型且只有一个可用类型时，退回默认类型以保持宽容；
		// 指定了未知类型则必须报错，避免静默使用错误的编码。
		if len(contentType) != 0 || len(mediaTypes) == 0 {
			return nil, NegotiateError{ContentType: contentType}
		}
		info = mediaTypes[0]
	}
	return n.serializer.EncoderForVersion(info.Serializer, n.encode), nil
}

// 根据 Content-Type 选择解码器，并将解码结果转换到协商器配置的目标版本。格式选择规则与 Encoder 一致。
func (n *clientNegotiator) Decoder(contentType string, params map[string]string) (Decoder, error) {
	mediaTypes := n.serializer.SupportedMediaTypes()
	info, ok := SerializerInfoForMediaType(mediaTypes, contentType)
	if !ok {
		if len(contentType) != 0 || len(mediaTypes) == 0 {
			return nil, NegotiateError{ContentType: contentType}
		}
		info = mediaTypes[0]
	}
	return n.serializer.DecoderToVersion(info.Serializer, n.decode), nil
}

// 选择支持流式解码的序列化器，并同时返回解码器、流式 Serializer 和 Framer。若格式不存在或未提供 StreamSerializer，则返回 Stream 标记的协商错误。
func (n *clientNegotiator) StreamDecoder(contentType string, params map[string]string) (Decoder, Serializer, Framer, error) {
	mediaTypes := n.serializer.SupportedMediaTypes()
	info, ok := SerializerInfoForMediaType(mediaTypes, contentType)
	if !ok {
		if len(contentType) != 0 || len(mediaTypes) == 0 {
			return nil, nil, nil, NegotiateError{ContentType: contentType, Stream: true}
		}
		info = mediaTypes[0]
	}
	if info.StreamSerializer == nil {
		return nil, nil, nil, NegotiateError{ContentType: info.MediaType, Stream: true}
	}
	return n.serializer.DecoderToVersion(info.Serializer, n.decode), info.StreamSerializer.Serializer, info.StreamSerializer.Framer, nil
}

// 创建客户端序列化协商器。它根据请求的 Content-Type 选择编码器、解码器或流式解码器，并将客户端发出的对象编码到指定 GroupVersion；该构造函数不负责业务对象转换本身。
func NewClientNegotiator(serializer NegotiatedSerializer, gv schema.GroupVersion) ClientNegotiator {
	return &clientNegotiator{
		serializer: serializer,
		encode:     gv,
	}
}

// simpleNegotiatedSerializer 只暴露单一媒体类型的 serializer，且不做版本转换；
// 用于只需要一个固定编码的简单场景（如测试或内部工具）。
type simpleNegotiatedSerializer struct {
	info SerializerInfo
}

func NewSimpleNegotiatedSerializer(info SerializerInfo) NegotiatedSerializer {
	return &simpleNegotiatedSerializer{info: info}
}

func (n *simpleNegotiatedSerializer) SupportedMediaTypes() []SerializerInfo {
	return []SerializerInfo{n.info}
}

func (n *simpleNegotiatedSerializer) EncoderForVersion(e Encoder, _ GroupVersioner) Encoder {
	return e
}

func (n *simpleNegotiatedSerializer) DecoderToVersion(d Decoder, _gv GroupVersioner) Decoder {
	return d
}
