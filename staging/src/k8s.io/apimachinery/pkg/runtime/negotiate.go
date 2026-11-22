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

// NegotiateError is returned when a ClientNegotiator is unable to locate
// a serializer for the requested operation.
// 中文说明：Stream 区分流式解码（如 watch）与一次性解码，便于调用方给出
// 更精确的错误信息。
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
	// TODO: `pretty=1` is handled in NegotiateOutputMediaType, consider moving it to this method
	// if client negotiators truly need to use it
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

// Decoder 与 Encoder 的选择逻辑一致，只是把对象转换到 decode 指定的版本。
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

// StreamDecoder 额外要求所选类型支持流式解码；否则报 Stream=true 的错误，
// 因为这是调用方请求了流式读取（例如 watch）而该格式无法满足。
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

// NewClientNegotiator will attempt to retrieve the appropriate encoder, decoder, or
// stream decoder for a given content type. Does not perform any conversion, but will
// encode the object to the desired group, version, and kind. Use when creating a client.
//
// 中文说明：encode 被设为 gv 而 decode 留空——客户端发出的对象总是目标版本，
// 而收到的对象由服务端的 TypeMeta 决定如何解释。
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
