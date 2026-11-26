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

package serializer

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/runtime/serializer/recognizer"
	"k8s.io/apimachinery/pkg/runtime/serializer/versioning"
)

// newSerializersForScheme 构造内置的 JSON/YAML/protobuf 序列化器列表，
// 并追加 options.serializers 中的自定义序列化器。
func newSerializersForScheme(scheme *runtime.Scheme, mf json.MetaFactory, options CodecFactoryOptions) []runtime.SerializerInfo {
	jsonSerializer := json.NewSerializerWithOptions(
		mf, scheme, scheme,
		json.SerializerOptions{Yaml: false, Pretty: false, Strict: options.Strict, StreamingCollectionsEncoding: options.StreamingCollectionsEncodingToJSON},
	)
	jsonSerializerType := runtime.SerializerInfo{
		MediaType:        runtime.ContentTypeJSON,
		MediaTypeType:    "application",
		MediaTypeSubType: "json",
		EncodesAsText:    true,
		Serializer:       jsonSerializer,
		// 严格模式单独实例化，供需要拒绝未知字段的调用方使用。
		StrictSerializer: json.NewSerializerWithOptions(
			mf, scheme, scheme,
			json.SerializerOptions{Yaml: false, Pretty: false, Strict: true, StreamingCollectionsEncoding: options.StreamingCollectionsEncodingToJSON},
		),
		// 流式序列化器用于 watch 等场景，按帧输出。
		StreamSerializer: &runtime.StreamSerializerInfo{
			EncodesAsText: true,
			Serializer:    jsonSerializer,
			Framer:        json.Framer,
		},
	}
	// Pretty 序列化器按需构造，避免不必要的开销。
	if options.Pretty {
		jsonSerializerType.PrettySerializer = json.NewSerializerWithOptions(
			mf, scheme, scheme,
			json.SerializerOptions{Yaml: false, Pretty: true, Strict: options.Strict},
		)
	}

	yamlSerializer := json.NewSerializerWithOptions(
		mf, scheme, scheme,
		json.SerializerOptions{Yaml: true, Pretty: false, Strict: options.Strict},
	)
	strictYAMLSerializer := json.NewSerializerWithOptions(
		mf, scheme, scheme,
		json.SerializerOptions{Yaml: true, Pretty: false, Strict: true},
	)
	protoSerializer := protobuf.NewSerializerWithOptions(scheme, scheme, protobuf.SerializerOptions{
		StreamingCollectionsEncoding: options.StreamingCollectionsEncodingToProtobuf,
	})
	protoRawSerializer := protobuf.NewRawSerializer(scheme, scheme)

	serializers := []runtime.SerializerInfo{
		jsonSerializerType,
		{
			MediaType:        runtime.ContentTypeYAML,
			MediaTypeType:    "application",
			MediaTypeSubType: "yaml",
			EncodesAsText:    true,
			Serializer:       yamlSerializer,
			StrictSerializer: strictYAMLSerializer,
		},
		{
			MediaType:        runtime.ContentTypeProtobuf,
			MediaTypeType:    "application",
			MediaTypeSubType: "vnd.kubernetes.protobuf",
			Serializer:       protoSerializer,
			// 注意：protobuf 不支持严格解码，回退到普通序列化器。
			StrictSerializer: protoSerializer,
			StreamSerializer: &runtime.StreamSerializerInfo{
				Serializer: protoRawSerializer,
				Framer:     protobuf.LengthDelimitedFramer,
			},
		},
	}

	for _, f := range options.serializers {
		serializers = append(serializers, f(scheme, scheme))
	}

	return serializers
}

// CodecFactory 提供按版本和内容类型获取 codec 与 serializer 的方法。
type CodecFactory struct {
	scheme    *runtime.Scheme
	universal runtime.Decoder
	accepts   []runtime.SerializerInfo

	legacySerializer runtime.Serializer
}

// CodecFactoryOptions 保存 CodecFactory 的行为配置。
type CodecFactoryOptions struct {
	// Strict 让所有序列化器运行在严格模式。
	Strict bool
	// Pretty 在非美化序列化器之外额外包含一个美化序列化器。
	Pretty bool

	// StreamingCollectionsEncodingToJSON 控制 JSON 是否对集合使用流式编码。
	StreamingCollectionsEncodingToJSON bool
	// StreamingCollectionsEncodingToProtobuf 控制 protobuf 是否对集合使用流式编码。
	StreamingCollectionsEncodingToProtobuf bool

	// serializers 保存通过 WithSerializer 追加的自定义序列化器构造函数。
	serializers []func(runtime.ObjectCreater, runtime.ObjectTyper) runtime.SerializerInfo
}

// CodecFactoryOptionsMutator 接收 options 指针并修改它。
// 实现该类型的函数可传给 NewCodecFactory() 构造函数。
type CodecFactoryOptionsMutator func(*CodecFactoryOptions)

// EnablePretty 在非美化序列化器之外额外包含一个美化序列化器。
func EnablePretty(options *CodecFactoryOptions) {
	options.Pretty = true
}

// DisablePretty 不包含美化序列化器。
func DisablePretty(options *CodecFactoryOptions) {
	options.Pretty = false
}

// EnableStrict 让所有序列化器运行在严格模式。
func EnableStrict(options *CodecFactoryOptions) {
	options.Strict = true
}

// DisableStrict 关闭严格模式。
func DisableStrict(options *CodecFactoryOptions) {
	options.Strict = false
}

// WithSerializer 在默认序列化器之外额外支持一个自定义序列化器。
func WithSerializer(f func(runtime.ObjectCreater, runtime.ObjectTyper) runtime.SerializerInfo) CodecFactoryOptionsMutator {
	return func(options *CodecFactoryOptions) {
		options.serializers = append(options.serializers, f)
	}
}

// WithStreamingCollectionEncodingToJSON 开启 JSON 集合的流式编码。
func WithStreamingCollectionEncodingToJSON() CodecFactoryOptionsMutator {
	return func(options *CodecFactoryOptions) {
		options.StreamingCollectionsEncodingToJSON = true
	}
}

// WithStreamingCollectionEncodingToProtobuf 开启 protobuf 集合的流式编码。
func WithStreamingCollectionEncodingToProtobuf() CodecFactoryOptionsMutator {
	return func(options *CodecFactoryOptions) {
		options.StreamingCollectionsEncodingToProtobuf = true
	}
}

// NewCodecFactory 提供获取受支持线格式的序列化器，以及定义首选内部/外部版本
// 的转换包装器的方法。未来随着内部版本使用减少，调用方可能改用默认序列化器，
// 只转换内部共享的对象（Status、通用 API machinery）。
//
// 可在构造工厂前传入 Mutator 修改 CodecFactoryOptions。
// 建议显式传入 mutator 而不是依赖默认值。
// 默认启用 Pretty，与此前支持的行为保持一致。
//
// TODO: 是否允许编译进其它编解码器？
// TODO: 改为接受 scheme 接口。
func NewCodecFactory(scheme *runtime.Scheme, mutators ...CodecFactoryOptionsMutator) CodecFactory {
	options := CodecFactoryOptions{Pretty: true}
	for _, fn := range mutators {
		fn(&options)
	}

	serializers := newSerializersForScheme(scheme, json.DefaultMetaFactory, options)
	return newCodecFactory(scheme, serializers)
}

// newCodecFactory 是供测试使用的辅助函数，允许指定不同的 metafactory。
func newCodecFactory(scheme *runtime.Scheme, serializers []runtime.SerializerInfo) CodecFactory {
	decoders := make([]runtime.Decoder, 0, len(serializers))
	var accepts []runtime.SerializerInfo
	alreadyAccepted := make(map[string]struct{})

	var legacySerializer runtime.Serializer
	for _, d := range serializers {
		decoders = append(decoders, d.Serializer)
		// 同一 MediaType 只暴露一次，避免重复协商。
		if _, ok := alreadyAccepted[d.MediaType]; ok {
			continue
		}
		alreadyAccepted[d.MediaType] = struct{}{}

		// 浅拷贝 StreamSerializer，避免调用方修改影响工厂内部状态。
		acceptedSerializerShallowCopy := d
		if d.StreamSerializer != nil {
			cloned := *d.StreamSerializer
			acceptedSerializerShallowCopy.StreamSerializer = &cloned
		}
		accepts = append(accepts, acceptedSerializerShallowCopy)

		if d.MediaType == runtime.ContentTypeJSON {
			legacySerializer = d.Serializer
		}
	}
	// 没有 JSON 序列化器时退化为第一个序列化器，保证 LegacyCodec 可用。
	if legacySerializer == nil {
		legacySerializer = serializers[0].Serializer
	}

	return CodecFactory{
		scheme:    scheme,
		universal: recognizer.NewDecoder(decoders...),

		accepts: accepts,

		legacySerializer: legacySerializer,
	}
}

// WithoutConversion 返回一个即使调用方请求转换也不执行转换的 NegotiatedSerializer。
func (f CodecFactory) WithoutConversion() runtime.NegotiatedSerializer {
	return WithoutConversionCodecFactory{f}
}

// SupportedMediaTypes 返回该工厂支持的 RFC2046 媒体类型。
func (f CodecFactory) SupportedMediaTypes() []runtime.SerializerInfo {
	return f.accepts
}

// LegacyCodec 把输出编码为给定 API 版本，并把任何可识别来源的输入解码为内部形式。
// 返回的 codec 始终以 JSON 编码输出。若类型不在给定版本列表中则返回错误。
//
// 该方法已废弃：客户端与服务端应通过 mime-type 协商序列化器并调用
// CodecForVersions。只需读取数据的调用方应使用 UniversalDecoder()。
//
// TODO: 让此调用只存在于 pkg/api，并用默认版本集合初始化；
// 其它调用方必须直接请求 Codec。
func (f CodecFactory) LegacyCodec(version ...schema.GroupVersion) runtime.Codec {
	return versioning.NewDefaultingCodecForScheme(f.scheme, f.legacySerializer, f.universal, schema.GroupVersions(version), runtime.InternalGroupVersioner)
}

// UniversalDeserializer 把该工厂可识别的任意存储数据转换为满足 runtime.Object
// 的 Go 对象。它不执行转换，也不执行默认值填充。
func (f CodecFactory) UniversalDeserializer() runtime.Decoder {
	return f.universal
}

// UniversalDecoder 返回可解码所有已知格式、所有已知 API 对象的 runtime.Decoder。
// 供只需反序列化磁盘上 API 对象、无需编码的客户端使用。它只解码在 scheme 中
// 注册的组。传入的 GroupVersions 可用于选择返回对象的替代版本——默认使用
// runtime.APIVersionInternal。若指定了版本，未识别的组会以编码时的版本返回
// （不做转换）。该解码器会执行默认值填充。
//
// TODO: 最终应移除 decoder，改为直接处理带版本的对象。
// TODO: 只接受 group versioner。
func (f CodecFactory) UniversalDecoder(versions ...schema.GroupVersion) runtime.Decoder {
	var versioner runtime.GroupVersioner
	if len(versions) == 0 {
		versioner = runtime.InternalGroupVersioner
	} else {
		versioner = schema.GroupVersions(versions)
	}
	return f.CodecForVersions(nil, f.universal, nil, versioner)
}

// CodecForVersions 用给定的序列化器创建 codec。若解码对象的组不在列表中，
// 则默认使用 runtime.APIVersionInternal。若未为某对象的组指定 encode，则该对象
// 不做转换。若 encode 或 decode 为 nil，则不执行转换。
func (f CodecFactory) CodecForVersions(encoder runtime.Encoder, decoder runtime.Decoder, encode runtime.GroupVersioner, decode runtime.GroupVersioner) runtime.Codec {
	// TODO: 这些仅为向后兼容保留，后续应移除。
	if encode == nil {
		encode = runtime.DisabledGroupVersioner
	}
	if decode == nil {
		decode = runtime.InternalGroupVersioner
	}
	return versioning.NewDefaultingCodecForScheme(f.scheme, encoder, decoder, encode, decode)
}

// DecoderToVersion 返回面向给定 group version 的解码器。
func (f CodecFactory) DecoderToVersion(decoder runtime.Decoder, gv runtime.GroupVersioner) runtime.Decoder {
	return f.CodecForVersions(nil, decoder, nil, gv)
}

// EncoderForVersion 返回面向给定 group version 的编码器。
func (f CodecFactory) EncoderForVersion(encoder runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	return f.CodecForVersions(encoder, nil, gv, nil)
}

// WithoutConversionCodecFactory 是显式忽略转换请求的 CodecFactory。
// 该包装器用于代码正在迁移离开转换逻辑的阶段（例如外部客户端），
// 将来 NegotiatedSerializer 签名变更后可能不再需要。
type WithoutConversionCodecFactory struct {
	CodecFactory
}

// EncoderForVersion 返回不做转换、但在序列化时设置对象 group version kind 的编码器。
func (f WithoutConversionCodecFactory) EncoderForVersion(serializer runtime.Encoder, version runtime.GroupVersioner) runtime.Encoder {
	return runtime.WithVersionEncoder{
		Version:     version,
		Encoder:     serializer,
		ObjectTyper: f.CodecFactory.scheme,
	}
}

// DecoderToVersion 返回不做转换的解码器。
func (f WithoutConversionCodecFactory) DecoderToVersion(serializer runtime.Decoder, _ runtime.GroupVersioner) runtime.Decoder {
	return runtime.WithoutVersionDecoder{
		Decoder: serializer,
	}
}
