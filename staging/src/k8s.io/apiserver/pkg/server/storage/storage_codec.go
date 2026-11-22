/*
Copyright 2017 The Kubernetes Authors.

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

package storage

import (
	"fmt"
	"mime"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/recognizer"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// StorageCodecConfig are the arguments passed to newStorageCodecFn
// 描述 etcd 中数据格式的版本与序列化配置；StorageVersion 是写入 etcd 的版本，
// MemoryVersion 是进程内对象使用的版本。
type StorageCodecConfig struct {
	StorageMediaType  string
	StorageSerializer runtime.StorageSerializer
	StorageVersion    schema.GroupVersion
	MemoryVersion     schema.GroupVersion
	Config            storagebackend.Config

	EncoderDecoratorFn func(runtime.Encoder) runtime.Encoder
	DecoderDecoratorFn func([]runtime.Decoder) []runtime.Decoder
}

// NewStorageCodec assembles a storage codec for the provided storage media type, the provided serializer, and the requested
// storage and memory versions.
//
// 编码时把对象统一转换到 StorageVersion 再落盘；解码时依次尝试多个解码器，
// 最终强制转换为 MemoryVersion，保证不同时期写入的 etcd 数据都能被读取。
func NewStorageCodec(opts StorageCodecConfig) (runtime.Codec, runtime.GroupVersioner, error) {
	mediaType, _, err := mime.ParseMediaType(opts.StorageMediaType)
	if err != nil {
		return nil, nil, fmt.Errorf("%q is not a valid mime-type", opts.StorageMediaType)
	}

	supportedMediaTypes := opts.StorageSerializer.SupportedMediaTypes()
	serializer, ok := runtime.SerializerInfoForMediaType(supportedMediaTypes, mediaType)
	if !ok {
		// 列出可用的媒体类型，便于定位配置错误。
		supportedMediaTypeList := make([]string, len(supportedMediaTypes))
		for i, mediaType := range supportedMediaTypes {
			supportedMediaTypeList[i] = mediaType.MediaType
		}
		return nil, nil, fmt.Errorf("unable to find serializer for %q, supported media types: %v", mediaType, supportedMediaTypeList)
	}

	s := serializer.Serializer

	// Give callers the opportunity to wrap encoders and decoders.  For decoders, each returned decoder will
	// be passed to the recognizer so that multiple decoders are available.
	// 装饰器允许上层注入压缩、加密等处理，且不改变版本选择逻辑。
	var encoder runtime.Encoder = s
	if opts.EncoderDecoratorFn != nil {
		encoder = opts.EncoderDecoratorFn(encoder)
	}
	decoders := []runtime.Decoder{
		// selected decoder as the primary
		s,
		// universal deserializer as a fallback
		opts.StorageSerializer.UniversalDeserializer(),
		// base64-wrapped universal deserializer as a last resort.
		// this allows reading base64-encoded protobuf, which should only exist if etcd2+protobuf was used at some point.
		// data written that way could exist in etcd2, or could have been migrated to etcd3.
		// TODO: flag this type of data if we encounter it, require migration (read to decode, write to persist using a supported encoder), and remove in 1.8
		// base64 包装是历史遗留格式，仅为兼容早期 etcd2+protobuf 写入的数据。
		runtime.NewBase64Serializer(nil, opts.StorageSerializer.UniversalDeserializer()),
	}
	if opts.DecoderDecoratorFn != nil {
		decoders = opts.DecoderDecoratorFn(decoders)
	}

	// 编码版本器优先选择 StorageVersion；无法精确匹配时按组回退到 MemoryVersion。
	encodeVersioner := runtime.NewMultiGroupVersioner(
		opts.StorageVersion,
		schema.GroupKind{Group: opts.StorageVersion.Group},
		schema.GroupKind{Group: opts.MemoryVersion.Group},
	)

	// Ensure the storage receives the correct version.
	encoder = opts.StorageSerializer.EncoderForVersion(
		encoder,
		encodeVersioner,
	)
	// 解码使用 Coercing 版本器：即使 etcd 中是旧版本，也强制转换到内存版本。
	decoder := opts.StorageSerializer.DecoderToVersion(
		recognizer.NewDecoder(decoders...),
		runtime.NewCoercingMultiGroupVersioner(
			opts.MemoryVersion,
			schema.GroupKind{Group: opts.MemoryVersion.Group},
			schema.GroupKind{Group: opts.StorageVersion.Group},
		),
	)

	return runtime.NewCodec(encoder, decoder), encodeVersioner, nil
}
