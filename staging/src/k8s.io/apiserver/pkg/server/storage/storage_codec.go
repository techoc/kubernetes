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

// StorageCodecConfig 是传递给 newStorageCodecFn 的参数，
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

// NewStorageCodec 根据给定的存储媒体类型、序列化器以及存储和内存版本组装一个存储编解码器。
// 编解码器根据存储媒体类型、序列化器及存储/内存版本组装。
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

	// 装饰器允许调用方包装编码器与解码器（借此注入压缩、加密等处理）；解码器返回列表中的
	// 每一个都会传给 recognizer，因此可同时保留多个解码器，且不改变版本选择逻辑。
	var encoder runtime.Encoder = s
	if opts.EncoderDecoratorFn != nil {
		encoder = opts.EncoderDecoratorFn(encoder)
	}
	decoders := []runtime.Decoder{
		// 将选中的解码器作为主解码器
		s,
		// 以通用反序列化器作为兜底
		opts.StorageSerializer.UniversalDeserializer(),
		// TODO: 遇到此类数据时标记它，要求迁移（读取时解码、写入时用受支持的编码器持久化），并在 1.8 中移除。
		// base64 包装的通用反序列化器是最后的兜底手段，用于读取 base64 编码的 protobuf；
		// 这类历史遗留格式只在早期用过 etcd2+protobuf 时才会出现，数据可能仍在 etcd2 中，
		// 也可能已迁移到 etcd3。
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

	// 确保存储收到正确的版本。
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
