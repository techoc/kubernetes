/*
Copyright 2018 The Kubernetes Authors.

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

package dynamic

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/client-go/features"
)

// basicScheme 仅注册 metav1 的基础类型，用于给 unstructured 对象补全 TypeMeta。
// parameterScheme 供 URL 参数编码使用，与 basicScheme 分开避免互相污染。
var basicScheme = runtime.NewScheme()
var parameterScheme = runtime.NewScheme()
var dynamicParameterCodec = runtime.NewParameterCodec(parameterScheme)

var versionV1 = schema.GroupVersion{Version: "v1"}

func init() {
	metav1.AddToGroupVersion(basicScheme, versionV1)
	metav1.AddToGroupVersion(parameterScheme, versionV1)
}

// newBasicNegotiatedSerializer 构造 dynamic 客户端使用的序列化器集合。
//
// 支持 JSON；客户端特性门控开启时额外支持 CBOR。序列化器允许注册表外的
// 任意 GVK，因为 dynamic 客户端本就面向编译期未知的 CRD。
func newBasicNegotiatedSerializer() basicNegotiatedSerializer {
	supportedMediaTypes := []runtime.SerializerInfo{
		{
			MediaType:        "application/json",
			MediaTypeType:    "application",
			MediaTypeSubType: "json",
			EncodesAsText:    true,
			// Creater/Typer 面向 unstructured，使解码不依赖 scheme 中已注册的类型。
			Serializer:       json.NewSerializerWithOptions(json.DefaultMetaFactory, unstructuredCreater{basicScheme}, unstructuredTyper{basicScheme}, json.SerializerOptions{}),
			PrettySerializer: json.NewSerializerWithOptions(json.DefaultMetaFactory, unstructuredCreater{basicScheme}, unstructuredTyper{basicScheme}, json.SerializerOptions{Pretty: true}),
			StreamSerializer: &runtime.StreamSerializerInfo{
				EncodesAsText: true,
				Serializer:    json.NewSerializerWithOptions(json.DefaultMetaFactory, basicScheme, basicScheme, json.SerializerOptions{}),
				Framer:        json.Framer,
			},
		},
	}
	if features.FeatureGates().Enabled(features.ClientsAllowCBOR) {
		supportedMediaTypes = append(supportedMediaTypes, runtime.SerializerInfo{
			MediaType:        "application/cbor",
			MediaTypeType:    "application",
			MediaTypeSubType: "cbor",
			Serializer:       cbor.NewSerializer(unstructuredCreater{basicScheme}, unstructuredTyper{basicScheme}),
			StreamSerializer: &runtime.StreamSerializerInfo{
				// 流式读取时不做转码，直接按 CBOR 解码。
				Serializer: cbor.NewSerializer(basicScheme, basicScheme, cbor.Transcode(false)),
				Framer:     cbor.NewFramer(),
			},
		})
	}
	return basicNegotiatedSerializer{supportedMediaTypes: supportedMediaTypes}
}

// basicNegotiatedSerializer 固定返回预置的媒体类型列表。
type basicNegotiatedSerializer struct {
	supportedMediaTypes []runtime.SerializerInfo
}

func (s basicNegotiatedSerializer) SupportedMediaTypes() []runtime.SerializerInfo {
	return s.supportedMediaTypes
}

func (s basicNegotiatedSerializer) EncoderForVersion(encoder runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	// 使用 permissiveTyper：历史行为允许写入缺少 apiVersion/kind 的 Unstructured。
	return runtime.WithVersionEncoder{
		Version:     gv,
		Encoder:     encoder,
		ObjectTyper: permissiveTyper{basicScheme},
	}
}

func (s basicNegotiatedSerializer) DecoderToVersion(decoder runtime.Decoder, gv runtime.GroupVersioner) runtime.Decoder {
	// dynamic 客户端不在客户端侧做版本转换，解码器原样返回。
	return decoder
}

// unstructuredCreater 在 scheme 未注册该 GVK 时回退为 unstructured 对象。
type unstructuredCreater struct {
	nested runtime.ObjectCreater
}

func (c unstructuredCreater) New(kind schema.GroupVersionKind) (runtime.Object, error) {
	out, err := c.nested.New(kind)
	if err == nil {
		return out, nil
	}
	// 未注册的类型仍可表示：创建 unstructured 并记住 GVK 即可。
	out = &unstructured.Unstructured{}
	out.GetObjectKind().SetGroupVersionKind(kind)
	return out, nil
}

// unstructuredTyper 从 unstructured 自身的 TypeMeta 推导 GVK，绕过 scheme 注册。
type unstructuredTyper struct {
	nested runtime.ObjectTyper
}

func (t unstructuredTyper) ObjectKinds(obj runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	kinds, unversioned, err := t.nested.ObjectKinds(obj)
	if err == nil {
		return kinds, unversioned, nil
	}
	// scheme 查不到但对象自带 GVK 时仍可确定类型；两者都缺则保留原错误。
	if _, ok := obj.(runtime.Unstructured); ok && !obj.GetObjectKind().GroupVersionKind().Empty() {
		return []schema.GroupVersionKind{obj.GetObjectKind().GroupVersionKind()}, false, nil
	}
	return nil, false, err
}

func (t unstructuredTyper) Recognizes(gvk schema.GroupVersionKind) bool {
	// 所有 GVK 一律视为可识别，否则 dynamic 客户端无法处理未知 CRD。
	return true
}

// 动态客户端历来允许写入请求传入缺少或为空 apiVersion/kind 的 Unstructured 对象。
// 该 Typer 对 runtime.Unstructured 一律返回类型且不报错，即使类型信息缺失或为空，
// 以维持既有兼容性。
//
// 该宽松行为是既有 API 兼容性的一部分：很多调用方只构造 data 字段就发起请求，
// 由 URL 决定实际资源类型。
type permissiveTyper struct {
	nested runtime.ObjectTyper
}

func (t permissiveTyper) ObjectKinds(obj runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	kinds, unversioned, err := t.nested.ObjectKinds(obj)
	if err == nil {
		return kinds, unversioned, nil
	}
	if _, ok := obj.(runtime.Unstructured); ok {
		return []schema.GroupVersionKind{obj.GetObjectKind().GroupVersionKind()}, false, nil
	}
	return nil, false, err
}

func (t permissiveTyper) Recognizes(gvk schema.GroupVersionKind) bool {
	return true
}
