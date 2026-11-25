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

// Package unstructuredscheme 提供完全基于 unstructured 对象的
// NegotiatedSerializer/Typer/Creator 实现，用于无需注册具体 Go 类型的
// 客户端场景（如 kubectl 的通用序列化）。
package unstructuredscheme

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/versioning"
)

// scheme 是共享的空 Scheme，仅用于 versioning codec 的 group/version 选择；
// 它不需要注册任何类型，因为 typer/creator 全部由 unstructured 实现承担。
var scheme = runtime.NewScheme()

// 返回的序列化器只支持 JSON/YAML，且不做任何 defaulting。
func NewUnstructuredNegotiatedSerializer() runtime.NegotiatedSerializer {
	return unstructuredNegotiatedSerializer{
		scheme:  scheme,
		typer:   NewUnstructuredObjectTyper(),
		creator: NewUnstructuredCreator(),
	}
}

// unstructuredNegotiatedSerializer 仅持有 typer/creator/scheme 三件套，
// 具体编解码完全交给内部构造的 json.Serializer。
type unstructuredNegotiatedSerializer struct {
	scheme  *runtime.Scheme
	typer   runtime.ObjectTyper
	creator runtime.ObjectCreater
}

// SupportedMediaTypes 暴露 JSON 和 YAML；只有 JSON 提供流式接口，
// 因为 YAML 流式解码需要额外的分帧策略。
func (s unstructuredNegotiatedSerializer) SupportedMediaTypes() []runtime.SerializerInfo {
	return []runtime.SerializerInfo{
		{
			MediaType:        "application/json",
			MediaTypeType:    "application",
			MediaTypeSubType: "json",
			EncodesAsText:    true,
			Serializer:       json.NewSerializerWithOptions(json.DefaultMetaFactory, s.creator, s.typer, json.SerializerOptions{}),
			PrettySerializer: json.NewSerializerWithOptions(json.DefaultMetaFactory, s.creator, s.typer, json.SerializerOptions{Pretty: true}),
			StreamSerializer: &runtime.StreamSerializerInfo{
				EncodesAsText: true,
				Serializer:    json.NewSerializerWithOptions(json.DefaultMetaFactory, s.creator, s.typer, json.SerializerOptions{}),
				Framer:        json.Framer,
			},
		},
		{
			MediaType:        "application/yaml",
			MediaTypeType:    "application",
			MediaTypeSubType: "yaml",
			EncodesAsText:    true,
			Serializer:       json.NewSerializerWithOptions(json.DefaultMetaFactory, s.creator, s.typer, json.SerializerOptions{Yaml: true}),
		},
	}
}

// EncoderForVersion 用 versioning codec 把对象转换到目标 group/version；
// defaulter 传 nil，因为 unstructured 没有 defaulting 逻辑。
func (s unstructuredNegotiatedSerializer) EncoderForVersion(encoder runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	return versioning.NewDefaultingCodecForScheme(s.scheme, encoder, nil, gv, nil)
}

// DecoderToVersion 与编码方向对称，同样不设置 defaulter。
func (s unstructuredNegotiatedSerializer) DecoderToVersion(decoder runtime.Decoder, gv runtime.GroupVersioner) runtime.Decoder {
	return versioning.NewDefaultingCodecForScheme(s.scheme, nil, decoder, nil, gv)
}

// unstructuredObjectTyper 直接信任对象自身携带的 apiVersion/kind，
// 不依赖任何已注册的类型表。
type unstructuredObjectTyper struct {
}

// NewUnstructuredObjectTyper 返回可处理非结构化对象的对象类型判断器。
func NewUnstructuredObjectTyper() runtime.ObjectTyper {
	return unstructuredObjectTyper{}
}

// ObjectKinds 只接受实现了 runtime.Unstructured 的对象；
// 对普通 Go 类型返回错误，避免静默地产生错误类型信息。
func (t unstructuredObjectTyper) ObjectKinds(obj runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	// 用于委托处理 Unstructured 之外的对象。
	if _, ok := obj.(runtime.Unstructured); !ok {
		return nil, false, fmt.Errorf("cannot type %T", obj)
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if len(gvk.Kind) == 0 {
		return nil, false, runtime.NewMissingKindErr("object has no kind field ")
	}
	if len(gvk.Version) == 0 {
		return nil, false, runtime.NewMissingVersionErr("object has no apiVersion field")
	}

	return []schema.GroupVersionKind{obj.GetObjectKind().GroupVersionKind()}, false, nil
}

// Recognizes 总是返回 true：unstructured 世界的类型信息来自数据本身，
// typer 无法也不应预先枚举。
func (t unstructuredObjectTyper) Recognizes(gvk schema.GroupVersionKind) bool {
	return true
}

// unstructuredCreator 根据请求的 GVK 创建空 Unstructured。
type unstructuredCreator struct{}

// NewUnstructuredCreator 返回始终创建非结构化对象的简单创建器。
func NewUnstructuredCreator() runtime.ObjectCreater {
	return unstructuredCreator{}
}

// New 创建的对象会预先设置 GVK，保证后续序列化能得到正确的 apiVersion/kind。
func (c unstructuredCreator) New(kind schema.GroupVersionKind) (runtime.Object, error) {
	ret := &unstructured.Unstructured{}
	ret.SetGroupVersionKind(kind)
	return ret, nil
}

// unstructuredDefaulter 是空实现占位，便于接口装配。
type unstructuredDefaulter struct {
}

// NewUnstructuredDefaulter 返回适用于非结构化类型、不设置默认值的默认值处理器。
func NewUnstructuredDefaulter() runtime.ObjectDefaulter {
	return unstructuredDefaulter{}
}

// Default 不做任何事：unstructured 对象不应在序列化层被改写。
func (d unstructuredDefaulter) Default(in runtime.Object) {
}
