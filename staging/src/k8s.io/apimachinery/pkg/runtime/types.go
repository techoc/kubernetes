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

// 本文件中的类型不属于任何具体 API 版本，可安全地用于所有版本的 API 对象。

// TypeMeta 是所有顶层 API 对象共用的类型元信息，通常应匿名内嵌到自定义类型中，例如：
//
//	type MyAwesomeAPIObject struct {
//	     runtime.TypeMeta    `json:""`
//	     ... // other fields
//	}
//
// func (obj *MyAwesomeAPIObject) SetGroupVersionKind(gvk *metav1.GroupVersionKind) { metav1.UpdateTypeMeta(obj,gvk) }; GroupVersionKind() *GroupVersionKind
//
// 这里提供 TypeMeta 只是为了复用方便；也可以在自己的包中定义包含相同字段的类型。
//
// 该类型不参与深拷贝生成（见下方指令），因为它只含两个字符串。
//
// +k8s:deepcopy-gen=false
// +protobuf=true
// +k8s:openapi-gen=true
type TypeMeta struct {
	// APIVersion 形如 "group/version"；core group 只写版本。
	// +optional
	APIVersion string `json:"apiVersion,omitempty" yaml:"apiVersion,omitempty" protobuf:"bytes,1,opt,name=apiVersion"`
	// Kind 是资源类型名，需与 APIVersion 配对使用。
	// +optional
	Kind string `json:"kind,omitempty" yaml:"kind,omitempty" protobuf:"bytes,2,opt,name=kind"`
}

// 常量集中定义 apiserver 支持的媒体类型，避免各处硬编码字符串不一致。
const (
	ContentTypeJSON         string = "application/json"
	ContentTypeYAML         string = "application/yaml"
	ContentTypeProtobuf     string = "application/vnd.kubernetes.protobuf"
	ContentTypeCBOR         string = "application/cbor"     // RFC 8949
	ContentTypeCBORSequence string = "application/cbor-seq" // RFC 8742
)

// RawExtension 用于在外部（版本化）API 类型中暂存尚未解析的扩展对象。
//
// 使用时，外部版本结构体中的对应字段声明为 RawExtension，内部结构体中的字段声明为 Object；
// 同时还必须把可能出现的插件类型注册到 Scheme 中。
//
// 内部版本结构体示例：
//
//	type MyAPIObject struct {
//		runtime.TypeMeta `json:""`
//		MyPlugin runtime.Object `json:"myPlugin"`
//	}
//
//	type PluginA struct {
//		AOption string `json:"aOption"`
//	}
//
// 外部版本结构体示例：
//
//	type MyAPIObject struct {
//		runtime.TypeMeta `json:""`
//		MyPlugin runtime.RawExtension `json:"myPlugin"`
//	}
//
//	type PluginA struct {
//		AOption string `json:"aOption"`
//	}
//
// 在线路上传输时，JSON 数据大致如下：
//
//	{
//		"kind":"MyAPIObject",
//		"apiVersion":"v1",
//		"myPlugin": {
//			"kind":"PluginA",
//			"aOption":"foo",
//		},
//	}
//
// 解码时，JSON/YAML 解码器先把数据写入外部版本结构体，扩展字段的原始 JSON 暂存在 Raw 中，尚未解析。
// 随后通过 pkg/conversion 转换为内部结构体；runtime 包的默认 Scheme 注册了相应转换函数，
// 会解析 Raw 中的数据、根据 TypeMeta 找到具体插件类型，并将对象写入 Object。
// 若扩展类型尚未注册，应结合 Unknown 的透传语义处理。
//
// Raw 与 Object 互斥地承载数据：解码后填 Raw，转换到内部版本后填 Object。
// 两者都标了 json:"-"，因此不会被直接序列化，实际输出由自定义 MarshalJSON 决定。
//
// +k8s:deepcopy-gen=true
// +protobuf=true
// +k8s:openapi-gen=true
type RawExtension struct {
	// Raw 保存扩展对象的原始序列化字节；该字段本身不负责推断媒体类型或内容编码。
	Raw []byte `json:"-" protobuf:"bytes,1,opt,name=raw"`
	// Object 保存已经解析出的运行时对象，便于内部版本结构体直接使用类型化对象。
	Object Object `json:"-"`
}

// Unknown 用于透传当前进程无法识别类型的 API 对象，例如来自尚未安装插件的对象。
// 它仍保留 TypeMeta 中的 Kind 和 APIVersion，使接收方能够判断对象声明的类型与版本。
// 对于未知对象，通常不应尝试修改其内部字段，而应尽可能原样传递。
//
// 这是"透传"语义的载体：apiserver 不认识该类型时仍能保留原始字节，
// 在后续版本或插件侧再解析，避免数据丢失。
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +protobuf=true
// +k8s:openapi-gen=true
type Unknown struct {
	TypeMeta `json:"" protobuf:"bytes,1,opt,name=typeMeta"`
	// Raw 保存完整的序列化对象；无法匹配已注册类型时，应优先原样透传，而不是丢弃或擅自改写。
	Raw []byte `json:"-" protobuf:"bytes,2,opt,name=raw"`
	// ContentEncoding 描述 Raw 使用的内容编码；为空表示未指定额外编码。
	ContentEncoding string `protobuf:"bytes,3,opt,name=contentEncoding"`
	// ContentType 描述 Raw 使用的序列化媒体类型；为空时按 ContentTypeJSON 处理。
	ContentType string `protobuf:"bytes,4,opt,name=contentType"`
}
