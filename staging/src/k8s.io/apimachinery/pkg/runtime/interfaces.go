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
	"io"
	"net/url"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// APIVersionInternal 可用于注册那些不应被视为稳定、也不应被序列化的类型。
	// 它只是一种约定，本包对它没有特殊处理；真正的内部类型由各 API 组的
	// internalversion 包统一注册。
	APIVersionInternal = "__internal"
)

// GroupVersioner 把一组可能的转换目标收敛为单一选项，是版本协商的核心抽象。
type GroupVersioner interface {
	// KindForGroupVersionKinds 为给定的一组输入 kind 返回期望的目标 group version kind；
	// 若没有已知目标则 ok 为 false。一般情况下，若返回的目标不在输入列表中，
	// 调用方应先 Scheme.New(target) 再在当前 Go 类型与目标类型之间做转换。
	// 更复杂的实现可以利用输入 kind 的附加信息来选择目标。
	KindForGroupVersionKinds(kinds []schema.GroupVersionKind) (target schema.GroupVersionKind, ok bool)
	// Identifier 返回该对象的字符串表示。
	// 两个不同 encoder 的 Identifier 相等，当且仅当对任意输入 kind 它们返回相同结果。
	Identifier() string
}

// Identifier 表示一个标识符。
// 两个不同对象的 Identifier 相等，当且仅当对任意输入它们产生的输出完全一致。
// 它被用作序列化结果缓存的键，因此等价性必须非常严格。
type Identifier string

// Encoder 把对象写入序列化形式。
type Encoder interface {
	// Encode writes an object to a stream. Implementations may return errors if the versions are
	// incompatible, or if no conversion is defined.
	Encode(obj Object, w io.Writer) error
	// Identifier 返回该 encoder 的标识符。
	// 两个不同 encoder 的 Identifier 相等，当且仅当对任意输入对象二者都会编码出相同表示。
	//
	// Identifier 用于配合 CacheableObject#CacheEncode。为了正确处理 CacheableObject，
	// Encode() 方法应类似下面这样，其中 doEncode() 是实现方真正的编码逻辑：
	//   func (e *MyEncoder) Encode(obj Object, w io.Writer) error {
	//     if co, ok := obj.(CacheableObject); ok {
	//       return co.CacheEncode(e.Identifier(), e.doEncode, w)
	//     }
	//     return e.doEncode(obj, w)
	//   }
	Identifier() Identifier
}

// NondeterministicEncoder 由那些在输出无需确定性时可以更高效地序列化对象的 Encoder 实现。
// 例如 CBOR 编码可以按任意顺序输出 map 条目与结构体字段。
type NondeterministicEncoder interface {
	Encoder

	// EncodeNondeterministic 把对象写入流。与 Encoder 的 Encode 不同，
	// 它不保证任意两次调用写出相同的字节序列；这些差异对通用解码器而言没有语义影响，
	// 例如 map 条目与结构体字段可能以任意顺序编码。
	EncodeNondeterministic(Object, io.Writer) error
}

// MemoryAllocator 负责分配内存。
// 把内存分配抽象成独立接口，使得在确实能显著提升性能的路径上可以跨多次操作复用内存，
// 而不必每次都依赖 GC 重新分配。
type MemoryAllocator interface {
	// Allocate 为 n 个字节预留内存。
	// 注意：实现不要求把返回的数组清零；调用方若需要干净内存须自行处理。
	Allocate(n uint64) []byte
}

// EncoderWithAllocator 以允许调用方管理额外内存分配的方式序列化对象。
type EncoderWithAllocator interface {
	Encoder
	// EncodeWithAllocator 与 Encode 一样把对象写入流，额外允许传入内存分配器，
	// 以便在序列化过程中高效复用内存。
	EncodeWithAllocator(obj Object, w io.Writer, memAlloc MemoryAllocator) error
}

// Decoder 尝试从数据中加载对象。
type Decoder interface {
	// Decode 使用 scheme 自带的类型信息，或传入的默认 kind/group/version 来反序列化数据。
	// 它返回解码后的对象、序列化数据中携带的 kind/group/version，或错误。
	// 若 into 非 nil，实现可将其作为目标类型以复用对象、避免重新分配，但该对象不保证被填充，
	// 返回值也不保证与 into 相同。若提供了默认值，默认会应用到数据上；若未提供或只提供部分默认值，
	// 可能利用 into 的类型来指导转换决策。
	Decode(data []byte, defaults *schema.GroupVersionKind, into Object) (Object, *schema.GroupVersionKind, error)
}

// Serializer 是把对象转换为序列化格式并还原的核心接口。
// 实现可以选择对对象做版本转换，但调用方不应对此做任何假设。
type Serializer interface {
	Encoder
	Decoder
}

// Codec 是处理对象版本化细节的 Serializer。
// 它与 Serializer 接口完全相同，因此更多是一种标记，告知使用方该实现会关心收到对象的版本。
type Codec Serializer

// ParameterCodec 定义在 API 对象与 url.Values 之间序列化/反序列化并进行必要转换的方法。
// 与普通 Codec 不同，查询参数不携带类型自描述信息，因此必须显式指定期望的版本。
type ParameterCodec interface {
	// DecodeParameters takes the given url.Values in the specified group version and decodes them
	// into the provided object, or returns an error.
	DecodeParameters(parameters url.Values, from schema.GroupVersion, into Object) error
	// EncodeParameters encodes the provided object as query parameters or returns an error.
	EncodeParameters(obj Object, to schema.GroupVersion) (url.Values, error)
}

// Framer 是创建遵循特定分帧（framing）模式读写器的工厂。
// 分帧用于在单一流上分隔多个对象，例如 watch 流中的逐条事件。
type Framer interface {
	NewFrameReader(r io.ReadCloser) io.ReadCloser
	NewFrameWriter(w io.Writer) io.Writer
}

// SerializerInfo 描述一种具体序列化格式的信息。
type SerializerInfo struct {
	// MediaType 是该序列化器在传输中使用的媒体类型值。
	MediaType string
	// MediaTypeType 是 MediaType 的第一部分（如 "application/json" 中的 "application"）。
	MediaTypeType string
	// MediaTypeSubType 是 MediaType 的第二部分（如 "application/json" 中的 "json"）。
	MediaTypeSubType string
	// EncodesAsText 表示该序列化器可安全地编码为 UTF-8 文本。
	EncodesAsText bool
	// Serializer 是该媒体类型下针对单个对象的序列化器。
	Serializer Serializer
	// PrettySerializer 若设置，可按偏向可读性的形式序列化对象（如缩进 JSON）。
	PrettySerializer Serializer
	// StrictSerializer 若设置，会以严格模式反序列化对象，遇到未知字段即报错。
	StrictSerializer Serializer
	// StreamSerializer 若设置，描述该媒体类型的流式序列化格式。
	StreamSerializer *StreamSerializerInfo
}

// StreamSerializerInfo 描述一种具体的流式序列化格式。
type StreamSerializerInfo struct {
	// EncodesAsText 表示该序列化器可安全地编码为 UTF-8 文本。
	EncodesAsText bool
	// Serializer 是流式传输时使用的顶层对象序列化器。
	Serializer
	// Framer 是用于获取"在传输中分隔对象"的流的工厂。
	Framer
}

// NegotiatedSerializer 用于为多种受支持的媒体类型获取 encoder、decoder 与 serializer。
// 通常由执行 HTTP 内容协商、需要接受多种格式的服务端组件使用。
type NegotiatedSerializer interface {
	// SupportedMediaTypes 是读写单个对象所支持的媒体类型。
	SupportedMediaTypes() []SerializerInfo

	// EncoderForVersion 返回一个 encoder，确保写入给定 serializer 的对象处于指定的组版本。
	EncoderForVersion(serializer Encoder, gv GroupVersioner) Encoder
	// DecoderToVersion 返回一个 decoder，确保给定 serializer 读出的对象默认处于指定的组版本。
	DecoderToVersion(serializer Decoder, gv GroupVersioner) Decoder
}

// ClientNegotiator 负责把 HTTP content type 转换为合适的 encoder/decoder。
// 可借助 NewClientNegotiator 或 NewVersionedClientNegotiator 从 NegotiatedSerializer 构造该接口。
type ClientNegotiator interface {
	// Encoder 为给定 contentType（如 application/json）及可选的媒体类型参数（如 pretty=1）
	// 返回合适的 encoder，或返回错误。找不到 serializer 时返回 NegotiateError。
	// 当前客户端实现把 params 视为 contentType 的可选修饰符，并忽略无法识别的参数。
	Encoder(contentType string, params map[string]string) (Encoder, error)
	// Decoder 为给定 contentType 及可选媒体类型参数返回合适的 decoder，或返回错误。
	// 找不到 serializer 时返回 NegotiateError；同样忽略无法识别的参数。
	Decoder(contentType string, params map[string]string) (Decoder, error)
	// StreamDecoder 为给定 contentType 及可选媒体类型参数返回合适的流式 decoder，或返回错误。
	// 找不到 serializer 时返回 NegotiateError。只要返回了 Decoder，就一定会同时返回 Serializer 与 Framer。
	// 当前客户端实现同样忽略无法识别的参数。
	StreamDecoder(contentType string, params map[string]string) (Decoder, Serializer, Framer, error)
}

// StorageSerializer 用于获取能够读写"静态存储数据"的 encoder、decoder 与 serializer。
// 典型使用方是需要读取文件的客户端工具，或持久化 REST 对象的服务端存储层。
type StorageSerializer interface {
	// SupportedMediaTypes 是读写对象所支持的媒体类型。
	SupportedMediaTypes() []SerializerInfo

	// UniversalDeserializer 返回一个能通过检视静态数据自动识别格式、从而读取多种格式对象的 Decoder。
	UniversalDeserializer() Decoder

	// EncoderForVersion 返回一个 encoder，确保写入给定 serializer 的对象处于指定组版本。
	EncoderForVersion(serializer Encoder, gv GroupVersioner) Encoder
	// DecoderToVersion 返回一个 decoder，确保给定 serializer 读出的对象默认处于指定组版本。
	DecoderToVersion(serializer Decoder, gv GroupVersioner) Decoder
}

// NestedObjectEncoder 是对象可选实现的接口，用于在序列化期间获得编码其内部嵌套对象
// 或 RawExtension 的机会。
type NestedObjectEncoder interface {
	EncodeNestedObjects(e Encoder) error
}

// NestedObjectDecoder 是对象可选实现的接口，用于在反序列化期间获得解码其内部嵌套对象
// 或 RawExtension 的机会。
// 在严格解码模式下（例如出现未知或重复字段），DecodeNestedObjects 可能返回非 nil 错误，
// 但实际上解码已经成功。因此调用方在短路返回前应先确认错误是否为 runtime.StrictDecodingError；
// 相应地，实现方也应保证只有在其余解码均成功时才返回 runtime.StrictDecodingError。
type NestedObjectDecoder interface {
	DecodeNestedObjects(d Decoder) error
}

///////////////////////////////////////////////////////////////////////////////
// 非 codec 接口

// ObjectDefaulter 为对象填充默认值。
type ObjectDefaulter interface {
	// Default 接收一个对象（必须是指针）并应用默认值。默认值填充不允许返回错误。
	Default(in Object)
}

// ObjectVersioner 负责对象在版本之间的转换，并给出某组下优先使用的版本列表。
type ObjectVersioner interface {
	ConvertToVersion(in Object, gv GroupVersioner) (out Object, err error)
	PrioritizedVersionsForGroup(group string) []schema.GroupVersion
}

// ObjectConvertor 把对象转换到另一个版本。
type ObjectConvertor interface {
	// Convert 尝试把一个对象转换为另一个，或返回错误。
	// 该方法不修改 in 对象，但 in 与 out 可能共享底层数据结构，
	// 因此修改 out 可能会同时影响 in。context 参数会传递给所有嵌套转换。
	Convert(in, out, context interface{}) error
	// ConvertToVersion 把给定对象转换到指定版本。该方法不修改 in 对象，但 in 与 out
	// 可能共享底层数据结构，修改 out 可能影响 in。它类似 Convert()，但额外负责选择正确的输出版本。
	ConvertToVersion(in Object, gv GroupVersioner) (out Object, err error)
	// ConvertFieldLabel 把字段选择器中的 label/value 转换为指定 GVK 下的等价形式。
	ConvertFieldLabel(gvk schema.GroupVersionKind, label, value string) (string, string, error)
}

// ObjectTyper 提供提取对象 APIVersion 与 Kind 的方法。
type ObjectTyper interface {
	// ObjectKinds 返回对象所有可能的 group/version/kind；若对象是 unversioned 的，
	// 第二个返回值 true；若对象未被识别则返回错误（IsNotRegisteredError 会返回 true）。
	ObjectKinds(Object) ([]schema.GroupVersionKind, bool, error)
	// Recognizes 返回 scheme 是否能处理给定的版本与 kind——更准确地说，
	// 该版本是否是一个可能的转换或解码目标。
	Recognizes(gvk schema.GroupVersionKind) bool
}

// ObjectCreater 提供按 kind 与版本实例化对象的方法。
type ObjectCreater interface {
	New(kind schema.GroupVersionKind) (out Object, err error)
}

// EquivalentResourceMapper 提供关于"与指定资源指向同一底层数据"的资源的信息。
// 例如 pods 与 pods/status 指向同一对象，只是子资源不同。
type EquivalentResourceMapper interface {
	// EquivalentResourcesFor 返回与 resource 指向同一底层数据的资源列表。
	// 若指定了 subresource，则只包含具有相同 subresource 的等价资源；指定资源本身可以出现在结果中。
	EquivalentResourcesFor(resource schema.GroupVersionResource, subresource string) []schema.GroupVersionResource
	// KindFor 返回指定 resource[/subresource] 期望的 kind；未知时返回零值。
	KindFor(resource schema.GroupVersionResource, subresource string) schema.GroupVersionKind
}

// EquivalentResourceRegistry 在 EquivalentResourceMapper 基础上，
// 允许注册已知的 resource[/subresource] -> kind 映射。
type EquivalentResourceRegistry interface {
	EquivalentResourceMapper
	// RegisterKindFor 登记指定 resource[/subresource] 的存在及其期望的 kind。
	RegisterKindFor(resource schema.GroupVersionResource, subresource string, kind schema.GroupVersionKind)
}

// ResourceVersioner 提供在 API 对象上读写 resourceVersion 的方法。
type ResourceVersioner interface {
	SetResourceVersion(obj Object, version string) error
	ResourceVersion(obj Object) (string, error)
}

// Namer 提供获取 API 对象名称与命名空间的方法。
type Namer interface {
	// Name 返回给定对象的名称。
	Name(obj Object) (string, error)
	// Namespace 返回给定对象的命名空间。
	Namespace(obj Object) (string, error)
}

// Object 接口必须由所有注册到 Scheme 的 API 类型实现。由于 scheme 中的对象预期会被序列化到线路上，
// Object 提供给 Scheme 的接口允许序列化器设置该对象所代表的 kind、version 与 group。
// 若对象不预期被序列化，也可以返回一个空操作的 ObjectKindAccessor。
type Object interface {
	GetObjectKind() schema.ObjectKind
	DeepCopyObject() Object
}

// CacheableObject 允许对象缓存其不同形式的序列化结果，避免对同一对象重复执行相同序列化。
type CacheableObject interface {
	// CacheEncode 把对象写入流。缓存未命中时会调用 <encode> 函数，且该函数取得对象的所有权。
	// 若 CacheableObject 是一个包装器，应把被包装对象的深拷贝传给 <encode>。
	// CacheEncode 假定对同一个 <id> 的两次不同调用，其 <encode> 函数也相同。
	CacheEncode(id Identifier, encode func(Object, io.Writer) error, w io.Writer) error
	// GetObject 返回待编码对象的深拷贝，调用方拥有返回对象的所有权。
	// 之所以返回拷贝，是为了避免调用方修改对象却忘记复制、从而影响所有使用者。
	// GetObject 返回的对象应与 CacheEncode 中传给 <encode> 的对象一致；
	// 若 CacheableObject 是包装器，则返回被包装对象的拷贝。
	GetObject() Object
}

// Unstructured 对象以 map[string]interface{} 存储值，且只允许能够序列化为 JSON 的值。
type Unstructured interface {
	Object
	// NewEmptyInstance 返回该具体类型的新实例，仅包含 kind/apiVersion，不含其他数据。
	// 对 unstructured 类型应调用此方法而非 reflect.New()，因为仅凭 Go 类型无法保留 kind/apiVersion 信息。
	NewEmptyInstance() Unstructured
	// UnstructuredContent 返回一个非 nil 的 map，包含该对象的内容。值可以是
	// []interface{}、map[string]interface{} 或任意基本类型；内容通常会在 JSON 之间往返。
	// 若要修改内容，应使用 SetUnstructuredContent。
	UnstructuredContent() map[string]interface{}
	// SetUnstructuredContent 用给定 map 更新对象内容。
	SetUnstructuredContent(map[string]interface{})
	// IsList 在该类型是列表或符合列表约定（含名为 "items" 的数组）时返回 true。
	IsList() bool
	// EachListItem 应把列表中的每个元素作为 Object 依次传给给定函数；任何错误都会终止迭代。
	// 若 IsList() 返回 false，该方法应直接返回错误，而不调用给定函数。
	EachListItem(func(Object) error) error
	// EachListItemWithAlloc 类似 EachListItem，但避免长期持有 items 切片的引用：
	// 它在把非指针元素传给 fn 之前会做一次浅拷贝。
	//
	// 若 fn 不会保留传入的元素，或保留时长与列表本身相同，使用 EachListItem 更节省内存。
	EachListItemWithAlloc(func(Object) error) error
}

// ApplyConfiguration 是根 apply 配置类型所实现的接口。
type ApplyConfiguration interface {
	// IsApplyConfiguration 由 apply 配置的根对象实现，用于标记其身份。
	IsApplyConfiguration()
}
