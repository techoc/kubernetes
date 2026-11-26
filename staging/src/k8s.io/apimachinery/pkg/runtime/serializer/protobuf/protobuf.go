/*
Copyright 2015 The Kubernetes Authors.

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

package protobuf

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/recognizer"
	"k8s.io/apimachinery/pkg/util/framer"
	"k8s.io/klog/v2"
)

var (
	// protoEncodingPrefix 是该序列化器编码消息的魔数：所有由此 schema 序列化的
	// proto 消息都以字节 0x6b 0x38 0x73 开头，第四个字节保留给编码风格。
	// 目前只定义了 0x00，表示后续字节流是 k8s.io.kubernetes.pkg.runtime.Unknown（proto2）消息。
	//
	// runtime.Unknown 消息的定义见 k8s.io/apimachinery/pkg/runtime/generated.proto。
	//
	// 该编码方案尚属实验性质，随时可能变更。
	//
	// 前缀是识别器判断"这是 Kubernetes protobuf 而非任意 protobuf"的依据；
	// 第 4 字节预留编码风格，便于将来扩展而不破坏现有解码器。
	protoEncodingPrefix = []byte{0x6b, 0x38, 0x73, 0x00}
)

// errNotMarshalable 表示对象未实现 protobuf 编解码接口，无法被该序列化器处理。
type errNotMarshalable struct {
	t reflect.Type
}

// Error 输出包含具体类型的错误信息，便于定位是哪个对象不兼容。
func (e errNotMarshalable) Error() string {
	return fmt.Sprintf("object %v does not implement the protobuf marshalling interface and cannot be encoded to a protobuf message", e.t)
}

// Status 把错误包装成 HTTP 406 Not Acceptable，表示服务端无法生成客户端要求的格式。
func (e errNotMarshalable) Status() metav1.Status {
	return metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusNotAcceptable,
		Reason:  metav1.StatusReason("NotAcceptable"),
		Message: e.Error(),
	}
}

// IsNotMarshalable 判断错误是否表示不可序列化：仅当错误非 nil 且不可序列化时返回 true。
func IsNotMarshalable(err error) bool {
	_, ok := err.(errNotMarshalable)
	return err != nil && ok
}

// NewSerializer 创建 Protobuf 序列化器，把带版本的对象编码为正确的 wire 格式。
// 传入 typer 时会设置对象的 group、version 和 kind；typer 为 nil 时按原样写入，
// 使用对象自带的类型信息。
func NewSerializer(creater runtime.ObjectCreater, typer runtime.ObjectTyper) *Serializer {
	return NewSerializerWithOptions(creater, typer, SerializerOptions{})
}

// NewSerializerWithOptions 创建带选项的 Protobuf 序列化器，把带版本的对象编码为正确的 wire 格式。
// 传入 typer 时会设置对象的 group、version 和 kind；typer 为 nil 时按原样写入，
// 使用对象自带的类型信息。
func NewSerializerWithOptions(creater runtime.ObjectCreater, typer runtime.ObjectTyper, opts SerializerOptions) *Serializer {
	return &Serializer{
		prefix:  protoEncodingPrefix,
		creater: creater,
		typer:   typer,
		options: opts,
	}
}

// Serializer 负责把带版本的对象编码为正确的 wire 格式。
//
// Serializer 把对象包进 runtime.Unknown 再编码，因此输出自带类型信息（自描述）。
type Serializer struct {
	prefix  []byte
	creater runtime.ObjectCreater
	typer   runtime.ObjectTyper

	options SerializerOptions
}

// SerializerOptions 保存 Proto 序列化器的配置选项。
type SerializerOptions struct {
	// StreamingCollectionsEncoding 开启后逐项编码集合，可大幅降低内存占用。
	//
	// 开启后 list 会逐项流式编码，避免把整个列表物化为一个巨大的 runtime.Unknown。
	StreamingCollectionsEncoding bool
}

var _ runtime.Serializer = &Serializer{}
var _ runtime.EncoderWithAllocator = &Serializer{}
var _ recognizer.RecognizingDecoder = &Serializer{}

const serializerIdentifier runtime.Identifier = "protobuf"

// Decode 先将数据转为 protobuf 消息，取出其中保存的 schema kind，套用给定的默认 gvk，
// 再把数据装入目标 schema kind 对应的对象或 into。若 into 是 *runtime.Unknown，只提取原始数据，
// 不做解码；若 into 未注册到 typer，则走常规的 protobuf 反序列化（MarshalTo 接口）直接解码。
// 若提供了 into 且原始数据的 kind/version/group 不完整，会用 into 的类型修正返回的 gvk。
// 成功或大多数错误情况下都会返回计算出的 schema kind。
func (s *Serializer) Decode(originalData []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	prefixLen := len(s.prefix)
	// 先做前缀与长度检查，避免把明显不是本格式的数据交给 protobuf 解析器。
	switch {
	case len(originalData) == 0:
		// TODO: 参照从 JSON 空对象 {} 解码时的默认值处理。
		return nil, nil, fmt.Errorf("empty data")
	case len(originalData) < prefixLen || !bytes.Equal(s.prefix, originalData[:prefixLen]):
		return nil, nil, fmt.Errorf("provided data does not appear to be a protobuf message, expected prefix %v", s.prefix)
	case len(originalData) == prefixLen:
		// TODO: 参照从 JSON 空对象 {} 解码时的默认值处理。
		return nil, nil, fmt.Errorf("empty body")
	}

	data := originalData[prefixLen:]
	unk := runtime.Unknown{}
	if err := unk.Unmarshal(data); err != nil {
		return nil, nil, err
	}

	// 以消息内嵌的 GVK 为准，缺失的字段用调用方提供的默认值补齐。
	actual := unk.GroupVersionKind()
	copyKindDefaults(&actual, gvk)

	if intoUnknown, ok := into.(*runtime.Unknown); ok && intoUnknown != nil {
		// 调用方只要原始数据：直接返回 Unknown，不做对象解码。
		*intoUnknown = unk
		if ok, _, _ := s.RecognizesData(unk.Raw); ok {
			intoUnknown.ContentType = runtime.ContentTypeProtobuf
		}
		return intoUnknown, &actual, nil
	}

	if into != nil {
		types, _, err := s.typer.ObjectKinds(into)
		switch {
		case runtime.IsNotRegisteredError(err):
			// into 未注册：退化为直接调用对象自身的 Unmarshal 方法。
			unmarshaler, ok := into.(unmarshaler)
			if !ok {
				return nil, &actual, errNotMarshalable{reflect.TypeOf(into)}
			}
			// 顶层反序列化先重置对象，再委托给对象自己处理。
			unmarshaler.Reset()
			if err := unmarshaler.Unmarshal(unk.Raw); err != nil {
				return nil, &actual, err
			}
			return into, &actual, nil
		case err != nil:
			return nil, &actual, err
		default:
			copyKindDefaults(&actual, &types[0])
			// 默认值填充后若仍没有 version 或 group，至少补上 group
			// （copyKindDefaults 在 version 已设置时不会补 Group）。这样在调用方和对象都
			// 没有提供更好信息时，into 的 group 也能确定下来。
			if len(actual.Version) == 0 && len(actual.Group) == 0 {
				actual.Group = types[0].Group
			}
		}
	}

	// 到这里仍缺 kind/version 说明数据与调用方都没提供足够信息，无法创建对象。
	if len(actual.Kind) == 0 {
		return nil, &actual, runtime.NewMissingKindErr(fmt.Sprintf("%#v", unk.TypeMeta))
	}
	if len(actual.Version) == 0 {
		return nil, &actual, runtime.NewMissingVersionErr(fmt.Sprintf("%#v", unk.TypeMeta))
	}

	return unmarshalToObject(s.typer, s.creater, &actual, into, unk.Raw)
}

// EncodeWithAllocator 把对象写入给定的 writer，并允许提供内存分配器以降低序列化时的内存开销。
func (s *Serializer) EncodeWithAllocator(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	return s.encode(obj, w, memAlloc)
}

// Encode 把对象序列化后写入给定的 writer。
func (s *Serializer) Encode(obj runtime.Object, w io.Writer) error {
	return s.encode(obj, w, &runtime.SimpleAllocator{})
}

// encode 处理 CacheableObject 的缓存命中，再执行真正的编码。
func (s *Serializer) encode(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	if co, ok := obj.(runtime.CacheableObject); ok {
		// 同一对象在 watch 中会被多次编码，CacheEncode 可复用上次结果。
		return co.CacheEncode(s.Identifier(), func(obj runtime.Object, w io.Writer) error { return s.doEncode(obj, w, memAlloc) }, w)
	}
	return s.doEncode(obj, w, memAlloc)
}

// doEncode 把对象包装为 runtime.Unknown（或复用已有的）并写出。
//
// 提供 Allocator 时按 Size() 精确分配一次，再直接 MarshalTo，避免中间缓冲。
func (s *Serializer) doEncode(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	if memAlloc == nil {
		//nolint:logcheck // Should not be reached in normal operations.
		klog.Error("a mandatory memory allocator wasn't provided, this might have a negative impact on performance, check invocations of EncodeWithAllocator method, falling back on runtime.SimpleAllocator")
		memAlloc = &runtime.SimpleAllocator{}
	}
	prefixSize := uint64(len(s.prefix))

	var unk runtime.Unknown
	switch t := obj.(type) {
	case *runtime.Unknown:
		// 已经是 Unknown，直接编码，不再嵌套一层。
		estimatedSize := prefixSize + uint64(t.Size())
		data := memAlloc.Allocate(estimatedSize)
		i, err := t.MarshalTo(data[prefixSize:])
		if err != nil {
			return err
		}
		copy(data, s.prefix)
		_, err = w.Write(data[:prefixSize+uint64(i)])
		return err
	default:
		// 从对象自身取 GVK；为空时由调用方保证类型信息正确。
		kind := obj.GetObjectKind().GroupVersionKind()
		unk = runtime.Unknown{
			TypeMeta: runtime.TypeMeta{
				Kind:       kind.Kind,
				APIVersion: kind.GroupVersion().String(),
			},
		}
	}
	if s.options.StreamingCollectionsEncoding {
		listData, err := getStreamingListData(obj)
		if err == nil {
			// 该路径不遵循自定义的 proto 序列化方法（如 json streaming），因为所有 proto 对象都实现了 proto 方法；
			// 仅对 list 生效：非 list 时 getStreamingListData 返回错误，继续走普通路径。
			return streamingEncodeUnknownList(w, unk, listData, memAlloc)
		}
	}

	switch t := obj.(type) {
	case bufferedMarshaller:
		// 未提供 Allocator 时，这条路径在写入阶段只分配一次；
		// 它要求调用方实现更高效的 Size 和 MarshalToSizedBuffer 方法。
		encodedSize := uint64(t.Size())
		estimatedSize := prefixSize + estimateUnknownSize(&unk, encodedSize)
		data := memAlloc.Allocate(estimatedSize)

		i, err := unk.NestedMarshalTo(data[prefixSize:], t, encodedSize)
		if err != nil {
			return err
		}

		copy(data, s.prefix)

		_, err = w.Write(data[:prefixSize+uint64(i)])
		return err

	case unbufferedMarshaller:
		// 该接口只能返回完整字节，无法预知大小，因此这条路径多一次分配与拷贝。
		data, err := t.Marshal()
		if err != nil {
			return err
		}
		unk.Raw = data

		estimatedSize := prefixSize + uint64(unk.Size())
		data = memAlloc.Allocate(estimatedSize)

		i, err := unk.MarshalTo(data[prefixSize:])
		if err != nil {
			return err
		}

		copy(data, s.prefix)

		_, err = w.Write(data[:prefixSize+uint64(i)])
		return err

	default:
		// TODO: 用其它 content type 和序列化器处理（第三方对象用 JSON）。
		return errNotMarshalable{reflect.TypeOf(obj)}
	}
}

// Identifier 实现 runtime.Encoder 接口。
func (s *Serializer) Identifier() runtime.Identifier {
	return serializerIdentifier
}

// RecognizesData 实现 RecognizingDecoder 接口。
//
// 第二个返回值恒为 false，表示识别结果不是"确定"的，调用方应继续尝试其他解码器。
func (s *Serializer) RecognizesData(data []byte) (bool, bool, error) {
	return bytes.HasPrefix(data, s.prefix), false, nil
}

// copyKindDefaults 在 dst 未设置值时用 src 的值补齐。
func copyKindDefaults(dst, src *schema.GroupVersionKind) {
	if src == nil {
		return
	}
	// 用给定的默认值补齐 kind 和 version。
	if len(dst.Kind) == 0 {
		dst.Kind = src.Kind
	}
	// 只有 version 缺失时才整体采用 src 的 group/version，避免出现 group 来自
	// 对象、version 来自默认值的非法组合。
	if len(dst.Version) == 0 && len(src.Version) > 0 {
		dst.Group = src.Group
		dst.Version = src.Version
	}
}

// bufferedMarshaller 是更高效的序列化接口，通过预先计算所需缓冲大小，
// 避免多次分配 byte buffer。
type bufferedMarshaller interface {
	runtime.ProtobufMarshaller
}

// 与 bufferedMarshaller 类似，但支持反向序列化，因不必频繁调用 Size() 而更高效。
type bufferedReverseMarshaller interface {
	runtime.ProtobufReverseMarshaller
}

// unbufferedMarshaller 是 gogo protobuf 的基础编码接口，会额外分配内存。
type unbufferedMarshaller interface {
	Marshal() ([]byte, error)
}

// unmarshaler 是反序列化用到的 gogo Message 与 Unmarshaler 接口子集。
type unmarshaler interface {
	// 反序列化前会在顶层消息上调用 Reset()，清除消息实例中的旧数据。
	Reset()
	// Unmarshal 从数据开头开始解码到消息中。
	Unmarshal([]byte) error
}

// estimateUnknownSize 估算 RawJSON 为 nil 的 runtime.Unknown 对象会占用的字节数，
// 以及给定缓冲区的预期大小。若 unk 的 RawJSON 非空，返回值不准确。
func estimateUnknownSize(unk *runtime.Unknown, byteSize uint64) uint64 {
	size := uint64(unk.Size())
	// protobuf 用 1 字节存 tag，用 varint 存数组长度（此处最多 8 字节，即 uint64），
	// 再加上数组本身的大小。
	size += 1 + 8 + byteSize
	return size
}

// NewRawSerializer 创建 Protobuf 序列化器，把带版本的对象编码为正确的 wire 格式。
// typer 非 nil 时会设置对象的 group、version 和 kind。该序列化器不写入类型信息，
// 因此不是自描述的：调用方必须知道所描述的类型才能解码。
//
// 该编码方案尚属实验性质，随时可能变更。
func NewRawSerializer(creater runtime.ObjectCreater, typer runtime.ObjectTyper) *RawSerializer {
	return &RawSerializer{
		creater: creater,
		typer:   typer,
	}
}

// RawSerializer 编码和解码对象时不添加 runtime.Unknown 包装（编码结果不含类型标识）。
//
// 因为没有类型包装，解码时必须由调用方提供目标类型，否则无法确定如何解析。
type RawSerializer struct {
	creater runtime.ObjectCreater
	typer   runtime.ObjectTyper
}

var _ runtime.Serializer = &RawSerializer{}

const rawSerializerIdentifier runtime.Identifier = "raw-protobuf"

// Decode 先将数据转为 protobuf 消息，取出其中保存的 schema kind，套用给定的默认 gvk，
// 再把数据装入目标 schema kind 对应的对象或 into。若 into 是 *runtime.Unknown，只提取原始数据，
// 不做解码；若 into 未注册到 typer，则走常规的 protobuf 反序列化（MarshalTo 接口）直接解码。
// 若提供了 into 且原始数据的 kind/version/group 不完整，会用 into 的类型修正返回的 gvk。
// 成功或大多数错误情况下都会返回计算出的 schema kind。
func (s *RawSerializer) Decode(originalData []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	if into == nil {
		// 裸 protobuf 不自描述，必须知道目标类型。
		return nil, nil, fmt.Errorf("this serializer requires an object to decode into: %#v", s)
	}

	if len(originalData) == 0 {
		// TODO: 参照从 JSON 空对象 {} 解码时的默认值处理。
		return nil, nil, fmt.Errorf("empty data")
	}
	data := originalData

	actual := &schema.GroupVersionKind{}
	copyKindDefaults(actual, gvk)

	if intoUnknown, ok := into.(*runtime.Unknown); ok && intoUnknown != nil {
		// 调用方要原始数据：包成 Unknown 并标注 protobuf 内容类型。
		intoUnknown.Raw = data
		intoUnknown.ContentEncoding = ""
		intoUnknown.ContentType = runtime.ContentTypeProtobuf
		intoUnknown.SetGroupVersionKind(*actual)
		return intoUnknown, actual, nil
	}

	types, _, err := s.typer.ObjectKinds(into)
	switch {
	case runtime.IsNotRegisteredError(err):
		// 未注册类型：直接调用对象自身的 Unmarshal。
		unmarshaler, ok := into.(unmarshaler)
		if !ok {
			return nil, actual, errNotMarshalable{reflect.TypeOf(into)}
		}
		// 顶层反序列化先重置对象，再委托给对象自己处理。
		unmarshaler.Reset()
		if err := unmarshaler.Unmarshal(data); err != nil {
			return nil, actual, err
		}
		return into, actual, nil
	case err != nil:
		return nil, actual, err
	default:
		copyKindDefaults(actual, &types[0])
		// 默认值填充后若仍没有 version 或 group，至少补上 group
		// （copyKindDefaults 在 version 已设置时不会补 Group）。这样在调用方和对象都
		// 没有提供更好信息时，group 也能确定下来。
		if len(actual.Version) == 0 && len(actual.Group) == 0 {
			actual.Group = types[0].Group
		}
	}

	if len(actual.Kind) == 0 {
		return nil, actual, runtime.NewMissingKindErr("<protobuf encoded body - must provide default type>")
	}
	if len(actual.Version) == 0 {
		return nil, actual, runtime.NewMissingVersionErr("<protobuf encoded body - must provide default type>")
	}

	return unmarshalToObject(s.typer, s.creater, actual, into, data)
}

// unmarshalToObject 是 raw 序列化器与普通序列化器解码逻辑的公共部分。
//
// 复用调用方提供的 into（若类型匹配），否则通过 creater 新建对象，再把数据反序列化进去。
func unmarshalToObject(typer runtime.ObjectTyper, creater runtime.ObjectCreater, actual *schema.GroupVersionKind, into runtime.Object, data []byte) (runtime.Object, *schema.GroupVersionKind, error) {
	// 必要时使用目标对象。
	obj, err := runtime.UseOrCreateObject(typer, creater, *actual, into)
	if err != nil {
		return nil, actual, err
	}

	unmarshaler, ok := obj.(unmarshaler)
	if !ok {
		return nil, actual, errNotMarshalable{reflect.TypeOf(obj)}
	}
	// 顶层反序列化先重置对象，再委托给对象自己处理。
	unmarshaler.Reset()
	if err := unmarshaler.Unmarshal(data); err != nil {
		return nil, actual, err
	}
	if actual != nil {
		obj.GetObjectKind().SetGroupVersionKind(*actual)
	}
	return obj, actual, nil
}

// Encode 把对象序列化后写入给定的 writer，Overrides 参数会被忽略。
func (s *RawSerializer) Encode(obj runtime.Object, w io.Writer) error {
	return s.encode(obj, w, &runtime.SimpleAllocator{})
}

// EncodeWithAllocator 把对象写入给定的 writer，并允许提供内存分配器以降低序列化时的内存开销。
func (s *RawSerializer) EncodeWithAllocator(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	return s.encode(obj, w, memAlloc)
}

// encode 处理 CacheableObject 的缓存命中后执行编码。
func (s *RawSerializer) encode(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	if co, ok := obj.(runtime.CacheableObject); ok {
		return co.CacheEncode(s.Identifier(), func(obj runtime.Object, w io.Writer) error { return s.doEncode(obj, w, memAlloc) }, w)
	}
	return s.doEncode(obj, w, memAlloc)
}

// doEncode 以 header=nil 调用通用编码逻辑，即不写长度前缀。
func (s *RawSerializer) doEncode(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	_, err := doEncode(obj, w, nil, memAlloc)
	return err
}

// doEncodeWithHeader 先写 field 标识与长度，再写对象本身。
//
// 主要用于流式编码列表：每个元素是一个 length-delimited 的 protobuf 字段。
func doEncodeWithHeader(obj any, w io.Writer, field byte, precomputedSize int, headerScratch []byte, memAlloc runtime.MemoryAllocator) (size int, err error) {
	// 字段标识与长度
	header := headerScratch[:1+sovGenerated(uint64(precomputedSize))]
	header[0] = field
	encodeVarintGenerated(header, len(header), uint64(precomputedSize))
	n, err := w.Write(header)
	size += n
	if err != nil {
		return size, err
	}
	// 对象内容
	n, err = doEncode(obj, w, &precomputedSize, memAlloc)
	size += n
	if err != nil {
		return size, err
	}
	// 写入字节数与预计算值不符说明 Size()/MarshalTo 实现不一致，
	// 继续写下去会产生损坏的流，必须立即报错。
	if n != precomputedSize {
		return size, fmt.Errorf("the size value was %d, but doEncode wrote %d bytes to data", precomputedSize, n)
	}
	return size, nil
}

// doEncode 尽可能使用分配器把对象编码写入 writer。
// 提供了 precomputedObjSize 时不再调用对象的 Size()。
// precomputedObjSize 不应包含头部字节（字段标识与长度）。
func doEncode(obj any, w io.Writer, precomputedObjSize *int, memAlloc runtime.MemoryAllocator) (int, error) {
	if memAlloc == nil {
		//nolint:logcheck // Should not be reached in normal operations.
		klog.Error("a mandatory memory allocator wasn't provided, this might have a negative impact on performance, check invocations of EncodeWithAllocator method, falling back on runtime.SimpleAllocator")
		memAlloc = &runtime.SimpleAllocator{}
	}
	switch t := obj.(type) {
	case bufferedReverseMarshaller:
		// 未提供 Allocator 时，这条路径在写入阶段只分配一次；
		// 它要求调用方实现更高效的 Size 和 MarshalToSizedBuffer 方法。
		if precomputedObjSize == nil {
			s := t.Size()
			precomputedObjSize = &s
		}
		data := memAlloc.Allocate(uint64(*precomputedObjSize))

		// 反向编码从缓冲区末尾向前写，省去频繁调用 Size()。
		n, err := t.MarshalToSizedBuffer(data)
		if err != nil {
			return 0, err
		}
		return w.Write(data[:n])

	case bufferedMarshaller:
		// 未提供 Allocator 时，这条路径在写入阶段只分配一次；
		// 同时要求调用方实现更高效的 Size 和 MarshalTo 方法。
		if precomputedObjSize == nil {
			s := t.Size()
			precomputedObjSize = &s
		}
		data := memAlloc.Allocate(uint64(*precomputedObjSize))

		n, err := t.MarshalTo(data)
		if err != nil {
			return 0, err
		}
		return w.Write(data[:n])

	case unbufferedMarshaller:
		// 这条路径会产生额外分配。
		data, err := t.Marshal()
		if err != nil {
			return 0, err
		}
		return w.Write(data)

	default:
		return 0, errNotMarshalable{reflect.TypeOf(obj)}
	}
}

// Identifier 实现 runtime.Encoder 接口。
func (s *RawSerializer) Identifier() runtime.Identifier {
	return rawSerializerIdentifier
}

// LengthDelimitedFramer 是 lengthDelimitedFramer 类型的导出变量。
var LengthDelimitedFramer = lengthDelimitedFramer{}

// 提供带长度前缀的帧读取和写入方法。
type lengthDelimitedFramer struct{}

// NewFrameWriter 为该序列化器实现流式分帧。
func (lengthDelimitedFramer) NewFrameWriter(w io.Writer) io.Writer {
	return framer.NewLengthDelimitedFrameWriter(w)
}

// NewFrameReader 为该序列化器实现流式分帧。
func (lengthDelimitedFramer) NewFrameReader(r io.ReadCloser) io.ReadCloser {
	return framer.NewLengthDelimitedFrameReader(r)
}
