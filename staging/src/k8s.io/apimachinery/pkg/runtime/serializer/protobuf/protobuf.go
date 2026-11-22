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
	// protoEncodingPrefix serves as a magic number for an encoded protobuf message on this serializer. All
	// proto messages serialized by this schema will be preceded by the bytes 0x6b 0x38 0x73, with the fourth
	// byte being reserved for the encoding style. The only encoding style defined is 0x00, which means that
	// the rest of the byte stream is a message of type k8s.io.kubernetes.pkg.runtime.Unknown (proto2).
	//
	// See k8s.io/apimachinery/pkg/runtime/generated.proto for details of the runtime.Unknown message.
	//
	// This encoding scheme is experimental, and is subject to change at any time.
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

// IsNotMarshalable checks the type of error, returns a boolean true if error is not nil and not marshalable false otherwise
func IsNotMarshalable(err error) bool {
	_, ok := err.(errNotMarshalable)
	return err != nil && ok
}

// NewSerializer creates a Protobuf serializer that handles encoding versioned objects into the proper wire form. If a typer
// is passed, the encoded object will have group, version, and kind fields set. If typer is nil, the objects will be written
// as-is (any type info passed with the object will be used).
func NewSerializer(creater runtime.ObjectCreater, typer runtime.ObjectTyper) *Serializer {
	return NewSerializerWithOptions(creater, typer, SerializerOptions{})
}

// NewSerializerWithOptions creates a Protobuf serializer that handles encoding versioned objects into the proper wire form. If a typer
// is passed, the encoded object will have group, version, and kind fields set. If typer is nil, the objects will be written
// as-is (any type info passed with the object will be used).
func NewSerializerWithOptions(creater runtime.ObjectCreater, typer runtime.ObjectTyper, opts SerializerOptions) *Serializer {
	return &Serializer{
		prefix:  protoEncodingPrefix,
		creater: creater,
		typer:   typer,
		options: opts,
	}
}

// Serializer handles encoding versioned objects into the proper wire form
//
// Serializer 把对象包进 runtime.Unknown 再编码，因此输出自带类型信息（自描述）。
type Serializer struct {
	prefix  []byte
	creater runtime.ObjectCreater
	typer   runtime.ObjectTyper

	options SerializerOptions
}

// SerializerOptions holds the options which are used to configure a Proto serializer.
type SerializerOptions struct {
	// StreamingCollectionsEncoding enables encoding collection, one item at the time, drastically reducing memory needed.
	//
	// 开启后 list 会逐项流式编码，避免把整个列表物化为一个巨大的 runtime.Unknown。
	StreamingCollectionsEncoding bool
}

var _ runtime.Serializer = &Serializer{}
var _ runtime.EncoderWithAllocator = &Serializer{}
var _ recognizer.RecognizingDecoder = &Serializer{}

const serializerIdentifier runtime.Identifier = "protobuf"

// Decode attempts to convert the provided data into a protobuf message, extract the stored schema kind, apply the provided default
// gvk, and then load that data into an object matching the desired schema kind or the provided into. If into is *runtime.Unknown,
// the raw data will be extracted and no decoding will be performed. If into is not registered with the typer, then the object will
// be straight decoded using normal protobuf unmarshalling (the MarshalTo interface). If into is provided and the original data is
// not fully qualified with kind/version/group, the type of the into will be used to alter the returned gvk. On success or most
// errors, the method will return the calculated schema kind.
func (s *Serializer) Decode(originalData []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	prefixLen := len(s.prefix)
	// 先做前缀与长度检查，避免把明显不是本格式的数据交给 protobuf 解析器。
	switch {
	case len(originalData) == 0:
		// TODO: treat like decoding {} from JSON with defaulting
		return nil, nil, fmt.Errorf("empty data")
	case len(originalData) < prefixLen || !bytes.Equal(s.prefix, originalData[:prefixLen]):
		return nil, nil, fmt.Errorf("provided data does not appear to be a protobuf message, expected prefix %v", s.prefix)
	case len(originalData) == prefixLen:
		// TODO: treat like decoding {} from JSON with defaulting
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
			// top-level unmarshal resets before delegating unmarshaling to the object
			unmarshaler.Reset()
			if err := unmarshaler.Unmarshal(unk.Raw); err != nil {
				return nil, &actual, err
			}
			return into, &actual, nil
		case err != nil:
			return nil, &actual, err
		default:
			copyKindDefaults(&actual, &types[0])
			// if the result of defaulting did not set a version or group, ensure that at least group is set
			// (copyKindDefaults will not assign Group if version is already set). This guarantees that the group
			// of into is set if there is no better information from the caller or object.
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

// EncodeWithAllocator writes an object to the provided writer.
// In addition, it allows for providing a memory allocator for efficient memory usage during object serialization.
func (s *Serializer) EncodeWithAllocator(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	return s.encode(obj, w, memAlloc)
}

// Encode serializes the provided object to the given writer.
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
			// Doesn't honor custom proto marshaling methods (like json streaming), because all proto objects implement proto methods.
			// 仅对 list 生效；非 list 时 getStreamingListData 返回错误，继续走普通路径。
			return streamingEncodeUnknownList(w, unk, listData, memAlloc)
		}
	}

	switch t := obj.(type) {
	case bufferedMarshaller:
		// this path performs a single allocation during write only when the Allocator wasn't provided
		// it also requires the caller to implement the more efficient Size and MarshalToSizedBuffer methods
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
		// this path performs extra allocations
		// 该接口只能返回完整字节，无法预知大小，因此多一次分配与拷贝。
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
		// TODO: marshal with a different content type and serializer (JSON for third party objects)
		return errNotMarshalable{reflect.TypeOf(obj)}
	}
}

// Identifier implements runtime.Encoder interface.
func (s *Serializer) Identifier() runtime.Identifier {
	return serializerIdentifier
}

// RecognizesData implements the RecognizingDecoder interface.
//
// 第二个返回值恒为 false，表示识别结果不是"确定"的，调用方应继续尝试其他解码器。
func (s *Serializer) RecognizesData(data []byte) (bool, bool, error) {
	return bytes.HasPrefix(data, s.prefix), false, nil
}

// copyKindDefaults defaults dst to the value in src if dst does not have a value set.
func copyKindDefaults(dst, src *schema.GroupVersionKind) {
	if src == nil {
		return
	}
	// apply kind and version defaulting from provided default
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

// bufferedMarshaller describes a more efficient marshalling interface that can avoid allocating multiple
// byte buffers by pre-calculating the size of the final buffer needed.
type bufferedMarshaller interface {
	runtime.ProtobufMarshaller
}

// Like bufferedMarshaller, but is able to marshal backwards, which is more efficient since it doesn't call Size() as frequently.
type bufferedReverseMarshaller interface {
	runtime.ProtobufReverseMarshaller
}

// unbufferedMarshaller 是 gogo protobuf 的基础编码接口，会额外分配内存。
type unbufferedMarshaller interface {
	Marshal() ([]byte, error)
}

// unmarshaler is the subset of gogo Message and Unmarshaler used by unmarshal
type unmarshaler interface {
	// Reset() is called on the top-level message before unmarshaling,
	// and clears all existing data from the message instance.
	Reset()
	// Unmarshal decodes from the start of the data into the message.
	Unmarshal([]byte) error
}

// estimateUnknownSize returns the expected bytes consumed by a given runtime.Unknown
// object with a nil RawJSON struct and the expected size of the provided buffer. The
// returned size will not be correct if RawJSOn is set on unk.
func estimateUnknownSize(unk *runtime.Unknown, byteSize uint64) uint64 {
	size := uint64(unk.Size())
	// protobuf uses 1 byte for the tag, a varint for the length of the array (at most 8 bytes - uint64 - here),
	// and the size of the array.
	size += 1 + 8 + byteSize
	return size
}

// NewRawSerializer creates a Protobuf serializer that handles encoding versioned objects into the proper wire form. If typer
// is not nil, the object has the group, version, and kind fields set. This serializer does not provide type information for the
// encoded object, and thus is not self describing (callers must know what type is being described in order to decode).
//
// This encoding scheme is experimental, and is subject to change at any time.
func NewRawSerializer(creater runtime.ObjectCreater, typer runtime.ObjectTyper) *RawSerializer {
	return &RawSerializer{
		creater: creater,
		typer:   typer,
	}
}

// RawSerializer encodes and decodes objects without adding a runtime.Unknown wrapper (objects are encoded without identifying
// type).
//
// 因为没有类型包装，解码时必须由调用方提供目标类型，否则无法确定如何解析。
type RawSerializer struct {
	creater runtime.ObjectCreater
	typer   runtime.ObjectTyper
}

var _ runtime.Serializer = &RawSerializer{}

const rawSerializerIdentifier runtime.Identifier = "raw-protobuf"

// Decode attempts to convert the provided data into a protobuf message, extract the stored schema kind, apply the provided default
// gvk, and then load that data into an object matching the desired schema kind or the provided into. If into is *runtime.Unknown,
// the raw data will be extracted and no decoding will be performed. If into is not registered with the typer, then the object will
// be straight decoded using normal protobuf unmarshalling (the MarshalTo interface). If into is provided and the original data is
// not fully qualified with kind/version/group, the type of the into will be used to alter the returned gvk. On success or most
// errors, the method will return the calculated schema kind.
func (s *RawSerializer) Decode(originalData []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	if into == nil {
		// 裸 protobuf 不自描述，必须知道目标类型。
		return nil, nil, fmt.Errorf("this serializer requires an object to decode into: %#v", s)
	}

	if len(originalData) == 0 {
		// TODO: treat like decoding {} from JSON with defaulting
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
		// top-level unmarshal resets before delegating unmarshaling to the object
		unmarshaler.Reset()
		if err := unmarshaler.Unmarshal(data); err != nil {
			return nil, actual, err
		}
		return into, actual, nil
	case err != nil:
		return nil, actual, err
	default:
		copyKindDefaults(actual, &types[0])
		// if the result of defaulting did not set a version or group, ensure that at least group is set
		// (copyKindDefaults will not assign Group if version is already set). This guarantees that the group
		// of into is set if there is no better information from the caller or object.
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

// unmarshalToObject is the common code between decode in the raw and normal serializer.
//
// 复用调用方提供的 into（若类型匹配），否则通过 creater 新建对象，再把数据反序列化进去。
func unmarshalToObject(typer runtime.ObjectTyper, creater runtime.ObjectCreater, actual *schema.GroupVersionKind, into runtime.Object, data []byte) (runtime.Object, *schema.GroupVersionKind, error) {
	// use the target if necessary
	obj, err := runtime.UseOrCreateObject(typer, creater, *actual, into)
	if err != nil {
		return nil, actual, err
	}

	unmarshaler, ok := obj.(unmarshaler)
	if !ok {
		return nil, actual, errNotMarshalable{reflect.TypeOf(obj)}
	}
	// top-level unmarshal resets before delegating unmarshaling to the object
	unmarshaler.Reset()
	if err := unmarshaler.Unmarshal(data); err != nil {
		return nil, actual, err
	}
	if actual != nil {
		obj.GetObjectKind().SetGroupVersionKind(*actual)
	}
	return obj, actual, nil
}

// Encode serializes the provided object to the given writer. Overrides is ignored.
func (s *RawSerializer) Encode(obj runtime.Object, w io.Writer) error {
	return s.encode(obj, w, &runtime.SimpleAllocator{})
}

// EncodeWithAllocator writes an object to the provided writer.
// In addition, it allows for providing a memory allocator for efficient memory usage during object serialization.
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
	// Field identifier and size
	header := headerScratch[:1+sovGenerated(uint64(precomputedSize))]
	header[0] = field
	encodeVarintGenerated(header, len(header), uint64(precomputedSize))
	n, err := w.Write(header)
	size += n
	if err != nil {
		return size, err
	}
	// Obj
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

// doEncode encodes provided object into writer using a allocator if possible.
// Avoids call by object Size if precomputedObjSize is provided.
// precomputedObjSize should not include header bytes (field identifier, size).
func doEncode(obj any, w io.Writer, precomputedObjSize *int, memAlloc runtime.MemoryAllocator) (int, error) {
	if memAlloc == nil {
		//nolint:logcheck // Should not be reached in normal operations.
		klog.Error("a mandatory memory allocator wasn't provided, this might have a negative impact on performance, check invocations of EncodeWithAllocator method, falling back on runtime.SimpleAllocator")
		memAlloc = &runtime.SimpleAllocator{}
	}
	switch t := obj.(type) {
	case bufferedReverseMarshaller:
		// this path performs a single allocation during write only when the Allocator wasn't provided
		// it also requires the caller to implement the more efficient Size and MarshalToSizedBuffer methods
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
		// this path performs a single allocation during write only when the Allocator wasn't provided
		// it also requires the caller to implement the more efficient Size and MarshalTo methods
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
		// this path performs extra allocations
		data, err := t.Marshal()
		if err != nil {
			return 0, err
		}
		return w.Write(data)

	default:
		return 0, errNotMarshalable{reflect.TypeOf(obj)}
	}
}

// Identifier implements runtime.Encoder interface.
func (s *RawSerializer) Identifier() runtime.Identifier {
	return rawSerializerIdentifier
}

// LengthDelimitedFramer is exported variable of type lengthDelimitedFramer
var LengthDelimitedFramer = lengthDelimitedFramer{}

// Provides length delimited frame reader and writer methods
type lengthDelimitedFramer struct{}

// NewFrameWriter implements stream framing for this serializer
func (lengthDelimitedFramer) NewFrameWriter(w io.Writer) io.Writer {
	return framer.NewLengthDelimitedFrameWriter(w)
}

// NewFrameReader implements stream framing for this serializer
func (lengthDelimitedFramer) NewFrameReader(r io.ReadCloser) io.ReadCloser {
	return framer.NewLengthDelimitedFrameReader(r)
}
