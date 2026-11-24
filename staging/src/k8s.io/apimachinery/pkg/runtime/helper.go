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
	"fmt"
	"io"
	"reflect"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/errors"
)

// unsafeObjectConvertor 使用不安全转换路径实现 ObjectConvertor。
// 它将转换委托给 Scheme.UnsafeConvertToVersion；输入与输出可能共享内存，调用方必须保证
// 不会在转换后修改或复用相关对象。
type unsafeObjectConvertor struct {
	*Scheme
}

var _ ObjectConvertor = unsafeObjectConvertor{}

// 在不复制输入对象的情况下，将对象转换到 outVersion。转换结果可能与输入对象共享底层字段；调用方不得修改、复用输出对象并假设输入保持独立。
func (c unsafeObjectConvertor) ConvertToVersion(in Object, outVersion GroupVersioner) (Object, error) {
	return c.Scheme.UnsafeConvertToVersion(in, outVersion)
}

// 创建使用 Scheme.UnsafeConvertToVersion 的 ObjectConvertor。该实现省去输入对象深拷贝，主要用于编码后不会把转换对象交还给调用方的版本化编解码流程。
func UnsafeObjectConvertor(scheme *Scheme) ObjectConvertor {
	return unsafeObjectConvertor{scheme}
}

// SetField 将 src 的值写入 v 中名为 fieldName 的字段。字段必须存在，且 src 的类型必须可赋值或可转换为字段类型。
// 方法先尝试直接赋值，再尝试反射类型转换；两种方式都不可行时返回错误。
func SetField(src interface{}, v reflect.Value, fieldName string) error {
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return fmt.Errorf("couldn't find %v field in %T", fieldName, v.Interface())
	}
	srcValue := reflect.ValueOf(src)
	if srcValue.Type().AssignableTo(field.Type()) {
		field.Set(srcValue)
		return nil
	}
	if srcValue.Type().ConvertibleTo(field.Type()) {
		field.Set(srcValue.Convert(field.Type()))
		return nil
	}
	return fmt.Errorf("couldn't assign/convert %v to %v", srcValue.Type(), field.Type())
}

// 按字段名读取 v 中的字段，并将字段值写入 dest 指向的变量。先检查字段是否存在，再验证目标类型是否可直接赋值；若不可赋值但可转换，则执行反射类型转换。
func Field(v reflect.Value, fieldName string, dest interface{}) error {
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return fmt.Errorf("couldn't find %v field in %T", fieldName, v.Interface())
	}
	// dest 必须是非 nil 指针，否则无法写回。
	destValue, err := conversion.EnforcePtr(dest)
	if err != nil {
		return err
	}
	if field.Type().AssignableTo(destValue.Type()) {
		destValue.Set(field)
		return nil
	}
	if field.Type().ConvertibleTo(destValue.Type()) {
		destValue.Set(field.Convert(destValue.Type()))
		return nil
	}
	return fmt.Errorf("couldn't assign/convert %v to %v", field.Type(), destValue.Type())
}

// 按字段名取得 v 中字段的地址，并将该指针写入 dest 指向的变量。与 Field 不同，本函数传递的是字段指针而不是字段值。
func FieldPtr(v reflect.Value, fieldName string, dest interface{}) error {
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return fmt.Errorf("couldn't find %v field in %T", fieldName, v.Interface())
	}
	v, err := conversion.EnforcePtr(dest)
	if err != nil {
		return err
	}
	// 取字段地址后再做赋值/转换，目标是存放指针的变量。
	field = field.Addr()
	if field.Type().AssignableTo(v.Type()) {
		v.Set(field)
		return nil
	}
	if field.Type().ConvertibleTo(v.Type()) {
		v.Set(field.Convert(v.Type()))
		return nil
	}
	return fmt.Errorf("couldn't assign/convert %v to %v", field.Type(), v.Type())
}

// 逐个编码 objects 中的对象，并用包含原始序列化字节的 Unknown 替换成功编码的元素。单个元素失败不会中止后续处理，所有错误会在遍历结束后聚合返回。
func EncodeList(e Encoder, objects []Object) error {
	var errs []error
	for i := range objects {
		data, err := Encode(e, objects[i])
		if err != nil {
			// 单个对象编码失败不影响其余对象，最后统一聚合错误。
			errs = append(errs, err)
			continue
		}
		// TODO: Set ContentEncoding and ContentType.
		objects[i] = &Unknown{Raw: data}
	}
	return errors.NewAggregate(errs)
}

// decodeListItem 依次尝试 decoder 解码 Unknown 中的原始字节。遇到
// NotRegisteredError 时换下一个 decoder；其它错误立即返回。全部解码器都
// 不认识该类型时，再尝试把 TypeMeta 解出到 obj 上并保留为 Unknown。
func decodeListItem(obj *Unknown, decoders []Decoder) (Object, error) {
	for _, decoder := range decoders {
		// TODO: Decode based on ContentType.
		obj, err := Decode(decoder, obj.Raw)
		if err != nil {
			if IsNotRegisteredError(err) {
				continue
			}
			return nil, err
		}
		return obj, nil
	}
	// 所有 decoder 都无法识别该对象时，保留 Unknown 原始数据；再给 decoder 一次机会，
	// 尝试仅解析 TypeMeta，以便至少保留对象的 GVK 信息。
	for _, decoder := range decoders {
		if err := DecodeInto(decoder, obj.Raw, obj); err == nil {
			return obj, nil
		}
	}
	return obj, nil
}

// DecodeList 原地处理列表中类型为 Unknown 的元素，并按传入顺序逐个尝试 decoder。
// 非 Unknown 元素保持不变；某个元素解码失败不会阻止后续元素处理，所有错误会在遍历结束后统一返回。
func DecodeList(objects []Object, decoders ...Decoder) []error {
	errs := []error(nil)
	for i, obj := range objects {
		switch t := obj.(type) {
		case *Unknown:
			decoded, err := decodeListItem(t, decoders)
			if err != nil {
				errs = append(errs, err)
				break
			}
			objects[i] = decoded
		}
	}
	return errs
}

// MultiObjectTyper 按顺序组合多个 ObjectTyper。查询对象类型时依次尝试各 typer，返回第一个成功结果；
// 判断 GVK 是否已知时，只要任意一个 typer 能识别就返回 true。适合组合多个 Scheme 的编解码器。
type MultiObjectTyper []ObjectTyper

var _ ObjectTyper = MultiObjectTyper{}

func (m MultiObjectTyper) ObjectKinds(obj Object) (gvks []schema.GroupVersionKind, unversionedType bool, err error) {
	for _, t := range m {
		gvks, unversionedType, err = t.ObjectKinds(obj)
		if err == nil {
			return
		}
	}
	return
}

func (m MultiObjectTyper) Recognizes(gvk schema.GroupVersionKind) bool {
	for _, t := range m {
		if t.Recognizes(gvk) {
			return true
		}
	}
	return false
}

// 将 objPtr 指向的对象重置为其 Go 类型的零值。首先验证参数是有效指针，再通过 reflect.Zero 构造对应零值并原地写回。
func SetZeroValue(objPtr Object) error {
	v, err := conversion.EnforcePtr(objPtr)
	if err != nil {
		return err
	}
	v.Set(reflect.Zero(v.Type()))
	return nil
}

// DefaultFramer 适用于对象在流中连续排列、且协议本身不需要额外分隔符的场景。
// 它不添加或解析帧边界，读写两端都直接透传底层流。
var DefaultFramer = defaultFramer{}

type defaultFramer struct{}

func (defaultFramer) NewFrameReader(r io.ReadCloser) io.ReadCloser { return r }
func (defaultFramer) NewFrameWriter(w io.Writer) io.Writer         { return w }

// WithVersionEncoder 在序列化对象时确保 GVK 设置为目标版本，但不转换对象的字段结构。
// 编码前临时设置 GVK，编码结束后恢复原值，避免修改调用方持有的对象。
type WithVersionEncoder struct {
	Version GroupVersioner
	Encoder
	ObjectTyper
}

// 编码对象时临时设置目标 GVK，但不执行对象结构的版本转换。若对象类型未注册，则交由底层 Encoder 按对象自身信息处理；编码结束后恢复原 GVK，避免修改调用方对象。
func (e WithVersionEncoder) Encode(obj Object, stream io.Writer) error {
	gvks, _, err := e.ObjectTyper.ObjectKinds(obj)
	if err != nil {
		// 未注册类型无法推断 GVK，直接交给底层 encoder，可能仍能按对象
		// 自带的 TypeMeta 序列化。
		if IsNotRegisteredError(err) {
			return e.Encoder.Encode(obj, stream)
		}
		return err
	}
	kind := obj.GetObjectKind()
	oldGVK := kind.GroupVersionKind()
	gvk := gvks[0]
	if e.Version != nil {
		preferredGVK, ok := e.Version.KindForGroupVersionKinds(gvks)
		if ok {
			gvk = preferredGVK
		}
	}

	// 只有当前 GVK 与目标 GVK 不一致时才临时修改，避免不必要的写入。
	if gvk != oldGVK {
		kind.SetGroupVersionKind(gvk)
		defer kind.SetGroupVersionKind(oldGVK)
	}

	return e.Encoder.Encode(obj, stream)
}

// WithoutVersionDecoder 在对象反序列化完成后清除对象上的 GVK。
type WithoutVersionDecoder struct {
	Decoder
}

// Decode 不执行版本转换，只在底层解码完成后清除返回对象上的 GVK。
func (d WithoutVersionDecoder) Decode(data []byte, defaults *schema.GroupVersionKind, into Object) (Object, *schema.GroupVersionKind, error) {
	obj, gvk, err := d.Decoder.Decode(data, defaults, into)
	if obj != nil {
		kind := obj.GetObjectKind()
		// clearing the gvk is just a convention of a codec
		kind.SetGroupVersionKind(schema.GroupVersionKind{})
	}
	return obj, gvk, err
}

// encoderWithAllocator 把支持内存分配器的 encoder 适配成普通 Encoder，
// 供只接受 Encoder 的调用方使用。
type encoderWithAllocator struct {
	encoder      EncoderWithAllocator
	memAllocator MemoryAllocator
}

// 将支持 EncoderWithAllocator 的实现包装为普通 Encoder，并固定关联 MemoryAllocator。后续 Encode 调用会把该分配器传递给底层实现。
func NewEncoderWithAllocator(e EncoderWithAllocator, a MemoryAllocator) Encoder {
	return &encoderWithAllocator{
		encoder:      e,
		memAllocator: a,
	}
}

// Encode 将对象交给底层 EncoderWithAllocator，并把配置的分配器一并传入。
func (e *encoderWithAllocator) Encode(obj Object, w io.Writer) error {
	return e.encoder.EncodeWithAllocator(obj, w, e.memAllocator)
}

// Identifier 返回底层编码器的标识符，保持缓存键语义与底层实现一致。
func (e *encoderWithAllocator) Identifier() Identifier {
	return e.encoder.Identifier()
}

// The legacy discovery endpoint requires that its response Encoder implement Serializer.
// https://github.com/kubernetes/kubernetes/blob/4a1340bfd58fdb3846d4342c101e0bcb574fbfb1/staging/src/k8s.io/apiserver/pkg/endpoints/discovery/util.go#L101-L107
var _ Serializer = nondeterministicEncoderToEncoderAdapter{}

// nondeterministicEncoderToEncoderAdapter 把 NondeterministicEncoder 适配成
// Serializer，使 discovery 等旧接口可以在不保证输出稳定的前提下复用。
type nondeterministicEncoderToEncoderAdapter struct {
	NondeterministicEncoder

	Decoder
}

func (e nondeterministicEncoderToEncoderAdapter) Encode(obj Object, w io.Writer) error {
	return e.EncodeNondeterministic(obj, w)
}

// 若 Serializer 同时实现 NondeterministicEncoder，则返回适配器并使用非确定性编码；否则原样返回 Serializer。非确定性编码可省去排序开销，但相同对象不保证产生字节完全相同的输出。
func UseNondeterministicEncoding(serializer Serializer) Encoder {
	if nondeterministic, ok := serializer.(NondeterministicEncoder); ok {
		return nondeterministicEncoderToEncoderAdapter{NondeterministicEncoder: nondeterministic, Decoder: serializer}
	}
	return serializer
}
