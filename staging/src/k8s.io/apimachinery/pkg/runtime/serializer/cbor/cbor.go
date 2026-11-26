/*
Copyright 2024 The Kubernetes Authors.

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

package cbor

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor/internal/modes"
	"k8s.io/apimachinery/pkg/runtime/serializer/recognizer"
	util "k8s.io/apimachinery/pkg/util/runtime"

	"github.com/fxamacker/cbor/v2"
)

// metaFactory 从 wire 字节中推断对象的 GroupVersionKind。
type metaFactory interface {
	// Interpret 返回对象 wire 格式中的 version 与 kind。
	Interpret(data []byte) (*schema.GroupVersionKind, error)
}

// defaultMetaFactory 通过宽松解码 TypeMeta 来推断 GVK。
type defaultMetaFactory struct{}

// Interpret 用宽松模式把 data 解码进 metav1.TypeMeta。
// 输入通常还包含 apiVersion/kind 之外的 map 键，因此必须用宽松模式，否则这些未知键会被拒绝。
func (mf *defaultMetaFactory) Interpret(data []byte) (*schema.GroupVersionKind, error) {
	var tm metav1.TypeMeta
	if err := modes.DecodeLax.Unmarshal(data, &tm); err != nil {
		return nil, fmt.Errorf("unable to determine group/version/kind: %w", err)
	}
	actual := tm.GetObjectKind().GroupVersionKind()
	return &actual, nil
}

// Serializer 是 CBOR 序列化器接口，组合了序列化、非确定性编码和识别解码能力。
type Serializer interface {
	runtime.Serializer
	runtime.NondeterministicEncoder
	recognizer.RecognizingDecoder

	// NewSerializer 返回该接口类型的值，而不是导出 serializer 类型再返回它的实例，
	// 因为 serializer 的零值不可直接使用。调用方不应自行实现 cbor.Serializer；
	// 这个未导出的接口方法正是为了阻止外部实现（见 https://go.dev/blog/module-compatibility）。
	private()
}

var _ Serializer = &serializer{}

// options 是 serializer 的可选配置集合。
type options struct {
	strict                       bool
	transcode                    bool
	streamingCollectionsEncoding bool
}

// Option 用于配置 serializer，采用函数式选项模式，便于后续新增配置而不破坏调用方。
type Option func(*options)

// Strict 配置 serializer 在解码遇到目标对象不存在的 map 键时返回严格解码错误。
// 该选项默认关闭。
func Strict(s bool) Option {
	return func(opts *options) {
		opts.strict = s
	}
}

// Transcode 配置 serializer 将解码得到的 runtime.RawExtension 或 metav1.FieldsV1 对象的
// "raw" 字节转码为 JSON。默认开启，以兼容那些假定这两种类型包含合法 JSON 的既有程序。
func Transcode(s bool) Option {
	return func(opts *options) {
		opts.transcode = s
	}
}

// StreamingCollectionsEncoding 仅用于测试。
func StreamingCollectionsEncoding(s bool) Option {
	return func(opts *options) {
		opts.streamingCollectionsEncoding = s
	}
}

// serializer 是 CBOR Serializer 的具体实现。
type serializer struct {
	metaFactory metaFactory
	creater     runtime.ObjectCreater
	typer       runtime.ObjectTyper
	options     options
}

// private 是阻止外部实现 Serializer 的哨兵方法。
func (serializer) private() {}

// NewSerializer 创建并返回按给定选项配置的 serializer。
// 默认选项等价于显式传入 Strict(false) 和 Transcode(true)。
func NewSerializer(creater runtime.ObjectCreater, typer runtime.ObjectTyper, options ...Option) Serializer {
	return newSerializer(&defaultMetaFactory{}, creater, typer, options...)
}

// newSerializer 是带可注入 metaFactory 的内部构造器（测试用）。
func newSerializer(metaFactory metaFactory, creater runtime.ObjectCreater, typer runtime.ObjectTyper, options ...Option) *serializer {
	s := &serializer{
		metaFactory: metaFactory,
		creater:     creater,
		typer:       typer,
	}
	// 这两个默认值就是“默认选项”的具体来源；随后用户选项可在其上覆盖。
	s.options.transcode = true
	s.options.streamingCollectionsEncoding = true
	for _, o := range options {
		o(&s.options)
	}
	return s
}

// Identifier 返回序列化器标识，用于编码缓存去重。
func (s *serializer) Identifier() runtime.Identifier {
	return "cbor"
}

// Encode 写出给定对象的 CBOR 表示。
//
// 由于 Encode 写出的数据总是包在 "self-described CBOR" tag 中，其编码形式始终以
// 0xd9d9f7 开头；该前缀可用作“魔数”来区分 CBOR 与其它协议。
//
// 对任意对象的默认序列化行为尽可能复刻 JSON 序列化器，使 CBOR 可以低改动地替换 JSON；
// 仅有少量例外，例如 CBOR 对整数与浮点数有不同表示，因此能保留两者的区别。
//
// 实现了 runtime.Unstructured 的对象将编码其 unstructured 内容，而不是其动态类型的字段。
func (s *serializer) Encode(obj runtime.Object, w io.Writer) error {
	return s.encode(modes.Encode, obj, w)
}

// EncodeNondeterministic 用非确定性编码模式输出，允许 map 键顺序等不稳定的表示。
func (s *serializer) EncodeNondeterministic(obj runtime.Object, w io.Writer) error {
	return s.encode(modes.EncodeNondeterministic, obj, w)
}

// encode 是 Encode 与 EncodeNondeterministic 的公共实现。
func (s *serializer) encode(mode modes.EncMode, obj runtime.Object, w io.Writer) error {
	// 先写 self-described tag，保证输出可被 RecognizesData 识别为 CBOR。
	if _, err := w.Write(selfDescribedCBOR); err != nil {
		return err
	}

	// 列表走流式编码可避免整体组装中间结构；失败则回退到通用编码。
	if s.options.streamingCollectionsEncoding {
		ok, err := streamEncodeCollections(obj, w, mode)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}

	var v interface{} = obj
	// Unstructured 对象应编码其内容 map，而不是对象的 Go 字段。
	if u, ok := obj.(runtime.Unstructured); ok {
		v = u.UnstructuredContent()
	}

	return mode.MarshalTo(v, w)
}

// gvkWithDefaults 用给定的 defaultGVK 为 actual 中缺失的 group/version/kind 补默认值。
func gvkWithDefaults(actual, defaultGVK schema.GroupVersionKind) schema.GroupVersionKind {
	if len(actual.Kind) == 0 {
		actual.Kind = defaultGVK.Kind
	}
	// group 与 version 都缺失时，整体采用默认值。
	if len(actual.Version) == 0 && len(actual.Group) == 0 {
		actual.Group = defaultGVK.Group
		actual.Version = defaultGVK.Version
	}
	// 只缺 version 且 group 已匹配时，仅补 version。
	if len(actual.Version) == 0 && actual.Group == defaultGVK.Group {
		actual.Version = defaultGVK.Version
	}
	return actual
}

// diagnose 返回一个格式正确的 CBOR 数据项的诊断记法（diagnostic notation），
// 用于在错误信息中展示原始内容。
func diagnose(data []byte) string {
	diag, err := modes.Diagnostic.Diagnose(data)
	if err != nil {
		// 输入已经必须是格式正确的 CBOR，理论上转诊断记法不应失败；
		// 若仍失败则退回十六进制输出，保证错误信息可用。
		util.HandleError(err)

		return hex.EncodeToString(data)
	}
	return diag
}

// unmarshal 把 CBOR 字节解码进 Go 对象。若 decoder 开启了严格模式，第一个返回值可能是
// 非 nil 的严格解码错误；若最后一个返回值非 nil，则解码整体失败，目标对象的状态不可依赖。
func (s *serializer) unmarshal(data []byte, into interface{}) (strict, lax error) {
	if u, ok := into.(runtime.Unstructured); ok {
		var content map[string]interface{}
		defer func() {
			switch u := u.(type) {
			case *unstructured.UnstructuredList:
				// UnstructuredList 的 SetUnstructuredContent 产生的对象与
				// UnstructuredJSONScheme 解码结果不同：
				//
				//   1. SetUnstructuredContent 会在列表的 Object 字段中保留 "items" 键，
				//      而 UnstructuredJSONScheme 会把它从 Object 中移除。
				//   2. SetUnstructuredContent 不会为 Items 中每个元素填充
				//      "apiVersion" 和 "kind"，UnstructuredJSONScheme 会（从列表 Kind
				//      推断单数 Kind）。
				//   3. SetUnstructuredContent 会忽略 "items" 中不是 JSON 对象或缺少
				//      "kind" 的元素，UnstructuredJSONScheme 在这两种情况下都会报错。
				//
				// 这里复刻 UnstructuredJSONScheme 的行为。
				var items []interface{}
				if uncast, present := content["items"]; present {
					var cast bool
					items, cast = uncast.([]interface{})
					if !cast {
						strict, lax = nil, fmt.Errorf("items field of UnstructuredList must be encoded as an array or null if present")
						return
					}
				}
				apiVersion, _ := content["apiVersion"].(string)
				kind, _ := content["kind"].(string)
				// 列表 Kind 的约定是单数 Kind + "List" 后缀，去掉后缀得到元素 Kind。
				kind = strings.TrimSuffix(kind, "List")
				var unstructureds []unstructured.Unstructured
				if len(items) > 0 {
					unstructureds = make([]unstructured.Unstructured, len(items))
				}
				for i := range items {
					object, cast := items[i].(map[string]interface{})
					if !cast {
						strict, lax = nil, fmt.Errorf("elements of the items field of UnstructuredList must be encoded as a map")
						return
					}

					// 与 UnstructuredJSONScheme 一致：仅当 "apiVersion" 和 "kind"
					// 都缺失、非字符串或为空时，才用推断出的单数 GVK 补齐。
					object["apiVersion"], _ = object["apiVersion"].(string)
					object["kind"], _ = object["kind"].(string)
					if object["apiVersion"] == "" && object["kind"] == "" {
						object["apiVersion"] = apiVersion
						object["kind"] = kind
					}

					if object["kind"] == "" {
						strict, lax = nil, runtime.NewMissingKindErr(diagnose(data))
						return
					}
					if object["apiVersion"] == "" {
						strict, lax = nil, runtime.NewMissingVersionErr(diagnose(data))
						return
					}

					unstructureds[i].Object = object
				}
				// items 已单独提取到 Items 字段，从 Object 中删除以避免重复。
				delete(content, "items")
				u.Object = content
				u.Items = unstructureds
			default:
				u.SetUnstructuredContent(content)
			}
		}()
		into = &content
	}

	if !s.options.strict {
		return nil, modes.DecodeLax.Unmarshal(data, into)
	}

	err := modes.Decode.Unmarshal(data, into)
	// TODO: UnknownFieldError 含义不明确：它只给出遇到的第一个有问题表项的索引，
	// 却没有说明该索引属于哪张 map。
	var unknownField *cbor.UnknownFieldError
	if errors.As(err, &unknownField) {
		// 与 JSON 不同，CBOR 对重复 map 键没有严格错误：按规范重复键的 map 本身就是非法的，
		// 会被整体拒绝。
		return runtime.NewStrictDecodingError([]error{unknownField}), modes.DecodeLax.Unmarshal(data, into)
	}
	return nil, err
}

// Decode 把 CBOR 数据解码为对象，并返回实际识别到的 GVK。
func (s *serializer) Decode(data []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	// 解码进 Unstructured 成功时不需要额外预扫 GVK，因为对象内容本身已包含元信息。
	if _, ok := into.(runtime.Unstructured); ok {
		if _, unmarshalErr := s.unmarshal(data, into); unmarshalErr != nil {
			actual, interpretErr := s.metaFactory.Interpret(data)
			if interpretErr != nil {
				return nil, nil, interpretErr
			}

			// 即使解码失败，也尽量用调用方期望的 gvk 补全返回的 actual，便于错误处理方使用。
			if gvk != nil {
				*actual = gvkWithDefaults(*actual, *gvk)
			}

			return nil, actual, unmarshalErr
		}

		actual := into.GetObjectKind().GroupVersionKind()
		if len(actual.Kind) == 0 {
			return nil, &actual, runtime.NewMissingKindErr(diagnose(data))
		}
		if len(actual.Version) == 0 {
			return nil, &actual, runtime.NewMissingVersionErr(diagnose(data))
		}

		return into, &actual, nil
	}

	actual, err := s.metaFactory.Interpret(data)
	if err != nil {
		return nil, nil, err
	}

	if gvk != nil {
		*actual = gvkWithDefaults(*actual, *gvk)
	}

	if into != nil {
		types, _, err := s.typer.ObjectKinds(into)
		if err != nil {
			return nil, actual, err
		}
		// 调用方预分配了目标对象时，以该对象的类型为准补齐缺失字段。
		*actual = gvkWithDefaults(*actual, types[0])
	}

	// kind/version 缺失属于协议错误，用诊断记法给出原始数据便于定位。
	if len(actual.Kind) == 0 {
		return nil, actual, runtime.NewMissingKindErr(diagnose(data))
	}
	if len(actual.Version) == 0 {
		return nil, actual, runtime.NewMissingVersionErr(diagnose(data))
	}

	obj, err := runtime.UseOrCreateObject(s.typer, s.creater, *actual, into)
	if err != nil {
		return nil, actual, err
	}

	strict, err := s.unmarshal(data, obj)
	if err != nil {
		return nil, actual, err
	}

	if s.options.transcode {
		if err := transcodeRawTypes(obj); err != nil {
			return nil, actual, err
		}
	}

	return obj, actual, strict
}

// selfDescribedCBOR 是 tag 55799 头部的 CBOR 编码。该 tag 定义于 RFC 8949 第 3.4.6 节
// "Self-Described CBOR"，包裹编码器的全部输出，本身没有特殊语义，
// 仅作为识别 CBOR 数据的魔数。
//
// 参见 https://www.rfc-editor.org/rfc/rfc8949.html#name-self-described-cbor。
var selfDescribedCBOR = []byte{0xd9, 0xd9, 0xf7}

// RecognizesData 通过 self-described CBOR 前缀判断数据是否为本序列化器的输出。
// 返回 false 表示不认识该格式（不做猜测）。
func (s *serializer) RecognizesData(data []byte) (ok, unknown bool, err error) {
	return bytes.HasPrefix(data, selfDescribedCBOR), false, nil
}

// NewSerializerInfo 使用给定的 creater 和 typer 返回 CBOR 的默认 SerializerInfo。
// 注意 strict serializer 与流式 serializer（关闭 transcode）分别用于不同的使用场景。
func NewSerializerInfo(creater runtime.ObjectCreater, typer runtime.ObjectTyper) runtime.SerializerInfo {
	return runtime.SerializerInfo{
		MediaType:        "application/cbor",
		MediaTypeType:    "application",
		MediaTypeSubType: "cbor",
		Serializer:       NewSerializer(creater, typer),
		StrictSerializer: NewSerializer(creater, typer, Strict(true)),
		StreamSerializer: &runtime.StreamSerializerInfo{
			Framer:     NewFramer(),
			Serializer: NewSerializer(creater, typer, Transcode(false)),
		},
	}
}
