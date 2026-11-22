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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/conversion/queryparams"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

// codec binds an encoder and decoder.
//
// 简单地把编码器与解码器组合成一个 Codec；两者可以是同一底层实现。
type codec struct {
	Encoder
	Decoder
}

// NewCodec creates a Codec from an Encoder and Decoder.
func NewCodec(e Encoder, d Decoder) Codec {
	return codec{e, d}
}

// Encode is a convenience wrapper for encoding to a []byte from an Encoder
func Encode(e Encoder, obj Object) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := e.Encode(obj, buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode is a convenience wrapper for decoding data into an Object.
//
// 不传默认 GVK 与目标对象，具体类型完全由数据自身携带的 TypeMeta 决定。
func Decode(d Decoder, data []byte) (Object, error) {
	obj, _, err := d.Decode(data, nil, nil)
	return obj, err
}

// DecodeInto performs a Decode into the provided object.
//
// 解码结果必须是调用方提供的那个对象；若解码器因类型不匹配新建了对象，
// 返回错误，避免调用方拿到一个未被填充的 into。
func DecodeInto(d Decoder, data []byte, into Object) error {
	out, gvk, err := d.Decode(data, nil, into)
	if err != nil {
		return err
	}
	if out != into {
		return fmt.Errorf("unable to decode %s into %v", gvk, reflect.TypeOf(into))
	}
	return nil
}

// EncodeOrDie is a version of Encode which will panic instead of returning an error. For tests.
func EncodeOrDie(e Encoder, obj Object) string {
	bytes, err := Encode(e, obj)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

// UseOrCreateObject returns obj if the canonical ObjectKind returned by the provided typer matches gvk, or
// invokes the ObjectCreator to instantiate a new gvk. Returns an error if the typer cannot find the object.
//
// 复用已有对象可以保留其内部状态（如 list 的 items 容量）；只有 GVK 不匹配时
// 才创建新对象，避免把数据解码进错误类型。
func UseOrCreateObject(t ObjectTyper, c ObjectCreater, gvk schema.GroupVersionKind, obj Object) (Object, error) {
	if obj != nil {
		kinds, _, err := t.ObjectKinds(obj)
		if err != nil {
			return nil, err
		}
		for _, kind := range kinds {
			if gvk == kind {
				return obj, nil
			}
		}
	}
	return c.New(gvk)
}

// NoopEncoder converts an Decoder to a Serializer or Codec for code that expects them but only uses decoding.
//
// 用于只解码、不需要编码的场合：满足 Serializer 接口但 Encode 总是报错。
type NoopEncoder struct {
	Decoder
}

var _ Serializer = NoopEncoder{}

const noopEncoderIdentifier Identifier = "noop"

// Encode 始终失败：NoopEncoder 只应出现在仅使用解码的代码路径中。
func (n NoopEncoder) Encode(obj Object, w io.Writer) error {
	// There is no need to handle runtime.CacheableObject, as we don't
	// process the obj at all.
	return fmt.Errorf("encoding is not allowed for this codec: %v", reflect.TypeOf(n.Decoder))
}

// Identifier implements runtime.Encoder interface.
func (n NoopEncoder) Identifier() Identifier {
	return noopEncoderIdentifier
}

// NoopDecoder converts an Encoder to a Serializer or Codec for code that expects them but only uses encoding.
//
// 与 NoopEncoder 对称：只编码、不解码。
type NoopDecoder struct {
	Encoder
}

var _ Serializer = NoopDecoder{}

// Decode 始终失败：NoopDecoder 只应出现在仅使用编码的代码路径中。
func (n NoopDecoder) Decode(data []byte, gvk *schema.GroupVersionKind, into Object) (Object, *schema.GroupVersionKind, error) {
	return nil, nil, fmt.Errorf("decoding is not allowed for this codec: %v", reflect.TypeOf(n.Encoder))
}

// NewParameterCodec creates a ParameterCodec capable of transforming url values into versioned objects and back.
func NewParameterCodec(scheme *Scheme) ParameterCodec {
	return &parameterCodec{
		typer:     scheme,
		convertor: scheme,
		creator:   scheme,
		defaulter: scheme,
	}
}

// parameterCodec implements conversion to and from query parameters and objects.
type parameterCodec struct {
	typer     ObjectTyper
	convertor ObjectConvertor
	creator   ObjectCreater
	defaulter ObjectDefaulter
}

var _ ParameterCodec = &parameterCodec{}

// DecodeParameters converts the provided url.Values into an object of type From with the kind of into, and then
// converts that object to into (if necessary). Returns an error if the operation cannot be completed.
//
// 若 into 本身支持 from 版本，直接解码并补默认值；否则先构造 from 版本对象，
// 解码、默认化后再转换到目标版本。
func (c *parameterCodec) DecodeParameters(parameters url.Values, from schema.GroupVersion, into Object) error {
	if len(parameters) == 0 {
		// 没有参数时保持 into 不变；默认值由调用方决定是否应用。
		return nil
	}
	targetGVKs, _, err := c.typer.ObjectKinds(into)
	if err != nil {
		return err
	}
	for i := range targetGVKs {
		if targetGVKs[i].GroupVersion() == from {
			if err := c.convertor.Convert(&parameters, into, nil); err != nil {
				return err
			}
			// in the case where we going into the same object we're receiving, default on the outbound object
			// 直接解码时，默认值必须应用在同一个对象上。
			if c.defaulter != nil {
				c.defaulter.Default(into)
			}
			return nil
		}
	}

	// 版本不同：以 from 版本创建临时对象，完成解码与默认化后再转换。
	input, err := c.creator.New(from.WithKind(targetGVKs[0].Kind))
	if err != nil {
		return err
	}
	if err := c.convertor.Convert(&parameters, input, nil); err != nil {
		return err
	}
	// if we have defaulter, default the input before converting to output
	if c.defaulter != nil {
		c.defaulter.Default(input)
	}
	return c.convertor.Convert(input, into, nil)
}

// EncodeParameters converts the provided object into the to version, then converts that object to url.Values.
// Returns an error if conversion is not possible.
func (c *parameterCodec) EncodeParameters(obj Object, to schema.GroupVersion) (url.Values, error) {
	gvks, _, err := c.typer.ObjectKinds(obj)
	if err != nil {
		return nil, err
	}
	gvk := gvks[0]
	// 只有目标版本与对象当前版本不同才需要转换，避免无谓的拷贝。
	if to != gvk.GroupVersion() {
		out, err := c.convertor.ConvertToVersion(obj, to)
		if err != nil {
			return nil, err
		}
		obj = out
	}
	return queryparams.Convert(obj)
}

// base64Serializer 在底层 serializer 之外套一层 base64 编码，用于把二进制
// 负载嵌入只能承载文本的通道。
type base64Serializer struct {
	Encoder
	Decoder

	identifier Identifier
}

// NewBase64Serializer 包装编解码器，并把标识符标记为 base64 变体。
func NewBase64Serializer(e Encoder, d Decoder) Serializer {
	return &base64Serializer{
		Encoder:    e,
		Decoder:    d,
		identifier: identifier(e),
	}
}

// identifier 生成可比较的标识串（JSON 编码的 name/encoder 映射），
// 供缓存按编码方式区分条目。
func identifier(e Encoder) Identifier {
	result := map[string]string{
		"name": "base64",
	}
	if e != nil {
		result["encoder"] = string(e.Identifier())
	}
	identifier, err := json.Marshal(result)
	if err != nil {
		//nolint:logcheck // Should not be reached.
		klog.Fatalf("Failed marshaling identifier for base64Serializer: %v", err)
	}
	return Identifier(identifier)
}

// Encode 对实现 CacheableObject 的对象走缓存编码路径，避免重复编码。
func (s base64Serializer) Encode(obj Object, stream io.Writer) error {
	if co, ok := obj.(CacheableObject); ok {
		return co.CacheEncode(s.Identifier(), s.doEncode, stream)
	}
	return s.doEncode(obj, stream)
}

func (s base64Serializer) doEncode(obj Object, stream io.Writer) error {
	e := base64.NewEncoder(base64.StdEncoding, stream)
	err := s.Encoder.Encode(obj, e)
	// Close 必须调用以冲刷 base64 尾部字节；即使 Encode 失败也要关闭。
	e.Close()
	return err
}

// Identifier implements runtime.Encoder interface.
func (s base64Serializer) Identifier() Identifier {
	return s.identifier
}

// Decode 先解码 base64，再交给底层解码器。
func (s base64Serializer) Decode(data []byte, defaults *schema.GroupVersionKind, into Object) (Object, *schema.GroupVersionKind, error) {
	out := make([]byte, base64.StdEncoding.DecodedLen(len(data)))
	n, err := base64.StdEncoding.Decode(out, data)
	if err != nil {
		return nil, nil, err
	}
	return s.Decoder.Decode(out[:n], defaults, into)
}

// SerializerInfoForMediaType returns the first info in types that has a matching media type (which cannot
// include media-type parameters), or the first info with an empty media type, or false if no type matches.
//
// 精确匹配优先；其次接受“无 media type”的兜底项，与 Content-Type 协商的宽松语义一致。
func SerializerInfoForMediaType(types []SerializerInfo, mediaType string) (SerializerInfo, bool) {
	for _, info := range types {
		if info.MediaType == mediaType {
			return info, true
		}
	}
	for _, info := range types {
		if len(info.MediaType) == 0 {
			return info, true
		}
	}
	return SerializerInfo{}, false
}

var (
	// InternalGroupVersioner will always prefer the internal version for a given group version kind.
	InternalGroupVersioner GroupVersioner = internalGroupVersioner{}
	// DisabledGroupVersioner will reject all kinds passed to it.
	DisabledGroupVersioner GroupVersioner = disabledGroupVersioner{}
)

const (
	internalGroupVersionerIdentifier = "internal"
	disabledGroupVersionerIdentifier = "disabled"
)

// internalGroupVersioner 把任意外部 kind 映射为内部版本；如果输入中已有内部
// 版本则直接返回。
type internalGroupVersioner struct{}

// KindForGroupVersionKinds returns an internal Kind if one is found, or converts the first provided kind to the internal version.
func (internalGroupVersioner) KindForGroupVersionKinds(kinds []schema.GroupVersionKind) (schema.GroupVersionKind, bool) {
	for _, kind := range kinds {
		if kind.Version == APIVersionInternal {
			return kind, true
		}
	}
	for _, kind := range kinds {
		return schema.GroupVersionKind{Group: kind.Group, Version: APIVersionInternal, Kind: kind.Kind}, true
	}
	return schema.GroupVersionKind{}, false
}

// Identifier implements GroupVersioner interface.
func (internalGroupVersioner) Identifier() string {
	return internalGroupVersionerIdentifier
}

// disabledGroupVersioner 拒绝一切 kind，用于显式关闭某个编码路径。
type disabledGroupVersioner struct{}

// KindForGroupVersionKinds returns false for any input.
func (disabledGroupVersioner) KindForGroupVersionKinds(kinds []schema.GroupVersionKind) (schema.GroupVersionKind, bool) {
	return schema.GroupVersionKind{}, false
}

// Identifier implements GroupVersioner interface.
func (disabledGroupVersioner) Identifier() string {
	return disabledGroupVersionerIdentifier
}

// Assert that schema.GroupVersion and GroupVersions implement GroupVersioner
var _ GroupVersioner = schema.GroupVersion{}
var _ GroupVersioner = schema.GroupVersions{}
var _ GroupVersioner = multiGroupVersioner{}

// multiGroupVersioner 把一组可接受的 group/kind 统一映射到 target 版本，
// 并可选地在无匹配时强制使用输入的第一个 kind（coerce）。
type multiGroupVersioner struct {
	target             schema.GroupVersion
	acceptedGroupKinds []schema.GroupKind
	coerce             bool
}

// NewMultiGroupVersioner returns the provided group version for any kind that matches one of the provided group kinds.
// Kind may be empty in the provided group kind, in which case any kind will match.
//
// 当目标与接受集合退化为同一 group 时直接返回 schema.GroupVersion，
// 减少一层间接调用。
func NewMultiGroupVersioner(gv schema.GroupVersion, groupKinds ...schema.GroupKind) GroupVersioner {
	if len(groupKinds) == 0 || (len(groupKinds) == 1 && groupKinds[0].Group == gv.Group) {
		return gv
	}
	return multiGroupVersioner{target: gv, acceptedGroupKinds: groupKinds}
}

// NewCoercingMultiGroupVersioner returns the provided group version for any incoming kind.
// Incoming kinds that match the provided groupKinds are preferred.
// Kind may be empty in the provided group kind, in which case any kind will match.
// Examples:
//
//	gv=mygroup/__internal, groupKinds=mygroup/Foo, anothergroup/Bar
//	KindForGroupVersionKinds(yetanother/v1/Baz, anothergroup/v1/Bar) -> mygroup/__internal/Bar (matched preferred group/kind)
//
//	gv=mygroup/__internal, groupKinds=mygroup, anothergroup
//	KindForGroupVersionKinds(yetanother/v1/Baz, anothergroup/v1/Bar) -> mygroup/__internal/Bar (matched preferred group)
//
//	gv=mygroup/__internal, groupKinds=mygroup, anothergroup
//	KindForGroupVersionKinds(yetanother/v1/Baz, yetanother/v1/Bar) -> mygroup/__internal/Baz (no preferred group/kind match, uses first kind in list)
func NewCoercingMultiGroupVersioner(gv schema.GroupVersion, groupKinds ...schema.GroupKind) GroupVersioner {
	return multiGroupVersioner{target: gv, acceptedGroupKinds: groupKinds, coerce: true}
}

// KindForGroupVersionKinds returns the target group version if any kind matches any of the original group kinds. It will
// use the originating kind where possible.
func (v multiGroupVersioner) KindForGroupVersionKinds(kinds []schema.GroupVersionKind) (schema.GroupVersionKind, bool) {
	for _, src := range kinds {
		for _, kind := range v.acceptedGroupKinds {
			if kind.Group != src.Group {
				continue
			}
			// kind 为空表示该 group 下任意 kind 都接受。
			if len(kind.Kind) > 0 && kind.Kind != src.Kind {
				continue
			}
			return v.target.WithKind(src.Kind), true
		}
	}
	// coerce 模式下即使没有匹配也把第一个 kind 转到目标版本。
	if v.coerce && len(kinds) > 0 {
		return v.target.WithKind(kinds[0].Kind), true
	}
	return schema.GroupVersionKind{}, false
}

// Identifier implements GroupVersioner interface.
//
// 标识符必须包含所有影响映射结果的输入（target、接受集合、coerce），
// 否则缓存会把不同配置的 versioner 混为一谈。
func (v multiGroupVersioner) Identifier() string {
	groupKinds := make([]string, 0, len(v.acceptedGroupKinds))
	for _, gk := range v.acceptedGroupKinds {
		groupKinds = append(groupKinds, gk.String())
	}
	result := map[string]string{
		"name":     "multi",
		"target":   v.target.String(),
		"accepted": strings.Join(groupKinds, ","),
		"coerce":   strconv.FormatBool(v.coerce),
	}
	identifier, err := json.Marshal(result)
	if err != nil {
		//nolint:logcheck // Should not be reached.
		klog.Fatalf("Failed marshaling Identifier for %#v: %v", v, err)
	}
	return string(identifier)
}
