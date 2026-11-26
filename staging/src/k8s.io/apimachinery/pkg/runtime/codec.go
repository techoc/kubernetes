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

// codec 把编码器与解码器绑定在一起。
//
// 简单地把编码器与解码器组合成一个 Codec；两者可以是同一底层实现。
type codec struct {
	Encoder
	Decoder
}

// 使用给定 Encoder 和 Decoder 创建 Codec。Codec 将编码与解码组合为一个接口，通常由调用方选择具体的版本转换包装器。
func NewCodec(e Encoder, d Decoder) Codec {
	return codec{e, d}
}

// 便捷编码函数：调用 Encoder 将对象写入缓冲区，并返回序列化后的字节。编码失败时返回错误。
func Encode(e Encoder, obj Object) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := e.Encode(obj, buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 便捷解码函数：调用 Decoder 从字节数据中恢复 API 对象。此入口不提供默认 GVK，也不复用目标对象，类型信息通常由序列化数据自身提供。
func Decode(d Decoder, data []byte) (Object, error) {
	obj, _, err := d.Decode(data, nil, nil)
	return obj, err
}

// 将数据解码到调用方提供的对象中。解码器必须实际填充该对象；如果解码器因类型不匹配而另行创建对象，本函数会返回错误，防止调用方误以为 into 已被更新。
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

// 调用 Encode 并返回字符串形式的结果。该辅助函数遇到编码错误时直接 panic，主要用于测试和已确认不会失败的场景。
func EncodeOrDie(e Encoder, obj Object) string {
	bytes, err := Encode(e, obj)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

// 比较对象当前的规范 GVK 与期望 GVK：匹配时复用原对象，以保留已有状态和底层容量；不匹配时通过 ObjectCreater 创建正确类型的新对象。无法识别对象类型时返回错误。
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

// NoopEncoder 把 Decoder 包装成 Serializer 或 Codec，供只用到解码能力、
// 却要求 Serializer/Codec 接口的代码使用。
//
// 用于只解码、不需要编码的场合：满足 Serializer 接口但 Encode 总是报错。
type NoopEncoder struct {
	Decoder
}

var _ Serializer = NoopEncoder{}

const noopEncoderIdentifier Identifier = "noop"

// Encode 始终失败：NoopEncoder 只应出现在仅使用解码的代码路径中。
func (n NoopEncoder) Encode(obj Object, w io.Writer) error {
	// 无需处理 runtime.CacheableObject：这里根本不触碰对象。
	return fmt.Errorf("encoding is not allowed for this codec: %v", reflect.TypeOf(n.Decoder))
}

// Identifier 实现 runtime.Encoder 接口。
func (n NoopEncoder) Identifier() Identifier {
	return noopEncoderIdentifier
}

// NoopDecoder 把 Encoder 包装成 Serializer 或 Codec，供只用到编码能力、
// 却要求 Serializer/Codec 接口的代码使用。
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

// 创建参数编解码器，用于在 URL 查询参数与指定版本的 API 对象之间转换。查询参数本身不携带 GVK，因此必须由调用方明确提供版本。
func NewParameterCodec(scheme *Scheme) ParameterCodec {
	return &parameterCodec{
		typer:     scheme,
		convertor: scheme,
		creator:   scheme,
		defaulter: scheme,
	}
}

// parameterCodec 负责对象与查询参数的双向转换。
type parameterCodec struct {
	typer     ObjectTyper
	convertor ObjectConvertor
	creator   ObjectCreater
	defaulter ObjectDefaulter
}

var _ ParameterCodec = &parameterCodec{}

// 把 URL 查询参数解码为 from 版本的对象，再按需要转换到 into 对象的版本。若 into 类型本身支持 from 版本，则可直接解码；否则先创建中间版本对象，解码并应用默认值后再转换。
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
			// 输入输出是同一个对象时，以出站对象为准做默认值填充。
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
	// 有 defaulter 时先给输入补默认值，再转换到输出。
	if c.defaulter != nil {
		c.defaulter.Default(input)
	}
	return c.convertor.Convert(input, into, nil)
}

// 先将输入对象转换到指定目标版本，再把该版本对象编码为 URL 查询参数。任一版本转换或参数编码失败都会返回错误。
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

// Identifier 实现 runtime.Encoder 接口。
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

// 在支持的序列化器列表中查找媒体类型。优先返回精确匹配项；若没有精确匹配，则可使用 MediaType 为空的默认项；都不存在时返回 false。媒体类型参数不参与匹配。
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
	// InternalGroupVersioner 对任何 group version kind 都优先选择内部版本。
	InternalGroupVersioner GroupVersioner = internalGroupVersioner{}
	// DisabledGroupVersioner 拒绝传入的所有 kind。
	DisabledGroupVersioner GroupVersioner = disabledGroupVersioner{}
)

const (
	internalGroupVersionerIdentifier = "internal"
	disabledGroupVersionerIdentifier = "disabled"
)

// internalGroupVersioner 把任意外部 kind 映射为内部版本；如果输入中已有内部
// 版本则直接返回。
type internalGroupVersioner struct{}

// KindForGroupVersionKinds 优先返回找到的内部 Kind，否则把第一个 kind 转成内部版本。
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

// Identifier 实现 GroupVersioner 接口。
func (internalGroupVersioner) Identifier() string {
	return internalGroupVersionerIdentifier
}

// disabledGroupVersioner 拒绝一切 kind，用于显式关闭某个编码路径。
type disabledGroupVersioner struct{}

// KindForGroupVersionKinds 对任何输入都返回 false。
func (disabledGroupVersioner) KindForGroupVersionKinds(kinds []schema.GroupVersionKind) (schema.GroupVersionKind, bool) {
	return schema.GroupVersionKind{}, false
}

// Identifier 实现 GroupVersioner 接口。
func (disabledGroupVersioner) Identifier() string {
	return disabledGroupVersionerIdentifier
}

// 断言 schema.GroupVersion 和 GroupVersions 实现 GroupVersioner。
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

// 创建一个 GroupVersioner：当输入 Kind 匹配任一指定 GroupKind 时，返回给定目标 GroupVersion。若某个 GroupKind 的 Kind 为空，则该项匹配该 Group 下任意 Kind。
func NewMultiGroupVersioner(gv schema.GroupVersion, groupKinds ...schema.GroupKind) GroupVersioner {
	if len(groupKinds) == 0 || (len(groupKinds) == 1 && groupKinds[0].Group == gv.Group) {
		return gv
	}
	return multiGroupVersioner{target: gv, acceptedGroupKinds: groupKinds}
}

// 创建一个可强制转换的 GroupVersioner。优先选择与给定 GroupKind 匹配的输入 Kind；若没有匹配项，也会把任意输入 Kind 转换到指定目标 GroupVersion。Kind 为空的 GroupKind 表示匹配该 Group 下所有 Kind。
func NewCoercingMultiGroupVersioner(gv schema.GroupVersion, groupKinds ...schema.GroupKind) GroupVersioner {
	return multiGroupVersioner{target: gv, acceptedGroupKinds: groupKinds, coerce: true}
}

// KindForGroupVersionKinds 在任一 kind 匹配原始 group kind 时返回目标 group version，
// 并尽可能沿用来源 kind。
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

// Identifier 实现 GroupVersioner 接口。
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
