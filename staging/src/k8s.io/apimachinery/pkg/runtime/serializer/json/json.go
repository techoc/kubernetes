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

package json

import (
	"encoding/json"
	"io"
	"strconv"

	kjson "sigs.k8s.io/json"
	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/recognizer"
	"k8s.io/apimachinery/pkg/util/framer"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/klog/v2"
)

// NewSerializer 创建一个 JSON 序列化器，负责将带版本的对象编码为正确的 JSON 格式。如果 typer
// 不为 nil，则会设置对象的 group、version 和 kind 字段。
// Deprecated: use NewSerializerWithOptions instead.
func NewSerializer(meta MetaFactory, creater runtime.ObjectCreater, typer runtime.ObjectTyper, pretty bool) *Serializer {
	return NewSerializerWithOptions(meta, creater, typer, SerializerOptions{false, pretty, false, false})
}

// NewYAMLSerializer 创建一个 YAML 序列化器，负责将带版本的对象编码为正确的 YAML 格式。如果 typer
// 不为 nil，则会设置对象的 group、version 和 kind 字段。该序列化器仅支持与 JSON 匹配的 YAML 子集，
// 若使用了无法序列化为 JSON 的 YAML 结构，则会报错。
// Deprecated: use NewSerializerWithOptions instead.
func NewYAMLSerializer(meta MetaFactory, creater runtime.ObjectCreater, typer runtime.ObjectTyper) *Serializer {
	return NewSerializerWithOptions(meta, creater, typer, SerializerOptions{true, false, false, false})
}

// NewSerializerWithOptions 创建一个 JSON/YAML 序列化器，负责将带版本的对象编码为正确的 JSON/YAML
// 格式。如果 typer 不为 nil，则会设置对象的 group、version 和 kind 字段。Options 会被复制到 Serializer 中，
// 且不可变。
func NewSerializerWithOptions(meta MetaFactory, creater runtime.ObjectCreater, typer runtime.ObjectTyper, options SerializerOptions) *Serializer {
	return &Serializer{
		meta:       meta,
		creater:    creater,
		typer:      typer,
		options:    options,
		identifier: identifier(options),
	}
}

// identifier 根据给定的 options 计算 Encoder 的 Identifier。
func identifier(options SerializerOptions) runtime.Identifier {
	result := map[string]string{
		"name":   "json",
		"yaml":   strconv.FormatBool(options.Yaml),
		"pretty": strconv.FormatBool(options.Pretty),
		"strict": strconv.FormatBool(options.Strict),
	}
	identifier, err := json.Marshal(result)
	if err != nil {
		//nolint:logcheck // Should not be reached.
		klog.Fatalf("Failed marshaling identifier for json Serializer: %v", err)
	}
	return runtime.Identifier(identifier)
}

// SerializerOptions 保存用于配置 JSON/YAML 序列化器的选项。
// 示例：
// (1) 配置 JSON 序列化器时，将 `Yaml` 设为 `false`。
// (2) 配置 YAML 序列化器时，将 `Yaml` 设为 `true`。
// (3) 配置可返回 strictDecodingError 的严格序列化器时，将 `Strict` 设为 `true`。
type SerializerOptions struct {
	// Yaml：配置 Serializer 使用 JSON（false）还是 YAML（true）。
	// 启用 `Yaml` 时，该序列化器仅支持与 JSON 匹配的 YAML 子集，
	// 若使用了无法序列化为 JSON 的结构，则会报错。
	Yaml bool

	// Pretty：配置启用 JSON 的 Serializer（`Yaml: false`）生成便于阅读的输出。
	// 当 `Yaml` 为 `true` 时，该选项会被静默忽略。
	Pretty bool

	// Strict：配置 Serializer 在解码 JSON 或 YAML 时遇到重复字段返回 strictDecodingError。
	// 注意，启用该选项的性能不如非严格模式，不应在快速路径中使用。
	Strict bool

	// StreamingCollectionsEncoding 支持逐项编码集合，从而大幅降低所需内存。
	StreamingCollectionsEncoding bool
}

// Serializer 负责将带版本的对象编码为正确的 JSON 格式
type Serializer struct {
	meta    MetaFactory
	options SerializerOptions
	creater runtime.ObjectCreater
	typer   runtime.ObjectTyper

	identifier runtime.Identifier
}

// Serializer 实现了 Serializer 接口
var _ runtime.Serializer = &Serializer{}
var _ recognizer.RecognizingDecoder = &Serializer{}

// gvkWithDefaults 返回使用给定默认值补全后的 group、kind 和 version。
// 仅填充缺失的部分；显式提供的 group/version 不会被覆盖，
// 因此默认值不会悄悄改变目标。
func gvkWithDefaults(actual, defaultGVK schema.GroupVersionKind) schema.GroupVersionKind {
	if len(actual.Kind) == 0 {
		actual.Kind = defaultGVK.Kind
	}
	if len(actual.Version) == 0 && len(actual.Group) == 0 {
		actual.Group = defaultGVK.Group
		actual.Version = defaultGVK.Version
	}
	if len(actual.Version) == 0 && actual.Group == defaultGVK.Group {
		actual.Version = defaultGVK.Version
	}
	return actual
}

// Decode 尝试将提供的数据转换为 YAML 或 JSON，提取其中保存的 schema kind，应用提供的默认 gvk，
// 然后将数据加载到与所需 schema kind 或提供的 into 相匹配的对象中。
// 如果 into 是 *runtime.Unknown，则会提取原始数据，不执行任何解码。
// 如果 into 未注册到 typer，则会直接使用常规的 JSON/YAML 反序列化来解码对象。
// 如果提供了 into，而原始数据未完整限定 kind/version/group，则会使用 into 的类型来修改返回的 gvk。
// 如果 into 为 nil，或数据的 gvk 与 into 的 gvk 不同，则会通过 ObjectCreater.New(gvk) 生成一个新对象。
// 成功时或大多数错误情况下，该方法都会返回计算得到的 schema kind。
// gvk 的计算优先级为：originalData > default gvk > into
func (s *Serializer) Decode(originalData []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	data := originalData
	// 先将 YAML 转换为 JSON，这样后续只需一条解码路径。
	if s.options.Yaml {
		altered, err := yaml.YAMLToJSON(data)
		if err != nil {
			return nil, nil, err
		}
		data = altered
	}

	// 读取文档中实际出现的类型信息。
	actual, err := s.meta.Interpret(data)
	if err != nil {
		return nil, nil, err
	}

	// 用调用方提供的默认值补全文档中省略的部分。
	if gvk != nil {
		*actual = gvkWithDefaults(*actual, *gvk)
	}

	// Unknown 表示“保留原始字节，交由调用方后续决定”，
	// 因此此处不尝试解析 schema。
	if unk, ok := into.(*runtime.Unknown); ok && unk != nil {
		unk.Raw = originalData
		unk.ContentType = runtime.ContentTypeJSON
		unk.GetObjectKind().SetGroupVersionKind(*actual)
		return unk, actual, nil
	}

	if into != nil {
		_, isUnstructured := into.(runtime.Unstructured)
		types, _, err := s.typer.ObjectKinds(into)
		switch {
		case runtime.IsNotRegisteredError(err), isUnstructured:
			// 未注册或 unstructured 目标：直接解码 JSON，不创建 scheme 对象。
			// 严格模式下的重复字段检查仍会执行。
			strictErrs, err := s.unmarshal(into, data, originalData)
			if err != nil {
				return nil, actual, err
			}

			// 直接解码到提供的 unstructured 对象时，
			// 提取从所提供数据中解码出的实际 gvk，
			// 并确保其非空。
			if isUnstructured {
				*actual = into.GetObjectKind().GroupVersionKind()
				if len(actual.Kind) == 0 {
					return nil, actual, runtime.NewMissingKindErr(string(originalData))
				}
				// TODO(109023): 等 unstructuredJSONScheme#Decode 也要求 apiVersion 后，这里同步收紧。
			}

			if len(strictErrs) > 0 {
				return into, actual, runtime.NewStrictDecodingError(strictErrs)
			}
			return into, actual, nil
		case err != nil:
			return nil, actual, err
		default:
			// into 对象已注册；其 GVK 作为下一级默认值。
			*actual = gvkWithDefaults(*actual, types[0])
		}
	}

	// 缺少 kind/version 时，无法将文档解析为某个 Go 类型。
	if len(actual.Kind) == 0 {
		return nil, actual, runtime.NewMissingKindErr(string(originalData))
	}
	if len(actual.Version) == 0 {
		return nil, actual, runtime.NewMissingVersionErr(string(originalData))
	}

	// 必要时使用目标对象
	obj, err := runtime.UseOrCreateObject(s.typer, s.creater, *actual, into)
	if err != nil {
		return nil, actual, err
	}

	strictErrs, err := s.unmarshal(obj, data, originalData)
	if err != nil {
		return nil, actual, err
	} else if len(strictErrs) > 0 {
		// 重复字段会被报告，但仍会返回对象，
		// 由调用方决定是否将该警告视为错误。
		return obj, actual, runtime.NewStrictDecodingError(strictErrs)
	}
	return obj, actual, nil
}

// Encode 将提供的对象序列化到给定的 writer。
func (s *Serializer) Encode(obj runtime.Object, w io.Writer) error {
	if co, ok := obj.(runtime.CacheableObject); ok {
		return co.CacheEncode(s.Identifier(), s.doEncode, w)
	}
	return s.doEncode(obj, w)
}

func (s *Serializer) doEncode(obj runtime.Object, w io.Writer) error {
	// YAML 输出通过先转为 JSON 再转回的方式生成，这保证了
	// 输出恰好是序列化器所支持的 JSON 子集对应的 YAML。
	if s.options.Yaml {
		json, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		data, err := yaml.JSONToYAML(json)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	}

	// 美化输出是为方便客户端阅读；输出必须仍是合法的 JSON。
	if s.options.Pretty {
		data, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	}
	// 集合可以逐项流式编码以限制内存占用；如果对象不是集合，
	// 则回退到标准编码器。
	if s.options.StreamingCollectionsEncoding {
		ok, err := streamEncodeCollections(obj, w)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	// json.Encoder 会追加换行符，该换行符同时也是流式读取器的帧分隔符。
	encoder := json.NewEncoder(w)
	return encoder.Encode(obj)
}

// IsStrict 指示序列化器
// 是否使用严格解码
func (s *Serializer) IsStrict() bool {
	return s.options.Strict
}

func (s *Serializer) unmarshal(into runtime.Object, data, originalData []byte) (strictErrs []error, err error) {
	// 如果反序列化器处于非严格模式，直接返回。
	// 区分大小写并保留整数：Kubernetes 的字段名精确匹配，
	// 且大整数不能被静默转换为 float64。
	if !s.options.Strict {
		if err := kjson.UnmarshalCaseSensitivePreserveInts(data, into); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if s.options.Yaml {
		// 严格模式下，将原始数据传给 YAMLToJSONStrict 转换器。
		// 这样做是为了捕获 YAML 中的重复字段，否则这些字段会在原本的 YAMLToJSON 转换中被丢弃。
		// TODO: 改造 YAMLToJSONStrict，使其能不中断地返回重复字段警告，这样就不必重复转换两次。
		_, err := yaml.YAMLToJSONStrict(originalData)
		if err != nil {
			strictErrs = append(strictErrs, err)
		}
	}

	var strictJSONErrs []error
	if u, isUnstructured := into.(runtime.Unstructured); isUnstructured {
		// Unstructured 是自定义的 unmarshaler，反序列化会委托给它，
		// 因此要检测严格的 JSON 错误，需要直接反序列化到该对象中。
		m := map[string]interface{}{}
		strictJSONErrs, err = kjson.UnmarshalStrict(data, &m)
		u.SetUnstructuredContent(m)
	} else {
		strictJSONErrs, err = kjson.UnmarshalStrict(data, into)
	}
	if err != nil {
		// 致命的解码错误，并非由严格模式引起
		return nil, err
	}
	// 重复字段警告会被收集起来，而不是作为致命错误返回。
	strictErrs = append(strictErrs, strictJSONErrs...)
	return strictErrs, nil
}

// Identifier 实现了 runtime.Encoder 接口。
func (s *Serializer) Identifier() runtime.Identifier {
	return s.identifier
}

// RecognizesData 实现了 RecognizingDecoder 接口。
func (s *Serializer) RecognizesData(data []byte) (ok, unknown bool, err error) {
	if s.options.Yaml {
		// 理论上可以查找 '---' 分隔符，
		// 但 YAML 是 JSON 的超集，因此 JSON 检查无法确认数据一定是 JSON。
		return false, true, nil
	}
	return utilyaml.IsJSONBuffer(data), false, nil
}

// Framer 是默认的 JSON 分帧行为，使用换行符分隔各个对象。
var Framer = jsonFramer{}

type jsonFramer struct{}

// NewFrameWriter 为该序列化器实现流式分帧
func (jsonFramer) NewFrameWriter(w io.Writer) io.Writer {
	// JSON 对象自带边界，因此可以直接写入 writer
	return w
}

// NewFrameReader 为该序列化器实现流式分帧
func (jsonFramer) NewFrameReader(r io.ReadCloser) io.ReadCloser {
	// 需要提取 JSON 数据块，以便传给 Decode()
	return framer.NewJSONFramedReader(r)
}

// YAMLFramer 是默认的 YAML 分帧行为，使用换行符分隔各个对象。
var YAMLFramer = yamlFramer{}

type yamlFramer struct{}

// NewFrameWriter 为该序列化器实现流式分帧
func (yamlFramer) NewFrameWriter(w io.Writer) io.Writer {
	return yamlFrameWriter{w}
}

// NewFrameReader 为该序列化器实现流式分帧
func (yamlFramer) NewFrameReader(r io.ReadCloser) io.ReadCloser {
	// 直接提取 YAML 文档块
	return utilyaml.NewDocumentDecoder(r)
}

type yamlFrameWriter struct {
	w io.Writer
}

// Write 使用 YAML 文档分隔符（`---` 后跟换行）分隔每个文档。
// 写入方必须写入格式良好的 YAML 文档（包含末尾换行）。
func (w yamlFrameWriter) Write(data []byte) (n int, err error) {
	if _, err := w.w.Write([]byte("---\n")); err != nil {
		return 0, err
	}
	return w.w.Write(data)
}
