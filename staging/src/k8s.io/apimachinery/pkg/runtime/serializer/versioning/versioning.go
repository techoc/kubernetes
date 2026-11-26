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

package versioning

import (
	"encoding/json"
	"io"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

// NewDefaultingCodecForScheme 为使用 scheme 的调用方提供便捷的构造器。
//
// 语义转换、类型创建、类型识别与默认值都委托给同一个 scheme；调用方只需
// 提供底层的编解码器与目标版本。
func NewDefaultingCodecForScheme(
	// TODO: 这里是否应该改成 scheme 接口？
	scheme *runtime.Scheme,
	encoder runtime.Encoder,
	decoder runtime.Decoder,
	encodeVersion runtime.GroupVersioner,
	decodeVersion runtime.GroupVersioner,
) runtime.Codec {
	return NewCodec(encoder, decoder, runtime.UnsafeObjectConvertor(scheme), scheme, scheme, scheme, encodeVersion, decodeVersion, scheme.Name())
}

// NewCodec 接收内部版本的对象，先转换为外部版本再序列化；
// 它假定传入的序列化器只处理外部版本。Codec 本身也是序列化器，
// 但通常与某个具体版本配合使用。
//
// encodeVersion 为 nil 时不做版本转换；decodeVersion 决定解码后的目标版本。
// identifier 在构造时计算一次，作为缓存键参与 CacheEncode。
func NewCodec(
	encoder runtime.Encoder,
	decoder runtime.Decoder,
	convertor runtime.ObjectConvertor,
	creater runtime.ObjectCreater,
	typer runtime.ObjectTyper,
	defaulter runtime.ObjectDefaulter,
	encodeVersion runtime.GroupVersioner,
	decodeVersion runtime.GroupVersioner,
	originalSchemeName string,
) runtime.Codec {
	internal := &codec{
		encoder:   encoder,
		decoder:   decoder,
		convertor: convertor,
		creater:   creater,
		typer:     typer,
		defaulter: defaulter,

		encodeVersion: encodeVersion,
		decodeVersion: decodeVersion,

		identifier: identifier(encodeVersion, encoder),

		originalSchemeName: originalSchemeName,
	}
	return internal
}

// codec 在底层编解码器之外叠加版本转换与默认值处理，因此底层序列化器
// 只需要理解外部版本（具体 API 版本的 JSON/YAML 等）。
type codec struct {
	encoder   runtime.Encoder
	decoder   runtime.Decoder
	convertor runtime.ObjectConvertor
	creater   runtime.ObjectCreater
	typer     runtime.ObjectTyper
	defaulter runtime.ObjectDefaulter

	encodeVersion runtime.GroupVersioner
	decodeVersion runtime.GroupVersioner

	identifier runtime.Identifier

	// originalSchemeName 可选，记录该 codec 来源 scheme 的名称。
	originalSchemeName string
}

var _ runtime.EncoderWithAllocator = &codec{}

// identifiersMap 缓存已计算的标识，避免每次构造 codec 都做一次 JSON 编组；
// 以 sync.Map 支持并发读写。
var identifiersMap sync.Map

// codecIdentifier 是 codec 标识的稳定序列化形式，字段名即缓存键的一部分，
// 改动会改变缓存键（可接受，因为进程内一致即可）。
type codecIdentifier struct {
	EncodeGV string `json:"encodeGV,omitempty"`
	Encoder  string `json:"encoder,omitempty"`
	Name     string `json:"name,omitempty"`
}

// identifier 根据 codec 的参数计算 Encoder 的 Identifier。
func identifier(encodeGV runtime.GroupVersioner, encoder runtime.Encoder) runtime.Identifier {
	result := codecIdentifier{
		Name: "versioning",
	}

	if encodeGV != nil {
		result.EncodeGV = encodeGV.Identifier()
	}
	if encoder != nil {
		result.Encoder = string(encoder.Identifier())
	}
	if id, ok := identifiersMap.Load(result); ok {
		return id.(runtime.Identifier)
	}
	identifier, err := json.Marshal(result)
	if err != nil {
		//nolint:logcheck // Should not be reached.
		klog.Fatalf("Failed marshaling identifier for codec: %v", err)
	}
	identifiersMap.Store(result, runtime.Identifier(identifier))
	return runtime.Identifier(identifier)
}

// Decode 先解码对象，再尝试转换为内部版本。若提供 into 且解码成功，返回的
// runtime.Object 就是 into。注意：若 into 与序列化版本一致，可能跳过转换。
//
// 严格解码错误不会立即返回：只要对象本身仍能解出，就先把错误累积起来，
// 连同转换结果一起交还调用方，让上层决定是否降级处理。
func (c *codec) Decode(data []byte, defaultGVK *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	// into 为 unstructured 且自带 group/version 时，创建新实例以强制走转换路径，
	// 避免 `into == obj` 时的短路。
	decodeInto := into
	if into != nil {
		if _, ok := into.(runtime.Unstructured); ok && !into.GetObjectKind().GroupVersionKind().GroupVersion().Empty() {
			decodeInto = reflect.New(reflect.TypeOf(into).Elem()).Interface().(runtime.Object)
		}
	}

	var strictDecodingErrs []error
	obj, gvk, err := c.decoder.Decode(data, defaultGVK, decodeInto)
	if err != nil {
		if strictErr, ok := runtime.AsStrictDecodingError(err); obj != nil && ok {
			// 记下 strictDecodingError，交由调用方决定如何处理。
			strictDecodingErrs = append(strictDecodingErrs, strictErr.Errors()...)
		} else {
			return nil, gvk, err
		}
	}

	if d, ok := obj.(runtime.NestedObjectDecoder); ok {
		if err := d.DecodeNestedObjects(runtime.WithoutVersionDecoder{Decoder: c.decoder}); err != nil {
			if strictErr, ok := runtime.AsStrictDecodingError(err); ok {
				// 记下 strictDecodingError，交由调用方决定如何处理。
				strictDecodingErrs = append(strictDecodingErrs, strictErr.Errors()...)
			} else {
				return nil, gvk, err

			}
		}
	}

	// 将严格解码错误汇总为一个。
	var strictDecodingErr error
	if len(strictDecodingErrs) > 0 {
		strictDecodingErr = runtime.NewStrictDecodingError(strictDecodingErrs)
	}
	// 指定了目标类型时走通用转换。
	if into != nil {
		// 需要时填充默认值。
		// 默认值必须在转换之前应用，因为默认值函数注册在外部类型上。
		if c.defaulter != nil {
			c.defaulter.Default(obj)
		}

		// into 就是原对象时，跳过转换。
		if into == obj {
			return into, gvk, strictDecodingErr
		}

		if err := c.convertor.Convert(obj, into, c.decodeVersion); err != nil {
			return nil, gvk, err
		}

		return into, gvk, strictDecodingErr
	}

	// 需要时填充默认值。
	if c.defaulter != nil {
		c.defaulter.Default(obj)
	}

	out, err := c.convertor.ConvertToVersion(obj, c.decodeVersion)
	if err != nil {
		return nil, gvk, err
	}
	return out, gvk, strictDecodingErr
}

// EncodeWithAllocator 确保对象以正确的 group 和 version 输出，必要时执行转换；
// 无版本对象（按 ObjectTyper 判断）按原样输出。还可通过内存分配器降低序列化开销。
func (c *codec) EncodeWithAllocator(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	return c.encode(obj, w, memAlloc)
}

// Encode 确保对象以正确的 group 和 version 输出，必要时执行转换；
// 无版本对象（按 ObjectTyper 判断）按原样输出。
func (c *codec) Encode(obj runtime.Object, w io.Writer) error {
	return c.encode(obj, w, nil)
}

// encode 先经过 CacheableObject 的编码缓存：命中时直接复用已序列化的字节，
// 避免重复的版本转换与序列化开销。
func (c *codec) encode(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	if co, ok := obj.(runtime.CacheableObject); ok {
		return co.CacheEncode(c.Identifier(), func(obj runtime.Object, w io.Writer) error { return c.doEncode(obj, w, memAlloc) }, w)
	}
	return c.doEncode(obj, w, memAlloc)
}

// doEncode 执行实际的版本转换与序列化。若提供的编码器不支持内存分配器，
// 则退回普通 Encode 并记录日志，而不是失败。
func (c *codec) doEncode(obj runtime.Object, w io.Writer, memAlloc runtime.MemoryAllocator) error {
	encodeFn := c.encoder.Encode
	if memAlloc != nil {
		if encoder, supportsAllocator := c.encoder.(runtime.EncoderWithAllocator); supportsAllocator {
			encodeFn = func(obj runtime.Object, w io.Writer) error {
				return encoder.EncodeWithAllocator(obj, w, memAlloc)
			}
		} else {
			//nolint:logcheck // Extending the API is not worth it for contextual, structured logging of this.
			klog.V(6).Infof("a memory allocator was provided but the encoder %s doesn't implement the runtime.EncoderWithAllocator, using regular encoder.Encode method", c.encoder.Identifier())
		}
	}
	// Unknown 已携带原始字节，无需转换，直接输出。
	switch obj := obj.(type) {
	case *runtime.Unknown:
		return encodeFn(obj, w)
	case runtime.Unstructured:
		// unstructured 列表可能包含多种 group version kind 的对象，不能仅因顶层类型
		// 与目标类型相同就短路；应交给转换器，让它有机会转换列表中的各项。
		if _, ok := obj.(*unstructured.UnstructuredList); !ok {
			// GVK 已正确或为空时跳过转换往返（这确实是权宜之计，但 kubectl 依赖这一旧行为）。
			objGVK := obj.GetObjectKind().GroupVersionKind()
			if len(objGVK.Version) == 0 {
				return encodeFn(obj, w)
			}
			targetGVK, ok := c.encodeVersion.KindForGroupVersionKinds([]schema.GroupVersionKind{objGVK})
			if !ok {
				return runtime.NewNotRegisteredGVKErrForTarget(c.originalSchemeName, objGVK, c.encodeVersion)
			}
			if targetGVK == objGVK {
				return encodeFn(obj, w)
			}
		}
	}

	gvks, isUnversioned, err := c.typer.ObjectKinds(obj)
	if err != nil {
		return err
	}

	objectKind := obj.GetObjectKind()
	old := objectKind.GroupVersionKind()
	// 编码后恢复原来的 GVK。
	// 编码过程中会临时改写对象的 GVK；无论成功失败都要恢复，避免调用方
	// 看到被副作用修改过的对象。
	defer objectKind.SetGroupVersionKind(old)

	// encodeVersion 为 nil 或对象无版本时，无需转换，只回填其自身 GVK。
	if c.encodeVersion == nil || isUnversioned {
		if e, ok := obj.(runtime.NestedObjectEncoder); ok {
			if err := e.EncodeNestedObjects(runtime.WithVersionEncoder{Encoder: c.encoder, ObjectTyper: c.typer}); err != nil {
				return err
			}
		}
		objectKind.SetGroupVersionKind(gvks[0])
		return encodeFn(obj, w)
	}

	// 必要时执行转换。
	out, err := c.convertor.ConvertToVersion(obj, c.encodeVersion)
	if err != nil {
		return err
	}

	if e, ok := out.(runtime.NestedObjectEncoder); ok {
		if err := e.EncodeNestedObjects(runtime.WithVersionEncoder{Version: c.encodeVersion, Encoder: c.encoder, ObjectTyper: c.typer}); err != nil {
			return err
		}
	}

	// 转换负责为出站对象设置正确的 group、version 和 kind。
	return encodeFn(out, w)
}

// Identifier 实现 runtime.Encoder 接口。
//
// 标识在构造时已计算并缓存，返回值恒定，可安全用作缓存键。
func (c *codec) Identifier() runtime.Identifier {
	return c.identifier
}
