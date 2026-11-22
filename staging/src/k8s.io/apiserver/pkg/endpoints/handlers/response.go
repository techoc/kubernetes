/*
Copyright 2017 The Kubernetes Authors.

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

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1beta1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	endpointsrequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/util/apihelpers"
	"k8s.io/klog/v2"
)

// watchEmbeddedEncoder performs encoding of the embedded object.
//
// NOTE: watchEmbeddedEncoder is NOT thread-safe.
//
// 每个 watch 请求独立持有一个实例；tableOptions 在首个事件后被就地修改，
// 因此不能跨请求共享。
type watchEmbeddedEncoder struct {
	encoder runtime.Encoder

	ctx context.Context

	// target, if non-nil, configures transformation type.
	// The other options are ignored if target is nil.
	target       *schema.GroupVersionKind
	tableOptions *metav1.TableOptions
	scope        *RequestScope

	// identifier of the encoder, computed lazily
	// 惰性计算是因为 tableOptions 会在编码过程中变化（首事件后去掉表头）。
	identifier runtime.Identifier
}

// newWatchEmbeddedEncoder 构造每个 watch 请求专用的嵌入对象编码器。
func newWatchEmbeddedEncoder(ctx context.Context, encoder runtime.Encoder, target *schema.GroupVersionKind, tableOptions *metav1.TableOptions, scope *RequestScope) *watchEmbeddedEncoder {
	return &watchEmbeddedEncoder{
		encoder:      encoder,
		ctx:          ctx,
		target:       target,
		tableOptions: tableOptions,
		scope:        scope,
	}
}

// Encode implements runtime.Encoder interface.
// CacheableObject 允许把同一对象的序列化结果在多个 watch 之间复用。
func (e *watchEmbeddedEncoder) Encode(obj runtime.Object, w io.Writer) error {
	if co, ok := obj.(runtime.CacheableObject); ok {
		return co.CacheEncode(e.Identifier(), e.doEncode, w)
	}
	return e.doEncode(obj, w)
}

// doEncode 执行转换与编码；转换失败时退化为编码原始对象，
// 保证 watch 流不因单个对象的转换问题而中断。
func (e *watchEmbeddedEncoder) doEncode(obj runtime.Object, w io.Writer) error {
	result, err := doTransformObject(e.ctx, obj, e.tableOptions, e.target, e.scope)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to transform object %v: %v", reflect.TypeOf(obj), err))
		result = obj
	}

	// When we are tranforming to a table, use the original table options when
	// we should print headers only on the first object - headers should be
	// omitted on subsequent events.
	// 注意：这里修改的是调用方传入的 tableOptions 指针，属于请求内状态。
	if e.tableOptions != nil && !e.tableOptions.NoHeaders {
		e.tableOptions.NoHeaders = true
		// With options change, we should recompute the identifier.
		// Clearing this will trigger lazy recompute when needed.
		e.identifier = ""
	}

	return e.encoder.Encode(result, w)
}

// Identifier implements runtime.Encoder interface.
func (e *watchEmbeddedEncoder) Identifier() runtime.Identifier {
	if e.identifier == "" {
		e.identifier = e.embeddedIdentifier()
	}
	return e.identifier
}

// watchEmbeddedEncoderIdentifier 是 identifier 的 JSON 序列化形态，
// 字段变化即意味着缓存键变化，因此必须覆盖所有影响输出的选项。
type watchEmbeddedEncoderIdentifier struct {
	Name      string              `json:"name,omitempty"`
	Encoder   string              `json:"encoder,omitempty"`
	Target    string              `json:"target,omitempty"`
	Options   metav1.TableOptions `json:"options,omitempty"`
	NoHeaders bool                `json:"noHeaders,omitempty"`
}

// embeddedIdentifier 计算缓存键；无转换时直接复用底层编码器的标识，
// 使缓存可以与普通请求共享。
func (e *watchEmbeddedEncoder) embeddedIdentifier() runtime.Identifier {
	if e.target == nil {
		// If no conversion is performed, we effective only use
		// the embedded identifier.
		return e.encoder.Identifier()
	}
	identifier := watchEmbeddedEncoderIdentifier{
		Name:    "watch-embedded",
		Encoder: string(e.encoder.Identifier()),
		Target:  e.target.String(),
	}
	// 只有 Table 的输出受 tableOptions 影响，其他转换类型无需纳入缓存键。
	if e.target.Kind == "Table" && e.tableOptions != nil {
		identifier.Options = *e.tableOptions
		identifier.NoHeaders = e.tableOptions.NoHeaders
	}

	result, err := json.Marshal(identifier)
	if err != nil {
		// identifier 只含字符串与基础类型，序列化失败属于不可恢复的编程错误。
		klog.Fatalf("Failed marshaling identifier for watchEmbeddedEncoder: %v", err)
	}
	return runtime.Identifier(result)
}

// watchEncoder performs encoding of the watch events.
//
// NOTE: watchEncoder is NOT thread-safe.
//
// 它把事件对象编码为 RawExtension，再包进 metav1.WatchEvent 编码到 framer。
// 每个 watch 请求一个实例，buffer 复用避免每事件分配。
type watchEncoder struct {
	ctx                  context.Context
	groupVersionResource schema.GroupVersionResource
	embeddedEncoder      runtime.Encoder
	encoder              runtime.Encoder
	framer               io.Writer

	// buffer 存放已编码的事件对象，eventBuffer 存放完整的 WatchEvent。
	buffer      runtime.Splice
	eventBuffer runtime.Splice

	currentEmbeddedIdentifier runtime.Identifier
	identifiers               map[watch.EventType]runtime.Identifier
}

// newWatchEncoder 构造 watch 编码器并初始化可复用的 splice buffer。
func newWatchEncoder(ctx context.Context, gvr schema.GroupVersionResource, embeddedEncoder runtime.Encoder, encoder runtime.Encoder, framer io.Writer) *watchEncoder {
	return &watchEncoder{
		ctx:                  ctx,
		groupVersionResource: gvr,
		embeddedEncoder:      embeddedEncoder,
		encoder:              encoder,
		framer:               framer,
		buffer:               runtime.NewSpliceBuffer(),
		eventBuffer:          runtime.NewSpliceBuffer(),
	}
}

// Encode encodes a given watch event.
// NOTE: if events object is implementing the CacheableObject interface,
//
//	the serialized version is cached in that object [not the event itself].
func (e *watchEncoder) Encode(event watch.Event) error {
	encodeFunc := func(obj runtime.Object, w io.Writer) error {
		return e.doEncode(obj, event, w)
	}
	// 缓存键包含事件类型，因为不同类型包装后输出不同。
	if co, ok := event.Object.(runtime.CacheableObject); ok {
		return co.CacheEncode(e.identifier(event.Type), encodeFunc, e.framer)
	}
	return encodeFunc(event.Object, e.framer)
}

// doEncode 先编码事件对象，再包装为 WatchEvent 编码后写入 framer。
func (e *watchEncoder) doEncode(obj runtime.Object, event watch.Event, w io.Writer) error {
	defer e.buffer.Reset()

	if err := e.embeddedEncoder.Encode(obj, e.buffer); err != nil {
		return fmt.Errorf("unable to encode watch object %T: %v", obj, err)
	}

	// ContentType is not required here because we are defaulting to the serializer type.
	outEvent := &metav1.WatchEvent{
		Type:   string(event.Type),
		Object: runtime.RawExtension{Raw: e.buffer.Bytes()},
	}

	defer e.eventBuffer.Reset()
	if err := e.encoder.Encode(outEvent, e.eventBuffer); err != nil {
		return fmt.Errorf("unable to encode watch object %T: %v (%#v)", outEvent, err, e)
	}

	_, err := w.Write(e.eventBuffer.Bytes())
	return err
}

// watchEncoderIdentifier 是 watch 编码器的缓存键组成。
type watchEncoderIdentifier struct {
	Name            string `json:"name,omitempty"`
	EmbeddedEncoder string `json:"embeddedEncoder,omitempty"`
	Encoder         string `json:"encoder,omitempty"`
	EventType       string `json:"eventType,omitempty"`
}

// identifier 按事件类型缓存缓存键；嵌入编码器标识变化（如 table 转表头状态改变）时整体失效。
func (e *watchEncoder) identifier(eventType watch.EventType) runtime.Identifier {
	// We need to take into account that in embeddedEncoder includes table
	// transformer, then its identifier is dynamic. As a result, whenever
	// the identifier of embeddedEncoder changes, we need to invalidate the
	// whole identifiers cache.
	// TODO(wojtek-t): Can we optimize it somehow?
	if e.currentEmbeddedIdentifier != e.embeddedEncoder.Identifier() {
		e.currentEmbeddedIdentifier = e.embeddedEncoder.Identifier()
		e.identifiers = map[watch.EventType]runtime.Identifier{}
	}
	if _, ok := e.identifiers[eventType]; !ok {
		e.identifiers[eventType] = e.typeIdentifier(eventType)
	}
	return e.identifiers[eventType]
}

// typeIdentifier 计算单个事件类型的缓存键。
func (e *watchEncoder) typeIdentifier(eventType watch.EventType) runtime.Identifier {
	// The eventType is a non-standard pattern. This is coming from the fact
	// that we're effectively serializing the whole watch event, but storing
	// it in serializations of the Object within the watch event.
	identifier := watchEncoderIdentifier{
		Name:            "watch",
		EmbeddedEncoder: string(e.embeddedEncoder.Identifier()),
		Encoder:         string(e.encoder.Identifier()),
		EventType:       string(eventType),
	}

	result, err := json.Marshal(identifier)
	if err != nil {
		klog.Fatalf("Failed marshaling identifier for watchEncoder: %v", err)
	}
	return runtime.Identifier(result)
}

// doTransformResponseObject is used for handling all requests, including watch.
//
// 把内部对象转换为客户端请求的形态（PartialObjectMetadata / Table）。
// Status 对象不转换：错误响应必须保持原样，否则客户端无法解析。
func doTransformObject(ctx context.Context, obj runtime.Object, opts interface{}, target *schema.GroupVersionKind, scope *RequestScope) (runtime.Object, error) {
	if _, ok := obj.(*metav1.Status); ok {
		return obj, nil
	}

	switch {
	case target == nil:
		// If we ever change that from a no-op, the identifier of
		// the watchEmbeddedEncoder has to be adjusted accordingly.
		return obj, nil

	case target.Kind == "PartialObjectMetadata":
		return asPartialObjectMetadata(obj, target.GroupVersion())

	case target.Kind == "PartialObjectMetadataList":
		return asPartialObjectMetadataList(obj, target.GroupVersion())

	case target.Kind == "Table":
		options, ok := opts.(*metav1.TableOptions)
		if !ok {
			return nil, fmt.Errorf("unexpected TableOptions, got %T", opts)
		}
		return asTable(ctx, obj, options, scope, target.GroupVersion())

	default:
		// 未知转换类型按 NotAcceptable 处理，列出可用媒体类型帮助客户端修正 Accept 头。
		accepted, _ := negotiation.MediaTypesForSerializer(apihelpers.GetMetaInternalVersionCodecs())
		err := negotiation.NewNotAcceptableError(accepted)
		return nil, err
	}
}

// optionsForTransform will load and validate any additional query parameter options for
// a conversion or return an error.
func optionsForTransform(mediaType negotiation.MediaTypeOptions, req *http.Request) (interface{}, error) {
	switch target := mediaType.Convert; {
	case target == nil:
	case target.Kind == "Table" && (target.GroupVersion() == metav1beta1.SchemeGroupVersion || target.GroupVersion() == metav1.SchemeGroupVersion):
		opts := &metav1.TableOptions{}
		// Table 的 includeObject 等选项通过查询参数传入，需要严格校验。
		if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), metav1.SchemeGroupVersion, opts); err != nil {
			return nil, err
		}
		switch errs := validation.ValidateTableOptions(opts); len(errs) {
		case 0:
			return opts, nil
		case 1:
			return nil, errors.NewBadRequest(fmt.Sprintf("Unable to convert to Table as requested: %v", errs[0].Error()))
		default:
			return nil, errors.NewBadRequest(fmt.Sprintf("Unable to convert to Table as requested: %v", errs))
		}
	}
	return nil, nil
}

// targetEncodingForTransform returns the appropriate serializer for the input media type
// 元数据类转换使用 meta 内部版本编解码器，其余情况沿用资源自身的序列化器。
func targetEncodingForTransform(scope *RequestScope, mediaType negotiation.MediaTypeOptions, req *http.Request) (schema.GroupVersionKind, runtime.NegotiatedSerializer, bool) {
	switch target := mediaType.Convert; {
	case target == nil:
	case (target.Kind == "PartialObjectMetadata" || target.Kind == "PartialObjectMetadataList" || target.Kind == "Table") &&
		(target.GroupVersion() == metav1beta1.SchemeGroupVersion || target.GroupVersion() == metav1.SchemeGroupVersion):
		return *target, apihelpers.GetMetaInternalVersionCodecs(), true
	}
	return scope.Kind, scope.Serializer, false
}

// transformResponseObject takes an object loaded from storage and performs any necessary transformations.
// Will write the complete response object.
// transformResponseObject is used only for handling non-streaming requests.
func transformResponseObject(ctx context.Context, scope *RequestScope, req *http.Request, w http.ResponseWriter, statusCode int, mediaType negotiation.MediaTypeOptions, result runtime.Object) {
	options, err := optionsForTransform(mediaType, req)
	if err != nil {
		scope.err(err, w, req)
		return
	}

	// ensure that for empty lists we don't return <nil> items.
	// This is safe to modify without deep-copying the object, as
	// List objects themselves are never cached.
	// 空列表若 Items 为 nil，JSON 会输出 null 而非 []，破坏客户端反序列化。
	if meta.IsListType(result) && meta.LenList(result) == 0 {
		if err := meta.SetList(result, []runtime.Object{}); err != nil {
			scope.err(err, w, req)
			return
		}
	}

	var obj runtime.Object
	do := func() {
		obj, err = doTransformObject(ctx, result, options, mediaType.Convert, scope)
	}
	// 单独计时转换阶段，便于区分存储耗时与转换耗时。
	endpointsrequest.TrackTransformResponseObjectLatency(ctx, do)

	if err != nil {
		scope.err(err, w, req)
		return
	}
	kind, serializer, _ := targetEncodingForTransform(scope, mediaType, req)
	responsewriters.WriteObjectNegotiated(serializer, scope, kind.GroupVersion(), w, req, statusCode, obj, false)
}

// errNotAcceptable indicates Accept negotiation has failed
type errNotAcceptable struct {
	message string
}

// newNotAcceptableError 构造 406 错误。
func newNotAcceptableError(message string) error {
	return errNotAcceptable{message}
}

func (e errNotAcceptable) Error() string {
	return e.message
}

// Status 让该错误可以被统一的状态响应写出逻辑识别为 406。
func (e errNotAcceptable) Status() metav1.Status {
	return metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusNotAcceptable,
		Reason:  metav1.StatusReason("NotAcceptable"),
		Message: e.Error(),
	}
}

// asTable 把任意对象转换为 Table 表示；行内对象按 includeObject 选项决定保留粒度。
func asTable(ctx context.Context, result runtime.Object, opts *metav1.TableOptions, scope *RequestScope, groupVersion schema.GroupVersion) (runtime.Object, error) {
	switch groupVersion {
	case metav1beta1.SchemeGroupVersion, metav1.SchemeGroupVersion:
	default:
		return nil, newNotAcceptableError(fmt.Sprintf("no Table exists in group version %s", groupVersion))
	}

	obj, err := scope.TableConvertor.ConvertToTable(ctx, result, opts)
	if err != nil {
		return nil, err
	}

	table := (*metav1.Table)(obj)

	for i := range table.Rows {
		item := &table.Rows[i]

		// watch cache 的初始事件结束书签必须保留对象，否则客户端无法识别分页边界。
		hasInitialEventsEndAnnotation, err := storage.HasInitialEventsEndBookmarkAnnotation(item.Object.Object)
		if err != nil {
			return nil, errors.NewInternalError(fmt.Errorf("unable to determine if the obj has the required annotation, err: %w", err))
		}

		switch {
		case opts.IncludeObject == metav1.IncludeObject:
			// 完整对象需要转换回请求的版本，否则客户端拿到内部版本结构。
			item.Object.Object, err = scope.Convertor.ConvertToVersion(item.Object.Object, scope.Kind.GroupVersion())
			if err != nil {
				return nil, err
			}
		// TODO: rely on defaulting for the value here?
		case opts.IncludeObject == metav1.IncludeMetadata, hasInitialEventsEndAnnotation, opts.IncludeObject == "":
			// 默认（空值）与 IncludeMetadata 一致，只返回元数据以减小响应体积。
			m, err := meta.Accessor(item.Object.Object)
			if err != nil {
				return nil, err
			}
			// TODO: turn this into an internal type and do conversion in order to get object kind automatically set?
			partial := meta.AsPartialObjectMetadata(m)
			partial.GetObjectKind().SetGroupVersionKind(groupVersion.WithKind("PartialObjectMetadata"))
			item.Object.Object = partial
		case opts.IncludeObject == metav1.IncludeNone:
			// IncludeNone 用于纯表格展示，行内不携带对象。
			item.Object.Object = nil
		default:
			err = errors.NewBadRequest(fmt.Sprintf("unrecognized includeObject value: %q", opts.IncludeObject))
			return nil, err
		}
	}

	return table, nil
}

// asPartialObjectMetadata 把单个对象裁剪为只含元数据的 PartialObjectMetadata。
func asPartialObjectMetadata(result runtime.Object, groupVersion schema.GroupVersion) (runtime.Object, error) {
	if meta.IsListType(result) {
		err := newNotAcceptableError(fmt.Sprintf("you requested PartialObjectMetadata, but the requested object is a list (%T)", result))
		return nil, err
	}
	switch groupVersion {
	case metav1beta1.SchemeGroupVersion, metav1.SchemeGroupVersion:
	default:
		return nil, newNotAcceptableError(fmt.Sprintf("no PartialObjectMetadataList exists in group version %s", groupVersion))
	}
	m, err := meta.Accessor(result)
	if err != nil {
		return nil, err
	}
	partial := meta.AsPartialObjectMetadata(m)
	partial.GetObjectKind().SetGroupVersionKind(groupVersion.WithKind("PartialObjectMetadata"))
	return partial, nil
}

// asPartialObjectMetadataList 把列表逐项裁剪为元数据，并保留分页相关字段。
func asPartialObjectMetadataList(result runtime.Object, groupVersion schema.GroupVersion) (runtime.Object, error) {
	li, ok := result.(metav1.ListInterface)
	if !ok {
		return nil, newNotAcceptableError(fmt.Sprintf("you requested PartialObjectMetadataList, but the requested object is not a list (%T)", result))
	}

	gvk := groupVersion.WithKind("PartialObjectMetadata")
	switch {
	case groupVersion == metav1beta1.SchemeGroupVersion:
		list := &metav1beta1.PartialObjectMetadataList{}
		err := meta.EachListItem(result, func(obj runtime.Object) error {
			m, err := meta.Accessor(obj)
			if err != nil {
				return err
			}
			partial := meta.AsPartialObjectMetadata(m)
			partial.GetObjectKind().SetGroupVersionKind(gvk)
			list.Items = append(list.Items, *partial)
			return nil
		})
		if err != nil {
			return nil, err
		}
		// ResourceVersion/Continue/RemainingItemCount 必须透传，否则分页失效。
		list.ResourceVersion = li.GetResourceVersion()
		list.Continue = li.GetContinue()
		list.RemainingItemCount = li.GetRemainingItemCount()
		return list, nil

	case groupVersion == metav1.SchemeGroupVersion:
		list := &metav1.PartialObjectMetadataList{}
		err := meta.EachListItem(result, func(obj runtime.Object) error {
			m, err := meta.Accessor(obj)
			if err != nil {
				return err
			}
			partial := meta.AsPartialObjectMetadata(m)
			partial.GetObjectKind().SetGroupVersionKind(gvk)
			list.Items = append(list.Items, *partial)
			return nil
		})
		if err != nil {
			return nil, err
		}
		list.ResourceVersion = li.GetResourceVersion()
		list.Continue = li.GetContinue()
		list.RemainingItemCount = li.GetRemainingItemCount()
		return list, nil

	default:
		return nil, newNotAcceptableError(fmt.Sprintf("no PartialObjectMetadataList exists in group version %s", groupVersion))
	}
}
