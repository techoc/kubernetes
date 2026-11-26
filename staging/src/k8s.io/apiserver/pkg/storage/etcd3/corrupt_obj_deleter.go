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

package etcd3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/klog/v2"
)

// 包装给定的 store 实现，增加对损坏对象执行不安全删除的支持。
//
// “unsafe” 指在无法解码/解密对象时仍允许删除，以避免一个损坏对象永久阻塞
// 该资源的 list 或写入；因此该包装只应由显式的运维命令使用。
func NewStoreWithUnsafeCorruptObjectDeletion(delegate storage.Interface, gr schema.GroupResource) storage.Interface {
	return &corruptObjectDeleter{
		Interface:     delegate,
		groupResource: gr,
	}
}

// WithCorruptObjErrorHandlingDecoder 包装解码器：把「对象无法解码」类错误
// 转换为 unsafe deleter 可识别的包装错误，以便判断能否走不安全删除。
func WithCorruptObjErrorHandlingDecoder(decoder Decoder) Decoder {
	return &corruptObjErrorInterpretingDecoder{Decoder: decoder}
}

// WithCorruptObjErrorHandlingTransformer 包装转换器：把「存储数据无法转换」类错误
// 转换为 unsafe deleter 可识别的包装错误，以便判断能否走不安全删除。
func WithCorruptObjErrorHandlingTransformer(transformer value.Transformer) value.Transformer {
	return &corruptObjErrorInterpretingTransformer{Transformer: transformer}
}

// 限制 LIST 操作中聚合的损坏对象错误数量。
//
// 上限用于避免一次 list 中大量损坏对象把错误响应撑得过大；超过上限后
// 中止并报告“列表被截断”。
const maxCorruptObjErrsToAggregate = 100

// corruptObjErrAggregatorFactory 构造一个聚合器，收集 LIST 期间遇到的损坏对象错误。
// maxCount 为最多聚合的错误数。
//
// maxCount<=0 表示不做特殊聚合，沿用默认的 list 错误聚合器。
func corruptObjErrAggregatorFactory(maxCount int) func() listItemErrors {
	if maxCount <= 0 {
		return defaultListErrorAggregatorFactory
	}
	return func() listItemErrors {
		return &corruptObjErrAggregator{maxCount: maxCount}
	}
}

var errTooMany = errors.New("too many errors, the list is truncated")

// 聚合 LIST 操作中的损坏对象错误。
//
// Append 返回 true 表示“继续聚合也无意义/必须中止”，由调用方停止遍历。
type corruptObjErrAggregator struct {
	errs     []error
	abortErr error
	maxCount int
}

// Append 记录一个 item 级错误，并返回是否应中止 list。
// 只有“对象损坏”类错误可以继续；其它错误（例如 etcd 连接失败）必须立即中止。
func (a *corruptObjErrAggregator) Append(key string, err error) bool {
	if a.abortErr != nil {
		return true
	}

	var corruptObjErr *corruptObjectError
	if errors.As(err, &corruptObjErr) {
		a.errs = append(a.errs, storage.NewCorruptObjError(key, corruptObjErr))
		if len(a.errs) >= a.maxCount {
			// 超过上限即中止遍历。
			a.abortErr = errTooMany
			return true
		}
		return false
	}

	// 非损坏对象错误，LIST 必须中止。
	a.abortErr = err
	return true
}

// Aggregate 汇总错误：没有任何损坏对象时直接返回 abortErr（或 nil）；
// 否则返回 aggregatedStorageError，并附带导致中止的原因。
func (a *corruptObjErrAggregator) Aggregate() error {
	switch {
	case len(a.errs) == 0 && a.abortErr != nil:
		return a.abortErr
	case len(a.errs) > 0:
		err := utilerrors.NewAggregate(a.errs)
		return &aggregatedStorageError{errs: err, abortErr: a.abortErr, resourcePrefix: "list"}
	default:
		return nil
	}
}

// corruptObjectDeleter 为 etcd 提供损坏对象的不安全删除能力。
type corruptObjectDeleter struct {
	storage.Interface
	groupResource schema.GroupResource
}

// Get 在底层 Get 失败时判断是否为“对象损坏”；若是，则用带 key 的
// 包装错误返回，供 registry 层识别并走 unsafe delete 流程。
func (s *corruptObjectDeleter) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	if err := s.Interface.Get(ctx, key, opts, out); err != nil {
		var corruptObjErr *corruptObjectError
		if !errors.As(err, &corruptObjErr) {
			// 不是损坏对象错误。
			return err
		}
		// registry 层的 unsafe deleter 会据此判断是否为损坏对象，
		// 从而启动不安全删除流程。
		return storage.NewCorruptObjError(key, corruptObjErr)
	}
	return nil
}

// GetList 处理 list 中聚合出的损坏对象错误：转换为带 causes 的
// 500 StoreReadError，让客户端/运维能看到每个损坏对象的位置。
func (s *corruptObjectDeleter) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	err := s.Interface.GetList(ctx, key, opts, listObj)
	if err == nil {
		return nil
	}

	var aggregatedErr *aggregatedStorageError
	if errors.As(err, &aggregatedErr) {
		// 已聚合出损坏对象列表。
		klog.V(5).ErrorS(aggregatedErr, "corrupt objects")
		return aggregatedErr.NewAPIStatusError(s.groupResource)
	}
	return err
}

// corruptObjErrorInterpretingDecoder 包装被装饰解码器返回的错误。
//
// 目前任何解码错误都归为 undecodable；未来可以按错误类型细分（例如网络错误不算损坏）。
type corruptObjErrorInterpretingDecoder struct {
	Decoder
}

// Decode 把解码失败包装为 corruptObjectError，并记录 revision 便于定位。
func (d *corruptObjErrorInterpretingDecoder) Decode(value []byte, objPtr runtime.Object, rev int64) error {
	// TODO: 目前所有错误都视为无法解码，未来可按需过滤。
	if err := d.Decoder.Decode(value, objPtr, rev); err != nil {
		return &corruptObjectError{err: err, errType: undecodable, revision: rev}
	}
	return nil
}

// decodeListItem 把数组中的字节值解码为对象。
//
// list 路径使用 uint64 的 revision，这里转换为 int64 以复用同一错误类型。
func (d *corruptObjErrorInterpretingDecoder) DecodeListItem(ctx context.Context, data []byte, rev uint64, newItemFunc func() runtime.Object) (runtime.Object, error) {
	// TODO: 目前所有错误都视为无法解码，未来可按需过滤。
	obj, err := d.Decoder.DecodeListItem(ctx, data, rev, newItemFunc)
	if err != nil {
		err = &corruptObjectError{err: err, errType: undecodable, revision: int64(rev)}
	}
	return obj, err
}

// 包装转换器返回的错误。
//
// 解密/反序列化失败通常意味着密钥轮换错误或数据被篡改，这类对象同样只能
// 通过 unsafe delete 清理。
type corruptObjErrorInterpretingTransformer struct {
	value.Transformer
}

// TransformFromStorage 把转换失败包装为 corruptObjectError（untransformable）。
func (t *corruptObjErrorInterpretingTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	// TODO: 目前所有错误都视为无法解码，未来可按需过滤，例如排除网络错误。
	out, stale, err := t.Transformer.TransformFromStorage(ctx, data, dataCtx)
	if err != nil {
		err = &corruptObjectError{err: err, errType: untransformable}
	}
	return out, stale, err
}

// corruptObjectError 仅供损坏对象删除器内部使用，表示：
// a) 存储中的数据转换失败，或 b) 数据无法解码为对象。
// 注意：该错误不携带损坏对象的身份信息（如对应的存储 key）。
type corruptObjectError struct {
	err      error
	errType  int
	revision int64
}

const (
	untransformable int = iota + 1
	undecodable
)

var typeToMessage = map[int]string{
	untransformable: "data from the storage is not transformable",
	undecodable:     "object not decodable",
}

func (e *corruptObjectError) Unwrap() error { return e.err }
func (e *corruptObjectError) Error() string {
	return fmt.Sprintf("%s revision=%d: %v", typeToMessage[e.errType], e.revision, e.err)
}

// 聚合一组 storage.StorageError。
//
// 它同时保留“损坏对象清单”与“中止原因”，最终被转换为一个带 causes 的
// API 错误，让每个损坏对象的 key 都出现在错误的 details.causes 中。
type aggregatedStorageError struct {
	resourcePrefix string
	errs           utilerrors.Aggregate
	// 聚合中止的原因；所有条目都正常处理时为 nil。
	abortErr error
}

func (e *aggregatedStorageError) Error() string {
	errs := e.errs.Errors()
	var b strings.Builder
	fmt.Fprintf(&b, "unable to transform or decode %d objects: {\n", len(errs))
	for _, err := range errs {
		fmt.Fprintf(&b, "\t%s\n", err.Error())
	}
	b.WriteString("}")
	if e.abortErr != nil {
		fmt.Fprintf(&b, ", aborted: %v", e.abortErr)
	}
	return b.String()
}

// NewAPIStatusError 根据聚合的 StorageError 列表构造 APIStatus 错误。
func (e *aggregatedStorageError) NewAPIStatusError(qualifiedResource schema.GroupResource) *apierrors.StatusError {
	errs := e.errs.Errors()
	var causes []metav1.StatusCause
	for _, err := range errs {
		var storageErr *storage.StorageError
		if errors.As(err, &storageErr) {
			causes = append(causes, metav1.StatusCause{
				Type:  metav1.CauseTypeUnexpectedServerResponse,
				Field: storageErr.Key,
				// TODO: 这里是否需要暴露内部错误信息？
				Message: err.Error(),
			})
		}
	}
	switch {
	case errors.Is(e.abortErr, errTooMany):
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeTooMany,
			Message: errTooMany.Error(),
		})
	case e.abortErr != nil:
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeUnexpectedServerResponse,
			Message: e.abortErr.Error(),
		})
	}

	// 顶层消息带上第一个底层错误，避免被只保留文本、丢弃 causes 的层吃掉，
	// 例如 watch cache 包装 reflector list 错误的场景。
	message := fmt.Sprintf("failed to read one or more %s from the storage: %v", qualifiedResource.String(), errs[0])
	if len(errs) > 1 {
		message = fmt.Sprintf("%s (and %d more)", message, len(errs)-1)
	}

	return &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Status: metav1.StatusFailure,
			Code:   http.StatusInternalServerError,
			Reason: metav1.StatusReasonStoreReadError,
			Details: &metav1.StatusDetails{
				Group:  qualifiedResource.Group,
				Kind:   qualifiedResource.Resource,
				Name:   e.resourcePrefix,
				Causes: causes,
			},
			Message: message,
		},
	}
}
