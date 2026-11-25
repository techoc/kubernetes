/*
Copyright 2016 The Kubernetes Authors.

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
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.opentelemetry.io/otel/attribute"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	etcdrpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	etcdfeature "k8s.io/apiserver/pkg/storage/feature"
	"k8s.io/apiserver/pkg/storage/value"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
)

const (
	// maxLimit 是从 etcd 拉取对象时页大小上限的最大增长值。该上限仅用于
	// kube-apiserver 自行增大页大小；若请求最初就指定了更大的 limit，
	// 则不会更改。
	maxLimit = 10000
)

// authenticatedDataString 满足 value.Context 接口，使用 key 对存储的数据做
// 认证。它不能防御同一 key 下历史加密值的重用，但可以阻止攻击者把来自
// 其他 key 的加密值搬过来用。更强的认证数据段应当包含 etcd3 的 Version
// 字段（每次写 key 时自增、key 删除时重置），但对 etcd 有写权限的攻击者
// 可以强制删除并重建 key 来削弱这一手段。
type authenticatedDataString string

// AuthenticatedData 实现 value.Context 接口。
// 认证数据是 etcd 中的完整 key：加密值被搬到另一个 key 下会因 AAD 不匹配而解密失败。
func (d authenticatedDataString) AuthenticatedData() []byte {
	return []byte(string(d))
}

var _ value.Context = authenticatedDataString("")

// store 是 storage.Interface 的 etcd3 实现。
//
// 所有写入都通过 OptimisticPut 携带预期 revision，冲突时重读并重试；
// 这依赖 etcd 的 MVCC 修订号来提供乐观并发控制，从而避免分布式锁。
type store struct {
	client             *kubernetes.Client
	codec              runtime.Codec
	versioner          storage.Versioner
	transformer        value.Transformer
	pathPrefix         string
	groupResource      schema.GroupResource
	watcher            *watcher
	leaseManager       *leaseManager
	decoder            Decoder
	listErrAggrFactory func() listItemErrors

	resourcePrefix string
	newListFunc    func() runtime.Object
	compactor      Compactor

	collectorMux          sync.RWMutex
	resourceSizeEstimator *resourceSizeEstimator
}

var _ storage.Interface = (*store)(nil)

// RequestWatchProgress 向 etcd 请求一次进度通知，使 watch 能确认已收到当前最新事件。
// 使用 watchContext 是因为无法从 watchCache 取回原始 watch 上下文。
func (s *store) RequestWatchProgress(ctx context.Context) error {
	// 使用 watchContext 是为了与创建 watch 时提供的 ctx 元数据保持一致。
	// 最理想的情况是复用 watch 创建时的同一个 context，但无法从 watchCache
	// 访问到它。
	return s.client.RequestProgress(s.watchContext(ctx))
}

// objState 是乐观更新循环中使用的对象快照：解码后的对象、修订号与原始字节。
type objState struct {
	obj   runtime.Object
	meta  *storage.ResponseMeta
	rev   int64
	data  []byte
	stale bool
}

// listItemErrors 在 LIST 操作期间累积保存一组单条对象的存储错误，最终作为
// 聚合错误返回。每次 Append 时，它会示意自己是否已满、或该错误是否为致命
// 错误。无论哪种情况，调用方都不应继续收集，而应直接返回该集合。
type listItemErrors interface {
	// Append 记录 LIST 操作中给定存储 key 的单条对象存储错误。
	// 一旦返回 true，调用方就应立即停止继续追加。
	Append(key string, err error) bool

	// Aggregate 返回聚合后的错误。
	Aggregate() error
}

// defaultListErrorAggregatorFactory 返回默认的 LIST 错误聚合器，以保持
// 向后兼容：一旦遇到第一个错误就中止 LIST 操作。
func defaultListErrorAggregatorFactory() listItemErrors { return &abortOnFirstError{} }

// abortOnFirstError 在遇到第一个错误时即中止 LIST（向后兼容行为）。
type abortOnFirstError struct {
	err error
}

// Append 记录首个错误并返回 true，指示调用方立即停止继续收集。
func (a *abortOnFirstError) Append(key string, err error) bool {
	a.err = err
	return true
}

// Aggregate 返回已记录的错误（未出错时为 nil）。
func (a *abortOnFirstError) Aggregate() error { return a.err }

// New 返回 storage.Interface 的 etcd3 实现。
//
// prefix 是 etcd 全局前缀（通常是 /registry），resourcePrefix 是资源在其中的根路径；
// 两者拼接后才是最终 key，因此路径规范化必须在构造期完成，避免运行期反复处理。
func New(c *kubernetes.Client, compactor Compactor, codec runtime.Codec, newFunc, newListFunc func() runtime.Object, prefix, resourcePrefix string, groupResource schema.GroupResource, transformer value.Transformer, leaseManagerConfig LeaseManagerConfig, decoder Decoder, versioner storage.Versioner) (*store, error) {
	// 为兼容 etcd2 实现；默认前缀 '/registry' 时为空操作；
	// 对不以 '/' 开头的自定义前缀同样保持与 etcd2 实现的兼容。
	pathPrefix := path.Join("/", prefix)
	if !strings.HasSuffix(pathPrefix, "/") {
		// 此处确保 pathPrefix 以 "/" 结尾，简化后续的 key 拼接。
		pathPrefix += "/"
	}
	if resourcePrefix == "" {
		return nil, fmt.Errorf("resourcePrefix cannot be empty")
	}
	if resourcePrefix == "/" {
		return nil, fmt.Errorf("resourcePrefix cannot be /")
	}
	if !strings.HasPrefix(resourcePrefix, "/") {
		return nil, fmt.Errorf("resourcePrefix needs to start from /")
	}

	listErrAggrFactory := defaultListErrorAggregatorFactory
	// 只有允许删除损坏对象时才启用"收集多个错误"的聚合器，
	// 使运维能一次看到所有坏对象，而不是修一个再跑一次。
	if utilfeature.DefaultFeatureGate.Enabled(features.AllowUnsafeMalformedObjectDeletion) {
		listErrAggrFactory = corruptObjErrAggregatorFactory(maxCorruptObjErrsToAggregate)
	}

	w := &watcher{
		client:        c.Client,
		codec:         codec,
		newFunc:       newFunc,
		groupResource: groupResource,
		versioner:     versioner,
		transformer:   transformer,
	}
	if newFunc == nil {
		w.objectType = "<unknown>"
	} else {
		w.objectType = reflect.TypeOf(newFunc()).String()
	}
	s := &store{
		client:             c,
		codec:              codec,
		versioner:          versioner,
		transformer:        transformer,
		pathPrefix:         pathPrefix,
		groupResource:      groupResource,
		watcher:            w,
		leaseManager:       newDefaultLeaseManager(c.Client, leaseManagerConfig),
		decoder:            decoder,
		listErrAggrFactory: listErrAggrFactory,

		resourcePrefix: resourcePrefix,
		newListFunc:    newListFunc,
		compactor:      compactor,
	}

	w.getResourceSizeEstimator = s.getResourceSizeEstimator
	w.getCurrentStorageRV = func(ctx context.Context) (uint64, error) {
		return s.GetCurrentResourceVersion(ctx)
	}
	etcdfeature.DefaultFeatureSupportChecker.CheckClient(c.Ctx(), c, storage.RequestWatchProgress)
	return s, nil
}

// CompactRevision 返回 etcd 当前的压缩修订号；没有 compactor 时返回 0。
func (s *store) CompactRevision() int64 {
	if s.compactor == nil {
		return 0
	}
	return s.compactor.CompactRevision()
}

// Versioner 实现 storage.Interface.Versioner。
func (s *store) Versioner() storage.Versioner {
	return s.versioner
}

// Close 释放资源大小估算器占用的后台协程。
func (s *store) Close() {
	stats := s.getResourceSizeEstimator()
	if stats != nil {
		stats.Close()
	}
}

// getResourceSizeEstimator 读锁保护：它可能在运行期被 EnableResourceSizeEstimation 替换。
func (s *store) getResourceSizeEstimator() *resourceSizeEstimator {
	s.collectorMux.RLock()
	defer s.collectorMux.RUnlock()
	return s.resourceSizeEstimator
}

// Get 实现 storage.Interface.Get。
func (s *store) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	preparedKey, err := s.prepareKey(key, false)
	if err != nil {
		return err
	}
	ctx, span := tracing.Start(ctx, "Get etcd3",
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)),
		attribute.String("key", key),
		attribute.String("group", s.groupResource.Group),
		attribute.String("resource", s.groupResource.Resource),
	)
	defer span.End(500 * time.Millisecond)
	startTime := time.Now()
	getResp, err := s.client.Kubernetes.Get(ctx, preparedKey, kubernetes.GetOptions{})
	metrics.RecordEtcdRequest("get", s.groupResource, err, startTime)
	if err != nil {
		span.AddEvent("Get call failed", attribute.String("err", err.Error()))
		return err
	}
	span.AddEvent("Get call succeeded")
	if err = s.validateMinimumResourceVersion(opts.ResourceVersion, uint64(getResp.Revision)); err != nil {
		return err
	}

	if getResp.KV == nil {
		// IgnoreNotFound 让调用方把"不存在"当作零值对象处理（如 create-on-update 的探测）。
		if opts.IgnoreNotFound {
			return runtime.SetZeroValue(out)
		}
		return storage.NewKeyNotFoundError(preparedKey, 0)
	}

	// 解密时把 key 作为认证数据传入，绑定密文与其存储位置。
	data, _, err := s.transformer.TransformFromStorage(ctx, getResp.KV.Value, authenticatedDataString(preparedKey))
	if err != nil {
		span.AddEvent("TransformFromStorage failed", attribute.String("err", err.Error()))
		return storage.NewInternalError(err)
	}
	span.AddEvent("TransformFromStorage succeeded")

	err = s.decoder.Decode(data, out, getResp.KV.ModRevision)
	if err != nil {
		span.AddEvent("Decode failed", attribute.Int("len", len(data)), attribute.String("err", err.Error()))
		recordDecodeError(s.groupResource, preparedKey)
		return err
	}
	span.AddEvent("Decode succeeded", attribute.Int("len", len(data)))
	return nil
}

// Create 实现 storage.Interface.Create。
//
// 创建用"预期 revision 为 0"的 OptimisticPut 实现：etcd 仅在 key 不存在时成功，
// 因此无需先读取即可保证唯一性。
func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	preparedKey, err := s.prepareKey(key, false)
	if err != nil {
		return err
	}
	ctx, span := tracing.Start(ctx, "Create etcd3",
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)),
		attribute.String("key", key),
		attribute.String("type", getTypeName(obj)),
		attribute.String("group", s.groupResource.Group),
		attribute.String("resource", s.groupResource.Resource),
	)
	defer span.End(500 * time.Millisecond)
	if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
		return storage.ErrResourceVersionSetOnCreate
	}
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}
	span.AddEvent("About to Encode")
	data, err := runtime.Encode(s.codec, obj)
	if err != nil {
		span.AddEvent("Encode failed", attribute.Int("len", len(data)), attribute.String("err", err.Error()))
		return err
	}
	span.AddEvent("Encode succeeded", attribute.Int("len", len(data)))

	// ttl 非零时通过 lease 实现自动过期；lease 复用由 leaseManager 负责，避免大量独立租约。
	var lease clientv3.LeaseID
	if ttl != 0 {
		lease, err = s.leaseManager.GetLease(ctx, int64(ttl))
		if err != nil {
			return err
		}
	}

	newData, err := s.transformer.TransformToStorage(ctx, data, authenticatedDataString(preparedKey))
	if err != nil {
		span.AddEvent("TransformToStorage failed", attribute.String("err", err.Error()))
		return storage.NewInternalError(err)
	}
	span.AddEvent("TransformToStorage succeeded")

	startTime := time.Now()
	txnResp, err := s.client.Kubernetes.OptimisticPut(ctx, preparedKey, newData, 0, kubernetes.PutOptions{LeaseID: lease})
	metrics.RecordEtcdRequest("create", s.groupResource, err, startTime)
	if err != nil {
		span.AddEvent("Txn call failed", attribute.String("err", err.Error()))
		return err
	}
	span.AddEvent("Txn call succeeded")

	if !txnResp.Succeeded {
		// revision=0 的事务失败即 key 已存在，映射为标准 AlreadyExists 错误。
		return storage.NewKeyExistsError(preparedKey, 0)
	}

	if out != nil {
		err = s.decoder.Decode(data, out, txnResp.Revision)
		if err != nil {
			span.AddEvent("Decode failed", attribute.Int("len", len(data)), attribute.String("err", err.Error()))
			recordDecodeError(s.groupResource, preparedKey)
			return err
		}
		span.AddEvent("Decode succeeded", attribute.Int("len", len(data)))
	}
	return nil
}

// Delete 实现 storage.Interface.Delete。
func (s *store) Delete(
	ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions,
	validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	preparedKey, err := s.prepareKey(key, false)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(out)
	if err != nil {
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}

	// 只有特性开启时才尊重"预期对象损坏"标记；关闭时一律按正常流程处理，
	// 防止旧客户端越过损坏检查。
	expectTransformOrDecodeError := false
	if utilfeature.DefaultFeatureGate.Enabled(features.AllowUnsafeMalformedObjectDeletion) {
		expectTransformOrDecodeError = opts.ExpectTransformOrDecodeError
	}
	return s.conditionalDelete(ctx, preparedKey, out, v, preconditions, validateDeletion, cachedExistingObject, expectTransformOrDecodeError)
}

// conditionalDelete 在满足前置条件与删除校验的前提下执行删除，冲突时重试。
//
// cachedExistingObject 来自 watch cache，通常是较新但可能陈旧的数据；
// 若基于它判定失败，会重新读取当前状态再判断，避免因缓存滞后而误拒或误删。
func (s *store) conditionalDelete(
	ctx context.Context, key string, out runtime.Object, v reflect.Value, preconditions *storage.Preconditions,
	validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, expectTransformOrDecodeError bool) error {
	getCurrentState := s.getCurrentState(ctx, key, v, false, expectTransformOrDecodeError)

	var origState *objState
	var err error
	var origStateIsCurrent bool
	if cachedExistingObject != nil && !expectTransformOrDecodeError {
		origState, err = s.getStateFromObject(cachedExistingObject)
	} else {
		origState, err = getCurrentState()
		origStateIsCurrent = true
	}
	if err != nil {
		return err
	}

	for {
		if preconditions != nil {
			if err := preconditions.Check(key, origState.obj); err != nil {
				if origStateIsCurrent {
					return err
				}

				// 我们手中的数据可能已过期。记住这份可能过期数据的 revision
				// 以及由此产生的更新错误
				cachedRev := origState.rev
				cachedUpdateErr := err

				// 真正执行一次读取
				origState, err = getCurrentState()
				if err != nil {
					return err
				}
				origStateIsCurrent = true

				// 结果发现缓存数据并未过期，直接返回该错误
				if cachedRev == origState.rev {
					return cachedUpdateErr
				}

				// 重试
				continue
			}
		}
		if err := validateDeletion(ctx, origState.obj); err != nil {
			if origStateIsCurrent {
				return err
			}

			// 我们手中的数据可能已过期。记住这份可能过期数据的 revision
			// 以及由此产生的更新错误
			cachedRev := origState.rev
			cachedUpdateErr := err

			// 真正执行一次读取
			origState, err = getCurrentState()
			if err != nil {
				return err
			}
			origStateIsCurrent = true

			// 结果发现缓存数据并未过期，直接返回该错误
			if cachedRev == origState.rev {
				return cachedUpdateErr
			}

			// 重试
			continue
		}

		startTime := time.Now()
		txnResp, err := s.client.Kubernetes.OptimisticDelete(ctx, key, origState.rev, kubernetes.DeleteOptions{
			GetOnFailure: true,
		})
		metrics.RecordEtcdRequest("delete", s.groupResource, err, startTime)
		if err != nil {
			return err
		}
		if !txnResp.Succeeded {
			klog.V(4).Infof("deletion of %s failed because of a conflict, going to retry", key)
			origState, err = s.getState(ctx, txnResp.KV, key, v, false, expectTransformOrDecodeError)
			if err != nil {
				return err
			}
			origStateIsCurrent = true
			continue
		}

		if !expectTransformOrDecodeError {
			err = s.decoder.Decode(origState.data, out, txnResp.Revision)
			if err != nil {
				recordDecodeError(s.groupResource, key)
				return err
			}
		}
		return nil
	}
}

// GuaranteedUpdate 实现 storage.Interface.GuaranteedUpdate。
//
// "保证"的含义：tryUpdate 可能被执行多次（冲突重试），因此它必须是幂等的，
// 且只能依据传入的旧对象与元数据计算新值，不能依赖外部可变状态。
func (s *store) GuaranteedUpdate(
	ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool,
	preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	preparedKey, err := s.prepareKey(key, false)
	if err != nil {
		return err
	}
	ctx, span := tracing.Start(ctx, "GuaranteedUpdate etcd3",
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)),
		attribute.String("key", key),
		attribute.String("type", getTypeName(destination)),
		attribute.String("group", s.groupResource.Group),
		attribute.String("resource", s.groupResource.Resource))
	defer span.End(500 * time.Millisecond)

	v, err := conversion.EnforcePtr(destination)
	if err != nil {
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}

	getCurrentState := s.getCurrentState(ctx, preparedKey, v, ignoreNotFound, false)

	var origState *objState
	var origStateIsCurrent bool
	if cachedExistingObject != nil {
		origState, err = s.getStateFromObject(cachedExistingObject)
	} else {
		origState, err = getCurrentState()
		origStateIsCurrent = true
	}
	if err != nil {
		return err
	}
	span.AddEvent("initial value restored")

	transformContext := authenticatedDataString(preparedKey)
	for {
		if err := preconditions.Check(preparedKey, origState.obj); err != nil {
			// 若我们的数据已是最新，直接返回该错误
			if origStateIsCurrent {
				return err
			}

			// 我们此前使用的数据可能已过期
			// 真正执行一次读取
			origState, err = getCurrentState()
			if err != nil {
				return err
			}
			origStateIsCurrent = true
			// 重试
			continue
		}

		ret, ttl, err := s.updateState(origState, tryUpdate)
		if err != nil {
			// 若我们的数据已是最新，直接返回该错误
			if origStateIsCurrent {
				return err
			}

			// 我们此前使用的数据可能已过期
			// 记住这份可能过期数据的 revision 以及由此产生的更新错误
			cachedRev := origState.rev
			cachedUpdateErr := err

			// 真正执行一次读取
			origState, err = getCurrentState()
			if err != nil {
				return err
			}
			origStateIsCurrent = true

			// 结果发现缓存数据并未过期，直接返回该错误
			if cachedRev == origState.rev {
				return cachedUpdateErr
			}

			// 重试
			continue
		}

		span.AddEvent("About to Encode")
		data, err := runtime.Encode(s.codec, ret)
		if err != nil {
			span.AddEvent("Encode failed", attribute.Int("len", len(data)), attribute.String("err", err.Error()))
			return err
		}
		span.AddEvent("Encode succeeded", attribute.Int("len", len(data)))
		if !origState.stale && bytes.Equal(data, origState.data) {
			// 如果本轮循环跳过了最初的 Get，就必须从 etcd 重新刷新一次，
			// 以确保存储中的数据与我们期望的序列化结果一致
			//
			// 短路径：序列化结果未变就跳过写入。但若数据来自外部缓存，
			// 必须先确认 etcd 中确实是同一份内容，否则会漏掉必要写入。
			if !origStateIsCurrent {
				origState, err = getCurrentState()
				if err != nil {
					return err
				}
				origStateIsCurrent = true
				if !bytes.Equal(data, origState.data) {
					// 原始数据已变化，重新开始循环
					continue
				}
			}
			// 在短路跳过写入之前，再次确认来自 etcd 的数据并未过期
			if !origState.stale {
				err = s.decoder.Decode(origState.data, destination, origState.rev)
				if err != nil {
					recordDecodeError(s.groupResource, preparedKey)
					return err
				}
				return nil
			}
		}

		newData, err := s.transformer.TransformToStorage(ctx, data, transformContext)
		if err != nil {
			span.AddEvent("TransformToStorage failed", attribute.String("err", err.Error()))
			return storage.NewInternalError(err)
		}
		span.AddEvent("TransformToStorage succeeded")

		var lease clientv3.LeaseID
		if ttl != 0 {
			lease, err = s.leaseManager.GetLease(ctx, int64(ttl))
			if err != nil {
				return err
			}
		}
		span.AddEvent("Transaction prepared")

		startTime := time.Now()

		txnResp, err := s.client.Kubernetes.OptimisticPut(ctx, preparedKey, newData, origState.rev, kubernetes.PutOptions{
			GetOnFailure: true,
			LeaseID:      lease,
		})
		metrics.RecordEtcdRequest("update", s.groupResource, err, startTime)
		if err != nil {
			span.AddEvent("Txn call failed", attribute.String("err", err.Error()))
			return err
		}
		span.AddEvent("Txn call completed")
		span.AddEvent("Transaction committed")
		if !txnResp.Succeeded {
			// 冲突：用失败响应中附带的最新值重启循环，避免额外一次读。
			klog.V(4).Infof("GuaranteedUpdate of %s failed because of a conflict, going to retry", preparedKey)
			origState, err = s.getState(ctx, txnResp.KV, preparedKey, v, ignoreNotFound, false)
			if err != nil {
				return err
			}
			span.AddEvent("Retry value restored")
			origStateIsCurrent = true
			continue
		}

		err = s.decoder.Decode(data, destination, txnResp.Revision)
		if err != nil {
			span.AddEvent("Decode failed", attribute.Int("len", len(data)), attribute.String("err", err.Error()))
			recordDecodeError(s.groupResource, preparedKey)
			return err
		}
		span.AddEvent("Decode succeeded", attribute.Int("len", len(data)))
		return nil
	}
}

func getNewItemFunc(listObj runtime.Object, v reflect.Value) func() runtime.Object {
	// 对于带有目标 group/version 的 unstructured list，要在实例化的列表项中保留该
	// group/version。必须保留 apiVersion，否则无结构化对象在 list 中会丢失类型信息。
	if unstructuredList, isUnstructured := listObj.(*unstructured.UnstructuredList); isUnstructured {
		if apiVersion := unstructuredList.GetAPIVersion(); len(apiVersion) > 0 {
			return func() runtime.Object {
				return &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": apiVersion}}
			}
		}
	}

	// 否则直接实例化一个空对象
	elem := v.Type().Elem()
	return func() runtime.Object {
		return reflect.New(elem).Interface().(runtime.Object)
	}
}

// Stats 返回对象数量统计；若启用估算器则同时给出大小信息。
func (s *store) Stats(ctx context.Context) (storage.Stats, error) {
	if collector := s.getResourceSizeEstimator(); collector != nil {
		return collector.Stats(ctx)
	}
	// 返回不含资源大小信息的统计值

	startTime := time.Now()
	prefix, err := s.prepareKey(s.resourcePrefix, true)
	if err != nil {
		return storage.Stats{}, err
	}
	count, err := s.client.Kubernetes.Count(ctx, prefix, kubernetes.CountOptions{})
	metrics.RecordEtcdRequest("listWithCount", s.groupResource, err, startTime)
	if err != nil {
		return storage.Stats{}, err
	}
	return storage.Stats{
		ObjectCount: count,
	}, nil
}

// EnableResourceSizeEstimation 启用后台资源大小估算；重复启用视为错误，
// 以免两个估算器同时运行争抢 etcd。
func (s *store) EnableResourceSizeEstimation(getKeys storage.KeysFunc) error {
	if getKeys == nil {
		return errors.New("KeysFunc cannot be nil")
	}
	s.collectorMux.Lock()
	defer s.collectorMux.Unlock()
	if s.resourceSizeEstimator != nil {
		return errors.New("resourceSizeEstimator already enabled")
	}
	s.resourceSizeEstimator = newResourceSizeEstimator(s.pathPrefix, getKeys)
	return nil
}

// TestOnlyResetResourceSizeEstimator 清除资源大小估算器，使后续调用
// EnableResourceSizeEstimation 能够成功。仅供测试使用。
func TestOnlyResetResourceSizeEstimator(s storage.Interface) {
	st, ok := s.(*store)
	if !ok {
		return
	}
	st.collectorMux.Lock()
	defer st.collectorMux.Unlock()
	if st.resourceSizeEstimator != nil {
		st.resourceSizeEstimator.Close()
		st.resourceSizeEstimator = nil
	}
}

// getKeys 仅取 key 不取值（WithKeysOnly），用于按需采样而非全量读取。
func (s *store) getKeys(ctx context.Context) ([]string, error) {
	startTime := time.Now()
	prefix, err := s.prepareKey(s.resourcePrefix, true)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.KV.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	metrics.RecordEtcdRequest("listOnlyKeys", s.groupResource, err, startTime)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
	}
	return keys, nil
}

// ReadinessCheck 实现 storage.Interface。
func (s *store) ReadinessCheck() error {
	return nil
}

// GetCurrentResourceVersion 读取当前 etcd 修订号。
// 用一个前缀上的 Get 探测：即使 key 不存在，响应头也带有当前 revision。
func (s *store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	preparedKey, err := s.prepareKey(s.resourcePrefix, false)
	if err != nil {
		return 0, err
	}

	startTime := time.Now()
	getResp, err := s.client.Kubernetes.Get(ctx, preparedKey, kubernetes.GetOptions{})
	metrics.RecordEtcdRequest("getCurrentResourceVersion", s.groupResource, err, startTime)
	if err != nil {
		return 0, err
	}

	if getResp.Revision == 0 {
		return 0, fmt.Errorf("the current resource version must be greater than 0")
	}
	return uint64(getResp.Revision), nil
}

// shouldStream 决定一次 list 请求是否使用 etcd RangeStream。
// 运行时探测服务端能力：老版本 etcd 不支持流式 Range，需回退到分页。
func shouldStream(opts storage.ListOptions) bool {
	// 只有递归列表才走流式路径，非递归请求只读取单个对象。
	if !opts.Recursive {
		return false
	}
	// etcd 不了解 selector 的匹配情况，带过滤的页无法携带正确的 limit。
	// 因此只对未过滤的分页使用流式读取。
	if opts.Predicate.Limit > 0 && !opts.Predicate.Empty() {
		return false
	}
	return utilfeature.DefaultFeatureGate.Enabled(features.EtcdRangeStream) &&
		etcdfeature.DefaultFeatureSupportChecker.Supports(storage.RangeStream)
}

// GetList 实现 storage.Interface。
//
// 分页与分块两条路径产出同构的 listChunk 序列，外层循环因此无需关心底层是
// Range 分页还是 RangeStream；这种抽象让流式优化可以独立开关与回退。
func (s *store) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	keyPrefix, err := s.prepareKey(key, opts.Recursive)
	if err != nil {
		return err
	}
	ctx, span := tracing.Start(ctx, fmt.Sprintf("List(recursive=%v) etcd3", opts.Recursive),
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)),
		attribute.String("key", key),
		attribute.String("resourceVersion", opts.ResourceVersion),
		attribute.String("resourceVersionMatch", string(opts.ResourceVersionMatch)),
		attribute.Int("limit", int(opts.Predicate.Limit)),
		attribute.String("continue", opts.Predicate.Continue))
	defer span.End(500 * time.Millisecond)
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return fmt.Errorf("need ptr to slice: %v", err)
	}

	// 设置合适的 clientv3 选项以过滤返回的数据集
	limit := opts.Predicate.Limit
	paging := opts.Predicate.Limit > 0
	newItemFunc := getNewItemFunc(listObj, v)

	withRev, continueKey, err := storage.ValidateListOptions(keyPrefix, s.versioner, opts)
	if err != nil {
		return err
	}

	// 循环直到从 etcd 取满请求的 limit，或已无更多结果
	var lastKey []byte
	var hasMore bool
	var count int64
	var numFetched int
	var numEvald int
	var streamed bool
	startTime := time.Now()
	// 这些指标用于理解处理 LIST 请求的开销，因此即使在出错情况下也要记录。
	// 用 defer 记录，保证提前 return 的分支也有指标，否则慢查询/错误会被统计漏掉。
	defer func() {
		numReturn := v.Len()
		metrics.RecordStorageListMetrics(s.groupResource, "", numFetched, numEvald, numReturn)
		metrics.RecordListLatency(s.groupResource, streamed, startTime)
	}()

	aggregator := s.listErrAggrFactory()

	chunks := s.pagedChunks(ctx, keyPrefix, opts, withRev, limit, continueKey)
	if shouldStream(opts) {
		// 探测失败时标记能力不支持，后续请求直接走分页，避免每次都尝试并失败。
		streamChunks, supported := s.streamChunks(ctx, keyPrefix, withRev, limit, continueKey)
		if supported {
			chunks = streamChunks
			streamed = true
		} else {
			etcdfeature.DefaultFeatureSupportChecker.MarkUnsupported(storage.RangeStream)
			klog.V(4).Infof("etcd server does not support RangeStream for %v; falling back to paginated list", s.groupResource)
		}
	}

	for chunk, chunkErr := range chunks {
		if chunkErr != nil {
			return chunkErr
		}
		numFetched += len(chunk.kvs)
		if chunk.revision != nil {
			// 首次拿到 revision 后固定下来，后续所有页必须在同一快照上读取，
			// 否则 list 结果会跨越不同时间点而不一致。
			if withRev == 0 {
				if err = s.validateMinimumResourceVersion(opts.ResourceVersion, uint64(*chunk.revision)); err != nil {
					return err
				}
				withRev = *chunk.revision
			} else if *chunk.revision != withRev {
				return fmt.Errorf("etcd returned revision %d for a list read at revision %d", *chunk.revision, withRev)
			}
		}
		if len(chunk.kvs) == 0 && chunk.hasMore {
			// etcd 声称还有数据却不返回任何 kv，继续循环会死锁/空转，直接报错。
			return fmt.Errorf("no results were found, but etcd indicated there were more values remaining")
		}
		count = chunk.count

		chunkLastKey, chunkEvaluated, limitReached, err := s.appendChunk(ctx, chunk.kvs, opts.Predicate, newItemFunc, aggregator, v, paging)
		if err != nil {
			return err
		}
		numEvald += chunkEvaluated
		if chunkLastKey != nil {
			lastKey = chunkLastKey
		}
		hasMore = chunk.hasMore || limitReached

		// 在块中途达到了 limit，或已无剩余结果
		if limitReached || !chunk.hasMore {
			break
		}
		// 正在分页且结果桶已经装满
		if paging && int64(v.Len()) >= opts.Predicate.Limit {
			break
		}
	}

	continueValue, remainingItemCount, err := storage.PrepareContinueToken(string(lastKey), keyPrefix, withRev, count, hasMore, opts)
	if err != nil {
		return err
	}
	return s.finalizeList(listObj, opts.Predicate, uint64(withRev), continueValue, remainingItemCount, aggregator, v)
}

// listChunk 表示一次列表读取中的一批 kv：分页 Range 中的一页，
// 或 RangeStream 中的一个块。
//
// revision 为 nil 表示该块尚未携带修订信息（流式响应首块可能如此）；
// count 是 etcd 给出的剩余 key 数，用于计算 remainingItemCount。
type listChunk struct {
	kvs      []*mvccpb.KeyValue
	revision *int64
	count    int64 // etcd 统计的该范围内剩余 key 数，含本批次
	hasMore  bool
}

// pagedChunks 以迭代器形式返回分页读取结果，直到取尽或满足 limit。
//
// 当有 selector 过滤时单页可能取不满，继续翻页并逐步翻倍 page size，
// 以减少 etcd 往返；上限 maxLimit 防止一次拉取过多。
func (s *store) pagedChunks(ctx context.Context, keyPrefix string, opts storage.ListOptions, withRev, limit int64, continueKey string) iter.Seq2[listChunk, error] {
	return func(yield func(listChunk, error) bool) {
		for {
			getResp, err := s.getList(ctx, keyPrefix, opts.Recursive, kubernetes.ListOptions{
				Revision: withRev,
				Limit:    limit,
				Continue: continueKey,
			})
			if err != nil {
				yield(listChunk{}, s.listReadError(ctx, err, withRev, len(opts.Predicate.Continue) > 0, continueKey, keyPrefix))
				return
			}
			hasMore := int64(len(getResp.Kvs)) < getResp.Count
			// 后续请求沿用同一个 resource version
			if withRev == 0 {
				withRev = getResp.Revision
			}
			// 在 yield 之前先记录下一个 continue key，因为消费方解码后会逐个把 kv 置为 nil
			if len(getResp.Kvs) > 0 {
				continueKey = string(getResp.Kvs[len(getResp.Kvs)-1].Key) + "\x00"
			}
			revision := withRev
			if !yield(listChunk{kvs: getResp.Kvs, revision: &revision, count: getResp.Count, hasMore: hasMore}, nil) {
				return
			}
			if !hasMore {
				return
			}
			if limit < maxLimit {
				// 结果不满一页是因为 field/label selector 过滤掉了部分对象。
				// 将页大小翻倍，减少访问 etcd 的总次数。
				limit *= 2
				if limit > maxLimit {
					limit = maxLimit
				}
			}
		}
	}
}

// listReadError 把 etcd 的列表读取错误映射为 GetList 返回的错误。
// ErrFutureRev 说明请求的 revision 尚未产生，返回 TooLargeResourceVersion 让客户端重试；
// 此时读取当前 RV 只为在错误信息中给出参考值，失败也不影响主错误。
func (s *store) listReadError(ctx context.Context, err error, withRev int64, paging bool, continueKey, keyPrefix string) error {
	if errors.Is(err, etcdrpc.ErrFutureRev) {
		currentRV, getRVErr := s.GetCurrentResourceVersion(ctx)
		if getRVErr != nil {
			// 若无法获取当前 RV，则回退使用 0。
			currentRV = 0
		}
		return storage.NewTooLargeResourceVersionError(uint64(withRev), currentRV, 0)
	}
	return interpretListError(err, paging, continueKey, keyPrefix)
}

// streamChunks 把整个列表作为单个 etcd RangeStream 读取：withRev 非零时
// 固定在该修订号上，最多读取 limit 个 key，若设置了 continueKey 则从其处续读。
// 当 etcd 服务端未实现 RangeStream 时，supported 为 false。
//
// 通过首个响应判断 Unimplemented：即使是支持流式的客户端库，连到旧服务端也会失败，
// 因此能力判定必须在拿到第一个块之后才能确定。
func (s *store) streamChunks(ctx context.Context, keyPrefix string, withRev, limit int64, continueKey string) (chunks iter.Seq2[listChunk, error], supported bool) {
	startTime := time.Now()
	paging := continueKey != ""
	startKey := keyPrefix
	if paging {
		startKey = continueKey
	}
	streamOpts := []clientv3.OpOption{clientv3.WithRange(clientv3.GetPrefixRangeEnd(keyPrefix))}
	if withRev > 0 {
		streamOpts = append(streamOpts, clientv3.WithRev(withRev))
	}
	if limit > 0 {
		streamOpts = append(streamOpts, clientv3.WithLimit(limit))
	}
	stream, streamErr := s.client.KV.GetStream(ctx, startKey, streamOpts...)
	var first clientv3.RangeStreamResponse
	var firstOk bool
	if streamErr == nil {
		first, firstOk = <-stream
		if firstOk && grpcstatus.Code(first.Err()) == grpccodes.Unimplemented {
			return nil, false
		}
	} else if grpcstatus.Code(streamErr) == grpccodes.Unimplemented {
		return nil, false
	}
	return func(yield func(listChunk, error) bool) {
		var err error
		defer func() {
			metrics.RecordEtcdRequest("listStream", s.groupResource, err, startTime)
		}()
		if err = streamErr; err != nil {
			yield(listChunk{}, s.listReadError(ctx, err, withRev, paging, continueKey, keyPrefix))
			return
		}
		estimator := s.getResourceSizeEstimator()
		var revision *int64
		if withRev > 0 {
			revision = &withRev
		}
		resp, ok := first, firstOk
		for ok {
			if err = resp.Err(); err != nil {
				yield(listChunk{}, s.listReadError(ctx, err, withRev, paging, continueKey, keyPrefix))
				return
			}
			rangeResp := resp.RangeResponse
			if estimator != nil && len(rangeResp.Kvs) > 0 {
				estimator.Update(rangeResp.Kvs)
			}
			// 固定 revision（pinned）的流，其最后一个 header 携带的是存储最新 revision，
			// 而不是 withRev。固定了 revision 时沿用请求值，避免误用服务端最新修订号破坏一致性假设。
			if revision == nil && rangeResp.Header != nil {
				revision = &rangeResp.Header.Revision
			}
			next, nextOk := <-stream
			// 在最后一个块上，More 为真表示范围被 limit 截断，因此即使流已结束
			// 仍存在下一页。
			if !yield(listChunk{kvs: rangeResp.Kvs, revision: revision, count: rangeResp.Count, hasMore: nextOk || rangeResp.More}, nil) {
				return
			}
			resp, ok = next, nextOk
		}
		if revision == nil {
			err = fmt.Errorf("rangeStream for %q completed without a revision", keyPrefix)
			yield(listChunk{}, err)
		}
	}, true
}

func (s *store) finalizeList(listObj runtime.Object, pred storage.SelectionPredicate, rev uint64, continueValue string, remainingItemCount *int64, aggregator listItemErrors, v reflect.Value) error {
	// 先检查聚合错误：即使部分元素失败，也必须在返回结果前暴露。
	if err := aggregator.Aggregate(); err != nil {
		return err
	}
	if v.IsNil() {
		// 为保证一致性，结果中绝不返回 nil 的 Items 指针。
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	}
	if err := s.versioner.UpdateList(listObj, rev, continueValue, remainingItemCount); err != nil {
		return err
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) {
		pred.SetShardInfoOnList(listObj)
	}
	return nil
}

func (s *store) processListItem(ctx context.Context, kv *mvccpb.KeyValue, pred storage.SelectionPredicate, newItemFunc func() runtime.Object, aggregator listItemErrors, v reflect.Value) (bool, error) {
	data, _, err := s.transformer.TransformFromStorage(ctx, kv.Value, authenticatedDataString(kv.Key))
	if err != nil {
		if done := aggregator.Append(string(kv.Key), storage.NewInternalError(fmt.Errorf("unable to transform key %q: %w", kv.Key, err))); done {
			return false, aggregator.Aggregate()
		}
		return false, nil
	}

	// 在解码对象之前先检查请求是否已经超时
	select {
	case <-ctx.Done():
		// 父上下文已被取消或超时，继续下去没有意义
		return false, storage.NewTimeoutError(string(kv.Key), "request did not complete within requested timeout")
	default:
	}

	obj, err := s.decoder.DecodeListItem(ctx, data, uint64(kv.ModRevision), newItemFunc)
	if err != nil {
		recordDecodeError(s.groupResource, string(kv.Key))
		if done := aggregator.Append(string(kv.Key), err); done {
			return false, aggregator.Aggregate()
		}
		return false, nil
	}

	// 无法设置版本并不妨碍该对象被提取出来
	if matched, err := pred.Matches(obj); err == nil && matched {
		v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
	}

	return true, nil
}

// appendChunk 把所有匹配 pred 的 kv 追加到 v 中。
//
// lastKey 是已处理到的最后一个 key，用于生成 continue token；
// 超过 limit 时提前返回 limitReached，让分页在此处截断。
func (s *store) appendChunk(ctx context.Context, kvs []*mvccpb.KeyValue, pred storage.SelectionPredicate, newItemFunc func() runtime.Object, aggregator listItemErrors, v reflect.Value, paging bool) (lastKey []byte, evaluated int, limitReached bool, err error) {
	// 避免为结果切片做小容量分配：此函数会在很多不同场景下被调用，
	// 而我们并不知道结果会被过滤掉多少
	if pred.Empty() {
		growSlice(v, len(kvs))
	} else {
		growSlice(v, 2048, len(kvs))
	}
	for i, kv := range kvs {
		if paging && int64(v.Len()) >= pred.Limit {
			return lastKey, evaluated, true, nil
		}
		lastKey = kv.Key
		ok, err := s.processListItem(ctx, kv, pred, newItemFunc, aggregator, v)
		if err != nil {
			return lastKey, evaluated, false, err
		}
		if ok {
			evaluated++
		}
		// 尽早释放 kv。长列表的解码可能耗时数秒。
		kvs[i] = nil
	}
	return lastKey, evaluated, false, nil
}

func (s *store) getList(ctx context.Context, keyPrefix string, recursive bool, options kubernetes.ListOptions) (resp kubernetes.ListResponse, err error) {
	startTime := time.Now()
	// 非递归 list 等价于单 key Get；统一包装成列表响应，使上层代码无需分支处理。
	if recursive {
		resp, err = s.client.Kubernetes.List(ctx, keyPrefix, options)
		metrics.RecordEtcdRequest("list", s.groupResource, err, startTime)
	} else {
		var getResp kubernetes.GetResponse
		getResp, err = s.client.Kubernetes.Get(ctx, keyPrefix, kubernetes.GetOptions{
			Revision: options.Revision,
		})
		metrics.RecordEtcdRequest("get", s.groupResource, err, startTime)
		if getResp.KV != nil {
			resp.Kvs = []*mvccpb.KeyValue{getResp.KV}
			resp.Count = 1
			resp.Revision = getResp.Revision
		} else {
			resp.Kvs = []*mvccpb.KeyValue{}
			resp.Count = 0
			resp.Revision = getResp.Revision
		}
	}

	stats := s.getResourceSizeEstimator()
	if len(resp.Kvs) > 0 && stats != nil {
		stats.Update(resp.Kvs)
	}
	return resp, err
}

// growSlice 接收一个切片值，把它的容量增长到"传入各尺寸中的最大值"
// 与 maxCapacity 二者中较小的那个。超过 maxCapacity 之后，追加时的分配
// 决策交给 Go 运行时。这让调用方可以对整个列表的潜在大小做出合理预估，
// 同时避免初始分配过于激进。若 sizes 为空，则直接增长到 maxCapacity。
func growSlice(v reflect.Value, maxCapacity int, sizes ...int) {
	cap := v.Cap()
	max := cap
	for _, size := range sizes {
		if size > max {
			max = size
		}
	}
	if len(sizes) == 0 || max > maxCapacity {
		max = maxCapacity
	}
	if max <= cap {
		return
	}
	if v.Len() > 0 {
		extra := reflect.MakeSlice(v.Type(), v.Len(), max)
		reflect.Copy(extra, v)
		v.Set(extra)
	} else {
		extra := reflect.MakeSlice(v.Type(), 0, max)
		v.Set(extra)
	}
}

// Watch 实现 storage.Interface.Watch。
func (s *store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	preparedKey, err := s.prepareKey(key, opts.Recursive)
	if err != nil {
		return nil, err
	}
	rev, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	return s.watcher.Watch(s.watchContext(ctx), preparedKey, int64(rev), opts)
}

// watchContext 要求 etcd 在失去 leader 后主动终止 watch 流。
// 否则流会在没有 leader 的情况下静默挂住，watch cache 无法感知数据停滞。
func (s *store) watchContext(ctx context.Context) context.Context {
	// etcd 服务端会在连续 3 个选举超时找不到 leader 后才取消现有流。
	// 3 目前是硬编码常量，选举超时默认为 1000ms。若集群健康，停止 leader
	// 时应当发生平滑的领导者移交（leader 停止前会主动移交领导权）。若
	// leader 被强制杀死，其他服务器需要一个选举超时的时间才能发现 leader
	// 丢失并发起新一轮选举（campaign）。
	return clientv3.WithRequireLeader(ctx)
}

// getCurrentState 返回一个惰性读取器：只在真正需要时才访问 etcd，
// 使缓存命中或前置条件失败时省去一次往返。
func (s *store) getCurrentState(ctx context.Context, key string, v reflect.Value, ignoreNotFound bool, expectTransformOrDecodeError bool) func() (*objState, error) {
	return func() (*objState, error) {
		startTime := time.Now()
		getResp, err := s.client.Kubernetes.Get(ctx, key, kubernetes.GetOptions{})
		metrics.RecordEtcdRequest("get", s.groupResource, err, startTime)
		if err != nil {
			return nil, err
		}
		return s.getState(ctx, getResp.KV, key, v, ignoreNotFound, expectTransformOrDecodeError)
	}
}

// getState 根据存储返回的响应构造一个新的 objState。若
// expectTransformOrDecodeError 为 true 而转换与解码都未失败，则返回
// InvalidObj 错误；若二者之一失败，返回的错误与 objState 的 'obj' 字段
// 都将为 nil。
//
// 后一种语义是"不安全删除"专用的：只有确认对象确实无法解密/解码时，才允许绕过
// 正常校验删除它，否则必须走常规路径。
func (s *store) getState(ctx context.Context, kv *mvccpb.KeyValue, key string, v reflect.Value, ignoreNotFound bool, expectTransformOrDecodeError bool) (*objState, error) {
	state := &objState{
		meta: &storage.ResponseMeta{},
	}

	if u, ok := v.Addr().Interface().(runtime.Unstructured); ok {
		state.obj = u.NewEmptyInstance()
	} else {
		state.obj = reflect.New(v.Type()).Interface().(runtime.Object)
	}

	if kv == nil {
		if !ignoreNotFound {
			return nil, storage.NewKeyNotFoundError(key, 0)
		}
		if err := runtime.SetZeroValue(state.obj); err != nil {
			return nil, err
		}
		return state, nil
	}

	state.rev = kv.ModRevision
	state.meta.ResourceVersion = uint64(state.rev)

	// 认证数据必须是同一个 key，否则把密文在不同对象间搬移会被拒绝。
	data, stale, err := s.transformer.TransformFromStorage(ctx, kv.Value, authenticatedDataString(key))
	if err != nil {
		if !expectTransformOrDecodeError {
			return nil, storage.NewInternalError(err)
		}

		// 明确表示我们没有拿到该对象
		state.obj = nil
		state.stale = true // this seems a more sane value here
		return state, nil
	}

	state.data = data
	state.stale = stale

	if err := s.decoder.Decode(state.data, state.obj, state.rev); err != nil {
		if !expectTransformOrDecodeError {
			recordDecodeError(s.groupResource, key)
			return nil, err
		}

		// 明确表示我们没有拿到该对象
		state.obj = nil
		return state, nil
	}

	if expectTransformOrDecodeError {
		return nil, storage.NewInvalidObjError(key, "unsafe deletion is not allowed because the object is decodable from storage")
	}

	return state, nil
}

// getStateFromObject 用调用方提供的（可能来自缓存的）对象构造状态，省去一次 etcd 读取。
// 序列化前必须先清空 resourceVersion：该字段不落盘，保留会导致字节比较失去意义。
func (s *store) getStateFromObject(obj runtime.Object) (*objState, error) {
	state := &objState{
		obj:  obj,
		meta: &storage.ResponseMeta{},
	}

	rv, err := s.versioner.ObjectResourceVersion(obj)
	if err != nil {
		return nil, fmt.Errorf("couldn't get resource version: %v", err)
	}
	state.rev = int64(rv)
	state.meta.ResourceVersion = uint64(state.rev)

	// 计算序列化形式——为此需要暂时清空对象的 resourceVersion 字段
	// （该字段不会存入 etcd）。
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return nil, fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}
	state.data, err = runtime.Encode(s.codec, obj)
	if err != nil {
		return nil, err
	}
	if err := s.versioner.UpdateObject(state.obj, uint64(rv)); err != nil {
		klog.Errorf("failed to update object version: %v", err)
	}
	return state, nil
}

// updateState 调用用户提供的更新函数并规范化结果，供写入 etcd 使用。
// ttl 通过指针返回，nil 表示不设置过期。
func (s *store) updateState(st *objState, userUpdate storage.UpdateFunc) (runtime.Object, uint64, error) {
	ret, ttlPtr, err := userUpdate(st.obj, *st.meta)
	if err != nil {
		return nil, 0, err
	}

	if err := s.versioner.PrepareObjectForStorage(ret); err != nil {
		return nil, 0, fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}
	var ttl uint64
	if ttlPtr != nil {
		ttl = *ttlPtr
	}
	return ret, ttl, nil
}

// validateMinimumResourceVersion 当给定的 minimumResourceVersion 大于存储可
// 提供的最新 actualRevision 时，返回"too large resource version"错误。
func (s *store) validateMinimumResourceVersion(minimumResourceVersion string, actualRevision uint64) error {
	if minimumResourceVersion == "" {
		return nil
	}
	minimumRV, err := s.versioner.ParseResourceVersion(minimumResourceVersion)
	if err != nil {
		return apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
	}
	// 强制满足 storage.Interface 的保证：返回数据的 resource version
	// "至少为 resourceVersion"。
	if minimumRV > actualRevision {
		return storage.NewTooLargeResourceVersionError(minimumRV, actualRevision, 0)
	}
	return nil
}

func (s *store) prepareKey(key string, recursive bool) (string, error) {
	key, err := storage.PrepareKey(s.resourcePrefix, key, recursive)
	if err != nil {
		return "", err
	}
	// 构造阶段已确保 pathPrefix 以 '/' 结尾，所以现在跳过 key 的前导 '/'。
	startIndex := 0
	if key[0] == '/' {
		startIndex = 1
	}
	return s.pathPrefix + key[startIndex:], nil
}

// recordDecodeError 按对象类型分类记录解码错误。
func recordDecodeError(groupResource schema.GroupResource, key string) {
	metrics.RecordDecodeError(groupResource)
	klog.V(4).Infof("Decoding %s \"%s\" failed", groupResource, key)
}

// getTypeName 返回对象的类型名称，用于上报信息。
func getTypeName(obj interface{}) string {
	return reflect.TypeOf(obj).String()
}
