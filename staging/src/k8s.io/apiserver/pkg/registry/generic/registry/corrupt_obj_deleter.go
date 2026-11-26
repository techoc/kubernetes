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

package registry

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	storeerr "k8s.io/apiserver/pkg/storage/errors"
	"k8s.io/apiserver/pkg/util/dryrun"

	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// 损坏对象删除器与 rest.GracefulDeleter 接口相同
var _ rest.GracefulDeleter = &corruptObjectDeleter{}

// NewCorruptObjectDeleter 返回的删除器可以不安全地删除损坏对象。
// 若对象实际可解码（并非损坏），删除会被拒绝。
//
// 注意：它跳过前置条件检查、finalizer 约束以及 registry 中
// AfterDelete 定义的所有删除后钩子。
//
// 警告：若被删除资源有依赖项，此操作可能破坏集群。
//
// 这是“逃生舱”：仅在对象已损坏到无法读取时，允许绕过常规删除流程直接清除，
// 因此刻意跳过准入、finalizer 与删除后钩子。
func NewCorruptObjectDeleter(store *Store) rest.GracefulDeleter {
	return &corruptObjectDeleter{store: store}
}

// corruptObjectDeleter 实现不安全的对象删除流程
type corruptObjectDeleter struct {
	store *Store
}

// errUnsafeDeleteDryRun 是删除校验器在 dry-run 请求中返回的哨兵错误。
// 删除事务前调用校验函数是全局不变量：Kubernetes 中所有删除都靠它执行删除准入，
// 任何存储实现都不能跳过。
//
// getState(key, expectTransformOrDecodeError=true)
// 键不存在          -> KeyNotFoundError，校验器不执行
// 可正常解码        -> InvalidObjError，校验器不执行
// 转换/解码失败     -> validateDeletion，然后 OptimisticDelete
//
// 拿到此错误说明对象在最新版本已损坏，删除本应继续。
//
// 借用删除校验函数作为探针：它被调用即说明对象确实无法解码，dry run 应当“成功”。
var errUnsafeDeleteDryRun = errors.New("aborting unsafe delete, dry run requested")

// errUnsafeDeleteDryRunBypassed 表示存储层在 dry-run 请求中未调用删除校验器就删除了对象。
// 任何 storage.Interface 实现都绝不应如此。该校验器与常规删除时承载删除准入的参数是同一个
// （见 Store.Delete）；触发此错误的后端存在漏洞，会在不执行删除准入的情况下删除对象。
// 一旦出现，说明存储实现存在安全缺陷（绕过删除准入），必须按内部错误上报。
var errUnsafeDeleteDryRunBypassed = errors.New("storage deleted the object despite the dry run request")

// Delete 对给定资源执行不安全删除。
//
// 注意：此函数绝不应用于任何正常删除流程，仅当用户设置
// IgnoreStoreReadErrorWithClusterBreakingPotential 删除选项时才使用。
//
// 返回的 runtime.Object 恒为 nil：损坏对象无法解码，调用方不应依赖其内容。
func (d *corruptObjectDeleter) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, opts *metav1.DeleteOptions) (runtime.Object, bool, error) {
	if opts == nil || !ptr.Deref[bool](opts.IgnoreStoreReadErrorWithClusterBreakingPotential, false) {
		// 这是开发错误，不应走到这里：不安全删除器只在启用选项时
		// 才会接入 rest 层。
		//
		// 双重保险：若装配逻辑被改坏，宁可报 500 也不要在无授权的情况下删除对象。
		return nil, false, apierrors.NewInternalError(errors.New("initialization error, expected normal deletion flow to be used"))
	}

	key, err := d.store.KeyFunc(ctx, name)
	if err != nil {
		return nil, false, err
	}
	qualifiedResource := d.store.qualifiedResourceFromContext(ctx)
	// 绕过 DryRunnableStorage 层，它从未为处理损坏对象而设计
	// （其 dry-run 模拟是 Get + decode）。
	// 下面的调用必须落在 etcd3 store，参见 Delete 中的说明。
	// https://github.com/kubernetes/kubernetes/blob/6a5cc912a3c2a2c241fe05abea563079bf574abd/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L336-L354
	//
	// 直接取内层存储，避免 DryRunnableStorage 的 Get+decode 模拟在损坏对象上失败。
	storageBackend := d.store.Storage.Storage
	klog.FromContext(ctx).V(1).Info("Going to perform unsafe object deletion", "object", klog.KRef(genericapirequest.NamespaceValue(ctx), name))
	out := d.store.NewFunc()
	// ExpectTransformOrDecodeError 强制从 etcd 实时读取，并且仅当对象
	// 损坏时才允许删除：找不到或可解码的对象都会报错。
	//
	// 该选项是安全边界：它保证只有真正损坏的对象才会被这条路径删除。
	storageOpts := storage.DeleteOptions{ExpectTransformOrDecodeError: true}
	// 缓存中没有旧对象，也无法从存储读取并解码成功，
	// 因此执行以下操作：
	//
	// a) 跳过前置条件检查
	// b) 跳过准入校验，rest.ValidateAllObjectFunc 会放行一切
	var nilPreconditions *storage.Preconditions = nil
	var nilCachedExistingObject runtime.Object = nil
	if dryrun.IsDryRun(opts.DryRun) {
		// dry run 时把校验函数当作“对象损坏确认器”：它被执行即代表删除本会成功。
		err := storageBackend.Delete(
			ctx, key, out, nilPreconditions,
			func(context.Context, runtime.Object) error { return errUnsafeDeleteDryRun },
			nilCachedExistingObject, storageOpts,
		)
		switch {
		case errors.Is(err, errUnsafeDeleteDryRun):
			// 对象在最新版本已损坏，删除本应继续。
			return nil, true, nil
		case err == nil:
			// 不应发生，说明 storage.Interface 实现有严重 bug，
			// 见 errUnsafeDeleteDryRunBypassed。
			utilruntime.HandleErrorWithContext(ctx, errUnsafeDeleteDryRunBypassed, "The storage bypassed delete admission on a dry run request", "object", klog.KRef(genericapirequest.NamespaceValue(ctx), name))
			return nil, false, apierrors.NewInternalError(errUnsafeDeleteDryRunBypassed)
		default:
			return nil, false, storeerr.InterpretDeleteError(err, qualifiedResource, name)
		}
	}
	// 真实删除：ValidateAllObjectFunc 无条件放行，因为无法解码对象、也就无法做有意义的校验。
	if err := storageBackend.Delete(ctx, key, out, nilPreconditions, rest.ValidateAllObjectFunc, nilCachedExistingObject, storageOpts); err != nil {
		return nil, false, storeerr.InterpretDeleteError(err, qualifiedResource, name)
	}
	// DELETE 已成功，但对象无法从存储读取，因此
	// 返回 nil 对象。
	return nil, true, nil
}
