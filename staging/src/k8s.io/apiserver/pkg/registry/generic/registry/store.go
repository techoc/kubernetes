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

package registry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/api/validation"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/sharding"
	"k8s.io/apiserver/pkg/storage"
	storeerr "k8s.io/apiserver/pkg/storage/errors"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/util/dryrun"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"
)

// FinishFunc 是 Begin 钩子返回的、用于完成某个操作的函数。参数 success 表示操作
// 最终是否提交；实现方应据此提交或回滚副作用。
type FinishFunc func(ctx context.Context, success bool)

// AfterDeleteFunc 是 Store.AfterDelete 钩子使用的类型。
type AfterDeleteFunc func(obj runtime.Object, options *metav1.DeleteOptions)

// BeginCreateFunc 是 Store.BeginCreate 钩子使用的类型。
type BeginCreateFunc func(ctx context.Context, obj runtime.Object, options *metav1.CreateOptions) (FinishFunc, error)

// AfterCreateFunc 是 Store.AfterCreate 钩子使用的类型。
type AfterCreateFunc func(obj runtime.Object, options *metav1.CreateOptions)

// BeginUpdateFunc 是 Store.BeginUpdate 钩子使用的类型。
type BeginUpdateFunc func(ctx context.Context, obj, old runtime.Object, options *metav1.UpdateOptions) (FinishFunc, error)

// AfterUpdateFunc 是 Store.AfterUpdate 钩子使用的类型。
type AfterUpdateFunc func(obj runtime.Object, options *metav1.UpdateOptions)

// GenericStore 接口可用于类型断言，以便访问底层的各策略（strategy）。
type GenericStore interface {
	GetCreateStrategy() rest.RESTCreateStrategy
	GetUpdateStrategy() rest.RESTUpdateStrategy
	GetDeleteStrategy() rest.RESTDeleteStrategy
}

// Store 实现了 k8s.io/apiserver/pkg/registry/rest.StandardStorage。它的设计
// 允许可被嵌入，使使用方可以实现所需的非通用函数。该对象可以被复制，
// 从而以不同方式使用却共享同一套底层行为。
//
// 除非另有说明，所有字段都是必填的。
//
// 该类型的预期用法是嵌入到某个与具体 Kind 对应的 RESTStorage 实现中。
// 该类型针对类 Kubernetes 资源提供 CRUD 语义，处理诸如 ResourceVersion
// 冲突检测等细节。RESTCreateStrategy、RESTUpdateStrategy 和
// RESTDeleteStrategy 对所有后端都是通用的，并封装了 API 特有的逻辑。
//
// TODO: 让默认暴露的方法与通用 RESTStorage 完全匹配
//
// 设计要点：Store 自身无状态（除可选的只读/同步字段外），所有并发安全
// 依赖底层 storage.Interface；它把 API 语义（校验、默认值、finalizer、
// resourceVersion 冲突检测）与后端存储解耦。
type Store struct {
	// NewFunc 返回该注册表在 GET 单个对象时返回的该类型的新实例，例如：
	//
	// curl GET /apis/group/version/namespaces/my-ns/myresource/name-of-object
	//
	// 每次调用必须返回新对象，避免调用方共享同一实例。
	NewFunc func() runtime.Object

	// NewListFunc 返回该类型的一个新列表；它是列举（list）该资源时返回的
	// 类型，例如：
	//
	// curl GET /apis/group/version/namespaces/my-ns/myresource
	NewListFunc func() runtime.Object

	// DefaultQualifiedResource 是资源的复数名。
	// 当上下文中不存在请求信息（request info）时使用该字段。
	// 详见 qualifiedResourceFromContext。
	DefaultQualifiedResource schema.GroupResource

	// SingularQualifiedResource 是资源的单数名。
	SingularQualifiedResource schema.GroupResource

	// KeyRootFunc 返回该资源的 etcd 根键；不应包含结尾的 "/"。
	// 它用于作用于整个集合的操作（列举和 watch）。
	//
	// KeyRootFunc 和 KeyFunc 必须一起提供，或者都不提供。
	KeyRootFunc func(ctx context.Context) string

	// KeyFunc 返回集合中某个特定对象的键。Create/Update/Get/Delete 会调用
	// KeyFunc。注意 'namespace' 可以从 ctx 获取。
	//
	// KeyFunc 和 KeyRootFunc 必须一起提供，或者都不提供。
	KeyFunc func(ctx context.Context, name string) (string, error)

	// ObjectNameFunc 返回对象的名称，或返回错误。
	ObjectNameFunc func(obj runtime.Object) (string, error)

	// TTLFunc 返回对象持久化时应设置的 TTL（存活时间）。existing 参数是当前
	// TTL 或本次操作的默认值。update 参数指示本次操作是否针对已存在的对象。
	//
	// 以 TTL 持久化的对象会在 TTL 到期后被驱逐。
	TTLFunc func(obj runtime.Object, existing uint64, update bool) (uint64, error)

	// PredicateFunc 返回与给定标签和字段对应的匹配器。当对象匹配给定的
	// 字段选择器和标签选择器时，返回的 SelectionPredicate 应当返回 true。
	PredicateFunc func(label labels.Selector, field fields.Selector) storage.SelectionPredicate

	// EnableGarbageCollection 影响 Update 和 Delete 请求的处理。启用垃圾回收后，
	// finalizer 才能在 store 删除对象之前完成该对象的收尾工作。
	//
	// 如果任何 store 启用了垃圾回收，就必须同时在 kube-controller-manager
	// 中启用它。
	EnableGarbageCollection bool

	// DeleteCollectionWorkers 是单次 DeleteCollection 调用中的最大工作协程数。
	// 集合中各条目的删除请求会并行下发。
	DeleteCollectionWorkers int

	// Decorator 是一个可选的出口钩子，作用于底层存储返回的对象。返回的对象
	// 可能是单个对象（如 Pod）或列表类型（如 PodList）。Decorator 是为位于
	// 存储之上的集成场景设计的，只应在不适合持久化该值的特定情况下使用，
	// 因为这类值无法被 watch。
	Decorator func(runtime.Object)

	// CreateStrategy 实现创建过程中的资源特定行为。
	CreateStrategy rest.RESTCreateStrategy
	// BeginCreate 是一个可选钩子，返回一个"类事务"的提交/回滚函数。该函数
	// 会在操作结束时、AfterCreate 和 Decorator 之前被调用，并通过参数指示
	// 操作是否成功。如果 BeginCreate 本身返回错误，则该函数不会被调用。
	// 几乎不应该有使用者需要这个钩子。
	BeginCreate BeginCreateFunc
	// AfterCreate 在资源创建之后、装饰之前执行后续操作，可选。
	AfterCreate AfterCreateFunc

	// UpdateStrategy 实现更新过程中的资源特定行为。
	UpdateStrategy rest.RESTUpdateStrategy
	// BeginUpdate 是一个可选钩子，返回一个"类事务"的提交/回滚函数。该函数
	// 会在操作结束时、AfterUpdate 和 Decorator 之前被调用，并通过参数指示
	// 操作是否成功。如果 BeginUpdate 本身返回错误，则该函数不会被调用。
	// 几乎不应该有使用者需要这个钩子。
	BeginUpdate BeginUpdateFunc
	// AfterUpdate 在资源更新之后、装饰之前执行后续操作，可选。
	AfterUpdate AfterUpdateFunc

	// DeleteStrategy 实现删除过程中的资源特定行为。
	DeleteStrategy rest.RESTDeleteStrategy
	// AfterDelete 在资源删除之后、装饰之前执行后续操作，可选。
	AfterDelete AfterDeleteFunc
	// ReturnDeletedObject 决定 Store 是否返回被删除的对象。否则返回通用的
	// 成功状态响应。
	ReturnDeletedObject bool
	// ShouldDeleteDuringUpdate 是一个可选函数，用于判断从 existing 到 obj
	// 的更新是否应当触发删除。
	// 若提供，除了标准的 finalizer、deletionTimestamp 和
	// deletionGracePeriodSeconds 检查外，还会检查该函数。
	ShouldDeleteDuringUpdate func(ctx context.Context, key string, obj, existing runtime.Object) bool

	// TableConvertor 是可选接口，用于把条目或条目列表转换为表格输出。
	// 未设置时使用默认实现。
	TableConvertor rest.TableConvertor

	// ResetFieldsStrategy 提供由策略重置、用户不应修改的字段。
	ResetFieldsStrategy rest.ResetFieldsStrategy

	// Storage 是该资源底层存储的接口。它被包装为 "DryRunnableStorage"，
	// 要么直接透传，要么仅执行 dry-run。
	//
	// 通过 DryRunnableStorage 包装，dry-run 请求可以走完全相同的代码路径，
	// 只是不落盘，从而保证 dry-run 与真实执行的校验行为一致。
	Storage DryRunnableStorage
	// StorageVersioner 在给定对象可能的 Kind 列表的情况下，输出对象在持久化
	// 到 etcd 之前会被转换成的 <group/version/kind>。
	// 如果 StorageVersioner 为 nil，apiserver 会在 discovery 文档中把
	// storageVersionHash 留空。
	StorageVersioner runtime.GroupVersioner

	// ReadinessCheckFunc 检查存储是否已就绪、可以接受请求。
	// 该字段可选，若设置则必须是线程安全的。
	ReadinessCheckFunc func() error

	// DestroyFunc 清理底层 Storage 使用的客户端；可选。
	// 若设置，DestroyFunc 必须以线程安全的方式实现，并要能应对被多次调用。
	DestroyFunc func()

	// corruptObjDeleter 实现不安全删除流程，用于支持删除损坏的对象。它会先
	// 尝试执行正常删除流程，如果正常删除流程因"对象损坏"错误而失败，
	// 则从存储中对该对象执行不安全删除。
	// NOTE: 它会跳过前置条件检查、finalizer 约束，以及注册表中 'AfterDelete'
	// 定义的任何删除后钩子。
	// WARNING: 如果该资源存在依赖，这可能破坏集群。仅在集群已损坏、且没有
	// 其他可行的修复手段时使用。
	corruptObjDeleter rest.GracefulDeleter
}

// Note: rest.StandardStorage 接口聚合了常见的 REST 动词。
var _ rest.StandardStorage = &Store{}
var _ rest.StorageWithReadiness = &Store{}
var _ rest.TableConvertor = &Store{}
var _ GenericStore = &Store{}

var _ rest.SingularNameProvider = &Store{}

var _ rest.CorruptObjectDeleterProvider = &Store{}

const (
	// OptimisticLockErrorMsg 是 resourceVersion 冲突时返回给客户端的固定提示，
	// 客户端工具会依赖这段文案提示用户重新读取。
	OptimisticLockErrorMsg        = "the object has been modified; please apply your changes to the latest version and try again"
	resourceCountPollPeriodJitter = 1.2
)

// NamespaceKeyRootFunc 是构造资源目录存储路径的默认函数，会强制遵循命名空间规则。
// 命名空间来自请求上下文，而不是对象本身，确保存储路径与 URL 一致。
func NamespaceKeyRootFunc(ctx context.Context, prefix string) string {
	key := prefix
	ns, ok := genericapirequest.NamespaceFrom(ctx)
	if ok && len(ns) > 0 {
		key = key + "/" + ns
	}
	return key
}

// NamespaceKeyFunc 是构造资源存储路径的默认函数：路径相对于给定前缀，并强制
// 遵循命名空间规则。如果上下文中不包含命名空间，则报错。
func NamespaceKeyFunc(ctx context.Context, prefix string, name string) (string, error) {
	key := NamespaceKeyRootFunc(ctx, prefix)
	ns, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok || len(ns) == 0 {
		return "", apierrors.NewBadRequest("Namespace parameter required.")
	}
	if len(name) == 0 {
		return "", apierrors.NewBadRequest("Name parameter required.")
	}
	// 拒绝含路径分隔符等非法片段的名称，防止键空间逃逸（例如 "../"）。
	if msgs := content.IsPathSegmentName(name); len(msgs) != 0 {
		return "", apierrors.NewBadRequest(fmt.Sprintf("Name parameter invalid: %q: %s", name, strings.Join(msgs, ";")))
	}
	key = key + "/" + name
	return key, nil
}

// NoNamespaceKeyFunc 是构造资源存储路径的默认函数：路径相对于给定前缀，
// 且不含命名空间。
func NoNamespaceKeyFunc(ctx context.Context, prefix string, name string) (string, error) {
	if len(name) == 0 {
		return "", apierrors.NewBadRequest("Name parameter required.")
	}
	if msgs := content.IsPathSegmentName(name); len(msgs) != 0 {
		return "", apierrors.NewBadRequest(fmt.Sprintf("Name parameter invalid: %q: %s", name, strings.Join(msgs, ";")))
	}
	key := prefix + "/" + name
	return key, nil
}

// storeKeyFuncs 把“由上下文推导键”和“由对象推导缓存键”两类需求分开：
// 前者用于请求路径，后者用于缓存层（缓存层拿不到请求上下文）。
type storeKeyFuncs struct {
	// storageRootKeyFunc 返回 list 和 watch 请求的资源相对存储路径前缀，
	// 例如 "/pods" 或 "/pods/<namespace>"。
	storageRootKeyFunc func(ctx context.Context) string
	// storageKeyFunc 返回单个对象的资源相对存储路径，
	// 例如 "/pods/<namespace>/<name>" 或 "/pods/<name>"。
	storageKeyFunc func(ctx context.Context, name string) (string, error)
	// cacheKeyFunc 返回单个对象的资源相对存储路径。它被传给接收对象而非
	// 请求上下文的缓存层，因此从对象元数据中推导命名空间和名称。
	cacheKeyFunc func(obj runtime.Object) (string, error)
}

// defaultStoreKeyFuncs 根据是否命名空间作用域选择默认键构造器。
func defaultStoreKeyFuncs(prefix string, isNamespaced bool) storeKeyFuncs {
	if isNamespaced {
		return newStoreKeyFuncs(
			isNamespaced,
			func(ctx context.Context) string {
				return NamespaceKeyRootFunc(ctx, prefix)
			},
			func(ctx context.Context, name string) (string, error) {
				return NamespaceKeyFunc(ctx, prefix, name)
			},
		)
	}

	return newStoreKeyFuncs(
		isNamespaced,
		func(ctx context.Context) string {
			return prefix
		},
		func(ctx context.Context, name string) (string, error) {
			return NoNamespaceKeyFunc(ctx, prefix, name)
		},
	)
}

// newStoreKeyFuncs 组装键函数集合，并派生基于对象元数据的 cacheKeyFunc。
func newStoreKeyFuncs(
	isNamespaced bool,
	storageRootKeyFunc func(ctx context.Context) string,
	storageKeyFunc func(ctx context.Context, name string) (string, error),
) storeKeyFuncs {
	return storeKeyFuncs{
		storageRootKeyFunc: storageRootKeyFunc,
		storageKeyFunc:     storageKeyFunc,
		cacheKeyFunc: func(obj runtime.Object) (string, error) {
			accessor, err := meta.Accessor(obj)
			if err != nil {
				return "", err
			}

			ctx := genericapirequest.NewContext()
			if isNamespaced {
				ctx = genericapirequest.WithNamespace(ctx, accessor.GetNamespace())
			}
			return storageKeyFunc(ctx, accessor.GetName())
		},
	}
}

// New 实现了 RESTStorage.New。
func (e *Store) New() runtime.Object {
	return e.NewFunc()
}

// ReadinessCheck 检查存储是否已就绪、可以接受请求。
func (e *Store) ReadinessCheck() error {
	if e.ReadinessCheckFunc != nil {
		return e.ReadinessCheckFunc()
	}
	return nil
}

// Destroy 在关闭时清理其持有的资源。
func (e *Store) Destroy() {
	if e.DestroyFunc != nil {
		e.DestroyFunc()
	}
}

// NewList 实现了 rest.Lister。
func (e *Store) NewList() runtime.Object {
	return e.NewListFunc()
}

// NamespaceScoped 指示该资源是否为命名空间作用域
func (e *Store) NamespaceScoped() bool {
	if e.CreateStrategy != nil {
		return e.CreateStrategy.NamespaceScoped()
	}
	if e.UpdateStrategy != nil {
		return e.UpdateStrategy.NamespaceScoped()
	}

	panic("programmer error: no CRUD for resource, override NamespaceScoped too")
}

// GetCreateStrategy 实现了 GenericStore。
func (e *Store) GetCreateStrategy() rest.RESTCreateStrategy {
	return e.CreateStrategy
}

// GetUpdateStrategy 实现了 GenericStore。
func (e *Store) GetUpdateStrategy() rest.RESTUpdateStrategy {
	return e.UpdateStrategy
}

// GetDeleteStrategy 实现了 GenericStore。
func (e *Store) GetDeleteStrategy() rest.RESTDeleteStrategy {
	return e.DeleteStrategy
}

// GetCorruptObjDeleter 返回不安全损坏对象删除器
func (e *Store) GetCorruptObjDeleter() rest.GracefulDeleter {
	return e.corruptObjDeleter
}

// List 按照 store 的 PredicateFunc 返回匹配标签和字段选择器的条目列表。
func (e *Store) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	// 未指定选择器时使用 Everything，而不是空选择器，语义更直观。
	label := labels.Everything()
	if options != nil && options.LabelSelector != nil {
		label = options.LabelSelector
	}
	field := fields.Everything()
	if options != nil && options.FieldSelector != nil {
		field = options.FieldSelector
	}
	out, err := e.ListPredicate(ctx, e.PredicateFunc(label, field), options)
	if err != nil {
		return nil, err
	}
	if e.Decorator != nil {
		e.Decorator(out)
	}
	return out, nil
}

// ListPredicate 返回匹配给定 SelectionPredicate 的全部条目的列表。
func (e *Store) ListPredicate(ctx context.Context, p storage.SelectionPredicate, options *metainternalversion.ListOptions) (runtime.Object, error) {
	if options == nil {
		// 默认应当直接由 etcd 服务该请求：空 ResourceVersion 表示读取最新
		// （或缓存最新）数据，而不是任意历史版本。
		options = &metainternalversion.ListOptions{ResourceVersion: ""}
	}
	p.Limit = options.Limit
	p.Continue = options.Continue
	if utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) && options.ShardSelector != "" {
		sel, err := sharding.Parse(options.ShardSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid shard selector: %w", err)
		}
		p.ShardSelector = sel
	}
	list := e.NewListFunc()
	qualifiedResource := e.qualifiedResourceFromContext(ctx)
	storageOpts := storage.ListOptions{
		ResourceVersion:      options.ResourceVersion,
		ResourceVersionMatch: options.ResourceVersionMatch,
		Predicate:            p,
		Recursive:            true,
	}

	// 如果请求还不是命名空间范围的，看看字段选择器能否收窄 watch 的范围。
	// 形如 metadata.namespace=foo 的字段选择器可以收窄为单命名空间查询，
	// 从而减少扫描范围；仅当命名空间名合法时才应用该优化。
	if requestNamespace, _ := genericapirequest.NamespaceFrom(ctx); len(requestNamespace) == 0 {
		if selectorNamespace, ok := p.MatchesSingleNamespace(); ok {
			if len(validation.ValidateNamespaceName(selectorNamespace, false)) == 0 {
				ctx = genericapirequest.WithNamespace(ctx, selectorNamespace)
			}
		}
	}

	// 选择器唯一确定一个对象时，退化为单键读取而非范围扫描。
	if name, ok := p.MatchesSingle(); ok {
		if key, err := e.KeyFunc(ctx, name); err == nil {
			storageOpts.Recursive = false
			err := e.Storage.GetList(ctx, key, storageOpts, list)
			return list, storeerr.InterpretListError(err, qualifiedResource)
		}
		// 如果无法基于当前上下文提取键，则跳过该优化
	}

	err := e.Storage.GetList(ctx, e.KeyRootFunc(ctx), storageOpts, list)
	return list, storeerr.InterpretListError(err, qualifiedResource)
}

// finishNothing 是一个什么都不做的 FinishFunc。
func finishNothing(context.Context, bool) {}

// maxNameGenerationCreateAttempts 是使用 generateName 创建、且创建尝试因名称
// 冲突错误而失败时，最多会重试创建的次数。
// 每次尝试都会使用一个全新随机生成的名称。
// 选择 8 作为上限，是因为它足以在每个 generateName 前缀下生成
// 100 万个名称，且任一生成名称与已有名称冲突的概率仅为 0.1%。
// 如果不重试，在约 500 个生成名称时冲突概率就达到 0.1%，
// 在约 4500 个生成名称时达到 50%。
const maxNameGenerationCreateAttempts = 8

// Create 按照对象中的唯一键插入一个新条目。
// 注意：注册表可能会修改传入的对象（例如在策略钩子中）。调用该函数的测试
// 如果希望比较输入和输出对象的差异，可能需要先调用 DeepCopy。
func (e *Store) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	// generateName 可能产生冲突，走带重试的路径；普通 create 无需重试。
	if needsNameGeneration(obj) {
		return e.createWithGenerateNameRetry(ctx, obj, createValidation, options)
	}

	return e.create(ctx, obj, createValidation, options)
}

// needsNameGeneration 在 obj 带有 generateName 但没有 name 时返回 true。
func needsNameGeneration(obj runtime.Object) bool {
	if objectMeta, err := meta.Accessor(obj); err == nil {
		if len(objectMeta.GetGenerateName()) > 0 && len(objectMeta.GetName()) == 0 {
			return true
		}
	}
	return false
}

// createWithGenerateNameRetry 在创建因名称冲突错误失败时，最多尝试创建 obj
// maxNameGenerationCreateAttempts 次。每次尝试都基于 generateName 随机生成
// 一个新名称。
func (e *Store) createWithGenerateNameRetry(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (resultObj runtime.Object, err error) {
	for i := 0; i < maxNameGenerationCreateAttempts; i++ {
		resultObj, err = e.create(ctx, obj.DeepCopyObject(), createValidation, options)
		if err == nil || !apierrors.IsAlreadyExists(err) {
			return resultObj, err
		}
	}
	return resultObj, err
}

// create 执行真正的创建流程（不含 generateName 重试）。
// 每次重试都必须使用对象副本，避免策略钩子对上一次尝试的修改泄漏到下一次。
func (e *Store) create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	var finishCreate FinishFunc = finishNothing

	// 尽早初始化元数据。
	if objectMeta, err := meta.Accessor(obj); err != nil {
		return nil, err
	} else {
		rest.FillObjectMetaSystemFields(objectMeta)
		if len(objectMeta.GetGenerateName()) > 0 && len(objectMeta.GetName()) == 0 {
			objectMeta.SetName(e.CreateStrategy.GenerateName(objectMeta.GetGenerateName()))
		}
	}

	if e.BeginCreate != nil {
		fn, err := e.BeginCreate(ctx, obj, options)
		if err != nil {
			return nil, err
		}
		finishCreate = fn
		// 默认按失败收尾；成功路径会先替换 finishCreate，避免 defer 重复调用。
		defer func() {
			finishCreate(ctx, false)
		}()
	}

	if err := rest.BeforeCreate(e.CreateStrategy, ctx, obj); err != nil {
		return nil, err
	}
	// 此时对象已完整成型。该调用 apiserver 处理链想要强制执行的校验器了。
	if createValidation != nil {
		if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	name, err := e.ObjectNameFunc(obj)
	if err != nil {
		return nil, err
	}
	key, err := e.KeyFunc(ctx, name)
	if err != nil {
		return nil, err
	}
	qualifiedResource := e.qualifiedResourceFromContext(ctx)
	ttl, err := e.calculateTTL(obj, 0, false)
	if err != nil {
		return nil, err
	}
	out := e.NewFunc()
	if err := e.Storage.Create(ctx, key, obj, out, ttl, dryrun.IsDryRun(options.DryRun)); err != nil {
		err = storeerr.InterpretCreateError(err, qualifiedResource, name)
		err = rest.CheckGeneratedNameError(ctx, e.CreateStrategy, err, obj)
		if !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		// AlreadyExists 时回读现有对象：若它正在删除，给错误加上说明，
		// 帮助客户端理解“同名对象尚未消失”而非单纯重名。
		if errGet := e.Storage.Get(ctx, key, storage.GetOptions{}, out); errGet != nil {
			return nil, err
		}
		accessor, errGetAcc := meta.Accessor(out)
		if errGetAcc != nil {
			return nil, err
		}
		if accessor.GetDeletionTimestamp() != nil {
			msg := &err.(*apierrors.StatusError).ErrStatus.Message
			*msg = fmt.Sprintf("object is being deleted: %s", *msg)
		}
		return nil, err
	}
	// 操作已成功。如果存在 finish 函数就调用它，
	// 然后确保 defer 不会再调用第二次。
	fn := finishCreate
	finishCreate = finishNothing
	fn(ctx, true)

	if e.AfterCreate != nil {
		e.AfterCreate(out, options)
	}
	if e.Decorator != nil {
		e.Decorator(out)
	}
	return out, nil
}

// ShouldDeleteDuringUpdate 是判断某对象是否应当在更新过程中被删除的默认函数。
// 它检查：新对象没有 finalizer、已存在对象的 deletionTimestamp 已设置，
// 且已存在对象的 deletionGracePeriodSeconds 为 0 或 nil
func ShouldDeleteDuringUpdate(ctx context.Context, key string, obj, existing runtime.Object) bool {
	newMeta, err := meta.Accessor(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return false
	}
	oldMeta, err := meta.Accessor(existing)
	if err != nil {
		utilruntime.HandleError(err)
		return false
	}
	if len(newMeta.GetFinalizers()) > 0 {
		// 新对象仍留有 finalizers 时不删除
		return false
	}
	if oldMeta.GetDeletionTimestamp() == nil {
		// 已存在对象尚未发起过删除请求时不删除
		return false
	}
	// 若已存在对象没有宽限期或宽限期为 0，则删除
	return oldMeta.GetDeletionGracePeriodSeconds() == nil || *oldMeta.GetDeletionGracePeriodSeconds() == 0
}

// deleteWithoutFinalizers 处理删除对象时忽略其 finalizer 列表。
// 用于已完成收尾（finalized）或从未初始化过收尾的对象。
func (e *Store) deleteWithoutFinalizers(ctx context.Context, name, key string, obj runtime.Object, preconditions *storage.Preconditions, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	out := e.NewFunc()
	klog.V(6).InfoS("Going to delete object from registry, triggered by update", "object", klog.KRef(genericapirequest.NamespaceValue(ctx), name))
	// 这里使用 rest.ValidateAllObjectFunc，因为该请求是 UPDATE 请求，且已经通过了 UPDATE 动词对应的准入检查。
	if err := e.Storage.Delete(ctx, key, out, preconditions, rest.ValidateAllObjectFunc, dryrun.IsDryRun(options.DryRun), nil, storage.DeleteOptions{}); err != nil {
		// 删除存在竞态：可能有多个更新请求
		// 并发移除对象上的所有 finalizers，因此我们
		// 忽略 NotFound 错误。
		if storage.IsNotFound(err) {
			_, err := e.finalizeDelete(ctx, obj, true, options)
			// 若 PUT 成功，客户端期望得到的是更新后的对象，
			// 但 finalizeDelete 返回的是 metav1.Status，
			// 所以改为返回请求中的对象。
			return obj, false, err
		}
		return nil, false, storeerr.InterpretDeleteError(err, e.qualifiedResourceFromContext(ctx), name)
	}
	_, err := e.finalizeDelete(ctx, out, true, options)
	// 若 PUT 成功，客户端期望得到的是更新后的对象，但
	// finalizeDelete 返回的是 metav1.Status，所以改为返回
	// 请求中的对象。
	return obj, false, err
}

// Update 对对象执行原子的更新与写入（update and set）。返回更新的结果或错误。
// 如果注册表允许 create-on-update，则会执行创建流程。
// 返回值中除对象和错误外还有一个布尔值，用于指示本次是否创建了对象。
//
// 整个“读取-修改-写回”在 GuaranteedUpdate 内原子完成，回调可能被重试，
// 因此回调必须无副作用（或把副作用放在 FinishFunc 中）。
func (e *Store) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	key, err := e.KeyFunc(ctx, name)
	if err != nil {
		return nil, false, err
	}

	var (
		creatingObj runtime.Object
		creating    = false
	)

	qualifiedResource := e.qualifiedResourceFromContext(ctx)
	storagePreconditions := &storage.Preconditions{}
	if preconditions := objInfo.Preconditions(); preconditions != nil {
		storagePreconditions.UID = preconditions.UID
		storagePreconditions.ResourceVersion = preconditions.ResourceVersion
	}

	out := e.NewFunc()

	// 仅当该类型允许 create-on-update，或正在强制允许 create（如 server-side-apply）时，
	// 才忽略 NotFound 错误
	ignoreNotFound := e.UpdateStrategy.AllowCreateOnUpdate(ctx) || forceAllowCreate
	// deleteObj 仅在确实执行删除时使用
	var deleteObj runtime.Object
	// GuaranteedUpdate 会在冲突时重试回调；existing 是存储中的最新对象。
	err = e.Storage.GuaranteedUpdate(ctx, key, out, ignoreNotFound, storagePreconditions, func(existing runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
		existingResourceVersion, err := e.Storage.Versioner().ObjectResourceVersion(existing)
		if err != nil {
			return nil, nil, err
		}
		if existingResourceVersion == 0 {
			if !e.UpdateStrategy.AllowCreateOnUpdate(ctx) && !forceAllowCreate {
				return nil, nil, apierrors.NewNotFound(qualifiedResource, name)
			}
		}

		// 基于已存在的对象，得到新对象
		obj, err := objInfo.UpdatedObject(ctx, existing)
		if err != nil {
			return nil, nil, err
		}

		// 如果 AllowUnconditionalUpdate() 为 true，且用户指定的对象没有
		// resource version，我们就用最新版本填充它。否则，检查用户指定的
		// 版本是否与最新存储对象的版本一致。
		newResourceVersion, err := e.Storage.Versioner().ObjectResourceVersion(obj)
		if err != nil {
			return nil, nil, err
		}
		doUnconditionalUpdate := newResourceVersion == 0 && e.UpdateStrategy.AllowUnconditionalUpdate(ctx)

		if existingResourceVersion == 0 {
			// 尽早初始化元数据。
			if objectMeta, err := meta.Accessor(obj); err != nil {
				return nil, nil, err
			} else {
				// 对 create-via-update 和 create-via-apply 请求清空元数据，以与 create
				// 行为保持一致。注意：这一步发生在前置条件检查之后。清空系统字段后
				// 重新填充，可防止客户端伪造 UID/时间戳等。
				rest.WipeObjectMetaSystemFields(objectMeta)
				rest.FillObjectMetaSystemFields(objectMeta)
			}

			var finishCreate FinishFunc = finishNothing

			if e.BeginCreate != nil {
				fn, err := e.BeginCreate(ctx, obj, newCreateOptionsFromUpdateOptions(options))
				if err != nil {
					return nil, nil, err
				}
				finishCreate = fn
				defer func() {
					finishCreate(ctx, false)
				}()
			}

			creating = true
			creatingObj = obj
			if err := rest.BeforeCreate(e.CreateStrategy, ctx, obj); err != nil {
				return nil, nil, err
			}
			// 此时对象已完整成型。该调用 apiserver 处理链想要强制执行的校验器了。
			if createValidation != nil {
				if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
					return nil, nil, err
				}
			}
			ttl, err := e.calculateTTL(obj, 0, false)
			if err != nil {
				return nil, nil, err
			}

			// 操作已成功。如果存在 finish 函数就调用它，
			// 然后确保 defer 不会再调用第二次。
			fn := finishCreate
			finishCreate = finishNothing
			fn(ctx, true)

			return obj, &ttl, nil
		}

		creating = false
		creatingObj = nil
		if doUnconditionalUpdate {
			// 把对象的 resource version 更新为与最新存储对象的
			// resource version 一致。
			err = e.Storage.Versioner().UpdateObject(obj, res.ResourceVersion)
			if err != nil {
				return nil, nil, err
			}
		} else {
			// 检查对象的 resource version 是否与最新的
			// resource version 匹配。
			if newResourceVersion == 0 {
				// TODO: Invalid 错误本应有一个 Resource 字段。
				// 该字段加上之后，我们应当填充 Resource 并把 Kind
				// 字段留空。见 #18526 中的讨论。
				qualifiedKind := schema.GroupKind{Group: qualifiedResource.Group, Kind: qualifiedResource.Resource}
				fieldErrList := field.ErrorList{field.Invalid(field.NewPath("metadata").Child("resourceVersion"), newResourceVersion, "must be specified for an update")}
				return nil, nil, apierrors.NewInvalid(qualifiedKind, name, fieldErrList)
			}
			if newResourceVersion != existingResourceVersion {
				return nil, nil, apierrors.NewConflict(qualifiedResource, name, fmt.Errorf(OptimisticLockErrorMsg))
			}
		}

		var finishUpdate FinishFunc = finishNothing

		if e.BeginUpdate != nil {
			fn, err := e.BeginUpdate(ctx, obj, existing, options)
			if err != nil {
				return nil, nil, err
			}
			finishUpdate = fn
			defer func() {
				finishUpdate(ctx, false)
			}()
		}

		if err := rest.BeforeUpdate(e.UpdateStrategy, ctx, obj, existing); err != nil {
			return nil, nil, err
		}

		// 忽略仅影响 managed fields 时间戳的变更。
		// FieldManager 无法感知规范化字段、默认值字段
		// 及其他改动带来的变化。
		obj, err = fieldmanager.IgnoreManagedFieldsTimestampsTransformer(ctx, obj, existing)
		if err != nil {
			return nil, nil, err
		}

		// 此时对象已完整成型。该调用 apiserver 处理链想要强制执行的校验器了。
		if updateValidation != nil {
			if err := updateValidation(ctx, obj.DeepCopyObject(), existing.DeepCopyObject()); err != nil {
				return nil, nil, err
			}
		}
		// 检查默认的 delete-during-update 条件，以及（若提供）存储自身实现的条件。
		// 移除最后一个 finalizer 的更新等价于删除：在这里转为删除流程，
		// 避免对象以“已无 finalizer 但仍存在”的状态残留。
		if ShouldDeleteDuringUpdate(ctx, key, obj, existing) &&
			(e.ShouldDeleteDuringUpdate == nil || e.ShouldDeleteDuringUpdate(ctx, key, obj, existing)) {
			deleteObj = obj
			return nil, nil, errEmptiedFinalizers
		}
		ttl, err := e.calculateTTL(obj, res.TTL, true)
		if err != nil {
			return nil, nil, err
		}

		// 操作已成功。如果存在 finish 函数就调用它，
		// 然后确保 defer 不会再调用第二次。
		fn := finishUpdate
		finishUpdate = finishNothing
		fn(ctx, true)

		if int64(ttl) != res.TTL {
			return obj, &ttl, nil
		}
		return obj, nil, nil
	}, dryrun.IsDryRun(options.DryRun), nil)

	if err != nil {
		// 删除该对象
		if err == errEmptiedFinalizers {
			return e.deleteWithoutFinalizers(ctx, name, key, deleteObj, storagePreconditions, newDeleteOptionsFromUpdateOptions(options))
		}
		if creating {
			err = storeerr.InterpretCreateError(err, qualifiedResource, name)
			err = rest.CheckGeneratedNameError(ctx, e.CreateStrategy, err, creatingObj)
		} else {
			err = storeerr.InterpretUpdateError(err, qualifiedResource, name)
		}
		return nil, false, err
	}

	if creating {
		if e.AfterCreate != nil {
			e.AfterCreate(out, newCreateOptionsFromUpdateOptions(options))
		}
	} else {
		if e.AfterUpdate != nil {
			e.AfterUpdate(out, options)
		}
	}
	if e.Decorator != nil {
		e.Decorator(out)
	}
	return out, creating, nil
}

// 这是一个辅助函数：在 create-on-update 路径中，把 UpdateOptions 转换为 CreateOptions。
func newCreateOptionsFromUpdateOptions(in *metav1.UpdateOptions) *metav1.CreateOptions {
	co := &metav1.CreateOptions{
		DryRun:          in.DryRun,
		FieldManager:    in.FieldManager,
		FieldValidation: in.FieldValidation,
	}
	co.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("CreateOptions"))
	return co
}

// 这是一个辅助函数：在 delete-on-update 路径中，把 UpdateOptions 转换为 DeleteOptions。
func newDeleteOptionsFromUpdateOptions(in *metav1.UpdateOptions) *metav1.DeleteOptions {
	do := &metav1.DeleteOptions{
		DryRun: in.DryRun,
	}
	do.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("DeleteOptions"))
	return do
}

// Get 从存储中检索条目。
func (e *Store) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	obj := e.NewFunc()
	key, err := e.KeyFunc(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := e.Storage.Get(ctx, key, storage.GetOptions{ResourceVersion: options.ResourceVersion}, obj); err != nil {
		return nil, storeerr.InterpretGetError(err, e.qualifiedResourceFromContext(ctx), name)
	}
	if e.Decorator != nil {
		e.Decorator(obj)
	}
	return obj, nil
}

// qualifiedResourceFromContext 尝试从上下文的请求信息（request info）中获取
// GroupResource。如果上下文中没有请求信息，则使用 DefaultQualifiedResource。
func (e *Store) qualifiedResourceFromContext(ctx context.Context) schema.GroupResource {
	if info, ok := genericapirequest.RequestInfoFrom(ctx); ok {
		return schema.GroupResource{Group: info.APIGroup, Resource: info.Resource}
	}
	// 某些实现会直接访问存储，因此上下文中没有 RequestInfo
	return e.DefaultQualifiedResource
}

var (
	errAlreadyDeleting   = fmt.Errorf("abort delete")
	errDeleteNow         = fmt.Errorf("delete now")
	errEmptiedFinalizers = fmt.Errorf("emptied finalizers")
)

// shouldOrphanDependents 在应当为孤立（orphaning）设置/更新
// FinalizerOrphanDependents finalizer 时返回 true。按优先级从高到低，
// 有三个因素会影响是否增删 FinalizerOrphanDependents：options、
// 对象已有的 finalizers，以及 e.DeleteStrategy.DefaultGarbageCollectionPolicy。
func shouldOrphanDependents(ctx context.Context, e *Store, accessor metav1.Object, options *metav1.DeleteOptions) bool {
	// 从该 REST 对象类型获取默认 GC 策略
	gcStrategy, ok := e.DeleteStrategy.(rest.GarbageCollectionDeleteStrategy)
	var defaultGCPolicy rest.GarbageCollectionPolicy
	if ok {
		defaultGCPolicy = gcStrategy.DefaultGarbageCollectionPolicy(ctx)
	}

	if defaultGCPolicy == rest.Unsupported {
		// 返回 false，表示我们不应 orphan（孤立其下属对象）
		return false
	}

	// 显式 OrphanDependents 优先于 PropagationPolicy，保持旧字段兼容。
	// 删除时设置了显式策略，它会覆盖一切
	//nolint:staticcheck // SA1019 backwards compatibility
	if options != nil && options.OrphanDependents != nil {
		//nolint:staticcheck // SA1019 backwards compatibility
		return *options.OrphanDependents
	}
	if options != nil && options.PropagationPolicy != nil {
		switch *options.PropagationPolicy {
		case metav1.DeletePropagationOrphan:
			return true
		case metav1.DeletePropagationBackground, metav1.DeletePropagationForeground:
			return false
		}
	}

	// 如果对象上设置了 finalizer，它会覆盖默认策略
	// 校验应确保两种情况不会同时成立。
	finalizers := accessor.GetFinalizers()
	for _, f := range finalizers {
		switch f {
		case metav1.FinalizerOrphanDependents:
			return true
		case metav1.FinalizerDeleteDependents:
			return false
		}
	}

	// 若存在，则从该 REST 对象类型获取默认 orphan 策略
	return defaultGCPolicy == rest.OrphanDependents
}

// shouldDeleteDependentsInForeground 在应当为前台删除设置/更新
// FinalizerDeleteDependents finalizer 时返回 true。按优先级从高到低，
// 有三个因素会影响是否增删 FinalizerDeleteDependents：options、
// 对象已有的 finalizers，以及 e.DeleteStrategy.DefaultGarbageCollectionPolicy。
func shouldDeleteDependentsInForeground(ctx context.Context, e *Store, accessor metav1.Object, options *metav1.DeleteOptions) bool {
	// 从该 REST 对象类型获取默认 GC 策略
	if gcStrategy, ok := e.DeleteStrategy.(rest.GarbageCollectionDeleteStrategy); ok && gcStrategy.DefaultGarbageCollectionPolicy(ctx) == rest.Unsupported {
		// 返回 false，表示我们不应以前台方式删除
		return false
	}

	// 如果删除时设置了显式策略，它会同时覆盖两者
	//nolint:staticcheck // SA1019 backwards compatibility
	if options != nil && options.OrphanDependents != nil {
		return false
	}
	if options != nil && options.PropagationPolicy != nil {
		switch *options.PropagationPolicy {
		case metav1.DeletePropagationForeground:
			return true
		case metav1.DeletePropagationBackground, metav1.DeletePropagationOrphan:
			return false
		}
	}

	// 如果对象上设置了 finalizer，它会覆盖默认策略
	// 校验已确保两种情况不会同时成立。
	finalizers := accessor.GetFinalizers()
	for _, f := range finalizers {
		switch f {
		case metav1.FinalizerDeleteDependents:
			return true
		case metav1.FinalizerOrphanDependents:
			return false
		}
	}

	return false
}

// deletionFinalizersForGarbageCollection 分析对象和删除选项，判断该对象
// 是否需要由垃圾回收器进行收尾处理。若需要，返回应施加的删除 finalizer
// 集合，以及一个布尔值，指示 finalizer 列表是否发生变化、需要更新。
//
// 返回的 finalizer 预期由垃圾回收器负责处理。如果该 store 禁用了垃圾回收，
// 本函数返回 false，以确保不会设置那些永远不会被清除的 finalizer。
func deletionFinalizersForGarbageCollection(ctx context.Context, e *Store, accessor metav1.Object, options *metav1.DeleteOptions) (bool, []string) {
	if !e.EnableGarbageCollection {
		// 未启用 GC 时绝不设置 GC finalizer，否则永远无人清理，对象会永久卡住。
		return false, []string{}
	}
	shouldOrphan := shouldOrphanDependents(ctx, e, accessor, options)
	shouldDeleteInForeground := shouldDeleteDependentsInForeground(ctx, e, accessor, options)
	newFinalizers := []string{}

	// 先移除两个 GC finalizer，如有需要再重新加回。
	for _, f := range accessor.GetFinalizers() {
		if f == metav1.FinalizerOrphanDependents || f == metav1.FinalizerDeleteDependents {
			continue
		}
		newFinalizers = append(newFinalizers, f)
	}

	if shouldOrphan {
		newFinalizers = append(newFinalizers, metav1.FinalizerOrphanDependents)
	}
	if shouldDeleteInForeground {
		newFinalizers = append(newFinalizers, metav1.FinalizerDeleteDependents)
	}

	oldFinalizerSet := sets.NewString(accessor.GetFinalizers()...)
	newFinalizersSet := sets.NewString(newFinalizers...)
	if oldFinalizerSet.Equal(newFinalizersSet) {
		return false, accessor.GetFinalizers()
	}
	return true, newFinalizers
}

// markAsDeleting 把 obj 的 DeletionGracePeriodSeconds 设为 0；若不存在已有的
// deletionTimestamp，或已有的 deletionTimestamp 时间更靠后，则把
// DeletionTimestamp 设为"现在"。finalizer 的使用方会关注此类更新，
// 只要对象 Finalizers 列表中含它们的 ID，就会对该对象执行收尾处理。
func markAsDeleting(obj runtime.Object, now time.Time) (err error) {
	objectMeta, kerr := meta.Accessor(obj)
	if kerr != nil {
		return kerr
	}
	// 这里处理不支持优雅删除（graceful deletion）的资源的 Generation 递增；支持优雅
	// 删除的资源由 pkg/api/rest/delete.go 处理。generation 递增让客户端能观察到
	// “删除已开始”这一代际变化。
	if objectMeta.GetDeletionTimestamp() == nil && objectMeta.GetGeneration() > 0 {
		objectMeta.SetGeneration(objectMeta.GetGeneration() + 1)
	}
	existingDeletionTimestamp := objectMeta.GetDeletionTimestamp()
	if existingDeletionTimestamp == nil || existingDeletionTimestamp.After(now) {
		metaNow := metav1.NewTime(now)
		objectMeta.SetDeletionTimestamp(&metaNow)
	}
	var zero int64 = 0
	objectMeta.SetDeletionGracePeriodSeconds(&zero)
	return nil
}

// updateForGracefulDeletionAndFinalizers 通过设置删除时间戳和宽限期秒数
// （优雅删除）、并更新 finalizer 列表（收尾处理），来更新给定对象；
// 它返回：
//
//  1. 一个错误
//  2. 一个布尔值：对象未找到、但应当被忽略
//  3. 一个布尔值：对象的宽限期已耗尽、应当立即删除
//  4. 一个带有已更新状态的新输出对象
//  5. 对象最后已存在状态的副本
func (e *Store) updateForGracefulDeletionAndFinalizers(ctx context.Context, name, key string, options *metav1.DeleteOptions, preconditions storage.Preconditions, deleteValidation rest.ValidateObjectFunc, in runtime.Object) (err error, ignoreNotFound, deleteImmediately bool, out, lastExisting runtime.Object) {
	lastGraceful := int64(0)
	var pendingFinalizers bool
	out = e.NewFunc()
	err = e.Storage.GuaranteedUpdate(
		ctx,
		key,
		out,
		false, /* ignoreNotFound 参数：此处不忽略 NotFound */
		&preconditions,
		storage.SimpleUpdate(func(existing runtime.Object) (runtime.Object, error) {
			if err := deleteValidation(ctx, existing); err != nil {
				return nil, err
			}
			graceful, pendingGraceful, err := rest.BeforeDelete(e.DeleteStrategy, ctx, existing, options)
			if err != nil {
				return nil, err
			}
			if pendingGraceful {
				// 删除已在进行中：不再接受通过 DeleteOptions 修改 finalizer，
				// 防止并发删除改变既定语义。
				return nil, errAlreadyDeleting
			}

			// 按 options 的要求增删 orphan finalizer。
			// 注意这发生在 pendingGraceful 检查之后，因此一旦删除
			// 已开始，就无法再通过 DeleteOptions 更新 finalizers。
			existingAccessor, err := meta.Accessor(existing)
			if err != nil {
				return nil, err
			}
			needsUpdate, newFinalizers := deletionFinalizersForGarbageCollection(ctx, e, existingAccessor, options)
			if needsUpdate {
				existingAccessor.SetFinalizers(newFinalizers)
			}

			pendingFinalizers = len(existingAccessor.GetFinalizers()) != 0
			if !graceful {
				// 若对象存在待处理的 finalizers 但不支持优雅删除，
				// 则把 DeleteGracePeriods 设为 0
				if pendingFinalizers {
					klog.V(6).InfoS("Object has pending finalizers, so the registry is going to update its status to deleting",
						"object", klog.KRef(genericapirequest.NamespaceValue(ctx), name), "gracePeriod", time.Second*0)
					err = markAsDeleting(existing, time.Now())
					if err != nil {
						return nil, err
					}
					return existing, nil
				}
				return nil, errDeleteNow
			}
			lastGraceful = *options.GracePeriodSeconds
			lastExisting = existing
			return existing, nil
		}),
		dryrun.IsDryRun(options.DryRun),
		nil,
	)
	switch err {
	case nil:
		// 若存在待处理的 finalizers，我们绝不立即删除对象。
		if pendingFinalizers {
			return nil, false, false, out, lastExisting
		}
		if lastGraceful > 0 {
			return nil, false, false, out, lastExisting
		}
		// 走到这里说明：注册表支持宽限期机制，而我们是有意地不带
		// 宽限期删除。此时我们可能与其他 k8s 组件发生竞态。如果其他
		// 组件赢得了竞态，对象将查不到，我们应当容忍 NotFound 错误。
		// 详见 https://github.com/kubernetes/kubernetes/issues/19403。
		return nil, true, true, out, lastExisting
	case errDeleteNow:
		// 我们已经把对象的宽限期更新为 0，或它本来就是 0，
		// 所以应当继续往下走，真正删除该对象。
		return nil, false, true, out, lastExisting
	case errAlreadyDeleting:
		out, err = e.finalizeDelete(ctx, in, true, options)
		return err, false, false, out, lastExisting
	default:
		return storeerr.InterpretUpdateError(err, e.qualifiedResourceFromContext(ctx), name), false, false, out, lastExisting
	}
}

// Delete 从存储中移除条目。
// 由于优雅删除策略的作用，options 可能被 rest.BeforeDelete 修改。
func (e *Store) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	key, err := e.KeyFunc(ctx, name)
	if err != nil {
		return nil, false, err
	}
	obj := e.NewFunc()
	qualifiedResource := e.qualifiedResourceFromContext(ctx)
	if err = e.Storage.Get(ctx, key, storage.GetOptions{}, obj); err != nil {
		return nil, false, storeerr.InterpretDeleteError(err, qualifiedResource, name)
	}

	// 把 "nil" 视为立即删除，以兼容 delete 的旧调用方；即 nil 选项按“立即删除”处理，
	// 兼容未传 DeleteOptions 的旧客户端。
	if options == nil {
		options = metav1.NewDeleteOptions(0)
	}
	var preconditions storage.Preconditions
	if options.Preconditions != nil {
		preconditions.UID = options.Preconditions.UID
		preconditions.ResourceVersion = options.Preconditions.ResourceVersion
	}
	graceful, pendingGraceful, err := rest.BeforeDelete(e.DeleteStrategy, ctx, obj, options)
	if err != nil {
		return nil, false, err
	}
	// 这意味着若删除已在进行中（pending），就无法再通过 DeleteOptions 更新 finalizers
	if pendingGraceful {
		out, err := e.finalizeDelete(ctx, obj, false, options)
		return out, false, err
	}
	// 检查 obj 是否有待处理的 finalizers
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, false, apierrors.NewInternalError(err)
	}
	pendingFinalizers := len(accessor.GetFinalizers()) != 0
	var ignoreNotFound bool
	var deleteImmediately bool = true
	var lastExisting, out runtime.Object

	// 通过发起正确的更新，来处理优雅删除与收尾处理（finalization）的各种
	// 组合情况。
	shouldUpdateFinalizers, _ := deletionFinalizersForGarbageCollection(ctx, e, accessor, options)
	// TODO: 移除该检查，因为我们现在已经支持无操作（no-op）更新。
	if graceful || pendingFinalizers || shouldUpdateFinalizers {
		err, ignoreNotFound, deleteImmediately, out, lastExisting = e.updateForGracefulDeletionAndFinalizers(ctx, name, key, options, preconditions, deleteValidation, obj)
		// 既然我们已经更新了对象，若 preconditions.ResourceVersion 已设置，也需更新它。
		if err == nil && deleteImmediately && preconditions.ResourceVersion != nil {
			accessor, err = meta.Accessor(out)
			if err != nil {
				return out, false, apierrors.NewInternalError(err)
			}
			resourceVersion := accessor.GetResourceVersion()
			preconditions.ResourceVersion = &resourceVersion
		}
	}

	// !deleteImmediately 覆盖了所有 err != nil 的情况。两者都保留是为了面向未来。
	if !deleteImmediately || err != nil {
		return out, false, err
	}

	// 在执行 dry-run 请求时，继续往下走本函数没有意义。更糟的是，
	// 它实际上会用数据库中未设置 finalizer 和 deletionTimestamp 的那个
	// 对象版本覆盖 "out"（因为上面的更新同样是 dry-run 的）。
	// 如果该版本已经在手，就直接返回它；否则可以调用 dry-run 的
	// delete，拿到对象的最新版本。
	if dryrun.IsDryRun(options.DryRun) && out != nil {
		return out, true, nil
	}

	// 立即删除，或不支持优雅删除
	klog.V(6).InfoS("Going to delete object from registry", "object", klog.KRef(genericapirequest.NamespaceValue(ctx), name))
	out = e.NewFunc()
	if err := e.Storage.Delete(ctx, key, out, &preconditions, storage.ValidateObjectFunc(deleteValidation), dryrun.IsDryRun(options.DryRun), nil, storage.DeleteOptions{}); err != nil {
		// 忽略 NotFound 错误的原因请参见我们设置 ignoreNotFound 的位置。
		// 并发删除时 NotFound 属预期：用最后一次看到的对象收尾，返回成功语义。
		if storage.IsNotFound(err) && ignoreNotFound && lastExisting != nil {
			// lastExisting 可能不是对象被删除前的最后状态，
			// 但它是最接近的近似值。
			out, err := e.finalizeDelete(ctx, lastExisting, true, options)
			return out, true, err
		}
		return nil, false, storeerr.InterpretDeleteError(err, qualifiedResource, name)
	}
	out, err = e.finalizeDelete(ctx, out, true, options)
	return out, true, err
}

// DeleteReturnsDeletedObject 实现了 rest.MayReturnFullObjectDeleter 接口
func (e *Store) DeleteReturnsDeletedObject() bool {
	return e.ReturnDeletedObject
}

// deleteCollectionPageSize 是 DeleteCollection 调用期间从存储列举对象时
// 使用的分页大小。它是变量，以便在测试中覆盖。
var deleteCollectionPageSize = int64(10000)

// DeleteCollection 从存储中移除 List 在给定 ListOptions 下返回的全部条目。
//
// DeleteCollection 目前不是原子的。可能出现只删除了部分对象就返回错误的
// 情况。成功时会返回被删除对象的列表。
//
// 实现采用“分页列举 + 工作池并发删除”：分页避免一次性加载大集合，
// 并发删除提高吞吐；但由于非原子，部分失败时已删除的对象不会回滚。
func (e *Store) DeleteCollection(ctx context.Context, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions, listOptions *metainternalversion.ListOptions) (runtime.Object, error) {
	if listOptions == nil {
		listOptions = &metainternalversion.ListOptions{}
	} else {
		// 复制一份，后续分页会修改 Continue/ResourceVersion，不能污染调用方。
		listOptions = listOptions.DeepCopy()
	}

	var items []runtime.Object

	workersNumber := e.DeleteCollectionWorkers
	if workersNumber < 1 {
		workersNumber = 1
	}
	wg := sync.WaitGroup{}
	// 确保 chanSize 不要太高（避免做无用功），但同时要足够高，
	// 使我们在处理完整个页面之前就能开始列举。
	chanSize := 2 * workersNumber
	if chanSize < 256 {
		chanSize = 256
	}
	toProcess := make(chan runtime.Object, chanSize)
	errs := make(chan error, workersNumber+1)
	workersExited := make(chan struct{})

	wg.Add(workersNumber)
	for i := 0; i < workersNumber; i++ {
		go func() {
			// panic 不会跨 goroutine 边界传播
			defer utilruntime.HandleCrashWithContext(ctx, func(_ context.Context, panicReason interface{}) {
				errs <- fmt.Errorf("DeleteCollection goroutine panicked: %v", panicReason)
			})
			defer wg.Done()

			for item := range toProcess {
				accessor, err := meta.Accessor(item)
				if err != nil {
					errs <- err
					return
				}
				// 对删除选项做 DeepCopy：各个优雅删除器会通过 delete 方法中调用的删除策略里的
				// 变更函数来传递改动。这种做法虽然一直都很丑陋，但单次调用时是能工作的。
				// 而通过 delete collection 发起多次调用时，作用于 pod/A 的修改可能改变
				// pod/B 最终使用的选项，因此每个对象使用独立的 options 副本，避免跨对象污染。
				if _, _, err := e.Delete(ctx, accessor.GetName(), deleteValidation, options.DeepCopy()); err != nil && !apierrors.IsNotFound(err) {
					klog.V(4).InfoS("Delete object in DeleteCollection failed", "object", klog.KObj(accessor), "err", err)
					errs <- err
					return
				}
			}
		}()
	}
	// 若所有 worker 都已退出，通知分发者。
	go func() {
		defer utilruntime.HandleCrashWithContext(ctx, func(_ context.Context, panicReason interface{}) {
			errs <- fmt.Errorf("DeleteCollection workers closer panicked: %v", panicReason)
		})
		wg.Wait()
		close(workersExited)
	}()

	hasLimit := listOptions.Limit > 0
	if listOptions.Limit == 0 {
		listOptions.Limit = deleteCollectionPageSize
	}

	// 分页执行列举请求，把所有条目投递给 worker。
	listObj, err := func() (runtime.Object, error) {
		defer close(toProcess)

		processedItems := 0
		var originalList runtime.Object
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			listObj, err := e.List(ctx, listOptions)
			if err != nil {
				return nil, err
			}

			newItems, err := meta.ExtractList(listObj)
			if err != nil {
				return nil, err
			}
			items = append(items, newItems...)

			for i := 0; i < len(newItems); i++ {
				select {
				case toProcess <- newItems[i]:
				case <-workersExited:
					klog.V(4).InfoS("workers already exited, and there are some items waiting to be processed", "queued/finished", i, "total", processedItems+len(newItems))
					// 尽可能把 worker 产生的错误向上传播。
					select {
					case err := <-errs:
						return nil, err
					default:
						return nil, fmt.Errorf("all DeleteCollection workers exited")
					}
				}
			}
			processedItems += len(newItems)

			// 若原始请求设置了 limit，执行完该页即结束。
			if hasLimit {
				return listObj, nil
			}

			if originalList == nil {
				originalList = listObj
				meta.SetList(originalList, nil)
			}

			// 若没有更多条目，返回列表。
			m, err := meta.ListAccessor(listObj)
			if err != nil {
				return nil, err
			}
			if len(m.GetContinue()) == 0 {
				meta.SetList(originalList, items)
				return originalList, nil
			}

			// 设置下一轮循环。
			listOptions.Continue = m.GetContinue()
			listOptions.ResourceVersion = ""
			listOptions.ResourceVersionMatch = ""
		}
	}()
	if err != nil {
		return nil, err
	}

	// 等待所有 worker 退出。
	<-workersExited

	select {
	case err := <-errs:
		return nil, err
	default:
		return listObj, nil
	}
}

// finalizeDelete 在 runHooks 为真时执行 Store 的 AfterDelete 钩子，
// 并在适当情况下返回经过装饰的已删除对象。
func (e *Store) finalizeDelete(ctx context.Context, obj runtime.Object, runHooks bool, options *metav1.DeleteOptions) (runtime.Object, error) {
	// runHooks=false 用于“已经处于删除中”的重复 DELETE，避免重复触发 AfterDelete。
	if runHooks && e.AfterDelete != nil {
		e.AfterDelete(obj, options)
	}
	if e.ReturnDeletedObject {
		if e.Decorator != nil {
			e.Decorator(obj)
		}
		return obj, nil
	}
	// 返回关于被删除对象的信息，使客户端能够验证对象确实被删除了，
	// 而不是在等待 finalizers 处理。
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	qualifiedResource := e.qualifiedResourceFromContext(ctx)
	details := &metav1.StatusDetails{
		Name:  accessor.GetName(),
		Group: qualifiedResource.Group,
		Kind:  qualifiedResource.Resource, // 是的，我们把 Kind 字段设置为 resource。
		UID:   accessor.GetUID(),
	}
	status := &metav1.Status{Status: metav1.StatusSuccess, Details: details}
	return status, nil
}

// Watch 为给定的 label 和 field 构造匹配器，并调用 WatchPredicate。
// 如果可以的话，应当定制 PredicateFunc，使其产生按键匹配的匹配器。
// SelectionPredicate 会自动为你完成这一点。
func (e *Store) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	label := labels.Everything()
	if options != nil && options.LabelSelector != nil {
		label = options.LabelSelector
	}
	field := fields.Everything()
	if options != nil && options.FieldSelector != nil {
		field = options.FieldSelector
	}
	predicate := e.PredicateFunc(label, field)

	resourceVersion := ""
	if options != nil {
		resourceVersion = options.ResourceVersion
		predicate.AllowWatchBookmarks = options.AllowWatchBookmarks
		if utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) && options.ShardSelector != "" {
			sel, err := sharding.Parse(options.ShardSelector)
			if err != nil {
				return nil, fmt.Errorf("invalid shard selector: %w", err)
			}
			predicate.ShardSelector = sel
		}
	}
	return e.WatchPredicate(ctx, predicate, resourceVersion, options.SendInitialEvents)
}

// WatchPredicate 启动一个 watch，观察匹配给定条件的条目。
func (e *Store) WatchPredicate(ctx context.Context, p storage.SelectionPredicate, resourceVersion string, sendInitialEvents *bool) (watch.Interface, error) {
	storageOpts := storage.ListOptions{
		ResourceVersion:   resourceVersion,
		Predicate:         p,
		Recursive:         true,
		SendInitialEvents: sendInitialEvents,
	}

	// 如果请求还不是命名空间范围的，看看字段选择器能否收窄 watch 的范围。
	// 与 List 相同的优化：字段选择器锁定单命名空间时缩小 watch 范围。
	if requestNamespace, _ := genericapirequest.NamespaceFrom(ctx); len(requestNamespace) == 0 {
		if selectorNamespace, ok := p.MatchesSingleNamespace(); ok {
			if len(validation.ValidateNamespaceName(selectorNamespace, false)) == 0 {
				ctx = genericapirequest.WithNamespace(ctx, selectorNamespace)
			}
		}
	}

	key := e.KeyRootFunc(ctx)
	if name, ok := p.MatchesSingle(); ok {
		if k, err := e.KeyFunc(ctx, name); err == nil {
			key = k
			storageOpts.Recursive = false
		}
		// 如果无法基于当前上下文提取键，则跳过该优化
	}

	w, err := e.Storage.Watch(ctx, key, storageOpts)
	if err != nil {
		return nil, err
	}
	if e.Decorator != nil {
		return newDecoratedWatcher(ctx, w, e.Decorator), nil
	}
	return w, nil
}

// calculateTTL 是一个辅助函数：获取对象更新后的 TTL，若 TTL 无法计算则
// 返回错误。若 defaultTTL 小于零，会被改为 1。零表示不设 TTL，而不是立即过期。
func (e *Store) calculateTTL(obj runtime.Object, defaultTTL int64, update bool) (ttl uint64, err error) {
	// TODO: 验证该断言是否仍然成立。

	// 如果由于服务端滞后、过期动作尚未执行，etcd 可能为节点返回负的 TTL
	// —— 我们要确保该值至少被设置。
	if defaultTTL < 0 {
		defaultTTL = 1
	}
	ttl = uint64(defaultTTL)
	if e.TTLFunc != nil {
		ttl, err = e.TTLFunc(obj, ttl, update)
	}
	return ttl, err
}

// CompleteWithOptions 用所提供的选项更新 store，并为常见字段设置默认值。
//
// 这里集中做“配置完整性校验 + 默认值填充 + 底层存储构造”，
// 任何必填项缺失都返回明确错误，避免运行期出现 nil panic。
func (e *Store) CompleteWithOptions(options *generic.StoreOptions) error {
	if e.DefaultQualifiedResource.Empty() {
		return fmt.Errorf("store %#v must have a non-empty qualified resource", e)
	}
	if e.SingularQualifiedResource.Empty() {
		return fmt.Errorf("store %#v must have a non-empty singular qualified resource", e)
	}
	if e.DefaultQualifiedResource.Group != e.SingularQualifiedResource.Group {
		return fmt.Errorf("store for %#v, singular and plural qualified resource's group name's must match", e)
	}
	if e.NewFunc == nil {
		return fmt.Errorf("store for %s must have NewFunc set", e.DefaultQualifiedResource.String())
	}
	if e.NewListFunc == nil {
		return fmt.Errorf("store for %s must have NewListFunc set", e.DefaultQualifiedResource.String())
	}
	if (e.KeyRootFunc == nil) != (e.KeyFunc == nil) {
		return fmt.Errorf("store for %s must set both KeyRootFunc and KeyFunc or neither", e.DefaultQualifiedResource.String())
	}

	if e.TableConvertor == nil {
		return fmt.Errorf("store for %s must set TableConvertor; rest.NewDefaultTableConvertor(e.DefaultQualifiedResource) can be used to output just name/creation time", e.DefaultQualifiedResource.String())
	}

	var isNamespaced bool
	switch {
	case e.CreateStrategy != nil:
		isNamespaced = e.CreateStrategy.NamespaceScoped()
	case e.UpdateStrategy != nil:
		isNamespaced = e.UpdateStrategy.NamespaceScoped()
	default:
		return fmt.Errorf("store for %s must have CreateStrategy or UpdateStrategy set", e.DefaultQualifiedResource.String())
	}

	if e.DeleteStrategy == nil {
		return fmt.Errorf("store for %s must have DeleteStrategy set", e.DefaultQualifiedResource.String())
	}

	if options.RESTOptions == nil {
		return fmt.Errorf("options for %s must have RESTOptions set", e.DefaultQualifiedResource.String())
	}

	attrFunc := options.AttrFunc
	if attrFunc == nil {
		if isNamespaced {
			attrFunc = storage.DefaultNamespaceScopedAttr
		} else {
			attrFunc = storage.DefaultClusterScopedAttr
		}
	}
	if e.PredicateFunc == nil {
		e.PredicateFunc = func(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
			return storage.SelectionPredicate{
				Label:    label,
				Field:    field,
				GetAttrs: attrFunc,
			}
		}
	}

	err := validateIndexers(options.Indexers)
	if err != nil {
		return err
	}

	opts, err := options.RESTOptions.GetRESTOptions(e.DefaultQualifiedResource, e.NewFunc())
	if err != nil {
		return err
	}

	// ResourcePrefix 必须来自底层 factory
	prefix := opts.ResourcePrefix
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	if prefix == "/" {
		return fmt.Errorf("store for %s has an invalid prefix %q", e.DefaultQualifiedResource.String(), opts.ResourcePrefix)
	}

	var keyFuncs storeKeyFuncs
	if e.KeyRootFunc == nil && e.KeyFunc == nil {
		// 未自定义键函数时，按命名空间作用域推导默认前缀。
		keyFuncs = defaultStoreKeyFuncs(prefix, isNamespaced)
	} else {
		keyFuncs = newStoreKeyFuncs(isNamespaced, e.KeyRootFunc, e.KeyFunc)
	}
	e.KeyRootFunc = keyFuncs.storageRootKeyFunc
	e.KeyFunc = keyFuncs.storageKeyFunc

	if e.DeleteCollectionWorkers == 0 {
		e.DeleteCollectionWorkers = opts.DeleteCollectionWorkers
	}

	e.EnableGarbageCollection = opts.EnableGarbageCollection

	if e.ObjectNameFunc == nil {
		e.ObjectNameFunc = func(obj runtime.Object) (string, error) {
			accessor, err := meta.Accessor(obj)
			if err != nil {
				return "", err
			}
			return accessor.GetName(), nil
		}
	}

	if e.Storage.Storage == nil {
		e.Storage.Codec = opts.StorageConfig.Codec
		var err error
		e.Storage.Storage, e.DestroyFunc, err = opts.Decorator(
			opts.StorageConfig,
			prefix,
			keyFuncs.cacheKeyFunc,
			e.NewFunc,
			e.NewListFunc,
			attrFunc,
			options.TriggerFunc,
			options.Indexers,
		)
		if err != nil {
			return err
		}
		e.StorageVersioner = opts.StorageConfig.EncodeVersioner

		if opts.CountMetricPollPeriod > 0 {
			stopFunc := e.startObservingCount(opts.CountMetricPollPeriod, opts.StorageObjectCountTracker)
			previousDestroy := e.DestroyFunc
			var once sync.Once
			// 用 once 包装：DestroyFunc 约定可能被多次调用，停止计数只能执行一次。
			e.DestroyFunc = func() {
				once.Do(func() {
					stopFunc()
					if previousDestroy != nil {
						previousDestroy()
					}
				})
			}
		}
	}
	if e.Storage.Storage != nil {
		e.ReadinessCheckFunc = e.Storage.Storage.ReadinessCheck
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.AllowUnsafeMalformedObjectDeletion) {
		e.corruptObjDeleter = NewCorruptObjectDeleter(e)
	}

	return nil
}

// startObservingCount 开始监控给定前缀并周期性更新指标。它返回一个用于停止采集的函数。
func (e *Store) startObservingCount(period time.Duration, objectCountTracker flowcontrolrequest.StorageObjectCountTracker) func() {
	ctx := genericapirequest.NewContext()
	prefix := e.KeyRootFunc(ctx)
	resourceName := e.DefaultQualifiedResource.String()
	klog.V(2).InfoS("Monitoring resource count at path", "resource", resourceName, "path", "<storage-prefix>/"+prefix)
	stopCh := make(chan struct{})
	go wait.JitterUntil(func() {
		stats, err := e.Storage.Stats(ctx)
		metrics.UpdateStoreStats(e.DefaultQualifiedResource, stats, err)
		if err != nil {
			klog.V(5).InfoS("Failed to update storage count metric", "err", err)
			return
		}

		if objectCountTracker != nil {
			objectCountTracker.Set(resourceName, stats)
		}
	}, period, resourceCountPollPeriodJitter, true, stopCh)
	return func() {
		metrics.DeleteStoreStats(e.DefaultQualifiedResource)
		close(stopCh)
	}
}

// ConvertToTable implements rest.TableConvertor；未配置转换器时回退到
// 只输出名称/创建时间的默认实现。
func (e *Store) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	if e.TableConvertor != nil {
		return e.TableConvertor.ConvertToTable(ctx, object, tableOptions)
	}
	return rest.NewDefaultTableConvertor(e.DefaultQualifiedResource).ConvertToTable(ctx, object, tableOptions)
}

// StorageVersion 返回对象持久化前会转换到的版本；nil 表示 discovery
// 中不提供 storageVersionHash。
func (e *Store) StorageVersion() runtime.GroupVersioner {
	return e.StorageVersioner
}

// GetResetFields 实现了 rest.ResetFieldsStrategy
func (e *Store) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	if e.ResetFieldsStrategy == nil {
		return nil
	}
	return e.ResetFieldsStrategy.GetResetFields()
}

// GetSingularName 返回资源单数名，用于 API 路径中的单数形式。
func (e *Store) GetSingularName() string {
	return e.SingularQualifiedResource.Resource
}

// validateIndexers 检查各索引器的名称前缀。
// 索引名必须以 "l:"（标签）或 "f:"（字段）开头，缓存层依赖该前缀
// 区分索引维度。
func validateIndexers(indexers *cache.Indexers) error {
	if indexers == nil {
		return nil
	}
	for indexName := range *indexers {
		if len(indexName) <= 2 || (indexName[:2] != "l:" && indexName[:2] != "f:") {
			return fmt.Errorf("index must prefix with \"l:\" or \"f:\"")
		}
	}
	return nil
}
