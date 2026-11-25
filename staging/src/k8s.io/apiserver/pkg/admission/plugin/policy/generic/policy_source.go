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

package generic

import (
	"context"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission/plugin/policy/internal/generic"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// 策略刷新间隔。
// TODO: 考虑缩短该间隔，或完全改为“检测策略变更已生效”的机制。
// 轮询间隔是“最终一致”的代价：事件只负责把缓存标脏，真正重编译由该循环
// 触发，因此从策略变更到生效存在一个固定上界。
const policyRefreshIntervalDefault = 1 * time.Second

// policyRefreshInterval 可在测试中通过 SetPolicyRefreshIntervalForTests 覆盖，
// 因此读写都必须持锁（atomic 不适用于 time.Duration 的跨字段语义）。
var policyRefreshInterval = policyRefreshIntervalDefault
var policyRefreshIntervalLock sync.Mutex

// policySource 是 Source[PolicyHook] 的实现：把策略与绑定 informer 组合成
// 一组可直接评估的 hook。
//
// 关键并发约定：
//   - policies 用 atomic.Pointer 发布，读者（Hooks/HasSynced）无锁；
//   - compiledPolicies 与 paramsCRDControllers 只由持有 lock 的
//     calculatePolicyData 写，因此其中所有 helper 都带 Locked 后缀。
type policySource[P runtime.Object, B runtime.Object, E Evaluator] struct {
	ctx                context.Context
	policyInformer     generic.Informer[P]
	bindingInformer    generic.Informer[B]
	restMapper         meta.RESTMapper
	newPolicyAccessor  func(P) PolicyAccessor
	newBindingAccessor func(B) BindingAccessor

	informerFactory informers.SharedInformerFactory
	dynamicClient   dynamic.Interface

	compiler func(P) E

	// 当前已编译的有效/激活 policy-binding 对列表。
	policies atomic.Pointer[[]PolicyHook[P, B, E]]
	// 策略缓存是否已变脏、需要重新编译。
	policiesDirty atomic.Bool

	lock             sync.Mutex
	compiledPolicies map[types.NamespacedName]compiledPolicyEntry[E]

	// 过渡方案，直到改用动态 informer factory。
	paramsCRDControllers map[schema.GroupVersionKind]*paramInfo
}

// paramInfo 记录为某个 param CRD 临时启动的 informer 及其生命周期控制。
type paramInfo struct {
	mapping meta.RESTMapping

	// param 变更或 informer 不再被使用时，应调用 cancel 函数
	// 停止并清理原来的 informer。
	// 取消函数同时用于回收 informer goroutine 和释放 shared factory 的引用。
	cancelFunc func()

	// 该 param 的 lister。
	informer informers.GenericInformer
}

// compiledPolicyEntry 缓存单个策略的编译产物；policyVersion 取
// resourceVersion，用来判断是否需要重新编译。
type compiledPolicyEntry[E Evaluator] struct {
	policyVersion string
	evaluator     E
}

// PolicyHook 是策略类 hook 的具体形态：策略本体 + 其全部绑定 + 参数
// informer + 已编译的 evaluator。
type PolicyHook[P runtime.Object, B runtime.Object, E Evaluator] struct {
	Policy   P
	Bindings []B

	// ParamInformer 是该策略 param CRD 的 informer；没有 param 或
	// 配置有误时为 nil。
	// ParamScope 决定 param 是命名空间级还是集群级，调度时据此解析引用。
	ParamInformer informers.GenericInformer
	ParamScope    meta.RESTScope

	// DynamicClient 用于缓存未命中时直接调用 API。
	DynamicClient dynamic.Interface
	// RESTMapper 用于在直接调用 API 前把 GVK 解析为 GVR。
	RESTMapper meta.RESTMapper

	// ConfigurationError 非空表示该 hook 配置有误（例如 param CRD 找不到），
	// 但 hook 仍会被返回，以便按 failurePolicy 决定拒绝还是忽略。
	Evaluator          E
	ConfigurationError error
}

var _ Source[PolicyHook[runtime.Object, runtime.Object, Evaluator]] = &policySource[runtime.Object, runtime.Object, Evaluator]{}

// NewPolicySource 构造一个策略来源。参数 informer 工厂用于按需为 param CRD
// 获取（或启动）informer；dynamicClient 与 restMapper 在缓存未命中时用于
// 直接访问 API。
func NewPolicySource[P runtime.Object, B runtime.Object, E Evaluator](
	policyInformer cache.SharedIndexInformer,
	bindingInformer cache.SharedIndexInformer,
	newPolicyAccessor func(P) PolicyAccessor,
	newBindingAccessor func(B) BindingAccessor,
	compiler func(P) E,
	paramInformerFactory informers.SharedInformerFactory,
	dynamicClient dynamic.Interface,
	restMapper meta.RESTMapper,
) Source[PolicyHook[P, B, E]] {
	res := &policySource[P, B, E]{
		compiler:             compiler,
		policyInformer:       generic.NewInformer[P](policyInformer),
		bindingInformer:      generic.NewInformer[B](bindingInformer),
		compiledPolicies:     map[types.NamespacedName]compiledPolicyEntry[E]{},
		newPolicyAccessor:    newPolicyAccessor,
		newBindingAccessor:   newBindingAccessor,
		paramsCRDControllers: map[schema.GroupVersionKind]*paramInfo{},
		informerFactory:      paramInformerFactory,
		dynamicClient:        dynamicClient,
		restMapper:           restMapper,
	}
	return res
}

// SetPolicyRefreshIntervalForTests 允许在测试期间覆盖刷新间隔；只应从测试调用。
// 返回值用于恢复默认值，调用方应以 defer 形式使用，避免测试间互相影响。
func SetPolicyRefreshIntervalForTests(interval time.Duration) func() {
	policyRefreshIntervalLock.Lock()
	defer policyRefreshIntervalLock.Unlock()
	policyRefreshInterval = interval
	return func() {
		policyRefreshIntervalLock.Lock()
		defer policyRefreshIntervalLock.Unlock()
		policyRefreshInterval = policyRefreshIntervalDefault
	}
}

// Run 实现 Source。它会先阻塞等待初始缓存同步，然后完成首次编译，再注册
// 事件处理器并启动周期性刷新，直到 ctx 被取消。
func (s *policySource[P, B, E]) Run(ctx context.Context) error {
	if s.ctx != nil {
		return fmt.Errorf("policy source already running")
	}

	// 先等待策略与 informer 的初始缓存同步，再协调任何内容；
	if !cache.WaitForNamedCacheSyncWithContext(ctx, s.UpstreamHasSynced) {
		err := ctx.Err()
		if err == nil {
			err = fmt.Errorf("initial cache sync for %T failed", s)
		}
		return err
	}

	s.ctx = ctx

	// 初始 list 完成后执行首次策略编译。
	s.notify()
	s.refreshPolicies()

	notifyFuncs := cache.ResourceEventHandlerFuncs{
		AddFunc: func(_ interface{}) {
			s.notify()
		},
		UpdateFunc: func(_, _ interface{}) {
			s.notify()
		},
		DeleteFunc: func(_ interface{}) {
			s.notify()
		},
	}
	handle, err := s.policyInformer.AddEventHandler(notifyFuncs)
	if err != nil {
		return err
	}
	defer func() {
		if err := s.policyInformer.RemoveEventHandler(handle); err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to remove policy event handler: %w", err))
		}
	}()

	bindingHandle, err := s.bindingInformer.AddEventHandler(notifyFuncs)
	if err != nil {
		return err
	}
	defer func() {
		if err := s.bindingInformer.RemoveEventHandler(bindingHandle); err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to remove binding event handler: %w", err))
		}
	}()

	// 启动一个 worker，定期检查策略数据是否变脏、
	// 是否需要重新编译。
	go func() {
		// 每隔 1 秒循环刷新策略，直到 context 被取消。
		// 每次循环重新读取间隔，使测试中调用 SetPolicyRefreshIntervalForTests
		// 能够生效。
		policyRefreshIntervalLock.Lock()
		interval := policyRefreshInterval
		policyRefreshIntervalLock.Unlock()
		wait.Until(s.refreshPolicies, interval, ctx.Done())
	}()

	// 阻塞到关闭；事件处理器通过上面的 defer 摘除，informer 由调用方管理。
	<-ctx.Done()
	return nil
}

// UpstreamHasSynced 表示策略与绑定两个 informer 都已完成初始 list。
func (s *policySource[P, B, E]) UpstreamHasSynced() bool {
	return s.policyInformer.HasSynced() && s.bindingInformer.HasSynced()
}

// HasSynced 实现 Source。
func (s *policySource[P, B, E]) HasSynced() bool {
	// 约定：已处理的 policy hooks 原子列表中永远不会存 nil；
	// 如果为 nil，说明还没有编译并存储全部策略。
	return s.Hooks() != nil
}

// Hooks 实现 Source。
func (s *policySource[P, B, E]) Hooks() []PolicyHook[P, B, E] {
	res := s.policies.Load()

	// 加载函数从不返回错误，理论上不会走到错误分支。
	if res == nil {
		// 尚未同步
		return nil
	}

	return *res
}

// refreshPolicies 在缓存被标脏时重算一次 hook 列表。
//
// 用 Swap(false) 原子地“认领”脏标记：即使计算过程中又有事件把标记置回 true，
// 下一次循环也会再算一遍，不会丢失更新。计算失败时主动重新标脏以重试。
func (s *policySource[P, B, E]) refreshPolicies() {
	if !s.UpstreamHasSynced() {
		return
	} else if !s.policiesDirty.Swap(false) {
		return
	}

	// 在清脏标记与计算策略之间，缓存再次被标脏也没关系：脏标记会被重新置位，
	// 下次同步比较 resourceVersion 后发现没有变化，就是一次空操作。
	klog.Infof("refreshing policies")
	policies, err := s.calculatePolicyData()

	// 有意无论是否有错误都存储策略列表：某个策略配置有误时虽会返回错误，
	// 但仍希望这些策略参与评估（例如 failAction 时需要报错）；
	// 若整个列表操作都失败了，就应该直接清空列表。
	s.policies.Store(&policies)

	if err != nil {
		// 同步策略过程中出错：重新标脏，稍后重试。
		utilruntime.HandleError(fmt.Errorf("encountered error syncing policies: %w. Rescheduling policy sync", err))
		s.notify()
	}
}

// notify 只标记脏位，不做任何计算；所有 informer 事件都走这里，避免在
// 事件回调里做重活阻塞 informer。
func (s *policySource[P, B, E]) notify() {
	s.policiesDirty.Store(true)
}

// calculatePolicyData 为每个策略计算其策略与绑定列表。生成过程中出错时，
// 返回错误以及已成功生成的部分策略列表：出错的策略 ConfigurationError
// 字段非空，但仍会包含在结果中。

// 注意返回的 error 是“部分失败”：调用方应照常发布 result，让有问题的策略
// 仍按 failurePolicy 参与评估，而不是整体失效。
func (s *policySource[P, B, E]) calculatePolicyData() ([]PolicyHook[P, B, E], error) {
	if !s.UpstreamHasSynced() {
		return nil, fmt.Errorf("cannot calculate policy data until upstream has synced")
	}

	// 粗粒度加锁，必要时可以细化。
	s.lock.Lock()
	defer s.lock.Unlock()

	// 先按“绑定指向的策略名”分组，再从策略 informer 反查；这样即使策略
	// 与绑定的到达顺序颠倒也能正确处理。
	policiesToBindings := map[types.NamespacedName][]B{}
	bindingList, err := s.bindingInformer.List(labels.Everything())
	if err != nil {
		// 除非类型配置有误（无法使用 meta.Accessor），
		// 否则不应发生。
		return nil, err
	}

	// 收集所有激活策略绑定的列表。
	for _, bindingSpec := range bindingList {
		bindingAccessor := s.newBindingAccessor(bindingSpec)
		policyKey := bindingAccessor.GetPolicyName()

		// 把该绑定加入对应策略的绑定列表。
		policiesToBindings[policyKey] = append(policiesToBindings[policyKey], bindingSpec)
	}

	result := make([]PolicyHook[P, B, E], 0, len(bindingList))
	// usedParams 记录本轮实际引用的 param GVK，函数末尾据此回收不再需要的
	// informer（见下方 orphaned param informers 清理）。
	usedParams := map[schema.GroupVersionKind]struct{}{}
	var errs []error
	for policyKey, bindingSpecs := range policiesToBindings {
		var inf generic.NamespacedLister[P] = s.policyInformer
		if len(policyKey.Namespace) > 0 {
			inf = s.policyInformer.Namespaced(policyKey.Namespace)
		}
		policySpec, err := inf.Get(policyKey.Name)
		if errors.IsNotFound(err) {
			// 绑定所指的策略不存在：可能是策略先于绑定被删除，
			// 也可能是绑定先被创建。
			//
			// 直接跳过引用了不存在策略的绑定；
			// 如果策略被重建，缓存会被标脏，
			// 本函数会再次运行。
			continue
		} else if err != nil {
			// 从缓存取值不应失败，且本函数在此之前已确认缓存同步完成，
			// 所以这里理论上不会发生。
			errs = append(errs, err)
			continue
		}

		var parsedParamKind *schema.GroupVersionKind
		policyAccessor := s.newPolicyAccessor(policySpec)

		if paramKind := policyAccessor.GetParamKind(); paramKind != nil {
			groupVersion, err := schema.ParseGroupVersion(paramKind.APIVersion)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to parse paramKind APIVersion: %w", err))
				continue
			}
			parsedParamKind = &schema.GroupVersionKind{
				Group:   groupVersion.Group,
				Version: groupVersion.Version,
				Kind:    paramKind.Kind,
			}

			// 临时做法：等共享 param informer 就绪后移除。
			usedParams[*parsedParamKind] = struct{}{}
		}

		paramInformer, paramScope, configurationError := s.ensureParamsForPolicyLocked(parsedParamKind)
		result = append(result, PolicyHook[P, B, E]{
			Policy:             policySpec,
			Bindings:           bindingSpecs,
			Evaluator:          s.compilePolicyLocked(policySpec),
			ParamInformer:      paramInformer,
			ParamScope:         paramScope,
			DynamicClient:      s.dynamicClient,
			RESTMapper:         s.restMapper,
			ConfigurationError: configurationError,
		})

		// 策略同步出错时应排队重新同步。若共享的 param informer 将来能在
		// CRD discovery 变化时通知我们，就可以去掉这段逻辑，完全依赖
		// informer 的通知。
		if configurationError != nil {
			errs = append(errs, configurationError)
		}
	}

	// 通过替换旧的已编译策略缓存来清理成为孤儿的策略
	// 注意判断依据是 policiesToBindings：没有绑定的策略不会被评估，其编译
	// 产物也就没有保留价值。
	for policyKey := range s.compiledPolicies {
		if _, wasSeen := policiesToBindings[policyKey]; !wasSeen {
			delete(s.compiledPolicies, policyKey)
		}
	}

	// 清理成为孤儿的 param informer。
	// 回收必须调用 cancelFunc，否则会泄漏 informer goroutine 和 API 连接。
	for paramKind, info := range s.paramsCRDControllers {
		if _, wasSeen := usedParams[paramKind]; !wasSeen {
			info.cancelFunc()
			delete(s.paramsCRDControllers, paramKind)
		}
	}

	err = nil
	if len(errs) > 0 {
		err = goerrors.Join(errs...)
	}
	return result, err
}

// ensureParamsForPolicyLocked 确保 paramKind 的 informer 已启动，
// 并返回该 informer 与 paramKind 的作用域。必须在持写锁时调用。
// 返回的错误会写入 PolicyHook.ConfigurationError，而不是让整个计算失败，
// 因此找不到 param CRD 时仍能对已存在部分完成评估。
func (s *policySource[P, B, E]) ensureParamsForPolicyLocked(paramSource *schema.GroupVersionKind) (informers.GenericInformer, meta.RESTScope, error) {
	if paramSource == nil {
		return nil, nil, nil
	} else if info, ok := s.paramsCRDControllers[*paramSource]; ok {
		return info.informer, info.mapping.Scope, nil
	}

	mapping, err := s.restMapper.RESTMapping(schema.GroupKind{
		Group: paramSource.Group,
		Kind:  paramSource.Kind,
	}, paramSource.Version)

	if err != nil {
		// 解析失败：返回错误以便（限流的）重试。
		// TODO(unconditional-evaluator): 原本想为该定义记录一个无条件报错的 evaluator
		return nil, nil, fmt.Errorf("failed to find resource referenced by paramKind: '%v'", *paramSource)
	}

	// 当前没有监听这个 param，为它启动一个 informer。
	// 每个 param GVK 使用独立的子 context，便于在不再被引用时精确回收；
	// 它同时继承 s.ctx，因此 apiserver 关闭时也能级联停止。
	instanceContext, instanceCancel := context.WithCancel(s.ctx)

	var informer informers.GenericInformer

	// 尝试查看给定的 informer factory 是否已有该类型的 informer；
	// 假定该 informer 已启动、并会启动与其关联的所有类型。
	if genericInformer, err := s.informerFactory.ForResource(mapping.Resource); err == nil {
		informer = genericInformer

		// 启动 informer。
		s.informerFactory.Start(instanceContext.Done())

	} else {
		// 退化为动态 JSON informer。不能使用共享的动态 informer：
		// 存在多个依赖方时无法正确清理 CRD informer
		// （既不能提前启动，也无法通过 stopCh 跟踪依赖）。
		// 因此这里每次按需新建一个独立 informer，并由 instanceCancel 单独停止。
		informer = dynamicinformer.NewFilteredDynamicInformer(
			s.dynamicClient,
			mapping.Resource,
			corev1.NamespaceAll,
			// 与 k8s 类型化 sharedInformerFactory 使用相同的 resync 间隔：
			// https://github.com/kubernetes/kubernetes/blob/7e0923899fed622efbc8679cca6b000d43633e38/cmd/kube-apiserver/app/server.go#L430
			10*time.Minute,
			cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			nil,
		)
		go informer.Informer().Run(instanceContext.Done())
	}

	klog.Infof("informer started for %v", *paramSource)
	ret := &paramInfo{
		mapping:    *mapping,
		cancelFunc: instanceCancel,
		informer:   informer,
	}
	s.paramsCRDControllers[*paramSource] = ret
	return ret.informer, mapping.Scope, nil
}

// 仅用于测试。
// getParamInformer 读取当前为该 param GVK 启动的 informer，不存在时返回 nil。
func (s *policySource[P, B, E]) getParamInformer(param schema.GroupVersionKind) (informers.GenericInformer, meta.RESTScope) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if info, ok := s.paramsCRDControllers[param]; ok {
		return info.informer, info.mapping.Scope
	}

	return nil, nil
}

// compilePolicyLocked 编译策略并返回其 evaluator；若策略自上次编译以来
// 没有变化，则返回缓存的 evaluator。必须在持写锁时调用。
// 缓存键是 resourceVersion 而非 spec 内容：只要对象被写入过（哪怕内容等价），
// 也会触发重编译，换来实现简单和绝不返回陈旧 evaluator 的保证。
func (s *policySource[P, B, E]) compilePolicyLocked(policySpec P) E {
	policyMeta, err := meta.Accessor(policySpec)
	if err != nil {
		// 只要 P 与 B 含 ObjectMeta 就不该发生，但遗憾的是
		// 目前无法用类型约束表达“可调用 meta.Accessor”。
		utilruntime.HandleError(err)
		var emptyEvaluator E
		return emptyEvaluator
	}
	key := types.NamespacedName{
		Namespace: policyMeta.GetNamespace(),
		Name:      policyMeta.GetName(),
	}

	compiledPolicy, wasCompiled := s.compiledPolicies[key]

	// 如果策略或绑定自上次编译后发生了变化，
	// 且没有配置错误（如 param CRD 缺失），则重新编译。
	if !wasCompiled ||
		compiledPolicy.policyVersion != policyMeta.GetResourceVersion() {

		compiledPolicy = compiledPolicyEntry[E]{
			policyVersion: policyMeta.GetResourceVersion(),
			evaluator:     s.compiler(policySpec),
		}
		s.compiledPolicies[key] = compiledPolicy
	}

	return compiledPolicy.evaluator
}
