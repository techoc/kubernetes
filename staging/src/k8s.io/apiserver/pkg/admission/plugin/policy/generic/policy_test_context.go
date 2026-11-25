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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/featuregate"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/util/compatibility"
)

// Logger 抽象出 *testing.T 与 *testing.B 的公共子集，使同一套测试脚手架
// 既可用于单元测试也可用于基准测试。
type Logger interface {
	Helper()
	Logf(format string, args ...interface{})
}

// PolicyTestContext 是测试 policy 插件所需的全部依赖的集合：
// 已初始化的 Plugin、可直接读取内部状态的 Source，以及承载假对象的各类 tracker。
// 它刻意把策略对象与绑定对象放在同一个 tracker 中，以便用真实的 informer 语义
// 复现“策略先于绑定可见”之类的时序问题。
type PolicyTestContext[P runtime.Object, B runtime.Object, E Evaluator] struct {
	context.Context
	Logger        Logger
	Plugin        *Plugin[PolicyHook[P, B, E]]
	Source        Source[PolicyHook[P, B, E]]
	Start         func() error
	DynamicClient dynamic.Interface

	scheme     *runtime.Scheme
	restMapper *meta.DefaultRESTMapper
	policyGVR  schema.GroupVersionResource
	bindingGVR schema.GroupVersionResource

	policyGVK  schema.GroupVersionKind
	bindingGVK schema.GroupVersionKind

	nativeTracker           clienttesting.ObjectTracker
	policyAndBindingTracker clienttesting.ObjectTracker
	unstructuredTracker     clienttesting.ObjectTracker
}

// NewPolicyTestContext 构造一套完整的测试环境：伪造 GVR/GVK、注册进 scheme 与
// RESTMapper、搭建 informer 与 tracker，然后像真实 apiserver 那样通过 initializer
// 注入客户端并执行 ValidateInitialization。
// 返回的 cancel 函数用于释放 informer goroutine；调用方务必 defer 它。
func NewPolicyTestContext[P, B runtime.Object, E Evaluator](
	logger Logger,
	newPolicyAccessor func(P) PolicyAccessor,
	newBindingAccessor func(B) BindingAccessor,
	compileFunc func(P) E,
	dispatcher dispatcherFactory[PolicyHook[P, B, E]],
	initialObjects []runtime.Object,
	paramMappings []meta.RESTMapping,
) (*PolicyTestContext[P, B, E], func(), error) {
	var Pexample P
	var Bexample B

	// 为给定的策略与绑定类型创建伪造的 resource 与 kind。
	// 使用独立的 example.com 组，避免与真实 API 组冲突；GVR/GVK 只在本测试上下文中有效。
	fakePolicyGVR := schema.GroupVersionResource{
		Group:    "policy.example.com",
		Version:  "v1",
		Resource: "fakepolicies",
	}
	fakeBindingGVR := schema.GroupVersionResource{
		Group:    "policy.example.com",
		Version:  "v1",
		Resource: "fakebindings",
	}
	fakePolicyGVK := fakePolicyGVR.GroupVersion().WithKind("FakePolicy")
	fakeBindingGVK := fakeBindingGVR.GroupVersion().WithKind("FakeBinding")

	// scheme 需同时识别策略/绑定类型、它们的 List 类型，以及参数（param）所引用的类型；
	// 参数类型以 unstructured 注册，因为真实场景下参数是任意 CRD。
	policySourceTestScheme, err := func() (*runtime.Scheme, error) {
		scheme := runtime.NewScheme()

		if err := fake.AddToScheme(scheme); err != nil {
			return nil, err
		}

		scheme.AddKnownTypeWithName(fakePolicyGVK, Pexample)
		scheme.AddKnownTypeWithName(fakeBindingGVK, Bexample)
		scheme.AddKnownTypeWithName(fakePolicyGVK.GroupVersion().WithKind(fakePolicyGVK.Kind+"List"), &FakeList[P]{})
		scheme.AddKnownTypeWithName(fakeBindingGVK.GroupVersion().WithKind(fakeBindingGVK.Kind+"List"), &FakeList[B]{})

		for _, mapping := range paramMappings {
			// 若类型已在 scheme 中则跳过。
			// 参数类型可能已被显式注册，重复注册会 panic。
			if scheme.Recognizes(mapping.GroupVersionKind) {
				continue
			}
			scheme.AddKnownTypeWithName(mapping.GroupVersionKind, &unstructured.Unstructured{})
			scheme.AddKnownTypeWithName(mapping.GroupVersionKind.GroupVersion().WithKind(mapping.GroupVersionKind.Kind+"List"), &unstructured.UnstructuredList{})
		}

		return scheme, nil
	}()
	if err != nil {
		return nil, nil, err
	}

	// RESTMapper 需覆盖插件可能查找的所有类型，包括 ConfigMap（常见参数类型）；
	// AddSpecific 用于保留参数映射中精确的资源复数形式与作用域。
	fakeRestMapper := func() *meta.DefaultRESTMapper {
		res := meta.NewDefaultRESTMapper([]schema.GroupVersion{
			{
				Group:   "",
				Version: "v1",
			},
		})

		res.Add(fakePolicyGVK, meta.RESTScopeRoot)
		res.Add(fakeBindingGVK, meta.RESTScopeRoot)
		res.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)

		for _, mapping := range paramMappings {
			res.AddSpecific(mapping.GroupVersionKind, mapping.Resource, mapping.Resource, mapping.Scope)
		}

		return res
	}()

	nativeClient := fake.NewSimpleClientset()
	dynamicClient := dynamicfake.NewSimpleDynamicClient(policySourceTestScheme)
	fakeInformerFactory := informers.NewSharedInformerFactory(nativeClient, 30*time.Second)

	// 专门为策略与绑定创建一个 object tracker。
	// 策略与绑定共用同一个 tracker，因此二者的写入共享同一条 watch 事件流，
	// 可以复现真实集群中两者的相对顺序。
	policiesAndBindingsTracker := clienttesting.NewObjectTracker(
		policySourceTestScheme,
		serializer.NewCodecFactory(policySourceTestScheme).UniversalDecoder())

	// 为策略与绑定创建一个 informer。
	// 直接用 ListWatch 包装 tracker 而非 fake clientset，是为了让 List/Watch
	// 走与真实 informer 完全相同的 code path（包括 watch-list 语义）。

	policyInformer := cache.NewSharedIndexInformer(
		cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return policiesAndBindingsTracker.List(fakePolicyGVR, fakePolicyGVK, "")
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return policiesAndBindingsTracker.Watch(fakePolicyGVR, "", options)
			},
		}, policiesAndBindingsTracker),
		Pexample,
		30*time.Second,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
	bindingInformer := cache.NewSharedIndexInformer(
		cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return policiesAndBindingsTracker.List(fakeBindingGVR, fakeBindingGVK, "")
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return policiesAndBindingsTracker.Watch(fakeBindingGVR, "", options)
			},
		}, policiesAndBindingsTracker),
		Bexample,
		30*time.Second,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)

	var source Source[PolicyHook[P, B, E]]
	// 与生产路径一致：插件通过 initializer 拿到 informer factory、dynamic client 与 RESTMapper，
	// 再据此构造 PolicySource；这里把 source 指针捕获出来以便测试直接读取编译结果。
	plugin := NewPlugin[PolicyHook[P, B, E]](
		admission.NewHandler(admission.Connect, admission.Create, admission.Delete, admission.Update),
		func(sif informers.SharedInformerFactory, i1 kubernetes.Interface, i2 dynamic.Interface, r meta.RESTMapper) Source[PolicyHook[P, B, E]] {
			source = NewPolicySource[P, B, E](
				policyInformer,
				bindingInformer,
				newPolicyAccessor,
				newBindingAccessor,
				compileFunc,
				sif,
				i2,
				r,
			)
			return source
		}, dispatcher)
	plugin.SetEnabled(true)

	featureGate := featuregate.NewFeatureGate()
	effectiveVersion := compatibility.DefaultBuildEffectiveVersion()
	testContext, testCancel := context.WithCancel(context.Background())
	genericInitializer := initializer.New(
		nativeClient,
		dynamicClient,
		fakeInformerFactory,
		fakeAuthorizer{},
		featureGate,
		effectiveVersion,
		testContext.Done(),
		fakeRestMapper,
	)
	genericInitializer.Initialize(plugin)
	plugin.SetRESTMapper(fakeRestMapper)

	// ValidateInitialization 在真实启动时也会执行；提前失败可以让测试脚手架本身的问题
	// 在构造阶段暴露，而不是等到 Dispatch 时出现难以定位的 nil。
	if err := plugin.ValidateInitialization(); err != nil {
		testCancel()
		return nil, nil, err
	}

	res := &PolicyTestContext[P, B, E]{
		Logger:        logger,
		Context:       testContext,
		Plugin:        plugin,
		Source:        source,
		DynamicClient: dynamicClient,

		restMapper:              fakeRestMapper,
		scheme:                  policySourceTestScheme,
		policyGVK:               fakePolicyGVK,
		bindingGVK:              fakeBindingGVK,
		policyGVR:               fakePolicyGVR,
		bindingGVR:              fakeBindingGVR,
		nativeTracker:           nativeClient.Tracker(),
		policyAndBindingTracker: policiesAndBindingsTracker,
		unstructuredTracker:     dynamicClient.Tracker(),
	}

	for _, obj := range initialObjects {
		err := res.updateOne(obj)
		if err != nil {
			testCancel()
			return nil, nil, err
		}
	}

	res.Start = func() error {
		fakeInformerFactory.Start(res.Done())
		go policyInformer.Run(res.Done())
		go bindingInformer.Run(res.Done())

		// 必须等到 Source 的 informer 同步完成，否则首个 Dispatch 可能看到空的策略集合。
		if !cache.WaitForCacheSync(res.Done(), res.Source.HasSynced) {
			return fmt.Errorf("timed out waiting for initial cache sync")
		}
		return nil
	}
	return res, testCancel, nil
}

// UpdateAndWait 在测试中更新给定对象（不存在则创建），并按对象类型
// 等待其被 policy source 同步。
//
// UpdateAndWait 写入对象后会阻塞等待 policy source 观察到新版本；
// 由于它会改写传入对象的 ResourceVersion，调用方不能依赖原始 RV。
func (p *PolicyTestContext[P, B, E]) UpdateAndWait(objects ...runtime.Object) error {
	return p.update(true, objects...)
}

// Update 在测试中更新给定对象（不存在则创建），但不等待同步。
//
// 注意 Update 会修改传入对象的 ResourceVersion。
//
// Update 只写入不等待，适用于故意构造“source 尚未同步”的中间状态。
func (p *PolicyTestContext[P, B, E]) Update(objects ...runtime.Object) error {
	return p.update(false, objects...)
}

// update 在测试中更新给定对象（不存在则创建），并按对象类型
// 等待其被 policy source 同步。
//
// update 是 Update/UpdateAndWait 的公共实现；超时统一为 5 秒，
// 一旦 source 未在期限内观察到此版本即视为测试失败，避免无限挂起。
func (p *PolicyTestContext[P, B, E]) update(wait bool, objects ...runtime.Object) error {
	for _, object := range objects {
		if err := p.updateOne(object); err != nil {
			return err
		}
	}

	if wait {
		timeoutCtx, timeoutCancel := context.WithTimeout(p, 5*time.Second)
		defer timeoutCancel()

		for _, object := range objects {
			if err := p.WaitForReconcile(timeoutCtx, object); err != nil {
				return fmt.Errorf("error waiting for reconcile of %v: %v", object, err)
			}
		}
	}
	return nil
}

// WaitForReconcile 按对象类型等待其被 policy source 同步。注意未绑定的
// 策略会被跳过，因此不要尝试等待一个没有绑定的策略：应先同时创建绑定与
// 策略，再等待。
//
// WaitForReconcile 按对象类型分三种等待方式：
// 策略/绑定等待其出现在 Hooks() 中，参数则等待 param informer 观察到相同 RV。
// 注意 Source 尚未同步时直接返回 nil，避免在 Start 之前调用时误判。
func (p *PolicyTestContext[P, B, E]) WaitForReconcile(timeoutCtx context.Context, object runtime.Object) error {
	if !p.Source.HasSynced() {
		return nil
	}

	objectMeta, err := meta.Accessor(object)
	if err != nil {
		return err
	}

	objectGVK, _, err := p.inferGVK(object)
	if err != nil {
		return err
	}

	switch objectGVK {
	case p.policyGVK:
		// 未绑定的策略不会进入 Hooks()，因此只对已绑定的策略轮询。
		return wait.PollUntilContextCancel(timeoutCtx, 100*time.Millisecond, true, func(ctx context.Context) (done bool, err error) {
			policies := p.Source.Hooks()
			for _, policy := range policies {
				policyMeta, err := meta.Accessor(policy.Policy)
				if err != nil {
					return true, err
				} else if policyMeta.GetName() == objectMeta.GetName() && policyMeta.GetResourceVersion() == objectMeta.GetResourceVersion() {
					return true, nil
				}
			}
			return false, nil
		})
	case p.bindingGVK:
		return wait.PollUntilContextCancel(timeoutCtx, 100*time.Millisecond, true, func(ctx context.Context) (done bool, err error) {
			policies := p.Source.Hooks()
			for _, policy := range policies {
				for _, binding := range policy.Bindings {
					bindingMeta, err := meta.Accessor(binding)
					if err != nil {
						return true, err
					} else if bindingMeta.GetName() == objectMeta.GetName() && bindingMeta.GetResourceVersion() == objectMeta.GetResourceVersion() {
						return true, nil
					}
				}
			}
			return false, nil
		})

	default:
		// 无需额外同步：参数写入后立即可见。
		// 循环等待直到能通过 param informer 的 get 观察到某个参数。
		// 参数类型没有专门索引，只能通过按需创建的 param informer 观察；
		// informer 可能尚未创建，因此轮询中要容忍 nil。
		return wait.PollUntilContextCancel(timeoutCtx, 100*time.Millisecond, true, func(ctx context.Context) (done bool, err error) {
			informer, scope := p.Source.(*policySource[P, B, E]).getParamInformer(objectGVK)
			if informer == nil {
				// informer 尚未创建，继续等待同步
				return false, nil
			}

			if !cache.WaitForCacheSync(timeoutCtx.Done(), informer.Informer().HasSynced) {
				return false, fmt.Errorf("timed out waiting for cache sync of param informer")
			}

			var lister cache.GenericNamespaceLister = informer.Lister()
			if scope == meta.RESTScopeNamespace {
				lister = informer.Lister().ByNamespace(objectMeta.GetNamespace())
			}

			fetched, err := lister.Get(objectMeta.GetName())
			if err != nil {
				if errors.IsNotFound(err) {
					return false, nil
				}
				return true, err
			}

			// 确保 RV 一致
			// 只比对名称不足以确认是最新写入，必须同时校验 RV。
			fetchedMeta, err := meta.Accessor(fetched)
			if err != nil {
				return true, err
			} else if fetchedMeta.GetResourceVersion() != objectMeta.GetResourceVersion() {
				return false, nil
			}

			return true, nil
		})
	}
}

// waitForDelete 轮询直到对象从 policy source（策略/绑定）或 param informer（参数）中消失。
// 对参数类型，informer 不存在意味着从未观察到该类型，可视为已删除。
func (p *PolicyTestContext[P, B, E]) waitForDelete(ctx context.Context, objectGVK schema.GroupVersionKind, name types.NamespacedName) error {
	srce := p.Source.(*policySource[P, B, E])

	return wait.PollUntilContextCancel(ctx, 50*time.Millisecond, true, func(ctx context.Context) (done bool, err error) {
		switch objectGVK {
		case p.policyGVK:
			for _, hook := range p.Source.Hooks() {
				accessor := srce.newPolicyAccessor(hook.Policy)
				if accessor.GetName() == name.Name && accessor.GetNamespace() == name.Namespace {
					p.Logger.Logf("waiting for %s at %v", name.Name, time.Now())
					return false, nil
				}
			}

			return true, nil
		case p.bindingGVK:
			for _, hook := range p.Source.Hooks() {
				for _, binding := range hook.Bindings {
					accessor := srce.newBindingAccessor(binding)
					if accessor.GetName() == name.Name && accessor.GetNamespace() == name.Namespace {
						p.Logger.Logf("waiting for %s at %v", name.Name, time.Now())
						return false, nil
					}
				}
			}
			return true, nil
		default:
			// 无需额外同步：参数写入后立即可见。
			// 循环等待直到该参数从 param informer 中消失。
			// informer 为 nil 说明从未观察到该类型，删除条件已满足。
			informer, scope := p.Source.(*policySource[P, B, E]).getParamInformer(objectGVK)
			if informer == nil {
				return true, nil
			}

			var lister cache.GenericNamespaceLister = informer.Lister()
			if scope == meta.RESTScopeNamespace {
				lister = informer.Lister().ByNamespace(name.Namespace)
			}

			obj, err := lister.Get(name.Name)
			if err != nil {
				if errors.IsNotFound(err) {
					return true, nil
				}
				p.Logger.Logf("error on %s: %v", name.Name, err)
				return false, err
			}
			p.Logger.Logf("waiting for %s to be gone at %v, got %#v", name.Name, time.Now(), obj)
			return false, nil
		}
	})
}

// updateOne 按对象类型写入对应 tracker，并在已存在时退化为 Update。
// 每次写入都重新生成 RV，使测试能够区分“旧版本已同步”与“新版本已同步”。
func (p *PolicyTestContext[P, B, E]) updateOne(object runtime.Object) error {
	objectMeta, err := meta.Accessor(object)
	if err != nil {
		return err
	}
	objectMeta.SetResourceVersion(string(uuid.NewUUID()))
	objectGVK, gvr, err := p.inferGVK(object)
	if err != nil {
		return err
	}

	switch objectGVK {
	case p.policyGVK:
		// tracker 没有 upsert，先试 Update，NotFound 时再 Create。
		err := p.policyAndBindingTracker.Update(p.policyGVR, object, objectMeta.GetNamespace())
		if errors.IsNotFound(err) {
			err = p.policyAndBindingTracker.Create(p.policyGVR, object, objectMeta.GetNamespace())
		}

		return err
	case p.bindingGVK:
		err := p.policyAndBindingTracker.Update(p.bindingGVR, object, objectMeta.GetNamespace())
		if errors.IsNotFound(err) {
			err = p.policyAndBindingTracker.Create(p.bindingGVR, object, objectMeta.GetNamespace())
		}

		return err
	default:
		// 参数对象走 dynamic client：unstructured 与 typed 分别落在不同 tracker，
		// 与真实客户端的分层保持一致。
		if _, ok := object.(*unstructured.Unstructured); ok {
			if err := p.unstructuredTracker.Create(gvr, object, objectMeta.GetNamespace()); err != nil {
				if errors.IsAlreadyExists(err) {
					return p.unstructuredTracker.Update(gvr, object, objectMeta.GetNamespace())
				}
				return err
			}
			return nil
		} else if err := p.nativeTracker.Create(gvr, object, objectMeta.GetNamespace()); err != nil {
			if errors.IsAlreadyExists(err) {
				return p.nativeTracker.Update(gvr, object, objectMeta.GetNamespace())
			}
		}
		return nil
	}
}

// DeleteAndWait 删除给定对象，并按对象类型等待其从 policy source 中消失。
//
// DeleteAndWait 删除对象并等待其从 source 中消失；删除不存在的对象被容忍，
// 以便测试可以写成幂等的清理逻辑。
func (p *PolicyTestContext[P, B, E]) DeleteAndWait(object ...runtime.Object) error {
	for _, object := range object {
		if err := p.deleteOne(object); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	timeoutCtx, timeoutCancel := context.WithTimeout(p, 5*time.Second)
	defer timeoutCancel()

	start := time.Now()
	for _, object := range object {
		accessor, err := meta.Accessor(object)
		if err != nil {
			return err
		}

		objectGVK, _, err := p.inferGVK(object)
		if err != nil {
			return err
		}

		if err := p.waitForDelete(
			timeoutCtx,
			objectGVK,
			types.NamespacedName{Name: accessor.GetName(), Namespace: accessor.GetNamespace()}); err != nil {
			return err
		}
		p.Logger.Logf("after wait on %s: %v", accessor.GetName(), time.Since(start))
	}
	return nil
}

// deleteOne 按对象类型从对应 tracker 删除；策略/绑定与参数走不同的存储，
// 因此不能统一用 Get 推断归属。
func (p *PolicyTestContext[P, B, E]) deleteOne(object runtime.Object) error {
	objectMeta, err := meta.Accessor(object)
	if err != nil {
		return err
	}
	objectMeta.SetResourceVersion(string(uuid.NewUUID()))
	objectGVK, gvr, err := p.inferGVK(object)
	if err != nil {
		return err
	}

	switch objectGVK {
	case p.policyGVK:
		return p.policyAndBindingTracker.Delete(p.policyGVR, objectMeta.GetNamespace(), objectMeta.GetName())
	case p.bindingGVK:
		return p.policyAndBindingTracker.Delete(p.bindingGVR, objectMeta.GetNamespace(), objectMeta.GetName())
	default:
		if _, ok := object.(*unstructured.Unstructured); ok {
			return p.unstructuredTracker.Delete(gvr, objectMeta.GetNamespace(), objectMeta.GetName())
		}
		return p.nativeTracker.Delete(gvr, objectMeta.GetNamespace(), objectMeta.GetName())
	}
}

// Dispatch 构造 AdmissionAttributes 并交给插件处理，模拟真实请求路径。
// new 与 old 至少有一个非 nil：Create 只有 new，Delete 只有 old。
// 对象上的 GVK 优先使用其自带值，缺失时由 inferGVK 从 scheme 推断。
func (p *PolicyTestContext[P, B, E]) Dispatch(
	new, old runtime.Object,
	operation admission.Operation,
) error {
	if old == nil && new == nil {
		return fmt.Errorf("both old and new objects cannot be nil")
	}

	nonNilObject := new
	if nonNilObject == nil {
		nonNilObject = old
	}

	gvk, gvr, err := p.inferGVK(nonNilObject)
	if err != nil {
		return err
	}

	nonNilMeta, err := meta.Accessor(nonNilObject)
	if err != nil {
		return err
	}

	return p.Plugin.Dispatch(
		p,
		admission.NewAttributesRecord(
			new,
			old,
			gvk,
			nonNilMeta.GetName(),
			nonNilMeta.GetNamespace(),
			gvr,
			"",
			operation,
			nil,
			false,
			nil,
		), admission.NewObjectInterfacesFromScheme(p.scheme))
}

// inferGVK 推断对象的 GVK 与 GVR。若对象自带 GVK 则直接使用；
// 否则从 scheme 中挑选，并优先选择伪造的 policy/binding 类型（同一 Go 类型可能注册多个 GVK）。
// 最后经 RESTMapper 校验，保证返回的 GVR 是插件真正能解析的资源。
func (p *PolicyTestContext[P, B, E]) inferGVK(object runtime.Object) (schema.GroupVersionKind, schema.GroupVersionResource, error) {
	objectGVK := object.GetObjectKind().GroupVersionKind()
	if objectGVK.Empty() {
		// 若对象没有 GVK，则向 scheme 询问首选 GVK
		knownKinds, _, err := p.scheme.ObjectKinds(object)
		if err != nil {
			return schema.GroupVersionKind{}, schema.GroupVersionResource{}, err
		} else if len(knownKinds) == 0 {
			return schema.GroupVersionKind{}, schema.GroupVersionResource{}, fmt.Errorf("no known GVKs for object in schema: %T", object)
		}
		toTake := 0

		// 若是伪造的策略/绑定类型，则优先选择该 GVK
		for i, knownKind := range knownKinds {
			if knownKind == p.policyGVK || knownKind == p.bindingGVK {
				toTake = i
				break
			}
		}

		objectGVK = knownKinds[toTake]
	}

	// 确保伪造的 rest mapper 认识该 GVK，避免出现隐晦错误。
	// 提前校验可以把“类型未注册”转成清晰错误，而不是后续 dispatch 中的隐晦失败。
	mapping, err := p.restMapper.RESTMapping(objectGVK.GroupKind(), objectGVK.Version)
	if err != nil {
		return schema.GroupVersionKind{}, schema.GroupVersionResource{}, err
	}
	return objectGVK, mapping.Resource, nil
}

// FakeList 是策略/绑定类型的 List 包装器；informer 的 List 路径要求 scheme 中存在
// 对应的 List 类型，否则无法解码。
type FakeList[T runtime.Object] struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []T
}

// DeepCopyObject 逐项深拷贝，避免 informer 缓存与测试持有同一份对象。
func (fl *FakeList[P]) DeepCopyObject() runtime.Object {
	copiedItems := make([]P, len(fl.Items))
	for i, item := range fl.Items {
		copiedItems[i] = item.DeepCopyObject().(P)
	}
	return &FakeList[P]{
		TypeMeta: fl.TypeMeta,
		ListMeta: fl.ListMeta,
		Items:    copiedItems,
	}
}

// fakeAuthorizer 一律放行，让测试聚焦于策略逻辑而非授权。
type fakeAuthorizer struct{}

// Authorize 始终返回 Allow。
func (f fakeAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	return authorizer.DecisionAllow, "", nil
}

// ConditionsAwareAuthorize 不感知条件，直接把 Authorize 的决策转换后返回。
func (f fakeAuthorizer) ConditionsAwareAuthorize(ctx context.Context, a authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(f.Authorize(ctx, a))
}

// EvaluateConditions 不被该 authorizer 支持。
func (fakeAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}
