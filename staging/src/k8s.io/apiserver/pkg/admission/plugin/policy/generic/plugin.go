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
	"errors"
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

// H 是 source 生成、dispatcher 消费的 Hook 类型。
// !TODO: 直接传入带有全部信息访问器的 Plugin[H]。
// 这两个工厂函数把“从 informer 构造策略源”和“构造分发器”解耦，
// 使 Plugin 保持泛型、不依赖 VAP/MAP 的具体类型。
type apiSourceFactory[H any] func(informers.SharedInformerFactory, kubernetes.Interface, dynamic.Interface, meta.RESTMapper) Source[H]
type dispatcherFactory[H any] func(authorizer.UnconditionalAuthorizer, *matching.Matcher, kubernetes.Interface) Dispatcher[H]

// ReloadableSource 在 HookSource 之上增加了运行重载循环的方法：
// 监听配置变化并阻塞直到 context 取消。
// 静态（清单文件）策略源需要感知文件变化，因此比 HookSource 多一个阻塞式重载循环。
type ReloadableSource[H any] interface {
	HookSource[H]
	// RunReloadLoop 监听配置变化并在检测到时重载；阻塞直到 ctx 取消。
	RunReloadLoop(ctx context.Context)
}

// StaticSourceFactory 从清单目录创建静态策略源；返回的 Source 应当已经调用过
// LoadInitial()。约定返回前已完成首次加载，这样调用方在启动分发器前不必再处理加载错误。
// 约定返回前已完成首次加载，这样调用方在启动分发器前不必再处理加载错误。
type StaticSourceFactory[H any] func(manifestsDir string) (ReloadableSource[H], error)

// admissionResources 是与基于 CEL 的准入特性相关的资源列表。
// 这些资源的写入会改变策略本身，若让 API 策略再拦截它们会形成自引用死锁。
var admissionResources = []schema.GroupResource{
	{Group: admissionregistrationv1.GroupName, Resource: "validatingadmissionpolicies"},
	{Group: admissionregistrationv1.GroupName, Resource: "validatingadmissionpolicybindings"},
	{Group: admissionregistrationv1.GroupName, Resource: "mutatingadmissionpolicies"},
	{Group: admissionregistrationv1.GroupName, Resource: "mutatingadmissionpolicybindings"},
}

// AdmissionPolicyManager（Plugin）是抽象的准入插件，提供了在其上定义
// Admit 或 Validate 所需的全部基础设施。
// Plugin 把策略的“来源”（API 对象或静态清单）与“执行”（dispatcher）
// 统一起来，具体策略类型只需嵌入并实现 Admit/Validate。字段均在 Initialize 之前
// 由 initializer 注入，之后只读，因此无需额外加锁。
type Plugin[H any] struct {
	*admission.Handler

	apiSourceFactory    apiSourceFactory[H]
	dispatcherFactory   dispatcherFactory[H]
	staticSourceFactory StaticSourceFactory[H]

	source     Source[H]
	dispatcher Dispatcher[H]
	matcher    *matching.Matcher

	// staticSource 只持有静态（基于清单）策略源的引用。Dispatch 会用它为准入配置
	// 资源（如 VAP/MAP/VAPB/MAPB）运行静态 hook，使静态策略能够保护这些资源，
	// 而不存在 API 策略的循环依赖问题。
	//API 策略不能拦截策略资源本身，但清单策略是只读的、不经过 API 写入，
	// 因此可以安全地保护这些资源。
	staticSource HookSource[H]

	// staticManifestsDir 是存放静态策略清单的目录路径；设置且特性门控开启时，
	// 策略也会从该目录加载。
	staticManifestsDir string

	// apiServerID 是本 API server 实例的身份标识，用于指标打标。
	apiServerID string

	informerFactory informers.SharedInformerFactory
	client          kubernetes.Interface
	restMapper      meta.RESTMapper
	dynamicClient   dynamic.Interface
	// admissionConfigResources 是准入配置类资源（VAP/MAP/VAPB/MAPB）：
	// API 策略会跳过它们以避免循环依赖，但静态策略可以安全地对其求值。
	admissionConfigResources sets.Set[schema.GroupResource]
	// excludedResources 是非持久化资源（auth/authz review），必须从所有策略
	// 求值中排除，包括静态策略。
	//认证/鉴权 review 本身不走持久化存储，拦截它们既无意义又可能导致
	// 鉴权链路递归。
	excludedResources sets.Set[schema.GroupResource]
	stopCh            <-chan struct{}
	authorizer        authorizer.UnconditionalAuthorizer
	enabled           bool
}

// 编译期断言：Plugin 必须实现 initializer 定义的所有注入接口。
var (
	_ initializer.WantsExternalKubeInformerFactory = &Plugin[any]{}
	_ initializer.WantsExternalKubeClientSet       = &Plugin[any]{}
	_ initializer.WantsRESTMapper                  = &Plugin[any]{}
	_ initializer.WantsDynamicClient               = &Plugin[any]{}
	_ initializer.WantsDrainedNotification         = &Plugin[any]{}
	_ initializer.WantsAPIServerID                 = &Plugin[any]{}
	_ initializer.WantsUnconditionalAuthorizer     = &Plugin[any]{}
	_ initializer.WantsExcludedAdmissionResources  = &Plugin[any]{}
	_ admission.InitializationValidator            = &Plugin[any]{}
)

// NewPlugin 构造插件骨架；enabled 默认为 false，由嵌入方根据特性开关决定是否启用。
func NewPlugin[H any](
	handler *admission.Handler,
	apiSourceFactory apiSourceFactory[H],
	dispatcherFactory dispatcherFactory[H],
) *Plugin[H] {
	return &Plugin[H]{
		Handler:           handler,
		apiSourceFactory:  apiSourceFactory,
		dispatcherFactory: dispatcherFactory,

		// 准入配置资源被 API 策略跳过（避免循环依赖），但由静态策略处理。
		admissionConfigResources: sets.New(admissionResources...),
		excludedResources:        sets.New[schema.GroupResource](),
	}
}

// 以下 Set* 方法实现 initializer 的注入接口，仅在 Initialize 阶段被调用一次。
func (c *Plugin[H]) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	c.informerFactory = f
}

func (c *Plugin[H]) SetExternalKubeClientSet(client kubernetes.Interface) {
	c.client = client
}

func (c *Plugin[H]) SetRESTMapper(mapper meta.RESTMapper) {
	c.restMapper = mapper
}

func (c *Plugin[H]) SetDynamicClient(client dynamic.Interface) {
	c.dynamicClient = client
}

func (c *Plugin[H]) SetDrainedNotification(stopCh <-chan struct{}) {
	c.stopCh = stopCh
}

func (c *Plugin[H]) SetUnconditionalAuthorizer(authorizer authorizer.UnconditionalAuthorizer) {
	c.authorizer = authorizer
}

func (c *Plugin[H]) SetMatcher(matcher *matching.Matcher) {
	c.matcher = matcher
}

func (c *Plugin[H]) SetEnabled(enabled bool) {
	c.enabled = enabled
}

func (c *Plugin[H]) SetExcludedAdmissionResources(excludedResources []schema.GroupResource) {
	c.excludedResources.Insert(excludedResources...)
}

// SetStaticManifestsDir 设置存放静态策略清单的目录。
func (c *Plugin[H]) SetStaticManifestsDir(dir string) {
	c.staticManifestsDir = dir
}

// GetStaticManifestsDir 返回存放静态策略清单的目录。
func (c *Plugin[H]) GetStaticManifestsDir() string {
	return c.staticManifestsDir
}

// SetStaticSourceFactory 设置创建静态策略源的工厂；
// 应在 ValidateInitialization 之前调用。
func (c *Plugin[H]) SetStaticSourceFactory(factory StaticSourceFactory[H]) {
	c.staticSourceFactory = factory
}

// SetAPIServerID 实现 WantsAPIServerID 接口。
// API server ID 用于指标打标，必须在 ValidateInitialization 之前设置。
func (c *Plugin[H]) SetAPIServerID(id string) {
	c.apiServerID = id
}

// GetAPIServerID 返回已保存的 API server ID。
func (c *Plugin[H]) GetAPIServerID() string {
	return c.apiServerID
}

// ValidateInitialization 在提供 clientset 与 informer factory 后创建并启动准入控制器。
//
// 本方法启动所有后台 goroutine（策略源、文件监听、分发器），生命周期统一
// 挂在 pluginContext 上，随 stopCh 关闭而取消。source 与 dispatcher 只在为 nil 时才创建，
// 便于测试预先注入替身。
func (c *Plugin[H]) ValidateInitialization() error {
	// enabled 默认为 false，是否置为 true（特性开关开启或其他条件）
	// 由嵌入本结构体的类型决定。
	if !c.enabled {
		return nil
	}
	// 启用后缺少任何依赖都属于装配错误，宁可启动失败也不要静默跳过策略。
	if c.Handler == nil {
		return errors.New("missing handler")
	}
	if c.informerFactory == nil {
		return errors.New("missing informer factory")
	}
	if c.client == nil {
		return errors.New("missing kubernetes client")
	}
	if c.restMapper == nil {
		return errors.New("missing rest mapper")
	}
	if c.dynamicClient == nil {
		return errors.New("missing dynamic client")
	}
	if c.stopCh == nil {
		return errors.New("missing stop channel")
	}
	if c.authorizer == nil {
		return errors.New("missing authorizer")
	}

	//配置了目录却不开启开关通常意味着配置写错了，直接报错比静默忽略更安全。
	if len(c.staticManifestsDir) > 0 && !utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		return fmt.Errorf("static policy manifests dir %q configured but %s feature gate is not enabled", c.staticManifestsDir, features.ManifestBasedAdmissionControlConfig)
	}

	namespaceInformer := c.informerFactory.Core().V1().Namespaces()

	// 只构造一次 matcher，已设置时不覆盖。
	//matcher 依赖 namespace lister 解析 namespaceSelector，必须在 informer 就绪后创建。
	if c.matcher == nil {
		c.matcher = matching.NewMatcher(namespaceInformer.Lister(), c.client)
	}
	if err := c.matcher.ValidateInitialization(); err != nil {
		return err
	}

	// 创建一个与 server 退出绑定的共享 context，供所有后台 goroutine 使用。
	//用一个 goroutine 把 stopCh 转换为 context，后续所有组件共享同一取消信号。
	pluginContext, pluginContextCancel := context.WithCancel(context.Background())
	go func() {
		defer pluginContextCancel()
		<-c.stopCh
	}()

	// 只构造一次 source，已设置时不覆盖。
	if c.source == nil {
		apiSource := c.apiSourceFactory(c.informerFactory, c.client, c.dynamicClient, c.restMapper)

		if len(c.staticManifestsDir) > 0 {
			if c.staticSourceFactory == nil {
				return fmt.Errorf("static policy manifests configured in %q but no static source factory is set", c.staticManifestsDir)
			}
			staticSource, err := c.staticSourceFactory(c.staticManifestsDir)
			if err != nil {
				return fmt.Errorf("failed to load static policy manifests from %q: %w", c.staticManifestsDir, err)
			}
			// 使用组合静态来源与 API 来源的复合源。
			c.source = NewCompositePolicySource(staticSource, apiSource)
			// 保留静态源的引用，使 Dispatch 能为通常被排除在策略求值之外的
			// 资源运行静态 hook。
			c.staticSource = staticSource
			// 在后台 goroutine 中启动文件监听。
			go staticSource.RunReloadLoop(pluginContext)
		} else {
			// 只使用基于 API 的来源。
			c.source = apiSource
		}

		//源在独立 goroutine 中运行；正常取消不算错误，其余错误上报给 runtime。
		go func() {
			err := c.source.Run(pluginContext)
			if err != nil && !errors.Is(err, context.Canceled) {
				utilruntime.HandleError(fmt.Errorf("policy source context unexpectedly closed: %w", err))
			}
		}()

		// 就绪条件同时包含 namespace 缓存与策略源，缺一不可，否则匹配可能基于不完整数据。
		c.SetReadyFunc(func() bool {
			return namespaceInformer.Informer().HasSynced() && c.source.HasSynced()
		})
	}

	// 只构造一次 dispatcher，已设置时不覆盖。
	if c.dispatcher == nil {
		c.dispatcher = c.dispatcherFactory(c.authorizer, c.matcher, c.client)

		err := c.dispatcher.Start(pluginContext)
		if err != nil && !errors.Is(err, context.Canceled) {
			utilruntime.HandleError(fmt.Errorf("policy dispatcher context unexpectedly closed: %w", err))
		}
	}

	return nil
}

// Dispatch 是策略执行的统一入口，按资源类型决定走哪条执行路径。
func (c *Plugin[H]) Dispatch(
	ctx context.Context,
	a admission.Attributes,
	o admission.ObjectInterfaces,
) (err error) {
	if !c.enabled {
		return nil
	}

	gr := a.GetResource().GroupResource()

	if c.isExcludedFromAllHooks(gr) {
		// 非持久化资源（auth/authz review）被排除在所有策略求值之外，避免破坏集群。
		return nil
	} else if c.isExcludedFromAPIHooks(gr) {
		// 准入配置资源（VAP/MAP/VAPB/MAPB）被 API 策略排除以避免循环依赖；
		// 但静态（基于清单）策略处理它们是安全的，不存在自我引用问题。
		if c.staticSource != nil {
			// 静态源未同步时无法判断策略，按 fail-closed 拒绝请求。
			if !c.staticSource.HasSynced() {
				return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
			}
			return c.dispatcher.Dispatch(ctx, a, o, c.staticSource.Hooks())
		}
		return nil
	}

	// 常规资源：等待缓存同步，超时或未就绪同样 fail-closed。
	if !c.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}

	return c.dispatcher.Dispatch(ctx, a, o, c.source.Hooks())
}

// isExcludedFromAllHooks 对不得被任何策略 hook 拦截的非持久化资源
// （auth/authz review）返回 true。
func (c *Plugin[H]) isExcludedFromAllHooks(gr schema.GroupResource) bool {
	return c.excludedResources.Has(gr)
}

// isExcludedFromAPIHooks 对准入配置类资源（VAP/MAP/VAPB/MAPB）返回 true：
// 它们被 API 来源的 hook 排除，但可由静态 hook 求值。
func (c *Plugin[H]) isExcludedFromAPIHooks(gr schema.GroupResource) bool {
	return c.admissionConfigResources.Has(gr)
}
