/*
Copyright 2018 The Kubernetes Authors.

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
	"io"

	"k8s.io/apiserver/pkg/cel/environment"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	admissionv1 "k8s.io/api/admission/v1"
	admissionv1beta1 "k8s.io/api/admission/v1beta1"
	v1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninit "k8s.io/apiserver/pkg/admission/initializer"
	admissionmetrics "k8s.io/apiserver/pkg/admission/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/config"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/namespace"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/object"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/rules"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/features"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-base/featuregate"
)

// Webhook 是一个抽象准入插件，提供了在其上定义 Admit 或 Validate 的全部基础设施。
// 它把“配置来源、匹配、调用”这些与具体插件无关的部分集中起来，
// Mutating/Validating 插件只需实现差异化的调度逻辑。
type Webhook struct {
	*admission.Handler

	// 创建 webhook 来源的工厂。
	apiSourceInformers  informers.SharedInformerFactory
	apiSourceFactory    sourceFactory
	staticSourceFactory StaticSourceFactory

	// 通过准入配置文件与 initializer 提供的配置。
	staticManifestsDir string // 静态 webhook 清单目录路径（可选）
	apiServerID        string // 本 API server 实例的身份标识，用于指标

	// hookSource 是准入时使用的 webhook 来源。配置了 staticManifestsDir 时，
	// 它是把 staticSource（从磁盘加载）与 apiSource（来自 API）组合起来的
	// 复合来源；否则直接指向 apiSource。
	hookSource Source
	// apiSource 从 Kubernetes API（基于 informer）提供 webhook 配置。
	apiSource Source
	// staticSource 只持有静态（基于清单）webhook 来源的引用，
	// 可用于把被排除资源的请求只路由给静态 hook。
	staticSource ReloadableSource

	// 准入期依赖。
	namespaceInformer coreinformers.NamespaceInformer
	clientManager     *webhookutil.ClientManager
	namespaceMatcher  *namespace.Matcher
	objectMatcher     *object.Matcher
	dispatcher        Dispatcher
	filterCompiler    cel.ConditionCompiler
	authorizer        authorizer.UnconditionalAuthorizer

	// 生命周期。
	stopCh <-chan struct{}

	// excludedAdmissionResources 是开启 excludeVirtualResources 时准入 webhook
	// 不得拦截的虚拟资源（auth/authz review），其内容由
	// SetExcludedAdmissionResources 从权威排除列表填充。
	excludedAdmissionResources sets.Set[schema.GroupResource]
	// excludeVirtualResources 缓存 ExcludeAdmissionWebhookVirtualResources 特性
	// 是否启用，由 InspectFeatureGates 一次性设置，避免每请求查询门控。
	excludeVirtualResources bool
}

var (
	_ genericadmissioninit.WantsExternalKubeClientSet      = &Webhook{}
	_ genericadmissioninit.WantsDrainedNotification        = &Webhook{}
	_ genericadmissioninit.WantsAPIServerID                = &Webhook{}
	_ genericadmissioninit.WantsExcludedAdmissionResources = &Webhook{}
	_ genericadmissioninit.WantsFeatures                   = &Webhook{}
	_ admission.Interface                                  = &Webhook{}
)

type sourceFactory func(f informers.SharedInformerFactory, excludedWebhookResources sets.Set[schema.GroupResource]) Source
type dispatcherFactory func(cm *webhookutil.ClientManager) Dispatcher

// ReloadableSource 在 Source 之上增加一个运行重载循环的方法：
// 监听配置变化并阻塞直到 context 取消。
type ReloadableSource interface {
	Source
	// RunReloadLoop 监听配置变化并在检测到时重载；阻塞直到 ctx 取消。
	RunReloadLoop(ctx context.Context)
}

// StaticSourceFactory 从清单目录创建静态 webhook 来源；
// 返回的 Source 应当已经调用过 LoadInitial()。
type StaticSourceFactory func(manifestsDir string) (ReloadableSource, error)

// NewWebhook 构造一个通用的准入 webhook。
// 客户端管理器同时注册 v1beta1 与 v1：webhook 服务端可能只支持旧版本，
// 由 ClientManager 在请求时协商。默认 resolver 可被后续 initializer 覆盖。
func NewWebhook(handler *admission.Handler, configFile io.Reader, sourceFactory sourceFactory, dispatcherFactory dispatcherFactory) (*Webhook, error) {
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		return nil, err
	}

	cm, err := webhookutil.NewClientManager(
		[]schema.GroupVersion{
			admissionv1beta1.SchemeGroupVersion,
			admissionv1.SchemeGroupVersion,
		},
		admissionv1beta1.AddToScheme,
		admissionv1.AddToScheme,
	)
	if err != nil {
		return nil, err
	}
	authInfoResolver, err := webhookutil.NewDefaultAuthenticationInfoResolver(cfg.KubeConfigFile)
	if err != nil {
		return nil, err
	}
	// 设置默认值，后续可能被覆盖。
	cm.SetAuthenticationInfoResolver(authInfoResolver)
	cm.SetServiceResolver(webhookutil.NewDefaultServiceResolver())

	return &Webhook{
		Handler:                    handler,
		apiSourceFactory:           sourceFactory,
		staticManifestsDir:         cfg.StaticManifestsDir,
		clientManager:              &cm,
		namespaceMatcher:           &namespace.Matcher{},
		objectMatcher:              &object.Matcher{},
		dispatcher:                 dispatcherFactory(&cm),
		filterCompiler:             cel.NewConditionCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion())),
		excludedAdmissionResources: sets.New[schema.GroupResource](),
	}, nil
}

// SetAuthenticationInfoResolverWrapper 设置 AuthenticationInfoResolverWrapper。
// TODO: 找到更好的装配方式，暂时先保持本改动最小。
func (a *Webhook) SetAuthenticationInfoResolverWrapper(wrapper webhookutil.AuthenticationInfoResolverWrapper) {
	a.clientManager.SetAuthenticationInfoResolverWrapper(wrapper)
}

// SetServiceResolver 为 webhook 准入插件设置 service resolver；
// 传入 nil 不生效，将使用默认 resolver。
func (a *Webhook) SetServiceResolver(sr webhookutil.ServiceResolver) {
	a.clientManager.SetServiceResolver(sr)
}

// SetStaticSourceFactory 设置创建静态 webhook 来源的工厂；
// 应在 SetExternalKubeInformerFactory 之前调用。
func (a *Webhook) SetStaticSourceFactory(factory StaticSourceFactory) {
	a.staticSourceFactory = factory
}

// SetAPIServerID 实现 WantsAPIServerID 接口。
// API server ID 用于指标打标，必须在 SetExternalKubeInformerFactory
// 之前设置。
func (a *Webhook) SetAPIServerID(id string) {
	a.apiServerID = id
}

// GetAPIServerID 返回已保存的 API server ID。
func (a *Webhook) GetAPIServerID() string {
	return a.apiServerID
}

// SetDrainedNotification 实现 WantsDrainedNotification 接口。
func (a *Webhook) SetDrainedNotification(stopCh <-chan struct{}) {
	a.stopCh = stopCh
}

// SetExcludedAdmissionResources 实现 WantsExcludedAdmissionResources 接口。
func (a *Webhook) SetExcludedAdmissionResources(excludedResources []schema.GroupResource) {
	a.excludedAdmissionResources.Insert(excludedResources...)
}

// InspectFeatureGates 实现 WantsFeatures 接口：缓存
// ExcludeAdmissionWebhookVirtualResources 特性是否启用，
// 使 Dispatch 无需每个请求都查询 feature gate。
func (a *Webhook) InspectFeatureGates(featureGates featuregate.FeatureGate) {
	a.excludeVirtualResources = featureGates.Enabled(features.ExcludeAdmissionWebhookVirtualResources)
}

// SetExternalKubeClientSet 实现 WantsExternalKubeClientSet 接口，
// 为需要它的准入插件设置外部 ClientSet。
func (a *Webhook) SetExternalKubeClientSet(client clientset.Interface) {
	a.namespaceMatcher.Client = client
}

// SetExternalKubeInformerFactory 实现 WantsExternalKubeInformerFactory 接口。
func (a *Webhook) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	namespaceInformer := f.Core().V1().Namespaces()
	a.namespaceMatcher.NamespaceLister = namespaceInformer.Lister()
	a.namespaceInformer = namespaceInformer

	// 暂存 informer factory，供 ValidateInitialization 中构造 apiSource 使用
	// （此时全部 initializer 已运行完，排除资源集合已就绪）。重置 apiSource，
	// 使得重新设置 informer factory 时会基于新工厂重建。
	a.apiSourceInformers = f
	a.apiSource = nil
}

// SetUnconditionalAuthorizer 注入 match condition 求值所需的授权器。
func (a *Webhook) SetUnconditionalAuthorizer(authorizer authorizer.UnconditionalAuthorizer) {
	a.authorizer = authorizer
}

// ValidateInitialization 实现 InitializationValidator 接口。
// API 来源与静态来源在这里创建（全部 initializer 运行之后），
// 因为 initializer 的 setter 调用顺序没有保证。
func (a *Webhook) ValidateInitialization() error {
	// 如需要则构造 apiSource。
	if a.apiSource == nil {
		if a.apiSourceInformers == nil {
			return fmt.Errorf("kubernetes client is not properly setup")
		}
		// 只有启用虚拟资源排除特性时才传入排除集合，否则传 nil 让来源不做额外过滤。
		if a.excludeVirtualResources {
			a.apiSource = a.apiSourceFactory(a.apiSourceInformers, a.excludedAdmissionResources)
		} else {
			a.apiSource = a.apiSourceFactory(a.apiSourceInformers, nil)
		}
	}

	if err := a.namespaceMatcher.Validate(); err != nil {
		return fmt.Errorf("namespaceMatcher is not properly setup: %v", err)
	}
	if err := a.clientManager.Validate(); err != nil {
		return fmt.Errorf("clientManager is not properly setup: %v", err)
	}

	// 防御：设置了静态清单目录但特性门控未开启时报错。
	if len(a.staticManifestsDir) > 0 && !utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		return fmt.Errorf("static webhook manifests dir %q configured but %s feature gate is not enabled", a.staticManifestsDir, features.ManifestBasedAdmissionControlConfig)
	}

	// 构造 hookSource。静态来源路径会启动 goroutine，因此加保护避免重复构造；
	// 仅 API 的路径只是廉价的指针赋值，必须反映最新的 apiSource。
	if len(a.staticManifestsDir) > 0 {
		if a.hookSource == nil {
			if a.staticSourceFactory == nil {
				return fmt.Errorf("static webhook manifests configured in %q but no static source factory is set", a.staticManifestsDir)
			}
			// 文件监听需要随 server 退出而停止，因此依赖 drained 通知。
			if a.stopCh == nil {
				return fmt.Errorf("stopCh not set: WantsDrainedNotification must be called before ValidateInitialization")
			}
			staticSource, err := a.staticSourceFactory(a.staticManifestsDir)
			if err != nil {
				return fmt.Errorf("failed to load static webhook manifests from %q: %w", a.staticManifestsDir, err)
			}
			a.staticSource = staticSource
			// 在后台 goroutine 中启动文件监听，随 server 退出而停止。
			staticCtx, staticCancel := context.WithCancel(context.Background())
			go func() {
				defer staticCancel()
				<-a.stopCh
			}()
			go staticSource.RunReloadLoop(staticCtx)
			// 使用组合静态来源与 API 来源的复合来源。
			a.hookSource = NewCompositeWebhookSource(staticSource, a.apiSource)
		}
	} else {
		a.hookSource = a.apiSource
	}

	// 就绪条件是 namespace informer 与 hook 来源都完成首次同步。
	a.SetReadyFunc(func() bool {
		return a.namespaceInformer.Informer().HasSynced() && a.hookSource.HasSynced()
	})

	return nil
}

// ShouldCallHook 在应调用 webhook 时返回调用详情，不应调用时返回 nil，
// 求值过程出错时返回错误。
// 匹配顺序：namespace selector -> object selector -> rules（含等价资源）。
// 前两步的匹配错误不会立即返回：如果最终规则不匹配，就不该因无关的
// selector 错误而拒绝请求；只有确定要调用 webhook 时才把错误抛给用户。
func (a *Webhook) ShouldCallHook(ctx context.Context, h webhook.WebhookAccessor, attr admission.Attributes, o admission.ObjectInterfaces, v VersionedAttributeAccessor) (*WebhookInvocation, *apierrors.StatusError) {
	matches, matchNsErr := a.namespaceMatcher.MatchNamespaceSelector(h, attr)
	// 对于不适用于本请求的 webhook 不应返回错误，即使 err 属于意外场景。
	if !matches && matchNsErr == nil {
		return nil, nil
	}

	// 对于不适用于本请求的 webhook 不应返回错误，即使 err 属于意外场景。
	matches, matchObjErr := a.objectMatcher.MatchObjectSelector(h, attr)
	if !matches && matchObjErr == nil {
		return nil, nil
	}

	var invocation *WebhookInvocation
	for _, r := range h.GetRules() {
		m := rules.Matcher{Rule: r, Attr: attr}
		if m.Matches() {
			invocation = &WebhookInvocation{
				Webhook:     h,
				Resource:    attr.GetResource(),
				Subresource: attr.GetSubresource(),
				Kind:        attr.GetKind(),
			}
			break
		}
	}
	// MatchPolicy=Equivalent 时还要检查等价资源（如 deployment 与 replicasets 的
	// scale 子资源），否则针对某一版本的规则会漏掉其它等价形式。
	if invocation == nil && h.GetMatchPolicy() != nil && *h.GetMatchPolicy() == v1.Equivalent {
		attrWithOverride := &attrWithResourceOverride{Attributes: attr}
		equivalents := o.GetEquivalentResourceMapper().EquivalentResourcesFor(attr.GetResource(), attr.GetSubresource())
		// 优先遵循排在前面的规则。
	OuterLoop:
		for _, r := range h.GetRules() {
			// 检查规则是否匹配任一等价资源。
			for _, equivalent := range equivalents {
				if equivalent == attr.GetResource() {
					// 排除 attr.GetResource()，前面已经检查过。
					continue
				}
				attrWithOverride.resource = equivalent
				m := rules.Matcher{Rule: r, Attr: attrWithOverride}
				if m.Matches() {
					kind := o.GetEquivalentResourceMapper().KindFor(equivalent, attr.GetSubresource())
					// 等价资源没有对应 Kind 时无法构造请求体，属于服务端配置错误。
					if kind.Empty() {
						return nil, apierrors.NewInternalError(fmt.Errorf("unable to convert to %v: unknown kind", equivalent))
					}
					invocation = &WebhookInvocation{
						Webhook:     h,
						Resource:    equivalent,
						Subresource: attr.GetSubresource(),
						Kind:        kind,
					}
					break OuterLoop
				}
			}
		}
	}

	if invocation == nil {
		return nil, nil
	}
	// 到这里已确定要调用 webhook，之前缓存的 selector 错误现在必须生效。
	if matchNsErr != nil {
		return nil, matchNsErr
	}
	if matchObjErr != nil {
		return nil, matchObjErr
	}
	matchConditions := h.GetMatchConditions()
	if len(matchConditions) > 0 {
		versionedAttr, err := v.VersionedAttribute(invocation.Kind)
		if err != nil {
			return nil, apierrors.NewInternalError(err)
		}

		matcher := h.GetCompiledMatcher(a.filterCompiler)
		matchResult := matcher.Match(ctx, versionedAttr, nil, a.authorizer)

		// match condition 求值失败时 fail closed：宁可拒绝请求，
		// 也不能让策略因求值错误而被静默绕过。
		if matchResult.Error != nil {
			klog.Warningf("Failed evaluating match conditions, failing closed %v: %v", h.GetName(), matchResult.Error)
			return nil, apierrors.NewForbidden(attr.GetResource().GroupResource(), attr.GetName(), matchResult.Error)
		} else if !matchResult.Matches {
			admissionmetrics.Metrics.ObserveMatchConditionExclusion(ctx, h.GetName(), "webhook", h.GetType(), string(attr.GetOperation()))
			// 不匹配则始终跳过该 webhook。
			return nil, nil
		}
	}

	return invocation, nil
}

// attrWithResourceOverride 只覆盖资源字段，其余属性透传，
// 用于在等价资源上复用同一个 rule matcher。
type attrWithResourceOverride struct {
	admission.Attributes
	resource schema.GroupVersionResource
}

// GetResource 返回被替换后的等价资源。
func (a *attrWithResourceOverride) GetResource() schema.GroupVersionResource { return a.resource }

// isExcludedFromAllHooks 对不允许被任何 webhook（静态或 REST 来源）拦截的
// 非持久化虚拟资源（auth/authz review）返回 true；
// 未启用 ExcludeAdmissionWebhookVirtualResources 特性时恒为 false（无操作）。
func (a *Webhook) isExcludedFromAllHooks(gr schema.GroupResource) bool {
	return a.excludeVirtualResources && a.excludedAdmissionResources.Has(gr)
}

// isExcludedFromAPIHooks 对准入配置类资源（webhook 配置与准入策略/绑定）返回 true：
// 这类资源被 API 来源的 webhook 排除以避免循环依赖，
// 但仍可被静态（基于清单）的 webhook 处理。
func (a *Webhook) isExcludedFromAPIHooks(attr admission.Attributes) bool {
	return rules.IsExemptAdmissionConfigurationResource(attr)
}

// Dispatch 由下游的 Validate 或 Admit 方法调用。
// 三层排除逻辑保护集群自身可用性：
//   - 认证/授权 review 等虚拟资源不经过任何 webhook，避免准入卡死认证链路；
//   - 准入配置资源不经过 API 来源的 webhook，避免自我引用死锁，
//     但静态清单来源仍可处理它们；
//   - 其余请求等就绪后按 hookSource 全量调度。
func (a *Webhook) Dispatch(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	if a.isExcludedFromAllHooks(attr.GetResource().GroupResource()) {
		// 虚拟的 auth/authz 资源被所有 webhook（静态与 REST 来源）排除，
		// 避免准入把集群卡死在自己的认证路径上。
		return nil
	}
	if a.isExcludedFromAPIHooks(attr) {
		// 准入配置资源被 API 来源的 webhook 排除以避免循环依赖；
		// 但静态（基于清单）的 webhook 处理它们是安全的，
		// 不存在自我引用问题。
		if a.staticSource != nil {
			if !a.staticSource.HasSynced() {
				return admission.NewForbidden(attr, fmt.Errorf("not yet ready to handle request"))
			}
			hooks := a.staticSource.Webhooks()
			return a.dispatcher.Dispatch(ctx, attr, o, hooks)
		}
		return nil
	}
	// 未就绪时拒绝而非放行：fail closed 保证配置尚未加载完时不会漏掉校验。
	if !a.WaitForReady() {
		return admission.NewForbidden(attr, fmt.Errorf("not yet ready to handle request"))
	}
	hooks := a.hookSource.Webhooks()
	return a.dispatcher.Dispatch(ctx, attr, o, hooks)
}
