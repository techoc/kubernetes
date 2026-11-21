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

package authorizer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	authzconfig "k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	authorizationcel "k8s.io/apiserver/pkg/authorization/cel"
	authorizationmetrics "k8s.io/apiserver/pkg/authorization/metrics"
	"k8s.io/apiserver/pkg/authorization/union"
	"k8s.io/apiserver/pkg/server/options/authorizationconfig/metrics"
	"k8s.io/apiserver/pkg/util/filesystem"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/apiserver/plugin/pkg/authorizer/webhook"
	webhookmetrics "k8s.io/apiserver/plugin/pkg/authorizer/webhook/metrics"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/auth/authorizer/abac"
	"k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes"
	"k8s.io/kubernetes/plugin/pkg/auth/authorizer/node"
	"k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac"
)

var _ = authorizer.Authorizer(&reloadableAuthorizerResolver{})
var _ = authorizer.RuleResolver(&reloadableAuthorizerResolver{})

// reloadableAuthorizerResolver 是支持热重载的授权器与规则解析器。
// 它持有一个以 atomic.Pointer 发布的当前解析器，使授权调用无需加锁即可读取最新配置。
type reloadableAuthorizerResolver struct {
	// initialConfig 保存用于启动后台重载的 ReloadFile，
	// 以及配置文件中未暴露但构造 webhook 所需的信息（拨号函数、退避设置等）。
	initialConfig Config

	apiServerID string

	reloadInterval         time.Duration
	requireNonWebhookTypes sets.Set[authzconfig.AuthorizerType]

	nodeAuthorizer *node.NodeAuthorizer
	rbacAuthorizer *rbac.RBACAuthorizer
	abacAuthorizer abac.PolicyList
	compiler       authorizationcel.Compiler // non-nil and shared across reloads.

	// lastLoadedLock 保护重载过程中的配置比对与 current 发布，避免并发重载相互覆盖。
	lastLoadedLock   sync.Mutex
	lastLoadedConfig *authzconfig.AuthorizationConfiguration
	lastReadData     []byte

	current atomic.Pointer[authorizerResolver]
}

// authorizerResolver 将授权器与规则解析器配对，作为一次配置快照整体发布。
type authorizerResolver struct {
	authorizer   authorizer.Authorizer
	ruleResolver authorizer.RuleResolver
}

// Authorize 委托给当前的授权器进行鉴权。
func (r *reloadableAuthorizerResolver) Authorize(ctx context.Context, a authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	return r.current.Load().authorizer.Authorize(ctx, a)
}

// ConditionsAwareAuthorize 委托给当前的授权器执行条件感知鉴权。
func (r *reloadableAuthorizerResolver) ConditionsAwareAuthorize(ctx context.Context, a authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return r.current.Load().authorizer.ConditionsAwareAuthorize(ctx, a)
}

// EvaluateConditions 委托给当前的授权器评估 CEL 条件。
func (r *reloadableAuthorizerResolver) EvaluateConditions(ctx context.Context, decision authorizer.ConditionsAwareDecision, data authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return r.current.Load().authorizer.EvaluateConditions(ctx, decision, data)
}

// RulesFor 委托给当前的规则解析器查询用户权限规则。
func (r *reloadableAuthorizerResolver) RulesFor(ctx context.Context, user user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	return r.current.Load().ruleResolver.RulesFor(ctx, user, namespace)
}

// newForConfig 根据给定的授权配置构造联合授权器与规则解析器。
func (r *reloadableAuthorizerResolver) newForConfig(authzConfig *authzconfig.AuthorizationConfiguration) (authorizer.Authorizer, authorizer.RuleResolver, error) {
	if len(authzConfig.Authorizers) == 0 {
		return nil, nil, fmt.Errorf("at least one authorization mode must be passed")
	}

	var (
		authorizers   []union.NamedAuthorizer
		ruleResolvers []authorizer.RuleResolver
	)

	// system:masters 组始终拥有最高权限，必须置于授权链最前且不可被配置移除。
	superuserAuthorizer := authorizerfactory.NewPrivilegedGroups(user.SystemPrivilegedGroup)
	authorizers = append(authorizers, union.NamedAuthorizer{
		AuthorizerName: "kubernetes.io/system-privileged-group",
		Authorizer:     superuserAuthorizer,
	})

	for _, configuredAuthorizer := range authzConfig.Authorizers {
		// Keep cases in sync with constant list in k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes/modes.go.
		switch configuredAuthorizer.Type {
		case authzconfig.AuthorizerType(modes.ModeNode):
			// Node/ABAC/RBAC 等授权器在启动时构建一次，重载时不允许新增未在初始配置中启用的类型。
			if r.nodeAuthorizer == nil {
				return nil, nil, fmt.Errorf("authorizer type Node is not allowed if it was not enabled at initial server startup")
			}
			authorizers = append(authorizers, union.NamedAuthorizer{
				AuthorizerName: configuredAuthorizer.Name,
				Authorizer:     authorizationmetrics.InstrumentedAuthorizer(string(configuredAuthorizer.Type), configuredAuthorizer.Name, r.nodeAuthorizer),
			})
			ruleResolvers = append(ruleResolvers, r.nodeAuthorizer)
		case authzconfig.AuthorizerType(modes.ModeAlwaysAllow):
			alwaysAllowAuthorizer := authorizerfactory.NewAlwaysAllowAuthorizer()
			authorizers = append(authorizers, union.NamedAuthorizer{
				AuthorizerName: configuredAuthorizer.Name,
				Authorizer:     authorizationmetrics.InstrumentedAuthorizer(string(configuredAuthorizer.Type), configuredAuthorizer.Name, alwaysAllowAuthorizer),
			})
			ruleResolvers = append(ruleResolvers, alwaysAllowAuthorizer)
		case authzconfig.AuthorizerType(modes.ModeAlwaysDeny):
			alwaysDenyAuthorizer := authorizerfactory.NewAlwaysDenyAuthorizer()
			authorizers = append(authorizers, union.NamedAuthorizer{
				AuthorizerName: configuredAuthorizer.Name,
				Authorizer:     authorizationmetrics.InstrumentedAuthorizer(string(configuredAuthorizer.Type), configuredAuthorizer.Name, alwaysDenyAuthorizer),
			})
			ruleResolvers = append(ruleResolvers, alwaysDenyAuthorizer)
		case authzconfig.AuthorizerType(modes.ModeABAC):
			if r.abacAuthorizer == nil {
				return nil, nil, fmt.Errorf("authorizer type ABAC is not allowed if it was not enabled at initial server startup")
			}
			authorizers = append(authorizers, union.NamedAuthorizer{
				AuthorizerName: configuredAuthorizer.Name,
				Authorizer:     authorizationmetrics.InstrumentedAuthorizer(string(configuredAuthorizer.Type), configuredAuthorizer.Name, r.abacAuthorizer),
			})
			ruleResolvers = append(ruleResolvers, r.abacAuthorizer)
		case authzconfig.AuthorizerType(modes.ModeWebhook):
			if r.initialConfig.WebhookRetryBackoff == nil {
				return nil, nil, errors.New("retry backoff parameters for authorization webhook has not been specified")
			}
			clientConfig, err := webhookutil.LoadKubeconfig(*configuredAuthorizer.Webhook.ConnectionInfo.KubeConfigFile, r.initialConfig.CustomDial)
			if err != nil {
				return nil, nil, err
			}
			if configuredAuthorizer.Webhook.Timeout.Duration != 0 {
				clientConfig.Timeout = configuredAuthorizer.Webhook.Timeout.Duration
			}
			// webhook 调用失败时的决策由 failurePolicy 决定，需在构造时确定。
			var decisionOnError authorizer.Decision
			switch configuredAuthorizer.Webhook.FailurePolicy {
			case authzconfig.FailurePolicyNoOpinion:
				decisionOnError = authorizer.DecisionNoOpinion
			case authzconfig.FailurePolicyDeny:
				decisionOnError = authorizer.DecisionDeny
			default:
				return nil, nil, fmt.Errorf("unknown failurePolicy %q", configuredAuthorizer.Webhook.FailurePolicy)
			}

			// 缓存关闭时强制将 TTL 置零，避免误用配置中残留的时长。
			authorizedTTL, unauthorizedTTL := configuredAuthorizer.Webhook.AuthorizedTTL.Duration, configuredAuthorizer.Webhook.UnauthorizedTTL.Duration
			if !configuredAuthorizer.Webhook.CacheAuthorizedRequests {
				authorizedTTL = 0
			}
			if !configuredAuthorizer.Webhook.CacheUnauthorizedRequests {
				unauthorizedTTL = 0
			}
			webhookAuthorizer, err := webhook.New(clientConfig,
				configuredAuthorizer.Webhook.SubjectAccessReviewVersion,
				authorizedTTL,
				unauthorizedTTL,
				*r.initialConfig.WebhookRetryBackoff,
				decisionOnError,
				configuredAuthorizer.Webhook.MatchConditions,
				configuredAuthorizer.Name,
				kubeapiserverWebhookMetrics{WebhookMetrics: webhookmetrics.NewWebhookMetrics(), MatcherMetrics: authorizationcel.NewMatcherMetrics()},
				r.compiler,
			)
			if err != nil {
				return nil, nil, err
			}
			authorizers = append(authorizers, union.NamedAuthorizer{
				AuthorizerName: configuredAuthorizer.Name,
				Authorizer:     authorizationmetrics.InstrumentedAuthorizer(string(configuredAuthorizer.Type), configuredAuthorizer.Name, webhookAuthorizer),
			})
			ruleResolvers = append(ruleResolvers, webhookAuthorizer)
		case authzconfig.AuthorizerType(modes.ModeRBAC):
			if r.rbacAuthorizer == nil {
				return nil, nil, fmt.Errorf("authorizer type RBAC is not allowed if it was not enabled at initial server startup")
			}
			authorizers = append(authorizers, union.NamedAuthorizer{
				AuthorizerName: configuredAuthorizer.Name,
				Authorizer:     authorizationmetrics.InstrumentedAuthorizer(string(configuredAuthorizer.Type), configuredAuthorizer.Name, r.rbacAuthorizer),
			})
			ruleResolvers = append(ruleResolvers, r.rbacAuthorizer)
		default:
			return nil, nil, fmt.Errorf("unknown authorization mode %s specified", configuredAuthorizer.Type)
		}
	}

	unioned, err := union.New(authorizers...)
	if err != nil {
		return nil, nil, err
	}
	return unioned, union.NewRuleResolvers(ruleResolvers...), nil
}

// kubeapiserverWebhookMetrics 为 kube-apiserver 定制的 webhook 指标集合。
type kubeapiserverWebhookMetrics struct {
	// kube-apiserver 不上报请求级指标。
	webhookmetrics.NoopRequestMetrics
	// 但会上报 webhook 指标。
	webhookmetrics.WebhookMetrics
	// 以及 matchCondition 指标。
	authorizationcel.MatcherMetrics
}

// runReload 定期检查授权配置文件，发现变化时重载授权器。
// 该方法会阻塞直到 ctx 结束。
func (r *reloadableAuthorizerResolver) runReload(ctx context.Context) {
	metrics.RegisterMetrics()
	metrics.RecordAuthorizationConfigLastConfigInfo(r.apiServerID, string(r.lastReadData))

	filesystem.WatchUntil(
		ctx,
		r.reloadInterval,
		r.initialConfig.ReloadFile,
		func() {
			r.checkFile(ctx)
		},
		func(err error) {
			klog.ErrorS(err, "watching authorization config file")
		},
	)
}

// checkFile 读取并校验授权配置文件，若内容或解析结果有变化则原子发布新的授权器。
func (r *reloadableAuthorizerResolver) checkFile(ctx context.Context) {
	r.lastLoadedLock.Lock()
	defer r.lastLoadedLock.Unlock()

	data, err := os.ReadFile(r.initialConfig.ReloadFile)
	if err != nil {
		klog.ErrorS(err, "reloading authorization config")
		metrics.RecordAuthorizationConfigAutomaticReloadFailure(r.apiServerID)
		return
	}
	if bytes.Equal(data, r.lastReadData) {
		// 字节完全一致，无需重复解析。
		return
	}
	klog.InfoS("found new authorization config data")
	r.lastReadData = data

	config, err := LoadAndValidateData(data, r.compiler, r.requireNonWebhookTypes)
	if err != nil {
		klog.ErrorS(err, "reloading authorization config")
		metrics.RecordAuthorizationConfigAutomaticReloadFailure(r.apiServerID)
		return
	}
	if reflect.DeepEqual(config, r.lastLoadedConfig) {
		// 解析后语义未变，无需重建授权器。
		return
	}
	klog.InfoS("found new authorization config")
	r.lastLoadedConfig = config

	authorizer, ruleResolver, err := r.newForConfig(config)
	if err != nil {
		klog.ErrorS(err, "reloading authorization config")
		metrics.RecordAuthorizationConfigAutomaticReloadFailure(r.apiServerID)
		return
	}
	klog.InfoS("constructed new authorizer")

	r.current.Store(&authorizerResolver{
		authorizer:   authorizer,
		ruleResolver: ruleResolver,
	})
	klog.InfoS("reloaded authz config")
	metrics.RecordAuthorizationConfigAutomaticReloadSuccess(r.apiServerID, string(data))
}
