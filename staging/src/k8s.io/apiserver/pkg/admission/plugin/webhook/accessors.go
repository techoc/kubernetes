/*
Copyright 2019 The Kubernetes Authors.

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

// Package webhook 提供 mutating 与 validating 两类准入 webhook 的公共抽象，
// 使匹配、调用与 CEL 条件评估逻辑可以只面向一个接口编写。
package webhook

import (
	"sync"

	v1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/matchconditions"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/namespace"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/object"
	"k8s.io/apiserver/pkg/cel/environment"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/rest"
)

// WebhookAccessor provides a common interface to both mutating and validating webhook types.
//
// 中文说明：两类 webhook 的 API 结构相似但类型不同，访问器把它们统一起来；
// 实现必须并发安全，因为同一访问器会被多个准入请求同时读取。
type WebhookAccessor interface {
	// This accessor provides the methods needed to support matching against webhook
	// predicates
	namespace.NamespaceSelectorProvider
	object.ObjectSelectorProvider

	// GetUID gets a string that uniquely identifies the webhook.
	//
	// 中文说明：UID 由配置 UID 与 webhook 名称组合而成，跨配置唯一；
	// 指标与日志用它区分同名 webhook。
	GetUID() string

	// GetConfigurationName gets the name of the webhook configuration that owns this webhook.
	GetConfigurationName() string

	// GetRESTClient gets the webhook client
	//
	// 中文说明：客户端按 clientConfig 惰性构造并缓存，ClientManager 负责
	// 复用连接与证书；返回 error 时调用方应按 failurePolicy 处理。
	GetRESTClient(clientManager *webhookutil.ClientManager) (*rest.RESTClient, error)

	// GetCompiledMatcher gets the compiled matcher object
	//
	// 中文说明：编译结果缓存于访问器内，首次调用后即复用；compiler 由
	// 插件持有，同一访问器只应被同一个 compiler 编译。
	GetCompiledMatcher(compiler cel.ConditionCompiler) matchconditions.Matcher

	// GetName gets the webhook Name field. Note that the name is scoped to the webhook
	// configuration and does not provide a globally unique identity, if a unique identity is
	// needed, use GetUID.
	GetName() string
	// GetClientConfig gets the webhook ClientConfig field.
	GetClientConfig() v1.WebhookClientConfig
	// GetRules gets the webhook Rules field.
	GetRules() []v1.RuleWithOperations
	// GetFailurePolicy gets the webhook FailurePolicy field.
	GetFailurePolicy() *v1.FailurePolicyType
	// GetMatchPolicy gets the webhook MatchPolicy field.
	GetMatchPolicy() *v1.MatchPolicyType
	// GetNamespaceSelector gets the webhook NamespaceSelector field.
	GetNamespaceSelector() *metav1.LabelSelector
	// GetObjectSelector gets the webhook ObjectSelector field.
	GetObjectSelector() *metav1.LabelSelector
	// GetSideEffects gets the webhook SideEffects field.
	GetSideEffects() *v1.SideEffectClass
	// GetTimeoutSeconds gets the webhook TimeoutSeconds field.
	GetTimeoutSeconds() *int32
	// GetAdmissionReviewVersions gets the webhook AdmissionReviewVersions field.
	GetAdmissionReviewVersions() []string

	// GetMatchConditions gets the webhook match conditions field.
	GetMatchConditions() []v1.MatchCondition

	// GetMutatingWebhook if the accessor contains a MutatingWebhook, returns it and true, else returns false.
	//
	// 中文说明：用 (value, ok) 而非返回 nil 接口，避免调用方对 nil 接口
	// 与 nil 指针的判断混淆。
	GetMutatingWebhook() (*v1.MutatingWebhook, bool)
	// GetValidatingWebhook if the accessor contains a ValidatingWebhook, returns it and true, else returns false.
	GetValidatingWebhook() (*v1.ValidatingWebhook, bool)

	// GetType returns the type of the accessor (validate or admit)
	//
	// 中文说明：返回固定字符串而非枚举，是因为它直接用作指标标签与日志字段。
	GetType() string
}

// NewMutatingWebhookAccessor creates an accessor for a MutatingWebhook.
//
// 中文说明：configurationName 用于日志定位；uid 需由调用方保证唯一。
func NewMutatingWebhookAccessor(uid, configurationName string, h *v1.MutatingWebhook) WebhookAccessor {
	return &mutatingWebhookAccessor{uid: uid, configurationName: configurationName, MutatingWebhook: h}
}

// mutatingWebhookAccessor 包装 MutatingWebhook，并惰性缓存派生结果。
//
// 中文说明：selector、client、matcher 的构造都可能失败或较慢，用 sync.Once
// 保证只做一次且并发安全；缓存结果包含 error，避免失败路径被反复重试。
type mutatingWebhookAccessor struct {
	*v1.MutatingWebhook
	uid               string
	configurationName string

	initObjectSelector sync.Once
	objectSelector     labels.Selector
	objectSelectorErr  error

	initNamespaceSelector sync.Once
	namespaceSelector     labels.Selector
	namespaceSelectorErr  error

	initClient sync.Once
	client     *rest.RESTClient
	clientErr  error

	compileMatcher  sync.Once
	compiledMatcher matchconditions.Matcher
}

func (m *mutatingWebhookAccessor) GetUID() string {
	return m.uid
}

func (m *mutatingWebhookAccessor) GetConfigurationName() string {
	return m.configurationName
}

func (m *mutatingWebhookAccessor) GetRESTClient(clientManager *webhookutil.ClientManager) (*rest.RESTClient, error) {
	// ClientManager 内部按配置缓存，这里再缓存一层是为了避免重复构造
	// ClientConfig 结构体与 map 查找。
	m.initClient.Do(func() {
		m.client, m.clientErr = clientManager.HookClient(hookClientConfigForWebhook(m))
	})
	return m.client, m.clientErr
}

func (m *mutatingWebhookAccessor) GetType() string {
	return "admit"
}

// GetCompiledMatcher 编译并缓存 matchConditions。
//
// 中文说明：webhook 的 matchConditions 不可使用 params，但可见 authorizer，
// 因此 OptionalVariableDeclarations 固定为 HasParams=false、HasAuthorizer=true。
func (m *mutatingWebhookAccessor) GetCompiledMatcher(compiler cel.ConditionCompiler) matchconditions.Matcher {
	m.compileMatcher.Do(func() {
		expressions := make([]cel.ExpressionAccessor, len(m.MutatingWebhook.MatchConditions))
		for i, matchCondition := range m.MutatingWebhook.MatchConditions {
			expressions[i] = &matchconditions.MatchCondition{
				Name:       matchCondition.Name,
				Expression: matchCondition.Expression,
			}
		}
		m.compiledMatcher = matchconditions.NewMatcher(compiler.CompileCondition(
			expressions,
			cel.OptionalVariableDeclarations{
				HasParams:     false,
				HasAuthorizer: true,
			},
			environment.StoredExpressions,
		), m.FailurePolicy, "webhook", "admit", m.Name)
	})
	return m.compiledMatcher
}

// GetParsedNamespaceSelector 把 LabelSelector 解析为可复用选择器并缓存。
//
// 中文说明：解析失败会缓存 error，配置修正后需重新构建访问器才能恢复。
func (m *mutatingWebhookAccessor) GetParsedNamespaceSelector() (labels.Selector, error) {
	m.initNamespaceSelector.Do(func() {
		m.namespaceSelector, m.namespaceSelectorErr = metav1.LabelSelectorAsSelector(m.NamespaceSelector)
	})
	return m.namespaceSelector, m.namespaceSelectorErr
}

// GetParsedObjectSelector 同上，针对对象标签选择器。
func (m *mutatingWebhookAccessor) GetParsedObjectSelector() (labels.Selector, error) {
	m.initObjectSelector.Do(func() {
		m.objectSelector, m.objectSelectorErr = metav1.LabelSelectorAsSelector(m.ObjectSelector)
	})
	return m.objectSelector, m.objectSelectorErr
}

func (m *mutatingWebhookAccessor) GetName() string {
	return m.Name
}

func (m *mutatingWebhookAccessor) GetClientConfig() v1.WebhookClientConfig {
	return m.ClientConfig
}

func (m *mutatingWebhookAccessor) GetRules() []v1.RuleWithOperations {
	return m.Rules
}

func (m *mutatingWebhookAccessor) GetFailurePolicy() *v1.FailurePolicyType {
	return m.FailurePolicy
}

func (m *mutatingWebhookAccessor) GetMatchPolicy() *v1.MatchPolicyType {
	return m.MatchPolicy
}

func (m *mutatingWebhookAccessor) GetNamespaceSelector() *metav1.LabelSelector {
	return m.NamespaceSelector
}

func (m *mutatingWebhookAccessor) GetObjectSelector() *metav1.LabelSelector {
	return m.ObjectSelector
}

func (m *mutatingWebhookAccessor) GetSideEffects() *v1.SideEffectClass {
	return m.SideEffects
}

func (m *mutatingWebhookAccessor) GetTimeoutSeconds() *int32 {
	return m.TimeoutSeconds
}

func (m *mutatingWebhookAccessor) GetAdmissionReviewVersions() []string {
	return m.AdmissionReviewVersions
}

func (m *mutatingWebhookAccessor) GetMatchConditions() []v1.MatchCondition {
	return m.MatchConditions
}

func (m *mutatingWebhookAccessor) GetMutatingWebhook() (*v1.MutatingWebhook, bool) {
	return m.MutatingWebhook, true
}

func (m *mutatingWebhookAccessor) GetValidatingWebhook() (*v1.ValidatingWebhook, bool) {
	return nil, false
}

// NewValidatingWebhookAccessor creates an accessor for a ValidatingWebhook.
func NewValidatingWebhookAccessor(uid, configurationName string, h *v1.ValidatingWebhook) WebhookAccessor {
	return &validatingWebhookAccessor{uid: uid, configurationName: configurationName, ValidatingWebhook: h}
}

// validatingWebhookAccessor 与 mutatingWebhookAccessor 结构对称，仅类型不同。
//
// 中文说明：故意不抽象为泛型：API 类型没有公共字段接口，强行泛型化反而
// 需要大量类型开关。
type validatingWebhookAccessor struct {
	*v1.ValidatingWebhook
	uid               string
	configurationName string

	initObjectSelector sync.Once
	objectSelector     labels.Selector
	objectSelectorErr  error

	initNamespaceSelector sync.Once
	namespaceSelector     labels.Selector
	namespaceSelectorErr  error

	initClient sync.Once
	client     *rest.RESTClient
	clientErr  error

	compileMatcher  sync.Once
	compiledMatcher matchconditions.Matcher
}

func (v *validatingWebhookAccessor) GetUID() string {
	return v.uid
}

func (v *validatingWebhookAccessor) GetConfigurationName() string {
	return v.configurationName
}

func (v *validatingWebhookAccessor) GetRESTClient(clientManager *webhookutil.ClientManager) (*rest.RESTClient, error) {
	v.initClient.Do(func() {
		v.client, v.clientErr = clientManager.HookClient(hookClientConfigForWebhook(v))
	})
	return v.client, v.clientErr
}

// GetCompiledMatcher 编译并缓存 matchConditions。
//
// 中文说明：与 mutating 版本唯一差别是传给 NewMatcher 的 operation 标签
// 为 "validating"，用于错误信息中指明阶段。
func (v *validatingWebhookAccessor) GetCompiledMatcher(compiler cel.ConditionCompiler) matchconditions.Matcher {
	v.compileMatcher.Do(func() {
		expressions := make([]cel.ExpressionAccessor, len(v.ValidatingWebhook.MatchConditions))
		for i, matchCondition := range v.ValidatingWebhook.MatchConditions {
			expressions[i] = &matchconditions.MatchCondition{
				Name:       matchCondition.Name,
				Expression: matchCondition.Expression,
			}
		}
		v.compiledMatcher = matchconditions.NewMatcher(compiler.CompileCondition(
			expressions,
			cel.OptionalVariableDeclarations{
				HasParams:     false,
				HasAuthorizer: true,
			},
			environment.StoredExpressions,
		), v.FailurePolicy, "webhook", "validating", v.Name)
	})
	return v.compiledMatcher
}

// GetParsedNamespaceSelector 把 LabelSelector 解析为可复用选择器并缓存。
func (v *validatingWebhookAccessor) GetParsedNamespaceSelector() (labels.Selector, error) {
	v.initNamespaceSelector.Do(func() {
		v.namespaceSelector, v.namespaceSelectorErr = metav1.LabelSelectorAsSelector(v.NamespaceSelector)
	})
	return v.namespaceSelector, v.namespaceSelectorErr
}

// GetParsedObjectSelector 同上，针对对象标签选择器。
func (v *validatingWebhookAccessor) GetParsedObjectSelector() (labels.Selector, error) {
	v.initObjectSelector.Do(func() {
		v.objectSelector, v.objectSelectorErr = metav1.LabelSelectorAsSelector(v.ObjectSelector)
	})
	return v.objectSelector, v.objectSelectorErr
}

func (m *validatingWebhookAccessor) GetType() string {
	return "validate"
}

func (v *validatingWebhookAccessor) GetName() string {
	return v.Name
}

func (v *validatingWebhookAccessor) GetClientConfig() v1.WebhookClientConfig {
	return v.ClientConfig
}

func (v *validatingWebhookAccessor) GetRules() []v1.RuleWithOperations {
	return v.Rules
}

func (v *validatingWebhookAccessor) GetFailurePolicy() *v1.FailurePolicyType {
	return v.FailurePolicy
}

func (v *validatingWebhookAccessor) GetMatchPolicy() *v1.MatchPolicyType {
	return v.MatchPolicy
}

func (v *validatingWebhookAccessor) GetNamespaceSelector() *metav1.LabelSelector {
	return v.NamespaceSelector
}

func (v *validatingWebhookAccessor) GetObjectSelector() *metav1.LabelSelector {
	return v.ObjectSelector
}

func (v *validatingWebhookAccessor) GetSideEffects() *v1.SideEffectClass {
	return v.SideEffects
}

func (v *validatingWebhookAccessor) GetTimeoutSeconds() *int32 {
	return v.TimeoutSeconds
}

func (v *validatingWebhookAccessor) GetAdmissionReviewVersions() []string {
	return v.AdmissionReviewVersions
}

func (v *validatingWebhookAccessor) GetMatchConditions() []v1.MatchCondition {
	return v.MatchConditions
}

func (v *validatingWebhookAccessor) GetMutatingWebhook() (*v1.MutatingWebhook, bool) {
	return nil, false
}

func (v *validatingWebhookAccessor) GetValidatingWebhook() (*v1.ValidatingWebhook, bool) {
	return v.ValidatingWebhook, true
}

// hookClientConfigForWebhook construct a webhookutil.ClientConfig using a WebhookAccessor to access
// v1beta1.MutatingWebhook and v1beta1.ValidatingWebhook API objects.  webhookutil.ClientConfig is used
// to create a HookClient and the purpose of the config struct is to share that with other packages
// that need to create a HookClient.
//
// 中文说明：URL 与 Service 互斥，API 校验已保证只有一个被设置；端口缺省为 443
// 是历史约定，避免要求用户总是显式写端口。
func hookClientConfigForWebhook(w WebhookAccessor) webhookutil.ClientConfig {
	ret := webhookutil.ClientConfig{Name: w.GetName(), CABundle: w.GetClientConfig().CABundle}
	if w.GetClientConfig().URL != nil {
		ret.URL = *w.GetClientConfig().URL
	}
	if w.GetClientConfig().Service != nil {
		ret.Service = &webhookutil.ClientConfigService{
			Name:      w.GetClientConfig().Service.Name,
			Namespace: w.GetClientConfig().Service.Namespace,
		}
		if w.GetClientConfig().Service.Port != nil {
			ret.Service.Port = *w.GetClientConfig().Service.Port
		} else {
			ret.Service.Port = 443
		}
		if w.GetClientConfig().Service.Path != nil {
			ret.Service.Path = *w.GetClientConfig().Service.Path
		}
	}
	return ret
}
