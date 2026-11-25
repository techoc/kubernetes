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

package storage

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	authenticationapiv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	authenticationtokenjwt "k8s.io/apiserver/pkg/authentication/token/jwt"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericfeatures "k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/klog/v2"
	authenticationapi "k8s.io/kubernetes/pkg/apis/authentication"
	authenticationvalidation "k8s.io/kubernetes/pkg/apis/authentication/validation"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
	token "k8s.io/kubernetes/pkg/serviceaccount"
)

const (
	maxAdmissionReviewWebhookTokenExpirationSeconds = 10 * 60
)

func (r *TokenREST) New() runtime.Object {
	return &authenticationapi.TokenRequest{}
}

// Destroy 在关闭时清理资源。
func (r *TokenREST) Destroy() {
	// 由于没有底层 store，这里不需要显式销毁任何东西。
}

// TokenREST 实现 serviceaccounts/{name}/token：验证请求者与绑定对象，构造 JWT Claims 并通过 TokenGenerator 签发短期凭据。
type TokenREST struct {
	svcaccts                     rest.Getter
	pods                         rest.Getter
	secrets                      rest.Getter
	nodes                        rest.Getter
	validatingWebhooks           *vwhGetter
	mutatingWebhooks             *mwhGetter
	authorizer                   authorizer.Authorizer
	issuer                       token.TokenGenerator
	auds                         authenticator.Audiences
	audsSet                      sets.Set[string]
	maxExpirationSeconds         int64
	extendExpiration             bool
	maxExtendedExpirationSeconds int64
}

var _ = rest.NamedCreater(&TokenREST{})
var _ = rest.GroupVersionKindProvider(&TokenREST{})
var _ = rest.SubresourceObjectMetaPreserver(&TokenREST{})

var gvk = schema.GroupVersionKind{
	Group:   authenticationapiv1.SchemeGroupVersion.Group,
	Version: authenticationapiv1.SchemeGroupVersion.Version,
	Kind:    "TokenRequest",
}

// Create 是 TokenRequest 主流程：校验 URL/ServiceAccount → 规范化 Audience/UID → 检查 BoundObjectRef → 校验并签发 Token。
func (r *TokenREST) Create(ctx context.Context, name string, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	req := obj.(*authenticationapi.TokenRequest)

	// 从 context 中取出 namespace（由 URL 解析写入）。
	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	// 如果请求体里指定了 name/namespace，必须与 URL 中的保持一致
	if len(req.Name) > 0 && req.Name != name {
		errs := field.ErrorList{field.Invalid(field.NewPath("metadata").Child("name"), req.Name, "must match the service account name if specified")}
		return nil, errors.NewInvalid(gvk.GroupKind(), name, errs)
	}
	if len(req.Namespace) > 0 && req.Namespace != namespace {
		errs := field.ErrorList{field.Invalid(field.NewPath("metadata").Child("namespace"), req.Namespace, "must match the service account namespace if specified")}
		return nil, errors.NewInvalid(gvk.GroupKind(), name, errs)
	}

	// 先读取目标 ServiceAccount；后续 token subject、UID 和绑定对象校验都以这份服务端对象为准。
	svcacctObj, err := r.svcaccts.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	svcacct := svcacctObj.(*api.ServiceAccount)

	attestations := req.Spec.Attestations

	// ServiceAccount UID 不匹配意味着同名账号可能已被删除重建；启用校验特性时直接拒绝旧请求。
	if len(req.UID) > 0 && req.UID != svcacct.UID {
		if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.TokenRequestServiceAccountUIDValidation) {
			return nil, errors.NewConflict(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, name, fmt.Errorf("the UID in the token request (%s) does not match the UID of the service account (%s)", req.UID, svcacct.UID))
		} else {
			audit.AddAuditAnnotation(ctx, "authentication.k8s.io/token-request-uid-mismatch", fmt.Sprintf("the UID in the token request (%s) does not match the UID of the service account (%s)", req.UID, svcacct.UID))
		}
	}

	// 未指定 Audience 时采用 API Server 配置；显式指定时后续会与服务器受众及绑定 Webhook 配置共同校验。
	if len(req.Spec.Audiences) == 0 {
		req.Spec.Audiences = r.auds
	}
	// 元数据字段未设置时补全
	if len(req.Name) == 0 {
		req.Name = svcacct.Name
	}
	if len(req.Namespace) == 0 {
		req.Namespace = svcacct.Namespace
	}
	if len(req.UID) == 0 {
		req.UID = svcacct.UID
	} else if req.UID != svcacct.UID {
		warning.AddWarning(ctx, "", fmt.Sprintf("the UID in the token request (%s) does not match the UID of the service account (%s) but TokenRequestServiceAccountUIDValidation is not enabled. In the future, this will return a conflict error", req.UID, svcacct.UID))
	}

	// 在生成 Claims 前固定当前时间，保证 Token 中 exp 与响应里的 ExpirationTimestamp 使用同一个时间基准。
	nowTime := time.Now()
	req.CreationTimestamp = metav1.NewTime(nowTime)

	// 清空 status
	req.Status = authenticationapi.TokenRequestStatus{}

	// 先执行 TokenRequest 静态字段校验，再运行 API Server 的 Admission 校验链。
	if errs := authenticationvalidation.ValidateTokenRequest(req); len(errs) != 0 {
		return nil, errors.NewInvalid(gvk.GroupKind(), "", errs)
	}
	if createValidation != nil {
		if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	var (
		pod        *api.Pod
		node       *api.Node
		secret     *api.Secret
		validating *admissionregistrationv1.ValidatingWebhookConfiguration
		mutating   *admissionregistrationv1.MutatingWebhookConfiguration
	)

	// BoundObjectRef 将 Token 绑定到具体对象：验证对象存在、类型受支持、ServiceAccount 关系及 UID 前置条件。
	if ref := req.Spec.BoundObjectRef; ref != nil {
		var uid types.UID

		// 根据 GVK 分派 Pod、Node、Secret 或受 Feature Gate 控制的 Webhook 绑定校验。
		gvk := schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)
		switch {
		case gvk.Group == "" && gvk.Kind == "Pod":
			newCtx := newContext(ctx, "pods", ref.Name, namespace, gvk)
			podObj, err := r.pods.Get(newCtx, ref.Name, &metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			pod = podObj.(*api.Pod)
			if name != pod.Spec.ServiceAccountName {
				return nil, errors.NewBadRequest(fmt.Sprintf("cannot bind token for serviceaccount %q to pod running with different serviceaccount name.", name))
			}
			uid = pod.UID
			if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenPodNodeInfo) {
				if nodeName := pod.Spec.NodeName; nodeName != "" {
					newCtx := newContext(ctx, "nodes", nodeName, "", api.SchemeGroupVersion.WithKind("Node"))
					// 设置 ResourceVersion=0，使该对象可以从 apiserver 的 watch 缓存中读取并提供
					nodeObj, err := r.nodes.Get(newCtx, nodeName, &metav1.GetOptions{ResourceVersion: "0"})
					if err != nil {
						nodeObj, err = r.nodes.Get(newCtx, nodeName, &metav1.GetOptions{}) // 任何错误都回退为实时查询
					}
					switch {
					case errors.IsNotFound(err):
						// 如果被引用的 Node 对象不存在，仍然把 Pod 名称写入 claims，
						// 这样客户端在检查 token 时至少能知道 Pod 被调度到了哪个节点，
						// 即使其中没有 UID。
						klog.V(4).ErrorS(err, "failed fetching node for pod", "pod", klog.KObj(pod), "podUID", pod.UID, "nodeName", nodeName)
						node = &api.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
					case err != nil:
						return nil, errors.NewInternalError(err)
					default:
						node = nodeObj.(*api.Node)
					}
				}
			}
		case gvk.Group == "" && gvk.Kind == "Node":
			if !utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenNodeBinding) {
				return nil, errors.NewBadRequest(fmt.Sprintf("cannot bind token to a Node object as the %q feature-gate is disabled", features.ServiceAccountTokenNodeBinding))
			}
			newCtx := newContext(ctx, "nodes", ref.Name, "", gvk)
			nodeObj, err := r.nodes.Get(newCtx, ref.Name, &metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			node = nodeObj.(*api.Node)
			uid = node.UID
		case gvk.Group == "" && gvk.Kind == "Secret":
			newCtx := newContext(ctx, "secrets", ref.Name, namespace, gvk)
			secretObj, err := r.secrets.Get(newCtx, ref.Name, &metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			secret = secretObj.(*api.Secret)
			uid = secret.UID
		case gvk.GroupVersion() == admissionregistrationv1.SchemeGroupVersion && (gvk.Kind == "ValidatingWebhookConfiguration" || gvk.Kind == "MutatingWebhookConfiguration"): // TODO(enj): 其他引用的 version 似乎被忽略了，这一点值得商榷……
			if !utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerWebhookAuthenticationToken) {
				return nil, errors.NewBadRequest(fmt.Sprintf("cannot bind token to object of type %s (feature gate %s is disabled)", gvk.String(), genericfeatures.APIServerWebhookAuthenticationToken))
			}

			if r.hasAnyKubeAudiences(req.Spec.Audiences) {
				return nil, errors.NewBadRequest("api server audiences are invalid for webhook token requests")
			}

			newCtx, err := getNewWebhookCtx(ctx, gvk, ref.Name)
			if err != nil {
				return nil, errors.NewInternalError(err)
			}

			// 校验已保证这两个取值都存在且只有一个值
			admissionReviewAPIGroup := attestations[authenticationapi.AttestationAdmissionReviewAPIGroups][0]
			audience := req.Spec.Audiences[0]

			req.Spec.ExpirationSeconds = min(req.Spec.ExpirationSeconds, maxAdmissionReviewWebhookTokenExpirationSeconds)

			if err := r.authorizeAdmissionWebhookAuthnTokenRequest(newCtx, svcacct, admissionReviewAPIGroup); err != nil {
				return nil, err
			}

			switch gvk.Kind {
			case "ValidatingWebhookConfiguration":
				validating, uid, err = getAndValidateWebhookConfig(newCtx, ref.Name, audience, admissionReviewAPIGroup, r.validatingWebhooks, nowTime)
			case "MutatingWebhookConfiguration":
				mutating, uid, err = getAndValidateWebhookConfig(newCtx, ref.Name, audience, admissionReviewAPIGroup, r.mutatingWebhooks, nowTime)
			default:
				return nil, errors.NewInternalError(fmt.Errorf("unhandled kind"))
			}
			if err != nil {
				klog.V(4).ErrorS(err, "validation for bound webhook failed", "kind", gvk.Kind)
				return nil, errors.NewForbidden(schema.GroupResource{Group: "", Resource: "serviceaccounts/token"}, svcacct.Name, fmt.Errorf("token request denied"))
			}
		default:
			return nil, errors.NewBadRequest(fmt.Sprintf("cannot bind token to object of type %s", gvk.String()))
		}
		if ref.UID != "" && uid != ref.UID {
			return nil, errors.NewConflict(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, ref.Name, fmt.Errorf("the UID in the bound object reference (%s) does not match the UID in record. The object might have been deleted and then recreated", ref.UID))
		}
	}

	// 把请求时长限制在服务器最大值内；后续还会处理 Pod 绑定 Token 的安全过渡期延长策略。
	if r.maxExpirationSeconds > 0 && req.Spec.ExpirationSeconds > r.maxExpirationSeconds {
		// 只有正数才合法
		warning.AddWarning(ctx, "", fmt.Sprintf("requested expiration of %d seconds shortened to %d seconds", req.Spec.ExpirationSeconds, r.maxExpirationSeconds))
		req.Spec.ExpirationSeconds = r.maxExpirationSeconds
	}

	// 为了让投影的 ServiceAccount token 平滑过渡，对过期时间做调整：
	// 超过请求的过期时间只告警而不失败；
	// 超过硬编码的延长过期时间则失败；
	// 且只对绑定到 Pod 的 token 做延长。
	var warnAfter int64
	exp := req.Spec.ExpirationSeconds
	if r.extendExpiration && pod != nil && req.Spec.ExpirationSeconds == token.WarnOnlyBoundTokenExpirationSeconds && r.isKubeAudiences(req.Spec.Audiences) {
		warnAfter = exp
		exp = r.maxExtendedExpirationSeconds
	}

	// 汇总 ServiceAccount 与绑定对象信息生成标准/私有 Claims，再交给配置的签发器生成实际 JWT。
	sc, pc, err := token.Claims(*svcacct, pod, secret, node, validating, mutating, exp, warnAfter, req.Spec.Audiences, attestations)
	if err != nil {
		return nil, err
	}
	tokdata, err := r.issuer.GenerateToken(ctx, sc, pc)
	if err != nil {
		return nil, errors.NewInternalError(fmt.Errorf("failed to generate token: %v", err))
	}

	// 填充 status
	out := req.DeepCopy()
	out.Status = authenticationapi.TokenRequestStatus{
		Token:               tokdata,
		ExpirationTimestamp: metav1.Time{Time: nowTime.Add(time.Duration(out.Spec.ExpirationSeconds) * time.Second)},
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenJTI) && len(sc.ID) > 0 {
		audit.AddAuditAnnotation(ctx, serviceaccount.IssuedCredentialIDAuditAnnotationKey, authenticationtokenjwt.CredentialIDForJTI(sc.ID))
	}
	return out, nil
}

// authorizeAdmissionWebhookAuthnTokenRequest 要求 ServiceAccount 对目标 AdmissionReview API Group 具有 attest 权限。
func (r *TokenREST) authorizeAdmissionWebhookAuthnTokenRequest(ctx context.Context, sa *api.ServiceAccount, admissionReviewAPIGroup string) error {
	attributes := authorizer.AttributesRecord{
		User: (&serviceaccount.ServiceAccountInfo{
			Name:      sa.Name,
			Namespace: sa.Namespace,
			UID:       string(sa.UID),
		}).UserInfo(),
		Verb:            "attest",
		APIVersion:      "*",
		APIGroup:        "authentication.k8s.io",
		Resource:        authenticationapi.AttestationAdmissionReviewAPIGroups,
		Name:            admissionReviewAPIGroup,
		ResourceRequest: true,
	}

	authorized, reason, err := r.authorizer.Authorize(ctx, attributes)
	if authorized == authorizer.DecisionAllow {
		return nil
	}

	msg := reason
	switch {
	case err != nil && len(reason) > 0:
		msg = fmt.Sprintf("%v: %s", err, reason)
	case err != nil:
		msg = err.Error()
	}

	return responsewriters.ForbiddenStatusError(attributes, msg)
}

func (r *TokenREST) GroupVersionKind(schema.GroupVersion) schema.GroupVersionKind {
	return gvk
}

func getNewWebhookCtx(ctx context.Context, gvk schema.GroupVersionKind, refName string) (context.Context, error) {
	var newCtx context.Context
	switch gvk.Kind {
	case "ValidatingWebhookConfiguration":
		newCtx = newContext(ctx, "validatingwebhookconfigurations", refName, "", gvk)
	case "MutatingWebhookConfiguration":
		newCtx = newContext(ctx, "mutatingwebhookconfigurations", refName, "", gvk)
	default:
		return nil, fmt.Errorf("unhandled kind")
	}
	return newCtx, nil
}

// newContext 返回 ctx 的副本，其中写入了新的 RequestInfo。
func newContext(ctx context.Context, resource, name, namespace string, gvk schema.GroupVersionKind) context.Context {
	newInfo := genericapirequest.RequestInfo{
		IsResourceRequest: true,
		Verb:              "get",
		Namespace:         namespace,
		Resource:          resource,
		Name:              name,
		Parts:             []string{resource, name},
		APIGroup:          gvk.Group,
		APIVersion:        gvk.Version,
	}
	return genericapirequest.WithRequestInfo(ctx, &newInfo)
}

// 如果 tokenAudiences 是 apiserver audiences 的真子集，isKubeAudiences 返回 true。
func (r *TokenREST) isKubeAudiences(tokenAudience []string) bool {
	// tokenAudiences 必须是 apiserver audiences 的真子集
	return r.audsSet.HasAll(tokenAudience...)
}

// 如果 tokenAudiences 中包含任意 apiserver audience，hasAnyKubeAudiences 返回 true。
func (r *TokenREST) hasAnyKubeAudiences(tokenAudience []string) bool {
	return r.audsSet.HasAny(tokenAudience...)
}

// PreserveRequestObjectMetaSystemFieldsOnSubresourceCreate 表示在创建子资源时，
// 需要保留 TokenRequest 的 UID。
func (r *TokenREST) PreserveRequestObjectMetaSystemFieldsOnSubresourceCreate() bool {
	return true
}

type whFieldGetter[T webhook] interface {
	get(ctx context.Context, name string, now time.Time) (T, iter.Seq[*webhookFields], error)
}

type webhookFields struct {
	config *admissionregistrationv1.WebhookClientConfig
	rules  []admissionregistrationv1.RuleWithOperations
}

type vwhGetter struct {
	validatingWebhooks webhookGetter[*admissionregistrationv1.ValidatingWebhookConfiguration]
}

func (v *vwhGetter) get(ctx context.Context, name string, now time.Time) (*admissionregistrationv1.ValidatingWebhookConfiguration, iter.Seq[*webhookFields], error) {
	wh, err := v.validatingWebhooks.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}

	if wh.DeletionTimestamp != nil && now.After(wh.DeletionTimestamp.Time) {
		return nil, nil, fmt.Errorf("deletion timestamp has passed for validating webhook configuration %q", wh.Name)
	}

	return wh, func(yield func(*webhookFields) bool) {
		for _, hook := range wh.Webhooks {
			if !yield(&webhookFields{config: &hook.ClientConfig, rules: hook.Rules}) {
				return
			}
		}
	}, nil
}

type mwhGetter struct {
	mutatingWebhooks webhookGetter[*admissionregistrationv1.MutatingWebhookConfiguration]
}

func (m *mwhGetter) get(ctx context.Context, name string, now time.Time) (*admissionregistrationv1.MutatingWebhookConfiguration, iter.Seq[*webhookFields], error) {
	wh, err := m.mutatingWebhooks.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}

	if wh.DeletionTimestamp != nil && now.After(wh.DeletionTimestamp.Time) {
		return nil, nil, fmt.Errorf("deletion timestamp has passed for mutating webhook configuration %q", wh.Name)
	}
	return wh, func(yield func(*webhookFields) bool) {
		for _, hook := range wh.Webhooks {
			if !yield(&webhookFields{config: &hook.ClientConfig, rules: hook.Rules}) {
				return
			}
		}
	}, nil
}

// getAndValidateWebhookConfig 确认 Webhook 配置尚未进入删除时间，并至少有一个 Webhook 同时匹配 Audience 和被证明的 API Group。
func getAndValidateWebhookConfig[T webhook](ctx context.Context, name, audience, admissionReviewAPIGroup string, getter whFieldGetter[T], now time.Time) (T, types.UID, error) {
	obj, whs, err := getter.get(ctx, name, now)
	if err != nil {
		return nil, "", err
	}

	// "*" 表示该 token 对所有 API 组有效，跳过检查。
	// 我们预期只有 kube-apiserver 会被授权请求该取值。
	if admissionReviewAPIGroup == "*" {
		return obj, obj.GetUID(), nil
	}

	// 检查配置中是否存在同时匹配所请求的 admissionReviewAPIGroup 与 audience 的 Webhook
	for wh := range whs {
		if validateAttestationAPIGroup(wh, admissionReviewAPIGroup) && validateWebhookAudience(wh, audience) {
			return obj, obj.GetUID(), nil
		}
	}

	return nil, "", fmt.Errorf("no matching webhook found in config %q", name)
}

// validateWebhookAudience 检查请求的 audience 是否至少与所匹配 Webhook 的 client config 一致：
// 用 URL 配置的 Webhook，audience 必须与 URL 完全相同；
// 用 Service 配置的 Webhook，audience 必须是 https://$name.$namespace.svc:$port[/$path]。
// 注意 Webhook 期望的 audience 与 Service 配置之间，结尾斜杠也要保持一致。
func validateWebhookAudience(hook *webhookFields, audience string) bool {
	cc := hook.config
	if cc.URL != nil && audience == *cc.URL {
		return true
	}
	if cc.Service != nil {
		port := int32(443)
		if cc.Service.Port != nil {
			port = *cc.Service.Port
		}
		path := "/"
		if cc.Service.Path != nil {
			path = *cc.Service.Path
			if !strings.HasPrefix(path, "/") {
				path = "/" + path
			}
		}
		svcAud := fmt.Sprintf("https://%s.%s.svc:%d%s", cc.Service.Name, cc.Service.Namespace, port, path)

		if audience == svcAud {
			return true
		}
	}
	return false
}

// validateAttestationAPIGroup 检查所声明的 API Group 是否与 Webhook 中至少一条 Rule 匹配。
// attestation 值为 "*" 的 admissionReviewAPIGroup 会跳过该检查，因为它代表所有 API Group；
// Rule 的 APIGroups 中的 "*" 可以匹配任意声明值。这里假定 matchConditions 中的 CEL 表达式
// 能够正确匹配本次 TokenRequest 对应的 AdmissionReview 请求。
func validateAttestationAPIGroup(hook *webhookFields, attestedGroup string) bool {
	for _, rule := range hook.rules {
		for _, group := range rule.APIGroups {
			if group == "*" || group == attestedGroup {
				return true
			}
		}
	}

	// 聚合 API Server 永远不需要跨组的等价 audience，因此这里
	// 不检查 equivalents。

	// 走到这里说明：a）没有任何 rules，或 b）有 rules 但没有一条匹配。
	// 如果没有 rules，该调用永远无法匹配，
	// 因此应当拒绝该请求。
	return false
}
