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

// Package serviceaccount 提供 ServiceAccount Token 的生成、验证和 OIDC 发现功能。
// 主要功能：
//   - 构建和验证 JWT Token 的 Claims（支持绑定 Pod、Secret、Node、Webhook 等对象）
//   - 使用 RSA/ECDSA/OpaqueSigner 生成签名的 JWT Token
//   - 验证 JWT Token 的签名、issuer、audience 和绑定对象状态
//   - 处理旧版 Secret-based Token 的验证和弃用迁移
//   - 提供 OIDC Discovery 端点的元数据和 JWKS
//   - 支持外部 JWT 签名器（external-jwt-signer）插件
//   - 记录 Token 使用相关的指标（legacy、stale、valid 等）
package serviceaccount

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gopkg.in/go-jose/go-jose.v2/jwt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/audit"
	apiserverserviceaccount "k8s.io/apiserver/pkg/authentication/serviceaccount"
	authenticationtokenjwt "k8s.io/apiserver/pkg/authentication/token/jwt"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/authentication"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// WarnOnlyBoundTokenExpirationSeconds 是注入的绑定 ServiceAccount Token 的过期时间。
	// 60*60+7 秒（略大于 1 小时）用于触发 projected token 的时效性监控。
	// 比标准过期时间稍长，用于安全滚动更新期间的过渡期。
	WarnOnlyBoundTokenExpirationSeconds = 60*60 + 7

	// ExpirationExtensionSeconds 是为安全滚动更新期间修改的 Token 扩展的过期时间。
	// 24*365 秒（约 1 年）确保旧 Pod 在新 token 完全部署后仍有足够时间使用旧 token。
	ExpirationExtensionSeconds = 24 * 365 * 60 * 60
)

var (
	// now 是 time.Now 的 stub，用于测试时注入固定时间
	now = time.Now
	// newUUID 是 uuid.NewString 的 stub，用于测试时注入固定 UUID
	newUUID = uuid.NewString
)

// privateClaims 是 JWT Token 中 Kubernetes 专有的 claim 结构。
// 通过 "kubernetes.io" 命名空间避免与标准 JWT claim 冲突。
type privateClaims struct {
	Kubernetes kubernetes `json:"kubernetes.io,omitempty"`
}

// kubernetes 包含 JWT Token 中所有 Kubernetes 相关的 claim。
// 支持绑定多种对象类型（Pod、Secret、Node、Webhook），但 Token 只能绑定一种对象类型。
type kubernetes struct {
	Namespace                      string                                     `json:"namespace,omitempty"`
	Svcacct                        ref                                        `json:"serviceaccount,omitempty"`
	Pod                            *ref                                       `json:"pod,omitempty"`
	Secret                         *ref                                       `json:"secret,omitempty"`
	Node                           *ref                                       `json:"node,omitempty"`
	ValidatingWebhookConfiguration *ref                                       `json:"validatingwebhookconfiguration,omitempty"`
	MutatingWebhookConfiguration   *ref                                       `json:"mutatingwebhookconfiguration,omitempty"`
	Attestations                   map[string]authentication.AttestationValue `json:"attestations,omitempty"`
	WarnAfter                      *jwt.NumericDate                           `json:"warnafter,omitempty"`
}

// ref 表示对 Kubernetes 对象的引用，包含名称和 UID。
// 用于验证 Token 绑定的对象是否仍然存在且 UID 匹配。
type ref struct {
	Name string `json:"name,omitempty"`
	UID  string `json:"uid,omitempty"`
}

// Claims 构建 ServiceAccount Token 的公共和私有 Claims。
// 支持将 Token 绑定到 Pod、Secret、Node 或 Webhook 对象，但只能绑定一种类型。
// 绑定对象的删除会使 Token 失效，实现细粒度的 Token 生命周期管理。
// 返回 jwt.Claims（标准 claim）和 privateClaims（Kubernetes 专有 claim）。
func Claims(sa core.ServiceAccount, pod *core.Pod, secret *core.Secret, node *core.Node, validating *admissionregistrationv1.ValidatingWebhookConfiguration, mutating *admissionregistrationv1.MutatingWebhookConfiguration, expirationSeconds, warnafter int64, audience []string, attestations map[string]authentication.AttestationValue) (*jwt.Claims, interface{}, error) {
	now := now()
	sc := &jwt.Claims{
		Subject:   apiserverserviceaccount.MakeUsername(sa.Namespace, sa.Name),
		Audience:  jwt.Audience(audience),
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now),
		Expiry:    jwt.NewNumericDate(now.Add(time.Duration(expirationSeconds) * time.Second)),
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenJTI) {
		sc.ID = newUUID()
	}
	pc := &privateClaims{
		Kubernetes: kubernetes{
			Namespace: sa.Namespace,
			Svcacct: ref{
				Name: sa.Name,
				UID:  string(sa.UID),
			},
			Attestations: attestations,
		},
	}

	if secret != nil && (node != nil || pod != nil) {
		return nil, nil, fmt.Errorf("internal error, token can only be bound to one object type")
	}
	switch {
	case pod != nil:
		pc.Kubernetes.Pod = &ref{
			Name: pod.Name,
			UID:  string(pod.UID),
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenPodNodeInfo) {
			// if this is bound to a pod and the node information is available, persist that too
			if node != nil {
				pc.Kubernetes.Node = &ref{
					Name: node.Name,
					UID:  string(node.UID),
				}
			}
		}
	case secret != nil:
		pc.Kubernetes.Secret = &ref{
			Name: secret.Name,
			UID:  string(secret.UID),
		}
	case node != nil:
		if !utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenNodeBinding) {
			return nil, nil, fmt.Errorf("token bound to Node object requested, but %q feature gate is disabled", features.ServiceAccountTokenNodeBinding)
		}
		pc.Kubernetes.Node = &ref{
			Name: node.Name,
			UID:  string(node.UID),
		}
	case validating != nil:
		pc.Kubernetes.ValidatingWebhookConfiguration = &ref{
			Name: validating.Name,
			UID:  string(validating.UID),
		}
	case mutating != nil:
		pc.Kubernetes.MutatingWebhookConfiguration = &ref{
			Name: mutating.Name,
			UID:  string(mutating.UID),
		}
	}

	if warnafter != 0 {
		pc.Kubernetes.WarnAfter = jwt.NewNumericDate(now.Add(time.Duration(warnafter) * time.Second))
	}

	return sc, pc, nil
}

// NewValidator 创建 ServiceAccount Token 的验证器。
// 验证器检查 Token 的时间有效性、绑定对象的存在性和 UID 匹配。
func NewValidator(getter ServiceAccountTokenGetter) Validator[privateClaims] {
	return &validator{
		getter: getter,
	}
}

// validator 实现 Validator[privateClaims] 接口，验证 ServiceAccount Token 的 claims。
// 不仅验证 JWT 标准 claim（过期时间、签发时间），还验证 Kubernetes 专有 claim（绑定对象状态）。
type validator struct {
	getter ServiceAccountTokenGetter
}

var _ = Validator[privateClaims](&validator{})

// Validate 验证 ServiceAccount Token 的完整有效性。
// 包括：JWT 标准 claim 验证（过期、签发时间）、ServiceAccount 存在性和 UID 匹配、
// 绑定对象（Pod/Secret/Node/Webhook）存在性和 UID 匹配、warnafter 阈值检查。
// 如果绑定对象已删除（考虑 leeway 时间窗口），Token 失效。
func (v *validator) Validate(ctx context.Context, _ string, public *jwt.Claims, private *privateClaims) (*apiserverserviceaccount.ServiceAccountInfo, error) {
	nowTime := now()
	err := public.Validate(jwt.Expected{
		Time: nowTime,
	})
	switch err {
	case nil:
		// successful validation

	case jwt.ErrExpired:
		return nil, errors.New("service account token has expired")

	case jwt.ErrNotValidYet:
		return nil, errors.New("service account token is not valid yet")

	case jwt.ErrIssuedInTheFuture:
		return nil, errors.New("service account token is issued in the future")

	// our current use of jwt.Expected above should make these cases impossible to hit
	case jwt.ErrInvalidAudience, jwt.ErrInvalidID, jwt.ErrInvalidIssuer, jwt.ErrInvalidSubject:
		klog.Errorf("service account token claim validation got unexpected validation failure: %v", err)
		return nil, fmt.Errorf("service account token claims could not be validated: %w", err) // safe to pass these errors back to the user

	default:
		klog.Errorf("service account token claim validation got unexpected error type: %T", err)                         // avoid leaking unexpected information into the logs
		return nil, errors.New("service account token claims could not be validated due to unexpected validation error") // return an opaque error
	}

	// consider things deleted prior to now()-leeway to be invalid
	invalidIfDeletedBefore := nowTime.Add(-jwt.DefaultLeeway)
	namespace := private.Kubernetes.Namespace
	saref := private.Kubernetes.Svcacct
	podref := private.Kubernetes.Pod
	noderef := private.Kubernetes.Node
	secref := private.Kubernetes.Secret
	valref := private.Kubernetes.ValidatingWebhookConfiguration
	mutref := private.Kubernetes.MutatingWebhookConfiguration
	// Make sure service account still exists (name and UID)
	serviceAccount, err := v.getter.GetServiceAccount(ctx, namespace, saref.Name)
	if err != nil {
		klog.V(4).Infof("Could not retrieve service account %s/%s: %v", namespace, saref.Name, err)
		return nil, err
	}

	if string(serviceAccount.UID) != saref.UID {
		klog.V(4).Infof("Service account UID no longer matches %s/%s: %q != %q", namespace, saref.Name, string(serviceAccount.UID), saref.UID)
		return nil, fmt.Errorf("service account UID (%s) does not match claim (%s)", serviceAccount.UID, saref.UID)
	}
	if serviceAccount.DeletionTimestamp != nil && serviceAccount.DeletionTimestamp.Time.Before(invalidIfDeletedBefore) {
		klog.V(4).Infof("Service account has been deleted %s/%s", namespace, saref.Name)
		return nil, fmt.Errorf("service account %s/%s has been deleted", namespace, saref.Name)
	}

	if secref != nil {
		// Make sure token hasn't been invalidated by deletion of the secret
		secret, err := v.getter.GetSecret(ctx, namespace, secref.Name)
		if err != nil {
			klog.V(4).Infof("Could not retrieve bound secret %s/%s for service account %s/%s: %v", namespace, secref.Name, namespace, saref.Name, err)
			return nil, errors.New("service account token has been invalidated")
		}
		if secref.UID != string(secret.UID) {
			klog.V(4).Infof("Secret UID no longer matches %s/%s: %q != %q", namespace, secref.Name, string(secret.UID), secref.UID)
			return nil, fmt.Errorf("secret UID (%s) does not match service account secret ref claim (%s)", secret.UID, secref.UID)
		}
		if secret.DeletionTimestamp != nil && secret.DeletionTimestamp.Time.Before(invalidIfDeletedBefore) {
			klog.V(4).Infof("Bound secret is deleted and awaiting removal: %s/%s for service account %s/%s", namespace, secref.Name, namespace, saref.Name)
			return nil, errors.New("service account token has been invalidated")
		}
	}

	var podName, podUID string
	if podref != nil {
		// Make sure token hasn't been invalidated by deletion of the pod
		pod, err := v.getter.GetPod(ctx, namespace, podref.Name)
		if err != nil {
			klog.V(4).Infof("Could not retrieve bound pod %s/%s for service account %s/%s: %v", namespace, podref.Name, namespace, saref.Name, err)
			return nil, errors.New("service account token has been invalidated")
		}
		if podref.UID != string(pod.UID) {
			klog.V(4).Infof("Pod UID no longer matches %s/%s: %q != %q", namespace, podref.Name, string(pod.UID), podref.UID)
			return nil, fmt.Errorf("pod UID (%s) does not match service account pod ref claim (%s)", pod.UID, podref.UID)
		}
		if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Time.Before(invalidIfDeletedBefore) {
			klog.V(4).Infof("Bound pod is deleted and awaiting removal: %s/%s for service account %s/%s", namespace, podref.Name, namespace, saref.Name)
			return nil, errors.New("service account token has been invalidated")
		}
		podName = podref.Name
		podUID = podref.UID
	}

	var nodeName, nodeUID string
	if noderef != nil {
		switch {
		case podref != nil:
			if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenPodNodeInfo) {
				// for pod-bound tokens, just extract the node claims
				nodeName = noderef.Name
				nodeUID = noderef.UID
			}
		case podref == nil:
			if !utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenNodeBindingValidation) {
				klog.V(4).Infof("ServiceAccount token is bound to a Node object, but the node bound token validation feature is disabled")
				return nil, fmt.Errorf("token is bound to a Node object but the %s feature gate is disabled", features.ServiceAccountTokenNodeBindingValidation)
			}

			node, err := v.getter.GetNode(ctx, noderef.Name)
			if err != nil {
				klog.V(4).Infof("Could not retrieve node object %q for service account %s/%s: %v", noderef.Name, namespace, saref.Name, err)
				return nil, errors.New("service account token has been invalidated")
			}
			if noderef.UID != string(node.UID) {
				klog.V(4).Infof("Node UID no longer matches %s: %q != %q", noderef.Name, string(node.UID), noderef.UID)
				return nil, fmt.Errorf("node UID (%s) does not match service account node ref claim (%s)", node.UID, noderef.UID)
			}
			if node.DeletionTimestamp != nil && node.DeletionTimestamp.Time.Before(invalidIfDeletedBefore) {
				klog.V(4).Infof("Node %q is deleted and awaiting removal for service account %s/%s", node.Name, namespace, saref.Name)
				return nil, errors.New("service account token has been invalidated")
			}
			nodeName = noderef.Name
			nodeUID = noderef.UID
		}
	}

	var (
		validatingName, validatingUID       string
		mutatingName, mutatingUID           string
		attestationAdmissionReviewAPIGroups []string
	)
	if valref != nil || mutref != nil {
		if !utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerWebhookAuthenticationToken) {
			return nil, fmt.Errorf("feature gate %q is disabled, but token is webhook bound", genericfeatures.APIServerWebhookAuthenticationToken)
		}

		attestationAdmissionReviewAPIGroups = private.Kubernetes.Attestations[authentication.AttestationAdmissionReviewAPIGroups]
		if len(attestationAdmissionReviewAPIGroups) == 0 {
			return nil, fmt.Errorf("bound object was a webhook config, but %q attestation is missing", authentication.AttestationAdmissionReviewAPIGroups)
		}
	}

	if valref != nil {
		validatingName, validatingUID, err = validateWebhookCfgRef(ctx, v.getValidatingWebhookMeta, "ValidatingWebhookConfiguration", &private.Kubernetes, valref, invalidIfDeletedBefore)
		if err != nil {
			return nil, err
		}
	}
	if mutref != nil {
		mutatingName, mutatingUID, err = validateWebhookCfgRef(ctx, v.getMutatingWebhookMeta, "MutatingWebhookConfiguration", &private.Kubernetes, mutref, invalidIfDeletedBefore)
		if err != nil {
			return nil, err
		}
	}

	// Check special 'warnafter' field for projected service account token transition.
	warnafter := private.Kubernetes.WarnAfter
	if warnafter != nil && *warnafter != 0 {
		if nowTime.After(warnafter.Time()) {
			secondsAfterWarn := nowTime.Unix() - warnafter.Time().Unix()
			auditInfo := fmt.Sprintf("subject: %s, seconds after warning threshold: %d", public.Subject, secondsAfterWarn)
			audit.AddAuditAnnotation(ctx, "authentication.k8s.io/stale-token", auditInfo)
			staleTokensTotal.WithContext(ctx).Inc()
		} else {
			validTokensTotal.WithContext(ctx).Inc()
		}
	}

	var jti string
	if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenJTI) {
		jti = public.ID
	}
	return &apiserverserviceaccount.ServiceAccountInfo{
		Namespace:                           private.Kubernetes.Namespace,
		Name:                                private.Kubernetes.Svcacct.Name,
		UID:                                 private.Kubernetes.Svcacct.UID,
		PodName:                             podName,
		PodUID:                              podUID,
		NodeName:                            nodeName,
		NodeUID:                             nodeUID,
		AttestationAdmissionReviewAPIGroups: attestationAdmissionReviewAPIGroups,
		ValidatingWebhookConfigurationName:  validatingName,
		ValidatingWebhookConfigurationUID:   validatingUID,
		MutatingWebhookConfigurationName:    mutatingName,
		MutatingWebhookConfigurationUID:     mutatingUID,
		CredentialID:                        authenticationtokenjwt.CredentialIDForJTI(jti),
	}, nil
}

func (v *validator) getValidatingWebhookMeta(ctx context.Context, name string) (metav1.ObjectMeta, error) {
	obj, err := v.getter.GetValidatingWebhookConfiguration(ctx, name)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}
	return obj.ObjectMeta, nil
}

func (v *validator) getMutatingWebhookMeta(ctx context.Context, name string) (metav1.ObjectMeta, error) {
	obj, err := v.getter.GetMutatingWebhookConfiguration(ctx, name)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}
	return obj.ObjectMeta, nil
}

// getter 是获取 Webhook 配置 ObjectMeta 的函数类型，用于 validateWebhookCfgRef 的泛型参数。
type getter func(ctx context.Context, name string) (metav1.ObjectMeta, error)

// validateWebhookCfgRef 验证 Webhook 配置对象的引用。
// 检查对象是否存在、UID 是否匹配、以及是否已被删除。
// 用于 ValidatingWebhookConfiguration 和 MutatingWebhookConfiguration 的通用验证逻辑。
func validateWebhookCfgRef(ctx context.Context, get getter, kind string, private *kubernetes, ref *ref, invalidIfDeletedBefore time.Time) (string, string, error) {
	obj, err := get(ctx, ref.Name)
	if err != nil {
		klog.V(4).Infof("Could not retrieve %s object %q for service account %s/%s: %v", kind, ref.Name, private.Namespace, private.Svcacct.Name, err)
		return "", "", errors.New("service account token has been invalidated")
	}
	if objUID := string(obj.UID); ref.UID != objUID {
		klog.V(4).Infof("%s UID no longer matches %s: %q != %q", kind, ref.Name, objUID, ref.UID)
		return "", "", fmt.Errorf("%s UID (%s) does not match service account validating ref claim (%s)", kind, objUID, ref.UID)
	}
	if deltime := obj.DeletionTimestamp; deltime != nil && deltime.Time.Before(invalidIfDeletedBefore) {
		klog.V(4).Infof("%s %q is deleted and awaiting removal for service account %s/%s", kind, obj.Name, private.Namespace, private.Svcacct.Name)
		return "", "", errors.New("service account token has been invalidated")
	}
	return ref.Name, ref.UID, nil
}
