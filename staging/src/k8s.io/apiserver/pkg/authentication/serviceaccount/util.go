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

// Package serviceaccount 提供 ServiceAccount 用户名/组名的构造与解析工具。
// 用户名格式固定为 system:serviceaccount:<namespace>:<name>，这个格式是
// 授权策略与审计日志的稳定契约，不能随意更改。
package serviceaccount

import (
	"fmt"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apiserver/pkg/authentication/user"
)

const (
	ServiceAccountUsernamePrefix    = "system:serviceaccount:"
	ServiceAccountUsernameSeparator = ":"
	ServiceAccountGroupPrefix       = "system:serviceaccounts:"
	AllServiceAccountsGroup         = "system:serviceaccounts"
	// ServiceAccount 的 /token 端点在审计事件中写入的注解键，值为所签发 token 的凭据标识。
	// 后续请求若把该标识带入 user.extra 并记入审计日志，就能据此追溯 token 的来源。
	IssuedCredentialIDAuditAnnotationKey = "authentication.kubernetes.io/issued-credential-id"
	// 在 user.extra 中标明认证请求来自哪个 Pod 的键。
	PodNameKey = "authentication.kubernetes.io/pod-name"
	// 在 user.extra 中标明认证请求来自哪个 Pod UID 的键。
	PodUIDKey = "authentication.kubernetes.io/pod-uid"
	// 在 user.extra 中标明认证请求来自哪个节点的键。
	NodeNameKey = "authentication.kubernetes.io/node-name"
	// 在 user.extra 中标明认证请求来自哪个节点 UID 的键。
	NodeUIDKey = "authentication.kubernetes.io/node-uid"
	// 在 user.extra 中标明认证请求关联的 ValidatingWebhookConfiguration 名称的键。
	ValidatingWebhookConfigurationNameKey = "authentication.kubernetes.io/validatingwebhookconfiguration-name"
	// 在 user.extra 中标明认证请求关联的 ValidatingWebhookConfiguration UID 的键。
	ValidatingWebhookConfigurationUIDKey = "authentication.kubernetes.io/validatingwebhookconfiguration-uid"
	// 在 user.extra 中标明认证请求关联的 MutatingWebhookConfiguration 名称的键。
	MutatingWebhookConfigurationNameKey = "authentication.kubernetes.io/mutatingwebhookconfiguration-name"
	// 在 user.extra 中标明认证请求关联的 MutatingWebhookConfiguration UID 的键。
	MutatingWebhookConfigurationUIDKey = "authentication.kubernetes.io/mutatingwebhookconfiguration-uid"
	// 承载证明信息（attestation）的 user.extra 键前缀。
	AttestationKeyPrefix = "attestation.authentication.kubernetes.io/"
	// 在 user.extra 中承载 "admissionReviewAPIGroups" claim 的键。
	AttestationAdmissionReviewAPIGroupsKey = AttestationKeyPrefix + authenticationv1.AttestationAdmissionReviewAPIGroups
)

// MakeUsername 根据 namespace 与 ServiceAccount 名生成用户名。
// 生成的用户名可交给 SplitUsername 还原出原来的 namespace 与 ServiceAccount 名。
func MakeUsername(namespace, name string) string {
	return ServiceAccountUsernamePrefix + namespace + ServiceAccountUsernameSeparator + name
}

// MatchesUsername 不分配内存即可判断用户名是否匹配给定的 namespace 与 name。
// 把 ServiceAccount 的 namespace/name 与已知字符串比较时应用此函数。
//
// 逐段比较前缀而不是先拆分再拼装，避免认证热路径上产生额外分配。
func MatchesUsername(namespace, name string, username string) bool {
	if !strings.HasPrefix(username, ServiceAccountUsernamePrefix) {
		return false
	}
	username = username[len(ServiceAccountUsernamePrefix):]

	if !strings.HasPrefix(username, namespace) {
		return false
	}
	username = username[len(namespace):]

	if !strings.HasPrefix(username, ServiceAccountUsernameSeparator) {
		return false
	}
	username = username[len(ServiceAccountUsernameSeparator):]

	return username == name
}

var invalidUsernameErr = fmt.Errorf("Username must be in the form %s", MakeUsername("namespace", "name"))

// SplitUsername 从用户名中解析出 namespace 与 ServiceAccount 名；
// 若用户名不是 MakeUsername 生成的合法格式，则返回错误。
//
// 除格式外还校验 namespace 与 name 是否符合 API 命名规则：用户名可能来自用户可控
// 的输入（例如 impersonation 头），必须拒绝能构造出非法对象名的情况。
func SplitUsername(username string) (string, string, error) {
	if !strings.HasPrefix(username, ServiceAccountUsernamePrefix) {
		return "", "", invalidUsernameErr
	}
	trimmed := strings.TrimPrefix(username, ServiceAccountUsernamePrefix)
	parts := strings.Split(trimmed, ServiceAccountUsernameSeparator)
	if len(parts) != 2 {
		return "", "", invalidUsernameErr
	}
	namespace, name := parts[0], parts[1]
	if len(apimachineryvalidation.ValidateNamespaceName(namespace, false)) != 0 {
		return "", "", invalidUsernameErr
	}
	if len(apimachineryvalidation.ValidateServiceAccountName(name, false)) != 0 {
		return "", "", invalidUsernameErr
	}
	return namespace, name, nil
}

// MakeGroupNames 生成指定 namespace 下 ServiceAccount 所属的组名。
//
// 每个 ServiceAccount 同时属于全局组和本 namespace 组，RBAC 可以按其中之一授权。
func MakeGroupNames(namespace string) []string {
	return []string{
		AllServiceAccountsGroup,
		MakeNamespaceGroupName(namespace),
	}
}

// MakeNamespaceGroupName 返回该 namespace 下所有 ServiceAccount 所属的组名。
func MakeNamespaceGroupName(namespace string) string {
	return ServiceAccountGroupPrefix + namespace
}

// UserInfo 根据 namespace、ServiceAccount 名和 UID 构造 user.Info。
func UserInfo(namespace, name, uid string) user.Info {
	return (&ServiceAccountInfo{
		Name:      name,
		Namespace: namespace,
		UID:       uid,
	}).UserInfo()
}

// ServiceAccountInfo 汇总构造 user.Info 所需的 ServiceAccount 身份信息。
// 可选字段（Pod/Node/webhook/attestation 等）只在调用方确实提供时写入 Extra，
// 这样审计与授权逻辑可以据此区分 token 的来源与绑定对象。
type ServiceAccountInfo struct {
	Name, Namespace, UID                                                  string
	PodName, PodUID                                                       string
	CredentialID                                                          string
	NodeName, NodeUID                                                     string
	ValidatingWebhookConfigurationName, ValidatingWebhookConfigurationUID string
	MutatingWebhookConfigurationName, MutatingWebhookConfigurationUID     string
	AttestationAdmissionReviewAPIGroups                                   []string
}

// UserInfo 把 ServiceAccountInfo 转换为 user.Info，并填充所有已设置的 Extra 键。
func (sa *ServiceAccountInfo) UserInfo() user.Info {
	info := &user.DefaultInfo{
		Name:   MakeUsername(sa.Namespace, sa.Name),
		UID:    sa.UID,
		Groups: MakeGroupNames(sa.Namespace),
	}

	if sa.PodName != "" && sa.PodUID != "" {
		if info.Extra == nil {
			info.Extra = make(map[string][]string)
		}
		info.Extra[PodNameKey] = []string{sa.PodName}
		info.Extra[PodUIDKey] = []string{sa.PodUID}
	}
	if sa.CredentialID != "" {
		if info.Extra == nil {
			info.Extra = make(map[string][]string)
		}
		info.Extra[user.CredentialIDKey] = []string{sa.CredentialID}
	}
	if sa.NodeName != "" {
		if info.Extra == nil {
			info.Extra = make(map[string][]string)
		}
		info.Extra[NodeNameKey] = []string{sa.NodeName}
		// 节点 UID 是可选字段，仅在设置了节点名时才会带上。
		if sa.NodeUID != "" {
			info.Extra[NodeUIDKey] = []string{sa.NodeUID}
		}
	}
	if sa.ValidatingWebhookConfigurationName != "" && sa.ValidatingWebhookConfigurationUID != "" {
		if info.Extra == nil {
			info.Extra = make(map[string][]string)
		}
		info.Extra[ValidatingWebhookConfigurationNameKey] = []string{sa.ValidatingWebhookConfigurationName}
		info.Extra[ValidatingWebhookConfigurationUIDKey] = []string{sa.ValidatingWebhookConfigurationUID}
	}

	if sa.MutatingWebhookConfigurationName != "" && sa.MutatingWebhookConfigurationUID != "" {
		if info.Extra == nil {
			info.Extra = make(map[string][]string)
		}
		info.Extra[MutatingWebhookConfigurationNameKey] = []string{sa.MutatingWebhookConfigurationName}
		info.Extra[MutatingWebhookConfigurationUIDKey] = []string{sa.MutatingWebhookConfigurationUID}
	}

	if len(sa.AttestationAdmissionReviewAPIGroups) > 0 {
		if info.Extra == nil {
			info.Extra = make(map[string][]string)
		}
		info.Extra[AttestationAdmissionReviewAPIGroupsKey] = sa.AttestationAdmissionReviewAPIGroups
	}

	return info
}

// IsServiceAccountToken 判断该 Secret 是否是这个 ServiceAccount 的合法 API token。
//
// 只检查类型与注解中的 name/uid，不验证 secret 内容；用于垃圾回收时判断
// 某个旧式 token Secret 是否仍属于该 ServiceAccount。
func IsServiceAccountToken(secret *v1.Secret, sa *v1.ServiceAccount) bool {
	if secret.Type != v1.SecretTypeServiceAccountToken {
		return false
	}

	name := secret.Annotations[v1.ServiceAccountNameKey]
	uid := secret.Annotations[v1.ServiceAccountUIDKey]
	if name != sa.Name {
		// 名称必须匹配
		return false
	}
	if len(uid) > 0 && uid != string(sa.UID) {
		// 若 secret 上带了 UID，也必须匹配
		return false
	}

	return true
}
