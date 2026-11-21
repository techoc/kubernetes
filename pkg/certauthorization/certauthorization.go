/*
Copyright 2020 The Kubernetes Authors.

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

package certauthorization

import (
	"context"
	"strings"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/klog/v2"
)

// IsAuthorizedForSignerName 判断 info 是否有权对名为 signerName 的虚拟 "signers" 资源执行 verb 操作。
// 如果用户没有针对该 signerName 的权限，还会用 {domain}/*（例如 kubernetes.io/*）
// 再做一次授权检查，从而支持一次性授予某个域下所有 signerName 的权限。
func IsAuthorizedForSignerName(ctx context.Context, authz authorizer.UnconditionalAuthorizer, info user.Info, verb, signerName string) bool {
	// 先检查用户是否对具体 signerName 有显式权限。
	attr := buildAttributes(info, verb, signerName)
	decision, reason, err := authz.Authorize(ctx, attr)
	switch {
	case err != nil:
		// 授权器报错时不直接拒绝：继续尝试下面的通配符路径，
		// 避免因单次检查失败就误拒本可放行的请求。
		klog.V(3).Infof("cannot authorize %q %q for policy: %v,%v", verb, attr.GetName(), reason, err)
	case decision == authorizer.DecisionAllow:
		return true
	}

	// 再检查用户是否拥有该 signerName 所属域的通配权限，
	// 例如对 kubernetes.io/example 检查 kubernetes.io/*。
	attr = buildWildcardAttributes(info, verb, signerName)
	decision, reason, err = authz.Authorize(ctx, attr)
	switch {
	case err != nil:
		klog.V(3).Infof("cannot authorize %q %q for policy: %v,%v", verb, attr.GetName(), reason, err)
	case decision == authorizer.DecisionAllow:
		return true
	}

	// 两条路径都未放行则拒绝。
	return false
}

// buildAttributes 构造针对某个具体 signerName 的授权属性。
// 使用虚拟的 "signers" 子资源和通配符 version，让 RBAC 规则可以按 signerName 精确授权。
func buildAttributes(info user.Info, verb, signerName string) authorizer.Attributes {
	return authorizer.AttributesRecord{
		User:            info,
		Verb:            verb,
		Name:            signerName,
		APIGroup:        "certificates.k8s.io",
		APIVersion:      "*",
		Resource:        "signers",
		ResourceRequest: true,
	}
}

// buildWildcardAttributes 构造针对 signerName 所属域的通配授权属性。
// 取 signerName 中第一个 "/" 前的部分作为域，拼接 "/*" 后复用 buildAttributes。
func buildWildcardAttributes(info user.Info, verb, signerName string) authorizer.Attributes {
	parts := strings.Split(signerName, "/")
	domain := parts[0]
	return buildAttributes(info, verb, domain+"/*")
}
