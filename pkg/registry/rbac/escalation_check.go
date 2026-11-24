/*
Copyright 2016 The Kubernetes Authors.

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

package rbac

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/kubernetes/pkg/apis/rbac"
)

// EscalationAllowed 识别 system:masters 超级用户组；API Server 特权 loopback 身份属于此组，可绕过常规防提权限制。
func EscalationAllowed(ctx context.Context) bool {
	u, ok := genericapirequest.UserFrom(ctx)
	if !ok {
		return false
	}

	// system:masters 是特殊的超级用户组，API Server 的特权 loopback 请求也使用该组身份；
	// 因此该组成员被视为拥有不受限制的操作权限。
	for _, group := range u.GetGroups() {
		if group == user.SystemPrivilegedGroup {
			return true
		}
	}

	return false
}

var roleResources = map[schema.GroupResource]bool{
	rbac.SchemeGroupVersion.WithResource("clusterroles").GroupResource(): true,
	rbac.SchemeGroupVersion.WithResource("roles").GroupResource():        true,
}

// RoleEscalationAuthorized 检查当前用户是否被明确授予 escalate 动词；该授权允许授予自身尚未拥有的 Role 规则。
func RoleEscalationAuthorized(ctx context.Context, a authorizer.UnconditionalAuthorizer) bool {
	if a == nil {
		return false
	}

	user, ok := genericapirequest.UserFrom(ctx)
	if !ok {
		return false
	}

	requestInfo, ok := genericapirequest.RequestInfoFrom(ctx)
	if !ok {
		return false
	}

	if !requestInfo.IsResourceRequest {
		return false
	}

	requestResource := schema.GroupResource{Group: requestInfo.APIGroup, Resource: requestInfo.Resource}
	if !roleResources[requestResource] {
		return false
	}

	attrs := authorizer.AttributesRecord{
		User:            user,
		Verb:            "escalate",
		APIGroup:        requestInfo.APIGroup,
		APIVersion:      "*",
		Resource:        requestInfo.Resource,
		Name:            requestInfo.Name,
		Namespace:       requestInfo.Namespace,
		ResourceRequest: true,
	}

	decision, _, err := a.Authorize(ctx, attrs)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf(
			"error authorizing user %#v to escalate %#v named %q in namespace %q: %v",
			user, requestResource, requestInfo.Name, requestInfo.Namespace, err,
		))
	}
	return decision == authorizer.DecisionAllow
}

// BindingAuthorized 检查用户是否被明确授予对目标 Role/ClusterRole 的 bind 动词，授权范围按 Binding namespace 决定。
func BindingAuthorized(ctx context.Context, roleRef rbac.RoleRef, bindingNamespace string, a authorizer.UnconditionalAuthorizer) bool {
	if a == nil {
		return false
	}

	user, ok := genericapirequest.UserFrom(ctx)
	if !ok {
		return false
	}

	attrs := authorizer.AttributesRecord{
		User: user,
		Verb: "bind",
		// 使用 Binding 所在的 namespace 执行 bind 授权；ClusterRoleBinding 则传入空 namespace。
		// 这样既能将特定 ClusterRole 的绑定权限委派到指定命名空间，也能授权创建集群范围的角色绑定。
		// RoleBinding 使用其 namespace 做 bind 授权；ClusterRoleBinding 传空 namespace，表示可绑定集群级角色。
		Namespace:       bindingNamespace,
		ResourceRequest: true,
	}

	// 此处发生在默认值填充和版本转换之后，因此从 RoleRef 读取的字段值已经稳定。
	// 非法 APIGroup 或 Name 会在前置校验阶段被拒绝。
	switch roleRef.Kind {
	case "ClusterRole":
		attrs.APIGroup = roleRef.APIGroup
		attrs.APIVersion = "*"
		attrs.Resource = "clusterroles"
		attrs.Name = roleRef.Name
	case "Role":
		attrs.APIGroup = roleRef.APIGroup
		attrs.APIVersion = "*"
		attrs.Resource = "roles"
		attrs.Name = roleRef.Name
	default:
		return false
	}

	decision, _, err := a.Authorize(ctx, attrs)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf(
			"error authorizing user %#v to bind %#v in namespace %s: %v",
			user, roleRef, bindingNamespace, err,
		))
	}
	return decision == authorizer.DecisionAllow
}
