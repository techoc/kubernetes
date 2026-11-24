/*
Copyright 2022 The Kubernetes Authors.

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

package mutatingadmissionpolicy

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/kubernetes/pkg/apis/admissionregistration"
	rbacregistry "k8s.io/kubernetes/pkg/registry/rbac"
)

// authorizeCreate 在创建变更策略前检查 ParamKind 对应参数资源的读取权限。
func (v *mutatingAdmissionPolicyStrategy) authorizeCreate(ctx context.Context, obj runtime.Object) error {
	policy := obj.(*admissionregistration.MutatingAdmissionPolicy)
	if policy.Spec.ParamKind == nil {
		// 新对象未配置 ParamRef（或 ParamKind），无需执行参数资源授权。
		return nil
	}

	return v.authorize(ctx, policy)
}

// authorizeUpdate 在更新变更策略时检查 ParamKind；仅参数类型变化时重新执行授权。
func (v *mutatingAdmissionPolicyStrategy) authorizeUpdate(ctx context.Context, obj, old runtime.Object) error {
	policy := obj.(*admissionregistration.MutatingAdmissionPolicy)
	if policy.Spec.ParamKind == nil {
		// 新对象未配置 ParamRef（或 ParamKind），无需执行参数资源授权。
		return nil
	}

	oldPolicy := old.(*admissionregistration.MutatingAdmissionPolicy)
	if oldPolicy.Spec.ParamKind != nil && *oldPolicy.Spec.ParamKind == *policy.Spec.ParamKind {
		// ParamKind 与旧对象完全相同，沿用此前已完成的授权结果。
		return nil
	}

	return v.authorize(ctx, policy)
}

// authorize 确保策略创建者/修改者有权读取所有 ParamKind 对象，避免通过准入规则间接读取未授权资源。
func (v *mutatingAdmissionPolicyStrategy) authorize(ctx context.Context, policy *admissionregistration.MutatingAdmissionPolicy) error {
	if v.authorizer == nil || policy.Spec.ParamKind == nil {
		return nil
	}

	// 对具有提权豁免资格的超级用户跳过后续授权检查。
	if rbacregistry.EscalationAllowed(ctx) {
		return nil
	}

	user, ok := genericapirequest.UserFrom(ctx)
	if !ok {
		return fmt.Errorf("cannot identify user to authorize read access to paramKind resources")
	}

	paramKind := policy.Spec.ParamKind
	// ParamKind 无法解析或 Discovery 失败时，按所有 Group/Version/Resource 需要 get 权限处理，采取失败关闭策略。
	// 默认要求用户对所有 API Group、Version 和 Resource 都具备相应权限；解析成功后再逐步收窄范围。
	resource, apiGroup, apiVersion := "*", "*", "*"
	if gv, err := schema.ParseGroupVersion(paramKind.APIVersion); err == nil {
		// API Version 解析成功后，只需针对解析出的 Group 和 Version 检查权限。
		apiGroup = gv.Group
		apiVersion = gv.Version
		if gvr, err := v.resourceResolver.Resolve(gv.WithKind(paramKind.Kind)); err == nil {
			// Kind 成功解析为 GVR 后，只需针对解析出的 Resource 检查权限。
			resource = gvr.Resource
		}
	}

	// 要求用户对 ParamKind 所引用类型的对象具有 get 读取权限。
	attrs := authorizer.AttributesRecord{
		User:            user,
		Verb:            "get",
		ResourceRequest: true,
		Name:            "*",
		Namespace:       "*",
		APIGroup:        apiGroup,
		APIVersion:      apiVersion,
		Resource:        resource,
	}

	d, _, err := v.authorizer.Authorize(ctx, attrs)
	if err != nil {
		return err
	}
	if d != authorizer.DecisionAllow {
		return fmt.Errorf(`user %v must have "get" permission on all objects of the referenced paramKind (kind=%s, apiVersion=%s)`, user, paramKind.Kind, paramKind.APIVersion)
	}
	return nil
}
