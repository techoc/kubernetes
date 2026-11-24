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

package serviceaccount

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	sa "k8s.io/kubernetes/plugin/pkg/admission/serviceaccount"
)

// strategy 定义 ServiceAccount 的 REST 写入策略，清理 Secret 引用中的客户端可控元数据并提示弃用注解。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating ServiceAccount
// objects via the REST API.
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

func (strategy) NamespaceScoped() bool {
	return true
}

// 创建前统一清理 Secrets 引用，只保留 Secret 名称，避免客户端伪造引用中的 UID/namespace 等系统字段。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	cleanSecretReferences(obj.(*api.ServiceAccount))
}

func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return validation.ValidateServiceAccount(obj.(*api.ServiceAccount))
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return warnIfHasEnforceMountableSecretsAnnotation(obj.(*api.ServiceAccount), nil)
}

// Canonicalize normalizes the object after validation.
func (strategy) Canonicalize(obj runtime.Object) {
}

func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	cleanSecretReferences(obj.(*api.ServiceAccount))
}

// ServiceAccount.Secrets 是对象引用列表，但服务端只保存 Name；其余 ObjectReference 字段不属于客户端可设置数据。
func cleanSecretReferences(serviceAccount *api.ServiceAccount) {
	for i, secret := range serviceAccount.Secrets {
		serviceAccount.Secrets[i] = api.ObjectReference{Name: secret.Name}
	}
}

func (strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateServiceAccountUpdate(obj.(*api.ServiceAccount), old.(*api.ServiceAccount))
}

// WarningsOnUpdate returns warnings for the given update.
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return warnIfHasEnforceMountableSecretsAnnotation(obj.(*api.ServiceAccount), old.(*api.ServiceAccount))
}

func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// 仅在新设置 enforce-mountable-secrets 注解时告警，避免旧对象每次无关更新都重复提示该注解已弃用。
func warnIfHasEnforceMountableSecretsAnnotation(serviceAccount, oldServiceAccount *api.ServiceAccount) []string {
	if oldServiceAccount != nil {
		_, ok := oldServiceAccount.Annotations[sa.EnforceMountableSecretsAnnotation]
		if ok {
			// skip warning if request isn't newly setting the annotation
			return nil
		}
	}
	_, ok := serviceAccount.Annotations[sa.EnforceMountableSecretsAnnotation]
	if ok {
		return []string{fmt.Sprintf("metadata.annotations[%s]: deprecated in v1.32+; prefer separate namespaces to isolate access to mounted secrets", sa.EnforceMountableSecretsAnnotation)}
	}
	return nil
}
