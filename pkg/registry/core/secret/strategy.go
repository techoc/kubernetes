/*
Copyright 2015 The Kubernetes Authors.

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

package secret

import (
	"context"
	"crypto/tls"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	pkgstorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
)

// strategy 定义 Secret 的 REST 写入策略，处理 Secret.Type 兼容、数据校验、字段选择器和 TLS 证书告警。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating Secret
// objects via the REST API.
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

var _ = rest.RESTCreateStrategy(Strategy)

var _ = rest.RESTUpdateStrategy(Strategy)

func (strategy) NamespaceScoped() bool {
	return true
}

// 创建前清理当前 Feature Gate 不允许设置的字段；Data/Type 的语义校验交给 Validate。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	secret := obj.(*api.Secret)
	dropDisabledFields(secret, nil)
}

// Validate 根据 Secret.Type 检查必需键、数据格式和类型相关约束。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	newSecret := obj.(*api.Secret)
	return validation.ValidateSecret(newSecret)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return warningsForSecret(obj.(*api.Secret))
}

func (strategy) Canonicalize(obj runtime.Object) {
}

func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// 兼容旧客户端：更新请求把 Type 留空时沿用旧 Type，避免无意将 Secret 类型重置为空字符串。
func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newSecret := obj.(*api.Secret)
	oldSecret := old.(*api.Secret)

	// this is weird, but consistent with what the validatedUpdate function used to do.
	if len(newSecret.Type) == 0 {
		newSecret.Type = oldSecret.Type
	}

	dropDisabledFields(newSecret, oldSecret)
}

// ValidateUpdate 根据新旧 Secret 比较更新约束；不同 Secret.Type 对 Data 中键和值有不同要求。
func (strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newSecret := obj.(*api.Secret)
	oldSecret := old.(*api.Secret)
	return validation.ValidateSecretUpdate(newSecret, oldSecret)
}

// WarningsOnUpdate returns warnings for the given update.
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return warningsForSecret(obj.(*api.Secret))
}

func dropDisabledFields(secret *api.Secret, oldSecret *api.Secret) {
}

func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// GetAttrs returns labels and fields of a given object for filtering purposes.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	secret, ok := obj.(*api.Secret)
	if !ok {
		return nil, nil, fmt.Errorf("not a secret")
	}
	return labels.Set(secret.Labels), SelectableFields(secret), nil
}

// Matcher returns a selection predicate for a given label and field selector.
func Matcher(label labels.Selector, field fields.Selector) pkgstorage.SelectionPredicate {
	return pkgstorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields returns a field set that can be used for filter selection
// Secret 除标准 ObjectMeta 字段外，还暴露 type 字段选择器，允许按 Opaque/TLS 等类型筛选。
func SelectableFields(obj *api.Secret) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
	secretSpecificFieldsSet := fields.Set{
		"type": string(obj.Type),
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, secretSpecificFieldsSet)
}

// 对 TLS 类型 Secret 额外解析证书与私钥，发现证书/私钥不匹配时返回 Warning；告警不替代字段级校验。
func warningsForSecret(secret *api.Secret) []string {
	var warnings []string
	if secret.Type == api.SecretTypeTLS {
		// Verify that the key matches the cert.
		_, err := tls.X509KeyPair(secret.Data[api.TLSCertKey], secret.Data[api.TLSPrivateKeyKey])
		if err != nil {
			warnings = append(warnings, err.Error())
		}
	}
	return warnings
}
