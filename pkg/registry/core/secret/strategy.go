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

// Strategy 是通过 REST API 创建和更新 Secret 时使用的默认策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

var _ = rest.RESTCreateStrategy(Strategy)

var _ = rest.RESTUpdateStrategy(Strategy)

// NamespaceScoped 表示 Secret 是命名空间级资源。
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

// WarningsOnCreate 返回创建 Secret 时的非阻断性警告，TLS Secret 会额外检查证书与私钥是否匹配。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return warningsForSecret(obj.(*api.Secret))
}

// Canonicalize 在校验后规范化对象；Secret 的原始数据不在此处改写。
func (strategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false：Secret 不能通过更新请求隐式创建。
func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// 兼容旧客户端：更新请求把 Type 留空时沿用旧 Type，避免无意将 Secret 类型重置为空字符串。
func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newSecret := obj.(*api.Secret)
	oldSecret := old.(*api.Secret)

	// 此兼容行为沿用旧版 validatedUpdate 的处理方式：客户端省略 Type 时保留原类型。
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

// WarningsOnUpdate 返回更新 Secret 时的非阻断性警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return warningsForSecret(obj.(*api.Secret))
}

// dropDisabledFields 清理 Feature Gate 关闭且旧 Secret 未使用的字段；旧对象已使用的字段需保留以兼容存量数据。
func dropDisabledFields(secret *api.Secret, oldSecret *api.Secret) {
}

// AllowUnconditionalUpdate 允许不携带 resourceVersion 的无条件更新。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// GetAttrs 提取 Secret 的 Labels 和可筛选字段，供 List/Watch 过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	secret, ok := obj.(*api.Secret)
	if !ok {
		return nil, nil, fmt.Errorf("not a secret")
	}
	return labels.Set(secret.Labels), SelectableFields(secret), nil
}

// Matcher 根据 LabelSelector 和 FieldSelector 创建存储层选择谓词。
func Matcher(label labels.Selector, field fields.Selector) pkgstorage.SelectionPredicate {
	return pkgstorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields 返回 Secret 支持 FieldSelector 查询的字段集合。
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
		// 校验私钥是否与证书匹配。
		_, err := tls.X509KeyPair(secret.Data[api.TLSCertKey], secret.Data[api.TLSPrivateKeyKey])
		if err != nil {
			warnings = append(warnings, err.Error())
		}
	}
	return warnings
}
