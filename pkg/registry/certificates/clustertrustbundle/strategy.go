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

// Package clustertrustbundle 提供 ClusterTrustBundle 的 Registry 接口及 RESTStorage 实现。
package clustertrustbundle

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/certificates"
	certvalidation "k8s.io/kubernetes/pkg/apis/certificates/validation"
)

// strategy 定义 ClusterTrustBundle 的 REST 写入策略。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是 ClusterTrustBundle 创建、更新和删除操作使用的默认策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

var _ rest.RESTCreateStrategy = Strategy
var _ rest.RESTUpdateStrategy = Strategy
var _ rest.RESTDeleteStrategy = Strategy

// NamespaceScoped 表示 ClusterTrustBundle 是集群级资源。
func (strategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 当前无需修改创建对象；实际字段约束由 Validate 执行。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {}

// Validate 校验 ClusterTrustBundle 的名称、授权范围及信任包内容。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	bundle := obj.(*certificates.ClusterTrustBundle)
	return certvalidation.ValidateClusterTrustBundle(bundle, certvalidation.ValidateClusterTrustBundleOptions{})
}

func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (strategy) Canonicalize(obj runtime.Object) {}

func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 当前不改写新旧对象；更新约束由 ValidateUpdate 负责检查。
func (s strategy) PrepareForUpdate(ctx context.Context, new, old runtime.Object) {}

// ValidateUpdate 对照旧对象校验 ClusterTrustBundle 更新是否合法。
func (s strategy) ValidateUpdate(ctx context.Context, new, old runtime.Object) field.ErrorList {
	newBundle := new.(*certificates.ClusterTrustBundle)
	oldBundle := old.(*certificates.ClusterTrustBundle)
	return certvalidation.ValidateClusterTrustBundleUpdate(newBundle, oldBundle)
}

func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
