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

// ReplicaSet 与 ReplicationController 的策略存在历史对应关系，修改相关行为时需要同步核对后者。

package replicaset

import (
	"context"
	"fmt"
	"strconv"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	apistorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/pod"
	"k8s.io/kubernetes/pkg/apis/apps"
	appsvalidation "k8s.io/kubernetes/pkg/apis/apps/validation"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// rsStrategy 定义 ReplicaSet 主资源策略，负责 Pod 模板字段清理、Generation、选择器和状态写入隔离。
type rsStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 定义通过 REST API 创建和更新 ReplicaSet 时使用的默认策略。
var Strategy = rsStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 编译期检查：确保 Strategy 实现对应的 REST 策略接口。
var _ = rest.GarbageCollectionDeleteStrategy(Strategy)

// DefaultGarbageCollectionPolicy 返回 DeleteDependents：删除 ReplicaSet 时级联删除其依赖对象。
func (rsStrategy) DefaultGarbageCollectionPolicy(ctx context.Context) rest.GarbageCollectionPolicy {
	return rest.DeleteDependents
}

// NamespaceScoped 表示 ReplicaSet 必须属于某个命名空间。
func (rsStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回 Status 子资源更新时需要重置、不能由请求修改的字段集合。
func (rsStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建 ReplicaSet 前清空由控制器维护的 Status。
// 创建时清空控制器 Status、初始化 Generation，并按 Feature Gate 处理 PodTemplate 字段。
func (rsStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	rs := obj.(*apps.ReplicaSet)
	rs.Status = apps.ReplicaSetStatus{}

	rs.Generation = 1

	pod.DropDisabledTemplateFields(&rs.Spec.Template, nil)
}

// PrepareForUpdate 清理更新时不允许终端用户设置的字段。
// 主资源更新不能伪造 Status；只有 Spec 改变才递增 Generation，单独状态变化不影响期望状态版本。
func (rsStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newRS := obj.(*apps.ReplicaSet)
	oldRS := old.(*apps.ReplicaSet)
	// 主资源更新请求不允许修改 Status。
	newRS.Status = oldRS.Status

	pod.DropDisabledTemplateFields(&newRS.Spec.Template, &oldRS.Spec.Template)

	// Generation 只表示 Spec 的期望状态版本。Status 写入者必须自行报告其观察到的 Generation，API Server 无法推断控制器读到的 Spec 版本。
	// Spec 的任何变化都会递增 Generation；Status 写入者应将其观察到的 Spec 版本对应的 Generation 写入 Status。
	// Status 的版本管理由客户端负责，因为 API Server 通常无法知道 Status 写入者实际观察到了哪个 Spec 版本。
	// 虽然当前对象同时包含 Spec 和 Status，但写入可能来自先读取、再更新、最后写回的流程，
	// 此时对象中的 Spec 不一定是控制器此前真正观察到的版本；未来 Status 也可能成为独立对象。
	if !apiequality.Semantic.DeepEqual(oldRS.Spec, newRS.Spec) {
		newRS.Generation = oldRS.Generation + 1
	}
}

// Validate 校验新创建的 ReplicaSet。
func (rsStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	rs := obj.(*apps.ReplicaSet)
	opts := pod.GetValidationOptionsFromPodTemplate(&rs.Spec.Template, nil)
	return appsvalidation.ValidateReplicaSet(rs, opts)
}

// WarningsOnCreate 返回创建 ReplicaSet 时发现的非阻断性警告。
func (rsStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newRS := obj.(*apps.ReplicaSet)
	var warnings []string
	if msgs := utilvalidation.IsDNS1123Label(newRS.Name); len(msgs) != 0 {
		warnings = append(warnings, fmt.Sprintf("metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: %v", msgs))
	}
	warnings = append(warnings, pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), &newRS.Spec.Template, nil)...)
	return warnings
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (rsStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false：创建 ReplicaSet 必须使用 POST，而不能通过更新请求创建。
func (rsStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 执行面向普通用户的 ReplicaSet 更新校验。
// ValidateUpdate 使用新旧 PodTemplate 生成校验选项，保留兼容字段规则并校验 Selector/Template 关系。
func (rsStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newReplicaSet := obj.(*apps.ReplicaSet)
	oldReplicaSet := old.(*apps.ReplicaSet)

	opts := pod.GetValidationOptionsFromPodTemplate(&newReplicaSet.Spec.Template, &oldReplicaSet.Spec.Template)
	return appsvalidation.ValidateReplicaSetUpdate(newReplicaSet, oldReplicaSet, opts)
}

// WarningsOnUpdate 返回本次更新产生的非阻断性警告。
func (rsStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	var warnings []string
	newReplicaSet := obj.(*apps.ReplicaSet)
	oldReplicaSet := old.(*apps.ReplicaSet)
	if newReplicaSet.Generation != oldReplicaSet.Generation {
		warnings = pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), &newReplicaSet.Spec.Template, &oldReplicaSet.Spec.Template)
	}
	return warnings
}

func (rsStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// ToSelectableFields 返回用于表示该对象的字段集合，供 FieldSelector 过滤使用。
// ToSelectableFields 暴露 metadata 字段和 status.replicas，供 API Server 的 FieldSelector 使用。
func ToSelectableFields(rs *apps.ReplicaSet) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&rs.ObjectMeta, true)
	rsSpecificFieldsSet := fields.Set{
		"status.replicas": strconv.Itoa(int(rs.Status.Replicas)),
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, rsSpecificFieldsSet)
}

// GetAttrs 提取指定对象的标签和字段，供资源过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	rs, ok := obj.(*apps.ReplicaSet)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a ReplicaSet")
	}
	return labels.Set(rs.ObjectMeta.Labels), ToSelectableFields(rs), nil
}

// MatchReplicaSet 是通用 etcd 后端使用的过滤谓词，用于将 Watch 事件仅发送给
// 对特定标签或字段感兴趣的 API Server 客户端。
// MatchReplicaSet 将标签/字段选择器与 GetAttrs 组合成 etcd Watch 过滤谓词。
func MatchReplicaSet(label labels.Selector, field fields.Selector) apistorage.SelectionPredicate {
	return apistorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

type rsStatusStrategy struct {
	rsStrategy
}

// StatusStrategy 专门处理 replicasets/status；恢复 Spec 后只对允许的 Status 字段执行校验。
var StatusStrategy = rsStatusStrategy{Strategy}

// GetResetFields 返回该策略会重置的字段集合，这些字段不允许用户通过当前子资源请求修改。
func (rsStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

// /status 请求不能更改期望 Spec；TerminatingReplicas 还需遵循 Feature Gate 的历史字段保留规则。
func (rsStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newRS := obj.(*apps.ReplicaSet)
	oldRS := old.(*apps.ReplicaSet)
	// Status 子资源更新请求不允许修改 Spec。
	newRS.Spec = oldRS.Spec
	dropDisabledStatusFields(&newRS.Status, &oldRS.Status)
}

// ValidateUpdate 校验 ReplicaSet Status 子资源的更新。
func (rsStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return appsvalidation.ValidateReplicaSetStatusUpdate(obj.(*apps.ReplicaSet), old.(*apps.ReplicaSet))
}

// WarningsOnUpdate 返回本次更新产生的非阻断性警告。
func (rsStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// dropDisabledStatusFields 从 ReplicaSet Status 中清除当前未启用特性对应的字段。
// 关闭 TerminatingReplicas 特性时，若旧对象已经使用该字段则保留，否则丢弃请求中新写入的值。
func dropDisabledStatusFields(rsStatus, oldRSStatus *apps.ReplicaSetStatus) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.DeploymentReplicaSetTerminatingReplicas) &&
		(oldRSStatus == nil || oldRSStatus.TerminatingReplicas == nil) {
		rsStatus.TerminatingReplicas = nil
	}
}
