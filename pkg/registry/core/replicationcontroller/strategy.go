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

// ReplicationController 与 ReplicaSet 的策略长期保持相近；调整共有的 PodTemplate、Generation 或校验规则时，需要同步核对 ReplicaSet。

package replicationcontroller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	apistorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

// rcStrategy 定义 ReplicationController 主资源的 REST 策略，处理依赖删除兼容、Pod 模板字段清理、Generation 和状态隔离。
type rcStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建和更新 Replication Controller 对象时应用的默认逻辑。
var Strategy = rcStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// DefaultGarbageCollectionPolicy 为 core/v1 返回 OrphanDependents 以兼容旧客户端，
// 其他 API 版本返回 DeleteDependents，默认级联删除受该 Controller 管理的 Pod。
func (rcStrategy) DefaultGarbageCollectionPolicy(ctx context.Context) rest.GarbageCollectionPolicy {
	var groupVersion schema.GroupVersion
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		groupVersion = schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
	}
	switch groupVersion {
	case corev1.SchemeGroupVersion:
		// 兼容旧行为
		return rest.OrphanDependents
	default:
		return rest.DeleteDependents
	}
}

// NamespaceScoped 返回 true，因为所有 Replication Controller 都必须属于某个命名空间。
func (rcStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回由策略重置、且用户不应修改的字段集合。
func (rcStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建前清空控制器的 Status：由 API Server 初始化 Generation，并按 Feature Gate 清理 PodTemplate 中不可用的字段。
func (rcStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	controller := obj.(*api.ReplicationController)
	controller.Status = api.ReplicationControllerStatus{}

	controller.Generation = 1

	pod.DropDisabledTemplateFields(controller.Spec.Template, nil)
}

// PrepareForUpdate 清理最终用户不允许在更新时设置的字段：普通更新不能改写控制器 Status，同时清理禁用的模板字段，并仅在 Spec 发生语义变化时递增 Generation。
func (rcStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newController := obj.(*api.ReplicationController)
	oldController := old.(*api.ReplicationController)
	// 更新时不允许设置 status
	newController.Status = oldController.Status

	pod.DropDisabledTemplateFields(newController.Spec.Template, oldController.Spec.Template)

	// 对 spec 的任何修改都会递增 generation 号，对 status 的修改则应反映对应对象的
	// generation 号。我们把管理 status 的负担交给客户端，因为在这里通常无法得知
	// 中包含 spec —— 但未来我们很可能会把 status 独立成单独的对象；即便不这么做，
	// status 的写入者看到的是哪个版本的 spec。乍看之下似乎可以做到 —— 因为 obj
	// 写入也可能来自读取-修改-写入的循环，此时 spec 的内容未必是控制器真正
	// *看到* 的那个 spec。
	if !apiequality.Semantic.DeepEqual(oldController.Spec, newController.Spec) {
		newController.Generation = oldController.Generation + 1
	}
}

// Validate 校验新建的 replication controller。
func (rcStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	controller := obj.(*api.ReplicationController)
	opts := pod.GetValidationOptionsFromPodTemplate(controller.Spec.Template, nil)

	// 执行命令式校验
	return corevalidation.ValidateReplicationController(controller, opts)
}

// WarningsOnCreate 返回创建给定对象时的警告信息。
func (rcStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newRC := obj.(*api.ReplicationController)
	var warnings []string
	if msgs := utilvalidation.IsDNS1123Label(newRC.Name); len(msgs) != 0 {
		warnings = append(warnings, fmt.Sprintf("metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: %v", msgs))
	}
	warnings = append(warnings, pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), newRC.Spec.Template, nil)...)
	return warnings
}

// Canonicalize 在校验之后规范化对象。
func (rcStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 对 replication controllers 为 false，也就是说必须用 POST 创建。
func (rcStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 是面向最终用户的默认更新校验：结合新旧 PodTemplate 判断兼容特性，历史转换失败注解中标记的不可转换 Selector 不允许被意外改写。
func (rcStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	oldRc := old.(*api.ReplicationController)
	newRc := obj.(*api.ReplicationController)

	opts := pod.GetValidationOptionsFromPodTemplate(newRc.Spec.Template, oldRc.Spec.Template)
	errs := corevalidation.ValidateReplicationControllerUpdate(newRc, oldRc, opts)

	for key, value := range helper.NonConvertibleFields(oldRc.Annotations) {
		parts := strings.Split(key, "/")
		if len(parts) != 2 {
			continue
		}
		brokenField := parts[1]

		switch {
		case strings.Contains(brokenField, "selector"):
			if !apiequality.Semantic.DeepEqual(oldRc.Spec.Selector, newRc.Spec.Selector) {
				errs = append(errs, field.Invalid(field.NewPath("spec").Child("selector"), newRc.Spec.Selector, "cannot update non-convertible selector"))
			}
		default:
			errs = append(errs, &field.Error{Type: field.ErrorTypeNotFound, BadValue: value, Field: brokenField, Detail: "unknown non-convertible field"})
		}
	}

	return errs
}

// WarningsOnUpdate 返回本次更新的警告信息。
func (rcStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	var warnings []string
	oldRc := old.(*api.ReplicationController)
	newRc := obj.(*api.ReplicationController)
	if oldRc.Generation != newRc.Generation {
		warnings = pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), oldRc.Spec.Template, newRc.Spec.Template)
	}
	return warnings
}

func (rcStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// ControllerToSelectableFields 暴露标准 ObjectMeta 字段和 status.replicas，供 FieldSelector 按副本状态过滤。
func ControllerToSelectableFields(controller *api.ReplicationController) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&controller.ObjectMeta, true)
	controllerSpecificFieldsSet := fields.Set{
		"status.replicas": strconv.Itoa(int(controller.Status.Replicas)),
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, controllerSpecificFieldsSet)
}

// GetAttrs 返回给定对象用于过滤的 labels 和 fields。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	rc, ok := obj.(*api.ReplicationController)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a replication controller")
	}
	return labels.Set(rc.ObjectMeta.Labels), ControllerToSelectableFields(rc), nil
}

// MatchController 将 LabelSelector/FieldSelector 与属性提取函数组合成 etcd LIST/WATCH 的筛选谓词。
func MatchController(label labels.Selector, field fields.Selector) apistorage.SelectionPredicate {
	return apistorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

type rcStatusStrategy struct {
	rcStrategy
}

// StatusStrategy 是更新对象 status 时调用的默认逻辑。
var StatusStrategy = rcStatusStrategy{Strategy}

// GetResetFields 返回由策略重置、且用户不应修改的字段集合。
func (rcStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

// PrepareForUpdate 针对 /status 只允许控制器回写 Status：Spec 从旧对象恢复，避免状态更新夹带副本数或 PodTemplate 修改。
func (rcStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newRc := obj.(*api.ReplicationController)
	oldRc := old.(*api.ReplicationController)
	// 更新时不允许设置 spec
	newRc.Spec = oldRc.Spec
}

func (rcStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return corevalidation.ValidateReplicationControllerStatusUpdate(obj.(*api.ReplicationController), old.(*api.ReplicationController))
}

// WarningsOnUpdate 返回状态更新的警告信息，这里不做任何警告。
func (rcStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
