/*
Copyright 2025 The Kubernetes Authors.

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

package devicetaintrule

import (
	"context"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/apis/resource/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// deviceTaintRuleStrategy 定义 DeviceTaintRule 的创建、更新、校验和 Status 子资源策略。
type deviceTaintRuleStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 和 StatusStrategy 分别定义 DeviceTaintRule 主资源及 /status 子资源的默认策略。
var (
	Strategy       = &deviceTaintRuleStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}
	StatusStrategy = &deviceTaintRuleStatusStrategy{deviceTaintRuleStrategy: Strategy}
)

// NamespaceScoped 返回 false，因为 DeviceTaintRule 是集群级资源。
func (deviceTaintRuleStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，防止客户端绕过控制器直接写入驱逐状态。
func (*deviceTaintRuleStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"resource.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"resource.k8s.io/v1beta2": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"resource.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清空客户端提交的 Status，并将 Generation 初始化为 1。
func (*deviceTaintRuleStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	rule := obj.(*resource.DeviceTaintRule)
	// 创建请求不能设置 Status；该字段由控制器维护。
	rule.Status = resource.DeviceTaintRuleStatus{}
	rule.Generation = 1
}

func (*deviceTaintRuleStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	rule := obj.(*resource.DeviceTaintRule)
	return validation.ValidateDeviceTaintRule(rule)
}

// WarningsOnCreate 返回创建 DeviceTaintRule 时的非阻断性警告；当前不产生额外警告。
func (*deviceTaintRuleStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 DeviceTaintRule；当前不执行额外转换。
func (*deviceTaintRuleStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，DeviceTaintRule 必须通过 POST 创建。
func (*deviceTaintRuleStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复旧 Status；Effect 变化且 TimeAdded 未被客户端修改时自动更新时间，并在 Spec 变化时递增 Generation。
func (*deviceTaintRuleStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	rule := obj.(*resource.DeviceTaintRule)
	oldRule := old.(*resource.DeviceTaintRule)
	rule.Status = oldRule.Status

	// 当 Taint Effect 发生变化且客户端没有主动修改 TimeAdded 时，自动将 TimeAdded 更新为当前时间。
	// 这样记录的是当前 Effect 的生效起点，而不是整个 DeviceTaintRule 对象的创建时间，
	// 便于计算受影响 ResourceClaim 的容忍持续时间。代价是客户端无法在修改 Effect 时强制保留旧时间。
	if rule.Spec.Taint.Effect != oldRule.Spec.Taint.Effect &&
		rule.Spec.Taint.TimeAdded.Equal(oldRule.Spec.Taint.TimeAdded) {
		// 时间编码精度只有秒，因此先截断到秒，避免生成无法经序列化往返保持不变的时间值，导致测试偶发失败。
		rule.Spec.Taint.TimeAdded = &metav1.Time{Time: time.Now().Truncate(time.Second)}
	}

	// Spec 发生变化时递增 Generation。
	if !apiequality.Semantic.DeepEqual(oldRule.Spec, rule.Spec) {
		rule.Generation = oldRule.Generation + 1
	}
}

// ValidateUpdate 根据新旧对象校验 DeviceTaintRule 更新是否合法。
func (*deviceTaintRuleStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateDeviceTaintRuleUpdate(obj.(*resource.DeviceTaintRule), old.(*resource.DeviceTaintRule))
}

// WarningsOnUpdate 返回 DeviceTaintRule 主资源更新时的非阻断性警告；当前不产生额外警告。
func (*deviceTaintRuleStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 对 v1beta2 和 v1alpha3 保留历史兼容行为；v1 及未来版本要求 ResourceVersion。
func (*deviceTaintRuleStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	reqInfo, _ := request.RequestInfoFrom(ctx)
	if reqInfo != nil && (reqInfo.APIVersion == "v1beta2" || reqInfo.APIVersion == "v1alpha3") {
		// 为兼容已发布的旧 API 版本，v1beta2 和 v1alpha3 必须保留历史上的无条件更新行为。
		return true
	}
	// v1 及未来 API 版本要求携带 ResourceVersion，避免无条件更新覆盖并发修改。
	return false
}

type deviceTaintRuleStatusStrategy struct {
	*deviceTaintRuleStrategy
}

// GetResetFields 声明 Status 子资源写入时需要重置的 Spec 和 metadata，确保该入口只能修改 Status。
func (*deviceTaintRuleStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"resource.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"resource.k8s.io/v1beta2": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"resource.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 恢复旧 Spec，并重置受保护的元数据字段，只允许更新 Status。
func (*deviceTaintRuleStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newRule := obj.(*resource.DeviceTaintRule)
	oldRule := old.(*resource.DeviceTaintRule)
	newRule.Spec = oldRule.Spec
	metav1.ResetObjectMetaForStatus(&newRule.ObjectMeta, &oldRule.ObjectMeta)
}

// ValidateUpdate 校验 DeviceTaintRule Status 更新是否合法。
func (r *deviceTaintRuleStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newRule := obj.(*resource.DeviceTaintRule)
	oldRule := old.(*resource.DeviceTaintRule)
	return validation.ValidateDeviceTaintRuleStatusUpdate(newRule, oldRule)
}

// WarningsOnUpdate 返回 DeviceTaintRule Status 更新时的非阻断性警告；当前不产生额外警告。
func (*deviceTaintRuleStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
