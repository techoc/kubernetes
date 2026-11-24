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

package service

import (
	"context"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	pkgstorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	serviceapi "k8s.io/kubernetes/pkg/api/service"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// svcStrategy 定义 Service 主资源的 REST 写入策略，重点维护 Status 隔离及 Service 类型相关字段之间的约束。
type svcStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 Service 时使用的默认策略。
var Strategy = svcStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true：Service 是命名空间级资源。
func (svcStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回 Status 子资源写入时由策略重置、用户不能直接修改的字段集合。
func (svcStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 设置与请求上下文相关的默认值，并清理创建请求中不允许用户设置的字段。
// 创建时清空 Status 和当前 Feature Gate 不允许写入的字段；ClusterIP/NodePort 分配由 Store 的 BeginCreate 钩子完成。
func (svcStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	service := obj.(*api.Service)
	service.Status = api.ServiceStatus{}

	dropServiceDisabledFields(service, nil)
}

// PrepareForUpdate 设置更新所需的上下文默认值，并清理更新请求中不允许用户修改的字段。
// 更新时恢复服务端维护的 Status，并根据 Service 新旧 Type 清理不再适用、且未被用户显式修改的分配字段。
func (svcStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newService := obj.(*api.Service)
	oldService := old.(*api.Service)
	newService.Status = oldService.Status

	dropServiceDisabledFields(newService, oldService)
	dropTypeDependentFields(newService, oldService)
}

// Validate 校验新创建的 Service。
// Validate 负责 Service 创建时的字段语义校验；地址池是否有空闲资源由分配事务在后续阶段处理。
func (svcStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	service := obj.(*api.Service)
	allErrs := validation.ValidateServiceCreate(service)
	return allErrs
}

// WarningsOnCreate 返回创建 Service 时发现的非阻断性警告。
func (svcStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return serviceapi.GetWarningsForService(obj.(*api.Service), nil)
}

// Canonicalize 在校验后规范化 Service 对象；当前策略无需额外处理。
func (svcStrategy) Canonicalize(obj runtime.Object) {
}

// Service 保留 PUT 创建语义：对象不存在时允许更新请求走创建路径，分配钩子仍会为其预留网络资源。
func (svcStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

func (strategy svcStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	allErrs := validation.ValidateServiceUpdate(obj.(*api.Service), old.(*api.Service))
	return allErrs
}

// WarningsOnUpdate 返回更新 Service 时发现的非阻断性警告。
func (svcStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return serviceapi.GetWarningsForService(obj.(*api.Service), old.(*api.Service))
}

func (svcStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// dropServiceDisabledFields 在关联 Feature Gate 关闭且旧 Service 未使用相关字段时，清理新对象中的该字段。
// 常见处理方式是：功能关闭并且旧对象也未使用该能力时，将新对象对应字段置空。
func dropServiceDisabledFields(newSvc *api.Service, oldSvc *api.Service) {}

type serviceStatusStrategy struct {
	svcStrategy
}

// StatusStrategy 将 Service 状态更新策略导出给 storage 包使用。
var StatusStrategy = serviceStatusStrategy{Strategy}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
func (serviceStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 清理 Status 更新请求中不允许用户修改的字段。
// status 子资源只允许更新服务端状态；Spec 从旧对象恢复，避免经 /status 修改 ClusterIP、端口或 Service Type。
func (serviceStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newService := obj.(*api.Service)
	oldService := old.(*api.Service)

	dropServiceStatusDisabledFields(newService, oldService)
	// 更新 Status 时不允许修改 Spec。
	newService.Spec = oldService.Spec
}

// ValidateUpdate 执行面向普通用户的 Service Status 更新校验。
func (serviceStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateServiceStatusUpdate(obj.(*api.Service), old.(*api.Service))
}

// WarningsOnUpdate returns warnings for the given update.
func (serviceStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	svc := obj.(*api.Service)
	var warnings []string

	if len(svc.Status.LoadBalancer.Ingress) > 0 {
		fieldPath := field.NewPath("status", "loadBalancer", "ingress")
		for i, ingress := range svc.Status.LoadBalancer.Ingress {
			if len(ingress.IP) > 0 {
				warnings = append(warnings, utilvalidation.GetWarningsForIP(fieldPath.Index(i), ingress.IP)...)
			}
		}
	}

	return warnings
}

// GetAttrs 提取 Service 的 Labels 和可筛选字段，供 LIST/WATCH 过滤使用。
// GetAttrs 提取 Service 标签和可选择字段，供 LIST/WATCH 的 LabelSelector 与 FieldSelector 过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	service, ok := obj.(*api.Service)
	if !ok {
		return nil, nil, fmt.Errorf("not a service")
	}
	return service.Labels, SelectableFields(service), nil
}

// Matcher 根据 LabelSelector 和 FieldSelector 创建 Service 存储层选择谓词。
func Matcher(label labels.Selector, field fields.Selector) pkgstorage.SelectionPredicate {
	return pkgstorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// SelectableFields 返回 Service 支持 FieldSelector 查询的字段集合。
// SelectableFields 暴露 metadata 字段及 spec.clusterIP/spec.type 两个 Service 专属字段选择器。
func SelectableFields(service *api.Service) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&service.ObjectMeta, true)
	serviceSpecificFieldsSet := fields.Set{
		"spec.clusterIP": service.Spec.ClusterIP,
		"spec.type":      string(service.Spec.Type),
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, serviceSpecificFieldsSet)
}

// dropServiceStatusDisabledFields 在关联 Feature Gate 关闭且旧 Service 未使用相关状态字段时，清理新对象中的该字段。
func dropServiceStatusDisabledFields(newSvc *api.Service, oldSvc *api.Service) {}

func sameStringSlice(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Service 是由 Type 决定字段适用范围的联合结构；切换类型时要谨慎清理自动分配字段，避免释放用户刚提交的值。
// 这是一个特殊处理：Service 包含多个相互关联的字段，为避免破坏客户端行为，类型切换时需要谨慎推断用户意图。
// Service 本质上是以 Spec.Type 为判别字段的联合结构；某些字段只适用于特定类型，切换到不适用的类型后必须清理。
// 通常情况下，API Server 尽量不修改用户输入。但对于从有限资源池分配、需要释放的字段，API 演进时必须处理其依赖关系。
// 这套模式具有较强特殊性，新增类似逻辑前应优先考虑其他设计。
// dropTypeDependentFields 只在字段不再适用于新 Type、原值看起来由系统分配且用户未同时改动它时才清空，以便后续分配器释放资源。
func dropTypeDependentFields(newSvc *api.Service, oldSvc *api.Service) {
	// 当前只在更新时清理字段，以降低行为变化带来的困惑；这里处理的场景都比较特殊。
	if oldSvc == nil {
		return
	}

	// 只有同时满足以下条件才清理字段：该字段对新类型已不适用；该值可能由系统自动分配；用户没有同时主动修改该字段（否则应由校验器报错）。

	// 如果用户切换到不需要 ClusterIP/ClusterIPs 的 Service 类型（包括 Headless 的 "None" 也属于显式值），则可以考虑清理部分关联字段。
	if needsClusterIP(oldSvc) && !needsClusterIP(newSvc) {
		if sameClusterIPs(oldSvc, newSvc) {
			// 这些网络地址将在后续分配事务中释放。
			newSvc.Spec.ClusterIP = ""
			newSvc.Spec.ClusterIPs = nil
		}
		if sameIPFamilies(oldSvc, newSvc) {
			newSvc.Spec.IPFamilies = nil
		}
		if sameIPFamilyPolicy(oldSvc, newSvc) {
			newSvc.Spec.IPFamilyPolicy = nil
		}
	}

	// 如果切换到不使用 NodePort 的类型，且用户没有修改任何 NodePort 值，则可以清空这些端口，后续由分配器释放。
	if needsNodePort(oldSvc) && !needsNodePort(newSvc) && sameNodePorts(oldSvc, newSvc) {
		for i := range newSvc.Spec.Ports {
			newSvc.Spec.Ports[i].NodePort = 0
		}
	}

	// 如果切换到不使用 HealthCheckNodePort 的类型，且用户没有修改该值，则可以清空它，后续由分配器释放。
	if needsHCNodePort(oldSvc) && !needsHCNodePort(newSvc) && sameHCNodePort(oldSvc, newSvc) {
		newSvc.Spec.HealthCheckNodePort = 0
	}

	// 如果切换到不需要 allocatedLoadBalancerNodePorts 的类型，且用户没有修改该字段，则可以安全清理。
	if oldSvc.Spec.Type == api.ServiceTypeLoadBalancer && newSvc.Spec.Type != api.ServiceTypeLoadBalancer {
		if newSvc.Spec.AllocateLoadBalancerNodePorts != nil && oldSvc.Spec.AllocateLoadBalancerNodePorts != nil {
			if *oldSvc.Spec.AllocateLoadBalancerNodePorts == *newSvc.Spec.AllocateLoadBalancerNodePorts {
				newSvc.Spec.AllocateLoadBalancerNodePorts = nil
			}
		}
	}

	// 如果切换到不需要 LoadBalancerClass 的类型，且用户没有修改该字段，则可以安全清理。
	if canSetLoadBalancerClass(oldSvc) && !canSetLoadBalancerClass(newSvc) && sameLoadBalancerClass(oldSvc, newSvc) {
		newSvc.Spec.LoadBalancerClass = nil
	}

	// 如果切换到不需要 ExternalTrafficPolicy 的类型，且用户没有修改该字段，则可以安全清理。
	if serviceapi.ExternallyAccessible(oldSvc) && !serviceapi.ExternallyAccessible(newSvc) && sameExternalTrafficPolicy(oldSvc, newSvc) {
		newSvc.Spec.ExternalTrafficPolicy = api.ServiceExternalTrafficPolicy("")
	}

	// 注意：Selector 等其他字段理论上也可以清理，但历史行为一直保留它们，而且它们不来自有限资源池，因此当前继续保持原值。

	// Clear the load-balancer status if it is no longer appropriate.  Although
	// LB de-provisioning is actually asynchronous, we don't need to expose the
	// user to that complexity.
	if newSvc.Spec.Type != api.ServiceTypeLoadBalancer {
		newSvc.Status.LoadBalancer = api.LoadBalancerStatus{}
	}
}

// ExternalName 不参与 ClusterIP 池分配；Headless Service 虽然使用 None，也仍走 Service 的相关 IP family 语义。
func needsClusterIP(svc *api.Service) bool {
	if svc.Spec.Type == api.ServiceTypeExternalName {
		return false
	}
	return true
}

func sameClusterIPs(oldSvc, newSvc *api.Service) bool {
	sameSingular := oldSvc.Spec.ClusterIP == newSvc.Spec.ClusterIP
	samePlural := sameStringSlice(oldSvc.Spec.ClusterIPs, newSvc.Spec.ClusterIPs)
	return sameSingular && samePlural
}

func sameIPFamilies(oldSvc, newSvc *api.Service) bool {
	return reflect.DeepEqual(oldSvc.Spec.IPFamilies, newSvc.Spec.IPFamilies)
}

func getIPFamilyPolicy(svc *api.Service) string {
	if svc.Spec.IPFamilyPolicy == nil {
		return ""
	}
	return string(*svc.Spec.IPFamilyPolicy)
}

func sameIPFamilyPolicy(oldSvc, newSvc *api.Service) bool {
	return getIPFamilyPolicy(oldSvc) == getIPFamilyPolicy(newSvc)
}

// 只有 NodePort 和 LoadBalancer 类型通常需要占用集群 NodePort 端口池。
func needsNodePort(svc *api.Service) bool {
	if svc.Spec.Type == api.ServiceTypeNodePort || svc.Spec.Type == api.ServiceTypeLoadBalancer {
		return true
	}
	return false
}

func sameNodePorts(oldSvc, newSvc *api.Service) bool {
	// Helper to make a set of NodePort values.
	allNodePorts := func(svc *api.Service) sets.Int {
		out := sets.NewInt()
		for i := range svc.Spec.Ports {
			if svc.Spec.Ports[i].NodePort != 0 {
				out.Insert(int(svc.Spec.Ports[i].NodePort))
			}
		}
		return out
	}

	oldPorts := allNodePorts(oldSvc)
	newPorts := allNodePorts(newSvc)

	// Users can add, remove, or modify ports, as long as they don't add any
	// net-new NodePorts.
	return oldPorts.IsSuperset(newPorts)
}

// HealthCheckNodePort 只用于 ExternalTrafficPolicy=Local 的 LoadBalancer Service。
func needsHCNodePort(svc *api.Service) bool {
	if svc.Spec.Type != api.ServiceTypeLoadBalancer {
		return false
	}
	if svc.Spec.ExternalTrafficPolicy != api.ServiceExternalTrafficPolicyLocal {
		return false
	}
	return true
}

func sameHCNodePort(oldSvc, newSvc *api.Service) bool {
	return oldSvc.Spec.HealthCheckNodePort == newSvc.Spec.HealthCheckNodePort
}

func canSetLoadBalancerClass(svc *api.Service) bool {
	return svc.Spec.Type == api.ServiceTypeLoadBalancer
}

func sameLoadBalancerClass(oldSvc, newSvc *api.Service) bool {
	if (oldSvc.Spec.LoadBalancerClass == nil) != (newSvc.Spec.LoadBalancerClass == nil) {
		return false
	}
	if oldSvc.Spec.LoadBalancerClass == nil {
		return true // both are nil
	}
	return *oldSvc.Spec.LoadBalancerClass == *newSvc.Spec.LoadBalancerClass
}

func sameExternalTrafficPolicy(oldSvc, newSvc *api.Service) bool {
	return oldSvc.Spec.ExternalTrafficPolicy == newSvc.Spec.ExternalTrafficPolicy
}
