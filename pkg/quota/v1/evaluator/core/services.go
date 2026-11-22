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

package core

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	api "k8s.io/kubernetes/pkg/apis/core"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
)

// serviceObjectCountName 是 Service 对象计数配额的资源名（count/services）。
var serviceObjectCountName = generic.ObjectCountQuotaResourceNameFor(corev1.SchemeGroupVersion.WithResource("services").GroupResource())

// serviceResources 是 Service 相关的配额资源列表。
// 包括对象计数、Service 数量、NodePort 数量和 LoadBalancer 数量。
var serviceResources = []corev1.ResourceName{
	serviceObjectCountName,
	corev1.ResourceServices,
	corev1.ResourceServicesNodePorts,
	corev1.ResourceServicesLoadBalancers,
}

// NewServiceEvaluator 创建 Service 配额评估器。
func NewServiceEvaluator(f quota.ListerForResourceFunc) quota.Evaluator {
	listFuncByNamespace := generic.ListResourceUsingListerFunc(f, corev1.SchemeGroupVersion.WithResource("services"))
	serviceEvaluator := &serviceEvaluator{listFuncByNamespace: listFuncByNamespace}
	return serviceEvaluator
}

// serviceEvaluator 实现 Service 配额使用量的计算。
type serviceEvaluator struct {
	// listFuncByNamespace 按命名空间列出 Service
	listFuncByNamespace generic.ListFuncByNamespace
}

// Constraints 验证 Service 是否满足配额要求。
// Service 没有额外的约束检查，直接返回 nil。
func (p *serviceEvaluator) Constraints(required []corev1.ResourceName, item runtime.Object) error {
	return nil
}

// GroupResource 返回此评估器跟踪的 GroupResource（Service）。
func (p *serviceEvaluator) GroupResource() schema.GroupResource {
	return corev1.SchemeGroupVersion.WithResource("services").GroupResource()
}

// Handles 判断评估器是否应该处理指定的 admission 操作。
// 处理 Create 和 Update 操作，因为 Service 类型可能变化（如 ClusterIP -> NodePort）。
// 不处理 subresource 操作。
func (p *serviceEvaluator) Handles(a admission.Attributes) bool {
	if a.GetSubresource() != "" {
		return false
	}
	operation := a.GetOperation()
	return admission.Create == operation || admission.Update == operation
}

// Matches 判断给定的 ResourceQuota 是否与输入项（Service）匹配。
// Service 不支持 scope 匹配。
func (p *serviceEvaluator) Matches(resourceQuota *corev1.ResourceQuota, item runtime.Object) (bool, error) {
	return generic.Matches(resourceQuota, item, p.MatchingResources, generic.MatchesNoScopeFunc)
}

// MatchingResources 从输入的资源列表中返回与 Service 配额相关的资源子集。
func (p *serviceEvaluator) MatchingResources(input []corev1.ResourceName) []corev1.ResourceName {
	return quota.Intersection(input, serviceResources)
}

// MatchingScopes 返回 Service 匹配的 scope selector 列表。
// Service 不支持 scope，始终返回空列表。
func (p *serviceEvaluator) MatchingScopes(item runtime.Object, scopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	return []corev1.ScopedResourceSelectorRequirement{}, nil
}

// UncoveredQuotaScopes 返回有限制但没有对应配额覆盖的 scope 列表。
// Service 不支持 scope，始终返回空列表。
func (p *serviceEvaluator) UncoveredQuotaScopes(limitedScopes []corev1.ScopedResourceSelectorRequirement, matchedQuotaScopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	return []corev1.ScopedResourceSelectorRequirement{}, nil
}

// toExternalServiceOrError 将 runtime.Object 转换为外部 Service 对象。
func toExternalServiceOrError(obj runtime.Object) (*corev1.Service, error) {
	svc := &corev1.Service{}
	switch t := obj.(type) {
	case *corev1.Service:
		svc = t
	case *api.Service:
		if err := k8s_api_v1.Convert_core_Service_To_v1_Service(t, svc, nil); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("expect *api.Service or *v1.Service, got %v", t)
	}
	return svc, nil
}

// Usage 计算单个 Service 的配额使用量。
// 根据 Service 类型计算不同的资源：
//   - ClusterIP：只计数 Service 本身
//   - NodePort：计数 Service 和 NodePort 数量
//   - LoadBalancer：计数 Service、NodePort 数量和 LoadBalancer 数量
func (p *serviceEvaluator) Usage(item runtime.Object) (corev1.ResourceList, error) {
	result := corev1.ResourceList{}
	svc, err := toExternalServiceOrError(item)
	if err != nil {
		return result, err
	}
	ports := len(svc.Spec.Ports)
	// 默认 Service 使用量：对象计数和 Service 计数为 1，NodePort 和 LoadBalancer 为 0
	result[serviceObjectCountName] = *(resource.NewQuantity(1, resource.DecimalSI))
	result[corev1.ResourceServices] = *(resource.NewQuantity(1, resource.DecimalSI))
	result[corev1.ResourceServicesLoadBalancers] = resource.Quantity{Format: resource.DecimalSI}
	result[corev1.ResourceServicesNodePorts] = resource.Quantity{Format: resource.DecimalSI}
	switch svc.Spec.Type {
	case corev1.ServiceTypeNodePort:
		// NodePort 类型的 Service 需要计数 NodePort 数量
		value := resource.NewQuantity(int64(ports), resource.DecimalSI)
		result[corev1.ResourceServicesNodePorts] = *value
	case corev1.ServiceTypeLoadBalancer:
		// LoadBalancer 类型需要计数 NodePort 和 LoadBalancer。
		// 如果抑制了 NodePort 的自动分配（AllocateLoadBalancerNodePorts=false），
		// 只计数显式设置了 NodePort 值的端口。
		if svc.Spec.AllocateLoadBalancerNodePorts != nil &&
			*svc.Spec.AllocateLoadBalancerNodePorts == false {
			result[corev1.ResourceServicesNodePorts] = *portsWithNodePorts(svc)
		} else {
			value := resource.NewQuantity(int64(ports), resource.DecimalSI)
			result[corev1.ResourceServicesNodePorts] = *value
		}
		result[corev1.ResourceServicesLoadBalancers] = *(resource.NewQuantity(1, resource.DecimalSI))
	}
	return result, nil
}

// portsWithNodePorts 计算 Service 中显式设置了 NodePort 值的端口数量。
func portsWithNodePorts(svc *corev1.Service) *resource.Quantity {
	count := 0
	for _, p := range svc.Spec.Ports {
		if p.NodePort != 0 {
			count++
		}
	}
	return resource.NewQuantity(int64(count), resource.DecimalSI)
}

// UsageStats 计算命名空间内所有 Service 的聚合配额使用统计。
func (p *serviceEvaluator) UsageStats(options quota.UsageStatsOptions) (quota.UsageStats, error) {
	return generic.CalculateUsageStats(options, p.listFuncByNamespace, generic.MatchesNoScopeFunc, p.Usage)
}

var _ quota.Evaluator = &serviceEvaluator{}

// GetQuotaServiceType 返回需要跟踪配额的 Service 类型。
// 只有 NodePort 和 LoadBalancer 类型需要配额跟踪，其他类型返回空字符串。
func GetQuotaServiceType(service *corev1.Service) corev1.ServiceType {
	switch service.Spec.Type {
	case corev1.ServiceTypeNodePort:
		return corev1.ServiceTypeNodePort
	case corev1.ServiceTypeLoadBalancer:
		return corev1.ServiceTypeLoadBalancer
	}
	return corev1.ServiceType("")
}
