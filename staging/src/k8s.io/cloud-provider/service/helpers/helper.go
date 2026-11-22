/*
Copyright 2019 The Kubernetes Authors.

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

// Package helpers 提供 ServiceController 与云实现共用的 Service/LB 工具函数。
// 这些逻辑从 k8s.io/kubernetes 内部包复制而来，使 in-tree 云厂商不依赖内部包路径。
package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	utilnet "k8s.io/utils/net"
)

/*
This file is duplicated from "k8s.io/kubernetes/pkg/api/v1/service/util.go"
in order for in-tree cloud providers to not depend on internal packages.
*/

const (
	// defaultLoadBalancerSourceRanges 在未指定来源范围时放行全部流量，保持向后兼容。
	defaultLoadBalancerSourceRanges = "0.0.0.0/0"

	// LoadBalancerCleanupFinalizer is the finalizer added to load balancer
	// services to ensure the Service resource is not fully deleted until
	// the correlating load balancer resources are deleted.
	// 该 finalizer 由 ServiceController 管理；外部 LB 实现若需额外清理，
	// 必须使用自己的 finalizer，不能依赖这一个。
	LoadBalancerCleanupFinalizer = "service.kubernetes.io/load-balancer-cleanup"
)

// IsAllowAll checks whether the utilnet.IPNet allows traffic from 0.0.0.0/0
// 仅识别 IPv4 全放行；IPv6 的 ::/0 不在判断范围内，调用方需注意。
func IsAllowAll(ipnets utilnet.IPNetSet) bool {
	for _, s := range ipnets.StringSlice() {
		if s == "0.0.0.0/0" {
			return true
		}
	}
	return false
}

// GetLoadBalancerSourceRanges first try to parse and verify LoadBalancerSourceRanges field from a service.
// If the field is not specified, turn to parse and verify the AnnotationLoadBalancerSourceRangesKey annotation from a service,
// extracting the source ranges to allow, and if not present returns a default (allow-all) value.
// 字段优先级：spec 字段 > 注解 > 全放行；返回错误时调用方应停止 LB 更新，避免放宽访问控制。
func GetLoadBalancerSourceRanges(service *v1.Service) (utilnet.IPNetSet, error) {
	var ipnets utilnet.IPNetSet
	var err error
	// if SourceRange field is specified, ignore sourceRange annotation
	if len(service.Spec.LoadBalancerSourceRanges) > 0 {
		specs := service.Spec.LoadBalancerSourceRanges
		ipnets, err = utilnet.ParseIPNets(specs...)

		if err != nil {
			return nil, fmt.Errorf("service.Spec.LoadBalancerSourceRanges: %v is not valid. Expecting a list of IP ranges. For example, 10.0.0.0/24. Error msg: %v", specs, err)
		}
	} else {
		val := service.Annotations[v1.AnnotationLoadBalancerSourceRangesKey]
		val = strings.TrimSpace(val)
		if val == "" {
			val = defaultLoadBalancerSourceRanges
		}
		// 注解是逗号分隔的 CIDR 列表；任何一个非法都整体失败，而不是静默丢弃。
		specs := strings.Split(val, ",")
		ipnets, err = utilnet.ParseIPNets(specs...)
		if err != nil {
			return nil, fmt.Errorf("%s: %s is not valid. Expecting a comma-separated list of source IP ranges. For example, 10.0.0.0/24,192.168.2.0/24", v1.AnnotationLoadBalancerSourceRangesKey, val)
		}
	}
	return ipnets, nil
}

// GetServiceHealthCheckPathPort returns the path and nodePort programmed into the Cloud LB Health Check
// 只有 Local 流量策略的 LoadBalancer 才需要健康检查；否则返回空路径与 0，
// 表示云实现应走普通（全节点）健康检查。
func GetServiceHealthCheckPathPort(service *v1.Service) (string, int32) {
	if !NeedsHealthCheck(service) {
		return "", 0
	}
	port := service.Spec.HealthCheckNodePort
	if port == 0 {
		return "", 0
	}
	return "/healthz", port
}

// RequestsOnlyLocalTraffic checks if service requests OnlyLocal traffic.
// 仅 LoadBalancer/NodePort 支持 ExternalTrafficPolicy=Local；其他类型一律视为否。
func RequestsOnlyLocalTraffic(service *v1.Service) bool {
	if service.Spec.Type != v1.ServiceTypeLoadBalancer &&
		service.Spec.Type != v1.ServiceTypeNodePort {
		return false
	}
	return service.Spec.ExternalTrafficPolicy == v1.ServiceExternalTrafficPolicyLocal
}

// NeedsHealthCheck checks if service needs health check.
// 只有 LoadBalancer 且要求 Local 流量时才需要按节点做健康检查。
func NeedsHealthCheck(service *v1.Service) bool {
	if service.Spec.Type != v1.ServiceTypeLoadBalancer {
		return false
	}
	return RequestsOnlyLocalTraffic(service)
}

// HasLBFinalizer checks if service contains LoadBalancerCleanupFinalizer.
func HasLBFinalizer(service *v1.Service) bool {
	for _, finalizer := range service.ObjectMeta.Finalizers {
		if finalizer == LoadBalancerCleanupFinalizer {
			return true
		}
	}
	return false
}

// LoadBalancerStatusEqual checks if load balancer status are equal
// 只比较 Ingress；nil 与空切片在 Semantic.DeepEqual 下被视为相等，避免无意义写入。
func LoadBalancerStatusEqual(l, r *v1.LoadBalancerStatus) bool {
	return apiequality.Semantic.DeepEqual(l.Ingress, r.Ingress)
}

// PatchService patches the given service's Status or ObjectMeta based on the original and
// updated ones. Change to spec will be ignored.
// 先重置 spec 再生成 patch，防止控制器用陈旧副本覆盖用户对 Service 规格的并发修改。
func PatchService(c corev1.CoreV1Interface, oldSvc, newSvc *v1.Service) (*v1.Service, error) {
	// Reset spec to make sure only patch for Status or ObjectMeta.
	newSvc.Spec = oldSvc.Spec

	patchBytes, err := getPatchBytes(oldSvc, newSvc)
	if err != nil {
		return nil, err
	}

	return c.Services(oldSvc.Namespace).Patch(context.TODO(), oldSvc.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")

}

// getPatchBytes 将两个 Service 的差异编码为 strategic merge patch；
// 调用方需保证传入对象的 spec 已对齐，否则会把 spec 变更一并提交。
func getPatchBytes(oldSvc, newSvc *v1.Service) ([]byte, error) {
	oldData, err := json.Marshal(oldSvc)
	if err != nil {
		return nil, fmt.Errorf("failed to Marshal oldData for svc %s/%s: %v", oldSvc.Namespace, oldSvc.Name, err)
	}

	newData, err := json.Marshal(newSvc)
	if err != nil {
		return nil, fmt.Errorf("failed to Marshal newData for svc %s/%s: %v", newSvc.Namespace, newSvc.Name, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Service{})
	if err != nil {
		return nil, fmt.Errorf("failed to CreateTwoWayMergePatch for svc %s/%s: %v", oldSvc.Namespace, oldSvc.Name, err)
	}
	return patchBytes, nil

}
