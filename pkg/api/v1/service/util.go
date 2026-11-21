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

package service

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	utilnet "k8s.io/utils/net"
)

const (
	defaultLoadBalancerSourceRanges = "0.0.0.0/0"
)

// IsAllowAll 检查 IPNet 集合是否包含 0.0.0.0/0，即允许所有来源的流量。
func IsAllowAll(ipnets utilnet.IPNetSet) bool {
	for _, s := range ipnets.StringSlice() {
		if s == "0.0.0.0/0" {
			return true
		}
	}
	return false
}

// GetLoadBalancerSourceRanges 先解析并校验 LoadBalancerSourceRanges 字段；
// 未指定时退而解析 sourceRange 注解；两者都没有则返回默认值（允许所有来源）。
func GetLoadBalancerSourceRanges(service *v1.Service) (utilnet.IPNetSet, error) {
	var ipnets utilnet.IPNetSet
	var err error
	// spec 字段优先于注解：两者同时存在时注解被忽略。
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
		specs := strings.Split(val, ",")
		ipnets, err = utilnet.ParseIPNets(specs...)
		if err != nil {
			return nil, fmt.Errorf("%s: %s is not valid. Expecting a comma-separated list of source IP ranges. For example, 10.0.0.0/24,192.168.2.0/24", v1.AnnotationLoadBalancerSourceRangesKey, val)
		}
	}
	return ipnets, nil
}

// ExternallyAccessible 判断 Service 是否可从集群外部访问。
func ExternallyAccessible(service *v1.Service) bool {
	return service.Spec.Type == v1.ServiceTypeLoadBalancer ||
		service.Spec.Type == v1.ServiceTypeNodePort ||
		(service.Spec.Type == v1.ServiceTypeClusterIP && len(service.Spec.ExternalIPs) > 0)
}

// ExternalPolicyLocal 判断 Service 是否外部可访问且 externalTrafficPolicy=Local。
func ExternalPolicyLocal(service *v1.Service) bool {
	if !ExternallyAccessible(service) {
		return false
	}
	return service.Spec.ExternalTrafficPolicy == v1.ServiceExternalTrafficPolicyLocal
}

// InternalPolicyLocal 判断 Service 的 internalTrafficPolicy 是否为 Local。
func InternalPolicyLocal(service *v1.Service) bool {
	if service.Spec.InternalTrafficPolicy == nil {
		return false
	}
	return *service.Spec.InternalTrafficPolicy == v1.ServiceInternalTrafficPolicyLocal
}

// NeedsHealthCheck 判断 Service 是否需要健康检查：仅 LoadBalancer 类型且 ETP=Local 时需要。
func NeedsHealthCheck(service *v1.Service) bool {
	if service.Spec.Type != v1.ServiceTypeLoadBalancer {
		return false
	}
	return ExternalPolicyLocal(service)
}
