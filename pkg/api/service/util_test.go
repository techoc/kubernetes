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
	"strings"
	"testing"

	api "k8s.io/kubernetes/pkg/apis/core"
	utilnet "k8s.io/utils/net"
)

// TestGetLoadBalancerSourceRanges 验证 LoadBalancer 来源范围的解析：
// annotation 和 spec 两条路径都接受合法的 CIDR 列表、去重并忽略空白，
// 遇到非法 CIDR 或空元素则报错；两者都未指定时默认放行全部地址。
func TestGetLoadBalancerSourceRanges(t *testing.T) {
	checkError := func(v string) {
		t.Helper()
		annotations := make(map[string]string)
		annotations[api.AnnotationLoadBalancerSourceRangesKey] = v
		svc := api.Service{}
		svc.Annotations = annotations
		_, err := GetLoadBalancerSourceRanges(&svc)
		if err == nil {
			t.Errorf("Expected error parsing: %q", v)
		}
		svc = api.Service{}
		svc.Spec.LoadBalancerSourceRanges = strings.Split(v, ",")
		// 同一组值分别从 annotation 和 spec 解析，两条路径都必须报错。
		_, err = GetLoadBalancerSourceRanges(&svc)
		if err == nil {
			t.Errorf("Expected error parsing: %q", v)
		}
	}
	// 前缀长度超出 IPv4 范围。
	checkError("10.0.0.1/33")
	// 域名不是 CIDR。
	checkError("foo.bar")
	// 列表中混入非法元素。
	checkError("10.0.0.1/32,*")
	// 尾随逗号会产生空元素。
	checkError("10.0.0.1/32,")
	// 逗号后的空白同样不能当成合法 CIDR。
	checkError("10.0.0.1/32, ")
	// 单个 IP 缺少 /prefix，不是合法的 CIDR。
	checkError("10.0.0.1")

	checkOK := func(v string) utilnet.IPNetSet {
		t.Helper()
		annotations := make(map[string]string)
		annotations[api.AnnotationLoadBalancerSourceRangesKey] = v
		svc := api.Service{}
		svc.Annotations = annotations
		_, err := GetLoadBalancerSourceRanges(&svc)
		if err != nil {
			t.Errorf("Unexpected error parsing: %q", v)
		}
		svc = api.Service{}
		svc.Spec.LoadBalancerSourceRanges = strings.Split(v, ",")
		// 返回 spec 路径的解析结果，供调用方检查去重后的条目数量。
		cidrs, err := GetLoadBalancerSourceRanges(&svc)
		if err != nil {
			t.Errorf("Unexpected error parsing: %q", v)
		}
		return cidrs
	}
	cidrs := checkOK("192.168.0.1/32")
	if len(cidrs) != 1 {
		t.Errorf("Expected exactly one CIDR: %v", cidrs.StringSlice())
	}
	// 重复的 CIDR 应被去重为一个，否则规则会被重复执行。
	cidrs = checkOK("192.168.0.1/32,192.168.0.1/32")
	if len(cidrs) != 1 {
		t.Errorf("Expected exactly one CIDR (after de-dup): %v", cidrs.StringSlice())
	}
	cidrs = checkOK("192.168.0.1/32,192.168.0.2/32")
	if len(cidrs) != 2 {
		t.Errorf("Expected two CIDRs: %v", cidrs.StringSlice())
	}
	// CIDR 前后的空白应被忽略，不能因此解析失败或产生第三条规则。
	cidrs = checkOK("  192.168.0.1/32 , 192.168.0.2/32   ")
	if len(cidrs) != 2 {
		t.Errorf("Expected two CIDRs: %v", cidrs.StringSlice())
	}
	// 未指定任何来源范围时，默认应放行全部地址。
	svc := api.Service{}
	cidrs, err := GetLoadBalancerSourceRanges(&svc)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(cidrs) != 1 {
		t.Errorf("Expected exactly one CIDR: %v", cidrs.StringSlice())
	}
	if !IsAllowAll(cidrs) {
		t.Errorf("Expected default to be allow-all: %v", cidrs.StringSlice())
	}
	// annotation 为空字符串与未指定等价，同样应放行全部地址。
	annotations := make(map[string]string)
	annotations[api.AnnotationLoadBalancerSourceRangesKey] = ""
	svc = api.Service{}
	svc.Annotations = annotations
	cidrs, err = GetLoadBalancerSourceRanges(&svc)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(cidrs) != 1 {
		t.Errorf("Expected exactly one CIDR: %v", cidrs.StringSlice())
	}
	if !IsAllowAll(cidrs) {
		t.Errorf("Expected default to be allow-all: %v", cidrs.StringSlice())
	}
}

// TestAllowAll 验证 IsAllowAll 只把 /0 掩码的地址段视为“放行全部”，
// 条目是否重复、是否与其他具体 CIDR 混合都不影响这一判断。
func TestAllowAll(t *testing.T) {
	checkAllowAll := func(allowAll bool, cidrs ...string) {
		t.Helper()
		ipnets, err := utilnet.ParseIPNets(cidrs...)
		if err != nil {
			t.Errorf("Unexpected error parsing cidrs: %v", cidrs)
		}
		if allowAll != IsAllowAll(ipnets) {
			t.Errorf("IsAllowAll did not return expected value for %v", cidrs)
		}
	}
	// 单个 /32 主机地址不是 allow-all。
	checkAllowAll(false, "10.0.0.1/32")
	// 多个具体地址段也不是 allow-all。
	checkAllowAll(false, "10.0.0.1/32", "10.0.0.2/32")
	// 重复条目不会把 /32 “放大”为 /0。
	checkAllowAll(false, "10.0.0.1/32", "10.0.0.1/32")

	// 0.0.0.0/0 覆盖全部 IPv4 地址。
	checkAllowAll(true, "0.0.0.0/0")
	// 任意地址的 /0 掩码都等价于放行全部。
	checkAllowAll(true, "192.168.0.0/0")
	// 列表中只要出现 /0，整体就是放行全部。
	checkAllowAll(true, "192.168.0.1/32", "0.0.0.0/0")
}

// TestExternallyAccessible 验证服务是否可从集群外访问：LoadBalancer 与 NodePort
// 类型天然外部可达，ClusterIP 需要显式配置 ExternalIPs，ExternalName 只是 DNS 别名，
// 即使设置了 ExternalIPs 也不算外部可达。
func TestExternallyAccessible(t *testing.T) {
	checkExternallyAccessible := func(expect bool, service *api.Service) {
		t.Helper()
		res := ExternallyAccessible(service)
		if res != expect {
			t.Errorf("Expected ExternallyAccessible = %v, got %v", expect, res)
		}
	}

	// 零值服务（未指定类型）默认只在集群内可达。
	checkExternallyAccessible(false, &api.Service{})
	// 普通 ClusterIP 不暴露到集群外。
	checkExternallyAccessible(false, &api.Service{
		Spec: api.ServiceSpec{
			Type: api.ServiceTypeClusterIP,
		},
	})
	// ClusterIP 配合 ExternalIPs 才可从外部访问。
	checkExternallyAccessible(true, &api.Service{
		Spec: api.ServiceSpec{
			Type:        api.ServiceTypeClusterIP,
			ExternalIPs: []string{"1.2.3.4"},
		},
	})
	// LoadBalancer 类型由云厂商分配外部入口。
	checkExternallyAccessible(true, &api.Service{
		Spec: api.ServiceSpec{
			Type: api.ServiceTypeLoadBalancer,
		},
	})
	// NodePort 类型通过节点端口暴露到集群外。
	checkExternallyAccessible(true, &api.Service{
		Spec: api.ServiceSpec{
			Type: api.ServiceTypeNodePort,
		},
	})
	// ExternalName 只做 CNAME 转发，不提供集群入口。
	checkExternallyAccessible(false, &api.Service{
		Spec: api.ServiceSpec{
			Type: api.ServiceTypeExternalName,
		},
	})
	// ExternalIPs 对 ExternalName 无意义，不能因此改判为外部可达。
	checkExternallyAccessible(false, &api.Service{
		Spec: api.ServiceSpec{
			Type:        api.ServiceTypeExternalName,
			ExternalIPs: []string{"1.2.3.4"},
		},
	})
}

// TestRequestsOnlyLocalTraffic 验证“仅本地流量”只在 NodePort 或 LoadBalancer
// 类型且 ExternalTrafficPolicy 为 Local 时为 true；Cluster 策略与未设置都表示不要求本地流量。
func TestRequestsOnlyLocalTraffic(t *testing.T) {
	checkRequestsOnlyLocalTraffic := func(requestsOnlyLocalTraffic bool, service *api.Service) {
		t.Helper()
		res := RequestsOnlyLocalTraffic(service)
		if res != requestsOnlyLocalTraffic {
			t.Errorf("Expected requests OnlyLocal traffic = %v, got %v",
				requestsOnlyLocalTraffic, res)
		}
	}

	// 未指定策略的服务不要求本地流量。
	checkRequestsOnlyLocalTraffic(false, &api.Service{})
	checkRequestsOnlyLocalTraffic(false, &api.Service{
		Spec: api.ServiceSpec{
			Type: api.ServiceTypeClusterIP,
		},
	})
	// NodePort/LoadBalancer 未设置 ExternalTrafficPolicy 时仍非 Local。
	checkRequestsOnlyLocalTraffic(false, &api.Service{
		Spec: api.ServiceSpec{
			Type: api.ServiceTypeNodePort,
		},
	})
	checkRequestsOnlyLocalTraffic(false, &api.Service{
		Spec: api.ServiceSpec{
			Type:                  api.ServiceTypeNodePort,
			ExternalTrafficPolicy: api.ServiceExternalTrafficPolicyCluster,
		},
	})
	// NodePort + Local：请求必须只转发到接收流量的节点上的端点。
	checkRequestsOnlyLocalTraffic(true, &api.Service{
		Spec: api.ServiceSpec{
			Type:                  api.ServiceTypeNodePort,
			ExternalTrafficPolicy: api.ServiceExternalTrafficPolicyLocal,
		},
	})
	// LoadBalancer + Cluster：允许转发到其他节点，不属于仅本地流量。
	checkRequestsOnlyLocalTraffic(false, &api.Service{
		Spec: api.ServiceSpec{
			Type:                  api.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: api.ServiceExternalTrafficPolicyCluster,
		},
	})
	// LoadBalancer + Local：应返回 true。
	checkRequestsOnlyLocalTraffic(true, &api.Service{
		Spec: api.ServiceSpec{
			Type:                  api.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: api.ServiceExternalTrafficPolicyLocal,
		},
	})
}

// TestNeedsHealthCheck 验证只有 LoadBalancer 类型且 ExternalTrafficPolicy 为 Local
// 的服务需要健康检查：Local 策略要求 LB 跳过没有健康端点的节点，其他组合无需此检查。
func TestNeedsHealthCheck(t *testing.T) {
	checkNeedsHealthCheck := func(needsHealthCheck bool, service *api.Service) {
		t.Helper()
		res := NeedsHealthCheck(service)
		if res != needsHealthCheck {
			t.Errorf("Expected needs health check = %v, got %v",
				needsHealthCheck, res)
		}
	}

	// ClusterIP 不直接接收外部流量，不需要健康检查。
	checkNeedsHealthCheck(false, &api.Service{
		Spec: api.ServiceSpec{
			Type: api.ServiceTypeClusterIP,
		},
	})
	// NodePort 的 Local 策略由 kube-proxy 处理，无需 LB 健康检查。
	checkNeedsHealthCheck(false, &api.Service{
		Spec: api.ServiceSpec{
			Type:                  api.ServiceTypeNodePort,
			ExternalTrafficPolicy: api.ServiceExternalTrafficPolicyCluster,
		},
	})
	checkNeedsHealthCheck(false, &api.Service{
		Spec: api.ServiceSpec{
			Type:                  api.ServiceTypeNodePort,
			ExternalTrafficPolicy: api.ServiceExternalTrafficPolicyLocal,
		},
	})
	// LoadBalancer + Cluster：云 LB 可转发到任意节点，无需节点级健康检查。
	checkNeedsHealthCheck(false, &api.Service{
		Spec: api.ServiceSpec{
			Type:                  api.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: api.ServiceExternalTrafficPolicyCluster,
		},
	})
	// LoadBalancer + Local：唯一需要健康检查的组合。
	checkNeedsHealthCheck(true, &api.Service{
		Spec: api.ServiceSpec{
			Type:                  api.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: api.ServiceExternalTrafficPolicyLocal,
		},
	})
}
