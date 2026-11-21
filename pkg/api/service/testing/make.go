/*
Copyright 2021 The Kubernetes Authors.

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

package testing

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	api "k8s.io/kubernetes/pkg/apis/core"
)

// Tweak 是一个修改 Service 的函数。
// 之所以设计成函数类型，是为了让调用方通过可变参数把多个修改操作按顺序组合起来。
type Tweak func(*api.Service)

// MakeService 用于比直接写 Go 结构体字面量更易读、更简洁地构造 Service 对象
// （构造出的对象能通过 API 校验）。默认情况下，它生成一个 ClusterIP 类型的 Service，
// 带一个端口和一个简单的 selector。调用方可以传入任意数量的 tweak 函数进一步修改结果。
func MakeService(name string, tweaks ...Tweak) *api.Service {
	// 注意：任何会被默认化（defaulting）填充的字段都必须在这里显式给出合法值，
	// 否则测试对象会依赖默认化逻辑，导致校验行为不一致。
	svc := &api.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: api.ServiceSpec{
			Selector:        map[string]string{"k": "v"},
			SessionAffinity: api.ServiceAffinityNone,
		},
	}
	// 默认使用 ClusterIP 类型。
	SetTypeClusterIP(svc)
	// 默认给一个端口。
	SetPorts(MakeServicePort("", 93, intstr.FromInt32(76), api.ProtocolTCP))(svc)

	// 按顺序应用 tweak：后传入的 tweak 可以覆盖前面的设置。
	for _, tweak := range tweaks {
		tweak(svc)
	}

	return svc
}

// SetTypeClusterIP 把 Service 类型设为 ClusterIP，并清理只属于其他类型的字段。
func SetTypeClusterIP(svc *api.Service) {
	svc.Spec.Type = api.ServiceTypeClusterIP
	// NodePort 只在 NodePort/LoadBalancer 类型下合法，切换类型后必须清零。
	for i := range svc.Spec.Ports {
		svc.Spec.Ports[i].NodePort = 0
	}
	svc.Spec.ExternalName = ""
	svc.Spec.ExternalTrafficPolicy = ""
	svc.Spec.AllocateLoadBalancerNodePorts = nil
	internalTrafficPolicy := api.ServiceInternalTrafficPolicyCluster
	svc.Spec.InternalTrafficPolicy = &internalTrafficPolicy
}

// SetTypeNodePort 把 Service 类型设为 NodePort，并清理只属于其他类型的字段。
func SetTypeNodePort(svc *api.Service) {
	svc.Spec.Type = api.ServiceTypeNodePort
	svc.Spec.ExternalTrafficPolicy = api.ServiceExternalTrafficPolicyCluster
	svc.Spec.ExternalName = ""
	svc.Spec.AllocateLoadBalancerNodePorts = nil
	internalTrafficPolicy := api.ServiceInternalTrafficPolicyCluster
	svc.Spec.InternalTrafficPolicy = &internalTrafficPolicy
}

// SetTypeLoadBalancer 把 Service 类型设为 LoadBalancer，并清理只属于其他类型的字段。
func SetTypeLoadBalancer(svc *api.Service) {
	svc.Spec.Type = api.ServiceTypeLoadBalancer
	svc.Spec.ExternalTrafficPolicy = api.ServiceExternalTrafficPolicyCluster
	// LoadBalancer 默认会分配 node port，这里显式置 true 以避免依赖服务端默认化。
	svc.Spec.AllocateLoadBalancerNodePorts = ptr.To(true)
	svc.Spec.ExternalName = ""
	internalTrafficPolicy := api.ServiceInternalTrafficPolicyCluster
	svc.Spec.InternalTrafficPolicy = &internalTrafficPolicy
}

// SetTypeExternalName 把 Service 类型设为 ExternalName，并清理只属于其他类型的字段。
func SetTypeExternalName(svc *api.Service) {
	svc.Spec.Type = api.ServiceTypeExternalName
	svc.Spec.ExternalName = "example.com"
	svc.Spec.ExternalTrafficPolicy = ""
	// ExternalName 类型的 Service 没有 ClusterIP，必须清空两处相关字段。
	svc.Spec.ClusterIP = ""
	svc.Spec.ClusterIPs = nil
	svc.Spec.AllocateLoadBalancerNodePorts = nil
	svc.Spec.InternalTrafficPolicy = nil
}

// SetPorts 设置 Service 的端口列表。
func SetPorts(ports ...api.ServicePort) Tweak {
	return func(svc *api.Service) {
		svc.Spec.Ports = ports
	}
}

// MakeServicePort 用于构造能通过 API 校验的 ServicePort 对象。
func MakeServicePort(name string, port int, tgtPort intstr.IntOrString, proto api.Protocol) api.ServicePort {
	return api.ServicePort{
		Name:       name,
		Port:       int32(port),
		TargetPort: tgtPort,
		Protocol:   proto,
	}
}

// SetHeadless 把 Service 设为 headless（无 ClusterIP），并清理其他字段。
func SetHeadless(svc *api.Service) {
	SetTypeClusterIP(svc)
	SetClusterIPs(api.ClusterIPNone)(svc)
}

// SetSelector 设置 Service 的 selector。
func SetSelector(sel map[string]string) Tweak {
	return func(svc *api.Service) {
		// 复制一份 map，避免调用方之后修改入参影响构造出的对象。
		svc.Spec.Selector = map[string]string{}
		for k, v := range sel {
			svc.Spec.Selector[k] = v
		}
	}
}

// SetClusterIP 设置 Service 的 ClusterIP 字段。
func SetClusterIP(ip string) Tweak {
	return func(svc *api.Service) {
		svc.Spec.ClusterIP = ip
	}
}

// SetClusterIPs 同时设置 Service 的 ClusterIP 和 ClusterIPs 字段。
func SetClusterIPs(ips ...string) Tweak {
	return func(svc *api.Service) {
		// 两个字段必须保持一致：ClusterIP 取列表中的第一个地址。
		svc.Spec.ClusterIP = ips[0]
		svc.Spec.ClusterIPs = ips
	}
}

// SetIPFamilies 设置 Service 的 IPFamilies 字段。
func SetIPFamilies(families ...api.IPFamily) Tweak {
	return func(svc *api.Service) {
		svc.Spec.IPFamilies = families
	}
}

// SetIPFamilyPolicy 设置 Service 的 IPFamilyPolicy 字段。
func SetIPFamilyPolicy(policy api.IPFamilyPolicy) Tweak {
	return func(svc *api.Service) {
		svc.Spec.IPFamilyPolicy = &policy
	}
}

// SetNodePorts 按顺序为每个端口设置 node port 值。
// 如果提供的值少于端口数量，剩余端口保持不变。
func SetNodePorts(values ...int) Tweak {
	return func(svc *api.Service) {
		for i := range svc.Spec.Ports {
			if i >= len(values) {
				break
			}
			svc.Spec.Ports[i].NodePort = int32(values[i])
		}
	}
}

// SetInternalTrafficPolicy 设置 Service 的 internalTrafficPolicy 字段。
func SetInternalTrafficPolicy(policy api.ServiceInternalTrafficPolicy) Tweak {
	return func(svc *api.Service) {
		svc.Spec.InternalTrafficPolicy = &policy
	}
}

// SetExternalTrafficPolicy 设置 Service 的 externalTrafficPolicy 字段。
func SetExternalTrafficPolicy(policy api.ServiceExternalTrafficPolicy) Tweak {
	return func(svc *api.Service) {
		svc.Spec.ExternalTrafficPolicy = policy
	}
}

// SetAllocateLoadBalancerNodePorts 设置 LB 类型 Service 是否分配 node port 的字段。
func SetAllocateLoadBalancerNodePorts(val bool) Tweak {
	return func(svc *api.Service) {
		svc.Spec.AllocateLoadBalancerNodePorts = ptr.To(val)
	}
}

// SetUniqueNodePorts 把所有 node port 设置为互不重复的值。
func SetUniqueNodePorts(svc *api.Service) {
	for i := range svc.Spec.Ports {
		svc.Spec.Ports[i].NodePort = int32(30000 + i)
	}
}

// SetHealthCheckNodePort 设置 Service 的 healthCheckNodePort 字段。
func SetHealthCheckNodePort(value int32) Tweak {
	return func(svc *api.Service) {
		svc.Spec.HealthCheckNodePort = value
	}
}

// SetSessionAffinity 设置 Service 的 SessionAffinity 字段。
func SetSessionAffinity(affinity api.ServiceAffinity) Tweak {
	return func(svc *api.Service) {
		svc.Spec.SessionAffinity = affinity
		// 亲和性配置必须与所选的亲和性类型匹配：ClientIP 需要带超时配置，
		// None 下则不能残留配置，否则 API 校验会拒绝。
		switch affinity {
		case api.ServiceAffinityNone:
			svc.Spec.SessionAffinityConfig = nil
		case api.ServiceAffinityClientIP:
			timeout := int32(10)
			svc.Spec.SessionAffinityConfig = &api.SessionAffinityConfig{
				ClientIP: &api.ClientIPConfig{
					TimeoutSeconds: &timeout,
				},
			}
		}
	}
}

// SetExternalName 设置 Service 的 ExternalName 字段。
func SetExternalName(val string) Tweak {
	return func(svc *api.Service) {
		svc.Spec.ExternalName = val
	}
}
