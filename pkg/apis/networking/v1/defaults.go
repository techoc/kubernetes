/*
Copyright 2017 The Kubernetes Authors.

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

package v1

import (
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 把本包的默认值注册函数挂到 scheme 上，供解码对象时自动填充默认值。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_NetworkPolicyPort 为 NetworkPolicyPort 未设置的协议字段填充默认值 TCP。
func SetDefaults_NetworkPolicyPort(obj *networkingv1.NetworkPolicyPort) {
	// 未显式指定协议时按 TCP 处理，这是 API 约定，也是大多数工作负载的实际使用方式。
	// Default any undefined Protocol fields to TCP.
	if obj.Protocol == nil {
		proto := v1.ProtocolTCP
		obj.Protocol = &proto
	}
}

// SetDefaults_NetworkPolicy 为 NetworkPolicy 推导默认的 policyTypes。
// 为了兼容早期只支持 Ingress 的 API 版本，未指定 policyTypes 时至少包含 Ingress。
func SetDefaults_NetworkPolicy(obj *networkingv1.NetworkPolicy) {
	if len(obj.Spec.PolicyTypes) == 0 {
		// 不指定 policyTypes 的策略隐含至少 "Ingress"；若同时定义了 egress 规则，则补上 Egress。
		// Any policy that does not specify policyTypes implies at least "Ingress".
		obj.Spec.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
		if len(obj.Spec.Egress) != 0 {
			obj.Spec.PolicyTypes = append(obj.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)
		}
	}
}

// SetDefaults_IngressClass 为 IngressClass 的参数引用填充默认作用域 Cluster。
// Scope 是必填字段，缺省时按集群级对象处理，保持与其它版本行为一致。
func SetDefaults_IngressClass(obj *networkingv1.IngressClass) {
	if obj.Spec.Parameters != nil && obj.Spec.Parameters.Scope == nil {
		obj.Spec.Parameters.Scope = ptr.To(networkingv1.IngressClassParametersReferenceScopeCluster)
	}
}
