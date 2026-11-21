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

package v1

import (
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 向 Scheme 注册 v1 版本的默认值处理函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ValidatingWebhook 为验证 webhook 设置默认值。
// 对于未指定的失败策略、匹配策略、选择器和超时时间，使用 v1 版本的默认语义；
// 其中失败策略默认为 Fail，以保证校验失败时不会放行请求。
func SetDefaults_ValidatingWebhook(obj *admissionregistrationv1.ValidatingWebhook) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1.Fail
		obj.FailurePolicy = &policy
	}
	// 匹配策略默认为 Equivalent，按等价资源而非精确版本匹配请求。
	if obj.MatchPolicy == nil {
		policy := admissionregistrationv1.Equivalent
		obj.MatchPolicy = &policy
	}
	// 空的标签选择器表示匹配全部命名空间/对象。
	if obj.NamespaceSelector == nil {
		selector := metav1.LabelSelector{}
		obj.NamespaceSelector = &selector
	}
	if obj.ObjectSelector == nil {
		selector := metav1.LabelSelector{}
		obj.ObjectSelector = &selector
	}
	// 默认超时 10 秒，避免 webhook 长时间阻塞请求。
	if obj.TimeoutSeconds == nil {
		obj.TimeoutSeconds = new(int32)
		*obj.TimeoutSeconds = 10
	}
}

// SetDefaults_MutatingWebhook 为变更 webhook 设置默认值。
// 默认值与验证 webhook 类似，另外把重新调用策略默认为 Never，
// 避免同一个 webhook 在一次请求中被重复调用。
func SetDefaults_MutatingWebhook(obj *admissionregistrationv1.MutatingWebhook) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1.Fail
		obj.FailurePolicy = &policy
	}
	if obj.MatchPolicy == nil {
		policy := admissionregistrationv1.Equivalent
		obj.MatchPolicy = &policy
	}
	if obj.NamespaceSelector == nil {
		selector := metav1.LabelSelector{}
		obj.NamespaceSelector = &selector
	}
	if obj.ObjectSelector == nil {
		selector := metav1.LabelSelector{}
		obj.ObjectSelector = &selector
	}
	if obj.TimeoutSeconds == nil {
		obj.TimeoutSeconds = new(int32)
		*obj.TimeoutSeconds = 10
	}
	if obj.ReinvocationPolicy == nil {
		never := admissionregistrationv1.NeverReinvocationPolicy
		obj.ReinvocationPolicy = &never
	}
}

// SetDefaults_Rule 为 webhook 规则设置默认值，
// 未指定作用域时默认匹配所有作用域（集群、命名空间及命名空间内对象）。
func SetDefaults_Rule(obj *admissionregistrationv1.Rule) {
	if obj.Scope == nil {
		s := admissionregistrationv1.AllScopes
		obj.Scope = &s
	}
}

// SetDefaults_ServiceReference 为 webhook 的服务引用设置默认值，
// 未指定端口时默认使用 HTTPS 标准端口 443。
func SetDefaults_ServiceReference(obj *admissionregistrationv1.ServiceReference) {
	if obj.Port == nil {
		obj.Port = ptr.To[int32](443)
	}
}

// SetDefaults_ValidatingAdmissionPolicySpec 为验证准入策略规格设置默认值，
// 未指定失败策略时默认为 Fail，防止策略表达式求值为 false 时误放行。
func SetDefaults_ValidatingAdmissionPolicySpec(obj *admissionregistrationv1.ValidatingAdmissionPolicySpec) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1.Fail
		obj.FailurePolicy = &policy
	}
}

// SetDefaults_MatchResources 为 MatchResources 设置默认值：
// 匹配策略默认为 Equivalent，空选择器表示匹配全部命名空间/对象。
func SetDefaults_MatchResources(obj *admissionregistrationv1.MatchResources) {
	if obj.MatchPolicy == nil {
		policy := admissionregistrationv1.Equivalent
		obj.MatchPolicy = &policy
	}
	if obj.NamespaceSelector == nil {
		selector := metav1.LabelSelector{}
		obj.NamespaceSelector = &selector
	}
	if obj.ObjectSelector == nil {
		selector := metav1.LabelSelector{}
		obj.ObjectSelector = &selector
	}
}

// SetDefaults_MutatingAdmissionPolicySpec 为变更准入策略规格设置默认值，
// 未指定失败策略时默认为 Fail。
func SetDefaults_MutatingAdmissionPolicySpec(obj *admissionregistrationv1.MutatingAdmissionPolicySpec) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1.Fail
		obj.FailurePolicy = &policy
	}
}
