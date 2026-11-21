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

package v1beta1

import (
	admissionregistrationv1beta1 "k8s.io/api/admissionregistration/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 向 Scheme 注册 v1beta1 版本的默认值处理函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ValidatingAdmissionPolicySpec 为验证准入策略规格设置默认值，
// 未指定失败策略时默认为 Fail。
func SetDefaults_ValidatingAdmissionPolicySpec(obj *admissionregistrationv1beta1.ValidatingAdmissionPolicySpec) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1beta1.Fail
		obj.FailurePolicy = &policy
	}
}

// SetDefaults_MatchResources 为 MatchResources 设置默认值：
// 匹配策略默认为 Equivalent，空选择器表示匹配全部命名空间/对象。
func SetDefaults_MatchResources(obj *admissionregistrationv1beta1.MatchResources) {
	if obj.MatchPolicy == nil {
		policy := admissionregistrationv1beta1.Equivalent
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

// SetDefaults_ValidatingWebhook 为验证 webhook 设置默认值。
// v1beta1 保留了历史默认值：失败策略为 Ignore、匹配策略为 Exact、超时 30 秒，
// 这些与 v1 版本不同，升级时需特别注意行为差异。
func SetDefaults_ValidatingWebhook(obj *admissionregistrationv1beta1.ValidatingWebhook) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1beta1.Ignore
		obj.FailurePolicy = &policy
	}
	if obj.MatchPolicy == nil {
		policy := admissionregistrationv1beta1.Exact
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
	if obj.SideEffects == nil {
		// TODO: revisit/remove this default and possibly make the field required when promoting to v1
		unknown := admissionregistrationv1beta1.SideEffectClassUnknown
		obj.SideEffects = &unknown
	}
	if obj.TimeoutSeconds == nil {
		obj.TimeoutSeconds = new(int32)
		*obj.TimeoutSeconds = 30
	}

	// 未指定 AdmissionReviewVersions 时，使用当前版本作为默认值。
	if len(obj.AdmissionReviewVersions) == 0 {
		obj.AdmissionReviewVersions = []string{admissionregistrationv1beta1.SchemeGroupVersion.Version}
	}
}

// SetDefaults_MutatingWebhook 为变更 webhook 设置默认值。
// 与验证 webhook 一样保留 v1beta1 的历史默认行为，
// 另外把重新调用策略默认为 Never。
func SetDefaults_MutatingWebhook(obj *admissionregistrationv1beta1.MutatingWebhook) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1beta1.Ignore
		obj.FailurePolicy = &policy
	}
	if obj.MatchPolicy == nil {
		policy := admissionregistrationv1beta1.Exact
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
	if obj.SideEffects == nil {
		// TODO: revisit/remove this default and possibly make the field required when promoting to v1
		unknown := admissionregistrationv1beta1.SideEffectClassUnknown
		obj.SideEffects = &unknown
	}
	if obj.TimeoutSeconds == nil {
		obj.TimeoutSeconds = new(int32)
		*obj.TimeoutSeconds = 30
	}
	if obj.ReinvocationPolicy == nil {
		never := admissionregistrationv1beta1.NeverReinvocationPolicy
		obj.ReinvocationPolicy = &never
	}

	// 未指定 AdmissionReviewVersions 时，使用当前版本作为默认值。
	if len(obj.AdmissionReviewVersions) == 0 {
		obj.AdmissionReviewVersions = []string{admissionregistrationv1beta1.SchemeGroupVersion.Version}
	}
}

// SetDefaults_ServiceReference 为 webhook 的服务引用设置默认值，
// 未指定端口时默认使用 HTTPS 标准端口 443。
func SetDefaults_ServiceReference(obj *admissionregistrationv1beta1.ServiceReference) {
	if obj.Port == nil {
		obj.Port = ptr.To[int32](443)
	}
}

// SetDefaults_MutatingAdmissionPolicySpec 为变更准入策略规格设置默认值，
// 未指定失败策略时默认为 Fail。
func SetDefaults_MutatingAdmissionPolicySpec(obj *admissionregistrationv1beta1.MutatingAdmissionPolicySpec) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1beta1.Fail
		obj.FailurePolicy = &policy
	}
}
