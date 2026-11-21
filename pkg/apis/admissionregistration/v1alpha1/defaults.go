/*
Copyright 2022 The Kubernetes Authors.

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

package v1alpha1

import (
	admissionregistrationv1alpha1 "k8s.io/api/admissionregistration/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 向 Scheme 注册 v1alpha1 版本的默认值处理函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ValidatingAdmissionPolicySpec 为验证准入策略规格设置默认值，
// 未指定失败策略时默认为 Fail。
func SetDefaults_ValidatingAdmissionPolicySpec(obj *admissionregistrationv1alpha1.ValidatingAdmissionPolicySpec) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1alpha1.Fail
		obj.FailurePolicy = &policy
	}
}

// SetDefaults_MatchResources 为 MatchResources 设置默认值：
// 匹配策略默认为 Equivalent，空选择器表示匹配全部命名空间/对象。
func SetDefaults_MatchResources(obj *admissionregistrationv1alpha1.MatchResources) {
	if obj.MatchPolicy == nil {
		policy := admissionregistrationv1alpha1.Equivalent
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

// SetDefaults_ParamRef 为参数引用设置默认值：
// 参数对象不存在时默认按 Deny 处理，避免因缺少参数而意外放行请求。
func SetDefaults_ParamRef(obj *admissionregistrationv1alpha1.ParamRef) {
	if obj.ParameterNotFoundAction == nil {
		v := admissionregistrationv1alpha1.DenyAction
		obj.ParameterNotFoundAction = &v
	}
}

// SetDefaults_MutatingAdmissionPolicySpec 为变更准入策略规格设置默认值，
// 未指定失败策略时默认为 Fail。
func SetDefaults_MutatingAdmissionPolicySpec(obj *admissionregistrationv1alpha1.MutatingAdmissionPolicySpec) {
	if obj.FailurePolicy == nil {
		policy := admissionregistrationv1alpha1.Fail
		obj.FailurePolicy = &policy
	}
}
