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

package testing

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/randfill"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apiv1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

// orderedGroupVersionKinds 让 GVK 可按字符串排序，使测试执行顺序稳定、失败输出可复现。
type orderedGroupVersionKinds []schema.GroupVersionKind

func (o orderedGroupVersionKinds) Len() int      { return len(o) }
func (o orderedGroupVersionKinds) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o orderedGroupVersionKinds) Less(i, j int) bool {
	return o[i].String() < o[j].String()
}

// TODO: add a reflexive test that verifies that all SetDefaults functions are registered
// TestDefaulting 验证哪些类型会在 fuzz 后触发默认值设置。
// 对 fuzz 出的对象调用 scheme.Default，检查结果是否与预期“会/不会”发生默认值变化。
func TestDefaulting(t *testing.T) {
	// 这些是已知带 defaulter 的类型；新增顶层 defaulter 时必须同步更新此列表。
	typesWithDefaulting := map[schema.GroupVersionKind]struct{}{
		{Group: "", Version: "v1", Kind: "ConfigMap"}:                                                              {},
		{Group: "", Version: "v1", Kind: "ConfigMapList"}:                                                          {},
		{Group: "", Version: "v1", Kind: "Endpoints"}:                                                              {},
		{Group: "", Version: "v1", Kind: "EndpointsList"}:                                                          {},
		{Group: "", Version: "v1", Kind: "EphemeralContainers"}:                                                    {},
		{Group: "", Version: "v1", Kind: "Namespace"}:                                                              {},
		{Group: "", Version: "v1", Kind: "NamespaceList"}:                                                          {},
		{Group: "", Version: "v1", Kind: "Node"}:                                                                   {},
		{Group: "", Version: "v1", Kind: "NodeList"}:                                                               {},
		{Group: "", Version: "v1", Kind: "PersistentVolume"}:                                                       {},
		{Group: "", Version: "v1", Kind: "PersistentVolumeList"}:                                                   {},
		{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"}:                                                  {},
		{Group: "", Version: "v1", Kind: "PersistentVolumeClaimList"}:                                              {},
		{Group: "", Version: "v1", Kind: "Pod"}:                                                                    {},
		{Group: "", Version: "v1", Kind: "PodList"}:                                                                {},
		{Group: "", Version: "v1", Kind: "PodTemplate"}:                                                            {},
		{Group: "", Version: "v1", Kind: "PodTemplateList"}:                                                        {},
		{Group: "", Version: "v1", Kind: "ReplicationController"}:                                                  {},
		{Group: "", Version: "v1", Kind: "ReplicationControllerList"}:                                              {},
		{Group: "", Version: "v1", Kind: "Secret"}:                                                                 {},
		{Group: "", Version: "v1", Kind: "SecretList"}:                                                             {},
		{Group: "", Version: "v1", Kind: "Service"}:                                                                {},
		{Group: "", Version: "v1", Kind: "ServiceList"}:                                                            {},
		{Group: "apps", Version: "v1beta1", Kind: "StatefulSet"}:                                                   {},
		{Group: "apps", Version: "v1beta1", Kind: "StatefulSetList"}:                                               {},
		{Group: "apps", Version: "v1beta2", Kind: "StatefulSet"}:                                                   {},
		{Group: "apps", Version: "v1beta2", Kind: "StatefulSetList"}:                                               {},
		{Group: "apps", Version: "v1", Kind: "StatefulSet"}:                                                        {},
		{Group: "apps", Version: "v1", Kind: "StatefulSetList"}:                                                    {},
		{Group: "autoscaling", Version: "v1", Kind: "HorizontalPodAutoscaler"}:                                     {},
		{Group: "autoscaling", Version: "v1", Kind: "HorizontalPodAutoscalerList"}:                                 {},
		{Group: "autoscaling", Version: "v2", Kind: "HorizontalPodAutoscaler"}:                                     {},
		{Group: "autoscaling", Version: "v2", Kind: "HorizontalPodAutoscalerList"}:                                 {},
		{Group: "autoscaling", Version: "v2beta1", Kind: "HorizontalPodAutoscaler"}:                                {},
		{Group: "autoscaling", Version: "v2beta1", Kind: "HorizontalPodAutoscalerList"}:                            {},
		{Group: "autoscaling", Version: "v2beta2", Kind: "HorizontalPodAutoscaler"}:                                {},
		{Group: "autoscaling", Version: "v2beta2", Kind: "HorizontalPodAutoscalerList"}:                            {},
		{Group: "batch", Version: "v1", Kind: "CronJob"}:                                                           {},
		{Group: "batch", Version: "v1", Kind: "CronJobList"}:                                                       {},
		{Group: "batch", Version: "v1", Kind: "Job"}:                                                               {},
		{Group: "batch", Version: "v1", Kind: "JobList"}:                                                           {},
		{Group: "batch", Version: "v1beta1", Kind: "CronJob"}:                                                      {},
		{Group: "batch", Version: "v1beta1", Kind: "CronJobList"}:                                                  {},
		{Group: "batch", Version: "v1beta1", Kind: "JobTemplate"}:                                                  {},
		{Group: "batch", Version: "v2alpha1", Kind: "CronJob"}:                                                     {},
		{Group: "batch", Version: "v2alpha1", Kind: "CronJobList"}:                                                 {},
		{Group: "batch", Version: "v2alpha1", Kind: "JobTemplate"}:                                                 {},
		{Group: "certificates.k8s.io", Version: "v1beta1", Kind: "PodCertificateRequest"}:                          {},
		{Group: "certificates.k8s.io", Version: "v1beta1", Kind: "PodCertificateRequestList"}:                      {},
		{Group: "certificates.k8s.io", Version: "v1", Kind: "PodCertificateRequest"}:                               {},
		{Group: "certificates.k8s.io", Version: "v1", Kind: "PodCertificateRequestList"}:                           {},
		{Group: "certificates.k8s.io", Version: "v1beta1", Kind: "CertificateSigningRequest"}:                      {},
		{Group: "certificates.k8s.io", Version: "v1beta1", Kind: "CertificateSigningRequestList"}:                  {},
		{Group: "discovery.k8s.io", Version: "v1", Kind: "EndpointSlice"}:                                          {},
		{Group: "discovery.k8s.io", Version: "v1", Kind: "EndpointSliceList"}:                                      {},
		{Group: "discovery.k8s.io", Version: "v1beta1", Kind: "EndpointSlice"}:                                     {},
		{Group: "discovery.k8s.io", Version: "v1beta1", Kind: "EndpointSliceList"}:                                 {},
		{Group: "extensions", Version: "v1beta1", Kind: "DaemonSet"}:                                               {},
		{Group: "extensions", Version: "v1beta1", Kind: "DaemonSetList"}:                                           {},
		{Group: "apps", Version: "v1beta2", Kind: "DaemonSet"}:                                                     {},
		{Group: "apps", Version: "v1beta2", Kind: "DaemonSetList"}:                                                 {},
		{Group: "apps", Version: "v1", Kind: "DaemonSet"}:                                                          {},
		{Group: "apps", Version: "v1", Kind: "DaemonSetList"}:                                                      {},
		{Group: "extensions", Version: "v1beta1", Kind: "Deployment"}:                                              {},
		{Group: "extensions", Version: "v1beta1", Kind: "DeploymentList"}:                                          {},
		{Group: "apps", Version: "v1beta1", Kind: "Deployment"}:                                                    {},
		{Group: "apps", Version: "v1beta1", Kind: "DeploymentList"}:                                                {},
		{Group: "apps", Version: "v1beta2", Kind: "Deployment"}:                                                    {},
		{Group: "apps", Version: "v1beta2", Kind: "DeploymentList"}:                                                {},
		{Group: "apps", Version: "v1", Kind: "Deployment"}:                                                         {},
		{Group: "apps", Version: "v1", Kind: "DeploymentList"}:                                                     {},
		{Group: "extensions", Version: "v1beta1", Kind: "Ingress"}:                                                 {},
		{Group: "extensions", Version: "v1beta1", Kind: "IngressList"}:                                             {},
		{Group: "apps", Version: "v1beta2", Kind: "ReplicaSet"}:                                                    {},
		{Group: "apps", Version: "v1beta2", Kind: "ReplicaSetList"}:                                                {},
		{Group: "apps", Version: "v1", Kind: "ReplicaSet"}:                                                         {},
		{Group: "apps", Version: "v1", Kind: "ReplicaSetList"}:                                                     {},
		{Group: "extensions", Version: "v1beta1", Kind: "ReplicaSet"}:                                              {},
		{Group: "extensions", Version: "v1beta1", Kind: "ReplicaSetList"}:                                          {},
		{Group: "extensions", Version: "v1beta1", Kind: "NetworkPolicy"}:                                           {},
		{Group: "extensions", Version: "v1beta1", Kind: "NetworkPolicyList"}:                                       {},
		{Group: "rbac.authorization.k8s.io", Version: "v1alpha1", Kind: "ClusterRoleBinding"}:                      {},
		{Group: "rbac.authorization.k8s.io", Version: "v1alpha1", Kind: "ClusterRoleBindingList"}:                  {},
		{Group: "rbac.authorization.k8s.io", Version: "v1alpha1", Kind: "RoleBinding"}:                             {},
		{Group: "rbac.authorization.k8s.io", Version: "v1alpha1", Kind: "RoleBindingList"}:                         {},
		{Group: "rbac.authorization.k8s.io", Version: "v1beta1", Kind: "ClusterRoleBinding"}:                       {},
		{Group: "rbac.authorization.k8s.io", Version: "v1beta1", Kind: "ClusterRoleBindingList"}:                   {},
		{Group: "rbac.authorization.k8s.io", Version: "v1beta1", Kind: "RoleBinding"}:                              {},
		{Group: "rbac.authorization.k8s.io", Version: "v1beta1", Kind: "RoleBindingList"}:                          {},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding"}:                            {},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBindingList"}:                        {},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "RoleBinding"}:                                   {},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "RoleBindingList"}:                               {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "DeviceTaintRule"}:                                   {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "DeviceTaintRuleList"}:                               {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "ResourceClaim"}:                                     {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "ResourceClaimList"}:                                 {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "ResourceClaimTemplate"}:                             {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "ResourceClaimTemplateList"}:                         {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "ResourceSlice"}:                                     {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "ResourceSliceList"}:                                 {},
		{Group: "resource.k8s.io", Version: "v1beta1", Kind: "ResourceClaim"}:                                      {},
		{Group: "resource.k8s.io", Version: "v1beta1", Kind: "ResourceClaimList"}:                                  {},
		{Group: "resource.k8s.io", Version: "v1beta1", Kind: "ResourceClaimTemplate"}:                              {},
		{Group: "resource.k8s.io", Version: "v1beta1", Kind: "ResourceClaimTemplateList"}:                          {},
		{Group: "resource.k8s.io", Version: "v1beta1", Kind: "ResourceSlice"}:                                      {},
		{Group: "resource.k8s.io", Version: "v1beta1", Kind: "ResourceSliceList"}:                                  {},
		{Group: "resource.k8s.io", Version: "v1beta2", Kind: "DeviceTaintRule"}:                                    {},
		{Group: "resource.k8s.io", Version: "v1beta2", Kind: "DeviceTaintRuleList"}:                                {},
		{Group: "resource.k8s.io", Version: "v1beta2", Kind: "ResourceClaim"}:                                      {},
		{Group: "resource.k8s.io", Version: "v1beta2", Kind: "ResourceClaimList"}:                                  {},
		{Group: "resource.k8s.io", Version: "v1beta2", Kind: "ResourceClaimTemplate"}:                              {},
		{Group: "resource.k8s.io", Version: "v1beta2", Kind: "ResourceClaimTemplateList"}:                          {},
		{Group: "resource.k8s.io", Version: "v1beta2", Kind: "ResourceSlice"}:                                      {},
		{Group: "resource.k8s.io", Version: "v1beta2", Kind: "ResourceSliceList"}:                                  {},
		{Group: "resource.k8s.io", Version: "v1", Kind: "DeviceTaintRule"}:                                         {},
		{Group: "resource.k8s.io", Version: "v1", Kind: "DeviceTaintRuleList"}:                                     {},
		{Group: "resource.k8s.io", Version: "v1", Kind: "ResourceClaim"}:                                           {},
		{Group: "resource.k8s.io", Version: "v1", Kind: "ResourceClaimList"}:                                       {},
		{Group: "resource.k8s.io", Version: "v1", Kind: "ResourceClaimTemplate"}:                                   {},
		{Group: "resource.k8s.io", Version: "v1", Kind: "ResourceClaimTemplateList"}:                               {},
		{Group: "resource.k8s.io", Version: "v1", Kind: "ResourceSlice"}:                                           {},
		{Group: "resource.k8s.io", Version: "v1", Kind: "ResourceSliceList"}:                                       {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "ResourcePoolStatusRequest"}:                         {},
		{Group: "resource.k8s.io", Version: "v1alpha3", Kind: "ResourcePoolStatusRequestList"}:                     {},
		{Group: "admissionregistration.k8s.io", Version: "v1alpha1", Kind: "ValidatingAdmissionPolicy"}:            {},
		{Group: "admissionregistration.k8s.io", Version: "v1alpha1", Kind: "ValidatingAdmissionPolicyList"}:        {},
		{Group: "admissionregistration.k8s.io", Version: "v1alpha1", Kind: "ValidatingAdmissionPolicyBinding"}:     {},
		{Group: "admissionregistration.k8s.io", Version: "v1alpha1", Kind: "ValidatingAdmissionPolicyBindingList"}: {},
		{Group: "admissionregistration.k8s.io", Version: "v1alpha1", Kind: "MutatingAdmissionPolicy"}:              {},
		{Group: "admissionregistration.k8s.io", Version: "v1alpha1", Kind: "MutatingAdmissionPolicyList"}:          {},
		{Group: "admissionregistration.k8s.io", Version: "v1alpha1", Kind: "MutatingAdmissionPolicyBinding"}:       {},
		{Group: "admissionregistration.k8s.io", Version: "v1alpha1", Kind: "MutatingAdmissionPolicyBindingList"}:   {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "ValidatingWebhookConfiguration"}:        {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "ValidatingWebhookConfigurationList"}:    {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "MutatingWebhookConfiguration"}:          {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "MutatingWebhookConfigurationList"}:      {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "ValidatingAdmissionPolicy"}:             {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "ValidatingAdmissionPolicyList"}:         {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "ValidatingAdmissionPolicyBinding"}:      {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "ValidatingAdmissionPolicyBindingList"}:  {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "MutatingAdmissionPolicy"}:               {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "MutatingAdmissionPolicyList"}:           {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "MutatingAdmissionPolicyBinding"}:        {},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "MutatingAdmissionPolicyBindingList"}:    {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "ValidatingAdmissionPolicy"}:                  {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "ValidatingAdmissionPolicyList"}:              {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "ValidatingAdmissionPolicyBinding"}:           {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "ValidatingAdmissionPolicyBindingList"}:       {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "ValidatingWebhookConfiguration"}:             {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "ValidatingWebhookConfigurationList"}:         {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingWebhookConfiguration"}:               {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingWebhookConfigurationList"}:           {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingAdmissionPolicy"}:                    {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingAdmissionPolicyBinding"}:             {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingAdmissionPolicyBindingList"}:         {},
		{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingAdmissionPolicyList"}:                {},
		{Group: "networking.k8s.io", Version: "v1", Kind: "NetworkPolicy"}:                                         {},
		{Group: "networking.k8s.io", Version: "v1", Kind: "NetworkPolicyList"}:                                     {},
		{Group: "networking.k8s.io", Version: "v1beta1", Kind: "Ingress"}:                                          {},
		{Group: "networking.k8s.io", Version: "v1beta1", Kind: "IngressList"}:                                      {},
		{Group: "networking.k8s.io", Version: "v1", Kind: "IngressClass"}:                                          {},
		{Group: "networking.k8s.io", Version: "v1", Kind: "IngressClassList"}:                                      {},
		{Group: "storage.k8s.io", Version: "v1beta1", Kind: "StorageClass"}:                                        {},
		{Group: "storage.k8s.io", Version: "v1beta1", Kind: "StorageClassList"}:                                    {},
		{Group: "storage.k8s.io", Version: "v1beta1", Kind: "CSIDriver"}:                                           {},
		{Group: "storage.k8s.io", Version: "v1beta1", Kind: "CSIDriverList"}:                                       {},
		{Group: "storage.k8s.io", Version: "v1", Kind: "StorageClass"}:                                             {},
		{Group: "storage.k8s.io", Version: "v1", Kind: "StorageClassList"}:                                         {},
		{Group: "storage.k8s.io", Version: "v1", Kind: "VolumeAttachment"}:                                         {},
		{Group: "storage.k8s.io", Version: "v1", Kind: "VolumeAttachmentList"}:                                     {},
		{Group: "storage.k8s.io", Version: "v1", Kind: "CSIDriver"}:                                                {},
		{Group: "storage.k8s.io", Version: "v1", Kind: "CSIDriverList"}:                                            {},
		{Group: "storage.k8s.io", Version: "v1beta1", Kind: "VolumeAttachment"}:                                    {},
		{Group: "storage.k8s.io", Version: "v1beta1", Kind: "VolumeAttachmentList"}:                                {},
		{Group: "authentication.k8s.io", Version: "v1", Kind: "TokenRequest"}:                                      {},
		{Group: "scheduling.k8s.io", Version: "v1alpha3", Kind: "PodGroup"}:                                        {},
		{Group: "scheduling.k8s.io", Version: "v1alpha3", Kind: "PodGroupList"}:                                    {},
		{Group: "scheduling.k8s.io", Version: "v1beta1", Kind: "PodGroup"}:                                         {},
		{Group: "scheduling.k8s.io", Version: "v1beta1", Kind: "PodGroupList"}:                                     {},
		{Group: "scheduling.k8s.io", Version: "v1alpha3", Kind: "CompositePodGroup"}:                               {},
		{Group: "scheduling.k8s.io", Version: "v1alpha3", Kind: "CompositePodGroupList"}:                           {},
		{Group: "scheduling.k8s.io", Version: "v1beta1", Kind: "PriorityClass"}:                                    {},
		{Group: "scheduling.k8s.io", Version: "v1", Kind: "PriorityClass"}:                                         {},
		{Group: "scheduling.k8s.io", Version: "v1beta1", Kind: "PriorityClassList"}:                                {},
		{Group: "scheduling.k8s.io", Version: "v1", Kind: "PriorityClassList"}:                                     {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1alpha1", Kind: "PriorityLevelConfiguration"}:           {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1alpha1", Kind: "PriorityLevelConfigurationList"}:       {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1beta1", Kind: "PriorityLevelConfiguration"}:            {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1beta1", Kind: "PriorityLevelConfigurationList"}:        {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1beta2", Kind: "PriorityLevelConfiguration"}:            {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1beta2", Kind: "PriorityLevelConfigurationList"}:        {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1beta3", Kind: "PriorityLevelConfiguration"}:            {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1beta3", Kind: "PriorityLevelConfigurationList"}:        {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1", Kind: "PriorityLevelConfiguration"}:                 {},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1", Kind: "PriorityLevelConfigurationList"}:             {},
	}

	scheme := legacyscheme.Scheme
	var testTypes orderedGroupVersionKinds
	for gvk := range scheme.AllKnownTypes() {
		// 内部版本不参与默认值测试，只检查对外的带版本类型。
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}
		testTypes = append(testTypes, gvk)
	}
	// 排序保证子测试执行顺序确定，便于定位失败。
	sort.Sort(testTypes)

	for _, gvk := range testTypes {
		t.Run(gvk.String(), func(t *testing.T) {
			// 每个子测试使用独立的 fuzzer 实例，避免受其他测试执行顺序的影响。
			f := randfill.New().NilChance(.5).NumElements(1, 1).RandSource(rand.NewSource(1))
			f.Funcs(
				// RawExtension 不走通用 fuzz，否则会生成无法序列化的随机字节。
				func(s *runtime.RawExtension, c randfill.Continue) {},
				func(s *metav1.LabelSelector, c randfill.Continue) {
					c.FillNoCustom(s)
					s.MatchExpressions = nil // 这部分需要专门构造 fuzz 数据
				},
				func(s *metav1.ListOptions, c randfill.Continue) {
					c.FillNoCustom(s)
					s.LabelSelector = "" // 选择器字符串需要专门 fuzz
					s.FieldSelector = "" // 选择器字符串需要专门 fuzz
				},
				func(s *extensionsv1beta1.ScaleStatus, c randfill.Continue) {
					c.FillNoCustom(s)
					s.TargetSelector = "" // 选择器字符串需要专门 fuzz
				},
				// admissionregistration.k8s.io/v1 类型的自定义 fuzz 函数：
				// 把这些字段置空，确保能触发对应的默认值填充。
				func(s *admissionregistrationv1.MutatingAdmissionPolicySpec, c randfill.Continue) {
					c.FillNoCustom(s)
					s.FailurePolicy = nil     // 置空以触发默认值
					s.ReinvocationPolicy = "" // 置空以触发默认值
				},
				func(s *admissionregistrationv1.ValidatingAdmissionPolicySpec, c randfill.Continue) {
					c.FillNoCustom(s)
					s.FailurePolicy = nil // 置空以触发默认值
				},
				func(s *admissionregistrationv1.MatchResources, c randfill.Continue) {
					c.FillNoCustom(s)
					s.MatchPolicy = nil       // 置空以触发默认值
					s.NamespaceSelector = nil // 置空以触发默认值
					s.ObjectSelector = nil    // 置空以触发默认值
				},
				func(s *admissionregistrationv1.Rule, c randfill.Continue) {
					c.FillNoCustom(s)
					s.Scope = nil // 置空以触发默认值
				},
			)

			_, expectedChanged := typesWithDefaulting[gvk]
			iter := 0
			changedOnce := false
			for {
				if iter > *roundtrip.FuzzIters {
					if !expectedChanged || changedOnce {
						break
					}
					// 早期上限是 300，但对 ResourceClaimList 不够：受初始条件影响，
					// fuzzer 可能迟迟生成不出触发默认值的组合（非空切片中的空字符串
					// 嵌套在另一个非空切片里）。
					if iter > 3000 {
						t.Errorf("expected %s to trigger defaulting due to fuzzing", gvk)
						break
					}
					// 预期会触发默认值的类型，继续循环直到 fuzzer 生成符合条件的对象；
					// 最坏情况下由上面的上限兜底报错。
				}
				iter++

				src, err := scheme.New(gvk)
				if err != nil {
					t.Fatal(err)
				}
				f.Fill(src)

				// 清空 GVK：默认值测试只关心字段变化，类型元信息会干扰比较。
				src.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})

				original := src.DeepCopyObject()

				// 在副本上调用 Default，与原始对象对比即可看出默认值是否生效。
				withDefaults := src.DeepCopyObject()
				scheme.Default(withDefaults)

				if !reflect.DeepEqual(original, withDefaults) {
					diff := cmp.Diff(original, withDefaults)
					// 只记录第一次差异，避免多次触发时刷屏。
					if !changedOnce {
						t.Logf("got diff (-fuzzed, +with defaults):\n%s", diff)
						changedOnce = true
					}
					// 未列入白名单的类型不应发生变化，否则说明多注册了 defaulter 或列表需要更新。
					if !expectedChanged {
						t.Errorf("{Group: \"%s\", Version: \"%s\", Kind: \"%s\"} did not expect defaults to be set - update expected or check defaulter registering: %s", gvk.Group, gvk.Version, gvk.Kind, diff)
					}
				}
			}
		})
	}

}

// BenchmarkPodDefaulting 测量对 fuzz 出的 Pod 执行默认值填充的开销。
// 预生成 100 个对象循环复用，计时只覆盖 Default 调用。
func BenchmarkPodDefaulting(b *testing.B) {
	f := randfill.New().NilChance(.5).NumElements(1, 1).RandSource(rand.NewSource(1))
	items := make([]apiv1.Pod, 100)
	for i := range items {
		f.Fill(&items[i])
	}

	scheme := legacyscheme.Scheme
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pod := &items[i%len(items)]

		scheme.Default(pod)
	}
	b.StopTimer()
}
