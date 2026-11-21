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

package controlplane

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	apinamingtest "k8s.io/apimachinery/pkg/api/apitesting/naming"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

// TestGroupVersions 验证已注册的 GroupVersion 集合符合预期。
func TestGroupVersions(t *testing.T) {
	// legacyUnsuffixedGroups 包含在决定 API 组需带 DNS 后缀之前发布的组。
	// 新组应以 ".k8s.io" 为后缀（见 PR #31887）。
	legacyUnsuffixedGroups := sets.NewString(
		"",
		"apps",
		"autoscaling",
		"batch",
		"extensions",
		"policy",
	)

	// 不应向 legacyUnsuffixedGroups 排除列表添加新组。
	if len(legacyUnsuffixedGroups) != 6 {
		t.Errorf("No additional unnamespaced groups should be created")
	}

	if err := apinamingtest.VerifyGroupNames(legacyscheme.Scheme, legacyUnsuffixedGroups); err != nil {
		t.Errorf("%v", err)
	}
}

// 这些类型注册在外部版本中，因此带 JSON tag；
// 但也注册在内部版本中（或被内部类型引用），
// 因此显式允许它们带 tag。
var typesAllowedTags = map[reflect.Type]bool{
	reflect.TypeOf(intstr.IntOrString{}):              true,
	reflect.TypeOf(metav1.Time{}):                     true,
	reflect.TypeOf(metav1.MicroTime{}):                true,
	reflect.TypeOf(metav1.Duration{}):                 true,
	reflect.TypeOf(metav1.TypeMeta{}):                 true,
	reflect.TypeOf(metav1.ListMeta{}):                 true,
	reflect.TypeOf(metav1.ObjectMeta{}):               true,
	reflect.TypeOf(metav1.OwnerReference{}):           true,
	reflect.TypeOf(metav1.LabelSelector{}):            true,
	reflect.TypeOf(metav1.LabelSelectorRequirement{}): true,
	reflect.TypeOf(metav1.FieldSelectorRequirement{}): true,
	reflect.TypeOf(metav1.GetOptions{}):               true,
	reflect.TypeOf(metav1.ListOptions{}):              true,
	reflect.TypeOf(metav1.DeleteOptions{}):            true,
	reflect.TypeOf(metav1.GroupResource{}):            true,
	reflect.TypeOf(metav1.GroupVersionKind{}):         true,
	reflect.TypeOf(metav1.GroupVersionResource{}):     true,
	reflect.TypeOf(metav1.Status{}):                   true,
	reflect.TypeOf(metav1.Condition{}):                true,
	reflect.TypeOf(runtime.RawExtension{}):            true,
}

// init 初始化允许的非标准类型列表。
func init() {
	for _, t := range []reflect.Type{
		reflect.TypeFor[schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy](),
		reflect.TypeFor[schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints](),
		reflect.TypeFor[schedulingv1alpha3.WorkloadPodGroupDisruptionMode](),
		reflect.TypeFor[schedulingv1alpha3.WorkloadPodGroupResourceClaim](),
	} {
		typesAllowedTags[t] = true
	}
}

// 这些字段是对标准 JSON 命名规则的有限例外。
// 仅在非标准字段名已发布且因兼容性无法修改时才可新增。
var allowedNonstandardJSONNames = map[reflect.Type]string{
	reflect.TypeOf(v1.DaemonEndpoint{}): "Port",
}

// TestTypeTags 验证 API 类型的 JSON tag 规范。
func TestTypeTags(t *testing.T) {
	if err := apinamingtest.VerifyTagNaming(legacyscheme.Scheme, typesAllowedTags, allowedNonstandardJSONNames); err != nil {
		t.Errorf("%v", err)
	}
}
