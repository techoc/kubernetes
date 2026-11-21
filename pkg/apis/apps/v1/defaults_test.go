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

package v1_test

import (
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	_ "k8s.io/kubernetes/pkg/apis/apps/install"
	. "k8s.io/kubernetes/pkg/apis/apps/v1"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// TestSetDefaultDaemonSetSpec 校验标签传播、更新策略与历史版本上限的默认化。
func TestSetDefaultDaemonSetSpec(t *testing.T) {
	defaultLabels := map[string]string{"foo": "bar"}
	maxUnavailable := intstr.FromInt32(1)
	maxSurge := intstr.FromInt32(0)
	period := int64(v1.DefaultTerminationGracePeriodSeconds)
	defaultTemplate := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			DNSPolicy:                     v1.DNSClusterFirst,
			RestartPolicy:                 v1.RestartPolicyAlways,
			SecurityContext:               &v1.PodSecurityContext{},
			TerminationGracePeriodSeconds: &period,
			SchedulerName:                 v1.DefaultSchedulerName,
		},
		ObjectMeta: metav1.ObjectMeta{
			Labels: defaultLabels,
		},
	}
	templateNoLabel := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			DNSPolicy:                     v1.DNSClusterFirst,
			RestartPolicy:                 v1.RestartPolicyAlways,
			SecurityContext:               &v1.PodSecurityContext{},
			TerminationGracePeriodSeconds: &period,
			SchedulerName:                 v1.DefaultSchedulerName,
		},
	}
	tests := []struct {
		original *appsv1.DaemonSet
		expected *appsv1.DaemonSet
	}{
		{ // 验证默认标签从 Pod 模板传播到 DaemonSet。
			original: &appsv1.DaemonSet{
				Spec: appsv1.DaemonSetSpec{
					Template: defaultTemplate,
				},
			},
			expected: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.DaemonSetSpec{
					Template: defaultTemplate,
					UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
						Type: appsv1.RollingUpdateDaemonSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDaemonSet{
							MaxUnavailable: &maxUnavailable,
							MaxSurge:       &maxSurge,
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{ // 已设置的标签不应被模板标签覆盖。
			original: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"bar": "foo",
					},
				},
				Spec: appsv1.DaemonSetSpec{
					Template:             defaultTemplate,
					RevisionHistoryLimit: ptr.To[int32](1),
				},
			},
			expected: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"bar": "foo",
					},
				},
				Spec: appsv1.DaemonSetSpec{
					Template: defaultTemplate,
					UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
						Type: appsv1.RollingUpdateDaemonSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDaemonSet{
							MaxUnavailable: &maxUnavailable,
							MaxSurge:       &maxSurge,
						},
					},
					RevisionHistoryLimit: ptr.To[int32](1),
				},
			},
		},
		{ // OnDelete 策略下不应填充 RollingUpdate 参数。
			original: &appsv1.DaemonSet{
				Spec: appsv1.DaemonSetSpec{
					Template: templateNoLabel,
					UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
						Type: appsv1.OnDeleteDaemonSetStrategyType,
					},
				},
			},
			expected: &appsv1.DaemonSet{
				Spec: appsv1.DaemonSetSpec{
					Template: templateNoLabel,
					UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
						Type: appsv1.OnDeleteDaemonSetStrategyType,
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{ // 验证默认标签键的取值来源。
			original: &appsv1.DaemonSet{
				Spec: appsv1.DaemonSetSpec{},
			},
			expected: &appsv1.DaemonSet{
				Spec: appsv1.DaemonSetSpec{
					Template: templateNoLabel,
					UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
						Type: appsv1.RollingUpdateDaemonSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDaemonSet{
							MaxUnavailable: &maxUnavailable,
							MaxSurge:       &maxSurge,
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
	}

	// 通过编解码往返触发默认化，再比较期望的 spec。
	for i, test := range tests {
		original := test.original
		expected := test.expected
		obj2 := roundTrip(t, runtime.Object(original))
		got, ok := obj2.(*appsv1.DaemonSet)
		if !ok {
			t.Errorf("(%d) unexpected object: %v", i, got)
			t.FailNow()
		}
		if !apiequality.Semantic.DeepEqual(got.Spec, expected.Spec) {
			t.Errorf("(%d) got different than expected\ngot:\n\t%+v\nexpected:\n\t%+v", i, got.Spec, expected.Spec)
		}
	}
}

// TestSetDefaultStatefulSet 校验 StatefulSet 各项默认值及特性门控对 MaxUnavailable 的影响。
func TestSetDefaultStatefulSet(t *testing.T) {
	defaultLabels := map[string]string{"foo": "bar"}
	var defaultPartition int32 = 0
	var defaultReplicas int32 = 1
	var notTheDefaultPartition int32 = 42

	period := int64(v1.DefaultTerminationGracePeriodSeconds)
	defaultTemplate := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			DNSPolicy:                     v1.DNSClusterFirst,
			RestartPolicy:                 v1.RestartPolicyAlways,
			SecurityContext:               &v1.PodSecurityContext{},
			TerminationGracePeriodSeconds: &period,
			SchedulerName:                 v1.DefaultSchedulerName,
		},
		ObjectMeta: metav1.ObjectMeta{
			Labels: defaultLabels,
		},
	}

	// 每条用例声明原始对象、期望结果与门控开关。
	tests := []struct {
		name                       string
		original                   *appsv1.StatefulSet
		expected                   *appsv1.StatefulSet
		enableMaxUnavailablePolicy bool
	}{
		{
			name: "labels and default update strategy",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:            &defaultReplicas,
					MinReadySeconds:     int32(0),
					Template:            defaultTemplate,
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition: &defaultPartition,
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{
			name: "Alternate update strategy",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.OnDeleteStatefulSetStrategyType,
					},
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:        &defaultReplicas,
					MinReadySeconds: int32(0),
					Template:        defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.OnDeleteStatefulSetStrategyType,
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{
			name: "Parallel pod management policy",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template:            defaultTemplate,
					PodManagementPolicy: appsv1.ParallelPodManagement,
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:        &defaultReplicas,
					MinReadySeconds: int32(0),
					Template:        defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					PodManagementPolicy: appsv1.ParallelPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition: &defaultPartition,
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{
			// 未显式设置策略类型时，默认化必须保留已有的 Partition 值。
			name: "UpdateStrategy.RollingUpdate.Partition is not lost when UpdateStrategy.Type is not set",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition: &notTheDefaultPartition,
						},
					},
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:        &defaultReplicas,
					MinReadySeconds: int32(0),
					Template:        defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition: &notTheDefaultPartition,
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{
			name: "no PVC policy specified",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &defaultReplicas,
					Template: defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition: &defaultPartition,
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{
			name: "PVC scaledown policy specified",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
					},
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:            &defaultReplicas,
					Template:            defaultTemplate,
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition: &defaultPartition,
						},
					},
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{
			name: "PVC with deletion policy specified",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
					},
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:            &defaultReplicas,
					Template:            defaultTemplate,
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition: &defaultPartition,
						},
					},
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{
			// Recreate 策略不使用滚动更新参数，默认化不应填充。
			name: "Recreate update strategy does not populate RollingUpdate",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RecreateStatefulSetStrategyType,
					},
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:            &defaultReplicas,
					Template:            defaultTemplate,
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RecreateStatefulSetStrategyType,
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		{
			// MaxUnavailable 默认化受特性门控控制，关闭时保持字段为空。
			name: "MaxUnavailable disabled, with maxUnavailable not specified",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &defaultReplicas,
					Template: defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition: ptr.To[int32](0),
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
			enableMaxUnavailablePolicy: false,
		},
		{
			name: "MaxUnavailable disabled, with default maxUnavailable specified",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition:      &defaultPartition,
							MaxUnavailable: ptr.To(intstr.FromInt32(1)),
						},
					},
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &defaultReplicas,
					Template: defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition:      ptr.To[int32](0),
							MaxUnavailable: ptr.To(intstr.FromInt32(1)),
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
			enableMaxUnavailablePolicy: false,
		},
		{
			name: "MaxUnavailable disabled, with non default maxUnavailable specified",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition:      &notTheDefaultPartition,
							MaxUnavailable: ptr.To(intstr.FromInt32(3)),
						},
					},
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &defaultReplicas,
					Template: defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition:      ptr.To[int32](42),
							MaxUnavailable: ptr.To(intstr.FromInt32(3)),
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
			enableMaxUnavailablePolicy: false,
		},
		{
			// 门控开启且未指定时，MaxUnavailable 默认为 1。
			name: "MaxUnavailable enabled, with no maxUnavailable specified",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &defaultReplicas,
					Template: defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition:      ptr.To[int32](0),
							MaxUnavailable: ptr.To(intstr.FromInt32(1)),
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
			enableMaxUnavailablePolicy: true,
		},
		{
			name: "MaxUnavailable enabled, with non default maxUnavailable specified",
			original: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: defaultTemplate,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition:      &notTheDefaultPartition,
							MaxUnavailable: ptr.To(intstr.FromInt32(3)),
						},
					},
				},
			},
			expected: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &defaultReplicas,
					Template: defaultTemplate,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
						RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
							Partition:      ptr.To[int32](42),
							MaxUnavailable: ptr.To(intstr.FromInt32(3)),
						},
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
			enableMaxUnavailablePolicy: true,
		},
	}

	// 每条用例在独立的门控状态下执行编解码往返。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MaxUnavailableStatefulSet, test.enableMaxUnavailablePolicy)

			obj2 := roundTrip(t, runtime.Object(test.original))
			got, ok := obj2.(*appsv1.StatefulSet)
			if !ok {
				t.Errorf("unexpected object: %v", got)
				t.FailNow()
			}
			if !apiequality.Semantic.DeepEqual(got.Spec, test.expected.Spec) {
				t.Errorf("got different than expected\ngot:\n\t%+v\nexpected:\n\t%+v", got.Spec, test.expected.Spec)
			}
		})
	}
}

// TestSetDefaultDeployment 校验副本数、滚动更新参数等默认值。
func TestSetDefaultDeployment(t *testing.T) {
	defaultIntOrString := intstr.FromString("25%")
	differentIntOrString := intstr.FromInt32(5)
	period := int64(v1.DefaultTerminationGracePeriodSeconds)
	defaultTemplate := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			DNSPolicy:                     v1.DNSClusterFirst,
			RestartPolicy:                 v1.RestartPolicyAlways,
			SecurityContext:               &v1.PodSecurityContext{},
			TerminationGracePeriodSeconds: &period,
			SchedulerName:                 v1.DefaultSchedulerName,
		},
	}
	tests := []struct {
		original *appsv1.Deployment
		expected *appsv1.Deployment
	}{
		{
			original: &appsv1.Deployment{},
			expected: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](1),
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDeployment{
							MaxSurge:       &defaultIntOrString,
							MaxUnavailable: &defaultIntOrString,
						},
					},
					RevisionHistoryLimit:    ptr.To[int32](10),
					ProgressDeadlineSeconds: ptr.To[int32](600),
					Template:                defaultTemplate,
				},
			},
		},
		{
			original: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: appsv1.DeploymentStrategy{
						RollingUpdate: &appsv1.RollingUpdateDeployment{
							MaxSurge: &differentIntOrString,
						},
					},
				},
			},
			expected: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDeployment{
							MaxSurge:       &differentIntOrString,
							MaxUnavailable: &defaultIntOrString,
						},
					},
					RevisionHistoryLimit:    ptr.To[int32](10),
					ProgressDeadlineSeconds: ptr.To[int32](600),
					Template:                defaultTemplate,
				},
			},
		},
		{
			original: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](3),
					Strategy: appsv1.DeploymentStrategy{
						Type:          appsv1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: nil,
					},
				},
			},
			expected: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](3),
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDeployment{
							MaxSurge:       &defaultIntOrString,
							MaxUnavailable: &defaultIntOrString,
						},
					},
					RevisionHistoryLimit:    ptr.To[int32](10),
					ProgressDeadlineSeconds: ptr.To[int32](600),
					Template:                defaultTemplate,
				},
			},
		},
		{
			original: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RecreateDeploymentStrategyType,
					},
					RevisionHistoryLimit: ptr.To[int32](0),
				},
			},
			expected: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RecreateDeploymentStrategyType,
					},
					RevisionHistoryLimit:    ptr.To[int32](0),
					ProgressDeadlineSeconds: ptr.To[int32](600),
					Template:                defaultTemplate,
				},
			},
		},
		{
			original: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RecreateDeploymentStrategyType,
					},
					ProgressDeadlineSeconds: ptr.To[int32](30),
					RevisionHistoryLimit:    ptr.To[int32](2),
				},
			},
			expected: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RecreateDeploymentStrategyType,
					},
					ProgressDeadlineSeconds: ptr.To[int32](30),
					RevisionHistoryLimit:    ptr.To[int32](2),
					Template:                defaultTemplate,
				},
			},
		},
	}

	// 通过往返编解码触发默认化并比较 spec。
	for _, test := range tests {
		original := test.original
		expected := test.expected
		obj2 := roundTrip(t, runtime.Object(original))
		got, ok := obj2.(*appsv1.Deployment)
		if !ok {
			t.Errorf("unexpected object: %v", got)
			t.FailNow()
		}
		if !apiequality.Semantic.DeepEqual(got.Spec, expected.Spec) {
			t.Errorf("object mismatch!\nexpected:\n\t%+v\ngot:\n\t%+v", got.Spec, expected.Spec)
		}
	}
}

// TestDefaultDeploymentAvailability 确保默认参数不会让滚动更新期间可用副本降为零。
func TestDefaultDeploymentAvailability(t *testing.T) {
	d := roundTrip(t, runtime.Object(&appsv1.Deployment{})).(*appsv1.Deployment)

	maxUnavailable, err := intstr.GetScaledValueFromIntOrPercent(d.Spec.Strategy.RollingUpdate.MaxUnavailable, int(*(d.Spec.Replicas)), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 默认参数必须始终至少保留一个可用副本。
	if *(d.Spec.Replicas)-int32(maxUnavailable) <= 0 {
		t.Fatalf("the default value of maxUnavailable can lead to no active replicas during rolling update")
	}
}

// TestSetDefaultReplicaSetReplicas 校验副本数未设置时才默认为 1，显式 0 需保留。
func TestSetDefaultReplicaSetReplicas(t *testing.T) {
	tests := []struct {
		rs             appsv1.ReplicaSet
		expectReplicas int32
	}{
		{
			rs: appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectReplicas: 1,
		},
		{
			rs: appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Replicas: ptr.To[int32](0),
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectReplicas: 0,
		},
		{
			rs: appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Replicas: ptr.To[int32](3),
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectReplicas: 3,
		},
	}

	for _, test := range tests {
		rs := &test.rs
		obj2 := roundTrip(t, runtime.Object(rs))
		rs2, ok := obj2.(*appsv1.ReplicaSet)
		if !ok {
			t.Errorf("unexpected object: %v", rs2)
			t.FailNow()
		}
		if rs2.Spec.Replicas == nil {
			t.Errorf("unexpected nil Replicas")
		} else if test.expectReplicas != *rs2.Spec.Replicas {
			t.Errorf("expected: %d replicas, got: %d", test.expectReplicas, *rs2.Spec.Replicas)
		}
	}
}

// TestDefaultRequestIsNotSetForReplicaSet 校验只设置 limit 时不会自动补出 request。
func TestDefaultRequestIsNotSetForReplicaSet(t *testing.T) {
	s := v1.PodSpec{}
	s.Containers = []v1.Container{
		{
			Resources: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("100m"),
				},
			},
		},
	}
	rs := &appsv1.ReplicaSet{
		Spec: appsv1.ReplicaSetSpec{
			Replicas: ptr.To[int32](3),
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
				Spec: s,
			},
		},
	}
	output := roundTrip(t, runtime.Object(rs))
	rs2 := output.(*appsv1.ReplicaSet)
	// 仅设置 CPU limit 的对象经过默认化后，request 应保持为零值。
	defaultRequest := rs2.Spec.Template.Spec.Containers[0].Resources.Requests
	requestValue := defaultRequest[v1.ResourceCPU]
	if requestValue.String() != "0" {
		t.Errorf("Expected 0 request value, got: %s", requestValue.String())
	}
}

// roundTrip 将对象编码、解码并转换为目标类型，以此触发完整的默认化流程。
func roundTrip(t *testing.T, obj runtime.Object) runtime.Object {
	// 使用 legacy codec 编码，使输出携带默认版本信息。
	data, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(SchemeGroupVersion), obj)
	if err != nil {
		t.Errorf("%v\n %#v", err, obj)
		return nil
	}
	// 通用解码器会在解码时应用默认值。
	obj2, err := runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), data)
	if err != nil {
		t.Errorf("%v\nData: %s\nSource: %#v", err, string(data), obj)
		return nil
	}
	// 显式转换为调用方期望的类型，避免解码结果类型不符。
	obj3 := reflect.New(reflect.TypeOf(obj).Elem()).Interface().(runtime.Object)
	err = legacyscheme.Scheme.Convert(obj2, obj3, nil)
	if err != nil {
		t.Errorf("%v\nSource: %#v", err, obj2)
		return nil
	}
	return obj3
}
