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

package fuzzer

import (
	"fmt"

	"sigs.k8s.io/randfill"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/kubernetes/pkg/apis/apps"
)

// Funcs 返回 apps API 组的 fuzz 函数集合。
// 这些函数把随机数据约束为满足默认值设置和校验要求的对象，
// 避免 roundtrip 测试因为无效对象而误报。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// ControllerRevision 的 Data 必须是合法的原始扩展内容，不能随机填成任意字节。
		func(r *apps.ControllerRevision, c randfill.Continue) {
			c.FillNoCustom(r)
			// 与 runtime.Object 的 fuzz 默认内容保持一致。
			r.Data = runtime.RawExtension{Raw: []byte(`{"apiVersion":"unknown.group/unknown","kind":"Something","someKey":"someValue"}`)}
		},
		// StatefulSet 的标签和状态字段需要与默认值设置一致，否则转换往返会丢失信息。
		func(s *apps.StatefulSet, c randfill.Continue) {
			c.FillNoCustom(s) // fuzz self without calling this function again

			// labels 为空时由 pod 模板标签补齐，与 API 默认行为一致。
			if len(s.Labels) == 0 {
				s.Labels = s.Spec.Template.Labels
			}
			// 这两个状态字段在外部版本是指针，nil 与非 nil 往返后会不一致，统一填成非 nil。
			if s.Status.ObservedGeneration == nil {
				s.Status.ObservedGeneration = new(int64)
			}
			if s.Status.CollisionCount == nil {
				s.Status.CollisionCount = new(int32)
			}
		},
		// StatefulSetSpec 的 selector、历史记录和 Pod 管理策略都有服务端默认值。
		func(s *apps.StatefulSetSpec, c randfill.Continue) {
			c.FillNoCustom(s)

			// selector 为空时会由 pod 模板标签推导，先补齐以避免往返后结果不同。
			if s.Selector == nil {
				s.Selector = &metav1.LabelSelector{MatchLabels: s.Template.Labels}
			}
			if s.RevisionHistoryLimit == nil {
				s.RevisionHistoryLimit = new(int32)
				*s.RevisionHistoryLimit = 10
			}
			// 空策略会被默认成 OrderedReady，这里显式给出合法值。
			if len(s.PodManagementPolicy) == 0 {
				s.PodManagementPolicy = apps.OrderedReadyPodManagement
			}
		},
		// PVC 保留策略的两个字段默认都是 Retain，随机空值会导致往返不一致。
		func(s *apps.StatefulSetPersistentVolumeClaimRetentionPolicy, c randfill.Continue) {
			c.FillNoCustom(s)

			if s == nil {
				s = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{}
			}
			// Retain 是默认策略，空字符串不是合法枚举值。
			if len(s.WhenDeleted) == 0 {
				s.WhenDeleted = apps.RetainPersistentVolumeClaimRetentionPolicyType
			}
			if len(s.WhenScaled) == 0 {
				s.WhenScaled = apps.RetainPersistentVolumeClaimRetentionPolicyType
			}
		},
		// StatefulSetUpdateStrategy 的 Type 必须是合法枚举，且要与 RollingUpdate 参数配套。
		func(s *apps.StatefulSetUpdateStrategy, c randfill.Continue) {
			c.FillNoCustom(s)

			strategyTypes := []apps.StatefulSetUpdateStrategyType{
				apps.RollingUpdateStatefulSetStrategyType,
				apps.OnDeleteStatefulSetStrategyType,
				apps.RecreateStatefulSetStrategyType}

			// 只从合法策略中取值，避免随机字符串进入转换链。
			s.Type = strategyTypes[c.Rand.Intn(len(strategyTypes))]
			// Recreate 和 OnDelete 不使用滚动更新参数，必须清空，否则校验会失败。
			if s.Type == apps.RecreateStatefulSetStrategyType || s.Type == apps.OnDeleteStatefulSetStrategyType {
				s.RollingUpdate = nil
			}

			// 外部版本该字段是指针，nil 与默认值 1 往返后不等价。
			if s.RollingUpdate != nil && s.RollingUpdate.MaxUnavailable == nil {
				s.RollingUpdate.MaxUnavailable = new(intstr.FromInt32(1))
			}
		},
		// Deployment 的 selector 和 labels 都由 pod 模板标签补齐，与默认逻辑一致。
		func(j *apps.Deployment, c randfill.Continue) {
			c.FillNoCustom(j)

			// 与默认值设置保持一致。
			if j.Spec.Selector == nil {
				j.Spec.Selector = &metav1.LabelSelector{MatchLabels: j.Spec.Template.Labels}
			}
			if len(j.Labels) == 0 {
				j.Labels = j.Spec.Template.Labels
			}
		},
		// DeploymentSpec 的两个指针字段在外部版本始终有值，随机 nil 无法往返。
		func(j *apps.DeploymentSpec, c randfill.Continue) {
			c.FillNoCustom(j) // fuzz self without calling this function again
			rhl := int32(c.Rand.Int31())
			pds := int32(c.Rand.Int31())
			j.RevisionHistoryLimit = &rhl
			j.ProgressDeadlineSeconds = &pds
		},
		// DeploymentStrategy 的策略类型和滚动参数必须互相匹配。
		func(j *apps.DeploymentStrategy, c randfill.Continue) {
			c.FillNoCustom(j) // fuzz self without calling this function again
			// 确保 strategyType 只取合法枚举值。
			strategyTypes := []apps.DeploymentStrategyType{apps.RecreateDeploymentStrategyType, apps.RollingUpdateDeploymentStrategyType}
			j.Type = strategyTypes[c.Rand.Intn(len(strategyTypes))]
			// 只有 RollingUpdate 策略才允许携带滚动参数。
			if j.Type != apps.RollingUpdateDeploymentStrategyType {
				j.RollingUpdate = nil
			} else {
				rollingUpdate := apps.RollingUpdateDeployment{}
				if c.Bool() {
					rollingUpdate.MaxUnavailable = intstr.FromInt32(c.Rand.Int31())
					rollingUpdate.MaxSurge = intstr.FromInt32(c.Rand.Int31())
				} else {
					rollingUpdate.MaxSurge = intstr.FromString(fmt.Sprintf("%d%%", c.Rand.Int31()))
				}
				j.RollingUpdate = &rollingUpdate
			}
		},
		// DaemonSet 的 template.Generation 是系统管理字段，默认会被清零。
		func(j *apps.DaemonSet, c randfill.Continue) {
			c.FillNoCustom(j)

			// 与 defaulter 的默认行为保持一致。
			j.Spec.Template.Generation = 0
			if len(j.ObjectMeta.Labels) == 0 {
				j.ObjectMeta.Labels = j.Spec.Template.ObjectMeta.Labels
			}
		},
		// DaemonSetSpec 的 RevisionHistoryLimit 在外部版本是指针，需随机出非 nil 值。
		func(j *apps.DaemonSetSpec, c randfill.Continue) {
			c.FillNoCustom(j) // fuzz self without calling this function again
			rhl := int32(c.Rand.Int31())
			j.RevisionHistoryLimit = &rhl
		},
		// DaemonSetUpdateStrategy 的策略类型与滚动参数需要配套生成。
		func(j *apps.DaemonSetUpdateStrategy, c randfill.Continue) {
			c.FillNoCustom(j) // fuzz self without calling this function again
			// 确保 strategyType 只取合法枚举值。
			strategyTypes := []apps.DaemonSetUpdateStrategyType{apps.RollingUpdateDaemonSetStrategyType, apps.OnDeleteDaemonSetStrategyType}
			j.Type = strategyTypes[c.Rand.Intn(len(strategyTypes))]
			// OnDelete 策略不接受滚动参数，否则 roundtrip 时会被清掉。
			if j.Type != apps.RollingUpdateDaemonSetStrategyType {
				j.RollingUpdate = nil
			} else {
				rollingUpdate := apps.RollingUpdateDaemonSet{}
				if c.Bool() {
					if c.Bool() {
						rollingUpdate.MaxUnavailable = intstr.FromInt32(c.Rand.Int31())
						rollingUpdate.MaxSurge = intstr.FromInt32(c.Rand.Int31())
					} else {
						rollingUpdate.MaxSurge = intstr.FromString(fmt.Sprintf("%d%%", c.Rand.Int31()))
					}
				}

				j.RollingUpdate = &rollingUpdate
			}
		},
		// ReplicaSet 的 selector 和 labels 默认取自 pod 模板标签。
		func(j *apps.ReplicaSet, c randfill.Continue) {
			c.FillNoCustom(j)

			// 与 defaulter 的默认行为保持一致。
			if j.Spec.Selector == nil {
				j.Spec.Selector = &metav1.LabelSelector{MatchLabels: j.Spec.Template.Labels}
			}
			if len(j.Labels) == 0 {
				j.Labels = j.Spec.Template.Labels
			}
		},
	}
}
