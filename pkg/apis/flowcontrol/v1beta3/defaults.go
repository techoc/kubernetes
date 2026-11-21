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

package v1beta3

import (
	"k8s.io/api/flowcontrol/v1beta3"
	"k8s.io/utils/ptr"
)

// flow-schema 的默认设置。
const (
	FlowSchemaDefaultMatchingPrecedence int32 = 1000
)

// priority-level-configuration 的默认设置。
const (
	PriorityLevelConfigurationDefaultHandSize                 int32 = 8
	PriorityLevelConfigurationDefaultQueues                   int32 = 64
	PriorityLevelConfigurationDefaultQueueLengthLimit         int32 = 50
	PriorityLevelConfigurationDefaultNominalConcurrencyShares int32 = 30
)

// SetDefaults_FlowSchemaSpec 为 flow schema 规格设置默认值：
// 未指定匹配优先级时使用默认值，使不同 schema 之间有确定的先后顺序。
func SetDefaults_FlowSchemaSpec(spec *v1beta3.FlowSchemaSpec) {
	if spec.MatchingPrecedence == 0 {
		spec.MatchingPrecedence = FlowSchemaDefaultMatchingPrecedence
	}
}

// SetDefaults_PriorityLevelConfiguration 为 PriorityLevelConfiguration 对象设置默认值。
// 由于需要检查往返注解是否存在，才能判断用户是否显式把 NominalConcurrencyShares 设为 0，
// 因此这里的默认化逻辑必须能看到对象的 annotations 字段。
func SetDefaults_PriorityLevelConfiguration(in *v1beta3.PriorityLevelConfiguration) {
	if limited := in.Spec.Limited; limited != nil {
		// 对 v1beta3，仅在同时满足以下条件时才对 NominalConcurrencyShares 应用默认值：
		//   a) NominalConcurrencyShares == 0，且
		//   b) 往返注解未设置（即用户没有显式要求保留零值）。
		if _, ok := in.Annotations[v1beta3.PriorityLevelPreserveZeroConcurrencySharesKey]; !ok && limited.NominalConcurrencyShares == 0 {
			limited.NominalConcurrencyShares = PriorityLevelConfigurationDefaultNominalConcurrencyShares
		}
	}
}

// SetDefaults_ExemptPriorityLevelConfiguration 为豁免型优先级级别配置设置默认值：
// 豁免级别不参与并发限制，名义并发份额和可借出比例默认为 0。
func SetDefaults_ExemptPriorityLevelConfiguration(eplc *v1beta3.ExemptPriorityLevelConfiguration) {
	if eplc.NominalConcurrencyShares == nil {
		eplc.NominalConcurrencyShares = ptr.To(int32(0))
	}
	if eplc.LendablePercent == nil {
		eplc.LendablePercent = ptr.To(int32(0))
	}
}

// SetDefaults_LimitedPriorityLevelConfiguration 为限制型优先级级别配置设置默认值：
// 可借出比例默认为 0；名义并发份额的默认值由 SetDefaults_PriorityLevelConfiguration 结合往返注解处理。
func SetDefaults_LimitedPriorityLevelConfiguration(in *v1beta3.LimitedPriorityLevelConfiguration) {
	if in.LendablePercent == nil {
		in.LendablePercent = ptr.To(int32(0))
	}
}

// SetDefaults_QueuingConfiguration 为排队配置设置默认值：
// 手数、队列数和队列长度限制分别使用默认常量，保证请求排队行为可预测。
func SetDefaults_QueuingConfiguration(cfg *v1beta3.QueuingConfiguration) {
	if cfg.HandSize == 0 {
		cfg.HandSize = PriorityLevelConfigurationDefaultHandSize
	}
	if cfg.Queues == 0 {
		cfg.Queues = PriorityLevelConfigurationDefaultQueues
	}
	if cfg.QueueLengthLimit == 0 {
		cfg.QueueLengthLimit = PriorityLevelConfigurationDefaultQueueLengthLimit
	}
}
