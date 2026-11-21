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

package v1beta1

import (
	"k8s.io/api/flowcontrol/v1beta1"
)

// flow-schema 的默认设置。
const (
	FlowSchemaDefaultMatchingPrecedence int32 = 1000
)

// priority-level-configuration 的默认设置。
// 注意此版本仍使用 AssuredConcurrencyShares 字段（v1beta3 起改名为 NominalConcurrencyShares）。
const (
	PriorityLevelConfigurationDefaultHandSize                 int32 = 8
	PriorityLevelConfigurationDefaultQueues                   int32 = 64
	PriorityLevelConfigurationDefaultQueueLengthLimit         int32 = 50
	PriorityLevelConfigurationDefaultAssuredConcurrencyShares int32 = 30
)

// SetDefaults_FlowSchemaSpec 为 flow schema 规格设置默认值：
// 未指定匹配优先级时使用默认值，使不同 schema 之间有确定的先后顺序。
func SetDefaults_FlowSchemaSpec(spec *v1beta1.FlowSchemaSpec) {
	if spec.MatchingPrecedence == 0 {
		spec.MatchingPrecedence = FlowSchemaDefaultMatchingPrecedence
	}
}

// SetDefaults_ExemptPriorityLevelConfiguration 为豁免型优先级级别配置设置默认值：
// 豁免级别不参与并发限制，名义并发份额和可借出比例默认为 0。
func SetDefaults_ExemptPriorityLevelConfiguration(eplc *v1beta1.ExemptPriorityLevelConfiguration) {
	if eplc.NominalConcurrencyShares == nil {
		eplc.NominalConcurrencyShares = new(int32)
		*eplc.NominalConcurrencyShares = 0
	}
	if eplc.LendablePercent == nil {
		eplc.LendablePercent = new(int32)
		*eplc.LendablePercent = 0
	}
}

// SetDefaults_LimitedPriorityLevelConfiguration 为限制型优先级级别配置设置默认值：
// 保证并发份额使用默认值，可借出比例默认为 0。
func SetDefaults_LimitedPriorityLevelConfiguration(lplc *v1beta1.LimitedPriorityLevelConfiguration) {
	if lplc.AssuredConcurrencyShares == 0 {
		lplc.AssuredConcurrencyShares = PriorityLevelConfigurationDefaultAssuredConcurrencyShares
	}
	if lplc.LendablePercent == nil {
		lplc.LendablePercent = new(int32)
		*lplc.LendablePercent = 0
	}
}

// SetDefaults_QueuingConfiguration 为排队配置设置默认值：
// 手数、队列数和队列长度限制分别使用默认常量，保证请求排队行为可预测。
func SetDefaults_QueuingConfiguration(cfg *v1beta1.QueuingConfiguration) {
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
