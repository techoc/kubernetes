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

package apihelpers

import (
	"sort"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// SetFlowSchemaCondition sets conditions.
// 相同 Type 的条件已存在时原地更新；仅当 Status 变化才推进 LastTransitionTime，
// 避免每次调谐都刷新时间戳导致外部观察者误判状态抖动。
func SetFlowSchemaCondition(flowSchema *flowcontrol.FlowSchema, newCondition flowcontrol.FlowSchemaCondition) {
	existingCondition := GetFlowSchemaConditionByType(flowSchema, newCondition.Type)
	if existingCondition == nil {
		flowSchema.Status.Conditions = append(flowSchema.Status.Conditions, newCondition)
		return
	}

	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		existingCondition.LastTransitionTime = newCondition.LastTransitionTime
	}

	existingCondition.Reason = newCondition.Reason
	existingCondition.Message = newCondition.Message
}

// GetFlowSchemaConditionByType gets conditions.
// 返回切片元素的指针，调用方可直接修改；未找到返回 nil。
func GetFlowSchemaConditionByType(flowSchema *flowcontrol.FlowSchema, conditionType flowcontrol.FlowSchemaConditionType) *flowcontrol.FlowSchemaCondition {
	for i := range flowSchema.Status.Conditions {
		if flowSchema.Status.Conditions[i].Type == conditionType {
			return &flowSchema.Status.Conditions[i]
		}
	}
	return nil
}

// SetPriorityLevelConfigurationCondition sets conditions.
// 语义与 SetFlowSchemaCondition 相同，同样只在 Status 变化时更新时间戳。
func SetPriorityLevelConfigurationCondition(priorityLevel *flowcontrol.PriorityLevelConfiguration, newCondition flowcontrol.PriorityLevelConfigurationCondition) {
	existingCondition := GetPriorityLevelConfigurationConditionByType(priorityLevel, newCondition.Type)
	if existingCondition == nil {
		priorityLevel.Status.Conditions = append(priorityLevel.Status.Conditions, newCondition)
		return
	}

	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		existingCondition.LastTransitionTime = newCondition.LastTransitionTime
	}

	existingCondition.Reason = newCondition.Reason
	existingCondition.Message = newCondition.Message
}

// GetPriorityLevelConfigurationConditionByType gets conditions.
func GetPriorityLevelConfigurationConditionByType(priorityLevel *flowcontrol.PriorityLevelConfiguration, conditionType flowcontrol.PriorityLevelConfigurationConditionType) *flowcontrol.PriorityLevelConfigurationCondition {
	for i := range priorityLevel.Status.Conditions {
		if priorityLevel.Status.Conditions[i].Type == conditionType {
			return &priorityLevel.Status.Conditions[i]
		}
	}
	return nil
}

var _ sort.Interface = FlowSchemaSequence{}

// FlowSchemaSequence holds sorted set of pointers to FlowSchema objects.
// FlowSchemaSequence implements `sort.Interface`
//
// 排序规则与流控匹配顺序一致：matchingPrecedence 小的在前，相同时按名称升序，
// 保证同一组配置在任何 API server 上得到相同顺序。
type FlowSchemaSequence []*flowcontrol.FlowSchema

// Len 实现 sort.Interface。
func (s FlowSchemaSequence) Len() int {
	return len(s)
}

// Less 先比较 matchingPrecedence，再以名称打破平局。
func (s FlowSchemaSequence) Less(i, j int) bool {
	// the flow-schema w/ lower matching-precedence is prior
	if ip, jp := s[i].Spec.MatchingPrecedence, s[j].Spec.MatchingPrecedence; ip != jp {
		return ip < jp
	}
	// sort alphabetically
	return s[i].Name < s[j].Name
}

// Swap 实现 sort.Interface。
func (s FlowSchemaSequence) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// metaInternalVersionCodecsWithCBOR 在基础 meta 编解码器上追加 CBOR 支持；
// 预先构造以避免每次请求都新建 codec factory。
var metaInternalVersionCodecsWithCBOR = serializer.NewCodecFactory(metainternalversionscheme.Scheme, serializer.WithSerializer(cbor.NewSerializerInfo))

// GetMetaInternalVersionCodecs returns a negotiated serializer that recognizes the types from
// k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme.Scheme. It will or will include a CBOR
// serializer if CBOR is enabled.
func GetMetaInternalVersionCodecs() runtime.NegotiatedSerializer {
	if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
		return metaInternalVersionCodecsWithCBOR
	}
	return metainternalversionscheme.Codecs
}
