/*
Copyright 2024 The Kubernetes Authors.

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

package mutating

import (
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

// key 唯一标识一次“策略+绑定+参数+变更序号”的调用。
// 用 NamespacedName 而非对象本身做键，避免把大对象放进集合，也天然处理同名重建。
type key struct {
	PolicyUID     types.NamespacedName
	BindingUID    types.NamespacedName
	ParamUID      types.NamespacedName
	MutationIndex int
}

// policyReinvokeContext 记录一次准入调用链中 mutating 策略的执行状态，
// 用于实现 reinvocation：当后续策略修改了对象时，重跑之前那些可重入的策略，
// 直到达到稳定状态或次数上限。
type policyReinvokeContext struct {
	// lastPolicyOutput holds the result of the last Policy admission plugin call
	lastPolicyOutput runtime.Object
	// previouslyInvokedReinvocablePolicys holds the set of policies that have been invoked and
	// should be reinvoked if a later mutation occurs
	previouslyInvokedReinvocablePolicies sets.Set[key]
	// reinvokePolicies holds the set of Policies that should be reinvoked
	reinvokePolicies sets.Set[key]
}

// ShouldReinvoke 判断指定策略本轮是否被标记为需要重跑。
func (rc *policyReinvokeContext) ShouldReinvoke(policy key) bool {
	return rc.reinvokePolicies.Has(policy)
}

// IsOutputChangedSinceLastPolicyInvocation 用语义相等判断对象自上次策略调用后是否变化；
// 使用 Semantic 而非 reflect.DeepEqual 是为了忽略零值与空值等无意义差异。
func (rc *policyReinvokeContext) IsOutputChangedSinceLastPolicyInvocation(object runtime.Object) bool {
	return !apiequality.Semantic.DeepEqual(rc.lastPolicyOutput, object)
}

// SetLastPolicyInvocationOutput 保存策略输出的快照。
// 必须深拷贝：调用方后续会继续修改对象，直接持有引用会让“是否变化”永远为 false。
func (rc *policyReinvokeContext) SetLastPolicyInvocationOutput(object runtime.Object) {
	if object == nil {
		rc.lastPolicyOutput = nil
		return
	}
	rc.lastPolicyOutput = object.DeepCopyObject()
}

// AddReinvocablePolicyToPreviouslyInvoked 记录一个已执行且支持重入的策略，惰性初始化集合。
func (rc *policyReinvokeContext) AddReinvocablePolicyToPreviouslyInvoked(policy key) {
	if rc.previouslyInvokedReinvocablePolicies == nil {
		rc.previouslyInvokedReinvocablePolicies = sets.New[key]()
	}
	rc.previouslyInvokedReinvocablePolicies.Insert(policy)
}

// RequireReinvokingPreviouslyInvokedPlugins 把“曾执行且可重入”的集合整体升级为“本轮需重跑”，
// 然后清空前者，避免同一策略在后续轮次被重复登记。仅在集合非空时分配，减少稳态开销。
func (rc *policyReinvokeContext) RequireReinvokingPreviouslyInvokedPlugins() {
	if len(rc.previouslyInvokedReinvocablePolicies) > 0 {
		if rc.reinvokePolicies == nil {
			rc.reinvokePolicies = sets.New[key]()
		}
		for s := range rc.previouslyInvokedReinvocablePolicies {
			rc.reinvokePolicies.Insert(s)
		}
		rc.previouslyInvokedReinvocablePolicies = sets.New[key]()
	}
}
