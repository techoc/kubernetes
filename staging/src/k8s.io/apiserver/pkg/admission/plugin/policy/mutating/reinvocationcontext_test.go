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
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestFullReinvocation 覆盖"输出变化会触发全部已执行策略重调用"的完整流程：
// 每次重调用都可能再次改变对象，因此需要反复重调用直到收敛。
func TestFullReinvocation(t *testing.T) {
	key1 := key{PolicyUID: types.NamespacedName{Name: "p1"}, BindingUID: types.NamespacedName{Name: "b1"}}
	key2 := key{PolicyUID: types.NamespacedName{Name: "p2"}, BindingUID: types.NamespacedName{Name: "b2"}}
	key3 := key{PolicyUID: types.NamespacedName{Name: "p3"}, BindingUID: types.NamespacedName{Name: "b3"}}

	cm1v1 := &v1.ConfigMap{Data: map[string]string{"v": "1"}}
	cm1v2 := &v1.ConfigMap{Data: map[string]string{"v": "2"}}

	rc := policyReinvokeContext{}

	// key1 is invoked and it updates the configmap
	rc.SetLastPolicyInvocationOutput(cm1v1)
	rc.RequireReinvokingPreviouslyInvokedPlugins()
	rc.AddReinvocablePolicyToPreviouslyInvoked(key1)

	assert.True(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v2))

	// key2 is invoked and it updates the configmap
	rc.SetLastPolicyInvocationOutput(cm1v2)
	rc.RequireReinvokingPreviouslyInvokedPlugins()
	rc.AddReinvocablePolicyToPreviouslyInvoked(key2)

	assert.True(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v1))

	// key3 is invoked but it does not change anything
	rc.AddReinvocablePolicyToPreviouslyInvoked(key3)

	assert.False(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v2))

	// key1 is reinvoked
	assert.True(t, rc.ShouldReinvoke(key1))
	rc.AddReinvocablePolicyToPreviouslyInvoked(key1)
	rc.SetLastPolicyInvocationOutput(cm1v1)

	assert.True(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v2))
	rc.RequireReinvokingPreviouslyInvokedPlugins()

	// key2 is reinvoked
	assert.True(t, rc.ShouldReinvoke(key2))
	rc.AddReinvocablePolicyToPreviouslyInvoked(key2)
	rc.SetLastPolicyInvocationOutput(cm1v2)

	assert.True(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v1))
	rc.RequireReinvokingPreviouslyInvokedPlugins()

	// key3 is reinvoked, because the reinvocations have changed the resource
	assert.True(t, rc.ShouldReinvoke(key3))
}

// TestPartialReinvocation 验证仅当对象在上次调用后确实变化时，才需要重调用其余策略：
// key1 被重调用后输出未变，因此 key2/key3 无需再执行。
func TestPartialReinvocation(t *testing.T) {
	key1 := key{PolicyUID: types.NamespacedName{Name: "p1"}, BindingUID: types.NamespacedName{Name: "b1"}}
	key2 := key{PolicyUID: types.NamespacedName{Name: "p2"}, BindingUID: types.NamespacedName{Name: "b2"}}
	key3 := key{PolicyUID: types.NamespacedName{Name: "p3"}, BindingUID: types.NamespacedName{Name: "b3"}}

	cm1v1 := &v1.ConfigMap{Data: map[string]string{"v": "1"}}
	cm1v2 := &v1.ConfigMap{Data: map[string]string{"v": "2"}}

	rc := policyReinvokeContext{}

	// key1 is invoked and it updates the configmap
	rc.SetLastPolicyInvocationOutput(cm1v1)
	rc.RequireReinvokingPreviouslyInvokedPlugins()
	rc.AddReinvocablePolicyToPreviouslyInvoked(key1)

	assert.True(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v2))

	// key2 is invoked and it updates the configmap
	rc.SetLastPolicyInvocationOutput(cm1v2)
	rc.RequireReinvokingPreviouslyInvokedPlugins()
	rc.AddReinvocablePolicyToPreviouslyInvoked(key2)

	assert.True(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v1))

	// key3 is invoked but it does not change anything
	rc.AddReinvocablePolicyToPreviouslyInvoked(key3)

	assert.False(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v2))

	// key1 is reinvoked but does not change anything
	assert.True(t, rc.ShouldReinvoke(key1))

	// key2 is not reinvoked because nothing changed since last invocation
	assert.False(t, rc.ShouldReinvoke(key2))

	// key3 is not reinvoked because nothing changed since last invocation
	assert.False(t, rc.ShouldReinvoke(key3))
}

// TestNoReinvocation 验证所有策略输出都未改变时不触发任何重调用。
func TestNoReinvocation(t *testing.T) {
	key1 := key{PolicyUID: types.NamespacedName{Name: "p1"}, BindingUID: types.NamespacedName{Name: "b1"}}
	key2 := key{PolicyUID: types.NamespacedName{Name: "p2"}, BindingUID: types.NamespacedName{Name: "b2"}}
	key3 := key{PolicyUID: types.NamespacedName{Name: "p3"}, BindingUID: types.NamespacedName{Name: "b3"}}

	cm1v1 := &v1.ConfigMap{Data: map[string]string{"v": "1"}}

	rc := policyReinvokeContext{}

	// key1 is invoked and it updates the configmap
	rc.AddReinvocablePolicyToPreviouslyInvoked(key1)
	rc.SetLastPolicyInvocationOutput(cm1v1)

	assert.False(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v1))

	// key2 is invoked but does not change anything
	rc.AddReinvocablePolicyToPreviouslyInvoked(key2)
	rc.SetLastPolicyInvocationOutput(cm1v1)

	assert.False(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v1))

	// key3 is invoked but it does not change anything
	rc.AddReinvocablePolicyToPreviouslyInvoked(key3)
	rc.SetLastPolicyInvocationOutput(cm1v1)

	assert.False(t, rc.IsOutputChangedSinceLastPolicyInvocation(cm1v1))

	// no keys are reinvoked
	assert.False(t, rc.ShouldReinvoke(key1))
	assert.False(t, rc.ShouldReinvoke(key2))
	assert.False(t, rc.ShouldReinvoke(key3))

}
