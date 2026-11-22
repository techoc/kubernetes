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

// 本文件用于检测 Framework 接口是否发生变化。
// 这对于 `github.com/kubernetes*` 组织下导入 Framework 的项目很重要，
// 例如 cluster-autoscaler，避免其代码被破坏。

package contract

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

// frameworkContract 固定了下游项目依赖的 Framework 方法签名；
// 若这些方法发生变化，下面的赋值将无法编译，从而及时暴露破坏性改动。
type frameworkContract interface {
	RunPreFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod) (*fwk.PreFilterResult, *fwk.Status, sets.Set[string])
	RunFilterPlugins(context.Context, fwk.CycleState, *v1.Pod, fwk.NodeInfo) *fwk.Status
	RunReservePluginsReserve(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status
}

// TestFrameworkContract 通过编译期赋值检测 Framework 接口方法是否被改动。
func TestFrameworkContract(t *testing.T) {
	var f framework.Framework
	var c frameworkContract = f
	assert.Nil(t, c)
}

// TestNewFramework 检测 NewFramework 返回的实例仍满足 framework.Framework 接口。
func TestNewFramework(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	var f interface{}
	if f, _ = runtime.NewFramework(ctx, nil, nil); f != nil {
		_, ok := f.(framework.Framework)
		assert.True(t, ok)
	}
}

// TestNewCycleState 检测 NewCycleState 返回的实例仍满足 *framework.CycleState 类型。
func TestNewCycleState(t *testing.T) {
	var state interface{} = framework.NewCycleState()
	_, ok := state.(*framework.CycleState)
	assert.True(t, ok)
}
