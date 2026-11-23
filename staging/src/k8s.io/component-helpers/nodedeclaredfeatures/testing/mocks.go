/*
Copyright The Kubernetes Authors.

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

// 本文件提供 nodedeclaredfeatures 测试用的 MockFeatureGate 与 MockFeature。
// 两者都把接口方法拆成“按需装配的函数字段”，未配置的方法会直接让测试失败，
// 从而避免 mock 静默返回零值掩盖被测代码漏调用接口的事实。
package testing

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期断言：确保 mock 始终满足真实接口，接口演进时能在构建阶段暴露。
var _ = types.FeatureGate((*MockFeatureGate)(nil))

// MockFeatureGate 用 map 记录 gate 状态，便于测试逐个设置，
// 而不需要构造真实的 feature gate 注册表。
type MockFeatureGate struct {
	t        *testing.T
	features map[string]bool
}

// SetEnabled 设置指定 gate 的开关状态。
func (m *MockFeatureGate) SetEnabled(gate string, enabled bool) {
	m.features[gate] = enabled
}

// Enabled 返回指定 gate 的开关状态；查询未设置的 gate 会让测试失败。
func (m *MockFeatureGate) Enabled(gate string) bool {
	// 区分“已设置但为 false”与“从未设置”：前者是合法测试输入，
	// 后者通常意味着被测代码查询了错误的 gate 名，必须报错而非静默返回 false。
	if enabled, known := m.features[gate]; known {
		return enabled
	}
	m.t.Errorf("unknown gate requested: %v", gate)
	return false
}

// NewMockFeatureGate 创建一个空的 MockFeatureGate。
func NewMockFeatureGate(t *testing.T) *MockFeatureGate {
	return &MockFeatureGate{
		t:        t,
		features: map[string]bool{},
	}
}

// 第二个编译期断言：MockFeature 与 MockFeatureGate 分别对应两个被 mock 的接口。
var _ = types.Feature((*MockFeature)(nil))

// MockFeature 是 types.Feature 接口的 mock 实现，把每个接口方法拆成可按需装配的函数字段。
// MockFeature 是 types.Feature 接口的 mock 实现。
// 用指针字段（**version.Version、**FeatureRequirements）区分"未配置"与
// "配置返回 nil"：后者是合法期望，前者必须在被调用时让测试失败。
type MockFeature struct {
	t                  testing.TB
	name               *string
	discover           func(cfg *types.NodeConfiguration) bool
	inferForScheduling func(podInfo *types.PodInfo) bool
	inferForUpdate     func(oldPodInfo, newPodInfo *types.PodInfo) bool
	maxVersion         **version.Version
	requirements       **types.FeatureRequirements
}

// Name 返回配置的特性名；未配置时让测试失败。
func (m *MockFeature) Name() string {
	if m.name == nil {
		m.t.Errorf("unexpected call to Name")
		return ""
	}
	return *m.name
}

// SetName 配置特性名。
func (m *MockFeature) SetName(name string) {
	m.name = &name
}

// Discover 调用配置的发现逻辑；未配置时让测试失败。
func (m *MockFeature) Discover(cfg *types.NodeConfiguration) bool {
	if m.discover == nil {
		m.t.Errorf("unexpected call to Discover")
		return false
	}
	return m.discover(cfg)
}

// SetDiscover 配置节点特性发现逻辑。
func (m *MockFeature) SetDiscover(discover func(cfg *types.NodeConfiguration) bool) {
	m.discover = discover
}

// InferForScheduling 调用配置的调度推断逻辑；未配置时让测试失败。
func (m *MockFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	if m.inferForScheduling == nil {
		m.t.Errorf("unexpected call to InferForScheduling")
		return false
	}
	return m.inferForScheduling(podInfo)
}

// SetInferForScheduling 配置调度推断逻辑。
func (m *MockFeature) SetInferForScheduling(inferForScheduling func(podInfo *types.PodInfo) bool) {
	m.inferForScheduling = inferForScheduling
}

// InferForUpdate 调用配置的更新推断逻辑；未配置时让测试失败。
func (m *MockFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	if m.inferForUpdate == nil {
		m.t.Errorf("unexpected call to InferForUpdate")
		return false
	}
	return m.inferForUpdate(oldPodInfo, newPodInfo)
}

// SetInferForUpdate 配置更新推断逻辑。
func (m *MockFeature) SetInferForUpdate(inferForUpdate func(oldPodInfo, newPodInfo *types.PodInfo) bool) {
	m.inferForUpdate = inferForUpdate
}

// MaxVersion 返回配置的版本上限；未配置时让测试失败。
func (m *MockFeature) MaxVersion() *version.Version {
	if m.maxVersion == nil {
		m.t.Errorf("unexpected call to MaxVersion")
		return nil
	}
	return *m.maxVersion
}

// SetMaxVersion 配置版本上限。
func (m *MockFeature) SetMaxVersion(maxVersion *version.Version) {
	m.maxVersion = &maxVersion
}

// Requirements 返回配置的依赖信息；未配置时让测试失败。
func (m *MockFeature) Requirements() *nodedeclaredfeatures.FeatureRequirements {
	if m.requirements == nil {
		m.t.Errorf("unexpected call to Requirements")
		return nil
	}
	return *m.requirements
}

// SetRequirements 配置依赖信息。
func (m *MockFeature) SetRequirements(req *nodedeclaredfeatures.FeatureRequirements) {
	m.requirements = &req
}

// NewMockFeature 创建一个未配置的 MockFeature。
// 接受 testing.TB 而非 *testing.T，便于在 benchmark 或 helper 中复用同一个 mock。
func NewMockFeature(t testing.TB) *MockFeature {
	return &MockFeature{
		t: t,
	}
}
