/*
Copyright 2025 The Kubernetes Authors.

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

package inplacepodresize

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// TestPodLevelResourcesResizeFeature_Requirements 锁定依赖声明：框架要据此判断
// 某个节点是否声明了该能力，少一个 gate 或多一个 gate 都会让 feature 在
// 本应可用/不可用的节点上被错误地声明。
func TestPodLevelResourcesResizeFeature_Requirements(t *testing.T) {
	feature := &podLevelResourcesResizeFeature{}
	reqs := feature.Requirements()
	if reqs == nil {
		t.Fatalf("Feature %s returned nil Requirements", feature.Name())
	}
	if reqs.EnabledFeatureGates == nil || len(reqs.EnabledFeatureGates) != 1 || reqs.EnabledFeatureGates[0] != IPPRPodLevelResourcesFeatureGate {
		t.Fatalf("Feature %s Requirements should declare exactly the %s feature gate", feature.Name(), IPPRPodLevelResourcesFeatureGate)
	}
}

// TestPodLevelResourcesResizeFeatureDiscover 检查 Discover 严格跟随 feature gate：
// 这个能力只由 gate 决定，不涉及运行时探测或静态配置，
// 所以开关打开就必须声明、关闭就必须不声明，没有中间状态。
func TestPodLevelResourcesResizeFeatureDiscover(t *testing.T) {
	tests := []struct {
		name        string
		featureGate bool
		expected    bool
	}{
		{
			name:        "FeatureGateEnabled",
			featureGate: true,
			expected:    true,
		},
		{
			name:        "FeatureGateDisabled",
			featureGate: false,
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &types.NodeConfiguration{FeatureGates: types.FeatureGateMap{IPPRPodLevelResourcesFeatureGate: tt.featureGate}}
			enabled := PodLevelResourcesResizeFeature.Discover(cfg)
			if want, got := tt.expected, enabled; want != got {
				t.Fatalf("want=%v,got=%v", want, got)
			}
		})
	}
}

// TestPodLevelResourcesResizeFeatureInferForScheduling 固定“调度阶段不关心此能力”：
// 原地调整 pod 级资源只会发生在已存在的 pod 上，调度新 pod 时不需要节点声明它。
func TestPodLevelResourcesResizeFeatureInferForScheduling(t *testing.T) {
	podInfo := &types.PodInfo{Spec: &v1.PodSpec{}, Status: &v1.PodStatus{}}
	if PodLevelResourcesResizeFeature.InferForScheduling(podInfo) {
		t.Fatalf("InferForScheduling should always be false")
	}
}

// TestPodLevelResourcesResizeFeatureInferForUpdate 覆盖 pod 级 resources 变化的几种形态：
// 新增、删除、请求量变化都要触发，字段未变或两边都为空则不触发。
// 这里用语义相等判断“是否变化”，是为了对齐 API 层的语义比较规则。
func TestPodLevelResourcesResizeFeatureInferForUpdate(t *testing.T) {
	tests := []struct {
		name       string
		oldPodInfo *types.PodInfo
		newPodInfo *types.PodInfo
		expected   bool
	}{
		{
			name: "NoResourcesChanged",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
				Status: &v1.PodStatus{},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
				Status: &v1.PodStatus{},
			},
			expected: false,
		},
		{
			name: "ResourcesAdded",
			oldPodInfo: &types.PodInfo{
				Spec:   &v1.PodSpec{},
				Status: &v1.PodStatus{},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
				Status: &v1.PodStatus{},
			},
			expected: true,
		},
		{
			name: "ResourcesRemoved",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
				Status: &v1.PodStatus{},
			},
			newPodInfo: &types.PodInfo{
				Spec:   &v1.PodSpec{},
				Status: &v1.PodStatus{},
			},
			expected: true,
		},
		{
			name: "ResourcesChanged",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
				Status: &v1.PodStatus{},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
				},
				Status: &v1.PodStatus{},
			},
			expected: true,
		},
		{
			name: "NilResources",
			oldPodInfo: &types.PodInfo{
				Spec:   &v1.PodSpec{},
				Status: &v1.PodStatus{},
			},
			newPodInfo: &types.PodInfo{
				Spec:   &v1.PodSpec{},
				Status: &v1.PodStatus{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if want, got := tt.expected, PodLevelResourcesResizeFeature.InferForUpdate(tt.oldPodInfo, tt.newPodInfo); want != got {
				t.Fatalf("want=%v,got=%v", want, got)
			}
		})
	}
}
