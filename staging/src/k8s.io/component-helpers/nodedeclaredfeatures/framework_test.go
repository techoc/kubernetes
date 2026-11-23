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

package nodedeclaredfeatures

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// mockFeature 用字段函数代替真实实现，让每个测试只提供关心的判定逻辑。
// 缺省行为刻意设为"可被发现但不产生要求"，减少用例样板。
type mockFeature struct {
	name               string
	discover           func(cfg *types.NodeConfiguration) bool
	inferForScheduling func(podInfo *types.PodInfo) bool
	inferForUpdate     func(oldPodInfo, newPodInfo *types.PodInfo) bool
	maxVersion         *version.Version
	requirements       func() *FeatureRequirements
}

// Name 返回 mock 特性的名字。
func (f *mockFeature) Name() string { return f.name }

// Discover 未配置时默认返回 true，让用例只需关心自己要验证的分支。
func (f *mockFeature) Discover(cfg *types.NodeConfiguration) bool {
	if f.discover != nil {
		return f.discover(cfg)
	}
	return true
}

// InferForScheduling 未配置时默认返回 false（不产生调度要求）。
func (f *mockFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	if f.inferForScheduling != nil {
		return f.inferForScheduling(podInfo)
	}
	return false
}

// InferForUpdate 未配置时默认返回 false（不产生更新要求）。
func (f *mockFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	if f.inferForUpdate != nil {
		return f.inferForUpdate(oldPodInfo, newPodInfo)
	}
	return false
}

// MaxVersion 返回用例配置的版本上限，nil 表示无上限。
func (f *mockFeature) MaxVersion() *version.Version { return f.maxVersion }

// Requirements 返回用例配置的依赖；未配置时调用会 panic，提醒测试补齐期望。
func (f *mockFeature) Requirements() *FeatureRequirements { return f.requirements() }

// mockFeatureGate 是 FeatureGate 的极简 map 实现，供发现逻辑测试使用。
type mockFeatureGate struct {
	features map[string]bool
}

// Enabled 查询 gate 是否打开；未配置的 gate 返回 false。
func (m *mockFeatureGate) Enabled(key string) bool {
	if m.features == nil {
		return false
	}
	return m.features[key]
}

// newMockFeatureGate 根据给定的 gate 状态表构造 mock。
func newMockFeatureGate(features map[string]bool) *mockFeatureGate {
	return &mockFeatureGate{features: features}
}

// newTestFramework 只按名字造特性，用于匹配类测试。
// New 本身不排序入参之外的东西，这里先排序是为了让预期列表稳定。
func newTestFramework(features ...string) *Framework {
	slices.Sort(features)

	allFeatures := make([]Feature, len(features))
	for i, name := range features {
		allFeatures[i] = &mockFeature{name: name}
	}

	return New(allFeatures)
}

// 发现逻辑涉及开关、排序和版本上限三条规则，逐个用例固定住行为。
func TestDiscoverNodeFeatures(t *testing.T) {
	featureMaxVersion := version.MustParse("1.38.0")
	registry := []types.Feature{
		&mockFeature{
			name: "FeatureA",
			discover: func(cfg *types.NodeConfiguration) bool {
				return cfg.FeatureGates.Enabled("feature-a")
			},
			requirements: func() *FeatureRequirements {
				return &FeatureRequirements{
					EnabledFeatureGates: []string{"feature-a"},
				}
			},
			maxVersion: featureMaxVersion,
		},
		&mockFeature{
			name: "FeatureB",
			discover: func(cfg *types.NodeConfiguration) bool {
				return cfg.FeatureGates.Enabled("feature-b")
			},
			requirements: func() *FeatureRequirements {
				return &FeatureRequirements{
					EnabledFeatureGates: []string{"feature-b"},
				}
			},
			maxVersion: featureMaxVersion,
		},
	}

	framework := New(registry)

	testCases := []struct {
		name     string
		config   *types.NodeConfiguration
		expected []string
	}{
		{
			name: "Feature Enabled",
			config: &types.NodeConfiguration{
				FeatureGates: newMockFeatureGate(map[string]bool{string("feature-a"): true}),
			},
			expected: []string{"FeatureA"},
		},
		{
			name: "multiple features enabled",
			config: &types.NodeConfiguration{
				FeatureGates: newMockFeatureGate(map[string]bool{
					string("feature-a"): true,
					string("feature-b"): true,
				}),
			},
			expected: []string{"FeatureA", "FeatureB"}, // 注册表按名字排序，输出必须有序
		},
		{
			name: "no features enabled",
			config: &types.NodeConfiguration{
				FeatureGates: newMockFeatureGate(map[string]bool{
					string("feature-a"): false,
					string("feature-b"): false,
				}),
			},
			expected: []string{},
		},
		{
			name: "feature past max version",
			config: &types.NodeConfiguration{
				FeatureGates: newMockFeatureGate(map[string]bool{string("feature-a"): true}),
				Version:      featureMaxVersion.AddMinor(1),
			},
			expected: []string{}, // 版本超出上限，特性不应被对外声明
		},
		{
			name: "feature past max version - pre-release version",
			config: &types.NodeConfiguration{
				FeatureGates: newMockFeatureGate(map[string]bool{string("feature-a"): true}),
				Version:      version.MustParse("1.39.0-alpha.2.39+049eafd34dfbd2"),
			},
			expected: []string{}, // 版本超出上限，特性不应被对外声明
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			features := framework.DiscoverNodeFeatures(tc.config)
			if !slices.Equal(tc.expected, features) {
				t.Errorf("expected %#v, got %#v", tc.expected, features)
			}
		})
	}
}

// 调度推断要同时考虑 Pod 内容、目标版本以及 nil 版本的参数错误。
func TestInferForPodScheduling(t *testing.T) {
	inferPodlevelResources := func(p *types.PodInfo) bool {
		return p.Spec.Resources != nil
	}
	podWithPodLevelResources := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-with-podlevelresources"},
		Spec: v1.PodSpec{
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("500m"),
				},
			},
		},
	}
	podWithoutPodLevelResources := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-without-podlevelresources"},
		Spec:       v1.PodSpec{},
	}

	// 提供一个带占位 gate 的默认依赖，避免每个用例重复构造。
	defaultMockRequirements := func() *FeatureRequirements {
		return &FeatureRequirements{
			EnabledFeatureGates: []string{"pod-level-resources"},
		}
	}

	testCases := []struct {
		name          string
		registry      []types.Feature
		newPod        *v1.Pod
		targetVersion *version.Version
		expectedReqs  []string
		expectErr     bool
		errContains   string
	}{
		{
			name: "pod with feature, inferred during scheduling",
			registry: []types.Feature{
				&mockFeature{
					name:               "PodLevelResources",
					inferForScheduling: inferPodlevelResources,
					requirements:       defaultMockRequirements,
				},
			},
			newPod:        podWithPodLevelResources,
			targetVersion: version.MustParse("1.30.0"),
			expectedReqs:  []string{"PodLevelResources"},
			expectErr:     false,
		},
		{
			name: "pod without feature",
			registry: []types.Feature{
				&mockFeature{
					name:               "PodLevelResources",
					inferForScheduling: inferPodlevelResources,
					requirements:       defaultMockRequirements,
				},
			},
			newPod:        podWithoutPodLevelResources,
			targetVersion: version.MustParse("1.30.0"),
			expectedReqs:  nil,
			expectErr:     false,
		},
		{
			name: "feature universally available, not inferred during create",
			registry: []types.Feature{
				&mockFeature{
					name:               "PodLevelResources",
					inferForScheduling: inferPodlevelResources,
					maxVersion:         version.MustParse("1.30.0"),
					requirements:       defaultMockRequirements,
				},
			},
			newPod:        podWithPodLevelResources,
			targetVersion: version.MustParse("1.31.0"),
			expectedReqs:  nil,
			expectErr:     false,
		},
		{
			name: "pre-release target version",
			registry: []types.Feature{
				&mockFeature{
					name:               "PodLevelResources",
					inferForScheduling: inferPodlevelResources,
					maxVersion:         version.MustParse("1.30.0"),
					requirements:       defaultMockRequirements,
				},
			},
			newPod:        podWithPodLevelResources,
			targetVersion: version.MustParse("0.0.0-alpha.2.39+049eafd34dfbd2"),
			expectedReqs:  []string{"PodLevelResources"},
			expectErr:     false,
		},
		{
			name: "exceeds max version",
			registry: []types.Feature{
				&mockFeature{
					name:               "PodLevelResources",
					inferForScheduling: inferPodlevelResources,
					maxVersion:         version.MustParse("1.30.0"),
				},
			},
			newPod:        podWithPodLevelResources,
			targetVersion: version.MustParse("1.40.0"),
			expectedReqs:  nil,
			expectErr:     false,
		},
		{
			name: "target version nil",
			registry: []types.Feature{
				&mockFeature{
					name:               "PodLevelResources",
					inferForScheduling: inferPodlevelResources,
					requirements:       defaultMockRequirements,
				},
			},
			newPod:        podWithPodLevelResources,
			targetVersion: nil,
			expectedReqs:  nil,
			expectErr:     true,
			errContains:   "target version cannot be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			framework := New(tc.registry)
			reqs, err := framework.InferForPodScheduling(&types.PodInfo{Spec: &tc.newPod.Spec, Status: &tc.newPod.Status}, tc.targetVersion)

			if tc.expectErr {
				if err == nil {
					t.Errorf("expected error, got none")
				} else if !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("expected %q to contain %q", err.Error(), tc.errContains)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				unmappedReqs, err := framework.Unmap(reqs)
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				if !reflect.DeepEqual(tc.expectedReqs, unmappedReqs) {
					t.Errorf("expected %#v, got %#v", tc.expectedReqs, unmappedReqs)
				}
			}
		})
	}
}

// 更新推断以新旧 Pod 的差异为输入，覆盖变化、无变化及版本上限情形。
func TestInferForPodUpdate(t *testing.T) {
	inferResize := func(oldPodInfo, newPodInfo *types.PodInfo) bool {
		oldCPU := oldPodInfo.Spec.Containers[0].Resources.Requests.Cpu()
		newCPU := newPodInfo.Spec.Containers[0].Resources.Requests.Cpu()
		if oldCPU != nil && newCPU != nil && !oldCPU.Equal(*newCPU) {
			return true
		}
		return false
	}

	basePod := func(cpuRequest string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "c1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse(cpuRequest)},
						},
					},
				},
			},
		}
	}

	podWith1CPU := basePod("1")
	podWith2CPU := basePod("2")

	// 统一的默认依赖信息，仅用于让 mock 特性的 Requirements() 有返回值。
	defaultMockRequirements := func() *FeatureRequirements {
		return &FeatureRequirements{
			EnabledFeatureGates: []string{"inplace-pod-resize"},
		}
	}

	testCases := []struct {
		name          string
		registry      []types.Feature
		oldPod        *v1.Pod
		newPod        *v1.Pod
		targetVersion *version.Version
		expectedReqs  []string
		expectErr     bool
		errContains   string
	}{
		{
			name: "pod update requires feature",
			registry: []types.Feature{
				&mockFeature{
					name:           "InPlacePodResize",
					inferForUpdate: inferResize,
					requirements:   defaultMockRequirements,
				},
			},
			oldPod:        podWith1CPU,
			newPod:        podWith2CPU,
			targetVersion: version.MustParse("1.30.0"),
			expectedReqs:  []string{"InPlacePodResize"},
			expectErr:     false,
		},
		{
			name: "pod update requires feature with pre-release version",
			registry: []types.Feature{
				&mockFeature{
					name:           "InPlacePodResize",
					inferForUpdate: inferResize,
					requirements:   defaultMockRequirements,
				},
			},
			oldPod:        podWith1CPU,
			newPod:        podWith2CPU,
			targetVersion: version.MustParse("1.35.0-alpha.2.39+049eafd34dfbd2"),
			expectedReqs:  []string{"InPlacePodResize"},
			expectErr:     false,
		},
		{
			name: "pod update does not require feature",
			registry: []types.Feature{
				&mockFeature{
					name:           "InPlacePodResize",
					inferForUpdate: inferResize,
					requirements:   defaultMockRequirements,
				},
			},
			oldPod:        podWith1CPU,
			newPod:        podWith1CPU,
			targetVersion: version.MustParse("1.30.0"),
			expectedReqs:  nil,
			expectErr:     false,
		},
		{
			name: "feature universally available, not inferred during update",
			registry: []types.Feature{
				&mockFeature{
					name:           "InPlacePodResize",
					inferForUpdate: inferResize,
					maxVersion:     version.MustParse("1.30.0"),
					requirements:   defaultMockRequirements,
				},
			},
			oldPod:        podWith1CPU,
			newPod:        podWith2CPU,
			targetVersion: version.MustParse("1.31.0"),
			expectedReqs:  nil,
			expectErr:     false,
		},
		{
			name: "target version nil",
			registry: []types.Feature{
				&mockFeature{
					name:           "InPlacePodResize",
					inferForUpdate: inferResize,
					requirements:   defaultMockRequirements,
				},
			},
			oldPod:        podWith1CPU,
			newPod:        podWith2CPU,
			targetVersion: nil,
			expectedReqs:  nil,
			expectErr:     true,
			errContains:   "target version cannot be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			framework := New(tc.registry)
			reqs, err := framework.InferForPodUpdate(&types.PodInfo{Spec: &tc.oldPod.Spec, Status: &tc.oldPod.Status}, &types.PodInfo{Spec: &tc.newPod.Spec, Status: &tc.newPod.Status}, tc.targetVersion)

			if tc.expectErr {
				if err == nil {
					t.Errorf("expected error, got none")
				} else if !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("expected %q to contain %q", err.Error(), tc.errContains)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				unmappedReqs, err := framework.Unmap(reqs)
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				if !reflect.DeepEqual(tc.expectedReqs, unmappedReqs) {
					t.Errorf("expected %#v, got %#v", tc.expectedReqs, unmappedReqs)
				}
			}
		})
	}
}

// TestMatchNode 覆盖匹配的核心语义：要求全部满足/部分缺失/全部缺失/无要求，
// 并对同一组数据分别调用 MatchNode 与 MatchNodeFeatureSet，确保两个入口行为一致。
// 最后单独验证 node 为 nil 时返回错误。
func TestMatchNode(t *testing.T) {
	framework := newTestFramework("feature-a", "feature-b", "feature-c")
	testCases := []struct {
		name                   string
		podFeatureRequirements []string
		nodeFeatures           []string
		expectedMatch          bool
		expectedUnsatisfied    []string
	}{
		{
			name:                   "all features match",
			podFeatureRequirements: []string{"feature-a", "feature-b"},
			nodeFeatures:           []string{"feature-a", "feature-b", "feature-c"},
			expectedMatch:          true,
			expectedUnsatisfied:    nil,
		},
		{
			name:                   "some features missing",
			podFeatureRequirements: []string{"feature-a", "feature-b"},
			nodeFeatures:           []string{"feature-a", "feature-c"},
			expectedMatch:          false,
			expectedUnsatisfied:    []string{"feature-b"},
		},
		{
			name:                   "all features missing",
			podFeatureRequirements: []string{"feature-a", "feature-b"},
			nodeFeatures:           []string{"feature-c"},
			expectedMatch:          false,
			expectedUnsatisfied:    []string{"feature-a", "feature-b"},
		},
		{
			name:                   "no node features",
			podFeatureRequirements: []string{"feature-a", "feature-b"},
			nodeFeatures:           []string{},
			expectedMatch:          false,
			expectedUnsatisfied:    []string{"feature-a", "feature-b"},
		},
		{
			name:                   "no requirements",
			podFeatureRequirements: []string{},
			nodeFeatures:           []string{"feature-a", "feature-b", "feature-c"},
			expectedMatch:          true,
			expectedUnsatisfied:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 两个入口共享判定核心，用同一组数据分别跑一遍防止行为分叉。
			matchVariations := []string{"MatchNode", "MatchNodeFeatureSet"}
			for _, variationName := range matchVariations {
				t.Run(variationName, func(t *testing.T) {
					var result *MatchResult
					var err error

					switch variationName {
					case "MatchNode":
						node := &v1.Node{Status: v1.NodeStatus{DeclaredFeatures: tc.nodeFeatures}}
						result, err = framework.MatchNode(framework.MustMapSorted(tc.podFeatureRequirements), node)
					case "MatchNodeFeatureSet":
						result, err = framework.MatchNodeFeatureSet(framework.MustMapSorted(tc.podFeatureRequirements), framework.MustMapSorted(tc.nodeFeatures))
					default:
						t.Fatalf("unknown match variation: %s", variationName)
					}

					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if tc.expectedMatch != result.IsMatch {
						t.Fatalf("expected match=%v, got %v", tc.expectedMatch, result.IsMatch)
					}
					if !tc.expectedMatch {
						want := sets.NewString(tc.expectedUnsatisfied...)
						got := sets.NewString(result.UnsatisfiedRequirements...)
						if !want.Equal(got) {
							t.Fatalf("expected unsatisfied=%v, got=%v", want.List(), got.List())
						}
					}
				})
			}
		})
	}

	// 单独验证 nil 节点：必须先判空，不能解引用。
	_, err := framework.MatchNode(framework.MustMapSorted([]string{"feature-a"}), nil)
	if err == nil {
		t.Fatalf("MatchNode should return an error for a nil node")
	}
}
