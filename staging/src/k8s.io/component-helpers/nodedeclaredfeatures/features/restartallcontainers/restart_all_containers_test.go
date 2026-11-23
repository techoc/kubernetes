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

package restartallcontainers

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// TestRequirements 检查声明的门与实现使用的门一致：
// 这里的名字既是节点发现依据，也是外部文档化的契约，不能被静默改写。
func TestRequirements(t *testing.T) {
	feature := &restartAllContainersFeature{}
	reqs := feature.Requirements()
	if reqs == nil {
		t.Fatalf("Feature %s returned nil Requirements", feature.Name())
	}
	if reqs.EnabledFeatureGates == nil || len(reqs.EnabledFeatureGates) != 1 || reqs.EnabledFeatureGates[0] != RestartAllContainersOnContainerExits {
		t.Fatalf("Feature %s Requirements should declare exactly the %s feature gate", feature.Name(), RestartAllContainersOnContainerExits)
	}
}

// TestDiscover 覆盖开关两端：节点声明完全取决于特性门状态，
// 没有额外的运行时探测，因此这两个分支就是全部行为。
func TestDiscover(t *testing.T) {
	tests := []struct {
		name               string
		featureGateEnabled bool
		expected           bool
	}{
		{
			name:               "both feature enabled",
			featureGateEnabled: true,
			expected:           true,
		},
		{
			name:               "restartAllContainers feature disabled",
			featureGateEnabled: false,
			expected:           false,
		},
	}

	feature := &restartAllContainersFeature{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := &types.NodeConfiguration{
				FeatureGates: types.FeatureGateMap{RestartAllContainersOnContainerExits: tc.featureGateEnabled},
			}
			enabled := feature.Discover(config)
			if want, got := tc.expected, enabled; want != got {
				t.Fatalf("want=%v,got=%v", want, got)
			}
		})
	}
}

// TestInferForScheduling 验证推断只看是否配置了重启整 pod 的规则，
// 且对 init 容器和普通容器一视同仁：真正决定是否需要该特性的是规则本身，
// 不是容器种类。没有规则时不应误判。
func TestInferForScheduling(t *testing.T) {
	tests := []struct {
		name     string
		pod      *v1.PodSpec
		expected bool
	}{
		{
			name: "init container with rules",
			pod: &v1.PodSpec{
				InitContainers: []v1.Container{
					containerWithRestartAllContainersAction(),
				},
				Containers: []v1.Container{{
					Name:  "name",
					Image: "image",
				}},
			},
			expected: true,
		},
		{
			name: "regular container with rules",
			pod: &v1.PodSpec{
				InitContainers: []v1.Container{{
					Name:  "name",
					Image: "image",
				}},
				Containers: []v1.Container{containerWithRestartAllContainersAction()},
			},
			expected: true,
		},
		{
			name: "no rules",
			pod: &v1.PodSpec{
				InitContainers: []v1.Container{{
					Name:  "name",
					Image: "image",
				}},
				Containers: []v1.Container{{
					Name:  "name2",
					Image: "image",
				}},
			},
			expected: false,
		},
	}

	feature := &restartAllContainersFeature{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			podInfo := &types.PodInfo{Spec: tc.pod}
			if want, got := tc.expected, feature.InferForScheduling(podInfo); want != got {
				t.Fatalf("want=%v,got=%v", want, got)
			}
		})
	}
}

// TestInferForUpdate 断言更新路径恒为 false：
// 该需求在调度时就能从 PodSpec 判定，更新阶段再推断只会重复甚至冲突。
func TestInferForUpdate(t *testing.T) {
	feature := &restartAllContainersFeature{}
	podInfo := &types.PodInfo{Spec: &v1.PodSpec{}}
	if feature.InferForUpdate(nil, podInfo) {
		t.Fatalf("expect InferForUpdate to be false")
	}
}

// containerWithRestartAllContainersAction 构造带完整规则的最小容器：
// 既要有退出码匹配条件，也要有 RestartAllContainers 动作，
// 只设其中一个不足以让推断判定为真。
func containerWithRestartAllContainersAction() v1.Container {
	restartPolicy := v1.ContainerRestartPolicyNever
	return v1.Container{
		Name:          "container",
		Image:         "image",
		RestartPolicy: &restartPolicy,
		RestartPolicyRules: []v1.ContainerRestartRule{
			{
				Action: v1.ContainerRestartRuleActionRestartAllContainers,
				ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
					Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
					Values:   []int32{1},
				},
			},
		},
	}
}
