//go:build !windows

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

package userns

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// 验证特性门控关闭时所有公开方法的行为。

// TestMakeUserNsManagerDisabled 验证门控关闭时仍可成功创建管理器。
func TestMakeUserNsManagerDisabled(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 先把仿真版本设为 1.35，使该特性门控在测试中仍可被关闭。
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, false)

	testUserNsPodsManager := &testUserNsPodsManager{}
	_, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	assert.NoError(t, err)
}

// TestReleaseDisabled 验证门控关闭时 Release 可以安全调用。
func TestReleaseDisabled(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, false)

	testUserNsPodsManager := &testUserNsPodsManager{}
	m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	require.NoError(t, err)

	m.Release(logger, "some-pod")
}

// TestGetOrCreateUserNamespaceMappingsDisabled 验证门控关闭时不会创建任何用户命名空间映射。
func TestGetOrCreateUserNamespaceMappingsDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, false)

	// 预置 true/false 指针，便于构造 HostUsers 的三种取值。
	trueVal := true
	falseVal := false

	tests := []struct {
		name    string
		pod     *v1.Pod
		success bool
	}{
		{
			name:    "pod is nil",
			pod:     nil,
			success: true,
		},
		{
			name: "hostUsers is nil",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: nil,
				},
			},
			success: true,
		},
		{
			name: "hostUsers is true",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &trueVal,
				},
			},
		},
		{
			name: "hostUsers is false",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &falseVal,
				},
			},
		},
	}

	// 覆盖 pod 为 nil 以及 HostUsers 为 nil/true/false 四种情况。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			testUserNsPodsManager := &testUserNsPodsManager{}
			m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
			require.NoError(t, err)

			userns, err := m.GetOrCreateUserNamespaceMappings(logger, test.pod, "")
			// 门控关闭时不应产生任何映射。
			assert.Nil(t, userns)
			if test.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// TestCleanupOrphanedPodUsernsAllocationsDisabled 验证门控关闭时孤儿分配清理可以安全执行。
func TestCleanupOrphanedPodUsernsAllocationsDisabled(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, false)

	testUserNsPodsManager := &testUserNsPodsManager{}
	m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	require.NoError(t, err)

	err = m.CleanupOrphanedPodUsernsAllocations(ctx, nil, nil)
	assert.NoError(t, err)
}
