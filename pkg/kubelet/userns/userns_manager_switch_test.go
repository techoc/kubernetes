//go:build !windows

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

package userns

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestMakeUserNsManagerSwitch 验证特性门控从开启切到关闭后重建管理器时，不会沿用磁盘上已记录的用户命名空间分配。
func TestMakeUserNsManagerSwitch(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 开启特性门控创建管理器，以便把 Pod 的用户命名空间分配记录到磁盘。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	pods := []types.UID{"pod-1", "pod-2"}

	testUserNsPodsManager := &testUserNsPodsManager{
		podDir: t.TempDir(),
		// 列出与稍后要记录的相同 Pod，这样第二次创建管理器时
		// 能从磁盘上找到带 userns 数据的这些 Pod。
		podList: pods,
	}
	m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	require.NoError(t, err)

	// 把 Pod 的用户命名空间范围写入磁盘。
	for _, podUID := range pods {
		pod := v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID}}
		_, err := m.GetOrCreateUserNamespaceMappings(logger, &pod, "")
		require.NoError(t, err, "failed to record userns range for pod %v", podUID)
	}

	// 验证门控关闭且磁盘上已有 Pod 记录时，重新初始化仍能正常工作。
	// 先把仿真版本设为 1.35，使该特性门控在测试中仍可被关闭。
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, false)
	m2, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	require.NoError(t, err)

	// 门控已关闭，不应有任何 Pod 被认为已分配。
	for _, pod := range pods {
		ok := m2.podAllocated(pod)
		assert.False(t, ok, "pod %q should not be allocated", pod)
	}
}

// TestGetOrCreateUserNamespaceMappingsSwitch 验证门控关闭后 GetOrCreateUserNamespaceMappings 变为空操作，返回 nil 映射。
func TestGetOrCreateUserNamespaceMappingsSwitch(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 开启特性门控，以便在磁盘上创建一些 Pod 记录。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	pods := []types.UID{"pod-1", "pod-2"}

	testUserNsPodsManager := &testUserNsPodsManager{
		podDir: t.TempDir(),
		// 列出与稍后要记录的相同 Pod，这样第二次创建管理器时
		// 能从磁盘上找到带 userns 数据的这些 Pod。
		podList: pods,
	}
	m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	require.NoError(t, err)

	// 把 Pod 的用户命名空间范围写入磁盘。
	for _, podUID := range pods {
		pod := v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID}}
		_, err := m.GetOrCreateUserNamespaceMappings(logger, &pod, "")
		require.NoError(t, err, "failed to record userns range for pod %v", podUID)
	}

	// 验证门控关闭且磁盘上已有 Pod 注册时，调用应为空操作。
	// 先把仿真版本设为 1.35，使该特性门控在测试中仍可被关闭。
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, false)
	// 在门控关闭的情况下重建管理器，并确认返回的 userns 映射为 nil。
	m2, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	require.NoError(t, err)

	for _, podUID := range pods {
		pod := v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID}}
		userns, err := m2.GetOrCreateUserNamespaceMappings(logger, &pod, "")

		assert.NoError(t, err, "failed to record userns range for pod %v", podUID)
		assert.Nil(t, userns, "userns range should be nil for pod %v", podUID)
	}
}

// TestCleanupOrphanedPodUsernsAllocationsSwitch 验证门控关闭后，清理孤儿 Pod 的 userns 分配仍可执行。
func TestCleanupOrphanedPodUsernsAllocationsSwitch(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	// 开启特性门控，以便在磁盘上创建一些 Pod 记录。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	// listPods 模拟磁盘上仍存在的 Pod，pods 只写入分配、不出现在列表中，用于触发孤儿清理。
	listPods := []types.UID{"pod-1", "pod-2"}
	pods := []types.UID{"pod-3", "pod-4"}
	testUserNsPodsManager := &testUserNsPodsManager{
		podDir:  t.TempDir(),
		podList: listPods,
	}

	m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	require.NoError(t, err)

	// 把 Pod 的用户命名空间范围写入磁盘。
	for _, podUID := range pods {
		pod := v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID}}
		_, err := m.GetOrCreateUserNamespaceMappings(logger, &pod, "")
		require.NoError(t, err, "failed to record userns range for pod %v", podUID)
	}

	// 验证门控关闭且已有 Pod 注册时，孤儿分配清理仍能正常工作。
	// 先把仿真版本设为 1.35，使该特性门控在测试中仍可被关闭。
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, false)
	err = m.CleanupOrphanedPodUsernsAllocations(ctx, nil, nil)
	require.NoError(t, err)

	// 门控已关闭，不应有任何 Pod 被认为已分配。
	for _, pod := range append(listPods, pods...) {
		ok := m.podAllocated(pod)
		assert.False(t, ok, "pod %q should not be allocated", pod)
	}
}
