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
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// 测试用的用户命名空间映射参数。
const (
	testUserNsLength = uint32(65536)
	// 跳过第一个块，避免与宿主上已占用的 UID 区间重叠。
	minimumMappingUID = testUserNsLength
	// 预留足够空间，可分配 2000 个用户命名空间。
	mappingLen  = testUserNsLength * 2000
	testMaxPods = 110
)

// testUserNsPodsManager 是 UserNsPodsManager 的测试实现，
// 用可控的字段模拟 pod 目录、磁盘 pod 列表、运行时支持情况与映射配置。
type testUserNsPodsManager struct {
	podDir         string
	podList        []types.UID
	userns         bool
	maxPods        int
	mappingFirstID uint32
	mappingLen     uint32
	userNsLength   uint32
}

// GetPodDir 返回 pod 目录；未配置时返回一个不会真正使用的占位路径。
func (m *testUserNsPodsManager) GetPodDir(podUID types.UID) string {
	if m.podDir == "" {
		return "/tmp/non-existent-dir.This-is-not-used-in-tests"
	}
	return m.podDir
}

// ListPodsFromDisk 返回模拟的磁盘 pod 列表；未配置时返回空列表。
func (m *testUserNsPodsManager) ListPodsFromDisk() ([]types.UID, error) {
	if len(m.podList) == 0 {
		return nil, nil
	}
	return m.podList, nil
}

// HandlerSupportsUserNamespaces 模拟运行时是否支持用户命名空间；
// runtimeHandler 为 "error" 时返回错误，用于覆盖错误分支。
func (m *testUserNsPodsManager) HandlerSupportsUserNamespaces(runtimeHandler string) (bool, error) {
	if runtimeHandler == "error" {
		return false, errors.New("unknown runtime")
	}
	return m.userns, nil
}

// GetKubeletMappings 返回 kubelet 可用的映射起始 ID 与长度；未配置时使用默认值。
func (m *testUserNsPodsManager) GetKubeletMappings(logger klog.Logger, idsPerPod uint32) (uint32, uint32, error) {
	if m.mappingFirstID != 0 {
		return m.mappingFirstID, m.mappingLen, nil
	}
	return minimumMappingUID, mappingLen, nil
}

// GetMaxPods 返回最大 pod 数；未配置时返回测试默认值。
func (m *testUserNsPodsManager) GetMaxPods() int {
	if m.maxPods != 0 {
		return m.maxPods
	}

	return testMaxPods
}

// TestUserNsManagerAllocate 验证 allocateOne 的分配行为：
// 分配区间互不重叠且长度正确，重复记录具有幂等性，重复分配与释放后区间可复用。
func TestUserNsManagerAllocate(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 启用用户命名空间特性门控，否则被测代码路径不会生效。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	customUserNsLength := uint32(1048576)

	cases := []struct {
		name           string
		userNsLength   uint32
		mappingFirstID uint32
		mappingLen     uint32
	}{
		{
			name:           "default",
			userNsLength:   testUserNsLength,
			mappingFirstID: minimumMappingUID,
			mappingLen:     mappingLen,
		},
		{
			name:           "custom",
			userNsLength:   customUserNsLength,
			mappingFirstID: customUserNsLength,
			mappingLen:     customUserNsLength * 2000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testUserNsPodsManager := &testUserNsPodsManager{
				userNsLength:   tc.userNsLength,
				mappingFirstID: tc.mappingFirstID,
				mappingLen:     tc.mappingLen,
			}
			idsPerPod := int64(tc.userNsLength)
			m, err := MakeUserNsManager(logger, testUserNsPodsManager, &idsPerPod)
			require.NoError(t, err)

			allocated, length, err := m.allocateOne(logger, "one")
			require.NoError(t, err)
			assert.Equal(t, tc.userNsLength, length, "m.isSet(%d).length=%v", allocated, length)
			assert.True(t, m.isSet(allocated), "m.isSet(%d)", allocated)

			// 用同一 manager 连续分配两次，验证两次分配互不重叠且长度一致。
			allocated2, length2, err := m.allocateOne(logger, "two")
			require.NoError(t, err)
			assert.NotEqual(t, allocated, allocated2, "allocated != allocated2")
			assert.Equal(t, length, length2, "length == length2")

			// 重复记录同一 pod 应幂等成功；分配范围一旦不同就必须报错，避免记录被静默覆盖。
			err = m.record(logger, "two", allocated2, length2)
			require.NoError(t, err)
			// 分配范围不同时应失败。
			err = m.record(logger, "two", allocated2+1, length2)
			require.Error(t, err)

			m.Release(logger, "one")
			m.Release(logger, "two")
			assert.False(t, m.isSet(allocated), "m.isSet(%d)", allocated)
			assert.False(t, m.isSet(allocated2), "m.nsSet(%d)", allocated2)

			var allocs []uint32
			// 批量分配大量范围，验证分配始终落在映射区间内。
			for i := 0; i < 1000; i++ {
				allocated, length, err = m.allocateOne(logger, types.UID(fmt.Sprintf("%d", i)))
				assert.Equal(t, tc.userNsLength, length, "length is not the expected. iter: %v", i)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, allocated, tc.mappingFirstID)
				// userns 区间最后一个 ID（allocated+userNsLength）不得越过映射上界。
				assert.LessOrEqual(t, allocated, tc.mappingFirstID+tc.mappingLen-tc.userNsLength)
				allocs = append(allocs, allocated)
			}
			for i, v := range allocs {
				assert.True(t, m.isSet(v), "m.isSet(%d) should be true", v)
				m.Release(logger, types.UID(fmt.Sprintf("%d", i)))
				assert.False(t, m.isSet(v), "m.isSet(%d) should be false", v)

				err = m.record(logger, types.UID(fmt.Sprintf("%d", i)), v, tc.userNsLength)
				require.NoError(t, err)
				m.Release(logger, types.UID(fmt.Sprintf("%d", i)))
				assert.False(t, m.isSet(v), "m.isSet(%d) should be false", v)
			}
		})
	}
}

// TestMakeUserNsManager 验证 MakeUserNsManager 在映射参数非法时拒绝构造：
// 起始 ID 或长度非整块、起始 ID 过小、区间容不下 maxPods、区间越界等都应报错。
func TestMakeUserNsManager(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	cases := []struct {
		name           string
		mappingFirstID uint32
		mappingLen     uint32
		maxPods        int
		success        bool
	}{
		{
			name:    "default",
			success: true,
		},
		{
			name:           "firstID not multiple",
			mappingFirstID: 65536 + 1,
		},
		{
			name:           "firstID is less than 65535",
			mappingFirstID: 1,
		},
		{
			name:           "mappingLen not multiple",
			mappingFirstID: 65536,
			mappingLen:     65536 + 1,
		},
		{
			name:           "range can't fit maxPods",
			mappingFirstID: 65536,
			mappingLen:     65536,
			maxPods:        2,
		},
		{
			name:           "range ends at INVALID_UID",
			mappingFirstID: 65536,
			mappingLen:     (1 << 32) - 65536,
		},
		{
			name:           "range ends past INVALID_UID",
			mappingFirstID: 327680,
			mappingLen:     4294836224,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testUserNsPodsManager := &testUserNsPodsManager{
				podDir:         t.TempDir(),
				mappingFirstID: tc.mappingFirstID,
				mappingLen:     tc.mappingLen,
				maxPods:        tc.maxPods,
			}
			_, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)

			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// TestUserNsManagerParseUserNsFile 验证 parseUserNsFileAndRecord 对用户命名空间文件的校验：
// 要求 UID/GID 映射各只有一条、长度为整块、容器内根 ID 为 0，且 UID 与 GID 起始 ID 一致。
func TestUserNsManagerParseUserNsFile(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	cases := []struct {
		name    string
		file    string
		success bool
	}{
		{
			name: "basic",
			file: `{
	                        "uidMappings":[ { "hostId":131072, "containerId":0, "length":65536 } ],
	                        "gidMappings":[ { "hostId":131072, "containerId":0, "length":65536 } ]
                               }`,
			success: true,
		},
		{
			name: "invalid length",
			file: `{
	                        "uidMappings":[ { "hostId":131072, "containerId":0, "length":0 } ],
	                        "gidMappings":[ { "hostId":131072, "containerId":0, "length":0 } ]
                               }`,
			success: false,
		},
		{
			name: "wrong offset",
			file: `{
	                        "uidMappings":[ {"hostId":131072, "containerId":0, "length":65536 } ],
	                        "gidMappings":[ {"hostId":1, "containerId":0, "length":65536 } ]
                               }`,
			success: false,
		},
		{
			name: "two GID mappings",
			file: `{
	                        "uidMappings":[ { "hostId":131072, "containerId":0, "length":userNsLength } ],
	                        "gidMappings":[ { "hostId":131072, "containerId":0, "length":userNsLength }, { "hostId":196608, "containerId":0, "length":65536 } ]
                               }`,
			success: false,
		},
		{
			name: "two UID mappings",
			file: `{
	                        "uidMappings":[ { "hostId":131072, "containerId":0, "length":65536 }, { "hostId":196608, "containerId":0, "length":65536 } ],
	                        "gidMappings":[ { "hostId":131072, "containerId":0, "length":65536 } ]
                               }`,
			success: false,
		},
		{
			name: "no root UID",
			file: `{
	                        "uidMappings":[ { "hostId":131072, "containerId":1, "length":65536 } ],
	                        "gidMappings":[ { "hostId":131072, "containerId":0, "length":65536 } ]
                               }`,
			success: false,
		},
		{
			name: "no root GID",
			file: `{
	                        "uidMappings":[ { "hostId":131072, "containerId":0, "length":65536 } ],
	                        "gidMappings":[ { "hostId":131072, "containerId":1, "length":65536 } ]
                               }`,
			success: false,
		},
	}

	testUserNsPodsManager := &testUserNsPodsManager{}
	m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	assert.NoError(t, err)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 不校验解析后的具体值：内容由 JSON 解析器保证，此处只关心校验逻辑是否按预期报错。
			_, err = m.parseUserNsFileAndRecord(logger, types.UID(tc.name), []byte(tc.file))
			if (tc.success && err == nil) || (!tc.success && err != nil) {
				return
			}

			t.Errorf("expected success: %v but got error: %v", tc.success, err)
		})
	}
}

// TestGetOrCreateUserNamespaceMappings 验证 GetOrCreateUserNamespaceMappings 的决策逻辑：
// 未开启、显式 opt-in 或运行时无支持时使用 NODE 模式，只有支持用户命名空间时使用 POD 模式。
func TestGetOrCreateUserNamespaceMappings(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	trueVal := true
	falseVal := false

	cases := []struct {
		name           string
		pod            *v1.Pod
		expMode        runtimeapi.NamespaceMode
		runtimeUserns  bool
		runtimeHandler string
		success        bool
	}{
		{
			name:    "no user namespace",
			pod:     &v1.Pod{},
			expMode: runtimeapi.NamespaceMode_NODE,
			success: true,
		},
		{
			name:    "nil pod",
			pod:     nil,
			expMode: runtimeapi.NamespaceMode_NODE,
			success: true,
		},
		{
			name: "opt-in to host user namespace",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &trueVal,
				},
			},
			expMode: runtimeapi.NamespaceMode_NODE,
			success: true,
		},
		{
			name: "user namespace",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &falseVal,
				},
			},
			expMode:       runtimeapi.NamespaceMode_POD,
			runtimeUserns: true,
			success:       true,
		},
		{
			name: "user namespace, but no runtime support",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &falseVal,
				},
			},
			runtimeUserns: false,
		},
		{
			name: "user namespace, but runtime returns error",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &falseVal,
				},
			},
			// 该 handler 名称会让 fake runtime 返回错误，用于覆盖错误分支。
			runtimeHandler: "error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			// 这些测试会创建 userns 文件，故使用临时目录作为真实存在的 podDir，避免污染工作区。
			testUserNsPodsManager := &testUserNsPodsManager{
				podDir: t.TempDir(),
				userns: tc.runtimeUserns,
			}
			m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
			assert.NoError(t, err)

			userns, err := m.GetOrCreateUserNamespaceMappings(logger, tc.pod, tc.runtimeHandler)
			if (tc.success && err != nil) || (!tc.success && err == nil) {
				t.Errorf("expected success: %v but got error: %v", tc.success, err)
			}

			if userns.GetMode() != tc.expMode {
				t.Errorf("expected mode: %v but got: %v", tc.expMode, userns.GetMode())
			}
		})
	}
}

// TestCleanupOrphanedPodUsernsAllocations 验证清理逻辑只保留仍然存在或正在运行的 pod 的分配，
// 其余已记录的分配都会被释放。
func TestCleanupOrphanedPodUsernsAllocations(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	cases := []struct {
		name                 string
		runningPods          []*kubecontainer.Pod
		pods                 []*v1.Pod
		listPods             []types.UID /* pods to list */
		podSetBeforeCleanup  []types.UID /* pods to record before cleanup */
		podSetAfterCleanup   []types.UID /* pods set expected after cleanup */
		podUnsetAfterCleanup []types.UID /* pods set expected after cleanup */
	}{
		{
			name:     "no stale pods",
			listPods: []types.UID{"pod-1", "pod-2"},
		},
		{
			name:                 "no stale pods set",
			podSetBeforeCleanup:  []types.UID{"pod-1", "pod-2"},
			listPods:             []types.UID{"pod-1", "pod-2"},
			podUnsetAfterCleanup: []types.UID{"pod-1", "pod-2"},
		},
		{
			name:                 "one running pod",
			listPods:             []types.UID{"pod-1", "pod-2"},
			podSetBeforeCleanup:  []types.UID{"pod-1", "pod-2"},
			runningPods:          []*kubecontainer.Pod{{ID: "pod-1"}},
			podSetAfterCleanup:   []types.UID{"pod-1"},
			podUnsetAfterCleanup: []types.UID{"pod-2"},
		},
		{
			name:                 "pod set before cleanup but not listed ==> unset",
			podSetBeforeCleanup:  []types.UID{"pod-1", "pod-2"},
			runningPods:          []*kubecontainer.Pod{{ID: "pod-1"}},
			podUnsetAfterCleanup: []types.UID{"pod-1", "pod-2"},
		},
		{
			name:                 "one pod",
			listPods:             []types.UID{"pod-1", "pod-2"},
			podSetBeforeCleanup:  []types.UID{"pod-1", "pod-2"},
			pods:                 []*v1.Pod{{ObjectMeta: metav1.ObjectMeta{UID: "pod-1"}}},
			podSetAfterCleanup:   []types.UID{"pod-1"},
			podUnsetAfterCleanup: []types.UID{"pod-2"},
		},
		{
			name:                 "no listed pods ==> all unset",
			podSetBeforeCleanup:  []types.UID{"pod-1", "pod-2"},
			podUnsetAfterCleanup: []types.UID{"pod-1", "pod-2"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testUserNsPodsManager := &testUserNsPodsManager{
				podDir:  t.TempDir(),
				podList: tc.listPods,
			}
			m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
			require.NoError(t, err)

			// 先把若干 pod 记录为已分配，制造清理前的状态，才能观察清理效果。
			for i, pod := range tc.podSetBeforeCleanup {
				err := m.record(logger, pod, uint32((i+1)*65536), 65536)
				require.NoError(t, err)
			}

			err = m.CleanupOrphanedPodUsernsAllocations(ctx, tc.pods, tc.runningPods)
			require.NoError(t, err)

			for _, pod := range tc.podSetAfterCleanup {
				ok := m.podAllocated(pod)
				assert.True(t, ok, "pod %q should be allocated", pod)
			}

			for _, pod := range tc.podUnsetAfterCleanup {
				ok := m.podAllocated(pod)
				assert.False(t, ok, "pod %q should not be allocated", pod)
			}
		})
	}
}

// failingUserNsPodsManager 在 testUserNsPodsManager 基础上让 ListPodsFromDisk 始终失败，
// 用于覆盖构造 manager 时读取磁盘 pod 出错的路径。
type failingUserNsPodsManager struct {
	testUserNsPodsManager
}

// ListPodsFromDisk 固定返回权限错误，模拟无法读取磁盘 pod 列表。
func (m *failingUserNsPodsManager) ListPodsFromDisk() ([]types.UID, error) {
	return nil, os.ErrPermission
}

// TestMakeUserNsManagerFailsListPod 验证从磁盘读取 pod 列表失败时，构造 manager 会返回错误。
func TestMakeUserNsManagerFailsListPod(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	testUserNsPodsManager := &failingUserNsPodsManager{}
	_, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "read pods from disk")
}

// TestRecordBounds 验证 record 的边界检查：仅允许一个 pod 时，越界的分配会被拒绝。
func TestRecordBounds(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, true)

	// 只允许分配 1 个 pod 的空间。
	testUserNsPodsManager := &testUserNsPodsManager{
		mappingFirstID: 65536,
		mappingLen:     65536,
		maxPods:        1,
	}
	m, err := MakeUserNsManager(logger, testUserNsPodsManager, nil)
	require.NoError(t, err)

	// 先占满唯一可用的映射空间，用于验证越界记录会被拒绝。
	// 第一个 pod 的分配应当成功。
	err = m.record(logger, types.UID(fmt.Sprintf("%d", 0)), 65536, 65536)
	require.NoError(t, err)

	// 空间已耗尽，下一次分配必须失败。
	err = m.record(logger, types.UID(fmt.Sprintf("%d", 2)), uint32(2*65536), 65536)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "out of range")
}
