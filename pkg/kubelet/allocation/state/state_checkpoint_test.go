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

package state

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
)

const testCheckpoint = "pod_status_manager_state"

// newTestStateCheckpoint 在临时目录上构造带空缓存的 stateCheckpoint。
func newTestStateCheckpoint(t *testing.T) *stateCheckpoint {
	logger, _ := ktesting.NewTestContext(t)
	testingDir := getTestDir(t)
	cache := NewStateMemory(logger, PodResourceInfoMap{})
	checkpointManager, err := checkpointmanager.NewCheckpointManager(testingDir)
	require.NoError(t, err, "failed to create checkpoint manager")
	checkpointName := "pod_state_checkpoint"
	sc := &stateCheckpoint{
		cache:             cache,
		checkpointManager: checkpointManager,
		checkpointName:    checkpointName,
	}
	return sc
}

// getTestDir 创建测试临时目录，并在用例结束时自动清理。
func getTestDir(t *testing.T) string {
	testingDir, err := os.MkdirTemp("", "pod_resource_allocation_state_test")
	require.NoError(t, err, "failed to create temp dir")
	t.Cleanup(func() {
		if err := os.RemoveAll(testingDir); err != nil {
			t.Fatal(err)
		}
	})
	return testingDir
}

// verifyPodResourceAllocation 逐 Pod、逐容器比较资源分配，
// 包括容器级、Pod 级和 emptyDir 卷限制。
func verifyPodResourceAllocation(t *testing.T, expected, actual *PodResourceInfoMap, msgAndArgs string) {
	for podUID, expectedPodInfo := range *expected {
		actualPodInfo, exists := (*actual)[podUID]
		require.True(t, exists, "actual state missing pod %s", podUID)

		// 校验容器级 requests/limits。
		require.Len(t, actualPodInfo.ContainerResources, len(expectedPodInfo.ContainerResources), msgAndArgs)
		for containerName, expectedCtrReq := range expectedPodInfo.ContainerResources {
			actualCtrReq, exists := actualPodInfo.ContainerResources[containerName]
			require.True(t, exists, "actual container %s missing", containerName)
			for name, expectedQty := range expectedCtrReq.Requests {
				require.True(t, expectedQty.Equal(actualCtrReq.Requests[name]), msgAndArgs)
			}
			for name, expectedQty := range expectedCtrReq.Limits {
				require.True(t, expectedQty.Equal(actualCtrReq.Limits[name]), msgAndArgs)
			}
		}

		// 校验 Pod 级资源；期望为 nil 时实际也必须为 nil。
		if expectedPodInfo.PodLevelResources == nil {
			require.Nil(t, actualPodInfo.PodLevelResources, msgAndArgs)
		} else {
			require.NotNil(t, actualPodInfo.PodLevelResources, msgAndArgs)
			for name, expectedQty := range expectedPodInfo.PodLevelResources.Requests {
				require.True(t, expectedQty.Equal(actualPodInfo.PodLevelResources.Requests[name]), msgAndArgs)
			}
			for name, expectedQty := range expectedPodInfo.PodLevelResources.Limits {
				require.True(t, expectedQty.Equal(actualPodInfo.PodLevelResources.Limits[name]), msgAndArgs)
			}
		}

		// 校验 emptyDir 卷限制。
		if expectedPodInfo.EmptyDirVolumeLimits == nil {
			require.Nil(t, actualPodInfo.EmptyDirVolumeLimits, msgAndArgs)
		} else {
			require.NotNil(t, actualPodInfo.EmptyDirVolumeLimits, msgAndArgs)
			require.Len(t, actualPodInfo.EmptyDirVolumeLimits, len(expectedPodInfo.EmptyDirVolumeLimits), msgAndArgs)
			for volName, expectedQty := range expectedPodInfo.EmptyDirVolumeLimits {
				actualQty, exists := actualPodInfo.EmptyDirVolumeLimits[volName]
				require.True(t, exists, "actual emptyDir volume %s missing", volName)
				require.True(t, expectedQty.Equal(*actualQty), msgAndArgs)
			}
		}
	}
}

// Test_stateCheckpoint_storeState 覆盖各种资源数量写法下的写入与恢复：
// 写入后新实例应能恢复，内容未变化时不应重写 checkpoint。
func Test_stateCheckpoint_storeState(t *testing.T) {
	type args struct {
		resInfoMap PodResourceInfoMap
	}
	type testCase struct {
		name string
		args args
	}

	var tests []testCase
	suffix := []string{"Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "n", "u", "m", "k", "M", "G", "T", "P", "E", ""}
	factor := []string{"1", "0.1", "0.03", "10", "100", "512", "1000", "1024", "700", "10000"}
	for _, fact := range factor {
		for _, suf := range suffix {
			if (suf == "E" || suf == "Ei") && (fact == "1000" || fact == "10000") {
				// 1000/10000 与 E/Ei 组合会溢出，跳过这些组合，参见
				// https://github.com/kubernetes/apimachinery/blob/95b78024e3feada7739b40426690b4f287933fd8/pkg/api/resource/quantity.go#L301
				continue
			}
			qStr := fmt.Sprintf("%s%s", fact, suf)

			// 用例 1：容器级、Pod 级和卷限制全部填充。
			tests = append(tests, testCase{
				name: fmt.Sprintf("resource - %s - all fields populated", qStr),
				args: args{
					resInfoMap: PodResourceInfoMap{
						"pod1": {
							ContainerResources: map[string]v1.ResourceRequirements{
								"container1": {
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse(qStr),
										v1.ResourceMemory: resource.MustParse(qStr),
									},
								},
							},
							PodLevelResources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse(qStr),
									v1.ResourceMemory: resource.MustParse(qStr),
								},
							},
							EmptyDirVolumeLimits: map[string]*resource.Quantity{
								"volume1": func() *resource.Quantity {
									q := resource.MustParse(qStr)
									return &q
								}(),
							},
						},
					},
				},
			})

			// 用例 2：只有容器级资源，Pod 级和卷限制为 nil。
			tests = append(tests, testCase{
				name: fmt.Sprintf("resource - %s - only container", qStr),
				args: args{
					resInfoMap: PodResourceInfoMap{
						"pod1": {
							ContainerResources: map[string]v1.ResourceRequirements{
								"container1": {
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse(qStr),
										v1.ResourceMemory: resource.MustParse(qStr),
									},
								},
							},
							PodLevelResources:    nil,
							EmptyDirVolumeLimits: nil,
						},
					},
				},
			})

			// 用例 3：容器级资源与卷限制，Pod 级资源为 nil。
			tests = append(tests, testCase{
				name: fmt.Sprintf("resource - %s - container and volume limits", qStr),
				args: args{
					resInfoMap: PodResourceInfoMap{
						"pod1": {
							ContainerResources: map[string]v1.ResourceRequirements{
								"container1": {
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse(qStr),
										v1.ResourceMemory: resource.MustParse(qStr),
									},
								},
							},
							PodLevelResources: nil,
							EmptyDirVolumeLimits: map[string]*resource.Quantity{
								"volume1": func() *resource.Quantity {
									q := resource.MustParse(qStr)
									return &q
								}(),
							},
						},
					},
				},
			})

			// 用例 4：容器级与 Pod 级资源，卷限制为 nil。
			tests = append(tests, testCase{
				name: fmt.Sprintf("resource - %s - container and pod-level", qStr),
				args: args{
					resInfoMap: PodResourceInfoMap{
						"pod1": {
							ContainerResources: map[string]v1.ResourceRequirements{
								"container1": {
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse(qStr),
										v1.ResourceMemory: resource.MustParse(qStr),
									},
								},
							},
							PodLevelResources: &v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse(qStr),
									v1.ResourceMemory: resource.MustParse(qStr),
								},
							},
							EmptyDirVolumeLimits: nil,
						},
					},
				},
			})
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			testDir := getTestDir(t)
			// 先写入原始状态，再从同一目录新建实例验证恢复。
			originalSC, err := NewStateCheckpoint(logger, testDir, testCheckpoint)
			require.NoError(t, err)

			for podUID, alloc := range tt.args.resInfoMap {
				err = originalSC.SetPodResourceInfo(logger, podUID, alloc)
				require.NoError(t, err)
			}

			actual := originalSC.GetPodResourceInfoMap()
			verifyPodResourceAllocation(t, &tt.args.resInfoMap, &actual, "stored pod resource allocation is not equal to original pod resource allocation")

			// 新实例应能从 checkpoint 恢复出与原始状态一致的数据。
			newSC, err := NewStateCheckpoint(logger, testDir, testCheckpoint)
			require.NoError(t, err)

			actual = newSC.GetPodResourceInfoMap()
			verifyPodResourceAllocation(t, &tt.args.resInfoMap, &actual, "restored pod resource allocation is not equal to original pod resource allocation")

			checkpointPath := filepath.Join(testDir, testCheckpoint)
			require.FileExists(t, checkpointPath)
			require.NoError(t, os.Remove(checkpointPath)) // 删除 checkpoint 文件，用于检测是否被重写。

			// 写入相同的分配值不应重写 checkpoint，避免无谓的磁盘 IO。
			for podUID, alloc := range tt.args.resInfoMap {
				require.NoError(t, originalSC.SetPodResourceInfo(logger, podUID, alloc))
				require.NoFileExists(t, checkpointPath, "checkpoint should not be re-written")
			}

			// 写入新值时应更新 checkpoint。
			require.NoError(t, originalSC.SetPodResourceInfo(logger, "foo-bar", PodResourceInfo{
				ContainerResources: map[string]v1.ResourceRequirements{
					"container1": {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")}},
				},
				PodLevelResources: &v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")}},
				EmptyDirVolumeLimits: map[string]*resource.Quantity{
					"volume1": resource.NewQuantity(1, resource.BinarySI),
				},
			}))
			require.FileExists(t, checkpointPath, "checkpoint should be re-written")
		})
	}
}

// Test_stateCheckpoint_formatUpgraded 验证旧格式 checkpoint 在新增字段后仍能加载，
// 缺失字段按零值处理。
func Test_stateCheckpoint_formatUpgraded(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 新增字段更可能加在 PodResourceAllocationInfo 上而不是 AllocationEntries 上；
	// 这里把 ResizeStatusEntries 当作新字段来模拟升级：
	// 旧 checkpoint 不含该字段，但不应影响加载。
	sc := newTestStateCheckpoint(t)

	// 准备不含 ResizeStatusEntries 的旧 checkpoint。
	const checkpointContent = `{"data":"{\"entries\":{\"pod1\":{\"ContainerResources\":{\"container1\":{\"requests\":{\"cpu\":\"1Ki\",\"memory\":\"1Ki\"}}}}}}","checksum":1178570812}`
	expectedPodResourceAllocation := PodResourceInfoMap{
		"pod1": {
			ContainerResources: map[string]v1.ResourceRequirements{
				"container1": {
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("1Ki"),
						v1.ResourceMemory: resource.MustParse("1Ki"),
					},
				},
			},
		},
	}
	// 从固定 JSON 内容反序列化旧格式 checkpoint。
	checkpoint := &Checkpoint{}
	err := checkpoint.UnmarshalCheckpoint([]byte(checkpointContent))
	require.NoError(t, err, "failed to unmarshal checkpoint")

	err = sc.checkpointManager.CreateCheckpoint(sc.checkpointName, checkpoint)
	require.NoError(t, err, "failed to create old checkpoint")

	// restoreState 应忽略缺失的新字段，只恢复已有数据。
	actualPodResourceAllocation, _, err := restoreState(logger, sc.checkpointManager, sc.checkpointName)
	require.NoError(t, err, "failed to restore state")

	require.Equal(t, expectedPodResourceAllocation, actualPodResourceAllocation, "pod resource allocation info is not equal")

	sc.cache = NewStateMemory(logger, actualPodResourceAllocation)

	actualPodResourceAllocation = sc.cache.GetPodResourceInfoMap()

	require.Equal(t, expectedPodResourceAllocation, actualPodResourceAllocation, "pod resource allocation info is not equal")
}
