/*
Copyright 2020 The Kubernetes Authors.

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
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	testutil "k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state/testing"
	"k8s.io/utils/ptr"
)

// testingCheckpoint 是测试使用的 checkpoint 名称，避免与其他 checkpoint 冲突。
const testingCheckpoint = "dramanager_checkpoint_test"

// TODO (https://github.com/kubernetes/kubernetes/issues/123552): 重新评估 checkpoint 中
// 存储的数据以及这些数据是否确有必要。

// TestCheckpointGetOrCreate 验证 Checkpointer.GetOrCreate 对新建、已有、损坏以及
// 跨版本 checkpoint 的加载行为。
func TestCheckpointGetOrCreate(t *testing.T) {
	testCases := []struct {
		description                string
		checkpointContent          string
		expectedError              string
		expectedClaimInfoStateList ClaimInfoStateList
	}{
		{
			description:                "new-checkpoint",
			expectedClaimInfoStateList: nil,
		},
		{
			description:       "single-claim-info-state",
			checkpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"ShareID\":null,\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}}]}","Checksum":2570581864}`,
			expectedClaimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
		},
		{
			description:       "single-claim-info-state-with-share-id",
			checkpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"ShareID\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\",\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}}]}","Checksum":736302156}`,
			expectedClaimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									ShareID:      ptr.To(types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
		},
		{
			description:       "claim-info-state-with-multiple-devices",
			checkpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"ShareID\":null,\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]},{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-2\",\"ShareID\":null,\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}}]}","Checksum":1223221469}`,
			expectedClaimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-2",
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
		},
		{
			description:       "two-claim-info-states",
			checkpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example-1\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"ShareID\":null,\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}},{\"ClaimUID\":\"4cf8db2d-06c0-7d70-1a51-e59b25b2c16c\",\"ClaimName\":\"example-2\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-2\",\"ShareID\":null,\"RequestNames\":null,\"CDIDeviceIDs\":null}]}}}]}","Checksum":2820117767}`,
			expectedClaimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example-1",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:   "worker-1",
									DeviceName: "dev-2",
								},
							},
						},
					},
					ClaimUID:  "4cf8db2d-06c0-7d70-1a51-e59b25b2c16c",
					ClaimName: "example-2",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
		},
		{
			description:       "incorrect-checksum",
			checkpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"Entries\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example-1\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}},{\"ClaimUID\":\"4cf8db2d-06c0-7d70-1a51-e59b25b2c16c\",\"ClaimName\":\"example-2\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-2\",\"RequestNames\":null,\"CDIDeviceIDs\":null}]}}}]}","Checksum":2930258365}`,
			expectedError:     "checkpoint is corrupted",
		},
		{
			description:       "invalid-JSON",
			checkpointContent: `{`,
			expectedError:     "unexpected end of JSON input",
		},
		{
			// 模拟升级场景：假设 `Device.RequestNames` 是新字段。
			// checkpoint 内容中不含该字段，但这不应妨碍 checkpoint 被正常加载。
			description:       "upgraded-structure",
			checkpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}}]}","Checksum":1137250765}`,
			expectedClaimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									RequestNames: nil,
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
		},
		{
			// 模拟升级场景：假设 `Device.RequestNames` 是新字段，且 API 版本从 v0 变为 v1。
			// 这些变化不应妨碍 checkpoint 被正常加载。
			description:       "upgraded-structure-and-version",
			checkpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v0\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}}]}","Checksum":1284376732}`,
			expectedClaimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									RequestNames: nil,
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
		},
	}

	// 创建临时目录存放 checkpoint 文件。
	testingDir, err := os.MkdirTemp("", "dramanager_state_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(testingDir); err != nil {
			t.Fatal(err)
		}
	}()

	// 创建测试用的 checkpoint 管理器。
	cpm, err := checkpointmanager.NewCheckpointManager(testingDir)
	require.NoError(t, err, "could not create testing checkpoint manager")

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 先清除上一个用例遗留的 checkpoint，保证每个用例从干净状态开始。
			require.NoError(t, cpm.RemoveCheckpoint(testingCheckpoint), "could not remove testing checkpoint")

			// 若用例提供了 checkpoint 内容，则写入一个模拟 checkpoint。
			if strings.TrimSpace(tc.checkpointContent) != "" {
				mock := &testutil.MockCheckpoint{Content: tc.checkpointContent}
				require.NoError(t, cpm.CreateCheckpoint(testingCheckpoint, mock), "could not create testing checkpoint")
			}

			// 基于测试目录构造 Checkpointer，后续验证其读取行为。
			checkpointer, err := NewCheckpointer(testingDir, testingCheckpoint)
			require.NoError(t, err, "could not create testing checkpointer")

			// 期望出错时校验错误信息，否则比对解析出的 ClaimInfoStateList。
			checkpoint, err := checkpointer.GetOrCreate()
			if strings.TrimSpace(tc.expectedError) != "" {
				assert.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err, "unexpected error")
				stateList, err := checkpoint.GetClaimInfoStateList()
				require.NoError(t, err, "could not get data entries from checkpoint")
				require.NoError(t, err)
				assert.Equal(t, tc.expectedClaimInfoStateList, stateList)
			}
		})
	}
}

// TestCheckpointStateStore 验证 Checkpointer.Store 写出的 checkpoint 内容
// （含校验和）与预期一致，并覆盖非法构造参数的错误路径。
func TestCheckpointStateStore(t *testing.T) {
	testCases := []struct {
		description               string
		claimInfoStateList        ClaimInfoStateList
		expectedCheckpointContent string
	}{
		{
			description: "single-claim-info-state",
			claimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
			expectedCheckpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"ShareID\":null,\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}}]}","Checksum":2570581864}`,
		},
		{
			description: "single-claim-info-state-with-share-id",
			claimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									ShareID:      ptr.To(types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
			expectedCheckpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"ShareID\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\",\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}}]}","Checksum":736302156}`,
		},
		{
			description: "claim-info-state-with-multiple-devices",
			claimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-2",
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
			expectedCheckpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"ShareID\":null,\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]},{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-2\",\"ShareID\":null,\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}}]}","Checksum":1223221469}`,
		},
		{
			description: "two-claim-info-states",
			claimInfoStateList: ClaimInfoStateList{
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:     "worker-1",
									DeviceName:   "dev-1",
									RequestNames: []string{"test request"},
									CDIDeviceIDs: []string{"example.com/example=cdi-example"},
								},
							},
						},
					},
					ClaimUID:  "067798be-454e-4be4-9047-1aa06aea63f7",
					ClaimName: "example-1",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
				{
					DriverState: map[string]DriverState{
						"test-driver.cdi.k8s.io": {
							Devices: []Device{
								{
									PoolName:   "worker-1",
									DeviceName: "dev-2",
								},
							},
						},
					},
					ClaimUID:  "4cf8db2d-06c0-7d70-1a51-e59b25b2c16c",
					ClaimName: "example-2",
					Namespace: "default",
					PodUIDs:   sets.New("139cdb46-f989-4f17-9561-ca10cfb509a6"),
				},
			},
			expectedCheckpointContent: `{"Data":"{\"kind\":\"DRACheckpoint\",\"apiVersion\":\"checkpoint.dra.kubelet.k8s.io/v1\",\"ClaimInfoStateList\":[{\"ClaimUID\":\"067798be-454e-4be4-9047-1aa06aea63f7\",\"ClaimName\":\"example-1\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-1\",\"ShareID\":null,\"RequestNames\":[\"test request\"],\"CDIDeviceIDs\":[\"example.com/example=cdi-example\"]}]}}},{\"ClaimUID\":\"4cf8db2d-06c0-7d70-1a51-e59b25b2c16c\",\"ClaimName\":\"example-2\",\"Namespace\":\"default\",\"PodUIDs\":{\"139cdb46-f989-4f17-9561-ca10cfb509a6\":{}},\"DriverState\":{\"test-driver.cdi.k8s.io\":{\"Devices\":[{\"PoolName\":\"worker-1\",\"DeviceName\":\"dev-2\",\"ShareID\":null,\"RequestNames\":null,\"CDIDeviceIDs\":null}]}}}]}","Checksum":2820117767}`,
		},
	}

	// 状态目录不能为空字符串，构造时应返回错误。
	if _, err := NewCheckpointer("", testingCheckpoint); err == nil {
		t.Fatal("expected error but got nil")
	}

	// 创建临时目录存放 checkpoint 文件。
	testingDir, err := os.MkdirTemp("", "dramanager_state_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(testingDir); err != nil {
			t.Fatal(err)
		}
	}()

	// checkpointName 为空时构造 Checkpointer 应返回错误。
	if _, err = NewCheckpointer(testingDir, ""); err == nil {
		t.Fatal("expected error but got nil")
	}

	// 清理可能存在的旧 checkpoint，确保测试从干净状态开始。
	cpm, err := checkpointmanager.NewCheckpointManager(testingDir)
	require.NoError(t, err, "could not create testing checkpoint manager")
	require.NoError(t, cpm.RemoveCheckpoint(testingCheckpoint), "could not remove testing checkpoint")

	cs, err := NewCheckpointer(testingDir, testingCheckpoint)
	require.NoError(t, err, "could not create testing checkpointState instance")

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 先由状态列表构造 checkpoint，再写入并从磁盘读回比对内容。
			checkpoint, err := NewCheckpoint(tc.claimInfoStateList)
			require.NoError(t, err, "could not create Checkpoint")

			err = cs.Store(checkpoint)
			require.NoError(t, err, "could not store checkpoint")

			err = cpm.GetCheckpoint(testingCheckpoint, checkpoint)
			require.NoError(t, err, "could not get checkpoint")

			checkpointContent, err := checkpoint.MarshalCheckpoint()
			require.NoError(t, err, "could not Marshal Checkpoint")
			assert.Equal(t, tc.expectedCheckpointContent, string(checkpointContent))
		})
	}
}
