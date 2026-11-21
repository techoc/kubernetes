/*
Copyright 2018 The Kubernetes Authors.

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

// Package state 实现 CPU Manager 的状态管理，包括 checkpoint 的序列化、版本迁移与校验。
package state

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	testutil "k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/cpuset"
)

// testingCheckpoint 是测试用 checkpoint 文件名。
const testingCheckpoint = "cpumanager_checkpoint_test"

// FeatureGateCombination 表示一组特性门控的开关组合，用于参数化测试。
type FeatureGateCombination map[featuregate.Feature]bool

// allFeatureGateCombinations 生成给定特性门控的所有开关组合，每个组合是特性名到启用状态的映射。
//
// 构造方式：从只含空组合的列表开始，每加入一个特性就把现有组合分别以禁用和启用两份追加，
// 因此组合数量按 2 的幂增长。示例（A、B 两个特性）：
//
//   - 初始：`[{}]`
//   - 加入 A：`[{A: false}, {A: true}]`
//   - 加入 B：`[{A: false, B: false}, {A: false, B: true}, {A: true, B: false}, {A: true, B: true}]`
func allFeatureGateCombinations(gates []featuregate.Feature) []FeatureGateCombination {
	combinations := []FeatureGateCombination{make(FeatureGateCombination)}
	for _, gate := range gates {
		var newCombinations []FeatureGateCombination
		for _, combination := range combinations {
			// 必须复制后再改，否则会污染已加入列表的组合。
			disabled := maps.Clone(combination)
			disabled[gate] = false
			newCombinations = append(newCombinations, disabled)

			// 启用分支可以复用原对象，因为它不会再被修改。
			combination[gate] = true
			newCombinations = append(newCombinations, combination)
		}
		combinations = newCombinations
	}
	return combinations
}

// describe 生成可读的组合名称，用作子测试名。
func describe(comb FeatureGateCombination) string {
	keys := slices.Sorted(maps.Keys(comb))
	// 空组合没有特性可描述，返回空字符串作为子测试名。
	if len(keys) == 0 {
		return ""
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s=%v", keys[0], comb[keys[0]])
	for _, key := range keys[1:] {
		fmt.Fprintf(&sb, ",%s=%v", key, comb[key])
	}
	return sb.String()
}

// TestCheckpointStateRestore 验证各种 checkpoint 内容（含历史版本）能被正确恢复或按预期报错。
func TestCheckpointStateRestore(t *testing.T) {
	testCases := []struct {
		description       string
		fgRequirements    FeatureGateCombination
		checkpointContent string
		policyName        string
		initialContainers containermap.ContainerMap
		expectedError     string
		expectedState     *stateMemory
	}{
		{
			"Restore non-existing checkpoint",
			nil,
			"",
			"none",
			containermap.ContainerMap{},
			"",
			&stateMemory{},
		},
		{
			"Fail to restore checkpoint without data section",
			nil,
			`{
				"policyName": "static",
				"defaultCPUSet": "7-9",
				"entries": {
					"pod": {
						"container1": "4-6",
						"container2": "1-3"
					}
				},
				"checksum": 1942532442,
				"dataChecksum": 1234
			}`,
			"none",
			containermap.ContainerMap{},
			"could not load v4 checkpoint: failed to deserialize cpu manager checkpoint data: unexpected end of JSON input",
			nil,
		},
		{
			"Fail to restore checkpoint without dataChecksum section",
			nil,
			`{
				"policyName": "static",
				"defaultCPUSet": "7-9",
				"entries": {
					"pod": {
						"container1": "4-6",
						"container2": "1-3"
					}
				},
				"checksum": 1942532442,
				"data": "{\"policyName\":\"static\",\"defaultCPUSet\":\"7-9\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"1-3\"}}}"
			}`,
			"none",
			containermap.ContainerMap{},
			"could not load v4 checkpoint: checkpoint is corrupted",
			nil,
		},
		// 下面的用例故意损坏 V2 部分，以验证恢复时只使用 data 段。
		{
			"Restore default CPU set",
			nil,
			`{
				"policyName": "other",
				"defaultCPUSet": "1-9",
				"checksum": 1234,
				"data": "{\"policyName\":\"none\",\"defaultCPUSet\":\"4-6\"}",
				"dataChecksum": 657950972
			}`,
			"none",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				defaultCPUSet: cpuset.New(4, 5, 6),
			},
		},
		// 下面的用例故意损坏 V2 部分，以验证恢复时只使用 data 段。
		{
			"Restore valid checkpoint with InPlacePodVerticalScalingExclusiveCPUs disabled",
			FeatureGateCombination{features.InPlacePodVerticalScalingExclusiveCPUs: false},
			`{
				"policyName": "other",
				"defaultCPUSet": "1-9",
				"checksum": 1234,
				"data": "{\"policyName\":\"static\",\"defaultCPUSet\":\"7-9\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"1-3\"}}}",
				"dataChecksum": 1420829534
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod": map[string]cpuset.CPUSet{
						"container1": cpuset.New(4, 5, 6),
						"container2": cpuset.New(1, 2, 3),
					},
				},
				defaultCPUSet: cpuset.New(7, 8, 9),
			},
		},
		// 下面的用例故意损坏 V2 部分，以验证恢复时只使用 data 段。
		{
			"Restore valid checkpoint with InPlacePodVerticalScalingExclusiveCPUs enabled",
			FeatureGateCombination{features.InPlacePodVerticalScalingExclusiveCPUs: true},
			`{
				"policyName": "other",
				"defaultCPUSet": "1-9",
				"checksum": 1234,
				"data": "{\"policyName\":\"static\",\"defaultCPUSet\":\"7-9\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"1-3\"}}}",
				"dataChecksum": 1420829534
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod": map[string]cpuset.CPUSet{
						"container1": cpuset.New(4, 5, 6),
						"container2": cpuset.New(1, 2, 3),
					},
				},
				defaultCPUSet: cpuset.New(7, 8, 9),
				baselines: ContainerCPUBaselines{
					"pod": {
						"container1": {
							Baseline: cpuset.New(4, 5, 6),
						},
						"container2": {
							Baseline: cpuset.New(1, 2, 3),
						},
					},
				},
			},
		},
		{
			"Fail to restore checkpoint with invalid checksum",
			nil,
			`{
				"policyName": "static",
				"defaultCPUSet": "7-9",
				"entries": {
					"pod": {
						"container1": "4-6",
						"container2": "1-3"
					}
				},
				"checksum": 1942532442,
				"data": "{\"policyName\":\"static\",\"defaultCPUSet\":\"7-9\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"1-3\"}}}",
				"dataChecksum": 1234
			}`,
			"static",
			containermap.ContainerMap{},
			"checkpoint is corrupted",
			nil,
		},
		{
			"Fail to restore checkpoint with invalid JSON",
			nil,
			`{`,
			"none",
			containermap.ContainerMap{},
			"unexpected end of JSON input",
			nil,
		},
		// 下面的用例故意让 V2 的 policyName 与 data 段不一致，以验证以 data 段为准。
		{
			"Fail to restore checkpoint with invalid policy name",
			nil,
			`{
				"policyName": "static",
				"defaultCPUSet": "7-9",
				"entries": {
					"pod": {
						"container1": "4-6",
						"container2": "1-3"
					}
				},
				"checksum": 1942532442,
				"data": "{\"policyName\":\"other\",\"defaultCPUSet\":\"7-9\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"1-3\"}}}",
				"dataChecksum": 1432271708
			}`,
			"none",
			containermap.ContainerMap{},
			`configured policy "none" differs from state checkpoint policy "other"`,
			nil,
		},
		// 下面的用例故意让 V2 的 defaultCPUSet 与 data 段不一致，以验证以 data 段为准。
		{
			"Fail to restore checkpoint with unparsable default CPU set",
			nil,
			`{
				"policyName": "none",
				"defaultCPUSet": "7-9",
				"checksum": 98002435,
				"data": "{\"policyName\":\"none\",\"defaultCPUSet\":\"1.3\"}",
				"dataChecksum": 3033143655
			}`,
			"none",
			containermap.ContainerMap{},
			`could not parse default CPU set "1.3": strconv.Atoi: parsing "1.3": invalid syntax`,
			nil,
		},
		// 下面的用例故意让 V2 的 entries 与 data 段不一致，以验证以 data 段为准。
		{
			"Fail to restore checkpoint with unparsable assignment entry",
			nil,
			`{
				"policyName": "static",
				"defaultCPUSet": "1-3",
				"entries": {
					"pod": {
						"container1": "4-6",
						"container2": "7-9"
					}
				},
				"checksum": 1659543133,
				"data": "{\"policyName\":\"static\",\"defaultCPUSet\":\"1-3\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"asd\"}}}",
				"dataChecksum": 3794806925
			}`,
			"static",
			containermap.ContainerMap{},
			`could not parse cpuset "asd" for container "container2" in pod "pod": strconv.Atoi: parsing "asd": invalid syntax`,
			nil,
		},
		// 下面的用例故意损坏 V2 部分，以验证恢复时只使用 data 段。
		{
			"Restore checkpoint ignoring unknown fields in data section",
			nil,
			`{
				"policyName": "other",
				"defaultCPUSet": "1-9",
				"checksum": 1234,
				"data": "{\"policyName\":\"none\",\"defaultCPUSet\":\"4-6\",\"unknownField\":\"value\"}",
				"dataChecksum": 3492408555
			}`,
			"none",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				defaultCPUSet: cpuset.New(4, 5, 6),
			},
		},
		{
			"Fail to restore checkpoint from checkpoint with v1 checksum",
			nil,
			`{
				"policyName": "none",
				"defaultCPUSet": "1-3",
				"checksum": 1694838852
			}`,
			"none",
			containermap.ContainerMap{},
			"checkpoint is corrupted",
			nil,
		},
		{
			"Fail to restore checkpoint from v1 (migration)",
			nil,
			`{
				"policyName": "static",
				"defaultCPUSet": "7-9",
				"entries": {
					"containerID1": "4-6",
					"containerID2": "1-3"
				},
				"checksum": 2026311253
			}`,
			"static",
			containermap.ContainerMap{},
			"cannot unmarshal string into Go struct field CPUManagerCheckpointV2.entries",
			nil,
		},
		{
			"Restore checkpoint from v2 (migration) with InPlacePodVerticalScalingExclusiveCPUs disabled",
			FeatureGateCombination{features.InPlacePodVerticalScalingExclusiveCPUs: false},
			`{
				"policyName": "static",
				"defaultCPUSet": "7-9",
				"entries": {
					"pod": {
						"container1": "4-6",
						"container2": "1-3"
					}
				},
				"checksum": 1942532442
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod": map[string]cpuset.CPUSet{
						"container1": cpuset.New(4, 5, 6),
						"container2": cpuset.New(1, 2, 3),
					},
				},
				defaultCPUSet: cpuset.New(7, 8, 9),
			},
		},
		{
			"Restore checkpoint from v2 (migration) with InPlacePodVerticalScalingExclusiveCPUs enabled",
			FeatureGateCombination{features.InPlacePodVerticalScalingExclusiveCPUs: true},
			`{
				"policyName": "static",
				"defaultCPUSet": "7-9",
				"entries": {
					"pod": {
						"container1": "4-6",
						"container2": "1-3"
					}
				},
				"checksum": 1942532442
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod": map[string]cpuset.CPUSet{
						"container1": cpuset.New(4, 5, 6),
						"container2": cpuset.New(1, 2, 3),
					},
				},
				defaultCPUSet: cpuset.New(7, 8, 9),
				baselines: ContainerCPUBaselines{
					"pod": {
						"container1": {
							Baseline: cpuset.New(4, 5, 6),
						},
						"container2": {
							Baseline: cpuset.New(1, 2, 3),
						},
					},
				},
			},
		},
		{
			"Restore checkpoint from v3 (migration) with PodLevelResourceManagers disabled and InPlacePodVerticalScalingExclusiveCPUs disabled",
			FeatureGateCombination{
				features.PodLevelResourceManagers:               false,
				features.InPlacePodVerticalScalingExclusiveCPUs: false,
			},
			`{
				"policyName": "static",
				"defaultCPUSet": "1-2",
				"entries": {
					"pod1": {
						"container1": "5-6",
						"container2": "3-4"
					}
				},
				"podEntries": {
					"pod2": {
						"cpuSet":"7-9"
					}
				},
				"checksum": 766259872
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod1": map[string]cpuset.CPUSet{
						"container1": cpuset.New(5, 6),
						"container2": cpuset.New(3, 4),
					},
				},
				defaultCPUSet: cpuset.New(1, 2),
			},
		},
		{
			"Restore checkpoint from v3 (migration) with PodLevelResourceManagers disabled and InPlacePodVerticalScalingExclusiveCPUs enabled",
			FeatureGateCombination{
				features.PodLevelResourceManagers:               false,
				features.InPlacePodVerticalScalingExclusiveCPUs: true,
			},
			`{
				"policyName": "static",
				"defaultCPUSet": "1-2",
				"entries": {
					"pod1": {
						"container1": "5-6",
						"container2": "3-4"
					}
				},
				"podEntries": {
					"pod2": {
						"cpuSet":"7-9"
					}
				},
				"checksum": 766259872
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod1": map[string]cpuset.CPUSet{
						"container1": cpuset.New(5, 6),
						"container2": cpuset.New(3, 4),
					},
				},
				defaultCPUSet: cpuset.New(1, 2),
				baselines: ContainerCPUBaselines{
					"pod1": {
						"container1": {
							Baseline: cpuset.New(5, 6),
						},
						"container2": {
							Baseline: cpuset.New(3, 4),
						},
					},
				},
			},
		},
		{
			"Restore checkpoint from v3 (migration) with PodLevelResourceManagers enabled and InPlacePodVerticalScalingExclusiveCPUs disabled",
			FeatureGateCombination{
				features.PodLevelResourceManagers:               true,
				features.InPlacePodVerticalScalingExclusiveCPUs: false,
			},
			`{
				"policyName": "static",
				"defaultCPUSet": "1-2",
				"entries": {
					"pod1": {
						"container1": "5-6",
						"container2": "3-4"
					}
				},
				"podEntries": {
					"pod2": {
						"cpuSet":"7-9"
					}
				},
				"checksum": 766259872
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod1": map[string]cpuset.CPUSet{
						"container1": cpuset.New(5, 6),
						"container2": cpuset.New(3, 4),
					},
				},
				defaultCPUSet: cpuset.New(1, 2),
				podAssignments: PodCPUAssignments{
					"pod2": PodEntry{
						CPUSet: cpuset.New(7, 8, 9),
					},
				},
			},
		},
		{
			"Restore checkpoint from v3 (migration) with PodLevelResourceManagers enabled and InPlacePodVerticalScalingExclusiveCPUs enabled",
			FeatureGateCombination{
				features.PodLevelResourceManagers:               true,
				features.InPlacePodVerticalScalingExclusiveCPUs: true,
			},
			`{
				"policyName": "static",
				"defaultCPUSet": "1-2",
				"entries": {
					"pod1": {
						"container1": "5-6",
						"container2": "3-4"
					}
				},
				"podEntries": {
					"pod2": {
						"cpuSet":"7-9"
					}
				},
				"checksum": 766259872
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod1": map[string]cpuset.CPUSet{
						"container1": cpuset.New(5, 6),
						"container2": cpuset.New(3, 4),
					},
				},
				defaultCPUSet: cpuset.New(1, 2),
				podAssignments: PodCPUAssignments{
					"pod2": PodEntry{
						CPUSet: cpuset.New(7, 8, 9),
					},
				},
				baselines: ContainerCPUBaselines{
					"pod1": {
						"container1": {
							Baseline: cpuset.New(5, 6),
						},
						"container2": {
							Baseline: cpuset.New(3, 4),
						},
					},
				},
			},
		},
		// 下面的用例故意损坏 V2 部分，以验证恢复时只使用 data 段。
		{
			"Restore valid v4 checkpoint with PodLevelResourceManagers disabled and InPlacePodVerticalScalingExclusiveCPUs disabled",
			FeatureGateCombination{
				features.PodLevelResourceManagers:               false,
				features.InPlacePodVerticalScalingExclusiveCPUs: false,
			},
			`{
				"policyName": "other",
				"defaultCPUSet": "1-9",
				"checksum": 1234,
				"data": "{\"policyName\":\"static\",\"defaultCPUSet\":\"1-3\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"7-9\"}},\"podEntries\":{\"pod\":{\"cpuSet\":\"4-10\"}},\"baselines\":{\"pod\":{\"container1\":{\"baseline\":\"4-5\"},\"container2\":{\"baseline\":\"7-8\"}}}}",
				"dataChecksum": 1032795074
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod": map[string]cpuset.CPUSet{
						"container1": cpuset.New(4, 5, 6),
						"container2": cpuset.New(7, 8, 9),
					},
				},
				defaultCPUSet: cpuset.New(1, 2, 3),
			},
		},
		// 下面的用例故意损坏 V2 部分，以验证恢复时只使用 data 段。
		{
			"Restore valid v4 checkpoint with PodLevelResourceManagers disabled and InPlacePodVerticalScalingExclusiveCPUs enabled",
			FeatureGateCombination{
				features.PodLevelResourceManagers:               false,
				features.InPlacePodVerticalScalingExclusiveCPUs: true,
			},
			`{
				"policyName": "other",
				"defaultCPUSet": "1-9",
				"checksum": 1234,
				"data": "{\"policyName\":\"static\",\"defaultCPUSet\":\"1-3\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"7-9\"}},\"podEntries\":{\"pod\":{\"cpuSet\":\"4-10\"}},\"baselines\":{\"pod\":{\"container1\":{\"baseline\":\"4-5\"},\"container2\":{\"baseline\":\"7-8\"}}}}",
				"dataChecksum": 1032795074
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod": map[string]cpuset.CPUSet{
						"container1": cpuset.New(4, 5, 6),
						"container2": cpuset.New(7, 8, 9),
					},
				},
				defaultCPUSet: cpuset.New(1, 2, 3),
				baselines: ContainerCPUBaselines{
					"pod": {
						"container1": {
							Baseline: cpuset.New(4, 5),
						},
						"container2": {
							Baseline: cpuset.New(7, 8),
						},
					},
				},
			},
		},
		// 下面的用例故意损坏 V2 部分，以验证恢复时只使用 data 段。
		{
			"Restore valid v4 checkpoint with PodLevelResourceManagers enabled and InPlacePodVerticalScalingExclusiveCPUs disabled",
			FeatureGateCombination{
				features.PodLevelResourceManagers:               true,
				features.InPlacePodVerticalScalingExclusiveCPUs: false,
			},
			`{
				"policyName": "other",
				"defaultCPUSet": "1-9",
				"checksum": 1234,
				"data": "{\"policyName\":\"static\",\"defaultCPUSet\":\"1-3\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"7-9\"}},\"podEntries\":{\"pod\":{\"cpuSet\":\"4-10\"}},\"baselines\":{\"pod\":{\"container1\":{\"baseline\":\"4-5\"},\"container2\":{\"baseline\":\"7-8\"}}}}",
				"dataChecksum": 1032795074
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod": map[string]cpuset.CPUSet{
						"container1": cpuset.New(4, 5, 6),
						"container2": cpuset.New(7, 8, 9),
					},
				},
				defaultCPUSet: cpuset.New(1, 2, 3),
				podAssignments: PodCPUAssignments{
					"pod": PodEntry{
						CPUSet: cpuset.New(4, 5, 6, 7, 8, 9, 10),
					},
				},
			},
		},
		// 下面的用例故意损坏 V2 部分，以验证恢复时只使用 data 段。
		{
			"Restore valid v4 checkpoint with PodLevelResourceManagers enabled and InPlacePodVerticalScalingExclusiveCPUs enabled",
			FeatureGateCombination{
				features.PodLevelResourceManagers:               true,
				features.InPlacePodVerticalScalingExclusiveCPUs: true,
			},
			`{
				"policyName": "other",
				"defaultCPUSet": "1-9",
				"checksum": 1234,
				"data": "{\"policyName\":\"static\",\"defaultCPUSet\":\"1-3\",\"entries\":{\"pod\":{\"container1\":\"4-6\",\"container2\":\"7-9\"}},\"podEntries\":{\"pod\":{\"cpuSet\":\"4-10\"}},\"baselines\":{\"pod\":{\"container1\":{\"baseline\":\"4-5\"},\"container2\":{\"baseline\":\"7-8\"}}}}",
				"dataChecksum": 1032795074
			}`,
			"static",
			containermap.ContainerMap{},
			"",
			&stateMemory{
				assignments: ContainerCPUAssignments{
					"pod": map[string]cpuset.CPUSet{
						"container1": cpuset.New(4, 5, 6),
						"container2": cpuset.New(7, 8, 9),
					},
				},
				defaultCPUSet: cpuset.New(1, 2, 3),
				podAssignments: PodCPUAssignments{
					"pod": PodEntry{
						CPUSet: cpuset.New(4, 5, 6, 7, 8, 9, 10),
					},
				},
				baselines: ContainerCPUBaselines{
					"pod": {
						"container1": {
							Baseline: cpuset.New(4, 5),
						},
						"container2": {
							Baseline: cpuset.New(7, 8),
						},
					},
				},
			},
		},
	}

	// 每个测试使用独立临时目录，避免 checkpoint 相互影响。
	testingDir, err := os.MkdirTemp("", "cpumanager_state_test")
	require.NoError(t, err)
	defer removeAll(testingDir, t)
	// checkpoint manager 负责把测试内容写入磁盘供 NewCheckpointState 读取。
	cpm, err := checkpointmanager.NewCheckpointManager(testingDir)
	require.NoErrorf(t, err, "could not create testing checkpoint manager: %v", err)

	// 本测试关注这两个特性门控对恢复行为的影响。
	featureGateList := []featuregate.Feature{
		features.PodLevelResourceManagers,
		features.InPlacePodVerticalScalingExclusiveCPUs,
	}
	// 遍历全部开关组合，确保迁移逻辑在各组合下都正确。
	for _, fgComb := range allFeatureGateCombinations(featureGateList) {
		// 在子测试中设置特性门控，测试结束后自动恢复。
		t.Run(describe(fgComb), func(t *testing.T) {
			for _, key := range slices.Sorted(maps.Keys(fgComb)) {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, key, fgComb[key])
			}

			for _, tc := range testCases {
				// 只运行特性门控要求与当前组合一致的用例。
				skip := false
				for fg, requiredState := range tc.fgRequirements {
					state, exist := fgComb[fg]
					if !exist || requiredState != state {
						skip = true
					}
				}
				if skip {
					continue
				}

				t.Run(tc.description, func(t *testing.T) {
					// 先删除旧 checkpoint，保证每个用例从干净状态开始。
					err = cpm.RemoveCheckpoint(testingCheckpoint)
					require.NoErrorf(t, err, "could not remove previous checkpoint: %v", err)

					// 内容为空表示不创建 checkpoint，用于测试首次启动场景。
					if strings.TrimSpace(tc.checkpointContent) != "" {
						checkpoint := &testutil.MockCheckpoint{Content: tc.checkpointContent}
						err = cpm.CreateCheckpoint(testingCheckpoint, checkpoint)
						require.NoErrorf(t, err, "could not create testing checkpoint: %v", err)
					}

					// 期望报错的用例还需确认错误被包装且包含具体原因。
					logger, _ := ktesting.NewTestContext(t)
					restoredState, err := NewCheckpointState(logger, testingDir, testingCheckpoint, tc.policyName, tc.initialContainers)
					if strings.TrimSpace(tc.expectedError) == "" {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
						require.ErrorContains(t, err, "could not restore state from checkpoint")
						require.ErrorContains(t, err, tc.expectedError)
						return
					}

					AssertStateEqual(t, restoredState, tc.expectedState)
				})
			}
		})
	}
}

// TestCheckpointStateStore 验证状态写入 checkpoint 后能被新实例读回。
func TestCheckpointStateStore(t *testing.T) {
	testCases := []struct {
		description    string
		fgRequirements FeatureGateCombination
		expectedState  *stateMemory
	}{
		{
			description:    "Store default cpu set",
			fgRequirements: nil,
			expectedState:  &stateMemory{defaultCPUSet: cpuset.New(1, 2, 3)},
		},
		{
			description:    "Store assignments",
			fgRequirements: FeatureGateCombination{features.InPlacePodVerticalScalingExclusiveCPUs: false},
			expectedState: &stateMemory{
				assignments: map[string]map[string]cpuset.CPUSet{
					"pod": {
						"container1": cpuset.New(1, 5, 8),
					},
				},
			},
		},
		{
			description:    "Store baselines",
			fgRequirements: FeatureGateCombination{features.InPlacePodVerticalScalingExclusiveCPUs: true},
			expectedState: &stateMemory{
				assignments: map[string]map[string]cpuset.CPUSet{
					"pod": {
						"container1": cpuset.New(1, 5, 8),
					},
				},
				baselines: ContainerCPUBaselines{
					"pod": {
						"container1": {
							Baseline: cpuset.New(1, 5),
						},
					},
				},
			},
		},
	}

	// 每个测试使用独立临时目录，避免 checkpoint 相互影响。
	testingDir, err := os.MkdirTemp("", "cpumanager_state_test")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(testingDir, t)

	cpm, err := checkpointmanager.NewCheckpointManager(testingDir)
	if err != nil {
		t.Fatalf("could not create testing checkpoint manager: %v", err)
	}

	// 本测试关注这两个特性门控对恢复行为的影响。
	featureGateList := []featuregate.Feature{
		features.PodLevelResourceManagers,
		features.InPlacePodVerticalScalingExclusiveCPUs,
	}
	// 遍历全部开关组合，确保迁移逻辑在各组合下都正确。
	for _, fgComb := range allFeatureGateCombinations(featureGateList) {
		// 在子测试中设置特性门控，测试结束后自动恢复。
		t.Run(describe(fgComb), func(t *testing.T) {
			for _, key := range slices.Sorted(maps.Keys(fgComb)) {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, key, fgComb[key])
			}

			for _, tc := range testCases {
				// 只运行特性门控要求与当前组合一致的用例。
				skip := false
				for fg, requiredState := range tc.fgRequirements {
					state, exist := fgComb[fg]
					if !exist || requiredState != state {
						skip = true
					}
				}
				if skip {
					continue
				}

				t.Run(tc.description, func(t *testing.T) {
					// 先删除旧 checkpoint，保证每个用例从干净状态开始。
					err = cpm.RemoveCheckpoint(testingCheckpoint)
					require.NoErrorf(t, err, "could not remove previous checkpoint: %v", err)

					logger, _ := ktesting.NewTestContext(t)
					cs1, err := NewCheckpointState(logger, testingDir, testingCheckpoint, "none", nil)
					if err != nil {
						t.Fatalf("could not create testing checkpointState instance: %v", err)
					}

					// 通过 cs1 写入数据，再新建 cs2 从磁盘恢复。
					cs1.SetDefaultCPUSet(tc.expectedState.defaultCPUSet)
					cs1.SetCPUAssignments(tc.expectedState.assignments)
					cs1.SetCPUBaselines(tc.expectedState.baselines)

					// 新建实例即触发 checkpoint 读取，从而验证往返一致性。
					cs2, err := NewCheckpointState(logger, testingDir, testingCheckpoint, "none", nil)
					if err != nil {
						t.Fatalf("could not create testing checkpointState instance: %v", err)
					}

					AssertStateEqual(t, cs2, tc.expectedState)
				})
			}
		})
	}
}

// TestCheckpointStateHelpers 验证 Set/Get/Delete 等状态辅助方法的一致性。
func TestCheckpointStateHelpers(t *testing.T) {
	testCases := []struct {
		description   string
		defaultCPUset cpuset.CPUSet
		assignments   map[string]map[string]cpuset.CPUSet
	}{
		{
			description:   "One container",
			defaultCPUset: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8),
			assignments: map[string]map[string]cpuset.CPUSet{
				"pod": {
					"c1": cpuset.New(0, 1),
				},
			},
		},
		{
			description:   "Two containers",
			defaultCPUset: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8),
			assignments: map[string]map[string]cpuset.CPUSet{
				"pod": {
					"c1": cpuset.New(0, 1),
					"c2": cpuset.New(2, 3, 4, 5),
				},
			},
		},
		{
			description:   "Container without assigned cpus",
			defaultCPUset: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8),
			assignments: map[string]map[string]cpuset.CPUSet{
				"pod": {
					"c1": cpuset.New(),
				},
			},
		},
	}

	// 每个测试使用独立临时目录，避免 checkpoint 相互影响。
	testingDir, err := os.MkdirTemp("", "cpumanager_state_test")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(testingDir, t)

	cpm, err := checkpointmanager.NewCheckpointManager(testingDir)
	if err != nil {
		t.Fatalf("could not create testing checkpoint manager: %v", err)
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 先删除旧 checkpoint，保证每个用例从干净状态开始。
			err = cpm.RemoveCheckpoint(testingCheckpoint)
			require.NoErrorf(t, err, "could not remove previous checkpoint: %v", err)

			logger, _ := ktesting.NewTestContext(t)
			state, err := NewCheckpointState(logger, testingDir, testingCheckpoint, "none", nil)
			if err != nil {
				t.Fatalf("could not create testing checkpointState instance: %v", err)
			}
			// 对每个容器执行写入、读取、删除三步验证。
			state.SetDefaultCPUSet(tc.defaultCPUset)

			for pod := range tc.assignments {
				for container, set := range tc.assignments[pod] {
					state.SetCPUSet(pod, container, set)
					// 写入后立即读回，确保内存状态与预期一致。
					if cpus, _ := state.GetCPUSet(pod, container); !cpus.Equals(set) {
						t.Fatalf("state inconsistent, got %q instead of %q", set, cpus)
					}

					// 删除后必须查不到，否则说明状态未真正清理。
					state.Delete(pod, container)
					if _, ok := state.GetCPUSet(pod, container); ok {
						t.Fatal("deleted container still existing in state")
					}
				}
			}
		})
	}
}

// TestCheckpointStateClear 验证 ClearState 会清空默认 CPU 集合与所有分配。
func TestCheckpointStateClear(t *testing.T) {
	testCases := []struct {
		description   string
		defaultCPUset cpuset.CPUSet
		assignments   map[string]map[string]cpuset.CPUSet
	}{
		{
			"Valid state",
			cpuset.New(1, 5, 10),
			map[string]map[string]cpuset.CPUSet{
				"pod": {
					"container1": cpuset.New(1, 4),
				},
			},
		},
	}

	// 向前兼容：降级后的 kubelet 必须能校验通过新版本写出的 checkpoint。
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 每个测试使用独立临时目录，避免 checkpoint 相互影响。
			testingDir, err := os.MkdirTemp("", "cpumanager_state_test")
			if err != nil {
				t.Fatal(err)
			}
			defer removeAll(testingDir, t)

			logger, _ := ktesting.NewTestContext(t)
			state, err := NewCheckpointState(logger, testingDir, testingCheckpoint, "none", nil)
			if err != nil {
				t.Fatalf("could not create testing checkpointState instance: %v", err)
			}

			state.SetDefaultCPUSet(tc.defaultCPUset)
			state.SetCPUAssignments(tc.assignments)

			// 清空后默认集合应为空，且所有容器分配都应消失。
			state.ClearState()
			if !cpuset.New().Equals(state.GetDefaultCPUSet()) {
				t.Fatal("cleared state with non-empty default cpu set")
			}
			for pod := range tc.assignments {
				for container := range tc.assignments[pod] {
					if _, ok := state.GetCPUSet(pod, container); ok {
						t.Fatalf("container %q in pod %q with non-default cpu set in cleared state", container, pod)
					}
				}
			}
		})
	}
}

// AssertStateEqual 逐字段比较两个 State 的默认集合、容器分配、Pod 分配和基线。
func AssertStateEqual(t *testing.T, sf State, sm State) {
	// 逐项比较而不是整体 DeepEqual，失败时能定位到具体字段。
	cpusetSf := sf.GetDefaultCPUSet()
	cpusetSm := sm.GetDefaultCPUSet()
	if !cpusetSf.Equals(cpusetSm) {
		t.Errorf("State CPUSet mismatch. Have %v, want %v", cpusetSf, cpusetSm)
	}

	cpuassignmentSf := sf.GetCPUAssignments()
	cpuassignmentSm := sm.GetCPUAssignments()
	if !reflect.DeepEqual(cpuassignmentSf, cpuassignmentSm) {
		t.Errorf("State CPU assignments mismatch. Have %s, want %s", cpuassignmentSf, cpuassignmentSm)
	}

	podcpuassignmentSf := sf.GetPodCPUAssignments()
	podcpuassignmentSm := sm.GetPodCPUAssignments()
	if !reflect.DeepEqual(podcpuassignmentSf, podcpuassignmentSm) {
		t.Errorf("State CPU assignments mismatch. Have %s, want %s", podcpuassignmentSf, podcpuassignmentSm)
	}

	cpubaselinesSf := sf.GetCPUBaselines()
	cpubaselinesSm := sm.GetCPUBaselines()
	if !reflect.DeepEqual(cpubaselinesSf, cpubaselinesSm) {
		t.Errorf("State CPU baseline entries mismatch. Have %s, want %s", cpubaselinesSf, cpubaselinesSm)
	}

}

// TestCPUManagerCheckpoint_MarshalCheckpoint_HashCompatibility 验证新版本写出的校验和与旧版本 kubelet 的计算方式一致，
func TestCPUManagerCheckpoint_MarshalCheckpoint_HashCompatibility(t *testing.T) {
	testCases := []struct {
		name                   string
		currentCheckpoint      any
		expectedLegacyChecksum func() checksum.Checksum
	}{
		{
			name: "V2 checkpoint",
			currentCheckpoint: &CPUManagerCheckpointV2{
				PolicyName:    "none",
				DefaultCPUSet: "1-3",
				Entries:       make(map[string]map[string]string),
			},
			expectedLegacyChecksum: func() checksum.Checksum {
				type CPUManagerCheckpoint struct {
					PolicyName    string                       `json:"policyName"`
					DefaultCPUSet string                       `json:"defaultCpuSet"`
					Entries       map[string]map[string]string `json:"entries,omitempty"`
					Checksum      checksum.Checksum            `json:"checksum"`
				}
				return checksum.New(&CPUManagerCheckpoint{
					PolicyName:    "none",
					DefaultCPUSet: "1-3",
					Entries:       make(map[string]map[string]string),
				})
			},
		},
		{
			name: "V3 checkpoint",
			currentCheckpoint: &CPUManagerCheckpointV3{
				PolicyName:    "none",
				DefaultCPUSet: "1-3",
				Entries:       make(map[string]map[string]string),
				PodEntries:    PodCPUAssignments{"pod": PodEntry{cpuset.New(4, 5)}},
			},
			expectedLegacyChecksum: func() checksum.Checksum {
				type CPUManagerCheckpoint struct {
					PolicyName    string                       `json:"policyName"`
					DefaultCPUSet string                       `json:"defaultCpuSet"`
					Entries       map[string]map[string]string `json:"entries,omitempty"`
					PodEntries    PodCPUAssignments            `json:"podEntries,omitempty"`
					Checksum      checksum.Checksum            `json:"checksum"`
				}
				return checksum.New(&CPUManagerCheckpoint{
					PolicyName:    "none",
					DefaultCPUSet: "1-3",
					Entries:       make(map[string]map[string]string),
					PodEntries:    PodCPUAssignments{"pod": PodEntry{cpuset.New(4, 5)}},
				})
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. 使用当前实现序列化，确认它仍以历史结构名计算校验和。
			data, err := tc.currentCheckpoint.(interface{ MarshalCheckpoint() ([]byte, error) }).MarshalCheckpoint()
			if err != nil {
				t.Fatalf("Failed to marshal checkpoint: %v", err)
			}

			// 2. 从原始 JSON 中取出实际写入的校验和。
			var result map[string]interface{}
			if err := json.Unmarshal(data, &result); err != nil {
				t.Fatalf("Failed to unmarshal JSON: %v", err)
			}

			actualChecksumFloat, ok := result["checksum"].(float64)
			if !ok {
				t.Fatalf("Checksum field missing or invalid type")
			}
			writtenChecksum := checksum.Checksum(uint64(actualChecksumFloat))

			// 3. 用旧版结构体计算期望校验和。
			expectedLegacyChecksum := tc.expectedLegacyChecksum()

			// 4. 二者必须一致，否则降级后 kubelet 会认为 checkpoint 损坏。
			// 4. 二者必须一致，否则降级后 kubelet 会认为 checkpoint 损坏；
			// 这是版本兼容性的关键约束。
			if writtenChecksum != expectedLegacyChecksum {
				t.Errorf("Written Checksum %d does not match legacy calculation %d. Forward compatibility broken.", writtenChecksum, expectedLegacyChecksum)
			}
		})
	}
}

// TestCPUManagerCheckpointV3_VerifyChecksum_Compatibility 验证 v3 checkpoint 同时接受历史与当前校验和。
func TestCPUManagerCheckpointV3_VerifyChecksum_Compatibility(t *testing.T) {
	testCases := []struct {
		name     string
		checksum checksum.Checksum
	}{
		{
			name:     "accepts legacy v3 checksum",
			checksum: 766259872,
		},
		{
			name:     "accepts current v3 checksum",
			checksum: 2284712151,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 两种校验和都应通过，保证升级过程中旧 checkpoint 不会被误判为损坏。
			cp := &CPUManagerCheckpointV3{
				PolicyName:    "static",
				DefaultCPUSet: "1-2",
				Entries: map[string]map[string]string{
					"pod1": {
						"container1": "5-6",
						"container2": "3-4",
					},
				},
				PodEntries: PodCPUAssignments{
					"pod2": {CPUSet: cpuset.New(7, 8, 9)},
				},
				Checksum: tc.checksum,
			}

			require.NoError(t, cp.VerifyChecksum())
		})
	}
}

// TestCPUManagerCheckpoint_RoundTrip 验证 checkpoint 序列化后能反序列化回等价对象。
func TestCPUManagerCheckpoint_RoundTrip(t *testing.T) {
	testCases := []struct {
		name     string
		original checkpointmanager.Checkpoint
		restored checkpointmanager.Checkpoint
		verify   func(t *testing.T, original, restored checkpointmanager.Checkpoint)
	}{
		{
			name: "V2 checkpoint",
			original: &CPUManagerCheckpointV2{
				PolicyName:    "static",
				DefaultCPUSet: "0-3",
				Entries: map[string]map[string]string{
					"pod1": {"container1": "4-5"},
				},
			},
			restored: newCPUManagerCheckpointV2(),
			verify: func(t *testing.T, original, restored checkpointmanager.Checkpoint) {
				o := original.(*CPUManagerCheckpointV2)
				r := restored.(*CPUManagerCheckpointV2)
				require.Equal(t, o.PolicyName, r.PolicyName)
				require.Equal(t, o.DefaultCPUSet, r.DefaultCPUSet)
				require.Equal(t, o.Entries, r.Entries)
			},
		},
		{
			name: "V3 checkpoint",
			original: &CPUManagerCheckpointV3{
				PolicyName:    "static",
				DefaultCPUSet: "0-3",
				Entries: map[string]map[string]string{
					"pod1": {"container1": "4-5"},
				},
				PodEntries: PodCPUAssignments{
					"pod2": {CPUSet: cpuset.New(6, 7)},
				},
			},
			restored: newCPUManagerCheckpointV3(),
			verify: func(t *testing.T, original, restored checkpointmanager.Checkpoint) {
				o := original.(*CPUManagerCheckpointV3)
				r := restored.(*CPUManagerCheckpointV3)
				require.Equal(t, o.PolicyName, r.PolicyName)
				require.Equal(t, o.DefaultCPUSet, r.DefaultCPUSet)
				require.Equal(t, o.Entries, r.Entries)
				require.Equal(t, o.PodEntries, r.PodEntries)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 序列化、反序列化并校验，覆盖完整的磁盘往返路径。
			data, err := tc.original.MarshalCheckpoint()
			require.NoError(t, err)

			require.NoError(t, tc.restored.UnmarshalCheckpoint(data))
			require.NoError(t, tc.restored.VerifyChecksum())

			tc.verify(t, tc.original, tc.restored)
		})
	}
}

// removeAll 删除测试临时目录，失败即终止测试。
func removeAll(dir string, t *testing.T) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("unable to remove dir %s: %v", dir, err)
	}
}
