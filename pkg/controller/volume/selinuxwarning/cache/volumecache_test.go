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

package cache

import (
	"reflect"
	"sort"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller/volume/selinuxwarning/internal/parse"
	"k8s.io/kubernetes/pkg/controller/volume/selinuxwarning/translator"
)

// getTestLoggers 返回两个 logger：默认 V(5) 的普通 logger（不输出 dump），
// 以及 V(dumpLogLevel) 的 logger（会输出缓存 dump）。
// 后者仅在断言失败时用于打印缓存内容，避免正常测试产生大量日志。
func getTestLoggers(t *testing.T) (klog.Logger, klog.Logger) {
	// 默认 V(5) 的 logger：正常测试不应输出缓存 dump，避免日志噪音。
	logger, _ := ktesting.NewTestContext(t)

	// V(dumpLogLevel) 的 logger：仅在断言失败需要诊断缓存内容时使用。
	logConfig := ktesting.NewConfig(ktesting.Verbosity(dumpLogLevel))
	dumpLogger := ktesting.NewLogger(t, logConfig)

	return logger, dumpLogger
}

// sortConflicts 按 Conflict.Pod 的字符串表示原地排序，
// 使基于 DeepEqual 的比较不依赖 map 迭代顺序。
func sortConflicts(conflicts []Conflict) {
	sort.Slice(conflicts, func(i, j int) bool {
		return conflicts[i].Pod.String() < conflicts[j].Pod.String()
	})
}

// verifyReverseIndexConsistency 校验正向索引（volume→pod）与反向索引（pod→volume）对称一致：
// 反向索引中的每个 (pod, volume) 必须能在正向索引找到，反之亦然；不一致时报告错误。
func verifyReverseIndexConsistency(t *testing.T, c *volumeCache) {
	t.Helper()

	// 校验反向索引里的每个 (pod, volume) 都能在正向索引中对应。
	for podKey, volumes := range c.podToVolumes {
		for volumeName := range volumes {
			volume, found := c.volumes[volumeName]
			if !found {
				t.Errorf("Reverse index has pod %s -> volume %s, but volume not in forward index", podKey, volumeName)
				continue
			}
			if _, found := volume.pods[podKey]; !found {
				t.Errorf("Reverse index has pod %s -> volume %s, but pod not in volume's pod list", podKey, volumeName)
			}
		}
	}

	// 反向校验正向索引里的每个 (volume, pod) 都能在反向索引中对应，确保两侧严格对称。
	for volumeName, volume := range c.volumes {
		for podKey := range volume.pods {
			podVolumes, found := c.podToVolumes[podKey]
			if !found {
				t.Errorf("Forward index has volume %s -> pod %s, but pod not in reverse index", volumeName, podKey)
				continue
			}
			if _, found := podVolumes[volumeName]; !found {
				t.Errorf("Forward index has volume %s -> pod %s, but volume not in pod's volume list", volumeName, podKey)
			}
		}
	}
}

// TestVolumeCache_DeleteAll 向缓存批量加入 4 个命名空间 × 4 个 pod × 4 个卷（共 64 条），
// 然后逐个 DeletePod，验证正向与反向索引都被清空且保持一致。
// 使用统一标签/策略以排除干扰冲突，仅聚焦删除后的清空行为。
func TestVolumeCache_DeleteAll(t *testing.T) {
	var podsToDelete []cache.ObjectName
	seLinuxTranslator := &translator.ControllerSELinuxTranslator{}
	c := NewVolumeLabelCache(seLinuxTranslator).(*volumeCache)
	logger, dumpLogger := getTestLoggers(t)

	// 准备阶段：向缓存加入大量卷，放大删除路径中索引清理可能遗漏的问题。
	for _, namespace := range []string{"ns1", "ns2", "ns3", "ns4"} {
		for _, name := range []string{"pod1", "pod2", "pod3", "pod4"} {
			for _, volumeName := range []v1.UniqueVolumeName{"vol1", "vol2", "vol3", "vol4"} {
				podKey := cache.ObjectName{Namespace: namespace, Name: name}
				podsToDelete = append(podsToDelete, podKey)
				conflicts := c.AddVolume(logger, volumeName, podKey, "label1", v1.SELinuxChangePolicyMountOption, "csiDriver1")
				if len(conflicts) != 0 {
					// 所有卷的标签与策略相同，理论上不应产生冲突，出现即说明逻辑有误。
					t.Errorf("AddVolume %s/%s %s returned unexpected conflicts: %+v", namespace, name, volumeName, conflicts)
				}
			}
		}
	}
	t.Log("Before deleting all pods:")
	c.dump(dumpLogger)

	verifyReverseIndexConsistency(t, c)

	// 执行阶段：逐个删除所有 pod，触发卷与反向索引的清理。
	for _, podKey := range podsToDelete {
		c.DeletePod(logger, podKey)
	}

	// 断言阶段：正向索引（volumes）应为空。
	if len(c.volumes) != 0 {
		t.Errorf("Expected cache to be empty, got %d volumes", len(c.volumes))
		c.dump(dumpLogger)
	}

	// 反向索引（podToVolumes）也必须为空；只清理一侧会导致陈旧映射残留。
	if len(c.podToVolumes) != 0 {
		t.Errorf("Expected reverse index to be empty, got %d pods", len(c.podToVolumes))
	}
	verifyReverseIndexConsistency(t, c)
}

// podWithVolume 汇总一条测试用例涉及的 Pod 与卷标识、SELinux 标签及变更策略。
type podWithVolume struct {
	podNamespace string
	podName      string
	volumeName   v1.UniqueVolumeName
	label        string
	changePolicy v1.PodSELinuxChangePolicy
}

// addReverseConflict 为每个 Conflict 追加一条方向相反的镜像条目（pod 与 otherPod 互换），
// 因为被测代码会对每对冲突同时报告两个方向，期望值需匹配该行为。
func addReverseConflict(conflicts []Conflict) []Conflict {
	newConflicts := make([]Conflict, 0, len(conflicts)*2)
	for _, c := range conflicts {
		reversedConflict := Conflict{
			PropertyName:       c.PropertyName,
			EventReason:        c.EventReason,
			Pod:                c.OtherPod,
			PropertyValue:      c.OtherPropertyValue,
			OtherPod:           c.Pod,
			OtherPropertyValue: c.PropertyValue,
		}
		newConflicts = append(newConflicts, c, reversedConflict)
	}
	return newConflicts
}

// TestVolumeCache_AddVolumeGetConflicts 表驱动验证 AddVolume 返回的冲突与 GetConflicts 一致：
// 覆盖新卷、同标签/策略复用卷、标签或策略冲突、pod 替换，以及 SELinux 标签各组成部分
// （user/role/category）缺失导致的可比与不可比情形；同时校验 podInfo 被正确缓存与索引一致。
func TestVolumeCache_AddVolumeGetConflicts(t *testing.T) {
	existingPods := []podWithVolume{
		{
			podNamespace: "ns1",
			podName:      "pod1-mountOption",
			volumeName:   "vol1",
			label:        "system_u:system_r:label1",
			changePolicy: v1.SELinuxChangePolicyMountOption,
		},
		{
			podNamespace: "ns2",
			podName:      "pod2-recursive",
			volumeName:   "vol2",
			label:        "", // Recursive 策略卷的标签会被清空，不应报告标签冲突
			changePolicy: v1.SELinuxChangePolicyRecursive,
		},
		{
			podNamespace: "ns3",
			podName:      "pod3-1",
			volumeName:   "vol3", // vol3 被两个相同标签 + Recursive 策略的 pod 使用
			label:        "",     // Recursive 策略卷的标签会被清空，不应报告标签冲突
			changePolicy: v1.SELinuxChangePolicyRecursive,
		},
		{
			podNamespace: "ns3",
			podName:      "pod3-2",
			volumeName:   "vol3", // vol3 被两个相同标签 + Recursive 策略的 pod 使用
			label:        "",     // Recursive 策略卷的标签会被清空，不应报告标签冲突
			changePolicy: v1.SELinuxChangePolicyRecursive,
		},
		{
			podNamespace: "ns4",
			podName:      "pod4-1",
			volumeName:   "vol4", // vol4 被两个相同标签 + MountOption 策略的 pod 使用
			label:        "system_u:system_r:label4",
			changePolicy: v1.SELinuxChangePolicyMountOption,
		},
		{
			podNamespace: "ns4",
			podName:      "pod4-2",
			volumeName:   "vol4", // vol4 被两个相同标签 + MountOption 策略的 pod 使用
			label:        "system_u:system_r:label4",
			changePolicy: v1.SELinuxChangePolicyMountOption,
		},
		{
			podNamespace: "ns5",
			podName:      "pod5",
			volumeName:   "vol5", // vol5 的标签缺少 user 与 role 部分
			label:        "::label5",
			changePolicy: v1.SELinuxChangePolicyMountOption,
		},
		{
			podNamespace: "ns6",
			podName:      "pod6",
			volumeName:   "vol6", // vol6 的标签缺少 user 部分
			label:        ":system_r:label6",
			changePolicy: v1.SELinuxChangePolicyMountOption,
		},
		{
			podNamespace: "ns7",
			podName:      "pod7",
			volumeName:   "vol7", // vol7 的标签缺少 user 与 role，但含 categories
			label:        "::label7:c0,c1",
			changePolicy: v1.SELinuxChangePolicyMountOption,
		},
		{
			podNamespace: "ns8",
			podName:      "pod8",
			volumeName:   "vol8", // vol8 没有任何标签
			label:        "",
			changePolicy: v1.SELinuxChangePolicyMountOption,
		},
	}

	tests := []struct {
		name              string
		initialPods       []podWithVolume
		podToAdd          podWithVolume
		expectedConflicts []Conflict // 反方向的冲突（pod/otherPod 互换）会由 addReverseConflict 自动补上
	}{
		{
			name:        "new volume in empty cache",
			initialPods: nil,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol-new",
				label:        "system_u:system_r:label-new",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: nil,
		},
		{
			name:        "new volume",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol-new",
				label:        "system_u:system_r:label-new",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: nil,
		},
		{
			name:        "existing volume in a new pod with existing policy and label",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol1",
				label:        "system_u:system_r:label1",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: nil,
		},
		{
			name:        "existing volume in a new pod with existing policy and new conflicting label",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol1",
				label:        "system_u:system_r:label-new",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: []Conflict{
				{
					PropertyName:       "SELinuxLabel",
					EventReason:        "SELinuxLabelConflict",
					Pod:                cache.ObjectName{Namespace: "testns", Name: "testpod"},
					PropertyValue:      "system_u:system_r:label-new",
					OtherPod:           cache.ObjectName{Namespace: "ns1", Name: "pod1-mountOption"},
					OtherPropertyValue: "system_u:system_r:label1",
				},
			},
		},
		{
			name:        "existing volume in a new pod with new conflicting policy",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol1",
				label:        "",
				changePolicy: v1.SELinuxChangePolicyRecursive,
			},
			expectedConflicts: []Conflict{
				{
					PropertyName:       "SELinuxChangePolicy",
					EventReason:        "SELinuxChangePolicyConflict",
					Pod:                cache.ObjectName{Namespace: "testns", Name: "testpod"},
					PropertyValue:      "Recursive",
					OtherPod:           cache.ObjectName{Namespace: "ns1", Name: "pod1-mountOption"},
					OtherPropertyValue: "MountOption",
				},
			},
		},
		{
			name:        "existing pod is replaced with different non-conflicting policy and label",
			initialPods: existingPods,
			podToAdd: podWithVolume{

				podNamespace: "ns2",
				podName:      "pod2-recursive",
				volumeName:   "vol2",                            // there is no other pod that uses vol2 -> change of policy and label is possible
				label:        "system_u:system_r:label-new",     // was label2 in the original pod2
				changePolicy: v1.SELinuxChangePolicyMountOption, // was Recursive in the original pod2
			},
			expectedConflicts: nil,
		},
		{
			name:        "existing pod is replaced with conflicting policy",
			initialPods: existingPods,
			podToAdd: podWithVolume{

				podNamespace: "ns3",
				podName:      "pod3-1",
				volumeName:   "vol3",                            // vol3 is used by pod3-2 with label3 and Recursive policy
				label:        "system_u:system_r:label-new",     // Technically, it's not possible to change a label of an existing pod, but we still check for conflicts
				changePolicy: v1.SELinuxChangePolicyMountOption, // ChangePolicy change can happen when CSIDriver is updated from SELinuxMount: false to SELinuxMount: true
			},
			expectedConflicts: []Conflict{
				{
					PropertyName:       "SELinuxChangePolicy",
					EventReason:        "SELinuxChangePolicyConflict",
					Pod:                cache.ObjectName{Namespace: "ns3", Name: "pod3-1"},
					PropertyValue:      "MountOption",
					OtherPod:           cache.ObjectName{Namespace: "ns3", Name: "pod3-2"},
					OtherPropertyValue: "Recursive",
				},
			},
		},
		{
			name:        "existing volume in a new pod with existing policy and new incomparable label (missing user and role)",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol5",
				label:        "system_u:system_r:label5",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: []Conflict{},
		},
		{
			name:        "existing volume in a new pod with conflicting policy with incomparable parts",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol5",
				label:        "::label6",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: []Conflict{
				{
					PropertyName:       "SELinuxLabel",
					EventReason:        "SELinuxLabelConflict",
					Pod:                cache.ObjectName{Namespace: "testns", Name: "testpod"},
					PropertyValue:      "::label6",
					OtherPod:           cache.ObjectName{Namespace: "ns5", Name: "pod5"},
					OtherPropertyValue: "::label5",
				},
			},
		},
		{
			name:        "existing volume in a new pod with existing policy and new incomparable label (missing user)",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol6",
				label:        "system_u::label6",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: []Conflict{},
		},
		{
			name:        "existing volume in a new pod with existing policy and new comparable label (missing categories)",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol7",
				label:        "system_u:system_r:label7",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: []Conflict{
				{
					PropertyName:       "SELinuxLabel",
					EventReason:        "SELinuxLabelConflict",
					Pod:                cache.ObjectName{Namespace: "testns", Name: "testpod"},
					PropertyValue:      "system_u:system_r:label7",
					OtherPod:           cache.ObjectName{Namespace: "ns7", Name: "pod7"},
					OtherPropertyValue: "::label7:c0,c1",
				},
			},
		},
		{
			name:        "existing volume in a new pod with existing policy and new incomparable label (missing everything)",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol8",
				label:        "system_u:system_r:label8",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: []Conflict{},
		},
		{
			name:        "existing volume in a new pod with existing policy and new comparable label (missing everything but categories)",
			initialPods: existingPods,
			podToAdd: podWithVolume{
				podNamespace: "testns",
				podName:      "testpod",
				volumeName:   "vol8",
				label:        "system_u:system_r:label8:c0,c1",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			expectedConflicts: []Conflict{
				{
					PropertyName:       "SELinuxLabel",
					EventReason:        "SELinuxLabelConflict",
					Pod:                cache.ObjectName{Namespace: "testns", Name: "testpod"},
					PropertyValue:      "system_u:system_r:label8:c0,c1",
					OtherPod:           cache.ObjectName{Namespace: "ns8", Name: "pod8"},
					OtherPropertyValue: "",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, dumpLogger := getTestLoggers(t)
			// 准备阶段：先把既有 pod 加入缓存，确保后续 AddVolume 与已有条目产生交互。
			seLinuxTranslator := &translator.ControllerSELinuxTranslator{}
			c := NewVolumeLabelCache(seLinuxTranslator).(*volumeCache)
			for _, podToAdd := range tt.initialPods {
				conflicts := c.AddVolume(logger, podToAdd.volumeName, cache.ObjectName{Namespace: podToAdd.podNamespace, Name: podToAdd.podName}, podToAdd.label, podToAdd.changePolicy, "csiDriver1")
				if len(conflicts) != 0 {
					t.Errorf("Initial AddVolume %s/%s %s returned unexpected conflicts: %+v", podToAdd.podNamespace, podToAdd.podName, podToAdd.volumeName, conflicts)
				}
			}

			// 执行阶段：加入待测 pod，观察返回的冲突。
			conflicts := c.AddVolume(logger, tt.podToAdd.volumeName, cache.ObjectName{Namespace: tt.podToAdd.podNamespace, Name: tt.podToAdd.podName}, tt.podToAdd.label, tt.podToAdd.changePolicy, "csiDriver1")

			// 断言阶段：补齐反向冲突并排序后再比较，避免顺序影响。
			expectedConflicts := addReverseConflict(tt.expectedConflicts)
			sortConflicts(conflicts)
			sortConflicts(expectedConflicts)
			if !reflect.DeepEqual(conflicts, expectedConflicts) {
				t.Errorf("AddVolume returned unexpected conflicts: %+v", conflicts)
				t.Logf("Expected conflicts: %+v", expectedConflicts)
				c.dump(dumpLogger)
			}
			// 即使存在冲突，pod 与卷仍应被写入缓存，冲突报告不应阻断添加。
			volume, ok := c.volumes[tt.podToAdd.volumeName]
			if !ok {
				t.Errorf("volume %s is not present in the cache", tt.podToAdd.volumeName)
			}
			podKey := cache.ObjectName{Namespace: tt.podToAdd.podNamespace, Name: tt.podToAdd.podName}
			existingInfo, ok := volume.pods[podKey]
			if !ok {
				t.Errorf("pod %s is not present in the cache", podKey)
			}
			expectedPodInfo := podInfo{
				seLinuxLabel: tt.podToAdd.label,
				seLinuxParts: parse.ParseSELinuxLabel(tt.podToAdd.label),
				changePolicy: tt.podToAdd.changePolicy,
			}
			if !reflect.DeepEqual(existingInfo, expectedPodInfo) {
				t.Errorf("pod %s has unexpected info: %+v", podKey, existingInfo)
			}

			// 每次添加后都校验双向索引一致性，及时暴露索引维护错误。
			verifyReverseIndexConsistency(t, c)

			// GetConflicts 的结果必须与 AddVolume 当时返回的冲突一致，保证查询接口可靠。
			receivedConflicts := c.GetConflicts(logger)
			sortConflicts(receivedConflicts)
			if !reflect.DeepEqual(receivedConflicts, expectedConflicts) {
				t.Errorf("SendConflicts returned unexpected conflicts: %+v", receivedConflicts)
				c.dump(dumpLogger)
			}
		})
	}
}

// TestVolumeCache_MultiVolumeConflicts 验证冲突按卷分别记录：
// 同一 pod 在 vol1、vol2 上分别与其他 pod 冲突时，GetConflicts 返回两者之和；
// 删除该 pod 后所有相关冲突消失；另外验证不同卷上完全相同的冲突条目会被去重。
func TestVolumeCache_MultiVolumeConflicts(t *testing.T) {
	logger, _ := getTestLoggers(t)
	seLinuxTranslator := &translator.ControllerSELinuxTranslator{}
	c := NewVolumeLabelCache(seLinuxTranslator).(*volumeCache)

	podA := cache.ObjectName{Namespace: "ns", Name: "podA"}
	podB := cache.ObjectName{Namespace: "ns", Name: "podB"}
	podC := cache.ObjectName{Namespace: "ns", Name: "podC"}

	// podB 使用 vol1，标签为 labelB。
	c.AddVolume(logger, "vol1", podB, "system_u:system_r:labelB", v1.SELinuxChangePolicyMountOption, "driver1")
	// podC 使用 vol2，标签为 labelC。
	c.AddVolume(logger, "vol2", podC, "system_u:system_r:labelC", v1.SELinuxChangePolicyMountOption, "driver1")

	// podA 以不同标签使用 vol1，预期与 podB 冲突。
	conflicts1 := c.AddVolume(logger, "vol1", podA, "system_u:system_r:labelA", v1.SELinuxChangePolicyMountOption, "driver1")
	if len(conflicts1) == 0 {
		t.Fatal("Expected conflicts on vol1 between podA and podB")
	}

	// podA 同时以不同标签使用 vol2，预期与 podC 冲突。
	conflicts2 := c.AddVolume(logger, "vol2", podA, "system_u:system_r:labelA", v1.SELinuxChangePolicyMountOption, "driver1")
	if len(conflicts2) == 0 {
		t.Fatal("Expected conflicts on vol2 between podA and podC")
	}

	// 核心断言：GetConflicts 必须返回两个卷上的冲突之和，而非只保留最后一个卷的。
	allConflicts := c.GetConflicts(logger)
	expectedCount := len(conflicts1) + len(conflicts2)
	if len(allConflicts) != expectedCount {
		t.Errorf("GetConflicts returned %d conflicts, expected %d (vol1: %d + vol2: %d)",
			len(allConflicts), expectedCount, len(conflicts1), len(conflicts2))
	}

	// 删除 podA 后，其引发的冲突应全部清除，验证删除会清理所有卷上的关联。
	c.DeletePod(logger, podA)
	remaining := c.GetConflicts(logger)
	if len(remaining) != 0 {
		t.Errorf("Expected no conflicts after deleting podA, got %d: %+v", len(remaining), remaining)
	}

	// 验证去重：podD 与 podE 在两个卷上以相同标签冲突。
	// 来自不同卷但内容完全相同的 Conflict 条目应被 GetConflicts 去重。
	podD := cache.ObjectName{Namespace: "ns", Name: "podD"}
	podE := cache.ObjectName{Namespace: "ns", Name: "podE"}

	c.AddVolume(logger, "vol3", podD, "system_u:system_r:labelD", v1.SELinuxChangePolicyMountOption, "driver1")
	c.AddVolume(logger, "vol4", podD, "system_u:system_r:labelD", v1.SELinuxChangePolicyMountOption, "driver1")

	conflictsVol3 := c.AddVolume(logger, "vol3", podE, "system_u:system_r:labelE", v1.SELinuxChangePolicyMountOption, "driver1")
	conflictsVol4 := c.AddVolume(logger, "vol4", podE, "system_u:system_r:labelE", v1.SELinuxChangePolicyMountOption, "driver1")

	if len(conflictsVol3) != len(conflictsVol4) {
		t.Fatalf("Expected same number of conflicts from vol3 and vol4 (%d vs %d)", len(conflictsVol3), len(conflictsVol4))
	}
	if len(conflictsVol3) == 0 {
		t.Fatal("Expected conflicts between podD and podE")
	}

	allConflicts = c.GetConflicts(logger)
	deCount := 0
	for _, conflict := range allConflicts {
		if conflict.Pod == podD || conflict.Pod == podE || conflict.OtherPod == podD || conflict.OtherPod == podE {
			deCount++
		}
	}
	if deCount != len(conflictsVol3) {
		t.Errorf("Expected %d deduplicated conflicts for podD/podE (from 2 volumes), got %d", len(conflictsVol3), deCount)
	}
}

// TestVolumeCache_DeletePodConflicts 表驱动验证 DeletePod 对冲突的影响：
// 删除冲突双方之一会清除该对冲突，删除无关 pod 保留既有冲突，
// 删除在多个卷上有冲突的 pod 会一并清除；同时校验双向冲突对称性与重复删除的幂等性。
func TestVolumeCache_DeletePodConflicts(t *testing.T) {
	podA := cache.ObjectName{Namespace: "ns", Name: "podA"}
	podB := cache.ObjectName{Namespace: "ns", Name: "podB"}
	podC := cache.ObjectName{Namespace: "ns", Name: "podC"}
	podD := cache.ObjectName{Namespace: "ns", Name: "podD"}

	tests := []struct {
		name string
		// 删除前预先加入缓存的 pod 列表。
		initialPods []podWithVolume
		// 本用例要删除的 pod。
		podToDelete cache.ObjectName
		// 为 true 时会再删一次，用于验证删除操作的幂等性。
		deleteTwice bool
		// 删除后仍应保留双向冲突的 pod 对。
		// 每一对 [2]cache.ObjectName 期望同时存在 (A→B) 与 (B→A) 两个方向。
		expectedSurvivingPairs [][2]cache.ObjectName
	}{
		{
			name: "delete one of two conflicting pods clears all conflicts",
			initialPods: []podWithVolume{
				{podNamespace: "ns", podName: "podA", volumeName: "vol1", label: "system_u:system_r:labelA", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podB", volumeName: "vol1", label: "system_u:system_r:labelB", changePolicy: v1.SELinuxChangePolicyMountOption},
			},
			podToDelete:            podA,
			expectedSurvivingPairs: nil,
		},
		{
			name: "delete non-conflicting pod preserves existing conflicts",
			initialPods: []podWithVolume{
				{podNamespace: "ns", podName: "podA", volumeName: "vol1", label: "system_u:system_r:labelA", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podB", volumeName: "vol1", label: "system_u:system_r:labelB", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podC", volumeName: "vol2", label: "system_u:system_r:labelC", changePolicy: v1.SELinuxChangePolicyMountOption},
			},
			podToDelete:            podC,
			expectedSurvivingPairs: [][2]cache.ObjectName{{podA, podB}},
		},
		{
			name: "three pods on same volume delete one leaves remaining pair conflict",
			initialPods: []podWithVolume{
				{podNamespace: "ns", podName: "podA", volumeName: "vol1", label: "system_u:system_r:labelA", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podB", volumeName: "vol1", label: "system_u:system_r:labelB", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podC", volumeName: "vol1", label: "system_u:system_r:labelC", changePolicy: v1.SELinuxChangePolicyMountOption},
			},
			podToDelete:            podA,
			expectedSurvivingPairs: [][2]cache.ObjectName{{podB, podC}},
		},
		{
			name: "delete pod with conflicts on multiple volumes",
			initialPods: []podWithVolume{
				{podNamespace: "ns", podName: "podB", volumeName: "vol1", label: "system_u:system_r:labelB", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podC", volumeName: "vol2", label: "system_u:system_r:labelC", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podA", volumeName: "vol1", label: "system_u:system_r:labelA", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podA", volumeName: "vol2", label: "system_u:system_r:labelA", changePolicy: v1.SELinuxChangePolicyMountOption},
			},
			podToDelete:            podA,
			expectedSurvivingPairs: nil,
		},
		{
			name: "delete pod preserves conflicts on unrelated volumes",
			initialPods: []podWithVolume{
				{podNamespace: "ns", podName: "podA", volumeName: "vol1", label: "system_u:system_r:labelA", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podB", volumeName: "vol1", label: "system_u:system_r:labelB", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podC", volumeName: "vol2", label: "system_u:system_r:labelC", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podD", volumeName: "vol2", label: "system_u:system_r:labelD", changePolicy: v1.SELinuxChangePolicyMountOption},
			},
			podToDelete:            podA,
			expectedSurvivingPairs: [][2]cache.ObjectName{{podC, podD}},
		},
		{
			name: "delete pod that was already deleted is a no-op",
			initialPods: []podWithVolume{
				{podNamespace: "ns", podName: "podA", volumeName: "vol1", label: "system_u:system_r:labelA", changePolicy: v1.SELinuxChangePolicyMountOption},
				{podNamespace: "ns", podName: "podB", volumeName: "vol1", label: "system_u:system_r:labelB", changePolicy: v1.SELinuxChangePolicyMountOption},
			},
			podToDelete:            podA,
			deleteTwice:            true,
			expectedSurvivingPairs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := getTestLoggers(t)
			seLinuxTranslator := &translator.ControllerSELinuxTranslator{}
			c := NewVolumeLabelCache(seLinuxTranslator).(*volumeCache)

			for _, pod := range tt.initialPods {
				c.AddVolume(logger, pod.volumeName, cache.ObjectName{Namespace: pod.podNamespace, Name: pod.podName}, pod.label, pod.changePolicy, "driver1")
			}

			c.DeletePod(logger, tt.podToDelete)
			if tt.deleteTwice {
				c.DeletePod(logger, tt.podToDelete)
			}

			remaining := c.GetConflicts(logger)

			// 被删除的 pod 不得出现在任何冲突中，确保删除清理彻底。
			for _, conflict := range remaining {
				if conflict.Pod == tt.podToDelete || conflict.OtherPod == tt.podToDelete {
					t.Errorf("found conflict involving deleted pod %s: %+v", tt.podToDelete, conflict)
				}
			}

			// 存活的冲突对必须双向对称，避免只清理了一个方向。
			for _, pair := range tt.expectedSurvivingPairs {
				hasForward := false
				hasReverse := false
				for _, conflict := range remaining {
					if conflict.Pod == pair[0] && conflict.OtherPod == pair[1] {
						hasForward = true
					}
					if conflict.Pod == pair[1] && conflict.OtherPod == pair[0] {
						hasReverse = true
					}
				}
				if !hasForward || !hasReverse {
					t.Errorf("expected symmetric conflict between %s and %s, got %+v", pair[0], pair[1], remaining)
				}
			}

			// 若没有期望保留的冲突对，则缓存中不应残留任何冲突。
			if len(tt.expectedSurvivingPairs) == 0 && len(remaining) != 0 {
				t.Errorf("expected no conflicts, got %+v", remaining)
			}

			verifyReverseIndexConsistency(t, c)
		})
	}
}

// TestVolumeCache_GetPodsForCSIDriver 验证按 CSI 驱动名聚合 pod：
// 先向三个驱动分别加入若干 pod（含共享卷场景），再断言每个驱动返回的 pod 列表正确，
// 未知驱动返回 nil。返回列表排序后再比较，消除 map 迭代顺序影响。
func TestVolumeCache_GetPodsForCSIDriver(t *testing.T) {
	seLinuxTranslator := &translator.ControllerSELinuxTranslator{}
	c := NewVolumeLabelCache(seLinuxTranslator).(*volumeCache)
	logger, dumpLogger := getTestLoggers(t)

	existingPods := map[string][]podWithVolume{
		"csiDriver1": {
			{
				podNamespace: "ns1",
				podName:      "pod1-mountOption",
				volumeName:   "vol1",
				label:        "label1",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
		},
		"csiDriver2": {
			{
				podNamespace: "ns2",
				podName:      "pod2-recursive",
				volumeName:   "vol2",
				label:        "label2",
				changePolicy: v1.SELinuxChangePolicyRecursive,
			},
			{
				podNamespace: "ns3",
				podName:      "pod3-1",
				volumeName:   "vol3", // vol3 被两个相同标签 + Recursive 策略的 pod 使用
				label:        "label3",
				changePolicy: v1.SELinuxChangePolicyRecursive,
			},
			{
				podNamespace: "ns3",
				podName:      "pod3-2",
				volumeName:   "vol3", // vol3 被两个相同标签 + Recursive 策略的 pod 使用
				label:        "label3",
				changePolicy: v1.SELinuxChangePolicyRecursive,
			},
		},
		"csiDriver3": {
			{
				podNamespace: "ns4",
				podName:      "pod4-1",
				volumeName:   "vol4", // vol4 被两个相同标签 + MountOption 策略的 pod 使用
				label:        "label4",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
			{
				podNamespace: "ns4",
				podName:      "pod4-2",
				volumeName:   "vol4", // vol4 被两个相同标签 + MountOption 策略的 pod 使用
				label:        "label4",
				changePolicy: v1.SELinuxChangePolicyMountOption,
			},
		},
	}

	for csiDriverName, pods := range existingPods {
		for _, podToAdd := range pods {
			conflicts := c.AddVolume(logger, podToAdd.volumeName, cache.ObjectName{Namespace: podToAdd.podNamespace, Name: podToAdd.podName}, podToAdd.label, podToAdd.changePolicy, csiDriverName)
			if len(conflicts) != 0 {
				t.Errorf("Initial AddVolume %s/%s %s returned unexpected conflicts: %+v", podToAdd.podNamespace, podToAdd.podName, podToAdd.volumeName, conflicts)
			}
		}
	}

	// 执行阶段：查询各 CSI 驱动对应的 pod 列表。
	expectedPods := map[string][]cache.ObjectName{
		"csiDriver1": {
			{Namespace: "ns1", Name: "pod1-mountOption"},
		},
		"csiDriver2": {
			{Namespace: "ns2", Name: "pod2-recursive"},
			{Namespace: "ns3", Name: "pod3-1"},
			{Namespace: "ns3", Name: "pod3-2"},
		},
		"csiDriver3": {
			{Namespace: "ns4", Name: "pod4-1"},
			{Namespace: "ns4", Name: "pod4-2"},
		},
		"csiDriver4": nil, // 完全未知的 CSI 驱动，应返回 nil
	}
	for csiDriverName, expectedPodsForDriver := range expectedPods {
		// 结果顺序取决于 map 迭代，先排序再比较以获得确定性断言。
		podsForDriver := c.GetPodsForCSIDriver(csiDriverName)
		sort.Slice(podsForDriver, func(i, j int) bool {
			return podsForDriver[i].String() < podsForDriver[j].String()
		})
		if !reflect.DeepEqual(podsForDriver, expectedPodsForDriver) {
			t.Errorf("GetPodsForCSIDriver(%s) returned unexpected pods: %+v", csiDriverName, podsForDriver)
			c.dump(dumpLogger)
		}
	}
}
