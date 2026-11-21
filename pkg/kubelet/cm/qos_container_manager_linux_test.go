//go:build linux

/*
Copyright 2021 The Kubernetes Authors.

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

package cm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

// activeTestPods 返回一组覆盖 Guaranteed 与 Burstable 的测试 Pod。
func activeTestPods() []*v1.Pod {
	return []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "12345678",
				Name:      "guaranteed-pod",
				Namespace: "test",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "foo",
						Image: "busybox",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("128Mi"),
								v1.ResourceCPU:    resource.MustParse("1"),
							},
							Limits: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("128Mi"),
								v1.ResourceCPU:    resource.MustParse("1"),
							},
						},
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "87654321",
				Name:      "burstable-pod-1",
				Namespace: "test",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "foo",
						Image: "busybox",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("128Mi"),
								v1.ResourceCPU:    resource.MustParse("1"),
							},
							Limits: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("256Mi"),
								v1.ResourceCPU:    resource.MustParse("2"),
							},
						},
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "01234567",
				Name:      "burstable-pod-2",
				Namespace: "test",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "foo",
						Image: "busybox",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("256Mi"),
								v1.ResourceCPU:    resource.MustParse("2"),
							},
						},
					},
				},
			},
		},
	}
}

// createTestQOSContainerManager 构造基于真实 cgroup 子系统信息的测试管理器，
// cgroupManager 可在用例中替换为 fake，避免写入 cgroupfs。
func createTestQOSContainerManager(logger klog.Logger) (*qosContainerManagerImpl, error) {
	subsystems, err := GetCgroupSubsystems()
	if err != nil {
		return nil, fmt.Errorf("failed to get mounted cgroup subsystems: %v", err)
	}

	cgroupRoot := ParseCgroupfsToCgroupName("/")
	cgroupRoot = NewCgroupName(cgroupRoot, defaultNodeAllocatableCgroupName)

	qosContainerManager := &qosContainerManagerImpl{
		subsystems:              subsystems,
		cgroupManager:           NewCgroupManager(logger, subsystems, "cgroupfs"),
		cgroupRoot:              cgroupRoot,
		qosReserved:             nil,
		memoryReservationPolicy: kubeletconfig.NoneMemoryReservationPolicy,
	}

	qosContainerManager.activePods = activeTestPods

	return qosContainerManager, nil
}

// TestQoSContainerCgroup 校验 setMemoryQoS 为 Guaranteed/Burstable cgroup
// 写入的 memory.min/memory.low 聚合值，包括 DRA 与 Pod 级资源场景。
func TestQoSContainerCgroup(t *testing.T) {
	burstableMin := resource.MustParse("384Mi")
	guaranteedMin := resource.MustParse("128Mi")

	tests := []struct {
		name                               string
		pods                               []*v1.Pod
		initialGuaranteed                  string
		initialBurstable                   string
		expectedGuaranteed                 string
		expectedBurstable                  string
		draNodeAllocatableResourcesEnabled bool
		podLevelResourcesEnabled           bool
	}{
		{
			name:               "writes aggregated memory min",
			pods:               activeTestPods(),
			initialGuaranteed:  "",
			initialBurstable:   "",
			expectedGuaranteed: strconv.FormatInt(burstableMin.Value()+guaranteedMin.Value(), 10),
			expectedBurstable:  strconv.FormatInt(burstableMin.Value(), 10),
		},
		{
			name: "writes zero memory min for best effort pod",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{UID: "99999999", Name: "besteffort-pod", Namespace: "test"},
					Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "foo", Image: "busybox"}}},
				},
			},
			initialGuaranteed:  "",
			initialBurstable:   "",
			expectedGuaranteed: "0",
			expectedBurstable:  "0",
		},
		{
			name: "writes zero memory min for burstable pod without memory request",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{UID: "88888888", Name: "burstable-pod-no-memory-request", Namespace: "test"},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "foo",
								Image: "busybox",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
								},
							},
						},
					},
				},
			},
			initialGuaranteed:  "",
			initialBurstable:   "",
			expectedGuaranteed: "0",
			expectedBurstable:  "0",
		},
		{
			name:               "clears stale memory min when all pods removed",
			pods:               nil,
			initialGuaranteed:  "1234",
			initialBurstable:   "5678",
			expectedGuaranteed: "0",
			expectedBurstable:  "0",
		},
		{
			name: "writes zero memory min for best effort pod with DRA",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{UID: "99999999", Name: "besteffort-pod-with-dra", Namespace: "test"},
					Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "foo", Image: "busybox"}}},
					Status: v1.PodStatus{
						NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
							{
								ResourceClaimName: "direct-claim",
								Containers:        []string{"foo"},
								Mapping: []v1.NodeAllocatableMappedResources{
									{Name: v1.ResourceMemory, Quantity: new(resource.MustParse("128Mi"))},
								},
							},
						},
					},
				},
			},
			draNodeAllocatableResourcesEnabled: true,
			initialGuaranteed:                  "",
			initialBurstable:                   "",
			expectedGuaranteed:                 "0",
			expectedBurstable:                  "0",
		},
		{
			name: "writes memory min including DRA allocations",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{UID: "12345678", Name: "guaranteed-pod", Namespace: "test"},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "foo",
								Image: "busybox",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("128Mi"),
										v1.ResourceCPU:    resource.MustParse("1"),
									},
									Limits: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("128Mi"),
										v1.ResourceCPU:    resource.MustParse("1"),
									},
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{UID: "87654321", Name: "burstable-pod", Namespace: "test"},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "foo",
								Image: "busybox",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("128Mi"),
										v1.ResourceCPU:    resource.MustParse("1"),
									},
									Limits: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("256Mi"),
										v1.ResourceCPU:    resource.MustParse("2"),
									},
								},
							},
						},
					},
					Status: v1.PodStatus{
						NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
							{
								ResourceClaimName: "direct-claim",
								Containers:        []string{"foo"},
								Mapping: []v1.NodeAllocatableMappedResources{
									{Name: v1.ResourceMemory, Quantity: new(resource.MustParse("128Mi"))},
								},
							},
						},
					},
				},
			},
			draNodeAllocatableResourcesEnabled: true,
			initialGuaranteed:                  "",
			initialBurstable:                   "",
			expectedGuaranteed:                 strconv.FormatInt(384*1024*1024, 10), // Guaranteed 128Mi + Burstable（128Mi 请求 + 128Mi DRA）= 384Mi
			expectedBurstable:                  strconv.FormatInt(256*1024*1024, 10), // Burstable 128Mi 请求 + 128Mi DRA = 256Mi
		},
		{
			name: "writes memory min with pod-level resources",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{UID: "12345678", Name: "guaranteed-pod", Namespace: "test"},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "foo",
								Image: "busybox",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("128Mi"),
										v1.ResourceCPU:    resource.MustParse("1"),
									},
									Limits: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("128Mi"),
										v1.ResourceCPU:    resource.MustParse("1"),
									},
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{UID: "87654321", Name: "burstable-pod-pod-level", Namespace: "test"},
					Spec: v1.PodSpec{
						Resources: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("512Mi"),
							},
						},
						Containers: []v1.Container{
							{
								Name:  "foo",
								Image: "busybox",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("128Mi"),
										v1.ResourceCPU:    resource.MustParse("1"),
									},
									Limits: v1.ResourceList{
										v1.ResourceMemory: resource.MustParse("256Mi"),
										v1.ResourceCPU:    resource.MustParse("2"),
									},
								},
							},
						},
					},
					Status: v1.PodStatus{
						NodeAllocatableResourceClaimStatuses: []v1.NodeAllocatableResourceClaimStatus{
							{
								ResourceClaimName: "direct-claim",
								Containers:        []string{"foo"},
								Mapping: []v1.NodeAllocatableMappedResources{
									{Name: v1.ResourceMemory, Quantity: new(resource.MustParse("128Mi"))},
								},
							},
						},
					},
				},
			},
			draNodeAllocatableResourcesEnabled: true,
			podLevelResourcesEnabled:           true,
			initialGuaranteed:                  "",
			initialBurstable:                   "",
			expectedGuaranteed:                 strconv.FormatInt(640*1024*1024, 10), // Guaranteed 128Mi + Burstable Pod 级 512Mi = 640Mi
			expectedBurstable:                  strconv.FormatInt(512*1024*1024, 10), // Burstable Pod 级请求 512Mi
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.DRANodeAllocatableResources, tc.draNodeAllocatableResourcesEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.PodLevelResources, tc.podLevelResourcesEnabled)
			// 每个子测试独立开关特性门控，避免 DRA / Pod 级资源用例相互影响。
			logger, _ := ktesting.NewTestContext(t)
			m, err := createTestQOSContainerManager(logger)
			require.NoError(t, err)
			// 只有 TieredReservation 策略才会写 memory.min。
			m.memoryReservationPolicy = kubeletconfig.TieredReservationMemoryReservationPolicy
			m.activePods = func() []*v1.Pod { return tc.pods }

			guaranteedUnified := map[string]string{}
			if tc.initialGuaranteed != "" {
				guaranteedUnified[Cgroup2MemoryMin] = tc.initialGuaranteed
			}
			burstableUnified := map[string]string{}
			if tc.initialBurstable != "" {
				burstableUnified[Cgroup2MemoryLow] = tc.initialBurstable
			}

			qosConfigs := map[v1.PodQOSClass]*CgroupConfig{
				v1.PodQOSGuaranteed: {
					Name: m.qosContainersInfo.Guaranteed,
					ResourceParameters: &ResourceConfig{
						Unified: guaranteedUnified,
					},
				},
				v1.PodQOSBurstable: {
					Name: m.qosContainersInfo.Burstable,
					ResourceParameters: &ResourceConfig{
						Unified: burstableUnified,
					},
				},
				v1.PodQOSBestEffort: {
					Name:               m.qosContainersInfo.BestEffort,
					ResourceParameters: &ResourceConfig{},
				},
			}

			m.setMemoryQoS(logger, qosConfigs)

			assert.Equal(t, tc.expectedGuaranteed, qosConfigs[v1.PodQOSGuaranteed].ResourceParameters.Unified[Cgroup2MemoryMin])
			assert.Equal(t, tc.expectedBurstable, qosConfigs[v1.PodQOSBurstable].ResourceParameters.Unified[Cgroup2MemoryLow])
		})
	}
}

// TestQoSContainerCgroupWithMemoryReservationPolicyNone 校验 None 策略下
// memory.min/memory.low 被显式归零。
func TestQoSContainerCgroupWithMemoryReservationPolicyNone(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	fakeCM := &fakeCgroupManager{}
	cgroupRoot := ParseCgroupfsToCgroupName("/")
	cgroupRoot = NewCgroupName(cgroupRoot, defaultNodeAllocatableCgroupName)
	m := &qosContainerManagerImpl{
		cgroupManager:           fakeCM,
		cgroupRoot:              cgroupRoot,
		activePods:              activeTestPods,
		memoryReservationPolicy: kubeletconfig.NoneMemoryReservationPolicy,
		qosContainersInfo: QOSContainersInfo{
			Guaranteed: cgroupRoot,
			Burstable:  NewCgroupName(cgroupRoot, "burstable"),
			BestEffort: NewCgroupName(cgroupRoot, "besteffort"),
		},
	}

	// 策略为 None 时 memory.min 必须显式归零，清除之前可能残留的限制。
	qosConfigs := map[v1.PodQOSClass]*CgroupConfig{
		v1.PodQOSGuaranteed: {
			Name:               m.qosContainersInfo.Guaranteed,
			ResourceParameters: &ResourceConfig{},
		},
		v1.PodQOSBurstable: {
			Name:               m.qosContainersInfo.Burstable,
			ResourceParameters: &ResourceConfig{},
		},
		v1.PodQOSBestEffort: {
			Name:               m.qosContainersInfo.BestEffort,
			ResourceParameters: &ResourceConfig{},
		},
	}

	m.setMemoryQoS(logger, qosConfigs)

	assert.Equal(t, "0", qosConfigs[v1.PodQOSGuaranteed].ResourceParameters.Unified[Cgroup2MemoryMin])
	assert.Equal(t, "0", qosConfigs[v1.PodQOSBurstable].ResourceParameters.Unified[Cgroup2MemoryLow])
}

// fakeCgroupManager 用于满足 Start() 对可用 CgroupManager 的要求：
// 所有方法均为桩实现，使 Start() 能在不操作真实 cgroup 的情况下完成。
type fakeCgroupManager struct {
	mutex   sync.Mutex
	created []*CgroupConfig
	updates []*CgroupConfig
}

// Update() 是本测试的观察点：
// 记录每次更新的 cgroup 配置，供后续断言。
func (f *fakeCgroupManager) Update(logger klog.Logger, config *CgroupConfig) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	copiedConfig := *config
	f.updates = append(f.updates, &copiedConfig)
	return nil
}

// Create() 必须成功，Start() 才能构造 QoS cgroup。
// 本测试不对 Create() 的行为做断言。
func (f *fakeCgroupManager) Create(l klog.Logger, config *CgroupConfig) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	copiedConfig := *config
	f.created = append(f.created, &copiedConfig)
	return nil
}

// Destroy 为空实现：测试不关心 cgroup 销毁。
func (f *fakeCgroupManager) Destroy(l klog.Logger, config *CgroupConfig) error { return nil }

// Validate 为空实现：跳过真实 cgroup 校验。
func (f *fakeCgroupManager) Validate(name CgroupName) error { return nil }

// Exists 固定返回 false，避免走真实 cgroup 查询路径。
func (f *fakeCgroupManager) Exists(name CgroupName) bool { return false }

// Name 返回 cgroup 的 cgroupfs 路径。
func (f *fakeCgroupManager) Name(name CgroupName) string { return name.ToCgroupfs() }

// CgroupName 把 cgroupfs 路径解析为 CgroupName。
func (f *fakeCgroupManager) CgroupName(name string) CgroupName {
	return ParseCgroupfsToCgroupName(name)
}

// Pids 固定返回 nil，测试不读取进程列表。
func (f *fakeCgroupManager) Pids(logger klog.Logger, name CgroupName) []int { return nil }

// ReduceCPULimits 为空实现。
func (f *fakeCgroupManager) ReduceCPULimits(logger klog.Logger, cgroupName CgroupName) error {
	return nil
}

// MemoryUsage 固定返回 0，模拟空 cgroup。
func (f *fakeCgroupManager) MemoryUsage(name CgroupName) (int64, error) { return int64(0), nil }

// GetCgroupConfig 返回 nil，测试不依赖读取真实配置。
func (f *fakeCgroupManager) GetCgroupConfig(name CgroupName, resource v1.ResourceName) (*ResourceConfig, error) {
	return nil, nil
}

// SetCgroupConfig 为空实现。
func (f *fakeCgroupManager) SetCgroupConfig(logger klog.Logger, name CgroupName, resourceConfig *ResourceConfig) error {
	return nil
}

// Version 固定返回 1，模拟 cgroup v1。
func (f *fakeCgroupManager) Version() int { return 1 }

// TestQOSCPUConfigUpdate 验证 UpdateCgroups() 依据当前活跃 Pod
// 为各 QoS 级别计算并更新正确的 CPU shares。
func TestQOSCPUConfigUpdate(t *testing.T) {

	// Guaranteed QoS 的 shares 固定（requests == limits），不会重算；
	// BestEffort 固定使用 MinShares，只有 Burstable 的 CPU shares
	// 取决于 Burstable CPU 请求的聚合值。

	tests := []struct {
		name                       string
		testPods                   ActivePodsFunc
		expectedBurstableCPUShares uint64 // 只会重算 Burstable QoS 的 CPU shares
	}{
		{
			name: "guaranteed-pods-only",
			testPods: func() []*v1.Pod {
				return []*v1.Pod{

					{
						ObjectMeta: metav1.ObjectMeta{
							UID:       types.UID(uuid.NewUUID()),
							Name:      "guaranteed-pod",
							Namespace: "test",
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "foo",
									Image: "busybox",
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("1"),
											v1.ResourceMemory: resource.MustParse("128Mi"),
										},
										Limits: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("1"),
											v1.ResourceMemory: resource.MustParse("128Mi"),
										},
									},
								},
							},
						},
					},
				}
			},
			// kubelet 无论该类 Pod 是否存在都会创建 QoS cgroup，
			// 因此 Burstable 类仍会得到 MinShares。
			expectedBurstableCPUShares: MinShares,
		},
		{
			name: "burstable-pods-only",
			testPods: func() []*v1.Pod {
				return []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							UID:       types.UID(uuid.NewUUID()),
							Name:      "burstable-pod",
							Namespace: "test",
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "foo",
									Image: "busybox",
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("1"),
											v1.ResourceMemory: resource.MustParse("128Mi"),
										},
										Limits: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("2"),
											v1.ResourceMemory: resource.MustParse("256Mi"),
										},
									},
								},
							},
						},
					},
				}
			},
			// 1 个 CPU 请求对应 1024 CPU shares。
			expectedBurstableCPUShares: 1024,
		},
		{
			name: "besteffort-pods-only",
			testPods: func() []*v1.Pod {
				return []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							UID:       types.UID(uuid.NewUUID()),
							Name:      "besteffort-pod",
							Namespace: "test",
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "foo",
									Image: "busybox",
								},
							},
						},
					},
				}
			},
			expectedBurstableCPUShares: MinShares,
		},
		{
			name: "guaranteed-and-burstable-pods",
			testPods: func() []*v1.Pod {
				return []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							UID:       types.UID(uuid.NewUUID()),
							Name:      "guaranteed-pod",
							Namespace: "test",
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "foo",
									Image: "busybox",
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("1"),
											v1.ResourceMemory: resource.MustParse("128Mi"),
										},
										Limits: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("1"),
											v1.ResourceMemory: resource.MustParse("128Mi"),
										},
									},
								},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							UID:       types.UID(uuid.NewUUID()),
							Name:      "burstable-pod",
							Namespace: "test",
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "foo",
									Image: "busybox",
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("1"),
											v1.ResourceMemory: resource.MustParse("128Mi"),
										},
										Limits: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("2"),
											v1.ResourceMemory: resource.MustParse("256Mi"),
										},
									},
								},
							},
						},
					},
				}
			},
			expectedBurstableCPUShares: 1024,
		},
		{
			name: "besteffort-and-burstable-pods",
			testPods: func() []*v1.Pod {
				return []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							UID:       types.UID(uuid.NewUUID()),
							Name:      "besteffort-pod",
							Namespace: "test",
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "foo",
									Image: "busybox",
								},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							UID:       types.UID(uuid.NewUUID()),
							Name:      "burstable-pod",
							Namespace: "test",
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "foo",
									Image: "busybox",
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("1"),
											v1.ResourceMemory: resource.MustParse("128Mi"),
										},
										Limits: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("2"),
											v1.ResourceMemory: resource.MustParse("256Mi"),
										},
									},
								},
							},
						},
					},
				}
			},
			expectedBurstableCPUShares: 1024,
		},
	}

	for _, testCase := range tests {

		t.Run(testCase.name, func(t *testing.T) {

			logger, ctx := ktesting.NewTestContext(t)

			testContainerManager, err := createTestQOSContainerManager(logger)
			if err != nil {
				t.Fatalf("Unable to create Test Qos Container Manager: %s", err)
				return
			}

			// 用 fake 替换真实 cgroupManager，隔离对宿主 cgroupfs 的写入。
			fakecgroupManager := &fakeCgroupManager{}
			testContainerManager.cgroupManager = fakecgroupManager

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			err = testContainerManager.Start(ctx, func() v1.ResourceList { return v1.ResourceList{} }, testCase.testPods)

			if err != nil {
				t.Fatalf("Start() failed: %s", err)
			}

			// UpdateCgroups() 每次调用都应更新全部 QoS cgroup，
			// 依据当前活跃 Pod 集合重新计算。
			err = testContainerManager.UpdateCgroups(logger)
			if err != nil {
				t.Fatalf("Error in UpdateCgroups(): %s", err)
			}
			cancel()

			// UpdateCgroups() 也可能正由 Start() 启动的后台协调循环执行。
			// 反复取快照，直到观察到包含全部 QoS cgroup 更新的一致状态。
			//
			// UpdateCgroups() 是同步且很快完成的，有限次重试足够：
			// 这样既避免测试抖动，又不会无限等待。
			maxRetryAttempts := 5

			var (
				foundBurstable  bool
				foundBestEffort bool
				foundGuaranteed bool
			)

			for i := 0; i < maxRetryAttempts; i++ {

				// 这些标志用于确认 UpdateCgroups() 是否更新了
				// 所有 QoS 类（cgroup）的 CPU shares。
				foundBurstable = false
				foundBestEffort = false
				foundGuaranteed = false

				// Start() 会启动后台 UpdateCgroups() goroutine，它可能仍在运行；
				// 先取快照，避免把后台更新误当成显式调用产生的更新。
				fakecgroupManager.mutex.Lock()
				updates := append([]*CgroupConfig(nil), fakecgroupManager.updates...)
				fakecgroupManager.mutex.Unlock()

				for _, config := range updates {

					if strings.HasSuffix(config.Name.ToCgroupfs(), "burstable") {
						foundBurstable = true
						if *config.ResourceParameters.CPUShares != testCase.expectedBurstableCPUShares {
							t.Fatalf("Expected CPU Shares for Burstable: %d Got: %d", testCase.expectedBurstableCPUShares, *config.ResourceParameters.CPUShares)
						}
						continue
					}

					if strings.HasSuffix(config.Name.ToCgroupfs(), "besteffort") {
						foundBestEffort = true
						if *config.ResourceParameters.CPUShares != MinShares {
							t.Fatalf("Expected CPU Shares for BestEffort: %d Got: %d", MinShares, *config.ResourceParameters.CPUShares)
						}
						continue
					}

					if config.Name.ToCgroupfs() == testContainerManager.cgroupRoot.ToCgroupfs() {
						foundGuaranteed = true
						if config.ResourceParameters != nil && config.ResourceParameters.CPUShares != nil {
							t.Fatalf("Expected CPU Shares for Guaranteed: <nil>, Got: %d", *config.ResourceParameters.CPUShares)
						}
					}
				}

				if foundBurstable && foundBestEffort && foundGuaranteed {
					break
				}

				time.Sleep(time.Millisecond * 10)
			}

			if !foundBurstable || !foundBestEffort || !foundGuaranteed {

				t.Fatalf(
					"did not observe all QoS cgroup updates after %d retries (guaranteed=%v, burstable=%v, besteffort=%v)",
					maxRetryAttempts, foundGuaranteed, foundBurstable, foundBestEffort,
				)
			}
		})
	}
}
