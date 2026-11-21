//go:build linux

/*
Copyright 2015 The Kubernetes Authors.

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
	"errors"
	"os"
	"path"
	"testing"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/opencontainers/cgroups"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	cadvisortest "k8s.io/kubernetes/pkg/kubelet/cadvisor/testing"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	cmqos "k8s.io/kubernetes/pkg/kubelet/cm/qos"
	"k8s.io/mount-utils"
	"k8s.io/utils/cpuset"
)

// mockCPUAllocationReader 用预置 map 模拟 cpumanager.Manager 的独占 CPU
// 与隔离级别查询。
type mockCPUAllocationReader struct {
	cpumanager.Manager
	sets            map[string]cpuset.CPUSet
	isolationLevels map[string]cmqos.ResourceIsolationLevel
}

// GetExclusiveCPUs 按 podUID/containerName 返回预置的独占 CPU 集合，未命中返回空集合。
func (m *mockCPUAllocationReader) GetExclusiveCPUs(podUID, containerName string) cpuset.CPUSet {
	key := podUID + "/" + containerName
	if cset, ok := m.sets[key]; ok {
		return cset
	}
	return cpuset.New()
}

// GetResourceIsolationLevel 返回预置的隔离级别；未显式设置时按是否存在独占 CPU 推断。
func (m *mockCPUAllocationReader) GetResourceIsolationLevel(pod *v1.Pod, container *v1.Container) cmqos.ResourceIsolationLevel {
	key := string(pod.UID) + "/" + container.Name
	if level, ok := m.isolationLevels[key]; ok {
		return level
	}
	// 未显式设置隔离级别时，按是否存在独占 CPU 推断，作为合理默认值；
	// TestContainerHasExclusiveCPUs 会显式设置 isolationLevels。
	if _, ok := m.sets[key]; ok {
		return cmqos.ResourceIsolationContainer
	}
	return cmqos.ResourceIsolationHost
}

// fakeContainerMgrMountInt 返回挂载了 cpuset/cpu/cpuacct/memory 子系统的 fake mounter。
func fakeContainerMgrMountInt() mount.Interface {
	return mount.NewFakeMounter(
		[]mount.MountPoint{
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuset"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpu"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuacct"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "memory"},
			},
		})
}

// TestCgroupMountValidationSuccess 校验合法 cgroup 挂载能通过系统要求检查。
func TestCgroupMountValidationSuccess(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	f, err := validateSystemRequirements(logger, fakeContainerMgrMountInt())
	assert.NoError(t, err)
	// cgroup v2 统一层级默认支持硬限流；v1 则取决于 cpu 子系统的挂载选项。
	if cgroups.IsCgroup2UnifiedMode() {
		assert.True(t, f.cpuHardcapping, "cpu hardcapping is expected to be enabled")
	} else {
		assert.False(t, f.cpuHardcapping, "cpu hardcapping is expected to be disabled")
	}
}

// TestCgroupMountValidationMemoryMissing 校验缺少 memory 子系统时校验失败（仅 cgroup v1）。
func TestCgroupMountValidationMemoryMissing(t *testing.T) {
	if cgroups.IsCgroup2UnifiedMode() {
		t.Skip("skipping cgroup v1 test on a cgroup v2 system")
	}
	logger, _ := ktesting.NewTestContext(t)
	mountInt := mount.NewFakeMounter(
		[]mount.MountPoint{
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuset"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpu"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuacct"},
			},
		})
	_, err := validateSystemRequirements(logger, mountInt)
	assert.Error(t, err)
}

// TestCgroupMountValidationMultipleSubsystem 校验单个挂载点包含多个子系统时仍能正确解析。
func TestCgroupMountValidationMultipleSubsystem(t *testing.T) {
	if cgroups.IsCgroup2UnifiedMode() {
		t.Skip("skipping cgroup v1 test on a cgroup v2 system")
	}
	logger, _ := ktesting.NewTestContext(t)
	mountInt := mount.NewFakeMounter(
		[]mount.MountPoint{
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuset", "memory"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpu"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuacct"},
			},
		})
	_, err := validateSystemRequirements(logger, mountInt)
	assert.NoError(t, err)
}

// TestGetCpuWeight 校验 cgroup v1 CPU shares 到 cgroup v2 cpu.weight 的换算与上限。
func TestGetCpuWeight(t *testing.T) {
	assert.Equal(t, uint64(0), getCPUWeight(nil))

	v := uint64(2)
	assert.Equal(t, uint64(1), getCPUWeight(&v))

	v = uint64(262144)
	assert.Equal(t, uint64(10000), getCPUWeight(&v))

	v = uint64(1000000000)
	assert.Equal(t, uint64(10000), getCPUWeight(&v))
}

// TestSoftRequirementsValidationSuccess 校验 cpu.cfs_* 文件可写时 cpuHardcapping 为 true。
func TestSoftRequirementsValidationSuccess(t *testing.T) {
	if cgroups.IsCgroup2UnifiedMode() {
		t.Skip("skipping cgroup v1 test on a cgroup v2 system")
	}
	logger, _ := ktesting.NewTestContext(t)
	req := require.New(t)
	tempDir, err := os.MkdirTemp("", "")
	req.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tempDir), "unable to remove dir %s", tempDir)
	})
	req.NoError(os.WriteFile(path.Join(tempDir, "cpu.cfs_period_us"), []byte("0"), os.ModePerm))
	req.NoError(os.WriteFile(path.Join(tempDir, "cpu.cfs_quota_us"), []byte("0"), os.ModePerm))
	mountInt := mount.NewFakeMounter(
		[]mount.MountPoint{
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuset"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpu"},
				Path:   tempDir,
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuacct", "memory"},
			},
		})
	f, err := validateSystemRequirements(logger, mountInt)
	assert.NoError(t, err)
	assert.True(t, f.cpuHardcapping, "cpu hardcapping is expected to be enabled")
}

// TestGetCapacity 校验 ephemeral-storage 容量的来源优先级，以及 cAdvisor 异常时的回退。
func TestGetCapacity(t *testing.T) {
	ephemeralStorageFromCapacity := int64(2000)
	ephemeralStorageFromCadvisor := int64(8000)

	logger, _ := ktesting.NewTestContext(t)

	mockCadvisor := cadvisortest.NewMockInterface(t)
	rootfs := cadvisorapi.FsInfo{
		Capacity: 8000,
	}
	mockCadvisor.EXPECT().RootFsInfo().Return(rootfs, nil)
	mockCadvisorError := cadvisortest.NewMockInterface(t)
	mockCadvisorError.EXPECT().RootFsInfo().Return(cadvisorapi.FsInfo{}, errors.New("Unable to get rootfs data from cAdvisor interface"))
	cases := []struct {
		name                                 string
		cm                                   *containerManagerImpl
		expectedResourceQuantity             *resource.Quantity
		expectedNoEphemeralStorage           bool
		disablelocalStorageCapacityIsolation bool
	}{
		{
			name: "capacity property has ephemeral-storage",
			cm: &containerManagerImpl{
				cadvisorInterface: mockCadvisor,
				capacity: v1.ResourceList{
					v1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStorageFromCapacity, resource.BinarySI),
				},
			},
			expectedResourceQuantity:   resource.NewQuantity(ephemeralStorageFromCapacity, resource.BinarySI),
			expectedNoEphemeralStorage: false,
		},
		{
			name: "capacity property does not have ephemeral-storage",
			cm: &containerManagerImpl{
				cadvisorInterface: mockCadvisor,
				capacity:          v1.ResourceList{},
			},
			expectedResourceQuantity:   resource.NewQuantity(ephemeralStorageFromCadvisor, resource.BinarySI),
			expectedNoEphemeralStorage: false,
		},
		{
			name: "capacity property does not have ephemeral-storage, error from rootfs",
			cm: &containerManagerImpl{
				cadvisorInterface: mockCadvisorError,
				capacity:          v1.ResourceList{},
			},
			expectedNoEphemeralStorage: true,
		},
		{
			name: "capacity property does not have ephemeral-storage, cadvisor interface is nil",
			cm: &containerManagerImpl{
				cadvisorInterface: nil,
				capacity:          v1.ResourceList{},
			},
			expectedNoEphemeralStorage: true,
		},
		{
			name: "capacity property has ephemeral-storage, but localStorageCapacityIsolation is disabled",
			cm: &containerManagerImpl{
				cadvisorInterface: mockCadvisor,
				capacity: v1.ResourceList{
					v1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStorageFromCapacity, resource.BinarySI),
				},
			},
			expectedResourceQuantity:             resource.NewQuantity(ephemeralStorageFromCapacity, resource.BinarySI),
			expectedNoEphemeralStorage:           true,
			disablelocalStorageCapacityIsolation: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 关闭 localStorageCapacityIsolation 时不应返回 ephemeral-storage。
			ret := c.cm.GetCapacity(logger, !c.disablelocalStorageCapacityIsolation)
			if v, exists := ret[v1.ResourceEphemeralStorage]; !exists {
				if !c.expectedNoEphemeralStorage {
					t.Errorf("did not get any ephemeral storage data")
				}
			} else {
				if v.Value() != c.expectedResourceQuantity.Value() {
					t.Errorf("got unexpected %s value, expected %d, got %d", v1.ResourceEphemeralStorage, c.expectedResourceQuantity.Value(), v.Value())
				}
			}
		})
	}
}

// TestNewPodContainerManager 校验按 CgroupsPerQOS 与 cgroup 驱动选择
// Pod 容器管理器实现。
func TestNewPodContainerManager(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	info := QOSContainersInfo{
		Guaranteed: CgroupName{"guaranteed"},
		BestEffort: CgroupName{"besteffort"},
		Burstable:  CgroupName{"burstable"},
	}
	QosEnabled := NodeConfig{
		CgroupsPerQOS: true,
	}
	QosDisabled := NodeConfig{
		CgroupsPerQOS: false,
	}

	cases := []struct {
		name string
		cm   *containerManagerImpl
	}{
		{
			name: "CgroupsPerQOS is disabled, return *podContainerManagerNoop",
			cm: &containerManagerImpl{
				qosContainerManager: &qosContainerManagerImpl{
					qosContainersInfo: info,
					cgroupManager:     NewCgroupManager(logger, &CgroupSubsystems{}, ""),
				},

				NodeConfig: QosDisabled,
			},
		},
		{
			name: "CgroupsPerQOS is enabled, return *podContainerManagerImpl",
			cm: &containerManagerImpl{
				qosContainerManager: &qosContainerManagerImpl{
					qosContainersInfo: info,
					cgroupManager:     NewCgroupManager(logger, &CgroupSubsystems{}, ""),
				},

				NodeConfig: QosEnabled,
			},
		},
		{
			name: "CgroupsPerQOS is enabled, use systemd",
			cm: &containerManagerImpl{
				qosContainerManager: &qosContainerManagerImpl{
					qosContainersInfo: info,
					cgroupManager:     NewCgroupManager(logger, &CgroupSubsystems{}, "systemd"),
				},

				NodeConfig: QosEnabled,
			},
		},
		{
			name: "CgroupsPerQOS is disabled, use systemd",
			cm: &containerManagerImpl{
				qosContainerManager: &qosContainerManagerImpl{
					qosContainersInfo: info,
					cgroupManager:     NewCgroupManager(logger, &CgroupSubsystems{}, "systemd"),
				},

				NodeConfig: QosDisabled,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			// 通过具体类型断言区分 noop 与真实实现，再校验字段透传。
			pcm := c.cm.NewPodContainerManager()
			if c.cm.NodeConfig.CgroupsPerQOS {
				assert.IsType(t, &podContainerManagerImpl{}, pcm)
				got := pcm.(*podContainerManagerImpl)
				assert.Equal(t, c.cm.subsystems, got.subsystems)
				assert.Equal(t, c.cm.cgroupManager, got.cgroupManager)
				assert.Equal(t, c.cm.PodPidsLimit, got.podPidsLimit)
				assert.Equal(t, c.cm.EnforceCPULimits, got.enforceCPULimits)
				assert.Equal(t, uint64(c.cm.CPUCFSQuotaPeriod/time.Microsecond), got.cpuCFSQuotaPeriod)

			} else {
				assert.IsType(t, &podContainerManagerNoop{}, pcm)
				got := pcm.(*podContainerManagerNoop)
				assert.Equal(t, c.cm.cgroupRoot, got.cgroupRoot)
			}
		})
	}
}

// TestContainerHasExclusiveCPUs 校验容器独占 CPU 判定，
// 覆盖 Pod 级资源与不同隔离级别的组合。
func TestContainerHasExclusiveCPUs(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	guaranteedQOSResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}
	burstableQOSResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}
	guaranteedQOSResourcesNonIntegerCPUNotExclusive := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("500m"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("500m"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}

	testCases := []struct {
		name                            string
		pod                             *v1.Pod
		containerName                   string
		cpuSets                         map[string]cpuset.CPUSet
		isolationLevels                 map[string]cmqos.ResourceIsolationLevel
		expectExclusiveCPUs             bool
		podLevelResourceManagersEnabled bool
	}{
		{
			name: "No pod-level resources, Guaranteed container, has exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			expectExclusiveCPUs:             true,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Guaranteed container, Integer CPUs, exclusive CPUs assigned",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationContainer},
			expectExclusiveCPUs:             true,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Guaranteed container, Integer CPUs, no exclusive CPUs assigned",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Guaranteed container, non-integer CPU",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResourcesNonIntegerCPUNotExclusive}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationPod},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Burstable container, has exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: burstableQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationPod},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, BestEffort container, has exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1"}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationPod},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Guaranteed container, Integer CPUs, no exclusive CPUs assigned",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: false,
		},
		{
			name: "Pod-level resources, BestEffort container",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1"}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Pod 级资源路径需要同时打开两个门控。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourceManagersEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResourceManagers, tc.podLevelResourceManagersEnabled)

			mockReader := &mockCPUAllocationReader{
				sets:            tc.cpuSets,
				isolationLevels: tc.isolationLevels,
			}
			cm := &containerManagerImpl{
				cpuManager: mockReader,
			}

			var targetContainer *v1.Container
			for i := range tc.pod.Spec.Containers {
				if tc.pod.Spec.Containers[i].Name == tc.containerName {
					targetContainer = &tc.pod.Spec.Containers[i]
					break
				}
			}
			require.NotNil(t, targetContainer, "container %s not found in pod spec", tc.containerName)

			result := cm.ContainerHasExclusiveCPUs(logger, tc.pod, targetContainer)
			assert.Equal(t, tc.expectExclusiveCPUs, result)
		})
	}
}

// TestPodHasExclusiveCPUs 校验 Pod 是否存在独占 CPU，
// 覆盖 init 容器与不同隔离级别的组合。
func TestPodHasExclusiveCPUs(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		name                            string
		pod                             *v1.Pod
		cpuSets                         map[string]cpuset.CPUSet
		isolationLevels                 map[string]cmqos.ResourceIsolationLevel
		expectExclusiveCPUs             bool
		podLevelResourceManagersEnabled bool
	}{
		{
			name: "No exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets:             map[string]cpuset.CPUSet{},
			expectExclusiveCPUs: false,
		},
		{
			name: "One container with exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/c1": cpuset.New(1),
			},
			expectExclusiveCPUs: true,
		},
		{
			name: "Init container with exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{Name: "init1"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/init1": cpuset.New(1),
			},
			expectExclusiveCPUs: true,
		},
		{
			name: "PodLevelResourceManagers enabled, One container with exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/c1": cpuset.New(1),
			},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationContainer},
			expectExclusiveCPUs:             true,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "PodLevelResourceManagers enabled, One container in pod shared pool (ResourceIsolationPod)",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/c1": cpuset.New(1),
			},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationPod},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "PodLevelResourceManagers enabled, Mixed containers (one exclusive, one shared)",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
					Containers: []v1.Container{
						{Name: "c1"},
						{Name: "c2"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/c1": cpuset.New(1),
				"pod1/c2": cpuset.New(2),
			},
			isolationLevels: map[string]cmqos.ResourceIsolationLevel{
				"pod1/c1": cmqos.ResourceIsolationContainer,
				"pod1/c2": cmqos.ResourceIsolationPod,
			},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "PodLevelResourceManagers disabled, Pod Level Resources Only (No Exclusive CPUs)",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets:                         map[string]cpuset.CPUSet{},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Pod 级资源路径需要同时打开两个门控。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourceManagersEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResourceManagers, tc.podLevelResourceManagersEnabled)

			mockReader := &mockCPUAllocationReader{
				sets:            tc.cpuSets,
				isolationLevels: tc.isolationLevels,
			}
			cm := &containerManagerImpl{
				cpuManager: mockReader,
			}

			result := cm.PodHasExclusiveCPUs(logger, tc.pod)
			assert.Equal(t, tc.expectExclusiveCPUs, result)
		})
	}
}
