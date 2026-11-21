/*
Copyright 2017 The Kubernetes Authors.

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

package stats

import (
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	cadvisorfs "github.com/google/cadvisor/lib/fs"
	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	critest "k8s.io/cri-api/pkg/apis/testing"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubelettypes "k8s.io/kubelet/pkg/types"
	"k8s.io/kubernetes/pkg/features"
	cadvisortest "k8s.io/kubernetes/pkg/kubelet/cadvisor/testing"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubecontainertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/pkg/kubelet/kuberuntime"
	kubepodtest "k8s.io/kubernetes/pkg/kubelet/pod/testing"
	serverstats "k8s.io/kubernetes/pkg/kubelet/server/stats"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

const (
	offsetInodeUsage = iota
	offsetUsage
)

const (
	// offsetCRI 用于将 CRI 统计与 cadvisor 统计区分开
	offsetCRI = 1000
)

const (
	seedRoot       = 0
	seedKubelet    = 200
	seedMisc       = 300
	seedSandbox0   = 1000
	seedContainer0 = 2000
	seedSandbox1   = 3000
	seedContainer1 = 4000
	seedContainer2 = 5000
	seedSandbox2   = 6000
	seedContainer3 = 7000
	seedSandbox3   = 8000
)

const (
	pName0 = "pod0"
	pName1 = "pod1"
	pName2 = "pod2"
)

const (
	cName0 = "container0-name"
	cName1 = "container1-name"
	cName2 = "container2-name"
	cName3 = "container3-name"
	cName5 = "container5-name"
	cName6 = "container6-name"
	cName7 = "container7-name"
	cName8 = "container8-name"
	cName9 = "container9-name"
)

const testPodLogDirectory = "/var/log/kube/pods/" // Use non-default path to ensure stats are collected properly

// TestCRIListPodStats 校验 ListPodStats 能合并 CRI 与 cadvisor 统计，
// 覆盖运行中、已终止的 sandbox 与容器、日志统计以及 PSI 等场景。
func TestCRIListPodStats(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletPSI, true)
	tCtx := ktesting.Init(t)
	var (
		imageFsMountpoint = "/test/mount/point"
		unknownMountpoint = "/unknown/mount/point"
		imageFsInfo       = getTestFsInfo(2000)
		rootFsInfo        = getTestFsInfo(1000)

		sandbox0           = makeFakePodSandbox("sandbox0-name", "sandbox0-uid", "sandbox0-ns", false)
		sandbox0Cgroup     = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox0.PodSandboxStatus.Metadata.Uid))
		container0         = makeFakeContainer(sandbox0, cName0, 0, false)
		containerStats0    = makeFakeContainerStats(container0, imageFsMountpoint)
		containerLogStats0 = makeFakeLogStats(1000)
		container1         = makeFakeContainer(sandbox0, cName1, 0, false)
		containerStats1    = makeFakeContainerStats(container1, unknownMountpoint)
		containerLogStats1 = makeFakeLogStats(2000)

		sandbox1           = makeFakePodSandbox("sandbox1-name", "sandbox1-uid", "sandbox1-ns", false)
		sandbox1Cgroup     = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox1.PodSandboxStatus.Metadata.Uid))
		container2         = makeFakeContainer(sandbox1, cName2, 0, false)
		containerStats2    = makeFakeContainerStats(container2, imageFsMountpoint)
		containerLogStats2 = makeFakeLogStats(3000)

		sandbox2           = makeFakePodSandbox("sandbox2-name", "sandbox2-uid", "sandbox2-ns", false)
		sandbox2Cgroup     = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox2.PodSandboxStatus.Metadata.Uid))
		container3         = makeFakeContainer(sandbox2, cName3, 0, true)
		containerStats3    = makeFakeContainerStats(container3, imageFsMountpoint)
		container4         = makeFakeContainer(sandbox2, cName3, 1, false)
		containerStats4    = makeFakeContainerStats(container4, imageFsMountpoint)
		containerLogStats4 = makeFakeLogStats(4000)

		// 运行中的 pod，含一个已终止容器和一个运行中容器
		sandbox3           = makeFakePodSandbox("sandbox3-name", "sandbox3-uid", "sandbox3-ns", false)
		sandbox3Cgroup     = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox3.PodSandboxStatus.Metadata.Uid))
		container5         = makeFakeContainer(sandbox3, cName5, 0, true)
		containerStats5    = makeFakeContainerStats(container5, imageFsMountpoint)
		containerLogStats5 = makeFakeLogStats(5000)
		container8         = makeFakeContainer(sandbox3, cName8, 0, false)
		containerStats8    = makeFakeContainerStats(container8, imageFsMountpoint)
		containerLogStats8 = makeFakeLogStats(6000)

		// 已终止的 pod sandbox
		sandbox4        = makeFakePodSandbox("sandbox1-name", "sandbox1-uid", "sandbox1-ns", true)
		container6      = makeFakeContainer(sandbox4, cName6, 0, true)
		containerStats6 = makeFakeContainerStats(container6, imageFsMountpoint)

		// 已终止的 pod
		sandbox5        = makeFakePodSandbox("sandbox1-name", "sandbox5-uid", "sandbox1-ns", true)
		container7      = makeFakeContainer(sandbox5, cName7, 0, true)
		containerStats7 = makeFakeContainerStats(container7, imageFsMountpoint)

		podLogName0  = "pod-log-0"
		podLogName1  = "pod-log-1"
		podLogStats0 = makeFakeLogStats(5000)
		podLogStats1 = makeFakeLogStats(6000)
	)

	var (
		mockCadvisor       = cadvisortest.NewMockInterface(t)
		mockPodManager     = new(kubepodtest.MockManager)
		resourceAnalyzer   = new(fakeResourceAnalyzer)
		fakeRuntimeService = critest.NewFakeRuntimeService()
		fakeImageService   = critest.NewFakeImageService()
	)

	infos := map[string]cadvisorapi.ContainerInfo{
		"/":                           getTestContainerInfo(seedRoot, "", "", ""),
		"/kubelet":                    getTestContainerInfo(seedKubelet, "", "", ""),
		"/system":                     getTestContainerInfo(seedMisc, "", "", ""),
		sandbox0.PodSandboxStatus.Id:  getTestContainerInfo(seedSandbox0, pName0, sandbox0.PodSandboxStatus.Metadata.Namespace, kubelettypes.PodInfraContainerName),
		sandbox0Cgroup:                getTestContainerInfo(seedSandbox0, "", "", ""),
		container0.ContainerStatus.Id: getTestContainerInfo(seedContainer0, pName0, sandbox0.PodSandboxStatus.Metadata.Namespace, cName0),
		container1.ContainerStatus.Id: getTestContainerInfo(seedContainer1, pName0, sandbox0.PodSandboxStatus.Metadata.Namespace, cName1),
		sandbox1.PodSandboxStatus.Id:  getTestContainerInfo(seedSandbox1, pName1, sandbox1.PodSandboxStatus.Metadata.Namespace, kubelettypes.PodInfraContainerName),
		sandbox1Cgroup:                getTestContainerInfo(seedSandbox1, "", "", ""),
		container2.ContainerStatus.Id: getTestContainerInfo(seedContainer2, pName1, sandbox1.PodSandboxStatus.Metadata.Namespace, cName2),
		sandbox2.PodSandboxStatus.Id:  getTestContainerInfo(seedSandbox2, pName2, sandbox2.PodSandboxStatus.Metadata.Namespace, kubelettypes.PodInfraContainerName),
		sandbox2Cgroup:                getTestContainerInfo(seedSandbox2, "", "", ""),
		container4.ContainerStatus.Id: getTestContainerInfo(seedContainer3, pName2, sandbox2.PodSandboxStatus.Metadata.Namespace, cName3),
		sandbox3Cgroup:                getTestContainerInfo(seedSandbox3, "", "", ""),
	}

	options := cadvisorapi.RequestOptions{
		IdType:    cadvisorapi.TypeName,
		Count:     2,
		Recursive: true,
	}

	mockCadvisor.EXPECT().ContainerInfoV2("/", options).Return(infos, nil)
	mockCadvisor.EXPECT().RootFsInfo().Return(rootFsInfo, nil)
	mockCadvisor.EXPECT().GetDirFsInfo(imageFsMountpoint).Return(imageFsInfo, nil)
	mockCadvisor.EXPECT().GetDirFsInfo(unknownMountpoint).Return(cadvisorapi.FsInfo{}, cadvisorfs.ErrNoSuchDevice)

	fakeRuntimeService.SetFakeSandboxes([]*critest.FakePodSandbox{
		sandbox0, sandbox1, sandbox2, sandbox3, sandbox4, sandbox5,
	})
	fakeRuntimeService.SetFakeContainers([]*critest.FakeContainer{
		container0, container1, container2, container3, container4, container5, container6, container7, container8,
	})
	fakeRuntimeService.SetFakeContainerStats([]*runtimeapi.ContainerStats{
		containerStats0, containerStats1, containerStats2, containerStats3, containerStats4, containerStats5, containerStats6, containerStats7, containerStats8,
	})

	ephemeralVolumes := makeFakeVolumeStats([]string{"ephVolume1, ephVolumes2"})
	persistentVolumes := makeFakeVolumeStats([]string{"persisVolume1, persisVolumes2"})
	resourceAnalyzer.podVolumeStats = serverstats.PodVolumeStats{
		EphemeralVolumes:  ephemeralVolumes,
		PersistentVolumes: persistentVolumes,
	}

	fakeStats := map[string]*volume.Metrics{
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sandbox0-ns", "sandbox0-name", types.UID("sandbox0-uid"), cName0):               containerLogStats0,
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sandbox0-ns", "sandbox0-name", types.UID("sandbox0-uid"), cName1):               containerLogStats1,
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sandbox1-ns", "sandbox1-name", types.UID("sandbox1-uid"), cName2):               containerLogStats2,
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sandbox2-ns", "sandbox2-name", types.UID("sandbox2-uid"), cName3):               containerLogStats4,
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sandbox3-ns", "sandbox3-name", types.UID("sandbox3-uid"), cName5):               containerLogStats5,
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sandbox3-ns", "sandbox3-name", types.UID("sandbox3-uid"), cName8):               containerLogStats8,
		filepath.Join(kuberuntime.BuildPodLogsDirectory(testPodLogDirectory, "sandbox0-ns", "sandbox0-name", types.UID("sandbox0-uid")), podLogName0): podLogStats0,
		filepath.Join(kuberuntime.BuildPodLogsDirectory(testPodLogDirectory, "sandbox1-ns", "sandbox1-name", types.UID("sandbox1-uid")), podLogName1): podLogStats1,
	}

	fakeOS := &kubecontainertest.FakeOS{}
	fakeOS.ReadDirFn = func(path string) ([]os.DirEntry, error) {
		var dirEntries []os.DirEntry
		mockDE := kubecontainertest.NewMockDirEntry(t)
		switch path {
		case kuberuntime.BuildPodLogsDirectory(testPodLogDirectory, "sandbox0-ns", "sandbox0-name", types.UID("sandbox0-uid")):
			mockDE.EXPECT().Name().Return(podLogName0)
		case kuberuntime.BuildPodLogsDirectory(testPodLogDirectory, "sandbox1-ns", "sandbox1-name", types.UID("sandbox1-uid")):
			mockDE.EXPECT().Name().Return(podLogName1)
		default:
			return nil, nil
		}
		mockDE.EXPECT().IsDir().Return(false)
		dirEntries = append(dirEntries, mockDE)
		return dirEntries, nil
	}

	provider := NewCRIStatsProvider(
		mockCadvisor,
		resourceAnalyzer,
		mockPodManager,
		fakeRuntimeService,
		fakeImageService,
		NewFakeHostStatsProviderWithData(fakeStats, fakeOS),
		false,
		false,
		fakeContainerStatsProvider{},
	)

	stats, err := provider.ListPodStats(tCtx)
	assert := assert.New(t)
	assert.NoError(err)
	assert.Len(stats, 4)

	podStatsMap := make(map[statsapi.PodReference]statsapi.PodStats)
	for _, s := range stats {
		podStatsMap[s.PodRef] = s
	}

	p0 := podStatsMap[statsapi.PodReference{Name: "sandbox0-name", UID: "sandbox0-uid", Namespace: "sandbox0-ns"}]
	assert.Equal(sandbox0.CreatedAt, p0.StartTime.UnixNano())
	assert.Len(p0.Containers, 2)

	checkEphemeralStorageStats(assert, p0, ephemeralVolumes, []*runtimeapi.ContainerStats{containerStats0, containerStats1},
		[]*volume.Metrics{containerLogStats0, containerLogStats1}, podLogStats0)

	containerStatsMap := make(map[string]statsapi.ContainerStats)
	for _, s := range p0.Containers {
		containerStatsMap[s.Name] = s
	}

	c0 := containerStatsMap[cName0]
	assert.Equal(container0.CreatedAt, c0.StartTime.UnixNano())
	checkCRICPUAndMemoryStats(assert, c0, infos[container0.ContainerStatus.Id].Stats[0])
	checkCRIIOStats(assert, c0, infos[container0.ContainerStatus.Id].Stats[0])
	assert.Nil(c0.Accelerators)
	checkCRIRootfsStats(assert, c0, containerStats0, &imageFsInfo)
	checkCRILogsStats(assert, c0, &rootFsInfo, containerLogStats0)

	c1 := containerStatsMap[cName1]
	assert.Equal(container1.CreatedAt, c1.StartTime.UnixNano())
	checkCRICPUAndMemoryStats(assert, c1, infos[container1.ContainerStatus.Id].Stats[0])
	checkCRIIOStats(assert, c1, infos[container1.ContainerStatus.Id].Stats[0])
	assert.Nil(c0.Accelerators)
	checkCRIRootfsStats(assert, c1, containerStats1, nil)
	checkCRILogsStats(assert, c1, &rootFsInfo, containerLogStats1)
	checkCRINetworkStats(assert, p0.Network, infos[sandbox0.PodSandboxStatus.Id].Stats[0].Network)
	checkCRIPodCPUAndMemoryStats(assert, p0, infos[sandbox0Cgroup].Stats[0])
	checkCRIPodSwapStats(assert, p0, infos[sandbox0Cgroup].Stats[0])
	checkCRIPodIOStats(assert, p0, infos[sandbox0Cgroup].Stats[0])

	checkContainersSwapStats(t, p0, infos[container0.Id], infos[container1.Id])

	p1 := podStatsMap[statsapi.PodReference{Name: "sandbox1-name", UID: "sandbox1-uid", Namespace: "sandbox1-ns"}]
	assert.Equal(sandbox1.CreatedAt, p1.StartTime.UnixNano())
	assert.Len(p1.Containers, 1)

	checkEphemeralStorageStats(assert, p1, ephemeralVolumes, []*runtimeapi.ContainerStats{containerStats2},
		[]*volume.Metrics{containerLogStats2}, podLogStats1)
	c2 := p1.Containers[0]
	assert.Equal(cName2, c2.Name)
	assert.Equal(container2.CreatedAt, c2.StartTime.UnixNano())
	checkCRICPUAndMemoryStats(assert, c2, infos[container2.ContainerStatus.Id].Stats[0])
	checkCRIIOStats(assert, c2, infos[container2.ContainerStatus.Id].Stats[0])
	assert.Nil(c0.Accelerators)
	checkCRIRootfsStats(assert, c2, containerStats2, &imageFsInfo)
	checkCRILogsStats(assert, c2, &rootFsInfo, containerLogStats2)
	checkCRINetworkStats(assert, p1.Network, infos[sandbox1.PodSandboxStatus.Id].Stats[0].Network)
	checkCRIPodCPUAndMemoryStats(assert, p1, infos[sandbox1Cgroup].Stats[0])
	checkCRIPodSwapStats(assert, p1, infos[sandbox1Cgroup].Stats[0])
	checkCRIPodIOStats(assert, p1, infos[sandbox1Cgroup].Stats[0])

	checkContainersSwapStats(t, p1, infos[container2.Id])

	p2 := podStatsMap[statsapi.PodReference{Name: "sandbox2-name", UID: "sandbox2-uid", Namespace: "sandbox2-ns"}]
	assert.Equal(sandbox2.CreatedAt, p2.StartTime.UnixNano())
	assert.Len(p2.Containers, 1)

	checkEphemeralStorageStats(assert, p2, ephemeralVolumes, []*runtimeapi.ContainerStats{containerStats4},
		[]*volume.Metrics{containerLogStats4}, nil)

	c3 := p2.Containers[0]
	assert.Equal(cName3, c3.Name)
	assert.Equal(container4.CreatedAt, c3.StartTime.UnixNano())
	checkCRICPUAndMemoryStats(assert, c3, infos[container4.ContainerStatus.Id].Stats[0])
	checkCRIIOStats(assert, c3, infos[container4.ContainerStatus.Id].Stats[0])
	assert.Nil(c0.Accelerators)
	checkCRIRootfsStats(assert, c3, containerStats4, &imageFsInfo)

	checkCRILogsStats(assert, c3, &rootFsInfo, containerLogStats4)
	checkCRINetworkStats(assert, p2.Network, infos[sandbox2.PodSandboxStatus.Id].Stats[0].Network)
	checkCRIPodCPUAndMemoryStats(assert, p2, infos[sandbox2Cgroup].Stats[0])
	checkCRIPodSwapStats(assert, p2, infos[sandbox2Cgroup].Stats[0])
	checkCRIPodIOStats(assert, p2, infos[sandbox2Cgroup].Stats[0])

	checkContainersSwapStats(t, p2, infos[container4.Id])

	p3 := podStatsMap[statsapi.PodReference{Name: "sandbox3-name", UID: "sandbox3-uid", Namespace: "sandbox3-ns"}]
	assert.Equal(sandbox3.CreatedAt, p3.StartTime.UnixNano())
	assert.Len(p3.Containers, 1)

	c8 := p3.Containers[0]
	assert.Equal(cName8, c8.Name)
	assert.Equal(container8.CreatedAt, c8.StartTime.UnixNano())
	assert.NotNil(c8.CPU.Time)
	assert.NotNil(c8.Memory.Time)
	checkCRIPodCPUAndMemoryStats(assert, p3, infos[sandbox3Cgroup].Stats[0])
	checkCRIPodSwapStats(assert, p3, infos[sandbox3Cgroup].Stats[0])
	checkCRIPodIOStats(assert, p3, infos[sandbox3Cgroup].Stats[0])
}

// TestListPodStatsStrictlyFromCRI 校验在仅使用 CRI 统计的情况下，
// ListPodStats 仍能正确返回 pod 统计。
func TestListPodStatsStrictlyFromCRI(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletPSI, true)
	if runtime.GOOS == "windows" {
		// TODO: 修复该测试失败后移除跳过逻辑。
		t.Skip("Skip failing test on Windows.")
	}
	var (
		imageFsMountpoint = "/test/mount/point"
		unknownMountpoint = "/unknown/mount/point"
		imageFsInfo       = getTestFsInfo(2000)
		rootFsInfo        = getTestFsInfo(1000)

		// CRI 与 cadvisor 都返回统计的 pod，其统计以 CRI 为准
		sandbox0           = makeFakePodSandbox("sandbox0-name", "sandbox0-uid", "sandbox0-ns", false)
		sandbox0Cgroup     = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox0.PodSandboxStatus.Metadata.Uid))
		container0         = makeFakeContainer(sandbox0, cName0, 0, false)
		containerStats0    = makeFakeContainerStatsStrictlyFromCRI(seedContainer0, container0, imageFsMountpoint)
		containerLogStats0 = makeFakeLogStats(1000)
		container1         = makeFakeContainer(sandbox0, cName1, 0, false)
		containerStats1    = makeFakeContainerStatsStrictlyFromCRI(seedContainer1, container1, unknownMountpoint)
		containerLogStats1 = makeFakeLogStats(2000)
		sandboxPodStats0   = makeFakePodSandboxStatsStrictlyFromCRI(seedSandbox0, sandbox0, containerStats0, containerStats1)

		// CRI 返回统计、cadvisor 不返回统计的 pod，其统计以 CRI 为准
		sandbox1           = makeFakePodSandbox("sandbox1-name", "sandbox1-uid", "sandbox1-ns", false)
		sandbox1Cgroup     = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox1.PodSandboxStatus.Metadata.Uid))
		container2         = makeFakeContainer(sandbox1, cName2, 0, false)
		containerStats2    = makeFakeContainerStatsStrictlyFromCRI(seedContainer2, container2, imageFsMountpoint)
		containerLogStats2 = makeFakeLogStats(3000)
		sandboxPodStats1   = makeFakePodSandboxStatsStrictlyFromCRI(seedSandbox1, sandbox1, containerStats2)

		podLogName0  = "pod-log-0"
		podLogName1  = "pod-log-1"
		podLogStats0 = makeFakeLogStats(5000)
		podLogStats1 = makeFakeLogStats(6000)
	)
	var (
		mockCadvisor       = cadvisortest.NewMockInterface(t)
		mockPodManager     = new(kubepodtest.MockManager)
		resourceAnalyzer   = new(fakeResourceAnalyzer)
		fakeRuntimeService = critest.NewFakeRuntimeService()
		fakeImageService   = critest.NewFakeImageService()
	)
	infos := map[string]cadvisorapi.ContainerInfo{
		"/":                           getTestContainerInfo(seedRoot, "", "", ""),
		"/kubelet":                    getTestContainerInfo(seedKubelet, "", "", ""),
		"/system":                     getTestContainerInfo(seedMisc, "", "", ""),
		sandbox0.PodSandboxStatus.Id:  getTestContainerInfo(seedSandbox0, pName0, sandbox0.PodSandboxStatus.Metadata.Namespace, kubelettypes.PodInfraContainerName),
		sandbox0Cgroup:                getTestContainerInfo(seedSandbox0, "", "", ""),
		container0.ContainerStatus.Id: getTestContainerInfo(seedContainer0, pName0, sandbox0.PodSandboxStatus.Metadata.Namespace, cName0),
		container1.ContainerStatus.Id: getTestContainerInfo(seedContainer1, pName0, sandbox0.PodSandboxStatus.Metadata.Namespace, cName1),
	}

	exceptedContainerStatsMap := map[string]statsapi.ContainerStats{
		cName0: getCRIContainerStatsStrictlyFromCRI(seedContainer0, cName0),
		cName1: getCRIContainerStatsStrictlyFromCRI(seedContainer1, cName1),
		cName2: getCRIContainerStatsStrictlyFromCRI(seedContainer2, cName2),
	}

	prf0 := statsapi.PodReference{Name: "sandbox0-name", UID: "sandbox0-uid", Namespace: "sandbox0-ns"}
	prf1 := statsapi.PodReference{Name: "sandbox1-name", UID: "sandbox1-uid", Namespace: "sandbox1-ns"}

	exceptedPodStatsMap := map[statsapi.PodReference]statsapi.PodStats{
		prf0: getPodSandboxStatsStrictlyFromCRI(seedSandbox0, sandbox0),
		prf1: getPodSandboxStatsStrictlyFromCRI(seedSandbox1, sandbox1),
	}

	options := cadvisorapi.RequestOptions{
		IdType:    cadvisorapi.TypeName,
		Count:     2,
		Recursive: true,
	}
	mockCadvisor.EXPECT().ContainerInfoV2("/", options).Return(infos, nil)
	mockCadvisor.EXPECT().RootFsInfo().Return(rootFsInfo, nil)
	mockCadvisor.EXPECT().GetDirFsInfo(imageFsMountpoint).Return(imageFsInfo, nil)
	mockCadvisor.EXPECT().GetDirFsInfo(unknownMountpoint).Return(cadvisorapi.FsInfo{}, cadvisorfs.ErrNoSuchDevice)
	fakeRuntimeService.SetFakeSandboxes([]*critest.FakePodSandbox{
		sandbox0, sandbox1,
	})
	fakeRuntimeService.SetFakeContainers([]*critest.FakeContainer{
		container0, container1, container2,
	})
	fakeRuntimeService.SetFakeContainerStats([]*runtimeapi.ContainerStats{
		containerStats0, containerStats1, containerStats2,
	})

	fakeRuntimeService.SetFakePodSandboxStats([]*runtimeapi.PodSandboxStats{
		sandboxPodStats0, sandboxPodStats1,
	})

	ephemeralVolumes := makeFakeVolumeStats([]string{"ephVolume1, ephVolumes2"})
	persistentVolumes := makeFakeVolumeStats([]string{"persisVolume1, persisVolumes2"})
	resourceAnalyzer.podVolumeStats = serverstats.PodVolumeStats{
		EphemeralVolumes:  ephemeralVolumes,
		PersistentVolumes: persistentVolumes,
	}
	fakeStats := map[string]*volume.Metrics{
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sandbox0-ns", "sandbox0-name", types.UID("sandbox0-uid"), cName0):               containerLogStats0,
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sandbox0-ns", "sandbox0-name", types.UID("sandbox0-uid"), cName1):               containerLogStats1,
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sandbox1-ns", "sandbox1-name", types.UID("sandbox1-uid"), cName2):               containerLogStats2,
		filepath.Join(kuberuntime.BuildPodLogsDirectory(testPodLogDirectory, "sandbox0-ns", "sandbox0-name", types.UID("sandbox0-uid")), podLogName0): podLogStats0,
		filepath.Join(kuberuntime.BuildPodLogsDirectory(testPodLogDirectory, "sandbox1-ns", "sandbox1-name", types.UID("sandbox1-uid")), podLogName1): podLogStats1,
	}
	fakeOS := &kubecontainertest.FakeOS{}
	fakeOS.ReadDirFn = func(path string) ([]os.DirEntry, error) {
		var dirEntries []os.DirEntry
		mockDE := kubecontainertest.NewMockDirEntry(t)
		switch path {
		case kuberuntime.BuildPodLogsDirectory(testPodLogDirectory, "sandbox0-ns", "sandbox0-name", types.UID("sandbox0-uid")):
			mockDE.EXPECT().Name().Return(podLogName0)
		case kuberuntime.BuildPodLogsDirectory(testPodLogDirectory, "sandbox1-ns", "sandbox1-name", types.UID("sandbox1-uid")):
			mockDE.EXPECT().Name().Return(podLogName1)
		default:
			return nil, nil
		}
		mockDE.EXPECT().IsDir().Return(false)
		dirEntries = append(dirEntries, mockDE)
		return dirEntries, nil
	}
	provider := NewCRIStatsProvider(
		mockCadvisor,
		resourceAnalyzer,
		mockPodManager,
		fakeRuntimeService,
		fakeImageService,
		NewFakeHostStatsProviderWithData(fakeStats, fakeOS),
		true,
		false,
		fakeContainerStatsProvider{},
	)

	cadvisorInfos, err := getCadvisorContainerInfo(logger, mockCadvisor)
	if err != nil {
		t.Errorf("failed to get container info from cadvisor: %v", err)
	}
	stats, err := provider.ListPodStats(tCtx)
	assert := assert.New(t)
	assert.NoError(err)
	assert.Len(stats, 2)
	podStatsMap := make(map[statsapi.PodReference]statsapi.PodStats)
	for _, s := range stats {
		podStatsMap[s.PodRef] = s
	}
	p0 := podStatsMap[prf0]
	assert.Equal(sandbox0.CreatedAt, p0.StartTime.UnixNano())
	assert.Len(p0.Containers, 2)

	checkEphemeralStorageStats(assert, p0, ephemeralVolumes, []*runtimeapi.ContainerStats{containerStats0, containerStats1},
		[]*volume.Metrics{containerLogStats0, containerLogStats1}, podLogStats0)

	containerStatsMap := make(map[string]statsapi.ContainerStats)
	for _, s := range p0.Containers {
		containerStatsMap[s.Name] = s
	}

	c0 := containerStatsMap[cName0]
	assert.Equal(container0.CreatedAt, c0.StartTime.UnixNano())
	checkCRICPUAndMemoryStatsForStrictlyFromCRI(assert, c0, exceptedContainerStatsMap[cName0])
	checkCRIIOStatsForStrictlyFromCRI(assert, c0, exceptedContainerStatsMap[cName0])
	assert.Nil(c0.Accelerators)
	checkCRIRootfsStats(assert, c0, containerStats0, &imageFsInfo)
	checkCRILogsStats(assert, c0, &rootFsInfo, containerLogStats0)

	c1 := containerStatsMap[cName1]
	assert.Equal(container1.CreatedAt, c1.StartTime.UnixNano())
	checkCRICPUAndMemoryStatsForStrictlyFromCRI(assert, c1, exceptedContainerStatsMap[cName1])
	checkCRIIOStatsForStrictlyFromCRI(assert, c1, exceptedContainerStatsMap[cName1])
	assert.Nil(c0.Accelerators)
	checkCRIRootfsStats(assert, c1, containerStats1, nil)
	checkCRILogsStats(assert, c1, &rootFsInfo, containerLogStats1)
	checkCRIPodCPUAndMemoryStatsStrictlyFromCRI(assert, p0, exceptedPodStatsMap[prf0])
	checkCRIPodIOStatsStrictlyFromCRI(assert, p0, exceptedPodStatsMap[prf0])
	assert.NotNil(cadvisorInfos[sandbox0Cgroup].Stats[0].Cpu)
	assert.NotNil(cadvisorInfos[sandbox0Cgroup].Stats[0].Memory)

	p1 := podStatsMap[prf1]
	assert.Equal(sandbox1.CreatedAt, p1.StartTime.UnixNano())
	assert.Len(p1.Containers, 1)

	checkEphemeralStorageStats(assert, p1, ephemeralVolumes, []*runtimeapi.ContainerStats{containerStats2},
		[]*volume.Metrics{containerLogStats2}, podLogStats1)
	c2 := p1.Containers[0]
	assert.Equal(cName2, c2.Name)
	assert.Equal(container2.CreatedAt, c2.StartTime.UnixNano())
	checkCRICPUAndMemoryStatsForStrictlyFromCRI(assert, c2, exceptedContainerStatsMap[cName2])
	checkCRIIOStatsForStrictlyFromCRI(assert, c2, exceptedContainerStatsMap[cName2])
	assert.Nil(c0.Accelerators)
	checkCRIRootfsStats(assert, c2, containerStats2, &imageFsInfo)
	checkCRILogsStats(assert, c2, &rootFsInfo, containerLogStats2)
	checkCRIPodCPUAndMemoryStatsStrictlyFromCRI(assert, p1, exceptedPodStatsMap[prf1])
	checkCRIPodIOStatsStrictlyFromCRI(assert, p1, exceptedPodStatsMap[prf1])

	if runtime.GOOS == "linux" {
		if _, ok := cadvisorInfos[sandbox1Cgroup]; ok {
			t.Errorf("expect no cadvisor stats for pod %v", prf1)
		}
	}
}

// TestCRIListPodCPUAndMemoryStats 校验 ListPodCPUAndMemoryStats 的 CPU/内存统计
// 合并行为，覆盖多种 sandbox 与容器状态。
func TestCRIListPodCPUAndMemoryStats(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletPSI, true)
	tCtx := ktesting.Init(t)

	var (
		imageFsMountpoint = "/test/mount/point"
		unknownMountpoint = "/unknown/mount/point"

		sandbox0        = makeFakePodSandbox("sandbox0-name", "sandbox0-uid", "sandbox0-ns", false)
		sandbox0Cgroup  = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox0.PodSandboxStatus.Metadata.Uid))
		container0      = makeFakeContainer(sandbox0, cName0, 0, false)
		containerStats0 = makeFakeContainerStats(container0, imageFsMountpoint)
		container1      = makeFakeContainer(sandbox0, cName1, 0, false)
		containerStats1 = makeFakeContainerStats(container1, unknownMountpoint)

		sandbox1        = makeFakePodSandbox("sandbox1-name", "sandbox1-uid", "sandbox1-ns", false)
		sandbox1Cgroup  = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox1.PodSandboxStatus.Metadata.Uid))
		container2      = makeFakeContainer(sandbox1, cName2, 0, false)
		containerStats2 = makeFakeContainerStats(container2, imageFsMountpoint)

		sandbox2        = makeFakePodSandbox("sandbox2-name", "sandbox2-uid", "sandbox2-ns", false)
		sandbox2Cgroup  = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox2.PodSandboxStatus.Metadata.Uid))
		container3      = makeFakeContainer(sandbox2, cName3, 0, true)
		containerStats3 = makeFakeContainerStats(container3, imageFsMountpoint)
		container4      = makeFakeContainer(sandbox2, cName3, 1, false)
		containerStats4 = makeFakeContainerStats(container4, imageFsMountpoint)

		// 运行中的 pod，含一个已终止容器和一个运行中容器
		sandbox3        = makeFakePodSandbox("sandbox3-name", "sandbox3-uid", "sandbox3-ns", false)
		sandbox3Cgroup  = "/" + cm.GetPodCgroupNameSuffix(types.UID(sandbox3.PodSandboxStatus.Metadata.Uid))
		container5      = makeFakeContainer(sandbox3, cName5, 0, true)
		containerStats5 = makeFakeContainerStats(container5, imageFsMountpoint)
		container8      = makeFakeContainer(sandbox3, cName8, 0, false)
		containerStats8 = makeFakeContainerStats(container8, imageFsMountpoint)

		// 已终止的 pod sandbox
		sandbox4        = makeFakePodSandbox("sandbox1-name", "sandbox1-uid", "sandbox1-ns", true)
		container6      = makeFakeContainer(sandbox4, cName6, 0, true)
		containerStats6 = makeFakeContainerStats(container6, imageFsMountpoint)

		// 已终止的 pod
		sandbox5        = makeFakePodSandbox("sandbox1-name", "sandbox5-uid", "sandbox1-ns", true)
		container7      = makeFakeContainer(sandbox5, cName7, 0, true)
		containerStats7 = makeFakeContainerStats(container7, imageFsMountpoint)

		// cadvisor 不返回统计的 pod
		sandbox6        = makeFakePodSandbox("sandbox6-name", "sandbox6-uid", "sandbox6-ns", false)
		container9      = makeFakeContainer(sandbox6, cName9, 0, false)
		containerStats9 = makeFakeContainerStats(container9, imageFsMountpoint)
	)

	var (
		mockCadvisor       = cadvisortest.NewMockInterface(t)
		mockPodManager     = new(kubepodtest.MockManager)
		resourceAnalyzer   = new(fakeResourceAnalyzer)
		fakeRuntimeService = critest.NewFakeRuntimeService()
	)

	infos := map[string]cadvisorapi.ContainerInfo{
		"/":                           getTestContainerInfo(seedRoot, "", "", ""),
		"/kubelet":                    getTestContainerInfo(seedKubelet, "", "", ""),
		"/system":                     getTestContainerInfo(seedMisc, "", "", ""),
		sandbox0.PodSandboxStatus.Id:  getTestContainerInfo(seedSandbox0, pName0, sandbox0.PodSandboxStatus.Metadata.Namespace, kubelettypes.PodInfraContainerName),
		sandbox0Cgroup:                getTestContainerInfo(seedSandbox0, "", "", ""),
		container0.ContainerStatus.Id: getTestContainerInfo(seedContainer0, pName0, sandbox0.PodSandboxStatus.Metadata.Namespace, cName0),
		container1.ContainerStatus.Id: getTestContainerInfo(seedContainer1, pName0, sandbox0.PodSandboxStatus.Metadata.Namespace, cName1),
		sandbox1.PodSandboxStatus.Id:  getTestContainerInfo(seedSandbox1, pName1, sandbox1.PodSandboxStatus.Metadata.Namespace, kubelettypes.PodInfraContainerName),
		sandbox1Cgroup:                getTestContainerInfo(seedSandbox1, "", "", ""),
		container2.ContainerStatus.Id: getTestContainerInfo(seedContainer2, pName1, sandbox1.PodSandboxStatus.Metadata.Namespace, cName2),
		sandbox2.PodSandboxStatus.Id:  getTestContainerInfo(seedSandbox2, pName2, sandbox2.PodSandboxStatus.Metadata.Namespace, kubelettypes.PodInfraContainerName),
		sandbox2Cgroup:                getTestContainerInfo(seedSandbox2, "", "", ""),
		container4.ContainerStatus.Id: getTestContainerInfo(seedContainer3, pName2, sandbox2.PodSandboxStatus.Metadata.Namespace, cName3),
		sandbox3Cgroup:                getTestContainerInfo(seedSandbox3, "", "", ""),
	}

	options := cadvisorapi.RequestOptions{
		IdType:    cadvisorapi.TypeName,
		Count:     2,
		Recursive: true,
	}

	mockCadvisor.EXPECT().ContainerInfoV2("/", options).Return(infos, nil)

	fakeRuntimeService.SetFakeSandboxes([]*critest.FakePodSandbox{
		sandbox0, sandbox1, sandbox2, sandbox3, sandbox4, sandbox5, sandbox6,
	})
	fakeRuntimeService.SetFakeContainers([]*critest.FakeContainer{
		container0, container1, container2, container3, container4, container5, container6, container7, container8, container9,
	})
	fakeRuntimeService.SetFakeContainerStats([]*runtimeapi.ContainerStats{
		containerStats0, containerStats1, containerStats2, containerStats3, containerStats4, containerStats5, containerStats6, containerStats7, containerStats8, containerStats9,
	})

	ephemeralVolumes := makeFakeVolumeStats([]string{"ephVolume1, ephVolumes2"})
	persistentVolumes := makeFakeVolumeStats([]string{"persisVolume1, persisVolumes2"})
	resourceAnalyzer.podVolumeStats = serverstats.PodVolumeStats{
		EphemeralVolumes:  ephemeralVolumes,
		PersistentVolumes: persistentVolumes,
	}

	provider := NewCRIStatsProvider(
		mockCadvisor,
		resourceAnalyzer,
		mockPodManager,
		fakeRuntimeService,
		nil,
		NewFakeHostStatsProvider(&kubecontainertest.FakeOS{}),
		false,
		false,
		fakeContainerStatsProvider{},
	)

	stats, err := provider.ListPodCPUAndMemoryStats(tCtx)
	assert := assert.New(t)
	assert.NoError(err)
	assert.Len(stats, 5)

	podStatsMap := make(map[statsapi.PodReference]statsapi.PodStats)
	for _, s := range stats {
		podStatsMap[s.PodRef] = s
	}

	p0 := podStatsMap[statsapi.PodReference{Name: "sandbox0-name", UID: "sandbox0-uid", Namespace: "sandbox0-ns"}]
	assert.Equal(sandbox0.CreatedAt, p0.StartTime.UnixNano())
	assert.Len(p0.Containers, 2)
	assert.Nil(p0.EphemeralStorage)
	assert.Nil(p0.VolumeStats)
	assert.Nil(p0.Network)
	assert.Nil(p0.IO)
	checkCRIPodCPUAndMemoryStats(assert, p0, infos[sandbox0Cgroup].Stats[0])

	containerStatsMap := make(map[string]statsapi.ContainerStats)
	for _, s := range p0.Containers {
		containerStatsMap[s.Name] = s
	}

	c0 := containerStatsMap[cName0]
	assert.Equal(container0.CreatedAt, c0.StartTime.UnixNano())
	checkCRICPUAndMemoryStats(assert, c0, infos[container0.ContainerStatus.Id].Stats[0])
	checkSwapStats(t, cName0, seedContainer0, infos[container0.ContainerStatus.Id], c0.Swap)
	assert.Nil(c0.Rootfs)
	assert.Nil(c0.Logs)
	assert.Nil(c0.Accelerators)
	assert.Nil(c0.UserDefinedMetrics)
	assert.Nil(c0.IO)
	c1 := containerStatsMap[cName1]
	assert.Equal(container1.CreatedAt, c1.StartTime.UnixNano())
	checkCRICPUAndMemoryStats(assert, c1, infos[container1.ContainerStatus.Id].Stats[0])
	checkSwapStats(t, cName1, seedContainer1, infos[container1.ContainerStatus.Id], c1.Swap)
	assert.Nil(c1.Rootfs)
	assert.Nil(c1.Logs)
	assert.Nil(c1.Accelerators)
	assert.Nil(c1.UserDefinedMetrics)
	assert.Nil(c1.IO)

	p1 := podStatsMap[statsapi.PodReference{Name: "sandbox1-name", UID: "sandbox1-uid", Namespace: "sandbox1-ns"}]
	assert.Equal(sandbox1.CreatedAt, p1.StartTime.UnixNano())
	assert.Len(p1.Containers, 1)
	assert.Nil(p1.EphemeralStorage)
	assert.Nil(p1.VolumeStats)
	assert.Nil(p1.Network)
	assert.Nil(p1.IO)
	checkCRIPodCPUAndMemoryStats(assert, p1, infos[sandbox1Cgroup].Stats[0])

	c2 := p1.Containers[0]
	assert.Equal(cName2, c2.Name)
	assert.Equal(container2.CreatedAt, c2.StartTime.UnixNano())
	checkCRICPUAndMemoryStats(assert, c2, infos[container2.ContainerStatus.Id].Stats[0])
	checkSwapStats(t, cName2, seedContainer2, infos[container2.ContainerStatus.Id], c2.Swap)
	assert.Nil(c2.Rootfs)
	assert.Nil(c2.Logs)
	assert.Nil(c2.Accelerators)
	assert.Nil(c2.UserDefinedMetrics)
	assert.Nil(c2.IO)

	p2 := podStatsMap[statsapi.PodReference{Name: "sandbox2-name", UID: "sandbox2-uid", Namespace: "sandbox2-ns"}]
	assert.Equal(sandbox2.CreatedAt, p2.StartTime.UnixNano())
	assert.Len(p2.Containers, 1)
	assert.Nil(p2.EphemeralStorage)
	assert.Nil(p2.VolumeStats)
	assert.Nil(p2.Network)
	assert.Nil(p2.IO)
	checkCRIPodCPUAndMemoryStats(assert, p2, infos[sandbox2Cgroup].Stats[0])

	c3 := p2.Containers[0]
	assert.Equal(cName3, c3.Name)
	assert.Equal(container4.CreatedAt, c3.StartTime.UnixNano())
	checkCRICPUAndMemoryStats(assert, c3, infos[container4.ContainerStatus.Id].Stats[0])
	checkSwapStats(t, cName3, seedContainer3, infos[container4.ContainerStatus.Id], c3.Swap)
	assert.Nil(c2.Rootfs)
	assert.Nil(c2.Logs)
	assert.Nil(c2.Accelerators)
	assert.Nil(c2.UserDefinedMetrics)
	assert.Nil(c2.IO)

	p3 := podStatsMap[statsapi.PodReference{Name: "sandbox3-name", UID: "sandbox3-uid", Namespace: "sandbox3-ns"}]
	assert.Equal(sandbox3.CreatedAt, p3.StartTime.UnixNano())
	assert.Len(p3.Containers, 1)

	c8 := p3.Containers[0]
	assert.Equal(cName8, c8.Name)
	assert.Equal(container8.CreatedAt, c8.StartTime.UnixNano())
	assert.NotNil(c8.CPU.Time)
	assert.NotNil(c8.Memory.Time)
	checkCRIPodCPUAndMemoryStats(assert, p3, infos[sandbox3Cgroup].Stats[0])

	p6 := podStatsMap[statsapi.PodReference{Name: "sandbox6-name", UID: "sandbox6-uid", Namespace: "sandbox6-ns"}]
	assert.Equal(sandbox6.CreatedAt, p6.StartTime.UnixNano())
	assert.Len(p6.Containers, 1)

	c9 := p6.Containers[0]
	assert.Equal(cName9, c9.Name)
	assert.Equal(container9.CreatedAt, c9.StartTime.UnixNano())
	assert.NotNil(c9.CPU.Time)
	assert.Equal(containerStats9.Cpu.Timestamp, p6.CPU.Time.UnixNano())
	assert.NotNil(c9.Memory.Time)
	assert.Equal(containerStats9.Memory.Timestamp, p6.Memory.Time.UnixNano())
}

// TestCRIPodCPUAndMemoryStats 校验单个 pod 的 CPU 与内存统计聚合结果。
func TestCRIPodCPUAndMemoryStats(t *testing.T) {
	tCtx := ktesting.Init(t)

	const (
		podName      = "test-pod"
		podNamespace = "test-ns"
		podUID       = types.UID(podName + "-uid")
	)
	var (
		sandbox0        = makeFakePodSandbox(podName, string(podUID), podNamespace, false)
		sandbox1        = makeFakePodSandbox(podName, string(podUID), podNamespace, true)
		container0      = makeFakeContainer(sandbox0, cName0, 0, false)
		containerStats0 = makeFakeContainerStatsStrictlyFromCRI(seedContainer0, container0, "")
		container1      = makeFakeContainer(sandbox0, cName1, 0, false)
		containerStats1 = makeFakeContainerStatsStrictlyFromCRI(seedContainer1, container1, "")
	)

	var (
		mockCadvisor               = cadvisortest.NewMockInterface(t)
		mockPodManager             = new(kubepodtest.MockManager)
		resourceAnalyzer           = new(fakeResourceAnalyzer)
		fakeRuntimeService         = critest.NewFakeRuntimeService()
		fakeContainerStatsProvider = fakeContainerStatsProvider{
			podStats: ptr.To(getPodSandboxStatsStrictlyFromCRI(seedSandbox0, sandbox0)),
		}
	)

	fakeRuntimeService.SetFakeSandboxes([]*critest.FakePodSandbox{
		sandbox0,
	})
	fakeRuntimeService.SetFakeContainers([]*critest.FakeContainer{
		container0, container1,
	})
	fakeRuntimeService.SetFakeContainerStats([]*runtimeapi.ContainerStats{
		containerStats0, containerStats1,
	})

	provider := NewCRIStatsProvider(
		mockCadvisor,
		resourceAnalyzer,
		mockPodManager,
		fakeRuntimeService,
		nil,
		NewFakeHostStatsProvider(&kubecontainertest.FakeOS{}),
		false,
		false,
		fakeContainerStatsProvider,
	)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: podNamespace,
			UID:       podUID,
		},
	}
	podStatus := &kubecontainer.PodStatus{
		ID:              podUID,
		Name:            podName,
		Namespace:       podNamespace,
		SandboxStatuses: []*runtimeapi.PodSandboxStatus{&sandbox0.PodSandboxStatus, &sandbox1.PodSandboxStatus},
		ContainerStatuses: []*kubecontainer.Status{
			{Name: cName0, CreatedAt: time.Unix(0, container0.CreatedAt)},
			{Name: cName1, CreatedAt: time.Unix(0, container1.CreatedAt)},
		},
	}

	stats, err := provider.PodCPUAndMemoryStats(tCtx, pod, podStatus)
	assert := assert.New(t)
	require.NoError(t, err)

	assert.Equal(sandbox0.CreatedAt, stats.StartTime.UnixNano())
	assert.Len(stats.Containers, 2)
	assert.Nil(stats.EphemeralStorage)
	assert.Nil(stats.VolumeStats)
	assert.Nil(stats.Network)
	assert.Nil(stats.IO)
	checkCRIPodCPUAndMemoryStatsStrictlyFromCRI(assert, *stats, getPodSandboxStatsStrictlyFromCRI(seedSandbox0, sandbox0))

	containerStatsMap := make(map[string]statsapi.ContainerStats)
	for _, s := range stats.Containers {
		containerStatsMap[s.Name] = s
	}

	c0 := containerStatsMap[cName0]
	assert.Equal(container0.CreatedAt, c0.StartTime.UnixNano())
	checkCRICPUAndMemoryStatsForStrictlyFromCRI(assert, c0, getCRIContainerStatsStrictlyFromCRI(seedContainer0, cName0))
	assert.Nil(c0.Rootfs)
	assert.Nil(c0.Logs)
	assert.Nil(c0.Accelerators)
	assert.Nil(c0.UserDefinedMetrics)
	assert.Nil(c0.IO)
	c1 := containerStatsMap[cName1]
	assert.Equal(container1.CreatedAt, c1.StartTime.UnixNano())
	checkCRICPUAndMemoryStatsForStrictlyFromCRI(assert, c1, getCRIContainerStatsStrictlyFromCRI(seedContainer1, cName1))
	assert.Nil(c1.Rootfs)
	assert.Nil(c1.Logs)
	assert.Nil(c1.Accelerators)
	assert.Nil(c1.UserDefinedMetrics)
	assert.Nil(c1.IO)
}

// TestCRIImagesFsStats 校验 ImageFs 统计的正确转换。
func TestCRIImagesFsStats(t *testing.T) {
	tCtx := ktesting.Init(t)
	var (
		imageFsMountpoint = "/test/mount/point"
		imageFsInfo       = getTestFsInfo(2000)
		imageFsUsage      = makeFakeImageFsUsage(imageFsMountpoint)
	)

	var (
		mockCadvisor       = cadvisortest.NewMockInterface(t)
		mockPodManager     = new(kubepodtest.MockManager)
		resourceAnalyzer   = new(fakeResourceAnalyzer)
		fakeRuntimeService = critest.NewFakeRuntimeService()
		fakeImageService   = critest.NewFakeImageService()
	)
	mockCadvisor.EXPECT().GetDirFsInfo(imageFsMountpoint).Return(imageFsInfo, nil)
	fakeImageService.SetFakeFilesystemUsage([]*runtimeapi.FilesystemUsage{
		imageFsUsage,
	})

	provider := NewCRIStatsProvider(
		mockCadvisor,
		resourceAnalyzer,
		mockPodManager,
		fakeRuntimeService,
		fakeImageService,
		NewFakeHostStatsProvider(&kubecontainertest.FakeOS{}),
		false,
		false,
		fakeContainerStatsProvider{},
	)

	stats, containerStats, err := provider.ImageFsStats(tCtx)
	assert := assert.New(t)
	assert.NoError(err)

	assert.Equal(imageFsUsage.Timestamp, stats.Time.UnixNano())
	assert.Equal(imageFsInfo.Available, *stats.AvailableBytes)
	assert.Equal(imageFsInfo.Capacity, *stats.CapacityBytes)
	assert.Equal(imageFsInfo.InodesFree, stats.InodesFree)
	assert.Equal(imageFsInfo.Inodes, stats.Inodes)
	assert.Equal(imageFsUsage.UsedBytes.Value, *stats.UsedBytes)
	assert.Equal(imageFsUsage.InodesUsed.Value, *stats.InodesUsed)

	assert.Equal(imageFsUsage.Timestamp, containerStats.Time.UnixNano())
	assert.Equal(imageFsInfo.Available, *containerStats.AvailableBytes)
	assert.Equal(imageFsInfo.Capacity, *containerStats.CapacityBytes)
	assert.Equal(imageFsInfo.InodesFree, containerStats.InodesFree)
	assert.Equal(imageFsInfo.Inodes, containerStats.Inodes)
	assert.Equal(imageFsUsage.UsedBytes.Value, *containerStats.UsedBytes)
	assert.Equal(imageFsUsage.InodesUsed.Value, *containerStats.InodesUsed)

}

// makeFakePodSandbox 构造用于测试的 fake sandbox；terminated 为 true 时状态为未就绪。
func makeFakePodSandbox(name, uid, namespace string, terminated bool) *critest.FakePodSandbox {
	p := &critest.FakePodSandbox{
		PodSandboxStatus: runtimeapi.PodSandboxStatus{
			Metadata: &runtimeapi.PodSandboxMetadata{
				Name:      name,
				Uid:       uid,
				Namespace: namespace,
			},
			State:     runtimeapi.PodSandboxState_SANDBOX_READY,
			CreatedAt: time.Now().UnixNano(),
		},
	}
	if terminated {
		p.PodSandboxStatus.State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
	}
	p.PodSandboxStatus.Id = strings.ReplaceAll(string(uuid.NewUUID()), "-", "")
	return p
}

// makeFakeContainer 构造属于指定 sandbox 的 fake 容器，并写入 pod/容器相关标签。
func makeFakeContainer(sandbox *critest.FakePodSandbox, name string, attempt uint32, terminated bool) *critest.FakeContainer {
	sandboxID := sandbox.PodSandboxStatus.Id
	c := &critest.FakeContainer{
		SandboxID: sandboxID,
		ContainerStatus: runtimeapi.ContainerStatus{
			Metadata:  &runtimeapi.ContainerMetadata{Name: name, Attempt: attempt},
			Image:     &runtimeapi.ImageSpec{},
			ImageRef:  "fake-image-ref",
			CreatedAt: time.Now().UnixNano(),
		},
	}
	c.ContainerStatus.Labels = map[string]string{
		"io.kubernetes.pod.name":       sandbox.Metadata.Name,
		"io.kubernetes.pod.uid":        sandbox.Metadata.Uid,
		"io.kubernetes.pod.namespace":  sandbox.Metadata.Namespace,
		"io.kubernetes.container.name": name,
	}
	if terminated {
		c.ContainerStatus.State = runtimeapi.ContainerState_CONTAINER_EXITED
	} else {
		c.ContainerStatus.State = runtimeapi.ContainerState_CONTAINER_RUNNING
	}
	c.ContainerStatus.Id = strings.ReplaceAll(string(uuid.NewUUID()), "-", "")
	return c
}

// makeFakeContainerStats 构造 fake 容器的 CRI 统计；已终止容器不返回 CPU/内存数据。
func makeFakeContainerStats(container *critest.FakeContainer, imageFsMountpoint string) *runtimeapi.ContainerStats {
	containerStats := &runtimeapi.ContainerStats{
		Attributes: &runtimeapi.ContainerAttributes{
			Id:       container.ContainerStatus.Id,
			Metadata: container.ContainerStatus.Metadata,
		},
		WritableLayer: &runtimeapi.FilesystemUsage{
			Timestamp:  time.Now().UnixNano(),
			FsId:       &runtimeapi.FilesystemIdentifier{Mountpoint: imageFsMountpoint},
			UsedBytes:  &runtimeapi.UInt64Value{Value: rand.Uint64() / 100},
			InodesUsed: &runtimeapi.UInt64Value{Value: rand.Uint64() / 100},
		},
	}
	if container.State == runtimeapi.ContainerState_CONTAINER_EXITED {
		containerStats.Cpu = nil
		containerStats.Memory = nil
	} else {
		containerStats.Cpu = &runtimeapi.CpuUsage{
			Timestamp:            time.Now().UnixNano(),
			UsageCoreNanoSeconds: &runtimeapi.UInt64Value{Value: rand.Uint64()},
		}
		containerStats.Memory = &runtimeapi.MemoryUsage{
			Timestamp:       time.Now().UnixNano(),
			WorkingSetBytes: &runtimeapi.UInt64Value{Value: rand.Uint64()},
		}
	}
	return containerStats
}

// makeFakeContainerStatsStrictlyFromCRI 使用 CRI 偏移量构造 CRI 容器统计，
// 以便与 cadvisor 统计区分。
func makeFakeContainerStatsStrictlyFromCRI(seed int, container *critest.FakeContainer, imageFsMountpoint string) *runtimeapi.ContainerStats {
	containerStats := &runtimeapi.ContainerStats{
		Attributes: &runtimeapi.ContainerAttributes{
			Id:       container.ContainerStatus.Id,
			Metadata: container.ContainerStatus.Metadata,
		},
		WritableLayer: &runtimeapi.FilesystemUsage{
			Timestamp:  timestamp.UnixNano(),
			FsId:       &runtimeapi.FilesystemIdentifier{Mountpoint: imageFsMountpoint},
			UsedBytes:  &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetFsUsage)},
			InodesUsed: &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetFsInodeUsage)},
		},
	}
	if container.State == runtimeapi.ContainerState_CONTAINER_EXITED {
		containerStats.Cpu = nil
		containerStats.Memory = nil
		containerStats.Io = nil
	} else {
		containerStats.Cpu = &runtimeapi.CpuUsage{
			Timestamp:            timestamp.UnixNano(),
			UsageCoreNanoSeconds: &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetCPUUsageCoreSeconds)},
			Psi:                  getCRITestPSIStats(seed),
		}
		containerStats.Memory = &runtimeapi.MemoryUsage{
			Timestamp:       timestamp.UnixNano(),
			WorkingSetBytes: &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetMemWorkingSetBytes)},
			UsageBytes:      &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetMemUsageBytes)},
			RssBytes:        &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetMemRSSBytes)},
			Psi:             getCRITestPSIStats(seed),
		}
		containerStats.Io = &runtimeapi.IoUsage{
			Timestamp: timestamp.UnixNano(),
			Psi:       getCRITestPSIStats(seed),
		}
	}
	return containerStats
}

// makeFakePodSandboxStatsStrictlyFromCRI 构造仅来自 CRI 的 sandbox 统计，
// 包含 CPU、内存、网络、PSI 及给定容器统计。
func makeFakePodSandboxStatsStrictlyFromCRI(seed int, podSandbox *critest.FakePodSandbox, podContainerStats ...*runtimeapi.ContainerStats) *runtimeapi.PodSandboxStats {
	podSandboxStats := &runtimeapi.PodSandboxStats{
		Attributes: &runtimeapi.PodSandboxAttributes{
			Id:       podSandbox.Id,
			Metadata: podSandbox.Metadata,
		},
		Linux: &runtimeapi.LinuxPodSandboxStats{},
	}
	podSandboxStats.Linux.Containers = append(podSandboxStats.Linux.Containers, podContainerStats...)
	if podSandbox.State == runtimeapi.PodSandboxState_SANDBOX_NOTREADY {
		podSandboxStats.Linux.Cpu = nil
		podSandboxStats.Linux.Memory = nil
		podSandboxStats.Linux.Io = nil
	} else {
		podSandboxStats.Linux.Cpu = &runtimeapi.CpuUsage{
			Timestamp:            timestamp.UnixNano(),
			UsageCoreNanoSeconds: &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetCPUUsageCoreSeconds)},
			Psi:                  getCRITestPSIStats(seed),
		}
		podSandboxStats.Linux.Memory = &runtimeapi.MemoryUsage{
			Timestamp:       timestamp.UnixNano(),
			WorkingSetBytes: &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetMemWorkingSetBytes)},
			UsageBytes:      &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetMemUsageBytes)},
			RssBytes:        &runtimeapi.UInt64Value{Value: uint64(seed + offsetCRI + offsetMemRSSBytes)},
			Psi:             getCRITestPSIStats(seed),
		}
		podSandboxStats.Linux.Io = &runtimeapi.IoUsage{
			Timestamp: timestamp.UnixNano(),
			Psi:       getCRITestPSIStats(seed),
		}
	}
	return podSandboxStats
}

// getCRITestPSIStats 构造包含 Full 与 Some 两部分的测试 PSI 统计。
func getCRITestPSIStats(seed int) *runtimeapi.PsiStats {
	return &runtimeapi.PsiStats{
		Full: getCRITestPSIData(seed),
		Some: getCRITestPSIData(seed),
	}
}

// getCRITestPSIData 构造单组测试 PSI 数据，其总时长由 seed 偏移得到。
func getCRITestPSIData(seed int) *runtimeapi.PsiData {
	return &runtimeapi.PsiData{
		Total:  uint64(seed + offsetPSIDataTotal),
		Avg10:  float64(10),
		Avg60:  float64(10),
		Avg300: float64(10),
	}
}

// getPodSandboxStatsStrictlyFromCRI 构造仅来自 CRI 的 summary API PodStats，
// 用于与 provider 输出比较。
func getPodSandboxStatsStrictlyFromCRI(seed int, podSandbox *critest.FakePodSandbox) statsapi.PodStats {
	psi := getTestPSIStats(seed)
	podStats := statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name:      podSandbox.Metadata.Name,
			UID:       podSandbox.Metadata.Uid,
			Namespace: podSandbox.Metadata.Namespace,
		},
		// summary API 中的 StartTime 即 pod 创建时间。
		StartTime: metav1.NewTime(time.Unix(0, podSandbox.CreatedAt)),
	}
	if podSandbox.State == runtimeapi.PodSandboxState_SANDBOX_NOTREADY {
		podStats.CPU = nil
		podStats.Memory = nil
		podStats.IO = nil
	} else {
		usageCoreNanoSeconds := uint64(seed + offsetCRI + offsetCPUUsageCoreSeconds)
		podStats.CPU = &statsapi.CPUStats{
			Time:                 metav1.NewTime(timestamp),
			UsageCoreNanoSeconds: &usageCoreNanoSeconds,
			PSI:                  cadvisorPSIToStatsPSI(&psi),
		}
		podStats.Memory = &statsapi.MemoryStats{
			Time:            metav1.NewTime(timestamp),
			WorkingSetBytes: ptr.To(uint64(seed + offsetCRI + offsetMemWorkingSetBytes)),
			UsageBytes:      ptr.To(uint64(seed + offsetCRI + offsetMemUsageBytes)),
			RSSBytes:        ptr.To(uint64(seed + offsetCRI + offsetMemRSSBytes)),
			PSI:             cadvisorPSIToStatsPSI(&psi),
		}
		podStats.IO = &statsapi.IOStats{
			Time: metav1.NewTime(timestamp),
			PSI:  cadvisorPSIToStatsPSI(&psi),
		}
	}

	return podStats
}

// makeFakeImageFsUsage 构造指定挂载点的镜像文件系统用量。
func makeFakeImageFsUsage(fsMountpoint string) *runtimeapi.FilesystemUsage {
	return &runtimeapi.FilesystemUsage{
		Timestamp:  time.Now().UnixNano(),
		FsId:       &runtimeapi.FilesystemIdentifier{Mountpoint: fsMountpoint},
		UsedBytes:  &runtimeapi.UInt64Value{Value: rand.Uint64()},
		InodesUsed: &runtimeapi.UInt64Value{Value: rand.Uint64()},
	}
}

// makeFakeVolumeStats 为给定卷名批量构造共享同一组随机容量数据的 VolumeStats。
func makeFakeVolumeStats(volumeNames []string) []statsapi.VolumeStats {
	volumes := make([]statsapi.VolumeStats, len(volumeNames))
	availableBytes := rand.Uint64()
	capacityBytes := rand.Uint64()
	usedBytes := rand.Uint64() / 100
	inodes := rand.Uint64()
	inodesFree := rand.Uint64()
	inodesUsed := rand.Uint64() / 100
	for i, name := range volumeNames {
		fsStats := statsapi.FsStats{
			Time:           metav1.NewTime(time.Now()),
			AvailableBytes: &availableBytes,
			CapacityBytes:  &capacityBytes,
			UsedBytes:      &usedBytes,
			Inodes:         &inodes,
			InodesFree:     &inodesFree,
			InodesUsed:     &inodesUsed,
		}
		volumes[i] = statsapi.VolumeStats{
			FsStats: fsStats,
			Name:    name,
		}
	}
	return volumes
}

// checkCRICPUAndMemoryStats 校验容器 CPU/内存统计与 cadvisor 数据一致，并在 PSI 开启时校验 PSI。
func checkCRICPUAndMemoryStats(assert *assert.Assertions, actual statsapi.ContainerStats, cs *cadvisorapi.ContainerStats) {
	assert.Equal(cs.Timestamp.UnixNano(), actual.CPU.Time.UnixNano())
	assert.Equal(cs.Cpu.Usage.Total, *actual.CPU.UsageCoreNanoSeconds)
	assert.Equal(cs.CpuInst.Usage.Total, *actual.CPU.UsageNanoCores)

	assert.Equal(cs.Memory.Usage, *actual.Memory.UsageBytes)
	assert.Equal(cs.Memory.WorkingSet, *actual.Memory.WorkingSetBytes)
	assert.Equal(cs.Memory.RSS, *actual.Memory.RSSBytes)
	assert.Equal(cs.Memory.ContainerData.Pgfault, *actual.Memory.PageFaults)
	assert.Equal(cs.Memory.ContainerData.Pgmajfault, *actual.Memory.MajorPageFaults)

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		checkCRIPSIStats(assert, &cs.Cpu.PSI, actual.CPU.PSI)
		checkCRIPSIStats(assert, &cs.Memory.PSI, actual.Memory.PSI)
	}
}

// checkCRIIOStats 在 PSI 开启时校验磁盘 IO 的 PSI 统计。
func checkCRIIOStats(assert *assert.Assertions, actual statsapi.ContainerStats, cs *cadvisorapi.ContainerStats) {
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		checkCRIPSIStats(assert, &cs.DiskIo.PSI, actual.IO.PSI)
	}
}

// checkCRICPUAndMemoryStatsForStrictlyFromCRI 校验仅来自 CRI 的容器 CPU/内存统计与期望值一致。
func checkCRICPUAndMemoryStatsForStrictlyFromCRI(assert *assert.Assertions, actual statsapi.ContainerStats, expected statsapi.ContainerStats) {
	assert.Equal(expected.CPU.Time.UnixNano(), actual.CPU.Time.UnixNano())
	assert.Equal(*expected.CPU.UsageCoreNanoSeconds, *actual.CPU.UsageCoreNanoSeconds)
	assert.Equal(*expected.Memory.WorkingSetBytes, *actual.Memory.WorkingSetBytes)
	assert.Equal(*expected.Memory.UsageBytes, *actual.Memory.UsageBytes)
	assert.Equal(*expected.Memory.RSSBytes, *actual.Memory.RSSBytes)

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		checkCRIPSIStatsStrictlyFromCRI(assert, expected.CPU.PSI, actual.CPU.PSI)
		checkCRIPSIStatsStrictlyFromCRI(assert, expected.Memory.PSI, actual.Memory.PSI)
	}
}

// checkCRIIOStatsForStrictlyFromCRI 在 PSI 开启时校验仅来自 CRI 的 IO PSI 统计。
func checkCRIIOStatsForStrictlyFromCRI(assert *assert.Assertions, actual statsapi.ContainerStats, expected statsapi.ContainerStats) {
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		checkCRIPSIStatsStrictlyFromCRI(assert, expected.IO.PSI, actual.IO.PSI)
	}
}

// checkCRIRootfsStats 校验容器 rootfs 统计：可写层用量来自 CRI，容量来自镜像文件系统信息。
func checkCRIRootfsStats(assert *assert.Assertions, actual statsapi.ContainerStats, cs *runtimeapi.ContainerStats, imageFsInfo *cadvisorapi.FsInfo) {
	assert.Equal(cs.WritableLayer.Timestamp, actual.Rootfs.Time.UnixNano())
	if imageFsInfo != nil {
		assert.Equal(imageFsInfo.Available, *actual.Rootfs.AvailableBytes)
		assert.Equal(imageFsInfo.Capacity, *actual.Rootfs.CapacityBytes)
		assert.Equal(*imageFsInfo.InodesFree, *actual.Rootfs.InodesFree)
		assert.Equal(*imageFsInfo.Inodes, *actual.Rootfs.Inodes)
	} else {
		assert.Nil(actual.Rootfs.AvailableBytes)
		assert.Nil(actual.Rootfs.CapacityBytes)
		assert.Nil(actual.Rootfs.InodesFree)
		assert.Nil(actual.Rootfs.Inodes)
	}
	assert.Equal(cs.WritableLayer.UsedBytes.Value, *actual.Rootfs.UsedBytes)
	assert.Equal(cs.WritableLayer.InodesUsed.Value, *actual.Rootfs.InodesUsed)
}

// checkCRILogsStats 校验容器日志统计与 rootfs 信息及日志卷 Metrics 一致。
func checkCRILogsStats(assert *assert.Assertions, actual statsapi.ContainerStats, rootFsInfo *cadvisorapi.FsInfo, logStats *volume.Metrics) {
	assert.Equal(rootFsInfo.Timestamp, actual.Logs.Time.Time)
	assert.Equal(rootFsInfo.Available, *actual.Logs.AvailableBytes)
	assert.Equal(rootFsInfo.Capacity, *actual.Logs.CapacityBytes)
	assert.Equal(*rootFsInfo.InodesFree, *actual.Logs.InodesFree)
	assert.Equal(*rootFsInfo.Inodes, *actual.Logs.Inodes)
	assert.Equal(uint64(logStats.Used.Value()), *actual.Logs.UsedBytes)
	assert.Equal(uint64(logStats.InodesUsed.Value()), *actual.Logs.InodesUsed)
}

// checkEphemeralStorageStats 校验 pod 的临时存储用量为容器可写层、容器日志与 pod 日志之和。
func checkEphemeralStorageStats(assert *assert.Assertions,
	actual statsapi.PodStats,
	volumes []statsapi.VolumeStats,
	containers []*runtimeapi.ContainerStats,
	containerLogStats []*volume.Metrics,
	podLogStats *volume.Metrics) {
	var totalUsed, inodesUsed uint64
	for _, container := range containers {
		totalUsed = totalUsed + container.WritableLayer.UsedBytes.Value
		inodesUsed = inodesUsed + container.WritableLayer.InodesUsed.Value
	}

	for _, volume := range volumes {
		totalUsed = totalUsed + *volume.FsStats.UsedBytes
		inodesUsed = inodesUsed + *volume.FsStats.InodesUsed
	}

	for _, logStats := range containerLogStats {
		totalUsed = totalUsed + uint64(logStats.Used.Value())
		inodesUsed = inodesUsed + uint64(logStats.InodesUsed.Value())
	}

	if podLogStats != nil {
		totalUsed = totalUsed + uint64(podLogStats.Used.Value())
		inodesUsed = inodesUsed + uint64(podLogStats.InodesUsed.Value())
	}

	assert.Equal(int(totalUsed), int(*actual.EphemeralStorage.UsedBytes))
	assert.Equal(int(inodesUsed), int(*actual.EphemeralStorage.InodesUsed))
}

// checkCRINetworkStats 校验网络统计与 cadvisor 数据一致。
func checkCRINetworkStats(assert *assert.Assertions, actual *statsapi.NetworkStats, expected *cadvisorapi.NetworkStats) {
	assert.Equal(expected.Interfaces[0].RxBytes, *actual.RxBytes)
	assert.Equal(expected.Interfaces[0].RxErrors, *actual.RxErrors)
	assert.Equal(expected.Interfaces[0].TxBytes, *actual.TxBytes)
	assert.Equal(expected.Interfaces[0].TxErrors, *actual.TxErrors)
}

// checkCRIPodCPUAndMemoryStats 校验 pod 级 CPU/内存统计与 cadvisor 数据一致。
func checkCRIPodCPUAndMemoryStats(assert *assert.Assertions, actual statsapi.PodStats, cs *cadvisorapi.ContainerStats) {
	if runtime.GOOS != "linux" {
		return
	}
	assert.Equal(cs.Timestamp.UnixNano(), actual.CPU.Time.UnixNano())
	assert.Equal(cs.Cpu.Usage.Total, *actual.CPU.UsageCoreNanoSeconds)
	assert.Equal(cs.CpuInst.Usage.Total, *actual.CPU.UsageNanoCores)

	assert.Equal(cs.Memory.Usage, *actual.Memory.UsageBytes)
	assert.Equal(cs.Memory.WorkingSet, *actual.Memory.WorkingSetBytes)
	assert.Equal(cs.Memory.RSS, *actual.Memory.RSSBytes)
	assert.Equal(cs.Memory.ContainerData.Pgfault, *actual.Memory.PageFaults)
	assert.Equal(cs.Memory.ContainerData.Pgmajfault, *actual.Memory.MajorPageFaults)

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		checkCRIPSIStats(assert, &cs.Cpu.PSI, actual.CPU.PSI)
		checkCRIPSIStats(assert, &cs.Memory.PSI, actual.Memory.PSI)
	}
}

// checkCRIPodIOStats 在 PSI 开启时校验 pod 级 IO PSI 统计。
func checkCRIPodIOStats(assert *assert.Assertions, actual statsapi.PodStats, cs *cadvisorapi.ContainerStats) {
	if runtime.GOOS != "linux" {
		return
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		checkCRIPSIStats(assert, &cs.DiskIo.PSI, actual.IO.PSI)
	}
}

// checkCRIPSIStats 校验 PSI 的 Full 与 Some 两部分与 cadvisor 数据一致。
func checkCRIPSIStats(assert *assert.Assertions, want *cadvisorapi.PSIStats, got *statsapi.PSIStats) {
	assert.NotNil(want)
	assert.NotNil(got)
	checkCRIPSIData(assert, want.Full, got.Full)
	checkCRIPSIData(assert, want.Some, got.Some)
}

// checkCRIPSIData 校验单组 PSI 数据的总时长与平均值。
func checkCRIPSIData(assert *assert.Assertions, want cadvisorapi.PSIData, got statsapi.PSIData) {
	assert.Equal(want.Total, got.Total)
	assert.InDelta(want.Avg10, got.Avg10, 0.01)
	assert.InDelta(want.Avg60, got.Avg60, 0.01)
	assert.InDelta(want.Avg300, got.Avg300, 0.01)
}

// checkCRIPSIStatsStrictlyFromCRI 校验仅来自 CRI 的 PSI Full/Some 数据与期望一致。
func checkCRIPSIStatsStrictlyFromCRI(assert *assert.Assertions, want, got *statsapi.PSIStats) {
	assert.NotNil(want)
	assert.NotNil(got)
	checkCRIPSIDataStrictlyFromCRI(assert, want.Full, got.Full)
	checkCRIPSIDataStrictlyFromCRI(assert, want.Some, got.Some)
}

// checkCRIPSIDataStrictlyFromCRI 校验仅来自 CRI 的单组 PSI 数据与期望一致。
func checkCRIPSIDataStrictlyFromCRI(assert *assert.Assertions, want, got statsapi.PSIData) {
	assert.Equal(want.Total, got.Total)
	assert.InDelta(want.Avg10, got.Avg10, 0.01)
	assert.InDelta(want.Avg60, got.Avg60, 0.01)
	assert.InDelta(want.Avg300, got.Avg300, 0.01)
}

// checkCRIPodSwapStats 校验 pod 交换分区统计与 cadvisor 数据一致。
func checkCRIPodSwapStats(assert *assert.Assertions, actual statsapi.PodStats, cs *cadvisorapi.ContainerStats) {
	if runtime.GOOS != "linux" {
		return
	}

	assert.Equal(cs.Timestamp.UnixNano(), actual.Swap.Time.UnixNano())
	assert.Equal(cs.Memory.Swap, *actual.Swap.SwapUsageBytes)
}

// checkCRIPodCPUAndMemoryStatsStrictlyFromCRI 校验仅来自 CRI 的 pod 级 CPU/内存统计与期望一致。
func checkCRIPodCPUAndMemoryStatsStrictlyFromCRI(assert *assert.Assertions, actual statsapi.PodStats, expected statsapi.PodStats) {
	if runtime.GOOS != "linux" {
		return
	}
	assert.Equal(expected.CPU.Time.UnixNano(), actual.CPU.Time.UnixNano())
	assert.Equal(*expected.CPU.UsageCoreNanoSeconds, *actual.CPU.UsageCoreNanoSeconds)
	assert.Equal(*expected.Memory.WorkingSetBytes, *actual.Memory.WorkingSetBytes)

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		checkCRIPSIStatsStrictlyFromCRI(assert, expected.CPU.PSI, actual.CPU.PSI)
		checkCRIPSIStatsStrictlyFromCRI(assert, expected.Memory.PSI, actual.Memory.PSI)
	}
}

// checkCRIPodIOStatsStrictlyFromCRI 在 PSI 开启时校验仅来自 CRI 的 pod 级 IO PSI 统计。
func checkCRIPodIOStatsStrictlyFromCRI(assert *assert.Assertions, actual statsapi.PodStats, expected statsapi.PodStats) {
	if runtime.GOOS != "linux" {
		return
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		checkCRIPSIStatsStrictlyFromCRI(assert, expected.IO.PSI, actual.IO.PSI)
	}
}

// makeFakeLogStats 构造使用量由 seed 偏移得到的日志卷 Metrics。
func makeFakeLogStats(seed int) *volume.Metrics {
	m := &volume.Metrics{}
	m.Used = resource.NewQuantity(int64(seed+offsetUsage), resource.BinarySI)
	m.InodesUsed = resource.NewQuantity(int64(seed+offsetInodeUsage), resource.BinarySI)
	return m
}

// TestGetContainerUsageNanoCores 校验 CPU 使用速率的缓存读写：首次读取为空，
// 更新后返回最新值，并正确处理缺失统计、缺失 CPU 数据等情况。
func TestGetContainerUsageNanoCores(t *testing.T) {
	var value0 uint64
	var value1 uint64 = 10000000000

	// 使用 100+ CPU 的大型容器进行测试
	var value2 uint64 = 188427786383

	tests := []struct {
		desc          string
		cpuUsageCache map[string]*cpuUsageRecord
		stats         *runtimeapi.ContainerStats
		expected      *uint64
	}{
		{
			desc:          "should return nil if stats is nil",
			cpuUsageCache: map[string]*cpuUsageRecord{},
		},
		{
			desc:          "should return nil if cpu stats is nil",
			cpuUsageCache: map[string]*cpuUsageRecord{},
			stats: &runtimeapi.ContainerStats{
				Attributes: &runtimeapi.ContainerAttributes{
					Id: "1",
				},
				Cpu: nil,
			},
		},
		{
			desc:          "should return nil if usageCoreNanoSeconds is nil",
			cpuUsageCache: map[string]*cpuUsageRecord{},
			stats: &runtimeapi.ContainerStats{
				Attributes: &runtimeapi.ContainerAttributes{
					Id: "1",
				},
				Cpu: &runtimeapi.CpuUsage{
					Timestamp:            1,
					UsageCoreNanoSeconds: nil,
				},
			},
		},
		{
			desc:          "should return nil if cpu stats is not cached yet",
			cpuUsageCache: map[string]*cpuUsageRecord{},
			stats: &runtimeapi.ContainerStats{
				Attributes: &runtimeapi.ContainerAttributes{
					Id: "1",
				},
				Cpu: &runtimeapi.CpuUsage{
					Timestamp: 1,
					UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
						Value: 10000000000,
					},
				},
			},
		},
		{
			desc: "should return zero value if cached cpu stats is equal to current value",
			stats: &runtimeapi.ContainerStats{
				Attributes: &runtimeapi.ContainerAttributes{
					Id: "1",
				},
				Cpu: &runtimeapi.CpuUsage{
					Timestamp: 1,
					UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
						Value: 10000000000,
					},
				},
			},
			cpuUsageCache: map[string]*cpuUsageRecord{
				"1": {
					stats: &runtimeapi.CpuUsage{
						Timestamp: 0,
						UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
							Value: 10000000000,
						},
					},
				},
			},
			expected: &value0,
		},
		{
			desc: "should return correct value if cached cpu stats is not equal to current value",
			stats: &runtimeapi.ContainerStats{
				Attributes: &runtimeapi.ContainerAttributes{
					Id: "1",
				},
				Cpu: &runtimeapi.CpuUsage{
					Timestamp: int64(time.Second / time.Nanosecond),
					UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
						Value: 20000000000,
					},
				},
			},
			cpuUsageCache: map[string]*cpuUsageRecord{
				"1": {
					stats: &runtimeapi.CpuUsage{
						Timestamp: 0,
						UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
							Value: 10000000000,
						},
					},
				},
			},
			expected: &value1,
		},
		{
			desc: "should return correct value if elapsed UsageCoreNanoSeconds exceeds 18446744073",
			stats: &runtimeapi.ContainerStats{
				Attributes: &runtimeapi.ContainerAttributes{
					Id: "1",
				},
				Cpu: &runtimeapi.CpuUsage{
					Timestamp: int64(time.Second / time.Nanosecond),
					UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
						Value: 68172016162105,
					},
				},
			},
			cpuUsageCache: map[string]*cpuUsageRecord{
				"1": {
					stats: &runtimeapi.CpuUsage{
						Timestamp: 0,
						UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
							Value: 67983588375722,
						},
					},
				},
			},
			expected: &value2,
		},
		{
			desc: "should return nil if cpuacct is reset to 0 in a live container",
			stats: &runtimeapi.ContainerStats{
				Attributes: &runtimeapi.ContainerAttributes{
					Id: "1",
				},
				Cpu: &runtimeapi.CpuUsage{
					Timestamp: 2,
					UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
						Value: 0,
					},
				},
			},
			cpuUsageCache: map[string]*cpuUsageRecord{
				"1": {
					stats: &runtimeapi.CpuUsage{
						Timestamp: 1,
						UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
							Value: 10000000000,
						},
					},
				},
			},
			expected: nil,
		},
	}
	logger, _ := ktesting.NewTestContext(t)
	for _, test := range tests {
		provider := &criStatsProvider{cpuUsageCache: test.cpuUsageCache}
		// 更新前，缓存值应为 nil
		cached := provider.getContainerUsageNanoCores(test.stats)
		assert.Nil(t, cached)

		// 更新缓存并获取最新值。
		real := provider.getAndUpdateContainerUsageNanoCores(logger, test.stats)
		assert.Equal(t, test.expected, real, test.desc)

		// 更新后，缓存值应为最新值
		cached = provider.getContainerUsageNanoCores(test.stats)
		assert.Equal(t, test.expected, cached, test.desc)
	}
}

// TestExtractIDFromCgroupPath 校验能从 cgroupfs 与 systemd 两种路径格式中提取容器 ID。
func TestExtractIDFromCgroupPath(t *testing.T) {
	tests := []struct {
		cgroupPath string
		expected   string
	}{
		{
			cgroupPath: "/kubepods/burstable/pod2fc932ce-fdcc-454b-97bd-aadfdeb4c340/9be25294016e2dc0340dd605ce1f57b492039b267a6a618a7ad2a7a58a740f32",
			expected:   "9be25294016e2dc0340dd605ce1f57b492039b267a6a618a7ad2a7a58a740f32",
		},
		{
			cgroupPath: "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod2fc932ce_fdcc_454b_97bd_aadfdeb4c340.slice/cri-containerd-aaefb9d8feed2d453b543f6d928cede7a4dbefa6a0ae7c9b990dd234c56e93b9.scope",
			expected:   "aaefb9d8feed2d453b543f6d928cede7a4dbefa6a0ae7c9b990dd234c56e93b9",
		},
		{
			cgroupPath: "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod2fc932ce_fdcc_454b_97bd_aadfdeb4c340.slice/cri-o-aaefb9d8feed2d453b543f6d928cede7a4dbefa6a0ae7c9b990dd234c56e93b9.scope",
			expected:   "aaefb9d8feed2d453b543f6d928cede7a4dbefa6a0ae7c9b990dd234c56e93b9",
		},
	}

	for _, test := range tests {
		id := extractIDFromCgroupPath(test.cgroupPath)
		assert.Equal(t, test.expected, id)
	}
}

// getCRIContainerStatsStrictlyFromCRI 构造仅来自 CRI 的容器统计，
// 供严格 CRI 模式的断言使用。
func getCRIContainerStatsStrictlyFromCRI(seed int, containerName string) statsapi.ContainerStats {
	psi := getTestPSIStats(seed)
	result := statsapi.ContainerStats{
		Name:      containerName,
		StartTime: metav1.NewTime(timestamp),
		CPU:       &statsapi.CPUStats{},
		Memory:    &statsapi.MemoryStats{},
		// CRI 不支持 UserDefinedMetrics。
		Rootfs: &statsapi.FsStats{},
		IO:     &statsapi.IOStats{},
	}

	result.CPU.Time = metav1.NewTime(timestamp)
	usageCoreNanoSeconds := uint64(seed + offsetCRI + offsetCPUUsageCoreSeconds)
	result.CPU.UsageCoreNanoSeconds = &usageCoreNanoSeconds
	result.CPU.PSI = cadvisorPSIToStatsPSI(&psi)

	result.Memory.Time = metav1.NewTime(timestamp)
	result.Memory.WorkingSetBytes = ptr.To(uint64(seed + offsetCRI + offsetMemWorkingSetBytes))
	result.Memory.UsageBytes = ptr.To(uint64(seed + offsetCRI + offsetMemUsageBytes))
	result.Memory.RSSBytes = ptr.To(uint64(seed + offsetCRI + offsetMemRSSBytes))
	result.Memory.PSI = cadvisorPSIToStatsPSI(&psi)

	result.IO.Time = metav1.NewTime(timestamp)
	result.IO.PSI = cadvisorPSIToStatsPSI(&psi)

	result.Rootfs.Time = metav1.NewTime(timestamp)
	usedBytes := uint64(seed + offsetCRI + offsetFsUsage)
	result.Rootfs.UsedBytes = &usedBytes

	inodesUsed := uint64(seed + offsetCRI + offsetFsInodeUsage)
	result.Rootfs.InodesUsed = &inodesUsed

	return result
}
