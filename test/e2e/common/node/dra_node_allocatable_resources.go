/*
Copyright The Kubernetes Authors.

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

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/dynamic-resource-allocation/resourceslice"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/common/node/framework/cgroups"
	"k8s.io/kubernetes/test/e2e/common/node/framework/podresize"
	drautils "k8s.io/kubernetes/test/e2e/dra/utils"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

type draContainerInfo struct {
	Name           string
	Resources      *cgroups.ContainerResources
	Claims         []*resourceapi.ResourceClaim
	HugepageReqLim string
}

var (
	overheadMem = resource.MustParse("100Mi")
	overheadCPU = resource.MustParse("100m")

	overheadDevices = []resourceapi.Device{
		{
			Name: "device-01",
			NodeAllocatableResources: map[v1.ResourceName]resourceapi.NodeAllocatableResource{
				v1.ResourceMemory: {
					Overhead: &resourceapi.NodeAllocatableOverhead{
						PerPod:       &overheadMem,
						PerContainer: &overheadMem,
					},
				},
				v1.ResourceCPU: {
					Overhead: &resourceapi.NodeAllocatableOverhead{
						PerPod:       &overheadCPU,
						PerContainer: &overheadCPU,
					},
				},
			},
		},
	}

	directDevices = []resourceapi.Device{
		{
			Name: "device-01",
			NodeAllocatableResources: map[v1.ResourceName]resourceapi.NodeAllocatableResource{
				v1.ResourceMemory: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("1000Mi")),
					},
				},
				v1.ResourceCPU: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("1")),
					},
				},
			},
		},
	}

	directDevices2 = []resourceapi.Device{
		{
			Name: "device-01",
			NodeAllocatableResources: map[v1.ResourceName]resourceapi.NodeAllocatableResource{
				v1.ResourceMemory: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("1000Mi")),
					},
				},
				v1.ResourceCPU: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("100m")),
					},
				},
			},
		},
		{
			Name: "device-02",
			NodeAllocatableResources: map[v1.ResourceName]resourceapi.NodeAllocatableResource{
				v1.ResourceMemory: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("2000Mi")),
					},
				},
				v1.ResourceCPU: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("100m")),
					},
				},
			},
		},
	}

	combinedDevices = []resourceapi.Device{
		{
			Name: "device-direct-01",
			NodeAllocatableResources: map[v1.ResourceName]resourceapi.NodeAllocatableResource{
				v1.ResourceMemory: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("1000Mi")),
					},
				},
				v1.ResourceCPU: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("1")),
					},
				},
			},
		},
		{
			Name: "device-overhead-01",
			NodeAllocatableResources: map[v1.ResourceName]resourceapi.NodeAllocatableResource{
				v1.ResourceMemory: {
					Overhead: &resourceapi.NodeAllocatableOverhead{
						PerPod:       &overheadMem,
						PerContainer: &overheadMem,
					},
				},
				v1.ResourceCPU: {
					Overhead: &resourceapi.NodeAllocatableOverhead{
						PerPod:       &overheadCPU,
						PerContainer: &overheadCPU,
					},
				},
			},
		},
	}

	mappingAndOverheadDevice = []resourceapi.Device{
		{
			Name: "device-mapping-overhead-01",
			NodeAllocatableResources: map[v1.ResourceName]resourceapi.NodeAllocatableResource{
				v1.ResourceMemory: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("1000Mi")),
					},
					Overhead: &resourceapi.NodeAllocatableOverhead{
						PerPod:       &overheadMem,
						PerContainer: &overheadMem,
					},
				},
				v1.ResourceCPU: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(resource.MustParse("1")),
					},
					Overhead: &resourceapi.NodeAllocatableOverhead{
						PerPod:       &overheadCPU,
						PerContainer: &overheadCPU,
					},
				},
			},
		},
	}

	hugepageDevices = []resourceapi.Device{
		{
			Name: "device-hugepages-01",
			NodeAllocatableResources: map[v1.ResourceName]resourceapi.NodeAllocatableResource{
				v1.ResourceName("hugepages-2Mi"): {
					Overhead: &resourceapi.NodeAllocatableOverhead{
						PerPod:       new(resource.MustParse("100Mi")),
						PerContainer: new(resource.MustParse("50Mi")),
					},
				},
			},
		},
	}

	res100mCPU1GiCg      = &cgroups.ContainerResources{CPUReq: "100m", CPULim: "200m", MemReq: "1Gi", MemLim: "2Gi"}
	res1CPU1GiCg         = &cgroups.ContainerResources{CPUReq: "1", CPULim: "2", MemReq: "1Gi", MemLim: "2Gi"}
	res2CPU2GiCg         = &cgroups.ContainerResources{CPUReq: "2", CPULim: "4", MemReq: "2Gi", MemLim: "4Gi"}
	res2500m2500MiCg     = &cgroups.ContainerResources{CPUReq: "2500m", CPULim: "4", MemReq: "2500Mi", MemLim: "4Gi"}
	req1CPU1GiCg         = &cgroups.ContainerResources{CPUReq: "1000m", MemReq: "1Gi"}
	res1CPU1GiGuaranteed = &cgroups.ContainerResources{CPUReq: "1", CPULim: "1", MemReq: "1Gi", MemLim: "1Gi"}

	claimCPUMem = &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cpu-mem-claim",
		},
	}
	claimCPUMem2 = &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cpu-mem-claim-2",
		},
	}
	claimHugepages = &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "hugepages-claim",
		},
	}
)

func doNodeAllocatableCgroupsTests(f *framework.Framework) {
	tests := []struct {
		name                              string
		resourceSliceDevices              []resourceapi.Device
		podResources                      *cgroups.ContainerResources
		containers                        []draContainerInfo
		unreferencedClaims                []*resourceapi.ResourceClaim
		expectedPodCgroup                 cgroups.ContainerResources
		expectedContainersCgroup          []cgroups.ContainerResources // 每个容器
		expectedContainersScoreMemRequest []int64                      // 每个容器
		requiresHugepages                 bool
		expectedPodHugepagesLimit         string
		expectedContainerHugepagesLimits  []string // 每个容器
	}{
		{
			name:                 "direct mappings",
			resourceSliceDevices: directDevices,
			containers: []draContainerInfo{
				{Name: "c1", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
				{Name: "c2", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 1000m (c1 来自 spec) + 1000m (c2 来自 spec) + 1000m (CPU claim)
				// CPU limit: 2000m (c1 来自 spec) + 2000m (c2 来自 spec) + 1000m (CPU claim)
				// Memory limit: 2048Mi (c1 来自 spec) + 2048Mi (c2 来自 spec) + 1000Mi (memory claim)
				CPUReq: "3000m",
				CPULim: "5000m",
				MemLim: "5096Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 1000m (CPU claim)
				// Memory limit: 2048Mi (来自 spec) + 1000Mi (memory claim)
				{CPUReq: "1000m", CPULim: "3000m", MemLim: "3048Mi"},
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 1000m (CPU claim)
				// Memory limit: 2048Mi (来自 spec) + 1000Mi (memory claim)
				{CPUReq: "1000m", CPULim: "3000m", MemLim: "3048Mi"},
			},
			expectedContainersScoreMemRequest: []int64{1524, 1524}, // 1024 (c1) + 1000/2 (Direct memory) = 1524Mi
		},
		{
			name:                 "overhead mappings",
			resourceSliceDevices: overheadDevices,
			containers: []draContainerInfo{
				{Name: "c1", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
				{Name: "c2", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 1000m (c1 来自 spec) + 1000m (c2 来自 spec) + 100m (CPU Claim - PerPod) + 200m (CPU Claim - PerContainer * 2)
				// CPU limit: 2000m (c1 来自 spec) + 2000m (c2 来自 spec) + 100m (CPU Claim - PerPod) + 200m (CPU Claim - PerContainer * 2)
				// Memory limit: 2048Mi (c1 来自 spec) + 2048Mi (c2 来自 spec) + 100Mi (Memory Claim - PerPod) + 200Mi (Memory Claim - PerContainer * 2)
				CPUReq: "2300m",
				CPULim: "4300m",
				MemLim: "4396Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 100m (CPU Claim - PerContainer) + 100m (CPU Claim - PerPod)
				// Memory limit: 2048Mi (来自 spec) + 100Mi (Memory Claim - PerContainer) + 100Mi (Memory Claim - PerPod)
				// 注意：PerPod 开销在两个容器中都完整计入，但在 Pod 级别只计一次，用于限制总上限。
				{CPUReq: "1000m", CPULim: "2200m", MemLim: "2248Mi"},
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 100m (CPU Claim - PerContainer) + 100m (CPU Claim - PerPod)
				// Memory limit: 2048Mi (来自 spec) + 100Mi (Memory Claim - PerContainer) + 100Mi (Memory Claim - PerPod)
				{CPUReq: "1000m", CPULim: "2200m", MemLim: "2248Mi"},
			},
			expectedContainersScoreMemRequest: []int64{1174, 1174}, // 1024 (c1) + 100 (PerContainer) + 100/2 (PerPod) = 1174Mi
		},
		{
			name:                 "combined direct and overhead mappings",
			resourceSliceDevices: combinedDevices,
			containers: []draContainerInfo{
				{Name: "c1", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem, claimCPUMem2}},
				{Name: "c2", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem, claimCPUMem2}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 1000m (c1 来自 spec) + 1000m (c2 来自 spec) + 1000m (Direct CPU) + 100m (CPU Claim - PerPod) + 200m (CPU Claim - PerContainer * 2)
				// CPU limit: 2000m (c1 来自 spec) + 2000m (c2 来自 spec) + 1000m (Direct CPU) + 100m (CPU Claim - PerPod) + 200m (CPU Claim - PerContainer * 2)
				// Memory limit: 2048Mi (c1 来自 spec) + 2048Mi (c2 来自 spec) + 1000Mi (Direct Mem) + 100Mi (Memory Claim - PerPod) + 200Mi (Memory Claim - PerContainer * 2)
				CPUReq: "3300m",
				CPULim: "5300m",
				MemLim: "5396Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 1000m (Direct CPU) + 100m (CPU Claim - PerContainer) + 100m (CPU Claim - PerPod)
				// Memory limit: 2048Mi (来自 spec) + 1000Mi (Direct Mem) + 100Mi (Memory Claim - PerContainer) + 100Mi (Memory Claim - PerPod)
				{CPUReq: "1000m", CPULim: "3200m", MemLim: "3248Mi"},
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 1000m (Direct CPU) + 100m (CPU Claim - PerContainer) + 100m (CPU Claim - PerPod)
				// Memory limit: 2048Mi (来自 spec) + 1000Mi (Direct Mem) + 100Mi (Memory Claim - PerContainer) + 100Mi (Memory Claim - PerPod)
				{CPUReq: "1000m", CPULim: "3200m", MemLim: "3248Mi"},
			},
			expectedContainersScoreMemRequest: []int64{1674, 1674}, // 1024 (c1) + 1000/2 (Direct Mem) + 100 (PerContainer) + 100/2 (PerPod) = 1674Mi
		},
		{
			name:                 "single device with both mapping and overhead",
			resourceSliceDevices: mappingAndOverheadDevice,
			containers: []draContainerInfo{
				{Name: "c1", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
				{Name: "c2", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 1000m (c1 来自 spec) + 1000m (c2 来自 spec) + 1000m (Direct CPU) + 100m (CPU Claim - PerPod) + 200m (CPU Claim - PerContainer * 2)
				// CPU limit: 2000m (c1 来自 spec) + 2000m (c2 来自 spec) + 1000m (Direct CPU) + 100m (CPU Claim - PerPod) + 200m (CPU Claim - PerContainer * 2)
				// Memory limit: 2048Mi (c1 来自 spec) + 2048Mi (c2 来自 spec) + 1000Mi (Direct Mem) + 100Mi (Memory Claim - PerPod) + 200Mi (Memory Claim - PerContainer * 2)
				CPUReq: "3300m",
				CPULim: "5300m",
				MemLim: "5396Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 1000m (Direct CPU) + 100m (CPU Claim - PerContainer) + 100m (CPU Claim - PerPod)
				// Memory limit: 2048Mi (来自 spec) + 1000Mi (Direct Mem) + 100Mi (Memory Claim - PerContainer) + 100Mi (Memory Claim - PerPod)
				{CPUReq: "1000m", CPULim: "3200m", MemLim: "3248Mi"},
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 1000m (Direct CPU) + 100m (CPU Claim - PerContainer) + 100m (CPU Claim - PerPod)
				// Memory limit: 2048Mi (来自 spec) + 1000Mi (Direct Mem) + 100Mi (Memory Claim - PerContainer) + 100Mi (Memory Claim -PerPod)
				{CPUReq: "1000m", CPULim: "3200m", MemLim: "3248Mi"},
			},
			expectedContainersScoreMemRequest: []int64{1674, 1674}, // 1024 (c1) + 1000/2 (Direct Mem) + 100 (PerContainer) + 100/2 (PerPod) = 1674Mi
		},
		{
			name:                 "unreferenced overhead claim",
			resourceSliceDevices: overheadDevices,
			unreferencedClaims:   []*resourceapi.ResourceClaim{claimCPUMem},
			containers: []draContainerInfo{
				{Name: "c1", Resources: res1CPU1GiCg},
				{Name: "c2", Resources: res1CPU1GiCg},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 1000m (c1 来自 spec) + 1000m (c2 来自 spec) + 100m (CPU Claim - PerPod)
				// CPU limit: 2000m (c1 来自 spec) + 2000m (c2 来自 spec) + 100m (CPU Claim - PerPod)
				// Memory limit: 2048Mi (c1 来自 spec) + 2048Mi (c2 来自 spec) + 100Mi (Memory Claim - PerPod)
				CPUReq: "2100m",
				CPULim: "4100m",
				MemLim: "4196Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2 (来自 spec - 未膨胀)
				// Memory limit: 2Gi (来自 spec - 未膨胀)
				{CPUReq: "1000m", CPULim: "2", MemLim: "2Gi"},
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2 (来自 spec - 未膨胀)
				// Memory limit: 2Gi (来自 spec - 未膨胀)
				{CPUReq: "1000m", CPULim: "2", MemLim: "2Gi"},
			},
			expectedContainersScoreMemRequest: []int64{1024, 1024}, // 1024 (c1) + 0 (DRA share) = 1024Mi
		},
		{
			name:                 "pod-level resources specified, container-level limits omitted",
			resourceSliceDevices: directDevices,
			podResources:         res2CPU2GiCg,
			containers: []draContainerInfo{
				{Name: "c1", Resources: nil, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
				{Name: "c2", Resources: nil, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 2000m (来自 pod 级 spec)
				// CPU limit: 4000m (来自 pod 级 spec)
				// Memory limit: 4096Mi (来自 pod 级 spec)
				CPUReq: "2000m",
				CPULim: "4000m",
				MemLim: "4096Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 空
				// CPU limit: 4 (直接继承 pod 级限制)
				// Memory limit: 4Gi (直接继承 pod 级限制)
				{CPULim: "4", MemLim: "4Gi"},
				// CPU request: 空
				// CPU limit: 4 (直接继承 pod 级限制)
				// Memory limit: 4Gi (直接继承 pod 级限制)
				{CPULim: "4", MemLim: "4Gi"},
			},
			expectedContainersScoreMemRequest: []int64{1024, 1024}, // pod 级 memory request (2048Mi) / 2 = 1024Mi
		},
		{
			name:                 "pod-level resources specified, container-level limits specified",
			resourceSliceDevices: overheadDevices,
			podResources:         res2500m2500MiCg,
			containers: []draContainerInfo{
				{Name: "c1", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
				{Name: "c2", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 2500m (来自 pod 级 spec)
				// CPU limit: 4000m (来自 pod 级 spec)
				// Memory limit: 4096Mi (来自 pod 级 spec)
				CPUReq: "2500m",
				CPULim: "4000m",
				MemLim: "4096Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 100m (CPU Claim - PerContainer) + 100m (CPU Claim - PerPod)
				// Memory limit: 2048Mi (来自 spec) + 100Mi (Memory Claim - PerContainer) + 100Mi (Memory Claim - PerPod)
				{CPUReq: "1000m", CPULim: "2200m", MemLim: "2248Mi"},
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec) + 100m (CPU Claim - PerContainer) + 100m (CPU Claim - PerPod)
				// Memory limit: 2048Mi (来自 spec) + 100Mi (Memory Claim - PerContainer) + 100Mi (Memory Claim - PerPod)
				{CPUReq: "1000m", CPULim: "2200m", MemLim: "2248Mi"},
			},
			expectedContainersScoreMemRequest: []int64{1250, 1250}, // c_effective (1174) + 剩余 ((2500-2348)/2) = 1174 + 76 = 1250Mi
		},
		{
			name:                 "hugepages overhead mapping",
			resourceSliceDevices: hugepageDevices,
			containers: []draContainerInfo{
				{Name: "c1", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimHugepages}, HugepageReqLim: "2Mi"},
				{Name: "c2", Resources: res1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimHugepages}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 1000m (c1 来自 spec) + 1000m (c2 来自 spec)
				// CPU limit: 2000m (c1 来自 spec) + 2000m (c2 来自 spec)
				// Memory limit: 2048Mi (c1 来自 spec) + 2048Mi (c2 来自 spec)
				CPUReq: "2000m",
				CPULim: "4000m",
				MemLim: "4096Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec)
				// Memory limit: 2Gi (来自 spec)
				{CPUReq: "1000m", CPULim: "2000m", MemLim: "2Gi"},
				// CPU request: 1000m (来自 spec)
				// CPU limit: 2000m (来自 spec)
				// Memory limit: 2Gi (来自 spec)
				{CPUReq: "1000m", CPULim: "2000m", MemLim: "2Gi"},
			},
			expectedContainersScoreMemRequest: []int64{1024, 1024},
			requiresHugepages:                 true,
			expectedPodHugepagesLimit:         "211812352", // Sum(limits) + PerPod (100Mi) + PerContainer*2 (100Mi) = 2Mi + 0 + 100Mi + 100Mi = 202Mi (211812352 bytes)
			expectedContainerHugepagesLimits: []string{
				"159383552", // Container 1: c1_lim (2Mi) + PerContainer (50Mi) + PerPod (100Mi) = 152Mi (159383552 bytes)
				"157286400", // Container 2: c2_lim (0) + PerContainer (50Mi) + PerPod (100Mi) = 150Mi (157286400 bytes)
			},
		},
		{
			name:                 "container-level requests and limits omitted (BestEffort)",
			resourceSliceDevices: directDevices,
			containers: []draContainerInfo{
				{Name: "c1", Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
				{Name: "c2", Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				CPUReq: "",
				CPULim: "",
				MemLim: "",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				{CPUReq: "", CPULim: "", MemLim: ""},
				{CPUReq: "", CPULim: "", MemLim: ""},
			},
			expectedContainersScoreMemRequest: []int64{0, 0},
		},
		{
			name:                 "container-level CPU/Memory limits omitted (Burstable)",
			resourceSliceDevices: directDevices,
			containers: []draContainerInfo{
				{Name: "c1", Resources: req1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
				{Name: "c2", Resources: req1CPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 1000m (c1 来自 spec) + 1000m (c2 来自 spec) + 1000m (DRA Direct CPU)
				CPUReq: "3000m",
				CPULim: "",
				MemLim: "",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 1000m (来自 spec)
				{CPUReq: "1000m", CPULim: "", MemLim: ""},
				// CPU request: 1000m (来自 spec)
				{CPUReq: "1000m", CPULim: "", MemLim: ""},
			},
			expectedContainersScoreMemRequest: []int64{1524, 1524}, // 1024 (c1) + 1000/2 (Direct memory) = 1524Mi
		},
		{
			name:                 "hugepages overhead mapping with container limits omitted",
			resourceSliceDevices: hugepageDevices,
			containers: []draContainerInfo{
				{Name: "c1", Claims: []*resourceapi.ResourceClaim{claimHugepages}},
				{Name: "c2", Claims: []*resourceapi.ResourceClaim{claimHugepages}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				CPUReq: "",
				CPULim: "",
				MemLim: "",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				{CPUReq: "", CPULim: "", MemLim: ""},
				{CPUReq: "", CPULim: "", MemLim: ""},
			},
			expectedContainersScoreMemRequest: []int64{0, 0},
			requiresHugepages:                 true,
			expectedPodHugepagesLimit:         "209715200", // Sum(limits) + PerPod (100Mi) + PerContainer*2 (100Mi) = 0 + 100Mi + 100Mi = 200Mi (209715200 bytes)
			expectedContainerHugepagesLimits: []string{
				"157286400", // Container 1: c1_lim (0) + claim - PerContainer (50Mi) + claim - PerPod (100Mi) = 150Mi
				"157286400", // Container 2: c2_lim (0) + claim - PerContainer (50Mi) + claim -PerPod (100Mi) = 150Mi
			},
		},
		{
			name:                 "Guaranteed QoS pod with direct mapping",
			resourceSliceDevices: directDevices,
			containers: []draContainerInfo{
				{Name: "c1", Resources: res1CPU1GiGuaranteed, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
				{Name: "c2", Resources: res1CPU1GiGuaranteed, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 1000m (c1 来自 spec) + 1000m (c2 来自 spec) + 1000m (DRA Direct CPU)
				// CPU limit: 1000m (c1 来自 spec) + 1000m (c2 来自 spec) + 1000m (DRA Direct CPU)
				// Memory limit: 1024Mi (c1 来自 spec) + 1024Mi (c2 来自 spec) + 1000Mi (DRA Direct memory)
				CPUReq: "3000m",
				CPULim: "3000m",
				MemLim: "3048Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 1000m (来自 spec)
				// CPU limit: 1000m (来自 spec) + 1000m (DRA Direct CPU)
				// Memory limit: 1024Mi (来自 spec) + 1000Mi (DRA Direct memory)
				{CPUReq: "1000m", CPULim: "2000m", MemLim: "2024Mi"},
				// CPU request: 1000m (来自 spec)
				// CPU limit: 1000m (来自 spec) + 1000m (DRA Direct CPU)
				// Memory limit: 1024Mi (来自 spec) + 1000Mi (DRA Direct memory)
				{CPUReq: "1000m", CPULim: "2000m", MemLim: "2024Mi"},
			},
			expectedContainersScoreMemRequest: []int64{0, 0},
		},
		{
			name:                 "direct mapping multiple claims",
			resourceSliceDevices: directDevices2,
			containers: []draContainerInfo{
				{Name: "c1", Resources: res100mCPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
				{Name: "c2", Resources: res100mCPU1GiCg, Claims: []*resourceapi.ResourceClaim{claimCPUMem2}},
			},
			expectedPodCgroup: cgroups.ContainerResources{
				// CPU request: 100m (c1 来自 spec) + 100m (c2 来自 spec) + 100m (direct1) + 100m (direct2)
				// CPU limit: 200m (c1 来自 spec) + 200m (c2 来自 spec) + 100m (direct1) + 100m (direct2)
				// Memory limit: 2048Mi (c1 来自 spec) + 2048Mi (c2 来自 spec) + 1000Mi (direct1) + 2000Mi (direct2)
				CPUReq: "400m",
				CPULim: "600m",
				MemLim: "7096Mi",
			},
			expectedContainersCgroup: []cgroups.ContainerResources{
				// CPU request: 100m (来自 spec)
				// CPU limit: 200m (来自 spec) + 100m (direct1)
				// Memory limit: 2048Mi (来自 spec) + 1000Mi (direct1)
				{CPUReq: "100m", CPULim: "300m", MemLim: "3048Mi"},
				// CPU request: 100m (来自 spec)
				// CPU limit: 200m (来自 spec) + 100m (direct2)
				// Memory limit: 2048Mi (来自 spec) + 2000Mi (direct2)
				{CPUReq: "100m", CPULim: "300m", MemLim: "4048Mi"},
			},
			expectedContainersScoreMemRequest: []int64{2024, 3024}, // c1: 1024 + 1000Mi = 2024Mi, c2: 1024 + 2000Mi = 3024Mi
		},
	}

	for _, tc := range tests {
		ginkgo.It(fmt.Sprintf("verifies cgroup inflation and container OOM adjustment with %s", tc.name), func(ctx context.Context) {
			tCtx := f.TContext(ctx)
			nodes := drautils.NewNodesNow(tCtx, 1, 4)
			if tc.requiresHugepages {
				node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, nodes.NodeNames[0], metav1.GetOptions{})
				framework.ExpectNoError(err)
				if limit, exists := node.Status.Allocatable[v1.ResourceName("hugepages-2Mi")]; !exists || limit.IsZero() {
					ginkgo.Skip("Skipping hugepages test because Node does not support hugepages-2Mi")
				}
			}
			driver := drautils.NewDriverInstance(tCtx)
			b := drautils.NewBuilderNow(tCtx, driver)

			// 使用这些自定义设备运行 driver
			driverResources := map[string]resourceslice.DriverResources{
				nodes.NodeNames[0]: {
					Pools: map[string]resourceslice.Pool{
						nodes.NodeNames[0]: {
							Slices: []resourceslice.Slice{{
								Devices: tc.resourceSliceDevices,
							}},
						},
					},
				},
			}
			driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes, driverResources)

			// 创建 claim 和 class
			createdClaims := createClaims(tCtx, b, tc.containers, tc.unreferencedClaims)

			pod := makeTestPod(tc.podResources, tc.containers, tc.unreferencedClaims, createdClaims)
			pod.Namespace = f.Namespace.Name

			podJSON, _ := json.Marshal(pod)
			tCtx.Logf("Pod JSON to create: %s", string(podJSON))

			ginkgo.By("creating pod and waiting for it to be running")
			podClient := e2epod.NewPodClient(f)
			pod = podClient.CreateSync(ctx, pod)

			// 验证节点上的 Pod 级 cgroup
			ginkgo.By("verifying pod cgroup limits on the node")
			err := cgroups.VerifyPodCgroups(ctx, f, pod, &tc.expectedPodCgroup)
			framework.ExpectNoError(err)

			ginkgo.By("verifying containers cgroup limits on the node")
			onCgroupV2 := cgroups.IsPodOnCgroupv2Node(f, pod.Name, pod.Spec.Containers[0].Name)
			for i, container := range pod.Spec.Containers {
				expectedContainer := &v1.Container{
					Name:      container.Name,
					Resources: *tc.expectedContainersCgroup[i].ResourceRequirements(),
				}
				if expectedContainer.Resources.Requests == nil {
					expectedContainer.Resources.Requests = container.Resources.Requests
				}
				err = cgroups.VerifyContainerCgroupValues(ctx, f, pod, expectedContainer, onCgroupV2)
				framework.ExpectNoError(err)
			}

			if tc.requiresHugepages {
				ginkgo.By("verifying pod hugepages limits on the node")
				err = cgroups.VerifyPodHugepagesLimit(ctx, f, pod, "2MB", tc.expectedPodHugepagesLimit, onCgroupV2)
				framework.ExpectNoError(err)

				ginkgo.By("verifying containers hugepages limits on the node")
				for i, container := range pod.Spec.Containers {
					err = cgroups.VerifyContainerHugepagesLimit(ctx, f, pod, container.Name, "2MB", tc.expectedContainerHugepagesLimits[i], onCgroupV2)
					framework.ExpectNoError(err)
				}
			}

			ginkgo.By("verifying containers oom_score_adj matches QoS rules with overhead")
			// 获取节点容量以检查精确调整量
			node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
			framework.ExpectNoError(err)
			nodeMemBytes := node.Status.Capacity.Memory().Value()

			qosClass := v1qos.GetPodQOS(pod)
			for i, container := range pod.Spec.Containers {
				var expectedScore int64
				switch qosClass {
				case v1.PodQOSGuaranteed:
					expectedScore = -997
				case v1.PodQOSBestEffort:
					expectedScore = 1000
				case v1.PodQOSBurstable:
					expectedScore = computeExpectedOomScoreAdj(tc.expectedContainersScoreMemRequest[i], nodeMemBytes)
				}
				err = cgroups.VerifyOomScoreAdjValue(f, pod, container.Name, fmt.Sprintf("%d", expectedScore))
				framework.ExpectNoError(err)
			}

			ginkgo.By("deleting pods")
			delErr := e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
			framework.ExpectNoError(delErr, "failed to delete pod %s", delErr)
		})
	}
}

func doNodeAllocatableResizeTests(f *framework.Framework) {
	tests := []struct {
		name                                            string
		resourceSliceDevices                            []resourceapi.Device
		podResources                                    *cgroups.ContainerResources
		desiredPodResources                             *cgroups.ContainerResources
		containers                                      []draContainerInfo
		desiredContainers                               []draContainerInfo
		unreferencedClaims                              []*resourceapi.ResourceClaim
		expectedPodCgroupAfterResize                    cgroups.ContainerResources
		expectedContainersCgroupAfterResize             []cgroups.ContainerResources // 每个容器
		expectedPodAllocatedResourcesAfterResize        v1.ResourceList
		expectedContainersAllocatedResourcesAfterResize []v1.ResourceList
	}{
		{
			name:                 "direct mappings with resize",
			resourceSliceDevices: directDevices,
			containers: []draContainerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "500m", CPULim: "1000m", MemReq: "500Mi", MemLim: "1000Mi"}, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			desiredContainers: []draContainerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "500m", CPULim: "1500m", MemReq: "1000Mi", MemLim: "2000Mi"}, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			// CPU request: 500m (来自调整后的 spec) + 1000m (DRA Direct CPU)
			// CPU limit: 1500m (来自调整后的 spec) + 1000m (DRA Direct CPU)
			// Memory limit: 2000Mi (来自调整后的 spec) + 1000Mi (DRA Direct memory)
			expectedPodCgroupAfterResize: cgroups.ContainerResources{CPUReq: "1500m", CPULim: "2500m", MemLim: "3000Mi"},
			expectedContainersCgroupAfterResize: []cgroups.ContainerResources{
				{CPUReq: "500m", CPULim: "2500m", MemLim: "3000Mi"},
			},
			// Pod 级 allocated resources 包含 DRA 资源
			expectedPodAllocatedResourcesAfterResize: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1500m"),
				v1.ResourceMemory: resource.MustParse("2000Mi"),
			},
			// 容器级 allocated resources 不包含 DRA 资源
			expectedContainersAllocatedResourcesAfterResize: []v1.ResourceList{
				{
					v1.ResourceCPU:    resource.MustParse("500m"),
					v1.ResourceMemory: resource.MustParse("1000Mi"),
				},
			},
		},
		{
			name:                 "overhead mappings with resize",
			resourceSliceDevices: overheadDevices,
			containers: []draContainerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "1", CPULim: "2", MemReq: "1Gi", MemLim: "2Gi"}, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			desiredContainers: []draContainerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "1500m", CPULim: "2500m", MemReq: "1500Mi", MemLim: "3000Mi"}, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			// CPU request: 1500m (来自调整后的 spec) + 100m (DRA PerPod CPU) + 100m (DRA PerContainer CPU)
			// CPU limit: 2500m (来自调整后的 spec) + 100m (DRA PerPod CPU) + 100m (DRA PerContainer CPU)
			// Memory limit: 3000Mi (来自调整后的 spec) + 100Mi (DRA PerPod memory) + 100Mi (DRA PerContainer memory)
			expectedPodCgroupAfterResize: cgroups.ContainerResources{CPUReq: "1700m", CPULim: "2700m", MemLim: "3200Mi"},
			expectedContainersCgroupAfterResize: []cgroups.ContainerResources{
				{CPUReq: "1500m", CPULim: "2700m", MemLim: "3200Mi"},
			},
			expectedPodAllocatedResourcesAfterResize: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1700m"),
				v1.ResourceMemory: resource.MustParse("1700Mi"),
			},
			expectedContainersAllocatedResourcesAfterResize: []v1.ResourceList{
				{
					v1.ResourceCPU:    resource.MustParse("1500m"),
					v1.ResourceMemory: resource.MustParse("1500Mi"),
				},
			},
		},
		{
			name:                 "pod-level resources resize with container level request/limit omitted",
			resourceSliceDevices: directDevices,
			podResources:         &cgroups.ContainerResources{CPUReq: "2", CPULim: "4", MemReq: "2Gi", MemLim: "4Gi"},
			containers: []draContainerInfo{
				{Name: "c1", Resources: nil, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			desiredPodResources: &cgroups.ContainerResources{CPUReq: "3", CPULim: "5", MemReq: "3Gi", MemLim: "5Gi"},
			desiredContainers: []draContainerInfo{
				{Name: "c1", Resources: nil, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			// CPU request: 3000m (来自调整后的 pod 级 spec)
			// CPU limit: 5000m (来自调整后的 pod 级 spec)
			// Memory limit: 5Gi (来自调整后的 pod 级 spec)
			expectedPodCgroupAfterResize: cgroups.ContainerResources{CPUReq: "3000m", CPULim: "5000m", MemLim: "5120Mi"},
			// 容器 spec 未显式指定 limit 时，容器级 limit 默认为 pod 级值
			expectedContainersCgroupAfterResize: []cgroups.ContainerResources{
				{CPULim: "5", MemLim: "5Gi"},
			},
			// Pod status 中的 allocated resources 完全基于 pod 级资源
			expectedPodAllocatedResourcesAfterResize: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("3Gi"),
			},
		},
		{
			name:                 "Pod level resize, container level request/limit present",
			resourceSliceDevices: directDevices,
			podResources:         &cgroups.ContainerResources{CPUReq: "2", CPULim: "4", MemReq: "2Gi", MemLim: "4Gi"},
			containers: []draContainerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "1", CPULim: "2", MemReq: "1Gi", MemLim: "2Gi"}, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			desiredPodResources: &cgroups.ContainerResources{CPUReq: "3", CPULim: "6", MemReq: "3Gi", MemLim: "6Gi"},
			desiredContainers: []draContainerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "1", CPULim: "2", MemReq: "1Gi", MemLim: "2Gi"}, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			expectedPodCgroupAfterResize: cgroups.ContainerResources{CPUReq: "3000m", CPULim: "6000m", MemLim: "6Gi"},
			expectedContainersCgroupAfterResize: []cgroups.ContainerResources{
				{CPUReq: "1000m", CPULim: "3000m", MemLim: "3048Mi"},
			},
			expectedPodAllocatedResourcesAfterResize: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("3Gi"),
			},
			expectedContainersAllocatedResourcesAfterResize: []v1.ResourceList{
				{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
		},
		{
			name:                 "pod level resources present, container level resize",
			resourceSliceDevices: directDevices,
			podResources:         &cgroups.ContainerResources{CPUReq: "2", CPULim: "4", MemReq: "2Gi", MemLim: "4Gi"},
			containers: []draContainerInfo{
				{Name: "c1", Resources: nil, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			desiredPodResources: &cgroups.ContainerResources{CPUReq: "2", CPULim: "4", MemReq: "2Gi", MemLim: "4Gi"},
			desiredContainers: []draContainerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "1", CPULim: "2", MemReq: "1Gi", MemLim: "2Gi"}, Claims: []*resourceapi.ResourceClaim{claimCPUMem}},
			},
			// 容器 resize 后 Pod cgroup 保持不变
			expectedPodCgroupAfterResize: cgroups.ContainerResources{CPUReq: "2000m", CPULim: "4000m", MemLim: "4Gi"},
			// resize 后容器 cgroup 更新并包含 DRA
			expectedContainersCgroupAfterResize: []cgroups.ContainerResources{
				{CPUReq: "1000m", CPULim: "3000m", MemLim: "3048Mi"},
			},
			// Pod 级 allocated resources 仅基于 pod 级资源
			expectedPodAllocatedResourcesAfterResize: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("2"),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			// 容器级 allocated resources 包含调整后的 spec 值（不含 DRA）
			expectedContainersAllocatedResourcesAfterResize: []v1.ResourceList{
				{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
		},
	}

	for _, tc := range tests {
		ginkgo.It(fmt.Sprintf("verifies cgroup settings and pod status after resize with %s", tc.name), func(ctx context.Context) {
			tCtx := f.TContext(ctx)
			nodes := drautils.NewNodesNow(tCtx, 1, 4)
			driver := drautils.NewDriverInstance(tCtx)
			b := drautils.NewBuilderNow(tCtx, driver)

			driverResources := map[string]resourceslice.DriverResources{
				nodes.NodeNames[0]: {
					Pools: map[string]resourceslice.Pool{
						nodes.NodeNames[0]: {
							Slices: []resourceslice.Slice{{
								Devices: tc.resourceSliceDevices,
							}},
						},
					},
				},
			}
			driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes, driverResources)

			createdClaims := createClaims(tCtx, b, tc.containers, tc.unreferencedClaims)

			pod := makeTestPod(tc.podResources, tc.containers, tc.unreferencedClaims, createdClaims)
			pod.Namespace = f.Namespace.Name

			podJSON, _ := json.Marshal(pod)
			tCtx.Logf("Pod JSON to create: %s", string(podJSON))

			ginkgo.By("creating pod and waiting for it to be running")
			podClient := e2epod.NewPodClient(f)
			pod = podClient.CreateSync(ctx, pod)

			originalContainers := mapToResizableContainerInfo(tc.containers)
			desiredContainers := mapToResizableContainerInfo(tc.desiredContainers)

			var originalPodResources *v1.ResourceRequirements
			if tc.podResources != nil {
				originalPodResources = tc.podResources.ResourceRequirements()
			}
			var desiredPodResources *v1.ResourceRequirements
			if tc.desiredPodResources != nil {
				desiredPodResources = tc.desiredPodResources.ResourceRequirements()
			}
			patch := podresize.MakeResizePatch(originalContainers, desiredContainers, originalPodResources, desiredPodResources)

			ginkgo.By("patching the pod for resize")
			patchedPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Patch(ctx, pod.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "resize")
			framework.ExpectNoError(err)

			ginkgo.By("waiting for resize actuation to complete")
			resizedPod := podresize.WaitForPodResizeActuation(ctx, f, podClient, pod, desiredContainers)

			ginkgo.By("verifying updated pod cgroup limits after resize")
			err = cgroups.VerifyPodCgroups(ctx, f, resizedPod, &tc.expectedPodCgroupAfterResize)
			framework.ExpectNoError(err)

			ginkgo.By("verifying updated container cgroup limits after resize")
			onCgroupV2 := cgroups.IsPodOnCgroupv2Node(f, resizedPod.Name, resizedPod.Spec.Containers[0].Name)
			for i, container := range resizedPod.Spec.Containers {
				expectedContainer := &v1.Container{
					Name:      container.Name,
					Resources: *tc.expectedContainersCgroupAfterResize[i].ResourceRequirements(),
				}
				if expectedContainer.Resources.Requests == nil {
					expectedContainer.Resources.Requests = container.Resources.Requests
				}
				err = cgroups.VerifyContainerCgroupValues(ctx, f, resizedPod, expectedContainer, onCgroupV2)
				framework.ExpectNoError(err)
			}

			ginkgo.By("verifying pod status updates match spec after resize")
			framework.ExpectNoError(verifyDRAPodLevelStatusResources(resizedPod, tc.expectedPodAllocatedResourcesAfterResize))

			ginkgo.By("verifying pod spec resources after patch")
			podresize.VerifyPodResources(patchedPod, desiredContainers, desiredPodResources)

			ginkgo.By("deleting pods")
			delErr := e2epod.DeletePodWithWait(ctx, f.ClientSet, resizedPod)
			framework.ExpectNoError(delErr, "failed to delete pod %s", delErr)
		})
	}
}

func makeTestPod(podResources *cgroups.ContainerResources, containers []draContainerInfo, unreferencedClaims []*resourceapi.ResourceClaim, createdClaims map[string]*resourceapi.ResourceClaim) *v1.Pod {
	var testContainers []v1.Container
	for _, container := range containers {
		c := cgroups.MakeContainerWithResources(container.Name, container.Resources, "trap exit TERM; while true; do sleep 1; done")
		if len(container.Claims) > 0 {
			c.Resources.Claims = make([]v1.ResourceClaim, len(container.Claims))
			for idx, claim := range container.Claims {
				c.Resources.Claims[idx] = v1.ResourceClaim{Name: claim.Name}
			}
		}
		if container.HugepageReqLim != "" {
			if c.Resources.Limits == nil {
				c.Resources.Requests = make(v1.ResourceList)
				c.Resources.Limits = make(v1.ResourceList)
			}
			c.Resources.Requests[v1.ResourceName("hugepages-2Mi")] = resource.MustParse(container.HugepageReqLim)
			c.Resources.Limits[v1.ResourceName("hugepages-2Mi")] = resource.MustParse(container.HugepageReqLim)
		}
		testContainers = append(testContainers, c)
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tester-1",
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers:    testContainers,
		},
	}
	cgroups.ConfigureHostPathForPodCgroup(pod)

	if podResources != nil {
		pod.Spec.Resources = podResources.ResourceRequirements()
	}

	uniqueClaims := sets.New[string]()
	for _, container := range containers {
		for _, claim := range container.Claims {
			uniqueClaims.Insert(claim.Name)
		}
	}
	for _, claim := range unreferencedClaims {
		uniqueClaims.Insert(claim.Name)
	}

	if uniqueClaims.Len() > 0 {
		sortedClaims := uniqueClaims.UnsortedList()
		slices.Sort(sortedClaims)
		pod.Spec.ResourceClaims = make([]v1.PodResourceClaim, len(sortedClaims))
		for idx, claimName := range sortedClaims {
			var actualClaimName *string
			if actualClaim, exists := createdClaims[claimName]; exists {
				actualClaimName = &actualClaim.Name
			}
			pod.Spec.ResourceClaims[idx] = v1.PodResourceClaim{
				Name:              claimName,
				ResourceClaimName: actualClaimName,
			}
		}
	}

	return pod
}

func computeExpectedOomScoreAdj(memReqMi int64, nodeMemBytes int64) int64 {
	// 这实现了 Kubelet 的 GetContainerOOMScoreAdjust()（位于 pkg/kubelet/qos/policy.go）中的 OOM score 计算公式。
	score := 1000 - (1000 * memReqMi * 1024 * 1024 / nodeMemBytes)
	if score < 2 {
		return 2
	}
	return score
}

func createClaims(tCtx ktesting.TContext, b *drautils.Builder, containers []draContainerInfo, unreferencedClaims []*resourceapi.ResourceClaim) map[string]*resourceapi.ResourceClaim {
	uniqueClaims := make(map[string]bool)
	for _, container := range containers {
		for _, claim := range container.Claims {
			uniqueClaims[claim.Name] = true
		}
	}
	for _, claim := range unreferencedClaims {
		uniqueClaims[claim.Name] = true
	}

	createdClaims := make(map[string]*resourceapi.ResourceClaim)
	for templateName := range uniqueClaims {
		createdClaimObj := b.Create(tCtx, b.ExternalClaim())[0]
		createdClaims[templateName] = createdClaimObj.(*resourceapi.ResourceClaim)
	}
	return createdClaims
}

func verifyDRAPodLevelStatusResources(gotPod *v1.Pod, wantAllocatedResources v1.ResourceList) error {
	ginkgo.GinkgoHelper()
	var errs []error
	if wantAllocatedResources != nil {
		if err := framework.Gomega().Expect(gotPod.Status.AllocatedResources).To(gomega.BeComparableTo(wantAllocatedResources)); err != nil {
			errs = append(errs, fmt.Errorf("pod[%s] status allocatedResources mismatch: %w", gotPod.Name, err))
		}
	}
	return errors.NewAggregate(errs)
}

func mapToResizableContainerInfo(containers []draContainerInfo) []podresize.ResizableContainerInfo {
	res := make([]podresize.ResizableContainerInfo, len(containers))
	for i, c := range containers {
		res[i] = podresize.ResizableContainerInfo{
			Name:      c.Name,
			Resources: c.Resources,
		}
	}
	return res
}

var _ = SIGDescribe("DRA Node Allocatable Resources", framework.WithSerial(), feature.DynamicResourceAllocation, framework.WithFeatureGate(features.DRANodeAllocatableResources), framework.WithFeatureGate(features.InPlacePodLevelResourcesVerticalScaling), func() {
	f := framework.NewDefaultFramework("dra-node-allocatable-resources")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func(ctx context.Context) {
		if framework.NodeOSDistroIs("windows") {
			e2eskipper.Skipf("not supported on windows -- skipping")
		}
	})

	doNodeAllocatableCgroupsTests(f)
	doNodeAllocatableResizeTests(f)
})
