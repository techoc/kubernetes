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

package cpumanager

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/admission"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/initoption"
	"k8s.io/utils/cpuset"
)

// containerRestartPolicyAlways 是用于构造可重启 init 容器的便捷别名
var (
	containerRestartPolicyAlways = v1.ContainerRestartPolicyAlways
)

// newNUMAAffinity 根据给定的 NUMA 节点编号构造位掩码；构造失败直接 panic，
// 因为它只用于测试中的固定输入
func newNUMAAffinity(bits ...int) bitmask.BitMask {
	affinity, err := bitmask.NewBitMask(bits...)
	if err != nil {
		panic(err)
	}
	return affinity
}

// staticPolicyTest 描述一个 static 策略测试用例：输入拓扑、预留 CPU、初始状态与 Pod，
// 以及期望的错误、是否分配成功和期望的 CPU 集合
type staticPolicyTest struct {
	description     string
	topo            *topology.CPUTopology
	numReservedCPUs int
	reservedCPUs    *cpuset.CPUSet
	podUID          string
	options         map[string]string
	containerName   string
	stAssignments   state.ContainerCPUAssignments
	stDefaultCPUSet cpuset.CPUSet
	pod             *v1.Pod
	topologyHint    *topologymanager.TopologyHint
	expErr          error
	expCPUAlloc     bool
	expCSet         cpuset.CPUSet
}

// 这不是真正的 Clone()，因此称为 PseudoClone：部分对象仅被只读访问，无需深拷贝
// 因而这里对它们直接复用引用
func (spt staticPolicyTest) PseudoClone() staticPolicyTest {
	return staticPolicyTest{
		description:     spt.description,
		topo:            spt.topo, // accessed in read-only
		numReservedCPUs: spt.numReservedCPUs,
		podUID:          spt.podUID,
		options:         spt.options, // accessed in read-only
		containerName:   spt.containerName,
		stAssignments:   spt.stAssignments.Clone(),
		stDefaultCPUSet: spt.stDefaultCPUSet.Clone(),
		pod:             spt.pod, // accessed in read-only
		expErr:          spt.expErr,
		expCPUAlloc:     spt.expCPUAlloc,
		expCSet:         spt.expCSet.Clone(),
	}
}

// TestStaticPolicyName 验证 static 策略的名称固定为 "static"
func TestStaticPolicyName(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	policy, err := NewStaticPolicy(logger, topoSingleSocketHT, 1, cpuset.New(), topologymanager.NewFakeManager(logger), nil)
	if err != nil {
		t.Fatalf("NewStaticPolicy() failed: %v", err)
	}

	policyName := policy.Name()
	if policyName != "static" {
		t.Errorf("StaticPolicy Name() error. expected: static, returned: %v",
			policyName)
	}
}

// TestStaticPolicyStart 验证策略启动时对状态的校验：
// 非空默认 cpuset 保持不变、空 cpuset 会按拓扑重建、预留 CPU 缺失或与已分配重叠时报错
func TestStaticPolicyStart(t *testing.T) {
	testCases := []staticPolicyTest{
		{
			description: "non-corrupted state",
			topo:        topoDualSocketHT,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"0": cpuset.New(0),
				},
			},
			stDefaultCPUSet: cpuset.New(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			expCSet:         cpuset.New(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
		},
		{
			description:     "empty cpuset",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(),
			expCSet:         cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
		},
		{
			description:     "reserved cores 0 & 6 are not present in available cpuset",
			topo:            topoDualSocketHT,
			numReservedCPUs: 2,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1),
			expErr:          fmt.Errorf("not all reserved cpus: \"0,6\" are present in defaultCpuSet: \"0-1\""),
		},
		{
			description:     "some of reserved cores are present in available cpuset (StrictCPUReservationOption)",
			topo:            topoDualSocketHT,
			numReservedCPUs: 2,
			options:         map[string]string{StrictCPUReservationOption: "true"},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1),
			expErr:          fmt.Errorf("some of strictly reserved cpus: \"0\" are present in defaultCpuSet: \"0-1\""),
		},
		{
			description: "assigned core 2 is still present in available cpuset",
			topo:        topoDualSocketHT,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"0": cpuset.New(0, 1, 2),
				},
			},
			stDefaultCPUSet: cpuset.New(2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			expErr:          fmt.Errorf("pod: fakePod, container: 0 cpuset: \"0-2\" overlaps with default cpuset \"2-11\""),
		},
		{
			description: "assigned core 2 is still present in available cpuset (StrictCPUReservationOption)",
			topo:        topoDualSocketHT,
			options:     map[string]string{StrictCPUReservationOption: "true"},
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"0": cpuset.New(0, 1, 2),
				},
			},
			stDefaultCPUSet: cpuset.New(2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			expErr:          fmt.Errorf("pod: fakePod, container: 0 cpuset: \"0-2\" overlaps with default cpuset \"2-11\""),
		},
		{
			description: "core 12 is not present in topology but is in state cpuset",
			topo:        topoDualSocketHT,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"0": cpuset.New(0, 1, 2),
					"1": cpuset.New(3, 4),
				},
			},
			stDefaultCPUSet: cpuset.New(5, 6, 7, 8, 9, 10, 11, 12),
			expErr:          fmt.Errorf("current set of available CPUs \"0-11\" doesn't match with CPUs in state \"0-12\""),
		},
		{
			description: "core 11 is present in topology but is not in state cpuset",
			topo:        topoDualSocketHT,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"0": cpuset.New(0, 1, 2),
					"1": cpuset.New(3, 4),
				},
			},
			stDefaultCPUSet: cpuset.New(5, 6, 7, 8, 9, 10),
			expErr:          fmt.Errorf("current set of available CPUs \"0-11\" doesn't match with CPUs in state \"0-10\""),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.CPUManagerPolicyAlphaOptions, true)
			p, err := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, cpuset.New(), topologymanager.NewFakeManager(logger), testCase.options)
			if err != nil {
				t.Fatalf("NewStaticPolicy() failed: %v", err)
			}
			policy := p.(*staticPolicy)
			st := &mockState{
				assignments:   testCase.stAssignments,
				defaultCPUSet: testCase.stDefaultCPUSet,
			}
			err = policy.Start(logger, st)
			if !reflect.DeepEqual(err, testCase.expErr) {
				t.Errorf("StaticPolicy Start() error (%v). expected error: %v but got: %v",
					testCase.description, testCase.expErr, err)
			}
			if err != nil {
				return
			}

			if !testCase.stDefaultCPUSet.IsEmpty() {
				for cpuid := 1; cpuid < policy.topology.NumCPUs; cpuid++ {
					if !st.defaultCPUSet.Contains(cpuid) {
						t.Errorf("StaticPolicy Start() error. expected cpuid %d to be present in defaultCPUSet", cpuid)
					}
				}
			}
			if !st.GetDefaultCPUSet().Equals(testCase.expCSet) {
				t.Errorf("State CPUSet is different than expected. Have %q wants: %q", st.GetDefaultCPUSet(),
					testCase.expCSet)
			}

		})
	}
}

// TestStaticPolicyAdd 验证 Allocate 的 CPU 分配结果，覆盖单/双/四 Socket、
// 超线程、完整物理核约束、预留 CPU 以及按 Socket 对齐等多种拓扑与选项组合
func TestStaticPolicyAdd(t *testing.T) {
	var largeTopoCPUids []int
	var largeTopoSock0CPUids []int
	var largeTopoSock1CPUids []int
	largeTopo := *topoQuadSocketFourWayHT
	for cpuid, val := range largeTopo.CPUDetails {
		largeTopoCPUids = append(largeTopoCPUids, cpuid)
		if val.SocketID == 0 {
			largeTopoSock0CPUids = append(largeTopoSock0CPUids, cpuid)
		} else if val.SocketID == 1 {
			largeTopoSock1CPUids = append(largeTopoSock1CPUids, cpuid)
		}
	}
	largeTopoCPUSet := cpuset.New(largeTopoCPUids...)
	largeTopoSock0CPUSet := cpuset.New(largeTopoSock0CPUids...)
	largeTopoSock1CPUSet := cpuset.New(largeTopoSock1CPUids...)

	// 以下用例必须在任何策略选项下表现一致，
	// 因此下面会对选项做排列组合来验证这一点

	optionsInsensitiveTestCases := []staticPolicyTest{
		{
			description:     "GuPodMultipleCores, SingleSocketHT, ExpectAllocOneCore",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 1,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(2, 3, 6, 7),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 1, 4, 5),
			pod:             makePod("fakePod", "fakeContainer3", "2000m", "2000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 5),
		},
		{
			description:     "GuPodMultipleCores, DualSocketHT, ExpectAllocOneSocket",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(2),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod:             makePod("fakePod", "fakeContainer3", "6000m", "6000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 3, 5, 7, 9, 11),
		},
		{
			description:     "GuPodMultipleCores, DualSocketHT, ExpectAllocThreeCores",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(1, 5),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 2, 3, 4, 6, 7, 8, 9, 10, 11),
			pod:             makePod("fakePod", "fakeContainer3", "6000m", "6000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(2, 3, 4, 8, 9, 10),
		},
		{
			description:     "GuPodMultipleCores, DualSocketNoHT, ExpectAllocOneSocket",
			topo:            topoDualSocketNoHT,
			numReservedCPUs: 1,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 1, 3, 4, 5, 6, 7),
			pod:             makePod("fakePod", "fakeContainer1", "4000m", "4000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(4, 5, 6, 7),
		},
		{
			description:     "GuPodMultipleCores, DualSocketNoHT, ExpectAllocFourCores",
			topo:            topoDualSocketNoHT,
			numReservedCPUs: 1,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(4, 5),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 1, 3, 6, 7),
			pod:             makePod("fakePod", "fakeContainer1", "4000m", "4000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 3, 6, 7),
		},
		{
			description:     "GuPodMultipleCores, DualSocketHT, ExpectAllocOneSocketOneCore",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(2),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod:             makePod("fakePod", "fakeContainer3", "8000m", "8000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 3, 4, 5, 7, 9, 10, 11),
		},
		{
			description:     "NonGuPod, SingleSocketHT, NoAlloc",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 1,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			pod:             makePod("fakePod", "fakeContainer1", "1000m", "2000m"),
			expErr:          nil,
			expCPUAlloc:     false,
			expCSet:         cpuset.New(),
		},
		{
			description:     "GuPodNonIntegerCore, SingleSocketHT, NoAlloc",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 1,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			pod:             makePod("fakePod", "fakeContainer4", "977m", "977m"),
			expErr:          nil,
			expCPUAlloc:     false,
			expCSet:         cpuset.New(),
		},
		{
			// Socket 0 的 CPU 全部可用，而每个 Socket 上都有部分 CPU 已被分配，
			// 预期分配 Socket 0 的全部 CPU
			description: "GuPodMultipleCores, topoQuadSocketFourWayHT, ExpectAllocSock0",
			topo:        topoQuadSocketFourWayHT,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(3, 11, 4, 5, 6, 7),
				},
			},
			stDefaultCPUSet: largeTopoCPUSet.Difference(cpuset.New(3, 11, 4, 5, 6, 7)),
			pod:             makePod("fakePod", "fakeContainer5", "72000m", "72000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         largeTopoSock0CPUSet,
		},
		{
			// 三个 Socket 上各只有 2 个完整核以及部分零散 CPU 可用，
			// 预期分配这三个 Socket 上可用的 2 个完整核对应的 CPU
			description: "GuPodMultipleCores, topoQuadSocketFourWayHT, ExpectAllocAllFullCoresFromThreeSockets",
			topo:        topoQuadSocketFourWayHT,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": largeTopoCPUSet.Difference(cpuset.New(1, 25, 13, 38, 2, 9, 11, 35, 23, 48, 12, 51,
						53, 173, 113, 233, 54, 61)),
				},
			},
			stDefaultCPUSet: cpuset.New(1, 25, 13, 38, 2, 9, 11, 35, 23, 48, 12, 51, 53, 173, 113, 233, 54, 61),
			pod:             makePod("fakePod", "fakeCcontainer5", "12000m", "12000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 25, 13, 38, 11, 35, 23, 48, 53, 173, 113, 233),
		},
		{
			// Socket 1 的全部 CPU、1 个完整核以及部分零散 CPU 可用，
			// 预期分配 Socket 1 的全部 CPU 以及该完整核的兄弟超线程
			description: "GuPodMultipleCores, topoQuadSocketFourWayHT, ExpectAllocAllSock1+FullCore",
			topo:        topoQuadSocketFourWayHT,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": largeTopoCPUSet.Difference(largeTopoSock1CPUSet.Union(cpuset.New(10, 34, 22, 47, 53,
						173, 61, 181, 108, 228, 115, 235))),
				},
			},
			stDefaultCPUSet: largeTopoSock1CPUSet.Union(cpuset.New(10, 34, 22, 47, 53, 173, 61, 181, 108, 228,
				115, 235)),
			pod:         makePod("fakePod", "fakeContainer5", "76000m", "76000m"),
			expErr:      nil,
			expCPUAlloc: true,
			expCSet:     largeTopoSock1CPUSet.Union(cpuset.New(10, 34, 22, 47)),
		},
	}

	// 策略默认行为下的测试用例
	defaultOptionsTestCases := []staticPolicyTest{
		{
			description:     "GuPodSingleCore, SingleSocketHT, ExpectAllocOneCPU",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 1,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			pod:             makePod("fakePod", "fakeContainer2", "1000m", "1000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(4), // 预期分配该不完整核的兄弟超线程
		},
		{
			// 整个系统中仅剩不完整的核可用，
			// 预期把这些不完整核的 CPU 全部分配出去
			description: "GuPodMultipleCores, topoQuadSocketFourWayHT, ExpectAllocCPUs",
			topo:        topoQuadSocketFourWayHT,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": largeTopoCPUSet.Difference(cpuset.New(10, 11, 53, 37, 55, 67, 52)),
				},
			},
			stDefaultCPUSet: cpuset.New(10, 11, 53, 67, 52),
			pod:             makePod("fakePod", "fakeContainer5", "5000m", "5000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(10, 11, 53, 67, 52),
		},
		{
			description:     "GuPodSingleCore, SingleSocketHT, ExpectError",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 1,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			pod:             makePod("fakePod", "fakeContainer2", "8000m", "8000m"),
			expErr:          fmt.Errorf("not enough cpus available to satisfy request: requested=8, available=7"),
			expCPUAlloc:     false,
			expCSet:         cpuset.New(),
		},
		{
			description:     "GuPodMultipleCores, SingleSocketHT, ExpectSameAllocation",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 1,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer3": cpuset.New(2, 3, 6, 7),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 1, 4, 5),
			pod:             makePod("fakePod", "fakeContainer3", "4000m", "4000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(2, 3, 6, 7),
		},
		{
			description:     "GuPodMultipleCores, DualSocketHT, NoAllocExpectError",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(1, 2, 3),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 4, 5, 6, 7, 8, 9, 10, 11),
			pod:             makePod("fakePod", "fakeContainer5", "10000m", "10000m"),
			expErr:          fmt.Errorf("not enough cpus available to satisfy request: requested=10, available=8"),
			expCPUAlloc:     false,
			expCSet:         cpuset.New(),
		},
		{
			description:     "GuPodMultipleCores, SingleSocketHT, NoAllocExpectError",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 1,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(1, 2, 3, 4, 5, 6),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 7),
			pod:             makePod("fakePod", "fakeContainer5", "2000m", "2000m"),
			expErr:          fmt.Errorf("not enough cpus available to satisfy request: requested=2, available=1"),
			expCPUAlloc:     false,
			expCSet:         cpuset.New(),
		},
		{
			// 当前仅有 7 个 CPU 可用，
			// 而 Pod 请求 76 个核，
			// 可用 CPU 少于请求量，因此预期报错
			description: "GuPodMultipleCores, topoQuadSocketFourWayHT, NoAlloc",
			topo:        topoQuadSocketFourWayHT,
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": largeTopoCPUSet.Difference(cpuset.New(10, 11, 53, 37, 55, 67, 52)),
				},
			},
			stDefaultCPUSet: cpuset.New(10, 11, 53, 37, 55, 67, 52),
			pod:             makePod("fakePod", "fakeContainer5", "76000m", "76000m"),
			expErr:          fmt.Errorf("not enough cpus available to satisfy request: requested=76, available=7"),
			expCPUAlloc:     false,
			expCSet:         cpuset.New(),
		},
	}

	// FullPCPUsOnlyOption 的测试用例
	smtalignOptionTestCases := []staticPolicyTest{
		{
			description: "GuPodSingleCore, SingleSocketHT, ExpectAllocOneCPU",
			topo:        topoSingleSocketHT,
			options: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			numReservedCPUs: 1,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			pod:             makePod("fakePod", "fakeContainer2", "1000m", "1000m"),
			expErr:          SMTAlignmentError{RequestedCPUs: 1, CpusPerCore: 2},
			expCPUAlloc:     false,
			expCSet:         cpuset.New(), // 拒绝分配该不完整核的兄弟超线程
		},
		{
			// 测试 SMT 级别 != 2 的情况（x86_64 上通常为 2）
			description: "GuPodMultipleCores, topoQuadSocketFourWayHT, ExpectAllocOneCPUs",
			topo:        topoQuadSocketFourWayHT,
			options: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			numReservedCPUs: 8,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: largeTopoCPUSet,
			pod:             makePod("fakePod", "fakeContainer15", "15000m", "15000m"),
			expErr:          SMTAlignmentError{RequestedCPUs: 15, CpusPerCore: 4},
			expCPUAlloc:     false,
			expCSet:         cpuset.New(),
		},
		{
			description: "GuPodManyCores, topoDualSocketHT, ExpectDoNotAllocPartialCPU",
			topo:        topoDualSocketHT,
			options: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			numReservedCPUs: 2,
			reservedCPUs:    newCPUSetPtr(1, 6),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 2, 3, 4, 5, 7, 8, 9, 10, 11),
			pod:             makePod("fakePod", "fakeContainerBug113537_1", "10000m", "10000m"),
			expErr:          SMTAlignmentError{RequestedCPUs: 10, CpusPerCore: 2, AvailablePhysicalCPUs: 8, CausedByPhysicalCPUs: true},
			expCPUAlloc:     false,
			expCSet:         cpuset.New(),
		},
		{
			description: "GuPodManyCores, topoDualSocketHT, AutoReserve, ExpectAllocAllCPUs",
			topo:        topoDualSocketHT,
			options: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			numReservedCPUs: 2,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(1, 2, 3, 4, 5, 7, 8, 9, 10, 11),
			pod:             makePod("fakePod", "fakeContainerBug113537_2", "10000m", "10000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 2, 3, 4, 5, 7, 8, 9, 10, 11),
		},
		{
			description: "GuPodManyCores, topoDualSocketHT, ExpectAllocAllCPUs",
			topo:        topoDualSocketHT,
			options: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			numReservedCPUs: 2,
			reservedCPUs:    newCPUSetPtr(0, 6),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(1, 2, 3, 4, 5, 7, 8, 9, 10, 11),
			pod:             makePod("fakePod", "fakeContainerBug113537_2", "10000m", "10000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 2, 3, 4, 5, 7, 8, 9, 10, 11),
		},
		{
			// 该用例用于验证 SMT 对齐检查会考虑已预分配给容器的 CPU，
			// 例如 kubelet 重启后从状态中恢复的分配结果
			description: "GuPodManyCores, DualSocketHT, ExpectReAllocCPUs",
			topo:        topoDualSocketHT,
			options: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			numReservedCPUs: 4,
			reservedCPUs:    newCPUSetPtr(0, 1, 6, 7),
			pod:             makePod("fakePod", "fakeContainer", "6", "6"),
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer": cpuset.New(2, 3, 4, 8, 9, 10),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 1, 5, 6, 7, 11),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(2, 3, 4, 8, 9, 10),
		},
		{
			description: "GuPodManyCores, DualSocketHT, NotEnoughAvailable, NoAlloc",
			topo:        topoDualSocketHT,
			options: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			numReservedCPUs: 4,
			reservedCPUs:    newCPUSetPtr(0, 1, 6, 7),
			pod:             makePod("fakePod", "fakeContainer", "10", "10"),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			expErr:          SMTAlignmentError{RequestedCPUs: 10, CpusPerCore: 2, AvailablePhysicalCPUs: 8, CausedByPhysicalCPUs: true},
			expCPUAlloc:     false,
		},
	}
	alignBySocketOptionTestCases := []staticPolicyTest{
		{
			description: "Align by socket: true, cpu's within same socket of numa in hint are part of allocation",
			topo:        topoDualSocketMultiNumaPerSocketHT,
			options: map[string]string{
				AlignBySocketOption: "true",
			},
			numReservedCPUs: 1,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(2, 11, 21, 22),
			pod:             makePod("fakePod", "fakeContainer2", "2000m", "2000m"),
			topologyHint:    &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0, 2), Preferred: true},
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(2, 11),
		},
		{
			description: "Align by socket: false, cpu's are taken strictly from NUMA nodes in hint",
			topo:        topoDualSocketMultiNumaPerSocketHT,
			options: map[string]string{
				AlignBySocketOption: "false",
			},
			numReservedCPUs: 1,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(2, 11, 21, 22),
			pod:             makePod("fakePod", "fakeContainer2", "2000m", "2000m"),
			topologyHint:    &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0, 2), Preferred: true},
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(2, 21),
		},
	}

	for _, testCase := range optionsInsensitiveTestCases {
		for _, options := range []map[string]string{
			nil,
			{
				FullPCPUsOnlyOption: "true",
			},
		} {
			tCase := testCase.PseudoClone()
			tCase.description = fmt.Sprintf("options=%v %s", options, testCase.description)
			tCase.options = options
			runStaticPolicyTestCase(t, tCase)
		}
	}

	for _, testCase := range defaultOptionsTestCases {
		runStaticPolicyTestCase(t, testCase)
	}
	for _, testCase := range smtalignOptionTestCases {
		runStaticPolicyTestCase(t, testCase)
	}
	for _, testCase := range alignBySocketOptionTestCases {
		runStaticPolicyTestCaseWithFeatureGate(t, testCase)
	}
}

// runStaticPolicyTestCase 执行单个 staticPolicyTest 用例：构造策略与状态、
// 调用 Allocate，并校验错误、分配结果与默认 cpuset 的一致性
func runStaticPolicyTestCase(t *testing.T, testCase staticPolicyTest) {
	logger, _ := ktesting.NewTestContext(t)
	tm := topologymanager.NewFakeManager(logger)
	if testCase.topologyHint != nil {
		tm = topologymanager.NewFakeManagerWithHint(logger, testCase.topologyHint)
	}
	cpus := cpuset.New()
	if testCase.reservedCPUs != nil {
		cpus = testCase.reservedCPUs.Clone()
	}
	policy, err := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, cpus, tm, testCase.options)
	if err != nil {
		t.Fatalf("NewStaticPolicy() failed: %v", err)
	}

	st := &mockState{
		assignments:   testCase.stAssignments,
		defaultCPUSet: testCase.stDefaultCPUSet,
		baselines:     state.ContainerCPUBaselines{},
	}

	container := &testCase.pod.Spec.Containers[0]
	err = policy.Allocate(logger, st, testCase.pod, container, lifecycle.AddOperation)
	if !reflect.DeepEqual(err, testCase.expErr) {
		t.Errorf("StaticPolicy Allocate() error (%v). expected add error: %q but got: %q",
			testCase.description, testCase.expErr, err)
	}

	if testCase.expCPUAlloc {
		cset, found := st.assignments[string(testCase.pod.UID)][container.Name]
		if !found {
			t.Errorf("StaticPolicy Allocate() error (%v). expected container %v to be present in assignments %v",
				testCase.description, container.Name, st.assignments)
		}

		if !cset.Equals(testCase.expCSet) {
			t.Errorf("StaticPolicy Allocate() error (%v). expected cpuset %s but got %s",
				testCase.description, testCase.expCSet, cset)
		}

		if !cset.Intersection(st.defaultCPUSet).IsEmpty() {
			t.Errorf("StaticPolicy Allocate() error (%v). expected cpuset %s to be disoint from the shared cpuset %s",
				testCase.description, cset, st.defaultCPUSet)
		}
	}

	if !testCase.expCPUAlloc {
		_, found := st.assignments[string(testCase.pod.UID)][container.Name]
		if found {
			t.Errorf("StaticPolicy Allocate() error (%v). Did not expect container %v to be present in assignments %v",
				testCase.description, container.Name, st.assignments)
		}
	}
}

// runStaticPolicyTestCaseWithFeatureGate 在开启 CPUManagerPolicyAlphaOptions 特性后执行用例
func runStaticPolicyTestCaseWithFeatureGate(t *testing.T, testCase staticPolicyTest) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.CPUManagerPolicyAlphaOptions, true)
	runStaticPolicyTestCase(t, testCase)
}

// TestStaticPolicyAllocateRecordsBaseline 验证独占 CPU 分配时会同时记录 baseline cpuset，
// 供原地垂直扩缩容判断当前容器是否仍持有原分配
func TestStaticPolicyAllocateRecordsBaseline(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.InPlacePodVerticalScalingExclusiveCPUs, true)

	logger, _ := ktesting.NewTestContext(t)
	tm := topologymanager.NewFakeManager(logger)
	topo := topoDualSocketHT // 任意拓扑都可以，这里选一个简单的
	opts := map[string]string{}
	policy, err := NewStaticPolicy(logger, topo, 1, cpuset.New(), tm, opts)
	if err != nil {
		t.Fatalf("NewStaticPolicy() failed: %v", err)
	}

	st := &mockState{
		assignments:   state.ContainerCPUAssignments{},
		defaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), // 与所选拓扑匹配
		baselines:     state.ContainerCPUBaselines{},
	}

	pod := makePod("testPod", "testContainer", "2000m", "2000m") // 任意能触发独占 CPU 分配的请求均可
	container := &pod.Spec.Containers[0]                         // 简写引用

	if err := policy.Allocate(logger, st, pod, container, lifecycle.AddOperation); err != nil {
		t.Fatalf("Allocate() failed: %v", err)
	}

	cset, ok := st.GetCPUSet(string(pod.UID), container.Name)
	if !ok {
		t.Fatal("expected container to be present in assignments")
	}

	baseline, ok := st.GetBaselineCPUSet(string(pod.UID), container.Name)
	if !ok {
		t.Fatal("expected container to be present in baselines")
	}

	if !baseline.Equals(cset) {
		t.Errorf("expected baseline %s to equal allocated cpuset %s", baseline, cset)
	}
}

// TestStaticPolicyReuseCPUs 验证普通 init 容器被移除后其 CPU 会归还默认池，可被复用
func TestStaticPolicyReuseCPUs(t *testing.T) {
	testCases := []struct {
		staticPolicyTest
		expCSetAfterAlloc  cpuset.CPUSet
		expCSetAfterRemove cpuset.CPUSet
	}{
		{
			staticPolicyTest: staticPolicyTest{
				description: "SingleSocketHT, DeAllocOneInitContainer",
				topo:        topoSingleSocketHT,
				pod: makeMultiContainerPod(
					[]struct{ request, limit string }{
						{"4000m", "4000m"}}, // 0, 1, 4, 5
					[]struct{ request, limit string }{
						{"2000m", "2000m"}}), // 0, 4
				containerName:   "initContainer-0",
				stAssignments:   state.ContainerCPUAssignments{},
				stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			},
			expCSetAfterAlloc:  cpuset.New(2, 3, 6, 7),
			expCSetAfterRemove: cpuset.New(1, 2, 3, 5, 6, 7),
		},
	}

	for _, testCase := range testCases {
		logger, _ := ktesting.NewTestContext(t)
		policy, err := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, cpuset.New(), topologymanager.NewFakeManager(logger), nil)
		if err != nil {
			t.Fatalf("NewStaticPolicy() failed: %v", err)
		}

		st := &mockState{
			assignments:   testCase.stAssignments,
			defaultCPUSet: testCase.stDefaultCPUSet,
		}
		pod := testCase.pod

		// 分配
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			_ = policy.Allocate(logger, st, pod, &container, lifecycle.AddOperation)
		}
		if !st.defaultCPUSet.Equals(testCase.expCSetAfterAlloc) {
			t.Errorf("StaticPolicy Allocate() error (%v). expected default cpuset %s but got %s",
				testCase.description, testCase.expCSetAfterAlloc, st.defaultCPUSet)
		}

		// 移除
		_ = policy.RemoveContainer(logger, st, string(pod.UID), testCase.containerName)

		if !st.defaultCPUSet.Equals(testCase.expCSetAfterRemove) {
			t.Errorf("StaticPolicy RemoveContainer() error (%v). expected default cpuset %sv but got %s",
				testCase.description, testCase.expCSetAfterRemove, st.defaultCPUSet)
		}
		if _, found := st.assignments[string(pod.UID)][testCase.containerName]; found {
			t.Errorf("StaticPolicy RemoveContainer() error (%v). expected (pod %v, container %v) not be in assignments %v",
				testCase.description, testCase.podUID, testCase.containerName, st.assignments)
		}
	}
}

// TestStaticPolicyDoNotReuseCPUs 验证可重启 init 容器（restartable sidecar）的 CPU 在分配后
// 不会被后续容器复用
func TestStaticPolicyDoNotReuseCPUs(t *testing.T) {
	testCases := []struct {
		staticPolicyTest
		expCSetAfterAlloc cpuset.CPUSet
	}{
		{
			staticPolicyTest: staticPolicyTest{
				description: "SingleSocketHT, Don't reuse CPUs of a restartable init container",
				topo:        topoSingleSocketHT,
				pod: makeMultiContainerPodWithOptions(
					[]*containerOptions{
						{request: "4000m", limit: "4000m", restartPolicy: v1.ContainerRestartPolicyAlways}}, // 0, 1, 4, 5
					[]*containerOptions{
						{request: "2000m", limit: "2000m"}}), // 2, 6
				stAssignments:   state.ContainerCPUAssignments{},
				stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			},
			expCSetAfterAlloc: cpuset.New(3, 7),
		},
	}

	for _, testCase := range testCases {
		logger, _ := ktesting.NewTestContext(t)
		policy, err := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, cpuset.New(), topologymanager.NewFakeManager(logger), nil)
		if err != nil {
			t.Fatalf("NewStaticPolicy() failed: %v", err)
		}

		st := &mockState{
			assignments:   testCase.stAssignments,
			defaultCPUSet: testCase.stDefaultCPUSet,
		}
		pod := testCase.pod

		// 分配
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			err := policy.Allocate(logger, st, pod, &container, lifecycle.AddOperation)
			if err != nil {
				t.Errorf("StaticPolicy Allocate() error (%v). expected no error but got %v",
					testCase.description, err)
			}
		}
		if !st.defaultCPUSet.Equals(testCase.expCSetAfterAlloc) {
			t.Errorf("StaticPolicy Allocate() error (%v). expected default cpuset %s but got %s",
				testCase.description, testCase.expCSetAfterAlloc, st.defaultCPUSet)
		}
	}
}

// TestStaticPolicyRemove 验证 RemoveContainer 会把容器占用的 CPU 归还默认池，
// 并清理状态中的分配记录；未知容器则不做任何变更
func TestStaticPolicyRemove(t *testing.T) {
	testCases := []staticPolicyTest{
		{
			description:   "SingleSocketHT, DeAllocOneContainer",
			topo:          topoSingleSocketHT,
			podUID:        "fakePod",
			containerName: "fakeContainer1",
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer1": cpuset.New(1, 2, 3),
				},
			},
			stDefaultCPUSet: cpuset.New(4, 5, 6, 7),
			expCSet:         cpuset.New(1, 2, 3, 4, 5, 6, 7),
		},
		{
			description:   "SingleSocketHT, DeAllocOneContainer, BeginEmpty",
			topo:          topoSingleSocketHT,
			podUID:        "fakePod",
			containerName: "fakeContainer1",
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer1": cpuset.New(1, 2, 3),
					"fakeContainer2": cpuset.New(4, 5, 6, 7),
				},
			},
			stDefaultCPUSet: cpuset.New(),
			expCSet:         cpuset.New(1, 2, 3),
		},
		{
			description:   "SingleSocketHT, DeAllocTwoContainer",
			topo:          topoSingleSocketHT,
			podUID:        "fakePod",
			containerName: "fakeContainer1",
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer1": cpuset.New(1, 3, 5),
					"fakeContainer2": cpuset.New(2, 4),
				},
			},
			stDefaultCPUSet: cpuset.New(6, 7),
			expCSet:         cpuset.New(1, 3, 5, 6, 7),
		},
		{
			description:   "SingleSocketHT, NoDeAlloc",
			topo:          topoSingleSocketHT,
			podUID:        "fakePod",
			containerName: "fakeContainer2",
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer1": cpuset.New(1, 3, 5),
				},
			},
			stDefaultCPUSet: cpuset.New(2, 4, 6, 7),
			expCSet:         cpuset.New(2, 4, 6, 7),
		},
	}

	for _, testCase := range testCases {
		logger, _ := ktesting.NewTestContext(t)
		policy, err := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, cpuset.New(), topologymanager.NewFakeManager(logger), nil)
		if err != nil {
			t.Fatalf("NewStaticPolicy() failed: %v", err)
		}

		st := &mockState{
			assignments:   testCase.stAssignments,
			defaultCPUSet: testCase.stDefaultCPUSet,
		}

		_ = policy.RemoveContainer(logger, st, testCase.podUID, testCase.containerName)

		if !st.defaultCPUSet.Equals(testCase.expCSet) {
			t.Errorf("StaticPolicy RemoveContainer() error (%v). expected default cpuset %s but got %s",
				testCase.description, testCase.expCSet, st.defaultCPUSet)
		}

		if _, found := st.assignments[testCase.podUID][testCase.containerName]; found {
			t.Errorf("StaticPolicy RemoveContainer() error (%v). expected (pod %v, container %v) not be in assignments %v",
				testCase.description, testCase.podUID, testCase.containerName, st.assignments)
		}
	}
}

// TestTopologyAwareAllocateCPUs 验证 allocateCPUs 在给定 Socket 位掩码时只在对应 Socket 内选核，
// 且无位掩码时按默认顺序分配
func TestTopologyAwareAllocateCPUs(t *testing.T) {
	testCases := []struct {
		description     string
		topo            *topology.CPUTopology
		stAssignments   state.ContainerCPUAssignments
		stDefaultCPUSet cpuset.CPUSet
		numRequested    int
		socketMask      bitmask.BitMask
		expCSet         cpuset.CPUSet
	}{
		{
			description:     "Request 2 CPUs, No BitMask",
			topo:            topoDualSocketHT,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			numRequested:    2,
			socketMask:      nil,
			expCSet:         cpuset.New(0, 6),
		},
		{
			description:     "Request 2 CPUs, BitMask on Socket 0",
			topo:            topoDualSocketHT,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			numRequested:    2,
			socketMask: func() bitmask.BitMask {
				mask, _ := bitmask.NewBitMask(0)
				return mask
			}(),
			expCSet: cpuset.New(0, 6),
		},
		{
			description:     "Request 2 CPUs, BitMask on Socket 1",
			topo:            topoDualSocketHT,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			numRequested:    2,
			socketMask: func() bitmask.BitMask {
				mask, _ := bitmask.NewBitMask(1)
				return mask
			}(),
			expCSet: cpuset.New(1, 7),
		},
		{
			description:     "Request 8 CPUs, BitMask on Socket 0",
			topo:            topoDualSocketHT,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			numRequested:    8,
			socketMask: func() bitmask.BitMask {
				mask, _ := bitmask.NewBitMask(0)
				return mask
			}(),
			expCSet: cpuset.New(0, 6, 2, 8, 4, 10, 1, 7),
		},
		{
			description:     "Request 8 CPUs, BitMask on Socket 1",
			topo:            topoDualSocketHT,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			numRequested:    8,
			socketMask: func() bitmask.BitMask {
				mask, _ := bitmask.NewBitMask(1)
				return mask
			}(),
			expCSet: cpuset.New(1, 7, 3, 9, 5, 11, 0, 6),
		},
	}
	for _, tc := range testCases {
		logger, _ := ktesting.NewTestContext(t)
		p, err := NewStaticPolicy(logger, tc.topo, 0, cpuset.New(), topologymanager.NewFakeManager(logger), nil)
		if err != nil {
			t.Fatalf("NewStaticPolicy() failed: %v", err)
		}
		policy := p.(*staticPolicy)
		st := &mockState{
			assignments:   tc.stAssignments,
			defaultCPUSet: tc.stDefaultCPUSet,
		}
		err = policy.Start(logger, st)
		if err != nil {
			t.Errorf("StaticPolicy Start() error (%v)", err)
			continue
		}

		cpuAlloc, err := policy.allocateCPUs(klog.Background(), st, tc.numRequested, tc.socketMask, cpuset.New())
		if err != nil {
			t.Errorf("StaticPolicy allocateCPUs() error (%v). expected CPUSet %v not error %v",
				tc.description, tc.expCSet, err)
			continue
		}

		if !tc.expCSet.Equals(cpuAlloc.CPUs) {
			t.Errorf("StaticPolicy allocateCPUs() error (%v). expected CPUSet %v but got %v",
				tc.description, tc.expCSet, cpuAlloc.CPUs)
		}
	}
}

// 以上测试用例未启用 kubelet 的 --reserved-cpus 选项，
// 以下测试用例配置了 --reserved-cpus
// staticPolicyTestWithResvList 是配置了显式预留 CPU 列表（--reserved-cpus）时的测试用例描述
type staticPolicyTestWithResvList struct {
	description      string
	topo             *topology.CPUTopology
	numReservedCPUs  int
	reserved         cpuset.CPUSet
	cpuPolicyOptions map[string]string
	stAssignments    state.ContainerCPUAssignments
	stDefaultCPUSet  cpuset.CPUSet
	pod              *v1.Pod
	expErr           error
	expNewErr        error
	expCPUAlloc      bool
	expCSet          cpuset.CPUSet
	expUncoreCache   cpuset.CPUSet // represents the expected UncoreCacheIDs
}

// TestStaticPolicyStartWithResvList 验证配置预留 CPU 列表时策略启动的校验逻辑，
// 包括严格预留（StrictCPUReservationOption）以及预留数量与列表不一致的错误
func TestStaticPolicyStartWithResvList(t *testing.T) {
	testCases := []staticPolicyTestWithResvList{
		{
			description:     "empty cpuset",
			topo:            topoDualSocketHT,
			numReservedCPUs: 2,
			reserved:        cpuset.New(0, 1),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(),
			expCSet:         cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
		},
		{
			description:      "empty cpuset with StrictCPUReservationOption enabled",
			topo:             topoDualSocketHT,
			numReservedCPUs:  2,
			reserved:         cpuset.New(0, 1),
			cpuPolicyOptions: map[string]string{StrictCPUReservationOption: "true"},
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(),
			expCSet:          cpuset.New(2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
		},
		{
			description:     "reserved cores 0 & 1 are not present in available cpuset",
			topo:            topoDualSocketHT,
			numReservedCPUs: 2,
			reserved:        cpuset.New(0, 1),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(2, 3, 4, 5),
			expErr:          fmt.Errorf("not all reserved cpus: \"0-1\" are present in defaultCpuSet: \"2-5\""),
		},
		{
			description:      "reserved cores 0 & 1 are present in available cpuset with StrictCPUReservationOption enabled",
			topo:             topoDualSocketHT,
			numReservedCPUs:  2,
			reserved:         cpuset.New(0, 1),
			cpuPolicyOptions: map[string]string{StrictCPUReservationOption: "true"},
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5),
			expErr:           fmt.Errorf("some of strictly reserved cpus: \"0-1\" are present in defaultCpuSet: \"0-5\""),
		},
		{
			description:     "inconsistency between numReservedCPUs and reserved",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reserved:        cpuset.New(0, 1),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			expNewErr:       fmt.Errorf("[cpumanager] unable to reserve the required amount of CPUs (size of 0-1 did not equal 1)"),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.CPUManagerPolicyAlphaOptions, true)
			p, err := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, testCase.reserved, topologymanager.NewFakeManager(logger), testCase.cpuPolicyOptions)
			if !reflect.DeepEqual(err, testCase.expNewErr) {
				t.Errorf("StaticPolicy Start() error (%v). expected error: %v but got: %v",
					testCase.description, testCase.expNewErr, err)
			}
			if err != nil {
				return
			}
			policy := p.(*staticPolicy)
			st := &mockState{
				assignments:   testCase.stAssignments,
				defaultCPUSet: testCase.stDefaultCPUSet,
			}
			err = policy.Start(logger, st)
			if !reflect.DeepEqual(err, testCase.expErr) {
				t.Errorf("StaticPolicy Start() error (%v). expected error: %v but got: %v",
					testCase.description, testCase.expErr, err)
			}
			if err != nil {
				return
			}

			if !st.GetDefaultCPUSet().Equals(testCase.expCSet) {
				t.Errorf("State CPUSet is different than expected. Have %q wants: %q", st.GetDefaultCPUSet(),
					testCase.expCSet)
			}

		})
	}
}

// TestStaticPolicyAddWithResvList 验证配置预留 CPU 列表时 Allocate 的分配结果
func TestStaticPolicyAddWithResvList(t *testing.T) {

	testCases := []staticPolicyTestWithResvList{
		{
			description:     "GuPodSingleCore, SingleSocketHT, ExpectError",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 1,
			reserved:        cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			pod:             makePod("fakePod", "fakeContainer2", "8000m", "8000m"),
			expErr:          fmt.Errorf("not enough cpus available to satisfy request: requested=8, available=7"),
			expCPUAlloc:     false,
			expCSet:         cpuset.New(),
		},
		{
			description:     "GuPodSingleCore, SingleSocketHT, ExpectAllocOneCPU",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 2,
			reserved:        cpuset.New(0, 1),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			pod:             makePod("fakePod", "fakeContainer2", "1000m", "1000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(4), // 预期分配该不完整核的兄弟超线程
		},
		{
			description:     "GuPodMultipleCores, SingleSocketHT, ExpectAllocOneCore",
			topo:            topoSingleSocketHT,
			numReservedCPUs: 2,
			reserved:        cpuset.New(0, 1),
			stAssignments: state.ContainerCPUAssignments{
				"fakePod": map[string]cpuset.CPUSet{
					"fakeContainer100": cpuset.New(2, 3, 6, 7),
				},
			},
			stDefaultCPUSet: cpuset.New(0, 1, 4, 5),
			pod:             makePod("fakePod", "fakeContainer3", "2000m", "2000m"),
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(4, 5),
		},
	}

	for _, testCase := range testCases {
		logger, _ := ktesting.NewTestContext(t)
		policy, err := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, testCase.reserved, topologymanager.NewFakeManager(logger), nil)
		if err != nil {
			t.Fatalf("NewStaticPolicy() failed: %v", err)
		}

		st := &mockState{
			assignments:   testCase.stAssignments,
			defaultCPUSet: testCase.stDefaultCPUSet,
		}

		container := &testCase.pod.Spec.Containers[0]
		err = policy.Allocate(logger, st, testCase.pod, container, lifecycle.AddOperation)
		if !reflect.DeepEqual(err, testCase.expErr) {
			t.Errorf("StaticPolicy Allocate() error (%v). expected add error: %v but got: %v",
				testCase.description, testCase.expErr, err)
		}

		if testCase.expCPUAlloc {
			cset, found := st.assignments[string(testCase.pod.UID)][container.Name]
			if !found {
				t.Errorf("StaticPolicy Allocate() error (%v). expected container %v to be present in assignments %v",
					testCase.description, container.Name, st.assignments)
			}

			if !cset.Equals(testCase.expCSet) {
				t.Errorf("StaticPolicy Allocate() error (%v). expected cpuset %s but got %s",
					testCase.description, testCase.expCSet, cset)
			}

			if !cset.Intersection(st.defaultCPUSet).IsEmpty() {
				t.Errorf("StaticPolicy Allocate() error (%v). expected cpuset %s to be disoint from the shared cpuset %s",
					testCase.description, cset, st.defaultCPUSet)
			}
		}

		if !testCase.expCPUAlloc {
			_, found := st.assignments[string(testCase.pod.UID)][container.Name]
			if found {
				t.Errorf("StaticPolicy Allocate() error (%v). Did not expect container %v to be present in assignments %v",
					testCase.description, container.Name, st.assignments)
			}
		}
	}
}

// WithPodUID 返回 Pod 的深拷贝并替换其 UID，便于在测试中区分不同 Pod
func WithPodUID(pod *v1.Pod, podUID string) *v1.Pod {
	ret := pod.DeepCopy()
	ret.UID = types.UID(podUID)
	return ret
}

// TestStaticPolicyAddWithUncoreAlignment 验证 PreferAlignByUnCoreCacheOption 下
// 容器 CPU 尽可能对齐到同一 uncore cache 的分配行为，覆盖多种拓扑、
// 奇偶 CPU 请求、预留 CPU 与严格预留等组合
func TestStaticPolicyAddWithUncoreAlignment(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.CPUManagerPolicyAlphaOptions, true)

	testCases := []staticPolicyTestWithResvList{
		{
			description:     "GuPodSingleContainerSaturating, DualSocketHTUncore, ExpectAllocOneUncore, FullUncoreAvail",
			topo:            topoDualSocketSingleNumaPerSocketSMTUncore,
			numReservedCPUs: 8,
			reserved:        cpuset.New(0, 1, 96, 97, 192, 193, 288, 289), // 注意：uncore 0 被占用 4 个 CPU，uncore 12 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// 从可用 CPU 中剔除已被部分占用的 uncore，模拟一个完全干净的初始状态
			stDefaultCPUSet: topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUs().Difference(
				cpuset.New().Union(
					topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUsInUncoreCaches(0),
				).Union(
					topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUsInUncoreCaches(12),
				),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"16000m", "16000m"}, // 该拓扑下每个 uncore 有 16 个 CPU
					},
				),
				"with-app-container-saturating",
			),
			expUncoreCache: cpuset.New(1),
		},
		{
			description:     "GuPodMainAndSidecarContainer, DualSocketHTUncore, ExpectAllocOneUncore, FullUncoreAvail",
			topo:            topoDualSocketSingleNumaPerSocketSMTUncore,
			numReservedCPUs: 8,
			reserved:        cpuset.New(0, 1, 96, 97, 192, 193, 288, 289), // 注意：uncore 0 被占用 4 个 CPU，uncore 12 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// 从可用 CPU 中剔除已被部分占用的 uncore，模拟一个完全干净的初始状态
			stDefaultCPUSet: topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUs().Difference(
				cpuset.New().Union(
					topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUsInUncoreCaches(0),
				).Union(
					topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUsInUncoreCaches(12),
				),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"12000m", "12000m"},
						{"2000m", "2000m"},
					},
				),
				"with-app-container-and-sidecar",
			),
			expUncoreCache: cpuset.New(1),
		},
		{
			description:     "GuPodSidecarAndMainContainer, DualSocketHTUncore, ExpectAllocOneUncore, FullUncoreAvail",
			topo:            topoDualSocketSingleNumaPerSocketSMTUncore,
			numReservedCPUs: 8,
			reserved:        cpuset.New(0, 1, 96, 97, 192, 193, 288, 289), // 注意：uncore 0 被占用 4 个 CPU，uncore 12 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// 从可用 CPU 中剔除已被部分占用的 uncore，模拟一个完全干净的初始状态
			stDefaultCPUSet: topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUs().Difference(
				cpuset.New().Union(
					topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUsInUncoreCaches(0),
				).Union(
					topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUsInUncoreCaches(12),
				),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"2000m", "2000m"},
						{"12000m", "12000m"},
					},
				),
				"with-sidecar-and-app-container",
			),
			expUncoreCache: cpuset.New(1),
		},
		{
			description:     "GuPodMainAndManySidecarContainer, DualSocketHTUncore, ExpectAllocOneUncore, FullUncoreAvail",
			topo:            topoDualSocketSingleNumaPerSocketSMTUncore,
			numReservedCPUs: 8,
			reserved:        cpuset.New(0, 1, 96, 97, 192, 193, 288, 289), // 注意：uncore 0 被占用 4 个 CPU，uncore 12 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// 从可用 CPU 中剔除已被部分占用的 uncore，模拟一个完全干净的初始状态
			stDefaultCPUSet: topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUs().Difference(
				cpuset.New().Union(
					topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUsInUncoreCaches(0),
				).Union(
					topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUsInUncoreCaches(12),
				),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"10000m", "10000m"},
						{"2000m", "2000m"},
						{"2000m", "2000m"},
						{"2000m", "2000m"},
					},
				),
				"with-app-container-and-multi-sidecar",
			),
			expUncoreCache: cpuset.New(1),
		},
		{
			description:     "GuPodMainAndSidecarContainer, DualSocketHTUncore, ExpectAllocTwoUncore",
			topo:            topoDualSocketSingleNumaPerSocketSMTUncore,
			numReservedCPUs: 8,
			reserved:        cpuset.New(0, 1, 96, 97, 192, 193, 288, 289), // 注意：uncore 0 被占用 4 个 CPU，uncore 12 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoDualSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"12000m", "12000m"},
						{"2000m", "2000m"},
					},
				),
				"with-app-container-and-sidecar",
			),
			expUncoreCache: cpuset.New(0, 1), // 预期对齐到 UncoreCacheIDs 0-1
		},
		{
			description:     "GuPodSingleContainer, SingleSocketSMTSmallUncore, ExpectAllocOneUncore",
			topo:            topoSingleSocketSingleNumaPerSocketSMTSmallUncore,
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 64, 65), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"8000m", "8000m"},
					},
				),
				"with-app-container-saturating",
			),
			expUncoreCache: cpuset.New(1),
		},
		{
			// best-effort 策略允许用紧凑（packed）方式调度更大的容器
			description:     "GuPodSingleContainer, SingleSocketSMTSmallUncore, ExpectAllocTwoUncore",
			topo:            topoSingleSocketSingleNumaPerSocketSMTSmallUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 64, 65), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// Uncore 1 已被完全分配
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUs().Difference(
				topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUsInUncoreCaches(1),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"12000m", "12000m"}, // 超过该拓扑单个 uncore cache 的容量
					},
				),
				"with-app-container-saturating",
			),
			expUncoreCache: cpuset.New(0, 2),
		},
		{
			// best-effort 策略允许用紧凑（packed）方式调度更大的容器
			description:     "GuPodSingleContainer, SingleSocketNoSMTSmallUncore, FragmentedUncore, ExpectAllocThreeUncore",
			topo:            topoSingleSocketSingleNumaPerSocketNoSMTSmallUncore, // 每个 uncore 4 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 2, 3), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// Uncore 2、3、5 已被完全分配
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketNoSMTSmallUncore.CPUDetails.CPUs().Difference(
				cpuset.New().Union(
					topoSingleSocketSingleNumaPerSocketNoSMTSmallUncore.CPUDetails.CPUsInUncoreCaches(2),
				).Union(
					topoSingleSocketSingleNumaPerSocketNoSMTSmallUncore.CPUDetails.CPUsInUncoreCaches(3),
				).Union(
					topoSingleSocketSingleNumaPerSocketNoSMTSmallUncore.CPUDetails.CPUsInUncoreCaches(5),
				),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"12000m", "12000m"}, // 相当于 3 个 uncore cache 的 CPU 数
					},
				),
				"with-app-container-saturating",
			),
			expUncoreCache: cpuset.New(1, 4, 6),
		},
		{
			// 按紧凑（packed）方式做 uncore cache 对齐
			description:     "GuPodMultiContainer, DualSocketSMTUncore, FragmentedUncore, ExpectAllocOneUncore",
			topo:            topoSmallDualSocketSingleNumaPerSocketNoSMTUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 32, 33), // 注意：uncore 0 被占用 2 个 CPU，uncore 4 被占用 2 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// uncore 1 已被完全分配
			stDefaultCPUSet: topoSmallDualSocketSingleNumaPerSocketNoSMTUncore.CPUDetails.CPUs().Difference(
				topoSmallDualSocketSingleNumaPerSocketNoSMTUncore.CPUDetails.CPUsInUncoreCaches(1),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"4000m", "4000m"},
						{"2000m", "2000m"},
					},
				),
				"with-multiple-container",
			),
			expUncoreCache: cpuset.New(0),
		},
		{
			// 按紧凑（packed）方式做 uncore cache 对齐
			description:     "GuPodMultiContainer, DualSocketSMTUncore, FragmentedUncore, ExpectAllocTwoUncore",
			topo:            topoSmallDualSocketSingleNumaPerSocketNoSMTUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 32, 33), // 注意：uncore 0 被占用 2 个 CPU，uncore 4 被占用 2 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// uncore 1 已被完全分配
			stDefaultCPUSet: topoSmallDualSocketSingleNumaPerSocketNoSMTUncore.CPUDetails.CPUs().Difference(
				topoSmallDualSocketSingleNumaPerSocketNoSMTUncore.CPUDetails.CPUsInUncoreCaches(1),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"4000m", "4000m"},
						{"4000m", "4000m"},
					},
				),
				"with-multiple-container",
			),
			expUncoreCache: cpuset.New(0, 2),
		},
		{
			// CPU 分配能够放进一个部分可用的 uncore cache
			description:     "GuPodMultiContainer, LargeSingleSocketSMTUncore, PartialUncoreFit, ExpectAllocTwoUncore",
			topo:            topoLargeSingleSocketSingleNumaPerSocketSMTUncore, // 每个 uncore 16 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 128, 129), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// 已从 uncore 1 分配了 4 个 CPU
			stDefaultCPUSet: topoLargeSingleSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUs().Difference(
				cpuset.New(8, 9, 136, 137),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"12000m", "12000m"},
						{"12000m", "12000m"},
					},
				),
				"with-multiple-container",
			),
			expUncoreCache: cpuset.New(0, 1),
		},
		{
			// CPU 分配无法放进一个部分可用的 uncore cache
			description:     "GuPodMultiContainer, LargeSingleSocketSMTUncore, PartialUncoreNoFit, ExpectAllocTwoUncore",
			topo:            topoLargeSingleSocketSingleNumaPerSocketSMTUncore, // 每个 uncore 16 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 128, 129), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// 已从 uncore 1 分配了 4 个 CPU
			stDefaultCPUSet: topoLargeSingleSocketSingleNumaPerSocketSMTUncore.CPUDetails.CPUs().Difference(
				cpuset.New(8, 9, 136, 137),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"14000m", "14000m"},
						{"14000m", "14000m"},
					},
				),
				"with-multiple-container",
			),
			expUncoreCache: cpuset.New(2, 3),
		},
		{
			// NPS=2 的分离缓存架构上的完整 NUMA 分配
			description:     "GuPodLargeSingleContainer, DualSocketNoSMTUncore, FullNUMAsAvail, ExpectAllocFullNUMA",
			topo:            topoDualSocketMultiNumaPerSocketUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 2, 3), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoDualSocketMultiNumaPerSocketUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"48000m", "48000m"}, // 相当于一个 NUMA 节点的 CPU 数
					},
				),
				"with-large-single-container",
			),
			expUncoreCache: cpuset.New(6, 7, 8, 9, 10, 11), // NUMA 节点 1 的 uncore cache
		},
		{
			// PreferAlignByUnCoreCacheOption 不会影响单块（monolithic）uncore 的 x86 架构
			description:     "GuPodSingleContainer, MonoUncoreCacheHT, ExpectAllocCPUSet",
			topo:            topoDualSocketSubNumaPerSocketHTMonolithicUncore, // Uncore cache 的 CPU = 整个 Socket 的 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 120, 121), // 注意：前 2 个核被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoDualSocketSubNumaPerSocketHTMonolithicUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"6000m", "6000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc:    true,
			expCSet:        cpuset.New(2, 3, 4, 122, 123, 124),
			expUncoreCache: cpuset.New(0),
		},
		{
			// PreferAlignByUnCoreCacheOption 在碎片化的单块缓存 x86 架构上的行为
			description:     "GuPodSingleContainer, MonoUncoreCacheHT, ExpectAllocCPUSet",
			topo:            topoSingleSocketSingleNumaPerSocketPCoreHTMonolithicUncore, // Uncore cache 的 CPU = 整个 Socket 的 CPU
			numReservedCPUs: 2,
			reserved:        cpuset.New(0, 1),
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// CPU 4-7 已被分配
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketPCoreHTMonolithicUncore.CPUDetails.CPUs().Difference(
				cpuset.New(4, 5, 6, 7),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"6000m", "6000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc:    true,
			expCSet:        cpuset.New(2, 3, 8, 9, 16, 17), // 与默认紧凑分配一致
			expUncoreCache: cpuset.New(0),
		},
		{
			// 与基于 ARM 的分离缓存架构的兼容性
			description:     "GuPodSingleContainer, LargeSingleSocketUncore, ExpectAllocOneUncore",
			topo:            topoLargeSingleSocketSingleNumaPerSocketUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 2, 3), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoLargeSingleSocketSingleNumaPerSocketUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"8000m", "8000m"},
					},
				),
				"with-single-container",
			),
			expUncoreCache: cpuset.New(1),
		},
		{
			// PreferAlignByUnCoreCacheOption 在碎片化的单块缓存 ARM 架构上的行为
			description:     "GuPodSingleContainer, MonoUncoreCacheHT, ExpectFragmentedAllocCPUSet",
			topo:            topoSingleSocketSingleNumaPerSocketUncore, // Uncore cache 的 CPU = 整个 Socket 的 CPU
			numReservedCPUs: 2,
			reserved:        cpuset.New(0, 1),
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// CPU 6-9、12-15、18-19 已被分配
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketUncore.CPUDetails.CPUs().Difference(
				cpuset.New().Union(
					cpuset.New(6, 7, 8, 9),
				).Union(
					cpuset.New(12, 13, 14, 15),
				).Union(
					cpuset.New(18, 19),
				),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"12000m", "12000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc:    true,
			expCSet:        cpuset.New(2, 3, 4, 5, 10, 11, 16, 17, 20, 21, 22, 23), // 与默认紧凑分配一致
			expUncoreCache: cpuset.New(0),
		},
		{
			// best-effort 策略可能跨越多个 uncore cache，
			// 每个 uncore cache 都只被部分分配
			description:     "GuPodSingleContainer, SingleSocketUncore, PartialUncore, ExpectBestEffortAllocTwoUncore",
			topo:            topoSmallSingleSocketSingleNumaPerSocketNoSMTUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 2, 3), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments: state.ContainerCPUAssignments{},
			// 每个 uncore 都已被部分分配了 4 个 CPU
			stDefaultCPUSet: topoSmallSingleSocketSingleNumaPerSocketNoSMTUncore.CPUDetails.CPUs().Difference(
				cpuset.New().Union(
					cpuset.New(8, 9, 10, 11),
				).Union(
					cpuset.New(16, 17, 18, 19),
				).Union(
					cpuset.New(24, 25, 26, 27),
				),
			),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"8000m", "8000m"}, // 相当于一个完整 uncore cache 的 CPU 数
					},
				),
				"with-single-container",
			),
			expUncoreCache: cpuset.New(0, 1), // 在 uncore cache 0 和 1 之间按 best-effort 分配
		},
		{
			// 在关闭 SMT 的处理器上请求奇数个 CPU
			description:     "odd integer cpu required on smt-disabled",
			topo:            topoSmallSingleSocketSingleNumaPerSocketNoSMTUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 2, 3), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption:            "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoSmallSingleSocketSingleNumaPerSocketNoSMTUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"5000m", "5000m"}, // 相当于一个完整 uncore cache 的 CPU 数
					},
				),
				"with-single-container",
			),
			expCPUAlloc: true,
			expCSet:     cpuset.New(8, 9, 10, 11, 12),
		},
		{
			// 在开启 SMT 的处理器上请求奇数个 CPU
			description:     "odd integer required on smt-enabled",
			topo:            topoSingleSocketSingleNumaPerSocketSMTSmallUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 64, 65), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"3000m", "3000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc: true,
			expCSet:     cpuset.New(2, 3, 66),
		},
		{
			// 在开启 SMT 的处理器上请求奇数个 CPU，且 uncore 上可用的也是奇数个 CPU
			description:     "odd integer required on odd integer partial uncore",
			topo:            topoSingleSocketSingleNumaPerSocketSMTSmallUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 3,
			reserved:        cpuset.New(0, 1, 64), // 注意：uncore 0 被占用 3 个 CPU
			cpuPolicyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"3000m", "3000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc: true,
			expCSet:     cpuset.New(2, 65, 66),
		},
		{
			// 在开启 SMT 的处理器上请求偶数个 CPU，而 uncore 上可用的是奇数个 CPU，
			// 偶数 CPU 的容器不会放进可用 CPU 数为奇数的 uncore cache
			description:     "even integer required on odd integer partial uncore",
			topo:            topoSingleSocketSingleNumaPerSocketSMTSmallUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 3,
			reserved:        cpuset.New(0, 1, 64), // 注意：uncore 0 被占用 3 个 CPU
			cpuPolicyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"4000m", "4000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc: true,
			expCSet:     cpuset.New(4, 5, 68, 69),
		},
		{
			// 在开启 SMT 的处理器上请求较大的奇数个 CPU
			description:     "large odd integer required on smt-enabled",
			topo:            topoSingleSocketSingleNumaPerSocketSMTSmallUncore, // 每个 uncore 8 个 CPU
			numReservedCPUs: 3,
			reserved:        cpuset.New(0, 1, 64), // 注意：uncore 0 被占用 3 个 CPU
			cpuPolicyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"11000m", "11000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc: true,
			expCSet:     cpuset.New(2, 65, 66, 4, 5, 6, 7, 68, 69, 70, 71), // uncore 1 完整分配、uncore 0 部分分配
		},
		{
			// 在开启超线程且为单块 uncore cache 的场景下请求奇数个 CPU
			description:     "odd integer required on HT monolithic uncore",
			topo:            topoDualSocketSubNumaPerSocketHTMonolithicUncore,
			numReservedCPUs: 3,
			reserved:        cpuset.New(0, 1, 120), // 注意：uncore 0 被占用 3 个 CPU
			cpuPolicyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"5000m", "5000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc: true,
			expCSet:     cpuset.New(2, 3, 121, 122, 123),
		},
		{
			// 在开启超线程且为单块 uncore cache 的场景下请求偶数个 CPU
			description:     "even integer required on HT monolithic uncore",
			topo:            topoDualSocketSubNumaPerSocketHTMonolithicUncore,
			numReservedCPUs: 3,
			reserved:        cpuset.New(0, 1, 120), // 注意：uncore 0 被占用 3 个 CPU
			cpuPolicyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"4000m", "4000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc: true,
			expCSet:     cpuset.New(2, 3, 122, 123), // 取完整核
		},
		{
			// 验证与分离缓存架构下 strict-cpu-reservation 特性的兼容性
			description:     "GuPodSingleContainer, SingleSocketSMTSmallUncore, StrictReserveCompatability",
			topo:            topoSingleSocketSingleNumaPerSocketSMTSmallUncore,
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 64, 65), // 注意：uncore 0 被占用 4 个 CPU
			cpuPolicyOptions: map[string]string{
				StrictCPUReservationOption:     "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoSingleSocketSingleNumaPerSocketSMTSmallUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"2000m", "2000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc: true,
			expCSet:     cpuset.New(2, 66), // 应避开预留的 cpuset
		},
		{
			// 验证与单块 uncore 架构下 strict-cpu-reservation 特性的兼容性
			description:     "GuPodSingleContainer, DualSocketHTMonoUncore, StrictReserveCompatability",
			topo:            topoDualSocketSubNumaPerSocketHTMonolithicUncore,
			numReservedCPUs: 4,
			reserved:        cpuset.New(0, 1, 120, 121), // 前两个核已被预留
			cpuPolicyOptions: map[string]string{
				StrictCPUReservationOption:     "true",
				PreferAlignByUnCoreCacheOption: "true",
			},
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: topoDualSocketSubNumaPerSocketHTMonolithicUncore.CPUDetails.CPUs(),
			pod: WithPodUID(
				makeMultiContainerPod(
					[]struct{ request, limit string }{}, // init 容器
					[]struct{ request, limit string }{ // 应用容器
						{"4000m", "4000m"},
					},
				),
				"with-single-container",
			),
			expCPUAlloc: true,
			expCSet:     cpuset.New(2, 3, 122, 123), // 紧凑分配并避开预留的 cpuset
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			policy, err := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, testCase.reserved, topologymanager.NewFakeManager(logger), testCase.cpuPolicyOptions)
			if err != nil {
				t.Fatalf("NewStaticPolicy() failed with %v", err)
			}

			st := &mockState{
				assignments:   testCase.stAssignments,
				defaultCPUSet: testCase.stDefaultCPUSet.Difference(testCase.reserved), // 保证 cpumanager 的不变量成立
			}

			for idx := range testCase.pod.Spec.Containers {
				container := &testCase.pod.Spec.Containers[idx]
				err := policy.Allocate(logger, st, testCase.pod, container, lifecycle.AddOperation)
				if err != nil {
					t.Fatalf("Allocate failed: pod=%q container=%q", testCase.pod.UID, container.Name)
				}
			}

			if testCase.expCPUAlloc {
				container := &testCase.pod.Spec.Containers[0]
				cset, found := st.assignments[string(testCase.pod.UID)][container.Name]
				if !found {
					t.Errorf("StaticPolicy Allocate() error (%v). expected container %v to be present in assignments %v",
						testCase.description, container.Name, st.assignments)
				}
				if !testCase.expCSet.Equals(cset) {
					t.Errorf("StaticPolicy Allocate() error (%v). expected CPUSet %v but got %v",
						testCase.description, testCase.expCSet, cset)
				}
				return
			}

			uncoreCacheIDs, err := getPodUncoreCacheIDs(st, testCase.topo, testCase.pod)
			if err != nil {
				t.Fatalf("uncore cache check: %v", err.Error())
			}
			ids := cpuset.New(uncoreCacheIDs...)

			if !ids.Equals(testCase.expUncoreCache) {
				t.Errorf("StaticPolicy Allocate() error (%v). expected UncoreCacheIDs %v but got %v",
					testCase.description, testCase.expUncoreCache, ids)
			}
		})
	}
}

// staticPolicyOptionTestCase 描述策略选项解析的测试用例
type staticPolicyOptionTestCase struct {
	description   string
	policyOptions map[string]string
	expectedError bool
	expectedValue StaticPolicyOptions
}

// TestStaticPolicyOptions 验证 NewStaticPolicyOptions 对合法/非法选项与取值的处理
func TestStaticPolicyOptions(t *testing.T) {
	testCases := []staticPolicyOptionTestCase{
		{
			description:   "nil args",
			policyOptions: nil,
			expectedError: false,
			expectedValue: StaticPolicyOptions{},
		},
		{
			description:   "empty args",
			policyOptions: map[string]string{},
			expectedError: false,
			expectedValue: StaticPolicyOptions{},
		},
		{
			description: "bad single arg",
			policyOptions: map[string]string{
				"badValue1": "",
			},
			expectedError: true,
		},
		{
			description: "bad multiple arg",
			policyOptions: map[string]string{
				"badValue1": "",
				"badvalue2": "aaaa",
			},
			expectedError: true,
		},
		{
			description: "good arg",
			policyOptions: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			expectedError: false,
			expectedValue: StaticPolicyOptions{
				FullPhysicalCPUsOnly: true,
			},
		},
		{
			description: "good arg, bad value",
			policyOptions: map[string]string{
				FullPCPUsOnlyOption: "enabled!",
			},
			expectedError: true,
		},

		{
			description: "bad arg intermixed",
			policyOptions: map[string]string{
				FullPCPUsOnlyOption: "1",
				"badvalue2":         "lorem ipsum",
			},
			expectedError: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			opts, err := NewStaticPolicyOptions(testCase.policyOptions)
			gotError := (err != nil)
			if gotError != testCase.expectedError {
				t.Fatalf("error with args %v expected error %v got %v: %v",
					testCase.policyOptions, testCase.expectedError, gotError, err)
			}

			if testCase.expectedError {
				return
			}

			if !reflect.DeepEqual(opts, testCase.expectedValue) {
				t.Fatalf("value mismatch with args %v expected value %v got %v",
					testCase.policyOptions, testCase.expectedValue, opts)
			}
		})
	}
}

// TestPolicyWithAlignBySocketAndDistributeCPUsAcrossNUMAEnabled 验证 AlignBySocket 与
// DistributeCPUsAcrossNUMA 同时开启时的优先级与分配结果
func TestPolicyWithAlignBySocketAndDistributeCPUsAcrossNUMAEnabled(t *testing.T) {
	testCases := []staticPolicyTest{
		{
			description: "AlignBySocket and DistributeCPUsAcrossNUMA both enabled",
			topo:        topoDualSocketMultiNumaPerSocketHTType2,
			options: map[string]string{
				AlignBySocketOption:            "true",
				DistributeCPUsAcrossNUMAOption: "true",
			},
			numReservedCPUs: 0,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
			pod:             makePod("fakePod", "fakeContainer", "6000m", "6000m"),
			topologyHint:    &topologymanager.TopologyHint{},
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 3, 5, 7, 9, 11),
		},
		{
			description: "AlignBySocket takes priority over DistributeCPUsAcrossNUMA",
			topo:        topoDualSocketMultiNumaPerSocketHTType2,
			options: map[string]string{
				AlignBySocketOption:            "true",
				DistributeCPUsAcrossNUMAOption: "true",
			},
			numReservedCPUs: 0,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 16, 17),
			pod:             makePod("fakePod", "fakeContainer", "6000m", "6000m"),
			topologyHint:    &topologymanager.TopologyHint{},
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 3, 5, 7, 9, 13),
		},
		{
			description: "AlignBySocket isn't enabled",
			topo:        topoDualSocketMultiNumaPerSocketHTType2,
			options: map[string]string{
				AlignBySocketOption:            "false",
				DistributeCPUsAcrossNUMAOption: "true",
			},
			numReservedCPUs: 0,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 16, 17),
			pod:             makePod("fakePod", "fakeContainer", "6000m", "6000m"),
			topologyHint:    &topologymanager.TopologyHint{},
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 4, 5, 8, 9, 12),
		},
		{
			description: "No assignment that satisfy AlignBySocket",
			topo:        topoDualSocketMultiNumaPerSocketHTType2,
			options: map[string]string{
				AlignBySocketOption:            "true",
				DistributeCPUsAcrossNUMAOption: "true",
			},
			numReservedCPUs: 0,
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(1, 2, 3, 4, 5, 8, 9, 12, 13),
			pod:             makePod("fakePod", "fakeContainer", "6000m", "6000m"),
			topologyHint:    &topologymanager.TopologyHint{},
			expErr:          nil,
			expCPUAlloc:     true,
			expCSet:         cpuset.New(1, 4, 5, 8, 9, 12),
		},
	}
	for _, testCase := range testCases {
		runStaticPolicyTestCaseWithFeatureGate(t, testCase)
	}
}

// staticPolicyAllocatePodTest 描述 Pod 级分配（AllocatePod）的测试用例
type staticPolicyAllocatePodTest struct {
	description                     string
	topo                            *topology.CPUTopology
	numReservedCPUs                 int
	reservedCPUs                    cpuset.CPUSet
	options                         map[string]string
	stAssignments                   state.ContainerCPUAssignments
	stDefaultCPUSet                 cpuset.CPUSet
	pod                             *v1.Pod
	topologyHint                    topologymanager.TopologyHint
	expErr                          error
	expPodAssignments               state.ContainerCPUAssignments
	expDefaultCPUSet                cpuset.CPUSet
	podLevelResourcesEnabled        bool
	podLevelResourceManagersEnabled bool
	requiredMetrics                 requiredMetrics
}

// requiredMetrics 描述用例执行后期望的指标计数
type requiredMetrics struct {
	expTotalAllocs              int
	expTotalErrors              int
	expExclusiveAssignments     int
	expPodSharedPoolAssignments int
}

// containerSpec 是构造测试 Pod 时使用的容器规格简写
type containerSpec struct {
	name          string
	request       string
	limit         string
	restartPolicy *v1.ContainerRestartPolicy
}

// makePodWithContainersAndPodLevelResources 构造带有 Pod 级资源（Spec.Resources）的测试 Pod，
// 并按 containerSpec 填充 init 容器与普通容器
func makePodWithContainersAndPodLevelResources(podName, podRequest, podLimit string, initContainers, appContainers []containerSpec) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			UID:  types.UID(podName),
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{},
			Containers:     []v1.Container{},
			RestartPolicy:  v1.RestartPolicyNever,
		},
	}

	for _, c := range initContainers {
		container := v1.Container{
			Name:  c.name,
			Image: "image",
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
		}
		if c.request != "" {
			container.Resources.Requests[v1.ResourceCPU] = resource.MustParse(c.request)
		}
		if c.limit != "" {
			container.Resources.Limits[v1.ResourceCPU] = resource.MustParse(c.limit)
		}
		if c.restartPolicy != nil {
			container.RestartPolicy = c.restartPolicy
		}
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, container)
	}

	for _, c := range appContainers {
		container := v1.Container{
			Name:  c.name,
			Image: "image",
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
		}
		if c.request != "" {
			container.Resources.Requests[v1.ResourceCPU] = resource.MustParse(c.request)
		}
		if c.limit != "" {
			container.Resources.Limits[v1.ResourceCPU] = resource.MustParse(c.limit)
		}
		pod.Spec.Containers = append(pod.Spec.Containers, container)
	}

	if podRequest != "" || podLimit != "" {
		pod.Spec.Resources = &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("300Mi"),
			},
			Limits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("300Mi"),
			},
		}
		if podRequest != "" {
			pod.Spec.Resources.Requests[v1.ResourceCPU] = resource.MustParse(podRequest)
		}
		if podLimit != "" {
			pod.Spec.Resources.Limits[v1.ResourceCPU] = resource.MustParse(podLimit)
		}
	} else {
		pod.Spec.Resources = nil
	}
	return pod
}

// TestStaticPolicyAllocatePod 验证 Pod 级资源管理下的分配结果与指标：
// 覆盖 Guaranteed/非 Guaranteed 容器、可重启 init 容器、共享池为空被拒绝以及 SMT 对齐错误等场景
func TestStaticPolicyAllocatePod(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	testCases := []staticPolicyAllocatePodTest{
		{
			description:     "should successfully allocate CPUs for a guaranteed pod with pod-level resources",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reservedCPUs:    cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod:             makePodWithPodLevelResources("pod1", "2", "2", "container1", "1", "1"),
			topologyHint:    topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:          nil,
			expPodAssignments: state.ContainerCPUAssignments{
				"pod1": map[string]cpuset.CPUSet{
					"container1": cpuset.New(2),
				},
			},
			expDefaultCPUSet:                cpuset.New(0, 1, 3, 4, 5, 6, 7, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              1,
				expExclusiveAssignments:     1,
				expPodSharedPoolAssignments: 0,
			},
		},
		{
			description:     "scope: pod, should allocate exclusive CPUs to a guaranteed pod with pod-level resources and guaranteed container, PodLevelResourceManagers enabled",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reservedCPUs:    cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod:             makePodWithPodLevelResources("gu-pod-level-resources", "2", "2", "gu-container", "1", "1"),
			topologyHint:    topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:          nil,
			expPodAssignments: state.ContainerCPUAssignments{
				"gu-pod-level-resources": map[string]cpuset.CPUSet{
					"gu-container": cpuset.New(2),
				},
			},
			expDefaultCPUSet:                cpuset.New(0, 1, 3, 4, 5, 6, 7, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              1,
				expExclusiveAssignments:     1,
				expPodSharedPoolAssignments: 0,
			},
		},
		{
			description:     "scope: pod, should allocate exclusive CPUs to a guaranteed pod with pod-level resources and non-guaranteed container, PodLevelResourceManagers enabled",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reservedCPUs:    cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod: makePodWithContainersAndPodLevelResources("gu-pod-level-resources", "1", "1", []containerSpec{}, []containerSpec{
				{name: "ngu-container"},
			}),
			topologyHint: topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:       nil,
			expPodAssignments: state.ContainerCPUAssignments{
				"gu-pod-level-resources": map[string]cpuset.CPUSet{
					"ngu-container": cpuset.New(6),
				},
			},
			expDefaultCPUSet:                cpuset.New(0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              1,
				expExclusiveAssignments:     0,
				expPodSharedPoolAssignments: 1,
			},
		},
		{
			description:     "scope: pod, should allocate exclusive CPUs to a guaranteed pod with pod-level resources and mix of guaranteed and non-guaranteed containers, PodLevelResourceManagers enabled",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reservedCPUs:    cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod: makePodWithContainersAndPodLevelResources("gu-pod-level-mix-ctn", "3", "3", []containerSpec{}, []containerSpec{
				{name: "gu-container-1", request: "1", limit: "1"},
				{name: "gu-container-2", request: "1", limit: "1"},
				{name: "ngu-container"},
			}),
			topologyHint: topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:       nil,
			expPodAssignments: state.ContainerCPUAssignments{
				"gu-pod-level-mix-ctn": {
					"gu-container-1": cpuset.New(6),
					"gu-container-2": cpuset.New(2),
					"ngu-container":  cpuset.New(8),
				},
			},
			expDefaultCPUSet:                cpuset.New(0, 1, 3, 4, 5, 7, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              3,
				expExclusiveAssignments:     2,
				expPodSharedPoolAssignments: 1,
			},
		},
		{
			description:     "scope: pod, should allocate exclusive CPUs to a guaranteed pod with pod-level resources and mix of guaranteed standard and init containers, PodLevelResourceManagers enabled",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reservedCPUs:    cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod: makePodWithContainersAndPodLevelResources("gu-pod-level-gu-init-ctn", "2", "2", []containerSpec{
				{name: "gu-init-container-1", request: "1", limit: "1"},
				{name: "gu-init-container-2", request: "1", limit: "1"},
			}, []containerSpec{
				{name: "gu-container-1", request: "1", limit: "1"},
				{name: "gu-container-2", request: "1", limit: "1"},
			}),
			topologyHint: topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:       nil,
			expPodAssignments: state.ContainerCPUAssignments{
				"gu-pod-level-gu-init-ctn": {
					"gu-init-container-1": cpuset.New(2),
					"gu-init-container-2": cpuset.New(2),
					"gu-container-1":      cpuset.New(2),
					"gu-container-2":      cpuset.New(8),
				},
			},
			expDefaultCPUSet:                cpuset.New(0, 1, 3, 4, 5, 6, 7, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              4,
				expExclusiveAssignments:     4,
				expPodSharedPoolAssignments: 0,
			},
		},
		{
			description:     "scope: pod, should allocate exclusive CPUs to a guaranteed pod with pod-level resources and mix of guaranteed standard and guaranteed restartable and non-guaranteed standard init containers, PodLevelResourceManagers enabled",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reservedCPUs:    cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod: makePodWithContainersAndPodLevelResources("gu-pod-level-gu-init-ctn", "3", "3", []containerSpec{
				{name: "ngu-init-container-1"},
				{name: "gu-init-restartable-2", request: "1", limit: "1", restartPolicy: &containerRestartPolicyAlways},
				{name: "ngu-init-container-3"},
			}, []containerSpec{
				{name: "gu-container-1", request: "1", limit: "1"},
				{name: "ngu-container-2"},
			}),
			topologyHint: topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:       nil,
			expPodAssignments: state.ContainerCPUAssignments{
				"gu-pod-level-gu-init-ctn": {
					"ngu-init-container-1":  cpuset.New(2, 6, 8),
					"gu-init-restartable-2": cpuset.New(6),
					"ngu-init-container-3":  cpuset.New(2, 8),
					"gu-container-1":        cpuset.New(2),
					"ngu-container-2":       cpuset.New(8),
				},
			},
			expDefaultCPUSet:                cpuset.New(0, 1, 3, 4, 5, 7, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              5,
				expExclusiveAssignments:     2,
				expPodSharedPoolAssignments: 3,
			},
		},
		{
			description:     "scope: pod, should allocate exclusive CPUs to a guaranteed pod with pod-level resources and mix of guaranteed standard and non-guaranteed restartable init containers, PodLevelResourceManagers enabled",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reservedCPUs:    cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod: makePodWithContainersAndPodLevelResources("gu-pod-level-gu-init-ctn", "3", "3", []containerSpec{
				{name: "ngu-init-container-1"},
				{name: "ngu-init-restartable-2", restartPolicy: &containerRestartPolicyAlways},
				{name: "ngu-init-container-3"},
			}, []containerSpec{
				{name: "gu-container-1", request: "1", limit: "1"},
				{name: "ngu-container-2"},
			}),
			topologyHint: topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:       nil,
			expPodAssignments: state.ContainerCPUAssignments{
				"gu-pod-level-gu-init-ctn": {
					"ngu-init-container-1":   cpuset.New(2, 6, 8),
					"ngu-init-restartable-2": cpuset.New(2, 8),
					"ngu-init-container-3":   cpuset.New(2, 6, 8),
					"gu-container-1":         cpuset.New(6),
					"ngu-container-2":        cpuset.New(2, 8),
				},
			},
			expDefaultCPUSet:                cpuset.New(0, 1, 3, 4, 5, 7, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              5,
				expExclusiveAssignments:     1,
				expPodSharedPoolAssignments: 4,
			},
		},
		{
			description:     "scope: pod, should reject a pod that would result in an empty pod shared pool",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reservedCPUs:    cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod: makePodWithContainersAndPodLevelResources("gu-pod-level-empty-pod-shared-pool", "2", "2", []containerSpec{}, []containerSpec{
				{name: "gu-container-1", request: "1", limit: "1"},
				{name: "gu-container-2", request: "1", limit: "1"},
				{name: "ngu-container"},
			}),
			topologyHint:                    topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:                          admission.NewEmptyPodSharedPoolError(fmt.Errorf("pod rejected, sum of exclusive container cpu requests equals pod budget, leaving no cpus for shared containers")),
			expPodAssignments:               state.ContainerCPUAssignments{},
			expDefaultCPUSet:                cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalErrors: 1,
			},
		},
		{
			description:     "scope: pod, should not allocate exclusive CPUs to a non-guaranteed pod with pod-level resources and guaranteed containers, PodLevelResourceManagers enabled",
			topo:            topoDualSocketHT,
			numReservedCPUs: 1,
			reservedCPUs:    cpuset.New(0),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod: makePodWithContainersAndPodLevelResources("ngu-pod-level-empty-pod-shared-pool", "2", "1", []containerSpec{}, []containerSpec{
				{name: "gu-container-1", request: "1", limit: "1"},
			}),
			topologyHint:                    topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:                          nil,
			expPodAssignments:               state.ContainerCPUAssignments{},
			expDefaultCPUSet:                cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalErrors: 0,
			},
		},
		{
			description: "scope: pod, should reject a pod due to SMT alignment error when FullPhysicalCPUsOnly is enabled",
			topo:        topoDualSocketHT,
			options: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			numReservedCPUs: 2,
			reservedCPUs:    cpuset.New(0, 1),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod: makePodWithContainersAndPodLevelResources("pod-smt-error", "3", "3", []containerSpec{}, []containerSpec{
				{name: "gu-container", request: "3", limit: "3"},
			}),
			topologyHint:                    topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:                          SMTAlignmentError{RequestedCPUs: 3, CpusPerCore: 2},
			expPodAssignments:               state.ContainerCPUAssignments{},
			expDefaultCPUSet:                cpuset.New(2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalErrors: 1,
			},
		},
		{
			description: "scope: pod, should successfully allocate CPUs for a pod when FullPhysicalCPUsOnly is enabled and CPU request is a multiple of SMT level",
			topo:        topoDualSocketHT,
			options: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			numReservedCPUs: 2,
			reservedCPUs:    cpuset.New(0, 1),
			stAssignments:   state.ContainerCPUAssignments{},
			stDefaultCPUSet: cpuset.New(2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
			pod: makePodWithContainersAndPodLevelResources("pod-smt-success", "4", "4", []containerSpec{}, []containerSpec{
				{name: "gu-container-1", request: "2", limit: "2"},
				{name: "gu-container-2", request: "2", limit: "2"},
			}),
			topologyHint: topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expErr:       nil,
			expPodAssignments: state.ContainerCPUAssignments{
				"pod-smt-success": map[string]cpuset.CPUSet{
					"gu-container-1": cpuset.New(2, 8),
					"gu-container-2": cpuset.New(4, 10),
				},
			},
			expDefaultCPUSet:                cpuset.New(3, 5, 6, 7, 9, 11),
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              2,
				expExclusiveAssignments:     2,
				expPodSharedPoolAssignments: 0,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.PodLevelResources, testCase.podLevelResourcesEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.PodLevelResourceManagers, testCase.podLevelResourceManagersEnabled)

			metrics.Register()
			metrics.ResourceManagerAllocationsTotal.Reset()
			metrics.ResourceManagerAllocationErrorsTotal.Reset()
			metrics.ResourceManagerContainerAssignments.Reset()

			policy, err := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, testCase.reservedCPUs, topologymanager.NewFakeManager(logger), testCase.options)
			if err != nil {
				t.Fatalf("NewStaticPolicy() failed: %v", err)
			}

			st := &mockState{
				assignments:   testCase.stAssignments,
				defaultCPUSet: testCase.stDefaultCPUSet,
			}

			err = policy.AllocatePod(logger, st, testCase.pod, lifecycle.AddOperation)
			if testCase.expErr != nil {
				require.Error(t, err)

				errors, err := testutil.GetCounterMetricValue(metrics.ResourceManagerAllocationErrorsTotal.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerPod))
				require.NoError(t, err)
				require.InDelta(t, float64(testCase.requiredMetrics.expTotalErrors), errors, 0.001, "expected allocation errors to be incremented")

				return
			}
			require.NoError(t, err)

			if !reflect.DeepEqual(st.GetCPUAssignments(), testCase.expPodAssignments) {
				t.Errorf("StaticPolicy AllocatePod() error (%v). expected assignments: %v but got: %v",
					testCase.description, testCase.expPodAssignments, st.GetCPUAssignments())
			}

			if !st.GetDefaultCPUSet().Equals(testCase.expDefaultCPUSet) {
				t.Errorf("StaticPolicy AllocatePod() error (%v). expected default cpuset: %v but got: %v",
					testCase.description, testCase.expDefaultCPUSet, st.GetDefaultCPUSet())
			}

			allocations, err := testutil.GetCounterMetricValue(metrics.ResourceManagerAllocationsTotal.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerPod))
			require.NoError(t, err)
			require.InDelta(t, float64(testCase.requiredMetrics.expTotalAllocs), allocations, 0.001, "unexpected number of allocations")

			exclusiveAssignments, err := testutil.GetCounterMetricValue(metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerExclusivePod))
			require.NoError(t, err)
			require.InDelta(t, float64(testCase.requiredMetrics.expExclusiveAssignments), exclusiveAssignments, 0.001, "unexpected number of assignments")

			podSharedPoolAssignments, err := testutil.GetCounterMetricValue(metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerSharedPod))
			require.NoError(t, err)
			require.InDelta(t, float64(testCase.requiredMetrics.expPodSharedPoolAssignments), podSharedPoolAssignments, 0.001, "unexpected number of assignments")
		})
	}
}

// TestSMTAlignmentErrorText 验证 SMTAlignmentError 的错误消息文本
func TestSMTAlignmentErrorText(t *testing.T) {
	type smtErrTestCase struct {
		name     string
		err      SMTAlignmentError
		expected string
	}

	testCases := []smtErrTestCase{
		{
			name:     "base SMT alignment error",
			err:      SMTAlignmentError{RequestedCPUs: 15, CpusPerCore: 4},
			expected: `SMT Alignment Error: requested 15 cpus not multiple cpus per core = 4`,
		},
		{
			// 注意这里显式写了 0。其本意是表示没有物理 CPU 可用，
			// 但在完全没有可用物理 CPU 的极端情况下，若不带这个显式标志，
			// 就无法区分这两种情况；PR#127959 之前打印的是旧消息
			name:     "base SMT alignment error, no physical CPUs, missing flag",
			err:      SMTAlignmentError{RequestedCPUs: 4, CpusPerCore: 2, AvailablePhysicalCPUs: 0},
			expected: `SMT Alignment Error: requested 4 cpus not multiple cpus per core = 2`,
		},
		{
			name:     "base SMT alignment error, no physical CPUs, explicit flag",
			err:      SMTAlignmentError{RequestedCPUs: 4, CpusPerCore: 2, AvailablePhysicalCPUs: 0, CausedByPhysicalCPUs: true},
			expected: `SMT Alignment Error: not enough free physical CPUs: available physical CPUs = 0, requested CPUs = 4, CPUs per core = 2`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got := testCase.err.Error()
			if got != testCase.expected {
				t.Errorf("got=%v expected=%v", got, testCase.expected)
			}
		})
	}
}

// TestValidatePodScopeResources 验证 validatePodScopeResources 对 Pod 级资源与容器资源组合的合法性判断，
// 特别是共享池（podSharedPool）是否会被耗尽
func TestValidatePodScopeResources(t *testing.T) {
	testCases := []struct {
		name          string
		pod           *v1.Pod
		scope         string
		features      map[featuregate.Feature]bool
		expectErr     bool
		topology      *topology.CPUTopology
		numReserved   int
		reservedCPUs  cpuset.CPUSet
		policyOptions map[string]string
	}{
		{
			name: "Valid: Pod-level != Guaranteed containers, has podSharedPool containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"4",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1", request: "2", limit: "2"},
					{name: "c2"},
				},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: false,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Valid: Only Guaranteed containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"4",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1", request: "2", limit: "2"},
					{name: "c2", request: "2", limit: "2"},
				},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: false,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Valid: Only podSharedPool containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"4",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1"},
					{name: "c2"},
				},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: false,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Valid: Pod-level == Guaranteed containers, has podSharedPool containers with request, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"6",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1", request: "3", limit: "3"},
					{name: "c2", request: "2", limit: "2"},
					{name: "c3", request: "500m", limit: "1000m"},
					{name: "c4"},
				},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: false,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Failure: Pod-level == Guaranteed containers, has podSharedPool containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"5",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1", request: "3", limit: "3"},
					{name: "c2", request: "2", limit: "2"},
					{name: "c3"},
				},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: true,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Valid: Pod-level resources with standard and restartable init containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2",
				[]*containerOptions{
					{name: "init-container1", request: "1", limit: "1"},
					{name: "restartable-init-container1", request: "1", limit: "1", restartPolicy: v1.ContainerRestartPolicyAlways},
				},
				[]*containerOptions{
					{name: "container1"},
				},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: false,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Valid: Pod-level resources equal init and standard container resources, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2",
				[]*containerOptions{
					{name: "init-container1", request: "2", limit: "2"},
				},
				[]*containerOptions{
					{name: "container1", request: "2", limit: "2"},
				},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: false,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Failure: Pod-level resources with shared standard init container and no available pool, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2",
				[]*containerOptions{
					{name: "restartable-init-container1", request: "2", limit: "2", restartPolicy: v1.ContainerRestartPolicyAlways},
					{name: "init-container1"},
				},
				[]*containerOptions{},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: true,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Valid: Pod-level resources with shared standard init container and no available pool, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2",
				[]*containerOptions{
					{name: "init-container1"},
					{name: "restartable-init-container1", request: "2", limit: "2", restartPolicy: v1.ContainerRestartPolicyAlways},
				},
				[]*containerOptions{},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: false,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Failure: Pod-level == Restartable guaranteed init containers, has podSharedPool containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2",
				[]*containerOptions{
					{name: "restartable-init-container1", request: "2", limit: "2", restartPolicy: v1.ContainerRestartPolicyAlways},
					{name: "restartable-init-container2", restartPolicy: v1.ContainerRestartPolicyAlways},
				},
				[]*containerOptions{},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: true,
			topology:  topoSingleSocketHT,
		},
		{
			name: "Valid: Pod-level resources with only restartable one init container and one standard container, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2",
				[]*containerOptions{
					{name: "restartable-init-container1", request: "1", limit: "1", restartPolicy: v1.ContainerRestartPolicyAlways},
				},
				[]*containerOptions{
					{name: "container1", request: "1", limit: "1"},
				},
			),
			scope:     topologymanager.PodTopologyScope,
			features:  map[featuregate.Feature]bool{pkgfeatures.PodLevelResources: true, pkgfeatures.PodLevelResourceManagers: true},
			expectErr: false,
			topology:  topoSingleSocketHT,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)

			for feature, enabled := range tc.features {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, feature, enabled)
			}

			fakeTopologyManager := topologymanager.NewFakeManagerWithScope(tc.scope)
			policy, err := NewStaticPolicy(logger, tc.topology, tc.numReserved, tc.reservedCPUs, fakeTopologyManager, tc.policyOptions)
			if err != nil {
				t.Fatalf("NewStaticPolicy() failed: %v", err)
			}

			err = policy.(*staticPolicy).validatePodScopeResources(logger, tc.pod)

			if tc.expectErr && err == nil {
				t.Error("Expected an error, but got none")
			}
			if !tc.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// newCPUSetPtr 构造 cpuset 并返回其指针，便于在测试用例字面量中直接使用
func newCPUSetPtr(cpus ...int) *cpuset.CPUSet {
	ret := cpuset.New(cpus...)
	return &ret
}

// getPodUncoreCacheIDs 汇总 Pod 各容器已分配 CPU 所属的 uncore cache ID，
// 用于校验分配是否落在预期的 uncore cache 上
func getPodUncoreCacheIDs(s state.Reader, topo *topology.CPUTopology, pod *v1.Pod) ([]int, error) {
	var uncoreCacheIDs []int
	for idx := range pod.Spec.Containers {
		container := &pod.Spec.Containers[idx]
		cset, ok := s.GetCPUSet(string(pod.UID), container.Name)
		if !ok {
			return nil, fmt.Errorf("GetCPUSet(%s, %s) not ok", pod.UID, container.Name)
		}
		for _, cpuID := range cset.UnsortedList() {
			info, ok := topo.CPUDetails[cpuID]
			if !ok {
				return nil, fmt.Errorf("cpuID %v not in topo.CPUDetails", cpuID)
			}
			uncoreCacheIDs = append(uncoreCacheIDs, info.UncoreCacheID)
		}
	}
	return uncoreCacheIDs, nil
}

// 以下 lifecycle 测试验证 static CPU manager 策略会根据给定的 lifecycle.Operation
// 决定执行还是跳过相应操作。
// 由于 Allocate* 与 GetTopologyHints* 在操作不受支持时不会返回错误
// （它们静默返回 nil/空 hint，以免中断所有 hint provider 的整体流程），
// 因此测试通过检查日志输出来确认操作是被执行还是被跳过。
//
// 测试中使用的数值（如 cpuset、Pod 资源请求）是任意的，
// 但都是能让代码走通正常路径的合法值；具体数值没有特殊含义。
// 这些测试不校验分配或 hint 结果是否正确，
// 只校验给定 lifecycle 操作下该操作被执行还是被跳过。

// TestStaticPolicyLifecycleAllocate 验证容器级 Allocate 对不同 lifecycle 操作的处理：
// AddOperation 执行分配，其余操作跳过并记录日志
func TestStaticPolicyLifecycleAllocate(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "CPUManager static policy processes AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "CPUManager static policy skips ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "CPUManager static policy skips empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			policy, err := NewStaticPolicy(logger, topoSingleSocketHT, 0, cpuset.New(), topologymanager.NewFakeManager(logger), nil)
			if err != nil {
				t.Fatalf("NewStaticPolicy() failed: %v", err)
			}

			st := &mockState{
				assignments:   state.ContainerCPUAssignments{},
				defaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			}
			pod := makeMultiContainerPod(nil, []struct{ request, limit string }{{"1", "2"}})
			container := pod.Spec.Containers[0]

			err = policy.Allocate(logger, st, pod, &container, testCase.operation)
			require.NoError(t, err)

			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "CPU Manager container-level resource allocation skipped, operation not supported by the static CPU manager policy"
			if testCase.skipped {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log '%s' not found in logs: %s", expectedLog, logs)
				}
			} else {
				if strings.Contains(logs, expectedLog) {
					t.Errorf("Unexpected log '%s' found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}

// TestStaticPolicyLifecycleAllocatePod 验证 Pod 级 AllocatePod 对不同 lifecycle 操作的处理
func TestStaticPolicyLifecycleAllocatePod(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "CPUManager static policy processes AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "CPUManager static policy skips ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "CPUManager static policy skips empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			policy, err := NewStaticPolicy(logger, topoSingleSocketHT, 0, cpuset.New(), topologymanager.NewFakeManager(logger), nil)
			if err != nil {
				t.Fatalf("NewStaticPolicy() failed: %v", err)
			}

			st := &mockState{
				assignments:   state.ContainerCPUAssignments{},
				defaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			}
			pod := makeMultiContainerPod(nil, []struct{ request, limit string }{{"1", "2"}})

			err = policy.AllocatePod(logger, st, pod, testCase.operation)
			require.NoError(t, err)

			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "CPU Manager pod-level resource allocation skipped, operation not supported by the static CPU manager policy"
			if testCase.skipped {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log '%s' not found in logs: %s", expectedLog, logs)
				}
			} else {
				if strings.Contains(logs, expectedLog) {
					t.Errorf("Unexpected log '%s' found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}

// TestStaticPolicyLifecycleGetTopologyHints 验证容器级 GetTopologyHints 对不同 lifecycle 操作的处理
func TestStaticPolicyLifecycleGetTopologyHints(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "CPUManager static policy processes AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "CPUManager static policy skips ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "CPUManager static policy skips empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			policy, err := NewStaticPolicy(logger, topoSingleSocketHT, 0, cpuset.New(), topologymanager.NewFakeManager(logger), nil)
			if err != nil {
				t.Fatalf("NewStaticPolicy() failed: %v", err)
			}

			st := &mockState{
				assignments:   state.ContainerCPUAssignments{},
				defaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			}
			pod := makeMultiContainerPod(nil, []struct{ request, limit string }{{"1", "2"}})
			container := pod.Spec.Containers[0]

			hints := policy.GetTopologyHints(logger, st, pod, &container, testCase.operation)
			if hints != nil {
				t.Errorf("Unexpected hints: %v", hints)
			}

			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "CPU Manager container-level hint generation skipped, operation not supported by the static CPU manager policy"
			if testCase.skipped {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log '%s' not found in logs: %s", expectedLog, logs)
				}
			} else {
				if strings.Contains(logs, expectedLog) {
					t.Errorf("Unexpected log '%s' found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}

// TestStaticPolicyLifecycleGetPodTopologyHints 验证 Pod 级 GetPodTopologyHints 对不同 lifecycle 操作的处理
func TestStaticPolicyLifecycleGetPodTopologyHints(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "CPUManager static policy processes AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "CPUManager static policy skips ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "CPUManager static policy skips empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			policy, err := NewStaticPolicy(logger, topoSingleSocketHT, 0, cpuset.New(), topologymanager.NewFakeManager(logger), nil)
			if err != nil {
				t.Fatalf("NewStaticPolicy() failed: %v", err)
			}

			st := &mockState{
				assignments:   state.ContainerCPUAssignments{},
				defaultCPUSet: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			}
			pod := makeMultiContainerPod(nil, []struct{ request, limit string }{{"1", "2"}})

			hints := policy.GetPodTopologyHints(logger, st, pod, testCase.operation)
			if hints != nil {
				t.Errorf("Unexpected hints: %v", hints)
			}

			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "CPU Manager pod hint generation skipped, operation not supported by the static CPU manager policy"
			if testCase.skipped {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log '%s' not found in logs: %s", expectedLog, logs)
				}
			} else {
				if strings.Contains(logs, expectedLog) {
					t.Errorf("Unexpected log '%s' found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}
