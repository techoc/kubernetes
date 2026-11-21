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

package stats

import (
	"reflect"
	"testing"
	"time"

	"github.com/Microsoft/hnslib"
	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubecontainertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/pkg/kubelet/kuberuntime"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

// fakeNetworkStatsProvider 是测试用的 HNS 网络统计提供者，数据来自内存中的容器列表。
type fakeNetworkStatsProvider struct {
	containers []containerStats
}

// fakeNetworkStats 保存测试用容器的网络统计信息。
type fakeNetworkStats struct {
	BytesReceived          uint64
	BytesSent              uint64
	PacketsReceived        uint64
	PacketsSent            uint64
	DroppedPacketsIncoming uint64
	DroppedPacketsOutgoing uint64
	EndpointId             string
	InstanceId             string
}

// fakeContainerProperties 是测试用容器的基本属性。
type fakeContainerProperties struct {
	ID string
}

// containerStats 把测试容器与其 HNS 网络统计关联起来。
type containerStats struct {
	container fakeContainerProperties
	hcsStats  []fakeNetworkStats
}

// GetHNSEndpointStats 按 endpoint 名称查找对应的 HNS endpoint 统计；找不到时返回空结构。
func (s fakeNetworkStatsProvider) GetHNSEndpointStats(endpointName string) (*hnslib.HNSEndpointStats, error) {
	eps := hnslib.HNSEndpointStats{}
	for _, c := range s.containers {
		for _, stat := range c.hcsStats {
			if endpointName == stat.InstanceId {
				eps = hnslib.HNSEndpointStats{
					EndpointID:      stat.EndpointId,
					BytesSent:       stat.BytesSent,
					BytesReceived:   stat.BytesReceived,
					PacketsReceived: stat.PacketsReceived,
					PacketsSent:     stat.PacketsSent,
				}
			}
		}
	}

	return &eps, nil
}

// HNSListEndpointRequest 返回去重后的 HNS endpoint 列表，并记录共享每个 endpoint 的容器。
func (s fakeNetworkStatsProvider) HNSListEndpointRequest() ([]hnslib.HNSEndpoint, error) {
	uniqueEndpoints := map[string]*hnslib.HNSEndpoint{}

	for _, c := range s.containers {
		for _, stat := range c.hcsStats {
			e, found := uniqueEndpoints[stat.EndpointId]
			if found {
				// 同一 endpoint 被多个容器共享，记录该容器
				e.SharedContainers = append(e.SharedContainers, c.container.ID)
				continue
			}

			uniqueEndpoints[stat.EndpointId] = &hnslib.HNSEndpoint{
				Name:             stat.EndpointId,
				Id:               stat.EndpointId,
				SharedContainers: []string{c.container.ID},
			}
		}
	}

	eps := []hnslib.HNSEndpoint{}
	for _, ep := range uniqueEndpoints {
		eps = append(eps, *ep)
	}

	return eps, nil
}

// Test_criStatsProvider_listContainerNetworkStats 测试按容器汇总 HNS 网络统计的行为，
// 覆盖单容器、共享 endpoint、多 endpoint 等场景。
func Test_criStatsProvider_listContainerNetworkStats(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Time{})
	tests := []struct {
		name    string
		fields  fakeNetworkStatsProvider
		want    map[string]*statsapi.NetworkStats
		wantErr bool
		skipped bool
	}{
		{
			name: "basic example",
			fields: fakeNetworkStatsProvider{
				containers: []containerStats{
					{
						container: fakeContainerProperties{
							ID: "c1",
						}, hcsStats: []fakeNetworkStats{
							{
								BytesReceived: 1,
								BytesSent:     10,
								EndpointId:    "test",
								InstanceId:    "test",
							},
						},
					},
					{
						container: fakeContainerProperties{
							ID: "c2",
						}, hcsStats: []fakeNetworkStats{
							{
								BytesReceived: 2,
								BytesSent:     20,
								EndpointId:    "test2",
								InstanceId:    "test2",
							},
						},
					},
				},
			},
			want: map[string]*statsapi.NetworkStats{
				"c1": {
					Time: v1.NewTime(fakeClock.Now()),
					InterfaceStats: statsapi.InterfaceStats{
						Name:    "test",
						RxBytes: ptr.To[uint64](1),
						TxBytes: ptr.To[uint64](10),
					},
					Interfaces: []statsapi.InterfaceStats{
						{
							Name:    "test",
							RxBytes: ptr.To[uint64](1),

							TxBytes: ptr.To[uint64](10),
						},
					},
				},
				"c2": {
					Time: v1.Time{},
					InterfaceStats: statsapi.InterfaceStats{
						Name:    "test2",
						RxBytes: ptr.To[uint64](2),
						TxBytes: ptr.To[uint64](20),
					},
					Interfaces: []statsapi.InterfaceStats{
						{
							Name:    "test2",
							RxBytes: ptr.To[uint64](2),
							TxBytes: ptr.To[uint64](20),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple containers same endpoint",
			fields: fakeNetworkStatsProvider{
				containers: []containerStats{
					{
						container: fakeContainerProperties{
							ID: "c1",
						}, hcsStats: []fakeNetworkStats{
							{
								BytesReceived: 1,
								BytesSent:     10,
								EndpointId:    "test",
								InstanceId:    "test",
							},
						},
					},
					{
						container: fakeContainerProperties{
							ID: "c2",
						}, hcsStats: []fakeNetworkStats{
							{
								BytesReceived: 2,
								BytesSent:     20,
								EndpointId:    "test2",
								InstanceId:    "test2",
							},
						},
					},
					{
						container: fakeContainerProperties{
							ID: "c3",
						}, hcsStats: []fakeNetworkStats{
							{
								BytesReceived: 3,
								BytesSent:     30,
								EndpointId:    "test2",
								InstanceId:    "test3",
							},
						},
					},
				},
			},
			want: map[string]*statsapi.NetworkStats{
				"c1": {
					Time: v1.NewTime(fakeClock.Now()),
					InterfaceStats: statsapi.InterfaceStats{
						Name:    "test",
						RxBytes: ptr.To[uint64](1),
						TxBytes: ptr.To[uint64](10),
					},
					Interfaces: []statsapi.InterfaceStats{
						{
							Name:    "test",
							RxBytes: ptr.To[uint64](1),

							TxBytes: ptr.To[uint64](10),
						},
					},
				},
				"c2": {
					Time: v1.Time{},
					InterfaceStats: statsapi.InterfaceStats{
						Name:    "test2",
						RxBytes: ptr.To[uint64](2),
						TxBytes: ptr.To[uint64](20),
					},
					Interfaces: []statsapi.InterfaceStats{
						{
							Name:    "test2",
							RxBytes: ptr.To[uint64](2),
							TxBytes: ptr.To[uint64](20),
						},
					},
				},
				"c3": {
					Time: v1.Time{},
					InterfaceStats: statsapi.InterfaceStats{
						Name:    "test2",
						RxBytes: ptr.To[uint64](2),
						TxBytes: ptr.To[uint64](20),
					},
					Interfaces: []statsapi.InterfaceStats{
						{
							Name:    "test2",
							RxBytes: ptr.To[uint64](2),
							TxBytes: ptr.To[uint64](20),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple stats instances of same interface only picks up first",
			fields: fakeNetworkStatsProvider{
				containers: []containerStats{
					{
						container: fakeContainerProperties{
							ID: "c1",
						}, hcsStats: []fakeNetworkStats{
							{
								BytesReceived: 1,
								BytesSent:     10,
								EndpointId:    "test",
								InstanceId:    "test",
							},
							{
								BytesReceived: 3,
								BytesSent:     30,
								EndpointId:    "test",
								InstanceId:    "test3",
							},
						},
					},
					{
						container: fakeContainerProperties{
							ID: "c2",
						}, hcsStats: []fakeNetworkStats{
							{
								BytesReceived: 2,
								BytesSent:     20,
								EndpointId:    "test2",
								InstanceId:    "test2",
							},
						},
					},
				},
			},
			want: map[string]*statsapi.NetworkStats{
				"c1": {
					Time: v1.NewTime(fakeClock.Now()),
					InterfaceStats: statsapi.InterfaceStats{
						Name:    "test",
						RxBytes: ptr.To[uint64](1),
						TxBytes: ptr.To[uint64](10),
					},
					Interfaces: []statsapi.InterfaceStats{
						{
							Name:    "test",
							RxBytes: ptr.To[uint64](1),

							TxBytes: ptr.To[uint64](10),
						},
					},
				},
				"c2": {
					Time: v1.Time{},
					InterfaceStats: statsapi.InterfaceStats{
						Name:    "test2",
						RxBytes: ptr.To[uint64](2),
						TxBytes: ptr.To[uint64](20),
					},
					Interfaces: []statsapi.InterfaceStats{
						{
							Name:    "test2",
							RxBytes: ptr.To[uint64](2),
							TxBytes: ptr.To[uint64](20),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple endpoints per container",
			fields: fakeNetworkStatsProvider{
				containers: []containerStats{
					{
						container: fakeContainerProperties{
							ID: "c1",
						}, hcsStats: []fakeNetworkStats{
							{
								BytesReceived: 1,
								BytesSent:     10,
								EndpointId:    "test",
								InstanceId:    "test",
							},
							{
								BytesReceived: 3,
								BytesSent:     30,
								EndpointId:    "test3",
								InstanceId:    "test3",
							},
						},
					},
					{
						container: fakeContainerProperties{
							ID: "c2",
						}, hcsStats: []fakeNetworkStats{
							{
								BytesReceived: 2,
								BytesSent:     20,
								EndpointId:    "test2",
								InstanceId:    "test2",
							},
						},
					},
				},
			},
			want: map[string]*statsapi.NetworkStats{
				"c1": {
					Time: v1.NewTime(fakeClock.Now()),
					InterfaceStats: statsapi.InterfaceStats{
						Name:    "test",
						RxBytes: ptr.To[uint64](1),
						TxBytes: ptr.To[uint64](10),
					},
					Interfaces: []statsapi.InterfaceStats{
						{
							Name:    "test",
							RxBytes: ptr.To[uint64](1),

							TxBytes: ptr.To[uint64](10),
						},
						{
							Name:    "test3",
							RxBytes: ptr.To[uint64](3),

							TxBytes: ptr.To[uint64](30),
						},
					},
				},
				"c2": {
					Time: v1.Time{},
					InterfaceStats: statsapi.InterfaceStats{
						Name:    "test2",
						RxBytes: ptr.To[uint64](2),
						TxBytes: ptr.To[uint64](20),
					},
					Interfaces: []statsapi.InterfaceStats{
						{
							Name:    "test2",
							RxBytes: ptr.To[uint64](2),
							TxBytes: ptr.To[uint64](20),
						},
					},
				},
			},
			wantErr: false,
			skipped: true,
		},
	}
	logger, _ := ktesting.NewTestContext(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// TODO: Remove skip once https://github.com/kubernetes/kubernetes/issues/116692 is fixed.
			if tt.skipped {
				t.Skip("Test temporarily skipped.")
			}
			p := &criStatsProvider{
				windowsNetworkStatsProvider: fakeNetworkStatsProvider{
					containers: tt.fields.containers,
				},
				clock: fakeClock,
			}
			got, err := p.listContainerNetworkStats(logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("listContainerNetworkStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("listContainerNetworkStats() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test_criStatsProvider_makeWinContainerStats 测试 Windows 容器统计的转换，
// 校验 CPU、内存、Rootfs 与日志统计的映射结果。
func Test_criStatsProvider_makeWinContainerStats(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Time{})
	containerStartTime := fakeClock.Now()

	cpuUsageTimestamp := int64(555555)
	cpuUsageNanoSeconds := uint64(0x123456)
	cpuUsageNanoCores := uint64(0x4000)
	memoryUsageTimestamp := int64(666666)
	memoryUsageWorkingSetBytes := uint64(0x11223344)
	memoryUsageAvailableBytes := uint64(0x55667788)
	memoryCommitBytes := uint64(0x99AABBCC)
	memoryUsagePageFaults := uint64(200)
	logStatsUsed := uint64(5000)
	logStatsInodesUsed := uint64(5050)

	// makeWindowsContainerStats 会调用 getPodContainerLogStats 来填充 ContainerStats.Logs
	c0LogStats := &volume.Metrics{
		Used:       resource.NewQuantity(int64(logStatsUsed), resource.BinarySI),
		InodesUsed: resource.NewQuantity(int64(logStatsInodesUsed), resource.BinarySI),
	}
	fakeStats := map[string]*volume.Metrics{
		kuberuntime.BuildContainerLogsDirectory(testPodLogDirectory, "sb0-ns", "sb0-name", types.UID("sb0-uid"), "c0"): c0LogStats,
	}
	fakeOS := &kubecontainertest.FakeOS{}
	fakeHostStatsProvider := NewFakeHostStatsProviderWithData(fakeStats, fakeOS)

	p := &criStatsProvider{
		clock:             fakeClock,
		hostStatsProvider: fakeHostStatsProvider,
	}

	inputStats := &runtimeapi.WindowsContainerStats{
		Attributes: &runtimeapi.ContainerAttributes{
			Metadata: &runtimeapi.ContainerMetadata{
				Name: "c0",
			},
		},
		Cpu: &runtimeapi.WindowsCpuUsage{
			Timestamp: cpuUsageTimestamp,
			UsageCoreNanoSeconds: &runtimeapi.UInt64Value{
				Value: cpuUsageNanoSeconds,
			},
			UsageNanoCores: &runtimeapi.UInt64Value{
				Value: cpuUsageNanoCores,
			},
		},
		Memory: &runtimeapi.WindowsMemoryUsage{
			Timestamp: memoryUsageTimestamp,
			AvailableBytes: &runtimeapi.UInt64Value{
				Value: memoryUsageAvailableBytes,
			},
			WorkingSetBytes: &runtimeapi.UInt64Value{
				Value: memoryUsageWorkingSetBytes,
			},
			CommitMemoryBytes: &runtimeapi.UInt64Value{
				Value: memoryCommitBytes,
			},
			PageFaults: &runtimeapi.UInt64Value{
				Value: memoryUsagePageFaults,
			},
		},
	}

	inputContainer := &runtimeapi.Container{
		CreatedAt: containerStartTime.Unix(),
		Metadata: &runtimeapi.ContainerMetadata{
			Name: "c0",
		},
	}

	inputRootFsInfo := &cadvisorapi.FsInfo{}

	// 供 makeWinContainerStats() 中的 getPodContainerLogStats() 调用使用
	inputPodSandboxMetadata := &runtimeapi.PodSandboxMetadata{
		Namespace: "sb0-ns",
		Name:      "sb0-name",
		Uid:       "sb0-uid",
	}

	logger, _ := ktesting.NewTestContext(t)
	got, err := p.makeWinContainerStats(logger, inputStats, inputContainer, inputRootFsInfo, make(map[string]*cadvisorapi.FsInfo), inputPodSandboxMetadata)

	expected := &statsapi.ContainerStats{
		Name:      "c0",
		StartTime: v1.NewTime(time.Unix(0, containerStartTime.Unix())),
		CPU: &statsapi.CPUStats{
			Time:                 v1.NewTime(time.Unix(0, cpuUsageTimestamp)),
			UsageNanoCores:       ptr.To[uint64](cpuUsageNanoCores),
			UsageCoreNanoSeconds: ptr.To[uint64](cpuUsageNanoSeconds),
		},
		Memory: &statsapi.MemoryStats{
			Time:            v1.NewTime(time.Unix(0, memoryUsageTimestamp)),
			AvailableBytes:  ptr.To[uint64](memoryUsageAvailableBytes),
			UsageBytes:      ptr.To[uint64](memoryCommitBytes),
			WorkingSetBytes: ptr.To[uint64](memoryUsageWorkingSetBytes),
			PageFaults:      ptr.To[uint64](memoryUsagePageFaults),
		},
		Rootfs: &statsapi.FsStats{},
		Logs: &statsapi.FsStats{
			Time:       c0LogStats.Time,
			UsedBytes:  ptr.To[uint64](logStatsUsed),
			InodesUsed: ptr.To[uint64](logStatsInodesUsed),
		},
	}

	if err != nil {
		t.Fatalf("makeWinContainerStats() error = %v, expected no error", err)
	}

	if !reflect.DeepEqual(got.CPU, expected.CPU) {
		t.Errorf("makeWinContainerStats() CPU = %v, expected %v", got.CPU, expected.CPU)
	}

	if !reflect.DeepEqual(got.Memory, expected.Memory) {
		t.Errorf("makeWinContainerStats() Memory = %v, want %v", got.Memory, expected.Memory)
	}

	if !reflect.DeepEqual(got.Rootfs, expected.Rootfs) {
		t.Errorf("makeWinContainerStats() Rootfs = %v, want %v", got.Rootfs, expected.Rootfs)
	}

	// 日志统计包含指向计算所得 resource 值的指针，因此这里不能使用 DeepEqual
	assert.Equal(t, *got.Logs.UsedBytes, logStatsUsed, "Logs.UsedBytes does not match expected value")
	assert.Equal(t, *got.Logs.InodesUsed, logStatsInodesUsed, "Logs.InodesUsed does not match expected value")
}
