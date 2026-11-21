//go:build windows

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

// Package winstats 提供 Windows 上获取节点与 Pod 级统计的客户端。
package winstats

import (
	"syscall"
	"time"
	"unsafe"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	"k8s.io/klog/v2"
)

// GetDiskFreeSpaceExW 的 kernel32 过程地址。
var (
	procGetDiskFreeSpaceEx = modkernel32.NewProc("GetDiskFreeSpaceExW")
)

// Client 是获取统计信息的接口。
type Client interface {
	// 返回节点与 Pod 级的容器信息映射。
	WinContainerInfos() (map[string]cadvisorapi.ContainerInfo, error)
	// 返回节点机器信息。
	WinMachineInfo(logger klog.Logger) (*cadvisorapi.MachineInfo, error)
	// 返回内核与运行时版本信息。
	WinVersionInfo() (*cadvisorapi.VersionInfo, error)
	// 返回指定目录所在文件系统的容量与使用情况。
	GetDirFsInfo(path string) (cadvisorapi.FsInfo, error)
}

// StatsClient 实现 Client 接口。
type StatsClient struct {
	client winNodeStatsClient
}

// winNodeStatsClient 是底层 Windows 统计采集接口，便于测试替换。
type winNodeStatsClient interface {
	// 开始监控节点指标。
	startMonitoring(logger klog.Logger) error
	// 返回当前节点指标。
	getNodeMetrics() (nodeMetrics, error)
	// 返回节点静态信息。
	getNodeInfo() nodeInfo
	// 返回节点机器信息。
	getMachineInfo(logger klog.Logger) (*cadvisorapi.MachineInfo, error)
	// 返回版本信息。
	getVersionInfo() (*cadvisorapi.VersionInfo, error)
}

// nodeMetrics 保存一次采样的节点指标。
type nodeMetrics struct {
	cpuUsageCoreNanoSeconds   uint64
	cpuUsageNanoCores         uint64
	memoryPrivWorkingSetBytes uint64
	memoryCommittedBytes      uint64
	timeStamp                 time.Time
	interfaceStats            []cadvisorapi.InterfaceStats
}

// nodeInfo 保存节点静态信息。
type nodeInfo struct {
	memoryPhysicalCapacityBytes uint64
	kernelVersion               string
	osImageVersion              string
	// startTime 是节点启动时间。
	startTime time.Time
}

// cpuUsageCoreNanoSecondsCache 保存 CPU 累计使用量的前后值，用于计算瞬时用量。
type cpuUsageCoreNanoSecondsCache struct {
	latestValue   uint64
	previousValue uint64
}

// newClient 构造 Client 并启动监控。
func newClient(logger klog.Logger, statsNodeClient winNodeStatsClient) (Client, error) {
	statsClient := new(StatsClient)
	statsClient.client = statsNodeClient

	err := statsClient.client.startMonitoring(logger)
	if err != nil {
		return nil, err
	}

	return statsClient, nil
}

// WinContainerInfos 返回容器信息映射，包含节点与 Pod 级统计，
// 对应 cadvisor 的 GetContainerInfoV2。
func (c *StatsClient) WinContainerInfos() (map[string]cadvisorapi.ContainerInfo, error) {
	infos := make(map[string]cadvisorapi.ContainerInfo)
	rootContainerInfo, err := c.createRootContainerInfo()
	if err != nil {
		return nil, err
	}

	infos["/"] = *rootContainerInfo

	return infos, nil
}

// WinMachineInfo 返回节点机器信息，对应 cadvisor 的 MachineInfo。
func (c *StatsClient) WinMachineInfo(logger klog.Logger) (*cadvisorapi.MachineInfo, error) {
	return c.client.getMachineInfo(logger)
}

// WinVersionInfo 返回内核与 Docker 运行时版本，对应 cadvisor 的 VersionInfo。
func (c *StatsClient) WinVersionInfo() (*cadvisorapi.VersionInfo, error) {
	return c.client.getVersionInfo()
}

// createRootContainerInfo 用节点指标构造根容器信息。
func (c *StatsClient) createRootContainerInfo() (*cadvisorapi.ContainerInfo, error) {
	nodeMetrics, err := c.client.getNodeMetrics()
	if err != nil {
		return nil, err
	}

	// 把节点指标转换为 cadvisor 容器统计。
	var stats []*cadvisorapi.ContainerStats
	stats = append(stats, &cadvisorapi.ContainerStats{
		Timestamp: nodeMetrics.timeStamp,
		Cpu: &cadvisorapi.CpuStats{
			Usage: cadvisorapi.CpuUsage{
				Total: nodeMetrics.cpuUsageCoreNanoSeconds,
			},
		},
		CpuInst: &cadvisorapi.CpuInstStats{
			Usage: cadvisorapi.CpuInstUsage{
				Total: nodeMetrics.cpuUsageNanoCores,
			},
		},
		Memory: &cadvisorapi.MemoryStats{
			WorkingSet: nodeMetrics.memoryPrivWorkingSetBytes,
			Usage:      nodeMetrics.memoryCommittedBytes,
		},
		Network: &cadvisorapi.NetworkStats{
			Interfaces: nodeMetrics.interfaceStats,
		},
	})

	nodeInfo := c.client.getNodeInfo()
	rootInfo := cadvisorapi.ContainerInfo{
		Spec: cadvisorapi.ContainerSpec{
			CreationTime: nodeInfo.startTime,
			HasCpu:       true,
			HasMemory:    true,
			HasNetwork:   true,
			Memory: cadvisorapi.MemorySpec{
				Limit: nodeInfo.memoryPhysicalCapacityBytes,
			},
		},
		Stats: stats,
	}

	return &rootInfo, nil
}

// GetDirFsInfo 返回文件系统容量与使用信息。
func (c *StatsClient) GetDirFsInfo(path string) (cadvisorapi.FsInfo, error) {
	var freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes int64
	var err error

	// 调用 GetDiskFreeSpaceExW 获取目录所在卷的容量。
	ret, _, err := syscall.Syscall6(
		procGetDiskFreeSpaceEx.Addr(),
		4,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)),
		0,
		0,
	)
	// 系统调用失败时返回错误。
	if ret == 0 {
		return cadvisorapi.FsInfo{}, err
	}

	return cadvisorapi.FsInfo{
		Timestamp: time.Now(),
		Capacity:  uint64(totalNumberOfBytes),
		Available: uint64(freeBytesAvailable),
		Usage:     uint64(totalNumberOfBytes - freeBytesAvailable),
	}, nil
}
