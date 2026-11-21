//go:build windows

/*
Copyright 2023 The Kubernetes Authors.

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

package winstats

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2/ktesting"
)

// TestMonitoring 验证性能计数器客户端能启动监控，并最终产出非零的节点指标。
func TestMonitoring(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	counterClient, err := NewPerfCounterClient(logger)
	assert.NoError(t, err)

	// 断言 startMonitoring 已被调用：nodeInfo 应当已经设置。
	assert.NotNil(t, counterClient.(*StatsClient).client.getNodeInfo())

	// 轮询等待，直到获得非零的节点指标。
	// 轮询上限设为 5 个采集周期，避免测试无限等待。
	if pollErr := wait.Poll(100*time.Millisecond, 5*perfCounterUpdatePeriod, func() (bool, error) {
		metrics, _ := counterClient.(*StatsClient).client.getNodeMetrics()
		if metrics.memoryPrivWorkingSetBytes != 0 {
			return true, nil
		}

		return false, nil
	}); pollErr != nil {
		t.Fatalf("Encountered error: `%v'", pollErr)
	}
}

// TestGetMachineInfo 验证从性能计数器客户端读取的机器信息字段。
func TestGetMachineInfo(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := perfCounterNodeStatsClient{
		nodeInfo: nodeInfo{
			memoryPhysicalCapacityBytes: 100,
		},
	}

	machineInfo, err := p.getMachineInfo(logger)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), machineInfo.MemoryCapacity)
	hostname, _ := os.Hostname()
	assert.Equal(t, hostname, machineInfo.MachineID)

	// 校验 SystemUUID 是合法的 UUID。
	_, err = uuid.Parse(machineInfo.SystemUUID)
	assert.NoError(t, err)

	id, err := strconv.Atoi(machineInfo.BootID)
	assert.NoError(t, err)
	assert.NotZero(t, id)
}

// TestGetVersionInfo 验证版本信息由内核版本与 OS 镜像版本正确映射。
func TestGetVersionInfo(t *testing.T) {
	client := perfCounterNodeStatsClient{
		nodeInfo: nodeInfo{
			kernelVersion:  "foo",
			osImageVersion: "lish",
		},
	}

	info, _ := client.getVersionInfo()
	expected := &cadvisorapi.VersionInfo{
		KernelVersion:      "foo",
		ContainerOsVersion: "lish",
	}
	assert.Equal(t, expected, info)
}

// TestCollectMetricsData 验证各性能计数器报错时采集结果的逐步恢复行为。
func TestCollectMetricsData(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := perfCounterNodeStatsClient{}

	cpuCounter := &fakePerfCounterImpl{
		value:      1,
		raiseError: true,
	}
	memWorkingSetCounter := &fakePerfCounterImpl{
		value:      2,
		raiseError: true,
	}
	memCommittedBytesCounter := &fakePerfCounterImpl{
		value:      3,
		raiseError: true,
	}
	networkAdapterCounter := newFakedNetworkCounters(true)

	// 先验证各计数器仍会报错时的行为。
	p.collectMetricsData(logger, cpuCounter, memWorkingSetCounter, memCommittedBytesCounter, networkAdapterCounter)
	metrics, _ := p.getNodeMetrics()
	expectedMetrics := nodeMetrics{}
	assert.Equal(t, expectedMetrics, metrics)

	cpuCounter.raiseError = false
	p.collectMetricsData(logger, cpuCounter, memWorkingSetCounter, memCommittedBytesCounter, networkAdapterCounter)
	metrics, _ = p.getNodeMetrics()
	assert.Equal(t, expectedMetrics, metrics)

	memWorkingSetCounter.raiseError = false
	p.collectMetricsData(logger, cpuCounter, memWorkingSetCounter, memCommittedBytesCounter, networkAdapterCounter)
	metrics, _ = p.getNodeMetrics()
	assert.Equal(t, expectedMetrics, metrics)

	memCommittedBytesCounter.raiseError = false
	p.collectMetricsData(logger, cpuCounter, memWorkingSetCounter, memCommittedBytesCounter, networkAdapterCounter)
	metrics, _ = p.getNodeMetrics()
	assert.Equal(t, expectedMetrics, metrics)

	// 所有计数器都正常后，期望得到完整的指标快照。
	networkAdapterCounter = newFakedNetworkCounters(false)
	p.collectMetricsData(logger, cpuCounter, memWorkingSetCounter, memCommittedBytesCounter, networkAdapterCounter)
	metrics, _ = p.getNodeMetrics()
	expectedMetrics = nodeMetrics{
		cpuUsageCoreNanoSeconds:   uint64(ProcessorCount()) * 1e7,
		cpuUsageNanoCores:         0,
		memoryPrivWorkingSetBytes: 2,
		memoryCommittedBytes:      3,
		interfaceStats:            networkAdapterCounter.listInterfaceStats(),
	}
	// 时间戳由采集时刻决定，先对齐时间戳再比较其余字段。
	assert.WithinDuration(t, time.Now(), metrics.timeStamp, 20*time.Millisecond)
	expectedMetrics.timeStamp = metrics.timeStamp
	assert.Equal(t, expectedMetrics, metrics)
}

// TestConvertCPUValue 验证 CPU 使用率值按核数换算为纳秒。
func TestConvertCPUValue(t *testing.T) {
	testCases := []struct {
		cpuValue uint64
		expected uint64
	}{
		{cpuValue: uint64(50), expected: uint64(2000000000)},
		{cpuValue: uint64(0), expected: uint64(0)},
		{cpuValue: uint64(100), expected: uint64(4000000000)},
	}
	var cpuCores = 4

	for _, tc := range testCases {
		p := perfCounterNodeStatsClient{}
		newValue := p.convertCPUValue(cpuCores, tc.cpuValue)
		assert.Equal(t, tc.expected, newValue)
	}
}

// TestGetCPUUsageNanoCores 验证基于前后两次采样的 CPU 使用率增量计算。
func TestGetCPUUsageNanoCores(t *testing.T) {
	// 期望值按 CPU 性能计数器的采集周期进行缩放。
	perfCounterUpdatePeriodSeconds := uint64(perfCounterUpdatePeriod / time.Second)
	testCases := []struct {
		latestValue   uint64
		previousValue uint64
		expected      uint64
	}{
		{latestValue: uint64(0), previousValue: uint64(0), expected: uint64(0)},
		{latestValue: uint64(2000000000), previousValue: uint64(0), expected: uint64(200000000 * perfCounterUpdatePeriodSeconds)},
		{latestValue: uint64(5000000000), previousValue: uint64(2000000000), expected: uint64(300000000 * perfCounterUpdatePeriodSeconds)},
	}

	for _, tc := range testCases {
		p := perfCounterNodeStatsClient{}
		p.cpuUsageCoreNanoSecondsCache = cpuUsageCoreNanoSecondsCache{
			latestValue:   tc.latestValue,
			previousValue: tc.previousValue,
		}
		cpuUsageNanoCores := p.getCPUUsageNanoCores()
		assert.Equal(t, tc.expected, cpuUsageNanoCores)
	}
}

// testGetPhysicallyInstalledSystemMemoryBytes 验证能读取到非零的物理内存总量。
func testGetPhysicallyInstalledSystemMemoryBytes(t *testing.T) {
	totalMemory, err := getPhysicallyInstalledSystemMemoryBytes()
	assert.NoError(t, err)
	assert.NotZero(t, totalMemory)
}

// TestGetSystemUUID 验证从注册表读取的系统 UUID 与 WMI 查询结果一致。
func TestGetSystemUUID(t *testing.T) {
	uuidFromRegistry, err := getSystemUUID()
	assert.NoError(t, err)

	// 通过 PowerShell WMI 查询作为交叉验证来源。
	uuidFromWmi, err := exec.Command("powershell.exe", "Get-WmiObject", "Win32_ComputerSystemProduct", "|", "Select-Object", "-ExpandProperty UUID").Output()
	assert.NoError(t, err)
	uuidFromWmiString := strings.Trim(string(uuidFromWmi), "\r\n")
	assert.Equal(t, uuidFromWmiString, uuidFromRegistry)
}
