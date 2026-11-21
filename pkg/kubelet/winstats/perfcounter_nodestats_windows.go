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

package winstats

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

const (
	bootIdRegistry = `SYSTEM\CurrentControlSet\Control\Session Manager\Memory Management\PrefetchParameters`
	bootIdKey      = `BootId`
)

// MemoryStatusEx 对应 Windows 的 MEMORYSTATUSEX 结构体。
// https://msdn.microsoft.com/en-us/library/windows/desktop/aa366770(v=vs.85).aspx
type MemoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// PerformanceInformation 对应 Windows 的 PERFORMANCE_INFORMATION 结构体。
// https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-performance_information
type PerformanceInformation struct {
	cb                     uint32
	CommitTotalPages       uint64
	CommitLimitPages       uint64
	CommitPeakPages        uint64
	PhysicalTotalPages     uint64
	PhysicalAvailablePages uint64
	SystemCachePages       uint64
	KernelTotalPages       uint64
	KernelPagesPages       uint64
	KernelNonpagedPages    uint64
	PageSize               uint64
	HandleCount            uint32
	ProcessCount           uint32
	ThreadCount            uint32
}

var (
	// kernel32.dll 系统调用
	modkernel32                 = windows.NewLazySystemDLL("kernel32.dll")
	procGlobalMemoryStatusEx    = modkernel32.NewProc("GlobalMemoryStatusEx")
	procGetActiveProcessorCount = modkernel32.NewProc("GetActiveProcessorCount")
	// psapi.dll 系统调用
	modpsapi               = windows.NewLazySystemDLL("psapi.dll")
	procGetPerformanceInfo = modpsapi.NewProc("GetPerformanceInfo")
)

const allProcessorGroups = 0xFFFF

// NewPerfCounterClient 创建基于 Windows 性能计数器的节点统计客户端。
func NewPerfCounterClient(logger klog.Logger) (Client, error) {
	// 初始化 CPU 用量缓存，首次采集后才有有效差分。
	initCache := cpuUsageCoreNanoSecondsCache{0, 0}
	return newClient(logger, &perfCounterNodeStatsClient{
		cpuUsageCoreNanoSecondsCache: initCache,
	})
}

// perfCounterNodeStatsClient 通过性能计数器提供 Windows 节点统计。
type perfCounterNodeStatsClient struct {
	nodeMetrics
	mu sync.RWMutex // mu 保护 nodeMetrics
	nodeInfo
	// cpuUsageCoreNanoSecondsCache 缓存节点 CPU 用量的前后两次采样值。
	cpuUsageCoreNanoSecondsCache
}

// startMonitoring 初始化静态节点信息与性能计数器，并启动指标采集和 CPU 用量缓存两个周期任务。
func (p *perfCounterNodeStatsClient) startMonitoring(logger klog.Logger) error {
	// 启动时一次性读取内存容量和系统版本，后续不再变化。
	memory, err := getPhysicallyInstalledSystemMemoryBytes()
	if err != nil {
		return err
	}

	osInfo, err := GetOSInfo()
	if err != nil {
		return err
	}

	p.nodeInfo = nodeInfo{
		kernelVersion:               osInfo.GetPatchVersion(),
		osImageVersion:              osInfo.ProductName,
		memoryPhysicalCapacityBytes: memory,
		startTime:                   time.Now(),
	}

	cpuCounter, err := newPerfCounter(cpuQuery)
	if err != nil {
		return err
	}

	memWorkingSetCounter, err := newPerfCounter(memoryPrivWorkingSetQuery)
	if err != nil {
		return err
	}

	memCommittedBytesCounter, err := newPerfCounter(memoryCommittedBytesQuery)
	if err != nil {
		return err
	}

	networkAdapterCounter, err := newNetworkCounters()
	if err != nil {
		return err
	}

	go wait.Forever(func() {
		// 按 perfCounterUpdatePeriod 周期采集计数器数据。
		p.collectMetricsData(logger, cpuCounter, memWorkingSetCounter, memCommittedBytesCounter, networkAdapterCounter)
	}, perfCounterUpdatePeriod)

	// 按 defaultCachePeriod 缓存 CPU 用量，供瞬时速率差分使用。
	go wait.Forever(func() {
		// 滚动保存上一次与最新一次累计值，差值即一个缓存周期内的增量。
		newValue := p.nodeMetrics.cpuUsageCoreNanoSeconds
		p.mu.Lock()
		defer p.mu.Unlock()
		p.cpuUsageCoreNanoSecondsCache = cpuUsageCoreNanoSecondsCache{
			previousValue: p.cpuUsageCoreNanoSecondsCache.latestValue,
			latestValue:   newValue,
		}
	}, defaultCachePeriod)

	return nil
}

// getMachineInfo 汇总主机名、UUID、BootID、内存容量和 CPU 拓扑，供 cAdvisor 接口使用。
func (p *perfCounterNodeStatsClient) getMachineInfo(logger klog.Logger) (*cadvisorapi.MachineInfo, error) {
	// 主机名、系统 UUID 与 BootID 共同构成 MachineInfo 的稳定标识。
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	systemUUID, err := getSystemUUID()
	if err != nil {
		return nil, err
	}

	bootId, err := getBootID()
	if err != nil {
		return nil, err
	}

	mi := &cadvisorapi.MachineInfo{
		NumCores:       ProcessorCount(),
		MemoryCapacity: p.nodeInfo.memoryPhysicalCapacityBytes,
		MachineID:      hostname,
		SystemUUID:     systemUUID,
		BootID:         bootId,
	}

	// Windows 上必须无条件填充拓扑：kubelet 启动校验（如 server.go 中
	// ReservedSystemCPUs 子集检查）和 static CPU manager 策略都依赖它，
	// 二者不受 WindowsCPUAndMemoryAffinity 门控。该门控只决定 kubelet 是否把
	// 亲和性下发给运行时（见 cpu_manager_windows.go 与
	// internal_container_lifecycle_windows.go），拓扑发现本身不在门控范围内。
	// 这与 Linux 行为一致：cAdvisor 总会填充 MachineInfo.Topology。
	numOfPhysicalCores, numOfSockets, topology, err := processorInfo(logger, relationAll)
	if err != nil {
		logger.Error(err, "failed to discover Windows CPU topology; continuing without topology data")
	} else {
		mi.NumPhysicalCores = numOfPhysicalCores
		mi.NumSockets = numOfSockets
		mi.Topology = topology
	}

	return mi, nil
}

// ProcessorCount 返回系统逻辑处理器数量。
// runtime.NumCPU() 只反映单个 Processor Group；一个组最多 64 个逻辑处理器，
// 超过后会拆分为多个组，因此这里用 GetActiveProcessorCount 查询所有组的总数。
// 参考：moby 中的同类问题 https://github.com/moby/moby/issues/38935#issuecomment-744638345
// 以及 hcsshim 的解法 https://github.com/microsoft/hcsshim/blob/master/internal/processorinfo/processor_count.go
func ProcessorCount() int {
	// 查询所有处理器组；调用失败时退回 runtime.NumCPU()。
	if amount := getActiveProcessorCount(allProcessorGroups); amount != 0 {
		return int(amount)
	}
	return runtime.NumCPU()
}

// getActiveProcessorCount 通过系统调用查询指定处理器组的逻辑 CPU 数。
func getActiveProcessorCount(groupNumber uint16) int {
	r0, _, _ := syscall.Syscall(procGetActiveProcessorCount.Addr(), 1, uintptr(groupNumber), 0, 0)
	return int(r0)
}

// getVersionInfo 返回启动时缓存的 Windows 内核版本与镜像版本。
func (p *perfCounterNodeStatsClient) getVersionInfo() (*cadvisorapi.VersionInfo, error) {
	return &cadvisorapi.VersionInfo{
		KernelVersion:      p.nodeInfo.kernelVersion,
		ContainerOsVersion: p.nodeInfo.osImageVersion,
	}, nil
}

// getNodeMetrics 在读锁保护下返回最近一次采集的节点指标快照。
func (p *perfCounterNodeStatsClient) getNodeMetrics() (nodeMetrics, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nodeMetrics, nil
}

// getNodeInfo 返回启动时采集的静态节点信息。
func (p *perfCounterNodeStatsClient) getNodeInfo() nodeInfo {
	return p.nodeInfo
}

// collectMetricsData 读取各性能计数器；任一读取失败就放弃本轮，
// 避免发布部分字段陈旧、部分字段新鲜的混合快照。
func (p *perfCounterNodeStatsClient) collectMetricsData(logger klog.Logger, cpuCounter, memWorkingSetCounter, memCommittedBytesCounter perfCounter, networkAdapterCounter *networkCounter) {
	// 依次读取 CPU、内存工作集、提交内存和网络适配器计数器。
	cpuValue, err := cpuCounter.getData()
	cpuCores := ProcessorCount()
	if err != nil {
		logger.Error(err, "Unable to get cpu perf counter data")
		return
	}

	memWorkingSetValue, err := memWorkingSetCounter.getData()
	if err != nil {
		logger.Error(err, "Unable to get memWorkingSet perf counter data")
		return
	}

	memCommittedBytesValue, err := memCommittedBytesCounter.getData()
	if err != nil {
		logger.Error(err, "Unable to get memCommittedBytes perf counter data")
		return
	}

	networkAdapterStats, err := networkAdapterCounter.getData(logger)
	if err != nil {
		logger.Error(err, "Unable to get network adapter perf counter data")
		return
	}

	// 整体替换 nodeMetrics，读侧只需短暂持锁即可拿到一致快照。
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodeMetrics = nodeMetrics{
		cpuUsageCoreNanoSeconds:   p.convertCPUValue(cpuCores, cpuValue),
		cpuUsageNanoCores:         p.getCPUUsageNanoCores(),
		memoryPrivWorkingSetBytes: memWorkingSetValue,
		memoryCommittedBytes:      memCommittedBytesValue,
		interfaceStats:            networkAdapterStats,
		timeStamp:                 time.Now(),
	}
}

// convertCPUValue 把性能计数器给出的全核 CPU 百分比换算为累计纳秒。
func (p *perfCounterNodeStatsClient) convertCPUValue(cpuCores int, cpuValue uint64) uint64 {
	// 把性能计数器给出的全核 CPU 百分比换算为累计纳秒：
	// (cpuPercentage / 100.0) * 核数 * 1e9。背景见
	// https://github.com/kubernetes/heapster/issues/650
	newValue := p.nodeMetrics.cpuUsageCoreNanoSeconds + uint64((float64(cpuValue)/100.0)*float64(cpuCores)*1e9)
	return newValue
}

// getCPUUsageNanoCores 用缓存的前后两次累计值差分估算瞬时 CPU 使用量（纳核心）。
func (p *perfCounterNodeStatsClient) getCPUUsageNanoCores() uint64 {
	// 按缓存周期归一化，避免周期长度变化影响速率含义。
	cachePeriodSeconds := uint64(defaultCachePeriod / time.Second)
	perfCounterUpdatePeriodSeconds := uint64(perfCounterUpdatePeriod / time.Second)
	cpuUsageNanoCores := ((p.cpuUsageCoreNanoSecondsCache.latestValue - p.cpuUsageCoreNanoSecondsCache.previousValue) * perfCounterUpdatePeriodSeconds) / cachePeriodSeconds
	return cpuUsageNanoCores
}

// getSystemUUID 从注册表读取并规范化系统 UUID。
func getSystemUUID() (string, error) {
	// HardwareConfig 下的 LastConfig 保存了主机/虚拟机的系统 UUID。
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `SYSTEM\HardwareConfig`, registry.QUERY_VALUE)
	if err != nil {
		return "", fmt.Errorf("failed to open registry key HKLM\\SYSTEM\\HardwareConfig: %w", err)
	}
	defer k.Close()

	uuid, _, err := k.GetStringValue("LastConfig")
	if err != nil {
		return "", fmt.Errorf("failed to read registry value LastConfig from key HKLM\\SYSTEM\\HardwareConfig: %w", err)
	}

	// 注册表中的 UUID 带花括号且大小写不一，统一成无括号大写形式。
	uuid = strings.Trim(uuid, "{")
	uuid = strings.Trim(uuid, "}")
	uuid = strings.ToUpper(uuid)
	return uuid, nil
}

// getPhysicallyInstalledSystemMemoryBytes 通过 GlobalMemoryStatusEx 读取操作系统可用的物理内存。
func getPhysicallyInstalledSystemMemoryBytes() (uint64, error) {
	// 这里用 GlobalMemoryStatusEx 而不是 GetPhysicallyInstalledSystemMemory：
	// 后者从 SMBIOS 固件表读取物理内存，部分虚拟机读不到并返回 ERROR_INVALID_DATA。
	// https://msdn.microsoft.com/en-us/library/windows/desktop/cc300158(v=vs.85).aspx
	// 而 Linux 的 total memory 取自 /proc/meminfo 的 MemTotal；
	// GlobalMemoryStatusEx 返回操作系统可用的物理内存，与 Linux 语义更接近。
	// https://www.kernel.org/doc/Documentation/filesystems/proc.txt
	// 调用前必须填写结构体长度，Windows API 依赖该字段做版本兼容。
	var statex MemoryStatusEx
	statex.Length = uint32(unsafe.Sizeof(statex))
	ret, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&statex)))

	if ret == 0 {
		return 0, errors.New("unable to read physical memory")
	}

	return statex.TotalPhys, nil
}

// GetPerformanceInfo 调用 psapi 获取 Windows 性能信息结构体。
func GetPerformanceInfo() (*PerformanceInformation, error) {
	// 同样先填写 cb 长度字段再调用。
	var pi PerformanceInformation
	pi.cb = uint32(unsafe.Sizeof(pi))
	ret, _, _ := procGetPerformanceInfo.Call(uintptr(unsafe.Pointer(&pi)), uintptr(pi.cb))
	if ret == 0 {
		return nil, errors.New("unable to read Windows performance information")
	}
	return &pi, nil
}

// getBootID 从注册表读取本次启动标识，用于区分节点是否重启。
func getBootID() (string, error) {
	// BootId 由内核在每次启动时写入 PrefetchParameters。
	regKey, err := registry.OpenKey(registry.LOCAL_MACHINE, bootIdRegistry, registry.READ)
	if err != nil {
		return "", err
	}
	defer regKey.Close()
	regValue, _, err := regKey.GetIntegerValue(bootIdKey)
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(regValue, 10), nil
}
