//go:build windows

/*
Copyright 2019 The Kubernetes Authors.

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
	"sync"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

// Windows 性能计数器路径：按网络适配器返回每秒包/字节数以及丢弃/错误计数。
const (
	packetsReceivedPerSecondQuery = "\\Network Adapter(*)\\Packets Received/sec"
	packetsSentPerSecondQuery     = "\\Network Adapter(*)\\Packets Sent/sec"
	bytesReceivedPerSecondQuery   = "\\Network Adapter(*)\\Bytes Received/sec"
	bytesSentPerSecondQuery       = "\\Network Adapter(*)\\Bytes Sent/sec"
	packetsReceivedDiscardedQuery = "\\Network Adapter(*)\\Packets Received Discarded"
	packetsReceivedErrorsQuery    = "\\Network Adapter(*)\\Packets Received Errors"
	packetsOutboundDiscardedQuery = "\\Network Adapter(*)\\Packets Outbound Discarded"
	packetsOutboundErrorsQuery    = "\\Network Adapter(*)\\Packets Outbound Errors"
)

// networkCounter 聚合各网络适配器的性能计数器与缓存结果。
type networkCounter struct {
	packetsReceivedPerSecondCounter perfCounter
	packetsSentPerSecondCounter     perfCounter
	bytesReceivedPerSecondCounter   perfCounter
	bytesSentPerSecondCounter       perfCounter
	packetsReceivedDiscardedCounter perfCounter
	packetsReceivedErrorsCounter    perfCounter
	packetsOutboundDiscardedCounter perfCounter
	packetsOutboundErrorsCounter    perfCounter

	mu           sync.RWMutex
	adapterStats map[string]cadvisorapi.InterfaceStats
}

// newNetworkCounters 创建全部 8 个性能计数器，任一失败即返回错误。
func newNetworkCounters() (*networkCounter, error) {
	packetsReceivedPerSecondCounter, err := newPerfCounter(packetsReceivedPerSecondQuery)
	if err != nil {
		return nil, err
	}

	packetsSentPerSecondCounter, err := newPerfCounter(packetsSentPerSecondQuery)
	if err != nil {
		return nil, err
	}

	bytesReceivedPerSecondCounter, err := newPerfCounter(bytesReceivedPerSecondQuery)
	if err != nil {
		return nil, err
	}

	bytesSentPerSecondCounter, err := newPerfCounter(bytesSentPerSecondQuery)
	if err != nil {
		return nil, err
	}

	packetsReceivedDiscardedCounter, err := newPerfCounter(packetsReceivedDiscardedQuery)
	if err != nil {
		return nil, err
	}

	packetsReceivedErrorsCounter, err := newPerfCounter(packetsReceivedErrorsQuery)
	if err != nil {
		return nil, err
	}

	packetsOutboundDiscardedCounter, err := newPerfCounter(packetsOutboundDiscardedQuery)
	if err != nil {
		return nil, err
	}

	packetsOutboundErrorsCounter, err := newPerfCounter(packetsOutboundErrorsQuery)
	if err != nil {
		return nil, err
	}

	return &networkCounter{
		packetsReceivedPerSecondCounter: packetsReceivedPerSecondCounter,
		packetsSentPerSecondCounter:     packetsSentPerSecondCounter,
		bytesReceivedPerSecondCounter:   bytesReceivedPerSecondCounter,
		bytesSentPerSecondCounter:       bytesSentPerSecondCounter,
		packetsReceivedDiscardedCounter: packetsReceivedDiscardedCounter,
		packetsReceivedErrorsCounter:    packetsReceivedErrorsCounter,
		packetsOutboundDiscardedCounter: packetsOutboundDiscardedCounter,
		packetsOutboundErrorsCounter:    packetsOutboundErrorsCounter,
		adapterStats:                    map[string]cadvisorapi.InterfaceStats{},
	}, nil
}

// getData 读取所有计数器并合并成按适配器聚合的接口统计。
func (n *networkCounter) getData(logger klog.Logger) ([]cadvisorapi.InterfaceStats, error) {
	packetsReceivedPerSecondData, err := n.packetsReceivedPerSecondCounter.getDataList()
	if err != nil {
		logger.Error(err, "Unable to get packetsReceivedPerSecond perf counter data")
		return nil, err
	}

	packetsSentPerSecondData, err := n.packetsSentPerSecondCounter.getDataList()
	if err != nil {
		logger.Error(err, "Unable to get packetsSentPerSecond perf counter data")
		return nil, err
	}

	bytesReceivedPerSecondData, err := n.bytesReceivedPerSecondCounter.getDataList()
	if err != nil {
		logger.Error(err, "Unable to get bytesReceivedPerSecond perf counter data")
		return nil, err
	}

	bytesSentPerSecondData, err := n.bytesSentPerSecondCounter.getDataList()
	if err != nil {
		logger.Error(err, "Unable to get bytesSentPerSecond perf counter data")
		return nil, err
	}

	packetsReceivedDiscardedData, err := n.packetsReceivedDiscardedCounter.getDataList()
	if err != nil {
		logger.Error(err, "Unable to get packetsReceivedDiscarded perf counter data")
		return nil, err
	}

	packetsReceivedErrorsData, err := n.packetsReceivedErrorsCounter.getDataList()
	if err != nil {
		logger.Error(err, "Unable to get packetsReceivedErrors perf counter data")
		return nil, err
	}

	packetsOutboundDiscardedData, err := n.packetsOutboundDiscardedCounter.getDataList()
	if err != nil {
		logger.Error(err, "Unable to get packetsOutboundDiscarded perf counter data")
		return nil, err
	}

	packetsOutboundErrorsData, err := n.packetsOutboundErrorsCounter.getDataList()
	if err != nil {
		logger.Error(err, "Unable to get packetsOutboundErrors perf counter data")
		return nil, err
	}

	// 合并与列举都在锁内完成，保证返回一致快照。
	n.mu.Lock()
	defer n.mu.Unlock()
	n.mergeCollectedData(
		packetsReceivedPerSecondData,
		packetsSentPerSecondData,
		bytesReceivedPerSecondData,
		bytesSentPerSecondData,
		packetsReceivedDiscardedData,
		packetsReceivedErrorsData,
		packetsOutboundDiscardedData,
		packetsOutboundErrorsData,
	)
	return n.listInterfaceStats(), nil
}

// mergeCollectedData 把本轮采集的数据合并进缓存；调用方必须持有写锁。
func (n *networkCounter) mergeCollectedData(packetsReceivedPerSecondData,
	packetsSentPerSecondData,
	bytesReceivedPerSecondData,
	bytesSentPerSecondData,
	packetsReceivedDiscardedData,
	packetsReceivedErrorsData,
	packetsOutboundDiscardedData,
	packetsOutboundErrorsData map[string]uint64) {
	// 收集本轮出现的所有适配器，用于清理已消失的缓存项。
	adapters := sets.New[string]()

	// 逐类合并数据，并汇总出现的适配器名称。
	adapters.Insert(n.mergePacketsReceivedPerSecondData(packetsReceivedPerSecondData)...)
	adapters.Insert(n.mergePacketsSentPerSecondData(packetsSentPerSecondData)...)
	adapters.Insert(n.mergeBytesReceivedPerSecondData(bytesReceivedPerSecondData)...)
	adapters.Insert(n.mergeBytesSentPerSecondData(bytesSentPerSecondData)...)
	adapters.Insert(n.mergePacketsReceivedDiscardedData(packetsReceivedDiscardedData)...)
	adapters.Insert(n.mergePacketsReceivedErrorsData(packetsReceivedErrorsData)...)
	adapters.Insert(n.mergePacketsOutboundDiscardedData(packetsOutboundDiscardedData)...)
	adapters.Insert(n.mergePacketsOutboundErrorsData(packetsOutboundErrorsData)...)

	// 删除本轮未出现的适配器缓存，避免上报已移除的网卡。
	for adapter := range n.adapterStats {
		if !adapters.Has(adapter) {
			delete(n.adapterStats, adapter)
		}
	}
}

// mergePacketsReceivedPerSecondData 累加接收包速率并返回出现的适配器名。
func (n *networkCounter) mergePacketsReceivedPerSecondData(packetsReceivedPerSecondData map[string]uint64) []string {
	var adapters []string
	for adapterName, value := range packetsReceivedPerSecondData {
		adapters = append(adapters, adapterName)
		newStat := n.adapterStats[adapterName]
		newStat.Name = adapterName
		newStat.RxPackets = newStat.RxPackets + value
		n.adapterStats[adapterName] = newStat
	}

	return adapters
}

// mergePacketsSentPerSecondData 累加发送包速率并返回出现的适配器名。
func (n *networkCounter) mergePacketsSentPerSecondData(packetsSentPerSecondData map[string]uint64) []string {
	var adapters []string
	for adapterName, value := range packetsSentPerSecondData {
		adapters = append(adapters, adapterName)
		newStat := n.adapterStats[adapterName]
		newStat.Name = adapterName
		newStat.TxPackets = newStat.TxPackets + value
		n.adapterStats[adapterName] = newStat
	}

	return adapters
}

// mergeBytesReceivedPerSecondData 累加接收字节速率并返回出现的适配器名。
func (n *networkCounter) mergeBytesReceivedPerSecondData(bytesReceivedPerSecondData map[string]uint64) []string {
	var adapters []string
	for adapterName, value := range bytesReceivedPerSecondData {
		adapters = append(adapters, adapterName)
		newStat := n.adapterStats[adapterName]
		newStat.Name = adapterName
		newStat.RxBytes = newStat.RxBytes + value
		n.adapterStats[adapterName] = newStat
	}

	return adapters
}

// mergeBytesSentPerSecondData 累加发送字节速率并返回出现的适配器名。
func (n *networkCounter) mergeBytesSentPerSecondData(bytesSentPerSecondData map[string]uint64) []string {
	var adapters []string
	for adapterName, value := range bytesSentPerSecondData {
		adapters = append(adapters, adapterName)
		newStat := n.adapterStats[adapterName]
		newStat.Name = adapterName
		newStat.TxBytes = newStat.TxBytes + value
		n.adapterStats[adapterName] = newStat
	}

	return adapters
}

// mergePacketsReceivedDiscardedData 写入接收丢包数：该计数器本身是累计值，直接覆盖。
func (n *networkCounter) mergePacketsReceivedDiscardedData(packetsReceivedDiscardedData map[string]uint64) []string {
	var adapters []string
	for adapterName, value := range packetsReceivedDiscardedData {
		adapters = append(adapters, adapterName)
		newStat := n.adapterStats[adapterName]
		newStat.Name = adapterName
		newStat.RxDropped = value
		n.adapterStats[adapterName] = newStat
	}

	return adapters
}

// mergePacketsReceivedErrorsData 写入接收错误数：该计数器本身是累计值，直接覆盖。
func (n *networkCounter) mergePacketsReceivedErrorsData(packetsReceivedErrorsData map[string]uint64) []string {
	var adapters []string
	for adapterName, value := range packetsReceivedErrorsData {
		adapters = append(adapters, adapterName)
		newStat := n.adapterStats[adapterName]
		newStat.Name = adapterName
		newStat.RxErrors = value
		n.adapterStats[adapterName] = newStat
	}

	return adapters
}

// mergePacketsOutboundDiscardedData 写入发送丢包数：该计数器本身是累计值，直接覆盖。
func (n *networkCounter) mergePacketsOutboundDiscardedData(packetsOutboundDiscardedData map[string]uint64) []string {
	var adapters []string
	for adapterName, value := range packetsOutboundDiscardedData {
		adapters = append(adapters, adapterName)
		newStat := n.adapterStats[adapterName]
		newStat.Name = adapterName
		newStat.TxDropped = value
		n.adapterStats[adapterName] = newStat
	}

	return adapters
}

// mergePacketsOutboundErrorsData 写入发送错误数：该计数器本身是累计值，直接覆盖。
func (n *networkCounter) mergePacketsOutboundErrorsData(packetsOutboundErrorsData map[string]uint64) []string {
	var adapters []string
	for adapterName, value := range packetsOutboundErrorsData {
		adapters = append(adapters, adapterName)
		newStat := n.adapterStats[adapterName]
		newStat.Name = adapterName
		newStat.TxErrors = value
		n.adapterStats[adapterName] = newStat
	}

	return adapters
}

// listInterfaceStats 把适配器统计 map 转为切片返回。
func (n *networkCounter) listInterfaceStats() []cadvisorapi.InterfaceStats {
	stats := make([]cadvisorapi.InterfaceStats, 0, len(n.adapterStats))
	for _, stat := range n.adapterStats {
		stats = append(stats, stat)
	}
	return stats
}
