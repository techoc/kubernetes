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
	"time"
	"unsafe"

	"github.com/JeffAshton/win_pdh"
)

const (
	// Windows 性能计数器路径：CPU、内存与网络指标。
	cpuQuery                  = "\\Processor(_Total)\\% Processor Time"
	memoryPrivWorkingSetQuery = "\\Process(_Total)\\Working Set - Private"
	memoryCommittedBytesQuery = "\\Memory\\Committed Bytes"
	// 性能计数器每 10 秒更新一次，与 cadvisor 默认的 housekeeping 间隔一致，
	// 参见 https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/cadvisor/cadvisor_linux.go
	perfCounterUpdatePeriod = 10 * time.Second
	// defaultCachePeriod 是每个 cpuUsage 的默认缓存周期，
	// 与 cadvisor 配置及容器统计的时间间隔保持一致，
	// 参见 https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/cadvisor/cadvisor_linux.go#L63
	defaultCachePeriod = 10 * time.Second
)

// perfCounter 抽象读取 Windows 性能计数器数据的能力。
type perfCounter interface {
	// getData 读取不含通配符的计数器值。
	getData() (uint64, error)
	// getDataList 读取含通配符的计数器值列表。
	getDataList() (map[string]uint64, error)
}

// perfCounterImpl 是基于 Windows PDH API 的 perfCounter 实现。
type perfCounterImpl struct {
	queryHandle   win_pdh.PDH_HQUERY
	counterHandle win_pdh.PDH_HCOUNTER
}

// newPerfCounter 打开 PDH 查询并注册指定计数器。
func newPerfCounter(counter string) (perfCounter, error) {
	var queryHandle win_pdh.PDH_HQUERY
	var counterHandle win_pdh.PDH_HCOUNTER

	// 每个计数器使用独立的 PDH 查询句柄。
	ret := win_pdh.PdhOpenQuery(0, 0, &queryHandle)
	if ret != win_pdh.ERROR_SUCCESS {
		return nil, errors.New("unable to open query through DLL call")
	}

	ret = win_pdh.PdhAddEnglishCounter(queryHandle, counter, 0, &counterHandle)
	if ret != win_pdh.ERROR_SUCCESS {
		return nil, fmt.Errorf("unable to add process counter: %s. Error code is %x", counter, ret)
	}

	ret = win_pdh.PdhCollectQueryData(queryHandle)
	if ret != win_pdh.ERROR_SUCCESS {
		return nil, fmt.Errorf("unable to collect data from counter. Error code is %x", ret)
	}

	return &perfCounterImpl{
		queryHandle:   queryHandle,
		counterHandle: counterHandle,
	}, nil
}

// getData 用于读取计数器名称中不含 * 的数据。
func (p *perfCounterImpl) getData() (uint64, error) {
	filledBuf, bufCount, err := p.getQueriedData()
	if err != nil {
		return 0, err
	}

	// 不含通配符时缓冲区通常只有一项。
	var data uint64 = 0
	for i := 0; i < int(bufCount); i++ {
		c := filledBuf[i]
		data = uint64(c.FmtValue.DoubleValue)
	}

	return data, nil
}

// getDataList 用于读取计数器名称中带 * 的数据，按名称聚合。
func (p *perfCounterImpl) getDataList() (map[string]uint64, error) {
	filledBuf, bufCount, err := p.getQueriedData()
	if err != nil {
		return nil, err
	}

	data := map[string]uint64{}
	for i := 0; i < int(bufCount); i++ {
		c := filledBuf[i]
		value := uint64(c.FmtValue.DoubleValue)
		name := win_pdh.UTF16PtrToString(c.SzName)
		data[name] = value
	}

	return data, nil
}

// getQueriedData 使用当前查询句柄读取计数器数据。
func (p *perfCounterImpl) getQueriedData() ([]win_pdh.PDH_FMT_COUNTERVALUE_ITEM_DOUBLE, uint32, error) {
	ret := win_pdh.PdhCollectQueryData(p.queryHandle)
	if ret != win_pdh.ERROR_SUCCESS {
		return nil, 0, fmt.Errorf("unable to collect data from counter. Error code is %x", ret)
	}

	// 先探测所需缓冲区大小，再分配足够空间重新读取。
	var bufSize, bufCount uint32
	var size = uint32(unsafe.Sizeof(win_pdh.PDH_FMT_COUNTERVALUE_ITEM_DOUBLE{}))
	var emptyBuf [1]win_pdh.PDH_FMT_COUNTERVALUE_ITEM_DOUBLE // 至少提供一个可寻址的空指针元素。

	ret = win_pdh.PdhGetFormattedCounterArrayDouble(p.counterHandle, &bufSize, &bufCount, &emptyBuf[0])
	if ret != win_pdh.PDH_MORE_DATA {
		return nil, 0, fmt.Errorf("unable to collect data from counter. Error code is %x", ret)
	}

	// 按 PDH 返回的计数重新分配缓冲区。
	filledBuf := make([]win_pdh.PDH_FMT_COUNTERVALUE_ITEM_DOUBLE, bufCount*size)
	ret = win_pdh.PdhGetFormattedCounterArrayDouble(p.counterHandle, &bufSize, &bufCount, &filledBuf[0])
	if ret != win_pdh.ERROR_SUCCESS {
		return nil, 0, fmt.Errorf("unable to collect data from counter. Error code is %x", ret)
	}

	return filledBuf, bufCount, nil
}
