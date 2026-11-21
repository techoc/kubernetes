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
	"errors"
	"testing"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/assert"

	"k8s.io/klog/v2/ktesting"
)

// fakeAdapterName 是测试用网络适配器名。
const fakeAdapterName = "fake-adapter"

// fakePerfCounterImpl 是可注入错误的性能计数器桩。
type fakePerfCounterImpl struct {
	// 返回的名称。
	name string
	// 返回的值。
	value uint64
	// 是否返回错误。
	raiseError bool
}

// getData 返回预设值或错误。
func (p *fakePerfCounterImpl) getData() (uint64, error) {
	if p.raiseError {
		return 0, errors.New("Expected getData error.")
	}
	return p.value, nil
}

// getDataList 返回单适配器的计数 map 或错误。
func (p *fakePerfCounterImpl) getDataList() (map[string]uint64, error) {
	if p.raiseError {
		return nil, errors.New("Expected getDataList error.")
	}

	data := make(map[string]uint64)
	data[p.name] = p.value
	return data, nil
}

// newFakedNetworkCounters 构造包含 8 个 fake 计数器的 networkCounter。
func newFakedNetworkCounters(raiseError bool) *networkCounter {
	counters := make([]*fakePerfCounterImpl, 8)
	for i := 0; i < 8; i++ {
		counters[i] = &fakePerfCounterImpl{
			name:       fakeAdapterName,
			value:      1,
			raiseError: raiseError,
		}
	}
	return &networkCounter{
		packetsReceivedPerSecondCounter: counters[0],
		packetsSentPerSecondCounter:     counters[1],
		bytesReceivedPerSecondCounter:   counters[2],
		bytesSentPerSecondCounter:       counters[3],
		packetsReceivedDiscardedCounter: counters[4],
		packetsReceivedErrorsCounter:    counters[5],
		packetsOutboundDiscardedCounter: counters[6],
		packetsOutboundErrorsCounter:    counters[7],
		adapterStats:                    map[string]cadvisorapi.InterfaceStats{},
	}
}

// TestNewNetworkCounters 验证 newNetworkCounters 在 Windows 上能成功创建。
func TestNewNetworkCounters(t *testing.T) {
	_, err := newNetworkCounters()
	assert.NoError(t, err)
}

// TestNetworkGetData 验证 getData 汇总各计数器并清理已消失的适配器。
func TestNetworkGetData(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	netCounter := newFakedNetworkCounters(false)

	// 加入一个已不在 adapterStats 缓存中的适配器，处理后应被清理。
	netCounter.adapterStats["other-fake-adapter"] = cadvisorapi.InterfaceStats{}

	// 第一次调用应只返回当前适配器的数据。
	data, err := netCounter.getData(logger)
	assert.NoError(t, err)

	// 确认只返回单个适配器的数据。
	expectedStats := cadvisorapi.InterfaceStats{
		Name:      fakeAdapterName,
		RxPackets: 1,
		TxPackets: 1,
		RxBytes:   1,
		TxBytes:   1,
		RxDropped: 1,
		RxErrors:  1,
		TxDropped: 1,
		TxErrors:  1,
	}
	assert.Equal(t, []cadvisorapi.InterfaceStats{expectedStats}, data)

	// 返回的是累计值，因此第二次调用时数值应翻倍。
	data, err = netCounter.getData(logger)
	assert.NoError(t, err)
	expectedStats = cadvisorapi.InterfaceStats{
		Name:      fakeAdapterName,
		RxPackets: 2,
		TxPackets: 2,
		RxBytes:   2,
		TxBytes:   2,
		RxDropped: 1,
		RxErrors:  1,
		TxDropped: 1,
		TxErrors:  1,
	}
	assert.Equal(t, []cadvisorapi.InterfaceStats{expectedStats}, data)
}

// TestNetworkGetDataFailures 验证任一计数器读取失败时 getData 返回错误。
func TestNetworkGetDataFailures(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 初始让所有计数器返回错误。
	netCounter := newFakedNetworkCounters(true)

	_, err := netCounter.getData(logger)
	expectedMsg := "Expected getDataList error."
	if err == nil || err.Error() != expectedMsg {
		t.Fatalf("expected error message `%s` but got `%v`", expectedMsg, err)
	}

	_, err = netCounter.getData(logger)
	// 逐个关闭错误，确认每个计数器失败都会向外传播。
	netCounter.packetsReceivedPerSecondCounter.(*fakePerfCounterImpl).raiseError = false
	if err == nil || err.Error() != expectedMsg {
		t.Fatalf("expected error message `%s` but got `%v`", expectedMsg, err)
	}

	_, err = netCounter.getData(logger)
	netCounter.packetsSentPerSecondCounter.(*fakePerfCounterImpl).raiseError = false
	if err == nil || err.Error() != expectedMsg {
		t.Fatalf("expected error message `%s` but got `%v`", expectedMsg, err)
	}

	_, err = netCounter.getData(logger)
	netCounter.bytesReceivedPerSecondCounter.(*fakePerfCounterImpl).raiseError = false
	if err == nil || err.Error() != expectedMsg {
		t.Fatalf("expected error message `%s` but got `%v`", expectedMsg, err)
	}

	_, err = netCounter.getData(logger)
	netCounter.bytesSentPerSecondCounter.(*fakePerfCounterImpl).raiseError = false
	if err == nil || err.Error() != expectedMsg {
		t.Fatalf("expected error message `%s` but got `%v`", expectedMsg, err)
	}

	_, err = netCounter.getData(logger)
	netCounter.packetsReceivedDiscardedCounter.(*fakePerfCounterImpl).raiseError = false
	if err == nil || err.Error() != expectedMsg {
		t.Fatalf("expected error message `%s` but got `%v`", expectedMsg, err)
	}

	_, err = netCounter.getData(logger)
	netCounter.packetsReceivedErrorsCounter.(*fakePerfCounterImpl).raiseError = false
	if err == nil || err.Error() != expectedMsg {
		t.Fatalf("expected error message `%s` but got `%v`", expectedMsg, err)
	}

	_, err = netCounter.getData(logger)
	netCounter.packetsOutboundDiscardedCounter.(*fakePerfCounterImpl).raiseError = false
	if err == nil || err.Error() != expectedMsg {
		t.Fatalf("expected error message `%s` but got `%v`", expectedMsg, err)
	}
}
