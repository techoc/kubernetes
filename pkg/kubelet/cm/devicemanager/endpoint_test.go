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

package devicemanager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	plugin "k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/plugin/v1beta1"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// monitorCallback 在设备健康状态变化、上报新设备或删除旧设备时被调用，
// devices 为设备的最新状态。
type monitorCallback func(logger klog.Logger, resourceName string, devices []*pluginapi.Device)

// newMockPluginManager 返回所有回调均为空实现的 mock 插件管理器。
func newMockPluginManager() *mockPluginManager {
	return &mockPluginManager{
		func(string) error { return nil },
		func(string, plugin.DevicePlugin) error { return nil },
		func(klog.Logger, string, string) {},
		func(string, *pluginapi.ListAndWatchResponse) {},
	}
}

// mockPluginManager 通过可替换的函数字段模拟插件管理器。
type mockPluginManager struct {
	cleanupPluginDirectory     func(string) error
	pluginConnected            func(string, plugin.DevicePlugin) error
	pluginDisconnected         func(klog.Logger, string, string)
	pluginListAndWatchReceiver func(string, *pluginapi.ListAndWatchResponse)
}

// CleanupPluginDirectory 委托给注入的清理函数。
func (m *mockPluginManager) CleanupPluginDirectory(r string) error {
	return m.cleanupPluginDirectory(r)
}

// PluginConnected 委托给注入的连接回调。
func (m *mockPluginManager) PluginConnected(_ context.Context, r string, p plugin.DevicePlugin) error {
	return m.pluginConnected(r, p)
}

// PluginDisconnected 委托给注入的断连回调。
func (m *mockPluginManager) PluginDisconnected(logger klog.Logger, r string, s string) {
	m.pluginDisconnected(logger, r, s)
}

// PluginListAndWatchReceiver 委托给注入的 ListAndWatch 回调。
func (m *mockPluginManager) PluginListAndWatchReceiver(_ klog.Logger, r string, lr *pluginapi.ListAndWatchResponse) {
	m.pluginListAndWatchReceiver(r, lr)
}

// esocketName 生成带纳秒时间戳的唯一 socket 文件名，避免用例冲突。
func esocketName() string {
	return fmt.Sprintf("mock%d.sock", time.Now().UnixNano())
}

// TestNewEndpoint 验证 endpoint 能完成创建与清理流程。
func TestNewEndpoint(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	socket := filepath.Join(os.TempDir(), esocketName())

	devs := []*pluginapi.Device{
		{ID: "ADeviceId", Health: pluginapi.Healthy},
	}

	p, e := esetup(tCtx, t, devs, socket, "mock", func(logger klog.Logger, n string, d []*pluginapi.Device) {})
	defer func() {
		err := ecleanup(logger, p, e)
		require.NoError(t, err)
	}()
}

// TestRun 验证 endpoint 先上报注册设备，再上报更新后的设备列表。
func TestRun(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	socket := filepath.Join(os.TempDir(), esocketName())

	devs := []*pluginapi.Device{
		{ID: "ADeviceId", Health: pluginapi.Healthy},
		{ID: "AnotherDeviceId", Health: pluginapi.Healthy},
		{ID: "AThirdDeviceId", Health: pluginapi.Unhealthy},
	}

	updated := []*pluginapi.Device{
		{ID: "ADeviceId", Health: pluginapi.Unhealthy},
		{ID: "AThirdDeviceId", Health: pluginapi.Healthy},
		{ID: "AFourthDeviceId", Health: pluginapi.Healthy},
	}

	callbackCount := 0
	callbackChan := make(chan int)
	callback := func(_ klog.Logger, n string, devices []*pluginapi.Device) {
		// 回调应被调用两次：一次插件注册，一次插件更新。
		if callbackCount > 2 {
			t.FailNow()
		}

		// 校验注册时上报的初始设备列表。
		if callbackCount == 0 {
			require.Len(t, devices, 3)
			require.Equal(t, devices[0].ID, devs[0].ID)
			require.Equal(t, devices[1].ID, devs[1].ID)
			require.Equal(t, devices[2].ID, devs[2].ID)
			require.Equal(t, devices[0].Health, devs[0].Health)
			require.Equal(t, devices[1].Health, devs[1].Health)
			require.Equal(t, devices[2].Health, devs[2].Health)
		}

		// 校验更新后上报的设备列表。
		if callbackCount == 1 {
			require.Len(t, devices, 3)
			require.Equal(t, devices[0].ID, updated[0].ID)
			require.Equal(t, devices[1].ID, updated[1].ID)
			require.Equal(t, devices[2].ID, updated[2].ID)
			require.Equal(t, devices[0].Health, updated[0].Health)
			require.Equal(t, devices[1].Health, updated[1].Health)
			require.Equal(t, devices[2].Health, updated[2].Health)
		}

		callbackCount++
		callbackChan <- callbackCount
	}

	p, e := esetup(tCtx, t, devs, socket, "mock", callback)
	defer func() {
		err := ecleanup(logger, p, e)
		require.NoError(t, err)
	}()

	go e.client.Run(tCtx)
	// 等待第一次回调（注册）。
	<-callbackChan

	p.Update(updated)

	// 等待第二次回调（更新）。
	<-callbackChan

	require.Equal(t, 2, callbackCount)
}

// TestAllocate 验证 Allocate 请求会转发给插件并原样返回分配结果。
func TestAllocate(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	socket := filepath.Join(os.TempDir(), esocketName())
	devs := []*pluginapi.Device{
		{ID: "ADeviceId", Health: pluginapi.Healthy},
	}
	callbackCount := 0
	callbackChan := make(chan int)
	p, e := esetup(tCtx, t, devs, socket, "mock", func(_ klog.Logger, n string, d []*pluginapi.Device) {
		callbackCount++
		callbackChan <- callbackCount
	})
	defer func() {
		err := ecleanup(logger, p, e)
		require.NoError(t, err)
	}()

	resp := new(pluginapi.AllocateResponse)
	contResp := new(pluginapi.ContainerAllocateResponse)
	contResp.Devices = append(contResp.Devices, &pluginapi.DeviceSpec{
		ContainerPath: "/dev/aaa",
		HostPath:      "/dev/aaa",
		Permissions:   "mrw",
	})

	contResp.Devices = append(contResp.Devices, &pluginapi.DeviceSpec{
		ContainerPath: "/dev/bbb",
		HostPath:      "/dev/bbb",
		Permissions:   "mrw",
	})

	contResp.Mounts = append(contResp.Mounts, &pluginapi.Mount{
		ContainerPath: "/container_dir1/file1",
		HostPath:      "host_dir1/file1",
		ReadOnly:      true,
	})

	resp.ContainerResponses = append(resp.ContainerResponses, contResp)

	p.SetAllocFunc(func(r *pluginapi.AllocateRequest, devs map[string]*pluginapi.Device) (*pluginapi.AllocateResponse, error) {
		return resp, nil
	})

	go e.client.Run(tCtx)
	// 等待注册回调，确保 client 已连接。
	select {
	case <-callbackChan:
		break
	case <-time.After(time.Second):
		t.FailNow()
	}

	// 分配结果应与插件返回的响应逐字段一致。
	respOut, err := e.allocate(tCtx, []string{"ADeviceId"})
	require.NoError(t, err)
	require.True(t, proto.Equal(resp, respOut))
}

// TestGetPreferredAllocation 验证首选分配请求会转发给插件并原样返回结果。
func TestGetPreferredAllocation(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	socket := filepath.Join(os.TempDir(), esocketName())
	callbackCount := 0
	callbackChan := make(chan int)
	p, e := esetup(tCtx, t, []*pluginapi.Device{}, socket, "mock", func(_ klog.Logger, n string, d []*pluginapi.Device) {
		callbackCount++
		callbackChan <- callbackCount
	})
	defer func() {
		err := ecleanup(logger, p, e)
		require.NoError(t, err)
	}()

	resp := &pluginapi.PreferredAllocationResponse{
		ContainerResponses: []*pluginapi.ContainerPreferredAllocationResponse{
			{DeviceIDs: []string{"device0", "device1", "device2"}},
		},
	}

	p.SetGetPreferredAllocFunc(func(r *pluginapi.PreferredAllocationRequest, devs map[string]*pluginapi.Device) (*pluginapi.PreferredAllocationResponse, error) {
		return resp, nil
	})

	go e.client.Run(tCtx)
	// 等待注册回调，确保 client 已连接。
	select {
	case <-callbackChan:
		break
	case <-time.After(time.Second):
		t.FailNow()
	}

	// 首选分配结果应与插件返回的响应一致。
	respOut, err := e.getPreferredAllocation(tCtx, []string{}, []string{}, -1)
	require.NoError(t, err)
	require.True(t, proto.Equal(resp, respOut))
}

// esetup 启动测试插件 stub、连接 client 并构造 endpoint，供各用例复用。
func esetup(ctx context.Context, t *testing.T, devs []*pluginapi.Device, socket, resourceName string, callback monitorCallback) (*plugin.Stub, *endpointImpl) {
	logger := klog.FromContext(ctx)
	m := newMockPluginManager()

	// ListAndWatch 响应先拷贝设备列表，再交给用例回调。
	m.pluginListAndWatchReceiver = func(r string, resp *pluginapi.ListAndWatchResponse) {
		var newDevs []*pluginapi.Device
		for _, d := range resp.Devices {
			newDevs = append(newDevs, d)
		}
		callback(klog.FromContext(ctx), resourceName, newDevs)
	}

	var dp plugin.DevicePlugin
	var wg sync.WaitGroup
	wg.Add(1)
	m.pluginConnected = func(r string, c plugin.DevicePlugin) error {
		dp = c
		wg.Done()
		return nil
	}

	p := plugin.NewDevicePluginStub(logger, devs, socket, resourceName, false, false)
	err := p.Start(ctx)
	require.NoError(t, err)

	c := plugin.NewPluginClient(resourceName, socket, m)
	err = c.Connect(ctx)
	require.NoError(t, err)

	// 等待 PluginConnected 回调拿到真正的 DevicePlugin 实现。
	wg.Wait()

	e := newEndpointImpl(dp)
	e.client = c

	m.pluginDisconnected = func(logger klog.Logger, r string, s string) {
		e.setStopTime(time.Now())
	}

	return p, e
}

// ecleanup 停止插件 stub 并断开 client。
func ecleanup(logger klog.Logger, p *plugin.Stub, e *endpointImpl) error {
	if err := p.Stop(logger); err != nil {
		return err
	}
	return e.client.Disconnect(logger)
}
