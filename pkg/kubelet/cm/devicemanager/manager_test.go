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
	"reflect"
	goruntime "runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	watcherapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/checkpoint"
	plugin "k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/plugin/v1beta1"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/initoption"
)

const (
	testResourceName = "fake-domain/resource"
)

// 在真实 ManagerImpl 外包一层 wrappedManagerImpl，
// 把设备更新回调暴露给测试，并创建用于接收 ListAndWatch 的 gRPC server。
func newWrappedManagerImpl(logger klog.Logger, socketPath string, manager *ManagerImpl) *wrappedManagerImpl {
	w := &wrappedManagerImpl{
		ManagerImpl: manager,
		callback:    manager.genericDeviceUpdateCallback,
	}
	w.socketdir, _ = filepath.Split(socketPath)
	w.server, _ = plugin.NewServer(logger, socketPath, w, w)
	return w
}

type wrappedManagerImpl struct {
	*ManagerImpl
	socketdir string
	callback  func(klog.Logger, string, []*pluginapi.Device)
}

// 把插件的 ListAndWatch 响应转交给内部回调，测试可借此拦截设备更新。
func (m *wrappedManagerImpl) PluginListAndWatchReceiver(logger klog.Logger, r string, resp *pluginapi.ListAndWatchResponse) {
	m.callback(logger, r, resp.Devices)
}

// 创建临时目录，并给出 manager 与插件各自的 socket 路径，
// 测试结束后由调用方负责清理。
func tmpSocketDir() (socketDir, socketName, pluginSocketName string, err error) {
	socketDir, err = os.MkdirTemp("", "device_plugin")
	if err != nil {
		return
	}
	socketName = filepath.Join(socketDir, "server.sock")
	pluginSocketName = filepath.Join(socketDir, "device-plugin.sock")
	os.MkdirAll(socketDir, 0755)
	return
}

// 验证仅创建（未启动）device manager 时不会返回错误。
func TestNewManagerImpl(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	socketDir, socketName, _, err := tmpSocketDir()
	topologyStore := topologymanager.NewFakeManager(logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(socketDir), "unable to remove dir %s", socketDir)
	})
	_, err = newManagerImpl(logger, socketName, nil, topologyStore)
	require.NoError(t, err)
	os.RemoveAll(socketDir)
}

// 验证 manager 能正常启动与停止，且 Stop 可重复调用而不报错（幂等）。
func TestNewManagerImplStart(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	socketDir, socketName, pluginSocketName, err := tmpSocketDir()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(socketDir), "unable to remove dir %s", socketDir)
	})
	m, _, p := setup(tCtx, t, []*pluginapi.Device{}, func(_ klog.Logger, n string, d []*pluginapi.Device) {}, socketName, pluginSocketName)
	err = cleanup(logger, m, p)
	require.NoError(t, err)
	// Stop 应允许多次调用。
	err = cleanup(logger, m, p)
	require.NoError(t, err)
}

// 验证在 probe 模式下 manager 也能正常启动与停止。
func TestNewManagerImplStartProbeMode(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	socketDir, socketName, pluginSocketName, err := tmpSocketDir()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(socketDir), "unable to remove dir %s", socketDir)
	})
	m, _, p, _ := setupInProbeMode(tCtx, t, []*pluginapi.Device{}, func(_ klog.Logger, n string, d []*pluginapi.Device) {}, socketName, pluginSocketName)
	err = cleanup(logger, m, p)
	require.NoError(t, err)
}

// 验证插件注册、重复注册和换设备重新注册三种情况：
// 注册后设备数正确；同一插件重复注册不会重复计数；换设备重新注册时旧设备被移除。
func TestDevicePluginReRegistration(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	// TODO：待 https://github.com/kubernetes/kubernetes/pull/115269 合并后移除跳过。
	if goruntime.GOOS == "windows" {
		t.Skip("Skipping test on Windows.")
	}
	socketDir, socketName, pluginSocketName, err := tmpSocketDir()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(socketDir), "unable to remove dir %s", socketDir)
	})
	devs := []*pluginapi.Device{
		{ID: "Dev1", Health: pluginapi.Healthy},
		{ID: "Dev2", Health: pluginapi.Healthy},
	}
	devsForRegistration := []*pluginapi.Device{
		{ID: "Dev3", Health: pluginapi.Healthy},
	}
	for _, preStartContainerFlag := range []bool{false, true} {
		for _, getPreferredAllocationFlag := range []bool{false, true} {
			m, ch, p1 := setup(tCtx, t, devs, nil, socketName, pluginSocketName)
			err = p1.Register(tCtx, socketName, testResourceName, "")
			require.NoError(t, err)

			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout while waiting for manager update")
			}
			capacity, allocatable, _ := m.GetCapacity(logger)
			resourceCapacity := capacity[v1.ResourceName(testResourceName)]
			resourceAllocatable := allocatable[v1.ResourceName(testResourceName)]
			require.Equal(t, resourceCapacity.Value(), resourceAllocatable.Value(), "capacity should equal to allocatable")
			require.Equal(t, int64(2), resourceAllocatable.Value(), "Devices are not updated.")

			p2 := plugin.NewDevicePluginStub(logger, devs, pluginSocketName+".new", testResourceName, preStartContainerFlag, getPreferredAllocationFlag)
			err = p2.Start(tCtx)
			require.NoError(t, err)
			err = p2.Register(tCtx, socketName, testResourceName, "")
			require.NoError(t, err)

			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout while waiting for manager update")
			}
			capacity, allocatable, _ = m.GetCapacity(logger)
			resourceCapacity = capacity[v1.ResourceName(testResourceName)]
			resourceAllocatable = allocatable[v1.ResourceName(testResourceName)]
			require.Equal(t, resourceCapacity.Value(), resourceAllocatable.Value(), "capacity should equal to allocatable")
			require.Equal(t, int64(2), resourceAllocatable.Value(), "Devices shouldn't change.")

			// 场景：插件以不同的设备集合重新注册，旧插件设备应被移除。
			p3 := plugin.NewDevicePluginStub(logger, devsForRegistration, pluginSocketName+".third", testResourceName, preStartContainerFlag, getPreferredAllocationFlag)
			err = p3.Start(tCtx)
			require.NoError(t, err)
			err = p3.Register(tCtx, socketName, testResourceName, "")
			require.NoError(t, err)

			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout while waiting for manager update")
			}
			capacity, allocatable, _ = m.GetCapacity(logger)
			resourceCapacity = capacity[v1.ResourceName(testResourceName)]
			resourceAllocatable = allocatable[v1.ResourceName(testResourceName)]
			require.Equal(t, resourceCapacity.Value(), resourceAllocatable.Value(), "capacity should equal to allocatable")
			require.Equal(t, int64(1), resourceAllocatable.Value(), "Devices of plugin previously registered should be removed.")
			err = p2.Stop(logger)
			require.NoError(t, err)
			err = p3.Stop(logger)
			require.NoError(t, err)
			err = cleanup(logger, m, p1)
			require.NoError(t, err)
		}
	}
}

// 与 TestDevicePluginReRegistration 场景相同，但走 kubelet probe 发现机制，
// 通过插件 manager 主动探测 socket 完成注册。
func TestDevicePluginReRegistrationProbeMode(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	// TODO：待 https://github.com/kubernetes/kubernetes/pull/115269 合并后移除跳过。
	if goruntime.GOOS == "windows" {
		t.Skip("Skipping test on Windows.")
	}
	socketDir, socketName, pluginSocketName, err := tmpSocketDir()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(socketDir), "unable to remove dir %s", socketDir)
	})
	devs := []*pluginapi.Device{
		{ID: "Dev1", Health: pluginapi.Healthy},
		{ID: "Dev2", Health: pluginapi.Healthy},
	}
	devsForRegistration := []*pluginapi.Device{
		{ID: "Dev3", Health: pluginapi.Healthy},
	}

	m, ch, p1, _ := setupInProbeMode(tCtx, t, devs, nil, socketName, pluginSocketName)

	// 等待插件完成首次注册并推送设备列表。
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.FailNow()
	}
	capacity, allocatable, _ := m.GetCapacity(logger)
	resourceCapacity := capacity[v1.ResourceName(testResourceName)]
	resourceAllocatable := allocatable[v1.ResourceName(testResourceName)]
	require.Equal(t, resourceCapacity.Value(), resourceAllocatable.Value(), "capacity should equal to allocatable")
	require.Equal(t, int64(2), resourceAllocatable.Value(), "Devices are not updated.")

	p2 := plugin.NewDevicePluginStub(logger, devs, pluginSocketName+".new", testResourceName, false, false)
	err = p2.Start(tCtx)
	require.NoError(t, err)
	// 等待第二个插件注册后的设备更新回调。
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.FailNow()
	}

	capacity, allocatable, _ = m.GetCapacity(logger)
	resourceCapacity = capacity[v1.ResourceName(testResourceName)]
	resourceAllocatable = allocatable[v1.ResourceName(testResourceName)]
	require.Equal(t, resourceCapacity.Value(), resourceAllocatable.Value(), "capacity should equal to allocatable")
	require.Equal(t, int64(2), resourceAllocatable.Value(), "Devices are not updated.")

	// 测试插件使用不同设备重新注册的场景。
	p3 := plugin.NewDevicePluginStub(logger, devsForRegistration, pluginSocketName+".third", testResourceName, false, false)
	err = p3.Start(tCtx)
	require.NoError(t, err)
	// 等待第三个插件注册后的设备更新回调。
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.FailNow()
	}

	capacity, allocatable, _ = m.GetCapacity(logger)
	resourceCapacity = capacity[v1.ResourceName(testResourceName)]
	resourceAllocatable = allocatable[v1.ResourceName(testResourceName)]
	require.Equal(t, resourceCapacity.Value(), resourceAllocatable.Value(), "capacity should equal to allocatable")
	require.Equal(t, int64(1), resourceAllocatable.Value(), "Devices of previous registered should be removed")
	err = p2.Stop(logger)
	require.NoError(t, err)
	err = p3.Stop(logger)
	require.NoError(t, err)
	err = cleanup(logger, m, p1)
	require.NoError(t, err)
}

// 构造并启动一个 device manager，同时在每次设备更新回调后向 channel 发信号，
// 方便测试同步等待。
func setupDeviceManager(t *testing.T, devs []*pluginapi.Device, callback monitorCallback, socketName string,
	topology []cadvisorapi.Node, logger klog.Logger) (Manager, <-chan interface{}) {
	topologyStore := topologymanager.NewFakeManager(logger)
	m, err := newManagerImpl(logger, socketName, topology, topologyStore)
	require.NoError(t, err)
	updateChan := make(chan interface{})

	w := newWrappedManagerImpl(logger, socketName, m)
	if callback != nil {
		w.callback = callback
	}

	originalCallback := w.callback
	w.callback = func(logger klog.Logger, resourceName string, devices []*pluginapi.Device) {
		originalCallback(logger, resourceName, devices)
		updateChan <- new(interface{})
	}
	activePods := func() []*v1.Pod {
		return []*v1.Pod{}
	}

	// 此处测试稳态；初始化流程会涉及 sourcesReady、containerMap 和 containerRunningSet，将在另一流程中测试。
	err = w.Start(logger, activePods, &sourcesReadyStub{}, containermap.NewContainerMap(), sets.New[string]())
	require.NoError(t, err)

	return w, updateChan
}

// 启动一个 fake 设备插件桩，模拟插件端 ListAndWatch 服务。
func setupDevicePlugin(ctx context.Context, t *testing.T, devs []*pluginapi.Device, pluginSocketName string) *plugin.Stub {
	p := plugin.NewDevicePluginStub(klog.FromContext(ctx), devs, pluginSocketName, testResourceName, false, false)
	err := p.Start(ctx)
	require.NoError(t, err)
	return p
}

// 创建并启动 plugin manager，并把 device manager 的 watcher handler 注册进去。
func setupPluginManager(t *testing.T, pluginSocketName string, m Manager) pluginmanager.PluginManager {
	pluginManager := pluginmanager.NewPluginManager(
		filepath.Dir(pluginSocketName), /* socket 目录 */
		&record.FakeRecorder{},
	)

	tCtx := ktesting.Init(t)
	runPluginManager(tCtx, pluginManager)
	pluginManager.AddHandler(watcherapi.DevicePlugin, m.GetWatcherHandler())
	return pluginManager
}

// 在后台启动 plugin manager，sourcesReady 恒为 true 以跳过源就绪等待。
func runPluginManager(ctx context.Context, pluginManager pluginmanager.PluginManager) {
	// FIXME：将 sets.Set[string] 替换为 sets.Set[string]。
	sourcesReady := config.NewSourcesReady(func(_ sets.Set[string]) bool { return true })
	go pluginManager.Run(ctx, sourcesReady, wait.NeverStop)
}

// 一站式装配 device manager 与 fake 插件，供大多数测试复用。
func setup(ctx context.Context, t *testing.T, devs []*pluginapi.Device, callback monitorCallback, socketName string, pluginSocketName string) (Manager, <-chan interface{}, *plugin.Stub) {
	logger := klog.FromContext(ctx)
	m, updateChan := setupDeviceManager(t, devs, callback, socketName, nil, logger)
	p := setupDevicePlugin(ctx, t, devs, pluginSocketName)
	return m, updateChan, p
}

// 在 setup 的基础上额外启动 plugin manager，用于 probe 模式测试。
func setupInProbeMode(ctx context.Context, t *testing.T, devs []*pluginapi.Device, callback monitorCallback, socketName string, pluginSocketName string) (Manager, <-chan interface{}, *plugin.Stub, pluginmanager.PluginManager) {
	logger := klog.FromContext(ctx)
	m, updateChan := setupDeviceManager(t, devs, callback, socketName, nil, logger)
	p := setupDevicePlugin(ctx, t, devs, pluginSocketName)
	pm := setupPluginManager(t, pluginSocketName, m)
	return m, updateChan, p, pm
}

// 先停插件再停 manager，避免 manager 在插件仍连接时退出。
func cleanup(logger klog.Logger, m Manager, p *plugin.Stub) error {
	if err := p.Stop(logger); err != nil {
		return err
	}
	return m.Stop(logger)
}

// 验证设备健康状态变化对 capacity/allocatable 的影响：
// 不健康设备计入 capacity 但不计入 allocatable；删除设备两者同减；
// 端点超过宽限期后资源被移除；写入 checkpoint 后重建管理器仍把该资源视为设备插件资源，
// 从而让调度期间创建的 Pod 被正确拒绝。
func TestUpdateCapacityAllocatable(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	socketDir, socketName, _, err := tmpSocketDir()
	topologyStore := topologymanager.NewFakeManager(logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(socketDir), "unable to remove dir %s", socketDir)
	})
	testManager, err := newManagerImpl(logger, socketName, nil, topologyStore)
	as := assert.New(t)
	as.NotNil(testManager)
	as.NoError(err)

	devs := []*pluginapi.Device{
		{ID: "Device1", Health: pluginapi.Healthy},
		{ID: "Device2", Health: pluginapi.Healthy},
		{ID: "Device3", Health: pluginapi.Unhealthy},
	}
	callback := testManager.genericDeviceUpdateCallback

	// 为 resource1 加入三台设备（两健康一不健康）：capacity 计 3，allocatable 只计 2。
	resourceName1 := "domain1.com/resource1"
	e1 := &endpointImpl{}
	testManager.endpoints[resourceName1] = endpointInfo{e: e1, opts: nil}
	callback(logger, resourceName1, devs)
	capacity, allocatable, removedResources := testManager.GetCapacity(logger)
	resource1Capacity, ok := capacity[v1.ResourceName(resourceName1)]
	as.True(ok)
	resource1Allocatable, ok := allocatable[v1.ResourceName(resourceName1)]
	as.True(ok)
	as.Equal(int64(3), resource1Capacity.Value())
	as.Equal(int64(2), resource1Allocatable.Value())
	as.Empty(removedResources)

	// 删除不健康设备只会降低 capacity，不影响 allocatable。
	devs1 := devs[:len(devs)-1]
	callback(logger, resourceName1, devs1)
	capacity, allocatable, removedResources = testManager.GetCapacity(logger)
	resource1Capacity, ok = capacity[v1.ResourceName(resourceName1)]
	as.True(ok)
	resource1Allocatable, ok = allocatable[v1.ResourceName(resourceName1)]
	as.True(ok)
	as.Equal(int64(2), resource1Capacity.Value())
	as.Equal(int64(2), resource1Allocatable.Value())
	as.Empty(removedResources)

	// 健康设备转为不健康，allocatable 减少 1，capacity 不变。
	devs[1].Health = pluginapi.Unhealthy
	callback(logger, resourceName1, devs)
	capacity, allocatable, removedResources = testManager.GetCapacity(logger)
	resource1Capacity, ok = capacity[v1.ResourceName(resourceName1)]
	as.True(ok)
	resource1Allocatable, ok = allocatable[v1.ResourceName(resourceName1)]
	as.True(ok)
	as.Equal(int64(3), resource1Capacity.Value())
	as.Equal(int64(1), resource1Allocatable.Value())
	as.Empty(removedResources)

	// 删除健康设备，capacity 与 allocatable 同时减少 1。
	devs2 := devs[1:]
	callback(logger, resourceName1, devs2)
	capacity, allocatable, removedResources = testManager.GetCapacity(logger)
	resource1Capacity, ok = capacity[v1.ResourceName(resourceName1)]
	as.True(ok)
	resource1Allocatable, ok = allocatable[v1.ResourceName(resourceName1)]
	as.True(ok)
	as.Equal(int64(0), resource1Allocatable.Value())
	as.Equal(int64(2), resource1Capacity.Value())
	as.Empty(removedResources)

	// 验证新增第二个资源时两者互不影响。
	resourceName2 := "resource2"
	e2 := &endpointImpl{socket: socketName}
	e2.client = plugin.NewPluginClient(resourceName2, socketName, testManager)
	eInfo := endpointInfo{e: e2, opts: nil}
	testManager.endpoints[resourceName2] = eInfo
	testManager.endpointStore[resourceName2] = map[string]*endpointInfo{socketName: &eInfo}
	callback(logger, resourceName2, devs)
	capacity, allocatable, removedResources = testManager.GetCapacity(logger)
	as.Len(capacity, 2)
	resource2Capacity, ok := capacity[v1.ResourceName(resourceName2)]
	as.True(ok)
	resource2Allocatable, ok := allocatable[v1.ResourceName(resourceName2)]
	as.True(ok)
	as.Equal(int64(3), resource2Capacity.Value())
	as.Equal(int64(1), resource2Allocatable.Value())
	as.Empty(removedResources)

	// 让 resourceName1 的端点超过宽限期：GetCapacity 应报告该资源已被移除，
	// 并从 healthyDevices/unhealthyDevices/endpoints 中清理。
	e1.setStopTime(time.Now().Add(-1*endpointStopGracePeriod - time.Duration(10)*time.Second))
	capacity, allocatable, removed := testManager.GetCapacity(logger)
	as.Equal([]string{resourceName1}, removed)
	as.NotContains(capacity, v1.ResourceName(resourceName1))
	as.NotContains(allocatable, v1.ResourceName(resourceName1))
	val, ok := capacity[v1.ResourceName(resourceName2)]
	as.True(ok)
	as.Equal(int64(3), val.Value())
	as.NotContains(testManager.healthyDevices, resourceName1)
	as.NotContains(testManager.unhealthyDevices, resourceName1)
	as.NotContains(testManager.endpoints, resourceName1)
	as.Len(testManager.endpoints, 1)

	// 断开 resourceName2 端点：stopTime 应被设置，allocate/preStartContainer 返回端点已停止错误。
	err = e2.client.Disconnect(logger)
	require.NoError(t, err)
	as.False(e2.stopTime.IsZero())
	_, err = e2.allocate(tCtx, []string{"Device1"})
	reflect.DeepEqual(err, fmt.Errorf(errEndpointStopped, e2))
	_, err = e2.preStartContainer(tCtx, []string{"Device1"})
	reflect.DeepEqual(err, fmt.Errorf(errEndpointStopped, e2))
	// 把 resourceName2 标记为不健康，capacity 保持 3 而 allocatable 归零。
	testManager.markResourceUnhealthy(logger, resourceName2)
	capacity, allocatable, removed = testManager.GetCapacity(logger)
	val, ok = capacity[v1.ResourceName(resourceName2)]
	as.True(ok)
	as.Equal(int64(3), val.Value())
	val, ok = allocatable[v1.ResourceName(resourceName2)]
	as.True(ok)
	as.Equal(int64(0), val.Value())
	as.Empty(removed)
	// 写入并重新读取 checkpoint：已停止的端点会被重建，capacity 置 0，
	// 但仍被视为设备插件资源，从而拒绝调度期间按旧容量准入的 Pod。
	err = testManager.writeCheckpoint(logger)
	as.NoError(err)
	testManager.healthyDevices = make(map[string]sets.Set[string])
	testManager.unhealthyDevices = make(map[string]sets.Set[string])
	err = testManager.readCheckpoint(logger)
	as.NoError(err)
	as.Len(testManager.endpoints, 1)
	as.Contains(testManager.endpoints, resourceName2)
	capacity, allocatable, removed = testManager.GetCapacity(logger)
	val, ok = capacity[v1.ResourceName(resourceName2)]
	as.True(ok)
	as.Equal(int64(0), val.Value())
	val, ok = allocatable[v1.ResourceName(resourceName2)]
	as.True(ok)
	as.Equal(int64(0), val.Value())
	as.Empty(removed)
	as.True(testManager.isDevicePluginResource(resourceName2))
}

// 验证多个资源各自的健康设备会被正确归类，且不健康设备被排除。
func TestGetAllocatableDevicesMultipleResources(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	socketDir, socketName, _, err := tmpSocketDir()
	topologyStore := topologymanager.NewFakeManager(logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(socketDir), "unable to remove dir %s", socketDir)
	})
	testManager, err := newManagerImpl(logger, socketName, nil, topologyStore)
	as := assert.New(t)
	as.NotNil(testManager)
	as.NoError(err)

	resource1Devs := []*pluginapi.Device{
		{ID: "R1Device1", Health: pluginapi.Healthy},
		{ID: "R1Device2", Health: pluginapi.Healthy},
		{ID: "R1Device3", Health: pluginapi.Unhealthy},
	}
	resourceName1 := "domain1.com/resource1"
	e1 := &endpointImpl{}
	testManager.endpoints[resourceName1] = endpointInfo{e: e1, opts: nil}
	testManager.genericDeviceUpdateCallback(logger, resourceName1, resource1Devs)

	resource2Devs := []*pluginapi.Device{
		{ID: "R2Device1", Health: pluginapi.Healthy},
	}
	resourceName2 := "other.domain2.org/resource2"
	e2 := &endpointImpl{}
	testManager.endpoints[resourceName2] = endpointInfo{e: e2, opts: nil}
	testManager.genericDeviceUpdateCallback(logger, resourceName2, resource2Devs)

	allocatableDevs := testManager.GetAllocatableDevices(logger)
	as.Len(allocatableDevs, 2)

	devInstances1, ok := allocatableDevs[resourceName1]
	as.True(ok)
	checkAllocatableDevicesConsistsOf(as, devInstances1, []string{"R1Device1", "R1Device2"})

	devInstances2, ok := allocatableDevs[resourceName2]
	as.True(ok)
	checkAllocatableDevicesConsistsOf(as, devInstances2, []string{"R2Device1"})

}

// 验证设备由不健康转为健康后，会出现在 allocatable 设备列表中。
func TestGetAllocatableDevicesHealthTransition(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	socketDir, socketName, _, err := tmpSocketDir()
	topologyStore := topologymanager.NewFakeManager(logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(socketDir), "unable to remove dir %s", socketDir)
	})
	testManager, err := newManagerImpl(logger, socketName, nil, topologyStore)
	as := assert.New(t)
	as.NotNil(testManager)
	as.NoError(err)

	resource1Devs := []*pluginapi.Device{
		{ID: "R1Device1", Health: pluginapi.Healthy},
		{ID: "R1Device2", Health: pluginapi.Healthy},
		{ID: "R1Device3", Health: pluginapi.Unhealthy},
	}

	// 为 resource1 添加三个设备，其中两个健康、一个不健康。
	// 预期 resource1 的可分配设备数为 2。
	resourceName1 := "domain1.com/resource1"
	e1 := &endpointImpl{}
	testManager.endpoints[resourceName1] = endpointInfo{e: e1, opts: nil}

	testManager.genericDeviceUpdateCallback(logger, resourceName1, resource1Devs)

	allocatableDevs := testManager.GetAllocatableDevices(logger)
	as.Len(allocatableDevs, 1)
	devInstances, ok := allocatableDevs[resourceName1]
	as.True(ok)
	checkAllocatableDevicesConsistsOf(as, devInstances, []string{"R1Device1", "R1Device2"})

	// 不健康设备恢复健康。
	resource1Devs = []*pluginapi.Device{
		{ID: "R1Device1", Health: pluginapi.Healthy},
		{ID: "R1Device2", Health: pluginapi.Healthy},
		{ID: "R1Device3", Health: pluginapi.Healthy},
	}
	testManager.genericDeviceUpdateCallback(logger, resourceName1, resource1Devs)

	allocatableDevs = testManager.GetAllocatableDevices(logger)
	as.Len(allocatableDevs, 1)
	devInstances, ok = allocatableDevs[resourceName1]
	as.True(ok)
	checkAllocatableDevicesConsistsOf(as, devInstances, []string{"R1Device1", "R1Device2", "R1Device3"})
}

// 断言 allocatable 设备集合与期望的设备 ID 列表完全一致。
func checkAllocatableDevicesConsistsOf(as *assert.Assertions, devInstances DeviceInstances, expectedDevs []string) {
	as.Equal(len(expectedDevs), len(devInstances))
	for _, deviceID := range expectedDevs {
		_, ok := devInstances[deviceID]
		as.True(ok)
	}
}

// 把设备 ID 列表包装成"无 NUMA 拓扑"的 checkpoint 结构。
func constructDevices(devices []string) checkpoint.DevicesPerNUMA {
	ret := checkpoint.DevicesPerNUMA{}
	for _, dev := range devices {
		ret[0] = append(ret[0], dev)
	}
	return ret
}

// containerAllocateResponseBuilder 用于以链式选项构造 ContainerAllocateResponse，
// 便于测试按需组合设备、挂载、环境变量与 CDI 设备。
type containerAllocateResponseBuilder struct {
	devices    map[string]string
	mounts     map[string]string
	envs       map[string]string
	cdiDevices []string
}

// containerAllocateResponseBuilderOption 是 containerAllocateResponseBuilder 的函数式选项。
type containerAllocateResponseBuilderOption func(*containerAllocateResponseBuilder)

// withDevices 设置 builder 要生成的设备映射（宿主机路径 -> 容器路径）。
func withDevices(devices map[string]string) containerAllocateResponseBuilderOption {
	return func(b *containerAllocateResponseBuilder) {
		b.devices = devices
	}
}

// withMounts 设置 builder 要生成的挂载映射（容器路径 -> 宿主机路径）。
func withMounts(mounts map[string]string) containerAllocateResponseBuilderOption {
	return func(b *containerAllocateResponseBuilder) {
		b.mounts = mounts
	}
}

// withEnvs 设置 builder 要注入的环境变量。
func withEnvs(envs map[string]string) containerAllocateResponseBuilderOption {
	return func(b *containerAllocateResponseBuilder) {
		b.envs = envs
	}
}

// withCDIDevices 设置 builder 要生成的 CDI 设备名称列表。
func withCDIDevices(cdiDevices ...string) containerAllocateResponseBuilderOption {
	return func(b *containerAllocateResponseBuilder) {
		b.cdiDevices = cdiDevices
	}
}

// newContainerAllocateResponse 按给定选项构造一个 ContainerAllocateResponse。
func newContainerAllocateResponse(opts ...containerAllocateResponseBuilderOption) *pluginapi.ContainerAllocateResponse {
	b := &containerAllocateResponseBuilder{}
	for _, opt := range opts {
		opt(b)
	}

	return b.Build()
}

// Build 根据当前配置生成 ContainerAllocateResponse：设备权限固定为 mrw，挂载只读，CDI 设备逐个包装。
func (b *containerAllocateResponseBuilder) Build() *pluginapi.ContainerAllocateResponse {
	resp := &pluginapi.ContainerAllocateResponse{}
	for k, v := range b.devices {
		resp.Devices = append(resp.Devices, &pluginapi.DeviceSpec{
			HostPath:      k,
			ContainerPath: v,
			Permissions:   "mrw",
		})
	}
	for k, v := range b.mounts {
		resp.Mounts = append(resp.Mounts, &pluginapi.Mount{
			ContainerPath: k,
			HostPath:      v,
			ReadOnly:      true,
		})
	}
	resp.Envs = make(map[string]string)
	for k, v := range b.envs {
		resp.Envs[k] = v
	}

	var cdiDevices []*pluginapi.CDIDevice
	for _, dev := range b.cdiDevices {
		cdiDevice := pluginapi.CDIDevice{
			Name: dev,
		}
		cdiDevices = append(cdiDevices, &cdiDevice)
	}
	resp.CdiDevices = cdiDevices

	return resp
}

// 验证写 checkpoint 后清空内存状态再读取，能完整恢复 pod/container 的设备分配、
// 环境变量与挂载信息，以及 allocated/healthy 设备集合。
func TestCheckpoint(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	resourceName1 := "domain1.com/resource1"
	resourceName2 := "domain2.com/resource2"
	resourceName3 := "domain2.com/resource3"
	as := assert.New(t)
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	as.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
	})
	ckm, err := checkpointmanager.NewCheckpointManager(tmpDir)
	as.NoError(err)
	testManager := &ManagerImpl{
		endpoints:         make(map[string]endpointInfo),
		healthyDevices:    make(map[string]sets.Set[string]),
		unhealthyDevices:  make(map[string]sets.Set[string]),
		allocatedDevices:  make(map[string]sets.Set[string]),
		podDevices:        newPodDevices(),
		checkpointManager: ckm,
	}

	testManager.podDevices.insert("pod1", "con1", resourceName1,
		constructDevices([]string{"dev1", "dev2"}),
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r1dev1": "/dev/r1dev1", "/dev/r1dev2": "/dev/r1dev2"}),
			withMounts(map[string]string{"/home/r1lib1": "/usr/r1lib1"}),
			withCDIDevices("domain1.com/resource1=dev1", "domain1.com/resource1=dev2"),
		),
	)
	testManager.podDevices.insert("pod1", "con1", resourceName2,
		constructDevices([]string{"dev1", "dev2"}),
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r2dev1": "/dev/r2dev1", "/dev/r2dev2": "/dev/r2dev2"}),
			withMounts(map[string]string{"/home/r2lib1": "/usr/r2lib1"}),
			withEnvs(map[string]string{"r2devices": "dev1 dev2"}),
		),
	)
	testManager.podDevices.insert("pod1", "con2", resourceName1,
		constructDevices([]string{"dev3"}),
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r1dev3": "/dev/r1dev3"}),
			withMounts(map[string]string{"/home/r1lib1": "/usr/r1lib1"}),
		),
	)
	testManager.podDevices.insert("pod2", "con1", resourceName1,
		constructDevices([]string{"dev4"}),
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r1dev4": "/dev/r1dev4"}),
			withMounts(map[string]string{"/home/r1lib1": "/usr/r1lib1"}),
		),
	)
	testManager.podDevices.insert("pod3", "con3", resourceName3,
		checkpoint.DevicesPerNUMA{nodeWithoutTopology: []string{"dev5"}},
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r3dev5": "/dev/r3dev5"}),
			withMounts(map[string]string{"/home/r3lib1": "/usr/r3lib1"}),
		),
	)

	testManager.healthyDevices[resourceName1] = sets.New[string]()
	testManager.healthyDevices[resourceName1].Insert("dev1")
	testManager.healthyDevices[resourceName1].Insert("dev2")
	testManager.healthyDevices[resourceName1].Insert("dev3")
	testManager.healthyDevices[resourceName1].Insert("dev4")
	testManager.healthyDevices[resourceName1].Insert("dev5")
	testManager.healthyDevices[resourceName2] = sets.New[string]()
	testManager.healthyDevices[resourceName2].Insert("dev1")
	testManager.healthyDevices[resourceName2].Insert("dev2")
	testManager.healthyDevices[resourceName3] = sets.New[string]()
	testManager.healthyDevices[resourceName3].Insert("dev5")

	expectedPodDevices := testManager.podDevices
	expectedAllocatedDevices := testManager.podDevices.devices()
	expectedAllDevices := testManager.healthyDevices

	err = testManager.writeCheckpoint(logger)

	as.NoError(err)
	testManager.podDevices = newPodDevices()
	err = testManager.readCheckpoint(logger)
	as.NoError(err)

	as.Equal(expectedPodDevices.size(), testManager.podDevices.size())
	for podUID, containerDevices := range expectedPodDevices.devs {
		for conName, resources := range containerDevices {
			for resource := range resources {
				expDevices := expectedPodDevices.containerDevices(podUID, conName, resource)
				testDevices := testManager.podDevices.containerDevices(podUID, conName, resource)
				as.True(reflect.DeepEqual(expDevices, testDevices))
				opts1 := expectedPodDevices.deviceRunContainerOptions(logger, podUID, conName)
				opts2 := testManager.podDevices.deviceRunContainerOptions(logger, podUID, conName)
				as.Equal(len(opts1.Envs), len(opts2.Envs))
				as.Equal(len(opts1.Mounts), len(opts2.Mounts))
				as.Equal(len(opts1.Devices), len(opts2.Devices))
			}
		}
	}
	as.True(reflect.DeepEqual(expectedAllocatedDevices, testManager.allocatedDevices))
	as.True(reflect.DeepEqual(expectedAllDevices, testManager.healthyDevices))
}

type activePodsStub struct {
	activePods []*v1.Pod
}

// 返回 stub 中记录的活跃 Pod 列表。
func (a *activePodsStub) getActivePods() []*v1.Pod {
	return a.activePods
}

// 替换 stub 中的活跃 Pod 列表。
func (a *activePodsStub) updateActivePods(newPods []*v1.Pod) {
	a.activePods = newPods
}

// MockEndpoint 通过可注入函数模拟设备插件的 endpoint。
type MockEndpoint struct {
	getPreferredAllocationFunc func(available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error)
	allocateFunc               func(devs []string) (*pluginapi.AllocateResponse, error)
	initChan                   chan []string
	socket                     string
}

// 记录本次 PreStartContainer 收到的设备列表，用于测试断言。
func (m *MockEndpoint) preStartContainer(_ context.Context, devs []string) (*pluginapi.PreStartContainerResponse, error) {
	m.initChan <- devs
	return &pluginapi.PreStartContainerResponse{}, nil
}

// 调用注入的 preferred allocation 函数；未注入时返回空结果。
func (m *MockEndpoint) getPreferredAllocation(_ context.Context, available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error) {
	if m.getPreferredAllocationFunc != nil {
		return m.getPreferredAllocationFunc(available, mustInclude, size)
	}
	return nil, nil
}

// 调用注入的 allocate 函数；未注入时返回空结果。
func (m *MockEndpoint) allocate(ctx context.Context, devs []string) (*pluginapi.AllocateResponse, error) {
	if m.allocateFunc != nil {
		return m.allocateFunc(devs)
	}
	return nil, nil
}

// MockEndpoint 忽略停止时间设置，测试中不需要模拟端点停止。
func (m *MockEndpoint) setStopTime(t time.Time) {}

// MockEndpoint 始终返回未停止。
func (m *MockEndpoint) isStopped() bool { return false }

// MockEndpoint 始终认为宽限期未过期。
func (m *MockEndpoint) stopGracePeriodExpired() bool { return false }

// 返回 MockEndpoint 的 socket 路径。
func (m *MockEndpoint) socketPath() string {
	return m.socket
}

// 构造只包含一个容器、并给定资源 Limits 的测试 Pod。
func makePod(limits v1.ResourceList) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: uuid.NewUUID(),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: limits,
					},
				},
			},
		},
	}
}

// 按测试资源描述构造一个带 checkpoint 管理器的 wrappedManagerImpl，
// 为各资源挂上行为可控的 MockEndpoint，并生成对应的设备实例。
func getTestManager(logger klog.Logger, tmpDir string, activePods ActivePodsFunc, testRes []TestResource) (*wrappedManagerImpl, error) {
	monitorCallback := func(logger klog.Logger, resourceName string, devices []*pluginapi.Device) {}
	ckm, err := checkpointmanager.NewCheckpointManager(tmpDir)
	if err != nil {
		return nil, err
	}
	m := &ManagerImpl{
		healthyDevices:        make(map[string]sets.Set[string]),
		unhealthyDevices:      make(map[string]sets.Set[string]),
		allocatedDevices:      make(map[string]sets.Set[string]),
		endpoints:             make(map[string]endpointInfo),
		podDevices:            newPodDevices(),
		devicesToReuse:        make(PodReusableDevices),
		topologyAffinityStore: topologymanager.NewFakeManager(logger),
		activePods:            activePods,
		sourcesReady:          &sourcesReadyStub{},
		checkpointManager:     ckm,
		allDevices:            NewResourceDeviceInstances(),
	}
	testManager := &wrappedManagerImpl{
		ManagerImpl: m,
		socketdir:   tmpDir,
		callback:    monitorCallback,
	}

	for _, res := range testRes {
		testManager.healthyDevices[res.resourceName] = sets.New[string](res.devs.Devices().UnsortedList()...)
		if res.resourceName == "domain1.com/resource1" {
			testManager.endpoints[res.resourceName] = endpointInfo{
				e:    &MockEndpoint{allocateFunc: allocateStubFunc()},
				opts: nil,
			}
		}
		if res.resourceName == "domain2.com/resource2" {
			testManager.endpoints[res.resourceName] = endpointInfo{
				e: &MockEndpoint{
					allocateFunc: func(devs []string) (*pluginapi.AllocateResponse, error) {
						resp := new(pluginapi.ContainerAllocateResponse)
						resp.Envs = make(map[string]string)
						for _, dev := range devs {
							switch dev {
							case "dev3":
								resp.Envs["key2"] = "val2"

							case "dev4":
								resp.Envs["key2"] = "val3"
							}
						}
						resps := new(pluginapi.AllocateResponse)
						resps.ContainerResponses = append(resps.ContainerResponses, resp)
						return resps, nil
					},
				},
				opts: nil,
			}
		}
		testManager.allDevices[res.resourceName] = makeDevice(res.devs, res.topology)

	}
	return testManager, nil
}

// TestResource 描述测试设备资源的数量、拓扑与分配结果。
type TestResource struct {
	resourceName     string
	resourceQuantity resource.Quantity
	devs             checkpoint.DevicesPerNUMA
	topology         bool
}

// 验证 filterByAffinity 会把可用设备按 NUMA 亲和性分成三组：
// 满足亲和性、不满足亲和性、无拓扑信息。
func TestFilterByAffinity(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	as := require.New(t)
	allDevices := ResourceDeviceInstances{
		"res1": map[string]*pluginapi.Device{
			"dev1": {
				ID: "dev1",
				Topology: &pluginapi.TopologyInfo{
					Nodes: []*pluginapi.NUMANode{
						{
							ID: 1,
						},
					},
				},
			},
			"dev2": {
				ID: "dev2",
				Topology: &pluginapi.TopologyInfo{
					Nodes: []*pluginapi.NUMANode{
						{
							ID: 1,
						},
						{
							ID: 2,
						},
					},
				},
			},
			"dev3": {
				ID: "dev3",
				Topology: &pluginapi.TopologyInfo{
					Nodes: []*pluginapi.NUMANode{
						{
							ID: 2,
						},
					},
				},
			},
			"dev4": {
				ID: "dev4",
				Topology: &pluginapi.TopologyInfo{
					Nodes: []*pluginapi.NUMANode{
						{
							ID: 2,
						},
					},
				},
			},
			"devwithouttopology": {
				ID: "dev5",
			},
		},
	}

	fakeAffinity, _ := bitmask.NewBitMask(2)
	fakeHint := topologymanager.TopologyHint{
		NUMANodeAffinity: fakeAffinity,
		Preferred:        true,
	}
	testManager := ManagerImpl{
		topologyAffinityStore: topologymanager.NewFakeManagerWithHint(logger, &fakeHint),
		allDevices:            allDevices,
	}

	testCases := []struct {
		available               sets.Set[string]
		fromAffinityExpected    sets.Set[string]
		notFromAffinityExpected sets.Set[string]
		withoutTopologyExpected sets.Set[string]
	}{
		{
			available:               sets.New[string]("dev1", "dev2"),
			fromAffinityExpected:    sets.New[string]("dev2"),
			notFromAffinityExpected: sets.New[string]("dev1"),
			withoutTopologyExpected: sets.New[string](),
		},
		{
			available:               sets.New[string]("dev1", "dev2", "dev3", "dev4"),
			fromAffinityExpected:    sets.New[string]("dev2", "dev3", "dev4"),
			notFromAffinityExpected: sets.New[string]("dev1"),
			withoutTopologyExpected: sets.New[string](),
		},
	}

	for _, testCase := range testCases {
		fromAffinity, notFromAffinity, withoutTopology := testManager.filterByAffinity(logger, "", "", "res1", testCase.available)
		as.Truef(fromAffinity.Equal(testCase.fromAffinityExpected), "expect devices from affinity to be %v but got %v", testCase.fromAffinityExpected, fromAffinity)
		as.Truef(notFromAffinity.Equal(testCase.notFromAffinityExpected), "expect devices not from affinity to be %v but got %v", testCase.notFromAffinityExpected, notFromAffinity)
		as.Truef(withoutTopology.Equal(testCase.withoutTopologyExpected), "expect devices without topology to be %v but got %v", testCase.notFromAffinityExpected, notFromAffinity)
	}
}

// 验证连续为多个 Pod 分配设备时的成功与失败路径，
// 并检查生成的 devices/mounts/envs 数量以及已分配设备计数。
func TestPodContainerDeviceAllocation(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	res1 := TestResource{
		resourceName:     "domain1.com/resource1",
		resourceQuantity: *resource.NewQuantity(int64(2), resource.DecimalSI),
		devs:             checkpoint.DevicesPerNUMA{0: []string{"dev1", "dev2"}},
		topology:         true,
	}
	res2 := TestResource{
		resourceName:     "domain2.com/resource2",
		resourceQuantity: *resource.NewQuantity(int64(1), resource.DecimalSI),
		devs:             checkpoint.DevicesPerNUMA{0: []string{"dev3", "dev4"}},
		topology:         false,
	}
	testResources := make([]TestResource, 2)
	testResources = append(testResources, res1)
	testResources = append(testResources, res2)
	as := require.New(t)
	podsStub := activePodsStub{
		activePods: []*v1.Pod{},
	}
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	as.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
	})
	testManager, err := getTestManager(logger, tmpDir, podsStub.getActivePods, testResources)
	as.NoError(err)

	testPods := []*v1.Pod{
		makePod(v1.ResourceList{
			v1.ResourceName(res1.resourceName): res1.resourceQuantity,
			v1.ResourceName("cpu"):             res1.resourceQuantity,
			v1.ResourceName(res2.resourceName): res2.resourceQuantity}),
		makePod(v1.ResourceList{
			v1.ResourceName(res1.resourceName): res2.resourceQuantity}),
		makePod(v1.ResourceList{
			v1.ResourceName(res2.resourceName): res2.resourceQuantity}),
	}
	testCases := []struct {
		description               string
		testPod                   *v1.Pod
		expectedContainerOptsLen  []int
		expectedAllocatedResName1 int
		expectedAllocatedResName2 int
		expErr                    error
	}{
		{
			description:               "Successful allocation of two Res1 resources and one Res2 resource",
			testPod:                   testPods[0],
			expectedContainerOptsLen:  []int{3, 2, 2},
			expectedAllocatedResName1: 2,
			expectedAllocatedResName2: 1,
			expErr:                    nil,
		},
		{
			description:               "Requesting to create a pod without enough resources should fail",
			testPod:                   testPods[1],
			expectedContainerOptsLen:  nil,
			expectedAllocatedResName1: 2,
			expectedAllocatedResName2: 1,
			expErr:                    fmt.Errorf("requested number of devices unavailable for domain1.com/resource1. Requested: 1, Available: 0"),
		},
		{
			description:               "Successful allocation of all available Res1 resources and Res2 resources",
			testPod:                   testPods[2],
			expectedContainerOptsLen:  []int{0, 0, 1},
			expectedAllocatedResName1: 2,
			expectedAllocatedResName2: 2,
			expErr:                    nil,
		},
	}
	activePods := []*v1.Pod{}
	for _, testCase := range testCases {
		pod := testCase.testPod
		activePods = append(activePods, pod)
		podsStub.updateActivePods(activePods)
		err := testManager.Allocate(tCtx, pod, &pod.Spec.Containers[0], lifecycle.AddOperation)
		if !reflect.DeepEqual(err, testCase.expErr) {
			t.Errorf("DevicePluginManager error (%v). expected error: %v but got: %v",
				testCase.description, testCase.expErr, err)
		}
		runContainerOpts, err := testManager.GetDeviceRunContainerOptions(tCtx, pod, &pod.Spec.Containers[0])
		if testCase.expErr == nil {
			as.NoError(err)
		}
		if testCase.expectedContainerOptsLen == nil {
			as.Nil(runContainerOpts)
		} else {
			as.Len(runContainerOpts.Devices, testCase.expectedContainerOptsLen[0])
			as.Len(runContainerOpts.Mounts, testCase.expectedContainerOptsLen[1])
			as.Len(runContainerOpts.Envs, testCase.expectedContainerOptsLen[2])
		}
		as.Equal(testCase.expectedAllocatedResName1, testManager.allocatedDevices[res1.resourceName].Len())
		as.Equal(testCase.expectedAllocatedResName2, testManager.allocatedDevices[res2.resourceName].Len())
	}

}

// 验证 devicesToAllocate 的三种准入失败场景：
// 无健康设备、资源未注册、此前分配的设备已不健康。
func TestPodContainerDeviceToAllocate(t *testing.T) {
	tCtx := ktesting.Init(t)
	resourceName1 := "domain1.com/resource1"
	resourceName2 := "domain2.com/resource2"
	resourceName3 := "domain2.com/resource3"
	as := require.New(t)
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	as.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
	})

	testManager := &ManagerImpl{
		endpoints:        make(map[string]endpointInfo),
		healthyDevices:   make(map[string]sets.Set[string]),
		unhealthyDevices: make(map[string]sets.Set[string]),
		allocatedDevices: make(map[string]sets.Set[string]),
		podDevices:       newPodDevices(),
		activePods:       func() []*v1.Pod { return []*v1.Pod{} },
		sourcesReady:     &sourcesReadyStub{},
	}

	testManager.podDevices.insert("pod1", "con1", resourceName1,
		constructDevices([]string{"dev1", "dev2"}),
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r2dev1": "/dev/r2dev1", "/dev/r2dev2": "/dev/r2dev2"}),
			withMounts(map[string]string{"/home/r2lib1": "/usr/r2lib1"}),
			withEnvs(map[string]string{"r2devices": "dev1 dev2"}),
		),
	)
	testManager.podDevices.insert("pod2", "con2", resourceName2,
		checkpoint.DevicesPerNUMA{nodeWithoutTopology: []string{"dev5"}},
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r1dev5": "/dev/r1dev5"}),
			withMounts(map[string]string{"/home/r1lib1": "/usr/r1lib1"}),
		),
	)
	testManager.podDevices.insert("pod3", "con3", resourceName3,
		checkpoint.DevicesPerNUMA{nodeWithoutTopology: []string{"dev5"}},
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r1dev5": "/dev/r1dev5"}),
			withMounts(map[string]string{"/home/r1lib1": "/usr/r1lib1"}),
		),
	)

	// resourceName1 没有健康设备；resourceName2 完全不出现在 healthyDevices 中，
	// 用以模拟该资源从未注册。
	testManager.healthyDevices[resourceName1] = sets.New[string]()
	testManager.healthyDevices[resourceName3] = sets.New[string]()
	// resourceName3 的 dev5 已不在健康列表中，模拟设备失效。
	// dev5 已不在健康设备列表中。
	testManager.healthyDevices[resourceName3].Insert("dev7")
	testManager.healthyDevices[resourceName3].Insert("dev8")

	testCases := []struct {
		description              string
		podUID                   string
		contName                 string
		resource                 string
		required                 int
		reusableDevices          sets.Set[string]
		expectedAllocatedDevices sets.Set[string]
		expErr                   error
	}{
		{
			description:              "Admission error in case no healthy devices to allocate present",
			podUID:                   "pod1",
			contName:                 "con1",
			resource:                 resourceName1,
			required:                 2,
			reusableDevices:          sets.New[string](),
			expectedAllocatedDevices: nil,
			expErr:                   fmt.Errorf("no healthy devices present; cannot allocate unhealthy devices %s", resourceName1),
		},
		{
			description:              "Admission error in case resource is not registered",
			podUID:                   "pod2",
			contName:                 "con2",
			resource:                 resourceName2,
			required:                 1,
			reusableDevices:          sets.New[string](),
			expectedAllocatedDevices: nil,
			expErr:                   fmt.Errorf("cannot allocate unregistered device %s", resourceName2),
		},
		{
			description:              "Admission error in case resource not devices previously allocated no longer healthy",
			podUID:                   "pod3",
			contName:                 "con3",
			resource:                 resourceName3,
			required:                 1,
			reusableDevices:          sets.New[string](),
			expectedAllocatedDevices: nil,
			expErr:                   fmt.Errorf("previously allocated devices are no longer healthy; cannot allocate unhealthy devices %s", resourceName3),
		},
	}

	for _, testCase := range testCases {
		allocDevices, err := testManager.devicesToAllocate(tCtx, testCase.podUID, testCase.contName, testCase.resource, testCase.required, testCase.reusableDevices)
		if !reflect.DeepEqual(err, testCase.expErr) {
			t.Errorf("devicePluginManager error (%v). expected error: %v but got: %v",
				testCase.description, testCase.expErr, err)
		}
		if !reflect.DeepEqual(allocDevices, testCase.expectedAllocatedDevices) {
			t.Errorf("devicePluginManager error (%v). expected error: %v but got: %v",
				testCase.description, testCase.expectedAllocatedDevices, allocDevices)
		}
	}

}

// 验证 devicesToAllocate 与 UpdateAllocatedDevices 并发执行时不会出现数据竞争，
// 且仍能为新容器选出一台设备。
func TestDevicesToAllocateConflictWithUpdateAllocatedDevices(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podToAllocate := "podToAllocate"
	containerToAllocate := "containerToAllocate"
	podToRemove := "podToRemove"
	containerToRemove := "containerToRemove"
	deviceID := "deviceID"
	resourceName := "domain1.com/resource"

	socket := filepath.Join(os.TempDir(), esocketName())
	devs := []*pluginapi.Device{
		{ID: deviceID, Health: pluginapi.Healthy},
	}
	p, e := esetup(tCtx, t, devs, socket, resourceName, func(logger klog.Logger, n string, d []*pluginapi.Device) {})

	waitUpdateAllocatedDevicesChan := make(chan struct{})
	waitSetGetPreferredAllocChan := make(chan struct{})

	p.SetGetPreferredAllocFunc(func(r *pluginapi.PreferredAllocationRequest, devs map[string]*pluginapi.Device) (*pluginapi.PreferredAllocationResponse, error) {
		waitSetGetPreferredAllocChan <- struct{}{}
		<-waitUpdateAllocatedDevicesChan
		return &pluginapi.PreferredAllocationResponse{
			ContainerResponses: []*pluginapi.ContainerPreferredAllocationResponse{
				{
					DeviceIDs: []string{deviceID},
				},
			},
		}, nil
	})

	testManager := &ManagerImpl{
		endpoints:             make(map[string]endpointInfo),
		healthyDevices:        make(map[string]sets.Set[string]),
		unhealthyDevices:      make(map[string]sets.Set[string]),
		allocatedDevices:      make(map[string]sets.Set[string]),
		podDevices:            newPodDevices(),
		activePods:            func() []*v1.Pod { return []*v1.Pod{} },
		sourcesReady:          &sourcesReadyStub{},
		topologyAffinityStore: topologymanager.NewFakeManager(logger),
	}

	testManager.endpoints[resourceName] = endpointInfo{
		e: e,
		opts: &pluginapi.DevicePluginOptions{
			GetPreferredAllocationAvailable: true,
		},
	}
	testManager.healthyDevices[resourceName] = sets.New[string](deviceID)
	testManager.podDevices.insert(podToRemove, containerToRemove, resourceName, nil, nil)

	go func() {
		<-waitSetGetPreferredAllocChan
		testManager.UpdateAllocatedDevices(logger)
		waitUpdateAllocatedDevicesChan <- struct{}{}
	}()

	set, err := testManager.devicesToAllocate(tCtx, podToAllocate, containerToAllocate, resourceName, 1, sets.New[string]())
	assert.NoError(t, err)
	assert.Equal(t, sets.New[string](deviceID), set)
}

// 验证 Pod 仍在活跃列表中时能返回容器运行选项，
// 而 Pod 被移除后再次查询应返回空结果。
func TestGetDeviceRunContainerOptions(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	res1 := TestResource{
		resourceName:     "domain1.com/resource1",
		resourceQuantity: *resource.NewQuantity(int64(2), resource.DecimalSI),
		devs:             checkpoint.DevicesPerNUMA{0: []string{"dev1", "dev2"}},
		topology:         true,
	}
	res2 := TestResource{
		resourceName:     "domain2.com/resource2",
		resourceQuantity: *resource.NewQuantity(int64(1), resource.DecimalSI),
		devs:             checkpoint.DevicesPerNUMA{0: []string{"dev3", "dev4"}},
		topology:         false,
	}

	testResources := make([]TestResource, 2)
	testResources = append(testResources, res1)
	testResources = append(testResources, res2)

	podsStub := activePodsStub{
		activePods: []*v1.Pod{},
	}
	as := require.New(t)

	tmpDir, err := os.MkdirTemp("", "checkpoint")
	as.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
	})

	testManager, err := getTestManager(logger, tmpDir, podsStub.getActivePods, testResources)
	as.NoError(err)

	pod1 := makePod(v1.ResourceList{
		v1.ResourceName(res1.resourceName): res1.resourceQuantity,
		v1.ResourceName(res2.resourceName): res2.resourceQuantity,
	})
	pod2 := makePod(v1.ResourceList{
		v1.ResourceName(res2.resourceName): res2.resourceQuantity,
	})

	activePods := []*v1.Pod{pod1, pod2}
	podsStub.updateActivePods(activePods)

	err = testManager.Allocate(tCtx, pod1, &pod1.Spec.Containers[0], lifecycle.AddOperation)
	as.NoError(err)
	err = testManager.Allocate(tCtx, pod2, &pod2.Spec.Containers[0], lifecycle.AddOperation)
	as.NoError(err)

	// Pod 在 activePods 中时，GetDeviceRunContainerOptions 应返回已分配设备的选项。
	runContainerOpts, err := testManager.GetDeviceRunContainerOptions(tCtx, pod1, &pod1.Spec.Containers[0])
	as.NoError(err)
	as.Len(runContainerOpts.Devices, 3)
	as.Len(runContainerOpts.Mounts, 2)
	as.Len(runContainerOpts.Envs, 2)

	activePods = []*v1.Pod{pod2}
	podsStub.updateActivePods(activePods)
	testManager.UpdateAllocatedDevices(logger)

	// Pod 从 activePods 移除后，UpdateAllocatedDevices 会释放其设备，
	// 此时再查询运行选项应返回 nil。
	runContainerOpts, err = testManager.GetDeviceRunContainerOptions(tCtx, pod1, &pod1.Spec.Containers[0])
	as.NoError(err)
	as.Nil(runContainerOpts)
}

// 验证普通 init 容器的设备会复用给后续容器，
// 且各普通容器之间的设备互不重叠。
func TestInitContainerDeviceAllocation(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	// 请求创建在 init 容器和普通容器中都申请 resourceName1 的 Pod，
	// 应成功，并将在 init 容器上分配的设备重新分配给普通容器。
	res1 := TestResource{
		resourceName:     "domain1.com/resource1",
		resourceQuantity: *resource.NewQuantity(int64(2), resource.DecimalSI),
		devs:             checkpoint.DevicesPerNUMA{0: []string{"dev1", "dev2"}},
		topology:         false,
	}
	res2 := TestResource{
		resourceName:     "domain2.com/resource2",
		resourceQuantity: *resource.NewQuantity(int64(1), resource.DecimalSI),
		devs:             checkpoint.DevicesPerNUMA{0: []string{"dev3", "dev4"}},
		topology:         true,
	}
	testResources := make([]TestResource, 2)
	testResources = append(testResources, res1)
	testResources = append(testResources, res2)
	as := require.New(t)
	podsStub := activePodsStub{
		activePods: []*v1.Pod{},
	}
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	as.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
	})

	testManager, err := getTestManager(logger, tmpDir, podsStub.getActivePods, testResources)
	as.NoError(err)

	podWithPluginResourcesInInitContainers := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: uuid.NewUUID(),
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name: string(uuid.NewUUID()),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(res1.resourceName): res2.resourceQuantity,
						},
					},
				},
				{
					Name: string(uuid.NewUUID()),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(res1.resourceName): res1.resourceQuantity,
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name: string(uuid.NewUUID()),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(res1.resourceName): res2.resourceQuantity,
							v1.ResourceName(res2.resourceName): res2.resourceQuantity,
						},
					},
				},
				{
					Name: string(uuid.NewUUID()),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(res1.resourceName): res2.resourceQuantity,
							v1.ResourceName(res2.resourceName): res2.resourceQuantity,
						},
					},
				},
			},
		},
	}
	podsStub.updateActivePods([]*v1.Pod{podWithPluginResourcesInInitContainers})
	for _, container := range podWithPluginResourcesInInitContainers.Spec.InitContainers {
		err = testManager.Allocate(tCtx, podWithPluginResourcesInInitContainers, &container, lifecycle.AddOperation)
	}
	for _, container := range podWithPluginResourcesInInitContainers.Spec.Containers {
		err = testManager.Allocate(tCtx, podWithPluginResourcesInInitContainers, &container, lifecycle.AddOperation)
	}
	as.NoError(err)
	podUID := string(podWithPluginResourcesInInitContainers.UID)
	initCont1 := podWithPluginResourcesInInitContainers.Spec.InitContainers[0].Name
	initCont2 := podWithPluginResourcesInInitContainers.Spec.InitContainers[1].Name
	normalCont1 := podWithPluginResourcesInInitContainers.Spec.Containers[0].Name
	normalCont2 := podWithPluginResourcesInInitContainers.Spec.Containers[1].Name
	initCont1Devices := testManager.podDevices.containerDevices(podUID, initCont1, res1.resourceName)
	initCont2Devices := testManager.podDevices.containerDevices(podUID, initCont2, res1.resourceName)
	normalCont1Devices := testManager.podDevices.containerDevices(podUID, normalCont1, res1.resourceName)
	normalCont2Devices := testManager.podDevices.containerDevices(podUID, normalCont2, res1.resourceName)
	as.Equal(1, initCont1Devices.Len())
	as.Equal(2, initCont2Devices.Len())
	as.Equal(1, normalCont1Devices.Len())
	as.Equal(1, normalCont2Devices.Len())
	as.True(initCont2Devices.IsSuperset(initCont1Devices))
	as.True(initCont2Devices.IsSuperset(normalCont1Devices))
	as.True(initCont2Devices.IsSuperset(normalCont2Devices))
	as.Equal(0, normalCont1Devices.Intersection(normalCont2Devices).Len())
}

// 验证可重启 init 容器的设备不会复用给其他容器，
// 而普通 init 容器的设备可被后续容器共享。
func TestRestartableInitContainerDeviceAllocation(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	// 请求创建在可重启 init 容器和普通容器中都申请 resourceName1 的 Pod，
	// 应成功，且不会把在 init 容器上分配的设备
	// 重新分配给普通容器。
	oneDevice := resource.NewQuantity(int64(1), resource.DecimalSI)
	twoDevice := resource.NewQuantity(int64(2), resource.DecimalSI)
	threeDevice := resource.NewQuantity(int64(3), resource.DecimalSI)
	res1 := TestResource{
		resourceName:     "domain1.com/resource1",
		resourceQuantity: *resource.NewQuantity(int64(6), resource.DecimalSI),
		devs: checkpoint.DevicesPerNUMA{
			0: []string{"dev1", "dev2", "dev3", "dev4", "dev5", "dev6"},
		},
		topology: false,
	}
	testResources := []TestResource{
		res1,
	}
	as := require.New(t)
	podsStub := activePodsStub{
		activePods: []*v1.Pod{},
	}
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	as.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
	})

	testManager, err := getTestManager(logger, tmpDir, podsStub.getActivePods, testResources)
	as.NoError(err)

	containerRestartPolicyAlways := v1.ContainerRestartPolicyAlways
	podWithPluginResourcesInRestartableInitContainers := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: uuid.NewUUID(),
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name: string(uuid.NewUUID()),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(res1.resourceName): *threeDevice,
						},
					},
				},
				{
					Name: string(uuid.NewUUID()),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(res1.resourceName): *oneDevice,
						},
					},
					RestartPolicy: &containerRestartPolicyAlways,
				},
				{
					Name: string(uuid.NewUUID()),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(res1.resourceName): *twoDevice,
						},
					},
					RestartPolicy: &containerRestartPolicyAlways,
				},
			},
			Containers: []v1.Container{
				{
					Name: string(uuid.NewUUID()),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(res1.resourceName): *oneDevice,
						},
					},
				},
				{
					Name: string(uuid.NewUUID()),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(res1.resourceName): *twoDevice,
						},
					},
				},
			},
		},
	}
	podsStub.updateActivePods([]*v1.Pod{podWithPluginResourcesInRestartableInitContainers})
	for _, container := range podWithPluginResourcesInRestartableInitContainers.Spec.InitContainers {
		err = testManager.Allocate(tCtx, podWithPluginResourcesInRestartableInitContainers, &container, lifecycle.AddOperation)
	}
	for _, container := range podWithPluginResourcesInRestartableInitContainers.Spec.Containers {
		err = testManager.Allocate(tCtx, podWithPluginResourcesInRestartableInitContainers, &container, lifecycle.AddOperation)
	}
	as.NoError(err)
	podUID := string(podWithPluginResourcesInRestartableInitContainers.UID)
	regularInitCont1 := podWithPluginResourcesInRestartableInitContainers.Spec.InitContainers[0].Name
	restartableInitCont2 := podWithPluginResourcesInRestartableInitContainers.Spec.InitContainers[1].Name
	restartableInitCont3 := podWithPluginResourcesInRestartableInitContainers.Spec.InitContainers[2].Name
	normalCont1 := podWithPluginResourcesInRestartableInitContainers.Spec.Containers[0].Name
	normalCont2 := podWithPluginResourcesInRestartableInitContainers.Spec.Containers[1].Name
	regularInitCont1Devices := testManager.podDevices.containerDevices(podUID, regularInitCont1, res1.resourceName)
	restartableInitCont2Devices := testManager.podDevices.containerDevices(podUID, restartableInitCont2, res1.resourceName)
	restartableInitCont3Devices := testManager.podDevices.containerDevices(podUID, restartableInitCont3, res1.resourceName)
	normalCont1Devices := testManager.podDevices.containerDevices(podUID, normalCont1, res1.resourceName)
	normalCont2Devices := testManager.podDevices.containerDevices(podUID, normalCont2, res1.resourceName)
	as.Equal(3, regularInitCont1Devices.Len())
	as.Equal(1, restartableInitCont2Devices.Len())
	as.Equal(2, restartableInitCont3Devices.Len())
	as.Equal(1, normalCont1Devices.Len())
	as.Equal(2, normalCont2Devices.Len())
	as.True(regularInitCont1Devices.IsSuperset(restartableInitCont2Devices))
	as.True(regularInitCont1Devices.IsSuperset(restartableInitCont3Devices))
	// regularInitCont1Devices 可与其他容器共享。

	dedicatedContainerDevices := []sets.Set[string]{
		restartableInitCont2Devices,
		restartableInitCont3Devices,
		normalCont1Devices,
		normalCont2Devices,
	}

	for i := 0; i < len(dedicatedContainerDevices)-1; i++ {
		for j := i + 1; j < len(dedicatedContainerDevices); j++ {
			t.Logf("containerDevices[%d] = %v", i, dedicatedContainerDevices[i])
			t.Logf("containerDevices[%d] = %v", j, dedicatedContainerDevices[j])
			as.Empty(dedicatedContainerDevices[i].Intersection(dedicatedContainerDevices[j]))
		}
	}
}

// 验证 UpdatePluginResources 只在节点可分配量不足时补充设备资源，
// 足够时保持不变。
func TestUpdatePluginResources(t *testing.T) {
	pod := &v1.Pod{}
	pod.UID = types.UID("testPod")

	resourceName1 := "domain1.com/resource1"
	devID1 := "dev1"

	resourceName2 := "domain2.com/resource2"
	devID2 := "dev2"

	as := assert.New(t)
	monitorCallback := func(logger klog.Logger, resourceName string, devices []*pluginapi.Device) {}
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	as.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
	})

	ckm, err := checkpointmanager.NewCheckpointManager(tmpDir)
	as.NoError(err)
	m := &ManagerImpl{
		allocatedDevices:  make(map[string]sets.Set[string]),
		healthyDevices:    make(map[string]sets.Set[string]),
		podDevices:        newPodDevices(),
		checkpointManager: ckm,
	}
	testManager := wrappedManagerImpl{
		ManagerImpl: m,
		callback:    monitorCallback,
	}
	testManager.podDevices.devs[string(pod.UID)] = make(containerDevices)

	// 分别申请一个 resource1 和一个 resource2。
	testManager.allocatedDevices[resourceName1] = sets.New[string]()
	testManager.allocatedDevices[resourceName1].Insert(devID1)
	testManager.allocatedDevices[resourceName2] = sets.New[string]()
	testManager.allocatedDevices[resourceName2].Insert(devID2)

	cachedNode := &v1.Node{
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				// 不申请 resource1，申请两个 resource2。
				v1.ResourceName(resourceName2): *resource.NewQuantity(int64(2), resource.DecimalSI),
			},
		},
	}
	nodeInfo := &schedulerframework.NodeInfo{}
	nodeInfo.SetNode(cachedNode)

	testManager.UpdatePluginResources(nodeInfo, &lifecycle.PodAdmitAttributes{Pod: pod})

	allocatableScalarResources := nodeInfo.Allocatable.ScalarResources
	// 节点可分配量不足需求，应把设备资源补入 ScalarResources。
	// nodeInfo 中的可分配量小于需求量，应执行更新。
	as.Equal(1, int(allocatableScalarResources[v1.ResourceName(resourceName1)]))
	// 节点可分配量已满足需求，保持原值不更新。
	// nodeInfo 中的可分配量大于需求量，应跳过更新。
	as.Equal(2, int(allocatableScalarResources[v1.ResourceName(resourceName2)]))
}

// 验证插件声明 PreStartRequired 时会调用 PreStartContainer RPC，
// 且请求数量为 0 时跳过该调用。
func TestDevicePreStartContainer(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	// 确保设备管理器被设备插件指示调用 `PreStartContainer` RPC 时，
	// 会在 endpoint 接口上调用 PreStartContainer。
	// 同时验证挂载、环境变量等的最终分配结果与预期一致。
	res1 := TestResource{
		resourceName:     "domain1.com/resource1",
		resourceQuantity: *resource.NewQuantity(int64(2), resource.DecimalSI),
		devs:             checkpoint.DevicesPerNUMA{0: []string{"dev1", "dev2"}},
		topology:         false,
	}
	as := require.New(t)
	podsStub := activePodsStub{
		activePods: []*v1.Pod{},
	}
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	as.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
	})

	testManager, err := getTestManager(logger, tmpDir, podsStub.getActivePods, []TestResource{res1})
	as.NoError(err)

	ch := make(chan []string, 1)
	testManager.endpoints[res1.resourceName] = endpointInfo{
		e: &MockEndpoint{
			initChan:     ch,
			allocateFunc: allocateStubFunc(),
		},
		opts: &pluginapi.DevicePluginOptions{PreStartRequired: true},
	}
	pod := makePod(v1.ResourceList{
		v1.ResourceName(res1.resourceName): res1.resourceQuantity})
	activePods := []*v1.Pod{}
	activePods = append(activePods, pod)
	podsStub.updateActivePods(activePods)
	err = testManager.Allocate(tCtx, pod, &pod.Spec.Containers[0], lifecycle.AddOperation)
	as.NoError(err)
	runContainerOpts, err := testManager.GetDeviceRunContainerOptions(tCtx, pod, &pod.Spec.Containers[0])
	as.NoError(err)
	var initializedDevs []string
	select {
	case <-time.After(time.Second):
		t.Fatalf("Timed out while waiting on channel for response from PreStartContainer RPC stub")
	case initializedDevs = <-ch:
		break
	}

	as.Contains(initializedDevs, "dev1")
	as.Contains(initializedDevs, "dev2")
	as.Len(initializedDevs, res1.devs.Devices().Len())

	expectedResps, err := allocateStubFunc()([]string{"dev1", "dev2"})
	as.NoError(err)
	as.Len(expectedResps.ContainerResponses, 1)
	expectedResp := expectedResps.ContainerResponses[0]
	as.Len(runContainerOpts.Devices, len(expectedResp.Devices))
	as.Len(runContainerOpts.Mounts, len(expectedResp.Mounts))
	as.Len(runContainerOpts.Envs, len(expectedResp.Envs))

	pod2 := makePod(v1.ResourceList{
		v1.ResourceName(res1.resourceName): *resource.NewQuantity(int64(0), resource.DecimalSI)})
	activePods = append(activePods, pod2)
	podsStub.updateActivePods(activePods)
	err = testManager.Allocate(tCtx, pod2, &pod2.Spec.Containers[0], lifecycle.AddOperation)
	as.NoError(err)
	_, err = testManager.GetDeviceRunContainerOptions(tCtx, pod2, &pod2.Spec.Containers[0])
	as.NoError(err)
	select {
	case <-time.After(time.Millisecond):
		t.Log("When pod resourceQuantity is 0,  PreStartContainer RPC stub will be skipped")
	case <-ch:
		break
	}
}

// 验证存在 checkpoint 时不应重置扩展资源容量，
// checkpoint 缺失（节点重建）时才应重置。
func TestResetExtendedResource(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	as := assert.New(t)
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	as.NoError(err)
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
	})
	ckm, err := checkpointmanager.NewCheckpointManager(tmpDir)
	as.NoError(err)
	testManager := &ManagerImpl{
		endpoints:         make(map[string]endpointInfo),
		healthyDevices:    make(map[string]sets.Set[string]),
		unhealthyDevices:  make(map[string]sets.Set[string]),
		allocatedDevices:  make(map[string]sets.Set[string]),
		podDevices:        newPodDevices(),
		checkpointManager: ckm,
	}

	extendedResourceName := "domain.com/resource"
	testManager.podDevices.insert("pod", "con", extendedResourceName,
		constructDevices([]string{"dev1"}),
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/dev1": "/dev/dev1"}),
			withMounts(map[string]string{"/home/lib1": "/usr/lib1"}),
		),
	)

	testManager.healthyDevices[extendedResourceName] = sets.New[string]()
	testManager.healthyDevices[extendedResourceName].Insert("dev1")
	// checkpoint 存在，说明节点未重建，不应重置扩展资源容量。
	// checkpoint 存在，表示节点未重新创建。
	err = testManager.writeCheckpoint(logger)
	require.NoError(t, err)

	as.False(testManager.ShouldResetExtendedResourceCapacity())

	// checkpoint 缺失，等价于节点重建，应重置扩展资源容量。
	// checkpoint 不存在，表示节点重新创建。
	ckpts, err := ckm.ListCheckpoints()
	as.NoError(err)
	for _, ckpt := range ckpts {
		err = ckm.RemoveCheckpoint(ckpt)
		as.NoError(err)
	}
	as.True(testManager.ShouldResetExtendedResourceCapacity())
}

// 返回一个确定性的 Allocate 桩：dev1/dev2 各自产生固定的设备、挂载与环境变量。
func allocateStubFunc() func(devs []string) (*pluginapi.AllocateResponse, error) {
	return func(devs []string) (*pluginapi.AllocateResponse, error) {
		resp := new(pluginapi.ContainerAllocateResponse)
		resp.Envs = make(map[string]string)
		for _, dev := range devs {
			switch dev {
			case "dev1":
				resp.Devices = append(resp.Devices, &pluginapi.DeviceSpec{
					ContainerPath: "/dev/aaa",
					HostPath:      "/dev/aaa",
					Permissions:   "mrw",
				})

				resp.Devices = append(resp.Devices, &pluginapi.DeviceSpec{
					ContainerPath: "/dev/bbb",
					HostPath:      "/dev/bbb",
					Permissions:   "mrw",
				})

				resp.Mounts = append(resp.Mounts, &pluginapi.Mount{
					ContainerPath: "/container_dir1/file1",
					HostPath:      "host_dir1/file1",
					ReadOnly:      true,
				})

			case "dev2":
				resp.Devices = append(resp.Devices, &pluginapi.DeviceSpec{
					ContainerPath: "/dev/ccc",
					HostPath:      "/dev/ccc",
					Permissions:   "mrw",
				})

				resp.Mounts = append(resp.Mounts, &pluginapi.Mount{
					ContainerPath: "/container_dir1/file2",
					HostPath:      "host_dir1/file2",
					ReadOnly:      true,
				})

				resp.Envs["key1"] = "val1"
			}
		}
		resps := new(pluginapi.AllocateResponse)
		resps.ContainerResponses = append(resps.ContainerResponses, resp)
		return resps, nil
	}
}

// 按 NUMA 分布构造设备 ID 到 pluginapi.Device 的映射，可选是否附带拓扑信息。
func makeDevice(devOnNUMA checkpoint.DevicesPerNUMA, topology bool) map[string]*pluginapi.Device {
	res := make(map[string]*pluginapi.Device)
	var topologyInfo *pluginapi.TopologyInfo
	for node, devs := range devOnNUMA {
		if topology {
			topologyInfo = &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: node}}}
		} else {
			topologyInfo = nil
		}
		for idx := range devs {
			res[devs[idx]] = &pluginapi.Device{ID: devs[idx], Topology: topologyInfo}
		}
	}
	return res
}

// 并发执行设备更新回调与拓扑 hint 计算，验证没有数据竞争；
// 发生竞态时 Go 运行时会直接终止进程。
func TestGetTopologyHintsWithUpdates(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	socketDir, socketName, _, err := tmpSocketDir()
	t.Cleanup(func() {
		require.NoErrorf(t, os.RemoveAll(socketDir), "unable to remove dir %s", socketDir)
	})
	require.NoError(t, err)

	devs := []*pluginapi.Device{}
	for i := 0; i < 1000; i++ {
		devs = append(devs, &pluginapi.Device{
			ID:     fmt.Sprintf("dev-%d", i),
			Health: pluginapi.Healthy,
			Topology: &pluginapi.TopologyInfo{
				Nodes: []*pluginapi.NUMANode{
					{ID: 0},
				},
			}})
	}
	testPod := makePod(v1.ResourceList{
		testResourceName: *resource.NewQuantity(int64(1), resource.DecimalSI),
	})
	topology := []cadvisorapi.Node{
		{Id: 0},
	}
	testCases := []struct {
		description string
		count       int
		devices     []*pluginapi.Device
		testfunc    func(manager *wrappedManagerImpl)
	}{
		{
			description: "GetTopologyHints data race when update device",
			count:       10,
			devices:     devs,
			testfunc: func(manager *wrappedManagerImpl) {
				manager.GetTopologyHints(logger, testPod, &testPod.Spec.Containers[0], lifecycle.AddOperation)
			},
		},
		{
			description: "GetPodTopologyHints data race when update device",
			count:       10,
			devices:     devs,
			testfunc: func(manager *wrappedManagerImpl) {
				manager.GetPodTopologyHints(logger, testPod, lifecycle.AddOperation)
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.description, func(t *testing.T) {
			m, _ := setupDeviceManager(t, nil, nil, socketName, topology, logger)
			defer func() {
				err := m.Stop(logger)
				require.NoError(t, err)
			}()
			mimpl := m.(*wrappedManagerImpl)

			wg := sync.WaitGroup{}
			wg.Add(2)

			updated := atomic.Bool{}
			updated.Store(false)
			go func() {
				defer wg.Done()
				for i := 0; i < test.count; i++ {
					// 模拟设备插件持续推送设备更新，与拓扑 hint 计算并发执行。
					mimpl.genericDeviceUpdateCallback(logger, testResourceName, devs)
				}
				updated.Store(true)
			}()
			go func() {
				defer wg.Done()
				for !updated.Load() {
					// 发生数据竞争时 Go 运行时会直接 throw 而无法 recover，
					// 例如 "Concurrent map iteration and map writing"。
					// 测试安静结束即表示无竞态；否则进程会自行退出并打印所有 goroutine 栈。
					test.testfunc(mimpl)
				}
			}()
			wg.Wait()
		})
	}
}

// 验证 UpdateAllocatedResourcesStatus 会把每台已分配设备的健康状态
// 写入容器状态中的 AllocatedResourcesStatus。
func TestUpdateAllocatedResourcesStatus(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	podUID := "test-pod-uid"
	containerName := "test-container"
	resourceName := "test-resource"

	tmpDir, err := os.MkdirTemp("", "checkpoint")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	defer func() {
		err = os.RemoveAll(tmpDir)
		if err != nil {
			t.Fatalf("Fail to remove tmpdir: %v", err)
		}
	}()
	ckm, err := checkpointmanager.NewCheckpointManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %v", err)
	}

	testManager := &ManagerImpl{
		endpoints:         make(map[string]endpointInfo),
		healthyDevices:    make(map[string]sets.Set[string]),
		unhealthyDevices:  make(map[string]sets.Set[string]),
		allocatedDevices:  make(map[string]sets.Set[string]),
		allDevices:        make(map[string]DeviceInstances),
		podDevices:        newPodDevices(),
		checkpointManager: ckm,
	}

	testManager.podDevices.insert(podUID, containerName, resourceName,
		constructDevices([]string{"dev1", "dev2"}),
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r1dev1": "/dev/r1dev1", "/dev/r1dev2": "/dev/r1dev2"}),
			withMounts(map[string]string{"/home/r1lib1": "/usr/r1lib1"}),
		),
	)

	testManager.genericDeviceUpdateCallback(logger, resourceName, []*pluginapi.Device{
		{ID: "dev1", Health: pluginapi.Healthy},
		{ID: "dev2", Health: pluginapi.Unhealthy},
	})

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(podUID),
		},
	}
	status := &v1.PodStatus{
		ContainerStatuses: []v1.ContainerStatus{
			{
				Name: containerName,
			},
		},
	}
	testManager.UpdateAllocatedResourcesStatus(logger, pod, status)

	expectedStatus := v1.ResourceStatus{
		Name: v1.ResourceName(resourceName),
		Resources: []v1.ResourceHealth{
			{
				ResourceID: "dev1",
				Health:     pluginapi.Healthy,
			},
			{
				ResourceID: "dev2",
				Health:     pluginapi.Unhealthy,
			},
		},
	}
	expectedContainerStatuses := []v1.ContainerStatus{
		{
			Name:                     containerName,
			AllocatedResourcesStatus: []v1.ResourceStatus{expectedStatus},
		},
	}

	// 为预期状态和实际状态排序资源。
	sortContainerStatuses(status.ContainerStatuses)
	sortContainerStatuses(expectedContainerStatuses)

	if !reflect.DeepEqual(status.ContainerStatuses, expectedContainerStatuses) {
		t.Errorf("UpdateAllocatedResourcesStatus failed, expected: %v, got: %v", expectedContainerStatuses, status.ContainerStatuses)
	}
}

// 按 ResourceID 对资源健康列表排序，便于比较。
func sortResourceHealth(resources []v1.ResourceHealth) {
	sort.SliceStable(resources, func(i, j int) bool {
		return resources[i].ResourceID < resources[j].ResourceID
	})
}

// 对每个容器状态中的资源健康列表排序，便于比较。
func sortContainerStatuses(statuses []v1.ContainerStatus) {
	for i := range statuses {
		for j := range statuses[i].AllocatedResourcesStatus {
			sortResourceHealth(statuses[i].AllocatedResourcesStatus[j].Resources)
		}
	}
}

// 验证 ResourceHealthStatus 特性门控下，设备健康变化会按 Pod 聚合后
// 推送到资源更新通道，且不重复推送。
func TestFeatureGateResourceHealthStatus(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	require.NoError(t, err, "err should be nil")
	defer func() {
		err = os.RemoveAll(tmpDir)
		if err != nil {
			t.Fatalf("Fail to remove tmpdir: %v", err)
		}
	}()
	ckm, err := checkpointmanager.NewCheckpointManager(tmpDir)
	require.NoError(t, err, "err should be nil")
	resourceName := "domain1.com/resource1"
	existDevices := map[string]DeviceInstances{}
	resourceNameMap := make(map[string]*pluginapi.Device)
	deviceUpdateNumber, deviceUpdateChanBuffer := 200, 100
	for i := 0; i < deviceUpdateNumber; i++ {
		resourceNameMap[fmt.Sprintf("dev%d", i)] = &pluginapi.Device{
			ID:     fmt.Sprintf("dev%d", i),
			Health: pluginapi.Healthy,
		}
	}
	existDevices[resourceName] = resourceNameMap

	testManager := &ManagerImpl{
		allDevices:        ResourceDeviceInstances(existDevices),
		endpoints:         make(map[string]endpointInfo),
		healthyDevices:    make(map[string]sets.Set[string]),
		unhealthyDevices:  make(map[string]sets.Set[string]),
		allocatedDevices:  make(map[string]sets.Set[string]),
		podDevices:        newPodDevices(),
		checkpointManager: ckm,
		update:            make(chan resourceupdates.Update, deviceUpdateChanBuffer),
	}

	for i := 0; i < deviceUpdateNumber; i++ {
		podID := fmt.Sprintf("pod%d", i)
		contID := fmt.Sprintf("con%d", i)
		devices := checkpoint.DevicesPerNUMA{0: []string{fmt.Sprintf("dev%d", i)}}
		testManager.podDevices.insert(podID, contID, resourceName,
			devices,
			newContainerAllocateResponse(
				withDevices(map[string]string{"/dev/r1dev1": "/dev/r1dev1", "/dev/r1dev2": "/dev/r1dev2"}),
				withMounts(map[string]string{"/home/r1lib1": "/usr/r1lib1"}),
			),
		)
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ResourceHealthStatus, true)

	for i := 0; i < deviceUpdateNumber; i++ {
		testManager.genericDeviceUpdateCallback(logger, resourceName, []*pluginapi.Device{
			{ID: "dev1", Health: pluginapi.Healthy},
		})
	}
	// 健康状态未变化时不应推送任何更新。
	assert.Empty(t, testManager.update)

	// 把所有设备置为不健康，应按 Pod 逐个推送资源更新。
	for i := 0; i < deviceUpdateNumber; i++ {
		testManager.genericDeviceUpdateCallback(logger, resourceName, []*pluginapi.Device{
			{ID: fmt.Sprintf("dev%d", i), Health: pluginapi.Unhealthy},
		})
	}
	for i := 0; i < deviceUpdateChanBuffer; i++ {
		u := <-testManager.update
		assert.Equal(t, resourceupdates.Update{
			PodUIDs: []string{fmt.Sprintf("pod%d", i)},
		}, u)
	}
}

// 验证不同资源下存在同名设备 ID 时，健康变化只通知拥有该设备的 Pod，
// 不会误伤同 ID 的其他资源。
func TestDeviceHealthUpdateWithDuplicateDeviceIDs(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tmpDir, err := os.MkdirTemp("", "checkpoint")
	require.NoError(t, err, "err should be nil")
	defer func() {
		err = os.RemoveAll(tmpDir)
		if err != nil {
			t.Fatalf("Fail to remove tmpdir: %v", err)
		}
	}()
	ckm, err := checkpointmanager.NewCheckpointManager(tmpDir)
	require.NoError(t, err, "err should be nil")

	// 两个资源暴露具有相同 ID 的设备。设备 ID 仅在
	// 单个资源范围内唯一。
	resourceName1 := "domain1.com/resource1"
	resourceName2 := "domain2.com/resource2"
	deviceID := "testdevice"
	existDevices := map[string]DeviceInstances{
		resourceName1: {deviceID: &pluginapi.Device{ID: deviceID, Health: pluginapi.Healthy}},
		resourceName2: {deviceID: &pluginapi.Device{ID: deviceID, Health: pluginapi.Healthy}},
	}

	testManager := &ManagerImpl{
		allDevices:        ResourceDeviceInstances(existDevices),
		endpoints:         make(map[string]endpointInfo),
		healthyDevices:    make(map[string]sets.Set[string]),
		unhealthyDevices:  make(map[string]sets.Set[string]),
		allocatedDevices:  make(map[string]sets.Set[string]),
		podDevices:        newPodDevices(),
		checkpointManager: ckm,
		update:            make(chan resourceupdates.Update, 10),
	}

	testManager.podDevices.insert("pod1", "con1", resourceName1,
		checkpoint.DevicesPerNUMA{0: []string{deviceID}},
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r1dev1": "/dev/r1dev1"}),
		),
	)
	testManager.podDevices.insert("pod2", "con2", resourceName2,
		checkpoint.DevicesPerNUMA{0: []string{deviceID}},
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r2dev1": "/dev/r2dev1"}),
		),
	)

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ResourceHealthStatus, true)

	// resource2 的设备变为不健康：仅必须通知 pod2。
	testManager.genericDeviceUpdateCallback(logger, resourceName2, []*pluginapi.Device{
		{ID: deviceID, Health: pluginapi.Unhealthy},
	})
	u := <-testManager.update
	assert.Equal(t, resourceupdates.Update{PodUIDs: []string{"pod2"}}, u)
	assert.Empty(t, testManager.update)

	// resource1 的设备变为不健康：仅必须通知 pod1。
	testManager.genericDeviceUpdateCallback(logger, resourceName1, []*pluginapi.Device{
		{ID: deviceID, Health: pluginapi.Unhealthy},
	})
	u = <-testManager.update
	assert.Equal(t, resourceupdates.Update{PodUIDs: []string{"pod1"}}, u)
	assert.Empty(t, testManager.update)
}

// 验证引用 DRA 扩展资源的 Pod 在 DRAExtendedResource 特性门控开启时准入通过，
// 关闭时准入失败。
func TestAdmitPodWithDRAResources(t *testing.T) {
	tCtx := ktesting.Init(t)
	testCases := map[string]struct {
		enableFeatureGate bool
		checkError        func(t require.TestingT, err error, msgAndArgs ...interface{})
	}{
		"DRAExtendedResource enabled": {
			enableFeatureGate: true,
			checkError:        require.NoError,
		},
		"DRAExtendedResource disabled": {
			enableFeatureGate: false,
			checkError:        require.Error,
		},
	}

	containerName := "container1"
	resourceName := "domain1.com/resource1"

	for description, test := range testCases {
		t.Run(description, func(t *testing.T) {
			if !test.enableFeatureGate {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.36"))
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAExtendedResource, test.enableFeatureGate)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: uuid.NewUUID(),
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: containerName,
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(resourceName): resource.MustParse("1"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
						RequestMappings: []v1.ContainerExtendedResourceRequest{
							{
								ContainerName: containerName,
								ResourceName:  resourceName,
							},
						},
					},
				},
			}

			require.True(t, isDRAExtendedResource(pod, containerName, resourceName))

			testManager := &ManagerImpl{
				devicesToReuse: make(PodReusableDevices),
				podDevices:     newPodDevices(),
				allocatedDevices: map[string]sets.Set[string]{
					resourceName: sets.New("Dev"),
				},
				activePods:   func() []*v1.Pod { return nil },
				sourcesReady: &sourcesReadyStub{},
			}

			err := testManager.Allocate(tCtx, pod, &pod.Spec.Containers[0], lifecycle.AddOperation)
			test.checkError(t, err)
		})
	}
}

// 验证插件断开后其设备全部转入不健康集合；
// 端点超过宽限期被回收时，相关资源从所有内部 map 中清理干净。
func TestEndpointSyncOnDisconnect(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	socketDir, socketName, _, err := tmpSocketDir()
	require.NoError(t, err)
	defer func() {
		if err := os.RemoveAll(socketDir); err != nil {
			logger.Error(err, "unable to remove socket directory", "dir", socketDir)
		}
	}()

	manager, err := newManagerImpl(logger, socketName, nil, nil)
	require.NoError(t, err)

	resourceName := "domain1.com/resource1"
	ep := &endpointImpl{
		resourceName: resourceName,
		client:       plugin.NewPluginClient(resourceName, socketName, manager),
		stopTime:     time.Now().Add(-endpointStopGracePeriod * 2), // 使宽限期过期。
		socket:       socketName,
	}

	eInfo := endpointInfo{e: ep, opts: nil}
	manager.endpoints[resourceName] = eInfo
	manager.endpointStore[resourceName] = map[string]*endpointInfo{socketName: &eInfo}
	devs := []*pluginapi.Device{
		{ID: "Device1", Health: pluginapi.Healthy},
		{ID: "Device2", Health: pluginapi.Healthy},
		{ID: "Device3", Health: pluginapi.Unhealthy},
	}
	manager.genericDeviceUpdateCallback(logger, resourceName, devs)

	// 断开连接后，该资源的所有设备都应移入不健康集合。
	err = ep.client.Disconnect(logger)
	require.NoError(t, err)

	require.Contains(t, manager.endpoints, resourceName)
	require.Contains(t, manager.healthyDevices, resourceName)
	require.Contains(t, manager.unhealthyDevices, resourceName)
	require.Len(t, manager.endpoints, 1)
	require.Empty(t, manager.healthyDevices[resourceName])
	require.Equal(t, len(devs), manager.unhealthyDevices[resourceName].Len())

	// 直接让端点宽限期过期，避免测试真正等待。
	ep.stopTime = time.Now().Add(-endpointStopGracePeriod * 2)
	// 调用 GetCapacity 触发端点清理，回归 https://github.com/kubernetes/kubernetes/issues/133702。
	manager.GetCapacity(logger)

	require.Empty(t, manager.endpoints)
	require.Empty(t, manager.healthyDevices)
	require.Empty(t, manager.unhealthyDevices)
}

// 以下生命周期测试验证设备管理器是否根据给定的 lifecycle.Operation
// 执行或跳过操作。
// 由于 Allocate* 在操作不受支持时不返回错误
// （为避免中断所有 hint provider 的整体操作，它会静默返回 nil），
// 因此测试通过检查日志输出来确认操作
// 是否得到处理。
//
// 测试所用的设备 ID、Pod 资源请求等值是任意选取的，
// 但足以让代码走过正常路径；具体数值没有特殊含义。
// 这些测试不验证分配结果是否正确，
// 只验证针对给定生命周期操作，
// 该操作是否得到处理或被跳过。

// 验证容器级 Allocate 只处理 AddOperation，其他操作会记录跳过日志并静默返回。
func TestLifecycleAllocate(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "DeviceManager processes AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "DeviceManager skips ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "DeviceManager skips empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			res := TestResource{
				resourceName:     "domain1.com/resource1",
				resourceQuantity: *resource.NewQuantity(int64(2), resource.DecimalSI),
				devs:             checkpoint.DevicesPerNUMA{0: []string{"dev1", "dev2"}},
				topology:         true,
			}
			testResources := []TestResource{res}

			podsStub := activePodsStub{
				activePods: []*v1.Pod{},
			}
			tmpDir, err := os.MkdirTemp("", "checkpoint")
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
			})

			testManager, err := getTestManager(logger, tmpDir, podsStub.getActivePods, testResources)
			require.NoError(t, err)

			pod := makePod(v1.ResourceList{v1.ResourceName(res.resourceName): res.resourceQuantity})
			activePods := []*v1.Pod{pod}
			podsStub.updateActivePods(activePods)

			err = testManager.Allocate(tCtx, pod, &pod.Spec.Containers[0], testCase.operation)
			require.NoError(t, err)

			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "Device Manager support only Allocate(add)"
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

// 验证 Pod 级 AllocatePod 对所有操作都不做实际分配，仅记录跳过日志。
func TestLifecycleAllocatePod(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "DeviceManager skips all pod-level operations including AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     true,
		},
		{
			description: "DeviceManager skips all pod-level operations including ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "DeviceManager skips all pod-level operations including empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			res := TestResource{
				resourceName:     "domain1.com/resource1",
				resourceQuantity: *resource.NewQuantity(int64(2), resource.DecimalSI),
				devs:             checkpoint.DevicesPerNUMA{0: []string{"dev1", "dev2"}},
				topology:         true,
			}
			testResources := []TestResource{res}

			podsStub := activePodsStub{
				activePods: []*v1.Pod{},
			}
			tmpDir, err := os.MkdirTemp("", "checkpoint")
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoErrorf(t, os.RemoveAll(tmpDir), "unable to remove dir %s", tmpDir)
			})

			testManager, err := getTestManager(logger, tmpDir, podsStub.getActivePods, testResources)
			require.NoError(t, err)

			pod := makePod(v1.ResourceList{v1.ResourceName(res.resourceName): res.resourceQuantity})
			activePods := []*v1.Pod{pod}
			podsStub.updateActivePods(activePods)

			err = testManager.AllocatePod(logger, pod, testCase.operation)
			require.NoError(t, err)

			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "Device Manager does not support pod level resource allocation"
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
