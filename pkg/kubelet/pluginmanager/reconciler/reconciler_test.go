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

package reconciler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/operationexecutor"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/pluginwatcher"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	// reconcilerLoopSleepDuration 是 reconciler 循环两次执行之间的等待时间。
	// 测试里设得极短，以便快速观察到状态变化。
	reconcilerLoopSleepDuration time.Duration = 1 * time.Nanosecond
)

var (
	socketDir         string
	supportedVersions = []string{"v1beta1", "v1beta2"}
)

// init 创建包级共享的 socket 临时目录。
func init() {
	d, err := os.MkdirTemp("", "reconciler_test")
	if err != nil {
		panic(fmt.Sprintf("Could not create a temp directory: %s", d))
	}
	socketDir = d
}

// cleanup 清空并重建 socket 目录，避免用例之间互相影响。
func cleanup(t *testing.T) {
	require.NoError(t, os.RemoveAll(socketDir))
	os.MkdirAll(socketDir, 0755)
}

// runReconciler 在后台启动 reconciler 循环。
func runReconciler(t *testing.T, reconciler Reconciler) {
	tCtx := ktesting.Init(t)
	go reconciler.Run(tCtx)
}

// waitForRegistration 轮询 ASW，直到出现 socket 与 UUID 都匹配的插件。
func waitForRegistration(
	t *testing.T,
	socketPath string,
	expectedUUID types.UID,
	asw cache.ActualStateOfWorld) {
	err := retryWithExponentialBackOff(
		time.Duration(500*time.Millisecond),
		func() (bool, error) {
			registeredPlugins := asw.GetRegisteredPlugins()
			for _, plugin := range registeredPlugins {
				if plugin.SocketPath == socketPath && plugin.UUID == expectedUUID {
					return true, nil
				}
			}
			return false, nil
		},
	)
	if err != nil {
		t.Fatalf("Timed out waiting for plugin to be registered:\n%s.", socketPath)
	}
}

// waitForUnregistration 轮询 ASW，直到指定 socket 的插件消失。
func waitForUnregistration(
	t *testing.T,
	socketPath string,
	asw cache.ActualStateOfWorld) {
	err := retryWithExponentialBackOff(
		time.Duration(500*time.Millisecond),
		func() (bool, error) {
			registeredPlugins := asw.GetRegisteredPlugins()
			for _, plugin := range registeredPlugins {
				if plugin.SocketPath == socketPath {
					return false, nil
				}
			}
			return true, nil
		},
	)

	if err != nil {
		t.Fatalf("Timed out waiting for plugin to be unregistered:\n%s.", socketPath)
	}
}

// retryWithExponentialBackOff 以 3 倍因子最多重试 6 次。
func retryWithExponentialBackOff(initialDuration time.Duration, fn wait.ConditionFunc) error {
	backoff := wait.Backoff{
		Duration: initialDuration,
		Factor:   3,
		Jitter:   0,
		Steps:    6,
	}
	return wait.ExponentialBackoff(backoff, fn)
}

// DummyImpl 是 PluginHandler 的空实现，用于测试 reconciler 的注册流程。
type DummyImpl struct{}

// NewDummyImpl 创建空实现的 PluginHandler。
func NewDummyImpl() *DummyImpl {
	return &DummyImpl{}
}

// ValidatePlugin 是空实现，始终返回 nil。
func (d *DummyImpl) ValidatePlugin(_ context.Context, pluginName string, endpoint string, versions []string) error {
	return nil
}

// RegisterPlugin 是空实现，始终返回 nil。
func (d *DummyImpl) RegisterPlugin(_ context.Context, pluginName string, endpoint string, versions []string, pluginClientTimeout *time.Duration) error {
	return nil
}

// DeRegisterPlugin 是空实现。
func (d *DummyImpl) DeRegisterPlugin(_ context.Context, pluginName, endpoint string) {
}

// Test_Run_Positive_DoNothing 启动 Run()，验证没有插件时
// ASW 与 DSW 都保持为空。
func Test_Run_Positive_DoNothing(t *testing.T) {
	defer cleanup(t)

	dsw := cache.NewDesiredStateOfWorld()
	asw := cache.NewActualStateOfWorld()
	fakeRecorder := &record.FakeRecorder{}
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeRecorder,
	))
	reconciler := NewReconciler(
		oex,
		reconcilerLoopSleepDuration,
		dsw,
		asw,
	)
	// 启动 reconciler 循环。
	runReconciler(t, reconciler)

	// 没有注册来源时，两个状态缓存都应保持为空。
	if len(asw.GetRegisteredPlugins()) != 0 {
		t.Fatalf("Test_Run_Positive_DoNothing: actual state of world should be empty but it's not")
	}
	if len(dsw.GetPluginsToRegister()) != 0 {
		t.Fatalf("Test_Run_Positive_DoNothing: desired state of world should be empty but it's not")
	}
}

// Test_Run_Positive_Register 向 DSW 加入一个插件并启动 Run()，
// 验证该插件最终出现在 ASW 中。
func Test_Run_Positive_Register(t *testing.T) {
	defer cleanup(t)

	tCtx := ktesting.Init(t)
	dsw := cache.NewDesiredStateOfWorld()
	asw := cache.NewActualStateOfWorld()
	di := NewDummyImpl()
	fakeRecorder := &record.FakeRecorder{}
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeRecorder,
	))
	reconciler := NewReconciler(
		oex,
		reconcilerLoopSleepDuration,
		dsw,
		asw,
	)
	reconciler.AddHandler(registerapi.DevicePlugin, cache.PluginHandler(di))

	// 启动 reconciler，把 DSW 中的插件同步到 ASW。
	reconcilerCtx := tCtx.WithCancel()
	defer reconcilerCtx.Cancel("stop reconciler")
	go reconciler.Run(reconcilerCtx)
	socketPath := filepath.Join(socketDir, "plugin.sock")
	pluginName := fmt.Sprintf("example-plugin")
	p := pluginwatcher.NewTestExamplePlugin(pluginName, registerapi.DevicePlugin, socketPath, supportedVersions...)
	require.NoError(t, p.Serve(tCtx, "v1beta1", "v1beta2"))
	// 启动插件服务后把 socket 路径写入 DSW，触发注册流程。
	defer func() {
		require.NoError(t, p.Stop(tCtx))
	}()
	require.NoError(t, dsw.AddOrUpdatePlugin(tCtx, socketPath))

	plugins := dsw.GetPluginsToRegister()
	waitForRegistration(t, socketPath, plugins[0].UUID, asw)

	// ASW 中应出现刚加入的插件。
	aswPlugins := asw.GetRegisteredPlugins()
	if len(aswPlugins) != 1 {
		t.Fatalf("Test_Run_Positive_Register: actual state of world length should be one but it's %d", len(aswPlugins))
	}
	if aswPlugins[0].SocketPath != socketPath {
		t.Fatalf("Test_Run_Positive_Register: expected\n%s\nin actual state of world, but got\n%v\n", socketPath, aswPlugins[0])
	}
}

// Test_Run_Positive_RegisterThenUnregister 先注册一个插件，确认其进入 ASW；
// 再从 DSW 删除并移除 socket，验证它最终从 ASW 中消失。
func Test_Run_Positive_RegisterThenUnregister(t *testing.T) {
	defer cleanup(t)

	tCtx := ktesting.Init(t)
	dsw := cache.NewDesiredStateOfWorld()
	asw := cache.NewActualStateOfWorld()
	di := NewDummyImpl()
	fakeRecorder := &record.FakeRecorder{}
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeRecorder,
	))
	reconciler := NewReconciler(
		oex,
		reconcilerLoopSleepDuration,
		dsw,
		asw,
	)
	reconciler.AddHandler(registerapi.DevicePlugin, cache.PluginHandler(di))

	// 启动 reconciler，把 DSW 中的插件同步到 ASW。
	reconcilerCtx := tCtx.WithCancel()
	defer reconcilerCtx.Cancel("stop reconciler")
	go reconciler.Run(reconcilerCtx)

	socketPath := filepath.Join(socketDir, "plugin.sock")
	pluginName := fmt.Sprintf("example-plugin")
	p := pluginwatcher.NewTestExamplePlugin(pluginName, registerapi.DevicePlugin, socketPath, supportedVersions...)
	require.NoError(t, p.Serve(tCtx, "v1beta1", "v1beta2"))
	require.NoError(t, dsw.AddOrUpdatePlugin(tCtx, socketPath))
	plugins := dsw.GetPluginsToRegister()
	waitForRegistration(t, socketPath, plugins[0].UUID, asw)

	// ASW 中应出现刚加入的插件。
	aswPlugins := asw.GetRegisteredPlugins()
	if len(aswPlugins) != 1 {
		t.Fatalf("Test_Run_Positive_RegisterThenUnregister: actual state of world length should be one but it's %d", len(aswPlugins))
	}
	if aswPlugins[0].SocketPath != socketPath {
		t.Fatalf("Test_Run_Positive_RegisterThenUnregister: expected\n%s\nin actual state of world, but got\n%v\n", socketPath, aswPlugins[0])
	}

	// 从 DSW 删除并移除 socket 文件，模拟插件退出。
	dsw.RemovePlugin(socketPath)
	os.Remove(socketPath)
	waitForUnregistration(t, socketPath, asw)

	// 注销后 ASW 中不应再包含该插件。
	aswPlugins = asw.GetRegisteredPlugins()
	if len(aswPlugins) != 0 {
		t.Fatalf("Test_Run_Positive_RegisterThenUnregister: actual state of world length should be zero but it's %d", len(aswPlugins))
	}
}

// Test_Run_Positive_ReRegister 注册插件后更新其时间戳，
// 验证 reconciler 会触发重新注册，ASW 中保留更新后的插件。
func Test_Run_Positive_ReRegister(t *testing.T) {
	defer cleanup(t)

	tCtx := ktesting.Init(t)
	dsw := cache.NewDesiredStateOfWorld()
	asw := cache.NewActualStateOfWorld()
	di := NewDummyImpl()
	fakeRecorder := &record.FakeRecorder{}
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeRecorder,
	))
	reconciler := NewReconciler(
		oex,
		reconcilerLoopSleepDuration,
		dsw,
		asw,
	)
	reconciler.AddHandler(registerapi.DevicePlugin, cache.PluginHandler(di))

	// 启动 reconciler，把 DSW 中的插件同步到 ASW。
	reconcilerCtx := tCtx.WithCancel()
	defer reconcilerCtx.Cancel("stop reconciler")
	go reconciler.Run(reconcilerCtx)

	socketPath := filepath.Join(socketDir, "plugin2.sock")
	pluginName := fmt.Sprintf("example-plugin2")
	p := pluginwatcher.NewTestExamplePlugin(pluginName, registerapi.DevicePlugin, socketPath, supportedVersions...)
	require.NoError(t, p.Serve(tCtx, "v1beta1", "v1beta2"))
	require.NoError(t, dsw.AddOrUpdatePlugin(tCtx, socketPath))
	plugins := dsw.GetPluginsToRegister()
	waitForRegistration(t, socketPath, plugins[0].UUID, asw)

	// 再次加入同一插件以更新时间戳。
	require.NoError(t, dsw.AddOrUpdatePlugin(tCtx, socketPath))
	// 时间戳变化会触发注销再注册；这一过程可能快到无法用 waitForUnregistration 捕捉，
	// 因此改为验证 ASW 中插件的时间戳已更新。
	plugins = dsw.GetPluginsToRegister()
	waitForRegistration(t, socketPath, plugins[0].UUID, asw)

	// ASW 中应出现刚加入的插件。
	aswPlugins := asw.GetRegisteredPlugins()
	if len(aswPlugins) != 1 {
		t.Fatalf("Test_Run_Positive_RegisterThenUnregister: actual state of world length should be one but it's %d", len(aswPlugins))
	}
	if aswPlugins[0].SocketPath != socketPath {
		t.Fatalf("Test_Run_Positive_RegisterThenUnregister: expected\n%s\nin actual state of world, but got\n%v\n", socketPath, aswPlugins[0])
	}
}
