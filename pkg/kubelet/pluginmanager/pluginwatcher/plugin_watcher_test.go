/*
Copyright 2018 The Kubernetes Authors.

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

package pluginwatcher

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/test/utils/ktesting"
)

var (
	// supportedVersions 是测试插件声明支持的注册 API 版本。
	supportedVersions = []string{"v1beta1", "v1beta2"}
)

// init 配置测试日志输出与详细级别。
func init() {
	var logLevel string

	flags := &flag.FlagSet{}
	// 打开 klog 到 stderr，并把 verbosity 设为 6。
	klog.InitFlags(flags)
	flag.Set("alsologtostderr", fmt.Sprintf("%t", true))
	flag.StringVar(&logLevel, "logLevel", "6", "test")
	flag.Lookup("v").Value.Set(logLevel)
}

// initTempDir 创建独立的临时目录。
func initTempDir(t *testing.T) string {
	// 创建独立目录：os.RemoveAll 不够原子，
	// 而 os.MkdirAll 在 Windows 上可能遇到 "Access Denied" 错误。
	d, err := os.MkdirTemp("", "plugin_test")
	if err != nil {
		t.Fatalf("Could not create a temp directory %s: %v", d, err)
	}

	return d
}

// waitForRegistration 轮询等待插件出现在期望状态缓存中。
func waitForRegistration(
	t *testing.T,
	socketPath string,
	dsw cache.DesiredStateOfWorld) {
	err := retryWithExponentialBackOff(
		time.Duration(500*time.Millisecond),
		func() (bool, error) {
			if dsw.PluginExists(socketPath) {
				return true, nil
			}
			return false, nil
		},
	)
	if err != nil {
		t.Fatalf("Timed out waiting for plugin to be added to desired state of world cache:\n%s.", socketPath)
	}
}

// waitForUnregistration 轮询等待插件从期望状态缓存中移除。
func waitForUnregistration(
	t *testing.T,
	socketPath string,
	dsw cache.DesiredStateOfWorld) {
	err := retryWithExponentialBackOff(
		time.Duration(500*time.Millisecond),
		func() (bool, error) {
			if !dsw.PluginExists(socketPath) {
				return true, nil
			}
			return false, nil
		},
	)

	if err != nil {
		t.Fatalf("Timed out waiting for plugin to be unregistered:\n%s.", socketPath)
	}
}

// retryWithExponentialBackOff 以指数退避执行条件函数，最多 6 步。
func retryWithExponentialBackOff(initialDuration time.Duration, fn wait.ConditionFunc) error {
	backoff := wait.Backoff{
		Duration: initialDuration,
		Factor:   3,
		Jitter:   0,
		Steps:    6,
	}
	return wait.ExponentialBackoff(backoff, fn)
}

// TestPluginRegistration 验证插件的注册与注销会同步到期望状态缓存。
func TestPluginRegistration(t *testing.T) {
	socketDir := initTempDir(t)
	defer os.RemoveAll(socketDir)

	tCtx := ktesting.Init(t)
	dsw := cache.NewDesiredStateOfWorld()
	newWatcher(t, socketDir, dsw, wait.NeverStop)

	// 循环创建 10 个不同 socket 的插件，逐一验证注册与注销。
	for i := 0; i < 10; i++ {
		socketPath := filepath.Join(socketDir, fmt.Sprintf("plugin-%d.sock", i))
		pluginName := fmt.Sprintf("example-plugin-%d", i)

		p := NewTestExamplePlugin(pluginName, registerapi.DevicePlugin, socketPath, supportedVersions...)
		require.NoError(t, p.Serve(tCtx, "v1beta1", "v1beta2"))

		pluginInfo := GetPluginInfo(p)
		waitForRegistration(t, pluginInfo.SocketPath, dsw)

		// 校验期望状态缓存中的插件列表。
		dswPlugins := dsw.GetPluginsToRegister()
		if len(dswPlugins) != 1 {
			t.Fatalf("TestPluginRegistration: desired state of world length should be 1 but it's %d", len(dswPlugins))
		}

		// 停止插件；它应从期望状态缓存中移除。
		require.NoError(t, p.Stop(tCtx))
		// 本地运行单元测试时，插件监听器可能无法捕获删除事件，因此依赖轮询等待。
		waitForUnregistration(t, pluginInfo.SocketPath, dsw)
		dswPlugins = dsw.GetPluginsToRegister()
		if len(dswPlugins) != 0 {
			t.Fatalf("TestPluginRegistration: desired state of world length should be 0 but it's %d", len(dswPlugins))
		}
	}
}

// TestPluginRegistrationSameName 验证同名同类型但 socket 路径不同的插件可以共存。
func TestPluginRegistrationSameName(t *testing.T) {
	socketDir := initTempDir(t)
	defer os.RemoveAll(socketDir)

	tCtx := ktesting.Init(t)
	dsw := cache.NewDesiredStateOfWorld()
	newWatcher(t, socketDir, dsw, wait.NeverStop)

	// 创建 10 个名称和类型相同、socket 路径不同的插件；
	// 它们都应存在于期望状态缓存中。
	pluginName := "dep-example-plugin"
	for i := 0; i < 10; i++ {
		socketPath := filepath.Join(socketDir, fmt.Sprintf("plugin-%d.sock", i))
		p := NewTestExamplePlugin(pluginName, registerapi.DevicePlugin, socketPath, supportedVersions...)
		require.NoError(t, p.Serve(tCtx, "v1beta1", "v1beta2"))

		pluginInfo := GetPluginInfo(p)
		waitForRegistration(t, pluginInfo.SocketPath, dsw)

		// 校验缓存中插件数量随创建数量递增。
		dswPlugins := dsw.GetPluginsToRegister()
		if len(dswPlugins) != i+1 {
			t.Fatalf("TestPluginRegistrationSameName: desired state of world length should be %d but it's %d", i+1, len(dswPlugins))
		}
	}
}

// TestPluginReRegistration 验证插件注销后以新名称重新注册时，缓存中的时间戳会更新。
func TestPluginReRegistration(t *testing.T) {
	socketDir := initTempDir(t)
	defer os.RemoveAll(socketDir)

	tCtx := ktesting.Init(t)
	dsw := cache.NewDesiredStateOfWorld()
	newWatcher(t, socketDir, dsw, wait.NeverStop)

	// 先创建一个插件，随后删除它，再以不同的名称重新创建。
	socketPath := filepath.Join(socketDir, "plugin-reregistration.sock")
	pluginName := "reregister-plugin"
	p := NewTestExamplePlugin(pluginName, registerapi.DevicePlugin, socketPath, supportedVersions...)
	require.NoError(t, p.Serve(tCtx, "v1beta1", "v1beta2"))
	pluginInfo := GetPluginInfo(p)
	lastTimestamp := time.Now()
	waitForRegistration(t, pluginInfo.SocketPath, dsw)

	// 重复 10 次：删除插件，再以不同的名称重新创建；
	// 更新后的插件应存在于期望状态缓存中。
	for i := 0; i < 10; i++ {
		// 停止插件；它应从期望状态缓存中移除。
		// 本地运行单元测试时删除事件可能无法被捕获。
		require.NoError(t, p.Stop(tCtx))
		waitForUnregistration(t, pluginInfo.SocketPath, dsw)

		// 以新名称重新创建插件。
		pluginName := fmt.Sprintf("dep-example-plugin-%d", i)
		p := NewTestExamplePlugin(pluginName, registerapi.DevicePlugin, socketPath, supportedVersions...)
		require.NoError(t, p.Serve(tCtx, "v1beta1", "v1beta2"))
		waitForRegistration(t, pluginInfo.SocketPath, dsw)

		// 校验缓存中只剩更新后的这个插件。
		dswPlugins := dsw.GetPluginsToRegister()
		if len(dswPlugins) != 1 {
			t.Fatalf("TestPluginReRegistration: desired state of world length should be 1 but it's %d", len(dswPlugins))
		}
		if !dswPlugins[0].Timestamp.After(lastTimestamp) {
			t.Fatalf("TestPluginReRegistration: for plugin %s timestamp of plugin is not updated", pluginName)
		}
		lastTimestamp = dswPlugins[0].Timestamp
	}
}

// TestPluginRegistrationAtKubeletStart 验证 kubelet 启动时已存在的插件会被发现并注册。
func TestPluginRegistrationAtKubeletStart(t *testing.T) {
	socketDir := initTempDir(t)
	defer os.RemoveAll(socketDir)

	tCtx := ktesting.Init(t)
	plugins := make([]*examplePlugin, 10)

	for i := 0; i < len(plugins); i++ {
		socketPath := filepath.Join(socketDir, fmt.Sprintf("plugin-%d.sock", i))
		pluginName := fmt.Sprintf("example-plugin-%d", i)

		p := NewTestExamplePlugin(pluginName, registerapi.DevicePlugin, socketPath, supportedVersions...)
		require.NoError(t, p.Serve(tCtx, "v1beta1", "v1beta2"))
		defer func(p *examplePlugin) {
			require.NoError(t, p.Stop(tCtx))
		}(p)

		plugins[i] = p
	}

	// 先在磁盘上创建 10 个插件 socket，然后启动监听器。
	dsw := cache.NewDesiredStateOfWorld()
	newWatcher(t, socketDir, dsw, wait.NeverStop)

	var wg sync.WaitGroup
	for i := 0; i < len(plugins); i++ {
		wg.Add(1)
		go func(p *examplePlugin) {
			defer wg.Done()

			pluginInfo := GetPluginInfo(p)
			// 校验插件已进入期望状态缓存。
			waitForRegistration(t, pluginInfo.SocketPath, dsw)
		}(plugins[i])
	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
		return
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("Timeout while waiting for the plugin registration status")
	}
}

// newWatcher 创建并启动 Watcher，失败时终止测试。
func newWatcher(t *testing.T, socketDir string, desiredStateOfWorldCache cache.DesiredStateOfWorld, stopCh <-chan struct{}) *Watcher {
	tCtx := ktesting.Init(t)
	w := NewWatcher(socketDir, desiredStateOfWorldCache)
	require.NoError(t, w.Start(tCtx, stopCh))

	return w
}
