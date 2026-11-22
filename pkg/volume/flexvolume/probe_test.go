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

package flexvolume

import (
	"fmt"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"

	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/utils/exec"
)

const (
	pluginDir       = "/flexvolume"
	driverName      = "fake-driver"
	errorDriverName = "error-driver"
)

// assertPathSuffix 断言 dir2 以 dir1 结尾。
func assertPathSuffix(t *testing.T, dir1 string, dir2 string) {
	assert.True(t, strings.HasSuffix(dir2, dir1))
}

// 验证 prober 初始化前已安装的驱动能被探测到。
func TestProberExistingDriverBeforeInit(t *testing.T) {
	// 准备
	driverPath, _, watcher, prober := initTestEnvironment(t)

	// 执行
	events, err := prober.Probe()

	// 断言
	// 首次 Probe 应返回 1 个插件，并注册 2 个 watch（pluginDir 及其当前所有子目录）。
	assert.Len(t, events, 1)
	assert.Equal(t, volume.ProbeAddOrUpdate, events[0].Op)
	plugDir := pluginDir
	if goruntime.GOOS == "windows" {
		plugDir = "\\flexvolume"
	}
	assertPathSuffix(t, plugDir, watcher.watches[0])
	assertPathSuffix(t, driverPath, watcher.watches[1])
	assert.NoError(t, err)

	// 不应再探测到插件。

	// 执行
	events, err = prober.Probe()
	// 断言
	assert.Empty(t, events)
	assert.NoError(t, err)
}

// 验证 prober 运行后新增的驱动能被探测到。
func TestProberAddRemoveDriver(t *testing.T) {
	// 准备
	_, fs, watcher, prober := initTestEnvironment(t)
	prober.Probe()
	events, err := prober.Probe()
	assert.NoError(t, err)
	assert.Empty(t, events)

	// 文件新增后调用 Probe，应返回 1 个事件。

	// 添加驱动
	const driverName2 = "fake-driver2"
	driverPath := filepath.Join(pluginDir, driverName2)
	executablePath := filepath.Join(driverPath, driverName2)
	installDriver(driverName2, fs)
	watcher.TriggerEvent(fsnotify.Create, driverPath)
	watcher.TriggerEvent(fsnotify.Create, executablePath)

	// 执行
	events, err = prober.Probe()

	// 断言
	assert.Len(t, events, 1)
	assert.Equal(t, volume.ProbeAddOrUpdate, events[0].Op)                   // 1 newly added
	assertPathSuffix(t, driverPath, watcher.watches[len(watcher.watches)-1]) // 检查最新的 watch
	assert.NoError(t, err)

	// 再次调用 Probe，应返回 0 个事件。

	// 执行
	events, err = prober.Probe()
	// 断言
	assert.Empty(t, events)
	assert.NoError(t, err)

	// 子目录新增非驱动文件后调用 Probe，应返回 1 个事件。
	fp := filepath.Join(driverPath, "dummyfile")
	fs.Create(fp)
	watcher.TriggerEvent(fsnotify.Create, fp)

	// 执行
	events, err = prober.Probe()

	// 断言
	assert.Len(t, events, 1)
	assert.Equal(t, volume.ProbeAddOrUpdate, events[0].Op)
	assert.NoError(t, err)

	// 再次调用 Probe，应返回 0 个事件。
	// 执行
	events, err = prober.Probe()
	// 断言
	assert.Empty(t, events)
	assert.NoError(t, err)

	// 驱动目录新增子目录后调用 Probe，应返回 1 个事件。
	subdirPath := filepath.Join(driverPath, "subdir")
	fs.Create(subdirPath)
	watcher.TriggerEvent(fsnotify.Create, subdirPath)

	// 执行
	events, err = prober.Probe()

	// 断言
	assert.Len(t, events, 1)
	assert.Equal(t, volume.ProbeAddOrUpdate, events[0].Op)
	assert.NoError(t, err)

	// 再次调用 Probe，应返回 0 个事件。
	// 执行
	events, err = prober.Probe()
	// 断言
	assert.Empty(t, events)
	assert.NoError(t, err)

	// 驱动目录移除子目录后调用 Probe，应返回 1 个事件。
	fs.Remove(subdirPath)
	watcher.TriggerEvent(fsnotify.Remove, subdirPath)

	// 执行
	events, err = prober.Probe()

	// 断言
	assert.Len(t, events, 1)
	assert.Equal(t, volume.ProbeAddOrUpdate, events[0].Op)
	assert.NoError(t, err)

	// 再次调用 Probe，应返回 0 个事件。
	// 执行
	events, err = prober.Probe()
	// 断言
	assert.Empty(t, events)
	assert.NoError(t, err)

	// 移除驱动可执行文件和目录后调用 Probe，应返回 1 个事件。
	fs.Remove(executablePath)
	fs.Remove(driverPath)
	watcher.TriggerEvent(fsnotify.Remove, executablePath)
	watcher.TriggerEvent(fsnotify.Remove, driverPath)
	// 执行与断言：1 个 ProbeRemove 事件
	events, err = prober.Probe()
	assert.Len(t, events, 1)
	assert.Equal(t, volume.ProbeRemove, events[0].Op)
	assert.NoError(t, err)

	// 执行与断言：0 个事件
	events, err = prober.Probe()
	assert.Empty(t, events)
	assert.NoError(t, err)
}

// 验证插件目录为空时的行为。
func TestEmptyPluginDir(t *testing.T) {
	// 准备
	fs := utilfs.NewTempFs()
	watcher := newFakeWatcher()
	prober := &flexVolumeProber{
		pluginDir: pluginDir,
		watcher:   watcher,
		fs:        fs,
		factory:   fakePluginFactory{},
	}
	prober.Init()

	// 执行
	events, err := prober.Probe()

	// 断言
	assert.Empty(t, events)
	assert.NoError(t, err)
}

// 触发移除插件目录的事件，新目录仍应被 watch。
func TestRemovePluginDir(t *testing.T) {
	// 准备
	driverPath, fs, watcher, _ := initTestEnvironment(t)
	err := fs.RemoveAll(pluginDir)
	assert.NoError(t, err)
	watcher.TriggerEvent(fsnotify.Remove, filepath.Join(driverPath, driverName))
	watcher.TriggerEvent(fsnotify.Remove, driverPath)
	watcher.TriggerEvent(fsnotify.Remove, pluginDir)

	// 执行：上述事件触发的 handler 应已做相应处理。

	// 断言
	assert.Len(t, watcher.watches, 3) // 2 from initial setup, 1 from new watch.
	plugDir := pluginDir
	if goruntime.GOOS == "windows" {
		plugDir = "\\flexvolume"
	}
	assertPathSuffix(t, plugDir, watcher.watches[len(watcher.watches)-1])
}

// 触发移除插件目录的事件，新目录仍应被 watch。
func TestNestedDriverDir(t *testing.T) {
	// 准备
	_, fs, watcher, _ := initTestEnvironment(t)
	// 断言
	assert.Len(t, watcher.watches, 2) // 2 from initial setup

	// 测试添加 testDriverName
	testDriverName := "testDriverName"
	testDriverPath := filepath.Join(pluginDir, testDriverName)
	fs.MkdirAll(testDriverPath, 0777)
	watcher.TriggerEvent(fsnotify.Create, testDriverPath)
	// 断言
	assert.Len(t, watcher.watches, 3) // 2 from initial setup, 1 from new watch.
	assertPathSuffix(t, testDriverPath, watcher.watches[len(watcher.watches)-1])

	// 测试在 testDriverName 内添加嵌套子目录
	basePath := testDriverPath
	for i := 0; i < 10; i++ {
		subdirName := "subdirName"
		subdirPath := filepath.Join(basePath, subdirName)
		fs.MkdirAll(subdirPath, 0777)
		watcher.TriggerEvent(fsnotify.Create, subdirPath)
		// 断言
		assert.Len(t, watcher.watches, 4+i) // 3 + newly added
		assertPathSuffix(t, subdirPath, watcher.watches[len(watcher.watches)-1])
		basePath = subdirPath
	}
}

// 触发多个事件并多次调用 Probe。
func TestProberMultipleEvents(t *testing.T) {
	const iterations = 5

	// 准备
	_, fs, watcher, prober := initTestEnvironment(t)
	for i := 0; i < iterations; i++ {
		newDriver := fmt.Sprintf("multi-event-driver%d", 1)
		installDriver(newDriver, fs)
		driverPath := filepath.Join(pluginDir, newDriver)
		watcher.TriggerEvent(fsnotify.Create, driverPath)
		watcher.TriggerEvent(fsnotify.Create, filepath.Join(driverPath, newDriver))
	}

	// 执行
	events, err := prober.Probe()

	// 断言
	assert.Len(t, events, 2)
	assert.Equal(t, volume.ProbeAddOrUpdate, events[0].Op)
	assert.Equal(t, volume.ProbeAddOrUpdate, events[1].Op)
	assert.NoError(t, err)
	for i := 0; i < iterations-1; i++ {
		events, err = prober.Probe()
		assert.Empty(t, events)
		assert.NoError(t, err)
	}
}

// TestProberError 验证驱动加载失败时 Probe 返回错误。
func TestProberError(t *testing.T) {
	fs := utilfs.NewTempFs()
	watcher := newFakeWatcher()
	prober := &flexVolumeProber{
		pluginDir: pluginDir,
		watcher:   watcher,
		fs:        fs,
		factory:   fakePluginFactory{errorDriver: driverName},
	}
	installDriver(driverName, fs)
	prober.Init()

	_, err := prober.Probe()
	assert.Error(t, err)
}

// TestProberSuccessAndError 验证部分驱动失败时仍返回成功驱动的事件。
func TestProberSuccessAndError(t *testing.T) {

	// 准备
	fs := utilfs.NewTempFs()
	watcher := newFakeWatcher()
	prober := &flexVolumeProber{
		pluginDir: pluginDir,
		watcher:   watcher,
		fs:        fs,
		factory:   fakePluginFactory{errorDriver: errorDriverName},
	}
	installDriver(driverName, fs)
	prober.Init()

	installDriver(errorDriverName, fs)
	driverPath := filepath.Join(pluginDir, errorDriverName)
	watcher.TriggerEvent(fsnotify.Create, filepath.Join(driverPath, errorDriverName))

	// 执行
	events, err := prober.Probe()

	// 断言
	assert.Len(t, events, 1)
	assert.Equal(t, volume.ProbeAddOrUpdate, events[0].Op)
	assert.Equal(t, driverName, events[0].PluginName)
	assert.Error(t, err)
}

// 在 mock 文件系统中安装一个模拟驱动（空文件）。
func installDriver(driverName string, fs utilfs.Filesystem) {
	driverPath := filepath.Join(pluginDir, driverName)
	fs.MkdirAll(driverPath, 0777)

	// 必须关闭文件，否则后续无法删除。
	f, _ := fs.Create(filepath.Join(driverPath, driverName))
	f.Close()
}

// 初始化 mock，安装一个驱动，然后初始化 prober。
func initTestEnvironment(t *testing.T) (
	driverPath string,
	fs utilfs.Filesystem,
	watcher *fakeWatcher,
	prober volume.DynamicPluginProber) {
	fs = utilfs.NewTempFs()
	watcher = newFakeWatcher()
	prober = &flexVolumeProber{
		pluginDir: pluginDir,
		watcher:   watcher,
		fs:        fs,
		factory:   fakePluginFactory{},
	}
	driverPath = filepath.Join(pluginDir, driverName)
	installDriver(driverName, fs)
	prober.Init()

	assert.NotNilf(t, watcher.eventHandler,
		"Expect watch event handler to be registered after prober init, but is not.")
	return
}

// 模拟的 Flexvolume 插件
// fakePluginFactory 是测试用的插件工厂，可指定某个驱动返回错误。
type fakePluginFactory struct {
	errorDriver string // 需要返回错误的驱动名
}

var _ PluginFactory = fakePluginFactory{}

// NewFlexVolumePlugin 返回测试用插件；errorDriver 指定的驱动会返回错误。
func (m fakePluginFactory) NewFlexVolumePlugin(_, driverName string, _ exec.Interface) (volume.VolumePlugin, error) {
	if driverName == m.errorDriver {
		return nil, fmt.Errorf("Flexvolume plugin error")
	}
	// 空壳 Flexvolume 插件；Prober 不会与之交互。
	return &flexVolumePlugin{driverName: driverName}, nil
}
