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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/utils/exec"
	utilstrings "k8s.io/utils/strings"
)

// flexVolumeProber 通过监听插件目录变更来发现 flexvolume 驱动。
type flexVolumeProber struct {
	mutex          sync.Mutex
	pluginDir      string         // Flexvolume 驱动所在目录
	runner         exec.Interface // 执行 flex 调用时使用的接口
	watcher        utilfs.FSWatcher
	factory        PluginFactory
	fs             utilfs.Filesystem
	ctx            context.Context
	probeAllNeeded bool
	eventsMap      map[string]volume.ProbeOperation // 键为驱动目录路径，值为对应的操作
}

// GetDynamicPluginProber 创建一个动态插件探测器。
func GetDynamicPluginProber(ctx context.Context, pluginDir string, runner exec.Interface) volume.DynamicPluginProber {
	return &flexVolumeProber{
		pluginDir: pluginDir,
		watcher:   utilfs.NewFsnotifyWatcher(),
		factory:   pluginFactory{},
		runner:    runner,
		fs:        &utilfs.DefaultFs{},
		ctx:       ctx,
	}
}

// Init 初始化探测器：全量探测一次，并创建插件目录、启动监听。
func (prober *flexVolumeProber) Init() error {
	prober.testAndSetProbeAllNeeded(true)
	prober.eventsMap = map[string]volume.ProbeOperation{}

	if err := prober.createPluginDir(); err != nil {
		return err
	}
	if err := prober.initWatcher(); err != nil {
		return err
	}

	return nil
}

// Probe 返回待处理的探测事件：probeAllNeeded 为真时全量扫描 pluginDir，否则处理 eventsMap 中的增量事件。
func (prober *flexVolumeProber) Probe() (events []volume.ProbeEvent, err error) {
	if prober.probeAllNeeded {
		prober.testAndSetProbeAllNeeded(false)
		return prober.probeAll()
	}

	return prober.probeMap()
}

// probeMap 处理 eventsMap 中累积的增量事件，处理后即删除对应条目。
func (prober *flexVolumeProber) probeMap() (events []volume.ProbeEvent, err error) {
	// TODO 改用并发 map，避免锁住整张表
	prober.mutex.Lock()
	defer prober.mutex.Unlock()
	probeEvents := []volume.ProbeEvent{}
	allErrs := []error{}
	for driverDirPathAbs, op := range prober.eventsMap {
		driverDirName := filepath.Base(driverDirPathAbs) // 例如 driverDirName = vendor~cifs
		probeEvent, pluginErr := prober.newProbeEvent(driverDirName, op)
		if pluginErr != nil {
			allErrs = append(allErrs, pluginErr)
			continue
		}
		probeEvents = append(probeEvents, probeEvent)

		delete(prober.eventsMap, driverDirPathAbs)
	}
	return probeEvents, errors.NewAggregate(allErrs)
}

// probeAll 扫描插件目录，为每个驱动子目录生成添加/更新事件。
func (prober *flexVolumeProber) probeAll() (events []volume.ProbeEvent, err error) {
	probeEvents := []volume.ProbeEvent{}
	allErrs := []error{}
	files, err := prober.fs.ReadDir(prober.pluginDir)
	if err != nil {
		return nil, fmt.Errorf("error reading the Flexvolume directory: %s", err)
	}
	for _, f := range files {
		// 只有名字不以 '.' 开头的目录才算插件；
		// 通常 pluginDir/dirname/dirname 应为可执行文件，
		// 但若目录名含 '~' 用于转义命名空间（如 vendor~cifs），
		// 则可执行文件是 pluginDir/dirname/cifs。
		if f.IsDir() && filepath.Base(f.Name())[0] != '.' {
			probeEvent, pluginErr := prober.newProbeEvent(f.Name(), volume.ProbeAddOrUpdate)
			if pluginErr != nil {
				allErrs = append(allErrs, pluginErr)
				continue
			}
			probeEvents = append(probeEvents, probeEvent)
		}
	}
	return probeEvents, errors.NewAggregate(allErrs)
}

// newProbeEvent 根据驱动目录名和操作构造探测事件；删除事件只需插件名，无需重新加载驱动。
func (prober *flexVolumeProber) newProbeEvent(driverDirName string, op volume.ProbeOperation) (volume.ProbeEvent, error) {
	probeEvent := volume.ProbeEvent{
		Op: op,
	}
	if op == volume.ProbeAddOrUpdate {
		plugin, pluginErr := prober.factory.NewFlexVolumePlugin(prober.pluginDir, driverDirName, prober.runner)
		if pluginErr != nil {
			pluginErr = fmt.Errorf(
				"error creating Flexvolume plugin from directory %s, skipping. Error: %s",
				driverDirName, pluginErr)
			return probeEvent, pluginErr
		}
		probeEvent.Plugin = plugin
		probeEvent.PluginName = plugin.GetPluginName()
	} else if op == volume.ProbeRemove {
		driverName := utilstrings.UnescapeQualifiedName(driverDirName)
		probeEvent.PluginName = driverName

	} else {
		return probeEvent, fmt.Errorf("Unknown Operation on directory: %s. ", driverDirName)
	}
	return probeEvent, nil
}

// handleWatchEvent 把文件系统事件转换为插件的添加/更新/删除事件。
func (prober *flexVolumeProber) handleWatchEvent(event fsnotify.Event) error {
	// event.Name 是被监听的路径。
	if filepath.Base(event.Name)[0] == '.' {
		// 忽略以 '.' 开头的文件
		return nil
	}

	eventPathAbs, err := filepath.Abs(event.Name)
	if err != nil {
		return err
	}
	parentPathAbs := filepath.Dir(eventPathAbs)
	pluginDirAbs, err := filepath.Abs(prober.pluginDir)
	if err != nil {
		return err
	}

	// 插件目录自身的事件
	if eventPathAbs == pluginDirAbs {
		// 插件目录被删除后需重建，否则监听随之失效。
		if event.Has(fsnotify.Remove) {
			if err := prober.createPluginDir(); err != nil {
				return err
			}
			if err := prober.addWatchRecursive(pluginDirAbs); err != nil {
				return err
			}
		}
		return nil
	}

	// 监听驱动目录下新增的子目录
	if event.Has(fsnotify.Create) {
		if err := prober.addWatchRecursive(eventPathAbs); err != nil {
			return err
		}
	}

	eventRelPathToPluginDir, err := filepath.Rel(pluginDirAbs, eventPathAbs)
	if err != nil {
		return err
	}

	// 具体驱动目录内的事件
	if len(eventRelPathToPluginDir) > 0 {
		driverDirName := strings.Split(eventRelPathToPluginDir, string(os.PathSeparator))[0]
		driverDirAbs := filepath.Join(pluginDirAbs, driverDirName)
		// 可执行文件被删除时触发 ProbeRemove 事件
		if event.Has(fsnotify.Remove) && (eventRelPathToPluginDir == getExecutablePathRel(driverDirName) || parentPathAbs == pluginDirAbs) {
			prober.updateEventsMap(driverDirAbs, volume.ProbeRemove)
		} else {
			prober.updateEventsMap(driverDirAbs, volume.ProbeAddOrUpdate)
		}
	}

	return nil
}

// getExecutablePathRel 返回 flex 插件可执行文件相对于插件目录的路径。
func getExecutablePathRel(driverDirName string) string {
	parts := strings.Split(driverDirName, "~")
	return filepath.Join(driverDirName, parts[len(parts)-1])
}

// updateEventsMap 记录某驱动目录待处理的操作；若即将全量探测则无需记录。
func (prober *flexVolumeProber) updateEventsMap(eventDirAbs string, op volume.ProbeOperation) {
	prober.mutex.Lock()
	defer prober.mutex.Unlock()
	if prober.probeAllNeeded {
		return
	}
	prober.eventsMap[eventDirAbs] = op
}

// addWatchRecursive 递归监听指定路径及其下所有目录。
// 若路径是目录的符号链接，只监听该链接本身，不监听其子目录。
//
// 每个文件或目录变更会触发两个事件：来自自身监听的一个，以及来自父目录监听的一个。
func (prober *flexVolumeProber) addWatchRecursive(filename string) error {
	// Windows 上可能传入带 C:\ 前缀的绝对路径；但下方 fs.Walk 会将其与 fs.root 拼接，
	// 而 C:\path 与 C:\another\path 无法正确拼接，因此先去掉前缀。
	filename = strings.TrimPrefix(filename, `C:\`)
	addWatch := func(path string, info os.FileInfo, err error) error {
		if err == nil && info.IsDir() {
			if err := prober.watcher.AddWatch(path); err != nil {
				klog.Errorf("Error recursively adding watch: %v", err)
			}
		}
		return nil
	}
	return prober.fs.Walk(filename, addWatch)
}

// initWatcher 创建文件系统监听器，并监听插件目录及其所有子目录。
func (prober *flexVolumeProber) initWatcher() error {
	err := prober.watcher.Init(func(event fsnotify.Event) {
		if err := prober.handleWatchEvent(event); err != nil {
			klog.Errorf("Flexvolume prober watch: %s", err)
		}
	}, func(err error) {
		klog.Errorf("Received an error from watcher: %s", err)
	})
	if err != nil {
		return fmt.Errorf("flex volume: %w", err)
	}

	if err := prober.addWatchRecursive(prober.pluginDir); err != nil {
		return fmt.Errorf("error adding watch on Flexvolume directory: %s", err)
	}

	prober.watcher.Run(prober.ctx)

	return nil
}

// createPluginDir 在插件目录不存在时创建它。
func (prober *flexVolumeProber) createPluginDir() error {
	if _, err := prober.fs.Stat(prober.pluginDir); os.IsNotExist(err) {
		klog.Warningf("Flexvolume plugin directory at %s does not exist. Recreating.", prober.pluginDir)
		err := prober.fs.MkdirAll(prober.pluginDir, 0755)
		if err != nil {
			return fmt.Errorf("error (re-)creating driver directory: %s", err)
		}
	}

	return nil
}

// testAndSetProbeAllNeeded 原子地更新 probeAllNeeded，并返回旧值。
func (prober *flexVolumeProber) testAndSetProbeAllNeeded(newval bool) (oldval bool) {
	prober.mutex.Lock()
	defer prober.mutex.Unlock()
	oldval, prober.probeAllNeeded = prober.probeAllNeeded, newval
	return
}
