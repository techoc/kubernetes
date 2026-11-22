/*
Copyright 2015 The Kubernetes Authors.

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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"text/template"

	"k8s.io/api/core/v1"
	utiltesting "k8s.io/client-go/util/testing"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/utils/exec"
)

const execScriptTempl1 = `#!/usr/bin/env bash
if [ "$1" == "init" -a $# -eq 1 ]; then
  echo -n '{
    "status": "Success"
  }'
  exit 0
fi

PATH=$2
if [ "$1" == "attach" -a $# -eq 2 ]; then
  echo -n '{
    "device": "{{.DevicePath}}",
    "status": "Success"
  }'
  exit 0
elif [ "$1" == "detach" -a $# -eq 2 ]; then
  echo -n '{
    "status": "Success"
  }'
  exit 0
elif [ "$1" == "getvolumename" -a $# -eq 4 ]; then
  echo -n '{
    "status": "Success",
    "volume": "fakevolume"
  }'
  exit 0
elif [ "$1" == "isattached" -a $# -eq 2 ]; then
  echo -n '{
    "status": "Success",
    "attached": true
  }'
  exit 0
fi

echo -n '{
  "status": "Not supported"
}'
exit 1

# Direct the arguments to a file to be tested against later
echo -n $@ &> {{.OutputFile}}
`

// 注意：Windows 下可执行文件通常需要扩展名。若文件名无扩展名，
// Windows 会按顺序尝试 $env:PATHEXT 中各扩展名匹配文件并执行。
// 例如存在 C:\\foo.bat 时，可直接运行 C:\\foo。
// 此处选择 .bat 是因为它默认就在 $env:PATHEXT 中；.ps1 不在列表内，
// 但 flexVolume 插件仍可能需要支持 PowerShell 脚本。
// 批处理没有参数个数变量，改为检查第 n 个参数是否为空字符串。
const execScriptTemplBat = `
@echo off

if "%1"=="init" if "%2"=="" (
    echo {"status": "Success"}
    exit 0
)
if "%1"=="attach" if "%3"=="" (
    echo {"device": "{{.DevicePath}}", "status": "Success"}
    exit 0
)

if "%1"=="detach" if "%3"=="" (
    echo {"status": "Success"}
    exit 0
)

if "%1"=="getvolumename" if "%5"=="" (
    echo {"status": "Success", "volume": "fakevolume"}
    exit 0
)

if "%1"=="isattached" if "%3"=="" (
    echo {"status": "Success", "attached": true}
    exit 0
)

echo {"status": "Not supported"}
exit 1
`

// installPluginUnderTest 在临时目录下创建可执行的假驱动脚本。
// 带 vendor 名时目录格式为 vendor~plugName，以模拟插件的供应商前缀命名。
func installPluginUnderTest(t *testing.T, vendorName, plugName, tmpDir string, execScriptTempl string, execTemplateData *map[string]interface{}) {
	vendoredName := plugName
	if vendorName != "" {
		vendoredName = fmt.Sprintf("%s~%s", vendorName, plugName)
	}
	pluginDir := filepath.Join(tmpDir, vendoredName)
	err := os.MkdirAll(pluginDir, 0777)
	if err != nil {
		t.Errorf("Failed to create plugin: %v", err)
	}
	pluginExec := filepath.Join(pluginDir, plugName)
	if goruntime.GOOS == "windows" {
		pluginExec = pluginExec + ".bat"
	}
	f, err := os.Create(pluginExec)
	if err != nil {
		t.Errorf("Failed to install plugin")
	}
	err = f.Chmod(0777)
	if err != nil {
		t.Errorf("Failed to set exec perms on plugin")
	}
	if execTemplateData == nil {
		execTemplateData = &map[string]interface{}{
			"DevicePath": "/dev/sdx",
			"OutputFile": filepath.Join(pluginDir, plugName+".out"),
		}
	}

	tObj := template.Must(template.New("test").Parse(execScriptTempl))
	buf := &bytes.Buffer{}
	if err := tObj.Execute(buf, *execTemplateData); err != nil {
		t.Errorf("Error in executing script template - %v", err)
	}
	execScript := buf.String()
	_, err = f.WriteString(execScript)
	if err != nil {
		t.Errorf("Failed to write plugin exec")
	}
	f.Close()
}

// TestCanSupport 验证插件发现与 CanSupport 判断：
// 仅识别驱动名匹配的 flexVolume 卷和 PV，空卷源返回 false。
func TestCanSupport(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("flexvolume_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugMgr := volume.VolumePluginMgr{}
	runner := exec.New()
	execScriptTempl := execScriptTempl1
	if goruntime.GOOS == "windows" {
		execScriptTempl = execScriptTemplBat
	}
	installPluginUnderTest(t, "kubernetes.io", "fakeAttacher", tmpDir, execScriptTempl, nil)
	if err := plugMgr.InitPlugins(nil, GetDynamicPluginProberWithoutWatcher(tmpDir, runner), volumetest.NewFakeVolumeHost(t, "fake", nil, nil)); err != nil {
		t.Fatalf("Could not initialize plugins: %v", err)
	}
	plugin, err := plugMgr.FindPluginByName("kubernetes.io/fakeAttacher")
	if err != nil {
		t.Fatalf("Can't find the plugin by name")
	}
	if plugin.GetPluginName() != "kubernetes.io/fakeAttacher" {
		t.Errorf("Wrong name: %s", plugin.GetPluginName())
	}
	if !plugin.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{FlexVolume: &v1.FlexVolumeSource{Driver: "kubernetes.io/fakeAttacher"}}}}) {
		t.Errorf("Expected true")
	}
	if !plugin.CanSupport(&volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{FlexVolume: &v1.FlexPersistentVolumeSource{Driver: "kubernetes.io/fakeAttacher"}}}}}) {
		t.Errorf("Expected true")
	}
	if plugin.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{}}}) {
		t.Errorf("Expected false")
	}
}

// TestGetAccessModes 验证 flexVolume 持久插件声明 RWO 和 ROX 两种访问模式。
func TestGetAccessModes(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("flexvolume_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugMgr := volume.VolumePluginMgr{}
	runner := exec.New()
	execScriptTempl := execScriptTempl1
	if goruntime.GOOS == "windows" {
		execScriptTempl = execScriptTemplBat
	}
	installPluginUnderTest(t, "kubernetes.io", "fakeAttacher", tmpDir, execScriptTempl, nil)
	if err := plugMgr.InitPlugins(nil, GetDynamicPluginProberWithoutWatcher(tmpDir, runner), volumetest.NewFakeVolumeHost(t, tmpDir, nil, nil)); err != nil {
		t.Fatalf("Could not initialize plugins: %v", err)
	}
	plugin, err := plugMgr.FindPersistentPluginByName("kubernetes.io/fakeAttacher")
	if err != nil {
		t.Fatalf("Can't find the plugin by name")
	}
	if !volumetest.ContainsAccessMode(plugin.GetAccessModes(), v1.ReadWriteOnce) || !volumetest.ContainsAccessMode(plugin.GetAccessModes(), v1.ReadOnlyMany) {
		t.Errorf("Expected two AccessModeTypes:  %s and %s", v1.ReadWriteOnce, v1.ReadOnlyMany)
	}
}

// GetDynamicPluginProberWithoutWatcher 构造一个使用假 watcher 的插件探测器，
// 使测试不依赖真实文件系统事件。
func GetDynamicPluginProberWithoutWatcher(pluginDir string, runner exec.Interface) volume.DynamicPluginProber {
	return &flexVolumeProber{
		pluginDir: pluginDir,
		watcher:   newFakeWatcher(),
		factory:   pluginFactory{},
		runner:    runner,
		fs:        &utilfs.DefaultFs{},
	}
}
