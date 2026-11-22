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

// Package flexvolume 测试 flexVolume 插件的卸载设备与分离逻辑。
package flexvolume

import (
	"testing"

	"k8s.io/kubernetes/test/utils/harness"
)

// TestDetach 验证 Detach 向驱动下发 detach 命令及设备路径和节点名。
// flexVolume 的分离操作由外部驱动实现，此处仅断言命令参数正确。
func TestDetach(tt *testing.T) {
	t := harness.For(tt)
	defer t.Close()

	plugin, _ := testPlugin(t)
	plugin.runner = fakeRunner(
		assertDriverCall(t, notSupportedOutput(), detachCmd,
			"sdx", "localhost"),
	)

	d, _ := plugin.NewDetacher()
	d.Detach("sdx", "localhost")
}

// TestUnmountDevice 验证 UnmountDevice 向驱动下发 unmountDevice 命令及挂载目录路径。
func TestUnmountDevice(tt *testing.T) {
	t := harness.For(tt)
	defer t.Close()

	plugin, rootDir := testPlugin(t)
	plugin.runner = fakeRunner(
		assertDriverCall(t, notSupportedOutput(), unmountDeviceCmd,
			rootDir+"/mount-dir"),
	)

	d, _ := plugin.NewDetacher()
	d.UnmountDevice(rootDir + "/mount-dir")
}
