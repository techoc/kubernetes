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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/test/utils/harness"
)

// TestAttach 验证附加器调用驱动的 attach 命令。
func TestAttach(tt *testing.T) {
	t := harness.For(tt)
	defer t.Close()

	spec := fakeVolumeSpec()

	plugin, _ := testPlugin(t)
	plugin.runner = fakeRunner(
		assertDriverCall(t, notSupportedOutput(), attachCmd,
			specJSON(plugin, spec, nil), "localhost"),
	)

	a, _ := plugin.NewAttacher()
	a.Attach(spec, "localhost")
}

// TestWaitForAttach 验证附加器调用驱动的 wait-for-attach 命令。
func TestWaitForAttach(tt *testing.T) {
	t := harness.For(tt)
	defer t.Close()

	spec := fakeVolumeSpec()
	var pod *v1.Pod
	plugin, _ := testPlugin(t)
	plugin.runner = fakeRunner(
		assertDriverCall(t, notSupportedOutput(), waitForAttachCmd, "/dev/sdx",
			specJSON(plugin, spec, nil)),
	)

	a, _ := plugin.NewAttacher()
	a.WaitForAttach(spec, "/dev/sdx", pod, 1*time.Second)
}

// TestMountDevice 验证附加器调用驱动的 mount-device 命令。
func TestMountDevice(tt *testing.T) {
	t := harness.For(tt)
	defer t.Close()

	spec := fakeVolumeSpec()

	plugin, rootDir := testPlugin(t)
	plugin.runner = fakeRunner(
		assertDriverCall(t, notSupportedOutput(), mountDeviceCmd, rootDir+"/mount-dir", "/dev/sdx",
			specJSON(plugin, spec, nil)),
	)

	a, _ := plugin.NewAttacher()
	a.MountDevice(spec, "/dev/sdx", rootDir+"/mount-dir", volume.DeviceMounterArgs{})
}

// TestIsVolumeAttached 验证附加器通过驱动的 is-attached 命令批量查询卷挂载状态。
func TestIsVolumeAttached(tt *testing.T) {
	t := harness.For(tt)
	defer t.Close()

	spec := fakeVolumeSpec()

	plugin, _ := testPlugin(t)
	plugin.runner = fakeRunner(
		assertDriverCall(t, notSupportedOutput(), isAttached, specJSON(plugin, spec, nil), "localhost"),
	)
	a, _ := plugin.NewAttacher()
	specs := []*volume.Spec{spec}
	a.VolumesAreAttached(specs, "localhost")
}
