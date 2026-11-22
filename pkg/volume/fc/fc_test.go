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

package fc

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"k8s.io/mount-utils"
	testingexec "k8s.io/utils/exec/testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	utiltesting "k8s.io/client-go/util/testing"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
)

// TestCanSupport 验证 FC 插件只识别 FC 类型的卷。
func TestCanSupport(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("fc_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, volumetest.NewFakeVolumeHost(t, tmpDir, nil, nil))

	plug, err := plugMgr.FindPluginByName("kubernetes.io/fc")
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}
	if plug.GetPluginName() != "kubernetes.io/fc" {
		t.Errorf("Wrong name: %s", plug.GetPluginName())
	}
	if plug.CanSupport(&volume.Spec{}) {
		t.Errorf("Expected false")
	}
	if plug.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{}}}) {
		t.Errorf("Expected false")
	}
	if !plug.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{FC: &v1.FCVolumeSource{}}}}) {
		t.Errorf("Expected true")
	}
	if plug.CanSupport(&volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{}}}) {
		t.Errorf("Expected false")
	}
	if plug.CanSupport(&volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{}}}}) {
		t.Errorf("Expected false")
	}
	if !plug.CanSupport(&volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{FC: &v1.FCVolumeSource{}}}}}) {
		t.Errorf("Expected true")
	}
}

// TestGetAccessModes 验证 FC 插件声明了预期的访问模式。
func TestGetAccessModes(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("fc_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, volumetest.NewFakeVolumeHost(t, tmpDir, nil, nil))

	plug, err := plugMgr.FindPersistentPluginByName("kubernetes.io/fc")
	if err != nil {
		t.Errorf("Can't find the plugin by name")
	}
	if !volumetest.ContainsAccessMode(plug.GetAccessModes(), v1.ReadWriteOnce) || !volumetest.ContainsAccessMode(plug.GetAccessModes(), v1.ReadOnlyMany) {
		t.Errorf("Expected two AccessModeTypes:  %s and %s", v1.ReadWriteOnce, v1.ReadOnlyMany)
	}
}

// fakeDiskManager 是测试中替代真实磁盘管理器的假实现。
type fakeDiskManager struct {
	tmpDir       string
	attachCalled bool
	detachCalled bool
}

// newFakeDiskManager 创建带有临时目录的假磁盘管理器。
func newFakeDiskManager() *fakeDiskManager {
	return &fakeDiskManager{
		tmpDir: utiltesting.MkTmpdirOrDie("fc_test"),
	}
}

// Cleanup 删除假管理器使用的临时目录。
func (fake *fakeDiskManager) Cleanup() {
	os.RemoveAll(fake.tmpDir)
}

// MakeGlobalPDName 返回临时目录作为全局文件卷路径。
func (fake *fakeDiskManager) MakeGlobalPDName(disk fcDisk) string {
	return fake.tmpDir
}

// MakeGlobalVDPDName 返回临时目录作为全局块设备卷路径。
func (fake *fakeDiskManager) MakeGlobalVDPDName(disk fcDisk) string {
	return fake.tmpDir
}

// AttachDisk 创建全局路径并记录附加调用。
func (fake *fakeDiskManager) AttachDisk(b fcDiskMounter) (string, error) {
	globalPath := b.manager.MakeGlobalPDName(*b.fcDisk)
	err := os.MkdirAll(globalPath, 0750)
	if err != nil {
		return "", err
	}
	fake.attachCalled = true
	return "", nil
}

// DetachDisk 删除全局路径并记录分离调用。
func (fake *fakeDiskManager) DetachDisk(c fcDiskUnmounter, mntPath string) error {
	globalPath := c.manager.MakeGlobalPDName(*c.fcDisk)
	err := os.RemoveAll(globalPath)
	if err != nil {
		return err
	}
	fake.detachCalled = true
	return nil
}

// DetachBlockFCDisk 删除块设备卷映射路径并记录分离调用。
func (fake *fakeDiskManager) DetachBlockFCDisk(c fcDiskUnmapper, mapPath, devicePath string) error {
	err := os.RemoveAll(mapPath)
	if err != nil {
		return err
	}
	fake.detachCalled = true
	return nil
}

// doTestPlugin 验证 FC 卷的挂载与卸载流程。
func doTestPlugin(t *testing.T, spec *volume.Spec) {
	tmpDir, err := utiltesting.MkTmpdir("fc_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, volumetest.NewFakeVolumeHost(t, tmpDir, nil, nil))

	plug, err := plugMgr.FindPluginByName("kubernetes.io/fc")
	if err != nil {
		t.Errorf("Can't find the plugin by name")
	}
	fakeManager := newFakeDiskManager()
	defer fakeManager.Cleanup()
	fakeMounter := mount.NewFakeMounter(nil)
	fakeExec := &testingexec.FakeExec{}
	mounter, err := plug.(*fcPlugin).newMounterInternal(spec, types.UID("poduid"), fakeManager, fakeMounter, fakeExec)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Errorf("Got a nil Mounter: %v", err)
	}

	path := mounter.GetPath()
	expectedPath := filepath.Join(tmpDir, "pods/poduid/volumes/kubernetes.io~fc/vol1")
	if path != expectedPath {
		t.Errorf("Unexpected path, expected %q, got: %q", expectedPath, path)
	}

	if err := mounter.SetUp(volume.MounterArgs{}); err != nil {
		t.Errorf("Expected success, got: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, volume path not created: %s", path)
		} else {
			t.Errorf("SetUp() failed: %v", err)
		}
	}

	fakeManager2 := newFakeDiskManager()
	defer fakeManager2.Cleanup()
	unmounter, err := plug.(*fcPlugin).newUnmounterInternal("vol1", types.UID("poduid"), fakeManager2, fakeMounter, fakeExec)
	if err != nil {
		t.Errorf("Failed to make a new Unmounter: %v", err)
	}
	if unmounter == nil {
		t.Errorf("Got a nil Unmounter: %v", err)
	}

	if err := unmounter.TearDown(); err != nil {
		t.Errorf("Expected success, got: %v", err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Errorf("TearDown() failed, volume path still exists: %s", path)
	} else if !os.IsNotExist(err) {
		t.Errorf("TearDown() failed: %v", err)
	}
}

// doTestPluginNilMounter 验证磁盘信息缺失时创建挂载器会失败。
func doTestPluginNilMounter(t *testing.T, spec *volume.Spec) {
	tmpDir, err := utiltesting.MkTmpdir("fc_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, volumetest.NewFakeVolumeHost(t, tmpDir, nil, nil))

	plug, err := plugMgr.FindPluginByName("kubernetes.io/fc")
	if err != nil {
		t.Errorf("Can't find the plugin by name")
	}
	fakeManager := newFakeDiskManager()
	defer fakeManager.Cleanup()
	fakeMounter := mount.NewFakeMounter(nil)
	fakeExec := &testingexec.FakeExec{}
	mounter, err := plug.(*fcPlugin).newMounterInternal(spec, types.UID("poduid"), fakeManager, fakeMounter, fakeExec)
	if err == nil {
		t.Errorf("Error failed to make a new Mounter is expected: %v", err)
	}
	if mounter != nil {
		t.Errorf("A nil Mounter is expected: %v", err)
	}
}

// TestPluginVolume 验证内联 FC 卷的挂载流程。
func TestPluginVolume(t *testing.T) {
	lun := int32(0)
	vol := &v1.Volume{
		Name: "vol1",
		VolumeSource: v1.VolumeSource{
			FC: &v1.FCVolumeSource{
				TargetWWNs: []string{"500a0981891b8dc5"},
				FSType:     "ext4",
				Lun:        &lun,
			},
		},
	}
	doTestPlugin(t, volume.NewSpecFromVolume(vol))
}

// TestPluginPersistentVolume 验证 FC 持久卷的挂载流程。
func TestPluginPersistentVolume(t *testing.T) {
	lun := int32(0)
	fs := v1.PersistentVolumeFilesystem
	vol := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol1",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FC: &v1.FCVolumeSource{
					TargetWWNs: []string{"500a0981891b8dc5"},
					FSType:     "ext4",
					Lun:        &lun,
				},
			},
			VolumeMode: &fs,
		},
	}
	doTestPlugin(t, volume.NewSpecFromPersistentVolume(vol, false))
}

// TestPluginVolumeWWIDs 验证基于 WWID 的内联 FC 卷挂载流程。
func TestPluginVolumeWWIDs(t *testing.T) {
	vol := &v1.Volume{
		Name: "vol1",
		VolumeSource: v1.VolumeSource{
			FC: &v1.FCVolumeSource{
				WWIDs:  []string{"3600508b400105e210000900000490000"},
				FSType: "ext4",
			},
		},
	}
	doTestPlugin(t, volume.NewSpecFromVolume(vol))
}

// TestPluginPersistentVolumeWWIDs 验证基于 WWID 的 FC 持久卷挂载流程。
func TestPluginPersistentVolumeWWIDs(t *testing.T) {
	fs := v1.PersistentVolumeFilesystem
	vol := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol1",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FC: &v1.FCVolumeSource{
					WWIDs:  []string{"3600508b400105e21 000900000490000"},
					FSType: "ext4",
				},
			},
			VolumeMode: &fs,
		},
	}
	doTestPlugin(t, volume.NewSpecFromPersistentVolume(vol, false))
}

// TestPluginVolumeNoDiskInfo 验证缺少磁盘信息的内联 FC 卷无法创建挂载器。
func TestPluginVolumeNoDiskInfo(t *testing.T) {
	vol := &v1.Volume{
		Name: "vol1",
		VolumeSource: v1.VolumeSource{
			FC: &v1.FCVolumeSource{
				FSType: "ext4",
			},
		},
	}
	doTestPluginNilMounter(t, volume.NewSpecFromVolume(vol))
}

// TestPluginPersistentVolumeNoDiskInfo 验证缺少磁盘信息的 FC 持久卷无法创建挂载器。
func TestPluginPersistentVolumeNoDiskInfo(t *testing.T) {
	fs := v1.PersistentVolumeFilesystem
	vol := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol1",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FC: &v1.FCVolumeSource{
					FSType: "ext4",
				},
			},
			VolumeMode: &fs,
		},
	}
	doTestPluginNilMounter(t, volume.NewSpecFromPersistentVolume(vol, false))
}

// TestPersistentClaimReadOnlyFlag 验证持久卷声明的只读标志会传递给挂载器。
func TestPersistentClaimReadOnlyFlag(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("fc_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	lun := int32(0)
	fs := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvA",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FC: &v1.FCVolumeSource{
					TargetWWNs: []string{"some_wwn"},
					FSType:     "ext4",
					Lun:        &lun,
				},
			},
			ClaimRef: &v1.ObjectReference{
				Name: "claimA",
			},
			VolumeMode: &fs,
		},
	}

	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claimA",
			Namespace: "nsA",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "pvA",
			VolumeMode: &fs,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	client := fake.NewSimpleClientset(pv, claim)

	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, volumetest.NewFakeVolumeHost(t, tmpDir, client, nil))
	plug, _ := plugMgr.FindPluginByName(fcPluginName)

	// 只读标志由持久卷声明卷源在创建挂载器时提供
	spec := volume.NewSpecFromPersistentVolume(pv, true)
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID("poduid")}}
	mounter, _ := plug.NewMounter(spec, pod)
	if mounter == nil {
		t.Fatalf("Got a nil Mounter")
	}

	if !mounter.GetAttributes().ReadOnly {
		t.Errorf("Expected true for mounter.IsReadOnly")
	}
}

// Test_getWwnsLun 验证从 FC 卷源中解析 WWN 与 lun。
func Test_getWwnsLun(t *testing.T) {
	num := int32(0)
	fc := &v1.FCVolumeSource{
		TargetWWNs: []string{"500a0981891b8dc5"},
		FSType:     "ext4",
		Lun:        &num,
	}
	wwn, lun, _, err := getWwnsLunWwids(fc)
	// 未解析出 WWN 和 lun 时报错
	if (len(wwn) == 0 && lun != "0") || err != nil {
		t.Errorf("no fc disk found")
	}
}

// Test_getWwids 验证从 FC 卷源中解析 WWID。
func Test_getWwids(t *testing.T) {
	fc := &v1.FCVolumeSource{
		FSType: "ext4",
		WWIDs:  []string{"3600508b400105e210000900000490000"},
	}
	_, _, wwid, err := getWwnsLunWwids(fc)
	// 未解析出 WWN 和 lun 时报错
	if len(wwid) == 0 || err != nil {
		t.Errorf("no fc disk found")
	}
}

// Test_getWwnsLunWwidsError 验证没有任何磁盘信息时会报错。
func Test_getWwnsLunWwidsError(t *testing.T) {
	fc := &v1.FCVolumeSource{
		FSType: "ext4",
	}
	wwn, lun, wwid, err := getWwnsLunWwids(fc)
	// 预期解析不出 WWN、lun 和 WWID
	if (len(wwn) != 0 && lun != "" && len(wwid) != 0) || err == nil {
		t.Errorf("unexpected fc disk found")
	}
}

// Test_ConstructVolumeSpec 验证能从挂载引用中还原 WWN+lun 或 WWID 信息。
func Test_ConstructVolumeSpec(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skipf("Test_ConstructVolumeSpec is not supported on GOOS=%s", runtime.GOOS)
	}
	fm := mount.NewFakeMounter(
		[]mount.MountPoint{
			{Device: "/dev/sdb", Path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod1"},
			{Device: "/dev/sdb", Path: "/var/lib/kubelet/plugins/kubernetes.io/fc/50060e801049cfd1-lun-0"},
			{Device: "/dev/sdc", Path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod2"},
			{Device: "/dev/sdc", Path: "/var/lib/kubelet/plugins/kubernetes.io/fc/volumeDevices/3600508b400105e210000900000490000"},
		})
	mountPaths := []string{
		"/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod1",
		"/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod2",
	}
	for _, path := range mountPaths {
		refs, err := fm.GetMountRefs(path)
		if err != nil {
			t.Errorf("couldn't get mountrefs. err: %v", err)
		}
		var globalPDPath string
		for _, ref := range refs {
			if strings.Contains(ref, "kubernetes.io/fc") {
				globalPDPath = ref
				break
			}
		}
		if len(globalPDPath) == 0 {
			t.Errorf("couldn't fetch mountrefs")
		}
		arr := strings.Split(globalPDPath, "/")
		if len(arr) < 1 {
			t.Errorf("failed to retrieve volume plugin information from globalPDPath: %v", globalPDPath)
		}
		volumeInfo := arr[len(arr)-1]
		if strings.Contains(volumeInfo, "-lun-") {
			wwnLun := strings.Split(volumeInfo, "-lun-")
			if len(wwnLun) < 2 {
				t.Errorf("failed to retrieve TargetWWN and Lun. volumeInfo is invalid: %v", volumeInfo)
			}
			lun, _ := strconv.Atoi(wwnLun[1])
			lun32 := int32(lun)
			if wwnLun[0] != "50060e801049cfd1" || lun32 != 0 {
				t.Errorf("failed to retrieve TargetWWN and Lun")
			}
		} else {
			if volumeInfo != "3600508b400105e210000900000490000" {
				t.Errorf("failed to retrieve WWIDs")
			}
		}
	}
}

// Test_ConstructVolumeSpecNoRefs 验证没有挂载引用时找不到全局卷路径。
func Test_ConstructVolumeSpecNoRefs(t *testing.T) {
	fm := mount.NewFakeMounter(
		[]mount.MountPoint{
			{Device: "/dev/sdd", Path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod1"},
		})
	mountPaths := []string{
		"/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod1",
	}
	for _, path := range mountPaths {
		refs, _ := fm.GetMountRefs(path)
		var globalPDPath string
		for _, ref := range refs {
			if strings.Contains(ref, "kubernetes.io/fc") {
				globalPDPath = ref
				break
			}
		}
		if len(globalPDPath) != 0 {
			t.Errorf("invalid globalPDPath")
		}
	}
}
