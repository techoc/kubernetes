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

package iscsi

import (
	"fmt"
	"os"
	"path/filepath"
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

// TestCanSupport 验证 iSCSI 插件仅识别 iSCSI 类型的内联卷和持久卷，
// 空规格或非 iSCSI 规格均应返回 false。
func TestCanSupport(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("iscsi_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, volumetest.NewFakeVolumeHost(t, tmpDir, nil, nil))

	plug, err := plugMgr.FindPluginByName("kubernetes.io/iscsi")
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}
	if plug.GetPluginName() != "kubernetes.io/iscsi" {
		t.Errorf("Wrong name: %s", plug.GetPluginName())
	}
	if plug.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{}}}) {
		t.Errorf("Expected false")
	}
	if plug.CanSupport(&volume.Spec{}) {
		t.Errorf("Expected false")
	}
	if !plug.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{ISCSI: &v1.ISCSIVolumeSource{}}}}) {
		t.Errorf("Expected true")
	}
	if plug.CanSupport(&volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{}}}) {
		t.Errorf("Expected false")
	}
	if plug.CanSupport(&volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{}}}}) {
		t.Errorf("Expected false")
	}
	if !plug.CanSupport(&volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{ISCSI: &v1.ISCSIPersistentVolumeSource{}}}}}) {
		t.Errorf("Expected true")
	}
}

// TestGetAccessModes 验证持久卷插件报告 ReadWriteOnce 和 ReadOnlyMany 两种访问模式。
func TestGetAccessModes(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("iscsi_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, volumetest.NewFakeVolumeHost(t, tmpDir, nil, nil))

	plug, err := plugMgr.FindPersistentPluginByName("kubernetes.io/iscsi")
	if err != nil {
		t.Errorf("Can't find the plugin by name")
	}
	if !volumetest.ContainsAccessMode(plug.GetAccessModes(), v1.ReadWriteOnce) || !volumetest.ContainsAccessMode(plug.GetAccessModes(), v1.ReadOnlyMany) {
		t.Errorf("Expected two AccessModeTypes:  %s and %s", v1.ReadWriteOnce, v1.ReadOnlyMany)
	}
}

// fakeDiskManager 用临时目录代替真实 iSCSI 磁盘的挂载管理器。
type fakeDiskManager struct {
	tmpDir string
}

// NewFakeDiskManager 创建一个以临时目录为挂载点的假磁盘管理器。
func NewFakeDiskManager() *fakeDiskManager {
	return &fakeDiskManager{
		tmpDir: utiltesting.MkTmpdirOrDie("iscsi_test"),
	}
}

// Cleanup 删除假管理器使用的临时目录。
func (fake *fakeDiskManager) Cleanup() {
	os.RemoveAll(fake.tmpDir)
}

// MakeGlobalPDName 返回全局挂载路径，这里直接用临时目录冒充。
func (fake *fakeDiskManager) MakeGlobalPDName(disk iscsiDisk) string {
	return fake.tmpDir
}

// MakeGlobalVDPDName 返回块设备的全局映射路径，同样复用临时目录。
func (fake *fakeDiskManager) MakeGlobalVDPDName(disk iscsiDisk) string {
	return fake.tmpDir
}

// AttachDisk 模拟挂载：建好全局路径并挂载一次，返回假的设备路径。
func (fake *fakeDiskManager) AttachDisk(b iscsiDiskMounter) (string, error) {
	globalPath := b.manager.MakeGlobalPDName(*b.iscsiDisk)
	err := os.MkdirAll(globalPath, 0750)
	if err != nil {
		return "", err
	}
	// 模拟全局挂载，使 fakeMounter 记录到与已挂载磁盘相符的挂载次数。
	b.mounter.MountSensitiveWithoutSystemd(globalPath, globalPath, b.fsType, nil, nil)

	return "/dev/sdb", nil
}

// DetachDisk 通过删除全局路径模拟卸载磁盘。
func (fake *fakeDiskManager) DetachDisk(c iscsiDiskUnmounter, mntPath string) error {
	globalPath := c.manager.MakeGlobalPDName(*c.iscsiDisk)
	err := os.RemoveAll(globalPath)
	if err != nil {
		return err
	}
	return nil
}

// DetachBlockISCSIDisk 通过删除全局映射路径模拟取消块设备映射。
func (fake *fakeDiskManager) DetachBlockISCSIDisk(c iscsiDiskUnmapper, mntPath string) error {
	globalPath := c.manager.MakeGlobalVDPDName(*c.iscsiDisk)
	err := os.RemoveAll(globalPath)
	if err != nil {
		return err
	}
	return nil
}

// doTestPlugin 走一遍插件的挂载/卸载流程，并核对卷路径的创建与清理。
func doTestPlugin(t *testing.T, spec *volume.Spec) {
	tmpDir, err := utiltesting.MkTmpdir("iscsi_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, volumetest.NewFakeVolumeHost(t, tmpDir, nil, nil))

	plug, err := plugMgr.FindPluginByName("kubernetes.io/iscsi")
	if err != nil {
		t.Errorf("Can't find the plugin by name")
	}
	fakeManager := NewFakeDiskManager()
	defer fakeManager.Cleanup()
	fakeMounter := mount.NewFakeMounter(nil)
	fakeExec := &testingexec.FakeExec{}
	mounter, err := plug.(*iscsiPlugin).newMounterInternal(spec, types.UID("poduid"), fakeManager, fakeMounter, fakeExec, nil)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Error("Got a nil Mounter")
	}

	path := mounter.GetPath()
	expectedPath := filepath.Join(tmpDir, "pods/poduid/volumes/kubernetes.io~iscsi/vol1")
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

	// 卸载器独立于挂载器测试，使用新的假管理器避免路径状态相互干扰。
	fakeManager2 := NewFakeDiskManager()
	defer fakeManager2.Cleanup()
	unmounter, err := plug.(*iscsiPlugin).newUnmounterInternal("vol1", types.UID("poduid"), fakeManager2, fakeMounter, fakeExec)
	if err != nil {
		t.Errorf("Failed to make a new Unmounter: %v", err)
	}
	if unmounter == nil {
		t.Error("Got a nil Unmounter")
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

// TestPluginVolume 以内联 iSCSI 卷规格验证挂载与卸载路径。
func TestPluginVolume(t *testing.T) {
	vol := &v1.Volume{
		Name: "vol1",
		VolumeSource: v1.VolumeSource{
			ISCSI: &v1.ISCSIVolumeSource{
				TargetPortal: "127.0.0.1:3260",
				IQN:          "iqn.2014-12.server:storage.target01",
				FSType:       "ext4",
				Lun:          0,
			},
		},
	}
	doTestPlugin(t, volume.NewSpecFromVolume(vol))
}

// TestPluginPersistentVolume 以持久卷规格验证挂载与卸载路径。
func TestPluginPersistentVolume(t *testing.T) {
	vol := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol1",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				ISCSI: &v1.ISCSIPersistentVolumeSource{
					TargetPortal: "127.0.0.1:3260",
					IQN:          "iqn.2014-12.server:storage.target01",
					FSType:       "ext4",
					Lun:          0,
				},
			},
		},
	}
	doTestPlugin(t, volume.NewSpecFromPersistentVolume(vol, false))
}

// TestPersistentClaimReadOnlyFlag 验证持久卷声明以只读方式使用时，挂载器属性中的 ReadOnly 为真。
func TestPersistentClaimReadOnlyFlag(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("iscsi_test")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvA",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				ISCSI: &v1.ISCSIPersistentVolumeSource{
					TargetPortal: "127.0.0.1:3260",
					IQN:          "iqn.2014-12.server:storage.target01",
					FSType:       "ext4",
					Lun:          0,
				},
			},
			ClaimRef: &v1.ObjectReference{
				Name: "claimA",
			},
		},
	}

	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claimA",
			Namespace: "nsA",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "pvA",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	client := fake.NewSimpleClientset(pv, claim)

	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, volumetest.NewFakeVolumeHost(t, tmpDir, client, nil))
	plug, _ := plugMgr.FindPluginByName(iscsiPluginName)

	// 声明卷的挂载器在下挂其他卷时会传入 readOnly，这里用 true 构造规格。
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

// TestPortalMounter 验证带端口的 portal 原样保留，不带端口的补上默认端口。
func TestPortalMounter(t *testing.T) {
	if portal := portalMounter("127.0.0.1"); portal != "127.0.0.1:3260" {
		t.Errorf("wrong portal: %s", portal)
	}
	if portal := portalMounter("127.0.0.1:3260"); portal != "127.0.0.1:3260" {
		t.Errorf("wrong portal: %s", portal)
	}
}

// testcase 描述一个规格解析用例及其期望的返回值。
type testcase struct {
	name      string
	defaultNs string
	spec      *volume.Spec
	// 期望 getISCSISecretNameAndNamespace / getISCSIInitiatorInfo 等函数返回的值
	expectedName          string
	expectedNs            string
	expectedIface         string
	expectedError         error
	expectedDiscoveryCHAP bool
	expectedSessionCHAP   bool
}

// TestGetSecretNameAndNamespaceForPV 验证 Secret 名称和命名空间的读取：
// 持久卷用 SecretRef 中指定的值，缺省命名空间取默认值，内联卷不返回 Secret。
func TestGetSecretNameAndNamespaceForPV(t *testing.T) {
	tests := []testcase{
		{
			name:      "persistent volume source",
			defaultNs: "default",
			spec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						PersistentVolumeSource: v1.PersistentVolumeSource{
							ISCSI: &v1.ISCSIPersistentVolumeSource{
								TargetPortal: "127.0.0.1:3260",
								IQN:          "iqn.2014-12.server:storage.target01",
								FSType:       "ext4",
								Lun:          0,
								SecretRef: &v1.SecretReference{
									Name:      "name",
									Namespace: "ns",
								},
							},
						},
					},
				},
			},
			expectedName:  "name",
			expectedNs:    "ns",
			expectedError: nil,
		},
		{
			name:      "persistent volume source without namespace",
			defaultNs: "default",
			spec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						PersistentVolumeSource: v1.PersistentVolumeSource{
							ISCSI: &v1.ISCSIPersistentVolumeSource{
								TargetPortal: "127.0.0.1:3260",
								IQN:          "iqn.2014-12.server:storage.target01",
								FSType:       "ext4",
								Lun:          0,
								SecretRef: &v1.SecretReference{
									Name: "name",
								},
							},
						},
					},
				},
			},
			expectedName:  "name",
			expectedNs:    "default",
			expectedError: nil,
		},
		{
			name:      "pod volume source",
			defaultNs: "default",
			spec: &volume.Spec{
				Volume: &v1.Volume{
					VolumeSource: v1.VolumeSource{
						ISCSI: &v1.ISCSIVolumeSource{
							TargetPortal: "127.0.0.1:3260",
							IQN:          "iqn.2014-12.server:storage.target01",
							FSType:       "ext4",
							Lun:          0,
						},
					},
				},
			},
			expectedName:  "",
			expectedNs:    "",
			expectedError: nil,
		},
	}
	for _, testcase := range tests {
		resultName, resultNs, err := getISCSISecretNameAndNamespace(testcase.spec, testcase.defaultNs)
		if err != testcase.expectedError || resultName != testcase.expectedName || resultNs != testcase.expectedNs {
			t.Errorf("%s failed: expected err=%v ns=%q name=%q, got %v/%q/%q", testcase.name, testcase.expectedError, testcase.expectedNs, testcase.expectedName,
				err, resultNs, resultName)
		}
	}

}

// TestGetISCSIInitiatorInfo 验证内联卷和持久卷均能读出 iSCSI 接口类型。
func TestGetISCSIInitiatorInfo(t *testing.T) {
	tests := []testcase{
		{
			name: "persistent volume source",
			spec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						PersistentVolumeSource: v1.PersistentVolumeSource{
							ISCSI: &v1.ISCSIPersistentVolumeSource{
								TargetPortal:   "127.0.0.1:3260",
								IQN:            "iqn.2014-12.server:storage.target01",
								FSType:         "ext4",
								Lun:            0,
								ISCSIInterface: "tcp",
							},
						},
					},
				},
			},
			expectedIface: "tcp",
			expectedError: nil,
		},
		{
			name: "pod volume source",
			spec: &volume.Spec{
				Volume: &v1.Volume{
					VolumeSource: v1.VolumeSource{
						ISCSI: &v1.ISCSIVolumeSource{
							TargetPortal:   "127.0.0.1:3260",
							IQN:            "iqn.2014-12.server:storage.target01",
							FSType:         "ext4",
							Lun:            0,
							ISCSIInterface: "tcp",
						},
					},
				},
			},
			expectedIface: "tcp",
			expectedError: nil,
		},
	}
	for _, testcase := range tests {
		resultIface, _, err := getISCSIInitiatorInfo(testcase.spec)
		if err != testcase.expectedError || resultIface != testcase.expectedIface {
			t.Errorf("%s failed: expected err=%v iface=%s, got %v/%s", testcase.name, testcase.expectedError, testcase.expectedIface,
				err, resultIface)
		}
	}
}

// TestGetISCSICHAP 验证发现阶段和会话阶段的 CHAP 认证开关读取，
// 并确认非 iSCSI 规格返回错误。
func TestGetISCSICHAP(t *testing.T) {
	tests := []testcase{
		{
			name: "persistent volume source",
			spec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						PersistentVolumeSource: v1.PersistentVolumeSource{
							ISCSI: &v1.ISCSIPersistentVolumeSource{
								DiscoveryCHAPAuth: true,
								SessionCHAPAuth:   true,
							},
						},
					},
				},
			},
			expectedDiscoveryCHAP: true,
			expectedSessionCHAP:   true,
			expectedError:         nil,
		},
		{
			name: "pod volume source",
			spec: &volume.Spec{
				Volume: &v1.Volume{
					VolumeSource: v1.VolumeSource{
						ISCSI: &v1.ISCSIVolumeSource{
							DiscoveryCHAPAuth: true,
							SessionCHAPAuth:   true,
						},
					},
				},
			},
			expectedDiscoveryCHAP: true,
			expectedSessionCHAP:   true,
			expectedError:         nil,
		},
		{
			name:                  "no volume",
			spec:                  &volume.Spec{},
			expectedDiscoveryCHAP: false,
			expectedSessionCHAP:   false,
			expectedError:         fmt.Errorf("Spec does not reference an ISCSI volume type"),
		},
	}
	for _, testcase := range tests {
		resultDiscoveryCHAP, _ := getISCSIDiscoveryCHAPInfo(testcase.spec)
		resultSessionCHAP, err := getISCSISessionCHAPInfo(testcase.spec)
		switch testcase.name {
		case "no volume":
			if err == nil || err.Error() != testcase.expectedError.Error() || resultDiscoveryCHAP != testcase.expectedDiscoveryCHAP || resultSessionCHAP != testcase.expectedSessionCHAP {
				t.Errorf("%s failed: expected err=%v DiscoveryCHAP=%v SessionCHAP=%v, got %v/%v/%v",
					testcase.name, testcase.expectedError, testcase.expectedDiscoveryCHAP, testcase.expectedSessionCHAP,
					err, resultDiscoveryCHAP, resultSessionCHAP)
			}
		default:
			if err != testcase.expectedError || resultDiscoveryCHAP != testcase.expectedDiscoveryCHAP || resultSessionCHAP != testcase.expectedSessionCHAP {
				t.Errorf("%s failed: expected err=%v DiscoveryCHAP=%v SessionCHAP=%v, got %v/%v/%v", testcase.name, testcase.expectedError, testcase.expectedDiscoveryCHAP, testcase.expectedSessionCHAP,
					err, resultDiscoveryCHAP, resultSessionCHAP)
			}
		}
	}
}

// TestGetVolumeSpec 验证从全局挂载路径能还原出 portal、IQN、LUN 和接口信息。
func TestGetVolumeSpec(t *testing.T) {
	path := "plugins/kubernetes.io/iscsi/volumeDevices/iface-default/127.0.0.1:3260-iqn.2014-12.server:storage.target01-lun-0"
	spec, _ := getVolumeSpecFromGlobalMapPath("test", path)

	portal := spec.PersistentVolume.Spec.PersistentVolumeSource.ISCSI.TargetPortal
	if portal != "127.0.0.1:3260" {
		t.Errorf("wrong portal: %v", portal)
	}
	iqn := spec.PersistentVolume.Spec.PersistentVolumeSource.ISCSI.IQN
	if iqn != "iqn.2014-12.server:storage.target01" {
		t.Errorf("wrong iqn: %v", iqn)
	}
	lun := spec.PersistentVolume.Spec.PersistentVolumeSource.ISCSI.Lun
	if lun != 0 {
		t.Errorf("wrong lun: %v", lun)
	}
	iface := spec.PersistentVolume.Spec.PersistentVolumeSource.ISCSI.ISCSIInterface
	if iface != "default" {
		t.Errorf("wrong ISCSIInterface: %v", iface)
	}
}

// TestGetVolumeSpec_no_lun 验证缺少 LUN 的路径被判定为格式错误。
func TestGetVolumeSpec_no_lun(t *testing.T) {
	path := "plugins/kubernetes.io/iscsi/volumeDevices/iface-default/127.0.0.1:3260-iqn.2014-12.server:storage.target01"
	_, err := getVolumeSpecFromGlobalMapPath("test", path)
	if !strings.Contains(err.Error(), "malformatted mnt path") {
		t.Errorf("should get error: malformatted mnt path")
	}
}

// TestGetVolumeSpec_no_iface 验证路径中缺少接口段时返回提取失败的错误。
func TestGetVolumeSpec_no_iface(t *testing.T) {
	path := "plugins/kubernetes.io/iscsi/volumeDevices/default/127.0.0.1:3260-iqn.2014-12.server:storage.target01-lun-0"
	_, err := getVolumeSpecFromGlobalMapPath("test", path)
	if !strings.Contains(err.Error(), "failed to retrieve iface") {
		t.Errorf("should get error: failed to retrieve iface")
	}
}
