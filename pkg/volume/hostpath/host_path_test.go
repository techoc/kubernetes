/*
Copyright 2014 The Kubernetes Authors.

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

package hostpath

import (
	"fmt"
	"os"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	utilpath "k8s.io/utils/path"
	"k8s.io/utils/ptr"
)

// newHostPathType 将字符串包装为 *v1.HostPathType。
func newHostPathType(pathType string) *v1.HostPathType {
	hostPathType := new(v1.HostPathType)
	*hostPathType = v1.HostPathType(pathType)
	return hostPathType
}

// newHostPathTypeList 将多个字符串转换为 *v1.HostPathType 切片。
func newHostPathTypeList(pathType ...string) []*v1.HostPathType {
	typeList := []*v1.HostPathType{}
	for _, ele := range pathType {
		typeList = append(typeList, newHostPathType(ele))
	}

	return typeList
}

// TestCanSupport 验证 hostPath 插件对 HostPath 卷与 PV 的支持判断，以及对无来源卷的拒绝。
func TestCanSupport(t *testing.T) {
	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(volume.VolumeConfig{}), nil /* prober */, volumetest.NewFakeKubeletVolumeHost(t, "fake", nil, nil))

	plug, err := plugMgr.FindPluginByName(hostPathPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}
	if plug.GetPluginName() != hostPathPluginName {
		t.Errorf("Wrong name: %s", plug.GetPluginName())
	}
	if !plug.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{}}}}) {
		t.Errorf("Expected true")
	}
	if !plug.CanSupport(&volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{HostPath: &v1.HostPathVolumeSource{}}}}}) {
		t.Errorf("Expected true")
	}
	if plug.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{}}}) {
		t.Errorf("Expected false")
	}
}

// TestGetAccessModes 验证 hostPath 持久卷只支持 ReadWriteOnce。
func TestGetAccessModes(t *testing.T) {
	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(volume.VolumeConfig{}), nil /* prober */, volumetest.NewFakeKubeletVolumeHost(t, "/tmp/fake", nil, nil))

	plug, err := plugMgr.FindPersistentPluginByName(hostPathPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}
	if len(plug.GetAccessModes()) != 1 || plug.GetAccessModes()[0] != v1.ReadWriteOnce {
		t.Errorf("Expected %s PersistentVolumeAccessMode", v1.ReadWriteOnce)
	}
}

// TestRecycler 验证可按 spec 找到 hostPath 的可回收插件。
func TestRecycler(t *testing.T) {
	plugMgr := volume.VolumePluginMgr{}
	pluginHost := volumetest.NewFakeKubeletVolumeHost(t, "/tmp/fake", nil, nil)
	plugMgr.InitPlugins([]volume.VolumePlugin{&hostPathPlugin{nil, volume.VolumeConfig{}, false}}, nil, pluginHost)

	spec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/foo"}}}}}
	_, err := plugMgr.FindRecyclablePluginBySpec(spec)
	if err != nil {
		t.Errorf("Can't find the plugin by name")
	}
}

// TestDeleter 验证 deleter 能删除 /tmp 下的 hostPath 目录。
func TestDeleter(t *testing.T) {
	// deleter 对 "/tmp" 路径有硬编码的正则校验，因此临时目录必须建在 /tmp 下。
	tempPath := fmt.Sprintf("/tmp/hostpath.%s", uuid.NewUUID())
	err := os.MkdirAll(tempPath, 0750)
	if err != nil {
		t.Fatalf("Failed to create tmp directory for deleter: %v", err)
	}
	defer os.RemoveAll(tempPath)
	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(volume.VolumeConfig{}), nil /* prober */, volumetest.NewFakeKubeletVolumeHost(t, "/tmp/fake", nil, nil))

	spec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{HostPath: &v1.HostPathVolumeSource{Path: tempPath}}}}}
	plug, err := plugMgr.FindDeletablePluginBySpec(spec)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}
	logger, _ := ktesting.NewTestContext(t)
	deleter, err := plug.NewDeleter(logger, spec)
	if err != nil {
		t.Errorf("Failed to make a new Deleter: %v", err)
	}
	if deleter.GetPath() != tempPath {
		t.Errorf("Expected %s but got %s", tempPath, deleter.GetPath())
	}
	if err := deleter.Delete(); err != nil {
		t.Errorf("Mock Recycler expected to return nil but got %s", err)
	}
	if exists, _ := utilpath.Exists(utilpath.CheckFollowSymlink, tempPath); exists {
		t.Errorf("Temp path expected to be deleted, but was found at %s", tempPath)
	}
}

// TestDeleterTempDir 验证 deleter 对 /tmp 路径的校验，拒绝裸 /tmp 与非 /tmp 路径。
func TestDeleterTempDir(t *testing.T) {
	tests := map[string]struct {
		expectedFailure bool
		path            string
	}{
		"just-tmp": {true, "/tmp"},
		"not-tmp":  {true, "/nottmp"},
		"good-tmp": {false, "/tmp/scratch"},
	}
	logger, _ := ktesting.NewTestContext(t)
	for name, test := range tests {
		plugMgr := volume.VolumePluginMgr{}
		plugMgr.InitPlugins(ProbeVolumePlugins(volume.VolumeConfig{}), nil /* prober */, volumetest.NewFakeKubeletVolumeHost(t, "/tmp/fake", nil, nil))
		spec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{HostPath: &v1.HostPathVolumeSource{Path: test.path}}}}}
		plug, _ := plugMgr.FindDeletablePluginBySpec(spec)
		deleter, _ := plug.NewDeleter(logger, spec)
		err := deleter.Delete()
		if err == nil && test.expectedFailure {
			t.Errorf("Expected failure for test '%s' but got nil err", name)
		}
		if err != nil && !test.expectedFailure {
			t.Errorf("Unexpected failure for test '%s': %v", name, err)
		}
	}
}

// TestProvisioner 验证 hostPath provisioner 能创建 PV，并设置容量与回收策略。
func TestProvisioner(t *testing.T) {
	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(volume.VolumeConfig{ProvisioningEnabled: true}),
		nil,
		volumetest.NewFakeKubeletVolumeHost(t, "/tmp/fake", nil, nil))
	plug, err := plugMgr.FindProvisionablePluginByName(hostPathPluginName)
	if err != nil {
		t.Fatalf("Can't find the plugin by name")
	}
	options := volume.VolumeOptions{
		PVC:                           volumetest.CreateTestPVC("1Gi", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}),
		PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
	}
	logger, _ := ktesting.NewTestContext(t)
	creator, err := plug.NewProvisioner(logger, options)
	if err != nil {
		t.Fatalf("Failed to make a new Provisioner: %v", err)
	}

	hostPathCreator, ok := creator.(*hostPathProvisioner)
	if !ok {
		t.Fatal("Not a hostPathProvisioner")
	}
	hostPathCreator.basePath = fmt.Sprintf("%s.%s", "hostPath_pv", uuid.NewUUID())

	pv, err := hostPathCreator.Provision(nil, nil)
	if err != nil {
		t.Errorf("Unexpected error creating volume: %v", err)
	}
	if pv.Spec.HostPath.Path == "" {
		t.Errorf("Expected pv.Spec.HostPath.Path to not be empty: %#v", pv)
	}
	expectedCapacity := resource.NewQuantity(1*1024*1024*1024, resource.BinarySI)
	actualCapacity := pv.Spec.Capacity[v1.ResourceStorage]
	expectedAmt := expectedCapacity.Value()
	actualAmt := actualCapacity.Value()
	if expectedAmt != actualAmt {
		t.Errorf("Expected capacity %+v but got %+v", expectedAmt, actualAmt)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimDelete {
		t.Errorf("Expected reclaim policy %+v but got %+v", v1.PersistentVolumeReclaimDelete, pv.Spec.PersistentVolumeReclaimPolicy)
	}

	os.RemoveAll(hostPathCreator.basePath)

}

// TestInvalidHostPath 验证包含 '..' 的 hostPath 在 SetUp 时被拒绝。
func TestInvalidHostPath(t *testing.T) {
	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(volume.VolumeConfig{}), nil /* prober */, volumetest.NewFakeKubeletVolumeHost(t, "fake", nil, nil))

	plug, err := plugMgr.FindPluginByName(hostPathPluginName)
	if err != nil {
		t.Fatalf("Unable to find plugin %s by name: %v", hostPathPluginName, err)
	}
	spec := &v1.Volume{
		Name:         "vol1",
		VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/no/backsteps/allowed/.."}},
	}
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID("poduid")}}
	mounter, err := plug.NewMounter(volume.NewSpecFromVolume(spec), pod)
	if err != nil {
		t.Fatal(err)
	}

	err = mounter.SetUp(volume.MounterArgs{})
	expectedMsg := "invalid HostPath `/no/backsteps/allowed/..`: must not contain '..'"
	if err.Error() != expectedMsg {
		t.Fatalf("expected error `%s` but got `%s`", expectedMsg, err)
	}
}

// TestPlugin 验证 hostPath mounter/unmounter 的完整挂载与卸载流程。
func TestPlugin(t *testing.T) {
	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(volume.VolumeConfig{}), nil /* prober */, volumetest.NewFakeKubeletVolumeHost(t, "fake", nil, nil))

	plug, err := plugMgr.FindPluginByName(hostPathPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}

	volPath := "/tmp/vol1"
	spec := &v1.Volume{
		Name:         "vol1",
		VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: volPath, Type: newHostPathType(string(v1.HostPathDirectoryOrCreate))}},
	}
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID("poduid")}}
	defer os.RemoveAll(volPath)
	mounter, err := plug.NewMounter(volume.NewSpecFromVolume(spec), pod)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Fatalf("Got a nil Mounter")
	}

	path := mounter.GetPath()
	if path != volPath {
		t.Errorf("Got unexpected path: %s", path)
	}

	if err := mounter.SetUp(volume.MounterArgs{}); err != nil {
		t.Errorf("Expected success, got: %v", err)
	}

	unmounter, err := plug.NewUnmounter("vol1", types.UID("poduid"))
	if err != nil {
		t.Errorf("Failed to make a new Unmounter: %v", err)
	}
	if unmounter == nil {
		t.Fatalf("Got a nil Unmounter")
	}

	if err := unmounter.TearDown(); err != nil {
		t.Errorf("Expected success, got: %v", err)
	}
}

// TestPersistentClaimReadOnlyFlag 验证从 PVC 派生的 spec 创建的 mounter 带有只读属性。
func TestPersistentClaimReadOnlyFlag(t *testing.T) {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvA",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "foo", Type: newHostPathType(string(v1.HostPathDirectoryOrCreate))},
			},
			ClaimRef: &v1.ObjectReference{
				Name: "claimA",
			},
		},
	}
	defer os.RemoveAll("foo")

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
	plugMgr.InitPlugins(ProbeVolumePlugins(volume.VolumeConfig{}), nil /* prober */, volumetest.NewFakeKubeletVolumeHost(t, "/tmp/fake", client, nil))
	plug, _ := plugMgr.FindPluginByName(hostPathPluginName)

	// PVC 卷源在创建 mounter 时会提供 readOnly 标志。
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

// setUp 创建测试用的目录与普通文件。
func setUp() error {
	err := os.MkdirAll("/tmp/ExistingFolder", os.FileMode(0755))
	if err != nil {
		return err
	}

	f, err := os.OpenFile("/tmp/ExistingFolder/foo", os.O_CREATE, os.FileMode(0644))
	if err != nil {
		return err
	}
	defer f.Close()

	return nil
}

// tearDown 清理 setUp 创建的目录。
func tearDown() {
	os.RemoveAll("/tmp/ExistingFolder")
}

// TestOSFileTypeChecker 验证文件类型检查器对目录、普通文件、socket 与设备文件的判定。
func TestOSFileTypeChecker(t *testing.T) {
	err := setUp()
	if err != nil {
		t.Error(err)
	}
	defer tearDown()
	testCases := []struct {
		name        string
		path        string
		desiredType string
		isDir       bool
		isFile      bool
		isSocket    bool
		isBlock     bool
		isChar      bool
	}{
		{
			name:        "Existing Folder",
			path:        "/tmp/ExistingFolder",
			desiredType: string(hostutil.FileTypeDirectory),
			isDir:       true,
		},
		{
			name:        "Existing File",
			path:        "/tmp/ExistingFolder/foo",
			desiredType: string(hostutil.FileTypeFile),
			isFile:      true,
		},
		{
			name:        "Existing Socket File",
			path:        "/tmp/ExistingFolder/foo",
			desiredType: string(v1.HostPathSocket),
			isSocket:    true,
		},
		{
			name:        "Existing Character Device",
			path:        "/tmp/ExistingFolder/foo",
			desiredType: string(v1.HostPathCharDev),
			isChar:      true,
		},
		{
			name:        "Existing Block Device",
			path:        "/tmp/ExistingFolder/foo",
			desiredType: string(v1.HostPathBlockDev),
			isBlock:     true,
		},
	}

	for i, tc := range testCases {
		fakeFTC := hostutil.NewFakeHostUtil(
			map[string]hostutil.FileType{
				tc.path: hostutil.FileType(tc.desiredType),
			})
		oftc := newFileTypeChecker(tc.path, fakeFTC)

		path := oftc.GetPath()
		if path != tc.path {
			t.Errorf("[%d: %q] got unexpected path: %s", i, tc.name, path)
		}

		exist := oftc.Exists()
		if !exist {
			t.Errorf("[%d: %q] path: %s does not exist", i, tc.name, path)
		}

		if tc.isDir {
			if !oftc.IsDir() {
				t.Errorf("[%d: %q] expected folder, got unexpected: %s", i, tc.name, path)
			}
			if oftc.IsFile() {
				t.Errorf("[%d: %q] expected folder, got unexpected file: %s", i, tc.name, path)
			}
			if oftc.IsSocket() {
				t.Errorf("[%d: %q] expected folder, got unexpected socket file: %s", i, tc.name, path)
			}
			if oftc.IsBlock() {
				t.Errorf("[%d: %q] expected folder, got unexpected block device: %s", i, tc.name, path)
			}
			if oftc.IsChar() {
				t.Errorf("[%d: %q] expected folder, got unexpected character device: %s", i, tc.name, path)
			}
		}

		if tc.isFile {
			if !oftc.IsFile() {
				t.Errorf("[%d: %q] expected file, got unexpected: %s", i, tc.name, path)
			}
			if oftc.IsDir() {
				t.Errorf("[%d: %q] expected file, got unexpected folder: %s", i, tc.name, path)
			}
			if oftc.IsSocket() {
				t.Errorf("[%d: %q] expected file, got unexpected socket file: %s", i, tc.name, path)
			}
			if oftc.IsBlock() {
				t.Errorf("[%d: %q] expected file, got unexpected block device: %s", i, tc.name, path)
			}
			if oftc.IsChar() {
				t.Errorf("[%d: %q] expected file, got unexpected character device: %s", i, tc.name, path)
			}
		}

		if tc.isSocket {
			if !oftc.IsSocket() {
				t.Errorf("[%d: %q] expected socket file, got unexpected: %s", i, tc.name, path)
			}
			if oftc.IsDir() {
				t.Errorf("[%d: %q] expected socket file, got unexpected folder: %s", i, tc.name, path)
			}
			if oftc.IsFile() {
				t.Errorf("[%d: %q] expected socket file, got unexpected file: %s", i, tc.name, path)
			}
			if oftc.IsBlock() {
				t.Errorf("[%d: %q] expected socket file, got unexpected block device: %s", i, tc.name, path)
			}
			if oftc.IsChar() {
				t.Errorf("[%d: %q] expected socket file, got unexpected character device: %s", i, tc.name, path)
			}
		}

		if tc.isChar {
			if !oftc.IsChar() {
				t.Errorf("[%d: %q] expected character device, got unexpected: %s", i, tc.name, path)
			}
			if oftc.IsDir() {
				t.Errorf("[%d: %q] expected character device, got unexpected folder: %s", i, tc.name, path)
			}
			if oftc.IsFile() {
				t.Errorf("[%d: %q] expected character device, got unexpected file: %s", i, tc.name, path)
			}
			if oftc.IsSocket() {
				t.Errorf("[%d: %q] expected character device, got unexpected socket file: %s", i, tc.name, path)
			}
			if oftc.IsBlock() {
				t.Errorf("[%d: %q] expected character device, got unexpected block device: %s", i, tc.name, path)
			}
		}

		if tc.isBlock {
			if !oftc.IsBlock() {
				t.Errorf("[%d: %q] expected block device, got unexpected: %s", i, tc.name, path)
			}
			if oftc.IsDir() {
				t.Errorf("[%d: %q] expected block device, got unexpected folder: %s", i, tc.name, path)
			}
			if oftc.IsFile() {
				t.Errorf("[%d: %q] expected block device, got unexpected file: %s", i, tc.name, path)
			}
			if oftc.IsSocket() {
				t.Errorf("[%d: %q] expected block device, got unexpected socket file: %s", i, tc.name, path)
			}
			if oftc.IsChar() {
				t.Errorf("[%d: %q] expected block device, got unexpected character device: %s", i, tc.name, path)
			}
		}
	}

}

// fakeHostPathTypeChecker 提供可配置的 hostPathTypeChecker 假实现，用于内部类型校验测试。
type fakeHostPathTypeChecker struct {
	name            string
	path            string
	pathType        *v1.HostPathType
	exists          bool
	isDir           bool
	isFile          bool
	isSocket        bool
	isBlock         bool
	isChar          bool
	validpathType   []*v1.HostPathType
	invalidpathType []*v1.HostPathType
}

// MakeFile 为空实现。
func (ftc *fakeHostPathTypeChecker) MakeFile() error { return nil }

// MakeDir 为空实现。
func (ftc *fakeHostPathTypeChecker) MakeDir() error { return nil }

// Exists 返回预设的存在性。
func (ftc *fakeHostPathTypeChecker) Exists() bool { return ftc.exists }

// IsFile 返回预设的文件标记。
func (ftc *fakeHostPathTypeChecker) IsFile() bool { return ftc.isFile }

// IsDir 返回预设的目录标记。
func (ftc *fakeHostPathTypeChecker) IsDir() bool { return ftc.isDir }

// IsBlock 返回预设的块设备标记。
func (ftc *fakeHostPathTypeChecker) IsBlock() bool { return ftc.isBlock }

// IsChar 返回预设的字符设备标记。
func (ftc *fakeHostPathTypeChecker) IsChar() bool { return ftc.isChar }

// IsSocket 返回预设的 socket 标记。
func (ftc *fakeHostPathTypeChecker) IsSocket() bool { return ftc.isSocket }

// GetPath 返回预设路径。
func (ftc *fakeHostPathTypeChecker) GetPath() string { return ftc.path }

// GetType 返回预设的 hostPath 类型。
func (ftc *fakeHostPathTypeChecker) GetType() (string, error) { return string(*ftc.pathType), nil }

// TestHostPathTypeCheckerInternal 校验每种 hostPath 类型与文件实际状态的匹配规则。
func TestHostPathTypeCheckerInternal(t *testing.T) {
	testCases := []fakeHostPathTypeChecker{
		{
			name:          "Existing Folder",
			path:          "/existingFolder",
			pathType:      ptr.To(v1.HostPathDirectory),
			isDir:         true,
			exists:        true,
			validpathType: newHostPathTypeList(string(v1.HostPathDirectoryOrCreate), string(v1.HostPathDirectory)),
			invalidpathType: newHostPathTypeList(string(v1.HostPathFileOrCreate), string(v1.HostPathFile),
				string(v1.HostPathSocket), string(v1.HostPathCharDev), string(v1.HostPathBlockDev)),
		},
		{
			name:          "New Folder",
			path:          "/newFolder",
			pathType:      ptr.To(v1.HostPathDirectory),
			isDir:         false,
			exists:        false,
			validpathType: newHostPathTypeList(string(v1.HostPathDirectoryOrCreate)),
			invalidpathType: newHostPathTypeList(string(v1.HostPathDirectory), string(v1.HostPathFile),
				string(v1.HostPathSocket), string(v1.HostPathCharDev), string(v1.HostPathBlockDev)),
		},
		{
			name:          "Existing File",
			path:          "/existingFile",
			pathType:      ptr.To(v1.HostPathFile),
			isFile:        true,
			exists:        true,
			validpathType: newHostPathTypeList(string(v1.HostPathFileOrCreate), string(v1.HostPathFile)),
			invalidpathType: newHostPathTypeList(string(v1.HostPathDirectoryOrCreate), string(v1.HostPathDirectory),
				string(v1.HostPathSocket), string(v1.HostPathCharDev), string(v1.HostPathBlockDev)),
		},
		{
			name:          "New File",
			path:          "/newFile",
			pathType:      ptr.To(v1.HostPathFile),
			isFile:        false,
			exists:        false,
			validpathType: newHostPathTypeList(string(v1.HostPathFileOrCreate)),
			invalidpathType: newHostPathTypeList(string(v1.HostPathDirectory),
				string(v1.HostPathSocket), string(v1.HostPathCharDev), string(v1.HostPathBlockDev)),
		},
		{
			name:          "Existing Socket",
			path:          "/existing.socket",
			pathType:      ptr.To(v1.HostPathSocket),
			isSocket:      true,
			isFile:        true,
			exists:        true,
			validpathType: newHostPathTypeList(string(v1.HostPathSocket), string(v1.HostPathFileOrCreate), string(v1.HostPathFile)),
			invalidpathType: newHostPathTypeList(string(v1.HostPathDirectoryOrCreate), string(v1.HostPathDirectory),
				string(v1.HostPathCharDev), string(v1.HostPathBlockDev)),
		},
		{
			name:          "Existing Character Device",
			path:          "/existing.char",
			pathType:      ptr.To(v1.HostPathCharDev),
			isChar:        true,
			isFile:        true,
			exists:        true,
			validpathType: newHostPathTypeList(string(v1.HostPathCharDev), string(v1.HostPathFileOrCreate), string(v1.HostPathFile)),
			invalidpathType: newHostPathTypeList(string(v1.HostPathDirectoryOrCreate), string(v1.HostPathDirectory),
				string(v1.HostPathSocket), string(v1.HostPathBlockDev)),
		},
		{
			name:          "Existing Block Device",
			path:          "/existing.block",
			pathType:      ptr.To(v1.HostPathBlockDev),
			isBlock:       true,
			isFile:        true,
			exists:        true,
			validpathType: newHostPathTypeList(string(v1.HostPathBlockDev), string(v1.HostPathFileOrCreate), string(v1.HostPathFile)),
			invalidpathType: newHostPathTypeList(string(v1.HostPathDirectoryOrCreate), string(v1.HostPathDirectory),
				string(v1.HostPathSocket), string(v1.HostPathCharDev)),
		},
	}

	for i, tc := range testCases {
		for _, pathType := range tc.validpathType {
			err := checkTypeInternal(&tc, pathType)
			if err != nil {
				t.Errorf("[%d: %q] [%q] expected nil, got %v", i, tc.name, string(*pathType), err)
			}
		}

		for _, pathType := range tc.invalidpathType {
			checkResult := checkTypeInternal(&tc, pathType)
			if checkResult == nil {
				t.Errorf("[%d: %q] [%q] expected error, got nil", i, tc.name, string(*pathType))
			}
		}
	}

}
