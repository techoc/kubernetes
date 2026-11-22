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

package configmap

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
)

// TestMakePayload 验证 MakePayload 的映射结果：默认模式、子目录、缺失键的可选处理、
// 权限与属主的覆盖及特性门控的影响。
func TestMakePayload(t *testing.T) {
	// 用固定值模拟 KeyToPath 中显式指定的 mode 和 user。
	caseMappingMode := int32(0400)
	caseMappingUser1 := int64(1001)
	caseMappingUser2 := int64(1002)

	cases := []struct {
		name      string
		mappings  []v1.KeyToPath
		configMap *v1.ConfigMap
		mode      int32
		user      *int64
		optional  bool
		payload   map[string]util.FileProjection
		success   bool

		disableUserFieldsGate bool
	}{
		{
			name: "no overrides",
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"foo": {Data: []byte("foo"), Mode: 0644},
				"bar": {Data: []byte("bar"), Mode: 0644},
			},
			success: true,
		},
		{
			name: "no overrides binary data",
			configMap: &v1.ConfigMap{
				BinaryData: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"foo": {Data: []byte("foo"), Mode: 0644},
				"bar": {Data: []byte("bar"), Mode: 0644},
			},
			success: true,
		},
		{
			name: "no overrides mixed data",
			configMap: &v1.ConfigMap{
				BinaryData: map[string][]byte{
					"foo": []byte("foo"),
				},
				Data: map[string]string{
					"bar": "bar",
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"foo": {Data: []byte("foo"), Mode: 0644},
				"bar": {Data: []byte("bar"), Mode: 0644},
			},
			success: true,
		},
		{
			name: "basic 1",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "path/to/foo.txt",
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"path/to/foo.txt": {Data: []byte("foo"), Mode: 0644},
			},
			success: true,
		},
		{
			name: "subdirs",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "path/to/1/2/3/foo.txt",
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"path/to/1/2/3/foo.txt": {Data: []byte("foo"), Mode: 0644},
			},
			success: true,
		},
		{
			name: "subdirs 2",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "path/to/1/2/3/foo.txt",
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"path/to/1/2/3/foo.txt": {Data: []byte("foo"), Mode: 0644},
			},
			success: true,
		},
		{
			name: "subdirs 3",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "path/to/1/2/3/foo.txt",
				},
				{
					Key:  "bar",
					Path: "another/path/to/the/esteemed/bar.bin",
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"path/to/1/2/3/foo.txt":                {Data: []byte("foo"), Mode: 0644},
				"another/path/to/the/esteemed/bar.bin": {Data: []byte("bar"), Mode: 0644},
			},
			success: true,
		},
		{
			name: "non existent key",
			mappings: []v1.KeyToPath{
				{
					Key:  "zab",
					Path: "path/to/foo.txt",
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode:    0644,
			success: false,
		},
		{
			name: "mapping with Mode",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "foo.txt",
					Mode: &caseMappingMode,
				},
				{
					Key:  "bar",
					Path: "bar.bin",
					Mode: &caseMappingMode,
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"foo.txt": {Data: []byte("foo"), Mode: caseMappingMode},
				"bar.bin": {Data: []byte("bar"), Mode: caseMappingMode},
			},
			success: true,
		},
		{
			name: "mapping with defaultMode",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "foo.txt",
				},
				{
					Key:  "bar",
					Path: "bar.bin",
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"foo.txt": {Data: []byte("foo"), Mode: 0644},
				"bar.bin": {Data: []byte("bar"), Mode: 0644},
			},
			success: true,
		},
		{
			name: "optional non existent key",
			mappings: []v1.KeyToPath{
				{
					Key:  "zab",
					Path: "path/to/foo.txt",
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode:     0644,
			optional: true,
			payload:  map[string]util.FileProjection{},
			success:  true,
		},
		{
			name: "mapping with defaultUser",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "foo.txt",
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			user: &caseMappingUser1,
			payload: map[string]util.FileProjection{
				"foo.txt": {Data: []byte("foo"), Mode: 0644, FsUser: &caseMappingUser1},
			},
			success: true,
		},
		{
			name: "mapping with User",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "foo.txt",
					User: &caseMappingUser2,
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"foo.txt": {Data: []byte("foo"), Mode: 0644, FsUser: &caseMappingUser2},
			},
			success: true,
		},
		{
			name: "mapping with defaultUser and User",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "foo.txt",
					User: &caseMappingUser2,
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			user: &caseMappingUser1,
			payload: map[string]util.FileProjection{
				"foo.txt": {Data: []byte("foo"), Mode: 0644, FsUser: &caseMappingUser2},
			},
			success: true,
		},
		{
			name:     "empty mappings",
			mappings: []v1.KeyToPath{},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
				BinaryData: map[string][]byte{
					"moo": []byte("moo"),
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"foo": {Data: []byte("foo"), Mode: 0644},
				"bar": {Data: []byte("bar"), Mode: 0644},
				"moo": {Data: []byte("moo"), Mode: 0644},
			},
			success: true,
		},
		{
			name:     "empty mappings with defaultUser",
			mappings: []v1.KeyToPath{},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
				BinaryData: map[string][]byte{
					"moo": []byte("moo"),
				},
			},
			mode: 0644,
			user: &caseMappingUser1,
			payload: map[string]util.FileProjection{
				"foo": {Data: []byte("foo"), Mode: 0644, FsUser: &caseMappingUser1},
				"bar": {Data: []byte("bar"), Mode: 0644, FsUser: &caseMappingUser1},
				"moo": {Data: []byte("moo"), Mode: 0644, FsUser: &caseMappingUser1},
			},
			success: true,
		},
		{
			name: "user fields with disabled feature gate",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "foo.txt",
					User: &caseMappingUser2,
				},
			},
			configMap: &v1.ConfigMap{
				Data: map[string]string{
					"foo": "foo",
					"bar": "bar",
				},
			},
			mode: 0644,
			user: &caseMappingUser1,
			payload: map[string]util.FileProjection{
				"foo.txt": {Data: []byte("foo"), Mode: 0644},
			},
			success:               true,
			disableUserFieldsGate: true,
		},
	}

	for _, tc := range cases {
		if tc.disableUserFieldsGate {
			// 门控默认开启，需先回退模拟版本才能真正关闭它。
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.37"))
		}
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AtomicWriteVolumeUserFields, !tc.disableUserFieldsGate)

		actualPayload, err := MakePayload(tc.mappings, tc.configMap, &tc.mode, tc.user, tc.optional)
		if err != nil && tc.success {
			t.Errorf("%v: unexpected failure making payload: %v", tc.name, err)
			continue
		}

		if err == nil && !tc.success {
			t.Errorf("%v: unexpected success making payload", tc.name)
			continue
		}

		if !tc.success {
			continue
		}

		// 仅供失败用例通过，无需继续比对 payload。
		if e, a := tc.payload, actualPayload; !reflect.DeepEqual(e, a) {
			t.Errorf("%v: expected and actual payload do not match", tc.name)
		}
	}
}

// newTestHost 创建临时根目录，并返回基于该目录的假 VolumeHost。
func newTestHost(t *testing.T, clientset clientset.Interface) (string, volume.VolumeHost) {
	tempDir, err := os.MkdirTemp("", "configmap_volume_test.")
	if err != nil {
		t.Fatalf("can't make a temp rootdir: %v", err)
	}

	return tempDir, volumetest.NewFakeVolumeHost(t, tempDir, clientset, emptydir.ProbeVolumePlugins())
}

// TestCanSupport 验证插件按名称查找，以及仅接受带 ConfigMap 源的卷规格。
func TestCanSupport(t *testing.T) {
	pluginMgr := volume.VolumePluginMgr{}
	tempDir, host := newTestHost(t, nil)
	defer os.RemoveAll(tempDir)
	// 本用例不涉及探针，传 nil 即可。
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, host)

	plugin, err := pluginMgr.FindPluginByName(configMapPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}
	if plugin.GetPluginName() != configMapPluginName {
		t.Errorf("Wrong name: %s", plugin.GetPluginName())
	}
	if !plugin.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: ""}}}}}) {
		t.Errorf("Expected true")
	}
	if plugin.CanSupport(&volume.Spec{}) {
		t.Errorf("Expected false")
	}
}

// TestPlugin 验证 ConfigMap 卷的完整生命周期：挂载、数据落盘、清理与卸载。
func TestPlugin(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_configmap_namespace"
		testName       = "test_configmap_name"

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		configMap     = configMap(testNamespace, testName)
		client        = fake.NewSimpleClientset(&configMap)
		pluginMgr     = volume.VolumePluginMgr{}
		tempDir, host = newTestHost(t, client)
	)

	defer os.RemoveAll(tempDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, host)

	plugin, err := pluginMgr.FindPluginByName(configMapPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}

	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, UID: testPodUID}}
	mounter, err := plugin.NewMounter(volume.NewSpecFromVolume(volumeSpec), pod)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Fatalf("Got a nil Mounter")
	}

	vName, err := plugin.GetVolumeName(volume.NewSpecFromVolume(volumeSpec))
	if err != nil {
		t.Errorf("Failed to GetVolumeName: %v", err)
	}
	if vName != "test_volume_name/test_configmap_name" {
		t.Errorf("Got unexpected VolumeName %v", vName)
	}

	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid/volumes/kubernetes.io~configmap/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	var mounterArgs volume.MounterArgs
	group := int64(1001)
	mounterArgs.FsGroup = &group
	err = mounter.SetUp(mounterArgs)
	if err != nil {
		t.Errorf("Failed to setup volume: %v", err)
	}
	if _, err := os.Stat(volumePath); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, volume path not created: %s", volumePath)
		} else {
			t.Errorf("SetUp() failed: %v", err)
		}
	}

	doTestConfigMapDataInVolume(volumePath, configMap, t)
	doTestCleanAndTeardown(plugin, testPodUID, testVolumeName, volumePath, t)
}

// TestPluginReboot 验证 ready 文件存在但卷目录并非挂载点的场景，即系统重启后的状态：
// 此时应重新挂载目录并向其中写入 ConfigMap 数据。
func TestPluginReboot(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid3")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_configmap_namespace"
		testName       = "test_configmap_name"

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		configMap     = configMap(testNamespace, testName)
		client        = fake.NewSimpleClientset(&configMap)
		pluginMgr     = volume.VolumePluginMgr{}
		rootDir, host = newTestHost(t, client)
	)

	defer os.RemoveAll(rootDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, host)

	plugin, err := pluginMgr.FindPluginByName(configMapPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}

	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, UID: testPodUID}}
	mounter, err := plugin.NewMounter(volume.NewSpecFromVolume(volumeSpec), pod)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Fatalf("Got a nil Mounter")
	}

	podMetadataDir := fmt.Sprintf("%v/pods/test_pod_uid3/plugins/kubernetes.io~configmap/test_volume_name", rootDir)
	// 提前放置 ready 文件，模拟重启后残留的元数据。
	util.SetReady(podMetadataDir)
	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid3/volumes/kubernetes.io~configmap/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	var mounterArgs volume.MounterArgs
	group := int64(1001)
	mounterArgs.FsGroup = &group
	err = mounter.SetUp(mounterArgs)
	if err != nil {
		t.Errorf("Failed to setup volume: %v", err)
	}
	if _, err := os.Stat(volumePath); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, volume path not created: %s", volumePath)
		} else {
			t.Errorf("SetUp() failed: %v", err)
		}
	}

	doTestConfigMapDataInVolume(volumePath, configMap, t)
	doTestCleanAndTeardown(plugin, testPodUID, testVolumeName, volumePath, t)
}

// TestPluginOptional 验证 ConfigMap 不存在且标记为 optional 时，卷仍能挂载为空目录。
func TestPluginOptional(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_configmap_namespace"
		testName       = "test_configmap_name"
		trueVal        = true

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		client        = fake.NewSimpleClientset()
		pluginMgr     = volume.VolumePluginMgr{}
		tempDir, host = newTestHost(t, client)
	)
	volumeSpec.VolumeSource.ConfigMap.Optional = &trueVal

	defer os.RemoveAll(tempDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, host)

	plugin, err := pluginMgr.FindPluginByName(configMapPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}

	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, UID: testPodUID}}
	mounter, err := plugin.NewMounter(volume.NewSpecFromVolume(volumeSpec), pod)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Errorf("Got a nil Mounter")
	}

	vName, err := plugin.GetVolumeName(volume.NewSpecFromVolume(volumeSpec))
	if err != nil {
		t.Errorf("Failed to GetVolumeName: %v", err)
	}
	if vName != "test_volume_name/test_configmap_name" {
		t.Errorf("Got unexpected VolumeName %v", vName)
	}

	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid/volumes/kubernetes.io~configmap/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	var mounterArgs volume.MounterArgs
	group := int64(1001)
	mounterArgs.FsGroup = &group
	err = mounter.SetUp(mounterArgs)
	if err != nil {
		t.Errorf("Failed to setup volume: %v", err)
	}
	if _, err := os.Stat(volumePath); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, volume path not created: %s", volumePath)
		} else {
			t.Errorf("SetUp() failed: %v", err)
		}
	}

	datadirSymlink := filepath.Join(volumePath, "..data")
	datadir, err := os.Readlink(datadirSymlink)
	if err != nil && os.IsNotExist(err) {
		t.Fatalf("couldn't find volume path's data dir, %s", datadirSymlink)
	} else if err != nil {
		t.Fatalf("couldn't read symlink, %s", datadirSymlink)
	}
	datadirPath := filepath.Join(volumePath, datadir)

	infos, err := ioutil.ReadDir(volumePath)
	if err != nil {
		t.Fatalf("couldn't find volume path, %s", volumePath)
	}
	if len(infos) != 0 {
		for _, fi := range infos {
			if fi.Name() != "..data" && fi.Name() != datadir {
				t.Errorf("empty data directory, %s, is not empty. Contains: %s", datadirSymlink, fi.Name())
			}
		}
	}

	infos, err = ioutil.ReadDir(datadirPath)
	if err != nil {
		t.Fatalf("couldn't find volume data path, %s", datadirPath)
	}
	if len(infos) != 0 {
		t.Errorf("empty data directory, %s, is not empty. Contains: %s", datadirSymlink, infos[0].Name())
	}

	doTestCleanAndTeardown(plugin, testPodUID, testVolumeName, volumePath, t)
}

// TestPluginKeysOptional 验证 optional 卷中缺失的键被跳过，其余键仍正常写入。
func TestPluginKeysOptional(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_configmap_namespace"
		testName       = "test_configmap_name"
		trueVal        = true

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		configMap     = configMap(testNamespace, testName)
		client        = fake.NewSimpleClientset(&configMap)
		pluginMgr     = volume.VolumePluginMgr{}
		tempDir, host = newTestHost(t, client)
	)
	volumeSpec.VolumeSource.ConfigMap.Items = []v1.KeyToPath{
		{Key: "data-1", Path: "data-1"},
		{Key: "data-2", Path: "data-2"},
		{Key: "data-3", Path: "data-3"},
		// optional 下该键应被忽略，不应导致挂载失败。
		{Key: "missing", Path: "missing"},
	}
	volumeSpec.VolumeSource.ConfigMap.Optional = &trueVal

	defer os.RemoveAll(tempDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, host)

	plugin, err := pluginMgr.FindPluginByName(configMapPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}

	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, UID: testPodUID}}
	mounter, err := plugin.NewMounter(volume.NewSpecFromVolume(volumeSpec), pod)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Errorf("Got a nil Mounter")
	}

	vName, err := plugin.GetVolumeName(volume.NewSpecFromVolume(volumeSpec))
	if err != nil {
		t.Errorf("Failed to GetVolumeName: %v", err)
	}
	if vName != "test_volume_name/test_configmap_name" {
		t.Errorf("Got unexpected VolumeName %v", vName)
	}

	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid/volumes/kubernetes.io~configmap/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	var mounterArgs volume.MounterArgs
	group := int64(1001)
	mounterArgs.FsGroup = &group
	err = mounter.SetUp(mounterArgs)
	if err != nil {
		t.Errorf("Failed to setup volume: %v", err)
	}
	if _, err := os.Stat(volumePath); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, volume path not created: %s", volumePath)
		} else {
			t.Errorf("SetUp() failed: %v", err)
		}
	}

	doTestConfigMapDataInVolume(volumePath, configMap, t)
	doTestCleanAndTeardown(plugin, testPodUID, testVolumeName, volumePath, t)
}

// volumeSpec 构造引用指定 ConfigMap 的卷规格。
func volumeSpec(volumeName, configMapName string, defaultMode int32) *v1.Volume {
	return &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			ConfigMap: &v1.ConfigMapVolumeSource{
				LocalObjectReference: v1.LocalObjectReference{
					Name: configMapName,
				},
				DefaultMode: &defaultMode,
			},
		},
	}
}

// TestInvalidConfigMapSetup 验证引用不存在的键时挂载失败，且不残留卷目录。
func TestInvalidConfigMapSetup(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_configmap_namespace"
		testName       = "test_configmap_name"

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		configMap     = configMap(testNamespace, testName)
		client        = fake.NewSimpleClientset(&configMap)
		pluginMgr     = volume.VolumePluginMgr{}
		tempDir, host = newTestHost(t, client)
	)
	volumeSpec.VolumeSource.ConfigMap.Items = []v1.KeyToPath{
		// 非 optional 卷引用缺失键，SetUp 应失败。
		{Key: "missing", Path: "missing"},
	}

	defer os.RemoveAll(tempDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, host)

	plugin, err := pluginMgr.FindPluginByName(configMapPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}

	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, UID: testPodUID}}
	mounter, err := plugin.NewMounter(volume.NewSpecFromVolume(volumeSpec), pod)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Errorf("Got a nil Mounter")
	}

	vName, err := plugin.GetVolumeName(volume.NewSpecFromVolume(volumeSpec))
	if err != nil {
		t.Errorf("Failed to GetVolumeName: %v", err)
	}
	if vName != "test_volume_name/test_configmap_name" {
		t.Errorf("Got unexpected VolumeName %v", vName)
	}

	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid/volumes/kubernetes.io~configmap/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	var mounterArgs volume.MounterArgs
	group := int64(1001)
	mounterArgs.FsGroup = &group
	err = mounter.SetUp(mounterArgs)
	if err == nil {
		t.Errorf("Expected setup to fail")
	}
	_, err = os.Stat(volumePath)
	if err == nil {
		t.Errorf("Expected %s to not exist", volumePath)
	}

	doTestCleanAndTeardown(plugin, testPodUID, testVolumeName, volumePath, t)
}

// configMap 构造包含三个数据键值对的测试用 ConfigMap。
func configMap(namespace, name string) v1.ConfigMap {
	return v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string]string{
			"data-1": "value-1",
			"data-2": "value-2",
			"data-3": "value-3",
		},
	}
}

// doTestConfigMapDataInVolume 逐键核对 ConfigMap 数据已按预期内容写入卷目录。
func doTestConfigMapDataInVolume(volumePath string, configMap v1.ConfigMap, t *testing.T) {
	for key, value := range configMap.Data {
		// 先确认文件存在，再读取内容，便于区分缺失与内容不符。
		configMapDataHostPath := filepath.Join(volumePath, key)
		if _, err := os.Stat(configMapDataHostPath); err != nil {
			t.Fatalf("SetUp() failed, couldn't find configMap data on disk: %v", configMapDataHostPath)
		} else {
			actualValue, err := os.ReadFile(configMapDataHostPath)
			if err != nil {
				t.Fatalf("Couldn't read configMap data from: %v", configMapDataHostPath)
			}

			if value != string(actualValue) {
				t.Errorf("Unexpected value; expected %q, got %q", value, actualValue)
			}
		}
	}
}

// doTestCleanAndTeardown 卸载卷并确认卷目录已被清除。
func doTestCleanAndTeardown(plugin volume.VolumePlugin, podUID types.UID, testVolumeName, volumePath string, t *testing.T) {
	unmounter, err := plugin.NewUnmounter(testVolumeName, podUID)
	if err != nil {
		t.Errorf("Failed to make a new Unmounter: %v", err)
	}
	if unmounter == nil {
		t.Fatalf("Got a nil Unmounter")
	}

	if err := unmounter.TearDown(); err != nil {
		t.Errorf("Expected success, got: %v", err)
	}
	if _, err := os.Stat(volumePath); err == nil {
		t.Errorf("TearDown() failed, volume path still exists: %s", volumePath)
	} else if !os.IsNotExist(err) {
		t.Errorf("TearDown() failed: %v", err)
	}
}

// hasPathSuffix 用平台相关的分隔符判断路径后缀，避免硬编码斜杠。
func hasPathSuffix(s, suffix string) bool {
	return strings.HasSuffix(s, filepath.FromSlash(suffix))
}
