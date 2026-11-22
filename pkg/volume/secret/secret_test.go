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

package secret

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
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

	"github.com/stretchr/testify/assert"
)

// TestMakePayload 验证 MakePayload 按 items 映射生成文件投影，并正确处理默认权限、用户及特性门控。
func TestMakePayload(t *testing.T) {
	caseMappingMode := int32(0400)
	caseMappingUser1 := int64(1001)
	caseMappingUser2 := int64(1002)

	cases := []struct {
		name     string
		mappings []v1.KeyToPath
		secret   *v1.Secret
		mode     int32
		user     *int64
		optional bool
		payload  map[string]util.FileProjection
		success  bool

		disableUserFieldsGate bool
	}{
		{
			name: "no overrides",
			secret: &v1.Secret{
				Data: map[string][]byte{
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
			name: "basic 1",
			mappings: []v1.KeyToPath{
				{
					Key:  "foo",
					Path: "path/to/foo.txt",
				},
			},
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
			secret: &v1.Secret{
				Data: map[string][]byte{
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
			name:     "empty mappings with defaultUser",
			mappings: []v1.KeyToPath{},
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
				},
			},
			mode: 0644,
			user: &caseMappingUser1,
			payload: map[string]util.FileProjection{
				"foo": {Data: []byte("foo"), Mode: 0644, FsUser: &caseMappingUser1},
				"bar": {Data: []byte("bar"), Mode: 0644, FsUser: &caseMappingUser1},
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
			secret: &v1.Secret{
				Data: map[string][]byte{
					"foo": []byte("foo"),
					"bar": []byte("bar"),
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
		// 关闭用户字段时需回退模拟版本，否则门控默认值会随版本变化。
		if tc.disableUserFieldsGate {
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.37"))
		}
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AtomicWriteVolumeUserFields, !tc.disableUserFieldsGate)

		actualPayload, err := MakePayload(tc.mappings, tc.secret, &tc.mode, tc.user, tc.optional)
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

		if e, a := tc.payload, actualPayload; !reflect.DeepEqual(e, a) {
			t.Errorf("%v: expected and actual payload do not match", tc.name)
		}
	}
}

// newTestHost 创建临时根目录及其对应的 FakeVolumeHost，调用方负责清理返回目录。
func newTestHost(t *testing.T, clientset clientset.Interface) (string, volume.VolumeHost) {
	tempDir, err := os.MkdirTemp("", "secret_volume_test.")
	if err != nil {
		t.Fatalf("can't make a temp rootdir: %v", err)
	}

	return tempDir, volumetest.NewFakeVolumeHost(t, tempDir, clientset, emptydir.ProbeVolumePlugins())
}

// TestCanSupport 验证 secret 插件只识别带 Secret 源的卷。
func TestCanSupport(t *testing.T) {
	pluginMgr := volume.VolumePluginMgr{}
	tempDir, host := newTestHost(t, nil)
	defer os.RemoveAll(tempDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, host)

	plugin, err := pluginMgr.FindPluginByName(secretPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}
	if plugin.GetPluginName() != secretPluginName {
		t.Errorf("Wrong name: %s", plugin.GetPluginName())
	}
	if !plugin.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{Secret: &v1.SecretVolumeSource{SecretName: ""}}}}) {
		t.Errorf("Expected true")
	}
	if plugin.CanSupport(&volume.Spec{}) {
		t.Errorf("Expected false")
	}
}

// TestPlugin 验证 secret 卷的挂载路径、数据写入、包装目录与指标，并验证卸载清理。
func TestPlugin(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_secret_namespace"
		testName       = "test_secret_name"

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		secret        = secret(testNamespace, testName)
		client        = fake.NewSimpleClientset(&secret)
		pluginMgr     = volume.VolumePluginMgr{}
		rootDir, host = newTestHost(t, client)
	)
	defer os.RemoveAll(rootDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, host)

	plugin, err := pluginMgr.FindPluginByName(secretPluginName)
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

	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid/volumes/kubernetes.io~secret/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	err = mounter.SetUp(volume.MounterArgs{})
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

	// secret 卷通过 empty-dir 插件创建包装目录，用于挂载点重建时的元数据恢复。
	podWrapperMetadataDir := fmt.Sprintf("%v/pods/test_pod_uid/plugins/kubernetes.io~empty-dir/wrapped_test_volume_name", rootDir)

	if _, err := os.Stat(podWrapperMetadataDir); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, empty-dir wrapper path is not created: %s", podWrapperMetadataDir)
		} else {
			t.Errorf("SetUp() failed: %v", err)
		}
	}
	doTestSecretDataInVolume(volumePath, secret, t)
	defer doTestCleanAndTeardown(plugin, testPodUID, testVolumeName, volumePath, t)

	// 卷指标目前只在 Linux 上提供。
	metrics, err := mounter.GetMetrics()
	if runtime.GOOS == "linux" {
		assert.NotEmpty(t, metrics)
		assert.NoError(t, err)
	} else {
		t.Skipf("Volume metrics not supported on %s", runtime.GOOS)
	}
}

// TestInvalidPathSecret 验证映射中不存在的 key 会导致挂载失败且不留下卷目录。
func TestInvalidPathSecret(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_secret_namespace"
		testName       = "test_secret_name"

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		secret        = secret(testNamespace, testName)
		client        = fake.NewSimpleClientset(&secret)
		pluginMgr     = volume.VolumePluginMgr{}
		rootDir, host = newTestHost(t, client)
	)
	volumeSpec.Secret.Items = []v1.KeyToPath{
		{Key: "missing", Path: "missing"},
	}

	defer os.RemoveAll(rootDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, host)

	plugin, err := pluginMgr.FindPluginByName(secretPluginName)
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

	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid/volumes/kubernetes.io~secret/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	var mounterArgs volume.MounterArgs
	err = mounter.SetUp(mounterArgs)
	if err == nil {
		t.Errorf("Expected error while setting up secret")
	}

	_, err = os.Stat(volumePath)
	if err == nil {
		t.Errorf("Expected path %s to not exist", volumePath)
	}
}

// TestPluginReboot 模拟重启后的状态：ready 文件仍在但卷目录不是挂载点，
// 此时应重新挂载卷目录并把 secret 数据写入其中。
func TestPluginReboot(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid3")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_secret_namespace"
		testName       = "test_secret_name"

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		secret        = secret(testNamespace, testName)
		client        = fake.NewSimpleClientset(&secret)
		pluginMgr     = volume.VolumePluginMgr{}
		rootDir, host = newTestHost(t, client)
	)
	defer os.RemoveAll(rootDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, host)

	plugin, err := pluginMgr.FindPluginByName(secretPluginName)
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

	podMetadataDir := fmt.Sprintf("%v/pods/test_pod_uid3/plugins/kubernetes.io~secret/test_volume_name", rootDir)
	util.SetReady(podMetadataDir)
	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid3/volumes/kubernetes.io~secret/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	err = mounter.SetUp(volume.MounterArgs{})
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

	doTestSecretDataInVolume(volumePath, secret, t)
	doTestCleanAndTeardown(plugin, testPodUID, testVolumeName, volumePath, t)
}

// TestPluginOptional 验证 secret 不存在但标记为 optional 时，挂载成功且卷目录为空。
func TestPluginOptional(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_secret_namespace"
		testName       = "test_secret_name"
		trueVal        = true

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		client        = fake.NewSimpleClientset()
		pluginMgr     = volume.VolumePluginMgr{}
		rootDir, host = newTestHost(t, client)
	)
	volumeSpec.Secret.Optional = &trueVal
	defer os.RemoveAll(rootDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, host)

	plugin, err := pluginMgr.FindPluginByName(secretPluginName)
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

	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid/volumes/kubernetes.io~secret/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	err = mounter.SetUp(volume.MounterArgs{})
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

	// secret 卷通过 empty-dir 插件创建包装目录，用于挂载点重建时的元数据恢复。
	podWrapperMetadataDir := fmt.Sprintf("%v/pods/test_pod_uid/plugins/kubernetes.io~empty-dir/wrapped_test_volume_name", rootDir)

	if _, err := os.Stat(podWrapperMetadataDir); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, empty-dir wrapper path is not created: %s", podWrapperMetadataDir)
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
				t.Errorf("empty data volume directory, %s, is not empty. Contains: %s", datadirSymlink, fi.Name())
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

	defer doTestCleanAndTeardown(plugin, testPodUID, testVolumeName, volumePath, t)
}

// TestPluginOptionalKeys 验证 optional 卷中缺失的 key 被跳过，其余 key 正常写入且不报错。
func TestPluginOptionalKeys(t *testing.T) {
	var (
		testPodUID     = types.UID("test_pod_uid")
		testVolumeName = "test_volume_name"
		testNamespace  = "test_secret_namespace"
		testName       = "test_secret_name"
		trueVal        = true

		volumeSpec    = volumeSpec(testVolumeName, testName, 0644)
		secret        = secret(testNamespace, testName)
		client        = fake.NewSimpleClientset(&secret)
		pluginMgr     = volume.VolumePluginMgr{}
		rootDir, host = newTestHost(t, client)
	)
	volumeSpec.VolumeSource.Secret.Items = []v1.KeyToPath{
		{Key: "data-1", Path: "data-1"},
		{Key: "data-2", Path: "data-2"},
		{Key: "data-3", Path: "data-3"},
		{Key: "missing", Path: "missing"},
	}
	volumeSpec.Secret.Optional = &trueVal
	defer os.RemoveAll(rootDir)
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探测器 */, host)

	plugin, err := pluginMgr.FindPluginByName(secretPluginName)
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

	volumePath := mounter.GetPath()
	if !hasPathSuffix(volumePath, "pods/test_pod_uid/volumes/kubernetes.io~secret/test_volume_name") {
		t.Errorf("Got unexpected path: %s", volumePath)
	}

	err = mounter.SetUp(volume.MounterArgs{})
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

	// secret 卷通过 empty-dir 插件创建包装目录，用于挂载点重建时的元数据恢复。
	podWrapperMetadataDir := fmt.Sprintf("%v/pods/test_pod_uid/plugins/kubernetes.io~empty-dir/wrapped_test_volume_name", rootDir)

	if _, err := os.Stat(podWrapperMetadataDir); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, empty-dir wrapper path is not created: %s", podWrapperMetadataDir)
		} else {
			t.Errorf("SetUp() failed: %v", err)
		}
	}
	doTestSecretDataInVolume(volumePath, secret, t)
	defer doTestCleanAndTeardown(plugin, testPodUID, testVolumeName, volumePath, t)

	// 卷指标目前只在 Linux 上提供。
	metrics, err := mounter.GetMetrics()
	if runtime.GOOS == "linux" {
		assert.NotEmpty(t, metrics)
		assert.NoError(t, err)
	} else {
		t.Skipf("Volume metrics not supported on %s", runtime.GOOS)
	}
}

// volumeSpec 构造引用指定 secret 的卷规格，并设置默认文件权限。
func volumeSpec(volumeName, secretName string, defaultMode int32) *v1.Volume {
	return &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				SecretName:  secretName,
				DefaultMode: &defaultMode,
			},
		},
	}
}

// secret 构造包含三个测试键值对的 Secret 对象。
func secret(namespace, name string) v1.Secret {
	return v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string][]byte{
			"data-1": []byte("value-1"),
			"data-2": []byte("value-2"),
			"data-3": []byte("value-3"),
		},
	}
}

// doTestSecretDataInVolume 逐项核对 secret 数据已按 key 写入卷目录且内容一致。
func doTestSecretDataInVolume(volumePath string, secret v1.Secret, t *testing.T) {
	for key, value := range secret.Data {
		secretDataHostPath := filepath.Join(volumePath, key)
		if _, err := os.Stat(secretDataHostPath); err != nil {
			t.Fatalf("SetUp() failed, couldn't find secret data on disk: %v", secretDataHostPath)
		} else {
			actualSecretBytes, err := os.ReadFile(secretDataHostPath)
			if err != nil {
				t.Fatalf("Couldn't read secret data from: %v", secretDataHostPath)
			}

			actualSecretValue := string(actualSecretBytes)
			if string(value) != actualSecretValue {
				t.Errorf("Unexpected value; expected %q, got %q", value, actualSecretValue)
			}
		}
	}
}

// doTestCleanAndTeardown 卸载卷并确认卷目录已被移除。
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

// hasPathSuffix 按当前平台的路径分隔符判断后缀，兼容 Windows 等非 Unix 平台。
func hasPathSuffix(s, suffix string) bool {
	return strings.HasSuffix(s, filepath.FromSlash(suffix))
}
