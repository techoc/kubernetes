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

// Package downwardapi 包含 downwardAPI 卷插件的单元测试。
package downwardapi

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	utiltesting "k8s.io/client-go/util/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/fieldpath"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
)

const (
	// downwardAPIDir 是 downwardAPI 卷存放数据的符号链接目录名。
	downwardAPIDir = "..data"
	// testPodUID 是测试中使用的固定 Pod UID。
	testPodUID = types.UID("test_pod_uid")
	// testNamespace 是测试中使用的固定命名空间。
	testNamespace = "test_metadata_namespace"
	// testName 是测试中使用的固定 Pod 名称。
	testName = "test_metadata_name"
)

// newTestHost 创建临时根目录及基于该目录、携带指定客户端的假卷主机。
func newTestHost(t *testing.T, clientset clientset.Interface) (string, volume.VolumeHost) {
	tempDir, err := utiltesting.MkTmpdir("downwardApi_volume_test.")
	if err != nil {
		t.Fatalf("can't make a temp rootdir: %v", err)
	}
	return tempDir, volumetest.NewFakeVolumeHost(t, tempDir, clientset, emptydir.ProbeVolumePlugins())
}

// TestCanSupport 验证 downwardAPI 插件按名可找到，且只支持 downwardAPI 卷源。
func TestCanSupport(t *testing.T) {
	pluginMgr := volume.VolumePluginMgr{}
	tmpDir, host := newTestHost(t, nil)
	defer os.RemoveAll(tmpDir)
	// 这里不探测插件健康状态，传 nil 探针即可。
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探针 */, host)

	plugin, err := pluginMgr.FindPluginByName(downwardAPIPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}
	if plugin.GetPluginName() != downwardAPIPluginName {
		t.Errorf("Wrong name: %s", plugin.GetPluginName())
	}
	if !plugin.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{DownwardAPI: &v1.DownwardAPIVolumeSource{}}}}) {
		t.Errorf("Expected true")
	}
	if plugin.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{}}}) {
		t.Errorf("Expected false")
	}
}

// TestDownwardAPI 覆盖标签、注解、名称、命名空间、路径及权限等投影场景。
func TestDownwardAPI(t *testing.T) {
	// 按 2023-01-10 SIG Testing 会议结论，在 Windows 上跳过这些测试。
	if runtime.GOOS == "windows" {
		t.Skip("Skipping test that fails on Windows")
	}

	labels1 := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	labels2 := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	annotations := map[string]string{
		"a1":        "value1",
		"a2":        "value2",
		"multiline": "c\nb\na",
	}
	testCases := []struct {
		name           string
		files          map[string]string
		modes          map[string]int32
		podLabels      map[string]string
		podAnnotations map[string]string
		steps          []testStep
	}{
		{
			name:      "test_labels",
			files:     map[string]string{"labels": "metadata.labels"},
			podLabels: labels1,
			steps: []testStep{
				// 涉及文件的步骤中，stepName 同时也是待校验的文件名。
				verifyMapInFile{stepName{"labels"}, labels1},
			},
		},
		{
			name:           "test_annotations",
			files:          map[string]string{"annotations": "metadata.annotations"},
			podAnnotations: annotations,
			steps: []testStep{
				verifyMapInFile{stepName{"annotations"}, annotations},
			},
		},
		{
			name:  "test_name",
			files: map[string]string{"name_file_name": "metadata.name"},
			steps: []testStep{
				verifyLinesInFile{stepName{"name_file_name"}, testName},
			},
		},
		{
			name:  "test_namespace",
			files: map[string]string{"namespace_file_name": "metadata.namespace"},
			steps: []testStep{
				verifyLinesInFile{stepName{"namespace_file_name"}, testNamespace},
			},
		},
		{
			name:      "test_write_twice_no_update",
			files:     map[string]string{"labels": "metadata.labels"},
			podLabels: labels1,
			steps: []testStep{
				reSetUp{stepName{"resetup"}, false, nil},
				verifyMapInFile{stepName{"labels"}, labels1},
			},
		},
		{
			name:      "test_write_twice_with_update",
			files:     map[string]string{"labels": "metadata.labels"},
			podLabels: labels1,
			steps: []testStep{
				reSetUp{stepName{"resetup"}, true, labels2},
				verifyMapInFile{stepName{"labels"}, labels2},
			},
		},
		{
			name: "test_write_with_unix_path",
			files: map[string]string{
				"these/are/my/labels":        "metadata.labels",
				"these/are/your/annotations": "metadata.annotations",
			},
			podLabels:      labels1,
			podAnnotations: annotations,
			steps: []testStep{
				verifyMapInFile{stepName{"these/are/my/labels"}, labels1},
				verifyMapInFile{stepName{"these/are/your/annotations"}, annotations},
			},
		},
		{
			name:      "test_write_with_two_consecutive_slashes_in_the_path",
			files:     map[string]string{"this//labels": "metadata.labels"},
			podLabels: labels1,
			steps: []testStep{
				verifyMapInFile{stepName{"this/labels"}, labels1},
			},
		},
		{
			name:  "test_default_mode",
			files: map[string]string{"name_file_name": "metadata.name"},
			steps: []testStep{
				verifyMode{stepName{"name_file_name"}, 0644},
			},
		},
		{
			name:  "test_item_mode",
			files: map[string]string{"name_file_name": "metadata.name"},
			modes: map[string]int32{"name_file_name": 0400},
			steps: []testStep{
				verifyMode{stepName{"name_file_name"}, 0400},
			},
		},
	}
	for _, testCase := range testCases {
		test := newDownwardAPITest(t, testCase.name, testCase.files, testCase.podLabels, testCase.podAnnotations, testCase.modes)
		for _, step := range testCase.steps {
			test.t.Logf("Test case: %q Step: %q", testCase.name, step.getName())
			step.run(test)
		}
		test.tearDown()
	}
}

// TestCollectDataWithUser 验证默认用户、显式用户及特性开关对文件属主的影响。
func TestCollectDataWithUser(t *testing.T) {
	caseMappingUser1 := int64(1001)
	caseMappingUser2 := int64(1002)

	cases := []struct {
		name     string
		mappings []v1.DownwardAPIVolumeFile
		mode     int32
		user     *int64
		payload  map[string]util.FileProjection

		disableUserFieldsGate bool
	}{
		{
			name: "mapping with defaultUser",
			mappings: []v1.DownwardAPIVolumeFile{
				{
					Path: "namespace_file_name",
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
			mode: 0644,
			user: &caseMappingUser1,
			payload: map[string]util.FileProjection{
				"namespace_file_name": {Data: []byte(testNamespace), Mode: 0644, FsUser: &caseMappingUser1},
			},
		},
		{
			name: "mapping with User",
			mappings: []v1.DownwardAPIVolumeFile{
				{
					Path: "namespace_file_name",
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
					User: &caseMappingUser1,
				},
			},
			mode: 0644,
			payload: map[string]util.FileProjection{
				"namespace_file_name": {Data: []byte(testNamespace), Mode: 0644, FsUser: &caseMappingUser1},
			},
		},
		{
			name: "mapping with defaultUser and User",
			mappings: []v1.DownwardAPIVolumeFile{
				{
					Path: "namespace_file_name",
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
					User: &caseMappingUser2,
				},
			},
			mode: 0644,
			user: &caseMappingUser1,
			payload: map[string]util.FileProjection{
				"namespace_file_name": {Data: []byte(testNamespace), Mode: 0644, FsUser: &caseMappingUser2},
			},
		},
		{
			name: "user fields with disabled feature gate",
			mappings: []v1.DownwardAPIVolumeFile{
				{
					Path: "namespace_file_name",
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
					User: &caseMappingUser2,
				},
			},
			mode: 0644,
			user: &caseMappingUser1,
			payload: map[string]util.FileProjection{
				"namespace_file_name": {Data: []byte(testNamespace), Mode: 0644},
			},
			disableUserFieldsGate: true,
		},
	}

	for _, tc := range cases {
		// 关闭特性时锁定仿真版本，确保用户字段被忽略。
		if tc.disableUserFieldsGate {
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.37"))
		}
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AtomicWriteVolumeUserFields, !tc.disableUserFieldsGate)

		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testName,
				Namespace: testNamespace,
				UID:       testPodUID,
			},
		}
		clientset := fake.NewSimpleClientset(&pod)
		tempDir, host := newTestHost(t, clientset)
		defer func() {
			if err := os.RemoveAll(tempDir); err != nil {
				t.Fatal(err)
			}
		}()

		actualPayload, err := CollectData(tc.mappings, &pod, host, &tc.mode, tc.user)
		if err != nil {
			t.Errorf("%v: unexpected failure making payload: %v", tc.name, err)
			continue
		}
		if e, a := tc.payload, actualPayload; !reflect.DeepEqual(e, a) {
			t.Errorf("%v: expected and actual payload do not match", tc.name)
		}
	}
}

// downwardAPITest 保存一个挂载好的 downwardAPI 卷及其测试上下文。
type downwardAPITest struct {
	t          *testing.T
	name       string
	plugin     volume.VolumePlugin
	pod        *v1.Pod
	mounter    volume.Mounter
	volumePath string
	rootDir    string
}

// newDownwardAPITest 根据文件映射和 Pod 元数据创建并挂载一个 downwardAPI 卷。
func newDownwardAPITest(t *testing.T, name string, volumeFiles, podLabels, podAnnotations map[string]string, modes map[string]int32) *downwardAPITest {
	defaultMode := int32(0644)
	var files []v1.DownwardAPIVolumeFile
	for path, fieldPath := range volumeFiles {
		file := v1.DownwardAPIVolumeFile{
			Path: path,
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: fieldPath,
			},
		}
		if mode, found := modes[path]; found {
			file.Mode = &mode
		}
		files = append(files, file)
	}
	podMeta := metav1.ObjectMeta{
		Name:        testName,
		Namespace:   testNamespace,
		Labels:      podLabels,
		Annotations: podAnnotations,
	}
	clientset := fake.NewSimpleClientset(&v1.Pod{ObjectMeta: podMeta})

	pluginMgr := volume.VolumePluginMgr{}
	rootDir, host := newTestHost(t, clientset)
	// 无需健康探测，传 nil 探针即可。
	pluginMgr.InitPlugins(ProbeVolumePlugins(), nil /* 探针 */, host)
	plugin, err := pluginMgr.FindPluginByName(downwardAPIPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}

	volumeSpec := &v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			DownwardAPI: &v1.DownwardAPIVolumeSource{
				DefaultMode: &defaultMode,
				Items:       files,
			},
		},
	}
	podMeta.UID = testPodUID
	pod := &v1.Pod{ObjectMeta: podMeta}
	mounter, err := plugin.NewMounter(volume.NewSpecFromVolume(volumeSpec), pod)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Fatalf("Got a nil Mounter")
	}

	volumePath := mounter.GetPath()

	err = mounter.SetUp(volume.MounterArgs{})
	if err != nil {
		t.Errorf("Failed to setup volume: %v", err)
	}

	// downwardAPI 卷应创建自己的空包装目录，用于存放卷元数据。
	podWrapperMetadataDir := fmt.Sprintf("%v/pods/%v/plugins/kubernetes.io~empty-dir/wrapped_%v", rootDir, testPodUID, name)

	if _, err := os.Stat(podWrapperMetadataDir); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, empty-dir wrapper path was not created: %s", podWrapperMetadataDir)
		} else {
			t.Errorf("SetUp() failed: %v", err)
		}
	}

	return &downwardAPITest{
		t:          t,
		plugin:     plugin,
		pod:        pod,
		mounter:    mounter,
		volumePath: volumePath,
		rootDir:    rootDir,
	}
}

// tearDown 卸载卷并清理测试临时目录。
func (test *downwardAPITest) tearDown() {
	unmounter, err := test.plugin.NewUnmounter(test.name, testPodUID)
	if err != nil {
		test.t.Errorf("Failed to make a new Unmounter: %v", err)
	}
	if unmounter == nil {
		test.t.Fatalf("Got a nil Unmounter")
	}

	if err := unmounter.TearDown(); err != nil {
		test.t.Errorf("Expected success, got: %v", err)
	}
	if _, err := os.Stat(test.volumePath); err == nil {
		test.t.Errorf("TearDown() failed, volume path still exists: %s", test.volumePath)
	} else if !os.IsNotExist(err) {
		test.t.Errorf("TearDown() failed: %v", err)
	}
	os.RemoveAll(test.rootDir)
}

// testStep 表示 downwardAPITest 中的一个命名步骤。
// 涉及文件的步骤中，步骤名同时作为该步骤要操作的文件名。
type testStep interface {
	// getName 返回步骤名称。
	getName() string
	// run 执行该步骤。
	run(*downwardAPITest)
}

// stepName 为步骤提供名称。
type stepName struct {
	name string
}

// getName 返回步骤名称。
func (step stepName) getName() string { return step.name }

// doVerifyLinesInFile 校验指定文件内容与期望值一致。
func doVerifyLinesInFile(t *testing.T, volumePath, filename string, expected string) {
	data, err := os.ReadFile(filepath.Join(volumePath, filename))
	if err != nil {
		t.Error(err.Error())
		return
	}
	actualStr := string(data)
	expectedStr := expected
	if actualStr != expectedStr {
		t.Errorf("Found `%s`, expected `%s`", actualStr, expectedStr)
	}
}

// verifyLinesInFile 校验文件中的文本与期望值一致。
type verifyLinesInFile struct {
	stepName
	expected string
}

// run 执行文件内容校验。
func (step verifyLinesInFile) run(test *downwardAPITest) {
	doVerifyLinesInFile(test.t, test.volumePath, step.name, step.expected)
}

// verifyMapInFile 校验文件中格式化的映射与期望值一致。
type verifyMapInFile struct {
	stepName
	expected map[string]string
}

// run 执行映射内容校验。
func (step verifyMapInFile) run(test *downwardAPITest) {
	doVerifyLinesInFile(test.t, test.volumePath, step.name, fieldpath.FormatMap(step.expected))
}

// verifyMode 校验文件权限与期望模式一致。
type verifyMode struct {
	stepName
	expectedMode int32
}

// run 执行文件权限校验。
func (step verifyMode) run(test *downwardAPITest) {
	fileInfo, err := os.Stat(filepath.Join(test.volumePath, step.name))
	if err != nil {
		test.t.Error(err.Error())
		return
	}

	actualMode := fileInfo.Mode()
	expectedMode := os.FileMode(step.expectedMode)
	if actualMode != expectedMode {
		test.t.Errorf("Found mode `%v` expected %v", actualMode, expectedMode)
	}
}

// reSetUp 表示一次重复 SetUp：可更新标签，并校验数据链接是否变化。
type reSetUp struct {
	stepName
	linkShouldChange bool
	newLabels        map[string]string
}

// run 重新执行 SetUp 并校验符号链接是否按预期变化。
func (step reSetUp) run(test *downwardAPITest) {
	if step.newLabels != nil {
		test.pod.ObjectMeta.Labels = step.newLabels
	}

	currentTarget, err := os.Readlink(filepath.Join(test.volumePath, downwardAPIDir))
	if err != nil {
		test.t.Errorf("labels file should be a link... %s\n", err.Error())
	}

	// 再次 SetUp，验证原子写入下数据链接的更新行为。
	if err = test.mounter.SetUp(volume.MounterArgs{}); err != nil {
		test.t.Errorf("Failed to re-setup volume: %v", err)
	}

	// 再次读取链接目标，与 SetUp 前比较以判断是否发生更新。
	currentTarget2, err := os.Readlink(filepath.Join(test.volumePath, downwardAPIDir))
	if err != nil {
		test.t.Errorf(".current should be a link... %s\n", err.Error())
	}

	switch {
	case step.linkShouldChange && currentTarget2 == currentTarget:
		test.t.Errorf("Got and update between the two Setup... Target link should NOT be the same\n")
	case !step.linkShouldChange && currentTarget2 != currentTarget:
		test.t.Errorf("No update between the two Setup... Target link should be the same %s %s\n",
			currentTarget, currentTarget2)
	}
}
