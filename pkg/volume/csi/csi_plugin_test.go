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

package csi

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	api "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	utiltesting "k8s.io/client-go/util/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
)

const (
	// volumeHostType 表示使用通用 FakeVolumeHost。
	volumeHostType int = iota
	// kubeletVolumeHostType 表示使用 kubelet 侧 FakeVolumeHost。
	kubeletVolumeHostType
	// attachDetachVolumeHostType 表示使用 attach-detach 侧 FakeVolumeHost。
	attachDetachVolumeHostType
)

// newTestPlugin 用 kubelet 侧 VolumeHost 构造测试用 csiPlugin，并返回插件与临时目录。
func newTestPlugin(t *testing.T, client *fakeclient.Clientset) (*csiPlugin, string) {
	return newTestPluginWithVolumeHost(t, client, kubeletVolumeHostType)
}

// newTestPluginWithAttachDetachVolumeHost 用 attach-detach 侧 VolumeHost 构造测试插件。
func newTestPluginWithAttachDetachVolumeHost(t *testing.T, client *fakeclient.Clientset) (*csiPlugin, string) {
	return newTestPluginWithVolumeHost(t, client, attachDetachVolumeHostType)
}

// newTestPluginWithVolumeHost 创建插件管理器并加载插件，同时配置 fake 客户端。
// hostType 决定采用哪种 VolumeHost 实现，以覆盖不同调用方的行为。
func newTestPluginWithVolumeHost(t *testing.T, client *fakeclient.Clientset, hostType int) (*csiPlugin, string) {
	tmpDir, err := utiltesting.MkTmpdir("csi-test")
	if err != nil {
		t.Fatalf("can't create temp dir: %v", err)
	}

	if client == nil {
		client = fakeclient.NewSimpleClientset()
	}

	// 预置 fakeNode，部分 VolumeHost 实现会按节点名查找 Node 对象。
	client.Tracker().Add(&api.Node{
		ObjectMeta: meta.ObjectMeta{
			Name: "fakeNode",
		},
		Spec: api.NodeSpec{},
	})

	// 启动 informer 并等待缓存同步，插件查询依赖 informer 的本地缓存。
	factory := informers.NewSharedInformerFactory(client, CsiResyncPeriod)
	csiDriverInformer := factory.Storage().V1().CSIDrivers()
	csiDriverLister := csiDriverInformer.Lister()
	volumeAttachmentInformer := factory.Storage().V1().VolumeAttachments()
	volumeAttachmentLister := volumeAttachmentInformer.Lister()

	factory.Start(wait.NeverStop)
	syncedTypes := factory.WaitForCacheSync(wait.NeverStop)
	if len(syncedTypes) != 2 {
		t.Fatalf("informers are not synced")
	}
	for ty, ok := range syncedTypes {
		if !ok {
			t.Fatalf("failed to sync: %#v", ty)
		}
	}

	var host volume.VolumeHost
	switch hostType {
	case volumeHostType:
		host = volumetest.NewFakeVolumeHostWithCSINodeName(t,
			tmpDir,
			client,
			ProbeVolumePlugins(),
			"fakeNode",
			csiDriverLister,
			nil,
		)
	case kubeletVolumeHostType:
		host = volumetest.NewFakeKubeletVolumeHostWithCSINodeName(t,
			tmpDir,
			client,
			ProbeVolumePlugins(),
			"fakeNode",
			csiDriverLister,
			volumeAttachmentLister,
		)
	case attachDetachVolumeHostType:
		host = volumetest.NewFakeAttachDetachVolumeHostWithCSINodeName(t,
			tmpDir,
			client,
			ProbeVolumePlugins(),
			"fakeNode",
			csiDriverLister,
			volumeAttachmentLister,
		)
	default:
		t.Fatalf("Unsupported volume host type")
	}

	fakeHost, ok := host.(volumetest.FakeVolumeHost)
	if !ok {
		t.Fatalf("Unsupported volume host type")
	}

	pluginMgr := fakeHost.GetPluginMgr()
	plug, err := pluginMgr.FindPluginByName(CSIPluginName)
	if err != nil {
		t.Fatalf("can't find plugin %v", CSIPluginName)
	}

	csiPlug, ok := plug.(*csiPlugin)
	if !ok {
		t.Fatalf("cannot assert plugin to be type csiPlugin")
	}

	return csiPlug, tmpDir
}

// registerFakePlugin 在全局 csiDrivers 中注册一个 fake CSI 驱动，供插件查询使用。
func registerFakePlugin(pluginName, endpoint string, versions []string, t *testing.T) {
	highestSupportedVersions, err := utilversion.HighestSupportedVersion(versions)
	if err != nil {
		t.Fatalf("unexpected error parsing versions (%v) for pluginName %q endpoint %q: %#v", versions, pluginName, endpoint, err)
	}

	csiDrivers.Clear()
	csiDrivers.Set(pluginName, Driver{
		endpoint:                endpoint,
		highestSupportedVersion: highestSupportedVersions,
	})
}

// TestPluginGetPluginName 验证插件名固定为 kubernetes.io/csi。
func TestPluginGetPluginName(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)
	if plug.GetPluginName() != "kubernetes.io/csi" {
		t.Errorf("unexpected plugin name %v", plug.GetPluginName())
	}
}

// TestPluginGetVolumeName 验证按驱动名与卷名拼接出卷标识，并覆盖非法 spec 场景。
func TestPluginGetVolumeName(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)
	testCases := []struct {
		name       string
		driverName string
		volName    string
		spec       *volume.Spec
		shouldFail bool
	}{
		{
			name:       "alphanum names",
			driverName: "testdr",
			volName:    "testvol",
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, "testdr", "testvol"), false),
		},
		{
			name:       "mixchar driver",
			driverName: "test.dr.cc",
			volName:    "testvol",
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, "test.dr.cc", "testvol"), false),
		},
		{
			name:       "mixchar volume",
			driverName: "testdr",
			volName:    "test-vol-name",
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, "testdr", "test-vol-name"), false),
		},
		{
			name:       "mixchars all",
			driverName: "test-driver",
			volName:    "test.vol.name",
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, "test-driver", "test.vol.name"), false),
		},
		{
			name:       "volume source with mixchars all",
			driverName: "test-driver",
			volName:    "test.vol.name",
			spec:       volume.NewSpecFromVolume(makeTestVol("test-pv", "test-driver")),
			// 未启用 CSI inline 特性，Volume 类型 spec 必然失败。
			shouldFail: true,
		},
		{
			name:       "missing spec",
			shouldFail: true,
		},
	}

	for _, tc := range testCases {
		t.Logf("testing: %s", tc.name)
		registerFakePlugin(tc.driverName, "endpoint", []string{"1.3.0"}, t)
		name, err := plug.GetVolumeName(tc.spec)
		if tc.shouldFail != (err != nil) {
			t.Fatal("shouldFail does match expected error")
		}
		if tc.shouldFail && err != nil {
			t.Log(err)
			continue
		}
		if name != fmt.Sprintf("%s%s%s", tc.driverName, volNameSep, tc.volName) {
			t.Errorf("unexpected volume name %s", name)
		}
	}
}

// TestPluginGetVolumeNameWithInline 验证 CSIDriver 仅支持持久卷时内联卷 spec 被拒绝。
func TestPluginGetVolumeNameWithInline(t *testing.T) {
	modes := []storage.VolumeLifecycleMode{
		storage.VolumeLifecyclePersistent,
	}
	driver := getTestCSIDriver(testDriver, nil, nil, modes)
	client := fakeclient.NewSimpleClientset(driver)
	plug, tmpDir := newTestPlugin(t, client)
	defer os.RemoveAll(tmpDir)
	testCases := []struct {
		name       string
		driverName string
		volName    string
		shouldFail bool
		spec       *volume.Spec
	}{
		{
			name:       "missing spec",
			shouldFail: true,
		},
		{
			name:       "alphanum names for pv",
			driverName: "testdr",
			volName:    "testvol",
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, "testdr", "testvol"), false),
		},
		{
			name:       "alphanum names for vol source",
			driverName: "testdr",
			volName:    "testvol",
			spec:       volume.NewSpecFromVolume(makeTestVol("test-pv", "testdr")),
			shouldFail: true,
		},
	}

	for _, tc := range testCases {
		t.Logf("testing: %s", tc.name)
		registerFakePlugin(tc.driverName, "endpoint", []string{"1.3.0"}, t)
		name, err := plug.GetVolumeName(tc.spec)
		if tc.shouldFail != (err != nil) {
			t.Fatal("shouldFail does match expected error")
		}
		if tc.shouldFail && err != nil {
			t.Log(err)
			continue
		}
		if name != fmt.Sprintf("%s%s%s", tc.driverName, volNameSep, tc.volName) {
			t.Errorf("unexpected volume name %s", name)
		}
	}
}

// TestPluginCanSupport 验证插件对不同 spec 是否声明支持。
func TestPluginCanSupport(t *testing.T) {
	tests := []struct {
		name       string
		spec       *volume.Spec
		canSupport bool
	}{
		{
			name:       "no spec provided",
			canSupport: false,
		},
		{
			name:       "can support volume source",
			spec:       volume.NewSpecFromVolume(makeTestVol("test-vol", testDriver)),
			canSupport: true,
		},
		{
			name:       "can support persistent volume source",
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 20, testDriver, testVol), true),
			canSupport: true,
		},
	}

	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)
	registerFakePlugin(testDriver, "endpoint", []string{"1.0.0"}, t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			actual := plug.CanSupport(tc.spec)
			if tc.canSupport != actual {
				t.Errorf("expecting canSupport %t, got %t", tc.canSupport, actual)
			}
		})
	}
}

// TestPluginConstructVolumeSpec 验证从卷信息文件重建 spec，包括 SELinux 上下文与特性开关的影响。
func TestPluginConstructVolumeSpec(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name                      string
		seLinuxMountEnabled       bool
		originSpec                *volume.Spec
		originSELinuxMountContext string
		specVolID                 string
		volHandle                 string
		expectedSELinuxContext    string
		podUID                    types.UID
	}{
		{
			name:      "construct spec1 from original persistent spec",
			specVolID: "test.vol.id",
			volHandle: "testvol-handle1",

			originSpec: volume.NewSpecFromPersistentVolume(makeTestPV("test.vol.id", 20, testDriver, "testvol-handle1"), true),
			podUID:     types.UID(fmt.Sprintf("%08X", rand.Uint64())),
		},
		{
			name:       "construct spec2 from original persistent spec",
			specVolID:  "spec2",
			volHandle:  "handle2",
			originSpec: volume.NewSpecFromPersistentVolume(makeTestPV("spec2", 20, testDriver, "handle2"), true),
			podUID:     types.UID(fmt.Sprintf("%08X", rand.Uint64())),
		},
		{
			name:                      "construct SELinux context from original persistent spec when the feature is enabled",
			seLinuxMountEnabled:       true,
			specVolID:                 "spec3",
			volHandle:                 "handle3",
			originSELinuxMountContext: "system_u:object_r:container_file_t:s0:c314,c894",
			originSpec:                volume.NewSpecFromPersistentVolume(makeTestPV("spec3", 20, testDriver, "handle3"), true),
			expectedSELinuxContext:    "system_u:object_r:container_file_t:s0:c314,c894",
			podUID:                    types.UID(fmt.Sprintf("%08X", rand.Uint64())),
		},
		{
			name:                      "construct empty SELinux from original persistent spec when the feature is disabled",
			seLinuxMountEnabled:       false,
			specVolID:                 "spec4",
			volHandle:                 "handle4",
			originSELinuxMountContext: "system_u:object_r:container_file_t:s0:c314,c894",
			originSpec:                volume.NewSpecFromPersistentVolume(makeTestPV("spec4", 20, testDriver, "handle4"), true),
			// 特性门控关闭时 SELinux 上下文会被清空。
			expectedSELinuxContext: "",
			podUID:                 types.UID(fmt.Sprintf("%08X", rand.Uint64())),
		},
	}

	registerFakePlugin(testDriver, "endpoint", []string{"1.0.0"}, t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.seLinuxMountEnabled {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, utilversion.MustParse("1.35"))
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMountReadWriteOncePod, tc.seLinuxMountEnabled)

			mounter, err := plug.NewMounter(
				tc.originSpec,
				&api.Pod{ObjectMeta: meta.ObjectMeta{UID: tc.podUID, Namespace: testns}},
			)
			if err != nil {
				t.Fatal(err)
			}
			if mounter == nil {
				t.Fatal("failed to create CSI mounter")
			}
			csiMounter := mounter.(*csiMountMgr)

			mountPath := filepath.Dir(csiMounter.GetPath())
			err = prepareVolumeInfoFile(mountPath, plug, tc.originSpec.Name(), csiMounter.volumeID, testDriver, string(csiMounter.volumeLifecycleMode), tc.originSELinuxMountContext)
			if err != nil {
				t.Fatalf("failed to save fake volume info file: %s", err)
			}

			// 从落盘的卷信息文件重建 spec，模拟 kubelet 重启后的恢复流程。
			rec, err := plug.ConstructVolumeSpec("test-pv", filepath.Dir(csiMounter.GetPath()))
			if err != nil {
				t.Fatal(err)
			}
			if rec.Spec == nil {
				t.Fatal("nil volume.Spec constructed")
			}

			// 检查重建出的 spec 各字段是否还原正确。
			if rec.Spec.PersistentVolume == nil || rec.Spec.PersistentVolume.Spec.CSI == nil {
				t.Fatal("CSIPersistentVolume not found in constructed spec ")
			}

			volHandle := rec.Spec.PersistentVolume.Spec.CSI.VolumeHandle
			if volHandle != tc.originSpec.PersistentVolume.Spec.CSI.VolumeHandle {
				t.Error("unexpected volumeHandle constructed:", volHandle)
			}
			driverName := rec.Spec.PersistentVolume.Spec.CSI.Driver
			if driverName != tc.originSpec.PersistentVolume.Spec.CSI.Driver {
				t.Error("unexpected driverName constructed:", driverName)
			}

			if rec.Spec.PersistentVolume.Spec.VolumeMode == nil {
				t.Fatalf("Volume mode has not been set.")
			}

			if *rec.Spec.PersistentVolume.Spec.VolumeMode != api.PersistentVolumeFilesystem {
				t.Errorf("Unexpected volume mode %q", *rec.Spec.PersistentVolume.Spec.VolumeMode)
			}

			if rec.Spec.Name() != tc.specVolID {
				t.Errorf("Unexpected spec name constructed %s", rec.Spec.Name())
			}

			if rec.SELinuxMountContext != tc.expectedSELinuxContext {
				t.Errorf("Expected SELinux context %q, got %q", tc.expectedSELinuxContext, rec.SELinuxMountContext)
			}
		})
	}
}

// TestPluginConstructVolumeSpecWithInline 验证持久卷与内联卷 spec 的重建，以及生命周期模式缺失时的失败路径。
func TestPluginConstructVolumeSpecWithInline(t *testing.T) {
	testCases := []struct {
		name       string
		originSpec *volume.Spec
		specVolID  string
		volHandle  string
		podUID     types.UID
		shouldFail bool
		modes      []storage.VolumeLifecycleMode
	}{
		{
			name:       "construct spec1 from persistent spec",
			specVolID:  "test.vol.id",
			volHandle:  "testvol-handle1",
			originSpec: volume.NewSpecFromPersistentVolume(makeTestPV("test.vol.id", 20, testDriver, "testvol-handle1"), true),
			podUID:     types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			modes:      []storage.VolumeLifecycleMode{storage.VolumeLifecyclePersistent},
		},
		{
			name:       "construct spec2 from persistent spec",
			specVolID:  "spec2",
			volHandle:  "handle2",
			originSpec: volume.NewSpecFromPersistentVolume(makeTestPV("spec2", 20, testDriver, "handle2"), true),
			podUID:     types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			modes:      []storage.VolumeLifecycleMode{storage.VolumeLifecyclePersistent},
		},
		{
			name:       "construct spec2 from persistent spec, missing mode",
			specVolID:  "spec2",
			volHandle:  "handle2",
			originSpec: volume.NewSpecFromPersistentVolume(makeTestPV("spec2", 20, testDriver, "handle2"), true),
			podUID:     types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			modes:      []storage.VolumeLifecycleMode{},
			shouldFail: true,
		},
		{
			name:       "construct spec from volume spec",
			specVolID:  "volspec",
			originSpec: volume.NewSpecFromVolume(makeTestVol("volspec", testDriver)),
			podUID:     types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			modes:      []storage.VolumeLifecycleMode{storage.VolumeLifecycleEphemeral},
		},
		{
			name:       "construct spec from volume spec2",
			specVolID:  "volspec2",
			originSpec: volume.NewSpecFromVolume(makeTestVol("volspec2", testDriver)),
			podUID:     types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			modes:      []storage.VolumeLifecycleMode{storage.VolumeLifecycleEphemeral},
		},
		{
			name:       "construct spec from volume spec2, missing mode",
			specVolID:  "volspec2",
			originSpec: volume.NewSpecFromVolume(makeTestVol("volspec2", testDriver)),
			podUID:     types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			modes:      []storage.VolumeLifecycleMode{},
			shouldFail: true,
		},
		{
			name:       "missing spec",
			podUID:     types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			shouldFail: true,
		},
	}

	registerFakePlugin(testDriver, "endpoint", []string{"1.0.0"}, t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			driver := getTestCSIDriver(testDriver, nil, nil, tc.modes)
			client := fakeclient.NewSimpleClientset(driver)
			plug, tmpDir := newTestPlugin(t, client)
			defer os.RemoveAll(tmpDir)

			mounter, err := plug.NewMounter(
				tc.originSpec,
				&api.Pod{ObjectMeta: meta.ObjectMeta{UID: tc.podUID, Namespace: testns}},
			)
			if tc.shouldFail && err != nil {
				t.Log(err)
				return
			}
			if !tc.shouldFail && err != nil {
				t.Fatal(err)
			}
			if mounter == nil {
				t.Fatal("failed to create CSI mounter")
			}
			csiMounter := mounter.(*csiMountMgr)

			mountPath := filepath.Dir(csiMounter.GetPath())
			err = prepareVolumeInfoFile(mountPath, plug, tc.originSpec.Name(), csiMounter.volumeID, testDriver, string(csiMounter.volumeLifecycleMode), "")
			if err != nil {
				t.Fatalf("failed to save fake volume info file: %s", err)
			}

			// 从数据文件重建 spec。
			rec, err := plug.ConstructVolumeSpec("test-pv", filepath.Dir(csiMounter.GetPath()))
			if err != nil {
				t.Fatal(err)
			}
			if rec.Spec == nil {
				t.Fatal("nil volume.Spec constructed")
			}

			if rec.Spec.Name() != tc.specVolID {
				t.Errorf("unexpected spec name constructed volume.Spec: %s", rec.Spec.Name())
			}

			switch {
			case rec.Spec.Volume != nil:
				if rec.Spec.Volume.CSI == nil {
					t.Error("missing CSIVolumeSource in constructed volume.Spec")
				}
				if rec.Spec.Volume.CSI.Driver != tc.originSpec.Volume.CSI.Driver {
					t.Error("unexpected driver in constructed volume source:", rec.Spec.Volume.CSI.Driver)
				}

			case rec.Spec.PersistentVolume != nil:
				if rec.Spec.PersistentVolume.Spec.CSI == nil {
					t.Fatal("missing CSIPersistentVolumeSource in constructed volume.spec")
				}
				volHandle := rec.Spec.PersistentVolume.Spec.CSI.VolumeHandle
				if volHandle != tc.originSpec.PersistentVolume.Spec.CSI.VolumeHandle {
					t.Error("unexpected volumeHandle constructed in persistent volume source:", volHandle)
				}
				driverName := rec.Spec.PersistentVolume.Spec.CSI.Driver
				if driverName != tc.originSpec.PersistentVolume.Spec.CSI.Driver {
					t.Error("unexpected driverName constructed in persistent volume source:", driverName)
				}
				if rec.Spec.PersistentVolume.Spec.VolumeMode == nil {
					t.Fatalf("Volume mode has not been set.")
				}
				if *rec.Spec.PersistentVolume.Spec.VolumeMode != api.PersistentVolumeFilesystem {
					t.Errorf("Unexpected volume mode %q", *rec.Spec.PersistentVolume.Spec.VolumeMode)
				}
			default:
				t.Fatal("invalid volume.Spec constructed")
			}

		})
	}
}

// TestPluginNewMounter 验证持久卷与内联卷均能创建 mounter，并检查其字段。
func TestPluginNewMounter(t *testing.T) {
	tests := []struct {
		name                string
		spec                *volume.Spec
		podUID              types.UID
		namespace           string
		volumeLifecycleMode storage.VolumeLifecycleMode
		shouldFail          bool
	}{
		{
			name:                "mounter from persistent volume source",
			spec:                volume.NewSpecFromPersistentVolume(makeTestPV("test-pv1", 20, testDriver, testVol), true),
			podUID:              types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			namespace:           "test-ns1",
			volumeLifecycleMode: storage.VolumeLifecyclePersistent,
		},
		{
			name:                "mounter from volume source",
			spec:                volume.NewSpecFromVolume(makeTestVol("test-vol1", testDriver)),
			podUID:              types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			namespace:           "test-ns2",
			volumeLifecycleMode: storage.VolumeLifecycleEphemeral,
			// NewMounter 在内联卷特性关闭时仍可工作。
			shouldFail: false,
		},
		{
			name:       "mounter from no spec provided",
			shouldFail: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plug, tmpDir := newTestPlugin(t, nil)
			defer os.RemoveAll(tmpDir)

			registerFakePlugin(testDriver, "endpoint", []string{"1.2.0"}, t)
			mounter, err := plug.NewMounter(
				test.spec,
				&api.Pod{ObjectMeta: meta.ObjectMeta{UID: test.podUID, Namespace: test.namespace}},
			)
			if test.shouldFail != (err != nil) {
				t.Fatal("Unexpected error:", err)
			}
			if test.shouldFail && err != nil {
				t.Log(err)
				return
			}

			if mounter == nil {
				t.Fatal("failed to create CSI mounter")
			}
			csiMounter := mounter.(*csiMountMgr)

			// 校验 mapper 各字段是否设置正确。
			if string(csiMounter.driverName) != testDriver {
				t.Error("mounter driver name not set")
			}
			if csiMounter.volumeID == "" {
				t.Error("mounter volume id not set")
			}
			if csiMounter.pod == nil {
				t.Error("mounter pod not set")
			}
			if string(csiMounter.podUID) != string(test.podUID) {
				t.Error("mounter podUID not set")
			}
			csiClient, err := csiMounter.csiClientGetter.Get()
			if csiClient == nil {
				t.Errorf("mounter csiClient is nil: %v", err)
			}
			if err != nil {
				t.Fatal(err)
			}
			if csiMounter.volumeLifecycleMode != test.volumeLifecycleMode {
				t.Error("unexpected driver mode:", csiMounter.volumeLifecycleMode)
			}
		})
	}
}

// TestPluginNewMounterWithInline 验证在驱动支持不同生命周期模式组合时 mounter 的创建行为。
func TestPluginNewMounterWithInline(t *testing.T) {
	bothModes := []storage.VolumeLifecycleMode{
		storage.VolumeLifecycleEphemeral,
		storage.VolumeLifecyclePersistent,
	}
	persistentMode := []storage.VolumeLifecycleMode{
		storage.VolumeLifecyclePersistent,
	}
	ephemeralMode := []storage.VolumeLifecycleMode{
		storage.VolumeLifecycleEphemeral,
	}
	tests := []struct {
		name                string
		spec                *volume.Spec
		podUID              types.UID
		namespace           string
		volumeLifecycleMode storage.VolumeLifecycleMode
		shouldFail          bool
	}{
		{
			name:       "mounter with missing spec",
			shouldFail: true,
		},
		{
			name: "mounter with spec with both volSrc and pvSrc",
			spec: &volume.Spec{
				Volume:           makeTestVol("test-vol1", testDriver),
				PersistentVolume: makeTestPV("test-pv1", 20, testDriver, testVol),
				ReadOnly:         true,
			},
			shouldFail: true,
		},
		{
			name:                "mounter with persistent volume source",
			spec:                volume.NewSpecFromPersistentVolume(makeTestPV("test-pv1", 20, testDriver, testVol), true),
			podUID:              types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			namespace:           "test-ns1",
			volumeLifecycleMode: storage.VolumeLifecyclePersistent,
		},
		{
			name:                "mounter with volume source",
			spec:                volume.NewSpecFromVolume(makeTestVol("test-vol1", testDriver)),
			podUID:              types.UID(fmt.Sprintf("%08X", rand.Uint64())),
			namespace:           "test-ns2",
			volumeLifecycleMode: storage.VolumeLifecycleEphemeral,
		},
	}

	runAll := func(t *testing.T, supported []storage.VolumeLifecycleMode) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				driver := getTestCSIDriver(testDriver, nil, nil, supported)
				fakeClient := fakeclient.NewSimpleClientset(driver)
				plug, tmpDir := newTestPlugin(t, fakeClient)
				defer os.RemoveAll(tmpDir)

				registerFakePlugin(testDriver, "endpoint", []string{"1.2.0"}, t)

				mounter, err := plug.NewMounter(
					test.spec,
					&api.Pod{ObjectMeta: meta.ObjectMeta{UID: test.podUID, Namespace: test.namespace}},
				)

				// 部分用例的输入 spec 本身不合法，预期创建 mounter 失败。
				shouldFail := test.shouldFail
				if shouldFail != (err != nil) {
					t.Fatal("Unexpected error:", err)
				}
				if shouldFail && err != nil {
					t.Log(err)
					return
				}

				if mounter == nil {
					t.Fatal("failed to create CSI mounter")
				}
				csiMounter := mounter.(*csiMountMgr)

				// 校验 mounter 各字段是否设置正确。
				if string(csiMounter.driverName) != testDriver {
					t.Error("mounter driver name not set")
				}
				if csiMounter.volumeID == "" {
					t.Error("mounter volume id not set")
				}
				if csiMounter.pod == nil {
					t.Error("mounter pod not set")
				}
				if string(csiMounter.podUID) != string(test.podUID) {
					t.Error("mounter podUID not set")
				}
				csiClient, err := csiMounter.csiClientGetter.Get()
				if csiClient == nil {
					t.Errorf("mounter csiClient is nil: %v", err)
				}
				if csiMounter.volumeLifecycleMode != test.volumeLifecycleMode {
					t.Error("unexpected driver mode:", csiMounter.volumeLifecycleMode)
				}
			})
		}
	}

	t.Run("both supported", func(t *testing.T) {
		runAll(t, bothModes)
	})
	t.Run("persistent supported", func(t *testing.T) {
		runAll(t, persistentMode)
	})
	t.Run("ephemeral supported", func(t *testing.T) {
		runAll(t, ephemeralMode)
	})
}

// TestPluginNewUnmounter 验证从保存的卷数据重建 unmounter 并恢复必要字段。
func TestPluginNewUnmounter(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	registerFakePlugin(testDriver, "endpoint", []string{"1.0.0"}, t)
	pv := makeTestPV("test-pv", 10, testDriver, testVol)

	// 先落盘卷数据，unmounter 需从文件恢复驱动信息以重建客户端。
	dir := filepath.Join(getTargetPath(testPodUID, pv.ObjectMeta.Name, plug.host), "/mount")
	if err := os.MkdirAll(dir, 0755); err != nil && !os.IsNotExist(err) {
		t.Errorf("failed to create dir [%s]: %v", dir, err)
	}

	if err := saveVolumeData(
		filepath.Dir(dir),
		volDataFileName,
		map[string]string{
			volDataKey.specVolID:  pv.ObjectMeta.Name,
			volDataKey.driverName: testDriver,
			volDataKey.volHandle:  testVol,
		},
	); err != nil {
		t.Fatalf("failed to save volume data: %v", err)
	}

	// 验证 unmounter 能从数据文件恢复上下文。
	unmounter, err := plug.NewUnmounter(pv.ObjectMeta.Name, testPodUID)
	csiUnmounter := unmounter.(*csiMountMgr)

	if err != nil {
		t.Fatalf("Failed to make a new Unmounter: %v", err)
	}

	if csiUnmounter == nil {
		t.Fatal("failed to create CSI Unmounter")
	}

	if csiUnmounter.podUID != testPodUID {
		t.Error("podUID not set")
	}

	csiClient, err := csiUnmounter.csiClientGetter.Get()
	if csiClient == nil {
		t.Errorf("mounter csiClient is nil: %v", err)
	}
}

// TestPluginNewAttacher 验证创建 attacher 时插件、客户端与超时均已设置。
func TestPluginNewAttacher(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	attacher, err := plug.NewAttacher()
	if err != nil {
		t.Fatalf("failed to create new attacher: %v", err)
	}

	csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, testWatchTimeout)
	if csiAttacher.plugin == nil {
		t.Error("plugin not set for attacher")
	}
	if csiAttacher.k8s == nil {
		t.Error("Kubernetes client not set for attacher")
	}
	if csiAttacher.watchTimeout == time.Duration(0) {
		t.Error("watch timeout not set for attacher")
	}
}

// TestPluginNewDetacher 验证创建 detacher 时插件、客户端与超时均已设置。
func TestPluginNewDetacher(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	detacher, err := plug.NewDetacher()
	if err != nil {
		t.Fatalf("failed to create new detacher: %v", err)
	}

	csiDetacher := getCsiAttacherFromVolumeDetacher(detacher, testWatchTimeout)
	if csiDetacher.plugin == nil {
		t.Error("plugin not set for detacher")
	}
	if csiDetacher.k8s == nil {
		t.Error("Kubernetes client not set for detacher")
	}
	if csiDetacher.watchTimeout == time.Duration(0) {
		t.Error("watch timeout not set for detacher")
	}
}

// TestPluginCanAttach 验证根据 CSIDriver 的 attachRequired 判断卷是否可挂载。
func TestPluginCanAttach(t *testing.T) {
	tests := []struct {
		name       string
		driverName string
		spec       *volume.Spec
		canAttach  bool
		shouldFail bool
	}{
		{
			name:       "non-attachable inline",
			driverName: "attachable-inline",
			spec:       volume.NewSpecFromVolume(makeTestVol("test-vol", "attachable-inline")),
			canAttach:  false,
		},
		{
			name:       "attachable PV",
			driverName: "attachable-pv",
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("test-vol", 20, "attachable-pv", testVol), true),
			canAttach:  true,
		},
		{
			name:       "incomplete spec",
			driverName: "attachable-pv",
			spec:       &volume.Spec{ReadOnly: true},
			canAttach:  false,
			shouldFail: true,
		},
		{
			name:       "nil spec",
			driverName: "attachable-pv",
			canAttach:  false,
			shouldFail: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			csiDriver := getTestCSIDriver(test.driverName, nil, &test.canAttach, nil)
			fakeCSIClient := fakeclient.NewSimpleClientset(csiDriver)
			plug, tmpDir := newTestPlugin(t, fakeCSIClient)
			defer os.RemoveAll(tmpDir)

			pluginCanAttach, err := plug.CanAttach(test.spec)
			if err != nil && !test.shouldFail {
				t.Fatalf("unexpected plugin.CanAttach error: %s", err)
			}
			if pluginCanAttach != test.canAttach {
				t.Fatalf("expecting plugin.CanAttach %t got %t", test.canAttach, pluginCanAttach)
			}
		})
	}
}

// TestPluginFindAttachablePlugin 验证按 spec 查找可挂载插件的结果与驱动的 attachRequired 一致。
func TestPluginFindAttachablePlugin(t *testing.T) {
	tests := []struct {
		name       string
		driverName string
		spec       *volume.Spec
		canAttach  bool
		shouldFail bool
	}{
		{
			name:       "non-attachable inline",
			driverName: "attachable-inline",
			spec:       volume.NewSpecFromVolume(makeTestVol("test-vol", "attachable-inline")),
			canAttach:  false,
		},
		{
			name:       "attachable PV",
			driverName: "attachable-pv",
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("test-vol", 20, "attachable-pv", testVol), true),
			canAttach:  true,
		},
		{
			name:       "incomplete spec",
			driverName: "attachable-pv",
			spec:       &volume.Spec{ReadOnly: true},
			canAttach:  false,
			shouldFail: true,
		},
		{
			name:       "nil spec",
			driverName: "attachable-pv",
			canAttach:  false,
			shouldFail: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpDir, err := utiltesting.MkTmpdir("csi-test")
			if err != nil {
				t.Fatalf("can't create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			client := fakeclient.NewSimpleClientset(
				getTestCSIDriver(test.driverName, nil, &test.canAttach, nil),
				&api.Node{
					ObjectMeta: meta.ObjectMeta{
						Name: "fakeNode",
					},
					Spec: api.NodeSpec{},
				},
			)
			factory := informers.NewSharedInformerFactory(client, CsiResyncPeriod)
			host := volumetest.NewFakeKubeletVolumeHostWithCSINodeName(t,
				tmpDir,
				client,
				ProbeVolumePlugins(),
				"fakeNode",
				factory.Storage().V1().CSIDrivers().Lister(),
				factory.Storage().V1().VolumeAttachments().Lister(),
			)

			plugMgr := host.GetPluginMgr()

			plugin, err := plugMgr.FindAttachablePluginBySpec(test.spec)
			if err != nil && !test.shouldFail {
				t.Fatalf("unexpected error calling pluginMgr.FindAttachablePluginBySpec: %s", err)
			}
			if (plugin != nil) != test.canAttach {
				t.Fatal("expecting attachable plugin, but got nil")
			}
		})
	}
}

// TestPluginCanDeviceMount 验证设备挂载能力由 CSIDriver 配置决定。
func TestPluginCanDeviceMount(t *testing.T) {
	tests := []struct {
		name           string
		driverName     string
		spec           *volume.Spec
		canDeviceMount bool
		shouldFail     bool
	}{
		{
			name:           "non device mountable inline",
			driverName:     "inline-driver",
			spec:           volume.NewSpecFromVolume(makeTestVol("test-vol", "inline-driver")),
			canDeviceMount: false,
		},
		{
			name:           "device mountable PV",
			driverName:     "device-mountable-pv",
			spec:           volume.NewSpecFromPersistentVolume(makeTestPV("test-vol", 20, "device-mountable-pv", testVol), true),
			canDeviceMount: true,
		},
		{
			name:           "incomplete spec",
			driverName:     "device-unmountable",
			spec:           &volume.Spec{ReadOnly: true},
			canDeviceMount: false,
			shouldFail:     true,
		},
		{
			name:           "missing spec",
			driverName:     "device-unmountable",
			canDeviceMount: false,
			shouldFail:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plug, tmpDir := newTestPlugin(t, nil)
			defer os.RemoveAll(tmpDir)

			pluginCanDeviceMount, err := plug.CanDeviceMount(test.spec)
			if err != nil && !test.shouldFail {
				t.Fatalf("unexpected error in plug.CanDeviceMount: %s", err)
			}
			if pluginCanDeviceMount != test.canDeviceMount {
				t.Fatalf("expecting plugin.CanAttach %t got %t", test.canDeviceMount, pluginCanDeviceMount)
			}
		})
	}
}

// TestPluginFindDeviceMountablePluginBySpec 验证按 spec 查找支持设备挂载的插件。
func TestPluginFindDeviceMountablePluginBySpec(t *testing.T) {
	tests := []struct {
		name           string
		driverName     string
		spec           *volume.Spec
		canDeviceMount bool
		shouldFail     bool
	}{
		{
			name:           "non device mountable inline",
			driverName:     "inline-driver",
			spec:           volume.NewSpecFromVolume(makeTestVol("test-vol", "inline-driver")),
			canDeviceMount: false,
		},
		{
			name:           "device mountable PV",
			driverName:     "device-mountable-pv",
			spec:           volume.NewSpecFromPersistentVolume(makeTestPV("test-vol", 20, "device-mountable-pv", testVol), true),
			canDeviceMount: true,
		},
		{
			name:           "incomplete spec",
			driverName:     "device-unmountable",
			spec:           &volume.Spec{ReadOnly: true},
			canDeviceMount: false,
			shouldFail:     true,
		},
		{
			name:           "missing spec",
			driverName:     "device-unmountable",
			canDeviceMount: false,
			shouldFail:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpDir, err := utiltesting.MkTmpdir("csi-test")
			if err != nil {
				t.Fatalf("can't create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			client := fakeclient.NewSimpleClientset(
				&api.Node{
					ObjectMeta: meta.ObjectMeta{
						Name: "fakeNode",
					},
					Spec: api.NodeSpec{},
				},
			)
			host := volumetest.NewFakeVolumeHostWithCSINodeName(t, tmpDir, client, ProbeVolumePlugins(), "fakeNode", nil, nil)
			plugMgr := host.GetPluginMgr()
			plug, err := plugMgr.FindDeviceMountablePluginBySpec(test.spec)
			if err != nil && !test.shouldFail {
				t.Fatalf("unexpected error in plugMgr.FindDeviceMountablePluginBySpec: %s", err)
			}
			if (plug != nil) != test.canDeviceMount {
				t.Fatalf("expecting deviceMountablePlugin, but got nil")
			}
		})
	}
}

// TestPluginNewBlockMapper 验证块设备 mapper 创建成功且数据文件已落盘。
func TestPluginNewBlockMapper(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	registerFakePlugin(testDriver, "endpoint", []string{"1.0.0"}, t)
	pv := makeTestPV("test-block-pv", 10, testDriver, testVol)
	mounter, err := plug.NewBlockVolumeMapper(
		volume.NewSpecFromPersistentVolume(pv, pv.Spec.PersistentVolumeSource.CSI.ReadOnly),
		&api.Pod{ObjectMeta: meta.ObjectMeta{UID: testPodUID, Namespace: testns}},
	)
	if err != nil {
		t.Fatalf("Failed to make a new BlockMapper: %v", err)
	}

	if mounter == nil {
		t.Fatal("failed to create CSI BlockMapper, mapper is nill")
	}
	csiMapper := mounter.(*csiBlockMapper)

	// 校验 mapper 各字段是否设置正确。
	if string(csiMapper.driverName) != testDriver {
		t.Error("CSI block mapper missing driver name")
	}
	if csiMapper.volumeID != testVol {
		t.Error("CSI block mapper missing volumeID")
	}

	if csiMapper.podUID == types.UID("") {
		t.Error("CSI block mapper missing pod.UID")
	}
	csiClient, err := csiMapper.csiClientGetter.Get()
	if csiClient == nil {
		t.Errorf("mapper csiClient is nil: %v", err)
	}

	// 数据文件落盘是 mapper 正常工作的前置条件。
	dataFile := getVolumeDeviceDataDir(csiMapper.spec.Name(), plug.host)
	if _, err := os.Stat(dataFile); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("data file not created %s", dataFile)
		} else {
			t.Fatal(err)
		}
	}
}

// TestPluginNewUnmapper 验证从保存的数据重建块设备 unmapper 并恢复卷信息。
func TestPluginNewUnmapper(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	registerFakePlugin(testDriver, "endpoint", []string{"1.0.0"}, t)
	pv := makeTestPV("test-pv", 10, testDriver, testVol)

	// 先落盘卷数据，unmapper 需从文件恢复驱动信息以重建客户端。
	dir := getVolumeDeviceDataDir(pv.ObjectMeta.Name, plug.host)
	if err := os.MkdirAll(dir, 0755); err != nil && !os.IsNotExist(err) {
		t.Errorf("failed to create dir [%s]: %v", dir, err)
	}

	if err := saveVolumeData(
		dir,
		volDataFileName,
		map[string]string{
			volDataKey.specVolID:  pv.ObjectMeta.Name,
			volDataKey.driverName: testDriver,
			volDataKey.volHandle:  testVol,
		},
	); err != nil {
		t.Fatalf("failed to save volume data: %v", err)
	}

	// 验证 unmapper 能从数据文件恢复上下文。
	unmapper, err := plug.NewBlockVolumeUnmapper(pv.ObjectMeta.Name, testPodUID)
	csiUnmapper := unmapper.(*csiBlockMapper)

	if err != nil {
		t.Fatalf("Failed to make a new Unmounter: %v", err)
	}

	if csiUnmapper == nil {
		t.Fatal("failed to create CSI Unmounter")
	}

	if csiUnmapper.podUID != testPodUID {
		t.Error("podUID not set")
	}

	if csiUnmapper.specName != pv.ObjectMeta.Name {
		t.Error("specName not set")
	}

	csiClient, err := csiUnmapper.csiClientGetter.Get()
	if csiClient == nil {
		t.Errorf("unmapper csiClient is nil: %v", err)
	}

	// 校验从数据文件加载出的卷信息。
	if string(csiUnmapper.driverName) != testDriver {
		t.Error("unmapper driverName not set")
	}
	if csiUnmapper.volumeID != testVol {
		t.Error("unmapper volumeHandle not set")
	}
}

// TestPluginConstructBlockVolumeSpec 验证从卷数据文件重建块卷 spec。
func TestPluginConstructBlockVolumeSpec(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name       string
		specVolID  string
		data       map[string]string
		shouldFail bool
	}{
		{
			name:      "valid spec name",
			specVolID: "test.vol.id",
			data:      map[string]string{volDataKey.specVolID: "test.vol.id", volDataKey.volHandle: "test-vol0", volDataKey.driverName: "test-driver0"},
		},
	}

	for _, tc := range testCases {
		t.Logf("test case: %s", tc.name)
		deviceDataDir := getVolumeDeviceDataDir(tc.specVolID, plug.host)

		// 在插件目录下放置数据文件，供 ConstructBlockVolumeSpec 读取。
		if tc.data != nil {
			if err := os.MkdirAll(deviceDataDir, 0755); err != nil && !os.IsNotExist(err) {
				t.Errorf("failed to create dir [%s]: %v", deviceDataDir, err)
			}
			if err := saveVolumeData(deviceDataDir, volDataFileName, tc.data); err != nil {
				t.Fatal(err)
			}
		}

		// 从数据文件重建块卷 spec。
		spec, err := plug.ConstructBlockVolumeSpec("test-podUID", tc.specVolID, getVolumeDevicePluginDir(tc.specVolID, plug.host))
		if tc.shouldFail {
			if err == nil {
				t.Fatal("expecting ConstructVolumeSpec to fail, but got nil error")
			}
			continue
		}

		if spec.PersistentVolume.Spec.VolumeMode == nil {
			t.Fatalf("Volume mode has not been set.")
		}

		if *spec.PersistentVolume.Spec.VolumeMode != api.PersistentVolumeBlock {
			t.Errorf("Unexpected volume mode %q", *spec.PersistentVolume.Spec.VolumeMode)
		}

		volHandle := spec.PersistentVolume.Spec.CSI.VolumeHandle
		if volHandle != tc.data[volDataKey.volHandle] {
			t.Errorf("expected volID %s, got volID %s", tc.data[volDataKey.volHandle], volHandle)
		}

		if spec.Name() != tc.specVolID {
			t.Errorf("Unexpected spec name %s", spec.Name())
		}
	}
}

// TestValidatePlugin 验证驱动注册时版本号的校验规则。
func TestValidatePlugin(t *testing.T) {
	testCases := []struct {
		pluginName string
		endpoint   string
		versions   []string
		shouldFail bool
	}{
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"v1.0.0"},
			shouldFail: false,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"0.3.0"},
			shouldFail: true,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"0.2.0"},
			shouldFail: true,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"0.2.0", "v0.3.0"},
			shouldFail: true,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"0.2.0", "v1.0.0"},
			shouldFail: false,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"0.2.0", "v1.2.3"},
			shouldFail: false,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"v1.2.3", "v0.3.0"},
			shouldFail: false,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"v1.2.3", "v0.3.0", "2.0.1"},
			shouldFail: false,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"v1.2.3", "4.9.12", "v0.3.0", "2.0.1"},
			shouldFail: false,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"v1.2.3", "boo", "v0.3.0", "2.0.1"},
			shouldFail: false,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"4.9.12", "2.0.1"},
			shouldFail: true,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{},
			shouldFail: true,
		},
		{
			pluginName: "test.plugin",
			endpoint:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions:   []string{"var", "boo", "foo"},
			shouldFail: true,
		},
	}

	for _, tc := range testCases {
		// 执行待校验的驱动注册。
		err := PluginHandler.ValidatePlugin(t.Context(), tc.pluginName, tc.endpoint, tc.versions)

		// 校验预期结果与实际错误是否一致。
		if tc.shouldFail && err == nil {
			t.Fatalf("expecting ValidatePlugin to fail, but got nil error for testcase: %#v", tc)
		}
		if !tc.shouldFail && err != nil {
			t.Fatalf("unexpected error during ValidatePlugin for testcase: %#v\r\n err:%v", tc, err)
		}
	}
}

// TestValidatePluginExistingDriver 验证同名或同 endpoint 驱动的冲突检测。
func TestValidatePluginExistingDriver(t *testing.T) {
	testCases := []struct {
		pluginName1 string
		endpoint1   string
		versions1   []string
		pluginName2 string
		endpoint2   string
		versions2   []string
		shouldFail  bool
	}{
		{
			pluginName1: "test.plugin",
			endpoint1:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions1:   []string{"v1.0.0"},
			pluginName2: "test.plugin2",
			endpoint2:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions2:   []string{"v1.0.0"},
			shouldFail:  false,
		},
		{
			pluginName1: "test.plugin",
			endpoint1:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions1:   []string{"v1.0.0"},
			pluginName2: "test.plugin",
			endpoint2:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions2:   []string{"v1.0.0"},
			shouldFail:  true,
		},
		{
			pluginName1: "test.plugin",
			endpoint1:   "/var/log/kubelet/plugins/myplugin/csi.sock",
			versions1:   []string{"v0.3.0", "v0.2.0", "v1.0.0"},
			pluginName2: "test.plugin",
			endpoint2:   "/var/log/kubelet/plugins_registry/myplugin/csi.sock",
			versions2:   []string{"v1.0.1"},
			shouldFail:  false,
		},
	}

	for _, tc := range testCases {
		// 先注册第一个驱动，再校验第二个是否冲突。
		highestSupportedVersions1, err := utilversion.HighestSupportedVersion(tc.versions1)
		if err != nil {
			t.Fatalf("unexpected error parsing version for testcase: %#v: %v", tc, err)
		}

		csiDrivers.Clear()
		csiDrivers.Set(tc.pluginName1, Driver{
			endpoint:                tc.endpoint1,
			highestSupportedVersion: highestSupportedVersions1,
		})

		// 校验第二个驱动能否与已注册驱动共存。
		err = PluginHandler.ValidatePlugin(t.Context(), tc.pluginName2, tc.endpoint2, tc.versions2)

		// 校验预期结果与实际错误是否一致。
		if tc.shouldFail && err == nil {
			t.Fatalf("expecting ValidatePlugin to fail, but got nil error for testcase: %#v", tc)
		}
		if !tc.shouldFail && err != nil {
			t.Fatalf("unexpected error during ValidatePlugin for testcase: %#v\r\n err:%v", tc, err)
		}
	}
}

// TestGetNodeAllocatableUpdatePeriod 验证从 CSIDriver 读取可分配资源更新周期，缺省为零。
func TestGetNodeAllocatableUpdatePeriod(t *testing.T) {
	tests := []struct {
		name     string
		driver   *storage.CSIDriver
		expected time.Duration
	}{
		{
			name:     "nil driver",
			driver:   nil,
			expected: 0,
		},
		{
			name: "nil NodeAllocatableUpdatePeriodSeconds",
			driver: &storage.CSIDriver{
				Spec: storage.CSIDriverSpec{
					NodeAllocatableUpdatePeriodSeconds: nil,
				},
			},
			expected: 0,
		},
		{
			name: "NodeAllocatableUpdatePeriodSeconds set to 60",
			driver: &storage.CSIDriver{
				Spec: storage.CSIDriverSpec{
					NodeAllocatableUpdatePeriodSeconds: &[]int64{60}[0],
				},
			},
			expected: 60 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := getNodeAllocatableUpdatePeriod(tc.driver)
			if actual != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, actual)
			}
		})
	}
}

// TestIsResourceExhaustError 验证仅 ResourceExhausted 错误码被识别为资源耗尽。
func TestIsResourceExhaustError(t *testing.T) {
	tests := []struct {
		name       string
		attachment *storage.VolumeAttachment
		expected   bool
	}{
		{
			name:       "nil attachment",
			attachment: nil,
			expected:   false,
		},
		{
			name: "nil AttachError",
			attachment: &storage.VolumeAttachment{
				Status: storage.VolumeAttachmentStatus{
					AttachError: nil,
				},
			},
			expected: false,
		},
		{
			name: "nil ErrorCode",
			attachment: &storage.VolumeAttachment{
				Status: storage.VolumeAttachmentStatus{
					AttachError: &storage.VolumeError{
						Message:   "an error occurred",
						ErrorCode: nil,
					},
				},
			},
			expected: false,
		},
		{
			name: "non-resource exhausted error code",
			attachment: &storage.VolumeAttachment{
				Status: storage.VolumeAttachmentStatus{
					AttachError: &storage.VolumeError{
						Message: "volume not found",
						ErrorCode: func() *int32 {
							code := int32(codes.NotFound)
							return &code
						}(),
					},
				},
			},
			expected: false,
		},
		{
			name: "resource exhausted error code",
			attachment: &storage.VolumeAttachment{
				Status: storage.VolumeAttachmentStatus{
					AttachError: &storage.VolumeError{
						Message: "resource exhausted",
						ErrorCode: func() *int32 {
							code := int32(codes.ResourceExhausted)
							return &code
						}(),
					},
				},
			},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := isResourceExhaustError(tc.attachment)
			if actual != tc.expected {
				t.Errorf("Expected isResourceExhaustError to return %v, but got %v", tc.expected, actual)
			}
		})
	}
}
