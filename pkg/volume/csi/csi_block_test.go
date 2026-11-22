/*
Copyright 2018 The Kubernetes Authors.

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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	api "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/volume"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// prepareBlockMapperTest 构造块设备映射器及对应卷规格，供块设备相关测试使用。
func prepareBlockMapperTest(plug *csiPlugin, specVolumeName string, t *testing.T) (*csiBlockMapper, *volume.Spec, *api.PersistentVolume, error) {
	registerFakePlugin(testDriver, "endpoint", []string{"1.0.0"}, t)
	pv := makeTestPV(specVolumeName, 10, testDriver, testVol)
	spec := volume.NewSpecFromPersistentVolume(pv, pv.Spec.PersistentVolumeSource.CSI.ReadOnly)
	mapper, err := plug.NewBlockVolumeMapper(
		spec,
		&api.Pod{ObjectMeta: metav1.ObjectMeta{UID: testPodUID, Namespace: testns, Name: testPod}},
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to make a new Mapper: %w", err)
	}
	csiMapper := mapper.(*csiBlockMapper)
	return csiMapper, spec, pv, nil
}

// TestBlockMapperGetGlobalMapPath 获取块设备全局映射路径的测试。
func TestBlockMapperGetGlobalMapPath(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	// TODO (vladimirvivien) 含斜杠的 specName 将无法工作
	testCases := []struct {
		name           string
		specVolumeName string
		path           string
	}{
		{
			name:           "simple specName",
			specVolumeName: "spec-0",
			path:           filepath.Join(tmpDir, fmt.Sprintf("plugins/kubernetes.io/csi/volumeDevices/%s/%s", "spec-0", "dev")),
		},
		{
			name:           "specName with dots",
			specVolumeName: "test.spec.1",
			path:           filepath.Join(tmpDir, fmt.Sprintf("plugins/kubernetes.io/csi/volumeDevices/%s/%s", "test.spec.1", "dev")),
		},
	}
	for _, tc := range testCases {
		t.Logf("test case: %s", tc.name)
		csiMapper, spec, _, err := prepareBlockMapperTest(plug, tc.specVolumeName, t)
		if err != nil {
			t.Fatalf("Failed to make a new Mapper: %v", err)
		}

		path, err := csiMapper.GetGlobalMapPath(spec)
		if err != nil {
			t.Errorf("mapper GetGlobalMapPath failed: %v", err)
		}

		if tc.path != path {
			t.Errorf("expecting path %s, got %s", tc.path, path)
		}
	}
}

// TestBlockMapperGetStagingPath 获取块设备 staging 路径的测试。
func TestBlockMapperGetStagingPath(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name           string
		specVolumeName string
		path           string
	}{
		{
			name:           "simple specName",
			specVolumeName: "spec-0",
			path:           filepath.Join(tmpDir, fmt.Sprintf("plugins/kubernetes.io/csi/volumeDevices/staging/%s", "spec-0")),
		},
		{
			name:           "specName with dots",
			specVolumeName: "test.spec.1",
			path:           filepath.Join(tmpDir, fmt.Sprintf("plugins/kubernetes.io/csi/volumeDevices/staging/%s", "test.spec.1")),
		},
	}
	for _, tc := range testCases {
		t.Logf("test case: %s", tc.name)
		csiMapper, _, _, err := prepareBlockMapperTest(plug, tc.specVolumeName, t)
		if err != nil {
			t.Fatalf("Failed to make a new Mapper: %v", err)
		}

		path := csiMapper.GetStagingPath()

		if tc.path != path {
			t.Errorf("expecting path %s, got %s", tc.path, path)
		}
	}
}

// TestBlockMapperGetPublishPath 获取块设备 publish 路径的测试。
func TestBlockMapperGetPublishPath(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name           string
		specVolumeName string
		path           string
	}{
		{
			name:           "simple specName",
			specVolumeName: "spec-0",
			path:           filepath.Join(tmpDir, fmt.Sprintf("plugins/kubernetes.io/csi/volumeDevices/publish/%s/%s", "spec-0", testPodUID)),
		},
		{
			name:           "specName with dots",
			specVolumeName: "test.spec.1",
			path:           filepath.Join(tmpDir, fmt.Sprintf("plugins/kubernetes.io/csi/volumeDevices/publish/%s/%s", "test.spec.1", testPodUID)),
		},
	}
	for _, tc := range testCases {
		t.Logf("test case: %s", tc.name)
		csiMapper, _, _, err := prepareBlockMapperTest(plug, tc.specVolumeName, t)
		if err != nil {
			t.Fatalf("Failed to make a new Mapper: %v", err)
		}

		path := csiMapper.getPublishPath()

		if tc.path != path {
			t.Errorf("expecting path %s, got %s", tc.path, path)
		}
	}
}

// TestBlockMapperGetDeviceMapPath 获取块设备映射路径的测试。
func TestBlockMapperGetDeviceMapPath(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name           string
		specVolumeName string
		path           string
	}{
		{
			name:           "simple specName",
			specVolumeName: "spec-0",
			path:           filepath.Join(tmpDir, fmt.Sprintf("pods/%s/volumeDevices/kubernetes.io~csi", testPodUID)),
		},
		{
			name:           "specName with dots",
			specVolumeName: "test.spec.1",
			path:           filepath.Join(tmpDir, fmt.Sprintf("pods/%s/volumeDevices/kubernetes.io~csi", testPodUID)),
		},
	}
	for _, tc := range testCases {
		t.Logf("test case: %s", tc.name)
		csiMapper, _, _, err := prepareBlockMapperTest(plug, tc.specVolumeName, t)
		if err != nil {
			t.Fatalf("Failed to make a new Mapper: %v", err)
		}

		path, volName := csiMapper.GetPodDeviceMapPath()

		if tc.path != path {
			t.Errorf("expecting path %s, got %s", tc.path, path)
		}

		if tc.specVolumeName != volName {
			t.Errorf("expecting volName %s, got %s", tc.specVolumeName, volName)
		}
	}
}

// TestBlockMapperSetupDevice 块设备映射器 SetUpDevice 的测试。
func TestBlockMapperSetupDevice(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	csiMapper, _, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	pvName := pv.GetName()
	nodeName := string(plug.host.GetNodeName())

	csiMapper.csiClient = setupClient(t, true)

	attachID := getAttachmentName(csiMapper.volumeID, string(csiMapper.driverName), string(nodeName))
	attachment := makeTestAttachment(attachID, nodeName, pvName)
	attachment.Status.Attached = true
	_, err = csiMapper.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to setup VolumeAttachment: %v", err)
	}
	t.Log("created attachment ", attachID)

	stagingPath, err := csiMapper.SetUpDevice()
	if err != nil {
		t.Fatalf("mapper failed to SetupDevice: %v", err)
	}

	// 校验 NodeStageVolume 是否 stage 到正确路径
	svols := csiMapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodeStagedVolumes()
	svol, ok := svols[csiMapper.volumeID]
	if !ok {
		t.Error("csi server may not have received NodeStageVolume call")
	}
	if svol.Path != stagingPath {
		t.Errorf("csi server expected device path %s, got %s", stagingPath, svol.Path)
	}
}

// TestBlockMapperSetupDeviceError 块设备映射器 SetUpDevice 错误处理与目录清理的测试。
func TestBlockMapperSetupDeviceError(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	csiMapper, _, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	pvName := pv.GetName()
	nodeName := string(plug.host.GetNodeName())

	csiMapper.csiClient = setupClient(t, true)
	fClient := csiMapper.csiClient.(*fakeCsiDriverClient)
	fClient.nodeClient.SetNextError(status.Error(codes.InvalidArgument, "mock final error"))

	attachID := getAttachmentName(csiMapper.volumeID, string(csiMapper.driverName), string(nodeName))
	attachment := makeTestAttachment(attachID, nodeName, pvName)
	attachment.Status.Attached = true
	_, err = csiMapper.k8s.StorageV1().VolumeAttachments().Create(context.Background(), attachment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to setup VolumeAttachment: %v", err)
	}
	t.Log("created attachment ", attachID)

	stagingPath, err := csiMapper.SetUpDevice()
	if err == nil {
		t.Fatal("mapper unexpectedly succeeded")
	}

	// 校验所有目录均已清理
	// 校验元数据 / staging / publish 目录均已删除
	dataDir := getVolumeDeviceDataDir(pv.ObjectMeta.Name, plug.host)
	if _, err := os.Stat(dataDir); err == nil {
		t.Errorf("volume publish data directory %s was not deleted", dataDir)
	}
	devDir := getVolumeDeviceDataDir(pv.ObjectMeta.Name, plug.host)
	if _, err := os.Stat(devDir); err == nil {
		t.Errorf("volume publish device directory %s was not deleted", devDir)
	}

	if _, err := os.Stat(stagingPath); err == nil {
		t.Errorf("volume staging path %s was not deleted", stagingPath)
	}
}

// TestBlockMapperSetupDeviceNoClientError CSI 客户端不可用时 SetUpDevice 返回瞬时错误的测试。
func TestBlockMapperSetupDeviceNoClientError(t *testing.T) {
	transientError := volumetypes.NewTransientOperationFailure("")
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	csiMapper, _, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	pvName := pv.GetName()
	nodeName := string(plug.host.GetNodeName())

	csiMapper.csiClient = setupClient(t, true)

	attachID := getAttachmentName(csiMapper.volumeID, string(csiMapper.driverName), string(nodeName))
	attachment := makeTestAttachment(attachID, nodeName, pvName)
	attachment.Status.Attached = true
	_, err = csiMapper.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to setup VolumeAttachment: %v", err)
	}
	t.Log("created attachment ", attachID)

	// 清空客户端缓存
	// 查询未知名称的驱动会失败，从而无法生成新客户端
	csiMapper.csiClient = nil
	csiMapper.csiClientGetter.csiClient = nil
	// 注意上面的 prepareBlockMapperTest 会创建名为 "test-driver" 的驱动
	csiMapper.csiClientGetter.driverName = "unknown-driver"

	_, err = csiMapper.SetUpDevice()
	if err == nil {
		t.Errorf("test should fail, but no error occurred")
	} else if reflect.TypeOf(transientError) != reflect.TypeOf(err) {
		t.Fatalf("expected exitError type: %v got: %v (%v)", reflect.TypeOf(transientError), reflect.TypeOf(err), err)
	}
}

// TestBlockMapperMapPodDevice 块设备映射器 MapPodDevice 的测试。
func TestBlockMapperMapPodDevice(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	csiMapper, _, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	pvName := pv.GetName()
	nodeName := string(plug.host.GetNodeName())

	csiMapper.csiClient = setupClient(t, true)

	attachID := getAttachmentName(csiMapper.volumeID, string(csiMapper.driverName), nodeName)
	attachment := makeTestAttachment(attachID, nodeName, pvName)
	attachment.Status.Attached = true
	_, err = csiMapper.k8s.StorageV1().VolumeAttachments().Create(context.Background(), attachment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to setup VolumeAttachment: %v", err)
	}
	t.Log("created attachment ", attachID)

	// 将设备映射到全局及 Pod 设备映射路径
	path, err := csiMapper.MapPodDevice()
	if err != nil {
		t.Fatalf("mapper failed to GetGlobalMapPath: %v", err)
	}

	// 校验 NodePublishVolume 是否 publish 到正确路径
	pvols := csiMapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodePublishedVolumes()
	pvol, ok := pvols[csiMapper.volumeID]
	if !ok {
		t.Error("csi server may not have received NodePublishVolume call")
	}

	publishPath := csiMapper.getPublishPath()
	if pvol.Path != publishPath {
		t.Errorf("csi server expected path %s, got %s", publishPath, pvol.Path)
	}
	if path != publishPath {
		t.Errorf("csi server expected path %s, but MapPodDevice returned %s", publishPath, path)
	}
}

// TestBlockMapperMapPodDeviceNotSupportAttach 驱动不支持 attach 时 MapPodDevice 的测试。
func TestBlockMapperMapPodDeviceNotSupportAttach(t *testing.T) {
	fakeClient := fakeclient.NewSimpleClientset()
	attachRequired := false
	fakeDriver := &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name: testDriver,
		},
		Spec: storagev1.CSIDriverSpec{
			AttachRequired: &attachRequired,
		},
	}
	_, err := fakeClient.StorageV1().CSIDrivers().Create(context.TODO(), fakeDriver, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create a fakeDriver: %v", err)
	}

	// 创建驱动后再创建插件；newTestPlugin 会等待 informer 同步，
	// 确保下面的 csiMapper.SetUpDevice 能在 lister 中看到 VolumeAttachment 对象。

	plug, tmpDir := newTestPlugin(t, fakeClient)
	defer os.RemoveAll(tmpDir)

	csiMapper, _, _, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}
	csiMapper.csiClient = setupClient(t, true)

	// 将设备映射到全局及 Pod 设备映射路径
	path, err := csiMapper.MapPodDevice()
	if err != nil {
		t.Fatalf("mapper failed to GetGlobalMapPath: %v", err)
	}
	publishPath := csiMapper.getPublishPath()
	if path != publishPath {
		t.Errorf("path %s and %s doesn't match", path, publishPath)
	}
}

// TestBlockMapperMapPodDeviceWithPodInfo 驱动要求 Pod 信息时 MapPodDevice 的测试。
func TestBlockMapperMapPodDeviceWithPodInfo(t *testing.T) {
	fakeClient := fakeclient.NewSimpleClientset()
	attachRequired := false
	podInfo := true
	fakeDriver := &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name: testDriver,
		},
		Spec: storagev1.CSIDriverSpec{
			PodInfoOnMount: &podInfo,
			AttachRequired: &attachRequired,
		},
	}
	_, err := fakeClient.StorageV1().CSIDrivers().Create(context.TODO(), fakeDriver, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create a fakeDriver: %v", err)
	}

	// 创建驱动后再创建插件；newTestPlugin 会等待 informer 同步，
	// 确保下面的 csiMapper.SetUpDevice 能在 lister 中看到 VolumeAttachment 对象。

	plug, tmpDir := newTestPlugin(t, fakeClient)
	defer os.RemoveAll(tmpDir)

	csiMapper, _, _, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}
	csiMapper.csiClient = setupClient(t, true)

	// 将设备映射到全局及 Pod 设备映射路径
	_, err = csiMapper.MapPodDevice()
	if err != nil {
		t.Fatalf("mapper failed to GetGlobalMapPath: %v", err)
	}
	pvols := csiMapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodePublishedVolumes()
	pvol, ok := pvols[csiMapper.volumeID]
	if !ok {
		t.Error("csi server may not have received NodePublishVolume call")
	}

	if !reflect.DeepEqual(pvol.VolumeContext, map[string]string{"csi.storage.k8s.io/pod.uid": "test-pod", "csi.storage.k8s.io/serviceAccount.name": "", "csi.storage.k8s.io/pod.name": "test-pod", "csi.storage.k8s.io/pod.namespace": "test-ns", "csi.storage.k8s.io/ephemeral": "false"}) {
		t.Error("csi mapper check pod info failed")
	}
}

// TestBlockMapperMapPodDeviceNoClientError CSI 客户端不可用时 MapPodDevice 返回瞬时错误的测试。
func TestBlockMapperMapPodDeviceNoClientError(t *testing.T) {
	transientError := volumetypes.NewTransientOperationFailure("")
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	csiMapper, _, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	pvName := pv.GetName()
	nodeName := string(plug.host.GetNodeName())

	csiMapper.csiClient = setupClient(t, true)

	attachID := getAttachmentName(csiMapper.volumeID, string(csiMapper.driverName), nodeName)
	attachment := makeTestAttachment(attachID, nodeName, pvName)
	attachment.Status.Attached = true
	_, err = csiMapper.k8s.StorageV1().VolumeAttachments().Create(context.Background(), attachment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to setup VolumeAttachment: %v", err)
	}
	t.Log("created attachment ", attachID)

	// 清空客户端缓存
	// 查询未知名称的驱动会失败，从而无法生成新客户端
	csiMapper.csiClient = nil
	csiMapper.csiClientGetter.csiClient = nil
	// 注意上面的 prepareBlockMapperTest 会创建名为 "test-driver" 的驱动
	csiMapper.csiClientGetter.driverName = "unknown-driver"

	_, err = csiMapper.MapPodDevice()
	if err == nil {
		t.Errorf("test should fail, but no error occurred")
	} else if reflect.TypeOf(transientError) != reflect.TypeOf(err) {
		t.Fatalf("expected exitError type: %v got: %v (%v)", reflect.TypeOf(transientError), reflect.TypeOf(err), err)
	}
}

// TestBlockMapperMapPodDeviceGetStageSecretsError 获取 stage secret 失败时 MapPodDevice 的错误处理测试。
func TestBlockMapperMapPodDeviceGetStageSecretsError(t *testing.T) {
	transientError := volumetypes.NewTransientOperationFailure("")
	plug, tmpDir := newTestPlugin(t, nil)
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Error(err)
		}
	}()

	csiMapper, _, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	// 为 PV 设置 stage secret
	pv.Spec.PersistentVolumeSource.CSI.NodePublishSecretRef = &api.SecretReference{
		Name:      "foo",
		Namespace: "default",
	}
	pvName := pv.GetName()
	nodeName := string(plug.host.GetNodeName())

	csiMapper.csiClient = setupClient(t, true)

	attachID := getAttachmentName(csiMapper.volumeID, string(csiMapper.driverName), nodeName)
	attachment := makeTestAttachment(attachID, nodeName, pvName)
	attachment.Status.Attached = true
	if _, err = csiMapper.k8s.StorageV1().VolumeAttachments().Create(context.Background(), attachment, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to setup VolumeAttachment: %v", err)
	}
	t.Log("created attachment ", attachID)

	_, err = csiMapper.MapPodDevice()
	if err == nil {
		t.Errorf("test should fail, but no error occurred")
	} else if !errors.As(err, &transientError) {
		t.Errorf("expected a transient error but got %v", err)
	}
}

// TestBlockMapperTearDownDevice 块设备映射器 TearDownDevice 的测试。
func TestBlockMapperTearDownDevice(t *testing.T) {
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	_, spec, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	// 保存卷数据
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

	unmapper, err := plug.NewBlockVolumeUnmapper(pv.ObjectMeta.Name, testPodUID)
	if err != nil {
		t.Fatalf("failed to make a new Unmapper: %v", err)
	}

	csiUnmapper := unmapper.(*csiBlockMapper)
	csiUnmapper.csiClient = setupClient(t, true)

	globalMapPath, err := csiUnmapper.GetGlobalMapPath(spec)
	if err != nil {
		t.Fatalf("unmapper failed to GetGlobalMapPath: %v", err)
	}

	err = csiUnmapper.TearDownDevice(globalMapPath, "/dev/test")
	if err != nil {
		t.Fatal(err)
	}

	// 校验 CSI 客户端确实调用了 node unpublish
	pubs := csiUnmapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodePublishedVolumes()
	if _, ok := pubs[csiUnmapper.volumeID]; ok {
		t.Error("csi server may not have received NodeUnpublishVolume call")
	}

	// 校验 CSI 客户端确实调用了 node unstage
	vols := csiUnmapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodeStagedVolumes()
	if _, ok := vols[csiUnmapper.volumeID]; ok {
		t.Error("csi server may not have received NodeUnstageVolume call")
	}
}

// TestBlockMapperTearDownDeviceNoClientError CSI 客户端不可用时 TearDownDevice 返回瞬时错误的测试。
func TestBlockMapperTearDownDeviceNoClientError(t *testing.T) {
	transientError := volumetypes.NewTransientOperationFailure("")
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	_, spec, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	// 保存卷数据
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

	unmapper, err := plug.NewBlockVolumeUnmapper(pv.ObjectMeta.Name, testPodUID)
	if err != nil {
		t.Fatalf("failed to make a new Unmapper: %v", err)
	}

	csiUnmapper := unmapper.(*csiBlockMapper)
	csiUnmapper.csiClient = setupClient(t, true)

	globalMapPath, err := csiUnmapper.GetGlobalMapPath(spec)
	if err != nil {
		t.Fatalf("unmapper failed to GetGlobalMapPath: %v", err)
	}

	// 清空客户端缓存
	// 查询未知名称的驱动会失败，从而无法生成新客户端
	csiUnmapper.csiClient = nil
	csiUnmapper.csiClientGetter.csiClient = nil
	// 注意上面的 prepareBlockMapperTest 会创建名为 "test-driver" 的驱动
	csiUnmapper.csiClientGetter.driverName = "unknown-driver"

	err = csiUnmapper.TearDownDevice(globalMapPath, "/dev/test")
	if err == nil {
		t.Errorf("test should fail, but no error occurred")
	} else if reflect.TypeOf(transientError) != reflect.TypeOf(err) {
		t.Fatalf("expected exitError type: %v got: %v (%v)", reflect.TypeOf(transientError), reflect.TypeOf(err), err)
	}
}

// TestVolumeSetupTeardown CSI 块设备完整创建与清理流程的测试。
func TestVolumeSetupTeardown(t *testing.T) {
	// 按 cs_block.go 开头的 setup + teardown 流程，创建并清理一个 CSI 块设备。
	// 重点验证清理完成后没有残留文件。

	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	csiMapper, spec, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	pvName := pv.GetName()
	nodeName := string(plug.host.GetNodeName())

	csiMapper.csiClient = setupClient(t, true)

	attachID := getAttachmentName(csiMapper.volumeID, string(csiMapper.driverName), string(nodeName))
	attachment := makeTestAttachment(attachID, nodeName, pvName)
	attachment.Status.Attached = true
	_, err = csiMapper.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to setup VolumeAttachment: %v", err)
	}
	t.Log("created attachment ", attachID)

	stagingPath, err := csiMapper.SetUpDevice()
	if err != nil {
		t.Fatalf("mapper failed to SetupDevice: %v", err)
	}
	// 校验 NodeStageVolume 是否 stage 到正确路径
	svols := csiMapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodeStagedVolumes()
	svol, ok := svols[csiMapper.volumeID]
	if !ok {
		t.Error("csi server may not have received NodeStageVolume call")
	}
	if svol.Path != stagingPath {
		t.Errorf("csi server expected device path %s, got %s", stagingPath, svol.Path)
	}

	path, err := csiMapper.MapPodDevice()
	if err != nil {
		t.Fatalf("mapper failed to GetGlobalMapPath: %v", err)
	}
	pvols := csiMapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodePublishedVolumes()
	pvol, ok := pvols[csiMapper.volumeID]
	if !ok {
		t.Error("csi server may not have received NodePublishVolume call")
	}
	publishPath := csiMapper.getPublishPath()
	if pvol.Path != publishPath {
		t.Errorf("csi server expected path %s, got %s", publishPath, pvol.Path)
	}
	if path != publishPath {
		t.Errorf("csi server expected path %s, but MapPodDevice returned %s", publishPath, path)
	}

	unmapper, err := plug.NewBlockVolumeUnmapper(pv.ObjectMeta.Name, testPodUID)
	if err != nil {
		t.Fatalf("failed to make a new Unmapper: %v", err)
	}

	csiUnmapper := unmapper.(*csiBlockMapper)
	csiUnmapper.csiClient = csiMapper.csiClient

	globalMapPath, err := csiUnmapper.GetGlobalMapPath(spec)
	if err != nil {
		t.Fatalf("unmapper failed to GetGlobalMapPath: %v", err)
	}

	err = csiUnmapper.UnmapPodDevice()
	if err != nil {
		t.Errorf("unmapper failed to call UnmapPodDevice: %v", err)
	}

	// GenerateUnmapDeviceFunc 使用 "" 作为 Pod UUID，是对所有使用该卷的 Pod 的全局操作
	unmapper, err = plug.NewBlockVolumeUnmapper(pv.ObjectMeta.Name, "")
	if err != nil {
		t.Fatalf("failed to make a new Unmapper: %v", err)
	}
	csiUnmapper = unmapper.(*csiBlockMapper)
	csiUnmapper.csiClient = csiMapper.csiClient

	err = csiUnmapper.TearDownDevice(globalMapPath, "/dev/test")
	if err != nil {
		t.Fatal(err)
	}
	pubs := csiUnmapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodePublishedVolumes()
	if _, ok := pubs[csiUnmapper.volumeID]; ok {
		t.Error("csi server may not have received NodeUnpublishVolume call")
	}
	vols := csiUnmapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodeStagedVolumes()
	if _, ok := vols[csiUnmapper.volumeID]; ok {
		t.Error("csi server may not have received NodeUnstageVolume call")
	}

	// 校验元数据 / staging / publish 目录均已删除
	dataDir := getVolumeDeviceDataDir(pv.ObjectMeta.Name, plug.host)
	if _, err := os.Stat(dataDir); err == nil {
		t.Errorf("volume publish data directory %s was not deleted", dataDir)
	}
	devDir := getVolumeDeviceDataDir(pv.ObjectMeta.Name, plug.host)
	if _, err := os.Stat(devDir); err == nil {
		t.Errorf("volume publish device directory %s was not deleted", devDir)
	}
	if _, err := os.Stat(publishPath); err == nil {
		t.Errorf("volume publish path %s was not deleted", publishPath)
	}
	publishDir := filepath.Dir(publishPath)
	if _, err := os.Stat(publishDir); err == nil {
		t.Errorf("volume publish parent directory %s was not deleted", publishDir)
	}
	if _, err := os.Stat(stagingPath); err == nil {
		t.Errorf("volume staging path %s was not deleted", stagingPath)
	}
}

// TestUnmapPodDeviceNoClientError CSI 客户端不可用时 UnmapPodDevice 返回瞬时错误的测试。
func TestUnmapPodDeviceNoClientError(t *testing.T) {
	transientError := volumetypes.NewTransientOperationFailure("")
	plug, tmpDir := newTestPlugin(t, nil)
	defer os.RemoveAll(tmpDir)

	csiMapper, spec, pv, err := prepareBlockMapperTest(plug, "test-pv", t)
	if err != nil {
		t.Fatalf("Failed to make a new Mapper: %v", err)
	}

	pvName := pv.GetName()
	nodeName := string(plug.host.GetNodeName())

	csiMapper.csiClient = setupClient(t, true)

	attachID := getAttachmentName(csiMapper.volumeID, string(csiMapper.driverName), string(nodeName))
	attachment := makeTestAttachment(attachID, nodeName, pvName)
	attachment.Status.Attached = true
	_, err = csiMapper.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to setup VolumeAttachment: %v", err)
	}
	t.Log("created attachment ", attachID)

	stagingPath, err := csiMapper.SetUpDevice()
	if err != nil {
		t.Fatalf("mapper failed to SetupDevice: %v", err)
	}
	// 校验 NodeStageVolume 是否 stage 到正确路径
	svols := csiMapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodeStagedVolumes()
	svol, ok := svols[csiMapper.volumeID]
	if !ok {
		t.Error("csi server may not have received NodeStageVolume call")
	}
	if svol.Path != stagingPath {
		t.Errorf("csi server expected device path %s, got %s", stagingPath, svol.Path)
	}

	path, err := csiMapper.MapPodDevice()
	if err != nil {
		t.Fatalf("mapper failed to GetGlobalMapPath: %v", err)
	}
	pvols := csiMapper.csiClient.(*fakeCsiDriverClient).nodeClient.GetNodePublishedVolumes()
	pvol, ok := pvols[csiMapper.volumeID]
	if !ok {
		t.Error("csi server may not have received NodePublishVolume call")
	}
	publishPath := csiMapper.getPublishPath()
	if pvol.Path != publishPath {
		t.Errorf("csi server expected path %s, got %s", publishPath, pvol.Path)
	}
	if path != publishPath {
		t.Errorf("csi server expected path %s, but MapPodDevice returned %s", publishPath, path)
	}

	unmapper, err := plug.NewBlockVolumeUnmapper(pv.ObjectMeta.Name, testPodUID)
	if err != nil {
		t.Fatalf("failed to make a new Unmapper: %v", err)
	}

	csiUnmapper := unmapper.(*csiBlockMapper)
	csiUnmapper.csiClient = csiMapper.csiClient

	_, err = csiUnmapper.GetGlobalMapPath(spec)
	if err != nil {
		t.Fatalf("unmapper failed to GetGlobalMapPath: %v", err)
	}

	// 清空客户端缓存
	// 查询未知名称的驱动会失败，从而无法生成新客户端
	csiUnmapper.csiClient = nil
	csiUnmapper.csiClientGetter.csiClient = nil
	// 注意上面的 prepareBlockMapperTest 会创建名为 "test-driver" 的驱动
	csiUnmapper.csiClientGetter.driverName = "unknown-driver"

	err = csiUnmapper.UnmapPodDevice()
	if err == nil {
		t.Errorf("test should fail, but no error occurred")
	} else if reflect.TypeOf(transientError) != reflect.TypeOf(err) {
		t.Fatalf("expected exitError type: %v got: %v (%v)", reflect.TypeOf(transientError), reflect.TypeOf(err), err)
	}
}
