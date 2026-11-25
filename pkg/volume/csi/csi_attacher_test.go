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
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/kubernetes/pkg/volume"
	fakecsi "k8s.io/kubernetes/pkg/volume/csi/fake"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

const (
	// testWatchTimeout 是等待卷附件状态变化的默认超时时间。
	testWatchTimeout = 10 * time.Second
	// testWatchFailTimeout 缩短预期失败用例的等待时间，避免拖慢测试。
	testWatchFailTimeout = 2 * time.Second
)

var (
	// bFalse 提供可复用的 bool 指针，供 CSIDriver 测试用例取址。
	bFalse = false
	// bTrue 提供可复用的 bool 指针，供 CSIDriver 测试用例取址。
	bTrue = true
)

// makeTestAttachment 构造指定 ID、节点和 PV 的卷附件，初始状态为未挂载。
func makeTestAttachment(attachID, nodeName, pvName string) *storage.VolumeAttachment {
	return &storage.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: attachID,
		},
		Spec: storage.VolumeAttachmentSpec{
			NodeName: nodeName,
			Attacher: "mock",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
		},
		Status: storage.VolumeAttachmentStatus{
			Attached:    false,
			AttachError: nil,
			DetachError: nil,
		},
	}
}

// markVolumeAttached 轮询客户端直到卷附件出现，再写入状态并通知 watcher。
// 附件由 Attach 的异步 goroutine 创建，必须等其落库后才能更新，否则会漏改。
func markVolumeAttached(t *testing.T, client clientset.Interface, watch *watch.RaceFreeFakeWatcher, attachID string, status storage.VolumeAttachmentStatus) {
	ticker := time.NewTicker(10 * time.Millisecond)
	var attach *storage.VolumeAttachment
	var err error
	defer ticker.Stop()
	// 等待附件被保存，避免异步 goroutine 尚未创建就更新。
	for i := 0; i < 100; i++ {
		attach, err = client.StorageV1().VolumeAttachments().Get(context.TODO(), attachID, metav1.GetOptions{})
		if err != nil {
			attach = nil
			if apierrors.IsNotFound(err) {
				<-ticker.C
				continue
			}
			t.Error(err)
		}
		if attach != nil {
			t.Logf("attachment found on try %d, stopping wait...", i)
			break
		}
	}
	t.Logf("stopped waiting for attachment")

	if attach == nil {
		t.Logf("attachment not found for id:%v", attachID)
	} else {
		attach.Status = status
		t.Logf("updating attachment %s with attach status %v", attachID, status)
		_, err := client.StorageV1().VolumeAttachments().Update(context.TODO(), attach, metav1.UpdateOptions{})
		if err != nil {
			t.Error(err)
		}
		if watch != nil {
			watch.Modify(attach)
		}
	}
}

// TestAttacherAttach 验证 Attach 在卷与驱动匹配时成功，不匹配或驱动报错时返回错误。
func TestAttacherAttach(t *testing.T) {
	testCases := []struct {
		name                string
		nodeName            string
		driverName          string
		volumeName          string
		attachID            string
		spec                *volume.Spec
		injectAttacherError bool
		shouldFail          bool
		watchTimeout        time.Duration
	}{
		{
			name:       "test ok 1",
			nodeName:   "testnode-01",
			driverName: "testdriver-01",
			volumeName: "testvol-01",
			attachID:   getAttachmentName("testvol-01", "testdriver-01", "testnode-01"),
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("pv01", 10, "testdriver-01", "testvol-01"), false),
		},
		{
			name:       "test ok 2",
			nodeName:   "node02",
			driverName: "driver02",
			volumeName: "vol02",
			attachID:   getAttachmentName("vol02", "driver02", "node02"),
			spec:       volume.NewSpecFromPersistentVolume(makeTestPV("pv01", 10, "driver02", "vol02"), false),
		},
		{
			name:         "mismatch vol",
			nodeName:     "node02",
			driverName:   "driver02",
			volumeName:   "vol01",
			attachID:     getAttachmentName("vol02", "driver02", "node02"),
			spec:         volume.NewSpecFromPersistentVolume(makeTestPV("pv01", 10, "driver02", "vol01"), false),
			shouldFail:   true,
			watchTimeout: testWatchFailTimeout,
		},
		{
			name:         "mismatch driver",
			nodeName:     "node02",
			driverName:   "driver000",
			volumeName:   "vol02",
			attachID:     getAttachmentName("vol02", "driver02", "node02"),
			spec:         volume.NewSpecFromPersistentVolume(makeTestPV("pv01", 10, "driver01", "vol02"), false),
			shouldFail:   true,
			watchTimeout: testWatchFailTimeout,
		},
		{
			name:         "mismatch node",
			nodeName:     "node000",
			driverName:   "driver000",
			volumeName:   "vol02",
			attachID:     getAttachmentName("vol02", "driver02", "node02"),
			spec:         volume.NewSpecFromPersistentVolume(makeTestPV("pv01", 10, "driver02", "vol02"), false),
			shouldFail:   true,
			watchTimeout: testWatchFailTimeout,
		},
		{
			name:                "attacher error",
			nodeName:            "node02",
			driverName:          "driver02",
			volumeName:          "vol02",
			attachID:            getAttachmentName("vol02", "driver02", "node02"),
			spec:                volume.NewSpecFromPersistentVolume(makeTestPV("pv01", 10, "driver02", "vol02"), false),
			injectAttacherError: true,
			shouldFail:          true,
		},
		{
			name:       "test with volume source",
			nodeName:   "node000",
			driverName: "driver000",
			volumeName: "vol02",
			attachID:   getAttachmentName("vol02", "driver02", "node02"),
			spec:       volume.NewSpecFromVolume(makeTestVol("pv01", "driver02")),
			shouldFail: true, // 内联卷源未启用 CSI
		},
		{
			name:       "missing spec",
			nodeName:   "node000",
			driverName: "driver000",
			volumeName: "vol02",
			attachID:   getAttachmentName("vol02", "driver02", "node02"),
			shouldFail: true, // 缺少 spec 时无法挂载
		},
	}

	// 逐个执行 attacher 用例
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("test case: %s", tc.name)
			fakeClient := fakeclient.NewSimpleClientset()
			plug, tmpDir := newTestPluginWithAttachDetachVolumeHost(t, fakeClient)
			defer os.RemoveAll(tmpDir)

			attacher, err := plug.NewAttacher()
			if err != nil {
				t.Fatalf("failed to create new attacher: %v", err)
			}

			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, tc.watchTimeout)

			var wg sync.WaitGroup
			wg.Add(1)
			go func(spec *volume.Spec, nodename string, fail bool) {
				defer wg.Done()
				attachID, err := csiAttacher.Attach(spec, types.NodeName(nodename))
				if !fail && err != nil {
					t.Errorf("expecting no failure, but got err: %v", err)
				}
				if fail && err == nil {
					t.Errorf("expecting failure, but got no err")
				}
				if attachID != "" {
					t.Errorf("expecting empty attachID, got %v", attachID)
				}
			}(tc.spec, tc.nodeName, tc.shouldFail)

			var status storage.VolumeAttachmentStatus
			if tc.injectAttacherError {
				status.Attached = false
				status.AttachError = &storage.VolumeError{
					Message: "attacher error",
				}
			} else {
				status.Attached = true
			}
			markVolumeAttached(t, csiAttacher.k8s, nil, tc.attachID, status)
			wg.Wait()
		})
	}
}

// TestAttacherAttachWithInline 验证内联卷源场景下 Attach 的行为：PV 可挂载，卷源与缺失 spec 均失败。
func TestAttacherAttachWithInline(t *testing.T) {
	testCases := []struct {
		name                string
		nodeName            string
		driverName          string
		volumeName          string
		attachID            string
		spec                *volume.Spec
		injectAttacherError bool
		shouldFail          bool
		watchTimeout        time.Duration
	}{
		{
			name:     "test ok 1 with PV",
			nodeName: "node01",
			attachID: getAttachmentName("vol01", "driver01", "node01"),
			spec:     volume.NewSpecFromPersistentVolume(makeTestPV("pv01", 10, "driver01", "vol01"), false),
		},
		{
			name:       "test failure, attach with volSrc",
			nodeName:   "node01",
			attachID:   getAttachmentName("vol01", "driver01", "node01"),
			spec:       volume.NewSpecFromVolume(makeTestVol("vol01", "driver01")),
			shouldFail: true,
		},
		{
			name:                "attacher error",
			nodeName:            "node02",
			attachID:            getAttachmentName("vol02", "driver02", "node02"),
			spec:                volume.NewSpecFromPersistentVolume(makeTestPV("pv02", 10, "driver02", "vol02"), false),
			injectAttacherError: true,
			shouldFail:          true,
		},
		{
			name:       "missing spec",
			nodeName:   "node02",
			attachID:   getAttachmentName("vol02", "driver02", "node02"),
			shouldFail: true,
		},
	}

	// 逐个执行 attacher 用例
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("test case: %s", tc.name)
			fakeClient := fakeclient.NewSimpleClientset()
			plug, tmpDir := newTestPluginWithAttachDetachVolumeHost(t, fakeClient)
			defer os.RemoveAll(tmpDir)

			attacher, err := plug.NewAttacher()
			if err != nil {
				t.Fatalf("failed to create new attacher: %v", err)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, tc.watchTimeout)

			var wg sync.WaitGroup
			wg.Add(1)
			go func(spec *volume.Spec, nodename string, fail bool) {
				defer wg.Done()
				attachID, err := csiAttacher.Attach(spec, types.NodeName(nodename))
				if fail != (err != nil) {
					t.Errorf("expecting no failure, but got err: %v", err)
				}
				if attachID != "" {
					t.Errorf("expecting empty attachID, got %v", attachID)
				}
			}(tc.spec, tc.nodeName, tc.shouldFail)

			var status storage.VolumeAttachmentStatus
			if tc.injectAttacherError {
				status.Attached = false
				status.AttachError = &storage.VolumeError{
					Message: "attacher error",
				}
			} else {
				status.Attached = true
			}
			markVolumeAttached(t, csiAttacher.k8s, nil, tc.attachID, status)
			wg.Wait()
		})
	}
}

// TestAttacherWithCSIDriver 验证 CanAttach 与 Attach 遵循 CSIDriver 的 AttachRequired 配置。
func TestAttacherWithCSIDriver(t *testing.T) {
	tests := []struct {
		name                   string
		driver                 string
		expectVolumeAttachment bool
		watchTimeout           time.Duration
	}{
		{
			name:                   "CSIDriver not attachable",
			driver:                 "not-attachable",
			expectVolumeAttachment: false,
		},
		{
			name:                   "CSIDriver is attachable",
			driver:                 "attachable",
			expectVolumeAttachment: true,
		},
		{
			name:                   "CSIDriver.AttachRequired not set  -> failure",
			driver:                 "nil",
			expectVolumeAttachment: true,
		},
		{
			name:                   "CSIDriver does not exist not set  -> failure",
			driver:                 "unknown",
			expectVolumeAttachment: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeClient := fakeclient.NewSimpleClientset(
				getTestCSIDriver("not-attachable", nil, &bFalse, nil),
				getTestCSIDriver("attachable", nil, &bTrue, nil),
				getTestCSIDriver("nil", nil, nil, nil),
			)
			plug, tmpDir := newTestPluginWithAttachDetachVolumeHost(t, fakeClient)
			defer os.RemoveAll(tmpDir)

			attacher, err := plug.NewAttacher()
			if err != nil {
				t.Fatalf("failed to create new attacher: %v", err)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, test.watchTimeout)
			spec := volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, test.driver, "test-vol"), false)

			pluginCanAttach, err := plug.CanAttach(spec)
			if err != nil {
				t.Fatalf("attacher.CanAttach failed: %s", err)
			}
			if pluginCanAttach != test.expectVolumeAttachment {
				t.Errorf("attacher.CanAttach does not match expected attachment status %t", test.expectVolumeAttachment)
			}

			if !pluginCanAttach {
				t.Log("plugin is not attachable")
				return
			}
			var wg sync.WaitGroup
			wg.Add(1)
			go func(volSpec *volume.Spec) {
				attachID, err := csiAttacher.Attach(volSpec, "fakeNode")
				defer wg.Done()

				if err != nil {
					t.Errorf("Attach() failed: %s", err)
				}
				if attachID != "" {
					t.Errorf("Expected empty attachID, got %q", attachID)
				}
			}(spec)

			if test.expectVolumeAttachment {
				expectedAttachID := getAttachmentName("test-vol", test.driver, "fakeNode")
				status := storage.VolumeAttachmentStatus{
					Attached: true,
				}
				markVolumeAttached(t, csiAttacher.k8s, nil, expectedAttachID, status)
			}
			wg.Wait()
		})
	}
}

// TestAttacherWaitForVolumeAttachmentWithCSIDriver 验证不可挂载驱动会跳过 WaitForAttach。
func TestAttacherWaitForVolumeAttachmentWithCSIDriver(t *testing.T) {
	// 故意不创建任何卷附件：若插件未对不可挂载驱动跳过等待，WaitForVolumeAttachment 会因找不到附件而报错。
	tests := []struct {
		name         string
		driver       string
		expectError  bool
		watchTimeout time.Duration
	}{
		{
			name:        "CSIDriver not attachable -> success",
			driver:      "not-attachable",
			expectError: false,
		},
		{
			name:        "CSIDriver is attachable -> failure",
			driver:      "attachable",
			expectError: true,
		},
		{
			name:        "CSIDriver.AttachRequired not set  -> failure",
			driver:      "nil",
			expectError: true,
		},
		{
			name:        "CSIDriver does not exist not set  -> failure",
			driver:      "unknown",
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeClient := fakeclient.NewSimpleClientset(
				getTestCSIDriver("not-attachable", nil, &bFalse, nil),
				getTestCSIDriver("attachable", nil, &bTrue, nil),
				getTestCSIDriver("nil", nil, nil, nil),
				&v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakeNode",
					},
					Spec: v1.NodeSpec{},
				},
			)
			plug, tmpDir := newTestPlugin(t, fakeClient)
			defer os.RemoveAll(tmpDir)

			attacher, err := plug.NewAttacher()
			if err != nil {
				t.Fatalf("failed to create new attacher: %v", err)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, test.watchTimeout)
			spec := volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, test.driver, "test-vol"), false)

			pluginCanAttach, err := plug.CanAttach(spec)
			if err != nil {
				t.Fatalf("plugin.CanAttach test failed: %s", err)
			}
			if !pluginCanAttach {
				t.Log("plugin is not attachable")
				return
			}

			_, err = csiAttacher.WaitForAttach(spec, "", nil, time.Second)
			if err != nil && !test.expectError {
				t.Errorf("Unexpected error: %s", err)
			}
			if err == nil && test.expectError {
				t.Errorf("Expected error, got none")
			}
		})
	}
}

// TestAttacherWaitForAttach 验证 WaitForAttach 返回已挂载附件的 ID，附件缺失或卷源无效时报错。
func TestAttacherWaitForAttach(t *testing.T) {
	tests := []struct {
		name             string
		driver           string
		makeAttachment   func() *storage.VolumeAttachment
		spec             *volume.Spec
		expectedAttachID string
		expectError      bool
		watchTimeout     time.Duration
	}{
		{
			name:   "successful attach",
			driver: "attachable",
			makeAttachment: func() *storage.VolumeAttachment {

				testAttachID := getAttachmentName("test-vol", "attachable", "fakeNode")
				successfulAttachment := makeTestAttachment(testAttachID, "fakeNode", "test-pv")
				successfulAttachment.Status.Attached = true
				return successfulAttachment
			},
			spec:             volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, "attachable", "test-vol"), false),
			expectedAttachID: getAttachmentName("test-vol", "attachable", "fakeNode"),
			expectError:      false,
		},
		{
			name: "failed attach with vol source",
			makeAttachment: func() *storage.VolumeAttachment {

				testAttachID := getAttachmentName("test-vol", "attachable", "fakeNode")
				successfulAttachment := makeTestAttachment(testAttachID, "fakeNode", "volSrc01")
				successfulAttachment.Status.Attached = true
				return successfulAttachment
			},
			spec:        volume.NewSpecFromVolume(makeTestVol("volSrc01", "attachable")),
			expectError: true,
		},
		{
			name:        "failed attach",
			driver:      "attachable",
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeClient := fakeclient.NewSimpleClientset()
			plug, tmpDir := newTestPlugin(t, fakeClient)
			defer os.RemoveAll(tmpDir)

			attacher, err := plug.NewAttacher()
			if err != nil {
				t.Fatalf("failed to create new attacher: %v", err)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, test.watchTimeout)

			if test.makeAttachment != nil {
				attachment := test.makeAttachment()
				_, err = csiAttacher.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create VolumeAttachment: %v", err)
				}
				gotAttachment, err := csiAttacher.k8s.StorageV1().VolumeAttachments().Get(context.TODO(), attachment.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("failed to get created VolumeAttachment: %v", err)
				}
				t.Logf("created test VolumeAttachment %+v", gotAttachment)
			}

			attachID, err := csiAttacher.WaitForAttach(test.spec, "", nil, time.Second)
			if err != nil && !test.expectError {
				t.Errorf("Unexpected error: %s", err)
			}
			if err == nil && test.expectError {
				t.Errorf("Expected error, got none")
			}
			if attachID != test.expectedAttachID {
				t.Errorf("Expected attachID %q, got %q", test.expectedAttachID, attachID)
			}
		})
	}
}

// TestAttacherWaitForAttachWithInline 验证内联卷源场景下 WaitForAttach 的行为：PV 附件返回 ID，其余情况报错。
func TestAttacherWaitForAttachWithInline(t *testing.T) {
	tests := []struct {
		name             string
		driver           string
		makeAttachment   func() *storage.VolumeAttachment
		spec             *volume.Spec
		expectedAttachID string
		expectError      bool
		watchTimeout     time.Duration
	}{
		{
			name: "successful attach with PV",
			makeAttachment: func() *storage.VolumeAttachment {

				testAttachID := getAttachmentName("test-vol", "attachable", "fakeNode")
				successfulAttachment := makeTestAttachment(testAttachID, "fakeNode", "test-pv")
				successfulAttachment.Status.Attached = true
				return successfulAttachment
			},
			spec:             volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, "attachable", "test-vol"), false),
			expectedAttachID: getAttachmentName("test-vol", "attachable", "fakeNode"),
			expectError:      false,
		},
		{
			name: "failed attach with volSrc",
			makeAttachment: func() *storage.VolumeAttachment {

				testAttachID := getAttachmentName("test-vol", "attachable", "fakeNode")
				successfulAttachment := makeTestAttachment(testAttachID, "fakeNode", "volSrc01")
				successfulAttachment.Status.Attached = true
				return successfulAttachment
			},
			spec:        volume.NewSpecFromVolume(makeTestVol("volSrc01", "attachable")),
			expectError: true,
		},
		{
			name:        "failed attach",
			driver:      "non-attachable",
			spec:        volume.NewSpecFromPersistentVolume(makeTestPV("test-pv", 10, "non-attachable", "test-vol"), false),
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeClient := fakeclient.NewSimpleClientset()
			plug, tmpDir := newTestPlugin(t, fakeClient)
			defer os.RemoveAll(tmpDir)

			attacher, err := plug.NewAttacher()
			if err != nil {
				t.Fatalf("failed to create new attacher: %v", err)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, test.watchTimeout)

			if test.makeAttachment != nil {
				attachment := test.makeAttachment()
				_, err = csiAttacher.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create VolumeAttachment: %v", err)
				}
				gotAttachment, err := csiAttacher.k8s.StorageV1().VolumeAttachments().Get(context.TODO(), attachment.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("failed to get created VolumeAttachment: %v", err)
				}
				t.Logf("created test VolumeAttachment %+v", gotAttachment)
			}

			attachID, err := csiAttacher.WaitForAttach(test.spec, "", nil, time.Second)
			if test.expectError != (err != nil) {
				t.Errorf("Unexpected error: %s", err)
				return
			}
			if attachID != test.expectedAttachID {
				t.Errorf("Expected attachID %q, got %q", test.expectedAttachID, attachID)
			}
		})
	}
}

// TestAttacherVolumesAreAttached 验证 VolumesAreAttached 按附件的 Attached 状态返回每个 spec 的挂载状态。
func TestAttacherVolumesAreAttached(t *testing.T) {
	type attachedSpec struct {
		volName  string
		spec     *volume.Spec
		attached bool
	}
	testCases := []struct {
		name          string
		attachedSpecs []attachedSpec
		watchTimeout  time.Duration
	}{
		{
			name: "attach and detach",
			attachedSpecs: []attachedSpec{
				{"vol0", volume.NewSpecFromPersistentVolume(makeTestPV("pv0", 10, testDriver, "vol0"), false), true},
				{"vol1", volume.NewSpecFromPersistentVolume(makeTestPV("pv1", 20, testDriver, "vol1"), false), true},
				{"vol2", volume.NewSpecFromPersistentVolume(makeTestPV("pv2", 10, testDriver, "vol2"), false), false},
				{"vol3", volume.NewSpecFromPersistentVolume(makeTestPV("pv3", 10, testDriver, "vol3"), false), false},
				{"vol4", volume.NewSpecFromPersistentVolume(makeTestPV("pv4", 20, testDriver, "vol4"), false), true},
			},
		},
		{
			name: "all detached",
			attachedSpecs: []attachedSpec{
				{"vol0", volume.NewSpecFromPersistentVolume(makeTestPV("pv0", 10, testDriver, "vol0"), false), false},
				{"vol1", volume.NewSpecFromPersistentVolume(makeTestPV("pv1", 20, testDriver, "vol1"), false), false},
				{"vol2", volume.NewSpecFromPersistentVolume(makeTestPV("pv2", 10, testDriver, "vol2"), false), false},
			},
		},
		{
			name: "all attached",
			attachedSpecs: []attachedSpec{
				{"vol0", volume.NewSpecFromPersistentVolume(makeTestPV("pv0", 10, testDriver, "vol0"), false), true},
				{"vol1", volume.NewSpecFromPersistentVolume(makeTestPV("pv1", 20, testDriver, "vol1"), false), true},
			},
		},
		{
			name: "include non-attable",
			attachedSpecs: []attachedSpec{
				{"vol0", volume.NewSpecFromPersistentVolume(makeTestPV("pv0", 10, testDriver, "vol0"), false), true},
				{"vol1", volume.NewSpecFromVolume(makeTestVol("pv1", testDriver)), false},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plug, tmpDir := newTestPluginWithAttachDetachVolumeHost(t, nil)
			defer os.RemoveAll(tmpDir)

			attacher, err := plug.NewAttacher()
			if err != nil {
				t.Fatalf("failed to create new attacher: %v", err)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, tc.watchTimeout)
			nodeName := "fakeNode"

			var specs []*volume.Spec
			// 创建并保存卷附件
			for _, attachedSpec := range tc.attachedSpecs {
				specs = append(specs, attachedSpec.spec)
				attachID := getAttachmentName(attachedSpec.volName, testDriver, nodeName)
				attachment := makeTestAttachment(attachID, nodeName, attachedSpec.spec.Name())
				attachment.Status.Attached = attachedSpec.attached
				_, err := csiAttacher.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to attach: %v", err)
				}
			}

			// 查询挂载状态
			stats, err := csiAttacher.VolumesAreAttached(specs, types.NodeName(nodeName))
			if err != nil {
				t.Fatal(err)
			}
			if len(tc.attachedSpecs) != len(stats) {
				t.Errorf("expecting %d attachment status, got %d", len(tc.attachedSpecs), len(stats))
			}

			// 逐个 spec 比较挂载状态
			for _, attached := range tc.attachedSpecs {
				stat, ok := stats[attached.spec]
				if attached.attached && !ok {
					t.Error("failed to retrieve attached status for:", attached.spec)
				}
				if attached.attached != stat {
					t.Errorf("expecting volume attachment %t, got %t", attached.attached, stat)
				}
			}
		})
	}
}

// TestAttacherVolumesAreAttachedWithInline 验证内联卷源场景下 VolumesAreAttached 只返回 PV 的挂载状态。
func TestAttacherVolumesAreAttachedWithInline(t *testing.T) {
	type attachedSpec struct {
		volName  string
		spec     *volume.Spec
		attached bool
	}
	testCases := []struct {
		name          string
		attachedSpecs []attachedSpec
		watchTimeout  time.Duration
	}{
		{
			name: "attach and detach with volume sources",
			attachedSpecs: []attachedSpec{
				{"vol0", volume.NewSpecFromPersistentVolume(makeTestPV("pv0", 10, testDriver, "vol0"), false), true},
				{"vol1", volume.NewSpecFromVolume(makeTestVol("pv1", testDriver)), false},
				{"vol2", volume.NewSpecFromPersistentVolume(makeTestPV("pv2", 10, testDriver, "vol2"), false), true},
				{"vol3", volume.NewSpecFromVolume(makeTestVol("pv3", testDriver)), false},
				{"vol4", volume.NewSpecFromPersistentVolume(makeTestPV("pv4", 20, testDriver, "vol4"), false), true},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plug, tmpDir := newTestPlugin(t, nil)
			defer os.RemoveAll(tmpDir)

			attacher, err := plug.NewAttacher()
			if err != nil {
				t.Fatalf("failed to create new attacher: %v", err)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, tc.watchTimeout)
			nodeName := "fakeNode"

			var specs []*volume.Spec
			// 创建并保存卷附件
			for _, attachedSpec := range tc.attachedSpecs {
				specs = append(specs, attachedSpec.spec)
				attachID := getAttachmentName(attachedSpec.volName, testDriver, nodeName)
				attachment := makeTestAttachment(attachID, nodeName, attachedSpec.spec.Name())
				attachment.Status.Attached = attachedSpec.attached
				_, err := csiAttacher.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to attach: %v", err)
				}
			}

			// 查询挂载状态
			stats, err := csiAttacher.VolumesAreAttached(specs, types.NodeName(nodeName))
			if err != nil {
				t.Fatal(err)
			}
			if len(tc.attachedSpecs) != len(stats) {
				t.Errorf("expecting %d attachment status, got %d", len(tc.attachedSpecs), len(stats))
			}

			// 逐个 spec 比较挂载状态
			for _, attached := range tc.attachedSpecs {
				stat, ok := stats[attached.spec]
				if attached.attached && !ok {
					t.Error("failed to retrieve attached status for:", attached.spec)
				}
				if attached.attached != stat {
					t.Errorf("expecting volume attachment %t, got %t", attached.attached, stat)
				}
			}
		})
	}
}

// TestAttacherDetach 验证 Detach 删除卷附件，NotFound 视为成功，其余 API 错误向上传播。
func TestAttacherDetach(t *testing.T) {
	nodeName := "fakeNode"
	testCases := []struct {
		name         string
		volID        string
		attachID     string
		shouldFail   bool
		reactor      func(action core.Action) (handled bool, ret runtime.Object, err error)
		watchTimeout time.Duration
	}{
		{name: "normal test", volID: "vol-001", attachID: getAttachmentName("vol-001", testDriver, nodeName)},
		{name: "normal test 2", volID: "vol-002", attachID: getAttachmentName("vol-002", testDriver, nodeName)},
		{name: "object not found", volID: "vol-non-existing", attachID: getAttachmentName("vol-003", testDriver, nodeName)},
		{
			name:       "API error",
			volID:      "vol-004",
			attachID:   getAttachmentName("vol-004", testDriver, nodeName),
			shouldFail: true, // 除 NotFound 外的 API 错误都应传播给调用方
			reactor: func(action core.Action) (handled bool, ret runtime.Object, err error) {
				// 对所有 DELETE 请求返回 Forbidden，模拟权限错误
				if action.Matches("delete", "volumeattachments") {
					return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), action.GetNamespace(), fmt.Errorf("mock error"))
				}
				return false, nil, nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("running test: %v", tc.name)
			fakeClient := fakeclient.NewSimpleClientset()
			plug, tmpDir := newTestPluginWithAttachDetachVolumeHost(t, fakeClient)
			defer os.RemoveAll(tmpDir)

			if tc.reactor != nil {
				fakeClient.PrependReactor("*", "*", tc.reactor)
			}

			attacher, err0 := plug.NewAttacher()
			if err0 != nil {
				t.Fatalf("failed to create new attacher: %v", err0)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, tc.watchTimeout)

			pv := makeTestPV("test-pv", 10, testDriver, tc.volID)
			spec := volume.NewSpecFromPersistentVolume(pv, pv.Spec.PersistentVolumeSource.CSI.ReadOnly)
			attachment := makeTestAttachment(tc.attachID, nodeName, "test-pv")
			_, err := csiAttacher.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to attach: %v", err)
			}
			volumeName, err := plug.GetVolumeName(spec)
			if err != nil {
				t.Errorf("test case %s failed: %v", tc.name, err)
			}

			err = csiAttacher.Detach(volumeName, types.NodeName(nodeName))
			if tc.shouldFail && err == nil {
				t.Fatal("expecting failure, but err = nil")
			}
			if !tc.shouldFail && err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			attach, err := csiAttacher.k8s.StorageV1().VolumeAttachments().Get(context.TODO(), tc.attachID, metav1.GetOptions{})
			if err != nil {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("unexpected err: %v", err)
				}
			} else {
				if attach == nil {
					t.Errorf("expecting attachment not to be nil, but it is")
				}
			}
		})
	}
}

// TestAttacherGetDeviceMountPath 验证 GetDeviceMountPath 生成的路径，及缺失 CSI 源、卷句柄时的失败行为。
func TestAttacherGetDeviceMountPath(t *testing.T) {
	// 准备环境
	// 创建新的 attacher
	fakeClient := fakeclient.NewSimpleClientset()
	plug, tmpDir := newTestPlugin(t, fakeClient)
	defer os.RemoveAll(tmpDir)
	attacher, err0 := plug.NewAttacher()
	if err0 != nil {
		t.Fatalf("failed to create new attacher: %v", err0)
	}
	csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, testWatchTimeout)

	pluginDir := csiAttacher.plugin.host.GetPluginDir(plug.GetPluginName())
	testCases := []struct {
		testName           string
		pvName             string
		volumeId           string
		skipPVCSISource    bool // 该用例清空 PV.Spec.CSI
		shouldFail         bool
		addVolSource       bool // 该用例添加 Volume.VolumeSource.CSI
		removeVolumeHandle bool // 该用例强制清空 CSI 卷句柄
	}{
		{
			testName: "success test",
			pvName:   "test-pv1",
			volumeId: "test-vol1",
		},
		{
			testName:        "fail test, failed to create device mount path due to missing CSI source",
			pvName:          "test-pv1",
			volumeId:        "test-vol1",
			skipPVCSISource: true,
			shouldFail:      true,
		},
		{
			testName:     "fail test, failed to create device mount path, CSIVolumeSource found",
			pvName:       "test-pv1",
			volumeId:     "test-vol1",
			addVolSource: true,
			shouldFail:   true,
		},
		{
			testName:           "fail test, failed to create device mount path, missing CSI volume handle",
			pvName:             "test-pv1",
			volumeId:           "test-vol1",
			shouldFail:         true,
			removeVolumeHandle: true,
		},
	}

	for _, tc := range testCases {
		t.Logf("Running test case: %s", tc.testName)
		var spec *volume.Spec

		// 构造 spec，按用例需要移除 CSI 源或卷句柄
		pv := makeTestPV(tc.pvName, 10, testDriver, tc.volumeId)
		if tc.removeVolumeHandle {
			pv.Spec.PersistentVolumeSource.CSI.VolumeHandle = ""
		}
		if tc.addVolSource {
			spec = volume.NewSpecFromVolume(makeTestVol(tc.pvName, testDriver))
		} else {
			spec = volume.NewSpecFromPersistentVolume(pv, pv.Spec.PersistentVolumeSource.CSI.ReadOnly)
			if tc.skipPVCSISource {
				spec.PersistentVolume.Spec.CSI = nil
			}
		}
		// 执行被测方法
		mountPath, err := csiAttacher.GetDeviceMountPath(spec)

		// 校验结果
		if err != nil && !tc.shouldFail {
			t.Errorf("test should not fail, but error occurred: %v", err)
		} else if err == nil {
			expectedMountPath := filepath.Join(pluginDir, testDriver, generateSha(tc.volumeId), globalMountInGlobalPath)
			if tc.shouldFail {
				t.Errorf("test should fail, but no error occurred")
			} else if mountPath != expectedMountPath {
				t.Errorf("mountPath does not equal expectedMountPath. Got: %s. Expected: %s", mountPath, expectedMountPath)
			}
		}
	}
}

// TestAttacherMountDevice 验证 MountDevice 的 stage 行为、fsGroup 传递、错误处理及卷数据文件落盘。
func TestAttacherMountDevice(t *testing.T) {
	pvName := "test-pv"
	var testFSGroup int64 = 3000
	nonFinalError := volumetypes.NewUncertainProgressError("")
	transientError := volumetypes.NewTransientOperationFailure("")

	testCases := []struct {
		testName                       string
		volName                        string
		devicePath                     string
		deviceMountPath                string
		stageUnstageSet                bool
		fsGroup                        *int64
		expectedVolumeMountGroup       string
		driverSupportsVolumeMountGroup bool
		shouldFail                     bool
		skipOnWindows                  bool
		createAttachment               bool
		populateDeviceMountPath        bool
		exitError                      error
		spec                           *volume.Spec
		watchTimeout                   time.Duration
		skipClientSetup                bool
	}{
		{
			testName:         "normal PV",
			volName:          "test-vol1",
			devicePath:       "path1",
			deviceMountPath:  "path2",
			stageUnstageSet:  true,
			createAttachment: true,
			spec:             volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:         "normal PV with mount options",
			volName:          "test-vol1",
			devicePath:       "path1",
			deviceMountPath:  "path2",
			stageUnstageSet:  true,
			createAttachment: true,
			spec:             volume.NewSpecFromPersistentVolume(makeTestPVWithMountOptions(pvName, 10, testDriver, "test-vol1", []string{"test-op"}), false),
		},
		{
			testName:         "normal PV but with missing attachment should result in no-change",
			volName:          "test-vol1",
			devicePath:       "path1",
			deviceMountPath:  "path2",
			stageUnstageSet:  true,
			createAttachment: false,
			shouldFail:       true,
			exitError:        transientError,
			spec:             volume.NewSpecFromPersistentVolume(makeTestPVWithMountOptions(pvName, 10, testDriver, "test-vol1", []string{"test-op"}), false),
		},
		{
			testName:         "no vol name",
			volName:          "",
			devicePath:       "path1",
			deviceMountPath:  "path2",
			stageUnstageSet:  true,
			shouldFail:       true,
			createAttachment: true,
			spec:             volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, ""), false),
		},
		{
			testName:         "no device path",
			volName:          "test-vol1",
			devicePath:       "",
			deviceMountPath:  "path2",
			stageUnstageSet:  true,
			shouldFail:       false,
			createAttachment: true,
			spec:             volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:         "no device mount path",
			volName:          "test-vol1",
			devicePath:       "path1",
			deviceMountPath:  "",
			stageUnstageSet:  true,
			shouldFail:       true,
			createAttachment: true,
			spec:             volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:         "stage_unstage cap not set",
			volName:          "test-vol1",
			devicePath:       "path1",
			deviceMountPath:  "path2",
			stageUnstageSet:  false,
			createAttachment: true,
			spec:             volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:         "failure with volume source",
			volName:          "test-vol1",
			devicePath:       "path1",
			deviceMountPath:  "path2",
			shouldFail:       true,
			createAttachment: true,
			spec:             volume.NewSpecFromVolume(makeTestVol(pvName, testDriver)),
		},
		{
			testName:         "pv with nodestage timeout should result in in-progress device",
			volName:          fakecsi.NodeStageTimeOut_VolumeID,
			devicePath:       "path1",
			deviceMountPath:  "path2",
			stageUnstageSet:  true,
			createAttachment: true,
			spec:             volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, fakecsi.NodeStageTimeOut_VolumeID), false),
			exitError:        nonFinalError,
			shouldFail:       true,
		},
		{
			testName:                "failure PV with existing data",
			volName:                 "test-vol1",
			devicePath:              "path1",
			deviceMountPath:         "path2",
			stageUnstageSet:         true,
			createAttachment:        true,
			populateDeviceMountPath: true,
			shouldFail:              true,
			// 注意：Windows 上 os.Chmod 行为不符合预期，此用例在 Windows 上不会因权限不足而失败，故跳过。
			// TODO: 待 Windows 文件权限支持完善后移除跳过。
			// https://github.com/kubernetes/kubernetes/pull/110921
			skipOnWindows: true,
			spec:          volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), true),
		},
		{
			testName:                       "fsgroup provided, driver supports volume mount group; expect fsgroup to be passed to NodeStageVolume",
			volName:                        "test-vol1",
			devicePath:                     "path1",
			deviceMountPath:                "path2",
			fsGroup:                        &testFSGroup,
			driverSupportsVolumeMountGroup: true,
			expectedVolumeMountGroup:       "3000",
			stageUnstageSet:                true,
			createAttachment:               true,
			spec:                           volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:                       "fsgroup not provided, driver supports volume mount group; expect fsgroup not to be passed to NodeStageVolume",
			volName:                        "test-vol1",
			devicePath:                     "path1",
			deviceMountPath:                "path2",
			driverSupportsVolumeMountGroup: true,
			expectedVolumeMountGroup:       "",
			stageUnstageSet:                true,
			createAttachment:               true,
			spec:                           volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:                       "fsgroup provided, driver does not support volume mount group; expect fsgroup not to be passed to NodeStageVolume",
			volName:                        "test-vol1",
			devicePath:                     "path1",
			deviceMountPath:                "path2",
			fsGroup:                        &testFSGroup,
			driverSupportsVolumeMountGroup: false,
			expectedVolumeMountGroup:       "",
			stageUnstageSet:                true,
			createAttachment:               true,
			spec:                           volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:                "driver not specified",
			volName:                 "test-vol1",
			devicePath:              "path1",
			deviceMountPath:         "path2",
			fsGroup:                 &testFSGroup,
			stageUnstageSet:         true,
			createAttachment:        true,
			populateDeviceMountPath: false,
			spec:                    volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, "not-found", "test-vol1"), false),
			exitError:               transientError,
			shouldFail:              true,
			skipClientSetup:         true,
		},
	}

	for _, tc := range testCases {
		user, err := user.Current()
		if err != nil {
			t.Logf("Current user could not be determined, assuming non-root: %v", err)
		} else {
			if tc.populateDeviceMountPath && user.Uid == "0" {
				t.Skipf("Skipping intentional failure on existing data when running as root.")
			}
		}
		t.Run(tc.testName, func(t *testing.T) {
			if tc.skipOnWindows && goruntime.GOOS == "windows" {
				t.Skipf("Skipping test case on Windows: %s", tc.testName)
			}
			t.Logf("Running test case: %s", tc.testName)

			// 准备环境
			// 创建新的 attacher
			fakeClient := fakeclient.NewSimpleClientset()
			plug, tmpDir := newTestPlugin(t, fakeClient)
			defer os.RemoveAll(tmpDir)

			attacher, err0 := plug.NewAttacher()
			if err0 != nil {
				t.Fatalf("failed to create new attacher: %v", err0)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, tc.watchTimeout)
			if !tc.skipClientSetup {
				csiAttacher.csiClient = setupClientWithVolumeMountGroup(t, tc.stageUnstageSet, tc.driverSupportsVolumeMountGroup)
			}

			if tc.deviceMountPath != "" {
				tc.deviceMountPath = filepath.Join(tmpDir, tc.deviceMountPath)
			}

			nodeName := string(csiAttacher.plugin.host.GetNodeName())
			attachID := getAttachmentName(tc.volName, testDriver, nodeName)

			if tc.createAttachment {
				// 准备卷附件
				attachment := makeTestAttachment(attachID, nodeName, pvName)
				_, err := csiAttacher.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to attach: %v", err)
				}
			}

			parent := filepath.Dir(tc.deviceMountPath)
			filePath := filepath.Join(parent, "newfile")
			if tc.populateDeviceMountPath {
				// 必须先创建 deviceMountPath，Mount 时才能在其中成功创建测试文件。
				err := os.MkdirAll(tc.deviceMountPath, 0750)
				if err != nil {
					t.Errorf("error attempting to create the directory")
				}
				_, err = os.Create(filePath)
				if err != nil {
					t.Errorf("error attempting to populate file on parent path: %v", err)
				}
				err = os.Chmod(parent, 0555)
				if err != nil {
					t.Errorf("error attempting to modify directory permissions: %v", err)
				}
			}

			// 执行被测方法
			err := csiAttacher.MountDevice(
				tc.spec,
				tc.devicePath,
				tc.deviceMountPath,
				volume.DeviceMounterArgs{FsGroup: tc.fsGroup})

			// 校验结果
			if err != nil {
				if !tc.shouldFail {
					t.Errorf("test should not fail, but error occurred: %v", err)
				}
				if tc.populateDeviceMountPath {
					// 预期 saveVolumeData 因权限不足失败，它负责创建 vol_data.json，故该文件不应存在。
					_, err := os.Stat(filepath.Join(parent, volDataFileName))
					if !os.IsNotExist(err) {
						t.Errorf("vol_data.json should not exist: %v", err)
					}
					_, err = os.Stat(filePath)
					if os.IsNotExist(err) {
						t.Errorf("expecting file to exist after err received: %v", err)
					}
					err = os.Chmod(parent, 0777)
					if err != nil {
						t.Errorf("failed to modify permissions after test: %v", err)
					}
				}
				if tc.exitError != nil && reflect.TypeOf(tc.exitError) != reflect.TypeOf(err) {
					t.Fatalf("expected exitError type: %v got: %v (%v)", reflect.TypeOf(tc.exitError), reflect.TypeOf(err), err)
				}
				return
			}
			if tc.shouldFail {
				t.Errorf("test should fail, but no error occurred")
			}

			// 验证调用确实走到 CSI 驱动
			numStaged := 1
			if !tc.stageUnstageSet {
				numStaged = 0
			}

			cdc := csiAttacher.csiClient.(*fakeCsiDriverClient)
			staged := cdc.nodeClient.GetNodeStagedVolumes()
			if len(staged) != numStaged {
				t.Errorf("got wrong number of staged volumes, expecting %v got: %v", numStaged, len(staged))
			}
			if tc.stageUnstageSet {
				vol, ok := staged[tc.volName]
				if !ok {
					t.Errorf("could not find staged volume: %s", tc.volName)
				}
				if vol.Path != tc.deviceMountPath {
					t.Errorf("expected mount path: %s. got: %s", tc.deviceMountPath, vol.Path)
				}
				if !reflect.DeepEqual(vol.MountFlags, tc.spec.PersistentVolume.Spec.MountOptions) {
					t.Errorf("expected mount options: %v, got: %v", tc.spec.PersistentVolume.Spec.MountOptions, vol.MountFlags)
				}
				if vol.VolumeMountGroup != tc.expectedVolumeMountGroup {
					t.Errorf("expected volume mount group %q, got: %q", tc.expectedVolumeMountGroup, vol.VolumeMountGroup)
				}
			}

			// 验证插件创建了 deviceMountPath 目录
			if tc.stageUnstageSet {
				s, err := os.Stat(tc.deviceMountPath)
				if err != nil {
					t.Errorf("expected staging directory %s to be created and be a directory, got error: %s", tc.deviceMountPath, err)
				} else {
					if !s.IsDir() {
						t.Errorf("expected staging directory %s to be directory, got something else", tc.deviceMountPath)
					}
				}
			}
		})
	}
}

// TestAttacherMountDeviceWithInline 验证内联卷源场景下 MountDevice 失败，PV 场景正常 stage 并传递 fsGroup。
func TestAttacherMountDeviceWithInline(t *testing.T) {
	pvName := "test-pv"
	var testFSGroup int64 = 3000
	testCases := []struct {
		testName                 string
		volName                  string
		devicePath               string
		deviceMountPath          string
		fsGroup                  *int64
		expectedVolumeMountGroup string
		stageUnstageSet          bool
		shouldFail               bool
		spec                     *volume.Spec
		watchTimeout             time.Duration
	}{
		{
			testName:        "normal PV",
			volName:         "test-vol1",
			devicePath:      "path1",
			deviceMountPath: "path2",
			stageUnstageSet: true,
			spec:            volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:        "failure with volSrc",
			volName:         "test-vol1",
			devicePath:      "path1",
			deviceMountPath: "path2",
			shouldFail:      true,
			spec:            volume.NewSpecFromVolume(makeTestVol(pvName, testDriver)),
		},
		{
			testName:        "no vol name",
			volName:         "",
			devicePath:      "path1",
			deviceMountPath: "path2",
			stageUnstageSet: true,
			shouldFail:      true,
			spec:            volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, ""), false),
		},
		{
			testName:        "no device path",
			volName:         "test-vol1",
			devicePath:      "",
			deviceMountPath: "path2",
			stageUnstageSet: true,
			shouldFail:      false,
			spec:            volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:        "no device mount path",
			volName:         "test-vol1",
			devicePath:      "path1",
			deviceMountPath: "",
			stageUnstageSet: true,
			shouldFail:      true,
			spec:            volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:        "stage_unstage cap not set",
			volName:         "test-vol1",
			devicePath:      "path1",
			deviceMountPath: "path2",
			stageUnstageSet: false,
			spec:            volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
		{
			testName:        "missing spec",
			volName:         "test-vol1",
			devicePath:      "path1",
			deviceMountPath: "path2",
			shouldFail:      true,
		},
		{
			testName:                 "fsgroup set",
			volName:                  "test-vol1",
			devicePath:               "path1",
			deviceMountPath:          "path2",
			fsGroup:                  &testFSGroup,
			expectedVolumeMountGroup: "3000",
			stageUnstageSet:          true,
			spec:                     volume.NewSpecFromPersistentVolume(makeTestPV(pvName, 10, testDriver, "test-vol1"), false),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			t.Logf("Running test case: %s", tc.testName)

			// 准备环境
			// 创建新的 attacher
			fakeClient := fakeclient.NewSimpleClientset()
			plug, tmpDir := newTestPlugin(t, fakeClient)
			defer os.RemoveAll(tmpDir)

			fakeWatcher := watch.NewRaceFreeFake()
			fakeClient.Fake.PrependWatchReactor("volumeattachments", core.DefaultWatchReactor(fakeWatcher, nil))

			attacher, err0 := plug.NewAttacher()
			if err0 != nil {
				t.Fatalf("failed to create new attacher: %v", err0)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, tc.watchTimeout)
			csiAttacher.csiClient = setupClientWithVolumeMountGroup(t, tc.stageUnstageSet, true /* volumeMountGroup */)

			if tc.deviceMountPath != "" {
				tc.deviceMountPath = filepath.Join(tmpDir, tc.deviceMountPath)
			}

			nodeName := string(csiAttacher.plugin.host.GetNodeName())
			attachID := getAttachmentName(tc.volName, testDriver, nodeName)

			// 准备卷附件
			attachment := makeTestAttachment(attachID, nodeName, pvName)
			_, err := csiAttacher.k8s.StorageV1().VolumeAttachments().Create(context.TODO(), attachment, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to attach: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				defer wg.Done()
				fakeWatcher.Delete(attachment)
			}()

			// 执行被测方法
			err = csiAttacher.MountDevice(
				tc.spec,
				tc.devicePath,
				tc.deviceMountPath,
				volume.DeviceMounterArgs{FsGroup: tc.fsGroup})

			// 校验结果
			if err != nil {
				if !tc.shouldFail {
					t.Errorf("test should not fail, but error occurred: %v", err)
				}
				return
			}
			if tc.shouldFail {
				t.Errorf("test should fail, but no error occurred")
			}

			// 验证调用确实走到 CSI 驱动
			numStaged := 1
			if !tc.stageUnstageSet {
				numStaged = 0
			}

			cdc := csiAttacher.csiClient.(*fakeCsiDriverClient)
			staged := cdc.nodeClient.GetNodeStagedVolumes()
			if len(staged) != numStaged {
				t.Errorf("got wrong number of staged volumes, expecting %v got: %v", numStaged, len(staged))
			}
			if tc.stageUnstageSet {
				vol, ok := staged[tc.volName]
				if !ok {
					t.Errorf("could not find staged volume: %s", tc.volName)
				}
				if vol.Path != tc.deviceMountPath {
					t.Errorf("expected mount path: %s. got: %s", tc.deviceMountPath, vol.Path)
				}
				if vol.VolumeMountGroup != tc.expectedVolumeMountGroup {
					t.Errorf("expected volume mount group %q, got: %q", tc.expectedVolumeMountGroup, vol.VolumeMountGroup)
				}
			}

			wg.Wait()
		})
	}
}

// TestAttacherUnmountDevice 验证 UnmountDevice 依据卷数据文件清理 stage 目录，以及客户端缺失时返回瞬时错误。
func TestAttacherUnmountDevice(t *testing.T) {
	transientError := volumetypes.NewTransientOperationFailure("")
	testCases := []struct {
		testName        string
		volID           string
		deviceMountPath string
		jsonFile        string
		createPV        bool
		stageUnstageSet bool
		shouldFail      bool
		watchTimeout    time.Duration
		exitError       error
		unsetClient     bool
	}{
		// 与 PV 无关路径的正向用例
		{
			testName:        "success, json file exists",
			volID:           "project/zone/test-vol1",
			deviceMountPath: "plugins/csi/" + generateSha("project/zone/test-vol1") + "/globalmount",
			jsonFile:        `{"driverName": "csi", "volumeHandle":"project/zone/test-vol1"}`,
			stageUnstageSet: true,
		},
		{
			testName:        "stage_unstage not set, PV agnostic path, unmount device is skipped",
			deviceMountPath: "plugins/csi/" + generateSha("project/zone/test-vol1") + "/globalmount",
			jsonFile:        `{"driverName":"test-driver","volumeHandle":"test-vol1"}`,
			stageUnstageSet: false,
		},
		// 与 PV 无关路径的负向用例
		{
			testName:        "success: json file doesn't exist, unmount device is skipped",
			deviceMountPath: "plugins/csi/" + generateSha("project/zone/test-vol1") + "/globalmount",
			jsonFile:        "",
			stageUnstageSet: true,
			createPV:        true,
		},
		{
			testName:        "fail: invalid json, fail to retrieve driver and volumeID from globalpath",
			volID:           "project/zone/test-vol1",
			deviceMountPath: "plugins/csi/" + generateSha("project/zone/test-vol1") + "/globalmount",
			jsonFile:        `{"driverName"}}`,
			stageUnstageSet: true,
			shouldFail:      true,
		},
		// 确保客户端未建立时返回瞬时错误
		{
			testName:        "fail with transient error, json file exists but client not found",
			volID:           "project/zone/test-vol1",
			deviceMountPath: "plugins/csi/" + generateSha("project/zone/test-vol1") + "/globalmount",
			jsonFile:        `{"driverName": "unknown-driver", "volumeHandle":"project/zone/test-vol1"}`,
			stageUnstageSet: true,
			shouldFail:      true,
			exitError:       transientError,
			unsetClient:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			t.Logf("Running test case: %s", tc.testName)
			// 准备环境
			// 创建新的 attacher
			fakeClient := fakeclient.NewSimpleClientset()
			plug, tmpDir := newTestPlugin(t, fakeClient)
			defer os.RemoveAll(tmpDir)
			attacher, err0 := plug.NewAttacher()
			if err0 != nil {
				t.Fatalf("failed to create new attacher: %v", err0)
			}
			csiAttacher := getCsiAttacherFromVolumeAttacher(attacher, tc.watchTimeout)
			csiAttacher.csiClient = setupClient(t, tc.stageUnstageSet)

			if tc.deviceMountPath != "" {
				tc.deviceMountPath = filepath.Join(tmpDir, tc.deviceMountPath)
			}
			// 将卷加入 NodeStagedVolumes，使 UnmountDevice 能找到它
			cdc := csiAttacher.csiClient.(*fakeCsiDriverClient)
			cdc.nodeClient.AddNodeStagedVolume(tc.volID, tc.deviceMountPath, nil)

			// 创建 stage 目录
			if tc.deviceMountPath != "" {
				if err := os.MkdirAll(tc.deviceMountPath, 0755); err != nil {
					t.Fatalf("error creating directory %s: %s", tc.deviceMountPath, err)
				}
			}
			dir := filepath.Dir(tc.deviceMountPath)
			// 写入卷数据 JSON 文件
			if tc.jsonFile != "" {
				dataPath := filepath.Join(dir, volDataFileName)
				if err := os.WriteFile(dataPath, []byte(tc.jsonFile), 0644); err != nil {
					t.Fatalf("error creating %s: %s", dataPath, err)
				}
			}
			if tc.createPV {
				// 为该对象创建 PV
				pvName := filepath.Base(dir)
				pv := makeTestPV(pvName, 5, "csi", tc.volID)
				_, err := csiAttacher.k8s.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
				if err != nil && !tc.shouldFail {
					t.Fatalf("Failed to create PV: %v", err)
				}
			}
			// 按需清空客户端，使后续查找新客户端失败
			if tc.unsetClient {
				csiAttacher.csiClient = nil
			}

			// 执行被测方法
			err := csiAttacher.UnmountDevice(tc.deviceMountPath)
			// 校验结果
			if err != nil {
				if !tc.shouldFail {
					t.Errorf("test should not fail, but error occurred: %v", err)
				}
				if tc.exitError != nil && reflect.TypeOf(tc.exitError) != reflect.TypeOf(err) {
					t.Fatalf("expected exitError type: %v got: %v (%v)", reflect.TypeOf(tc.exitError), reflect.TypeOf(err), err)
				}
				return
			}
			if tc.shouldFail {
				t.Errorf("test should fail, but no error occurred")
			}

			// 验证调用确实走到 CSI 驱动
			expectedSet := 0
			if !tc.stageUnstageSet || tc.volID == "" {
				expectedSet = 1
			}
			staged := cdc.nodeClient.GetNodeStagedVolumes()
			if len(staged) != expectedSet {
				t.Errorf("got wrong number of staged volumes, expecting %v got: %v", expectedSet, len(staged))
			}

			_, ok := staged[tc.volID]
			if ok && tc.stageUnstageSet && tc.volID != "" {
				t.Errorf("found unexpected staged volume: %s", tc.volID)
			} else if !ok && !tc.stageUnstageSet {
				t.Errorf("could not find expected staged volume: %s", tc.volID)
			}

			if tc.jsonFile != "" && !tc.shouldFail {
				dataPath := filepath.Join(dir, volDataFileName)
				if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
					if err != nil {
						t.Errorf("error checking file %s: %s", dataPath, err)
					} else {
						t.Errorf("json file %s should not exists, but it does", dataPath)
					}
				} else {
					t.Logf("json file %s was correctly removed", dataPath)
				}
			}
		})
	}
}

// getCsiAttacherFromVolumeAttacher 将 Attach 接口断言为 csiAttacher 并设置测试用等待超时。
// watchTimeout 为 0 时使用默认值。
func getCsiAttacherFromVolumeAttacher(attacher volume.Attacher, watchTimeout time.Duration) *csiAttacher {
	if watchTimeout == 0 {
		watchTimeout = testWatchTimeout
	}
	csiAttacher := attacher.(*csiAttacher)
	csiAttacher.watchTimeout = watchTimeout
	return csiAttacher
}

// getCsiAttacherFromVolumeDetacher 将 Detach 接口断言为 csiAttacher 并设置测试用等待超时。
// watchTimeout 为 0 时使用默认值。
func getCsiAttacherFromVolumeDetacher(detacher volume.Detacher, watchTimeout time.Duration) *csiAttacher {
	if watchTimeout == 0 {
		watchTimeout = testWatchTimeout
	}
	csiAttacher := detacher.(*csiAttacher)
	csiAttacher.watchTimeout = watchTimeout
	return csiAttacher
}

// getCsiAttacherFromDeviceMounter 将设备挂载接口断言为 csiAttacher 并设置测试用等待超时。
// watchTimeout 为 0 时使用默认值。
func getCsiAttacherFromDeviceMounter(deviceMounter volume.DeviceMounter, watchTimeout time.Duration) *csiAttacher {
	if watchTimeout == 0 {
		watchTimeout = testWatchTimeout
	}
	csiAttacher := deviceMounter.(*csiAttacher)
	csiAttacher.watchTimeout = watchTimeout
	return csiAttacher
}

// getCsiAttacherFromDeviceUnmounter 将设备卸载接口断言为 csiAttacher 并设置测试用等待超时。
// watchTimeout 为 0 时使用默认值。
func getCsiAttacherFromDeviceUnmounter(deviceUnmounter volume.DeviceUnmounter, watchTimeout time.Duration) *csiAttacher {
	if watchTimeout == 0 {
		watchTimeout = testWatchTimeout
	}
	csiAttacher := deviceUnmounter.(*csiAttacher)
	csiAttacher.watchTimeout = watchTimeout
	return csiAttacher
}

// generateSha 计算卷句柄的 SHA256，与生产代码生成挂载路径的方式保持一致。
func generateSha(handle string) string {
	result := sha256.Sum256([]byte(fmt.Sprintf("%s", handle)))
	return fmt.Sprintf("%x", result)
}
