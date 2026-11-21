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

package util

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2/ktesting"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	"k8s.io/kubernetes/pkg/volume/fc"

	"k8s.io/client-go/kubernetes/fake"
	utiltesting "k8s.io/client-go/util/testing"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
)

const (
	testHostName      = "test-hostname"
	socketPath        = "/var/run/kmsplugin"
	migratedVolume    = "migrated-volume-name"
	nonMigratedVolume = "non-migrated-volume-name"
	testNodeName      = "test-node-name"
)

var (
	dirOrCreate = v1.HostPathType(v1.HostPathDirectoryOrCreate)
	nodeName    = kubetypes.NodeName(testNodeName)
	hostPath    = &v1.HostPathVolumeSource{
		Path: socketPath,
		Type: &dirOrCreate,
	}
	migratedObjectReference    = v1.ObjectReference{Namespace: "default", Name: "migrated-pvc"}
	nonMigratedObjectReference = v1.ObjectReference{Namespace: "default", Name: "non-migrated-pvc"}
	fsVolumeMode               = new(v1.PersistentVolumeMode)
)

// vaTest 描述 CreateVolumeSpecWithNodeMigration 的单个测试场景：
// 给定 Pod 与节点名，声明期望得到的 Volume、PersistentVolume 或错误信息。
type vaTest struct {
	desc                 string
	createNodeName       kubetypes.NodeName
	pod                  *v1.Pod
	wantVolume           *v1.Volume
	wantPersistentVolume *v1.PersistentVolume
	wantErrorMessage     string
}

// Test_CreateVolumeSpec 验证 CSI 迁移场景下卷规格的生成逻辑：
// 可迁移的内联卷或 PVC 应转换为 CSI PersistentVolume，不可迁移的卷保持原样，
// 当节点上没有对应 CSINode 时则跳过转换。
func Test_CreateVolumeSpec(t *testing.T) {
	for _, test := range []vaTest{
		{
			desc:           "inline volume type that does not support csi migration",
			createNodeName: nodeName,
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kube-apiserver",
					Namespace: "default",
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: migratedVolume,
							VolumeSource: v1.VolumeSource{
								HostPath: hostPath,
							},
						},
					},
				},
			},
			wantVolume: &v1.Volume{
				Name: migratedVolume,
				VolumeSource: v1.VolumeSource{
					HostPath: hostPath,
				},
			},
		},
		{
			desc:           "inline volume type that supports csi migration",
			createNodeName: nodeName,
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kube-apiserver",
					Namespace: "default",
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: migratedVolume,
							VolumeSource: v1.VolumeSource{
								GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
									PDName:    "test-disk",
									FSType:    "ext4",
									Partition: 0,
									ReadOnly:  false,
								},
							},
						},
					},
				},
			},
			wantPersistentVolume: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pd.csi.storage.gke.io-test-disk",
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:           "pd.csi.storage.gke.io",
							VolumeHandle:     "projects/UNSPECIFIED/zones/UNSPECIFIED/disks/test-disk",
							FSType:           "ext4",
							ReadOnly:         false,
							VolumeAttributes: map[string]string{"partition": ""},
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{"ReadWriteOnce"},
					VolumeMode:  fsVolumeMode,
				},
			},
		},
		{
			desc:           "pv type that does not support csi migration",
			createNodeName: nodeName,
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kube-apiserver",
					Namespace: "default",
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: nonMigratedVolume,
							VolumeSource: v1.VolumeSource{
								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
									ClaimName: "non-migrated-pvc",
									ReadOnly:  false,
								},
							},
						},
					},
				},
			},
			wantPersistentVolume: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: nonMigratedVolume,
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						ScaleIO: &v1.ScaleIOPersistentVolumeSource{},
					},
					ClaimRef: &nonMigratedObjectReference,
				},
			},
		},
		{
			desc:           "pv type that supports csi migration",
			createNodeName: nodeName,
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kube-apiserver",
					Namespace: "default",
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: migratedVolume,
							VolumeSource: v1.VolumeSource{
								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
									ClaimName: "migrated-pvc",
									ReadOnly:  false,
								},
							},
						},
					},
				},
			},
			wantPersistentVolume: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: migratedVolume,
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:           "pd.csi.storage.gke.io",
							VolumeHandle:     "projects/UNSPECIFIED/zones/UNSPECIFIED/disks/test-disk",
							FSType:           "ext4",
							ReadOnly:         false,
							VolumeAttributes: map[string]string{"partition": ""},
						},
					},
					ClaimRef: &migratedObjectReference,
				},
			},
		},
		{
			desc:           "CSINode not found for a volume type that completes csi migration",
			createNodeName: kubetypes.NodeName("another-node"),
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kube-apiserver",
					Namespace: "default",
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: migratedVolume,
							VolumeSource: v1.VolumeSource{
								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
									ClaimName: "migrated-pvc",
									ReadOnly:  false,
								},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			plugMgr, intreeToCSITranslator, csiTranslator, pvLister, pvcLister := setup(testNodeName, t)
			actualSpec, err := CreateVolumeSpecWithNodeMigration(logger, test.pod.Spec.Volumes[0], test.pod, test.createNodeName, plugMgr, pvcLister, pvLister, intreeToCSITranslator, csiTranslator)

			// 期望生成卷规格时，实际结果不应为空。
			if actualSpec == nil && (test.wantPersistentVolume != nil || test.wantVolume != nil) {
				t.Errorf("got volume spec is nil")
			}

			// 错误信息按子串匹配，避免测试与错误文案的无关改动耦合。
			if (len(test.wantErrorMessage) > 0 && err == nil) || (err != nil && !strings.Contains(err.Error(), test.wantErrorMessage)) {
				t.Errorf("got err %v, want err with message %v", err, test.wantErrorMessage)
			}

			// 期望迁移结果时，逐字段比对 PV 名称与 Spec。
			if test.wantPersistentVolume != nil {
				if actualSpec.PersistentVolume == nil {
					t.Errorf("gotVolumeWithCSIMigration is nil")
				}

				gotVolumeWithCSIMigration := *actualSpec.PersistentVolume
				if gotVolumeWithCSIMigration.Name != test.wantPersistentVolume.Name {
					t.Errorf("got volume name is %v, want volume name is %v", gotVolumeWithCSIMigration.Name, test.wantPersistentVolume.Name)

				}
				if !reflect.DeepEqual(gotVolumeWithCSIMigration.Spec, test.wantPersistentVolume.Spec) {
					t.Errorf("got volume.Spec and want.Spec diff is %s", cmp.Diff(gotVolumeWithCSIMigration.Spec, test.wantPersistentVolume.Spec))
				}
			}
			// 不可迁移的卷应保持原始 VolumeSource 不变。
			if test.wantVolume != nil {
				if actualSpec.Volume == nil {
					t.Errorf("gotVolume is nil")
				}

				gotVolume := *actualSpec.Volume
				if !reflect.DeepEqual(gotVolume, *test.wantVolume) {
					t.Errorf("got volume and want diff is %s", cmp.Diff(gotVolume, test.wantVolume))
				}
			}
		})
	}
}

// setup 构造测试所需的插件管理器、CSI 转换器，
// 以及包含可迁移与不可迁移 PV、已绑定 PVC 的 lister。
func setup(nodeName string, t *testing.T) (*volume.VolumePluginMgr, csimigration.PluginManager, csitrans.CSITranslator, tf.PersistentVolumeLister, tf.PersistentVolumeClaimLister) {
	tmpDir, err := utiltesting.MkTmpdir("csi-test")
	if err != nil {
		t.Fatalf("can't make a temp dir: %v", err)
	}
	// 临时目录仅用于模拟插件宿主，测试结束立即清理。
	defer os.RemoveAll(tmpDir)

	// fsVolumeMode 是包级变量，被各测试用例共享，这里统一初始化。
	*fsVolumeMode = v1.PersistentVolumeFilesystem

	csiTranslator := csitrans.New()
	intreeToCSITranslator := csimigration.NewPluginManager(csiTranslator)
	kubeClient := fake.NewSimpleClientset()

	factory := informers.NewSharedInformerFactory(kubeClient, time.Minute)
	csiDriverInformer := factory.Storage().V1().CSIDrivers()
	csiDriverLister := csiDriverInformer.Lister()
	volumeAttachmentInformer := factory.Storage().V1().VolumeAttachments()
	volumeAttachmentLister := volumeAttachmentInformer.Lister()

	plugMgr := &volume.VolumePluginMgr{}
	fakeAttachDetachVolumeHost := volumetest.NewFakeAttachDetachVolumeHostWithCSINodeName(t,
		tmpDir,
		kubeClient,
		fc.ProbeVolumePlugins(),
		nodeName,
		csiDriverLister,
		volumeAttachmentLister,
	)

	plugMgr.Host = fakeAttachDetachVolumeHost

	pvLister := tf.PersistentVolumeLister{
		{
			ObjectMeta: metav1.ObjectMeta{Name: migratedVolume},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
						PDName:    "test-disk",
						FSType:    "ext4",
						Partition: 0,
						ReadOnly:  false,
					},
				},
				ClaimRef: &migratedObjectReference,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: nonMigratedVolume},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeSource: v1.PersistentVolumeSource{
					ScaleIO: &v1.ScaleIOPersistentVolumeSource{},
				},
				ClaimRef: &nonMigratedObjectReference,
			},
		},
	}

	pvcLister := tf.PersistentVolumeClaimLister{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "migrated-pvc", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: migratedVolume},
			Status: v1.PersistentVolumeClaimStatus{
				Phase: v1.ClaimBound,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "non-migrated-pvc", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: nonMigratedVolume},
			Status: v1.PersistentVolumeClaimStatus{
				Phase: v1.ClaimBound,
			},
		},
	}

	return plugMgr, intreeToCSITranslator, csiTranslator, pvLister, pvcLister
}
