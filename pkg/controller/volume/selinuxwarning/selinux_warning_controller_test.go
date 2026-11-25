/*
Copyright 2024 The Kubernetes Authors.

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

package selinuxwarning

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	volumecache "k8s.io/kubernetes/pkg/controller/volume/selinuxwarning/cache"
	"k8s.io/kubernetes/pkg/volume"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/utils/ptr"
)

// 测试用的固定命名空间和 PVC UID，方便在各测试对象间保持一致。
const (
	namespace = "ns1"
	pvcUID    = "uid1"
)

// TestSELinuxWarningController_Sync 验证控制器对 Pod 的同步逻辑：
// 覆盖卷注册、SELinux 标签与变更策略冲突检测、事件上报以及失效 Pod 的清理。
func TestSELinuxWarningController_Sync(t *testing.T) {
	tests := []struct {
		// name 为测试用例名称。
		name string
		// existingPVCs 为预置到 informer 缓存中的 PVC。
		existingPVCs []*v1.PersistentVolumeClaim
		// existingPVs 为预置到 informer 缓存中的 PV。
		existingPVs []*v1.PersistentVolume
		// existingCSIDrivers 为预置的 CSI 驱动对象。
		existingCSIDrivers []*storagev1.CSIDriver
		// existingPods 为预置到 informer 缓存中的 Pod。
		existingPods []*v1.Pod

		// pod 为本次 sync 操作的目标 Pod 键。
		pod cache.ObjectName
		// csiDriverSELinuxEnabled 控制测试插件是否宣称支持 SELinux 挂载。
		csiDriverSELinuxEnabled bool
		// conflicts 为假缓存为该 Pod 准备的冲突列表。
		conflicts []volumecache.Conflict
		// expectError 表示期望 sync 返回错误。
		expectError bool
		// expectedAddedVolumes 为期望写入卷缓存的卷信息。
		expectedAddedVolumes []addedVolume
		// expectedEvents 为期望记录的事件。
		expectedEvents []string
		// expectedDeletedPods 为期望从卷缓存中删除的 Pod 键。
		expectedDeletedPods []cache.ObjectName
	}{
		{
			name: "existing pod with no volumes",
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectedEvents:          nil,
			expectedAddedVolumes:    nil,
		},
		{
			name: "existing pod with unbound PVC",
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("non-existing-pvc", "vol1").build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectError:             true, // PVC 缺失，sync 应报错以便重新入队并按指数退避重试
			expectedEvents:          nil,
			expectedAddedVolumes:    nil,
		},
		{
			name: "existing pod with fully bound PVC",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("pvc1", "vol1").build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectedEvents:          nil,
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        ":::s0:c1,c2",
					changePolicy: v1.SELinuxChangePolicyMountOption,
					csiDriver:    "ebs.csi.aws.com", // 该 PV 是伪造的 EBS 卷
				},
			},
		},
		{
			name: "existing pod with fully bound PVC, Recursive change policy",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", ptr.To(v1.SELinuxChangePolicyRecursive)).withPVC("pvc1", "vol1").build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectedEvents:          nil,
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        "", // Recursive 策略下标签会被清空
					changePolicy: v1.SELinuxChangePolicyRecursive,
					csiDriver:    "ebs.csi.aws.com", // 该 PV 是伪造的 EBS 卷
				},
			},
		},
		{
			name: "existing pod with inline volume",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withInlineVolume().build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectedEvents:          nil,
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/ebs.csi.aws.com-inlinevol1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        ":::s0:c1,c2",
					changePolicy: v1.SELinuxChangePolicyMountOption,
					csiDriver:    "ebs.csi.aws.com", // 该内联卷是 AWS EBS 卷
				},
			},
		},
		{
			name: "existing pod with inline volume and PVC",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("pvc1", "vol1").withInlineVolume().build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectedEvents:          nil,
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        ":::s0:c1,c2",
					changePolicy: v1.SELinuxChangePolicyMountOption,
					csiDriver:    "ebs.csi.aws.com", // 该 PV 是伪造的 EBS 卷
				},
				{
					volumeName:   "fake-plugin/ebs.csi.aws.com-inlinevol1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        ":::s0:c1,c2",
					changePolicy: v1.SELinuxChangePolicyMountOption,
					csiDriver:    "ebs.csi.aws.com", // 该内联卷是 AWS EBS 卷
				},
			},
		},
		{
			name: "existing pod with PVC generates conflict, the other pod exists",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("pvc1", "vol1").build(),
				pod("pod2", "s0:c98,c99", nil).build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			conflicts: []volumecache.Conflict{
				{
					PropertyName:       "SELinuxLabel",
					EventReason:        "SELinuxLabelConflict",
					Pod:                cache.ObjectName{Namespace: namespace, Name: "pod1"},
					PropertyValue:      ":::s0:c1,c2",
					OtherPod:           cache.ObjectName{Namespace: namespace, Name: "pod2"},
					OtherPropertyValue: ":::s0:c98,c99",
				},
				{
					PropertyName:       "SELinuxLabel",
					EventReason:        "SELinuxLabelConflict",
					Pod:                cache.ObjectName{Namespace: namespace, Name: "pod2"},
					PropertyValue:      ":::s0:c98,c99",
					OtherPod:           cache.ObjectName{Namespace: namespace, Name: "pod1"},
					OtherPropertyValue: ":::s0:c1,c2",
				},
			},
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        ":::s0:c1,c2",
					changePolicy: v1.SELinuxChangePolicyMountOption,
					csiDriver:    "ebs.csi.aws.com", // 该 PV 是伪造的 EBS 卷
				},
			},
			expectedEvents: []string{
				`Normal SELinuxLabelConflict SELinuxLabel ":::s0:c1,c2" conflicts with pod pod2 that uses the same volume as this pod with SELinuxLabel ":::s0:c98,c99". If both pods land on the same node, only one of them may access the volume.`,
				`Normal SELinuxLabelConflict SELinuxLabel ":::s0:c98,c99" conflicts with pod pod1 that uses the same volume as this pod with SELinuxLabel ":::s0:c1,c2". If both pods land on the same node, only one of them may access the volume.`,
			},
		},
		{
			name: "existing pod with Recursive policy does not generate conflicts",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", ptr.To(v1.SELinuxChangePolicyRecursive)).withPVC("pvc1", "vol1").build(),
				pod("pod2", "s0:c98,c99", ptr.To(v1.SELinuxChangePolicyRecursive)).build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			conflicts:               []volumecache.Conflict{},
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        "", // Recursive 策略下标签会被清空
					changePolicy: v1.SELinuxChangePolicyRecursive,
					csiDriver:    "ebs.csi.aws.com", // 该 PV 是伪造的 EBS 卷
				},
			},
		},
		{
			name: "existing pod with Recursive policy does not conflict with pod with MountOption policy label, only with the policy",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", ptr.To(v1.SELinuxChangePolicyRecursive)).withPVC("pvc1", "vol1").build(),
				pod("pod2", "s0:c98,c99", ptr.To(v1.SELinuxChangePolicyMountOption)).withPVC("pvc1", "vol1").build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			conflicts: []volumecache.Conflict{
				{
					PropertyName:       "SELinuxChangePolicy",
					EventReason:        "SELinuxChangePolicyConflict",
					Pod:                cache.ObjectName{Namespace: namespace, Name: "pod1"},
					PropertyValue:      string(v1.SELinuxChangePolicyRecursive),
					OtherPod:           cache.ObjectName{Namespace: namespace, Name: "pod2"},
					OtherPropertyValue: string(v1.SELinuxChangePolicyMountOption),
				},
				{
					PropertyName:       "SELinuxChangePolicy",
					EventReason:        "SELinuxChangePolicyConflict",
					Pod:                cache.ObjectName{Namespace: namespace, Name: "pod2"},
					PropertyValue:      string(v1.SELinuxChangePolicyMountOption),
					OtherPod:           cache.ObjectName{Namespace: namespace, Name: "pod1"},
					OtherPropertyValue: string(v1.SELinuxChangePolicyRecursive),
				},
			},
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        "", // Recursive 策略下标签会被清空
					changePolicy: v1.SELinuxChangePolicyRecursive,
					csiDriver:    "ebs.csi.aws.com", // 该 PV 是伪造的 EBS 卷
				},
			},
			expectedEvents: []string{
				`Normal SELinuxChangePolicyConflict SELinuxChangePolicy "Recursive" conflicts with pod pod2 that uses the same volume as this pod with SELinuxChangePolicy "MountOption". If both pods land on the same node, only one of them may access the volume.`,
				`Normal SELinuxChangePolicyConflict SELinuxChangePolicy "MountOption" conflicts with pod pod1 that uses the same volume as this pod with SELinuxChangePolicy "Recursive". If both pods land on the same node, only one of them may access the volume.`,
			},
		},
		{
			name: "existing pod with PVC generates conflict, the other pod doesn't exist",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("pvc1", "vol1").build(),
				// pod2 不在 informer 缓存中
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			conflicts: []volumecache.Conflict{
				{
					PropertyName:       "SELinuxLabel",
					EventReason:        "SELinuxLabelConflict",
					Pod:                cache.ObjectName{Namespace: namespace, Name: "pod1"},
					PropertyValue:      ":::s0:c1,c2",
					OtherPod:           cache.ObjectName{Namespace: namespace, Name: "pod2"},
					OtherPropertyValue: ":::s0:c98,c99",
				},
				{
					PropertyName:       "SELinuxLabel",
					EventReason:        "SELinuxLabelConflict",
					Pod:                cache.ObjectName{Namespace: namespace, Name: "pod2"},
					PropertyValue:      ":::s0:c98,c99",
					OtherPod:           cache.ObjectName{Namespace: namespace, Name: "pod1"},
					OtherPropertyValue: ":::s0:c1,c2",
				},
			},
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        ":::s0:c1,c2",
					changePolicy: v1.SELinuxChangePolicyMountOption,
					csiDriver:    "ebs.csi.aws.com", // 该 PV 是伪造的 EBS 卷
				},
			},
			expectedEvents: []string{
				// 不存在的 Pod 不应收到事件
				`Normal SELinuxLabelConflict SELinuxLabel ":::s0:c1,c2" conflicts with pod pod2 that uses the same volume as this pod with SELinuxLabel ":::s0:c98,c99". If both pods land on the same node, only one of them may access the volume.`,
			},
		},
		{
			name: "empty label implies Recursive policy",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "", ptr.To(v1.SELinuxChangePolicyMountOption)).withPVC("pvc1", "vol1").build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			conflicts:               []volumecache.Conflict{},
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        "",
					changePolicy: v1.SELinuxChangePolicyRecursive, // 标签为空时回退到 Recursive 策略
					csiDriver:    "ebs.csi.aws.com",
				},
			},
		},
		{
			name: "pending pod is processed",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("pvc1", "vol1").withPhase(v1.PodPending).build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectedEvents:          nil,
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        ":::s0:c1,c2",
					changePolicy: v1.SELinuxChangePolicyMountOption,
					csiDriver:    "ebs.csi.aws.com",
				},
			},
		},
		{
			name: "unknown pod is processed",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("pvc1", "vol1").withPhase(v1.PodUnknown).build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectedEvents:          nil,
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        ":::s0:c1,c2",
					changePolicy: v1.SELinuxChangePolicyMountOption,
					csiDriver:    "ebs.csi.aws.com",
				},
			},
		},
		{
			name: "succeeded pod is removed from the cache",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("pvc1", "vol1").withPhase(v1.PodSucceeded).build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectedEvents:          nil,
			expectedAddedVolumes:    nil,
			expectedDeletedPods:     []cache.ObjectName{{Namespace: namespace, Name: "pod1"}},
		},
		{
			name: "failed pod is removed from the cache",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("pvc1", "vol1").withPhase(v1.PodFailed).build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectedEvents:          nil,
			expectedAddedVolumes:    nil,
			expectedDeletedPods:     []cache.ObjectName{{Namespace: namespace, Name: "pod1"}},
		},
		{
			name:         "deleted pod",
			existingPods: []*v1.Pod{
				// pod1 不在 informer 缓存中
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: true,
			expectError:             false,
			expectedEvents:          nil,
			expectedAddedVolumes:    nil,
			expectedDeletedPods:     []cache.ObjectName{{Namespace: namespace, Name: "pod1"}},
		},
		{
			name: "existing pod with fully bound PVC and CSIDriver.SELinuxMount disabled",
			existingPVCs: []*v1.PersistentVolumeClaim{
				pvcBoundToPV("pv1", "pvc1"),
			},
			existingPVs: []*v1.PersistentVolume{
				pvBoundToPVC("pv1", "pvc1"),
			},
			existingPods: []*v1.Pod{
				pod("pod1", "s0:c1,c2", nil).withPVC("pvc1", "vol1").build(),
			},
			pod:                     cache.ObjectName{Namespace: namespace, Name: "pod1"},
			csiDriverSELinuxEnabled: false,
			expectedEvents:          nil,
			expectedAddedVolumes: []addedVolume{
				{
					volumeName:   "fake-plugin/pv1",
					podKey:       cache.ObjectName{Namespace: namespace, Name: "pod1"},
					label:        "",                              // 驱动不支持 SELinuxMount 时清空标签
					changePolicy: v1.SELinuxChangePolicyRecursive, // 驱动不支持 SELinuxMount 时回退到 Recursive
					csiDriver:    "ebs.csi.aws.com",               // 该 PV 是伪造的 EBS 卷
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			defer wg.Wait()
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			_, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
			plugin.SupportsSELinux = tt.csiDriverSELinuxEnabled

			fakeClient := fake.NewClientset()
			fakeInformerFactory := informers.NewSharedInformerFactory(fakeClient, controller.NoResyncPeriodFunc())
			podInformer := fakeInformerFactory.Core().V1().Pods()
			pvcInformer := fakeInformerFactory.Core().V1().PersistentVolumeClaims()
			pvInformer := fakeInformerFactory.Core().V1().PersistentVolumes()
			csiDriverInformer := fakeInformerFactory.Storage().V1().CSIDrivers()

			c, err := NewController(
				ctx,
				fakeClient,
				podInformer,
				pvcInformer,
				pvInformer,
				csiDriverInformer,
				[]volume.VolumePlugin{plugin},
				nil,
			)
			if err != nil {
				t.Fatalf("failed to create controller: %v", err)
			}
			// 用假卷缓存记录调用并按需注入冲突，避免依赖真实实现
			labelCache := &fakeVolumeCache{
				conflictsToSend: map[cache.ObjectName][]volumecache.Conflict{
					{Namespace: tt.pod.Namespace, Name: tt.pod.Name}: tt.conflicts,
				},
			}
			c.labelCache = labelCache
			fakeRecorder := record.NewFakeRecorder(10)
			c.eventRecorder = fakeRecorder

			// 启动 informer 并等待缓存同步完成
			fakeInformerFactory.Start(ctx.Done())
			fakeInformerFactory.WaitForCacheSync(ctx.Done())
			// 启动控制器；sync 逻辑不依赖其运行，此处仅模拟真实运行环境
			wg.Go(func() {
				c.Run(ctx, 1)
			})

			// 直接写入 informer 的本地存储，注入测试对象
			for _, pvc := range tt.existingPVCs {
				_ = pvcInformer.Informer().GetStore().Add(pvc)
			}
			for _, pv := range tt.existingPVs {
				_ = pvInformer.Informer().GetStore().Add(pv)
			}
			for _, pod := range tt.existingPods {
				_ = podInformer.Informer().GetStore().Add(pod)
			}

			// 对缓存中的目标 Pod 调用 sync
			err = c.sync(ctx, tt.pod)

			// 校验卷缓存、事件和删除记录
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return // 出错时不再检查其余部分
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			sortAddedVolumes(tt.expectedAddedVolumes)
			sortAddedVolumes(labelCache.addedVolumes)
			if !reflect.DeepEqual(tt.expectedAddedVolumes, labelCache.addedVolumes) {
				t.Errorf("unexpected added volumes, expected \n%+v\ngot\n%+v", tt.expectedAddedVolumes, labelCache.addedVolumes)
			}

			events := collectEvents(fakeRecorder.Events)
			receivedSet := sets.New(events...)
			expectedSet := sets.New(tt.expectedEvents...)
			if !receivedSet.Equal(expectedSet) {
				t.Errorf("got unexpected events: %+v", receivedSet.Difference(expectedSet))
				t.Errorf("missing events: %+v", expectedSet.Difference(receivedSet))
			}

			if !reflect.DeepEqual(tt.expectedDeletedPods, labelCache.deletedPods) {
				t.Errorf("unexpected deleted pods, expected \n%+v\ngot\n%+v", tt.expectedDeletedPods, labelCache.deletedPods)
			}
		})
	}
}

// pv 创建一个使用 AWS EBS 作为卷源的伪造 PV。
func pv(name string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
					VolumeID: name,
				},
			},
		},
	}
}

// pvc 创建一个绑定所需命名空间和 UID 的伪造 PVC。
func pvc(name string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       pvcUID,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
		},
	}
}

// pvBoundToPVC 创建一个与指定 PVC 绑定的 PV。
func pvBoundToPVC(pvName, pvcName string) *v1.PersistentVolume {
	pv := pv(pvName)
	pv.Spec.ClaimRef = &v1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		Namespace:  namespace,
		Name:       pvcName,
		UID:        pvcUID,
		APIVersion: "v1",
	}
	pv.Status.Phase = v1.VolumeBound
	return pv
}

// pvcBoundToPV 创建一个与指定 PV 绑定的 PVC。
func pvcBoundToPV(pvName, pvcName string) *v1.PersistentVolumeClaim {
	pvc := pvc(pvcName)
	pvc.Spec.VolumeName = pvName
	pvc.Status.Phase = v1.ClaimBound

	return pvc
}

// podBuilder 以流式方式构造测试用 Pod，减少重复的字段设置。
type podBuilder struct {
	// pod 为当前正在构造的 Pod 对象。
	pod *v1.Pod
}

// pod 创建一个带有基础容器和 emptyDir 卷的 Pod 构造器；
// level 为空时不设置 SELinuxOptions，以覆盖未指定标签的场景。
func pod(podName, level string, changePolicy *v1.PodSELinuxChangePolicy) *podBuilder {
	var opts *v1.SELinuxOptions
	if level != "" {
		opts = &v1.SELinuxOptions{
			Level: level,
		}
	}
	return &podBuilder{
		pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns1",
				Name:      podName,
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "container1",
						Image: "image1",
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "vol1",
								MountPath: "/mnt",
							},
						},
					},
				},
				SecurityContext: &v1.PodSecurityContext{
					SELinuxChangePolicy: changePolicy,
					SELinuxOptions:      opts,
				},
				Volumes: []v1.Volume{
					{
						Name: "emptyDir1",
						VolumeSource: v1.VolumeSource{
							EmptyDir: &v1.EmptyDirVolumeSource{},
						},
					},
				},
			},
			Status: v1.PodStatus{
				Phase: v1.PodRunning,
			},
		},
	}
}

// withPhase 设置 Pod 的阶段，用于测试不同阶段的处理逻辑。
func (b *podBuilder) withPhase(phase v1.PodPhase) *podBuilder {
	b.pod.Status.Phase = phase
	return b
}

// withInlineVolume 为 Pod 添加一个 AWS EBS 内联卷及其挂载点。
func (b *podBuilder) withInlineVolume() *podBuilder {
	b.pod.Spec.Volumes = append(b.pod.Spec.Volumes, v1.Volume{
		Name: "inlineVolume",
		VolumeSource: v1.VolumeSource{
			AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
				VolumeID: "inlinevol1",
			},
		},
	})
	b.pod.Spec.Containers[0].VolumeMounts = append(b.pod.Spec.Containers[0].VolumeMounts, v1.VolumeMount{
		Name:      "inlineVolume",
		MountPath: "/mnt",
	})
	return b
}

// withPVC 为 Pod 添加一个引用指定 PVC 的卷及其挂载点。
func (b *podBuilder) withPVC(pvcName, volumeName string) *podBuilder {
	b.pod.Spec.Volumes = append(b.pod.Spec.Volumes, v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		},
	})
	b.pod.Spec.Containers[0].VolumeMounts = append(b.pod.Spec.Containers[0].VolumeMounts, v1.VolumeMount{
		Name:      volumeName,
		MountPath: "/mnt",
	})
	return b
}

// build 返回构造完成的 Pod 对象。
func (b *podBuilder) build() *v1.Pod {
	return b.pod
}

// addedVolume 记录一次 AddVolume 调用的参数，供断言使用。
type addedVolume struct {
	// volumeName 为卷的唯一名称。
	volumeName v1.UniqueVolumeName
	// podKey 为该卷所属 Pod 的键。
	podKey cache.ObjectName
	// label 为该 Pod 的 SELinux 标签。
	label string
	// changePolicy 为该 Pod 的 SELinux 变更策略。
	changePolicy v1.PodSELinuxChangePolicy
	// csiDriver 为该卷对应的 CSI 驱动名称。
	csiDriver string
}

// sortAddedVolumes 按卷名和 Pod 键排序，消除遍历顺序带来的不确定性。
func sortAddedVolumes(a []addedVolume) {
	sort.Slice(a, func(i, j int) bool {
		ikey := string(a[i].volumeName) + "/" + a[i].podKey.String()
		jkey := string(a[j].volumeName) + "/" + a[j].podKey.String()
		return ikey < jkey
	})
}

// fakeVolumeCache 是 VolumeCache 的测试替身，记录调用并按配置返回冲突。
type fakeVolumeCache struct {
	// addedVolumes 记录所有 AddVolume 调用。
	addedVolumes []addedVolume
	// conflictsToSend 按 Pod 键配置 AddVolume 时要返回的冲突。
	conflictsToSend map[cache.ObjectName][]volumecache.Conflict
	// deletedPods 记录所有 DeletePod 调用。
	deletedPods []cache.ObjectName
}

// 编译期检查 fakeVolumeCache 实现了 VolumeCache 接口。
var _ volumecache.VolumeCache = &fakeVolumeCache{}

// AddVolume 记录卷信息并返回该 Pod 预置的冲突列表。
func (f *fakeVolumeCache) AddVolume(logger klog.Logger, volumeName v1.UniqueVolumeName, podKey cache.ObjectName, label string, changePolicy v1.PodSELinuxChangePolicy, csiDriver string) []volumecache.Conflict {
	f.addedVolumes = append(f.addedVolumes, addedVolume{
		volumeName:   volumeName,
		podKey:       podKey,
		label:        label,
		changePolicy: changePolicy,
		csiDriver:    csiDriver,
	})
	conflicts := f.conflictsToSend[podKey]
	return conflicts
}

// DeletePod 记录被删除的 Pod 键。
func (f *fakeVolumeCache) DeletePod(logger klog.Logger, podKey cache.ObjectName) {
	f.deletedPods = append(f.deletedPods, podKey)
}

// GetPodsForCSIDriver 返回使用指定 CSI 驱动的所有 Pod 键。
func (f *fakeVolumeCache) GetPodsForCSIDriver(driverName string) []cache.ObjectName {
	pods := []cache.ObjectName{}
	for _, v := range f.addedVolumes {
		if v.csiDriver == driverName {
			pods = append(pods, v.podKey)
		}
	}
	return pods
}

// GetConflicts 汇总返回预置的全部冲突。
func (f *fakeVolumeCache) GetConflicts(logger klog.Logger) []volumecache.Conflict {
	result := make([]volumecache.Conflict, 0)
	for _, conflicts := range f.conflictsToSend {
		result = append(result, conflicts...)
	}
	return result
}

// collectEvents 非阻塞地排空 FakeRecorder 的事件通道，
// 避免等待事件到达导致测试挂起。
func collectEvents(source <-chan string) []string {
	done := false
	events := make([]string, 0)
	for !done {
		select {
		case event := <-source:
			events = append(events, event)
		default:
			done = true
		}
	}
	return events
}
