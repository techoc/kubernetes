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

package populator

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
)

// TestFindAndAddActivePods_FindAndRemoveDeletedPods 验证期望状态世界的增删全流程：
// findAndAddActivePods 能把活跃 Pod 引用的卷加入 DSW，手动删除后再次调用可重新加入，
// 而 findAndRemoveDeletedPods 会在 Pod 从 informer 中消失后把对应卷清除。
func TestFindAndAddActivePods_FindAndRemoveDeletedPods(t *testing.T) {
	fakeVolumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	fakeClient := &fake.Clientset{}

	fakeInformerFactory := informers.NewSharedInformerFactory(fakeClient, controller.NoResyncPeriodFunc())
	fakePodInformer := fakeInformerFactory.Core().V1().Pods()

	fakesDSW := cache.NewDesiredStateOfWorld(fakeVolumePluginMgr)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dswp-test-pod",
			UID:       "dswp-test-pod-uid",
			Namespace: "dswp-test",
		},
		Spec: v1.PodSpec{
			NodeName: "dswp-test-host",
			Volumes: []v1.Volume{
				{
					Name: "dswp-test-volume-name",
					VolumeSource: v1.VolumeSource{
						RBD: &v1.RBDVolumeSource{
							RBDImage: "dswp-test-fake-device",
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPhase("Running"),
		},
	}

	// 直接写入 informer 的本地 store，省去启动 informer 与 apiserver 的开销。
	fakePodInformer.Informer().GetStore().Add(pod)

	// DSW 以唯一 Pod 名标识 Pod，需按生产逻辑生成同一标识。
	podName := util.GetUniquePodName(pod)

	// 期望的卷名由 fake 插件名与 RBD 镜像名拼接而成，须与生产命名规则一致。
	generatedVolumeName := "fake-plugin/" + pod.Spec.Volumes[0].RBD.RBDImage

	pvcLister := fakeInformerFactory.Core().V1().PersistentVolumeClaims().Lister()
	pvLister := fakeInformerFactory.Core().V1().PersistentVolumes().Lister()

	// 组装被测 populator：各 lister 指向 fake informer，使测试无需真实 apiserver。
	csiTranslator := csitrans.New()
	dswp := &desiredStateOfWorldPopulator{
		loopSleepDuration:        100 * time.Millisecond,
		listPodsRetryDuration:    3 * time.Second,
		desiredStateOfWorld:      fakesDSW,
		volumePluginMgr:          fakeVolumePluginMgr,
		podLister:                fakePodInformer.Lister(),
		pvcLister:                pvcLister,
		pvLister:                 pvLister,
		csiMigratedPluginManager: csimigration.NewPluginManager(csiTranslator),
		intreeToCSITranslator:    csiTranslator,
	}

	// 先把节点加入 DSW 管理列表，否则卷无法挂到该节点下。
	dswp.desiredStateOfWorld.AddNode(k8stypes.NodeName(pod.Spec.NodeName))
	logger, _ := ktesting.NewTestContext(t)
	dswp.findAndAddActivePods(logger)

	expectedVolumeName := v1.UniqueVolumeName(generatedVolumeName)

	// 校验 Pod 引用的卷已加入 DSW。
	volumeExists := dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, k8stypes.NodeName(pod.Spec.NodeName))
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	// 手动从 DSW 删除 Pod 与卷，构造需要重新发现的场景。
	dswp.desiredStateOfWorld.DeletePod(podName, expectedVolumeName, k8stypes.NodeName(pod.Spec.NodeName))

	// 校验卷已从 DSW 中移除。
	volumeExists = dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, k8stypes.NodeName(pod.Spec.NodeName))
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	// 再次运行发现逻辑，验证卷可被重新加入。
	dswp.findAndAddActivePods(logger)

	// 校验第二次发现后卷重新出现在 DSW。
	volumeExists = dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, k8stypes.NodeName(pod.Spec.NodeName))
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	fakePodInformer.Informer().GetStore().Delete(pod)
	dswp.findAndRemoveDeletedPods(logger)
	// 校验 Pod 从 informer 删除后，卷被 findAndRemoveDeletedPods 清除。
	volumeExists = dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, k8stypes.NodeName(pod.Spec.NodeName))
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

}

// TestFindAndRemoveNonattachableVolumes 验证卷插件变为不可挂载后，
// findAndRemoveDeletedPods 会将其从 DSW 中清除，避免后续尝试挂载。
func TestFindAndRemoveNonattachableVolumes(t *testing.T) {
	fakeVolumePluginMgr, fakeVolumePlugin := volumetesting.GetTestVolumePluginMgr(t)
	fakeClient := &fake.Clientset{}

	fakeInformerFactory := informers.NewSharedInformerFactory(fakeClient, controller.NoResyncPeriodFunc())
	fakePodInformer := fakeInformerFactory.Core().V1().Pods()

	fakesDSW := cache.NewDesiredStateOfWorld(fakeVolumePluginMgr)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dswp-test-pod",
			UID:       "dswp-test-pod-uid",
			Namespace: "dswp-test",
		},
		Spec: v1.PodSpec{
			NodeName: "dswp-test-host",
			Volumes: []v1.Volume{
				{
					Name: "dswp-test-volume-name",
					VolumeSource: v1.VolumeSource{
						CSI: &v1.CSIVolumeSource{
							Driver: "dswp-test-fake-csi-driver",
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPhase("Running"),
		},
	}

	// 直接写入 informer 的本地 store，省去启动 informer 与 apiserver 的开销。
	fakePodInformer.Informer().GetStore().Add(pod)

	// CSI 卷名由 fake 插件名与驱动名拼接，须与生产命名规则一致。
	generatedVolumeName := "fake-plugin/dswp-test-fake-csi-driver"
	pvcLister := fakeInformerFactory.Core().V1().PersistentVolumeClaims().Lister()
	pvLister := fakeInformerFactory.Core().V1().PersistentVolumes().Lister()

	// 组装被测 populator：各 lister 指向 fake informer，使测试无需真实 apiserver。
	csiTranslator := csitrans.New()
	dswp := &desiredStateOfWorldPopulator{
		loopSleepDuration:        100 * time.Millisecond,
		listPodsRetryDuration:    3 * time.Second,
		desiredStateOfWorld:      fakesDSW,
		volumePluginMgr:          fakeVolumePluginMgr,
		podLister:                fakePodInformer.Lister(),
		pvcLister:                pvcLister,
		pvLister:                 pvLister,
		csiMigratedPluginManager: csimigration.NewPluginManager(csiTranslator),
		intreeToCSITranslator:    csiTranslator,
	}

	// 先把节点加入 DSW 管理列表。
	dswp.desiredStateOfWorld.AddNode(k8stypes.NodeName(pod.Spec.NodeName))
	logger, _ := ktesting.NewTestContext(t)
	dswp.findAndAddActivePods(logger)

	expectedVolumeName := v1.UniqueVolumeName(generatedVolumeName)

	// 校验 CSI 卷已加入 DSW。
	volumeExists := dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, k8stypes.NodeName(pod.Spec.NodeName))
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	// 将插件标记为不可挂载，模拟卷能力变化。
	fakeVolumePlugin.NonAttachable = true

	dswp.findAndRemoveDeletedPods(logger)

	// 卷变为不可挂载后应被清理出 DSW。
	volumeExists = dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, k8stypes.NodeName(pod.Spec.NodeName))
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}
}
