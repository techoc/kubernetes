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

package metrics

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	controllervolumetesting "k8s.io/kubernetes/pkg/controller/volume/attachdetach/testing"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

// TestVolumesInUseMetricCollection 验证采集器能根据 Pod 的 PVC 卷统计出每个节点上
// 各 CSI 插件的在用卷数（含 CSI 迁移后的插件名转换）。
func TestVolumesInUseMetricCollection(t *testing.T) {
	fakeVolumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	fakeClient := &fake.Clientset{}

	fakeInformerFactory := informers.NewSharedInformerFactory(fakeClient, controller.NoResyncPeriodFunc())
	fakePodInformer := fakeInformerFactory.Core().V1().Pods()
	// 构造一个已调度到节点上的 Pod，引用一个已绑定 PVC。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "metric-test-pod",
			UID:       "metric-test-pod-uid",
			Namespace: "metric-test",
		},
		Spec: v1.PodSpec{
			NodeName: "metric-test-host",
			Volumes: []v1.Volume{
				{
					Name: "metric-test-volume-name",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "metric-test-pvc",
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPhase("Running"),
		},
	}

	// 直接写入 informer 本地存储，模拟缓存变更事件，避免启动整个 informer。
	fakePodInformer.Informer().GetStore().Add(pod)
	pvcInformer := fakeInformerFactory.Core().V1().PersistentVolumeClaims()
	pvInformer := fakeInformerFactory.Core().V1().PersistentVolumes()

	// PVC 与 PV 成对构造并提供绑定引用，确保卷规范能被正确解析。
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "metric-test-pvc",
			Namespace: "metric-test",
			UID:       "metric-test-pvc-1",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany, v1.ReadWriteOnce},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("2G"),
				},
			},
			VolumeName: "test-metric-pv-1",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			UID:  "test-metric-pv-1",
			Name: "test-metric-pv-1",
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("5G"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				// GCE CSI 迁移完成后，此类卷由 PD CSI 插件处理，故使用 GCEPersistentDisk 源即可验证迁移映射。
				GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany},
			// 假装该卷已绑定，满足卷规范解析的前置条件。
			ClaimRef: &v1.ObjectReference{UID: "metric-test-pvc-1", Namespace: "metric-test"},
		},
	}
	// 将 PVC 与 PV 写入各自的 lister 缓存，供采集器解析卷规范使用。
	pvcInformer.Informer().GetStore().Add(pvc)
	pvInformer.Informer().GetStore().Add(pv)
	pvcLister := pvcInformer.Lister()
	pvLister := pvInformer.Lister()

	csiTranslator := csitrans.New()
	// asw/dsw 传 nil：本测试只统计在用卷，不访问状态世界。
	// lister 传 nil：本测试只访问状态世界，不解析 Pod/PVC/PV。
	metricCollector := newAttachDetachStateCollector(
		pvcLister,
		fakePodInformer.Lister(),
		pvLister,
		nil,
		nil,
		fakeVolumePluginMgr,
		csimigration.NewPluginManager(csiTranslator),
		csiTranslator)
	logger, _ := ktesting.NewTestContext(t)
	nodeUseMap := metricCollector.getVolumeInUseCount(logger)
	// 至少应统计到一个节点，否则说明 Pod 被错误跳过。
	if len(nodeUseMap) < 1 {
		t.Errorf("Expected one volume in use got %d", len(nodeUseMap))
	}
	// 断言指标按节点名分组，且插件名为迁移后的 CSI 插件名。
	testNodeMetric := nodeUseMap["metric-test-host"]
	pluginUseCount, ok := testNodeMetric["fake-plugin:pd.csi.storage.gke.io"]
	if !ok {
		t.Errorf("Expected fake plugin pvc got nothing")
	}

	if pluginUseCount < 1 {
		t.Errorf("Expected at least in-use volume metric got %d", pluginUseCount)
	}
}

// TestTotalVolumesMetricCollection 验证采集器能分别统计期望状态世界与实际状态世界中的卷数，
// 且两者都按插件名分组上报。
func TestTotalVolumesMetricCollection(t *testing.T) {
	fakeVolumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	// 用真实的状态世界对象：分别添加一个待挂载卷和一个已挂载卷。
	dsw := cache.NewDesiredStateOfWorld(fakeVolumePluginMgr)
	asw := cache.NewActualStateOfWorld(fakeVolumePluginMgr)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")

	// 必须先添加节点，Pod 才能关联到该节点。
	dsw.AddNode(nodeName)
	_, err := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	logger, _ := ktesting.NewTestContext(t)
	// 在实际状态世界中添加已挂载卷，构造期望与实际各一卷的场景。
	asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, "", true)

	csiTranslator := csitrans.New()
	metricCollector := newAttachDetachStateCollector(
		nil,
		nil,
		nil,
		asw,
		dsw,
		fakeVolumePluginMgr,
		csimigration.NewPluginManager(csiTranslator),
		csiTranslator)

	totalVolumesMap := metricCollector.getTotalVolumesCount()
	// 期望与实际状态应各占一个维度，共两个条目。
	if len(totalVolumesMap) != 2 {
		t.Errorf("Expected 2 states, got %d", len(totalVolumesMap))
	}

	dswCount, ok := totalVolumesMap["desired_state_of_world"]
	if !ok {
		t.Errorf("Expected desired_state_of_world, got nothing")
	}

	// 断言卷被归入 fake-plugin，计数为 1。
	fakePluginCount := dswCount["fake-plugin"]
	if fakePluginCount != 1 {
		t.Errorf("Expected 1 fake-plugin volume in DesiredStateOfWorld, got %d", fakePluginCount)
	}

	aswCount, ok := totalVolumesMap["actual_state_of_world"]
	if !ok {
		t.Errorf("Expected actual_state_of_world, got nothing")
	}

	fakePluginCount = aswCount["fake-plugin"]
	if fakePluginCount != 1 {
		t.Errorf("Expected 1 fake-plugin volume in ActualStateOfWorld, got %d", fakePluginCount)
	}
}
