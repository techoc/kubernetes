/*
Copyright 2016 The Kubernetes Authors.

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

package attachdetach

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	kcache "k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	controllervolumetesting "k8s.io/kubernetes/pkg/controller/volume/attachdetach/testing"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	intreePDUniqueNamePrefix = "kubernetes.io/gce-pd/"
	csiPDUniqueNamePrefix    = "kubernetes.io/csi/pd.csi.storage.gke.io^projects/UNSPECIFIED/zones/UNSPECIFIED/disks/"
)

// createADC 用测试用的 informer 工厂和假客户端创建 Attach/Detach 控制器，
// 并断言返回的具体实现类型。
func createADC(t testing.TB, tCtx ktesting.TContext, fakeKubeClient *fake.Clientset,
	informerFactory informers.SharedInformerFactory, plugins []volume.VolumePlugin) *attachDetachController {

	adcObj, err := NewAttachDetachController(
		tCtx,
		fakeKubeClient,
		informerFactory.Core().V1().Pods(),
		informerFactory.Core().V1().Nodes(),
		informerFactory.Core().V1().PersistentVolumeClaims(),
		informerFactory.Core().V1().PersistentVolumes(),
		informerFactory.Storage().V1().CSINodes(),
		informerFactory.Storage().V1().CSIDrivers(),
		informerFactory.Storage().V1().VolumeAttachments(),
		plugins,
		nil, /* prober */
		false,
		1*time.Second,
		false,
		DefaultTimerConfig,
	)

	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: <%v>", err)
	}
	return adcObj.(*attachDetachController)
}

// Test_NewAttachDetachController_Positive 验证控制器在空插件列表下也能正常创建。
func Test_NewAttachDetachController_Positive(t *testing.T) {
	tCtx := ktesting.Init(t)

	// 准备测试客户端与 informer 工厂。
	fakeKubeClient := controllervolumetesting.CreateTestClient(tCtx.Logger())
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())

	// 执行：创建控制器，若不报错即通过。
	createADC(t, tCtx, fakeKubeClient, informerFactory, nil)
}

// Test_AttachDetachControllerStateOfWorldPopulators_Positive 验证两个 populate 方法能把节点状态中的
// 已挂载卷（含 CSI 迁移后的卷名）和 Pod 引用的卷分别填入实际状态与期望状态。
func Test_AttachDetachControllerStateOfWorldPopulators_Positive(t *testing.T) {
	tCtx := ktesting.Init(t)
	logger := tCtx.Logger()

	// 准备测试客户端、informer 工厂以及测试插件与 CSI 插件。
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())

	var plugins []volume.VolumePlugin
	plugins = append(plugins, controllervolumetesting.CreateTestPlugin(false)...)
	plugins = append(plugins, csi.ProbeVolumePlugins()...)

	adc := createADC(t, tCtx, fakeKubeClient, informerFactory, plugins)

	// 启动 informer 并等待缓存同步，然后依次填充实际状态与期望状态。
	informerFactory.Start(tCtx.Done())
	informerFactory.WaitForCacheSync(tCtx.Done())

	err := adc.populateActualStateOfWorld(logger)
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: <%v>", err)
	}

	err = adc.populateDesiredStateOfWorld(logger)
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: %v", err)
	}

	// 断言实际状态包含所有节点上已挂载的卷，且挂载标记与 VolumesInUse 一致。
	nodes, err := adc.nodeLister.List(labels.Everything())
	if err != nil {
		t.Fatalf("Failed to list nodes in indexer. Expected: <no error> Actual: %v", err)
	}

	for _, node := range nodes {
		nodeName := types.NodeName(node.Name)
		inUseVolumes := sets.New(node.Status.VolumesInUse...)
		allAttachedVolumes := map[v1.UniqueVolumeName]cache.AttachedVolume{}
		for _, v := range adc.actualStateOfWorld.GetAttachedVolumesForNode(nodeName) {
			allAttachedVolumes[v.VolumeName] = v
		}

		for _, attachedVolume := range node.Status.VolumesAttached {
			attachedState := adc.actualStateOfWorld.GetAttachState(attachedVolume.Name, nodeName)
			if attachedState != cache.AttachStateAttached {
				t.Fatalf("Run failed with error. Node %s, volume %s not found", nodeName, attachedVolume.Name)
			}
			inUse := inUseVolumes.Has(attachedVolume.Name)
			mounted := allAttachedVolumes[attachedVolume.Name].MountedByNode
			if mounted != inUse {
				t.Fatalf("Node %s, volume %s MountedByNode %v unexpected", nodeName, attachedVolume.Name, mounted)
			}
		}
	}

	pods, err := adc.podLister.List(labels.Everything())
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: %v", err)
	}
	for _, pod := range pods {
		// 测试客户端中的 Pod 都只有一个卷且共享同一个卷；pdName 是 CSI 迁移后转换出来的卷名的一部分。
		volumeName := v1.UniqueVolumeName(csiPDUniqueNamePrefix + "pdName")
		nodeName := types.NodeName(pod.Spec.NodeName)
		found := adc.desiredStateOfWorld.VolumeExists(volumeName, nodeName)
		if !found {
			t.Fatalf("Run failed with error. Volume %s, node %s not found in DesiredStateOfWorld",
				pod.Spec.Volumes[0].Name,
				pod.Spec.NodeName)
		}
	}
}

// largeClusterClient 构造一个包含 numNodes 个节点的假客户端，每个节点挂载 100 个卷，
// 用于基准测试大规模场景下填充实际状态的性能。
func largeClusterClient(t testing.TB, numNodes int) *fake.Clientset {
	// 创建 numNodes 个节点，每个节点带 100 个已挂载/使用中的卷。
	fakeKubeClient := fake.NewSimpleClientset()

	for i := 0; i < numNodes; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				Labels: map[string]string{
					"name": nodeName,
				},
				Annotations: map[string]string{
					util.ControllerManagedAttachAnnotation: "true",
				},
			},
		}
		for j := 0; j < 100; j++ {
			volumeName := v1.UniqueVolumeName(fmt.Sprintf("test-volume/vol-%d-%d", i, j))
			node.Status.VolumesAttached = append(node.Status.VolumesAttached, v1.AttachedVolume{
				Name:       volumeName,
				DevicePath: fmt.Sprintf("/dev/disk/by-id/vol-%d-%d", i, j),
			})
			node.Status.VolumesInUse = append(node.Status.VolumesInUse, volumeName)
			_, err := fakeKubeClient.CoreV1().PersistentVolumes().Create(context.Background(), &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("vol-%d-%d", i, j),
				},
			}, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to create PV: %v", err)
			}
		}
		_, err := fakeKubeClient.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create node: %v", err)
		}
	}

	return fakeKubeClient
}

// BenchmarkPopulateActualStateOfWorld 测量大规模集群下 populateActualStateOfWorld 的耗时。
func BenchmarkPopulateActualStateOfWorld(b *testing.B) {
	fakeKubeClient := largeClusterClient(b, 10000)
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())

	tCtx := ktesting.Init(b)
	adc := createADC(b, tCtx, fakeKubeClient, informerFactory, nil)

	// 启动 informer 并等待缓存同步后开始计时，避免把初始化开销计入基准结果。
	informerFactory.Start(tCtx.Done())
	informerFactory.WaitForCacheSync(tCtx.Done())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := adc.populateActualStateOfWorld(tCtx.Logger())
		if err != nil {
			b.Fatalf("Run failed with error. Expected: <no error> Actual: <%v>", err)
		}
	}
}

// BenchmarkNodeUpdate 测量大规模集群下处理节点更新事件的耗时。
func BenchmarkNodeUpdate(b *testing.B) {
	fakeKubeClient := largeClusterClient(b, 3000)
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())

	tCtx := ktesting.Init(b)
	logger := tCtx.Logger()
	adc := createADC(b, tCtx, fakeKubeClient, informerFactory, nil)

	informerFactory.Start(tCtx.Done())
	informerFactory.WaitForCacheSync(tCtx.Done())

	err := adc.populateActualStateOfWorld(logger.V(2))
	if err != nil {
		b.Fatalf("Run failed with error. Expected: <no error> Actual: <%v>", err)
	}

	node, err := fakeKubeClient.CoreV1().Nodes().Get(context.Background(), "node-123", metav1.GetOptions{})
	if err != nil {
		b.Fatalf("Run failed with error. Expected: <no error> Actual: <%v>", err)
	}
	// 先做一次 GC 并重置计时，确保基准结果只反映 nodeUpdate 的开销。
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adc.nodeUpdate(logger, node, node)
	}
}

// Test_AttachDetachControllerRecovery 验证在 populate 各阶段之间插入新 Pod 时控制器仍能恢复：
// 表驱动覆盖无新增 Pod、ASW 与 DSW 填充之间新增、DSW 填充与协调器启动之间新增，以及两处都新增的场景。
func Test_AttachDetachControllerRecovery(t *testing.T) {
	testCases := []struct {
		testName   string
		extraPods1 []*v1.Pod
		extraPods2 []*v1.Pod
	}{
		{
			testName: "No extra pods",
		},
		{
			testName:   "Add a new pod between ASW and DSW ppoulators",
			extraPods1: []*v1.Pod{controllervolumetesting.NewPodWithVolume("newpod-1", "volumeName2", "mynode-1")},
		},
		{
			testName:   "Add a new pod between DSW ppoulator and reconciler run",
			extraPods2: []*v1.Pod{controllervolumetesting.NewPodWithVolume("newpod-1", "volumeName2", "mynode-1")},
		},
		{
			testName: "Add a new pod between ASW and DSW ppoulators and another between DSW ppoulator and reconciler run",
			// 所有 Pod（包括已有 Pod）共享同一个底层卷，会转换出相同的 PV 名；但每个新增 Pod 位于不同节点，因此期望挂载卷数为 1+extraPodsNum。
			extraPods1: []*v1.Pod{controllervolumetesting.NewPodWithVolume("newpod-1", "volumeName2", "mynode-1")},
			extraPods2: []*v1.Pod{controllervolumetesting.NewPodWithVolume("newpod-2", "volumeName3", "mynode-2")},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			t.Parallel()
			attachDetachRecoveryTestCase(t, tc.extraPods1, tc.extraPods2)
		})
	}
}

// attachDetachRecoveryTestCase 执行单个恢复场景：在 ASW/DSW 填充的不同阶段插入 Pod，
// 启动协调循环后轮询测试插件，确认所有应挂载的卷已挂载、应卸载的卷已卸载。
func attachDetachRecoveryTestCase(t *testing.T, extraPods1 []*v1.Pod, extraPods2 []*v1.Pod) {
	tCtx := ktesting.Init(t)
	logger := tCtx.Logger()

	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, time.Second*1)
	plugins := controllervolumetesting.CreateTestPlugin(true)
	var prober volume.DynamicPluginProber = nil // TODO (#51147) inject mock
	nodeInformer := informerFactory.Core().V1().Nodes().Informer()
	csiNodeInformer := informerFactory.Storage().V1().CSINodes().Informer()
	podInformer := informerFactory.Core().V1().Pods().Informer()
	var podsNum, extraPodsNum, nodesNum, i int

	// 创建控制器，并确保测试结束时先取消上下文再等待协程退出。
	var wg sync.WaitGroup
	defer wg.Wait()
	defer tCtx.Cancel("test case terminating")

	adcObj, err := NewAttachDetachController(
		tCtx,
		fakeKubeClient,
		informerFactory.Core().V1().Pods(),
		informerFactory.Core().V1().Nodes(),
		informerFactory.Core().V1().PersistentVolumeClaims(),
		informerFactory.Core().V1().PersistentVolumes(),
		informerFactory.Storage().V1().CSINodes(),
		informerFactory.Storage().V1().CSIDrivers(),
		informerFactory.Storage().V1().VolumeAttachments(),
		plugins,
		prober,
		false,
		1*time.Second,
		false,
		DefaultTimerConfig,
	)

	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: <%v>", err)
	}

	adc := adcObj.(*attachDetachController)

	pods, err := fakeKubeClient.CoreV1().Pods(v1.NamespaceAll).List(tCtx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: %v", err)
	}

	for _, pod := range pods.Items {
		_ = podInformer.GetIndexer().Add(&pod)
		podsNum++
	}
	nodes, err := fakeKubeClient.CoreV1().Nodes().List(tCtx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: %v", err)
	}
	for _, node := range nodes.Items {
		_ = nodeInformer.GetIndexer().Add(&node)
		nodesNum++
	}

	csiNodes, err := fakeKubeClient.StorageV1().CSINodes().List(tCtx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: %v", err)
	}
	for _, csiNode := range csiNodes.Items {
		_ = csiNodeInformer.GetIndexer().Add(&csiNode)
	}

	informerFactory.Start(tCtx.Done())

	if !kcache.WaitForNamedCacheSyncWithContext(tCtx,
		informerFactory.Core().V1().Pods().Informer().HasSynced,
		informerFactory.Core().V1().Nodes().Informer().HasSynced,
		informerFactory.Storage().V1().CSINodes().Informer().HasSynced) {
		t.Fatalf("Error waiting for the informer caches to sync")
	}

	// 等待 informer 缓存中确实出现预期的节点、Pod 和 CSINode 对象。
	i = 0
	nodeList, err := informerFactory.Core().V1().Nodes().Lister().List(labels.Everything())
	for len(nodeList) < nodesNum {
		if err != nil {
			t.Fatalf("Error getting list of nodes %v", err)
		}
		if i > 100 {
			t.Fatalf("Time out while waiting for the node informer sync: found %d nodes, expected %d nodes", len(nodeList), nodesNum)
		}
		time.Sleep(100 * time.Millisecond)
		nodeList, err = informerFactory.Core().V1().Nodes().Lister().List(labels.Everything())
		i++
	}
	i = 0
	podList, err := informerFactory.Core().V1().Pods().Lister().List(labels.Everything())
	for len(podList) < podsNum {
		if err != nil {
			t.Fatalf("Error getting list of nodes %v", err)
		}
		if i > 100 {
			t.Fatalf("Time out while waiting for the pod informer sync: found %d pods, expected %d pods", len(podList), podsNum)
		}
		time.Sleep(100 * time.Millisecond)
		podList, err = informerFactory.Core().V1().Pods().Lister().List(labels.Everything())
		i++
	}
	i = 0
	csiNodesList, err := informerFactory.Storage().V1().CSINodes().Lister().List(labels.Everything())
	for len(csiNodesList) < nodesNum {
		if err != nil {
			t.Fatalf("Error getting list of csi nodes %v", err)
		}
		if i > 100 {
			t.Fatalf("Time out while waiting for the csinodes informer sync: found %d csinodes, expected %d csinodes", len(csiNodesList), nodesNum)
		}
		time.Sleep(100 * time.Millisecond)
		csiNodesList, err = informerFactory.Storage().V1().CSINodes().Lister().List(labels.Everything())
		i++
	}

	// 填充实际状态。
	err = adc.populateActualStateOfWorld(logger)
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: <%v>", err)
	}

	for _, newPod := range extraPods1 {
		// 在 ASW 与 DSW 填充之间新增 Pod，验证它能被期望状态填充器后续发现。
		_, err = adc.kubeClient.CoreV1().Pods(newPod.ObjectMeta.Namespace).Create(tCtx, newPod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Run failed with error. Failed to create a new pod: <%v>", err)
		}
		extraPodsNum++
		podInformer.GetIndexer().Add(newPod)

	}

	// 填充期望状态。
	err = adc.populateDesiredStateOfWorld(logger)
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: %v", err)
	}

	for _, newPod := range extraPods2 {
		// 在 DSW 填充之后、协调器启动之前新增 Pod，验证 DSW populator 循环能补上。
		_, err = adc.kubeClient.CoreV1().Pods(newPod.ObjectMeta.Namespace).Create(tCtx, newPod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Run failed with error. Failed to create a new pod: <%v>", err)
		}
		extraPodsNum++
		podInformer.GetIndexer().Add(newPod)
	}

	wg.Go(func() {
		adc.reconciler.Run(tCtx)
	})
	wg.Go(func() {
		adc.desiredStateOfWorldPopulator.Run(tCtx)
	})

	// 等待协调器至少执行一次同步。
	time.Sleep(time.Second * 1)

	testPlugin := plugins[0].(*controllervolumetesting.TestPlugin)
	for i = 0; i <= 10; i++ {
		var attachedVolumesNum int = 0
		var detachedVolumesNum int = 0

		time.Sleep(time.Second * 1) // 等待一秒后再轮询
		for _, volumeList := range testPlugin.GetAttachedVolumes() {
			attachedVolumesNum += len(volumeList)
		}
		for _, volumeList := range testPlugin.GetDetachedVolumes() {
			detachedVolumesNum += len(volumeList)
		}

		// 每个新增 Pod 都应触发挂载；它们共享同一个卷（+1），而仅出现在节点状态中的卷都应该被卸载。
		if attachedVolumesNum == 1+extraPodsNum && detachedVolumesNum == nodesNum {
			break
		}
		if i == 10 { // 超时 10 秒
			t.Fatalf("Waiting for the volumes to attach/detach timed out: attached %d (expected %d); detached %d (%d)",
				attachedVolumesNum, 1+extraPodsNum, detachedVolumesNum, nodesNum)
		}
	}

	if testPlugin.GetErrorEncountered() {
		t.Fatalf("Fatal error encountered in the testing volume plugin")
	}
}

// vaTest 描述一个 VolumeAttachment 恢复用例：指定 Pod、PV、VA 的状态，以及期望的挂载/卸载调用和 ASW 状态。
type vaTest struct {
	testName               string
	volName                string
	podName                string
	podNodeName            string
	pvName                 string
	vaName                 string
	vaNodeName             string
	vaAttachStatus         bool
	csiMigration           bool
	expected_attaches      map[string][]string
	expected_detaches      map[string][]string
	expectedASWAttachState cache.AttachState
}

// Test_ADC_VolumeAttachmentRecovery 表驱动验证 VolumeAttachment 的恢复行为：
// 覆盖已删除 Pod 的悬空 VA（无论 VA 状态如何都应卸载）、迁移卷的已调度 Pod（应标记为已挂载），以及迁移卷的已删除 Pod（应标记为不确定）。
func Test_ADC_VolumeAttachmentRecovery(t *testing.T) {
	for _, tc := range []vaTest{
		{ // Pod 已删除、VA 状态为 attached：悬空卷应被卸载
			testName:          "VA status is attached",
			volName:           "vol1",
			pvName:            "pv1",
			vaName:            "va1",
			vaNodeName:        "mynode-1",
			vaAttachStatus:    true,
			expected_attaches: map[string][]string{},
			expected_detaches: map[string][]string{"mynode-1": {"vol1"}},
		},
		{ // Pod 已删除、VA 状态为 unattached：悬空卷同样应被卸载
			testName:          "VA status is unattached",
			volName:           "vol1",
			pvName:            "pv1",
			vaName:            "va1",
			vaNodeName:        "mynode-1",
			vaAttachStatus:    false,
			expected_attaches: map[string][]string{},
			expected_detaches: map[string][]string{"mynode-1": {"vol1"}},
		},
		{ // Pod 已调度且卷已迁移、VA 状态为 unattached：卷应被标记为已挂载
			testName:               "Scheduled Pod with migrated PV",
			volName:                "vol1",
			podNodeName:            "mynode-1",
			pvName:                 "pv1",
			vaName:                 "va1",
			vaNodeName:             "mynode-1",
			vaAttachStatus:         false,
			csiMigration:           true,
			expectedASWAttachState: cache.AttachStateAttached,
		},
		{ // Pod 已删除且卷已迁移、VA 状态为 unattached：卷应被标记为不确定
			testName:               "Deleted Pod with migrated PV",
			volName:                "vol1",
			pvName:                 "pv1",
			vaName:                 "va1",
			vaNodeName:             "mynode-1",
			vaAttachStatus:         false,
			csiMigration:           true,
			expectedASWAttachState: cache.AttachStateUncertain,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			volumeAttachmentRecoveryTestCase(t, tc)
		})
	}
}

// volumeAttachmentRecoveryTestCase 执行单个 VolumeAttachment 恢复用例：
// 准备 Pod/PV/VA 等对象，填充 ASW 与 DSW，启动协调循环，并根据是否启用 CSI 迁移选择状态断言或插件调用断言。
func volumeAttachmentRecoveryTestCase(t *testing.T, tc vaTest) {
	tCtx := ktesting.Init(t)
	logger := tCtx.Logger()

	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, time.Second*1)
	var plugins []volume.VolumePlugin

	plugins = append(plugins, controllervolumetesting.CreateTestPlugin(false)...)
	plugins = append(plugins, csi.ProbeVolumePlugins()...)

	nodeInformer := informerFactory.Core().V1().Nodes().Informer()
	podInformer := informerFactory.Core().V1().Pods().Informer()
	pvInformer := informerFactory.Core().V1().PersistentVolumes().Informer()
	vaInformer := informerFactory.Storage().V1().VolumeAttachments().Informer()

	// 创建控制器，并确保测试结束时先取消上下文再等待协程退出。
	var wg sync.WaitGroup
	defer wg.Wait()
	defer tCtx.Cancel("test case terminating")

	adc := createADC(t, tCtx, fakeKubeClient, informerFactory, plugins)

	// 把测试插件预先创建的对象加入对应的 informer 索引器。
	pods, err := fakeKubeClient.CoreV1().Pods(v1.NamespaceAll).List(tCtx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: %v", err)
	}
	for _, pod := range pods.Items {
		_ = podInformer.GetIndexer().Add(&pod)
	}
	nodes, err := fakeKubeClient.CoreV1().Nodes().List(tCtx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: %v", err)
	}
	for _, node := range nodes.Items {
		_ = nodeInformer.GetIndexer().Add(&node)
	}

	if tc.csiMigration {
		newNode := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: tc.podNodeName,
				Labels: map[string]string{
					"name": tc.podNodeName,
				},
				Annotations: map[string]string{
					util.ControllerManagedAttachAnnotation: "true",
				},
			},
			Status: v1.NodeStatus{
				VolumesAttached: []v1.AttachedVolume{
					{
						Name:       v1.UniqueVolumeName(csiPDUniqueNamePrefix + tc.volName),
						DevicePath: "fake/path",
					},
				},
			},
		}
		_, err = adc.kubeClient.CoreV1().Nodes().Update(tCtx, newNode, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Run failed with error. Failed to create a new pod: <%v>", err)
		}
		nodeInformer.GetIndexer().Add(&newNode)
	}
	// 按用例要求创建并加入相关对象。
	if tc.podName != "" {
		newPod := controllervolumetesting.NewPodWithVolume(tc.podName, tc.volName, tc.podNodeName)
		_, err = adc.kubeClient.CoreV1().Pods(newPod.ObjectMeta.Namespace).Create(tCtx, newPod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Run failed with error. Failed to create a new pod: <%v>", err)
		}
		podInformer.GetIndexer().Add(newPod)
	}
	if tc.pvName != "" {
		var newPv *v1.PersistentVolume
		if tc.csiMigration {
			// NewPV 返回的是已迁移的 GCE PD 卷。
			newPv = controllervolumetesting.NewPV(tc.pvName, tc.volName)
		} else {
			// 未启用迁移时使用不受迁移影响的 NFS 卷。
			newPv = controllervolumetesting.NewNFSPV(tc.pvName, tc.volName)
		}
		_, err = adc.kubeClient.CoreV1().PersistentVolumes().Create(tCtx, newPv, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Run failed with error. Failed to create a new pv: <%v>", err)
		}
		pvInformer.GetIndexer().Add(newPv)
	}
	if tc.vaName != "" {
		newVa := controllervolumetesting.NewVolumeAttachment(tc.vaName, tc.pvName, tc.vaNodeName, tc.vaAttachStatus)
		_, err = adc.kubeClient.StorageV1().VolumeAttachments().Create(tCtx, newVa, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Run failed with error. Failed to create a new volumeAttachment: <%v>", err)
		}
		vaInformer.GetIndexer().Add(newVa)
	}

	// 等待 informer 缓存同步完成。
	informerFactory.Start(tCtx.Done())

	if !kcache.WaitForNamedCacheSyncWithContext(tCtx,
		informerFactory.Core().V1().Pods().Informer().HasSynced,
		informerFactory.Core().V1().Nodes().Informer().HasSynced,
		informerFactory.Core().V1().PersistentVolumes().Informer().HasSynced,
		informerFactory.Storage().V1().VolumeAttachments().Informer().HasSynced) {
		t.Fatalf("Error waiting for the informer caches to sync")
	}

	// 填充实际状态。
	err = adc.populateActualStateOfWorld(logger)
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: <%v>", err)
	}

	// 填充期望状态。
	err = adc.populateDesiredStateOfWorld(logger)
	if err != nil {
		t.Fatalf("Run failed with error. Expected: <no error> Actual: %v", err)
	}
	// 启动协调器与期望状态填充循环。
	wg.Go(func() {
		adc.reconciler.Run(tCtx)
	})
	wg.Go(func() {
		adc.desiredStateOfWorldPopulator.Run(tCtx)
	})
	if tc.csiMigration {
		verifyExpectedVolumeState(t, adc, tc)
	} else {
		// 未启用 CSI 迁移时，通过测试插件验证预期的挂载与卸载调用确实发生。
		testPlugin := plugins[0].(*controllervolumetesting.TestPlugin)
		verifyAttachDetachCalls(t, testPlugin, tc)
	}
}

// verifyExpectedVolumeState 在 CSI 迁移场景下断言 ASW 中的挂载状态：
// 迁移后的 CSI 卷名应处于期望状态，而树内卷名不应被标记为已挂载。
func verifyExpectedVolumeState(t *testing.T, adc *attachDetachController, tc vaTest) {
	// 启用 CSI 迁移后，PV 的挂载状态应以 CSI 卷名记录。
	attachedState := adc.actualStateOfWorld.GetAttachState(
		v1.UniqueVolumeName(csiPDUniqueNamePrefix+tc.volName), types.NodeName(tc.vaNodeName))
	if attachedState != tc.expectedASWAttachState {
		t.Fatalf("Expected attachedState %v, but it is %v", tc.expectedASWAttachState, attachedState)
	}

	// 启用 CSI 迁移后树内卷名不应被标记为已挂载，因而应处于 detached 状态。
	attachedState = adc.actualStateOfWorld.GetAttachState(
		v1.UniqueVolumeName(intreePDUniqueNamePrefix+tc.volName), types.NodeName(tc.vaNodeName))
	if attachedState != cache.AttachStateDetached {
		t.Fatalf("Expected attachedState not to be %v, but it is %v", cache.AttachStateDetached, attachedState)
	}
}

// verifyAttachDetachCalls 轮询测试插件，验证期望的节点-卷挂载与卸载调用都已出现：
// 协调操作是异步的，因此失败前会重试若干次，最后一轮仍不满足才报错。
func verifyAttachDetachCalls(t *testing.T, testPlugin *controllervolumetesting.TestPlugin, tc vaTest) {
	for tries := 0; tries <= 10; tries++ { // 失败前多轮询几次，等待异步操作完成
		expected_op_map := tc.expected_attaches
		plugin_map := testPlugin.GetAttachedVolumes()
		verify_op := "attach"
		volFound, nodeFound := false, false
		for i := 0; i <= 1; i++ { // 依次校验挂载与卸载
			if i == 1 {
				expected_op_map = tc.expected_detaches
				plugin_map = testPlugin.GetDetachedVolumes()
				verify_op = "detach"
			}
			// 校验期望映射中的每个 (节点, 卷) 都出现在插件的实际记录中。
			for expectedNode, expectedVolumeList := range expected_op_map {
				var volumeList []string
				volumeList, nodeFound = plugin_map[expectedNode]
				if !nodeFound && tries == 10 {
					t.Fatalf("Expected node not found, node:%v, op: %v, tries: %d",
						expectedNode, verify_op, tries)
				}
				for _, expectedVolume := range expectedVolumeList {
					volFound = slices.Contains(volumeList, expectedVolume)
					if !volFound && tries == 10 {
						t.Fatalf("Expected %v operation not found, node:%v, volume: %v, tries: %d",
							verify_op, expectedNode, expectedVolume, tries)
					}
				}
			}
		}
		if nodeFound && volFound {
			break
		}
		time.Sleep(time.Second * 1)
	}

	if testPlugin.GetErrorEncountered() {
		t.Fatalf("Fatal error encountered in the testing volume plugin")
	}
}

// TestPodDelete_Tombstone 验证 Pod 删除事件即使以墓碑形式送达，也能从期望状态中正确移除其卷。
func TestPodDelete_Tombstone(t *testing.T) {
	tCtx := ktesting.Init(t)
	logger := tCtx.Logger()

	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())

	adc := createADC(t, tCtx, fakeKubeClient, informerFactory,
		append(controllervolumetesting.CreateTestPlugin(false), csi.ProbeVolumePlugins()...))

	pod := controllervolumetesting.NewPodWithVolume("pod1", "vol1", "node1")
	volumeName := v1.UniqueVolumeName(csiPDUniqueNamePrefix + "pdName")

	adc.desiredStateOfWorld.AddNode("node1")
	adc.podAdd(logger, pod)

	if !adc.desiredStateOfWorld.VolumeExists(volumeName, "node1") {
		t.Fatalf("expected volume %s to exist in dsw", volumeName)
	}

	// 传入墓碑对象，模拟 informer 丢失删除事件现场的情况。
	adc.podDelete(logger, kcache.DeletedFinalStateUnknown{Key: "mynamespace/pod1", Obj: pod})

	if adc.desiredStateOfWorld.VolumeExists(volumeName, "node1") {
		t.Errorf("expected volume %s to be removed from dsw", volumeName)
	}
}
