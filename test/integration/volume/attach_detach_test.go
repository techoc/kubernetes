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

package volume

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoinformers "k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	basemetric "k8s.io/component-base/metrics"
	metricstestutil "k8s.io/component-base/metrics/testutil"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/controller/podgc"
	podgcmetrics "k8s.io/kubernetes/pkg/controller/podgc/metrics"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach"
	volumecache "k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/metrics"
	"k8s.io/kubernetes/pkg/controller/volume/persistentvolume"
	persistentvolumeoptions "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/options"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func fakePodWithVol(namespace string) *v1.Pod {
	fakePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "fakepod",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "fake-container",
					Image: "nginx",
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "fake-mount",
							MountPath: "/var/www/html",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "fake-mount",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/var/www/html",
						},
					},
				},
			},
			NodeName: "node-sandbox",
		},
	}
	return fakePod
}

func fakePodWithPVC(name, pvcName, namespace string) (*v1.Pod, *v1.PersistentVolumeClaim) {
	fakePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "fake-container",
					Image: "nginx",
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "fake-mount",
							MountPath: "/var/www/html",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "fake-mount",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
			NodeName: "node-sandbox",
		},
	}
	class := "fake-sc"
	fakePVC := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pvcName,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("5Gi"),
				},
			},
			StorageClassName: &class,
		},
	}
	return fakePod, fakePVC
}

var defaultTimerConfig = attachdetach.TimerConfig{
	ReconcilerLoopPeriod:                              100 * time.Millisecond,
	ReconcilerMaxWaitForUnmountDuration:               6 * time.Second,
	DesiredStateOfWorldPopulatorLoopSleepPeriod:       1 * time.Second,
	DesiredStateOfWorldPopulatorListPodsRetryDuration: 3 * time.Second,
}

// TestPodTerminationWithNodeOOSDetach 验证：给非优雅关机的节点打上 `out-of-service` taint 后，
// 其上所有 Pod 会被立即终止、卷被立即 detach，无需等待默认超时时间。
func TestPodTerminationWithNodeOOSDetach(t *testing.T) {
	// 没有运行 serviceaccount controller，需禁用 ServiceAccount admission plugin。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, ctrl, pvCtrl, podgcCtrl, informers := createAdClients(tCtx, t, server, defaultSyncPeriod, attachdetach.TimerConfig{
		ReconcilerLoopPeriod:                        100 * time.Millisecond,
		ReconcilerMaxWaitForUnmountDuration:         6 * time.Second,
		DesiredStateOfWorldPopulatorLoopSleepPeriod: 24 * time.Hour,
		// 用很大的时长禁用测试中的 DesiredStateOfWorldPopulator.findAndAddActivePods 循环。
		DesiredStateOfWorldPopulatorListPodsRetryDuration: 24 * time.Hour,
	})

	namespaceName := "test-node-oos"
	nodeName := "node-sandbox"
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Annotations: map[string]string{
				util.ControllerManagedAttachAnnotation: "true",
			},
		},
	}
	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	_, err := testClient.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to created node : %v", err)
	}

	pod := fakePodWithVol(namespaceName)
	if _, err := testClient.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create pod : %v", err)
	}

	// 启动 controller 循环。
	podInformer := informers.Core().V1().Pods().Informer()
	informers.Start(tCtx.Done())
	informers.WaitForCacheSync(tCtx.Done())
	go ctrl.Run(tCtx)
	go pvCtrl.Run(tCtx)
	go podgcCtrl.Run(tCtx)

	waitToObservePods(t, podInformer, 1)
	// 等待卷挂载完成。
	waitForVolumeToBeAttached(tCtx, t, testClient, pod.Name, nodeName)

	// 更新 node status 标记卷正在使用，attach-detach controller 据此判断能否安全 detach。
	node.Status.VolumesInUse = append(node.Status.VolumesInUse, "kubernetes.io/mock-provisioner/fake-mount")
	node, err = testClient.CoreV1().Nodes().UpdateStatus(context.TODO(), node, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("error in patch volumeInUse status to nodes: %s", err)
	}
	// 带 grace period 删除 Pod，使其卡在 Terminating 状态。
	gracePeriod := int64(300)
	err = testClient.CoreV1().Pods(namespaceName).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	})
	if err != nil {
		t.Fatalf("error in deleting pod: %v", err)
	}

	// 确认 DeletionTimestamp 非空，即 Pod 处于 Terminating 状态。
	waitForPodDeletionTimestampToSet(tCtx, t, testClient, pod.Name, pod.Namespace)

	// 给节点打上 `out-of-service` taint。
	taint := v1.Taint{
		Key:    v1.TaintNodeOutOfService,
		Effect: v1.TaintEffectNoExecute,
	}
	node.Spec.Taints = append(node.Spec.Taints, taint)
	if _, err := testClient.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("error in patch oos taint to node: %v", err)
	}
	waitForNodeToBeTainted(tCtx, t, testClient, v1.TaintNodeOutOfService, nodeName)

	// 检查 Pod 是否被强制删除：只有节点带 out-of-service taint、处于 NotReady 且 Pod 为 Terminating 时才会强制删除。
	waitForMetric(tCtx, t, podgcmetrics.DeletingPodsTotal.WithLabelValues(namespaceName, podgcmetrics.PodGCReasonTerminatingOutOfService), 1, "terminating-pod-metric")
	// 检查卷是否被强制 detach。注意 metric 是累计值。
	waitForMetric(tCtx, t, metrics.ForceDetachMetricCounter.WithLabelValues(metrics.ForceDetachReasonOutOfService), 1, "detach-metric")

}

// 通过集成测试验证：即使 AttachDetach controller 漏掉了 Pod 删除事件，
// Desired State of World populator 仍会完成清理。
func TestPodDeletionWithDswp(t *testing.T) {
	// 没有运行 serviceaccount controller，需禁用 ServiceAccount admission plugin。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	namespaceName := "test-pod-deletion"
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-sandbox",
			Annotations: map[string]string{
				util.ControllerManagedAttachAnnotation: "true",
			},
		},
	}

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, ctrl, pvCtrl, podgcCtrl, informers := createAdClients(tCtx, t, server, defaultSyncPeriod, defaultTimerConfig)

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	pod := fakePodWithVol(namespaceName)

	if _, err := testClient.CoreV1().Nodes().Create(tCtx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to created node : %v", err)
	}

	// 启动 controller 循环。
	go informers.Core().V1().Nodes().Informer().Run(tCtx.Done())
	if _, err := testClient.CoreV1().Pods(ns.Name).Create(tCtx, pod, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create pod : %v", err)
	}

	podInformer := informers.Core().V1().Pods().Informer()
	go podInformer.Run(tCtx.Done())

	go informers.Core().V1().PersistentVolumeClaims().Informer().Run(tCtx.Done())
	go informers.Core().V1().PersistentVolumes().Informer().Run(tCtx.Done())
	go informers.Storage().V1().VolumeAttachments().Informer().Run(tCtx.Done())
	initCSIObjects(tCtx.Done(), informers)
	go ctrl.Run(tCtx)
	// 运行 pvCtrl，避免其创建时启动的 goroutine 泄漏。
	go pvCtrl.Run(tCtx)
	go podgcCtrl.Run(tCtx)

	waitToObservePods(t, podInformer, 1)
	podKey, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		t.Fatalf("MetaNamespaceKeyFunc failed with : %v", err)
	}

	podInformerObj, _, err := podInformer.GetStore().GetByKey(podKey)

	if err != nil {
		t.Fatalf("Pod not found in Pod Informer cache : %v", err)
	}

	waitForPodsInDSWP(t, ctrl.GetDesiredStateOfWorld())
	// 停止触发 Pod 事件。
	err = podInformer.GetStore().Delete(podInformerObj)
	if err != nil {
		t.Fatalf("Error deleting pod : %v", err)
	}

	waitToObservePods(t, podInformer, 0)
	// populator 循环每 1 分钟执行一次。
	waitForPodFuncInDSWP(t, ctrl.GetDesiredStateOfWorld(), 80*time.Second, "expected 0 pods in dsw after pod delete", 0)
}

func initCSIObjects(stopCh <-chan struct{}, informers clientgoinformers.SharedInformerFactory) {
	go informers.Storage().V1().CSINodes().Informer().Run(stopCh)
	go informers.Storage().V1().CSIDrivers().Informer().Run(stopCh)
}

func TestPodUpdateWithADC(t *testing.T) {
	// 没有运行 serviceaccount controller，需禁用 ServiceAccount admission plugin。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()
	namespaceName := "test-pod-update"

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-sandbox",
			Annotations: map[string]string{
				util.ControllerManagedAttachAnnotation: "true",
			},
		},
	}

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, ctrl, pvCtrl, podgcCtrl, informers := createAdClients(tCtx, t, server, defaultSyncPeriod, defaultTimerConfig)

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	pod := fakePodWithVol(namespaceName)
	podStopCh := make(chan struct{})
	defer close(podStopCh)

	if _, err := testClient.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to created node : %v", err)
	}

	go informers.Core().V1().Nodes().Informer().Run(podStopCh)

	if _, err := testClient.CoreV1().Pods(ns.Name).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create pod : %v", err)
	}

	podInformer := informers.Core().V1().Pods().Informer()
	go podInformer.Run(podStopCh)

	// 启动 controller 循环。
	go informers.Core().V1().PersistentVolumeClaims().Informer().Run(tCtx.Done())
	go informers.Core().V1().PersistentVolumes().Informer().Run(tCtx.Done())
	go informers.Storage().V1().VolumeAttachments().Informer().Run(tCtx.Done())
	initCSIObjects(tCtx.Done(), informers)
	go ctrl.Run(tCtx)
	// 运行 pvCtrl，避免其创建时启动的 goroutine 泄漏。
	go pvCtrl.Run(tCtx)
	go podgcCtrl.Run(tCtx)

	waitToObservePods(t, podInformer, 1)
	podKey, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		t.Fatalf("MetaNamespaceKeyFunc failed with : %v", err)
	}

	_, _, err = podInformer.GetStore().GetByKey(podKey)

	if err != nil {
		t.Fatalf("Pod not found in Pod Informer cache : %v", err)
	}

	waitForPodsInDSWP(t, ctrl.GetDesiredStateOfWorld())

	pod.Status.Phase = v1.PodSucceeded

	if _, err := testClient.CoreV1().Pods(ns.Name).UpdateStatus(context.TODO(), pod, metav1.UpdateOptions{}); err != nil {
		t.Errorf("Failed to update pod : %v", err)
	}

	waitForPodFuncInDSWP(t, ctrl.GetDesiredStateOfWorld(), 20*time.Second, "expected 0 pods in dsw after pod completion", 0)
}

// waitToObservePods 等待 podInformer 观察到 Pod。应在 RC manager 启动前调用，
// 避免 RC manager 不去接管已有 Pod、反而创建新的 Pod。
func waitToObservePods(t *testing.T, podInformer cache.SharedIndexInformer, podNum int) {
	if err := wait.Poll(100*time.Millisecond, 60*time.Second, func() (bool, error) {
		objects := podInformer.GetIndexer().List()
		if len(objects) == podNum {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
}

// 等待 Pod 出现在 Desired State of World 中。
func waitForPodsInDSWP(t *testing.T, dswp volumecache.DesiredStateOfWorld) {
	if err := wait.Poll(time.Millisecond*500, wait.ForeverTestTimeout, func() (bool, error) {
		pods := dswp.GetPodToAdd()
		if len(pods) > 0 {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("Pod not added to desired state of world : %v", err)
	}
}

// 等待 Pod 出现在 Desired State of World 中。
func waitForPodFuncInDSWP(t *testing.T, dswp volumecache.DesiredStateOfWorld, checkTimeout time.Duration, failMessage string, podCount int) {
	if err := wait.Poll(time.Millisecond*500, checkTimeout, func() (bool, error) {
		pods := dswp.GetPodToAdd()
		if len(pods) == podCount {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("%s but got error %v", failMessage, err)
	}
}

func createAdClients(ctx context.Context, t *testing.T, server *kubeapiservertesting.TestServer, syncPeriod time.Duration, timers attachdetach.TimerConfig) (*clientset.Clientset, attachdetach.AttachDetachController, *persistentvolume.PersistentVolumeController, *podgc.PodGCController, clientgoinformers.SharedInformerFactory) {
	config := restclient.CopyConfig(server.ClientConfig)
	config.QPS = 1000000
	config.Burst = 1000000
	resyncPeriod := 12 * time.Hour
	testClient := clientset.NewForConfigOrDie(server.ClientConfig)

	host := volumetest.NewFakeVolumeHost(t, "/tmp/fake", nil, nil)
	plugin := &volumetest.FakeVolumePlugin{
		PluginName:             provisionerPluginName,
		Host:                   host,
		Config:                 volume.VolumeConfig{},
		LastProvisionerOptions: volume.VolumeOptions{},
		NewAttacherCallCount:   0,
		NewDetacherCallCount:   0,
		Mounters:               nil,
		Unmounters:             nil,
		Attachers:              nil,
		Detachers:              nil,
	}
	plugins := []volume.VolumePlugin{plugin}
	informers := clientgoinformers.NewSharedInformerFactory(testClient, resyncPeriod)
	ctrl, err := attachdetach.NewAttachDetachController(
		ctx,
		testClient,
		informers.Core().V1().Pods(),
		informers.Core().V1().Nodes(),
		informers.Core().V1().PersistentVolumeClaims(),
		informers.Core().V1().PersistentVolumes(),
		informers.Storage().V1().CSINodes(),
		informers.Storage().V1().CSIDrivers(),
		informers.Storage().V1().VolumeAttachments(),
		plugins,
		nil, /* prober */
		false,
		5*time.Second,
		false,
		timers,
	)

	if err != nil {
		t.Fatalf("Error creating AttachDetach : %v", err)
	}

	// 创建 pv controller。
	controllerOptions := persistentvolumeoptions.NewPersistentVolumeControllerOptions()
	params := persistentvolume.ControllerParameters{
		KubeClient:                testClient,
		SyncPeriod:                controllerOptions.PVClaimBinderSyncPeriod,
		VolumePlugins:             plugins,
		VolumeInformer:            informers.Core().V1().PersistentVolumes(),
		ClaimInformer:             informers.Core().V1().PersistentVolumeClaims(),
		ClassInformer:             informers.Storage().V1().StorageClasses(),
		PodInformer:               informers.Core().V1().Pods(),
		NodeInformer:              informers.Core().V1().Nodes(),
		EnableDynamicProvisioning: false,
	}
	podgcCtrl := podgc.NewPodGCInternal(
		ctx,
		testClient,
		informers.Core().V1().Pods(),
		informers.Core().V1().Nodes(),
		0,
		500*time.Millisecond,
		time.Second,
	)
	pvCtrl, err := persistentvolume.NewController(ctx, params)
	if err != nil {
		t.Fatalf("Failed to create PV controller: %v", err)
	}
	return testClient, ctrl, pvCtrl, podgcCtrl, informers
}

// 通过集成测试验证：即使 AttachDetach controller 漏掉了 Pod 添加事件，
// Desired State of World populator 仍会把 Pod 加进来。
func TestPodAddedByDswp(t *testing.T) {
	// 没有运行 serviceaccount controller，需禁用 ServiceAccount admission plugin。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()
	namespaceName := "test-pod-deletion"

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-sandbox",
			Annotations: map[string]string{
				util.ControllerManagedAttachAnnotation: "true",
			},
		},
	}

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, ctrl, pvCtrl, podgcCtrl, informers := createAdClients(tCtx, t, server, defaultSyncPeriod, defaultTimerConfig)

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	pod := fakePodWithVol(namespaceName)
	podStopCh := make(chan struct{})

	if _, err := testClient.CoreV1().Nodes().Create(tCtx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to created node : %v", err)
	}

	go informers.Core().V1().Nodes().Informer().Run(podStopCh)

	if _, err := testClient.CoreV1().Pods(ns.Name).Create(tCtx, pod, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create pod : %v", err)
	}

	podInformer := informers.Core().V1().Pods().Informer()
	go podInformer.Run(podStopCh)

	// 启动 controller 循环。
	go informers.Core().V1().PersistentVolumeClaims().Informer().Run(tCtx.Done())
	go informers.Core().V1().PersistentVolumes().Informer().Run(tCtx.Done())
	go informers.Storage().V1().VolumeAttachments().Informer().Run(tCtx.Done())
	initCSIObjects(tCtx.Done(), informers)
	go ctrl.Run(tCtx)
	// 运行 pvCtrl，避免其创建时启动的 goroutine 泄漏。
	go pvCtrl.Run(tCtx)
	go podgcCtrl.Run(tCtx)

	waitToObservePods(t, podInformer, 1)
	podKey, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		t.Fatalf("MetaNamespaceKeyFunc failed with : %v", err)
	}

	_, _, err = podInformer.GetStore().GetByKey(podKey)

	if err != nil {
		t.Fatalf("Pod not found in Pod Informer cache : %v", err)
	}

	waitForPodsInDSWP(t, ctrl.GetDesiredStateOfWorld())

	// 停止触发 Pod 事件。
	close(podStopCh)
	podNew := pod.DeepCopy()
	newPodName := "newFakepod"
	podNew.SetName(newPodName)
	err = podInformer.GetStore().Add(podNew)
	if err != nil {
		t.Fatalf("Error adding pod : %v", err)
	}

	waitToObservePods(t, podInformer, 2)

	// findAndAddActivePods 循环每 3 分钟执行一次。
	waitForPodFuncInDSWP(t, ctrl.GetDesiredStateOfWorld(), 200*time.Second, "expected 2 pods in dsw after pod addition", 2)
}

func TestPVCBoundWithADC(t *testing.T) {
	// 没有运行 serviceaccount controller，需禁用 ServiceAccount admission plugin。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")

	namespaceName := "test-pod-deletion"

	testClient, ctrl, pvCtrl, podgcCtrl, informers := createAdClients(tCtx, t, server, defaultSyncPeriod, attachdetach.TimerConfig{
		ReconcilerLoopPeriod:                        100 * time.Millisecond,
		ReconcilerMaxWaitForUnmountDuration:         6 * time.Second,
		DesiredStateOfWorldPopulatorLoopSleepPeriod: 24 * time.Hour,
		// 用很大的时长禁用测试中的 DesiredStateOfWorldPopulator.findAndAddActivePods 循环。
		DesiredStateOfWorldPopulatorListPodsRetryDuration: 24 * time.Hour,
	})

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-sandbox",
			Annotations: map[string]string{
				util.ControllerManagedAttachAnnotation: "true",
			},
		},
	}
	if _, err := testClient.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to created node : %v", err)
	}

	// 带未绑定 PVC 的 Pod。
	pvcs := []*v1.PersistentVolumeClaim{}
	for i := range 3 {
		pod, pvc := fakePodWithPVC(fmt.Sprintf("fakepod-pvcnotbound-%d", i), fmt.Sprintf("fakepvc-%d", i), namespaceName)
		if _, err := testClient.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
			t.Errorf("Failed to create pod : %v", err)
		}
		if _, err := testClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
			t.Errorf("Failed to create pvc : %v", err)
		}
		pvcs = append(pvcs, pvc)
	}
	// 不带 PVC 的 Pod。
	podNew := fakePodWithVol(namespaceName)
	podNew.SetName("fakepod")
	if _, err := testClient.CoreV1().Pods(podNew.Namespace).Create(context.TODO(), podNew, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create pod : %v", err)
	}

	// 启动 controller 循环。
	informers.Start(tCtx.Done())
	informers.WaitForCacheSync(tCtx.Done())
	initCSIObjects(tCtx.Done(), informers)
	go ctrl.Run(tCtx)
	go pvCtrl.Run(tCtx)
	go podgcCtrl.Run(tCtx)

	waitToObservePods(t, informers.Core().V1().Pods().Informer(), 4)
	// 给 attachdetach controller 足够时间将 Pod 填入 DSWP。
	time.Sleep(10 * time.Second)
	waitForPodFuncInDSWP(t, ctrl.GetDesiredStateOfWorld(), 60*time.Second, "expected 1 pod in dsw", 1)
	for _, pvc := range pvcs {
		createPVForPVC(t, testClient, pvc)
	}
	waitForPodFuncInDSWP(t, ctrl.GetDesiredStateOfWorld(), 60*time.Second, "expected 4 pods in dsw after PVCs are bound", 4)
}

// createPVForPVC 创建 PV，由 pv controller 完成与 PVC 的绑定。
func createPVForPVC(t *testing.T, testClient *clientset.Clientset, pvc *v1.PersistentVolumeClaim) {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("fakepv-%s", pvc.Name),
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity:    pvc.Spec.Resources.Requests,
			AccessModes: pvc.Spec.AccessModes,
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/var/www/html",
				},
			},
			ClaimRef:         &v1.ObjectReference{Name: pvc.Name, Namespace: pvc.Namespace},
			StorageClassName: *pvc.Spec.StorageClassName,
		},
	}
	if _, err := testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create pv : %v", err)
	}
}

// waitForPodDeletionTimestampToSet 等待 Pod 被设置 DeletionTimestamp。
func waitForPodDeletionTimestampToSet(tCtx context.Context, t *testing.T, testingClient *clientset.Clientset, podName, podNamespace string) {
	if err := wait.PollUntilContextCancel(tCtx, 100*time.Millisecond, false, func(context.Context) (bool, error) {
		pod, err := testingClient.CoreV1().Pods(podNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if pod.DeletionTimestamp != nil {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("Failed to get deletionTimestamp of pod: %s, namespace: %s", podName, podNamespace)
	}
}

// waitForVolumeToBeAttached 等待卷被挂载到节点。
func waitForVolumeToBeAttached(ctx context.Context, t *testing.T, testingClient *clientset.Clientset, podName, nodeName string) {
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 120*time.Second, false, func(context.Context) (bool, error) {
		node, err := testingClient.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		if len(node.Status.VolumesAttached) >= 1 {
			return true, nil
		}
		if err != nil {
			t.Fatalf("Failed to get the node : %v", err)
		}
		return false, nil
	}); err != nil {
		t.Fatalf("Failed to attach volume to pod: %s for node: %s", podName, nodeName)
	}
}

// waitForNodeToBeTainted 等待节点被加上指定 taint。
func waitForNodeToBeTainted(ctx context.Context, t *testing.T, testingClient *clientset.Clientset, taintKey, nodeName string) {
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 60*time.Second, false, func(context.Context) (bool, error) {
		node, err := testingClient.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, taint := range node.Spec.Taints {
			if taint.Key == taintKey {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("Failed to add taint: %s to node: %s", taintKey, nodeName)
	}
}

func waitForMetric(ctx context.Context, t *testing.T, m basemetric.CounterMetric, expectedCount float64, identifier string) {
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 60*time.Second, false, func(ctx context.Context) (done bool, err error) {
		gotCount, err := metricstestutil.GetCounterMetricValue(m)
		if err != nil {
			t.Fatal(err, identifier)
		}
		t.Logf("expected metric count %g but got %g for %s", expectedCount, gotCount, identifier)
		// metric 是全局累计值，因此用 >= 只校验最小期望。
		if gotCount >= expectedCount {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("Failed to match the count of metrics to expect: %v", expectedCount)
	}
}
