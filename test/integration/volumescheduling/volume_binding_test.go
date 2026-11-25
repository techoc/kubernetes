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

package volumescheduling

// 本文件测试 VolumeScheduling 特性。

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller/volume/persistentvolume"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	testutil "k8s.io/kubernetes/test/integration/util"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

type testConfig struct {
	client   clientset.Interface
	ns       string
	stop     <-chan struct{}
	teardown func()
	testCtx  *testutil.TestContext
}

var (
	// 立即删除 API 对象
	deletePeriod = int64(0)
	deleteOption = metav1.DeleteOptions{GracePeriodSeconds: &deletePeriod}

	modeWait      = storagev1.VolumeBindingWaitForFirstConsumer
	modeImmediate = storagev1.VolumeBindingImmediate

	classWait         = "wait"
	classImmediate    = "immediate"
	classDynamic      = "dynamic"
	classTopoMismatch = "topomismatch"

	sharedClasses = map[string]*storagev1.StorageClass{
		classImmediate: makeStorageClass(classImmediate, &modeImmediate),
		classWait:      makeStorageClass(classWait, &modeWait),
	}
)

const (
	node1                 = "node-1"
	node2                 = "node-2"
	podLimit              = 50
	volsPerPod            = 3
	nodeAffinityLabelKey  = "kubernetes.io/hostname"
	provisionerPluginName = "mock-provisioner.kubernetes.io"
)

type testPV struct {
	name        string
	scName      string
	preboundPVC string
	node        string
}

type testPVC struct {
	name       string
	scName     string
	preboundPV string
}

func TestVolumeBinding(t *testing.T) {
	config := setupCluster(t, "volume-scheduling-", 2, 0, 0)
	defer config.teardown()

	cases := map[string]struct {
		pod  *v1.Pod
		pvs  []*testPV
		pvcs []*testPVC
		// 创建这些对象，但最终不应被绑定
		unboundPvcs []*testPVC
		unboundPvs  []*testPV
		shouldFail  bool
	}{
		"immediate can bind": {
			pod:  makePod("pod-i-canbind", config.ns, []string{"pvc-i-canbind"}),
			pvs:  []*testPV{{"pv-i-canbind", classImmediate, "", node1}},
			pvcs: []*testPVC{{"pvc-i-canbind", classImmediate, ""}},
		},
		"immediate cannot bind": {
			pod:         makePod("pod-i-cannotbind", config.ns, []string{"pvc-i-cannotbind"}),
			unboundPvcs: []*testPVC{{"pvc-i-cannotbind", classImmediate, ""}},
			shouldFail:  true,
		},
		"immediate pvc prebound": {
			pod:  makePod("pod-i-pvc-prebound", config.ns, []string{"pvc-i-prebound"}),
			pvs:  []*testPV{{"pv-i-pvc-prebound", classImmediate, "", node1}},
			pvcs: []*testPVC{{"pvc-i-prebound", classImmediate, "pv-i-pvc-prebound"}},
		},
		"immediate pv prebound": {
			pod:  makePod("pod-i-pv-prebound", config.ns, []string{"pvc-i-pv-prebound"}),
			pvs:  []*testPV{{"pv-i-prebound", classImmediate, "pvc-i-pv-prebound", node1}},
			pvcs: []*testPVC{{"pvc-i-pv-prebound", classImmediate, ""}},
		},
		"wait can bind": {
			pod:  makePod("pod-w-canbind", config.ns, []string{"pvc-w-canbind"}),
			pvs:  []*testPV{{"pv-w-canbind", classWait, "", node1}},
			pvcs: []*testPVC{{"pvc-w-canbind", classWait, ""}},
		},
		"wait cannot bind": {
			pod:         makePod("pod-w-cannotbind", config.ns, []string{"pvc-w-cannotbind"}),
			unboundPvcs: []*testPVC{{"pvc-w-cannotbind", classWait, ""}},
			shouldFail:  true,
		},
		"wait pvc prebound": {
			pod:  makePod("pod-w-pvc-prebound", config.ns, []string{"pvc-w-prebound"}),
			pvs:  []*testPV{{"pv-w-pvc-prebound", classWait, "", node1}},
			pvcs: []*testPVC{{"pvc-w-prebound", classWait, "pv-w-pvc-prebound"}},
		},
		"wait pv prebound": {
			pod:  makePod("pod-w-pv-prebound", config.ns, []string{"pvc-w-pv-prebound"}),
			pvs:  []*testPV{{"pv-w-prebound", classWait, "pvc-w-pv-prebound", node1}},
			pvcs: []*testPVC{{"pvc-w-pv-prebound", classWait, ""}},
		},
		"wait can bind two": {
			pod: makePod("pod-w-canbind-2", config.ns, []string{"pvc-w-canbind-2", "pvc-w-canbind-3"}),
			pvs: []*testPV{
				{"pv-w-canbind-2", classWait, "", node2},
				{"pv-w-canbind-3", classWait, "", node2},
			},
			pvcs: []*testPVC{
				{"pvc-w-canbind-2", classWait, ""},
				{"pvc-w-canbind-3", classWait, ""},
			},
			unboundPvs: []*testPV{
				{"pv-w-canbind-5", classWait, "", node1},
			},
		},
		"wait cannot bind two": {
			pod: makePod("pod-w-cannotbind-2", config.ns, []string{"pvc-w-cannotbind-1", "pvc-w-cannotbind-2"}),
			unboundPvcs: []*testPVC{
				{"pvc-w-cannotbind-1", classWait, ""},
				{"pvc-w-cannotbind-2", classWait, ""},
			},
			unboundPvs: []*testPV{
				{"pv-w-cannotbind-1", classWait, "", node2},
				{"pv-w-cannotbind-2", classWait, "", node1},
			},
			shouldFail: true,
		},
		"mix immediate and wait": {
			pod: makePod("pod-mix-bound", config.ns, []string{"pvc-w-canbind-4", "pvc-i-canbind-2"}),
			pvs: []*testPV{
				{"pv-w-canbind-4", classWait, "", node1},
				{"pv-i-canbind-2", classImmediate, "", node1},
			},
			pvcs: []*testPVC{
				{"pvc-w-canbind-4", classWait, ""},
				{"pvc-i-canbind-2", classImmediate, ""},
			},
		},
	}

	for name, test := range cases {
		klog.Infof("Running test %v", name)

		// 创建两个 StorageClass
		suffix := rand.String(4)
		classes := map[string]*storagev1.StorageClass{}
		classes[classImmediate] = makeStorageClass(fmt.Sprintf("immediate-%v", suffix), &modeImmediate)
		classes[classWait] = makeStorageClass(fmt.Sprintf("wait-%v", suffix), &modeWait)
		for _, sc := range classes {
			if _, err := config.client.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create StorageClass %q: %v", sc.Name, err)
			}
		}

		// 创建 PV
		for _, pvConfig := range test.pvs {
			pv := makePV(pvConfig.name, classes[pvConfig.scName].Name, pvConfig.preboundPVC, config.ns, pvConfig.node)
			if _, err := config.client.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolume %q: %v", pv.Name, err)
			}
		}

		for _, pvConfig := range test.unboundPvs {
			pv := makePV(pvConfig.name, classes[pvConfig.scName].Name, pvConfig.preboundPVC, config.ns, pvConfig.node)
			if _, err := config.client.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolume %q: %v", pv.Name, err)
			}
		}

		// 等待 PV 变为可用状态，以避免 PV 控制器中的竞态条件
		// https://github.com/kubernetes/kubernetes/issues/85320
		for _, pvConfig := range test.pvs {
			if err := waitForPVPhase(config.client, pvConfig.name, v1.VolumeAvailable); err != nil {
				t.Fatalf("PersistentVolume %q failed to become available: %v", pvConfig.name, err)
			}
		}

		for _, pvConfig := range test.unboundPvs {
			if err := waitForPVPhase(config.client, pvConfig.name, v1.VolumeAvailable); err != nil {
				t.Fatalf("PersistentVolume %q failed to become available: %v", pvConfig.name, err)
			}
		}

		// 创建 PVC
		for _, pvcConfig := range test.pvcs {
			pvc := makePVC(pvcConfig.name, config.ns, &classes[pvcConfig.scName].Name, pvcConfig.preboundPV)
			if _, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
			}
		}
		for _, pvcConfig := range test.unboundPvcs {
			pvc := makePVC(pvcConfig.name, config.ns, &classes[pvcConfig.scName].Name, pvcConfig.preboundPV)
			if _, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
			}
		}

		// 创建 Pod
		if _, err := config.client.CoreV1().Pods(config.ns).Create(context.TODO(), test.pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Pod %q: %v", test.pod.Name, err)
		}
		if test.shouldFail {
			if err := waitForPodUnschedulable(config.client, test.pod); err != nil {
				t.Errorf("Pod %q was not unschedulable: %v", test.pod.Name, err)
			}
		} else {
			if err := waitForPodToSchedule(config.client, test.pod); err != nil {
				t.Errorf("Failed to schedule Pod %q: %v", test.pod.Name, err)
			}
		}

		// 验证 PVC/PV 绑定
		for _, pvc := range test.pvcs {
			validatePVCPhase(t, config.client, pvc.name, config.ns, v1.ClaimBound, false)
		}
		for _, pvc := range test.unboundPvcs {
			validatePVCPhase(t, config.client, pvc.name, config.ns, v1.ClaimPending, false)
		}
		for _, pv := range test.pvs {
			validatePVPhase(t, config.client, pv.name, v1.VolumeBound)
		}
		for _, pv := range test.unboundPvs {
			validatePVPhase(t, config.client, pv.name, v1.VolumeAvailable)
		}

		// 强制删除对象，但它们可能不会立即被移除
		deleteTestObjects(config.client, config.ns, deleteOption)
	}
}

// TestVolumeBindingRescheduling 测试调度器会在需要时重试调度。
func TestVolumeBindingRescheduling(t *testing.T) {
	config := setupCluster(t, "volume-scheduling-", 2, 0, 0)
	defer config.teardown()

	storageClassName := "local-storage"

	cases := map[string]struct {
		pod        *v1.Pod
		pvcs       []*testPVC
		pvs        []*testPV
		trigger    func(config *testConfig)
		shouldFail bool
	}{
		"reschedule on WaitForFirstConsumer dynamic storage class add": {
			pod: makePod("pod-reschedule-onclassadd-dynamic", config.ns, []string{"pvc-reschedule-onclassadd-dynamic"}),
			pvcs: []*testPVC{
				{"pvc-reschedule-onclassadd-dynamic", "", ""},
			},
			trigger: func(config *testConfig) {
				sc := makeDynamicProvisionerStorageClass(storageClassName, &modeWait, nil)
				if _, err := config.client.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{}); err != nil {
					t.Fatalf("Failed to create StorageClass %q: %v", sc.Name, err)
				}
			},
			shouldFail: false,
		},
		"reschedule on WaitForFirstConsumer static storage class add": {
			pod: makePod("pod-reschedule-onclassadd-static", config.ns, []string{"pvc-reschedule-onclassadd-static"}),
			pvcs: []*testPVC{
				{"pvc-reschedule-onclassadd-static", "", ""},
			},
			trigger: func(config *testConfig) {
				sc := makeStorageClass(storageClassName, &modeWait)
				if _, err := config.client.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{}); err != nil {
					t.Fatalf("Failed to create StorageClass %q: %v", sc.Name, err)
				}
				// 为该 StorageClass 创建 PV，以模拟静态制备器的行为。
				pv := makePV("pv-reschedule-onclassadd-static", storageClassName, "", "", node1)
				if pv, err := config.client.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
					t.Fatalf("Failed to create PersistentVolume %q: %v", pv.Name, err)
				}
			},
			shouldFail: false,
		},
		"reschedule on delay binding PVC add": {
			pod: makePod("pod-reschedule-onpvcadd", config.ns, []string{"pvc-reschedule-onpvcadd"}),
			pvs: []*testPV{
				{
					name:   "pv-reschedule-onpvcadd",
					scName: classWait,
					node:   node1,
				},
			},
			trigger: func(config *testConfig) {
				pvc := makePVC("pvc-reschedule-onpvcadd", config.ns, &classWait, "")
				if _, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
					t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
				}
			},
			shouldFail: false,
		},
	}

	for name, test := range cases {
		klog.Infof("Running test %v", name)

		if test.pod == nil {
			t.Fatal("pod is required for this test")
		}

		// 创建未绑定的 PVC
		for _, pvcConfig := range test.pvcs {
			pvc := makePVC(pvcConfig.name, config.ns, &storageClassName, "")
			if _, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
			}
		}

		// 创建 PV
		for _, pvConfig := range test.pvs {
			pv := makePV(pvConfig.name, sharedClasses[pvConfig.scName].Name, pvConfig.preboundPVC, config.ns, pvConfig.node)
			if _, err := config.client.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolume %q: %v", pv.Name, err)
			}
		}

		// 创建 Pod
		if _, err := config.client.CoreV1().Pods(config.ns).Create(context.TODO(), test.pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Pod %q: %v", test.pod.Name, err)
		}

		// 等待 Pod 变为不可调度状态。
		klog.Infof("Waiting for pod is unschedulable")
		if err := waitForPodUnschedulable(config.client, test.pod); err != nil {
			t.Errorf("Failed as Pod %s was not unschedulable: %v", test.pod.Name, err)
		}

		// 触发
		test.trigger(config)

		// 等待 Pod 被调度或变为不可调度状态。
		if !test.shouldFail {
			klog.Infof("Waiting for pod is scheduled")
			if err := waitForPodToSchedule(config.client, test.pod); err != nil {
				t.Errorf("Failed to schedule Pod %q: %v", test.pod.Name, err)
			}
		} else {
			klog.Infof("Waiting for pod is unschedulable")
			if err := waitForPodUnschedulable(config.client, test.pod); err != nil {
				t.Errorf("Failed as Pod %s was not unschedulable: %v", test.pod.Name, err)
			}
		}

		// 强制删除对象，但它们可能不会立即被移除
		deleteTestObjects(config.client, config.ns, deleteOption)
	}
}

// TestVolumeBindingStress 创建 <podLimit> 个 Pod，每个 Pod 带有 <volsPerPod> 个未绑定或预绑定的 PVC。
// PV 是预先创建好的。
func TestVolumeBindingStress(t *testing.T) {
	testVolumeBindingStress(t, 0, false, 0)
}

// 与 TestVolumeBindingStress 类似，但带有调度器重新同步。在真实集群中，
// 由于各种事件（如 service/node 更新事件），调度器会频繁地
// 重试调度失败的 Pod。这有助于检测可能的竞态条件。
func TestVolumeBindingStressWithSchedulerResync(t *testing.T) {
	testVolumeBindingStress(t, time.Second, false, 0)
}

// 与 TestVolumeBindingStress 类似，但使用快速的动态制备
func TestVolumeBindingDynamicStressFast(t *testing.T) {
	testVolumeBindingStress(t, 0, true, 0)
}

// 与 TestVolumeBindingStress 类似，但使用缓慢的动态制备
func TestVolumeBindingDynamicStressSlow(t *testing.T) {
	testVolumeBindingStress(t, 0, true, 10)
}

func testVolumeBindingStress(t *testing.T, schedulerResyncPeriod time.Duration, dynamic bool, provisionDelaySeconds int) {
	config := setupCluster(t, "volume-binding-stress-", 1, schedulerResyncPeriod, provisionDelaySeconds)
	defer config.teardown()

	// 将最大卷数限制设为测试将创建的 PVC 数量
	// TODO: 当最大卷数限制支持通过 storageclass 设置时，移除此处设置
	t.Setenv("KUBE_MAX_PD_VOLS", fmt.Sprintf("%v", podLimit*volsPerPod))

	scName := &classWait
	if dynamic {
		scName = &classDynamic
		sc := makeDynamicProvisionerStorageClass(*scName, &modeWait, nil)
		if _, err := config.client.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create StorageClass %q: %v", sc.Name, err)
		}
	}

	klog.Infof("Start creating PVs and PVCs")
	// 为所有 Pod 创建足够的 PV 和 PVC
	podVolumesCount := podLimit * volsPerPod
	pvs := make([]*v1.PersistentVolume, podVolumesCount)
	pvcs := make([]*v1.PersistentVolumeClaim, podVolumesCount)
	workqueue.ParallelizeUntil(context.TODO(), 16, podVolumesCount, func(i int) {
		var (
			pv      *v1.PersistentVolume
			pvc     *v1.PersistentVolumeClaim
			pvName  = fmt.Sprintf("pv-stress-%v", i)
			pvcName = fmt.Sprintf("pvc-stress-%v", i)
		)
		// 动态制备测试不创建 PV
		if !dynamic {
			if rand.Int()%2 == 0 {
				// 静态未绑定 PV
				pv = makePV(pvName, *scName, "", "", node1)
			} else {
				// 静态预绑定 PV
				pv = makePV(pvName, classImmediate, pvcName, config.ns, node1)
			}
			if pv, err := config.client.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolume %q: %v", pv.Name, err)
			}
			pvs[i] = pv
		}
		if pv != nil && pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == pvcName {
			pvc = makePVC(pvcName, config.ns, &classImmediate, pv.Name)
		} else {
			pvc = makePVC(pvcName, config.ns, scName, "")
		}
		if pvc, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
		}
		pvcs[i] = pvc
	})

	klog.Infof("Start creating Pods")
	pods := make([]*v1.Pod, podLimit)
	workqueue.ParallelizeUntil(context.TODO(), 16, podLimit, func(i int) {
		// 生成该 Pod 所用全部 PVC 的名称列表
		podPvcs := []string{}
		for j := i * volsPerPod; j < (i+1)*volsPerPod; j++ {
			podPvcs = append(podPvcs, pvcs[j].Name)
		}

		pod := makePod(fmt.Sprintf("pod%03d", i), config.ns, podPvcs)
		if pod, err := config.client.CoreV1().Pods(config.ns).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
		}
		pods[i] = pod
	})

	klog.Infof("Start validating pod scheduled")
	// 验证 Pod 已被调度
	workqueue.ParallelizeUntil(context.TODO(), 16, len(pods), func(i int) {
		pod := pods[i]
		// 压力测试使用更长的超时时间，因为出现
		// PV 同步错误的概率更高
		if err := waitForPodToScheduleWithTimeout(config.client, pod, 2*time.Minute); err != nil {
			t.Errorf("Failed to schedule Pod %q: %v", pod.Name, err)
		}
	})

	klog.Infof("Start validating PVCs scheduled")
	// 验证 PVC/PV 绑定
	workqueue.ParallelizeUntil(context.TODO(), 16, len(pvcs), func(i int) {
		validatePVCPhase(t, config.client, pvcs[i].Name, config.ns, v1.ClaimBound, dynamic)
	})

	// 动态制备测试不验证 PV
	if !dynamic {
		klog.Infof("Start validating PVs scheduled")
		workqueue.ParallelizeUntil(context.TODO(), 16, len(pvs), func(i int) {
			validatePVPhase(t, config.client, pvs[i].Name, v1.VolumeBound)
		})
	}
}

func testVolumeBindingWithAffinity(t *testing.T, anti bool, numNodes, numPods, numPVsFirstNode int) {
	config := setupCluster(t, "volume-pod-affinity-", numNodes, 0, 0)
	defer config.teardown()

	pods := []*v1.Pod{}
	pvcs := []*v1.PersistentVolumeClaim{}

	// 为第一个节点创建 PV
	for i := range numPVsFirstNode {
		pv := makePV(fmt.Sprintf("pv-node1-%v", i), classWait, "", "", node1)
		if pv, err := config.client.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create PersistentVolume %q: %v", pv.Name, err)
		}
	}

	// 为其余节点各创建 1 个 PV
	for i := 2; i <= numNodes; i++ {
		pv := makePV(fmt.Sprintf("pv-node%v-0", i), classWait, "", "", fmt.Sprintf("node-%v", i))
		if pv, err := config.client.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create PersistentVolume %q: %v", pv.Name, err)
		}
	}

	// 创建 Pod
	for i := range numPods {
		// 每个 Pod 创建一个 PVC
		pvc := makePVC(fmt.Sprintf("pvc-%v", i), config.ns, &classWait, "")
		if pvc, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
		}
		pvcs = append(pvcs, pvc)

		// 创建带有 Pod affinity 的 Pod
		pod := makePod(fmt.Sprintf("pod%03d", i), config.ns, []string{pvc.Name})
		pod.Spec.Affinity = &v1.Affinity{}
		affinityTerms := []v1.PodAffinityTerm{
			{
				LabelSelector: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "app",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"volume-binding-test"},
						},
					},
				},
				TopologyKey: nodeAffinityLabelKey,
			},
		}
		if anti {
			pod.Spec.Affinity.PodAntiAffinity = &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: affinityTerms,
			}
		} else {
			pod.Spec.Affinity.PodAffinity = &v1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: affinityTerms,
			}
		}

		if pod, err := config.client.CoreV1().Pods(config.ns).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
		}
		pods = append(pods, pod)
	}

	// 验证 Pod 已被调度
	scheduledNodes := sets.NewString()
	for _, pod := range pods {
		if err := waitForPodToSchedule(config.client, pod); err != nil {
			t.Errorf("Failed to schedule Pod %q: %v", pod.Name, err)
		} else {
			// 记录 Pod 被调度到的所有节点
			pod, err = config.client.CoreV1().Pods(config.ns).Get(context.TODO(), pod.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("Failed to get Pod %q: %v", pod.Name, err)
			}
			if pod.Spec.NodeName == "" {
				t.Fatalf("Pod %q node name unset after scheduling", pod.Name)
			}
			scheduledNodes.Insert(pod.Spec.NodeName)
		}
	}

	// 验证 affinity 策略
	if anti {
		// Pod 应该被分散到不同的节点上
		if scheduledNodes.Len() != numPods {
			t.Errorf("Pods were scheduled across %v nodes instead of %v", scheduledNodes.Len(), numPods)
		}
	} else {
		// Pod 应该都被调度到同一个节点上
		if scheduledNodes.Len() != 1 {
			t.Errorf("Pods were scheduled across %v nodes instead of %v", scheduledNodes.Len(), 1)
		}
	}

	// 验证 PVC 绑定
	for _, pvc := range pvcs {
		validatePVCPhase(t, config.client, pvc.Name, config.ns, v1.ClaimBound, false)
	}
}

func TestVolumeBindingWithAntiAffinity(t *testing.T) {
	numNodes := 10
	// 创建与节点数量相同的 Pod
	numPods := numNodes
	// 在 node1 上创建更多 PV，以提高选中 node1 的概率
	numPVsFirstNode := 10 * numNodes

	testVolumeBindingWithAffinity(t, true, numNodes, numPods, numPVsFirstNode)
}

func TestVolumeBindingWithAffinity(t *testing.T) {
	numPods := 10
	// 创建更多节点，以提高选中非 node1 节点上 PV 的概率
	numNodes := 10 * numPods
	// 在第一个节点上创建 numPods 个 PV
	numPVsFirstNode := numPods

	testVolumeBindingWithAffinity(t, true, numNodes, numPods, numPVsFirstNode)
}

func TestPVAffinityConflict(t *testing.T) {
	config := setupCluster(t, "volume-scheduling-", 3, 0, 0)
	defer config.teardown()

	pv := makePV("local-pv", classImmediate, "", "", node1)
	pvc := makePVC("local-pvc", config.ns, &classImmediate, "")

	// 创建 PV
	if _, err := config.client.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create PersistentVolume %q: %v", pv.Name, err)
	}

	// 创建 PVC
	if _, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
	}

	// 等待 PVC 绑定
	if err := waitForPVCBound(config.client, pvc); err != nil {
		t.Fatalf("PVC %q failed to bind: %v", pvc.Name, err)
	}

	nodeMarkers := []interface{}{
		markNodeAffinity,
		markNodeSelector,
	}
	for i := range nodeMarkers {
		podName := "local-pod-" + strconv.Itoa(i+1)
		pod := makePod(podName, config.ns, []string{"local-pvc"})
		nodeMarkers[i].(func(*v1.Pod, string))(pod, "node-2")
		// 创建 Pod
		if _, err := config.client.CoreV1().Pods(config.ns).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
		}
		// 给调度器一些时间尝试调度 Pod
		if err := waitForPodUnschedulable(config.client, pod); err != nil {
			t.Errorf("Failed as Pod %s was not unschedulable: %v", pod.Name, err)
		}
		// 检查 Pod 的状态条件
		p, err := config.client.CoreV1().Pods(config.ns).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to access Pod %s status: %v", podName, err)
		}
		if strings.Compare(string(p.Status.Phase), "Pending") != 0 {
			t.Fatalf("Failed as Pod %s was in: %s state and not in expected: Pending state", podName, p.Status.Phase)
		}
		if strings.Compare(p.Status.Conditions[0].Reason, "Unschedulable") != 0 {
			t.Fatalf("Failed as Pod %s reason was: %s but expected: Unschedulable", podName, p.Status.Conditions[0].Reason)
		}
		if !strings.Contains(p.Status.Conditions[0].Message, "node(s) didn't match Pod's node affinity") {
			t.Fatalf("Failed as Pod's %s failure message does not contain expected message: node(s) didn't match Pod's node affinity. Got message %q", podName, p.Status.Conditions[0].Message)
		}
		// 删除测试 Pod
		if err := config.client.CoreV1().Pods(config.ns).Delete(context.TODO(), podName, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("Failed to delete Pod %s: %v", podName, err)
		}
	}
}

func TestVolumeProvision(t *testing.T) {
	config := setupCluster(t, "volume-scheduling", 1, 0, 0)
	defer config.teardown()

	type testcaseType struct {
		pod             *v1.Pod
		pvs             []*testPV
		boundPvcs       []*testPVC
		provisionedPvcs []*testPVC
		// 创建这些对象，但最终不应被绑定
		unboundPvcs []*testPVC
		shouldFail  bool
	}

	cases := map[string]testcaseType{
		"wait provisioned": {
			pod:             makePod("pod-pvc-canprovision", config.ns, []string{"pvc-canprovision"}),
			provisionedPvcs: []*testPVC{{"pvc-canprovision", classWait, ""}},
		},
		"topolgy unsatisfied": {
			pod:         makePod("pod-pvc-topomismatch", config.ns, []string{"pvc-topomismatch"}),
			unboundPvcs: []*testPVC{{"pvc-topomismatch", classTopoMismatch, ""}},
			shouldFail:  true,
		},
		"wait one bound, one provisioned": {
			pod:             makePod("pod-pvc-canbind-or-provision", config.ns, []string{"pvc-w-canbind", "pvc-canprovision"}),
			pvs:             []*testPV{{"pv-w-canbind", classWait, "", node1}},
			boundPvcs:       []*testPVC{{"pvc-w-canbind", classWait, ""}},
			provisionedPvcs: []*testPVC{{"pvc-canprovision", classWait, ""}},
		},
		"one immediate pv prebound, one wait provisioned": {
			pod:             makePod("pod-i-pv-prebound-w-provisioned", config.ns, []string{"pvc-i-pv-prebound", "pvc-canprovision"}),
			pvs:             []*testPV{{"pv-i-prebound", classImmediate, "pvc-i-pv-prebound", node1}},
			boundPvcs:       []*testPVC{{"pvc-i-pv-prebound", classImmediate, ""}},
			provisionedPvcs: []*testPVC{{"pvc-canprovision", classWait, ""}},
		},
		"wait one pv prebound, one provisioned": {
			pod:             makePod("pod-w-pv-prebound-w-provisioned", config.ns, []string{"pvc-w-pv-prebound", "pvc-canprovision"}),
			pvs:             []*testPV{{"pv-w-prebound", classWait, "pvc-w-pv-prebound", node1}},
			boundPvcs:       []*testPVC{{"pvc-w-pv-prebound", classWait, ""}},
			provisionedPvcs: []*testPVC{{"pvc-canprovision", classWait, ""}},
		},
		"immediate provisioned by controller": {
			pod: makePod("pod-i-unbound", config.ns, []string{"pvc-controller-provisioned"}),
			// 即时绑定模式的 PVC 预期由控制器制备，
			// 这里将其视为"已绑定"，因为它应处于与已绑定 PVC
			// 相同的状态，即处于 Bound 状态且没有 selectedNode annotation。
			boundPvcs: []*testPVC{{"pvc-controller-provisioned", classImmediate, ""}},
		},
	}

	run := func(t *testing.T, test testcaseType) {
		t.Log("Creating StorageClass")
		suffix := rand.String(4)
		classes := map[string]*storagev1.StorageClass{}
		classes[classImmediate] = makeDynamicProvisionerStorageClass(fmt.Sprintf("immediate-%v", suffix), &modeImmediate, nil)
		classes[classWait] = makeDynamicProvisionerStorageClass(fmt.Sprintf("wait-%v", suffix), &modeWait, nil)
		topo := []v1.TopologySelectorTerm{
			{
				MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
					{
						Key:    nodeAffinityLabelKey,
						Values: []string{node2},
					},
				},
			},
		}
		classes[classTopoMismatch] = makeDynamicProvisionerStorageClass(fmt.Sprintf("topomismatch-%v", suffix), &modeWait, topo)
		for _, sc := range classes {
			if _, err := config.client.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create StorageClass %q: %v", sc.Name, err)
			}
		}

		t.Log("Creating PVs")
		for _, pvConfig := range test.pvs {
			pv := makePV(pvConfig.name, classes[pvConfig.scName].Name, pvConfig.preboundPVC, config.ns, pvConfig.node)
			if _, err := config.client.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolume %q: %v", pv.Name, err)
			}
			if err := waitForPVPhase(config.client, pvConfig.name, v1.VolumeAvailable); err != nil {
				t.Fatalf("PersistentVolume %q failed to become available: %v", pvConfig.name, err)
			}
		}

		t.Log("Creating PVCs")
		for _, pvcConfig := range test.boundPvcs {
			pvc := makePVC(pvcConfig.name, config.ns, &classes[pvcConfig.scName].Name, pvcConfig.preboundPV)
			if _, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
			}
		}

		t.Log("Creating unbound PVCs")
		for _, pvcConfig := range test.unboundPvcs {
			pvc := makePVC(pvcConfig.name, config.ns, &classes[pvcConfig.scName].Name, pvcConfig.preboundPV)
			if _, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
			}
		}

		t.Log("Creating unbound PVCs which should be dynamically provisioned")
		for _, pvcConfig := range test.provisionedPvcs {
			pvc := makePVC(pvcConfig.name, config.ns, &classes[pvcConfig.scName].Name, pvcConfig.preboundPV)
			if _, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
			}
		}

		t.Log("Creating the pod to schedule")
		if _, err := config.client.CoreV1().Pods(config.ns).Create(context.TODO(), test.pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Pod %q: %v", test.pod.Name, err)
		}
		if test.shouldFail {
			if err := waitForPodUnschedulable(config.client, test.pod); err != nil {
				t.Errorf("Pod %q was not unschedulable: %v", test.pod.Name, err)
			}
		} else {
			if err := waitForPodToSchedule(config.client, test.pod); err != nil {
				t.Errorf("Failed to schedule Pod %q: %v", test.pod.Name, err)
			}
		}

		t.Log("Validating PVC/PV binding")
		for _, pvc := range test.boundPvcs {
			validatePVCPhase(t, config.client, pvc.name, config.ns, v1.ClaimBound, false)
		}
		for _, pvc := range test.unboundPvcs {
			validatePVCPhase(t, config.client, pvc.name, config.ns, v1.ClaimPending, false)
		}
		for _, pvc := range test.provisionedPvcs {
			validatePVCPhase(t, config.client, pvc.name, config.ns, v1.ClaimBound, true)
		}
		for _, pv := range test.pvs {
			validatePVPhase(t, config.client, pv.name, v1.VolumeBound)
		}

		// 强制删除对象，但它们可能不会立即被移除
		t.Log("Deleting test objects")
		deleteTestObjects(config.client, config.ns, deleteOption)
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) { run(t, test) })
	}
}

// TestCapacity 覆盖涉及 CSIStorageCapacity 对象的不同场景。
func TestCapacity(t *testing.T) {
	config := setupCluster(t, "volume-scheduling", 1, 0, 0)
	defer config.teardown()

	type testcaseType struct {
		pod               *v1.Pod
		pvcs              []*testPVC
		haveCapacity      bool
		capacitySupported bool
	}

	cases := map[string]testcaseType{
		"baseline": {
			pod:  makePod("pod-pvc-canprovision", config.ns, []string{"pvc-canprovision"}),
			pvcs: []*testPVC{{"pvc-canprovision", classWait, ""}},
		},
		"out of space": {
			pod:               makePod("pod-pvc-canprovision", config.ns, []string{"pvc-canprovision"}),
			pvcs:              []*testPVC{{"pvc-canprovision", classWait, ""}},
			capacitySupported: true,
		},
		"with space": {
			pod:               makePod("pod-pvc-canprovision", config.ns, []string{"pvc-canprovision"}),
			pvcs:              []*testPVC{{"pvc-canprovision", classWait, ""}},
			capacitySupported: true,
			haveCapacity:      true,
		},
		"ignored": {
			pod:          makePod("pod-pvc-canprovision", config.ns, []string{"pvc-canprovision"}),
			pvcs:         []*testPVC{{"pvc-canprovision", classWait, ""}},
			haveCapacity: true,
		},
	}

	run := func(t *testing.T, test testcaseType) {
		// 创建 StorageClass
		suffix := rand.String(4)
		classes := map[string]*storagev1.StorageClass{}
		classes[classImmediate] = makeDynamicProvisionerStorageClass(fmt.Sprintf("immediate-%v", suffix), &modeImmediate, nil)
		classes[classWait] = makeDynamicProvisionerStorageClass(fmt.Sprintf("wait-%v", suffix), &modeWait, nil)
		topo := []v1.TopologySelectorTerm{
			{
				MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
					{
						Key:    nodeAffinityLabelKey,
						Values: []string{node2},
					},
				},
			},
		}
		classes[classTopoMismatch] = makeDynamicProvisionerStorageClass(fmt.Sprintf("topomismatch-%v", suffix), &modeWait, topo)
		for _, sc := range classes {
			if _, err := config.client.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create StorageClass %q: %v", sc.Name, err)
			}
		}

		// 该 provisioner 实际上不是 CSI driver，
		// 但在这里无关紧要。
		if test.capacitySupported {
			if _, err := config.client.StorageV1().CSIDrivers().Create(context.TODO(),
				&storagev1.CSIDriver{
					ObjectMeta: metav1.ObjectMeta{
						Name: provisionerPluginName,
					},
					Spec: storagev1.CSIDriverSpec{
						StorageCapacity: &test.capacitySupported,
					},
				},
				metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create CSIDriver: %v", err)
			}

			// kube-scheduler 可能需要一些时间才能获取到 CSIDriver 对象。
			// 如果缺少该对象，调度将不会考虑容量因素，
			// 这不是我们要测试的场景。
			time.Sleep(5 * time.Second)
		}

		// 创建 CSIStorageCapacity
		if test.haveCapacity {
			if _, err := config.client.StorageV1().CSIStorageCapacities("default").Create(context.TODO(),
				&storagev1.CSIStorageCapacity{
					ObjectMeta: metav1.ObjectMeta{
						GenerateName: "foo-",
					},
					StorageClassName: classes[classWait].Name,
					NodeTopology:     &metav1.LabelSelector{},
					// 大于 makePVC 中使用的 5Gi。
					Capacity: resource.NewQuantity(6*1024*1024*1024, resource.BinarySI),
				},
				metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create CSIStorageCapacity: %v", err)
			}
		}

		// 创建 PVC
		for _, pvcConfig := range test.pvcs {
			pvc := makePVC(pvcConfig.name, config.ns, &classes[pvcConfig.scName].Name, pvcConfig.preboundPV)
			if _, err := config.client.CoreV1().PersistentVolumeClaims(config.ns).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
			}
		}

		// 创建 Pod
		if _, err := config.client.CoreV1().Pods(config.ns).Create(context.TODO(), test.pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Pod %q: %v", test.pod.Name, err)
		}

		// 容量不足会阻止 Pod 的调度和绑定。
		shouldFail := test.capacitySupported && !test.haveCapacity
		if shouldFail {
			if err := waitForPodUnschedulable(config.client, test.pod); err != nil {
				t.Errorf("Pod %q was not unschedulable: %v", test.pod.Name, err)
			}
		} else {
			if err := waitForPodToSchedule(config.client, test.pod); err != nil {
				t.Errorf("Failed to schedule Pod %q: %v", test.pod.Name, err)
			}
		}

		// 验证
		for _, pvc := range test.pvcs {
			if shouldFail {
				validatePVCPhase(t, config.client, pvc.name, config.ns, v1.ClaimPending, false)
			} else {
				validatePVCPhase(t, config.client, pvc.name, config.ns, v1.ClaimBound, true)
			}
		}

		// 强制删除对象，但它们可能不会立即被移除
		deleteTestObjects(config.client, config.ns, deleteOption)
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) { run(t, test) })
	}
}

// TestRescheduleProvisioning 验证当制备失败时，PV 控制器会移除
// PVC 上的 selectedNode annotation，以便重新安排卷制备。
func TestRescheduleProvisioning(t *testing.T) {
	testCtx := testutil.InitTestAPIServer(t, "reschedule-volume-provision", nil)

	clientset := testCtx.ClientSet
	ns := testCtx.NS.Name

	defer func() {
		deleteTestObjects(clientset, ns, metav1.DeleteOptions{})
	}()

	ctrl, informerFactory, err := initPVController(t, testCtx, 0)
	if err != nil {
		t.Fatalf("Failed to create PV controller: %v", err)
	}

	// 准备节点和 StorageClass。
	testNode := makeNode(1)
	if _, err := clientset.CoreV1().Nodes().Create(context.TODO(), testNode, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Node %q: %v", testNode.Name, err)
	}
	scName := "fail-provision"
	sc := makeDynamicProvisionerStorageClass(scName, &modeWait, nil)
	// 预期该 StorageClass 制备失败。
	sc.Parameters[volumetest.ExpectProvisionFailureKey] = ""
	if _, err := clientset.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create StorageClass %q: %v", sc.Name, err)
	}

	// 创建一个带有 selected-node annotation 的 PVC。
	pvcName := "pvc-fail-to-provision"
	pvc := makePVC(pvcName, ns, &scName, "")
	pvc.Annotations = map[string]string{"volume.kubernetes.io/selected-node": node1}
	pvc, err = clientset.CoreV1().PersistentVolumeClaims(ns).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create PersistentVolumeClaim %q: %v", pvc.Name, err)
	}
	// 验证创建的 PVC 上存在 selectedNode annotation。
	selectedNodeAnn, exist := pvc.Annotations["volume.kubernetes.io/selected-node"]
	if !exist || selectedNodeAnn != node1 {
		t.Fatalf("Created pvc is not annotated as expected")
	}

	// 启动控制器。
	go ctrl.Run(testCtx.Ctx)
	informerFactory.Start(testCtx.Ctx.Done())
	informerFactory.WaitForCacheSync(testCtx.Ctx.Done())

	// 验证控制器为重新安排制备而移除了该 annotation。
	if err := waitForProvisionAnn(clientset, pvc, false); err != nil {
		t.Errorf("Expect to reschedule provision for PVC %v/%v, but still found selected-node annotation on it", ns, pvcName)
	}
}

func setupCluster(t *testing.T, nsName string, numberOfNodes int, resyncPeriod time.Duration, provisionDelaySeconds int) *testConfig {
	testCtx := testutil.InitTestSchedulerWithOptions(t, testutil.InitTestAPIServer(t, nsName, nil), resyncPeriod)
	testutil.SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)

	clientset := testCtx.ClientSet
	ns := testCtx.NS.Name

	ctrl, informerFactory, err := initPVController(t, testCtx, provisionDelaySeconds)
	if err != nil {
		t.Fatalf("Failed to create PV controller: %v", err)
	}
	go ctrl.Run(testCtx.Ctx)
	// 在所有控制器配置完成并运行后启动 informer factory。
	informerFactory.Start(testCtx.Ctx.Done())
	informerFactory.WaitForCacheSync(testCtx.Ctx.Done())

	// 创建共享对象
	// 创建节点
	for i := range numberOfNodes {
		testNode := makeNode(i + 1)
		if _, err := clientset.CoreV1().Nodes().Create(context.TODO(), testNode, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Node %q: %v", testNode.Name, err)
		}
	}

	// 创建 StorageClass
	for _, sc := range sharedClasses {
		if _, err := clientset.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create StorageClass %q: %v", sc.Name, err)
		}
	}

	return &testConfig{
		client: clientset,
		ns:     ns,
		stop:   testCtx.Ctx.Done(),
		teardown: func() {
			klog.Infof("test cluster %q start to tear down", ns)
			deleteTestObjects(clientset, ns, metav1.DeleteOptions{})
		},
	}
}

func initPVController(t *testing.T, testCtx *testutil.TestContext, provisionDelaySeconds int) (*persistentvolume.PersistentVolumeController, informers.SharedInformerFactory, error) {
	clientset := testCtx.ClientSet
	// 供控制器使用的 informer factory
	informerFactory := informers.NewSharedInformerFactory(clientset, 0)

	// 启动用于卷绑定的 PV 控制器。
	host := volumetest.NewFakeVolumeHost(t, "/tmp/fake", nil, nil)
	provisionerNames := sets.New(
		provisionerPluginName,
		multiDriverAProvisionerName,
		multiDriverBProvisionerName,
		mixedEnabledProvisionerName,
		mixedDisabledProvisionerName,
	)
	plugins := make([]volume.VolumePlugin, 0, provisionerNames.Len())
	for provisionerName := range provisionerNames {
		plugins = append(plugins, &volumetest.FakeVolumePlugin{
			PluginName:             provisionerName,
			Host:                   host,
			Config:                 volume.VolumeConfig{},
			LastProvisionerOptions: volume.VolumeOptions{},
			ProvisionDelaySeconds:  provisionDelaySeconds,
			NewAttacherCallCount:   0,
			NewDetacherCallCount:   0,
			Mounters:               nil,
			Unmounters:             nil,
			Attachers:              nil,
			Detachers:              nil,
		})
	}

	params := persistentvolume.ControllerParameters{
		KubeClient: clientset,
		// 使用较短的重新同步周期来重试 API 更新冲突，原因是
		// https://github.com/kubernetes/kubernetes/issues/85320
		SyncPeriod:                5 * time.Second,
		VolumePlugins:             plugins,
		VolumeInformer:            informerFactory.Core().V1().PersistentVolumes(),
		ClaimInformer:             informerFactory.Core().V1().PersistentVolumeClaims(),
		ClassInformer:             informerFactory.Storage().V1().StorageClasses(),
		PodInformer:               informerFactory.Core().V1().Pods(),
		NodeInformer:              informerFactory.Core().V1().Nodes(),
		EnableDynamicProvisioning: true,
	}
	ctrl, err := persistentvolume.NewController(testCtx.Ctx, params)
	if err != nil {
		return nil, nil, err
	}

	return ctrl, informerFactory, nil
}

func deleteTestObjects(client clientset.Interface, ns string, option metav1.DeleteOptions) {
	client.CoreV1().Pods(ns).DeleteCollection(context.TODO(), option, metav1.ListOptions{})
	client.CoreV1().PersistentVolumeClaims(ns).DeleteCollection(context.TODO(), option, metav1.ListOptions{})
	client.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), option, metav1.ListOptions{})
	client.StorageV1().StorageClasses().DeleteCollection(context.TODO(), option, metav1.ListOptions{})
	client.StorageV1().CSIDrivers().DeleteCollection(context.TODO(), option, metav1.ListOptions{})
	client.StorageV1().CSIStorageCapacities("default").DeleteCollection(context.TODO(), option, metav1.ListOptions{})
}

func makeStorageClass(name string, mode *storagev1.VolumeBindingMode) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Provisioner:       "kubernetes.io/no-provisioner",
		VolumeBindingMode: mode,
	}
}

func makeDynamicProvisionerStorageClass(name string, mode *storagev1.VolumeBindingMode, allowedTopologies []v1.TopologySelectorTerm) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Provisioner:       provisionerPluginName,
		VolumeBindingMode: mode,
		AllowedTopologies: allowedTopologies,
		Parameters:        map[string]string{},
	}
}

func makePV(name, scName, pvcName, ns, node string) *v1.PersistentVolume {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("5Gi"),
			},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			StorageClassName: scName,
			PersistentVolumeSource: v1.PersistentVolumeSource{
				Local: &v1.LocalVolumeSource{
					Path: "/test-path",
				},
			},
		},
	}

	if pvcName != "" {
		pv.Spec.ClaimRef = &v1.ObjectReference{Name: pvcName, Namespace: ns}
	}

	if node != "" {
		pv.Spec.NodeAffinity = &v1.VolumeNodeAffinity{
			Required: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      nodeAffinityLabelKey,
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{node},
							},
						},
					},
				},
			},
		}
	}

	return pv
}

func makePVC(name, ns string, scName *string, volumeName string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
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
			StorageClassName: scName,
			VolumeName:       volumeName,
		},
	}
}

func makePod(name, ns string, pvcs []string) *v1.Pod {
	volumes := []v1.Volume{}
	for i, pvc := range pvcs {
		volumes = append(volumes, v1.Volume{
			Name: fmt.Sprintf("vol%v", i),
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc,
				},
			},
		})
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"app": "volume-binding-test",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "write-pod",
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"/bin/sh"},
					Args:    []string{"-c", "while true; do sleep 1; done"},
				},
			},
			Volumes: volumes,
		},
	}
}

// makeNode creates a node with the name "node-<index>"
func makeNode(index int) *v1.Node {
	name := fmt.Sprintf("node-%d", index)
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{nodeAffinityLabelKey: name},
		},
		Spec: v1.NodeSpec{Unschedulable: false},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourcePods: *resource.NewQuantity(podLimit, resource.DecimalSI),
			},
			Conditions: []v1.NodeCondition{
				{
					Type:              v1.NodeReady,
					Status:            v1.ConditionTrue,
					Reason:            "schedulable condition",
					LastHeartbeatTime: metav1.Time{Time: time.Now()},
				},
			},
		},
	}
}

func validatePVCPhase(t *testing.T, client clientset.Interface, pvcName string, ns string, phase v1.PersistentVolumeClaimPhase, isProvisioned bool) {
	claim, err := client.CoreV1().PersistentVolumeClaims(ns).Get(context.TODO(), pvcName, metav1.GetOptions{})
	if err != nil {
		t.Errorf("Failed to get PVC %v/%v: %v", ns, pvcName, err)
	}

	if claim.Status.Phase != phase {
		t.Errorf("PVC %v/%v phase not %v, got %v", ns, pvcName, phase, claim.Status.Phase)
	}

	// Check whether the bound claim is provisioned/bound as expect.
	if phase == v1.ClaimBound {
		if err := validateProvisionAnn(claim, isProvisioned); err != nil {
			t.Errorf("Provisoning annotation on PVC %v/%v not as expected: %v", ns, pvcName, err)
		}
	}
}

func validateProvisionAnn(claim *v1.PersistentVolumeClaim, volIsProvisioned bool) error {
	selectedNode, provisionAnnoExist := claim.Annotations["volume.kubernetes.io/selected-node"]
	if volIsProvisioned {
		if !provisionAnnoExist {
			return fmt.Errorf("PVC %v/%v expected to be provisioned, but no selected-node annotation found", claim.Namespace, claim.Name)
		}
		if selectedNode != node1 {
			return fmt.Errorf("PVC %v/%v expected to be annotated as %v, but got %v", claim.Namespace, claim.Name, node1, selectedNode)
		}
	}
	if !volIsProvisioned && provisionAnnoExist {
		return fmt.Errorf("PVC %v/%v not expected to be provisioned, but found selected-node annotation", claim.Namespace, claim.Name)
	}

	return nil
}

func waitForProvisionAnn(client clientset.Interface, pvc *v1.PersistentVolumeClaim, annShouldExist bool) error {
	return wait.Poll(time.Second, 30*time.Second, func() (bool, error) {
		claim, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(context.TODO(), pvc.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if err := validateProvisionAnn(claim, annShouldExist); err == nil {
			return true, nil
		}
		return false, nil
	})
}

func validatePVPhase(t *testing.T, client clientset.Interface, pvName string, phase v1.PersistentVolumePhase) {
	pv, err := client.CoreV1().PersistentVolumes().Get(context.TODO(), pvName, metav1.GetOptions{})
	if err != nil {
		t.Errorf("Failed to get PV %v: %v", pvName, err)
	}

	if pv.Status.Phase != phase {
		t.Errorf("PV %v phase not %v, got %v", pvName, phase, pv.Status.Phase)
	}
}

func waitForPVPhase(client clientset.Interface, pvName string, phase v1.PersistentVolumePhase) error {
	return wait.PollImmediate(time.Second, 30*time.Second, func() (bool, error) {
		pv, err := client.CoreV1().PersistentVolumes().Get(context.TODO(), pvName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if pv.Status.Phase == phase {
			return true, nil
		}
		return false, nil
	})
}

func waitForPVCBound(client clientset.Interface, pvc *v1.PersistentVolumeClaim) error {
	return wait.Poll(time.Second, 30*time.Second, func() (bool, error) {
		claim, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(context.TODO(), pvc.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if claim.Status.Phase == v1.ClaimBound {
			return true, nil
		}
		return false, nil
	})
}

func markNodeAffinity(pod *v1.Pod, node string) {
	affinity := &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      nodeAffinityLabelKey,
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{node},
							},
						},
					},
				},
			},
		},
	}
	pod.Spec.Affinity = affinity
}

func markNodeSelector(pod *v1.Pod, node string) {
	ns := map[string]string{
		nodeAffinityLabelKey: node,
	}
	pod.Spec.NodeSelector = ns
}
