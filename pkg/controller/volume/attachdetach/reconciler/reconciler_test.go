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

package reconciler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/metrics/legacyregistry"
	metricstestutil "k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/metrics"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/statusupdater"
	controllervolumetesting "k8s.io/kubernetes/pkg/controller/volume/attachdetach/testing"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/kubernetes/pkg/volume/util/types"
	utilstrings "k8s.io/utils/strings"
)

const (
	// reconcilerLoopPeriod 是对账循环的轮询间隔，测试中设置得很短以加快执行。
	reconcilerLoopPeriod = 10 * time.Millisecond
	// syncLoopPeriod 是全量同步循环的间隔；测试里设为极长，避免周期性同步干扰断言。
	syncLoopPeriod = 100 * time.Minute
	// maxWaitForUnmountDuration 是卷已卸载前允许等待的最长时间，用于触发超时强制 detach。
	maxWaitForUnmountDuration = 50 * time.Millisecond
	// maxLongWaitForUnmountDuration 模拟节点失联场景下的超长等待窗口。
	maxLongWaitForUnmountDuration = 4200 * time.Second
	// volumeAttachedCheckTimeout 是等待卷在上报状态中出现的轮询上限。
	volumeAttachedCheckTimeout = 5 * time.Second
)

// registerMetrics 保证指标只注册一次；计数器是全局的，多次注册会 panic。
var registerMetrics sync.Once

// Test_Run_Positive_DoNothing 调用 Run()，验证在没有待处理卷时不触发任何 attach 或 detach。
// Calls Run()
// Verifies there are no calls to attach or detach.
func Test_Run_Positive_DoNothing(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)

	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nsu := statusupdater.NewNodeStatusUpdater(
		fakeKubeClient, informerFactory.Core().V1().Nodes().Lister(), asw)
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 0 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, true /* expectZeroNewAttacherCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 0 /* expectedAttachCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)
}

// Test_Run_Positive_OneDesiredVolumeAttach 在期望状态缓存中放入一个 node/volume/pod 元组并调用 Run()，
// 验证产生一次 attach 调用且不产生 detach 调用。
// Populates desiredStateOfWorld cache with one node/volume/pod tuple.
// Calls Run()
// Verifies there is one attach call and no detach calls.
func Test_Run_Positive_OneDesiredVolumeAttach(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	// 先确认卷尚未出现在期望状态中，避免缓存预置数据干扰后续断言。
	volumeExists := dsw.VolumeExists(volumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName,
			nodeName)
	}

	// 添加 pod 会连带把其使用的卷加入期望状态。
	_, podErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)
	if podErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
}

// Test_Run_Positive_OneDesiredVolumeAttachThenDetachWithUnmountedVolume 验证完整生命周期：
// 先放入一个 node/volume/pod 元组并调用 Run()，期望一次 attach；
// 随后将卷标记为已卸载并从期望状态删除元组，期望一次 detach 且不再有新 attach。
// Populates desiredStateOfWorld cache with one node/volume/pod tuple.
// Calls Run()
// Verifies there is one attach call and no detach calls.
// Marks the node/volume as unmounted.
// Deletes the node/volume/pod tuple from desiredStateOfWorld cache.
// Verifies there is one detach call and no (new) attach calls.
func Test_Run_Positive_OneDesiredVolumeAttachThenDetachWithUnmountedVolume(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	volumeExists := dsw.VolumeExists(volumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName,
			nodeName)
	}

	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// Act
	dsw.DeletePod(types.UniquePodName(podName), generatedVolumeName, nodeName)
	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Deleted pod %q from volume %q/node %q. Volume should also be deleted but it still exists.",
			podName,
			generatedVolumeName,
			nodeName)
	}
	// 模拟卷已卸载：先设置已挂载再清空，使 reconciler 认定可以立即 detach 而无需等待超时。
	asw.SetVolumesMountedByNode(logger, []v1.UniqueVolumeName{generatedVolumeName}, nodeName)
	asw.SetVolumesMountedByNode(logger, nil, nodeName)

	// Assert
	waitForNewDetacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, false /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 1 /* expectedDetachCallCount */, fakePlugin)
}

// Test_Run_Positive_OneDesiredVolumeAttachThenDetachWithMountedVolume 验证卷仍处于挂载状态时，
// 从期望状态删除 pod 不会立即 detach，而是等定时器触发强制 detach（超时原因），
// 同时检查对应的强制 detach 指标增加一次。
// Populates desiredStateOfWorld cache with one node/volume/pod tuple.
// Calls Run()
// Verifies there is one attach call and no detach calls.
// Deletes the node/volume/pod tuple from desiredStateOfWorld cache without first marking the node/volume as unmounted.
// Verifies there is one detach call and no (new) attach calls.
func Test_Run_Positive_OneDesiredVolumeAttachThenDetachWithMountedVolume(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	registerMetrics.Do(func() {
		legacyregistry.MustRegister(metrics.ForceDetachMetricCounter)
	})

	// NOTE 原文：该值来自全局变量，测试开始时不一定为 0。因此先记录初值，后续只比较增量。
	// NOTE: This value is being pulled from a global variable, so it won't necessarily be 0 at the start of the test
	initialForceDetachCountTimeout, err := metricstestutil.GetCounterMetricValue(metrics.ForceDetachMetricCounter.WithLabelValues(metrics.ForceDetachReasonTimeout))
	if err != nil {
		t.Errorf("Error getting initialForceDetachCountTimeout")
	}

	initialForceDetachCountOutOfService, err := metricstestutil.GetCounterMetricValue(metrics.ForceDetachMetricCounter.WithLabelValues(metrics.ForceDetachReasonOutOfService))
	if err != nil {
		t.Errorf("Error getting initialForceDetachCountOutOfService")
	}

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)

	volumeExists := dsw.VolumeExists(volumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName,
			nodeName)
	}

	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// Act
	dsw.DeletePod(types.UniquePodName(podName), generatedVolumeName, nodeName)
	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Deleted pod %q from volume %q/node %q. Volume should also be deleted but it still exists.",
			podName,
			generatedVolumeName,
			nodeName)
	}

	// Assert -- Timer will trigger detach
	waitForNewDetacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, false /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 1 /* expectedDetachCallCount */, fakePlugin)

	// 超时触发的强制 detach 应在 Timeout 原因的计数器上加一。
	// Force detach metric due to timeout
	testForceDetachMetric(t, int(initialForceDetachCountTimeout)+1, metrics.ForceDetachReasonTimeout)
	// 本场景不应产生 OutOfService 原因的强制 detach，因此断言计数保持初值。
	// We shouldn't see any additional force detaches, so only consider the initial count
	testForceDetachMetric(t, int(initialForceDetachCountOutOfService), metrics.ForceDetachReasonOutOfService)
}

// Test_Run_Negative_OneDesiredVolumeAttachThenDetachWithUnmountedVolumeUpdateStatusFail 在节点状态更新失败时，
// 先放入一个 node/volume/pod 元组并调用 Run()，期望一次 attach；标记卷已卸载并删除元组后，
// 由于状态更新失败，验证不会产生 detach 调用，也不会有新 attach 调用。
// Populates desiredStateOfWorld cache with one node/volume/pod tuple.
// Has node update fail
// Calls Run()
// Verifies there is one attach call and no detach calls.
// Marks the node/volume as unmounted.
// Deletes the node/volume/pod tuple from desiredStateOfWorld cache.
// Verifies there are NO detach call and no (new) attach calls.
func Test_Run_Negative_OneDesiredVolumeAttachThenDetachWithUnmountedVolumeUpdateStatusFail(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	// 注入会失败的状态更新器，用于验证 detach 前置的状态更新失败会阻断 detach。
	nsu := statusupdater.NewFakeNodeStatusUpdater(true /* returnError */)
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	volumeExists := dsw.VolumeExists(volumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName,
			nodeName)
	}

	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// Act
	dsw.DeletePod(types.UniquePodName(podName), generatedVolumeName, nodeName)
	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Deleted pod %q from volume %q/node %q. Volume should also be deleted but it still exists.",
			podName,
			generatedVolumeName,
			nodeName)
	}
	asw.SetVolumesMountedByNode(logger, []v1.UniqueVolumeName{generatedVolumeName}, nodeName)
	asw.SetVolumesMountedByNode(logger, nil, nodeName)

	// Assert
	// 状态更新失败时，卷会被重新上报为已挂载，因此不应触发 detach。
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, false /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)
}

// Test_Run_OneVolumeAttachAndDetachMultipleNodesWithReadWriteMany 创建一个 ReadWriteMany 卷，
// 在期望状态中放入两个指向该卷的 node/volume/pod 元组：调用 Run() 后期望两次 attach、零次 detach；
// 逐个删除元组（均不先标记卸载）后，期望分别产生对应的 detach，且不再有新 attach。
// Creates a volume with accessMode ReadWriteMany
// Populates desiredStateOfWorld cache with two node/volume/pod tuples pointing to the created volume
// Calls Run()
// Verifies there are two attach calls and no detach calls.
// Deletes the first node/volume/pod tuple from desiredStateOfWorld cache without first marking the node/volume as unmounted.
// Verifies there is one detach call and no (new) attach calls.
// Deletes the second node/volume/pod tuple from desiredStateOfWorld cache without first marking the node/volume as unmounted.
// Verifies there are two detach calls and no (new) attach calls.
func Test_Run_OneVolumeAttachAndDetachMultipleNodesWithReadWriteMany(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	podName1 := "pod-uid1"
	podName2 := "pod-uid2"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	// ReadWriteMany 允许多个节点同时挂载，这是两个 pod 能分别 attach 的前提。
	volumeSpec.PersistentVolume.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteMany}
	nodeName1 := k8stypes.NodeName("node-name1")
	nodeName2 := k8stypes.NodeName(volumetesting.MultiAttachNode)
	dsw.AddNode(nodeName1)
	dsw.AddNode(nodeName2)

	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1, podName1), volumeSpec, nodeName1)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	_, podAddErr = dsw.AddPod(types.UniquePodName(podName2), controllervolumetesting.NewPod(podName2, podName2), volumeSpec, nodeName2)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 2 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForTotalAttachCallCount(t, 2 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)
	waitForAttachedToNodesCount(t, 2 /* expectedNodeCount */, generatedVolumeName, asw)

	// Act
	dsw.DeletePod(types.UniquePodName(podName1), generatedVolumeName, nodeName1)
	volumeExists := dsw.VolumeExists(generatedVolumeName, nodeName1)
	if volumeExists {
		t.Fatalf(
			"Deleted pod %q from volume %q/node %q. Volume should also be deleted but it still exists.",
			podName1,
			generatedVolumeName,
			nodeName1)
	}

	// 未标记卸载，因此需要等待定时器触发 detach。
	// Assert -- Timer will trigger detach
	waitForNewDetacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForTotalAttachCallCount(t, 2 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, false /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForTotalDetachCallCount(t, 1 /* expectedDetachCallCount */, fakePlugin)

	// Act
	dsw.DeletePod(types.UniquePodName(podName2), generatedVolumeName, nodeName2)
	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName2)
	if volumeExists {
		t.Fatalf(
			"Deleted pod %q from volume %q/node %q. Volume should also be deleted but it still exists.",
			podName2,
			generatedVolumeName,
			nodeName2)
	}

	// 删除第二个 pod 后，同样等待定时器触发第二次 detach。
	// Assert -- Timer will trigger detach
	waitForNewDetacherCallCount(t, 2 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForTotalAttachCallCount(t, 2 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, false /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForTotalDetachCallCount(t, 2 /* expectedDetachCallCount */, fakePlugin)
}

// Test_Run_OneVolumeAttachAndDetachMultipleNodesWithReadWriteOnce 创建一个 ReadWriteOnce 卷，
// 在期望状态中放入两个指向它的 node/volume/pod 元组：调用 Run() 后只有一个节点能 attach 成功，
// 另一个节点会被标记 multiattach 错误。删除成功 attach 的节点上的 pod 后，
// 卷先 detach 再 attach 到另一个节点，故最终为两次 attach、一次 detach。
// Creates a volume with accessMode ReadWriteOnce
// Populates desiredStateOfWorld cache with two ode/volume/pod tuples pointing to the created volume
// Calls Run()
// Verifies there is one attach call and no detach calls.
// Deletes the node/volume/pod tuple from desiredStateOfWorld which succeeded in attaching
// Verifies there are two attach call and one detach call.
func Test_Run_OneVolumeAttachAndDetachMultipleNodesWithReadWriteOnce(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	podName1 := "pod-uid1"
	podName2 := "pod-uid2"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	volumeSpec.PersistentVolume.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	nodeName1 := k8stypes.NodeName("node-name1")
	nodeName2 := k8stypes.NodeName("node-name2")
	dsw.AddNode(nodeName1)
	dsw.AddNode(nodeName2)

	// 同时加入两个 pod，以触发 reconciler 中潜在的竞态条件。
	// Add both pods at the same time to provoke a potential race condition in the reconciler
	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1, podName1), volumeSpec, nodeName1)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}
	_, podAddErr = dsw.AddPod(types.UniquePodName(podName2), controllervolumetesting.NewPod(podName2, podName2), volumeSpec, nodeName2)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForTotalAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)
	waitForAttachedToNodesCount(t, 1 /* expectedNodeCount */, generatedVolumeName, asw)

	nodesForVolume := asw.GetNodesForAttachedVolume(generatedVolumeName)

	// 检查 multiattach 错误是否已标记在至少一个卷+节点上。
	// check if multiattach is marked
	// at least one volume+node should be marked with multiattach error
	nodeAttachedTo := nodesForVolume[0]
	waitForMultiAttachErrorOnNode(t, nodeAttachedTo, dsw)

	// Act
	// 找出实际 attach 成功的节点，后续只删除该节点上的 pod 才能触发 detach。
	podToDelete := ""
	if nodesForVolume[0] == nodeName1 {
		podToDelete = podName1
	} else if nodesForVolume[0] == nodeName2 {
		podToDelete = podName2
	} else {
		t.Fatal("Volume attached to unexpected node")
	}

	dsw.DeletePod(types.UniquePodName(podToDelete), generatedVolumeName, nodesForVolume[0])
	volumeExists := dsw.VolumeExists(generatedVolumeName, nodesForVolume[0])
	if volumeExists {
		t.Fatalf(
			"Deleted pod %q from volume %q/node %q. Volume should also be deleted but it still exists.",
			podToDelete,
			generatedVolumeName,
			nodesForVolume[0])
	}

	// Assert
	waitForNewDetacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, false /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForTotalDetachCallCount(t, 1 /* expectedDetachCallCount */, fakePlugin)
	waitForNewAttacherCallCount(t, 2 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForTotalAttachCallCount(t, 2 /* expectedAttachCallCount */, fakePlugin)
}

// Test_Run_OneVolumeAttachAndDetachUncertainNodesWithReadWriteOnce 创建 ReadWriteOnce 卷：
// 先把卷 attach 到名为 "uncertain-node" 的节点，该 attach 调用因超时失败但卷实际已挂上；
// 删除该 pod 后，再创建调度到普通节点的 pod，验证卷能成功 attach 到新节点。
// Creates a volume with accessMode ReadWriteOnce
// First create a pod which will try to attach the volume to the a node named "uncertain-node". The attach call for this node will
// fail for timeout, but the volume will be actually attached to the node after the call.
// Secondly, delete this pod.
// Lastly, create a pod scheduled to a normal node which will trigger attach volume to the node. The attach should return successfully.
func Test_Run_OneVolumeAttachAndDetachUncertainNodesWithReadWriteOnce(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	podName1 := "pod-uid1"
	podName2 := "pod-uid2"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	volumeSpec.PersistentVolume.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	nodeName1 := k8stypes.NodeName(volumetesting.UncertainAttachNode)
	nodeName2 := k8stypes.NodeName("node-name2")
	dsw.AddNode(nodeName1)
	dsw.AddNode(nodeName2)

	// Act
	go reconciler.Run(ctx)

	// Add the pod in which the volume is attached to the uncertain node
	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1, podName1), volumeSpec, nodeName1)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	// 给异步 attach 留出时间：卷会被加入实际状态（asw），但因 attach 调用失败不应上报为已挂载。
	time.Sleep(1 * time.Second)
	// Volume is added to asw. Because attach operation fails, volume should not be reported as attached to the node.
	waitForVolumeAddedToNode(t, generatedVolumeName, nodeName1, asw)
	verifyVolumeAttachedToNode(t, generatedVolumeName, nodeName1, cache.AttachStateAttached, asw)
	verifyVolumeReportedAsAttachedToNode(t, logger, generatedVolumeName, nodeName1, true, asw, volumeAttachedCheckTimeout)

	// 卷加入节点时默认被标记为已挂载，之后依据节点状态中的 VolumeInUse 更新；
	// 这里手动清空，否则删除操作会因挂载状态而被延迟。
	// When volume is added to the node, it is set to mounted by default. Then the status will be updated by checking node status VolumeInUse.
	// Without this, the delete operation will be delayed due to mounted status
	asw.SetVolumesMountedByNode(logger, nil, nodeName1)

	dsw.DeletePod(types.UniquePodName(podName1), generatedVolumeName, nodeName1)

	waitForVolumeRemovedFromNode(t, generatedVolumeName, nodeName1, asw)

	// 再添加一个 pod，尝试把卷挂到另一个节点。
	// Add a second pod which tries to attach the volume to a different node.
	generatedVolumeName, podAddErr = dsw.AddPod(types.UniquePodName(podName2), controllervolumetesting.NewPod(podName2, podName2), volumeSpec, nodeName2)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}
	waitForVolumeAttachStateToNode(t, generatedVolumeName, nodeName2, cache.AttachStateAttached, asw)
}

// Test_Run_UpdateNodeStatusFailBeforeOneVolumeDetachNodeWithReadWriteOnce 验证 detach 前的节点状态更新失败：
// 卷 attach 后删除 pod，注入失败的状态更新器，检查卷仍保持在已挂载状态并继续上报，
// 直到新的变化（再次触发 detach）到来前都不需要更新状态。
func Test_Run_UpdateNodeStatusFailBeforeOneVolumeDetachNodeWithReadWriteOnce(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	rc := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	reconciliationLoopFunc := rc.(*reconciler).reconciliationLoopFunc(ctx)
	podName1 := "pod-uid1"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	volumeSpec.PersistentVolume.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	nodeName1 := k8stypes.NodeName("node-name1")
	dsw.AddNode(nodeName1)

	// 添加 pod，使卷 attach 到 FailDetachNode。
	// Add the pod in which the volume is attached to the FailDetachNode
	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1, podName1), volumeSpec, nodeName1)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	// Act
	reconciliationLoopFunc(ctx)

	// 卷应被加入 asw 并上报为已挂载到该节点。
	// Volume is added to asw, volume should be reported as attached to the node.
	waitForVolumeAddedToNode(t, generatedVolumeName, nodeName1, asw)
	verifyVolumeAttachedToNode(t, generatedVolumeName, nodeName1, cache.AttachStateAttached, asw)
	verifyVolumeReportedAsAttachedToNode(t, logger, generatedVolumeName, nodeName1, true, asw, volumeAttachedCheckTimeout)

	// Delete the pod
	dsw.DeletePod(types.UniquePodName(podName1), generatedVolumeName, nodeName1)

	// 注入失败的状态更新器，模拟节点状态更新失败。
	// Mock NodeStatusUpdate fail
	rc.(*reconciler).nodeStatusUpdater = statusupdater.NewFakeNodeStatusUpdater(true /* returnError */)
	reconciliationLoopFunc(ctx)
	// 首次 detach 至少要等 maxWaitForUnmountDuration（测试中 50ms）之后才会发生，等待 100ms 确保越过该窗口。
	// The first detach will be triggered after at least 50ms (maxWaitForUnmountDuration in test).
	time.Sleep(100 * time.Millisecond)
	reconciliationLoopFunc(ctx)
	// detach 前会先从上报列表中移除该卷（RemoveVolumeFromReportAsAttached）；
	// 状态更新失败后控制器又把卷加回已挂载列表。GetVolumesToReportAttached 会触发状态更新，
	// 此后在下一次变化（重新触发 detach）前都不再需要更新该卷状态。
	// Right before detach operation is performed, the volume will be first removed from being reported
	// as attached on node status (RemoveVolumeFromReportAsAttached). After UpdateNodeStatus operation which is expected to fail,
	// controller then added the volume back as attached.
	// verifyVolumeReportedAsAttachedToNode will check volume is in the list of volume attached that needs to be updated
	// in node status. By calling this function (GetVolumesToReportAttached), node status should be updated, and the volume
	// will not need to be updated until new changes are applied (detach is triggered again)
	verifyVolumeAttachedToNode(t, generatedVolumeName, nodeName1, cache.AttachStateAttached, asw)
	verifyVolumeReportedAsAttachedToNode(t, logger, generatedVolumeName, nodeName1, true, asw, volumeAttachedCheckTimeout)

}

// Test_Run_OneVolumeDetachFailNodeWithReadWriteOnce 验证 ReadWriteOnce 卷在 detach 失败后的处理：
// 卷状态变为 Uncertain 且不再上报为已挂载；同一节点新增 pod 后不再触发 detach，卷恢复 Attached；
// 再向另一节点添加 pod 时，卷仍挂在前一节点，两个节点都无需状态更新。
func Test_Run_OneVolumeDetachFailNodeWithReadWriteOnce(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	podName1 := "pod-uid1"
	podName2 := "pod-uid2"
	podName3 := "pod-uid3"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	volumeSpec.PersistentVolume.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	nodeName1 := k8stypes.NodeName(volumetesting.FailDetachNode)
	nodeName2 := k8stypes.NodeName("node-name2")
	dsw.AddNode(nodeName1)
	dsw.AddNode(nodeName2)

	// Act
	go reconciler.Run(ctx)

	// 添加 pod，使卷 attach 到 FailDetachNode。
	// Add the pod in which the volume is attached to the FailDetachNode
	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1, podName1), volumeSpec, nodeName1)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	// Volume is added to asw, volume should be reported as attached to the node.
	waitForVolumeAddedToNode(t, generatedVolumeName, nodeName1, asw)
	verifyVolumeAttachedToNode(t, generatedVolumeName, nodeName1, cache.AttachStateAttached, asw)
	verifyVolumeReportedAsAttachedToNode(t, logger, generatedVolumeName, nodeName1, true, asw, volumeAttachedCheckTimeout)

	// 删除 pod，但该节点上的 detach 会失败。
	// Delete the pod, but detach will fail
	dsw.DeletePod(types.UniquePodName(podName1), generatedVolumeName, nodeName1)

	waitForVolumeAttachStateToNode(t, generatedVolumeName, nodeName1, cache.AttachStateUncertain, asw)
	verifyVolumeReportedAsAttachedToNode(t, logger, generatedVolumeName, nodeName1, false, asw, volumeAttachedCheckTimeout)

	// Add a second pod which tries to attach the volume to the same node.
	// After adding pod to the same node, detach will not be triggered any more,
	// the volume gets attached and reported as attached to the node.
	generatedVolumeName, podAddErr = dsw.AddPod(types.UniquePodName(podName2), controllervolumetesting.NewPod(podName2, podName2), volumeSpec, nodeName1)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}
	// 休眠 1 秒，验证第二个 pod 加入后不会再触发 detach。
	// Sleep 1s to verify no detach are triggered after second pod is added in the future.
	time.Sleep(1000 * time.Millisecond)
	verifyVolumeAttachedToNode(t, generatedVolumeName, nodeName1, cache.AttachStateAttached, asw)
	verifyVolumeReportedAsAttachedToNode(t, logger, generatedVolumeName, nodeName1, true, asw, volumeAttachedCheckTimeout)
	// verifyVolumeNoStatusUpdateNeeded(t, logger, generatedVolumeName, nodeName1, asw)

	// Add a third pod which tries to attach the volume to a different node.
	// At this point, volume is still attached to first node. There are no status update for both nodes.
	generatedVolumeName, podAddErr = dsw.AddPod(types.UniquePodName(podName3), controllervolumetesting.NewPod(podName3, podName3), volumeSpec, nodeName2)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}
	verifyVolumeAttachedToNode(t, generatedVolumeName, nodeName1, cache.AttachStateAttached, asw)
	verifyVolumeNoStatusUpdateNeeded(t, logger, generatedVolumeName, nodeName1, asw)
	verifyVolumeNoStatusUpdateNeeded(t, logger, generatedVolumeName, nodeName2, asw)
}

// Test_Run_OneVolumeAttachAndDetachTimeoutNodesWithReadWriteOnce 创建 ReadWriteOnce 卷：
// 先尝试把卷挂到 "timeout-node"，attach 调用超时失败但卷实际已挂上；
// 删除该 pod 后再创建调度到普通节点的 pod，验证卷能成功挂到新节点。
// Creates a volume with accessMode ReadWriteOnce
// First create a pod which will try to attach the volume to the a node named "timeout-node". The attach call for this node will
// fail for timeout, but the volume will be actually attached to the node after the call.
// Secondly, delete the this pod.
// Lastly, create a pod scheduled to a normal node which will trigger attach volume to the node. The attach should return successfully.
func Test_Run_OneVolumeAttachAndDetachTimeoutNodesWithReadWriteOnce(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)
	podName1 := "pod-uid1"
	podName2 := "pod-uid2"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	volumeSpec.PersistentVolume.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	nodeName1 := k8stypes.NodeName(volumetesting.TimeoutAttachNode)
	nodeName2 := k8stypes.NodeName("node-name2")
	dsw.AddNode(nodeName1)
	dsw.AddNode(nodeName2)

	// Act
	go reconciler.Run(ctx)

	// 添加 pod，使卷尝试挂到超时节点。
	// Add the pod in which the volume is attached to the timeout node
	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1, podName1), volumeSpec, nodeName1)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}

	// 卷被加入 asw，但因 attach 调用超时失败，不应上报为已挂载。
	// Volume is added to asw. Because attach operation fails, volume should not be reported as attached to the node.
	waitForVolumeAddedToNode(t, generatedVolumeName, nodeName1, asw)
	verifyVolumeAttachedToNode(t, generatedVolumeName, nodeName1, cache.AttachStateUncertain, asw)
	verifyVolumeReportedAsAttachedToNode(t, logger, generatedVolumeName, nodeName1, false, asw, volumeAttachedCheckTimeout)

	// 卷加入节点时默认设为已挂载，之后依据节点状态中的 VolumeInUse 更新；
	// 手动清空，否则删除操作会被挂载状态延迟。
	// When volume is added to the node, it is set to mounted by default. Then the status will be updated by checking node status VolumeInUse.
	// Without this, the delete operation will be delayed due to mounted status
	asw.SetVolumesMountedByNode(logger, nil, nodeName1)

	dsw.DeletePod(types.UniquePodName(podName1), generatedVolumeName, nodeName1)

	waitForVolumeRemovedFromNode(t, generatedVolumeName, nodeName1, asw)

	// 再添加一个 pod，尝试把卷挂到另一个节点。
	// Add a second pod which tries to attach the volume to a different node.
	generatedVolumeName, podAddErr = dsw.AddPod(types.UniquePodName(podName2), controllervolumetesting.NewPod(podName2, podName2), volumeSpec, nodeName2)
	if podAddErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podAddErr)
	}
	waitForVolumeAttachStateToNode(t, generatedVolumeName, nodeName2, cache.AttachStateAttached, asw)
}

// Test_Run_OneVolumeDetachOnOutOfServiceTaintedNode 验证带 node.kubernetes.io/out-of-service 污点节点的行为：
// 在期望状态放入一个 node/volume/pod 元组并调用 Run()，期望一次 attach、零次 detach；
// 不先标记卸载就删除 pod 后，由于节点带 out-of-service 污点，
// 无需等待 maxWaitForUnmountDuration 即立即 detach，同时 OutOfService 原因的强制 detach 指标加一。
// Populates desiredStateOfWorld cache with one node/volume/pod tuple.
// The node has node.kubernetes.io/out-of-service taint present.
//
// maxWaitForUnmountDuration 被设得很长（4200 秒），使 detach 不会因超时立即发生。
// The maxWaitForUnmountDuration is longer (in this case it is 4200 * time.Second so that detach does not happen
// immediately due to timeout.
//
// Calls Run()
// Verifies there is one attach call and no detach calls.
// Deletes the pod from desiredStateOfWorld cache without first marking the node/volume as unmounted.
// Verifies there is one detach call and no (new) attach calls.
func Test_Run_OneVolumeDetachOnOutOfServiceTaintedNode(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	registerMetrics.Do(func() {
		legacyregistry.MustRegister(metrics.ForceDetachMetricCounter)
	})

	// NOTE 原文：该值来自全局变量，测试开始时不一定为 0，后续只比较增量。
	// NOTE: This value is being pulled from a global variable, so it won't necessarily be 0 at the start of the test
	initialForceDetachCountOutOfService, err := metricstestutil.GetCounterMetricValue(metrics.ForceDetachMetricCounter.WithLabelValues(metrics.ForceDetachReasonOutOfService))
	if err != nil {
		t.Errorf("Error getting initialForceDetachCountOutOfService")
	}

	initialForceDetachCountTimeout, err := metricstestutil.GetCounterMetricValue(metrics.ForceDetachMetricCounter.WithLabelValues(metrics.ForceDetachReasonTimeout))
	if err != nil {
		t.Errorf("Error getting initialForceDetachCountTimeout")
	}

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxLongWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad,
		nsu, nodeLister, fakeRecorder)
	podName1 := "pod-uid1"
	volumeName1 := v1.UniqueVolumeName("volume-name1")
	volumeSpec1 := controllervolumetesting.GetTestVolumeSpec(string(volumeName1), volumeName1)
	nodeName1 := k8stypes.NodeName("worker-0")
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: string(nodeName1)},
		Spec: v1.NodeSpec{
			Taints: []v1.Taint{{Key: v1.TaintNodeOutOfService, Effect: v1.TaintEffectNoExecute}},
		},
	}
	// 把节点加入 informer 存储并打上 out-of-service 污点，供 reconciler 查询。
	informerFactory.Core().V1().Nodes().Informer().GetStore().Add(node1)
	dsw.AddNode(nodeName1)
	volumeExists := dsw.VolumeExists(volumeName1, nodeName1)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName1,
			nodeName1)
	}

	generatedVolumeName, podErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1,
		podName1), volumeSpec1, nodeName1)
	if podErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// 删除 pod 时卷尚未卸载，正常应等到 maxLongWaitForUnmountDuration 到期才 detach；
	// 但此节点带 out-of-service 污点，因此会跳过等待立即 detach。
	// Delete the pod and the volume will be detached only after the maxLongWaitForUnmountDuration expires as volume is
	//not unmounted. Here maxLongWaitForUnmountDuration is used to mimic that node is out of service.
	// But in this case the node has the node.kubernetes.io/out-of-service taint and hence it will not wait for
	// maxLongWaitForUnmountDuration and will progress to detach immediately.
	dsw.DeletePod(types.UniquePodName(podName1), generatedVolumeName, nodeName1)
	// Assert -- Detach will be triggered if node has out of service taint
	waitForNewDetacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, false /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 1 /* expectedDetachCallCount */, fakePlugin)

	// 因 out-of-service 污点触发的强制 detach：OutOfService 计数器加一，Timeout 计数器保持初值。
	// Force detach metric due to out-of-service taint
	testForceDetachMetric(t, int(initialForceDetachCountOutOfService)+1, metrics.ForceDetachReasonOutOfService)
	// We shouldn't see any additional force detaches, so only consider the initial count
	testForceDetachMetric(t, int(initialForceDetachCountTimeout), metrics.ForceDetachReasonTimeout)
}

// Test_Run_OneVolumeDetachOnNoOutOfServiceTaintedNode 验证不带 out-of-service 污点节点的行为：
// 放入一个 node/volume/pod 元组并调用 Run()，期望一次 attach、零次 detach；
// 不先标记卸载就删除 pod 后，由于节点没有该污点，必须等待 maxWaitForUnmountDuration 到期，
// 因此验证此时不产生 detach，也不产生新的 attach。
// Populates desiredStateOfWorld cache with one node/volume/pod tuple.
// The node does not have the node.kubernetes.io/out-of-service taint present.
//
// maxWaitForUnmountDuration 设得很长（4200 秒），使 detach 不会因超时立即发生。
// The maxWaitForUnmountDuration is longer (in this case it is 4200 * time.Second so that detach does not happen
// immediately due to timeout.
//
// Calls Run()
// Verifies there is one attach call and no detach calls.
// Deletes the pod from desiredStateOfWorld cache without first marking the node/volume as unmounted.
// Verifies there is no detach call and no (new) attach calls.
func Test_Run_OneVolumeDetachOnNoOutOfServiceTaintedNode(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxLongWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad,
		nsu, nodeLister, fakeRecorder)
	podName1 := "pod-uid1"
	volumeName1 := v1.UniqueVolumeName("volume-name1")
	volumeSpec1 := controllervolumetesting.GetTestVolumeSpec(string(volumeName1), volumeName1)
	nodeName1 := k8stypes.NodeName("worker-0")
	// 节点不带任何污点，作为上一测试的对照。
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: string(nodeName1)},
	}
	informerFactory.Core().V1().Nodes().Informer().GetStore().Add(node1)
	dsw.AddNode(nodeName1)
	volumeExists := dsw.VolumeExists(volumeName1, nodeName1)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName1,
			nodeName1)
	}

	generatedVolumeName, podErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1,
		podName1), volumeSpec1, nodeName1)
	if podErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// 删除 pod 时卷尚未卸载：由于节点没有 out-of-service 污点，
	// 必须等待 maxLongWaitForUnmountDuration 到期，因此不会立即 detach。
	// Delete the pod and the volume will be detached only after the maxLongWaitForUnmountDuration expires as volume is
	// not unmounted. Here maxLongWaitForUnmountDuration is used to mimic that node is out of service.
	// But in this case the node does not have the node.kubernetes.io/out-of-service taint and hence it will wait for
	// maxLongWaitForUnmountDuration and will not be detached immediately.
	dsw.DeletePod(types.UniquePodName(podName1), generatedVolumeName, nodeName1)
	// Assert -- Detach will be triggered only after maxLongWaitForUnmountDuration expires
	waitForNewDetacherCallCount(t, 0 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)
}

// Test_Run_OneVolumeDetachOnUnhealthyNode 验证节点健康状态对 detach 等待的影响：
// 节点初始健康，放入 node/volume/pod 元组并调用 Run()，期望一次 attach、零次 detach；
// 不先标记卸载就删除 pod 后，节点健康时即使超过 maxWaitForUnmountDuration 也不 detach；
// 将节点标记为不健康后，则在该超时后触发 detach。
// Populates desiredStateOfWorld cache with one node/volume/pod tuple.
// The node starts as healthy.
//
// Calls Run()
// Verifies there is one attach call and no detach calls.
// Deletes the pod from desiredStateOfWorld cache without first marking the node/volume as unmounted.
// Verifies that the volume is NOT detached after maxWaitForUnmountDuration.
// Marks the node as unhealthy.
// Verifies that the volume is detached after maxWaitForUnmountDuration.
func Test_Run_OneVolumeDetachOnUnhealthyNode(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad,
		nsu, nodeLister, fakeRecorder)
	podName1 := "pod-uid1"
	volumeName1 := v1.UniqueVolumeName("volume-name1")
	volumeSpec1 := controllervolumetesting.GetTestVolumeSpec(string(volumeName1), volumeName1)
	nodeName1 := k8stypes.NodeName("worker-0")
	// 节点初始状态为 Ready=True。
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: string(nodeName1)},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{
					Type:   v1.NodeReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}
	informerFactory.Core().V1().Nodes().Informer().GetStore().Add(node1)
	dsw.AddNode(nodeName1)
	volumeExists := dsw.VolumeExists(volumeName1, nodeName1)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName1,
			nodeName1)
	}

	generatedVolumeName, podErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1,
		podName1), volumeSpec1, nodeName1)
	if podErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// 节点健康且卷未卸载，即使等待超过 maxWaitForUnmountDuration 也不 detach。
	// Act
	// Delete the pod and the volume will be detached even after the maxWaitForUnmountDuration expires as volume is
	// not unmounted and the node is healthy.
	dsw.DeletePod(types.UniquePodName(podName1), generatedVolumeName, nodeName1)
	time.Sleep(maxWaitForUnmountDuration * 5)
	// Assert
	waitForNewDetacherCallCount(t, 0 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// 将节点标记为不健康（Ready=False），应使等待中的 detach 立即生效。
	// Act
	// Mark the node unhealthy
	node2 := node1.DeepCopy()
	node2.Status.Conditions[0].Status = v1.ConditionFalse
	informerFactory.Core().V1().Nodes().Informer().GetStore().Update(node2)
	// Assert -- Detach was triggered after maxWaitForUnmountDuration
	waitForNewDetacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, false /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 1 /* expectedDetachCallCount */, fakePlugin)
}

// Test_Run_OneVolumeDetachOnUnhealthyNodeWithForceDetachOnUnmountDisabled 验证禁用卸载时强制 detach 的行为：
// 节点初始健康，放入元组并调用 Run()，期望一次 attach、零次 detach；
// 删除 pod 后节点健康时不 detach，标记不健康后若禁用卸载时强制 detach 仍不 detach；
// 随后打上 out-of-service 污点，则仍只有一次 attach，但会触发一次 detach。
// Populates desiredStateOfWorld cache with one node/volume/pod tuple.
// The node starts as healthy.
//
// Calls Run()
// Verifies there is one attach call and no detach calls.
// Deletes the pod from desiredStateOfWorld cache without first marking the node/volume as unmounted.
// Verifies that the volume is NOT detached after maxWaitForUnmountDuration.
// Marks the node as unhealthy.
// Sets forceDetachOnUmountDisabled to true.
// Verifies that the volume is not detached after maxWaitForUnmountDuration.
//
// Then applies the node.kubernetes.io/out-of-service taint.
// Verifies that there is still just one attach call.
// Verifies there is now one detach call.
func Test_Run_OneVolumeDetachOnUnhealthyNodeWithForceDetachOnUnmountDisabled(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	registerMetrics.Do(func() {
		legacyregistry.MustRegister(metrics.ForceDetachMetricCounter)
	})
	// NOTE: This value is being pulled from a global variable, so it won't necessarily be 0 at the start of the test
	initialForceDetachCountOutOfService, err := metricstestutil.GetCounterMetricValue(metrics.ForceDetachMetricCounter.WithLabelValues(metrics.ForceDetachReasonOutOfService))
	if err != nil {
		t.Errorf("Error getting initialForceDetachCountOutOfService")
	}

	initialForceDetachCountTimeout, err := metricstestutil.GetCounterMetricValue(metrics.ForceDetachMetricCounter.WithLabelValues(metrics.ForceDetachReasonTimeout))
	if err != nil {
		t.Errorf("Error getting initialForceDetachCountTimeout")
	}

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestVolumePluginMgr(t)
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
	asw := cache.NewActualStateOfWorld(volumePluginMgr)
	fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		fakeKubeClient,
		volumePluginMgr,
		fakeRecorder,
		// 最后一个布尔参数 forceDetachOnUnmountDisabled 设为 true，禁用因未卸载而强制 detach。
		fakeHandler))
	informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
	nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
	nodeLister := informerFactory.Core().V1().Nodes().Lister()
	reconciler := NewReconciler(
		reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, true, dsw, asw, ad,
		nsu, nodeLister, fakeRecorder)
	podName1 := "pod-uid1"
	volumeName1 := v1.UniqueVolumeName("volume-name1")
	volumeSpec1 := controllervolumetesting.GetTestVolumeSpec(string(volumeName1), volumeName1)
	nodeName1 := k8stypes.NodeName("worker-0")
	// 节点初始状态为 Ready=True。
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: string(nodeName1)},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{
					Type:   v1.NodeReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}
	addErr := informerFactory.Core().V1().Nodes().Informer().GetStore().Add(node1)
	if addErr != nil {
		t.Fatalf("Add node failed. Expected: <no error> Actual: <%v>", addErr)
	}
	dsw.AddNode(nodeName1)
	volumeExists := dsw.VolumeExists(volumeName1, nodeName1)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName1,
			nodeName1)
	}

	generatedVolumeName, podErr := dsw.AddPod(types.UniquePodName(podName1), controllervolumetesting.NewPod(podName1,
		podName1), volumeSpec1, nodeName1)
	if podErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podErr)
	}

	// Act
	go reconciler.Run(ctx)

	// Assert
	waitForNewAttacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// 节点健康且卷未卸载，即使超过等待时间也不 detach。
	// Act
	// Delete the pod and the volume will be detached even after the maxWaitForUnmountDuration expires as volume is
	// not unmounted and the node is healthy.
	dsw.DeletePod(types.UniquePodName(podName1), generatedVolumeName, nodeName1)
	time.Sleep(maxWaitForUnmountDuration * 5)
	// Assert
	waitForNewDetacherCallCount(t, 0 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// 将节点标记为不健康，但因禁用了卸载时强制 detach，仍不应触发 detach。
	// Act
	// Mark the node unhealthy
	node2 := node1.DeepCopy()
	node2.Status.Conditions[0].Status = v1.ConditionFalse
	updateErr := informerFactory.Core().V1().Nodes().Informer().GetStore().Update(node2)
	if updateErr != nil {
		t.Fatalf("Update node failed. Expected: <no error> Actual: <%v>", updateErr)
	}
	// Assert -- Detach was not triggered after maxWaitForUnmountDuration
	waitForNewDetacherCallCount(t, 0 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, true /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 0 /* expectedDetachCallCount */, fakePlugin)

	// 到此为止不应有强制 detach：OutOfService 与 Timeout 计数器均保持初值。
	// Force detach metric due to out-of-service taint
	// We shouldn't see any additional force detaches, so only consider the initial count
	testForceDetachMetric(t, int(initialForceDetachCountOutOfService), metrics.ForceDetachReasonOutOfService)
	testForceDetachMetric(t, int(initialForceDetachCountTimeout), metrics.ForceDetachReasonTimeout)

	// 给不健康节点打上 out-of-service 污点，该污点可绕过 forceDetachOnUnmountDisabled 的限制。
	// Act
	// Taint the node
	node3 := node2.DeepCopy()
	node3.Spec.Taints = append(node3.Spec.Taints, v1.Taint{Key: v1.TaintNodeOutOfService, Effect: v1.TaintEffectNoExecute})
	updateErr = informerFactory.Core().V1().Nodes().Informer().GetStore().Update(node3)
	if updateErr != nil {
		t.Fatalf("Update node failed. Expected: <no error> Actual: <%v>", updateErr)
	}
	// Assert -- Detach was triggered after maxWaitForUnmountDuration
	waitForNewDetacherCallCount(t, 1 /* expectedCallCount */, fakePlugin)
	verifyNewAttacherCallCount(t, false /* expectZeroNewAttacherCallCount */, fakePlugin)
	waitForAttachCallCount(t, 1 /* expectedAttachCallCount */, fakePlugin)
	verifyNewDetacherCallCount(t, false /* expectZeroNewDetacherCallCount */, fakePlugin)
	waitForDetachCallCount(t, 1 /* expectedDetachCallCount */, fakePlugin)

	// 污点触发一次强制 detach：OutOfService 计数器加一，Timeout 计数器保持初值。
	// Force detach metric due to out-of-service taint
	// We should see one more force detach, so consider the initial count + 1
	testForceDetachMetric(t, int(initialForceDetachCountOutOfService)+1, metrics.ForceDetachReasonOutOfService)
	// We shouldn't see any additional force detaches, so only consider the initial count
	testForceDetachMetric(t, int(initialForceDetachCountTimeout), metrics.ForceDetachReasonTimeout)
}

// Test_ReportWaitingOnDetach 以表驱动方式验证 reportMultiAttachError 生成的事件文案：
// 根据使用卷的 pod 是否在同一命名空间，分别生成等待 detach 的普通提示、
// 同命名空间 pod 列表或跨命名空间 pod 计数的提示，并逐条核对 FakeRecorder 收到的事件。
func Test_ReportWaitingOnDetach(t *testing.T) {
	// nodeWithPods 描述一个节点及其上使用该卷的 pod 列表（命名空间限定名）。
	type nodeWithPods struct {
		name     k8stypes.NodeName
		podNames []string
	}
	// 每个用例包含名称、节点分布和期望收到的事件文本。
	tests := []struct {
		name           string
		nodes          []nodeWithPods
		expectedEvents []string
	}{
		{
			// 仅一个节点使用卷：提示卷已被独占，需等待 detach。
			"no pods use the volume",
			[]nodeWithPods{
				{"node1", []string{"ns1/pod1"}},
			},
			[]string{"Warning FailedAttachVolume Waiting for detach for volume \"volume-name\" Volume is already exclusively attached to one node, waiting on detach before it can be attached to another node"},
		},
		{
			// 两个 pod 在同一命名空间：事件里列出该命名空间内的 pod 名。
			"pods in the same namespace use the volume",
			[]nodeWithPods{
				{"node1", []string{"ns1/pod1"}},
				{"node2", []string{"ns1/pod2"}},
			},
			[]string{"Warning FailedAttachVolume Waiting for detach for volume \"volume-name\" Volume is already used by pod(s) pod2"},
		},
		{
			// 两个 pod 在不同命名空间：事件只统计跨命名空间的 pod 数量。
			"pods in another namespace use the volume",
			[]nodeWithPods{
				{"node1", []string{"ns1/pod1"}},
				{"node2", []string{"ns2/pod2"}},
			},
			[]string{"Warning FailedAttachVolume Waiting for detach for volume \"volume-name\" Volume is already used by 1 pod(s) in different namespaces"},
		},
		{
			// 同时存在同命名空间与跨命名空间的 pod：两类信息都要出现在事件中。
			"pods both in the same and another namespace use the volume",
			[]nodeWithPods{
				{"node1", []string{"ns1/pod1"}},
				{"node2", []string{"ns2/pod2"}},
				{"node3", []string{"ns1/pod3"}},
			},
			[]string{"Warning FailedAttachVolume Waiting for detach for volume \"volume-name\" Volume is already used by pod(s) pod3 and 1 pod(s) in different namespaces"},
		},
	}

	for _, test := range tests {
		logger, _ := ktesting.NewTestContext(t)
		// Arrange
		t.Logf("Test %q starting", test.name)
		volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
		dsw := cache.NewDesiredStateOfWorld(volumePluginMgr)
		asw := cache.NewActualStateOfWorld(volumePluginMgr)
		fakeKubeClient := controllervolumetesting.CreateTestClient(logger)
		fakeRecorder := record.NewFakeRecorder(100)
		fakeHandler := volumetesting.NewBlockVolumePathHandler()
		ad := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
			fakeKubeClient,
			volumePluginMgr,
			fakeRecorder,
			fakeHandler))
		informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
		nodeLister := informerFactory.Core().V1().Nodes().Lister()
		nsu := statusupdater.NewFakeNodeStatusUpdater(false /* returnError */)
		rc := NewReconciler(
			reconcilerLoopPeriod, maxWaitForUnmountDuration, syncLoopPeriod, false, false, dsw, asw, ad, nsu, nodeLister, fakeRecorder)

		nodes := []k8stypes.NodeName{}
		for _, n := range test.nodes {
			dsw.AddNode(n.name)
			nodes = append(nodes, n.name)
			for _, podName := range n.podNames {
				volumeName := v1.UniqueVolumeName("volume-name")
				volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
				volumeSpec.PersistentVolume.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
				// 用节点名和 pod 名拼出唯一 UID，避免不同节点的同名 pod 在期望状态中冲突。
				uid := string(n.name) + "-" + podName // unique UID
				namespace, name := utilstrings.SplitQualifiedName(podName)
				pod := controllervolumetesting.NewPod(uid, name)
				pod.Namespace = namespace
				_, err := dsw.AddPod(types.UniquePodName(uid), pod, volumeSpec, n.name)
				if err != nil {
					t.Fatalf("Error adding pod %s to DSW: %s", podName, err)
				}
			}
		}
		// 只对 node1 上的卷调用上报，触发 multiattach 错误事件。
		// Act
		volumes := dsw.GetVolumesToAttach()
		for _, vol := range volumes {
			if vol.NodeName == "node1" {
				rc.(*reconciler).reportMultiAttachError(logger, vol, nodes)
			}
		}

		// Assert
		// 关闭事件通道后遍历全部事件，逐条比对；同时检查是否有缺失或额外事件。
		close(fakeRecorder.Events)
		index := 0
		for event := range fakeRecorder.Events {
			if len(test.expectedEvents) < index {
				t.Errorf("Test %q: unexpected event received: %s", test.name, event)
			} else {
				expectedEvent := test.expectedEvents[index]
				if expectedEvent != event {
					t.Errorf("Test %q: event %d: expected %q, got %q", test.name, index, expectedEvent, event)
				}
			}
			index++
		}
		for i := index; i < len(test.expectedEvents); i++ {
			t.Errorf("Test %q: event %d: expected %q, got none", test.name, i, test.expectedEvents[i])
		}
	}
}

// waitForMultiAttachErrorOnNode 轮询期望状态，等待 non-attached 节点上的卷被标记 multiattach 错误。
func waitForMultiAttachErrorOnNode(
	t *testing.T,
	attachedNode k8stypes.NodeName,
	dsow cache.DesiredStateOfWorld) {
	// 只要发现任一卷在非目标节点上且已上报 multiattach 错误即视为成功。
	multAttachCheckFunc := func() (bool, error) {
		for _, volumeToAttach := range dsow.GetVolumesToAttach() {
			if volumeToAttach.NodeName != attachedNode {
				if volumeToAttach.MultiAttachErrorReported {
					return true, nil
				}
			}
		}
		t.Logf("Warning: MultiAttach error not yet set on Node. Will retry.")
		return false, nil
	}

	err := retryWithExponentialBackOff(100*time.Millisecond, multAttachCheckFunc)
	if err != nil {
		t.Fatalf("Timed out waiting for MultiAttach Error to be set on non-attached node")
	}
}

// waitForNewAttacherCallCount 轮询等待假插件创建 attacher 的次数达到期望值。
func waitForNewAttacherCallCount(
	t *testing.T,
	expectedCallCount int,
	fakePlugin *volumetesting.FakeVolumePlugin) {
	err := retryWithExponentialBackOff(
		time.Duration(5*time.Millisecond),
		func() (bool, error) {
			actualCallCount := fakePlugin.GetNewAttacherCallCount()
			if actualCallCount >= expectedCallCount {
				return true, nil
			}
			t.Logf(
				"Warning: Wrong NewAttacherCallCount. Expected: <%v> Actual: <%v>. Will retry.",
				expectedCallCount,
				actualCallCount)
			return false, nil
		},
	)

	if err != nil {
		t.Fatalf(
			"Timed out waiting for NewAttacherCallCount. Expected: <%v> Actual: <%v>",
			expectedCallCount,
			fakePlugin.GetNewAttacherCallCount())
	}
}

// waitForNewDetacherCallCount 轮询等待假插件创建 detacher 的次数达到期望值。
func waitForNewDetacherCallCount(
	t *testing.T,
	expectedCallCount int,
	fakePlugin *volumetesting.FakeVolumePlugin) {
	err := retryWithExponentialBackOff(
		time.Duration(5*time.Millisecond),
		func() (bool, error) {
			actualCallCount := fakePlugin.GetNewDetacherCallCount()
			if actualCallCount >= expectedCallCount {
				return true, nil
			}
			t.Logf(
				"Warning: Wrong NewDetacherCallCount. Expected: <%v> Actual: <%v>. Will retry.",
				expectedCallCount,
				actualCallCount)
			return false, nil
		},
	)

	if err != nil {
		t.Fatalf(
			"Timed out waiting for NewDetacherCallCount. Expected: <%v> Actual: <%v>",
			expectedCallCount,
			fakePlugin.GetNewDetacherCallCount())
	}
}

// waitForAttachCallCount 轮询等待任一 attacher 的 attach 调用次数等于期望值；
// 若期望为零且没有 attacher 则直接返回，避免无意义等待。
func waitForAttachCallCount(
	t *testing.T,
	expectedAttachCallCount int,
	fakePlugin *volumetesting.FakeVolumePlugin) {
	// 没有 attacher 且期望次数为零时，断言自然成立。
	if len(fakePlugin.GetAttachers()) == 0 && expectedAttachCallCount == 0 {
		return
	}

	err := retryWithExponentialBackOff(
		time.Duration(5*time.Millisecond),
		func() (bool, error) {
			for i, attacher := range fakePlugin.GetAttachers() {
				actualCallCount := attacher.GetAttachCallCount()
				if actualCallCount == expectedAttachCallCount {
					return true, nil
				}
				t.Logf(
					"Warning: Wrong attacher[%v].GetAttachCallCount(). Expected: <%v> Actual: <%v>. Will try next attacher.",
					i,
					expectedAttachCallCount,
					actualCallCount)
			}

			t.Logf(
				"Warning: No attachers have expected AttachCallCount. Expected: <%v>. Will retry.",
				expectedAttachCallCount)
			return false, nil
		},
	)

	if err != nil {
		t.Fatalf(
			"No attachers have expected AttachCallCount. Expected: <%v>",
			expectedAttachCallCount)
	}
}

// waitForTotalAttachCallCount 与 waitForAttachCallCount 不同：累加所有 attacher 的调用次数后比较总数，
// 用于多节点场景下断言全局 attach 次数。
func waitForTotalAttachCallCount(
	t *testing.T,
	expectedAttachCallCount int,
	fakePlugin *volumetesting.FakeVolumePlugin) {
	// 没有 attacher 且期望次数为零时，断言自然成立。
	if len(fakePlugin.GetAttachers()) == 0 && expectedAttachCallCount == 0 {
		return
	}

	err := retryWithExponentialBackOff(
		time.Duration(5*time.Millisecond),
		func() (bool, error) {
			totalCount := 0
			for _, attacher := range fakePlugin.GetAttachers() {
				totalCount += attacher.GetAttachCallCount()
			}
			if totalCount == expectedAttachCallCount {
				return true, nil
			}
			t.Logf(
				"Warning: Wrong total GetAttachCallCount(). Expected: <%v> Actual: <%v>. Will retry.",
				expectedAttachCallCount,
				totalCount)

			return false, nil
		},
	)

	if err != nil {
		t.Fatalf(
			"Total AttachCallCount does not match expected value. Expected: <%v>",
			expectedAttachCallCount)
	}
}

// waitForDetachCallCount 轮询等待任一 detacher 的 detach 调用次数等于期望值。
func waitForDetachCallCount(
	t *testing.T,
	expectedDetachCallCount int,
	fakePlugin *volumetesting.FakeVolumePlugin) {
	// 没有 detacher 且期望次数为零时，断言自然成立。
	if len(fakePlugin.GetDetachers()) == 0 && expectedDetachCallCount == 0 {
		return
	}

	err := retryWithExponentialBackOff(
		time.Duration(5*time.Millisecond),
		func() (bool, error) {
			for i, detacher := range fakePlugin.GetDetachers() {
				actualCallCount := detacher.GetDetachCallCount()
				if actualCallCount == expectedDetachCallCount {
					return true, nil
				}
				t.Logf(
					"Wrong detacher[%v].GetDetachCallCount(). Expected: <%v> Actual: <%v>. Will try next detacher.",
					i,
					expectedDetachCallCount,
					actualCallCount)
			}

			t.Logf(
				"Warning: No detachers have expected DetachCallCount. Expected: <%v>. Will retry.",
				expectedDetachCallCount)
			return false, nil
		},
	)

	if err != nil {
		t.Fatalf(
			"No detachers have expected DetachCallCount. Expected: <%v>",
			expectedDetachCallCount)
	}
}

// waitForTotalDetachCallCount 累加所有 detacher 的调用次数后比较总数，用于多节点 detach 断言。
func waitForTotalDetachCallCount(
	t *testing.T,
	expectedDetachCallCount int,
	fakePlugin *volumetesting.FakeVolumePlugin) {
	// 没有 detacher 且期望次数为零时，断言自然成立。
	if len(fakePlugin.GetDetachers()) == 0 && expectedDetachCallCount == 0 {
		return
	}

	err := retryWithExponentialBackOff(
		time.Duration(5*time.Millisecond),
		func() (bool, error) {
			totalCount := 0
			for _, detacher := range fakePlugin.GetDetachers() {
				totalCount += detacher.GetDetachCallCount()
			}
			if totalCount == expectedDetachCallCount {
				return true, nil
			}
			t.Logf(
				"Warning: Wrong total GetDetachCallCount(). Expected: <%v> Actual: <%v>. Will retry.",
				expectedDetachCallCount,
				totalCount)

			return false, nil
		},
	)

	if err != nil {
		t.Fatalf(
			"Total DetachCallCount does not match expected value. Expected: <%v>",
			expectedDetachCallCount)
	}
}

// waitForAttachedToNodesCount 轮询等待卷被挂到指定数量的节点上。
func waitForAttachedToNodesCount(
	t *testing.T,
	expectedNodeCount int,
	volumeName v1.UniqueVolumeName,
	asw cache.ActualStateOfWorld) {

	err := retryWithExponentialBackOff(
		time.Duration(5*time.Millisecond),
		func() (bool, error) {
			count := len(asw.GetNodesForAttachedVolume(volumeName))
			if count == expectedNodeCount {
				return true, nil
			}
			t.Logf(
				"Warning: Wrong number of nodes having <%v> attached. Expected: <%v> Actual: <%v>. Will retry.",
				volumeName,
				expectedNodeCount,
				count)

			return false, nil
		},
	)

	if err != nil {
		count := len(asw.GetNodesForAttachedVolume(volumeName))
		t.Fatalf(
			"Wrong number of nodes having <%v> attached. Expected: <%v> Actual: <%v>",
			volumeName,
			expectedNodeCount,
			count)
	}
}

// verifyNewAttacherCallCount 断言 attacher 的创建次数为零（仅在 expectZero 为真时检查）。
func verifyNewAttacherCallCount(
	t *testing.T,
	expectZeroNewAttacherCallCount bool,
	fakePlugin *volumetesting.FakeVolumePlugin) {

	if expectZeroNewAttacherCallCount &&
		fakePlugin.GetNewAttacherCallCount() != 0 {
		t.Fatalf(
			"Wrong NewAttacherCallCount. Expected: <0> Actual: <%v>",
			fakePlugin.GetNewAttacherCallCount())
	}
}

// waitForVolumeAttachStateToNode 轮询等待卷在指定节点上达到期望的挂载状态（Attached/Uncertain 等）。
func waitForVolumeAttachStateToNode(
	t *testing.T,
	volumeName v1.UniqueVolumeName,
	nodeName k8stypes.NodeName,
	expectedAttachState cache.AttachState,
	asw cache.ActualStateOfWorld) {

	err := retryWithExponentialBackOff(
		// 挂载状态的转换由异步对账驱动，轮询间隔较长以容忍状态滞后。
		time.Duration(500*time.Millisecond),
		func() (bool, error) {
			attachState := asw.GetAttachState(volumeName, nodeName)
			if attachState == expectedAttachState {
				return true, nil
			}
			t.Logf("Warning: expected attach state: %v, actual attach state: %v. Will retry.",
				expectedAttachState, attachState)
			return false, nil
		},
	)

	// 超时后再读一次状态：若在等待期间已达成期望值，则视为通过，避免边界时刻的误判。
	attachState := asw.GetAttachState(volumeName, nodeName)
	if err != nil && attachState != expectedAttachState {
		t.Fatalf("Volume <%v> is not in expected attach state: %v, actual attach state: %v",
			volumeName, expectedAttachState, attachState)
	}

	t.Logf("Volume <%v> is attached to node <%v>: %v", volumeName, nodeName, expectedAttachState)
}

// waitForVolumeAddedToNode 轮询等待卷出现在实际状态（asw）的指定节点上。
func waitForVolumeAddedToNode(
	t *testing.T,
	volumeName v1.UniqueVolumeName,
	nodeName k8stypes.NodeName,
	asw cache.ActualStateOfWorld) {

	err := retryWithExponentialBackOff(
		time.Duration(500*time.Millisecond),
		func() (bool, error) {
			volumes := asw.GetAttachedVolumes()
			for _, volume := range volumes {
				if volume.VolumeName == volumeName && volume.NodeName == nodeName {
					return true, nil
				}
			}
			t.Logf(
				"Warning: Volume <%v> is not added to node  <%v> yet. Will retry.",
				volumeName,
				nodeName)

			return false, nil
		},
	)

	if err != nil {
		t.Fatalf(
			"Volume <%v> is not added to node  <%v>. %v",
			volumeName,
			nodeName, err)
	}
}

// waitForVolumeRemovedFromNode 轮询等待卷从实际状态（asw）的指定节点上移除。
func waitForVolumeRemovedFromNode(
	t *testing.T,
	volumeName v1.UniqueVolumeName,
	nodeName k8stypes.NodeName,
	asw cache.ActualStateOfWorld) {

	err := retryWithExponentialBackOff(
		time.Duration(500*time.Millisecond),
		func() (bool, error) {
			volumes := asw.GetAttachedVolumes()
			exist := false
			for _, volume := range volumes {
				if volume.VolumeName == volumeName && volume.NodeName == nodeName {
					exist = true
				}
			}
			if exist {
				t.Logf(
					"Warning: Volume <%v> is not removed from the node  <%v> yet. Will retry.",
					volumeName,
					nodeName)

				return false, nil
			}
			return true, nil

		},
	)

	if err != nil {
		t.Fatalf(
			"Volume <%v> is not removed from node  <%v>. %v",
			volumeName,
			nodeName, err)
	}
}

// verifyVolumeAttachedToNode 同步断言卷在指定节点上的挂载状态，不做等待。
func verifyVolumeAttachedToNode(
	t *testing.T,
	volumeName v1.UniqueVolumeName,
	nodeName k8stypes.NodeName,
	expectedAttachState cache.AttachState,
	asw cache.ActualStateOfWorld,
) {
	attachState := asw.GetAttachState(volumeName, nodeName)
	if attachState != expectedAttachState {
		t.Fatalf("Check volume <%v> is attached to node <%v>, got %v, expected %v",
			volumeName,
			nodeName,
			attachState,
			expectedAttachState)
	}
	t.Logf("Volume <%v> is attached to node <%v>: %v", volumeName, nodeName, attachState)
}

// verifyVolumeReportedAsAttachedToNode 轮询等待卷在节点上报列表中出现（或不出现），
// 用于验证 node status 的更新结果。
func verifyVolumeReportedAsAttachedToNode(
	t *testing.T,
	logger klog.Logger,
	volumeName v1.UniqueVolumeName,
	nodeName k8stypes.NodeName,
	isAttached bool,
	asw cache.ActualStateOfWorld,
	timeout time.Duration,
) {
	var result bool
	var lastErr error
	// 状态上报是异步的，用 PollUntilContextTimeout 在超时前反复检查。
	err := wait.PollUntilContextTimeout(context.TODO(), 50*time.Millisecond, timeout, false, func(context.Context) (done bool, err error) {
		volumes := asw.GetVolumesToReportAttached(logger)
		for _, volume := range volumes[nodeName] {
			if volume.Name == volumeName {
				result = true
			}
		}

		if result == isAttached {
			t.Logf("Volume <%v> is reported as attached to node <%v>: %v", volumeName, nodeName, result)
			return true, nil
		}
		lastErr = fmt.Errorf("Check volume <%v> is reported as attached to node <%v>, got %v, expected %v",
			volumeName,
			nodeName,
			result,
			isAttached)
		return false, nil
	})
	if err != nil {
		t.Fatalf("last error: %q, wait timeout: %q", lastErr, err.Error())
	}

}

// verifyVolumeNoStatusUpdateNeeded 断言卷未出现在需要上报状态更新的列表中。
func verifyVolumeNoStatusUpdateNeeded(
	t *testing.T,
	logger klog.Logger,
	volumeName v1.UniqueVolumeName,
	nodeName k8stypes.NodeName,
	asw cache.ActualStateOfWorld,
) {
	volumes := asw.GetVolumesToReportAttached(logger)
	for _, volume := range volumes[nodeName] {
		if volume.Name == volumeName {
			t.Fatalf("Check volume <%v> is reported as need to update status on node <%v>, expected false",
				volumeName,
				nodeName)
		}
	}
	t.Logf("Volume <%v> is not reported as need to update status on node <%v>", volumeName, nodeName)
}

// verifyNewDetacherCallCount 断言 detacher 的创建次数为零（仅在 expectZero 为真时检查）。
func verifyNewDetacherCallCount(
	t *testing.T,
	expectZeroNewDetacherCallCount bool,
	fakePlugin *volumetesting.FakeVolumePlugin) {

	if expectZeroNewDetacherCallCount &&
		fakePlugin.GetNewDetacherCallCount() != 0 {
		t.Fatalf("Wrong NewDetacherCallCount. Expected: <0> Actual: <%v>",
			fakePlugin.GetNewDetacherCallCount())
	}
}

// retryWithExponentialBackOff 使用固定参数的指数退避执行条件函数，供各 wait 辅助函数复用。
func retryWithExponentialBackOff(initialDuration time.Duration, fn wait.ConditionFunc) error {
	backoff := wait.Backoff{
		Duration: initialDuration,
		// Factor=3、Steps=6：约 100ms 起步，最多 6 次尝试，覆盖异步操作的典型收敛时间。
		Factor: 3,
		Jitter: 0,
		Steps:  6,
	}
	return wait.ExponentialBackoff(backoff, fn)
}

// 原注释：verifies the force detach metric with reason。
// verifies the force detach metric with reason
// testForceDetachMetric 校验给定原因（reason）的强制 detach 计数器是否等于期望值。
func testForceDetachMetric(t *testing.T, inputForceDetachMetricCounter int, reason string) {
	t.Helper()

	actualForceDetachMericCounter, err := metricstestutil.GetCounterMetricValue(metrics.ForceDetachMetricCounter.WithLabelValues(reason))
	if err != nil {
		t.Errorf("Error getting actualForceDetachMericCounter")
	}
	if actualForceDetachMericCounter != float64(inputForceDetachMetricCounter) {
		t.Errorf("Expected desiredForceDetachMericCounter to be %d, got %v", inputForceDetachMetricCounter, actualForceDetachMericCounter)
	}
}
